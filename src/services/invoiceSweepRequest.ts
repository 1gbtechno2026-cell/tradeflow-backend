import { Types } from "mongoose";
import { config } from "../config.js";
import { OrderInvoiceRequest, countKeyOf, type IInvoiceTask, type RequestTaskStatus } from "../models/OrderInvoiceRequest.js";
import { OrderJobRun } from "../models/OrderJobRun.js";
import { INVOICE_QUEUE } from "../queue.js";
import { resolveInvoiceTargets, type InvoiceTarget, type OrderDownloadResult } from "./invoiceOnDemand.js";
import { endOpenInvoiceTasks } from "./invoiceRequests.js";
import { classified, classifyMessage, countsOf, hostName, TIMED_OUT, type ClassifiedError } from "./requestCommon.js";
import { openRequestTasksQuietly, syncRequestTasksQuietly } from "./syncTaskLog.js";

/**
 * One run of the invoice sweep, recorded as a request of the log — the
 * reference's nightly "Invoice fetch" (source `system-cron`), with a task per
 * order, listed in Batch requests and All tasks like any invoice fetch.
 *
 * It RECORDS the sweep; it does not run it. The sweep (services/invoiceSweep.ts)
 * still picks its own orders, downloads them itself inside its one worker job
 * and does its own captured_at / settled bookkeeping. This file only writes
 * what the sweep tells it:
 *
 *   onPhase    the orders of a phase, before it starts → the request (made at
 *              the first phase that has any) and its tasks; a run with nothing
 *              to download makes no request
 *   onStart    an order is taken        → its task `in_progress`
 *   onResult   the order's answer       → `success`, or `failed` with the
 *              reference's error class ("Nothing available yet" while Flipkart
 *              has not issued it)
 *   stopped()  Batch requests → Cancel was pressed on the request — read every
 *              few seconds; the sweep gives up between two orders
 *   finish()   whatever is still open is closed, the request ends
 *
 * The request names the sweep's own queue (`order-invoice`), not the batch
 * queue, so the batch reaper and watchdog leave it alone. It cannot be
 * resumed: a sweep whose worker stopped under it is closed as timed out when
 * that worker starts again (closeInterruptedSweepRequests), and the next run
 * takes whatever is still missing — as the sweep always did.
 */

/** "refetch" = the late re-downloads of data-quality SO-5 (services/invoiceRefetch.ts); its count is on the request only when it had orders. */
export type SweepPhase = "capture" | "recapture" | "refetch";

export interface SweepRecorderDeps {
  resolveOrders: (userId: string, orderIds: string[]) => Promise<Map<string, InvoiceTarget>>;
  /** How often the request is read for a stop (ms). */
  stopPollMs: number;
}

const recorderDefaults: SweepRecorderDeps = { resolveOrders: resolveInvoiceTargets, stopPollMs: 3000 };

const col = () => OrderInvoiceRequest.collection;

/** What the sweep's own result says, as a task ending. */
export function taskEndOf(r: OrderDownloadResult): { status: "success" | "failed"; why: ClassifiedError | null; parts: string[]; sizeKb: number; detail: string } {
  if (r.status === "downloaded") return { status: "success", why: null, parts: r.parts || [], sizeKb: r.sizeKb || 0, detail: "" };
  if (r.status === "already") return { status: "success", why: null, parts: [], sizeKb: 0, detail: "Already stored — left as it is" };
  const why = r.status === "unknown_order" ? classified("order_not_found", "the order is not in Order Units") : r.status === "no_session" ? classified("not_logged_in", r.error || "no usable saved session for this Platform ID") : classifyMessage(r.error || "");
  return { status: "failed", why, parts: [], sizeKb: 0, detail: why.detail };
}

export function recordSweepAsRequest(userId: string, input: { source: "system-cron" | "user"; by: string; ledgerRunId?: string; capPerPhase: number }, overrides: Partial<SweepRecorderDeps> = {}) {
  const deps = { ...recorderDefaults, ...overrides };
  let requestId = "";
  let stopped = false;
  let poll: NodeJS.Timeout | null = null;
  const known = new Set<string>();
  const counted: Record<SweepPhase, number> = { capture: 0, recapture: 0, refetch: 0 };
  const refetchCount = () => (counted.refetch ? { refetch_count: counted.refetch } : {});
  const quiet = (what: string) => (err: unknown) => console.warn(`[invoice-sweep] request ${requestId.slice(-8) || "—"}: ${what} not recorded — ${err instanceof Error ? err.message.split("\n")[0] : err}`);

  const taskOf = (orderId: string, targets: Map<string, InvoiceTarget>): IInvoiceTask => {
    const t = targets.get(orderId);
    return { orderId, email: t?.email || "", accountId: t?.accountId || "", status: "queued", errorClass: "", error: "", detail: "", parts: [], sizeKb: 0, startedAt: null, finishedAt: null };
  };

  const onPhase = async (phase: SweepPhase, orderIds: string[]) => {
    const fresh = [...new Set(orderIds.map((id) => String(id).trim()).filter(Boolean))].filter((id) => !known.has(id));
    counted[phase] += orderIds.length;
    if (!fresh.length) return;
    try {
      const targets = await deps.resolveOrders(userId, fresh);
      const tasks = fresh.map((id) => taskOf(id, targets));
      for (const id of fresh) known.add(id);
      const now = new Date();
      if (!requestId) {
        const request = await OrderInvoiceRequest.create({
          userId: new Types.ObjectId(userId),
          platform: "FLIPKART",
          source: input.source,
          createdBy: input.by,
          status: "in_progress",
          message: "",
          params: { order_ids_count: tasks.length, force: false, sweep: true, capture_count: counted.capture, recapture_count: counted.recapture, ...refetchCount(), cap_per_phase: input.capPerPhase },
          tasks,
          counts: countsOf(tasks.map((t) => t.status)),
          queue: INVOICE_QUEUE,
          systemId: config.systemId,
          host: hostName(),
          workerHost: hostName(),
          startedAt: now,
          finishedAt: null,
        });
        requestId = String(request._id);
        await openRequestTasksQuietly("invoices", request.toObject());
        // The sweep's own ledger row points at the request, so the panel's row and Stop reach it.
        if (input.ledgerRunId) await OrderJobRun.updateOne({ _id: input.ledgerRunId }, { $set: { refId: requestId, "meta.request_id": requestId } }).catch(() => undefined);
        poll = setInterval(() => {
          void OrderInvoiceRequest.findById(requestId)
            .select("status")
            .lean()
            .then((row) => {
              if (row && row.status !== "queued" && row.status !== "in_progress") stopped = true;
            })
            .catch(() => undefined);
        }, deps.stopPollMs);
        poll.unref();
        console.log(`[invoice-sweep] request ${requestId}: ${tasks.length} order(s) to ${phase} (${input.source})`);
        return;
      }
      // A later phase of the same run: its orders join the request — while it is still open.
      const res = await col().updateOne(
        { _id: new Types.ObjectId(requestId), status: "in_progress" },
        { $push: { tasks: { $each: tasks } }, $inc: { "counts.total": tasks.length, "counts.queued": tasks.length, "params.order_ids_count": tasks.length }, $set: { "params.capture_count": counted.capture, "params.recapture_count": counted.recapture, ...(counted.refetch ? { "params.refetch_count": counted.refetch } : {}), updatedAt: now } } as Record<string, unknown>
      );
      if (res.modifiedCount) await syncRequestTasksQuietly("invoices", requestId);
    } catch (err) {
      quiet(`the ${phase} phase`)(err);
    }
  };

  const onStart = async (orderId: string) => {
    if (!requestId) return;
    const now = new Date();
    await col()
      .updateOne(
        { _id: new Types.ObjectId(requestId), status: "in_progress", tasks: { $elemMatch: { orderId, status: "queued" } } },
        { $set: { "tasks.$.status": "in_progress", "tasks.$.startedAt": now, updatedAt: now }, $inc: { "counts.queued": -1, "counts.in_progress": 1 } }
      )
      .catch(quiet(orderId));
  };

  const onResult = async (r: OrderDownloadResult) => {
    if (!requestId) return;
    const end = taskEndOf(r);
    const now = new Date();
    const set = {
      "tasks.$.status": end.status,
      "tasks.$.errorClass": end.why?.errorClass || "",
      "tasks.$.error": end.why?.error || "",
      "tasks.$.detail": end.detail,
      "tasks.$.parts": end.parts,
      "tasks.$.sizeKb": end.sizeKb,
      "tasks.$.finishedAt": now,
      updatedAt: now,
    };
    try {
      // The task as the sweep left it: taken (the usual case), or still queued — an order answered without being taken (not in Order Units, no session).
      for (const from of ["in_progress", "queued"] as const) {
        const res = await col().updateOne(
          { _id: new Types.ObjectId(requestId), tasks: { $elemMatch: { orderId: r.orderId, status: from } }, ...(from === "queued" ? { status: "in_progress" } : {}) },
          { $set: set, $inc: { [`counts.${from}`]: -1, [`counts.${countKeyOf(end.status as RequestTaskStatus)}`]: 1 } }
        );
        if (res.modifiedCount) break;
      }
    } catch (err) {
      quiet(r.orderId)(err);
    }
  };

  /** The run has ended (or broke with `err`): what is still open is closed and the request ends. */
  const finish = async (err?: unknown): Promise<{ requestId: string; stopped: boolean }> => {
    if (poll) clearInterval(poll);
    if (!requestId) return { requestId, stopped };
    try {
      const reason = err ? `The sweep stopped: ${err instanceof Error ? err.message.split("\n")[0].slice(0, 160) : String(err)}` : "The sweep ended before this order was taken";
      // A request the user stopped is closed already and this changes nothing on it.
      await endOpenInvoiceTasks(requestId, { status: "failed", ...pick(classified("internal_error", reason)) }, err ? reason : "", { alsoRunning: { status: "failed", ...pick(classified("internal_error", reason)) } });
      const row = await OrderInvoiceRequest.findById(requestId).select("status").lean();
      if (row?.status === "cancelled") stopped = true;
      await syncRequestTasksQuietly("invoices", requestId);
    } catch (e) {
      quiet("the end of the run")(e);
    }
    return { requestId, stopped };
  };

  return { onPhase, onStart, onResult, finish, stopped: () => stopped, requestId: () => requestId };
}

const pick = (c: ClassifiedError) => ({ errorClass: c.errorClass, error: c.error, detail: c.detail });

/**
 * (worker, invoice role, at start) A sweep request of this machine that is
 * still open belongs to a worker that is gone — the sweep is one job at a
 * time on one worker. Its unfinished orders time out, the request ends, and
 * its ledger row stops reading "running" (which would hold every later sweep).
 */
export async function closeInterruptedSweepRequests(): Promise<number> {
  const open = await OrderInvoiceRequest.find({ status: { $in: ["queued", "in_progress"] }, queue: INVOICE_QUEUE, systemId: config.systemId, host: hostName() })
    .select("_id userId")
    .lean();
  let closed = 0;
  for (const r of open) {
    const id = String(r._id);
    if (!(await endOpenInvoiceTasks(id, { status: "timedout", errorClass: "", error: TIMED_OUT }, TIMED_OUT, { alsoRunning: { status: "timedout", errorClass: "", error: TIMED_OUT } }))) continue;
    await OrderJobRun.updateOne({ userId: r.userId, type: "invoices", refId: id, status: { $in: ["queued", "running"] } }, { $set: { status: "failed", finishedAt: new Date(), message: "The worker restarted while this sweep was running — the next sweep takes what is left" } }).catch(() => undefined);
    await syncRequestTasksQuietly("invoices", id);
    closed++;
  }
  if (closed) console.log(`[invoice-sweep] closed ${closed} sweep request(s) an earlier worker left open`);
  return closed;
}
