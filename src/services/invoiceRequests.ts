import type { Job } from "bullmq";
import { Types } from "mongoose";
import type { APIRequestContext } from "playwright";
import { config } from "../config.js";
import { OrderInvoiceRequest, type IInvoiceTask, type IOrderInvoiceRequest, type RequestTaskStatus } from "../models/OrderInvoiceRequest.js";
import { OrderJobRun } from "../models/OrderJobRun.js";
import { PlatformId } from "../models/PlatformId.js";
import { getInvoiceBatchQueue, INVOICE_BATCH_QUEUE, type InvoiceBatchJobData } from "../queue.js";
import { sleep } from "./browser.js";
import { invoiceStoreEnabled } from "./invoiceCapture.js";
import { downloadAndStoreInvoice, resolveInvoiceTargets, type InvoiceTarget } from "./invoiceOnDemand.js";
import { openOrderApiSession } from "./orderApi.js";
import { cookiesFor } from "./orderFetch.js";
import {
  ABORTED_WHILE_RUNNING,
  assertValueLimit,
  CANCELLED_BEFORE_START,
  classified,
  classifyPlatformError,
  countsOf,
  distinct,
  emptyCounts,
  ERROR_CLASSES,
  hostName,
  openRequestRun,
  RequestInputError,
  requestProgress,
  splitValues,
  withLiveness,
  WORKER_NOT_PICKED_UP,
  workerAlive,
  writeRequestRun,
  type ClassifiedError,
  type LedgerModel,
} from "./requestCommon.js";

/**
 * "Download Order Invoices" as a batch — the reference's "Invoice fetch".
 *
 *   createInvoiceRequest      up to 2000 distinct order ids → one
 *                             order_invoice_requests document, one task per
 *                             order; each order is resolved to its account
 *                             (an order not in Order Units fails at once),
 *                             one queue job per account
 *   runInvoiceBatchJob        (worker) one account: session → for each of its
 *                             orders the proven store step of the by-order-id
 *                             download (invoiceOnDemand.downloadAndStoreInvoice:
 *                             zip-aware API download → S3 → registerInvoice →
 *                             units stamped), ALWAYS downloading again — the
 *                             button re-extracts — with the order's result and
 *                             the counts written as each one ends
 *   stopInvoiceRequest        queued orders → cancelled "Cancelled before it
 *                             started"; an order being downloaded finishes
 *   requeueStrandedInvoiceJobs  (worker) a request whose jobs were lost gets
 *                             them back; a job re-run takes only the orders
 *                             that have not ended
 *   failUnclaimedInvoiceRequests  (API) a request no worker took within a few
 *                             minutes, with no worker listening on the queue,
 *                             is failed: "The worker has not picked this up —
 *                             restart the worker"
 *
 * The ledger (order_job_runs, type "invoices", refId = request id) reads
 * `queued` until a worker takes the first order, `running` only while orders
 * are really being downloaded — that is what the 3-hourly sweep's
 * single-flight check waits on — and is closed by whoever ends the request.
 *
 * Speed: one order is ≈1.7 s of work (measured 2026-11-23: download, merge,
 * S3) plus the 0.25 s pause, one after another per account, accounts in
 * parallel. With INVOICE_BATCH_CONCURRENCY=12 and the orders spread over at
 * least 12 accounts that is ≈0.17 s per order — a 1,448-order batch in about
 * four minutes; a batch that sits on one account runs at ≈2 s per order.
 */

export const INVOICE_BATCH_CONCURRENCY = Math.max(1, Math.floor(Number(process.env.INVOICE_BATCH_CONCURRENCY) || 12));
/** Pause between two orders of one account (ms), as the by-order-id download pauses. */
const ORDER_PAUSE_MS = Math.max(0, Number(process.env.INVOICE_BATCH_PAUSE_MS ?? 250));
/** How long a request may wait for a worker before it is failed — only when no worker is listening. */
export const REQUEST_PICKUP_MS = Math.max(30_000, Number(process.env.REQUEST_PICKUP_MS) || 3 * 60 * 1000);
/** An open request quiet this long with jobs missing from the queue gets them back. */
const STRANDED_AFTER_MS = 2 * 60 * 1000;
const REAP_EVERY_MS = 60 * 1000;
const WATCHDOG_EVERY_MS = 30 * 1000;
const LEDGER_EVERY_MS = 2000;

const OPEN = ["queued", "in_progress"] as const;

/** What the batch needs from outside. The worker and the API use `invoiceBatchDefaults`; the checks swap parts of it. */
export interface InvoiceBatchDeps {
  /** order → owning account and its unit rows. */
  resolveOrders: (userId: string, orderIds: string[]) => Promise<Map<string, InvoiceTarget>>;
  /** The account's order-API session, or null when it has no usable saved session. */
  openSession: (accountId: string) => Promise<APIRequestContext | null>;
  /** One order: download → store → register → stamp. Throws on any failure. */
  download: (ctx: APIRequestContext, target: InvoiceTarget) => Promise<{ size: number; parts: string[] }>;
  storeReady: () => boolean;
  ledger: LedgerModel;
  queueName: string;
  pauseMs: number;
}

export const invoiceBatchDefaults: InvoiceBatchDeps = {
  resolveOrders: resolveInvoiceTargets,
  openSession: async (accountId) => {
    const acc = Types.ObjectId.isValid(accountId) ? await PlatformId.findById(accountId).lean() : null;
    const cookies = cookiesFor(acc as Parameters<typeof cookiesFor>[0]);
    if (!cookies.length) return null;
    try {
      return await openOrderApiSession(cookies);
    } catch {
      return null;
    }
  },
  download: downloadAndStoreInvoice,
  storeReady: invoiceStoreEnabled,
  ledger: OrderJobRun,
  queueName: INVOICE_BATCH_QUEUE,
  pauseMs: ORDER_PAUSE_MS,
};

const jobIdOf = (requestId: string, accountId: string) => `${requestId}-${accountId}`;
const col = () => OrderInvoiceRequest.collection;

// ── shape ───────────────────────────────────────────────────────────────────

type InvoiceRequestRow = Partial<IOrderInvoiceRequest> & { _id: unknown };

/** The request in the reference's batch shape; `tasks` only when asked for. */
export function publicInvoiceRequest(r: InvoiceRequestRow, withTasks = false) {
  const id = String(r._id);
  const counts = { ...emptyCounts(0), ...(r.counts || {}) };
  const open = r.status === "queued" || r.status === "in_progress";
  return {
    request_id: id,
    platform: r.platform || "FLIPKART",
    task_type: "invoice_fetch",
    source: r.source || "user",
    status: r.status || "queued",
    triggered_by: r.createdBy || "",
    parent_id: r.parentId || null,
    retry_of: r.retryOf || null,
    params: { order_ids_count: r.params?.order_ids_count ?? counts.total, force: Boolean(r.params?.force) },
    message: r.message || "",
    created_at: r.createdAt ?? null,
    started_at: r.startedAt ?? null,
    finished_at: r.finishedAt ?? null,
    counts,
    ...requestProgress(counts, r.startedAt, r.finishedAt),
    can_cancel: open,
    host: r.host || "",
    worker_host: r.workerHost || "",
    ...(withTasks
      ? {
          tasks: (r.tasks || []).map((t, i) => ({
            id: i + 1,
            request_id: id,
            platform: r.platform || "FLIPKART",
            task_type: "invoice_fetch",
            source: r.source || "user",
            status: t.status,
            email: t.email || "",
            order_id: t.orderId,
            unit_id: null,
            triggered_by: r.createdBy || "",
            created_at: r.createdAt ?? null,
            started_at: t.startedAt ?? null,
            finished_at: t.finishedAt ?? null,
            duration_seconds: t.startedAt && t.finishedAt ? Math.round((new Date(t.finishedAt).getTime() - new Date(t.startedAt).getTime()) / 10) / 100 : null,
            error: t.error || "",
            error_code: t.errorClass || "",
            detail: t.detail || "",
            parts: t.parts || [],
            size_kb: t.sizeKb || 0,
          })),
        }
      : {}),
  };
}

/** An aggregation expression that recounts `counts` from the tasks array — used where many tasks change in one write. */
function recountStage() {
  const n = (status: RequestTaskStatus) => ({ $size: { $filter: { input: "$tasks", as: "t", cond: { $eq: ["$$t.status", status] } } } });
  return {
    $set: {
      counts: {
        total: { $size: "$tasks" },
        queued: n("queued"),
        in_progress: n("in_progress"),
        success: n("success"),
        failed: n("failed"),
        cancelled: n("cancelled"),
        aborted: n("aborted"),
        timed_out: n("timedout"),
      },
    },
  };
}

/** A pipeline stage that ends every task in `from` with one status and one sentence. */
function endTasksStage(from: RequestTaskStatus[], patch: Partial<IInvoiceTask>) {
  // $literal: a sentence is data here, never a field path or an operator.
  const literal = Object.fromEntries(Object.entries(patch).map(([k, v]) => [k, { $literal: v }]));
  return { $set: { tasks: { $map: { input: "$tasks", as: "t", in: { $cond: [{ $in: ["$$t.status", from] }, { $mergeObjects: ["$$t", literal] }, "$$t"] } } } } };
}

// ── the ledger row ──────────────────────────────────────────────────────────

const ledgerWrittenAt = new Map<string, number>();

async function syncLedger(requestId: string, deps: InvoiceBatchDeps, opts: { final?: boolean; throttle?: boolean; parked?: string } = {}) {
  if (opts.throttle) {
    const last = ledgerWrittenAt.get(requestId) || 0;
    if (Date.now() - last < LEDGER_EVERY_MS) return;
  }
  ledgerWrittenAt.set(requestId, Date.now());
  const row = await OrderInvoiceRequest.findById(requestId)
    .select(opts.final ? "userId status counts startedAt finishedAt message tasks.orderId tasks.status tasks.error tasks.email" : "userId status counts startedAt finishedAt message")
    .lean();
  if (!row) return;
  const failedItems = opts.final
    ? (row.tasks || []).filter((t) => t.status === "failed" || t.status === "timedout" || t.status === "aborted").map((t) => ({ key: `${t.orderId}${t.email ? ` · ${t.email}` : ""}`, reason: t.error || t.status }))
    : undefined;
  await writeRequestRun(
    { userId: row.userId, type: "invoices", refId: requestId },
    { status: row.status, counts: { ...emptyCounts(0), ...row.counts }, startedAt: row.startedAt, finishedAt: row.finishedAt, message: row.status === "queued" || row.status === "in_progress" ? "" : row.message, parked: opts.parked, failedItems },
    deps.ledger,
    // The request has ended (whatever this caller thought when it started reading): this write may close the row.
    { final: row.status === "completed" || row.status === "cancelled" }
  );
  if (opts.final) ledgerWrittenAt.delete(requestId);
}

// ── create / read / stop ────────────────────────────────────────────────────

/** Order ids as typed → upper-cased, de-duplicated, in the order given. */
export function parseInvoiceOrderIds(value: unknown): string[] {
  return distinct(splitValues(value).map((id) => id.toUpperCase()));
}

export async function createInvoiceRequest(
  userId: string,
  input: { orderIds: unknown; force?: boolean; by?: string; source?: string; platform?: string },
  overrides: Partial<InvoiceBatchDeps> = {}
) {
  const deps = { ...invoiceBatchDefaults, ...overrides };
  const orderIds = parseInvoiceOrderIds(input.orderIds);
  if (!orderIds.length) throw new RequestInputError("Order IDs are required");
  assertValueLimit("Order IDs", orderIds);
  if (!deps.storeReady()) throw new RequestInputError("Invoice storage is not configured on this server (INVOICE_S3_BUCKET)", 503, "store_not_configured");
  const force = input.force === true;

  // Without force, an order another open request already holds is left to that request.
  let alreadyQueued: string[] = [];
  if (!force) {
    const held = await OrderInvoiceRequest.aggregate<{ _id: string }>([
      { $match: { userId: new Types.ObjectId(userId), status: { $in: [...OPEN] } } },
      { $unwind: "$tasks" },
      { $match: { "tasks.status": { $in: [...OPEN] }, "tasks.orderId": { $in: orderIds } } },
      { $group: { _id: "$tasks.orderId" } },
    ]);
    alreadyQueued = held.map((h) => h._id);
  }
  const skip = new Set(alreadyQueued);
  const wanted = orderIds.filter((id) => !skip.has(id));
  if (!wanted.length) throw new RequestInputError(`All ${orderIds.length} order(s) are already queued in another invoice request`, 409, "already_queued");

  const targets = await deps.resolveOrders(userId, wanted);
  const now = new Date();
  const tasks: IInvoiceTask[] = wanted.map((orderId) => {
    const t = targets.get(orderId);
    const base = { orderId, email: t?.email || "", accountId: t?.accountId || "", detail: "", parts: [], sizeKb: 0, startedAt: null };
    return t
      ? { ...base, status: "queued" as const, errorClass: "", error: "", finishedAt: null }
      : { ...base, status: "failed" as const, errorClass: "order_not_found", error: ERROR_CLASSES.order_not_found, finishedAt: now };
  });
  const counts = countsOf(tasks.map((t) => t.status));
  const accounts = distinct(tasks.filter((t) => t.status === "queued").map((t) => t.accountId));
  const nothingToRun = counts.queued === 0;

  const request = await OrderInvoiceRequest.create({
    userId: new Types.ObjectId(userId),
    platform: "FLIPKART",
    source: input.source || "user",
    createdBy: String(input.by || "").trim(),
    status: nothingToRun ? "completed" : "queued",
    message: nothingToRun ? "None of these orders is in Order Units" : "",
    params: { order_ids_count: orderIds.length, force },
    tasks,
    counts,
    queue: deps.queueName,
    systemId: config.systemId,
    host: hostName(),
    // startedAt is the moment a worker takes the first order; a request with nothing to run never starts.
    startedAt: null,
    finishedAt: nothingToRun ? now : null,
  });
  const requestId = String(request._id);
  await openRequestRun(
    { userId, type: "invoices", refId: requestId, trigger: "user", by: String(input.by || "").trim(), total: counts.total, message: `Invoice fetch · ${counts.total} order(s) waiting for the worker`, meta: { request: "invoice_fetch", platform: "FLIPKART", params: { order_ids_count: orderIds.length, force }, queue: deps.queueName } },
    deps.ledger
  ).catch(() => undefined);

  if (nothingToRun) {
    await syncLedger(requestId, deps, { final: true });
  } else {
    try {
      await getInvoiceBatchQueue(deps.queueName).addBulk(accounts.map((accountId) => ({ name: "account", data: { requestId, accountId }, opts: { jobId: jobIdOf(requestId, accountId) } })));
    } catch (err) {
      // Nothing can run a request that never reached the queue: end it here, with the reason.
      const reason = `Could not queue the request: ${err instanceof Error ? err.message.split("\n")[0].slice(0, 160) : String(err)}`;
      await endOpenTasks(request._id, { status: "failed", errorClass: "internal_error", error: ERROR_CLASSES.internal_error, detail: reason }, reason);
      await syncLedger(requestId, deps, { final: true });
      throw new RequestInputError(reason, 503, "queue_unavailable");
    }
  }
  console.log(`[invoice-batch] request ${requestId}: ${counts.queued} order(s) on ${accounts.length} account(s) queued, ${counts.failed} not found${alreadyQueued.length ? `, ${alreadyQueued.length} already queued elsewhere` : ""}${input.by ? ` (${input.by})` : ""}`);
  return {
    ...publicInvoiceRequest(request.toObject()),
    tasks_created: counts.total,
    accounts: accounts.length,
    not_found: counts.failed,
    already_queued: alreadyQueued.length,
    already_queued_order_ids: alreadyQueued.slice(0, 200),
  };
}

/** End every queued (and, when asked, running) task of an open request in one write, and close the request. */
async function endOpenTasks(id: Types.ObjectId, patch: { status: RequestTaskStatus; errorClass: string; error: string; detail?: string }, message: string, opts: { onlyIfQueued?: boolean; requestStatus?: "completed" | "cancelled"; alsoRunning?: Partial<IInvoiceTask> } = {}) {
  const now = new Date();
  const res = await col().updateOne({ _id: id, status: opts.onlyIfQueued ? "queued" : { $in: [...OPEN] } }, [
    endTasksStage(["queued"], { status: patch.status, errorClass: patch.errorClass, error: patch.error, detail: patch.detail || "", finishedAt: now }),
    ...(opts.alsoRunning ? [endTasksStage(["in_progress"], { ...opts.alsoRunning, finishedAt: now })] : []),
    recountStage(),
    { $set: { status: opts.requestStatus || "completed", message: { $literal: message }, finishedAt: now, updatedAt: now } },
  ]);
  return res.modifiedCount === 1;
}

/** Both reads say whether a worker is behind an open request (`worker_alive`, `stalled`, `stalled_message`). */
export async function getInvoiceRequest(userId: string, requestId: string) {
  if (!Types.ObjectId.isValid(requestId)) return null;
  const row = await OrderInvoiceRequest.findOne({ _id: requestId, userId }).lean();
  return row ? (await withLiveness([{ row: publicInvoiceRequest(row, true), queue: row.queue || INVOICE_BATCH_QUEUE }], "order"))[0] : null;
}

export async function listInvoiceRequests(userId: string, limit = 20) {
  const rows = await OrderInvoiceRequest.find({ userId }).sort({ createdAt: -1 }).limit(Math.min(100, Math.max(1, limit))).select("-tasks").lean();
  return withLiveness(rows.map((r) => ({ row: publicInvoiceRequest(r), queue: r.queue || INVOICE_BATCH_QUEUE })), "order");
}

/**
 * Stop: every order still queued becomes cancelled "Cancelled before it
 * started" and its job leaves the queue; an order being downloaded finishes.
 * With no worker alive nothing would ever finish those, so they are closed as
 * aborted. Stopping a request that has already ended changes nothing.
 */
export async function stopInvoiceRequest(userId: string, requestId: string, by = "", overrides: Partial<InvoiceBatchDeps> = {}) {
  const deps = { ...invoiceBatchDefaults, ...overrides };
  if (!Types.ObjectId.isValid(requestId)) return null;
  const row = await OrderInvoiceRequest.findOne({ _id: requestId, userId }).select("status queue tasks.accountId tasks.status").lean();
  if (!row) return null;
  let removed = 0;
  if (row.status === "queued" || row.status === "in_progress") {
    const alive = await workerAlive(row.queue || deps.queueName);
    const stopped = await endOpenTasks(
      new Types.ObjectId(requestId),
      { status: "cancelled", errorClass: "", error: CANCELLED_BEFORE_START },
      `Stopped by ${by || "a user"}`,
      { requestStatus: "cancelled", alsoRunning: alive ? undefined : { status: "aborted", error: ABORTED_WHILE_RUNNING } }
    );
    if (stopped) {
      await OrderInvoiceRequest.updateOne({ _id: requestId }, { $set: { stoppedBy: by } }).catch(() => undefined);
      const queue = getInvoiceBatchQueue(row.queue || deps.queueName);
      for (const accountId of distinct((row.tasks || []).filter((t) => t.status === "queued" || t.status === "in_progress").map((t) => t.accountId))) {
        // A job a worker holds cannot be removed (it throws); it ends by itself at its next order.
        const job = await queue.getJob(jobIdOf(requestId, accountId)).catch(() => null);
        if (job && (await job.remove().then(() => true, () => false))) removed++;
      }
      await syncLedger(requestId, deps, { final: true });
      console.log(`[invoice-batch] request ${requestId} stopped by ${by || "a user"} — ${removed} account job(s) removed from the queue`);
    }
  }
  const updated = await OrderInvoiceRequest.findById(requestId).select("-tasks").lean();
  return updated ? { ...publicInvoiceRequest(updated), removed } : null;
}

// ── worker ──────────────────────────────────────────────────────────────────

async function claimTask(id: Types.ObjectId, i: number): Promise<boolean> {
  const now = new Date();
  const res = await col().updateOne(
    { _id: id, status: { $in: [...OPEN] }, [`tasks.${i}.status`]: "queued" },
    { $set: { [`tasks.${i}.status`]: "in_progress", [`tasks.${i}.startedAt`]: now, status: "in_progress", updatedAt: now }, $inc: { "counts.queued": -1, "counts.in_progress": 1 } }
  );
  return res.modifiedCount === 1;
}

async function finishTask(id: Types.ObjectId, i: number, from: "queued" | "in_progress", end: { status: "success" } | ({ status: "failed" } & ClassifiedError), extra: { parts?: string[]; sizeKb?: number } = {}) {
  const now = new Date();
  const failed = end.status === "failed" ? end : null;
  await col().updateOne(
    // A queued task is ended without being claimed only while the request is open — a stopped request keeps its cancellations.
    { _id: id, [`tasks.${i}.status`]: from, ...(from === "queued" ? { status: { $in: [...OPEN] } } : {}) },
    {
      $set: {
        [`tasks.${i}.status`]: end.status,
        [`tasks.${i}.errorClass`]: failed?.errorClass || "",
        [`tasks.${i}.error`]: failed?.error || "",
        [`tasks.${i}.detail`]: failed?.detail || "",
        [`tasks.${i}.parts`]: extra.parts || [],
        [`tasks.${i}.sizeKb`]: extra.sizeKb || 0,
        [`tasks.${i}.finishedAt`]: now,
        updatedAt: now,
      },
      $inc: { [`counts.${from}`]: -1, [`counts.${end.status}`]: 1 },
    }
  );
}

/** Done once nothing is queued or running — the last job to finish closes the request and its ledger row. */
async function settleInvoiceRequest(requestId: string, deps: InvoiceBatchDeps) {
  const now = new Date();
  const res = await col().updateOne(
    { _id: new Types.ObjectId(requestId), status: { $in: [...OPEN] }, "counts.queued": 0, "counts.in_progress": 0 },
    { $set: { status: "completed", finishedAt: now, updatedAt: now } }
  );
  if (res.modifiedCount) {
    const row = await OrderInvoiceRequest.findById(requestId).select("counts").lean();
    console.log(`[invoice-batch] request ${requestId} finished: ${row?.counts.success ?? 0} downloaded, ${row?.counts.failed ?? 0} failed`);
  }
  await syncLedger(requestId, deps, { final: true });
}

/** One account of one request: its orders that have not ended, one after another over one session. */
export async function runInvoiceBatchJob(job: Job<InvoiceBatchJobData>, overrides: Partial<InvoiceBatchDeps> = {}): Promise<void> {
  const deps = { ...invoiceBatchDefaults, ...overrides };
  const { requestId, accountId } = job.data;
  if (!Types.ObjectId.isValid(requestId)) return;
  const id = new Types.ObjectId(requestId);
  const request = await OrderInvoiceRequest.findById(id).select("userId status tasks.orderId tasks.accountId tasks.status").lean();
  if (!request || (request.status !== "queued" && request.status !== "in_progress")) return;
  const userId = String(request.userId);
  const mine = request.tasks.map((t, i) => ({ i, orderId: t.orderId, status: t.status, accountId: t.accountId })).filter((t) => t.accountId === accountId && (t.status === "queued" || t.status === "in_progress"));
  if (!mine.length) {
    await settleInvoiceRequest(requestId, deps);
    return;
  }

  let ctx: APIRequestContext | null = null;
  try {
    // An order left "in progress" belongs to a run of this job that died (this job is the
    // account's only runner): it goes back to queued and is downloaded again.
    const stale = mine.filter((t) => t.status === "in_progress");
    if (stale.length) {
      const now = new Date();
      await col().bulkWrite(
        stale.map((t) => ({
          updateOne: {
            filter: { _id: id, [`tasks.${t.i}.status`]: "in_progress" },
            update: { $set: { [`tasks.${t.i}.status`]: "queued", [`tasks.${t.i}.startedAt`]: null, updatedAt: now }, $inc: { "counts.in_progress": -1, "counts.queued": 1 } },
          },
        })),
        { ordered: false }
      );
      console.log(`[invoice-batch] ${requestId.slice(-8)} / ${accountId}: ${stale.length} interrupted order(s) picked up again`);
    }
    await col().updateOne({ _id: id, startedAt: null }, { $set: { startedAt: new Date() } });
    await col().updateOne({ _id: id }, { $set: { workerHost: hostName() } });

    ctx = await deps.openSession(accountId);
    if (!ctx) {
      for (const t of mine) await finishTask(id, t.i, "queued", { status: "failed", ...classified("not_logged_in", "no usable saved session for this Platform ID") });
      return;
    }
    const targets = await deps.resolveOrders(userId, mine.map((t) => t.orderId));
    let sessionDead: ClassifiedError | null = null;
    for (const t of mine) {
      if (sessionDead) {
        // The session answered "not logged in" on an earlier order: the rest would only repeat it.
        await finishTask(id, t.i, "queued", { status: "failed", ...sessionDead });
        continue;
      }
      if (!(await claimTask(id, t.i))) {
        const fresh = await OrderInvoiceRequest.findById(id).select("status").lean();
        if (!fresh || (fresh.status !== "queued" && fresh.status !== "in_progress")) break; // stopped
        continue; // ended by someone else
      }
      await syncLedger(requestId, deps, { throttle: true });
      const target = targets.get(t.orderId);
      if (!target) {
        await finishTask(id, t.i, "in_progress", { status: "failed", ...classified("order_not_found", "the order is no longer in Order Units") });
        continue;
      }
      try {
        const stored = await deps.download(ctx, target);
        await finishTask(id, t.i, "in_progress", { status: "success" }, { parts: stored.parts, sizeKb: Math.round(stored.size / 1024) });
      } catch (err) {
        const why = classifyPlatformError(err);
        await finishTask(id, t.i, "in_progress", { status: "failed", ...why });
        if (why.errorClass === "not_logged_in") sessionDead = classified("not_logged_in", `session refused on ${t.orderId}`);
        // Flipkart asked for room: give it some before this account's next order.
        if (why.errorClass === "rate_limited") await sleep(5000);
      }
      await syncLedger(requestId, deps, { throttle: true });
      if (deps.pauseMs) await sleep(deps.pauseMs);
    }
  } finally {
    await ctx?.dispose().catch(() => undefined);
    await settleInvoiceRequest(requestId, deps);
  }
}

// ── reapers ─────────────────────────────────────────────────────────────────

/**
 * Worker side — the twin of the update reaper. A job is fire-and-forget; one
 * lost with its worker (or removed after stalling too often) would leave its
 * orders queued forever. An open request of this deployment that has been
 * quiet a while gets a job back for every account that still has orders to
 * run and no job on the queue. `staleMs` 0 is what a starting worker uses.
 */
export async function requeueStrandedInvoiceJobs(opts: { staleMs?: number; queueName?: string } = {}): Promise<number> {
  const queueName = opts.queueName || INVOICE_BATCH_QUEUE;
  const before = new Date(Date.now() - (opts.staleMs ?? STRANDED_AFTER_MS));
  const open = await OrderInvoiceRequest.find({ status: { $in: [...OPEN] }, systemId: config.systemId, queue: queueName, updatedAt: { $lte: before } })
    .select("_id tasks.accountId tasks.status")
    .lean();
  if (!open.length) return 0;
  const queue = getInvoiceBatchQueue(queueName);
  let requeued = 0;
  for (const r of open) {
    const requestId = String(r._id);
    for (const accountId of distinct((r.tasks || []).filter((t) => t.status === "queued" || t.status === "in_progress").map((t) => t.accountId))) {
      // A live job (waiting / active / delayed) means the account is already being handled.
      if (await queue.getJob(jobIdOf(requestId, accountId)).catch(() => null)) continue;
      await queue.add("account", { requestId, accountId }, { jobId: jobIdOf(requestId, accountId) }).catch(() => undefined);
      requeued++;
    }
  }
  if (requeued) console.log(`[invoice-batch] re-enqueued ${requeued} stranded account job(s) across ${open.length} open request(s)`);
  return requeued;
}

let reaping = false;

/** Start the periodic resume sweep (worker, invoice-batch role). */
export function startInvoiceRequestReaper(queueName = INVOICE_BATCH_QUEUE) {
  const tick = (staleMs?: number) => {
    if (reaping) return;
    reaping = true;
    void requeueStrandedInvoiceJobs({ staleMs, queueName })
      .catch((e) => console.error("[invoice-batch] reaper:", e instanceof Error ? e.message : e))
      .finally(() => (reaping = false));
  };
  tick(0);
  const t = setInterval(() => tick(), REAP_EVERY_MS);
  t.unref();
  return t;
}

/**
 * API side. A worker started before this queue existed never takes its jobs,
 * and says nothing about it. With no worker heartbeat on the queue:
 *   · a request nobody has started within `pickupMs` is failed — every order
 *     "The worker has not picked this up — restart the worker" — and its jobs
 *     are removed, so a later restart does not run a request already answered;
 *   · a request that was running is left resumable, but its ledger row stops
 *     reading `running` (the 3-hourly sweep's single-flight check reads that
 *     row) until a worker takes it up again.
 * With a heartbeat nothing is touched: the request is only waiting its turn.
 */
export async function failUnclaimedInvoiceRequests(opts: { pickupMs?: number; queueName?: string; ledger?: LedgerModel } = {}): Promise<{ failed: number; parked: number }> {
  const queueName = opts.queueName || INVOICE_BATCH_QUEUE;
  const deps = { ...invoiceBatchDefaults, queueName, ...(opts.ledger ? { ledger: opts.ledger } : {}) };
  const out = { failed: 0, parked: 0 };
  const before = new Date(Date.now() - (opts.pickupMs ?? REQUEST_PICKUP_MS));
  const waiting = await OrderInvoiceRequest.find({ status: { $in: [...OPEN] }, systemId: config.systemId, queue: queueName, updatedAt: { $lte: before } })
    .select("_id userId status counts tasks.accountId tasks.status")
    .lean();
  if (!waiting.length || (await workerAlive(queueName))) return out;
  const queue = getInvoiceBatchQueue(queueName);
  for (const r of waiting) {
    const requestId = String(r._id);
    if (r.status === "queued") {
      const ended = await endOpenTasks(r._id as Types.ObjectId, { status: "failed", errorClass: "internal_error", error: WORKER_NOT_PICKED_UP }, WORKER_NOT_PICKED_UP, { onlyIfQueued: true });
      if (!ended) continue;
      for (const accountId of distinct((r.tasks || []).filter((t) => t.status === "queued").map((t) => t.accountId))) {
        const job = await queue.getJob(jobIdOf(requestId, accountId)).catch(() => null);
        await job?.remove().catch(() => undefined);
      }
      await syncLedger(requestId, deps, { final: true, parked: WORKER_NOT_PICKED_UP });
      out.failed++;
      console.warn(`[invoice-batch] request ${requestId} failed: ${WORKER_NOT_PICKED_UP}`);
    } else {
      const left = (r.counts?.queued || 0) + (r.counts?.in_progress || 0);
      const parked = await deps.ledger.updateOne(
        { userId: r.userId, type: "invoices", refId: requestId, status: "running" },
        { $set: { status: "failed", finishedAt: new Date(), message: `The worker stopped with ${left} order(s) left — restart the worker to resume` } }
      );
      if (parked.modifiedCount) out.parked++;
    }
  }
  return out;
}

/** Start the watchdog (API process). */
export function startInvoiceRequestWatchdog() {
  let busy = false;
  const t = setInterval(() => {
    if (busy) return;
    busy = true;
    void failUnclaimedInvoiceRequests()
      .catch((e) => console.error("[invoice-batch] watchdog:", e instanceof Error ? e.message : e))
      .finally(() => (busy = false));
  }, WATCHDOG_EVERY_MS);
  t.unref();
  return t;
}
