/**
 * Batch requests → Cancel and Retry, and the chained requests — WITHOUT
 * Flipkart, without a worker, and without ever putting a job on a live queue.
 *
 *   · Cancel one request by id, for every kind: what was queued ends
 *     "Cancelled before it started", what was running finishes or is cut off,
 *     the request reads `cancelled` and keeps its tally
 *   · Retry: the reference's two bodies and ours (one error class), exactly the chosen tasks' items, the skip of
 *     items already queued or running, source `system-retry`, parent / child
 *     links, who pressed it, and each kind's own refusals in its own words
 *   · the chains: after order → one update per burst (debounce, ceiling, skip),
 *     delivered → one invoice fetch per finished update, a sweep run recorded
 *     as a request — each with its switch on and off
 *
 * What is real: Mongo and, for the two kinds that have a queue of their own,
 * Redis.
 *   update, invoice fetch, coupons   their real collections, rows of an INVENTED
 *                                    workspace id, deleted at the end. Invoice
 *                                    and coupon requests are really created and
 *                                    really stopped — on THROWAWAY queue names,
 *                                    obliterated at the end. An update is really
 *                                    stopped (its stop only reads the live queue
 *                                    for jobs that do not exist); no update is
 *                                    ever created through the real function,
 *                                    which would enqueue on the live queue.
 *   fetch, cancel, reschedule        THROWAWAY collections (verify_ra_<run>_…)
 *                                    with the real schemas, dropped at the end.
 *                                    No cancel or reschedule request exists in
 *                                    the real collections at any moment, and the
 *                                    functions that would start one are replaced
 *                                    by recorders — except where the real one is
 *                                    called to show that it REFUSES (a past date,
 *                                    units that are not in Order Units), which it
 *                                    does before writing anything.
 *   the task log                     the real collection, rows of the invented
 *                                    workspace, deleted at the end.
 *
 *   npm run test:request-actions
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import mongoose, { Types, type Model } from "mongoose";
import { config } from "../config.js";
import { CancelRequest } from "../models/CancelRequest.js";
import { OrderCouponRequest, OrderCouponTask } from "../models/OrderCouponRequest.js";
import { OrderInvoiceRequest, type RequestCounts } from "../models/OrderInvoiceRequest.js";
import { OrderJobRun, type IOrderJobRun } from "../models/OrderJobRun.js";
import { OrderSyncTask } from "../models/OrderSyncTask.js";
import { OrderUpdateRequest, type IOrderUpdateRequest } from "../models/OrderUpdateRequest.js";
import { RescheduleRequest } from "../models/RescheduleRequest.js";
import { getCouponQueue, getInvoiceBatchQueue, INVOICE_QUEUE } from "../queue.js";
import { afterOrderDefaults, makeAfterOrderChain } from "../services/afterOrderChain.js";
import { createCouponRequest, stopCouponRequest, type CouponDeps } from "../services/couponRequests.js";
import { createInvoiceRequest, listInvoiceRequests, stopInvoiceRequest, type InvoiceBatchDeps } from "../services/invoiceRequests.js";
import { recordSweepAsRequest, taskEndOf } from "../services/invoiceSweepRequest.js";
import { runInvoiceSweep } from "../services/invoiceSweep.js";
import { reserveOrderFetch } from "../services/orderFetch.js";
import type { CancelRequestInput } from "../services/orderCancel.js";
import type { RescheduleRequestInput } from "../services/orderReschedule.js";
import { currentUpdateRequestId, type PreparedUpdateUnit } from "../services/orderUpdateQueue.js";
import { CHAIN_AFTER_ORDER, CHAIN_DELIVERED_CHECK, CHAIN_INVOICE_SWEEP, deliveredCheckDefaults, noteDeliveredOrder, queueInvoicesAfterUpdate } from "../services/requestChains.js";
import { ABORTED_WHILE_RUNNING, CANCELLED_BEFORE_START, clearedByStop, RequestInputError, TIMED_OUT } from "../services/requestCommon.js";
import { holdUpdatePreparation, UPDATE_PREPARING_MESSAGE, type FetchBody } from "../services/startRequests.js";
import { actionDefaults, parseRetryBody, retryBatch, stopBatch, type ActionDeps, type UnitsQueue } from "../services/syncActions.js";
import { getBatch, listBatches, listTasks, shortId, type LiveProbes } from "../services/syncBatches.js";
import { logCounts, openFetchTasks, openTasksAmong, syncRequestTasks, taskLog, type LogDeps } from "../services/syncTaskLog.js";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** The switches are read when their module loads, so "off" is checked in a process started with them off. */
if (process.argv.includes("--switches")) {
  console.log(JSON.stringify({ after_order: afterOrderDefaults.enabled, delivered_check: deliveredCheckDefaults.enabled, invoice_sweep: CHAIN_INVOICE_SWEEP }));
  process.exit(0);
}

let passed = 0;
const failures: string[] = [];

async function check(name: string, fn: () => void | Promise<void>) {
  try {
    await fn();
    passed++;
    console.log(`  ok    ${name}`);
  } catch (err) {
    failures.push(name);
    console.log(`  FAIL  ${name}\n        ${err instanceof Error ? err.message.split("\n").slice(0, 10).join("\n        ") : err}`);
  }
}

/** The refusal a call makes: its sentence, status and code. */
async function refusal(fn: () => Promise<unknown>): Promise<{ message: string; status: number; code: string }> {
  try {
    await fn();
  } catch (err) {
    return { message: err instanceof Error ? err.message : String(err), status: err instanceof RequestInputError ? err.status : 0, code: err instanceof RequestInputError ? err.code : "" };
  }
  throw new Error("expected a refusal, and the call went through");
}

const oid = () => new Types.ObjectId();
const MIN = 60_000;
const DAY = 24 * 60 * MIN;
const ago = (ms: number) => new Date(Date.now() - ms);
const run = Date.now().toString(36);
const names: string[] = [];

function throwaway<T>(real: Model<T>, name: string): Model<T> {
  const coll = `verify_ra_${run}_${name}`;
  names.push(coll);
  return mongoose.model<T>(`VerifyRa${name}${run}`, real.schema, coll) as Model<T>;
}

const HOST = "verify-host";
const PID = 4242;
const SYSTEM = "verify-system";
const counts = (c: Partial<RequestCounts>) => ({ total: 0, queued: 0, in_progress: 0, success: 0, failed: 0, cancelled: 0, aborted: 0, timed_out: 0, ...c });

async function main() {
  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false });
  const userId = String(oid()); // a workspace that does not exist
  const uid = new Types.ObjectId(userId);
  const invoiceQueue = `verify-ra-invoice-${run}`;
  const couponQueue = `verify-ra-coupon-${run}`;
  const Ledger = throwaway(OrderJobRun, "ledger") as Model<IOrderJobRun>;
  const log: LogDeps = { Tasks: OrderSyncTask, Ledger, Update: OrderUpdateRequest, Invoice: OrderInvoiceRequest, Coupon: OrderCouponRequest, CouponTask: OrderCouponTask, Cancel: throwaway(CancelRequest, "cancel"), Reschedule: throwaway(RescheduleRequest, "reschedule") };
  const raw = <T>(m: Model<T>) => m.collection;

  // What the running system would answer, set by each check.
  const aliveQueues = new Set<string>();
  let fetchLive = false;
  const probes: LiveProbes = { queueHasWorker: async (q) => aliveQueues.has(q), fetchLive: () => fetchLive, host: HOST, pid: PID, systemId: SYSTEM };

  // The invoice and coupon batches run for real — on throwaway queues, with invented orders and accounts.
  const invoiceDeps: Partial<InvoiceBatchDeps> = {
    queueName: invoiceQueue,
    ledger: Ledger,
    storeReady: () => true,
    resolveOrders: async (u, orderIds) => new Map(orderIds.filter((id) => !id.startsWith("ODGHOST")).map((id) => [id, { userId: u, accountId: `acc-${id.slice(-1)}`, email: `inv${id.slice(-1)}@verify.invalid`, orderId: id, unitIds: [`${id}1`], rowIds: [] }])),
  };
  const couponDeps: Partial<CouponDeps> = {
    queueName: couponQueue,
    ledger: Ledger,
    listAccounts: async (_u, emails) => (emails || []).filter((e) => /^cp\d+@verify\.invalid$/.test(e)).map((email) => ({ email, accountId: `acc-${email.split("@")[0]}` })),
  };

  // Recorders in place of the starts that must never run here: a fetch, an update, a cancel, a reschedule.
  // Each writes the request the real function would — already ended, so nothing could ever take it.
  const calls: { fetch: Array<{ body: FetchBody; extra: { source: string; params: Record<string, unknown> } }>; update: Array<{ units: PreparedUpdateUnit[]; opts: { by: string; parentId: string; params: Record<string, unknown> } }>; cancel: CancelRequestInput[]; reschedule: RescheduleRequestInput[]; stopFetch: string[]; freed: Array<{ kind: string; requestId: string; unitIds: string[] }> } = { fetch: [], update: [], cancel: [], reschedule: [], stopFetch: [], freed: [] };
  const jobs = new Map<string, "active" | "waiting">();
  const unitsQueue: UnitsQueue = {
    state: async (jobId) => jobs.get(jobId) || "missing",
    remove: async (jobId) => {
      if (jobs.get(jobId) === "active") return false;
      jobs.delete(jobId);
      return true;
    },
  };
  let liveFetchRun: { id: string; finish: (cancelled: boolean) => { counts: RequestCounts; written: Promise<void> } } | null = null;
  const deps: Partial<ActionDeps> = {
    log,
    probes,
    unitsQueue: () => unitsQueue,
    freeOrderRows: async (kind, _u, requestId, unitIds) => {
      calls.freed.push({ kind, requestId, unitIds: [...unitIds].sort() });
      return unitIds.length;
    },
    // Force kill, as the real run ends: its accounts closed in the log, its row closed as stopped with the counts.
    stopFetch: async (u) => {
      calls.stopFetch.push(u);
      if (!liveFetchRun) return;
      const end = liveFetchRun.finish(true);
      await end.written;
      await Ledger.updateOne({ _id: liveFetchRun.id }, { $set: { status: "stopped", finishedAt: new Date(), "meta.task_counts": end.counts } });
      fetchLive = false;
    },
    stopInvoice: (u, id, by) => stopInvoiceRequest(u, id, by, invoiceDeps),
    stopCoupon: (u, id, by) => stopCouponRequest(u, id, by, couponDeps),
    startInvoice: (u, input) => createInvoiceRequest(u, input, invoiceDeps),
    startCoupon: (u, input) => createCouponRequest(u, input, couponDeps),
    startFetch: async (u, body, extra) => {
      calls.fetch.push({ body, extra });
      const _id = oid();
      const now = new Date();
      await raw(Ledger).insertOne({ _id, userId: new Types.ObjectId(u), type: "fetch", trigger: body.trigger, by: body.by, status: "completed", total: body.emails.length, processed: body.emails.length, success: body.emails.length, failed: 0, cancelled: 0, avgMsPerItem: 0, message: "", failedItems: [], refId: "", startedAt: now, finishedAt: now, createdAt: now, updatedAt: now, meta: { source: extra.source, host: HOST, pid: PID, params: { platform: "FLIPKART", selection_method: "email_ids", email_ids_count: body.emails.length, fetch_orders_after_date: body.afterDate, force_fetch: body.forceFetch, parent_id: body.parentId, follows: body.follows, ...extra.params }, task_counts: counts({ total: body.emails.length, success: body.emails.length }) } });
      return { request_id: String(_id), tasks_created: body.emails.length };
    },
    startUpdate: async (u, units, opts) => {
      calls.update.push({ units, opts });
      const _id = oid();
      const now = new Date();
      const byEmail = new Map<string, PreparedUpdateUnit[]>();
      for (const x of units) byEmail.set(x.email, [...(byEmail.get(x.email) || []), x]);
      await raw(OrderUpdateRequest).insertOne({ _id, userId: new Types.ObjectId(u), source: "system-retry", selectionMethod: "unit_ids", params: opts.params, parentId: opts.parentId, systemId: SYSTEM, deliveredOrderIds: [], createdBy: opts.by, status: "done", mode: "api", windows: 1, accounts: [...byEmail].map(([email, list]) => ({ email, platformId: null, units: list.map((x) => ({ orderId: x.orderId, itemId: x.itemId, unitId: x.unitId, orderUrl: x.orderUrl })), total: list.length, status: "done", done: list.length, failed: 0, statusChanged: 0, otpNew: 0, via: "api", message: "", startedAt: now, finishedAt: now })), logs: [], startedAt: now, finishedAt: now, createdAt: now, updatedAt: now });
      return { id: String(_id), units: units.length };
    },
    startCancel: async (u, input) => {
      calls.cancel.push(input);
      const _id = oid();
      const now = new Date();
      const units = input.ids.map((unitId) => ({ orderId: `OD${unitId}`, unitId, itemId: "", email: "c@verify.invalid", productName: "", status: input.dryRun ? "dry_run" : "cancelled", message: "", startedAt: now, finishedAt: now }));
      await raw(log.Cancel).insertOne({ _id, userId: new Types.ObjectId(u), type: input.type, input: input.ids, dryRun: Boolean(input.dryRun), urgent: Boolean(input.urgent), createdBy: input.by || "", retryOf: input.retryOf || "", source: input.source || "", systemId: SYSTEM, stoppedBy: "", stoppedAt: null, status: "done", units, orders: [], rejected: [], finishedAt: now, createdAt: now, updatedAt: now });
      return { id: String(_id), units, rejected: [] };
    },
    startReschedule: async (u, input) => {
      calls.reschedule.push(input);
      const _id = oid();
      const now = new Date();
      const units = input.ids.map((unitId) => ({ orderId: `OD${unitId}`, unitId, itemId: "", email: "r@verify.invalid", productName: "", status: "rescheduled", message: "", offeredDates: [], chosen: input.date, token: "", startedAt: now, finishedAt: now }));
      await raw(log.Reschedule).insertOne({ _id, userId: new Types.ObjectId(u), type: input.type, input: input.ids, date: input.date, createdBy: input.by || "", retryOf: input.retryOf || "", source: input.source || "", systemId: SYSTEM, stoppedBy: "", stoppedAt: null, confirm: false, status: "done", units, rejected: [], finishedAt: now, createdAt: now, updatedAt: now });
      return { id: String(_id), units, rejected: [] };
    },
  };
  const batchOf = async (id: string) => (await getBatch(userId, id, log, probes))!;
  const rowsOf = async (id: string) => OrderSyncTask.find({ requestId: id }).select("key email orderId unitId status error errorCode source triggeredBy -_id").lean();
  const byKey = async (id: string) => Object.fromEntries((await rowsOf(id)).map((r) => [r.key, r]));
  console.log(`invented workspace ${userId} · throwaway collections verify_ra_${run}_* · throwaway queues ${invoiceQueue}, ${couponQueue}\n`);

  try {
    // ── CANCEL ──────────────────────────────────────────────────────────────
    console.log("CANCEL — POST /sync/batches/:id/cancel");
    const ledgerBase = { userId: uid, type: "fetch", trigger: "user", by: "asha", total: 0, processed: 0, success: 0, failed: 0, cancelled: 0, avgMsPerItem: 0, message: "", failedItems: [], refId: "" };
    const fetchParams = { platform: "FLIPKART", selection_method: "email_ids", email_ids_count: 4, fetch_orders_after_date: "2026-11-22", force_fetch: true, enable_login: false, run_update_after_fetch: true, invoices: "off" };

    await check("fetch running in this process: Force kill is called; accounts not reached end cancelled, the one in flight aborted; the batch reads cancelled and keeps its tally", async () => {
      const _id = oid();
      const createdAt = ago(2 * MIN);
      await raw(Ledger).insertOne({ ...ledgerBase, _id, status: "running", total: 4, startedAt: createdAt, finishedAt: null, createdAt, updatedAt: new Date(), meta: { host: HOST, pid: PID, platform: "FLIPKART", params: fetchParams } });
      const emails = ["a1", "a2", "a3", "a4"].map((n) => `${n}@verify.invalid`);
      const t = openFetchTasks({ userId, runId: String(_id), source: "user", by: "asha", emails, skipped: [], createdAt }, log);
      t.started(emails[0]);
      t.succeeded(emails[0]);
      t.started(emails[1]);
      await taskLog.flush();
      liveFetchRun = { id: String(_id), finish: t.finish };
      fetchLive = true;
      assert.deepEqual([(await batchOf(String(_id))).can_cancel, (await batchOf(String(_id))).can_retry], [true, false]);
      const out = (await stopBatch(userId, String(_id), "ravi", deps))!;
      assert.deepEqual(calls.stopFetch, [userId]);
      assert.deepEqual([out.message, out.status, out.can_cancel, out.can_retry, out.cancelled_by], ["Request cancelled", "cancelled", false, true, "ravi"]);
      assert.deepEqual(out.counts, counts({ total: 4, success: 1, aborted: 1, cancelled: 2 }));
      assert.deepEqual([out.finished_count, out.progress_percent], [4, 100], "progress 100% once it has ended");
      const rows = await byKey(String(_id));
      assert.deepEqual([rows[emails[1]].status, rows[emails[1]].error, rows[emails[2]].status, rows[emails[2]].error], ["aborted", ABORTED_WHILE_RUNNING, "cancelled", CANCELLED_BEFORE_START]);
      liveFetchRun = null;
    });
    await check("fetch whose process is gone (this machine, another pid): nothing to kill — the row is closed as stopped and its open accounts with it", async () => {
      const _id = oid();
      const createdAt = ago(3 * MIN);
      await raw(Ledger).insertOne({ ...ledgerBase, _id, status: "running", total: 3, startedAt: createdAt, finishedAt: null, createdAt, updatedAt: createdAt, meta: { host: HOST, pid: PID + 1, platform: "FLIPKART", params: fetchParams, task_counts: counts({ total: 3, queued: 1, in_progress: 1, success: 1 }) } });
      const emails = ["b1", "b2", "b3"].map((n) => `${n}@verify.invalid`);
      const t = openFetchTasks({ userId, runId: String(_id), source: "user", by: "asha", emails, skipped: [], createdAt }, log);
      t.started(emails[0]);
      t.succeeded(emails[0]);
      t.started(emails[1]);
      await taskLog.flush();
      calls.stopFetch.length = 0;
      const out = (await stopBatch(userId, String(_id), "ravi", deps))!;
      assert.equal(calls.stopFetch.length, 0, "Force kill is not called for a run this process does not have");
      assert.deepEqual([out.status, out.message], ["cancelled", "Request cancelled"]);
      assert.deepEqual(out.counts, counts({ total: 3, success: 1, aborted: 1, cancelled: 1 }));
      const row = await Ledger.findById(_id).lean();
      assert.deepEqual([row?.status, row?.message], ["stopped", "Stopped by ravi — no process was running it"]);
    });
    await check("fetch on another machine: not this machine's to stop — can_cancel false with the reason, the action answers 409", async () => {
      const _id = oid();
      const createdAt = ago(MIN);
      await raw(Ledger).insertOne({ ...ledgerBase, _id, status: "running", total: 2, startedAt: createdAt, finishedAt: null, createdAt, updatedAt: new Date(), meta: { host: "other-mac", pid: 7, platform: "FLIPKART", params: fetchParams, task_counts: counts({ total: 2, queued: 2 }) } });
      const b = await batchOf(String(_id));
      assert.deepEqual([b.can_cancel, b.cancel_note], [false, "This fetch is running on other-mac — cancel it from that machine"]);
      assert.deepEqual(await refusal(() => stopBatch(userId, String(_id), "ravi", deps)), { message: "This fetch is running on other-mac — cancel it from that machine", status: 409, code: "other_machine" });
      assert.equal((await Ledger.findById(_id).lean())?.status, "running", "untouched");
      await Ledger.updateOne({ _id }, { $set: { status: "completed", finishedAt: new Date() } });
    });

    const unit = (orderId: string, unitId: string) => ({ orderId, itemId: "", unitId, orderUrl: "" });
    const updateIds: Record<string, string> = {};
    await check("update — its real stop (cancelUpdateRequest): queued accounts cancelled 'Cancelled before it started', a running one is left to finish its unit — the batch reads in progress ('Cancelling') until it has, then cancelled", async () => {
      const _id = oid();
      updateIds.stopped = String(_id);
      const createdAt = ago(2 * MIN);
      const acc = (email: string, status: string, units: ReturnType<typeof unit>[], done: number, extra: Record<string, unknown> = {}) => ({ email, platformId: null, units, total: units.length, status, done, failed: 0, statusChanged: 0, otpNew: 0, via: "", message: "", startedAt: status === "queued" ? null : createdAt, finishedAt: status === "done" ? createdAt : null, ...extra });
      await raw(OrderUpdateRequest).insertOne({
        _id, userId: uid, source: "manual", selectionMethod: "default", params: { platform: "FLIPKART", selection_method: "default", force: false }, parentId: "", systemId: SYSTEM, deliveredOrderIds: [], createdBy: "asha", status: "running", mode: "api", windows: 3,
        accounts: [acc("u1@verify.invalid", "done", [unit("ODU1", "U11"), unit("ODU1", "U12")], 2), acc("u2@verify.invalid", "running", [unit("ODU2", "U21"), unit("ODU2", "U22")], 1), acc("u3@verify.invalid", "queued", [unit("ODU3", "U31"), unit("ODU3", "U32"), unit("ODU4", "U41")], 0)],
        logs: [], startedAt: createdAt, finishedAt: null, createdAt, updatedAt: new Date(),
      });
      assert.equal((await batchOf(String(_id))).can_cancel, true);
      const out = (await stopBatch(userId, String(_id), "ravi", deps))!;
      assert.deepEqual([out.status, out.can_cancel, out.can_retry, out.message, out.cancel_note], ["in_progress", false, false, "Cancelling — what is running now finishes first", "Cancel was already pressed — the account being worked is finishing"]);
      assert.deepEqual(out.counts, counts({ total: 7, success: 3, in_progress: 1, cancelled: 3 }), "3 done · 1 still being read by its worker · 3 cancelled");
      assert.deepEqual(await refusal(() => stopBatch(userId, String(_id), "ravi", deps)), { message: "Cancel was already pressed — the account being worked is finishing", status: 409, code: "already_cancelling" });
      const rows = await byKey(String(_id));
      assert.deepEqual([rows["u3@verify.invalid|U31"].status, rows["u3@verify.invalid|U31"].error, rows["u2@verify.invalid|U22"].status], ["cancelled", CANCELLED_BEFORE_START, "in_progress"]);
      // What the worker on u2 does when it sees the request cancelled after its unit: the account ends, the request stays cancelled.
      await OrderUpdateRequest.updateOne({ _id }, { $set: { "accounts.$[a].status": "cancelled", "accounts.$[a].message": "Stopped after 1 unit(s)", "accounts.$[a].finishedAt": new Date() } }, { arrayFilters: [{ "a.email": "u2@verify.invalid" }] });
      const ended = await batchOf(String(_id));
      assert.deepEqual(ended.counts, counts({ total: 7, success: 3, cancelled: 4 }));
      assert.deepEqual([ended.status, ended.progress_percent, ended.can_retry], ["cancelled", 100, true]);
    });
    await check("the update a control means when it names none (Force kill, /update/stop, the panel's Update row, /update/status): never an after-order update while one a person or a schedule started is open, or was the last such", async () => {
      const Update = throwaway(OrderUpdateRequest, "pick") as unknown as Model<IOrderUpdateRequest>;
      const put = async (source: string, status: string, minutesAgo: number) => {
        const _id = oid();
        const at = ago(minutesAgo * MIN);
        await raw(Update).insertOne({ _id, userId: uid, source, selectionMethod: "order_ids", params: {}, parentId: "", systemId: SYSTEM, deliveredOrderIds: [], createdBy: source === "manual" ? "asha" : "", status, mode: "api", windows: 1, accounts: [], logs: [], startedAt: at, finishedAt: null, createdAt: at, updatedAt: at });
        return String(_id);
      };
      const manual = await put("manual", "running", 30);
      const afterOrder = await put("system-order", "queued", 1);
      assert.deepEqual([await currentUpdateRequestId(userId, true, Update), await currentUpdateRequestId(userId, false, Update)], [manual, manual], "the large running update, not the after-order one queued since");
      await Update.updateOne({ _id: manual }, { $set: { status: "done", finishedAt: new Date() } });
      assert.equal(await currentUpdateRequestId(userId, true, Update), afterOrder, "alone and open: the after-order update is the one to stop");
      assert.equal(await currentUpdateRequestId(userId, false, Update), afterOrder, "…and the one the panel follows while it runs");
      await Update.updateOne({ _id: afterOrder }, { $set: { status: "done", finishedAt: new Date() } });
      assert.deepEqual([await currentUpdateRequestId(userId, true, Update), await currentUpdateRequestId(userId, false, Update)], ["", manual], "nothing open: no stop; the panel shows the last one somebody started");
      const schedule = await put("schedule", "queued", 0);
      assert.equal(await currentUpdateRequestId(userId, true, Update), schedule, "a schedule's update is a request like the user's");
    });

    const invoiceIds: Record<string, string> = {};
    await check("invoice fetch — its real stop, on a throwaway queue: every queued order 'Cancelled before it started', jobs off the queue", async () => {
      const made = await createInvoiceRequest(userId, { orderIds: ["ODINV1", "ODINV2", "ODINV3", "ODGHOST9"], by: "asha" }, invoiceDeps);
      invoiceIds.stopped = made.request_id;
      assert.deepEqual([made.status, made.counts.queued, made.counts.failed], ["queued", 3, 1]);
      const out = (await stopBatch(userId, made.request_id, "ravi", deps))!;
      assert.deepEqual([out.status, out.task_type, out.can_cancel, out.can_retry], ["cancelled", "invoice_fetch", false, true]);
      assert.deepEqual(out.counts, counts({ total: 4, failed: 1, cancelled: 3 }));
      assert.deepEqual(out.stop, { removed: 3 });
      assert.equal(out.progress_percent, 100);
      const rows = await byKey(made.request_id);
      assert.deepEqual([rows.ODINV1.status, rows.ODINV1.error, rows.ODGHOST9.errorCode], ["cancelled", CANCELLED_BEFORE_START, "order_not_found"]);
      const left = await getInvoiceBatchQueue(invoiceQueue).getJobCounts("waiting", "active", "delayed");
      assert.equal(left.waiting + left.active + left.delayed, 0);
    });
    const couponIds: Record<string, string> = {};
    await check("coupon apply — its real stop, on a throwaway queue", async () => {
      const made = await createCouponRequest(userId, { links: "https://www.flipkart.com/coupon/x", emails: ["cp1@verify.invalid", "cp2@verify.invalid", "nobody@verify.invalid"], by: "asha" }, couponDeps);
      couponIds.stopped = made.request_id;
      const out = (await stopBatch(userId, made.request_id, "ravi", deps))!;
      assert.deepEqual([out.status, out.task_type, out.can_cancel], ["cancelled", "company_coupon_apply", false]);
      assert.deepEqual(out.counts, counts({ total: 3, failed: 1, cancelled: 2 }));
      assert.equal((await byKey(made.request_id))["cp1@verify.invalid"].error, CANCELLED_BEFORE_START);
    });

    const cancelUnit = (unitId: string, email: string, status: string, extra: Record<string, unknown> = {}) => ({ orderId: `OD${unitId.slice(0, 2)}`, unitId, itemId: "", email, productName: "", status, message: "", startedAt: null, finishedAt: null, ...extra });
    const cancelDoc = (extra: Record<string, unknown>) => ({ userId: uid, type: "unit", input: [], dryRun: false, urgent: false, createdBy: "asha", retryOf: "", source: "", systemId: SYSTEM, stoppedBy: "", stoppedAt: null, status: "running", units: [], orders: [], rejected: [], finishedAt: null, createdAt: ago(2 * MIN), updatedAt: new Date(), ...extra });
    const cancelIds: Record<string, string> = {};
    await check("cancel units: accounts no worker holds are closed and their Order rows freed; the account a worker is on is left to finish — the request stays open, and cannot be cancelled twice", async () => {
      const _id = oid();
      cancelIds.stopped = String(_id);
      const id = String(_id);
      await raw(log.Cancel).insertOne({
        _id,
        ...cancelDoc({
          units: [
            cancelUnit("A11", "ca@verify.invalid", "queued"), cancelUnit("A12", "ca@verify.invalid", "queued"),
            cancelUnit("B11", "cb@verify.invalid", "running", { startedAt: new Date() }), cancelUnit("B12", "cb@verify.invalid", "queued"),
            cancelUnit("C11", "cc@verify.invalid", "running", { startedAt: ago(40 * MIN) }), cancelUnit("C12", "cc@verify.invalid", "queued"),
            cancelUnit("D11", "cd@verify.invalid", "cancelled", { startedAt: ago(MIN), finishedAt: ago(MIN) }),
          ],
          orders: [{ orderId: "ODURGENT1", email: "ca@verify.invalid", status: "queued", message: "", unitsFound: 0, startedAt: null, finishedAt: null }],
        }),
      });
      await syncRequestTasks("cancel", id, log);
      aliveQueues.add("order-cancel");
      jobs.set(`${id}-ca@verify.invalid`, "waiting");
      jobs.set(`${id}-cb@verify.invalid`, "active"); // a worker holds it
      // cc has no job at all: its worker died with C11 in hand.
      calls.freed.length = 0;
      const out = (await stopBatch(userId, id, "ravi", deps))!;
      assert.deepEqual(out.stop, { closed_accounts: 2, running_accounts: 1, freed_rows: 4 });
      assert.deepEqual(calls.freed, [{ kind: "cancel", requestId: id, unitIds: ["A11", "A12", "C11", "C12"] }]);
      assert.equal(jobs.has(`${id}-ca@verify.invalid`), false, "the waiting job left the queue");
      assert.equal(jobs.get(`${id}-cb@verify.invalid`), "active", "the job a worker holds is not touched");
      assert.deepEqual([out.status, out.message, out.can_cancel, out.cancel_note], ["in_progress", "Cancelling — what is running now finishes first", false, "Cancel was already pressed — the account being worked is finishing"]);
      assert.deepEqual(out.counts, counts({ total: 8, success: 1, in_progress: 1, queued: 1, cancelled: 4, aborted: 1 }));
      const rows = await byKey(id);
      assert.deepEqual([rows["u:A11"].status, rows["u:A11"].error, rows["o:ODURGENT1"].status, rows["u:C11"].status, rows["u:C11"].error, rows["u:C12"].status, rows["u:B11"].status, rows["u:B12"].status], ["cancelled", CANCELLED_BEFORE_START, "cancelled", "aborted", ABORTED_WHILE_RUNNING, "cancelled", "in_progress", "queued"]);
      const doc = await log.Cancel.findById(_id).lean();
      assert.deepEqual([doc?.stoppedBy, doc?.status, doc?.units.find((u) => u.unitId === "A11")?.message], ["ravi", "running", clearedByStop("ravi")]);
      assert.deepEqual(await refusal(() => stopBatch(userId, id, "ravi", deps)), { message: "Cancel was already pressed — the account being worked is finishing", status: 409, code: "already_cancelling" });
    });
    await check("…and once that worker has finished (its unit cancelled, the rest left 'Cleared by …' as the new worker code does) the batch reads cancelled with its tally, in the Cancelled filter and not the Completed one", async () => {
      const id = cancelIds.stopped;
      const now = new Date();
      // What runCancelJob writes: B11 it had in hand ends; B12 it reads the mark for and leaves; the request closes "done".
      await log.Cancel.updateOne({ _id: id }, { $set: { "units.$[b].status": "cancelled", "units.$[b].finishedAt": now, "units.$[l].status": "failed", "units.$[l].message": clearedByStop("ravi"), "units.$[l].finishedAt": now, status: "done", finishedAt: now } }, { arrayFilters: [{ "b.unitId": "B11" }, { "l.unitId": "B12" }] });
      const b = await batchOf(id);
      assert.deepEqual([b.status, b.can_cancel, b.can_retry], ["cancelled", false, true]);
      assert.deepEqual(b.counts, counts({ total: 8, success: 2, cancelled: 5, aborted: 1 }), "2 done · 1 aborted · 5 cancelled");
      assert.deepEqual([b.finished_count, b.progress_percent, b.retryable_count], [8, 100, 6]);
      const listed = async (status: string) => (await listBatches(userId, { status, task_type: "cancel" }, log, probes)).results.map((r) => r.request_id);
      assert.ok((await listed("cancelled")).includes(id));
      assert.ok(!(await listed("completed")).includes(id));
    });
    await check("cancel units with no worker on the queue: an 'active' job is a dead worker's — everything is closed at once", async () => {
      const _id = oid();
      const id = String(_id);
      await raw(log.Cancel).insertOne({ _id, ...cancelDoc({ units: [cancelUnit("E11", "ce@verify.invalid", "running", { startedAt: new Date() }), cancelUnit("E12", "ce@verify.invalid", "queued")] }) });
      aliveQueues.delete("order-cancel");
      jobs.set(`${id}-ce@verify.invalid`, "active");
      const out = (await stopBatch(userId, id, "ravi", deps))!;
      assert.deepEqual([out.status, out.message], ["cancelled", "Request cancelled"]);
      assert.deepEqual(out.counts, counts({ total: 2, aborted: 1, cancelled: 1 }));
      aliveQueues.add("order-cancel");
    });
    await check("cancel units queued on another machine's Redis: refused — nothing here could take its jobs off", async () => {
      const _id = oid();
      await raw(log.Cancel).insertOne({ _id, ...cancelDoc({ systemId: "other-mac", status: "queued", units: [cancelUnit("F11", "cf@verify.invalid", "queued")] }) });
      const r = await refusal(() => stopBatch(userId, String(_id), "ravi", deps));
      assert.deepEqual([r.status, r.code], [409, "other_machine"]);
      assert.match(r.message, /^This request's jobs are on another machine's queue \(other-mac\)/);
      await log.Cancel.updateOne({ _id }, { $set: { status: "done", finishedAt: new Date(), "units.0.status": "cancelled" } });
    });
    const rUnit = (unitId: string, status: string, extra: Record<string, unknown> = {}) => ({ orderId: `OD${unitId.slice(0, 2)}`, unitId, itemId: "", email: "rs@verify.invalid", productName: "", status, message: "", offeredDates: [], chosen: "", token: "", startedAt: null, finishedAt: null, ...extra });
    const rescheduleDoc = (extra: Record<string, unknown>) => ({ userId: uid, type: "unit", input: [], date: "2099-01-05", createdBy: "asha", retryOf: "", source: "", systemId: SYSTEM, stoppedBy: "", stoppedAt: null, confirm: false, status: "queued", units: [], rejected: [], finishedAt: null, createdAt: ago(MIN), updatedAt: new Date(), ...extra });
    await check("reschedule: the same — queued units cancelled, Order rows freed, the request cancelled", async () => {
      const _id = oid();
      const id = String(_id);
      await raw(log.Reschedule).insertOne({ _id, ...rescheduleDoc({ units: [rUnit("R11", "queued"), rUnit("R12", "queued")] }) });
      aliveQueues.add("order-reschedule");
      jobs.set(`${id}-rs@verify.invalid`, "waiting");
      calls.freed.length = 0;
      const out = (await stopBatch(userId, id, "ravi", deps))!;
      assert.deepEqual([out.status, out.task_type], ["cancelled", "reschedule"]);
      assert.deepEqual(out.counts, counts({ total: 2, cancelled: 2 }));
      assert.deepEqual(calls.freed, [{ kind: "reschedule", requestId: id, unitIds: ["R11", "R12"] }]);
      assert.equal((await byKey(id))["u:R11"].error, CANCELLED_BEFORE_START);
    });
    await check("a request that has ended cannot be cancelled (409 not_open); an id of another workspace is not found", async () => {
      assert.deepEqual(await refusal(() => stopBatch(userId, invoiceIds.stopped, "ravi", deps)), { message: "This request has already ended — there is nothing to cancel", status: 409, code: "not_open" });
      assert.equal(await stopBatch(String(oid()), invoiceIds.stopped, "ravi", deps), null);
      assert.equal(await retryBatch(String(oid()), invoiceIds.stopped, { all_incomplete: true }, "ravi", deps), null);
    });

    // ── RETRY ───────────────────────────────────────────────────────────────
    console.log("\nRETRY — POST /sync/batches/:id/retry");
    await check("the reference's two bodies and ours (error_code), and the ones refused", () => {
      assert.deepEqual(parseRetryBody({ statuses: ["failed"] }), { statuses: ["failed"], errorCode: "", allIncomplete: false });
      assert.deepEqual(parseRetryBody({ statuses: ["failed"], error_code: "nothing_available" }), { statuses: ["failed"], errorCode: "nothing_available", allIncomplete: false });
      assert.deepEqual(parseRetryBody({ all_incomplete: true }), { statuses: ["failed", "timedout", "aborted", "cancelled"], errorCode: "", allIncomplete: true });
      assert.deepEqual(parseRetryBody({ statuses: ["timed_out", "Aborted"] }).statuses, ["timedout", "aborted"], "the counts key `timed_out` is taken for the status `timedout`");
      assert.throws(() => parseRetryBody({}), /Say what to retry/);
      assert.throws(() => parseRetryBody({ statuses: ["success"] }), /Only unsuccessful tasks can be retried/);
      assert.throws(() => parseRetryBody({ statuses: ["failed"], error_code: "made_up" }), /is not an error class/);
    });

    const invTask = (orderId: string, status: string, errorClass = "", error = "") => ({ orderId, email: `inv${orderId.slice(-1)}@verify.invalid`, accountId: `acc-${orderId.slice(-1)}`, status, errorClass, error, detail: "", parts: [], sizeKb: 0, startedAt: status === "queued" || status === "cancelled" ? null : ago(30 * MIN), finishedAt: status === "queued" ? null : ago(29 * MIN) });
    await check("invoice fetch, { statuses: ['failed'] }: ONE new request for exactly the failed orders — source system-retry, parent and child linked, who pressed it, the same Force setting", async () => {
      const _id = oid();
      invoiceIds.original = String(_id);
      const tasks = [invTask("ODRT1", "success"), invTask("ODRT2", "success"), invTask("ODRT3", "failed", "nothing_available", "Nothing available yet"), invTask("ODRT4", "failed", "not_logged_in", "Account is not logged in"), invTask("ODRT5", "timedout", "", TIMED_OUT), invTask("ODRT6", "cancelled", "", CANCELLED_BEFORE_START), invTask("ODRT7", "aborted", "", ABORTED_WHILE_RUNNING)];
      const createdAt = ago(31 * MIN);
      await raw(OrderInvoiceRequest).insertOne({ _id, userId: uid, platform: "FLIPKART", source: "user", createdBy: "asha", status: "cancelled", message: "Stopped by asha", params: { order_ids_count: 7, force: true }, tasks, counts: counts({ total: 7, success: 2, failed: 2, timed_out: 1, cancelled: 1, aborted: 1 }), retryOf: "", parentId: "", queue: invoiceQueue, systemId: SYSTEM, host: HOST, workerHost: "", stoppedBy: "asha", startedAt: ago(30 * MIN), finishedAt: ago(29 * MIN), createdAt, updatedAt: createdAt });
      const before = await batchOf(String(_id));
      assert.deepEqual([before.can_retry, before.can_cancel, before.incomplete_count, before.retryable_count], [true, false, 5, 5]);
      const out = (await retryBatch(userId, String(_id), { statuses: ["failed"] }, "ravi", deps))!;
      invoiceIds.retryFailed = out.request_id;
      assert.deepEqual(
        { parent_id: out.parent_id, task_type: out.task_type, source: out.source, triggered_by: out.triggered_by, statuses: out.statuses, tasks_matched: out.tasks_matched, tasks_retried: out.tasks_retried, tasks_created: out.tasks_created, skipped_in_progress: out.skipped_in_progress, not_retried: out.not_retried },
        { parent_id: String(_id), task_type: "invoice_fetch", source: "system-retry", triggered_by: "ravi", statuses: ["failed"], tasks_matched: 2, tasks_retried: 2, tasks_created: 2, skipped_in_progress: 0, not_retried: 0 }
      );
      assert.equal(out.message, "Retry queued — 2 failed task(s) as a new request");
      const doc = (await OrderInvoiceRequest.findById(out.request_id).lean())!;
      assert.deepEqual([doc.source, doc.createdBy, doc.retryOf, doc.parentId, doc.status, doc.queue], ["system-retry", "ravi", String(_id), String(_id), "queued", invoiceQueue]);
      assert.deepEqual(doc.tasks.map((t) => t.orderId).sort(), ["ODRT3", "ODRT4"]);
      assert.deepEqual(doc.params, { order_ids_count: 2, force: true, retry_of: String(_id), parent_id: String(_id), follows: "retry", retry_statuses: ["failed"] });
      const child = await batchOf(out.request_id);
      assert.deepEqual([child.source, child.triggered_by?.name, child.parent_id, child.follows?.label, child.follows?.what], ["system-retry", "ravi", String(_id), `Retry of ${shortId(String(_id))}`, "retry"]);
      assert.ok(child.settings.some((s) => s.label === "Retried" && s.value === "Failed"));
      assert.deepEqual((await batchOf(String(_id))).child_ids, [out.request_id], "the original's child_ids gained the new request");
      const rows = await rowsOf(out.request_id);
      assert.ok(rows.length === 2 && rows.every((r) => r.source === "system-retry" && r.triggeredBy === "ravi"), "its task rows carry the source and who pressed");
      assert.equal((await listBatches(userId, { source: "system-retry" }, log, probes)).results.some((r) => r.request_id === out.request_id), true, "the Retry source filter finds it");
    });
    await check("{ all_incomplete: true }: failed + timed out + aborted + cancelled — and items already queued or running are skipped (the two the first retry holds)", async () => {
      const out = (await retryBatch(userId, invoiceIds.original, { all_incomplete: true }, "ravi", deps))!;
      invoiceIds.retryAll = out.request_id;
      assert.deepEqual([out.tasks_matched, out.tasks_retried, out.skipped_in_progress, out.all_incomplete], [5, 3, 2, true]);
      assert.deepEqual([...out.skipped_items].sort(), ["ODRT3", "ODRT4"]);
      assert.equal(out.message, "Retry queued — 3 failed, timed-out, aborted or cancelled task(s) as a new request; 2 already queued or running were skipped");
      const doc = (await OrderInvoiceRequest.findById(out.request_id).lean())!;
      assert.deepEqual(doc.tasks.map((t) => t.orderId).sort(), ["ODRT5", "ODRT6", "ODRT7"]);
      assert.deepEqual((await batchOf(invoiceIds.original)).child_ids.sort(), [invoiceIds.retryFailed, out.request_id].sort());
    });
    await check("{ statuses: ['failed'], error_code }: one error class — here its one order is already queued, so nothing is queued (409 already_queued)", async () => {
      assert.deepEqual(await refusal(() => retryBatch(userId, invoiceIds.original, { statuses: ["failed"], error_code: "nothing_available" }, "ravi", deps)), { message: "Nothing was queued — all 1 item(s) are already queued or running in another request", status: 409, code: "already_queued" });
      // Once the request holding it has ended, the same body queues exactly that order.
      await stopInvoiceRequest(userId, invoiceIds.retryFailed, "verify", invoiceDeps);
      const out = (await retryBatch(userId, invoiceIds.original, { statuses: ["failed"], error_code: "nothing_available" }, "ravi", deps))!;
      const doc = (await OrderInvoiceRequest.findById(out.request_id).lean())!;
      assert.deepEqual([doc.tasks.map((t) => t.orderId), doc.params.error_code, out.error_code], [["ODRT3"], "nothing_available", "nothing_available"]);
      assert.ok((await batchOf(out.request_id)).settings.some((s) => s.label === "Error class" && s.value === "Nothing available yet"));
      await stopInvoiceRequest(userId, out.request_id, "verify", invoiceDeps);
      await stopInvoiceRequest(userId, invoiceIds.retryAll, "verify", invoiceDeps);
    });
    await check("refused: a request still open, one whose tasks are no longer kept, and a body that matches no task", async () => {
      const open = await createInvoiceRequest(userId, { orderIds: ["ODOPEN1"], by: "asha" }, invoiceDeps);
      assert.deepEqual(await refusal(() => retryBatch(userId, open.request_id, { all_incomplete: true }, "ravi", deps)), { message: "This request is still open — cancel it or wait for it to end, then retry", status: 409, code: "still_open" });
      assert.equal((await batchOf(open.request_id)).can_retry, false);
      await stopInvoiceRequest(userId, open.request_id, "verify", invoiceDeps);
      const old = oid();
      const createdAt = ago(8 * DAY);
      await raw(OrderInvoiceRequest).insertOne({ _id: old, userId: uid, platform: "FLIPKART", source: "user", createdBy: "asha", status: "completed", message: "", params: { order_ids_count: 1, force: false }, tasks: [invTask("ODOLD1", "failed", "nothing_available", "Nothing available yet")], counts: counts({ total: 1, failed: 1 }), retryOf: "", parentId: "", queue: invoiceQueue, systemId: SYSTEM, host: HOST, workerHost: "", stoppedBy: "", startedAt: createdAt, finishedAt: createdAt, createdAt, updatedAt: createdAt });
      assert.deepEqual(await refusal(() => retryBatch(userId, String(old), { all_incomplete: true }, "ravi", deps)), { message: "The tasks of this request are no longer kept (tasks are kept for 7 days) — start a new request instead", status: 409, code: "tasks_expired" });
      assert.deepEqual([(await batchOf(String(old))).can_retry, (await batchOf(String(old))).retryable_count], [false, 0]);
      assert.deepEqual(await refusal(() => retryBatch(userId, invoiceIds.original, { statuses: ["failed"], error_code: "rate_limited" }, "ravi", deps)), { message: 'Nothing to retry — this request has no failed task of the class "Platform is limiting requests; try again later"', status: 409, code: "nothing_to_retry" });
    });

    await check("coupon apply: the failed accounts, the same links — more accounts than a person may type are taken", async () => {
      const made = await createCouponRequest(userId, { links: ["https://www.flipkart.com/coupon/a", "https://www.flipkart.com/coupon/b"], emails: ["cp1@verify.invalid", "cp2@verify.invalid", "cp3@verify.invalid"], by: "asha" }, couponDeps);
      const id = made.request_id;
      const rid = new Types.ObjectId(id);
      // How the worker would have left it: cp1 done, cp2 failed (not logged in), cp3 failed (platform unavailable).
      const now = new Date();
      await OrderCouponTask.updateOne({ requestId: rid, email: "cp1@verify.invalid" }, { $set: { status: "success", startedAt: now, finishedAt: now } });
      await OrderCouponTask.updateOne({ requestId: rid, email: "cp2@verify.invalid" }, { $set: { status: "failed", errorClass: "not_logged_in", error: "Account is not logged in", startedAt: now, finishedAt: now } });
      await OrderCouponTask.updateOne({ requestId: rid, email: "cp3@verify.invalid" }, { $set: { status: "failed", errorClass: "platform_unavailable", error: "Platform did not respond; try again later", startedAt: now, finishedAt: now } });
      await OrderCouponRequest.updateOne({ _id: rid }, { $set: { status: "completed", startedAt: now, finishedAt: now, counts: counts({ total: 3, success: 1, failed: 2 }) } });
      for (const email of ["cp1@verify.invalid", "cp2@verify.invalid", "cp3@verify.invalid"]) await (await getCouponQueue(couponQueue).getJob(`${id}-${email}`))?.remove();
      const out = (await retryBatch(userId, id, { statuses: ["failed"] }, "ravi", deps))!;
      const doc = (await OrderCouponRequest.findById(out.request_id).lean())!;
      assert.deepEqual([doc.source, doc.createdBy, doc.retryOf, doc.parentId, doc.links], ["system-retry", "ravi", id, id, ["https://www.flipkart.com/coupon/a", "https://www.flipkart.com/coupon/b"]]);
      assert.deepEqual((await OrderCouponTask.find({ requestId: doc._id }).select("email").lean()).map((t) => t.email).sort(), ["cp2@verify.invalid", "cp3@verify.invalid"]);
      assert.deepEqual([out.task_type, out.tasks_retried, doc.params.retry_of, (await batchOf(id)).child_ids], ["company_coupon_apply", 2, id, [out.request_id]]);
      couponIds.retry = out.request_id;
      // The 2000 a person may type does not bind a retry: 2,300 e-mails are accepted (none of them an account here → each a failed task, no job).
      const many = Array.from({ length: 2300 }, (_, i) => `x${i}@verify.invalid`);
      await assert.rejects(() => createCouponRequest(userId, { links: "https://www.flipkart.com/coupon/a", emails: many }, couponDeps), /Too many values in "Email addresses"/);
      const big = await createCouponRequest(userId, { links: "https://www.flipkart.com/coupon/a", emails: many, anyNumberOfEmails: true, source: "system-retry", retryOf: id }, couponDeps);
      assert.deepEqual([big.status, big.counts.total, big.counts.failed], ["completed", 2300, 2300]);
    });

    await check("fetch: the failed accounts, the same date and settings — and the fetch's own single flight answers in its own words", async () => {
      const _id = oid();
      const id = String(_id);
      const createdAt = ago(20 * MIN);
      await raw(Ledger).insertOne({ ...ledgerBase, _id, status: "running", total: 4, startedAt: createdAt, finishedAt: null, createdAt, updatedAt: createdAt, meta: { host: HOST, pid: PID, platform: "FLIPKART", params: fetchParams } });
      const emails = ["g1", "g2", "g3"].map((n) => `${n}@verify.invalid`);
      const t = openFetchTasks({ userId, runId: id, source: "user", by: "asha", emails, skipped: ["g4@verify.invalid"], createdAt }, log);
      for (const e of emails) t.started(e);
      t.succeeded(emails[0]);
      t.succeeded(emails[1]);
      t.failed(emails[2], "page.goto: Timeout 30000ms exceeded.");
      const end = t.finish(false);
      await end.written;
      await Ledger.updateOne({ _id }, { $set: { status: "completed", finishedAt: new Date(), "meta.task_counts": end.counts } });
      calls.fetch.length = 0;
      const out = (await retryBatch(userId, id, { statuses: ["failed"] }, "ravi", deps))!;
      const call = calls.fetch[0];
      assert.deepEqual({ ...call.body, emails: [...call.body.emails].sort() }, { platform: "FLIPKART", emails: ["g3@verify.invalid", "g4@verify.invalid"], afterDate: "2026-11-22", enableLogin: false, forceFetch: true, runUpdateAfterFetch: true, invoices: "off", trigger: "user", by: "ravi", parentId: id, follows: "retry" });
      assert.deepEqual(call.extra, { source: "system-retry", params: { retry_of: id, parent_id: id, follows: "retry", retry_statuses: ["failed"] } });
      const child = await batchOf(out.request_id);
      assert.deepEqual([child.source, child.triggered_by?.name, child.parent_id, child.follows?.label], ["system-retry", "ravi", id, `Retry of ${shortId(id)}`]);
      assert.deepEqual((await batchOf(id)).child_ids, [out.request_id]);
      // One error class only.
      calls.fetch.length = 0;
      await retryBatch(userId, id, { statuses: ["failed"], error_code: "platform_unavailable" }, "ravi", deps);
      assert.deepEqual(calls.fetch[0].body.emails, ["g3@verify.invalid"]);
      // The real start, with the workspace's one fetch slot taken: refused before anything is read or written.
      const release = reserveOrderFetch(userId);
      try {
        assert.deepEqual(await refusal(() => retryBatch(userId, id, { statuses: ["failed"] }, "ravi", { ...deps, startFetch: actionDefaults.startFetch })), { message: "Order fetch is already running", status: 0, code: "" });
      } finally {
        release();
      }
    });

    await check("update: exactly the units of the chosen tasks, as the request stored them — a unit already queued in another update is skipped; the update-being-prepared guard answers in its words", async () => {
      const _id = oid();
      const id = String(_id);
      const createdAt = ago(15 * MIN);
      const acc = (email: string, status: string, units: ReturnType<typeof unit>[], done: number, failed: number, message = "") => ({ email, platformId: null, units, total: units.length, status, done, failed, statusChanged: 0, otpNew: 0, via: "api", message, startedAt: createdAt, finishedAt: createdAt });
      await raw(OrderUpdateRequest).insertOne({
        _id, userId: uid, source: "manual", selectionMethod: "order_status", params: { platform: "FLIPKART", selection_method: "order_status", force: false, statuses_to_update: ["Shipped"] }, parentId: "", systemId: SYSTEM, deliveredOrderIds: [], createdBy: "asha", status: "done", mode: "api", windows: 3,
        accounts: [
          acc("w1@verify.invalid", "done", [unit("ODW1", "W11"), unit("ODW1", "W12")], 1, 1),
          acc("w2@verify.invalid", "failed", [unit("ODW2", "W21"), unit("ODW3", "W31")], 0, 0, "Session: Status is \"pending\", not logged_in — re-login this ID"),
          acc("w3@verify.invalid", "failed", [unit("ODW4", "W41")], 0, 0, TIMED_OUT),
        ],
        logs: [{ time: createdAt, level: "warn", message: "w1@verify.invalid: ODW1/W12: order page gave no data for ODW1/W12 (no status, timeline or product) — row left as it was" }],
        startedAt: createdAt, finishedAt: createdAt, createdAt, updatedAt: createdAt,
      });
      updateIds.original = id;
      // W21 is queued in another, open update.
      const held = oid();
      await raw(OrderUpdateRequest).insertOne({ _id: held, userId: uid, source: "schedule", selectionMethod: "schedule_active", params: {}, parentId: "", systemId: SYSTEM, deliveredOrderIds: [], createdBy: "schedule", status: "queued", mode: "api", windows: 3, accounts: [{ ...acc("w2@verify.invalid", "queued", [unit("ODW2", "W21")], 0, 0), startedAt: null, finishedAt: null }], logs: [], startedAt: null, finishedAt: null, createdAt: new Date(), updatedAt: new Date() });
      updateIds.held = String(held);
      await syncRequestTasks("update", String(held), log);
      calls.update.length = 0;
      const out = (await retryBatch(userId, id, { all_incomplete: true }, "ravi", deps))!;
      const call = calls.update[0];
      assert.deepEqual(call.units.map((u) => `${u.email}|${u.orderId}|${u.unitId}`).sort(), ["w1@verify.invalid|ODW1|W12", "w2@verify.invalid|ODW3|W31", "w3@verify.invalid|ODW4|W41"]);
      assert.deepEqual([out.tasks_matched, out.tasks_retried, out.skipped_in_progress, out.skipped_items], [4, 3, 1, ["W21"]]);
      assert.deepEqual([call.opts.by, call.opts.parentId, call.opts.params.selection_method, call.opts.params.units_count, call.opts.params.order_ids_count, call.opts.params.retry_of], ["ravi", id, "unit_ids", 3, 3, id]);
      const child = await batchOf(out.request_id);
      assert.deepEqual([child.source, child.triggered_by?.name, child.parent_id, child.summary], ["system-retry", "ravi", id, "Orders: 3 · Units: 3"]);
      assert.ok(child.settings.some((s) => s.label === "Selected by" && s.value === "Explicit unit IDs"));
      assert.deepEqual((await batchOf(id)).child_ids, [out.request_id]);
      // Only what timed out.
      calls.update.length = 0;
      await retryBatch(userId, id, { statuses: ["timedout"] }, "ravi", deps);
      assert.deepEqual(calls.update[0].units.map((u) => u.unitId), ["W41"]);
      // The slot the update trigger holds while it puts a request together.
      const release = holdUpdatePreparation(userId)!;
      try {
        calls.update.length = 0;
        assert.deepEqual(await refusal(() => retryBatch(userId, id, { all_incomplete: true }, "ravi", deps)), { message: UPDATE_PREPARING_MESSAGE, status: 409, code: "update_preparing" });
        assert.equal(calls.update.length, 0);
      } finally {
        release();
      }
      await OrderUpdateRequest.updateOne({ _id: held }, { $set: { status: "cancelled", finishedAt: new Date(), "accounts.0.status": "cancelled" } });
    });

    await check("cancel units: what is stored failed (a failure, a cleared unit, a timed-out one, an order page not read) goes to the cancel's own request function, dry run kept — never what Flipkart refused", async () => {
      const _id = oid();
      const id = String(_id);
      const done = ago(10 * MIN);
      await raw(log.Cancel).insertOne({
        _id,
        ...cancelDoc({
          dryRun: true, status: "done", finishedAt: done, createdAt: ago(12 * MIN),
          units: [
            cancelUnit("K11", "ck@verify.invalid", "failed", { message: "page.goto: Timeout 30000ms exceeded.", startedAt: done, finishedAt: done }),
            cancelUnit("K12", "ck@verify.invalid", "not_cancellable", { message: "Not eligible for cancellation", startedAt: done, finishedAt: done }),
            cancelUnit("K13", "ck@verify.invalid", "dry_run", { startedAt: done, finishedAt: done }),
            cancelUnit("K14", "ck@verify.invalid", "failed", { message: "Cleared by asha — reset for re-run", finishedAt: done }),
            cancelUnit("K15", "ck@verify.invalid", "failed", { message: TIMED_OUT, startedAt: done, finishedAt: done }),
          ],
          orders: [{ orderId: "ODPAGE1", email: "ck@verify.invalid", status: "failed", message: "Session expired — re-login this ID", unitsFound: 0, startedAt: done, finishedAt: done }],
        }),
      });
      cancelIds.original = id;
      await syncRequestTasks("cancel", id, log);
      const before = await batchOf(id);
      assert.deepEqual([before.incomplete_count, before.retryable_count, before.can_retry], [5, 4, true], "5 unsuccessful, 4 of them retryable — K12 is Flipkart's refusal");
      calls.cancel.length = 0;
      const out = (await retryBatch(userId, id, { statuses: ["failed"] }, "ravi", deps))!;
      assert.deepEqual(calls.cancel[0], { type: "unit", ids: ["K11"], extraOrders: [{ orderId: "ODPAGE1", email: "ck@verify.invalid" }], dryRun: true, by: "ravi", retryOf: id, failIfEmpty: true, source: "system-retry" });
      assert.deepEqual([out.tasks_matched, out.tasks_retried, out.not_retried, out.task_type], [3, 2, 1, "cancel"]);
      const child = await batchOf(out.request_id);
      assert.deepEqual([child.source, child.triggered_by?.name, child.parent_id, child.follows?.label], ["system-retry", "ravi", id, `Retry of ${shortId(id)}`]);
      assert.deepEqual((await batchOf(id)).child_ids, [out.request_id]);
      calls.cancel.length = 0;
      await retryBatch(userId, id, { all_incomplete: true }, "ravi", deps);
      assert.deepEqual([[...calls.cancel[0].ids].sort(), calls.cancel[0].extraOrders, calls.cancel[0].dryRun], [["K11", "K14", "K15"], [{ orderId: "ODPAGE1", email: "ck@verify.invalid" }], true]);
      calls.cancel.length = 0;
      assert.deepEqual(await refusal(() => retryBatch(userId, id, { statuses: ["failed"], error_code: "not_allowed_now" }, "ravi", deps)), { message: "Nothing to retry — Flipkart itself refused these units (not cancellable); they are not submitted again", status: 409, code: "nothing_to_retry" });
      assert.equal(calls.cancel.length, 0);
    });
    await check("…and with the REAL cancel function behind it, units that are not in Order Units are refused by its own check — no cancel request is written", async () => {
      const before = await CancelRequest.countDocuments({ userId: uid });
      const r = await refusal(() => retryBatch(userId, cancelIds.original, { statuses: ["timedout"] }, "ravi", { ...deps, startCancel: actionDefaults.startCancel }));
      assert.match(r.message, /^Nothing to retry — 1 × Not found in Order Units/);
      assert.equal(await CancelRequest.countDocuments({ userId: uid }), before);
      assert.equal(before, 0, "the real cancel collection holds nothing of this workspace");
    });
    await check("reschedule: its failed units and its date — not_available / needs_review are Flipkart's answers and stay; a date that has passed is refused by the reschedule's own check", async () => {
      const _id = oid();
      const id = String(_id);
      const done = ago(10 * MIN);
      await raw(log.Reschedule).insertOne({ _id, ...rescheduleDoc({ status: "done", finishedAt: done, createdAt: ago(12 * MIN), units: [rUnit("S11", "failed", { message: "order page gave no data (blank page) — try again later", startedAt: done, finishedAt: done }), rUnit("S12", "not_available", { message: "This option is not available on this order", startedAt: done, finishedAt: done }), rUnit("S13", "needs_review", { startedAt: done, finishedAt: done }), rUnit("S14", "rescheduled", { startedAt: done, finishedAt: done })] }) });
      await syncRequestTasks("reschedule", id, log);
      assert.deepEqual([(await batchOf(id)).incomplete_count, (await batchOf(id)).retryable_count], [3, 1]);
      calls.reschedule.length = 0;
      const out = (await retryBatch(userId, id, { all_incomplete: true }, "ravi", deps))!;
      assert.deepEqual(calls.reschedule[0], { type: "unit", ids: ["S11"], date: "2099-01-05", by: "ravi", retryOf: id, failIfEmpty: true, source: "system-retry" });
      assert.deepEqual([out.tasks_matched, out.tasks_retried, out.not_retried], [3, 1, 2]);
      assert.deepEqual((await batchOf(id)).child_ids, [out.request_id]);
      const past = oid();
      await raw(log.Reschedule).insertOne({ _id: past, ...rescheduleDoc({ date: "2026-11-01", status: "done", finishedAt: done, createdAt: ago(12 * MIN), units: [rUnit("T11", "failed", { message: "Session expired — re-login this ID", startedAt: done, finishedAt: done })] }) });
      await syncRequestTasks("reschedule", String(past), log);
      const r = await refusal(() => retryBatch(userId, String(past), { statuses: ["failed"] }, "ravi", { ...deps, startReschedule: actionDefaults.startReschedule }));
      assert.equal(r.message, "Reschedule date must be today or later");
      assert.equal(await RescheduleRequest.countDocuments({ userId: uid }), 0, "the real reschedule collection holds nothing of this workspace");
    });
    await check("the skip rule reads the task log: open tasks of one type among these orders / accounts, in any request", async () => {
      const _id = oid();
      await raw(OrderUpdateRequest).insertOne({ _id, userId: uid, source: "manual", selectionMethod: "order_ids", params: {}, parentId: "", systemId: SYSTEM, deliveredOrderIds: [], createdBy: "asha", status: "running", mode: "api", windows: 1, accounts: [{ email: "z1@verify.invalid", platformId: null, units: [unit("ODZ1", "Z11"), unit("ODZ2", "Z21")], total: 2, status: "running", done: 1, failed: 0, statusChanged: 0, otpNew: 0, via: "", message: "", startedAt: new Date(), finishedAt: null }], logs: [], startedAt: new Date(), finishedAt: null, createdAt: new Date(), updatedAt: new Date() });
      updateIds.open = String(_id);
      await syncRequestTasks("update", String(_id), log);
      const open = await openTasksAmong(userId, "update", "orderId", ["ODZ1", "ODZ2", "ODZ9"], log);
      assert.deepEqual(open.map((t) => `${t.orderId}/${t.unitId}`), ["ODZ2/Z21"], "Z11 has finished; Z21 is still open");
      assert.deepEqual(await openTasksAmong(userId, "invoice_fetch", "orderId", ["ODZ2"], log), [], "another task type on the same order does not count");
      assert.deepEqual(await openTasksAmong(String(oid()), "update", "orderId", ["ODZ2"], log), [], "nor another workspace's");
    });

    // ── CHAINS ──────────────────────────────────────────────────────────────
    console.log("\nCHAINS — the requests nobody presses");
    await check("after order: a burst of 200 placed orders is ONE update request, queued when the burst goes quiet", async () => {
      const made: Array<{ orderIds: string[]; batchIds: string[] }> = [];
      const chain = makeAfterOrderChain({ enabled: true, quietMs: 250, maxWaitMs: 5000, ordersInOpenUpdate: async () => new Set(), createUpdate: async (_u, orderIds, batchIds) => (made.push({ orderIds, batchIds }), { id: "req-1", units: orderIds.length }), log: () => undefined });
      for (let i = 0; i < 200; i++) {
        chain.note(userId, `OD${1000 + i}`, i < 120 ? "batch-A" : "batch-B");
        if (i % 40 === 0) await sleep(60);
      }
      chain.note(userId, "od1000", "batch-A"); // the same order noted twice is one order
      assert.equal(made.length, 0, "nothing is queued while orders keep arriving");
      assert.equal(chain.pending(userId), 200);
      await sleep(500);
      assert.deepEqual([made.length, made[0].orderIds.length, made[0].batchIds.sort(), chain.pending(userId)], [1, 200, ["batch-A", "batch-B"], 0]);
      // A second burst later is a second request.
      chain.note(userId, "OD5000");
      await sleep(450);
      assert.deepEqual([made.length, made[1].orderIds], [2, ["OD5000"]]);
    });
    await check("after order: the ceiling — orders that never stop arriving still get their request after the maximum wait", async () => {
      const made: number[] = [];
      const chain = makeAfterOrderChain({ enabled: true, quietMs: 300, maxWaitMs: 700, ordersInOpenUpdate: async () => new Set(), createUpdate: async (_u, orderIds) => (made.push(orderIds.length), { id: "req", units: orderIds.length }), log: () => undefined });
      const started = Date.now();
      for (let i = 0; i < 9; i++) {
        chain.note(userId, `ODC${i}`);
        await sleep(150);
      }
      assert.ok(made.length >= 1 && made[0] >= 3 && made[0] <= 6, `first request after ${Date.now() - started} ms with ${made[0]} order(s) — never quiet for 300 ms`);
      await sleep(900);
      assert.equal(made.reduce((a, b) => a + b, 0), 9, "every order was queued once");
    });
    await check("after order: an order already in an open update is skipped; all of them → nothing is queued; over the cap → queued at once", async () => {
      const made: string[][] = [];
      const chain = makeAfterOrderChain({ enabled: true, quietMs: 60_000, maxWaitMs: 60_000, maxOrders: 5, ordersInOpenUpdate: async (u, ids) => new Set((await openTasksAmong(u, "update", "orderId", ids, log)).map((t) => t.orderId)), createUpdate: async (_u, orderIds) => (made.push(orderIds), { id: "req", units: orderIds.length }), log: () => undefined });
      chain.note(userId, "ODZ2"); // held by the open update of the check above
      chain.note(userId, "ODNEW1");
      const first = (await chain.flush(userId))!;
      assert.deepEqual([first.status, first.orders, first.already_in_update, made[0]], ["queued", 1, 1, ["ODNEW1"]]);
      chain.note(userId, "ODZ2");
      const second = (await chain.flush(userId))!;
      assert.deepEqual([second.status, second.reason, made.length], ["skipped", "every order is already in an open update", 1]);
      for (let i = 0; i < 5; i++) chain.note(userId, `ODCAP${i}`);
      for (let i = 0; i < 100 && made.length < 2; i++) await sleep(50);
      assert.deepEqual([made.length, made[1]?.length, chain.pending(userId)], [2, 5, 0], "five orders reach the cap: queued without waiting for the minute");
      const none = makeAfterOrderChain({ enabled: true, ordersInOpenUpdate: async () => new Set(), createUpdate: async () => Promise.reject(new Error("No active order units to update. Fetch first, or paste a logged-in email + order id.")), log: () => undefined });
      none.note(userId, "ODGONE1");
      assert.equal((await none.flush(userId))!.status, "skipped", "units that are not in Order Units are nothing to update, not a failure");
    });
    await check("after order: an order whose account still has a checkout queued or running waits for the next flush; one that has waited the limit is queued anyway", async () => {
      const made: string[][] = [];
      let checkingOut = new Set(["ODBUSY1"]);
      const chain = makeAfterOrderChain({ enabled: true, quietMs: 200, maxWaitMs: 5000, deferMaxMs: 60_000, ordersInOpenUpdate: async () => new Set(), ordersCheckingOut: async (_u, ids) => new Set(ids.filter((id) => checkingOut.has(id))), createUpdate: async (_u, orderIds) => (made.push(orderIds), { id: "req", units: orderIds.length }), log: () => undefined });
      chain.note(userId, "ODBUSY1", "batch-X");
      chain.note(userId, "ODFREE1", "batch-X");
      const first = (await chain.flush(userId))!;
      assert.deepEqual([first.status, first.orders, first.deferred, made[0], chain.pending(userId)], ["queued", 1, 1, ["ODFREE1"], 1], "the free account's order now; the other waits");
      // Its account's checkouts have ended: one quiet period on, it is queued.
      checkingOut = new Set();
      await sleep(450);
      assert.deepEqual([made.length, made[1], chain.pending(userId)], [2, ["ODBUSY1"], 0]);
      // An account that never stops checking out: its order waits the limit, then is queued anyway.
      const late: string[][] = [];
      const stuck = makeAfterOrderChain({ enabled: true, quietMs: 60_000, maxWaitMs: 60_000, deferMaxMs: 300, ordersInOpenUpdate: async () => new Set(), ordersCheckingOut: async (_u, ids) => new Set(ids), createUpdate: async (_u, orderIds) => (late.push(orderIds), { id: "req", units: orderIds.length }), log: () => undefined });
      stuck.note(userId, "ODSTUCK1");
      const waited = (await stuck.flush(userId))!;
      assert.deepEqual([waited.status, waited.deferred, waited.reason, stuck.pending(userId), late.length], ["skipped", 1, "every account is still checking out — tried again after the next quiet period", 1, 0]);
      await sleep(350);
      const forced = (await stuck.flush(userId))!;
      assert.deepEqual([forced.status, forced.deferred, late[0]], ["queued", 0, ["ODSTUCK1"]]);
      // The real reader (read only): orders this workspace does not have are on no account, so none waits.
      assert.equal((await afterOrderDefaults.ordersCheckingOut(userId, ["ODNOSUCH1", "ODNOSUCH2"])).size, 0);
    });
    await check("after order, switched off (CHAIN_AFTER_ORDER=0): nothing is noted, nothing is queued", async () => {
      let called = 0;
      const chain = makeAfterOrderChain({ enabled: false, quietMs: 50, ordersInOpenUpdate: async () => new Set(), createUpdate: async () => (called++, { id: "x", units: 0 }), log: () => undefined });
      chain.note(userId, "ODOFF1");
      await sleep(150);
      assert.deepEqual([chain.pending(userId), called, await chain.flush(userId)], [0, 0, null]);
    });

    const doneUpdate = async (extra: Record<string, unknown> = {}) => {
      const _id = oid();
      const at = new Date();
      await raw(OrderUpdateRequest).insertOne({ _id, userId: uid, source: "schedule", selectionMethod: "schedule_active", params: { platform: "FLIPKART", selection_method: "schedule_active" }, parentId: "", systemId: SYSTEM, deliveredOrderIds: [], createdBy: "schedule", status: "done", mode: "api", windows: 3, accounts: [{ email: "d1@verify.invalid", platformId: null, units: [unit("ODDEL1", "D11")], total: 1, status: "done", done: 1, failed: 0, statusChanged: 1, otpNew: 0, via: "api", message: "", startedAt: at, finishedAt: at }], logs: [], startedAt: at, finishedAt: at, createdAt: at, updatedAt: at, ...extra });
      return String(_id);
    };
    const delivered = { enabled: true, ordersWithInvoice: async (_u: string, ids: string[]) => ids.filter((id) => id === "ODDEL3"), createInvoice: ((u, input) => createInvoiceRequest(u, input, invoiceDeps)) as typeof createInvoiceRequest };
    await check("delivered check: the orders an update saw arrive at Delivered get ONE invoice fetch when the update ends — source system-update, parent = the update, orders with a stored invoice left out", async () => {
      const id = await doneUpdate();
      updateIds.delivered = id;
      for (const orderId of ["ODDEL1", "ODDEL2", "ODDEL1", "ODDEL3"]) await noteDeliveredOrder(id, orderId, delivered);
      assert.deepEqual((await OrderUpdateRequest.findById(id).lean())?.deliveredOrderIds, ["ODDEL1", "ODDEL2", "ODDEL3"], "an order is noted once");
      const out = await queueInvoicesAfterUpdate(id, delivered);
      assert.deepEqual([out.status, out.request_ids.length, out.orders, out.already_stored], ["queued", 1, 2, 1]);
      const doc = (await OrderInvoiceRequest.findById(out.request_ids[0]).lean())!;
      assert.deepEqual([doc.source, doc.createdBy, doc.parentId, doc.retryOf, doc.tasks.map((t) => t.orderId).sort(), doc.params.follows], ["system-update", "", id, "", ["ODDEL1", "ODDEL2"], "update"]);
      const child = await batchOf(out.request_ids[0]);
      assert.deepEqual([child.source, child.triggered_by, child.parent_id, child.follows?.label, child.follows?.what, child.summary], ["system-update", null, id, "Follows a nightly update", "nightly update", "Orders: 2 · After nightly update"]);
      assert.ok(child.settings.some((s) => s.label === "Queued after nightly update saw Delivered" && s.value === "Yes"));
      assert.deepEqual((await batchOf(id)).child_ids, out.request_ids, "the update's child_ids name the invoice fetch");
      assert.deepEqual(((await OrderUpdateRequest.findById(id).lean())?.params as { invoice_fetch?: { status: string } }).invoice_fetch?.status, "queued", "what happened is on the update");
      assert.ok((await listBatches(userId, { source: "system-update" }, log, probes)).results.some((r) => r.request_id === out.request_ids[0]), "the Delivered check source filter finds it");
      assert.ok((await listTasks(userId, { source: "system-update" }, log)).results.length >= 2, "and its tasks");
      // Two jobs ending the same update together queue once.
      const again = await queueInvoicesAfterUpdate(id, delivered);
      assert.deepEqual([again.status, again.reason, await OrderInvoiceRequest.countDocuments({ userId: uid, parentId: id })], ["skipped", "already handled for this update", 1]);
      await stopInvoiceRequest(userId, out.request_ids[0], "verify", invoiceDeps);
    });
    await check("delivered check: nothing delivered → nothing; every order already stored → nothing; an update that was stopped → left to the sweep; switched off (CHAIN_DELIVERED_CHECK=0) → nothing is noted or queued", async () => {
      const before = await OrderInvoiceRequest.countDocuments({ userId: uid });
      assert.equal((await queueInvoicesAfterUpdate(await doneUpdate(), delivered)).status, "nothing");
      const stored = await queueInvoicesAfterUpdate(await doneUpdate({ deliveredOrderIds: ["ODDEL3"] }), delivered);
      assert.deepEqual([stored.status, stored.reason, stored.already_stored], ["skipped", "every order already has a stored invoice", 1]);
      const stopped = await doneUpdate({ status: "cancelled", deliveredOrderIds: ["ODDEL7"] });
      assert.deepEqual(await queueInvoicesAfterUpdate(stopped, delivered), { status: "skipped", request_ids: [], orders: 0, already_stored: 0, reason: "the update did not finish" });
      const off = await doneUpdate();
      await noteDeliveredOrder(off, "ODDEL8", { ...delivered, enabled: false });
      assert.deepEqual((await OrderUpdateRequest.findById(off).lean())?.deliveredOrderIds, []);
      await OrderUpdateRequest.updateOne({ _id: off }, { $set: { deliveredOrderIds: ["ODDEL8"] } });
      assert.equal((await queueInvoicesAfterUpdate(off, { ...delivered, enabled: false })).status, "off");
      assert.equal(await OrderInvoiceRequest.countDocuments({ userId: uid }), before, "no invoice request was made by any of these");
    });

    await check("a sweep run is a request of the log: source system-cron, a task per order, written as the sweep goes — phase 2's orders join it, results carry the reference's error classes", async () => {
      const rec = recordSweepAsRequest(userId, { source: "system-cron", by: "", capPerPhase: 200 }, { resolveOrders: invoiceDeps.resolveOrders!, stopPollMs: 100 });
      await rec.onPhase("capture", ["ODSW1", "ODSW2", "ODSW3", "ODGHOST1"]);
      const id = rec.requestId();
      invoiceIds.sweep = id;
      const open = (await OrderInvoiceRequest.findById(id).lean())!;
      assert.deepEqual([open.source, open.createdBy, open.status, open.queue, open.systemId, open.tasks.length], ["system-cron", "", "in_progress", INVOICE_QUEUE, config.systemId, 4]);
      assert.deepEqual(open.params, { order_ids_count: 4, force: false, sweep: true, capture_count: 4, recapture_count: 0, cap_per_phase: 200 });
      await rec.onResult({ orderId: "ODGHOST1", status: "unknown_order" });
      await rec.onStart("ODSW1");
      assert.deepEqual({ ...(await OrderInvoiceRequest.findById(id).lean())!.counts }, counts({ total: 4, queued: 2, in_progress: 1, failed: 1 }));
      await rec.onResult({ orderId: "ODSW1", status: "downloaded", units: 1, parts: ["product", "transport"], sizeKb: 91 });
      await rec.onStart("ODSW2");
      await rec.onResult({ orderId: "ODSW2", status: "failed", units: 1, error: "invoice download answered HTTP 404" });
      await rec.onResult({ orderId: "ODSW3", status: "no_session", units: 1 });
      await rec.onPhase("recapture", ["ODSW4", "ODSW1"]);
      await rec.onStart("ODSW4");
      await rec.onResult({ orderId: "ODSW4", status: "downloaded", units: 1, parts: ["product", "protect_promise"], sizeKb: 120 });
      assert.equal(rec.stopped(), false);
      await rec.finish();
      const doc = (await OrderInvoiceRequest.findById(id).lean())!;
      assert.deepEqual([doc.status, doc.tasks.length], ["completed", 5], "ODSW1 was already a task: it is not listed twice");
      assert.deepEqual({ ...doc.counts }, counts({ total: 5, success: 2, failed: 3 }));
      assert.deepEqual([doc.params.capture_count, doc.params.recapture_count, doc.params.order_ids_count], [4, 2, 5]);
      const t = Object.fromEntries(doc.tasks.map((x) => [x.orderId, x]));
      assert.deepEqual([t.ODSW1.status, t.ODSW1.parts, t.ODSW2.errorClass, t.ODSW2.error, t.ODSW3.errorClass, t.ODGHOST1.errorClass], ["success", ["product", "transport"], "nothing_available", "Nothing available yet", "not_logged_in", "order_not_found"]);
      assert.ok(t.ODSW1.startedAt && t.ODSW1.finishedAt, "taken and answered are both on the task");
      const b = await batchOf(id);
      assert.deepEqual([b.source, b.task_type, b.triggered_by, b.status, b.summary, b.can_retry], ["system-cron", "invoice_fetch", null, "completed", "Orders: 5 · Capture: 4 · Re-capture: 2", true]);
      assert.ok(b.settings.some((s) => s.label === "Capture (delivered, no invoice yet)" && s.value === "4") && b.settings.some((s) => s.label === "Re-capture and settle" && s.value === "2"));
      assert.deepEqual(await logCounts(id), { ...doc.counts }, "its task rows = its counters");
      assert.equal((await OrderSyncTask.findOne({ requestId: id, key: "ODSW1" }).lean())?.detail, "Downloaded: product, transport", "a download's task says which parts it stored");
      assert.ok((await listBatches(userId, { source: "system-cron", task_type: "invoice_fetch" }, log, probes)).results.some((r) => r.request_id === id), "the Nightly cron source filter finds it");
      assert.equal(taskEndOf({ orderId: "X", status: "already" }).status, "success");
      // "Nothing available yet" retried like any invoice fetch: an ordinary batch on the batch queue.
      const retry = (await retryBatch(userId, id, { statuses: ["failed"], error_code: "nothing_available" }, "ravi", deps))!;
      const retried = (await OrderInvoiceRequest.findById(retry.request_id).lean())!;
      assert.deepEqual([retried.queue, retried.source, retried.tasks.map((x) => x.orderId)], [invoiceQueue, "system-retry", ["ODSW2"]]);
      await stopInvoiceRequest(userId, retry.request_id, "verify", invoiceDeps);
    });
    await check("Cancel on a sweep's request stops the sweep between two orders; what it never took ends 'Cancelled before it started'", async () => {
      const rec = recordSweepAsRequest(userId, { source: "user", by: "asha", capPerPhase: 200 }, { resolveOrders: invoiceDeps.resolveOrders!, stopPollMs: 100 });
      await rec.onPhase("capture", ["ODST1", "ODST2", "ODST3"]);
      const id = rec.requestId();
      await rec.onStart("ODST1");
      await rec.onResult({ orderId: "ODST1", status: "downloaded", units: 1, parts: ["product"], sizeKb: 80 });
      const out = (await stopBatch(userId, id, "ravi", deps))!;
      assert.deepEqual([out.status, out.source, out.triggered_by?.name], ["cancelled", "user", "asha"]);
      assert.deepEqual(out.counts, counts({ total: 3, success: 1, cancelled: 2 }));
      await sleep(350);
      assert.equal(rec.stopped(), true, "the sweep's own cancelled() now answers true: it takes no further order");
      await rec.onResult({ orderId: "ODST2", status: "downloaded", units: 1, parts: ["product"], sizeKb: 80 });
      assert.deepEqual((await rec.finish()).stopped, true);
      assert.deepEqual({ ...(await OrderInvoiceRequest.findById(id).lean())!.counts }, counts({ total: 3, success: 1, cancelled: 2 }), "an answer for an order the stop had already cancelled changes nothing");
    });
    await check("the sync panel's invoices row reads only the batches people start: ?source=user,system-retry&sweep=0 leaves out the sweep's runs and the Delivered check's fetches; without the filters every request is listed, as before", async () => {
      const all = await listInvoiceRequests(userId, 100);
      const people = await listInvoiceRequests(userId, 100, { sources: ["user", "system-retry"], sweep: false });
      // The public row does not carry `params.sweep`: which of them are sweep runs is read from their documents.
      const sweeps = new Set((await OrderInvoiceRequest.find({ userId: uid, "params.sweep": true }).select("_id").lean()).map((r) => String(r._id)));
      const isSweep = (r: { request_id: string }) => sweeps.has(r.request_id);
      assert.ok(all.some((r) => r.source === "system-update") && all.some(isSweep), "the unfiltered list has a Delivered-check fetch and a sweep run");
      assert.ok(people.length > 0 && people.every((r) => (r.source === "user" || r.source === "system-retry") && !isSweep(r)), `only batches people started: ${people.map((r) => `${r.source}${isSweep(r) ? " (sweep)" : ""}`).join(", ")}`);
      assert.deepEqual(people.map((r) => r.request_id), all.filter((r) => (r.source === "user" || r.source === "system-retry") && !isSweep(r)).map((r) => r.request_id), "the same requests, newest first");
      assert.ok((await listInvoiceRequests(userId, 100, { sweep: true })).every(isSweep), "sweep=1: the sweep's runs");
    });
    await check("a sweep with nothing to download makes no request (the real sweep, on a workspace with no orders)", async () => {
      const before = await OrderInvoiceRequest.countDocuments({ userId: uid, "params.sweep": true });
      const rec = recordSweepAsRequest(userId, { source: "system-cron", by: "", capPerPhase: 200 }, { resolveOrders: invoiceDeps.resolveOrders! });
      const result = await runInvoiceSweep(userId, { cancelled: rec.stopped, onPhase: rec.onPhase, onStart: rec.onStart, onResult: rec.onResult });
      await rec.finish();
      assert.deepEqual([result.phase1.candidates, result.phase2.candidates, rec.requestId()], [0, 0, ""]);
      assert.equal(await OrderInvoiceRequest.countDocuments({ userId: uid, "params.sweep": true }), before);
    });
    await check("the three switches: on by default, each off when its variable is 0", () => {
      const asSet = (name: string) => !["0", "off", "false", "no"].includes(String(process.env[name] ?? "1").trim().toLowerCase());
      assert.deepEqual([CHAIN_AFTER_ORDER, CHAIN_DELIVERED_CHECK, CHAIN_INVOICE_SWEEP], [asSet("CHAIN_AFTER_ORDER"), asSet("CHAIN_DELIVERED_CHECK"), asSet("CHAIN_INVOICE_SWEEP")]);
      const child = (env: Record<string, string>) => {
        const out = spawnSync(process.execPath, [...process.execArgv, fileURLToPath(import.meta.url), "--switches"], { env: { ...process.env, ...env }, encoding: "utf8" });
        return JSON.parse(out.stdout.split("\n").find((l) => l.startsWith("{")) || "{}");
      };
      assert.deepEqual(child({ CHAIN_AFTER_ORDER: "0", CHAIN_DELIVERED_CHECK: "0", CHAIN_INVOICE_SWEEP: "0" }), { after_order: false, delivered_check: false, invoice_sweep: false });
      assert.deepEqual(child({ CHAIN_AFTER_ORDER: "1", CHAIN_DELIVERED_CHECK: "0", CHAIN_INVOICE_SWEEP: "1" }), { after_order: true, delivered_check: false, invoice_sweep: true });
      assert.deepEqual(child({ CHAIN_AFTER_ORDER: "", CHAIN_DELIVERED_CHECK: "", CHAIN_INVOICE_SWEEP: "" }), { after_order: true, delivered_check: true, invoice_sweep: true });
    });
    await check("system-email (Order email) is a filter choice that matches nothing: no request has that source", async () => {
      assert.deepEqual([(await listBatches(userId, { source: "system-email" }, log, probes)).count, (await listTasks(userId, { source: "system-email" }, log)).count], [0, 0]);
    });
  } finally {
    // Everything this run made: the invented workspace's rows in the real collections, the throwaway collections, the two throwaway queues.
    await taskLog.flush();
    const removed = {
      tasks: (await OrderSyncTask.deleteMany({ userId: uid })).deletedCount,
      updates: (await OrderUpdateRequest.deleteMany({ userId: uid })).deletedCount,
      invoices: (await OrderInvoiceRequest.deleteMany({ userId: uid })).deletedCount,
      coupons: (await OrderCouponRequest.deleteMany({ userId: uid })).deletedCount,
      couponTasks: (await OrderCouponTask.deleteMany({ userId: uid })).deletedCount,
    };
    for (const coll of names) await mongoose.connection.db!.dropCollection(coll).catch(() => undefined);
    for (const name of [invoiceQueue, couponQueue]) {
      const queue = name === invoiceQueue ? getInvoiceBatchQueue(name) : getCouponQueue(name);
      await queue.obliterate({ force: true }).catch(() => undefined);
      await queue.close().catch(() => undefined);
    }
    await getInvoiceBatchQueue(INVOICE_QUEUE).close().catch(() => undefined);
    const left =
      (await OrderSyncTask.countDocuments({ userId: uid })) + (await OrderUpdateRequest.countDocuments({ userId: uid })) + (await OrderInvoiceRequest.countDocuments({ userId: uid })) + (await OrderCouponRequest.countDocuments({ userId: uid })) + (await OrderCouponTask.countDocuments({ userId: uid })) + (await CancelRequest.countDocuments({ userId: uid })) + (await RescheduleRequest.countDocuments({ userId: uid })) + (await OrderJobRun.countDocuments({ userId: uid }));
    const strays = (await mongoose.connection.db!.listCollections({ name: { $regex: `^verify_ra_${run}_` } }).toArray()).length;
    console.log(`\ncleanup: ${removed.tasks} task row(s), ${removed.updates} update, ${removed.invoices} invoice and ${removed.coupons} coupon request(s) (${removed.couponTasks} coupon task(s)) of the invented workspace deleted — ${left} left; ${names.length} throwaway collection(s) dropped (${strays} left); 2 throwaway queues obliterated`);
    if (left || strays) failures.push("cleanup");
    await mongoose.disconnect();
  }
  console.log(`${passed} passed, ${failures.length} failed`);
  process.exit(failures.length ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
