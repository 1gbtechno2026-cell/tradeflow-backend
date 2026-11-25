import { Types } from "mongoose";
import { Order } from "../models/Order.js";
import type { RequestTaskStatus } from "../models/OrderInvoiceRequest.js";
import { SYNC_TASK_KEEP_MS, SYNC_TASK_TYPE_OF, type SyncTaskType } from "../models/OrderSyncTask.js";
import { getCancelQueue, getRescheduleQueue } from "../queue.js";
import { createCouponRequest, stopCouponRequest } from "./couponRequests.js";
import { triggerFetch } from "./fetchTrigger.js";
import { parseInvoiceMode } from "./invoiceCapture.js";
import { createInvoiceRequest, stopInvoiceRequest } from "./invoiceRequests.js";
import { requestCancellation, type CancelRequestInput } from "./orderCancel.js";
import { stopOrderFetch } from "./orderFetch.js";
import { requestReschedule, type RescheduleRequestInput } from "./orderReschedule.js";
import { cancelUpdateRequest, createUpdateRequest, type PreparedUpdateUnit } from "./orderUpdateQueue.js";
import { clearedByStop, distinct, ERROR_CLASSES, RequestInputError } from "./requestCommon.js";
import { liveProbes } from "./requestLogLive.js";
import { holdUpdatePreparation, UPDATE_PREPARING_MESSAGE, type FetchBody } from "./startRequests.js";
import { cancelRefusal, findRequest, getBatch, INCOMPLETE, shortId, TASK_STATUS_LABEL, type LiveProbes } from "./syncBatches.js";
import { logDefaults, openTasksAmong, rowsInStep, syncRequestTasks, taskLog, updateTaskKey, type LogDeps, type OpenTask, type RequestFacts } from "./syncTaskLog.js";

/**
 * Batch requests — the two actions on a request.
 *
 *   stopBatch    POST /api/orders/sync/batches/:id/cancel
 *                the reference's row action "Cancel", for every kind. It calls
 *                the stop each kind already has, by the request's id:
 *
 *                  fetch       Force kill (stopOrderFetch) — when this process
 *                              is running it; a row no process is behind is
 *                              closed as stopped
 *                  update      POST /update/stop (cancelUpdateRequest)
 *                  invoices    POST /invoice/requests/:id/stop
 *                  coupons     POST /company-coupon/requests/:id/stop
 *                  cancel,     had no stop, only an admin Force-clear. Here:
 *                  reschedule  accounts no worker has taken are closed and
 *                              their Order rows freed; an account a worker is
 *                              on finishes the unit in hand
 *
 *                What was queued ends "Cancelled before it started", what was
 *                running finishes or ends "Stopped while running: the request
 *                was cancelled", and the request reads `cancelled` with its
 *                tally kept.
 *
 *   retryBatch   POST /api/orders/sync/batches/:id/retry
 *                { statuses: ["failed"] } | { all_incomplete: true } (the
 *                reference's) | { statuses: […], error_code } (ours: one
 *                error class of an ending) → ONE new request of the same kind
 *                and settings for exactly those tasks' items, source
 *                `system-retry`, parent = the request it retries. An item
 *                that is queued or running in any request is skipped. Each
 *                kind is started through the function its own trigger uses,
 *                so every refusal that trigger has is answered here in the
 *                same words.
 *
 * Cancel and reschedule act on real orders. Their retry goes through
 * requestCancellation / requestReschedule — the functions the Cancel and
 * Reschedule modals post to — so a unit that is delivered, cancelled or
 * already being cancelled is refused there as it always was; a dry run stays a
 * dry run; a reschedule date that has passed is refused. What Flipkart itself
 * refused (not cancellable, date change not offered, awaiting review) is never
 * submitted again.
 */

// ── what the actions need from outside ──────────────────────────────────────

/** A cancel / reschedule request's job for one account, on its queue. */
export interface UnitsQueue {
  /** "active" = a worker holds it now. */
  state: (jobId: string) => Promise<"active" | "waiting" | "missing">;
  /** Take a job no worker holds off the queue. False when it could not be (a worker took it meanwhile). */
  remove: (jobId: string) => Promise<boolean>;
}

type UnitsKind = "cancel" | "reschedule";

export interface ActionDeps {
  log: LogDeps;
  probes: LiveProbes;
  stopFetch: (userId: string) => Promise<void>;
  stopUpdate: (userId: string, requestId: string) => Promise<unknown>;
  stopInvoice: (userId: string, requestId: string, by: string) => Promise<unknown>;
  stopCoupon: (userId: string, requestId: string, by: string) => Promise<unknown>;
  unitsQueue: (kind: UnitsKind) => UnitsQueue;
  /** Free the Order rows a cancel / reschedule request parked for these units. Answers how many. */
  freeOrderRows: (kind: UnitsKind, userId: string, requestId: string, unitIds: string[]) => Promise<number>;
  startFetch: (userId: string, body: FetchBody, extra: { source: string; params: Record<string, unknown> }) => Promise<{ request_id: string; tasks_created: number }>;
  startUpdate: (userId: string, units: PreparedUpdateUnit[], opts: { by: string; parentId: string; params: Record<string, unknown> }) => Promise<{ id: string; units: number }>;
  startInvoice: (userId: string, input: { orderIds: string[]; force: boolean; by: string; source: string; retryOf: string; parentId: string; params: Record<string, unknown> }) => Promise<{ request_id: string; tasks_created: number }>;
  startCoupon: (userId: string, input: { links: string[]; emails: string[]; by: string; source: string; retryOf: string; parentId: string; params: Record<string, unknown>; anyNumberOfEmails: boolean }) => Promise<{ request_id: string; tasks_created: number }>;
  startCancel: (userId: string, input: CancelRequestInput) => Promise<{ id: string; units: unknown[]; rejected: unknown[] }>;
  startReschedule: (userId: string, input: RescheduleRequestInput) => Promise<{ id: string; units: unknown[]; rejected: unknown[] }>;
  holdUpdate: (userId: string) => (() => void) | null;
}

function bullQueue(kind: UnitsKind): UnitsQueue {
  const queue = kind === "cancel" ? getCancelQueue() : getRescheduleQueue();
  return {
    state: async (jobId) => {
      const job = await queue.getJob(jobId).catch(() => null);
      if (!job) return "missing";
      return (await job.isActive().catch(() => false)) ? "active" : "waiting";
    },
    // A job a worker holds cannot be removed (it throws).
    remove: async (jobId) => {
      const job = await queue.getJob(jobId).catch(() => null);
      return job ? job.remove().then(() => true, () => false) : true;
    },
  };
}

export const actionDefaults: ActionDeps = {
  log: logDefaults,
  probes: liveProbes,
  stopFetch: stopOrderFetch,
  stopUpdate: cancelUpdateRequest,
  stopInvoice: stopInvoiceRequest,
  stopCoupon: stopCouponRequest,
  unitsQueue: bullQueue,
  freeOrderRows: async (kind, userId, requestId, unitIds) => {
    if (!unitIds.length) return 0;
    const res =
      kind === "cancel"
        ? await Order.updateMany(
            { userId, unit_id: { $in: unitIds }, cancel_request_id: requestId, cancel_status: { $in: ["queued", "running"] } },
            { $set: { cancel_status: "", cancel_request_id: "", cancel_requested_at: null, cancel_requested_by: "", cancel_error: "" } }
          )
        : await Order.updateMany(
            { userId, unit_id: { $in: unitIds }, reschedule_request_id: requestId, reschedule_status: { $in: ["queued", "running"] } },
            { $set: { reschedule_status: "", reschedule_request_id: "", reschedule_date: "", reschedule_message: "" } }
          );
    return res.modifiedCount || 0;
  },
  startFetch: (userId, body, extra) => triggerFetch(userId, body, extra),
  startUpdate: async (userId, units, opts) => {
    const request = await createUpdateRequest(userId, { selectionMethod: "unit_ids" }, { source: "system-retry", by: opts.by, parentId: opts.parentId, params: opts.params, units });
    return { id: request.id, units: request.counts.units };
  },
  startInvoice: (userId, input) => createInvoiceRequest(userId, input),
  startCoupon: (userId, input) => createCouponRequest(userId, input),
  startCancel: (userId, input) => requestCancellation(userId, input),
  startReschedule: (userId, input) => requestReschedule(userId, input),
  holdUpdate: holdUpdatePreparation,
};

const isOpen = (f: RequestFacts) => f.status === "queued" || f.status === "in_progress";
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// ── stop ────────────────────────────────────────────────────────────────────

/** How long the answer waits for a fetch that was force-killed to close its own row. */
const FETCH_STOP_WAIT_MS = 4000;

async function stopFetchRun(userId: string, f: RequestFacts, by: string, deps: ActionDeps): Promise<void> {
  const id = f.head.requestId;
  const mine = Boolean(f.host) && f.host === deps.probes.host && f.pid === deps.probes.pid;
  if (mine && deps.probes.fetchLive(userId)) {
    // Force kill, as POST /fetch/stop does: the run closes its own row as stopped and its accounts in the task log.
    await deps.stopFetch(userId);
    const until = Date.now() + FETCH_STOP_WAIT_MS;
    while (Date.now() < until) {
      const row = await deps.log.Ledger.findById(id).select("status").lean();
      if (!row || (row.status !== "queued" && row.status !== "running")) break;
      await sleep(200);
    }
    return;
  }
  // No process is behind this row — it names this machine and a process that is gone, or (written before 2026-11-24)
  // no machine at all: there is nothing to kill, the row is closed here and its open accounts end with it.
  await deps.log.Ledger.updateOne({ _id: id, type: "fetch", status: { $in: ["queued", "running"] } }, { $set: { status: "stopped", finishedAt: new Date(), message: `Stopped by ${by || "a user"} — no process was running it` } });
  await syncRequestTasks("fetch", id, deps.log);
}

/**
 * Cancel a cancel / reschedule request. An account whose job no worker holds
 * is closed here — its job leaves the queue, its queued units end "Cleared by
 * … — the request was cancelled" (a unit a dead worker left running ends as
 * cut off) and the Order rows it parked are freed, so the units can be
 * submitted again. An account a worker is on is left to that worker: it
 * finishes the unit in hand and, reading the request's `stoppedAt` before the
 * next one, leaves the rest (services/orderCancel.ts, orderReschedule.ts — a
 * worker started before 2026-11-24 does not read it and finishes the account).
 * The request ends once nothing of it is running.
 */
async function stopUnitsRequest(userId: string, f: RequestFacts, by: string, deps: ActionDeps): Promise<{ closed_accounts: number; running_accounts: number; freed_rows: number }> {
  const kind = f.head.kind as UnitsKind;
  const id = f.head.requestId;
  const _id = new Types.ObjectId(id);
  const open = ["queued", "running"];
  const now = new Date();
  const who = by || "a user";
  const out = { closed_accounts: 0, running_accounts: 0, freed_rows: 0 };

  let units: Array<{ unitId: string; email: string; status: string }> = [];
  let orders: Array<{ email: string; status: string }> = [];
  if (kind === "cancel") {
    // A request written before urgent mode has no `orders` array, and an array update on a missing path is refused.
    await deps.log.Cancel.updateOne({ _id, orders: { $exists: false } }, { $set: { orders: [] } });
    const row = await deps.log.Cancel.findOne({ _id, userId }).select("status units.unitId units.email units.status orders.email orders.status").lean();
    if (!row || row.status === "done") return out;
    units = row.units || [];
    orders = row.orders || [];
    await deps.log.Cancel.updateOne({ _id, stoppedAt: null }, { $set: { stoppedAt: now, stoppedBy: who } });
  } else {
    const row = await deps.log.Reschedule.findOne({ _id, userId }).select("status units.unitId units.email units.status").lean();
    if (!row || row.status === "done") return out;
    units = row.units || [];
    await deps.log.Reschedule.updateOne({ _id, stoppedAt: null }, { $set: { stoppedAt: now, stoppedBy: who } });
  }

  // With no worker on the queue an "active" job is a dead worker's: nobody will finish that account.
  const workerThere = await deps.probes.queueHasWorker(f.queue).catch(() => true);
  const queue = deps.unitsQueue(kind);
  const emails = distinct([...units, ...orders].filter((u) => open.includes(u.status)).map((u) => u.email));
  const closed: string[] = [];
  for (const email of emails) {
    const jobId = `${id}-${email}`;
    const held = async () => workerThere && (await queue.state(jobId)) === "active";
    if ((await held()) || (!(await queue.remove(jobId)) && (await held()))) out.running_accounts += 1;
    else closed.push(email);
  }
  out.closed_accounts = closed.length;

  if (closed.length) {
    const note = clearedByStop(who);
    const unitFilter = { "u.email": { $in: closed }, "u.status": { $in: open } };
    const unitSet = { "units.$[u].status": "failed", "units.$[u].message": note, "units.$[u].finishedAt": now };
    if (kind === "cancel") {
      await deps.log.Cancel.updateOne(
        { _id },
        { $set: { ...unitSet, "orders.$[o].status": "failed", "orders.$[o].message": note, "orders.$[o].finishedAt": now } },
        { arrayFilters: [unitFilter, { "o.email": { $in: closed }, "o.status": { $in: open } }] }
      );
    } else {
      await deps.log.Reschedule.updateOne({ _id }, { $set: unitSet }, { arrayFilters: [unitFilter] });
    }
    out.freed_rows = await deps.freeOrderRows(kind, userId, id, units.filter((u) => closed.includes(u.email) && open.includes(u.status)).map((u) => u.unitId));
  }

  // Nothing of it queued or running any more: the request has ended (it reads "cancelled" — stoppedAt is set).
  if (kind === "cancel") await deps.log.Cancel.updateOne({ _id, status: { $ne: "done" }, "units.status": { $nin: open }, "orders.status": { $nin: open } }, { $set: { status: "done", finishedAt: now } });
  else await deps.log.Reschedule.updateOne({ _id, status: { $ne: "done" }, "units.status": { $nin: open } }, { $set: { status: "done", finishedAt: now } });
  await syncRequestTasks(kind, id, deps.log);
  return out;
}

/**
 * Cancel one request by id. Null = no such request in this workspace. A
 * request that has ended, one that is another machine's to stop, and one
 * already being cancelled are refused with the reason (409).
 */
export async function stopBatch(userId: string, requestId: string, by = "", overrides: Partial<ActionDeps> = {}) {
  const deps = { ...actionDefaults, ...overrides };
  const f = await findRequest(userId, requestId, deps.log);
  if (!f) return null;
  if (!isOpen(f)) throw new RequestInputError("This request has already ended — there is nothing to cancel", 409, "not_open");
  const refusal = cancelRefusal(f, deps.probes);
  if (refusal) throw new RequestInputError(refusal, 409, f.stopRequested ? "already_cancelling" : "other_machine");

  let detail: Record<string, unknown> = {};
  if (f.head.kind === "fetch") await stopFetchRun(userId, f, by, deps);
  else if (f.head.kind === "update") detail = ((await deps.stopUpdate(userId, requestId)) as Record<string, unknown> | null) || {};
  else if (f.head.kind === "invoices") detail = { removed: ((await deps.stopInvoice(userId, requestId, by)) as { removed?: number } | null)?.removed ?? 0 };
  else if (f.head.kind === "coupons") detail = { removed: ((await deps.stopCoupon(userId, requestId, by)) as { removed?: number } | null)?.removed ?? 0 };
  else detail = await stopUnitsRequest(userId, f, by, deps);

  await taskLog.flush();
  const batch = await getBatch(userId, requestId, deps.log, deps.probes);
  if (!batch) return null;
  const ended = batch.status !== "queued" && batch.status !== "in_progress";
  console.log(`[batches] ${SYNC_TASK_TYPE_OF[f.head.kind]} ${shortId(requestId)} cancelled by ${by || "a user"} → ${batch.status}`);
  return {
    ...batch,
    message: ended ? "Request cancelled" : "Cancelling — what is running now finishes first",
    cancelled_by: by,
    // What the kind's own stop answered: jobs removed from the queue, accounts closed / still running.
    stop: detail,
  };
}

// ── retry ───────────────────────────────────────────────────────────────────

export interface RetryBody {
  statuses: RequestTaskStatus[];
  /** Only the failed tasks of this error class ("" = every class). */
  errorCode: string;
  allIncomplete: boolean;
}

/**
 * The reference's two bodies — { statuses: ["failed"] } (an ending: a line of
 * ISSUES, or the row's Retry) and { all_incomplete: true } (failed + timed out
 * + aborted + cancelled) — and ours, { statuses: […], error_code }: one error
 * class of an ending (the class lines the dashboard lists under it).
 */
export function parseRetryBody(body: unknown): RetryBody {
  const b = (body && typeof body === "object" ? body : {}) as Record<string, unknown>;
  if (b.all_incomplete === true) return { statuses: [...INCOMPLETE], errorCode: "", allIncomplete: true };
  const raw = Array.isArray(b.statuses) ? b.statuses : typeof b.statuses === "string" ? b.statuses.split(/[\s,]+/) : [];
  // The counts spell it `timed_out`, a task's status `timedout`: either is taken.
  const statuses = distinct(raw.map((s) => String(s ?? "").trim().toLowerCase()).filter(Boolean).map((s) => (s === "timed_out" ? "timedout" : s)));
  if (!statuses.length) throw new RequestInputError('Say what to retry: { "statuses": ["failed"] } or { "all_incomplete": true }');
  const bad = statuses.filter((s) => !INCOMPLETE.includes(s as RequestTaskStatus));
  if (bad.length) throw new RequestInputError(`Only unsuccessful tasks can be retried (${INCOMPLETE.join(", ")}) — not "${bad[0]}"`);
  const errorCode = String(b.error_code ?? "").trim();
  if (errorCode && !(errorCode in ERROR_CLASSES)) throw new RequestInputError(`"${errorCode}" is not an error class`);
  return { statuses: statuses as RequestTaskStatus[], errorCode, allIncomplete: false };
}

interface RetryTask {
  key: string;
  email: string;
  orderId: string;
  unitId: string;
  status: RequestTaskStatus;
}

interface Started {
  request_id: string;
  tasks_created: number;
  /** Tasks left out because this kind never submits them again (Flipkart's own refusals). */
  not_retried: number;
}

const nothing = (message: string) => new RequestInputError(message, 409, "nothing_to_retry");

/** "failed", "failed or timed-out", "failed, timed-out, aborted or cancelled". */
function statusWords(statuses: RequestTaskStatus[]): string {
  const words = statuses.map((s) => (s === "timedout" ? "timed-out" : s));
  return words.length < 2 ? words.join("") : `${words.slice(0, -1).join(", ")} or ${words[words.length - 1]}`;
}

/** The reference's skip: an item that is queued or running in ANY request is left to that request. */
async function leaveOpenItems(userId: string, taskType: SyncTaskType, by: "orderId" | "email", tasks: RetryTask[], sameItem: (open: OpenTask, task: RetryTask) => boolean, deps: ActionDeps): Promise<{ take: RetryTask[]; skipped: RetryTask[] }> {
  const open = await openTasksAmong(userId, taskType, by, tasks.map((t) => t[by]), deps.log);
  if (!open.length) return { take: tasks, skipped: [] };
  const held = new Map<string, OpenTask[]>();
  for (const o of open) held.set(o[by], [...(held.get(o[by]) || []), o]);
  const take: RetryTask[] = [];
  const skipped: RetryTask[] = [];
  for (const t of tasks) ((held.get(t[by]) || []).some((o) => sameItem(o, t)) ? skipped : take).push(t);
  return { take, skipped };
}

const sameAccount = () => true;
const sameOrder = () => true;
const sameUnit = (open: OpenTask, task: RetryTask) => open.unitId === task.unitId;

/**
 * Queue the unsuccessful tasks of a request again, as one new request. Null =
 * no such request in this workspace. Refused (409) while the request is still
 * open, once its tasks are no longer kept, when no task matches, and when
 * every matching item is already queued or running elsewhere; anything the
 * kind's own trigger refuses is answered in its words.
 */
export async function retryBatch(userId: string, requestId: string, rawBody: unknown, by = "", overrides: Partial<ActionDeps> = {}) {
  const deps = { ...actionDefaults, ...overrides };
  const body = parseRetryBody(rawBody);
  const f = await findRequest(userId, requestId, deps.log);
  if (!f) return null;
  const kind = f.head.kind;
  const taskType = SYNC_TASK_TYPE_OF[kind];
  if (isOpen(f)) throw new RequestInputError("This request is still open — cancel it or wait for it to end, then retry", 409, "still_open");
  if (f.head.createdAt.getTime() < Date.now() - SYNC_TASK_KEEP_MS) throw new RequestInputError("The tasks of this request are no longer kept (tasks are kept for 7 days) — start a new request instead", 409, "tasks_expired");

  // The task rows decide what is retried: they are brought in line with the request first, however long that takes.
  await taskLog.flush();
  if (!(await rowsInStep(kind, requestId, deps.log))) await syncRequestTasks(kind, requestId, deps.log);
  const tasks = (await deps.log.Tasks.find({ requestId, status: { $in: body.statuses }, ...(body.errorCode ? { errorCode: body.errorCode } : {}) })
    .select("key email orderId unitId status -_id")
    .lean()) as RetryTask[];
  const what = `${statusWords(body.statuses)} task${body.errorCode ? ` of the class "${ERROR_CLASSES[body.errorCode as keyof typeof ERROR_CLASSES]}"` : ""}`;
  if (!tasks.length) throw nothing(`Nothing to retry — this request has no ${what}`);

  const retryParams: Record<string, unknown> = { retry_of: requestId, parent_id: requestId, follows: "retry", retry_statuses: body.statuses, ...(body.errorCode ? { error_code: body.errorCode } : {}) };
  let split: { take: RetryTask[]; skipped: RetryTask[] };
  let started: Started;
  const allHeld = (n: number) => new RequestInputError(`Nothing was queued — all ${n} item(s) are already queued or running in another request`, 409, "already_queued");

  if (kind === "fetch") {
    split = await leaveOpenItems(userId, taskType, "email", tasks, sameAccount, deps);
    if (!split.take.length) throw allHeld(tasks.length);
    const p = f.params;
    const made = await deps.startFetch(
      userId,
      {
        platform: "FLIPKART",
        emails: distinct(split.take.map((t) => t.email)),
        afterDate: String(p.fetch_orders_after_date || ""),
        // Logging accounts in is the dashboard's Enable login, not a retry's: an account that is not logged in is skipped and listed, as in any fetch.
        enableLogin: false,
        forceFetch: p.force_fetch === true,
        runUpdateAfterFetch: p.run_update_after_fetch === true,
        invoices: parseInvoiceMode(p.invoices),
        trigger: "user",
        by,
        parentId: requestId,
        follows: "retry",
      },
      { source: "system-retry", params: retryParams }
    );
    started = { request_id: made.request_id, tasks_created: made.tasks_created, not_retried: 0 };
  } else if (kind === "coupons") {
    split = await leaveOpenItems(userId, taskType, "email", tasks, sameAccount, deps);
    if (!split.take.length) throw allHeld(tasks.length);
    const doc = await deps.log.Coupon.findById(requestId).select("links").lean();
    const made = await deps.startCoupon(userId, { links: doc?.links || [], emails: distinct(split.take.map((t) => t.email)), by, source: "system-retry", retryOf: requestId, parentId: requestId, params: retryParams, anyNumberOfEmails: true });
    started = { request_id: made.request_id, tasks_created: made.tasks_created, not_retried: 0 };
  } else if (kind === "invoices") {
    split = await leaveOpenItems(userId, taskType, "orderId", tasks, sameOrder, deps);
    if (!split.take.length) throw allHeld(tasks.length);
    const made = await deps.startInvoice(userId, { orderIds: distinct(split.take.map((t) => t.orderId)), force: f.params.force === true, by, source: "system-retry", retryOf: requestId, parentId: requestId, params: retryParams });
    started = { request_id: made.request_id, tasks_created: made.tasks_created, not_retried: 0 };
  } else if (kind === "update") {
    // The slot the update trigger takes while it puts a request together: a retry is one more such request.
    const release = deps.holdUpdate(userId);
    if (!release) throw new RequestInputError(UPDATE_PREPARING_MESSAGE, 409, "update_preparing");
    try {
      split = await leaveOpenItems(userId, taskType, "orderId", tasks, sameUnit, deps);
      if (!split.take.length) throw allHeld(tasks.length);
      // The units as the request itself stored them — the selection is not resolved a second time.
      const doc = await deps.log.Update.findById(requestId).select("accounts.email accounts.platformId accounts.units").lean();
      const wanted = new Set(split.take.map((t) => t.key));
      const units: PreparedUpdateUnit[] = [];
      for (const a of doc?.accounts || []) {
        for (const u of a.units || []) {
          if (wanted.has(updateTaskKey(a.email, u.unitId))) units.push({ email: a.email, platformId: a.platformId, orderId: u.orderId, itemId: u.itemId || "", unitId: u.unitId, orderUrl: u.orderUrl || "" });
        }
      }
      if (!units.length) throw nothing("Nothing to retry — the request no longer lists these units");
      const made = await deps.startUpdate(userId, units, {
        by,
        parentId: requestId,
        params: { platform: "FLIPKART", selection_method: "unit_ids", force: false, statuses_to_update: [], status_keys: [], email_ids_count: 0, order_ids_count: distinct(units.map((u) => u.orderId)).length, order_email_pairs_count: 0, units_count: units.length, ...retryParams },
      });
      started = { request_id: made.id, tasks_created: made.units, not_retried: 0 };
    } finally {
      release();
    }
  } else if (kind === "cancel") {
    const doc = await deps.log.Cancel.findById(requestId).select("dryRun units.unitId units.status orders.orderId orders.email orders.status").lean();
    // Stored "failed" is all a cancel retry ever takes — a failure, a unit cleared before it ran, one that timed out. Never what Flipkart refused.
    const failedUnits = new Set((doc?.units || []).filter((u) => u.status === "failed").map((u) => u.unitId));
    const failedOrders = new Map((doc?.orders || []).filter((o) => o.status === "failed").map((o) => [o.orderId, o.email]));
    const eligible = tasks.filter((t) => (t.key.startsWith("o:") ? failedOrders.has(t.orderId) : failedUnits.has(t.unitId)));
    if (!eligible.length) throw nothing("Nothing to retry — Flipkart itself refused these units (not cancellable); they are not submitted again");
    split = await leaveOpenItems(userId, taskType, "orderId", eligible, sameUnit, deps);
    if (!split.take.length) throw allHeld(eligible.length);
    const unitIds = split.take.filter((t) => !t.key.startsWith("o:")).map((t) => t.unitId);
    const orders = split.take.filter((t) => t.key.startsWith("o:")).map((t) => ({ orderId: t.orderId, email: failedOrders.get(t.orderId) || t.email }));
    const common = { dryRun: Boolean(doc?.dryRun), by, retryOf: requestId, failIfEmpty: true, source: "system-retry" };
    // As "Retry failed" of the Cancellations tab builds it: units by unit id, order pages that could not be read as urgent orders.
    const made = unitIds.length
      ? await deps.startCancel(userId, { type: "unit", ids: unitIds, extraOrders: orders, ...common })
      : await deps.startCancel(userId, { type: "order", ids: orders.map((o) => o.orderId), accounts: Object.fromEntries(orders.map((o) => [o.orderId, o.email])), urgent: true, ...common });
    started = { request_id: made.id, tasks_created: made.units.length + orders.length, not_retried: tasks.length - eligible.length };
  } else {
    const doc = await deps.log.Reschedule.findById(requestId).select("date units.unitId units.status").lean();
    const failedUnits = new Set((doc?.units || []).filter((u) => u.status === "failed").map((u) => u.unitId));
    const eligible = tasks.filter((t) => failedUnits.has(t.unitId));
    if (!eligible.length) throw nothing("Nothing to retry — Flipkart itself answered for these units (date change not offered, or awaiting review); they are not submitted again");
    split = await leaveOpenItems(userId, taskType, "orderId", eligible, sameUnit, deps);
    if (!split.take.length) throw allHeld(eligible.length);
    // The date the request asked for; one that has passed is refused by the reschedule's own check.
    const made = await deps.startReschedule(userId, { type: "unit", ids: split.take.map((t) => t.unitId), date: String(doc?.date || ""), by, retryOf: requestId, failIfEmpty: true, source: "system-retry" });
    started = { request_id: made.id, tasks_created: made.units.length, not_retried: tasks.length - eligible.length };
  }

  const label = (t: RetryTask) => t.unitId || t.orderId || t.email;
  console.log(`[batches] ${taskType} ${shortId(requestId)} retried by ${by || "a user"} → ${shortId(started.request_id)}: ${split.take.length} item(s), ${split.skipped.length} already queued or running${started.not_retried ? `, ${started.not_retried} not retried` : ""}`);
  return {
    message: `Retry queued — ${split.take.length} ${statusWords(body.statuses)} task(s) as a new request${split.skipped.length ? `; ${split.skipped.length} already queued or running were skipped` : ""}`,
    request_id: started.request_id,
    short_id: shortId(started.request_id),
    parent_id: requestId,
    platform: f.head.platform,
    task_type: taskType,
    source: "system-retry",
    triggered_by: by,
    statuses: body.statuses,
    status_labels: body.statuses.map((s) => TASK_STATUS_LABEL[s]),
    error_code: body.errorCode || null,
    all_incomplete: body.allIncomplete,
    tasks_matched: tasks.length,
    tasks_retried: split.take.length,
    tasks_created: started.tasks_created,
    skipped_in_progress: split.skipped.length,
    skipped_items: split.skipped.slice(0, 200).map(label),
    not_retried: started.not_retried,
    timestamp: Math.floor(Date.now() / 1000),
  };
}
