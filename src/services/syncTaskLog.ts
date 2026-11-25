import { Types, type Model } from "mongoose";
import { CancelRequest, type ICancelRequest } from "../models/CancelRequest.js";
import { OrderCouponRequest, OrderCouponTask, type IOrderCouponRequest, type IOrderCouponTask } from "../models/OrderCouponRequest.js";
import { OrderInvoiceRequest, countKeyOf, type IOrderInvoiceRequest, type RequestCounts, type RequestStatus, type RequestTaskStatus } from "../models/OrderInvoiceRequest.js";
import { OrderJobRun, type IOrderJobRun } from "../models/OrderJobRun.js";
import { OrderSyncTask, SYNC_TASK_TYPE_OF, type IOrderSyncTask, type SyncTaskKind, type SyncTaskType } from "../models/OrderSyncTask.js";
import { OrderUpdateRequest, type IOrderUpdateRequest, type IUpdateAccount, type IUpdateLog } from "../models/OrderUpdateRequest.js";
import { RescheduleRequest, type IRescheduleRequest } from "../models/RescheduleRequest.js";
import { ABORTED_WHILE_RUNNING, CANCELLED_BEFORE_START, classified, classifyMessage, countsOf, emptyCounts, ERROR_CLASSES, TIMED_OUT, type ErrorClass } from "./requestCommon.js";

/**
 * The task log (order_sync_tasks) — how it is written.
 *
 * A request of any kind already keeps what its job needs in its own document.
 * The log is the same work as one row per task, so "All tasks" can list,
 * filter and page across every request. Three things write it, and all three
 * go through this file:
 *
 *   syncRequestTasks   reads ONE request document, works out the task rows it
 *                      implies (the seeds), and inserts the missing rows and
 *                      moves the rows that are behind — in bulk, one read of
 *                      the log and one bulkWrite. Called when a request is
 *                      created, stopped and settled, by the API's keeper for
 *                      open requests (services/syncBatches.ts), and for
 *                      history. A row that has ended is never changed by it.
 *   taskLog.mark       the code doing the work says how one task went. The
 *                      marks are buffered and written together about once a
 *                      second — never one round trip per task. Used where the
 *                      request document does not hold the answer per task:
 *                      update (per unit), fetch (per account), coupons.
 *   openFetchTasks     the fetch has no request document with its accounts:
 *                      its rows are inserted at the start of the run and its
 *                      per-account counts are kept on the ledger row
 *                      (meta.task_counts).
 *
 * Why mirror, and not read the invoice tasks and order_coupon_tasks where they
 * are: "All tasks" is one list with one sort, one page and one set of filters
 * across six kinds. A union over an embedded array, a second collection and
 * four other shapes cannot be paged or counted in the database; one collection
 * with its own indexes can. The request's own counters stay what the batch
 * list shows, and syncRequestTasks is what keeps the rows equal to them.
 *
 * A worker started before this file existed writes nothing here. Its requests
 * still get their rows: the keeper reads the request document and moves them.
 * What such a worker never recorded per task — which unit of an account failed
 * in an update — is taken from the request's own log lines where they still
 * say, and is marked as not recorded where they do not.
 */

export const OPEN_TASK: RequestTaskStatus[] = ["queued", "in_progress"];
const isOpen = (s: string) => s === "queued" || s === "in_progress";

export interface LogDeps {
  Tasks: Model<IOrderSyncTask>;
  Ledger: Model<IOrderJobRun>;
  Update: Model<IOrderUpdateRequest>;
  Invoice: Model<IOrderInvoiceRequest>;
  Coupon: Model<IOrderCouponRequest>;
  CouponTask: Model<IOrderCouponTask>;
  Cancel: Model<ICancelRequest>;
  Reschedule: Model<IRescheduleRequest>;
}

/** The real collections. The checks swap in throwaway ones with the same schemas. */
export const logDefaults: LogDeps = {
  Tasks: OrderSyncTask,
  Ledger: OrderJobRun,
  Update: OrderUpdateRequest,
  Invoice: OrderInvoiceRequest,
  Coupon: OrderCouponRequest,
  CouponTask: OrderCouponTask,
  Cancel: CancelRequest,
  Reschedule: RescheduleRequest,
};

// ── shapes ──────────────────────────────────────────────────────────────────

/** One task as its request document implies it. */
export interface TaskSeed {
  key: string;
  email: string;
  orderId: string;
  unitId: string;
  status: RequestTaskStatus;
  error: string;
  errorCode: string;
  detail: string;
  startedAt: Date | null;
  finishedAt: Date | null;
}

export interface RequestHead {
  userId: Types.ObjectId;
  requestId: string;
  kind: SyncTaskKind;
  platform: string;
  source: string;
  triggeredBy: string;
  createdAt: Date;
}

/** A request of any kind as the batch list needs it — read without its task arrays. */
export interface RequestFacts {
  head: RequestHead;
  status: RequestStatus;
  /** The request's own counters, in the reference's keys. */
  counts: RequestCounts;
  startedAt: Date | null;
  finishedAt: Date | null;
  updatedAt: Date | null;
  params: Record<string, unknown>;
  parentId: string;
  retryOf: string;
  childIds: string[];
  message: string;
  /** The BullMQ queue its jobs are on; "" for a fetch, which runs inside the API. */
  queue: string;
  /** The deployment whose Redis holds its jobs ("" = not recorded on this document). */
  systemId: string;
  /** Fetch only: the machine and process running it. */
  host: string;
  pid: number;
  /**
   * How much of the request has task rows: "all", or "named" — a fetch that
   * ran before the log existed kept only the accounts it could not fetch (and
   * at most its first 500 e-mails), so only those can be listed.
   */
  tasksRecorded: "all" | "named";
  /**
   * Cancel / reschedule only: Batch requests → Cancel has been pressed and an
   * account a worker holds is still finishing. The request stays open until
   * it has; it cannot be cancelled a second time.
   */
  stopRequested: boolean;
  /**
   * How many of the unsuccessful tasks a retry may take, when that is fewer
   * than all of them: a cancel or reschedule never re-submits what Flipkart
   * itself refused (not cancellable, date change not offered, awaiting
   * review). null = every unsuccessful task.
   */
  retryable: number | null;
  /**
   * How many FAILED tasks a retry may take, when that is fewer than
   * counts.failed: a fetch from before the log can only take the accounts its
   * rows name (services/syncBatches.ts countNamedRetryable). Unset = counts.failed.
   */
  retryableFailed?: number | null;
}

type Outcome = Pick<TaskSeed, "status" | "error" | "errorCode" | "detail">;

const firstLine = (text: unknown) => String(text ?? "").split("\n")[0].slice(0, 300);

function failedAs(errorClass: ErrorClass, detail = "", error = ""): Outcome {
  return { status: "failed", error: error || ERROR_CLASSES[errorClass], errorCode: errorClass, detail: firstLine(detail) };
}

/** A status and the sentence kept beside it → what the task row carries. */
export function outcomeOf(status: RequestTaskStatus, message = "", platformAction = false): Outcome {
  if (status === "failed") {
    const c = classifyMessage(message, platformAction);
    return { status, error: c.error, errorCode: c.errorClass, detail: firstLine(message) };
  }
  if (status === "cancelled") return { status, error: CANCELLED_BEFORE_START, errorCode: "", detail: firstLine(message) };
  if (status === "aborted") return { status, error: ABORTED_WHILE_RUNNING, errorCode: "", detail: firstLine(message) };
  if (status === "timedout") return { status, error: TIMED_OUT, errorCode: "", detail: message === TIMED_OUT ? "" : firstLine(message) };
  return { status, error: "", errorCode: "", detail: status === "success" ? firstLine(message) : "" };
}

const CLEARED_RE = /^Cleared by/;
/** Sources the system starts by itself: nobody "triggered" them. A retry is pressed by someone, so it keeps their name. */
const isSystemSource = (source: string) => source.startsWith("system-") && source !== "system-retry";

function headOf(kind: SyncTaskKind, r: { _id: unknown; userId?: unknown; createdAt?: Date | null }, source: string, by: string, platform = "FLIPKART"): RequestHead {
  return {
    userId: new Types.ObjectId(String(r.userId)),
    requestId: String(r._id),
    kind,
    platform: platform || "FLIPKART",
    source,
    triggeredBy: isSystemSource(source) ? "" : String(by || "").trim(),
    createdAt: r.createdAt ? new Date(r.createdAt) : new Date(),
  };
}

function normalCounts(c: Partial<RequestCounts> | null | undefined): RequestCounts {
  const out = { ...emptyCounts(0) };
  for (const k of Object.keys(out) as Array<keyof RequestCounts>) out[k] = Math.max(0, Math.floor(Number(c?.[k]) || 0));
  return out;
}

function addCounts(into: RequestCounts, c: RequestCounts) {
  for (const k of Object.keys(into) as Array<keyof RequestCounts>) into[k] += c[k];
}

// ── fetch (order_job_runs, type "fetch") ────────────────────────────────────

type LedgerRow = Partial<IOrderJobRun> & { _id: unknown };

const SKIPPED_RE = /^not logged in/i;

/** fetch: the ledger's trigger is the reference's source word; a row may name another (`meta.source`). */
export function sourceOfLedger(run: LedgerRow): string {
  return String((run.meta as { source?: unknown } | undefined)?.source || run.trigger || "user");
}

export function statusOfLedger(status: string | undefined): RequestStatus {
  if (status === "queued") return "queued";
  if (status === "running") return "in_progress";
  if (status === "stopped") return "cancelled";
  return "completed";
}

function fetchAccountsOf(run: LedgerRow): { accounts: number; done: number; failedNamed: number; skipped: number } {
  const meta = (run.meta || {}) as { report?: { accountsQueued?: number; accountsDone?: number }; emails_count?: number; emails?: unknown[] };
  const items = run.failedItems || [];
  const skipped = items.filter((i) => SKIPPED_RE.test(i.reason || "")).length;
  // A run that finished keeps its report; one that never did still holds the number of accounts as its total.
  const accounts = Math.max(0, Number(meta.report?.accountsQueued ?? meta.emails_count ?? (Array.isArray(meta.emails) && meta.emails.length ? meta.emails.length : run.total)) || 0);
  const done = Math.min(accounts, Math.max(0, Number(meta.report?.accountsDone) || 0));
  return { accounts, done, failedNamed: Math.min(items.length - skipped, accounts - done), skipped };
}

/** How the accounts that did not finish ended, by how the run ended. */
function fetchRestStatus(run: LedgerRow): RequestTaskStatus {
  if (run.status === "queued" || run.status === "running") return "queued";
  if (run.status === "stopped") return "cancelled";
  if (run.status === "failed") return /^Could not start/.test(run.message || "") ? "failed" : "timedout";
  return "failed";
}

/**
 * A fetch counts ACCOUNTS (the reference's fetch task), not the unit ids its
 * ledger row counts. A run of this version keeps them on the row
 * (meta.task_counts); an older one is read from its report and failed items.
 */
export function countsOfFetch(run: LedgerRow): RequestCounts {
  const kept = (run.meta as { task_counts?: Partial<RequestCounts> } | undefined)?.task_counts;
  if (kept && typeof kept === "object") return normalCounts(kept);
  const { accounts, done, failedNamed, skipped } = fetchAccountsOf(run);
  const c = emptyCounts(0);
  c.total = accounts + skipped;
  c.failed = skipped;
  if (run.status === "queued" || run.status === "running") {
    c.queued = accounts;
    return c;
  }
  c.success = done;
  c.failed += failedNamed;
  c[countKeyOf(fetchRestStatus(run))] += accounts - done - failedNamed;
  return c;
}

export function factsOfFetch(run: LedgerRow): RequestFacts {
  const meta = (run.meta || {}) as Record<string, unknown> & { params?: Record<string, unknown>; child_ids?: unknown[]; since?: string; host?: string; pid?: number; task_counts?: unknown };
  const params: Record<string, unknown> = { ...(meta.params || {}) };
  if (!params.fetch_orders_after_date && meta.since) {
    const since = new Date(String(meta.since));
    if (!Number.isNaN(since.getTime())) params.fetch_orders_after_date = istDay(since);
  }
  const source = sourceOfLedger(run);
  return {
    head: headOf("fetch", run, source, run.by || "", String(meta.platform || "FLIPKART")),
    status: statusOfLedger(run.status),
    counts: countsOfFetch(run),
    startedAt: run.startedAt ?? null,
    finishedAt: run.finishedAt ?? null,
    updatedAt: run.updatedAt ?? null,
    params,
    parentId: String(params.parent_id || params.retry_of || ""),
    retryOf: String(params.retry_of || ""),
    childIds: (Array.isArray(meta.child_ids) ? meta.child_ids : []).map(String),
    message: run.message || "",
    queue: "",
    systemId: "",
    host: String(meta.host || ""),
    pid: Number(meta.pid) || 0,
    tasksRecorded: meta.task_counts ? "all" : "named",
    stopRequested: false,
    retryable: null,
  };
}

/**
 * The rows of a fetch that ran before the log existed: only what its ledger
 * row still names — the accounts it could not fetch, and the e-mails it kept
 * (a row keeps at most its first 500, and loses them when it finishes).
 */
export function seedsOfLegacyFetch(run: LedgerRow): TaskSeed[] {
  const meta = (run.meta || {}) as { emails?: unknown[] };
  const seeds = new Map<string, TaskSeed>();
  const base = { orderId: "", unitId: "", startedAt: null, finishedAt: run.finishedAt ?? null };
  for (const item of run.failedItems || []) {
    const email = String(item.key || "").trim().toLowerCase();
    if (!email) continue;
    const out = SKIPPED_RE.test(item.reason || "") ? failedAs("not_logged_in", item.reason) : outcomeOf("failed", item.reason);
    seeds.set(email, { key: email, email, ...base, ...out });
  }
  const rest = fetchRestStatus(run);
  const { done } = fetchAccountsOf(run);
  let fetched = 0;
  for (const raw of Array.isArray(meta.emails) ? meta.emails : []) {
    const email = String(raw || "").trim().toLowerCase();
    if (!email || seeds.has(email)) continue;
    // Which accounts a stopped run had finished was not kept; they were taken in this order.
    const status: RequestTaskStatus = run.status === "completed" || fetched < done ? "success" : rest;
    fetched += status === "success" ? 1 : 0;
    seeds.set(email, { key: email, email, ...base, finishedAt: isOpen(status) ? null : base.finishedAt, ...outcomeOf(status, status === "failed" ? run.message || "" : "") });
  }
  return [...seeds.values()];
}

// ── update (order_update_requests) ──────────────────────────────────────────

type UpdateRow = Partial<IOrderUpdateRequest> & { _id: unknown };
type UpdateAccountLike = Pick<IUpdateAccount, "email" | "status" | "total" | "done" | "failed" | "message" | "startedAt" | "finishedAt"> & { units?: IUpdateAccount["units"] };

export function sourceOfUpdate(source: string | undefined): string {
  const s = String(source || "manual");
  if (s === "manual" || s === "cancel") return "user";
  if (s === "schedule" || s === "ofd") return "system-cron";
  return s;
}

export function statusOfUpdate(status: string | undefined): RequestStatus {
  if (status === "running") return "in_progress";
  if (status === "done") return "completed";
  if (status === "cancelled") return "cancelled";
  return "queued";
}

/**
 * Stop (POST /update/stop, Batch requests → Cancel) marks the request
 * cancelled at once; the worker on an account finishes the unit in hand and,
 * reading the request before the next one, leaves the rest. An account still
 * "running" this long after its request was stopped belongs to a worker that
 * is gone — the worker's own rule for a running account (orderUpdateQueue.ts
 * STALE_ACCOUNT_MS).
 */
export const UPDATE_STOP_GRACE_MS = 20 * 60 * 1000;

/** How an update ended, for the accounts it left unfinished: stopped or closed, when, and the moment it is read. */
export interface UpdateEnd {
  stopped: boolean;
  at: number;
  now: number;
}

/** null while the request is open (queued, running). */
export function updateEndOf(r: { status?: string; finishedAt?: Date | null }, now = Date.now()): UpdateEnd | null {
  if (r.status !== "cancelled" && r.status !== "done") return null;
  return { stopped: r.status === "cancelled", at: r.finishedAt ? new Date(r.finishedAt).getTime() : 0, now };
}

/** The request was stopped, and a worker may still be finishing the unit in hand. */
const stillFinishing = (end: UpdateEnd) => end.stopped && end.at > 0 && end.now - end.at < UPDATE_STOP_GRACE_MS;

/**
 * One account of an update, as tasks (a task = one unit). The account keeps
 * how many of its units were refreshed and how many failed; what is left is
 * told by the account's own status:
 *   queued     still waiting
 *   running    one unit is being read, the others wait
 *   done       nothing is left
 *   failed     the account could not be read at all (its session, our
 *              database) — or it timed out with the worker gone
 *   cancelled  the request was stopped: never started — except the one unit
 *              a force-clear found being read, which was cut off
 * Once the request has ended (`end`), an account it left queued never starts
 * (cancelled), and one left running has only the unit in hand: still being
 * read for UPDATE_STOP_GRACE_MS after the stop, cut off with its worker after
 * that (aborted); its other units never start (cancelled).
 */
export function countsOfUpdateAccount(a: UpdateAccountLike, end: UpdateEnd | null = null): RequestCounts {
  const total = Math.max(0, Number(a.total) || a.units?.length || 0);
  const failed = Math.min(total, Math.max(0, Number(a.failed) || 0));
  const success = Math.min(total - failed, Math.max(0, Number(a.done) || 0));
  const rest = total - success - failed;
  const c = emptyCounts(0);
  c.total = total;
  c.success = success;
  c.failed = failed;
  if (a.status === "queued") {
    if (end) c.cancelled = rest;
    else c.queued = rest;
  } else if (a.status === "running") {
    const inHand = Math.min(1, rest);
    if (!end) {
      c.in_progress = inHand;
      c.queued = rest - inHand;
    } else {
      if (stillFinishing(end)) c.in_progress = inHand;
      else c.aborted = inHand;
      c.cancelled = rest - inHand;
    }
  } else if (a.status === "done") c.success += rest;
  else if (a.status === "failed") {
    if (a.message === TIMED_OUT) c.timed_out = rest;
    else c.failed += rest;
  } else {
    c.aborted = CLEARED_RE.test(a.message || "") && a.startedAt ? Math.min(1, rest) : 0;
    c.cancelled = rest - c.aborted;
  }
  return c;
}

export function countsOfUpdate(r: UpdateRow, now = Date.now()): RequestCounts {
  const c = emptyCounts(0);
  const end = updateEndOf(r, now);
  for (const a of r.accounts || []) addCounts(c, countsOfUpdateAccount(a, end));
  return c;
}

/** Accounts of a stopped update whose worker is still finishing the unit in hand: while there are any, the request reads in progress. */
export function finishingAccountsOf(r: UpdateRow, now = Date.now()): number {
  const end = updateEndOf(r, now);
  return end && stillFinishing(end) ? (r.accounts || []).filter((a) => a.status === "running").length : 0;
}

/**
 * countsOfUpdateAccount, summed over a request's accounts in the database —
 * so a list never brings the account arrays over (an update of 335 accounts
 * is 70 KB of them; a page of 25 such requests took over a second to arrive).
 * Also `finishingAccounts` (finishingAccountsOf).
 */
export function updateTallyProject(now = Date.now()) {
  const total = { $let: { vars: { t: { $ifNull: ["$$a.total", 0] } }, in: { $cond: [{ $gt: ["$$t", 0] }, "$$t", { $size: { $ifNull: ["$$a.units", []] } }] } } };
  const nonNeg = (field: string) => ({ $max: [0, { $ifNull: [field, 0] }] });
  const cleared = { $and: [{ $regexMatch: { input: { $ifNull: ["$$a.message", ""] }, regex: "^Cleared by" } }, { $ne: [{ $ifNull: ["$$a.startedAt", null] }, null] }] };
  const one = { $min: [1, "$$rest"] };
  const when = (status: string, then: unknown) => ({ $cond: [{ $eq: ["$$a.status", status] }, then, 0] });
  // The request has ended (updateEndOf); stopped less than the grace ago, a worker may still be finishing the unit in hand (stillFinishing).
  const ended = { $in: ["$status", ["cancelled", "done"]] };
  const finishing = { $and: [{ $eq: ["$status", "cancelled"] }, { $gt: [{ $ifNull: ["$finishedAt", null] }, new Date(now - UPDATE_STOP_GRACE_MS)] }] };
  // What an open request still has to do on the account: all of a queued one, all but the unit in hand of a running one.
  const waiting = { $add: [when("queued", "$$rest"), when("running", { $subtract: ["$$rest", one] })] };
  const perAccount = {
    $let: {
      vars: { total },
      in: {
        $let: {
          vars: { failed: { $min: ["$$total", nonNeg("$$a.failed")] } },
          in: {
            $let: {
              vars: { success: { $min: [{ $subtract: ["$$total", "$$failed"] }, nonNeg("$$a.done")] } },
              in: {
                $let: {
                  vars: { rest: { $subtract: ["$$total", { $add: ["$$success", "$$failed"] }] }, timedOut: { $eq: [{ $ifNull: ["$$a.message", ""] }, TIMED_OUT] } },
                  in: {
                    total: "$$total",
                    queued: { $cond: [ended, 0, waiting] },
                    in_progress: when("running", { $cond: [{ $or: [{ $not: [ended] }, finishing] }, one, 0] }),
                    success: { $add: ["$$success", when("done", "$$rest")] },
                    failed: { $add: ["$$failed", when("failed", { $cond: ["$$timedOut", 0, "$$rest"] })] },
                    cancelled: { $add: [when("cancelled", { $subtract: ["$$rest", { $cond: [cleared, one, 0] }] }), { $cond: [ended, waiting, 0] }] },
                    aborted: { $add: [when("cancelled", { $cond: [cleared, one, 0] }), when("running", { $cond: [{ $and: [ended, { $not: [finishing] }] }, one, 0] })] },
                    timed_out: when("failed", { $cond: ["$$timedOut", "$$rest", 0] }),
                    finishing: when("running", { $cond: [finishing, 1, 0] }),
                  },
                },
              },
            },
          },
        },
      },
    },
  };
  const keys = ["total", "queued", "in_progress", "success", "failed", "cancelled", "aborted", "timed_out"];
  return [
    { $addFields: { perAccount: { $map: { input: { $ifNull: ["$accounts", []] }, as: "a", in: perAccount } } } },
    { $addFields: { tally: Object.fromEntries(keys.map((k) => [k, { $sum: `$perAccount.${k}` }])), finishingAccounts: { $sum: "$perAccount.finishing" } } },
    { $project: { accounts: 0, logs: 0, perAccount: 0 } },
  ];
}

export function factsOfUpdate(r: UpdateRow & { tally?: Partial<RequestCounts>; finishingAccounts?: number }): RequestFacts {
  const source = sourceOfUpdate(r.source);
  const params: Record<string, unknown> = { ...(r.params || {}) };
  if (!params.selection_method && r.selectionMethod) params.selection_method = r.selectionMethod;
  const doc = r as UpdateRow & { retryOf?: string; systemId?: string };
  // Stopped, with a worker still finishing the unit in hand: the request has not ended yet ("Cancelling"), and cannot be cancelled again.
  const finishing = r.tally ? Math.max(0, Number(r.finishingAccounts) || 0) : finishingAccountsOf(r);
  return {
    head: headOf("update", r, source, r.createdBy || "", String(params.platform || "FLIPKART")),
    status: finishing ? "in_progress" : statusOfUpdate(r.status),
    counts: r.tally ? normalCounts(r.tally) : countsOfUpdate(r),
    startedAt: r.startedAt ?? null,
    finishedAt: r.finishedAt ?? null,
    updatedAt: r.updatedAt ?? null,
    params,
    parentId: String(r.parentId || doc.retryOf || params.retry_of || ""),
    retryOf: String(doc.retryOf || params.retry_of || ""),
    childIds: [],
    message: "",
    queue: "order-update",
    systemId: String(doc.systemId || ""),
    host: "",
    pid: 0,
    tasksRecorded: "all",
    stopRequested: finishing > 0,
    retryable: null,
  };
}

const UNIT_LOG_RE = /^(\S+): ([^\s/]+)\/(\S+): (.*)$/;
/** Log lines that name a unit without saying it failed: the API read fell back to the page, or the OTP-only pass missed. */
const NOT_A_FAILURE_RE = /^API read failed .* — page$|^OTP scrape failed|^OTP \d|→/;

/** The units a request's own log lines say failed: `${email}|${unitId}` → the sentence. */
export function failedUnitsInLogs(logs: IUpdateLog[] | undefined): Map<string, string> {
  const out = new Map<string, string>();
  for (const l of logs || []) {
    if (l.level !== "warn" && l.level !== "error") continue;
    const m = String(l.message || "").match(UNIT_LOG_RE);
    if (!m || NOT_A_FAILURE_RE.test(m[4])) continue;
    out.set(`${m[1].toLowerCase()}|${m[3]}`, m[4]);
  }
  return out;
}

export const updateTaskKey = (email: string, unitId: string) => `${String(email).toLowerCase()}|${unitId}`;

const NOT_RECORDED = "One of this account's failed units — the request's log no longer says which";

/**
 * The rows of an update from its document. An account's units are read in the
 * order they are stored, so the first `done + failed` of them have finished;
 * which of those failed is what the request's log lines name. Where the log
 * no longer names enough of them (it keeps 300 lines), the remaining failures
 * are put on the last finished units and say so in `detail`. What a request
 * that has ended left unfinished ends as countsOfUpdateAccount says.
 */
export function seedsOfUpdate(r: UpdateRow, now = Date.now()): TaskSeed[] {
  const named = failedUnitsInLogs(r.logs);
  const seeds = new Map<string, TaskSeed>();
  const end = updateEndOf(r, now);
  for (const a of r.accounts || []) {
    const email = String(a.email || "").toLowerCase();
    const units = a.units || [];
    const c = countsOfUpdateAccount({ ...a, total: units.length }, end);
    const flatFailed = Math.min(units.length, Math.max(0, Number(a.failed) || 0));
    const flatDone = Math.min(units.length - flatFailed, Math.max(0, Number(a.done) || 0));
    const finished = flatDone + flatFailed;
    const failedAt = new Set<number>();
    for (let i = 0; i < finished && failedAt.size < flatFailed; i++) if (named.has(updateTaskKey(email, units[i].unitId))) failedAt.add(i);
    for (let i = finished - 1; i >= 0 && failedAt.size < flatFailed; i--) failedAt.add(i);
    const ended = a.finishedAt ?? r.finishedAt ?? null;
    let inFlight = c.in_progress + c.aborted;
    const restStatus: RequestTaskStatus = a.status === "queued" || a.status === "running" ? (end ? "cancelled" : "queued") : a.status === "done" ? "success" : a.status === "failed" ? (a.message === TIMED_OUT ? "timedout" : "failed") : "cancelled";
    units.forEach((u, i) => {
      const key = updateTaskKey(email, u.unitId);
      const base = { key, email, orderId: u.orderId, unitId: u.unitId };
      if (i < finished) {
        const text = named.get(key);
        const out = failedAt.has(i) ? (text ? outcomeOf("failed", text) : { ...failedAs("unknown"), detail: NOT_RECORDED }) : outcomeOf("success");
        seeds.set(key, { ...base, ...out, startedAt: null, finishedAt: ended ?? r.updatedAt ?? null });
        return;
      }
      if (inFlight > 0) {
        inFlight -= 1;
        // A running account's unit in hand — cut off when the request has ended and its worker did not finish it (countsOfUpdateAccount).
        const status: RequestTaskStatus = a.status === "running" && c.in_progress ? "in_progress" : "aborted";
        seeds.set(key, { ...base, ...outcomeOf(status, a.message || ""), startedAt: a.startedAt ?? null, finishedAt: status === "aborted" ? ended : null });
        return;
      }
      seeds.set(key, { ...base, ...outcomeOf(restStatus, restStatus === "success" ? "" : a.message || ""), startedAt: null, finishedAt: isOpen(restStatus) ? null : ended });
    });
  }
  return [...seeds.values()];
}

// ── invoice fetch (order_invoice_requests) ──────────────────────────────────

type InvoiceRow = Partial<IOrderInvoiceRequest> & { _id: unknown };

export function factsOfInvoice(r: InvoiceRow): RequestFacts {
  const source = String(r.source || "user");
  return {
    head: headOf("invoices", r, source, r.createdBy || "", r.platform),
    status: (r.status || "queued") as RequestStatus,
    counts: normalCounts(r.counts),
    startedAt: r.startedAt ?? null,
    finishedAt: r.finishedAt ?? null,
    updatedAt: r.updatedAt ?? null,
    params: { ...((r.params as Record<string, unknown> | undefined) || {}) },
    parentId: r.parentId || r.retryOf || "",
    retryOf: r.retryOf || "",
    childIds: [],
    message: r.message || "",
    queue: r.queue || "order-invoice-batch",
    systemId: r.systemId || "",
    host: r.host || "",
    pid: 0,
    tasksRecorded: "all",
    stopRequested: false,
    retryable: null,
  };
}

/** What a downloaded order received, as its task says it (All tasks shows it): "Downloaded: product, transport". */
export function downloadedDetail(parts: string[] | undefined): string {
  const list = (parts || []).map((p) => String(p || "").trim()).filter(Boolean);
  return list.length ? `Downloaded: ${list.join(", ")}` : "";
}

export function seedsOfInvoice(r: InvoiceRow): TaskSeed[] {
  const seeds = new Map<string, TaskSeed>();
  for (const t of r.tasks || []) {
    seeds.set(t.orderId, {
      key: t.orderId,
      email: t.email || "",
      orderId: t.orderId,
      unitId: "",
      status: t.status,
      error: t.error || "",
      errorCode: t.errorClass || "",
      // A download keeps which invoice parts it stored (the sync panel listed them before step 5; now the task says it).
      detail: t.detail || (t.status === "success" ? downloadedDetail(t.parts) : ""),
      startedAt: t.startedAt ?? null,
      finishedAt: t.finishedAt ?? null,
    });
  }
  return [...seeds.values()];
}

// ── company coupons (order_coupon_requests + order_coupon_tasks) ────────────

type CouponRow = Partial<IOrderCouponRequest> & { _id: unknown };
type CouponTaskRow = Pick<IOrderCouponTask, "email" | "status" | "errorClass" | "error" | "note" | "startedAt" | "finishedAt">;

export function factsOfCoupon(r: CouponRow): RequestFacts {
  const source = String(r.source || "user");
  return {
    head: headOf("coupons", r, source, r.createdBy || "", r.platform),
    status: (r.status || "queued") as RequestStatus,
    counts: normalCounts(r.counts),
    startedAt: r.startedAt ?? null,
    finishedAt: r.finishedAt ?? null,
    updatedAt: r.updatedAt ?? null,
    params: { ...((r.params as Record<string, unknown> | undefined) || {}) },
    parentId: r.parentId || r.retryOf || "",
    retryOf: r.retryOf || "",
    childIds: [],
    message: r.message || "",
    queue: r.queue || "order-coupon-apply",
    systemId: r.systemId || "",
    host: r.host || "",
    pid: 0,
    tasksRecorded: "all",
    stopRequested: false,
    retryable: null,
  };
}

export function seedOfCouponTask(t: CouponTaskRow): TaskSeed {
  const email = String(t.email || "").toLowerCase();
  return { key: email, email, orderId: "", unitId: "", status: t.status, error: t.error || "", errorCode: t.errorClass || "", detail: t.note || "", startedAt: t.startedAt ?? null, finishedAt: t.finishedAt ?? null };
}

// ── cancel / reschedule (their units arrays) ────────────────────────────────

type CancelRow = Partial<ICancelRequest> & { _id: unknown };
type RescheduleRow = Partial<IRescheduleRequest> & { _id: unknown };

/** A unit that was force-cleared or timed out is stored as "failed" with the sentence that says so. */
function closedFromOutside(message: string, startedAt: unknown): RequestTaskStatus | null {
  if (message === TIMED_OUT) return "timedout";
  if (CLEARED_RE.test(message)) return startedAt ? "aborted" : "cancelled";
  return null;
}

export function cancelUnitStatus(status: string, message = "", startedAt: unknown = null): RequestTaskStatus {
  if (status === "queued") return "queued";
  if (status === "running") return "in_progress";
  if (status === "cancelled" || status === "dry_run" || status === "read") return "success";
  if (status === "not_cancellable") return "failed";
  return closedFromOutside(message, startedAt) || "failed";
}

export function rescheduleUnitStatus(status: string, message = "", startedAt: unknown = null): RequestTaskStatus {
  if (status === "queued") return "queued";
  if (status === "running") return "in_progress";
  if (status === "rescheduled") return "success";
  if (status === "not_available" || status === "needs_review") return "failed";
  return closedFromOutside(message, startedAt) || "failed";
}

export const NEEDS_REVIEW_ERROR = "Flipkart offered dates, but confirming is switched off on this server (RESCHEDULE_CONFIRM) — nothing was changed";

/**
 * The same mapping as cancelUnitStatus / rescheduleUnitStatus, counted in the
 * database, so a list never loads the unit arrays (the newest 50 cancel
 * requests hold 11,000 unit entries between them). Gives `tally` — the
 * reference's counts — and `startedAtMin`, the first moment a unit was taken.
 */
export function unitTallyProject(kind: "cancel" | "reschedule") {
  const arrays = kind === "cancel" ? ["$units", "$orders"] : ["$units"];
  const success = kind === "cancel" ? ["cancelled", "dry_run", "read"] : ["rescheduled"];
  const refused = kind === "cancel" ? ["not_cancellable"] : ["not_available", "needs_review"];
  const message = { $ifNull: ["$$x.message", ""] };
  const cleared = { $regexMatch: { input: message, regex: "^Cleared by" } };
  const timedOut = { $eq: [message, TIMED_OUT] };
  const started = { $ne: [{ $ifNull: ["$$x.startedAt", null] }, null] };
  const failed = { $eq: ["$$x.status", "failed"] };
  const n = (cond: unknown) => ({ $add: arrays.map((input) => ({ $size: { $filter: { input: { $ifNull: [input, []] }, as: "x", cond } } })) });
  return {
    tally: {
      total: { $add: arrays.map((input) => ({ $size: { $ifNull: [input, []] } })) },
      queued: n({ $eq: ["$$x.status", "queued"] }),
      in_progress: n({ $eq: ["$$x.status", "running"] }),
      success: n({ $in: ["$$x.status", success] }),
      failed: n({ $or: [{ $in: ["$$x.status", refused] }, { $and: [failed, { $not: [cleared] }, { $not: [timedOut] }] }] }),
      cancelled: n({ $and: [failed, cleared, { $not: [started] }] }),
      aborted: n({ $and: [failed, cleared, started] }),
      timed_out: n({ $and: [failed, timedOut] }),
    },
    startedAtMin: { $min: { $concatArrays: arrays.map((input) => ({ $ifNull: [`${input}.startedAt`, []] })) } },
    // What a retry may take: everything stored as "failed" (a failure, a cleared unit, a timed-out one) — never Flipkart's own refusals.
    retryable: n(failed),
  };
}

export function countsOfCancel(r: CancelRow): RequestCounts {
  return countsOf([...(r.units || []).map((u) => cancelUnitStatus(u.status, u.message, u.startedAt)), ...(r.orders || []).map((o) => cancelUnitStatus(o.status, o.message, o.startedAt))]);
}

export function countsOfReschedule(r: RescheduleRow): RequestCounts {
  return countsOf((r.units || []).map((u) => rescheduleUnitStatus(u.status, u.message, u.startedAt)));
}

function minStarted(rows: Array<{ startedAt?: Date | null }>): Date | null {
  let min = 0;
  for (const r of rows) {
    const t = r.startedAt ? new Date(r.startedAt).getTime() : 0;
    if (t && (!min || t < min)) min = t;
  }
  return min ? new Date(min) : null;
}

/**
 * cancel / reschedule keep three statuses: queued, running, done. One that
 * Batch requests → Cancel stopped (`stoppedAt`) reads "cancelled" once it has
 * ended, as the other kinds do.
 */
function statusOfUnitsRequest(status: string | undefined, stoppedAt?: Date | null): RequestStatus {
  return status === "running" ? "in_progress" : status === "done" ? (stoppedAt ? "cancelled" : "completed") : "queued";
}

type Tallied = { tally?: Partial<RequestCounts>; startedAtMin?: Date | null; inputCount?: number; rejectedCount?: number; retryable?: number };

export function factsOfCancel(r: CancelRow & Tallied): RequestFacts {
  const counts = r.tally ? normalCounts(r.tally) : countsOfCancel(r);
  const doc = r as CancelRow & { systemId?: string; source?: string };
  // "Retry failed" of the Cancellations tab keeps the user's source; Batch requests → Retry records "system-retry".
  const source = String(doc.source || "user");
  return {
    head: headOf("cancel", r, source, r.createdBy || ""),
    status: statusOfUnitsRequest(r.status, r.stoppedAt),
    counts,
    startedAt: r.tally ? r.startedAtMin ?? null : minStarted([...(r.units || []), ...(r.orders || [])]),
    finishedAt: r.finishedAt ?? null,
    updatedAt: r.updatedAt ?? null,
    params: {
      units_count: counts.total,
      processing_method: r.type === "unit" ? "unit_ids" : "order_ids",
      ids_count: r.inputCount ?? (r.input || []).length,
      rejected_count: r.rejectedCount ?? (r.rejected || []).length,
      ...(r.urgent ? { urgent: true } : {}),
      ...(r.dryRun ? { dry_run: true } : {}),
      ...(r.retryOf ? { retry_of: r.retryOf } : {}),
    },
    parentId: r.retryOf || "",
    retryOf: r.retryOf || "",
    childIds: [],
    message: "",
    queue: "order-cancel",
    systemId: String(doc.systemId || ""),
    host: "",
    pid: 0,
    tasksRecorded: "all",
    stopRequested: Boolean(r.stoppedAt) && r.status !== "done",
    // Stored "failed" is everything a retry takes: a real failure, a unit cleared before it ran, one that timed out.
    retryable: r.retryable ?? [...(r.units || []), ...(r.orders || [])].filter((u) => u.status === "failed").length,
  };
}

export function factsOfReschedule(r: RescheduleRow & Tallied): RequestFacts {
  const counts = r.tally ? normalCounts(r.tally) : countsOfReschedule(r);
  const doc = r as RescheduleRow & { systemId?: string; source?: string };
  // The Reschedule tab's own retry keeps the user's source; Batch requests → Retry records "system-retry".
  const source = String(doc.source || "user");
  return {
    head: headOf("reschedule", r, source, r.createdBy || ""),
    status: statusOfUnitsRequest(r.status, r.stoppedAt),
    counts,
    startedAt: r.tally ? r.startedAtMin ?? null : minStarted(r.units || []),
    finishedAt: r.finishedAt ?? null,
    updatedAt: r.updatedAt ?? null,
    params: {
      units_count: counts.total,
      processing_method: r.type === "unit" ? "unit_ids" : "order_ids",
      ids_count: r.inputCount ?? (r.input || []).length,
      rejected_count: r.rejectedCount ?? (r.rejected || []).length,
      reschedule_date: r.date || "",
      confirm: Boolean(r.confirm),
      ...(r.retryOf ? { retry_of: r.retryOf } : {}),
    },
    parentId: r.retryOf || "",
    retryOf: r.retryOf || "",
    childIds: [],
    message: "",
    queue: "order-reschedule",
    systemId: String(doc.systemId || ""),
    host: "",
    pid: 0,
    tasksRecorded: "all",
    stopRequested: Boolean(r.stoppedAt) && r.status !== "done",
    retryable: r.retryable ?? (r.units || []).filter((u) => u.status === "failed").length,
  };
}

export function seedsOfCancel(r: CancelRow): TaskSeed[] {
  const seeds = new Map<string, TaskSeed>();
  for (const o of r.orders || []) {
    const status = cancelUnitStatus(o.status, o.message, o.startedAt);
    const key = `o:${o.orderId}`;
    seeds.set(key, { key, email: o.email || "", orderId: o.orderId, unitId: "", ...outcomeOf(status, o.message || "", true), startedAt: o.startedAt ?? null, finishedAt: o.finishedAt ?? null });
  }
  for (const u of r.units || []) {
    const status = cancelUnitStatus(u.status, u.message, u.startedAt);
    const key = `u:${u.unitId}`;
    const out = u.status === "not_cancellable" ? failedAs("not_allowed_now", u.message) : outcomeOf(status, u.message || "", true);
    seeds.set(key, { key, email: u.email || "", orderId: u.orderId, unitId: u.unitId, ...out, startedAt: u.startedAt ?? null, finishedAt: u.finishedAt ?? null });
  }
  return [...seeds.values()];
}

export function seedsOfReschedule(r: RescheduleRow): TaskSeed[] {
  const seeds = new Map<string, TaskSeed>();
  for (const u of r.units || []) {
    const status = rescheduleUnitStatus(u.status, u.message, u.startedAt);
    const key = `u:${u.unitId}`;
    const out =
      u.status === "not_available" ? failedAs("not_allowed_now", u.message) : u.status === "needs_review" ? failedAs("internal_error", u.message, NEEDS_REVIEW_ERROR) : outcomeOf(status, u.message || "", true);
    seeds.set(key, { key, email: u.email || "", orderId: u.orderId, unitId: u.unitId, ...out, startedAt: u.startedAt ?? null, finishedAt: u.finishedAt ?? null });
  }
  return [...seeds.values()];
}

// ── dates ───────────────────────────────────────────────────────────────────

const IST_MS = 330 * 60 * 1000;

/** The India-time calendar day of a moment, YYYY-MM-DD. */
export function istDay(at: Date): string {
  return new Date(at.getTime() + IST_MS).toISOString().slice(0, 10);
}

// ── reading a request ───────────────────────────────────────────────────────

const LIGHT = {
  fetch: "-meta.emails",
  invoices: "-tasks",
};

function ids(list: string[]): Types.ObjectId[] {
  return list.filter((id) => Types.ObjectId.isValid(id)).map((id) => new Types.ObjectId(id));
}

/**
 * The requests of one kind as the batch list needs them — never their task
 * arrays: an update with its accounts counted in the database, an invoice
 * request without its tasks, cancel and reschedule with their units counted
 * in the database.
 */
export async function loadFacts(kind: SyncTaskKind, requestIds: string[], deps: LogDeps = logDefaults): Promise<Map<string, RequestFacts>> {
  const out = new Map<string, RequestFacts>();
  const _id = { $in: ids(requestIds) };
  if (!_id.$in.length) return out;
  if (kind === "fetch") for (const r of await deps.Ledger.find({ _id, type: "fetch" }).select(LIGHT.fetch).lean()) out.set(String(r._id), factsOfFetch(r));
  else if (kind === "update") {
    const rows = await (deps.Update as unknown as Model<Record<string, unknown>>).aggregate<UpdateRow & { tally: Partial<RequestCounts> }>([{ $match: { _id } }, ...updateTallyProject()]);
    for (const r of rows) out.set(String(r._id), factsOfUpdate(r));
  }
  else if (kind === "invoices") for (const r of await deps.Invoice.find({ _id }).select(LIGHT.invoices).lean()) out.set(String(r._id), factsOfInvoice(r));
  else if (kind === "coupons") for (const r of await deps.Coupon.find({ _id }).lean()) out.set(String(r._id), factsOfCoupon(r));
  else {
    const model = (kind === "cancel" ? deps.Cancel : deps.Reschedule) as unknown as Model<Record<string, unknown>>;
    const rows = await model.aggregate<(CancelRow & RescheduleRow) & Tallied>([
      { $match: { _id } },
      {
        $project: {
          userId: 1, type: 1, dryRun: 1, urgent: 1, date: 1, confirm: 1, createdBy: 1, retryOf: 1, status: 1, source: 1, systemId: 1, stoppedAt: 1, stoppedBy: 1, createdAt: 1, updatedAt: 1, finishedAt: 1,
          inputCount: { $size: { $ifNull: ["$input", []] } },
          rejectedCount: { $size: { $ifNull: ["$rejected", []] } },
          ...unitTallyProject(kind),
        },
      },
    ]);
    for (const r of rows) out.set(String(r._id), kind === "cancel" ? factsOfCancel(r as CancelRow & Tallied) : factsOfReschedule(r as RescheduleRow & Tallied));
  }
  return out;
}

/** One request with the task rows it implies. Null when it does not exist. `seeds` null = the log is the only record (a fetch of this version). */
export async function loadSeeds(kind: SyncTaskKind, requestId: string, deps: LogDeps = logDefaults): Promise<{ facts: RequestFacts; seeds: TaskSeed[] | null } | null> {
  if (!Types.ObjectId.isValid(requestId)) return null;
  if (kind === "fetch") {
    const r = await deps.Ledger.findOne({ _id: requestId, type: "fetch" }).lean();
    if (!r) return null;
    const facts = factsOfFetch(r);
    return { facts, seeds: facts.tasksRecorded === "all" ? null : seedsOfLegacyFetch(r) };
  }
  // Only what the rows need comes over: a unit's order page link and product name stay where they are (an update of
  // 2,700 units is 1 MB whole, a quarter of that without its links). An invoice task's parts come: its row names them.
  if (kind === "update") {
    const r = await deps.Update.findById(requestId).select("-accounts.units.orderUrl -accounts.units.itemId -accounts.platformId -accounts.via").lean();
    return r ? { facts: factsOfUpdate(r), seeds: seedsOfUpdate(r) } : null;
  }
  if (kind === "invoices") {
    const r = await deps.Invoice.findById(requestId).select("-tasks.sizeKb -tasks.accountId").lean();
    return r ? { facts: factsOfInvoice(r), seeds: seedsOfInvoice(r) } : null;
  }
  if (kind === "coupons") {
    const r = await deps.Coupon.findById(requestId).lean();
    if (!r) return null;
    const tasks = await deps.CouponTask.find({ requestId: r._id }).select("email status errorClass error note startedAt finishedAt").lean();
    return { facts: factsOfCoupon(r), seeds: tasks.map(seedOfCouponTask) };
  }
  if (kind === "cancel") {
    const r = await deps.Cancel.findById(requestId).select("-units.productName -units.itemId").lean();
    return r ? { facts: factsOfCancel(r), seeds: seedsOfCancel(r) } : null;
  }
  const r = await deps.Reschedule.findById(requestId).select("-units.productName -units.itemId -units.offeredDates -units.token").lean();
  return r ? { facts: factsOfReschedule(r), seeds: seedsOfReschedule(r) } : null;
}

/** Are the rows of this request what its own counters say? Read without its task arrays. */
export async function rowsInStep(kind: SyncTaskKind, requestId: string, deps: LogDeps = logDefaults): Promise<boolean> {
  const f = (await loadFacts(kind, [requestId], deps)).get(requestId);
  if (!f) return true;
  const rows = await logCounts(requestId, deps);
  const open = f.status === "queued" || f.status === "in_progress";
  // A fetch writes its own rows; all a pass can add is closing the ones left open by a run that has ended.
  if (kind === "fetch") return f.tasksRecorded === "all" || rows.total > 0 ? open || rows.queued + rows.in_progress === 0 : f.counts.total === 0;
  return sameCounts(rows, f.counts);
}

// ── writing the log ─────────────────────────────────────────────────────────

const WRITE_BATCH = 1000;

function rowOf(head: RequestHead, seed: TaskSeed, now: Date) {
  return {
    userId: head.userId,
    requestId: head.requestId,
    kind: head.kind,
    key: seed.key,
    platform: head.platform,
    taskType: SYNC_TASK_TYPE_OF[head.kind],
    source: head.source,
    status: seed.status,
    email: seed.email,
    orderId: seed.orderId,
    unitId: seed.unitId,
    triggeredBy: head.triggeredBy,
    error: seed.error,
    errorCode: seed.errorCode,
    detail: seed.detail,
    startedAt: seed.startedAt,
    finishedAt: seed.finishedAt,
    createdAt: head.createdAt,
    updatedAt: now,
  };
}

/** Insert rows a batch at a time; a row that is already there (the pair is unique) is left as it is. */
async function insertRows(deps: LogDeps, rows: Array<ReturnType<typeof rowOf>>): Promise<number> {
  let inserted = 0;
  for (let i = 0; i < rows.length; i += WRITE_BATCH) {
    try {
      const res = await deps.Tasks.collection.insertMany(rows.slice(i, i + WRITE_BATCH), { ordered: false });
      inserted += res.insertedCount;
    } catch (err) {
      const e = err as { code?: number; result?: { insertedCount?: number }; insertedCount?: number; writeErrors?: Array<{ code?: number }> };
      const dupOnly = e.code === 11000 || (Array.isArray(e.writeErrors) && e.writeErrors.every((w) => w.code === 11000));
      if (!dupOnly) throw err;
      inserted += Number(e.result?.insertedCount ?? e.insertedCount ?? 0);
    }
  }
  return inserted;
}

/** The task rows of one request, by status, as the log holds them now. */
export async function logCounts(requestId: string, deps: LogDeps = logDefaults): Promise<RequestCounts> {
  const rows = await deps.Tasks.aggregate<{ _id: RequestTaskStatus; n: number }>([{ $match: { requestId } }, { $group: { _id: "$status", n: { $sum: 1 } } }]);
  const c = emptyCounts(0);
  for (const r of rows) {
    c.total += r.n;
    c[countKeyOf(r._id)] += r.n;
  }
  return c;
}

export function sameCounts(a: RequestCounts, b: RequestCounts): boolean {
  return (Object.keys(a) as Array<keyof RequestCounts>).every((k) => a[k] === b[k]);
}

/** How an open row of a fetch ends when the run ended without closing it: stopped, or its process gone. */
async function closeFetchRows(facts: RequestFacts, run: { status?: string; message?: string }, deps: LogDeps): Promise<number> {
  if (facts.status === "queued" || facts.status === "in_progress") return 0;
  const now = new Date();
  const stopped = run.status === "stopped";
  const neverStarted = stopped ? outcomeOf("cancelled") : outcomeOf(run.status === "failed" ? "timedout" : "failed", run.message || "");
  const running = stopped ? outcomeOf("aborted") : neverStarted;
  const a = await deps.Tasks.collection.updateMany({ requestId: facts.head.requestId, status: "queued" }, { $set: { ...neverStarted, finishedAt: now, updatedAt: now } });
  const b = await deps.Tasks.collection.updateMany({ requestId: facts.head.requestId, status: "in_progress" }, { $set: { ...running, finishedAt: now, updatedAt: now } });
  const changed = (a.modifiedCount || 0) + (b.modifiedCount || 0);
  // A run of this version counts by its rows, and one whose process died never wrote its last counts: they follow
  // the rows now. An older run keeps the counts its ledger row implies — its rows are only the accounts it named.
  if (changed && facts.tasksRecorded === "all") {
    const counts = await logCounts(facts.head.requestId, deps);
    await deps.Ledger.updateOne({ _id: facts.head.requestId }, { $set: { "meta.task_counts": counts } }).catch(() => undefined);
  }
  return changed;
}

export interface SyncResult {
  kind: SyncTaskKind;
  requestId: string;
  found: boolean;
  seeds: number;
  inserted: number;
  moved: number;
}

/**
 * Bring the task rows of one request in line with its document: rows that are
 * missing are inserted, rows still open that the document has moved on are
 * moved. A row that has ended is left alone — whoever ended it (the worker's
 * own mark, an earlier pass) knew more than the document does.
 */
export async function syncRequestTasks(kind: SyncTaskKind, requestId: string, deps: LogDeps = logDefaults): Promise<SyncResult> {
  const out: SyncResult = { kind, requestId, found: false, seeds: 0, inserted: 0, moved: 0 };
  const loaded = await loadSeeds(kind, requestId, deps);
  if (!loaded) return out;
  out.found = true;
  const { facts } = loaded;
  if (kind === "fetch") {
    const run = await deps.Ledger.findById(requestId).select("status message").lean();
    const has = await deps.Tasks.exists({ requestId });
    // A fetch of this version wrote its own rows; all that can be left to do is close the ones its process never did.
    if (!loaded.seeds || has) {
      out.moved = await closeFetchRows(facts, run || {}, deps);
      return out;
    }
  }
  const seeds = loaded.seeds || [];
  out.seeds = seeds.length;
  if (!seeds.length) return out;
  const existing = new Map<string, string>();
  for (const row of await deps.Tasks.find({ requestId }).select("key status -_id").lean()) existing.set(row.key, row.status);
  const now = new Date();
  const missing = seeds.filter((s) => !existing.has(s.key));
  if (missing.length) out.inserted = await insertRows(deps, missing.map((s) => rowOf(facts.head, s, now)));
  const moves = seeds.filter((s) => {
    const have = existing.get(s.key);
    return have !== undefined && isOpen(have) && have !== s.status;
  });
  for (let i = 0; i < moves.length; i += WRITE_BATCH) {
    const res = await deps.Tasks.collection.bulkWrite(
      moves.slice(i, i + WRITE_BATCH).map((s) => ({
        updateOne: {
          // Only while the row is still open: a mark that landed since this pass read the log wins.
          filter: { requestId, key: s.key, status: { $in: OPEN_TASK } },
          update: { $set: { status: s.status, error: s.error, errorCode: s.errorCode, detail: s.detail, startedAt: s.startedAt, finishedAt: s.finishedAt, updatedAt: now } },
        },
      })),
      { ordered: false }
    );
    out.moved += res.modifiedCount || 0;
  }
  return out;
}

/**
 * A request has just been created, and its document is in hand: its rows are
 * written from it — nothing is read back. Never fails the caller.
 */
export async function openRequestTasksQuietly(
  kind: Exclude<SyncTaskKind, "fetch">,
  doc: { _id: unknown },
  opts: { couponTasks?: CouponTaskRow[]; deps?: LogDeps } = {}
): Promise<void> {
  const deps = opts.deps || logDefaults;
  try {
    const facts = kind === "update" ? factsOfUpdate(doc) : kind === "invoices" ? factsOfInvoice(doc) : kind === "coupons" ? factsOfCoupon(doc) : kind === "cancel" ? factsOfCancel(doc) : factsOfReschedule(doc);
    const seeds = kind === "update" ? seedsOfUpdate(doc) : kind === "invoices" ? seedsOfInvoice(doc) : kind === "coupons" ? (opts.couponTasks || []).map(seedOfCouponTask) : kind === "cancel" ? seedsOfCancel(doc) : seedsOfReschedule(doc);
    const now = new Date();
    await insertRows(deps, seeds.map((s) => rowOf(facts.head, s, now)));
  } catch (err) {
    console.warn(`[task-log] ${kind} ${String(doc._id).slice(-8)}: rows not opened — ${err instanceof Error ? err.message.split("\n")[0] : err}`);
  }
}

/**
 * Bring a request's rows in line, for callers that must never fail because of
 * the log (a job, a route that has already done its work). The request's own
 * counters are compared with its rows first — a small read — and its document
 * is read whole only when they differ: a worker that marks its tasks as it
 * goes leaves nothing to do here.
 */
export async function syncRequestTasksQuietly(kind: SyncTaskKind, requestId: string, deps: LogDeps = logDefaults): Promise<void> {
  try {
    await taskLog.flush();
    if (await rowsInStep(kind, requestId, deps)) return;
    await syncRequestTasks(kind, requestId, deps);
  } catch (err) {
    console.warn(`[task-log] ${kind} ${String(requestId).slice(-8)}: ${err instanceof Error ? err.message.split("\n")[0] : err}`);
  }
}

const SOON_EVERY_MS = 5000;
const syncedSoonAt = new Map<string, number>();

/**
 * For a job that has just finished ONE account of a request (cancel,
 * reschedule, an invoice batch): bring the rows in line, but not once per
 * account — a request of four hundred accounts would read its whole document
 * four hundred times. At most once every few seconds per request, and always
 * when `final` (the request has just ended). What a skipped call leaves behind
 * the next one, or the keeper's pass, picks up.
 */
export async function syncRequestTasksSoon(kind: SyncTaskKind, requestId: string, final = false, deps: LogDeps = logDefaults): Promise<void> {
  const last = syncedSoonAt.get(requestId) || 0;
  if (!final && Date.now() - last < SOON_EVERY_MS) return;
  if (final) syncedSoonAt.delete(requestId);
  else syncedSoonAt.set(requestId, Date.now());
  await syncRequestTasksQuietly(kind, requestId, deps);
}

/**
 * Update: one account has ended. Its units that are still open end the way
 * the account did — failed with the account's reason (its session, our
 * database), cancelled when the request was stopped, timed out — in one write
 * over that account's rows only (their keys start with its e-mail).
 */
export async function closeUpdateAccountRows(requestId: string, email: string, account: { status: string; message?: string }, deps: LogDeps = logDefaults): Promise<void> {
  const status: RequestTaskStatus | null = account.status === "done" ? "success" : account.status === "failed" ? (account.message === TIMED_OUT ? "timedout" : "failed") : account.status === "cancelled" ? "cancelled" : null;
  if (!status) return;
  try {
    await taskLog.flush();
    const now = new Date();
    const prefix = `${String(email).toLowerCase()}|`;
    await deps.Tasks.collection.updateMany(
      { requestId: String(requestId), key: { $gte: prefix, $lt: `${prefix}\uffff` }, status: { $in: OPEN_TASK } },
      { $set: { ...outcomeOf(status, status === "success" ? "" : account.message || ""), finishedAt: now, updatedAt: now } }
    );
  } catch (err) {
    console.warn(`[task-log] update ${String(requestId).slice(-8)} / ${email}: ${err instanceof Error ? err.message.split("\n")[0] : err}`);
  }
}

// ── what is queued or running right now ─────────────────────────────────────

export interface OpenTask {
  requestId: string;
  email: string;
  orderId: string;
  unitId: string;
}

const OPEN_LOOKUP_BATCH = 1000;

/**
 * The tasks of one type that are queued or running in ANY request of the
 * workspace, among these orders (or, for the account tasks of a fetch or a
 * coupon apply, these e-mails). This is the reference's "Items already queued
 * or running are skipped": a retry and the chained requests leave such an
 * item to the request that already holds it. Read off the log's own indexes
 * (userId + orderId / email), a thousand values a query.
 */
export async function openTasksAmong(userId: string, taskType: SyncTaskType, by: "orderId" | "email", values: string[], deps: LogDeps = logDefaults): Promise<OpenTask[]> {
  const wanted = [...new Set(values.filter(Boolean))];
  const out: OpenTask[] = [];
  if (!wanted.length || !Types.ObjectId.isValid(userId)) return out;
  await taskLog.flush();
  const uid = new Types.ObjectId(userId);
  for (let at = 0; at < wanted.length; at += OPEN_LOOKUP_BATCH) {
    const rows = await deps.Tasks.find({ userId: uid, [by]: { $in: wanted.slice(at, at + OPEN_LOOKUP_BATCH) }, status: { $in: OPEN_TASK }, taskType })
      .select("requestId email orderId unitId -_id")
      .lean();
    for (const r of rows) out.push({ requestId: r.requestId, email: r.email || "", orderId: r.orderId || "", unitId: r.unitId || "" });
  }
  return out;
}

// ── marks: how one task went, said by the code that did it ──────────────────

export interface TaskMark {
  status: RequestTaskStatus;
  /** Failed: the raw sentence (it is classified here). */
  message?: string;
  /** Failed: the class, when the caller already knows it. */
  errorCode?: string;
  error?: string;
  detail?: string;
  /** cancel / reschedule: an unexplained refusal is the platform's. */
  platformAction?: boolean;
  startedAt?: Date | null;
  finishedAt?: Date | null;
}

const FLUSH_EVERY_MS = 1000;
const FLUSH_AT = 400;

/**
 * Marks wait here and are written together: a job that finishes forty tasks a
 * second makes one bulkWrite a second, not forty round trips. Two marks for
 * one task that arrive within the same second (taken, then finished) become
 * one write. A mark moves a row that is still open and no other: a task that
 * has ended keeps its ending. A mark for a row the log does not have changes
 * nothing — the next syncRequestTasks creates it from the document.
 */
class TaskLogBuffer {
  private pending = new Map<string, { requestId: string; key: string; set: Record<string, unknown>; tasks: LogDeps["Tasks"] }>();
  private timer: NodeJS.Timeout | null = null;
  private writing: Promise<void> = Promise.resolve();
  /** Inserts still on their way (a fetch inserts its accounts while it starts): marks for that request wait for them. */
  private opening = new Map<string, Promise<unknown>>();

  /** Rows of `requestId` are being inserted: its marks are written after `done` settles. */
  waitFor(requestId: string, done: Promise<unknown>) {
    const p = done.catch(() => undefined).finally(() => {
      if (this.opening.get(requestId) === p) this.opening.delete(requestId);
    });
    this.opening.set(requestId, p);
  }

  mark(requestId: string, key: string, mark: TaskMark, deps: LogDeps = logDefaults) {
    if (!requestId || !key) return;
    const now = new Date();
    const out: Outcome =
      mark.status === "failed" && mark.errorCode
        ? { status: "failed", error: mark.error || ERROR_CLASSES[mark.errorCode as ErrorClass] || classified("unknown").error, errorCode: mark.errorCode, detail: firstLine(mark.detail ?? mark.message) }
        : outcomeOf(mark.status, mark.message ?? mark.detail ?? "", mark.platformAction);
    const id = `${requestId}\u0000${key}`;
    const set: Record<string, unknown> = { ...(this.pending.get(id)?.set || {}), ...out, updatedAt: now };
    if (mark.startedAt !== undefined) set.startedAt = mark.startedAt;
    else if (mark.status === "in_progress") set.startedAt = now;
    if (mark.finishedAt !== undefined) set.finishedAt = mark.finishedAt;
    else if (!isOpen(mark.status)) set.finishedAt = now;
    else set.finishedAt = null;
    this.pending.set(id, { requestId, key, set, tasks: deps.Tasks });
    if (this.pending.size >= FLUSH_AT) void this.flush();
    else if (!this.timer) {
      this.timer = setTimeout(() => void this.flush(), FLUSH_EVERY_MS);
      this.timer.unref();
    }
  }

  /** Write everything waiting. Resolves when it is in the database; never rejects. */
  flush(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    const batch = [...this.pending.values()];
    this.pending.clear();
    if (!batch.length) return this.writing;
    const waits = [...new Set(batch.map((b) => b.requestId))].map((id) => this.opening.get(id)).filter(Boolean);
    this.writing = this.writing
      .then(() => Promise.all(waits))
      .then(async () => {
        // One collection in the app; the checks write a throwaway one beside it.
        for (const tasks of new Set(batch.map((b) => b.tasks))) {
          const mine = batch.filter((b) => b.tasks === tasks);
          for (let i = 0; i < mine.length; i += WRITE_BATCH) {
            await tasks.collection.bulkWrite(
              // Only a row still open: one that has ended — by an earlier mark, or timed out by the keeper — stays as it ended.
              mine.slice(i, i + WRITE_BATCH).map((b) => ({ updateOne: { filter: { requestId: b.requestId, key: b.key, status: { $in: OPEN_TASK } }, update: { $set: b.set } } })),
              { ordered: false }
            );
          }
        }
      })
      .catch((err) => console.warn(`[task-log] ${batch.length} mark(s) not written: ${err instanceof Error ? err.message.split("\n")[0] : err}`));
    return this.writing;
  }
}

export const taskLog = new TaskLogBuffer();

// ── fetch: its rows and its per-account counts ──────────────────────────────

const FETCH_COUNTS_EVERY_MS = 2000;

/**
 * The task rows of one fetch run. A fetch has no document that lists its
 * accounts, so the run itself opens the rows (every account queued, the ones
 * skipped because they are not logged in already failed), marks each account
 * as it is taken and as it ends, and keeps the per-account counts on its
 * ledger row (meta.task_counts) for the batch list.
 */
export interface FetchTaskRun {
  started(email: string): void;
  succeeded(email: string): void;
  failed(email: string, reason: string): void;
  /**
   * Close the run: an account that never started is cancelled and one being
   * fetched is cut off (the run was stopped). `counts` is final at once — the
   * ledger row is closed with it; `written` resolves when the rows are.
   */
  finish(cancelled: boolean): { counts: RequestCounts; written: Promise<void> };
}

export function openFetchTasks(
  input: { userId: string; runId: string; source: string; by: string; emails: string[]; skipped: string[]; createdAt?: Date },
  deps: LogDeps = logDefaults
): FetchTaskRun {
  const { runId } = input;
  const head = headOf("fetch", { _id: runId, userId: input.userId, createdAt: input.createdAt || new Date() }, input.source || "user", input.by);
  const now = new Date();
  const counts = emptyCounts(0);
  counts.total = input.emails.length + input.skipped.length;
  counts.queued = input.emails.length;
  counts.failed = input.skipped.length;
  const state = new Map<string, RequestTaskStatus>(input.emails.map((e) => [e.toLowerCase(), "queued"]));
  const seed = (email: string, out: Outcome, finishedAt: Date | null): TaskSeed => ({ key: email.toLowerCase(), email: email.toLowerCase(), orderId: "", unitId: "", ...out, startedAt: null, finishedAt });
  const rows = [
    ...input.emails.map((e) => rowOf(head, seed(e, outcomeOf("queued"), null), now)),
    ...input.skipped.filter((e) => !state.has(e.toLowerCase())).map((e) => rowOf(head, seed(e, failedAs("not_logged_in", "not logged in — skipped"), now), now)),
  ];
  const opened = insertRows(deps, rows).catch((err) => console.warn(`[task-log] fetch ${runId.slice(-8)}: rows not opened — ${err instanceof Error ? err.message.split("\n")[0] : err}`));
  taskLog.waitFor(runId, opened);

  let dirty = true;
  const writeCounts = () => {
    if (!dirty) return Promise.resolve();
    dirty = false;
    // While the row is open: a run the keeper has closed (its process was taken for dead) keeps the counts of its rows.
    return deps.Ledger.updateOne({ _id: runId, status: { $in: ["queued", "running"] } }, { $set: { "meta.task_counts": { ...counts } } }).then(
      () => undefined,
      () => undefined
    );
  };
  void writeCounts();
  const timer = setInterval(() => void writeCounts(), FETCH_COUNTS_EVERY_MS);
  timer.unref();

  const move = (email: string, to: RequestTaskStatus, mark: TaskMark) => {
    const key = String(email || "").toLowerCase();
    const from = state.get(key);
    if (!from || !isOpen(from)) return;
    state.set(key, to);
    counts[countKeyOf(from)] -= 1;
    counts[countKeyOf(to)] += 1;
    dirty = true;
    taskLog.mark(runId, key, mark, deps);
  };

  return {
    started: (email) => move(email, "in_progress", { status: "in_progress" }),
    succeeded: (email) => move(email, "success", { status: "success" }),
    failed: (email, reason) => move(email, "failed", { status: "failed", message: reason }),
    finish(cancelled) {
      clearInterval(timer);
      for (const [email, status] of state) {
        if (status === "in_progress") move(email, cancelled ? "aborted" : "failed", { status: cancelled ? "aborted" : "failed", message: cancelled ? "" : "The run ended before this account did" });
        else if (status === "queued") move(email, cancelled ? "cancelled" : "failed", { status: cancelled ? "cancelled" : "failed", message: cancelled ? "" : "The run ended before this account was taken" });
      }
      const written = (async () => {
        await opened;
        await taskLog.flush();
        dirty = true;
        await writeCounts();
      })();
      return { counts: { ...counts }, written };
    },
  };
}
