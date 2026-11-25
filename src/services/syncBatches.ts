import { Types, type Model } from "mongoose";
import { REQUEST_STATUSES, REQUEST_TASK_STATUSES, type RequestCounts, type RequestStatus, type RequestTaskStatus } from "../models/OrderInvoiceRequest.js";
import { SYNC_TASK_KEEP_MS, SYNC_TASK_KINDS, SYNC_TASK_TYPE_OF, type IOrderSyncTask, type SyncTaskKind } from "../models/OrderSyncTask.js";
import { ABORTED_WHILE_RUNNING, CANCELLED_BEFORE_START, ERROR_CLASSES, finishedCount, RequestInputError, TIMED_OUT, type ErrorClass } from "./requestCommon.js";
import { loadFacts, logCounts, logDefaults, sameCounts, syncRequestTasks, UPDATE_STOP_GRACE_MS, type LogDeps, type RequestFacts } from "./syncTaskLog.js";

/**
 * Batch requests + All tasks — the read model.
 *
 *   listBatches   GET /api/orders/sync/batches        every request of every kind, newest first
 *   getBatch      GET /api/orders/sync/batches/:id    one request, with its settings as labels
 *   batchIssues   GET /api/orders/sync/batches/:id/issues?status=failed
 *   listTasks     GET /api/orders/sync/tasks          the task log (order_sync_tasks)
 *
 * The answers are in the reference platform's shape (its /order/sync/batches/
 * and /order/sync/tasks/). A request lives in the collection of its kind:
 *
 *   fetch                 order_job_runs (type "fetch")
 *   update                order_update_requests
 *   invoice_fetch         order_invoice_requests
 *   company_coupon_apply  order_coupon_requests
 *   cancel                order_cancel_requests
 *   reschedule            order_reschedule_requests
 *
 * It is built here, in Trade Flow, because all six models are Trade Flow's;
 * the dashboard only forwards the query. A page is cut in the database: each
 * collection gives its newest (page × size) ids, the six lists are merged and
 * sliced by one aggregation, and only the requests on the page are then read
 * — without their task arrays (services/syncTaskLog.ts loadFacts).
 */

export const BATCH_PAGE_SIZE = 25;
export const TASK_PAGE_SIZE = 50;
const PAGE_SIZE_MAX = 200;

/** The reference's task-type list, in its order. `review` is its seventh; nothing here creates one, so it matches no request. */
export const TASK_TYPES = ["fetch", "update", "invoice_fetch", "reschedule", "cancel", "company_coupon_apply", "review"] as const;
export const TASK_SOURCES = ["user", "system-cron", "system-order", "system-email", "system-update", "system-retry", "system-repair"] as const;

const KIND_OF_TASK_TYPE: Record<string, SyncTaskKind> = Object.fromEntries(SYNC_TASK_KINDS.map((k) => [SYNC_TASK_TYPE_OF[k], k]));

/** The 16 error classes as the reference's `error_classes`: [{ code, label }]. */
export function errorClassList(): Array<{ code: string; label: string }> {
  return (Object.keys(ERROR_CLASSES) as ErrorClass[]).map((code) => ({ code, label: ERROR_CLASSES[code] }));
}

// ── query parsing ───────────────────────────────────────────────────────────

const str = (v: unknown) => (Array.isArray(v) ? String(v[0] ?? "") : String(v ?? "")).trim();

function pageOf(query: Record<string, unknown>, fallbackSize: number): { page: number; pageSize: number; skip: number } {
  const page = Math.max(1, Math.floor(Number(str(query.page)) || 1));
  const pageSize = Math.min(PAGE_SIZE_MAX, Math.max(1, Math.floor(Number(str(query.page_size)) || fallbackSize)));
  return { page, pageSize, skip: (page - 1) * pageSize };
}

/** A calendar day in India time → the moment it starts or ends. Created from / Created to are days the user picked in IST. */
export function istDayBound(day: string, end: boolean): Date {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) throw new RequestInputError(`"${day}" is not a date (YYYY-MM-DD)`);
  const at = new Date(`${day}T${end ? "23:59:59.999" : "00:00:00.000"}+05:30`);
  if (Number.isNaN(at.getTime())) throw new RequestInputError(`"${day}" is not a date (YYYY-MM-DD)`);
  return at;
}

function createdRange(query: Record<string, unknown>): { $gte?: Date; $lte?: Date } | null {
  const from = str(query.created_from);
  const to = str(query.created_to);
  if (!from && !to) return null;
  return { ...(from ? { $gte: istDayBound(from, false) } : {}), ...(to ? { $lte: istDayBound(to, true) } : {}) };
}

/** Trade Flow reads Flipkart only: a platform filter that leaves Flipkart out matches nothing; one that includes it changes nothing. */
function platformExcluded(query: Record<string, unknown>): boolean {
  const wanted = str(query.platform).split(",").map((p) => p.trim().toUpperCase()).filter(Boolean);
  return wanted.length > 0 && !wanted.includes("FLIPKART");
}

// ── the six collections ─────────────────────────────────────────────────────

type AnyModel = Model<Record<string, unknown>>;

function modelOf(kind: SyncTaskKind, deps: LogDeps): AnyModel {
  const m = kind === "fetch" ? deps.Ledger : kind === "update" ? deps.Update : kind === "invoices" ? deps.Invoice : kind === "coupons" ? deps.Coupon : kind === "cancel" ? deps.Cancel : deps.Reschedule;
  return m as unknown as AnyModel;
}

/** A batch status as each collection stores it — the filter that finds it. */
function storedStatus(kind: SyncTaskKind, status: RequestStatus): Record<string, unknown> {
  if (kind === "fetch") return { status: { queued: "queued", in_progress: "running", completed: { $in: ["completed", "failed"] }, cancelled: "stopped" }[status] };
  if (kind === "update") {
    // A stopped update whose worker is still finishing the unit in hand reads "in progress" until it has (syncTaskLog.ts factsOfUpdate).
    const finishing = { status: "cancelled", finishedAt: { $gt: new Date(Date.now() - UPDATE_STOP_GRACE_MS) }, "accounts.status": "running" };
    if (status === "in_progress") return { $or: [{ status: "running" }, finishing] };
    if (status === "cancelled") return { status: "cancelled", $nor: [finishing] };
    return { status: status === "queued" ? "queued" : "done" };
  }
  if (kind === "cancel" || kind === "reschedule") {
    // These two end "done" either way; one that Batch requests → Cancel stopped carries `stoppedAt` and reads cancelled.
    if (status === "completed") return { status: "done", stoppedAt: null };
    if (status === "cancelled") return { status: "done", stoppedAt: { $ne: null } };
    return { status: status === "queued" ? "queued" : "running" };
  }
  return { status };
}

/** A source as each collection stores it (services/syncTaskLog.ts sourceOf…). null = that kind never has it. */
function storedSource(kind: SyncTaskKind, source: string): Record<string, unknown> | null {
  if (kind === "fetch") return { $or: [{ "meta.source": source }, { "meta.source": null, trigger: source }] };
  if (kind === "update") return { source: source === "user" ? { $in: ["manual", "cancel", "user"] } : source === "system-cron" ? { $in: ["schedule", "ofd", "system-cron"] } : source };
  if (kind === "cancel" || kind === "reschedule") return { source: source === "user" ? { $in: [null, "", "user"] } : source };
  return { source: source === "user" ? { $in: [null, "", "user"] } : source };
}

/** The filter of one collection for a batches query, or null when the query leaves that kind out. */
function batchMatch(kind: SyncTaskKind, userId: Types.ObjectId, query: Record<string, unknown>): Record<string, unknown> | null {
  const taskType = str(query.task_type);
  if (taskType && KIND_OF_TASK_TYPE[taskType] !== kind) return null;
  const and: Array<Record<string, unknown>> = [{ userId }];
  if (kind === "fetch") and.push({ type: "fetch" });
  const status = str(query.status);
  if (status) {
    if (!REQUEST_STATUSES.includes(status as RequestStatus)) return null;
    and.push(storedStatus(kind, status as RequestStatus));
  }
  const source = str(query.source);
  if (source) {
    const stored = storedSource(kind, source);
    if (!stored) return null;
    and.push(stored);
  }
  const created = createdRange(query);
  if (created) and.push({ createdAt: created });
  return and.length === 1 ? and[0] : { $and: and };
}

// ── progress, ETA ───────────────────────────────────────────────────────────

/**
 * ETA from the measured rate. Every time an open request is read (the list,
 * one request, the keeper's pass) its finished count is noted here with the
 * time. The rate is what finished between the oldest note of the last
 * RATE_WINDOW_MS and now — the recent rate, which follows a batch that speeds
 * up as its lanes fill or slows as accounts start failing. Until there are
 * two notes at least RATE_MIN_SPAN_MS apart (a first read, a restarted API)
 * the lifetime average is used: time since the start ÷ tasks finished.
 */
const RATE_WINDOW_MS = 60_000;
const RATE_MIN_SPAN_MS = 8_000;
const RATE_KEEP_MS = 3 * 60_000;
const progressNotes = new Map<string, Array<{ at: number; finished: number }>>();

export function noteProgress(requestId: string, finished: number, at = Date.now()): void {
  const notes = progressNotes.get(requestId) || [];
  const last = notes[notes.length - 1];
  // One note a second is plenty; a re-run that set its counters back starts the notes again.
  if (last && finished < last.finished) notes.length = 0;
  else if (last && at - last.at < 1000) return;
  notes.push({ at, finished });
  while (notes.length > 2 && at - notes[0].at > RATE_KEEP_MS) notes.shift();
  progressNotes.set(requestId, notes);
}

export function forgetProgress(requestId: string): void {
  progressNotes.delete(requestId);
}

/** Tasks per second over the recent window, or null when the notes cannot say yet. */
export function recentRate(requestId: string, now = Date.now()): number | null {
  const notes = progressNotes.get(requestId);
  if (!notes || notes.length < 2) return null;
  const newest = notes[notes.length - 1];
  const oldest = notes.find((n) => now - n.at <= RATE_WINDOW_MS) || notes[notes.length - 2];
  const span = newest.at - oldest.at;
  const done = newest.finished - oldest.finished;
  if (span < RATE_MIN_SPAN_MS || done <= 0) return null;
  return done / (span / 1000);
}

const round1 = (n: number) => Math.round(n * 10) / 10;

/**
 * How the ETA is worked out. "reference" (the default) is the reference
 * platform's own arithmetic, which all three ETAs captured from it reproduce
 * to the second: tasks left × `avg_seconds_per_task` — the lifetime average
 * AFTER it has been rounded to one decimal, half to even (21,140 left × 0.5 s
 * = 2h 56m 10s; 20,698 × 0.1 = 34m 30s; 1,385 × 0.1 = 138 s). That rounding is
 * why its estimate jumps: an average of 0.13 s and one of 0.06 s both count as
 * 0.1. "recent" (ours, BATCH_ETA=recent) is the measured recent rate
 * described above, which follows a batch that speeds up or slows down.
 */
export type EtaMode = "recent" | "reference";
export const ETA_MODE: EtaMode = String(process.env.BATCH_ETA || "").trim().toLowerCase() === "recent" ? "recent" : "reference";

/** Half to even, as the reference's own rounding does (138.5 → 138). */
function roundHalfEven(n: number): number {
  const floor = Math.floor(n);
  const diff = n - floor;
  if (Math.abs(diff - 0.5) < 1e-9) return floor % 2 === 0 ? floor : floor + 1;
  return Math.round(n);
}

/**
 * The reference's derived numbers. `avg_seconds_per_task` and
 * `duration_seconds` are its own arithmetic (time since `started_at` ÷ tasks
 * finished, one decimal; whole seconds). `eta_seconds` is, by ETA_MODE, tasks
 * left × that rounded average (the reference's), or tasks left ÷ the rate —
 * `rate` when the recent one is known, else the lifetime average.
 */
export function progressOf(counts: RequestCounts, startedAt: Date | null, finishedAt: Date | null, opts: { now?: number; rate?: number | null; open?: boolean; mode?: EtaMode } = {}) {
  const now = opts.now ?? Date.now();
  const finished = finishedCount(counts);
  const start = startedAt ? new Date(startedAt).getTime() : 0;
  const end = finishedAt ? new Date(finishedAt).getTime() : now;
  const seconds = start ? Math.max(0, (end - start) / 1000) : null;
  const left = counts.queued + counts.in_progress;
  const open = opts.open ?? !finishedAt;
  const lifetime = seconds != null && finished > 0 && seconds > 0 ? finished / seconds : null;
  const avg = seconds != null && finished > 0 ? round1(seconds / finished) : null;
  const base = { finished_count: finished, progress_percent: counts.total ? round1((finished / counts.total) * 100) : 0, avg_seconds_per_task: avg, duration_seconds: seconds == null ? null : Math.round(seconds) };
  if (!open || !left) return { ...base, eta_seconds: 0, eta_basis: null };
  if ((opts.mode ?? ETA_MODE) === "reference") return { ...base, eta_seconds: avg == null ? null : roundHalfEven(left * avg), eta_basis: avg == null ? null : "reference_rounded_average" };
  const rate = opts.rate && opts.rate > 0 ? opts.rate : lifetime;
  return { ...base, eta_seconds: rate ? Math.round(left / rate) : null, eta_basis: opts.rate && opts.rate > 0 ? "recent_rate" : rate ? "lifetime_average" : null };
}

// ── settings as the reference labels them ───────────────────────────────────

/** `selection_method` → what "Selected by" shows. "All eligible" is the reference's; the others are the titles of its Update mode banner. */
const SELECTED_BY: Record<string, string> = {
  default: "All eligible",
  all_accounts: "All eligible",
  active: "All eligible",
  schedule_active: "All eligible",
  order_status: "Status filter",
  schedule_ofd: "Status filter",
  email_ids: "Email accounts",
  emails: "Email accounts",
  email: "Email accounts",
  order_status_and_email_ids: "Status + email filter",
  status_email: "Status + email filter",
  order_ids: "Explicit order IDs",
  unit_ids: "Explicit unit IDs",
  order_email_pairs: "CSV email + order ID pairs",
  csv: "CSV email + order ID pairs",
};

const yesNo = (v: unknown) => (v ? "Yes" : "No");
const num = (v: unknown) => Math.max(0, Math.floor(Number(v) || 0));
const list = (v: unknown) => (Array.isArray(v) ? v.map(String).filter(Boolean) : []);

export interface SettingLine {
  label: string;
  value: string;
}

/** Is this request the update a fetch queued when it ended? */
function afterFetch(f: RequestFacts): boolean {
  return f.head.kind === "update" && (f.params.selection_method === "after_fetch" || f.params.after_fetch === true || f.params.follows === "fetch");
}

/** "nightly fetch" when the request it follows was the schedule's, as the reference words it. */
function parentWords(parent: RequestFacts | undefined): string {
  if (!parent) return "";
  const type = SYNC_TASK_TYPE_OF[parent.head.kind].replace(/_/g, " ");
  return parent.head.source === "system-cron" ? `nightly ${type}` : type;
}

/** "a nightly fetch", "an update", "an invoice fetch". */
const withArticle = (words: string) => `${/^[aeiou]/i.test(words) ? "an" : "a"} ${words}`;

/** Is this request the invoice fetch an update queued when it saw units arrive at Delivered? */
function afterUpdate(f: RequestFacts): boolean {
  return f.head.kind === "invoices" && f.params.follows === "update";
}

/** Is this request one run of the invoice sweep (capture + re-capture)? */
function sweepRun(f: RequestFacts): boolean {
  return f.head.kind === "invoices" && f.params.sweep === true;
}

/**
 * SETTINGS of the Details modal: each recorded parameter under the
 * reference's label ("Selected by", "Statuses", "Force (skip in-progress
 * check)", "Queued after nightly fetch", "After", "Emails", "Orders",
 * "Units"). Empty when nothing was recorded — the modal then says "No
 * settings were recorded for this request."
 */
export function settingsOf(f: RequestFacts, parent?: RequestFacts): SettingLine[] {
  const p = f.params;
  const out: SettingLine[] = [];
  const add = (label: string, value: unknown) => out.push({ label, value: String(value) });
  const method = String(p.selection_method || p.processing_method || "");
  const statuses = list(p.statuses_to_update);
  switch (f.head.kind) {
    case "fetch":
      if (!Object.keys(p).length) break;
      if (method) add("Selected by", SELECTED_BY[method] || method);
      if (num(p.email_ids_count)) add("Emails", num(p.email_ids_count));
      if (p.fetch_orders_after_date) add("After", p.fetch_orders_after_date);
      if (p.force_fetch !== undefined) add("Force (skip in-progress check)", yesNo(p.force_fetch));
      if (p.enable_login !== undefined) add("Enable login", yesNo(p.enable_login));
      if (p.run_update_after_fetch !== undefined) add("Update after fetch", yesNo(p.run_update_after_fetch));
      if (p.follows === "enable_login") add("Queued after Enable login", "Yes");
      break;
    case "update":
      if (afterFetch(f)) add(`Queued after ${parentWords(parent) || "fetch"}`, "Yes");
      else if (method) add("Selected by", SELECTED_BY[method] || method);
      if (statuses.length) add("Statuses", statuses.join(", "));
      if (num(p.email_ids_count)) add("Emails", num(p.email_ids_count));
      if (num(p.order_ids_count)) add("Orders", num(p.order_ids_count));
      if (num(p.order_email_pairs_count)) add("Pairs", num(p.order_email_pairs_count));
      if (num(p.units_count)) add("Units", num(p.units_count));
      if (p.follows === "order") add("Queued after an order was placed", "Yes");
      if (p.force !== undefined && !afterFetch(f)) add("Force (skip in-progress check)", yesNo(p.force));
      break;
    case "invoices":
      if (afterUpdate(f)) add(`Queued after ${parentWords(parent) || "update"} saw Delivered`, "Yes");
      if (p.order_ids_count !== undefined) add("Orders", num(p.order_ids_count));
      if (sweepRun(f)) {
        add("Capture (delivered, no invoice yet)", num(p.capture_count));
        add("Re-capture and settle", num(p.recapture_count));
        if (num(p.cap_per_phase)) add("Cap per phase", num(p.cap_per_phase));
      } else if (p.force !== undefined) add("Force (skip in-progress check)", yesNo(p.force));
      break;
    case "coupons":
      if (method) add("Selected by", SELECTED_BY[method] || method);
      if (p.coupon_links_count !== undefined) add("Coupon links", num(p.coupon_links_count));
      if (num(p.email_ids_count)) add("Emails", num(p.email_ids_count));
      break;
    case "cancel":
    case "reschedule":
      if (method) add("Selected by", SELECTED_BY[method] || method);
      add("Units", num(p.units_count));
      if (p.reschedule_date) add("Reschedule date", p.reschedule_date);
      if (p.urgent) add("Urgent (read the order page in Chrome)", "Yes");
      if (p.dry_run) add("Dry run", "Yes");
      if (num(p.rejected_count)) add("Refused before queueing", num(p.rejected_count));
      break;
  }
  if (f.retryOf) {
    add("Retry of", f.retryOf);
    const retried = list(p.retry_statuses);
    if (retried.length) add("Retried", retried.map((s) => TASK_STATUS_LABEL[s as RequestTaskStatus] || s).join(", "));
    if (p.error_code) add("Error class", ERROR_CLASSES[String(p.error_code) as ErrorClass] || String(p.error_code));
  }
  return out;
}

/**
 * The grey line of the Request cell, as the reference writes it:
 * "Statuses: … · After nightly fetch", "After: 2026-11-22 · Force",
 * "Emails: 2 · After: …", "Orders: 763", "Units: 69".
 */
export function summaryOf(f: RequestFacts, parent?: RequestFacts): string {
  const p = f.params;
  const parts: string[] = [];
  const statuses = list(p.statuses_to_update);
  switch (f.head.kind) {
    case "fetch":
      if (num(p.email_ids_count)) parts.push(`Emails: ${num(p.email_ids_count)}`);
      if (p.fetch_orders_after_date) parts.push(`After: ${p.fetch_orders_after_date}`);
      if (p.force_fetch) parts.push("Force");
      break;
    case "update":
      if (statuses.length) parts.push(`Statuses: ${statuses.join(", ")}`);
      if (num(p.email_ids_count)) parts.push(`Emails: ${num(p.email_ids_count)}`);
      if (num(p.order_ids_count)) parts.push(`Orders: ${num(p.order_ids_count)}`);
      if (num(p.order_email_pairs_count)) parts.push(`Pairs: ${num(p.order_email_pairs_count)}`);
      if (num(p.units_count)) parts.push(`Units: ${num(p.units_count)}`);
      if (afterFetch(f)) parts.push(`After ${parentWords(parent) || "fetch"}`);
      if (p.follows === "order") parts.push("After order");
      if (p.force) parts.push("Force");
      break;
    case "invoices":
      parts.push(`Orders: ${num(p.order_ids_count ?? f.counts.total)}`);
      if (afterUpdate(f)) parts.push(`After ${parentWords(parent) || "update"}`);
      if (sweepRun(f)) parts.push(`Capture: ${num(p.capture_count)}`, `Re-capture: ${num(p.recapture_count)}`);
      else if (p.force) parts.push("Force");
      break;
    case "coupons":
      if (p.coupon_links_count !== undefined) parts.push(`Links: ${num(p.coupon_links_count)}`);
      if (num(p.email_ids_count)) parts.push(`Emails: ${num(p.email_ids_count)}`);
      break;
    case "cancel":
      parts.push(`Units: ${num(p.units_count)}`);
      break;
    case "reschedule":
      parts.push(`Units: ${num(p.units_count)}`);
      if (p.reschedule_date) parts.push(`Date: ${p.reschedule_date}`);
      break;
  }
  return parts.join(" · ");
}

// ── liveness ────────────────────────────────────────────────────────────────

/**
 * What the read model asks the running system. The API passes the real ones
 * (services/requestLogLive.ts liveProbes): a queue's connected workers (and
 * the workers' heartbeat), whether a request's jobs still sit on its queue,
 * and this process's own fetch. The checks pass their own.
 */
export interface LiveProbes {
  /** Is a worker connected to this queue? Unknown counts as yes — nothing is called dead on a guess. */
  queueHasWorker: (queue: string) => Promise<boolean>;
  /** Is this process running (or preparing) a fetch for the workspace? */
  fetchLive: (userId: string) => boolean;
  /**
   * Does a job of this request still sit on its queue — waiting, delayed, or
   * held by a worker that died with it (BullMQ hands such a job to the next
   * worker)? A worker that comes back runs it, so such a request is never
   * closed. Unknown counts as yes; left out (the checks) = nothing waits.
   */
  jobsWaiting?: (f: RequestFacts) => Promise<boolean>;
  host: string;
  pid: number;
  systemId: string;
}

export const noProbes: LiveProbes = { queueHasWorker: async () => true, fetchLive: () => false, host: "", pid: 0, systemId: "" };

/**
 * A request open with no worker on its queue this long — the queue empty of
 * workers reading after reading, and nothing written on the request in that
 * time — is closed (its unfinished tasks time out).
 */
export const REQUEST_STALE_MS = Math.max(60_000, Number(process.env.REQUEST_STALE_MS) || 30 * 60 * 1000);
/** The same, for a request that does not say which machine or deployment it belongs to (written before 2026-11-24). */
export const REQUEST_STALE_UNOWNED_MS = Math.max(REQUEST_STALE_MS, Number(process.env.REQUEST_STALE_UNOWNED_MS) || 24 * 60 * 60 * 1000);

export interface Liveness {
  /** null = the request has ended, or belongs to another machine. */
  worker_alive: boolean | null;
  stalled: boolean;
  stalled_message: string;
  /** Set when the keeper may close it: why. Only ever for a request of this machine / deployment, or one that names none and is a day silent. */
  stale: string;
}

const NOT_STALLED: Liveness = { worker_alive: null, stalled: false, stalled_message: "", stale: "" };

/** Two readings of a queue further apart than this do not make one stretch without a worker: what happened between them was not seen. */
export const QUEUE_WATCH_GAP_MS = 2 * 60 * 1000;

/**
 * Per queue: since when this process has found no worker on it, reading after
 * reading. A worker restart is seconds without one (a worker that knows the
 * heartbeat covers even those) and must never be taken for a worker that is
 * gone; neither may an API that starts before its worker. So the stretch
 * starts at the first reading that finds none, and only readings that follow
 * each other closely keep it going.
 */
export class QueueWatch {
  private stretches = new Map<string, { since: number; at: number }>();
  constructor(private readonly gapMs = QUEUE_WATCH_GAP_MS) {}
  /** One reading of the queue. Answers since when it has had no worker (null = it has one now). */
  note(queue: string, alive: boolean, now = Date.now()): number | null {
    if (alive) {
      this.stretches.delete(queue);
      return null;
    }
    const last = this.stretches.get(queue);
    const since = last && now - last.at <= this.gapMs ? last.since : now;
    this.stretches.set(queue, { since, at: now });
    return since;
  }
}

/** This process's watch: the keeper reads the queue of every open request every few seconds; a list being looked at reads them too. */
export const queueWatch = new QueueWatch();

/**
 * Is anything behind an open request? Pure — the caller has already asked the
 * queue. A fetch runs inside the API: it is alive when this process is running
 * it, dead when its row names this machine and another process. A worker job
 * is alive while a worker is connected to its queue; with none, it is stalled,
 * and may be closed (`stale`) once its queue has had no worker for
 * REQUEST_STALE_MS (`noWorkerSince`, from the caller's QueueWatch) and nothing
 * was written on it in that time. Without `noWorkerSince` nothing is stale.
 */
export function livenessOfRequest(f: RequestFacts, ctx: { now: number; probes: LiveProbes; queueAlive: boolean; noWorkerSince?: number | null }): Liveness {
  if (f.status !== "queued" && f.status !== "in_progress") return NOT_STALLED;
  const { probes, now } = ctx;
  const lastWrite = f.updatedAt ? new Date(f.updatedAt).getTime() : f.head.createdAt.getTime();
  const quiet = now - lastWrite;
  const left = f.counts.queued + f.counts.in_progress;
  if (f.head.kind === "fetch") {
    if (f.host && f.host !== probes.host) return NOT_STALLED;
    const mine = f.host === probes.host && f.pid === probes.pid && probes.fetchLive(String(f.head.userId));
    if (mine) return { worker_alive: true, stalled: false, stalled_message: "", stale: "" };
    const gone = f.host ? f.pid !== probes.pid || quiet >= REQUEST_STALE_MS : quiet >= REQUEST_STALE_UNOWNED_MS;
    // A row this very process wrote a moment ago (the run is just starting, or just ending) is not called dead.
    if (!gone && f.host && quiet < 60_000) return { worker_alive: true, stalled: false, stalled_message: "", stale: "" };
    return {
      worker_alive: false,
      stalled: true,
      stalled_message: `The API process running this fetch is gone with ${left} account(s) left`,
      stale: gone ? (f.host ? "the API process that ran it is gone" : "no process has written it for a day and it names no machine") : "",
    };
  }
  if (ctx.queueAlive) return { worker_alive: true, stalled: false, stalled_message: "", stale: "" };
  const unit = f.head.kind === "coupons" ? "account" : f.head.kind === "invoices" ? "order" : "unit";
  const owned = f.systemId && probes.systemId && f.systemId === probes.systemId;
  const foreign = f.systemId && probes.systemId && f.systemId !== probes.systemId;
  // Another deployment's request: its worker heartbeats on another Redis, so this API cannot tell.
  if (foreign) return NOT_STALLED;
  // Waiting in a queue is not silence, and a restart is not a worker gone: the silence that counts starts at the later
  // of the request's last write and the moment its queue was first found without a worker.
  const silent = now - Math.max(lastWrite, ctx.noWorkerSince ?? now);
  const stale = owned ? silent >= REQUEST_STALE_MS : silent >= REQUEST_STALE_UNOWNED_MS;
  return {
    worker_alive: false,
    stalled: true,
    stalled_message: f.status === "queued" ? "No worker is listening on this queue — restart the worker" : `The worker stopped with ${left} ${unit}(s) left — restart the worker to resume`,
    stale: stale ? (owned ? "no worker on its queue for 30 minutes and nothing written in that time" : "no worker on its queue for a day and nothing written in that time") : "",
  };
}

async function livenessFor(rows: RequestFacts[], probes: LiveProbes): Promise<Map<string, Liveness>> {
  const alive = new Map<string, boolean>();
  const now = Date.now();
  const out = new Map<string, Liveness>();
  for (const f of rows) {
    const open = f.status === "queued" || f.status === "in_progress";
    if (open && f.queue && !alive.has(f.queue)) {
      alive.set(f.queue, await probes.queueHasWorker(f.queue).catch(() => true));
      queueWatch.note(f.queue, alive.get(f.queue) ?? true, now);
    }
    out.set(f.head.requestId, livenessOfRequest(f, { now, probes, queueAlive: alive.get(f.queue) ?? true }));
  }
  return out;
}

// ── shape ───────────────────────────────────────────────────────────────────

/** The 8 characters of a request id that tell it from its neighbours: the LAST 8 (an ObjectId's first 8 are its creation second). */
export const shortId = (id: string) => String(id).slice(-8);

/** The reference's `triggered_by`: who pressed the button — null for the system's own requests. */
function triggeredBy(name: string): { id: null; name: string; email: string } | null {
  return name ? { id: null, name, email: name.includes("@") ? name : "" } : null;
}

/**
 * Why Cancel is not offered on an open request, or "". A fetch runs inside
 * one machine's API and a cancel / reschedule is taken off one deployment's
 * queue: from anywhere else there is nothing to stop.
 */
export function cancelRefusal(f: RequestFacts, probes: Pick<LiveProbes, "host" | "systemId">): string {
  if (f.stopRequested) return "Cancel was already pressed — the account being worked is finishing";
  if (f.head.kind === "fetch" && f.host && probes.host && f.host !== probes.host) return `This fetch is running on ${f.host} — cancel it from that machine`;
  if ((f.head.kind === "cancel" || f.head.kind === "reschedule") && f.systemId && probes.systemId && f.systemId !== probes.systemId) return `This request's jobs are on another machine's queue (${f.systemId}) — cancel it from that machine`;
  return "";
}

/** The unsuccessful tasks a retry would take (services/syncActions.ts retryBatch): none once the log has dropped them. */
export function retryableCount(f: RequestFacts, now = Date.now()): number {
  if (f.head.createdAt.getTime() < now - SYNC_TASK_KEEP_MS) return 0;
  return f.retryable ?? f.counts.failed + f.counts.cancelled + f.counts.aborted + f.counts.timed_out;
}

/** The failed tasks a retry of { statuses: ["failed"] } would take. */
export function retryableFailedCount(f: RequestFacts, now = Date.now()): number {
  if (f.head.createdAt.getTime() < now - SYNC_TASK_KEEP_MS) return 0;
  return f.retryableFailed ?? f.counts.failed;
}

/**
 * A fetch from before the log has rows only for the accounts its record still
 * names (tasks_recorded "named"), and a retry takes rows: what it offers is
 * counted from them — never from the run's account totals, which stay what the
 * row shows. One read for every such fetch of a page.
 */
async function countNamedRetryable(rows: RequestFacts[], deps: LogDeps, now = Date.now()): Promise<void> {
  const named = rows.filter((f) => f.tasksRecorded === "named" && f.status !== "queued" && f.status !== "in_progress" && f.head.createdAt.getTime() >= now - SYNC_TASK_KEEP_MS);
  if (!named.length) return;
  const found = await deps.Tasks.aggregate<{ _id: { r: string; s: RequestTaskStatus }; n: number }>([
    { $match: { requestId: { $in: named.map((f) => f.head.requestId) }, status: { $in: INCOMPLETE } } },
    { $group: { _id: { r: "$requestId", s: "$status" }, n: { $sum: 1 } } },
  ]).option({ maxTimeMS: 20_000 });
  for (const f of named) {
    const mine = found.filter((x) => x._id.r === f.head.requestId);
    f.retryable = mine.reduce((n, x) => n + x.n, 0);
    f.retryableFailed = mine.filter((x) => x._id.s === "failed").reduce((n, x) => n + x.n, 0);
  }
}

function batchRow(f: RequestFacts, extra: { parent?: RequestFacts; children: string[]; live: Liveness; probes: LiveProbes }) {
  const open = f.status === "queued" || f.status === "in_progress";
  const id = f.head.requestId;
  const cancelNote = open ? cancelRefusal(f, extra.probes) : "";
  if (open) noteProgress(id, finishedCount(f.counts));
  else forgetProgress(id);
  const params: Record<string, unknown> = { ...f.params, ...(afterFetch(f) ? { after_fetch: true } : {}) };
  return {
    request_id: id,
    short_id: shortId(id),
    platform: f.head.platform,
    task_type: SYNC_TASK_TYPE_OF[f.head.kind],
    source: f.head.source,
    status: f.status,
    triggered_by: triggeredBy(f.head.triggeredBy),
    parent_id: f.parentId || null,
    child_ids: [...new Set([...f.childIds, ...extra.children])],
    params,
    created_at: f.head.createdAt,
    started_at: f.startedAt,
    finished_at: open ? null : f.finishedAt,
    counts: f.counts,
    ...progressOf(f.counts, f.startedAt, open ? null : f.finishedAt, { rate: open ? recentRate(id) : null, open }),
    can_cancel: open && !cancelNote,
    can_retry: !open && retryableCount(f) > 0,
    // Ours, beside the reference's keys.
    kind: f.head.kind,
    /** What "Retry all incomplete" would take, and what the row's Retry of the failed tasks would (a fetch from before the log: only the accounts it names). */
    retryable_count: retryableCount(f),
    retryable_failed_count: retryableFailedCount(f),
    /** Why an open request cannot be cancelled from here ("" when it can, and when it has ended). */
    cancel_note: cancelNote,
    summary: summaryOf(f, extra.parent),
    settings: settingsOf(f, extra.parent),
    follows: f.parentId ? { request_id: f.parentId, short_id: shortId(f.parentId), what: f.retryOf === f.parentId ? "retry" : parentWords(extra.parent) || "request", label: f.retryOf === f.parentId ? `Retry of ${shortId(f.parentId)}` : `Follows ${withArticle(parentWords(extra.parent) || "request")}` } : null,
    message: f.message,
    worker_alive: extra.live.worker_alive,
    stalled: extra.live.stalled,
    stalled_message: extra.live.stalled_message,
    tasks_recorded: f.tasksRecorded,
    tasks_note: f.tasksRecorded === "named" && f.counts.total > 0 ? "This fetch ran before tasks were logged — only the accounts its record still names are listed under View tasks" : "",
  };
}

export type BatchRow = ReturnType<typeof batchRow>;

/** Which request (of any kind) follows or retries each of these ids — the reference's `child_ids`. */
async function childrenOf(userId: Types.ObjectId, rows: RequestFacts[], deps: LogDeps): Promise<Map<string, string[]>> {
  const out = new Map<string, string[]>();
  const requestIds = rows.map((f) => f.head.requestId);
  if (!requestIds.length) return out;
  // A request that follows or retries another is made after it — and a retry within the days its tasks are kept. So only
  // the requests created in that window are looked at (each collection's createdAt index), never a whole collection.
  const times = rows.map((f) => f.head.createdAt.getTime());
  const createdAt = { $gte: new Date(Math.min(...times)), $lte: new Date(Math.max(...times) + SYNC_TASK_KEEP_MS + 24 * 60 * 60 * 1000) };
  const put = (parent: unknown, child: unknown) => {
    const key = String(parent || "");
    if (!key || !requestIds.includes(key)) return;
    out.set(key, [...(out.get(key) || []), String(child)]);
  };
  const within = { $in: requestIds };
  const [updates, invoices, coupons, cancels, reschedules, fetches] = await Promise.all([
    modelOf("update", deps).find({ userId, createdAt, $or: [{ parentId: within }, { retryOf: within }] }).select("parentId retryOf").lean(),
    modelOf("invoices", deps).find({ userId, createdAt, $or: [{ parentId: within }, { retryOf: within }] }).select("parentId retryOf").lean(),
    modelOf("coupons", deps).find({ userId, createdAt, $or: [{ parentId: within }, { retryOf: within }] }).select("parentId retryOf").lean(),
    modelOf("cancel", deps).find({ userId, createdAt, retryOf: within }).select("retryOf").lean(),
    modelOf("reschedule", deps).find({ userId, createdAt, retryOf: within }).select("retryOf").lean(),
    modelOf("fetch", deps).find({ userId, type: "fetch", createdAt, $or: [{ "meta.params.parent_id": within }, { "meta.params.retry_of": within }] }).select("meta.params.parent_id meta.params.retry_of").lean(),
  ]);
  for (const r of [...updates, ...invoices, ...coupons, ...cancels, ...reschedules]) put(r.parentId || r.retryOf, r._id);
  for (const r of fetches) {
    const params = ((r.meta as { params?: Record<string, unknown> } | undefined)?.params || {}) as Record<string, unknown>;
    put(params.parent_id || params.retry_of, r._id);
  }
  return out;
}

/** The kind of a request id: it is looked up in each collection (six reads by _id). Only full ids — never the 8-character one. */
export async function findRequest(userId: string, requestId: string, deps: LogDeps = logDefaults): Promise<RequestFacts | null> {
  if (!Types.ObjectId.isValid(requestId) || String(requestId).length !== 24) return null;
  const found = await Promise.all(SYNC_TASK_KINDS.map((kind) => loadFacts(kind, [requestId], deps).then((m) => m.get(requestId) || null)));
  const facts = found.find(Boolean) || null;
  return facts && String(facts.head.userId) === String(userId) ? facts : null;
}

async function parentsOf(rows: RequestFacts[], userId: string, deps: LogDeps): Promise<Map<string, RequestFacts>> {
  const known = new Map(rows.map((f) => [f.head.requestId, f]));
  const out = new Map<string, RequestFacts>();
  for (const id of new Set(rows.map((f) => f.parentId).filter(Boolean))) {
    const parent = known.get(id) || (await findRequest(userId, id, deps).catch(() => null));
    if (parent) out.set(id, parent);
  }
  return out;
}

// ── GET /sync/batches ───────────────────────────────────────────────────────

export async function listBatches(userId: string, query: Record<string, unknown>, deps: LogDeps = logDefaults, probes: LiveProbes = noProbes) {
  const { page, pageSize, skip } = pageOf(query, BATCH_PAGE_SIZE);
  const base = { count: 0, page, page_size: pageSize, task_types: [...TASK_TYPES], results: [] as BatchRow[] };
  if (!Types.ObjectId.isValid(userId) || platformExcluded(query)) return base;
  const uid = new Types.ObjectId(userId);
  const kinds = SYNC_TASK_KINDS.map((kind) => ({ kind, match: batchMatch(kind, uid, query) })).filter((k): k is { kind: SyncTaskKind; match: Record<string, unknown> } => Boolean(k.match));
  if (!kinds.length) return base;

  // Each collection gives the ids of its newest (skip + size) requests; the merge and the slice happen in the database.
  const heads = (k: { kind: SyncTaskKind; match: Record<string, unknown> }) => [{ $match: k.match }, { $sort: { createdAt: -1 as const } }, { $limit: skip + pageSize }, { $project: { _id: 1, createdAt: 1, kind: { $literal: k.kind } } }];
  const [first, ...rest] = kinds;
  const pipeline = [
    ...heads(first),
    ...rest.map((k) => ({ $unionWith: { coll: modelOf(k.kind, deps).collection.name, pipeline: heads(k) } })),
    { $sort: { createdAt: -1 as const, _id: -1 as const } },
    { $skip: skip },
    { $limit: pageSize },
  ];
  const [ids, counts] = await Promise.all([
    modelOf(first.kind, deps).aggregate<{ _id: Types.ObjectId; kind: SyncTaskKind }>(pipeline).option({ maxTimeMS: 20_000 }),
    Promise.all(kinds.map((k) => modelOf(k.kind, deps).countDocuments(k.match).maxTimeMS(20_000))),
  ]);
  base.count = counts.reduce((a, b) => a + b, 0);
  if (!ids.length) return base;

  const facts = new Map<string, RequestFacts>();
  await Promise.all(
    SYNC_TASK_KINDS.map(async (kind) => {
      const mine = ids.filter((r) => r.kind === kind).map((r) => String(r._id));
      if (mine.length) for (const [id, f] of await loadFacts(kind, mine, deps)) facts.set(id, f);
    })
  );
  const rows = ids.map((r) => facts.get(String(r._id))).filter((f): f is RequestFacts => Boolean(f));
  const [children, parents, live] = await Promise.all([childrenOf(uid, rows, deps), parentsOf(rows, userId, deps), livenessFor(rows, probes), countNamedRetryable(rows, deps)]);
  base.results = rows.map((f) => batchRow(f, { parent: parents.get(f.parentId), children: children.get(f.head.requestId) || [], live: live.get(f.head.requestId) || NOT_STALLED, probes }));
  return base;
}

// ── keeping a request's rows in line when it is read ────────────────────────

const READ_SYNC_EVERY_MS = 3000;
/** How long a read waits for the rows to be brought in line; a big request on a slow link finishes behind the answer. */
const READ_SYNC_WAIT_MS = 2500;
const readSyncedAt = new Map<string, number>();

/**
 * Before a request's tasks are shown: are its rows what its document says? The
 * request's own counters against the rows' — when they differ (a worker that
 * writes no rows, a pass that has not come round yet) the rows are brought in
 * line now. At most once every few seconds per request, and the read waits
 * for it only so long. A request older than the log keeps has no rows to bring.
 */
export async function ensureRequestRows(f: RequestFacts, deps: LogDeps = logDefaults): Promise<{ synced: boolean; agree: boolean }> {
  const id = f.head.requestId;
  if (f.head.createdAt.getTime() < Date.now() - SYNC_TASK_KEEP_MS) return { synced: false, agree: true };
  const have = await logCounts(id, deps);
  if (f.tasksRecorded === "all" ? sameCounts(have, f.counts) : have.total > 0) return { synced: false, agree: true };
  const last = readSyncedAt.get(id) || 0;
  if (Date.now() - last < READ_SYNC_EVERY_MS) return { synced: false, agree: false };
  readSyncedAt.set(id, Date.now());
  if (readSyncedAt.size > 500) for (const [k, at] of readSyncedAt) if (Date.now() - at > 60_000) readSyncedAt.delete(k);
  const done = syncRequestTasks(f.head.kind, id, deps).then(
    () => true,
    () => false
  );
  const inTime = await Promise.race([done, new Promise<false>((resolve) => setTimeout(() => resolve(false), READ_SYNC_WAIT_MS).unref())]);
  if (!inTime) return { synced: false, agree: false };
  return { synced: true, agree: f.tasksRecorded !== "all" || sameCounts(await logCounts(id, deps), f.counts) };
}

// ── GET /sync/batches/:id ───────────────────────────────────────────────────

export const TASK_STATUS_LABEL: Record<RequestTaskStatus, string> = {
  queued: "Queued",
  in_progress: "In progress",
  success: "Completed",
  failed: "Failed",
  cancelled: "Cancelled",
  aborted: "Aborted",
  timedout: "Timed out",
};

/** What the Error column shows for a task that ended without an error class. */
const STATUS_SENTENCE: Partial<Record<RequestTaskStatus, string>> = { cancelled: CANCELLED_BEFORE_START, aborted: ABORTED_WHILE_RUNNING, timedout: TIMED_OUT };

export const INCOMPLETE: RequestTaskStatus[] = ["failed", "timedout", "aborted", "cancelled"];

/** ISSUES of the Details modal: every unsuccessful ending with how many tasks it took, failures by error class. */
async function issueSummary(requestId: string, deps: LogDeps) {
  const rows = await deps.Tasks.aggregate<{ _id: { s: RequestTaskStatus; c: string }; n: number }>([
    { $match: { requestId, status: { $in: INCOMPLETE } } },
    { $group: { _id: { s: "$status", c: "$errorCode" }, n: { $sum: 1 } } },
  ]);
  return rows
    .map((r) => ({
      status: r._id.s,
      status_label: TASK_STATUS_LABEL[r._id.s] || r._id.s,
      error_code: r._id.c || null,
      label: r._id.c ? ERROR_CLASSES[r._id.c as ErrorClass] || r._id.c : STATUS_SENTENCE[r._id.s] || ERROR_CLASSES.unknown,
      count: r.n,
    }))
    .sort((a, b) => INCOMPLETE.indexOf(a.status) - INCOMPLETE.indexOf(b.status) || b.count - a.count);
}

export async function getBatch(userId: string, requestId: string, deps: LogDeps = logDefaults, probes: LiveProbes = noProbes) {
  const f = await findRequest(userId, requestId, deps);
  if (!f) return null;
  const uid = new Types.ObjectId(userId);
  const [children, parents, live, rows] = await Promise.all([childrenOf(uid, [f], deps), parentsOf([f], userId, deps), livenessFor([f], probes), ensureRequestRows(f, deps).catch(() => ({ synced: false, agree: false }))]);
  // After the rows were brought in line: a fetch from before the log offers what they hold.
  await countNamedRetryable([f], deps);
  const row = batchRow(f, { parent: parents.get(f.parentId), children: children.get(requestId) || [], live: live.get(requestId) || NOT_STALLED, probes });
  const issues = await issueSummary(requestId, deps);
  const c = f.counts;
  return {
    ...row,
    // OUTCOME, in the modal's own lines.
    outcome: { tasks: c.total, completed: c.success, failed: c.failed, timed_out: c.timed_out, stopped: c.aborted, cancelled: c.cancelled, still_queued: c.queued, still_running: c.in_progress, avg_seconds_per_task: row.avg_seconds_per_task },
    issues,
    incomplete_count: c.failed + c.timed_out + c.aborted + c.cancelled,
    // What "Retry all incomplete" would take: the same, less what a cancel / reschedule never re-submits (Flipkart's own refusals).
    retryable_count: retryableCount(f),
    // False only while the task rows are still being brought in line with the request (a second read agrees).
    tasks_in_step: rows.agree,
  };
}

// ── GET /sync/batches/:id/issues ────────────────────────────────────────────

const ISSUE_ITEMS_MAX = 10_000;

/**
 * The tasks of a request that ended one way (`status`, default failed),
 * grouped by error class, with every item — for "Copy all". `copy_text` is
 * the group's order ids (its e-mails, for account tasks), one a line.
 */
export async function batchIssues(userId: string, requestId: string, query: Record<string, unknown>, deps: LogDeps = logDefaults) {
  const f = await findRequest(userId, requestId, deps);
  if (!f) return null;
  const status = (str(query.status) || "failed") as RequestTaskStatus;
  if (!INCOMPLETE.includes(status)) throw new RequestInputError(`status must be one of ${INCOMPLETE.join(", ")}`);
  await ensureRequestRows(f, deps).catch(() => undefined);
  const rows = await deps.Tasks.find({ requestId, status })
    .sort({ errorCode: 1, _id: 1 })
    .limit(ISSUE_ITEMS_MAX + 1)
    .select("email orderId unitId error errorCode detail")
    .lean();
  const truncated = rows.length > ISSUE_ITEMS_MAX;
  const groups = new Map<string, { error_code: string | null; label: string; count: number; items: Array<{ id: string; email: string | null; order_id: string | null; unit_id: string | null; error: string; detail: string }> }>();
  for (const t of rows.slice(0, ISSUE_ITEMS_MAX)) {
    const code = t.errorCode || "";
    const g = groups.get(code) || { error_code: code || null, label: code ? ERROR_CLASSES[code as ErrorClass] || code : STATUS_SENTENCE[status] || ERROR_CLASSES.unknown, count: 0, items: [] };
    g.count += 1;
    g.items.push({ id: String(t._id), email: t.email || null, order_id: t.orderId || null, unit_id: t.unitId || null, error: t.error || "", detail: t.detail || "" });
    groups.set(code, g);
  }
  const copyOf = (items: Array<{ email: string | null; order_id: string | null }>) => [...new Set(items.map((i) => i.order_id || i.email || "").filter(Boolean))].join("\n");
  const out = [...groups.values()].sort((a, b) => b.count - a.count).map((g) => ({ ...g, copy_text: copyOf(g.items) }));
  return {
    request_id: requestId,
    short_id: shortId(requestId),
    task_type: SYNC_TASK_TYPE_OF[f.head.kind],
    status,
    status_label: TASK_STATUS_LABEL[status],
    count: truncated ? await deps.Tasks.countDocuments({ requestId, status }) : rows.length,
    truncated,
    groups: out,
    copy_text: copyOf(out.flatMap((g) => g.items)),
  };
}

// ── GET /sync/tasks ─────────────────────────────────────────────────────────

type TaskRow = Pick<IOrderSyncTask, "requestId" | "platform" | "taskType" | "source" | "status" | "email" | "orderId" | "unitId" | "triggeredBy" | "createdAt" | "startedAt" | "finishedAt" | "error" | "errorCode" | "detail"> & { _id: unknown };

function publicTask(t: TaskRow) {
  const started = t.startedAt ? new Date(t.startedAt).getTime() : 0;
  const finished = t.finishedAt ? new Date(t.finishedAt).getTime() : 0;
  return {
    id: String(t._id),
    request_id: t.requestId || null,
    short_request_id: t.requestId ? shortId(t.requestId) : null,
    platform: t.platform || "FLIPKART",
    task_type: t.taskType,
    source: t.source || "user",
    status: t.status,
    email: t.email || null,
    order_id: t.orderId || null,
    unit_id: t.unitId || null,
    triggered_by: triggeredBy(t.triggeredBy || ""),
    created_at: t.createdAt,
    started_at: t.startedAt ?? null,
    finished_at: t.finishedAt ?? null,
    duration_seconds: started && finished ? Math.max(0, Math.round((finished - started) / 1000)) : null,
    error: t.error || null,
    error_code: t.errorCode || null,
    // Ours: the raw reason behind `error`.
    detail: t.detail || "",
  };
}

/**
 * All tasks — or the tasks of one request (`request_id`, the full id). Rows
 * older than 7 days are out, as on the reference: the collection's TTL removes
 * them within a minute or so of that age, and the query never returns one in
 * between.
 */
export async function listTasks(userId: string, query: Record<string, unknown>, deps: LogDeps = logDefaults) {
  const { page, pageSize, skip } = pageOf(query, TASK_PAGE_SIZE);
  const base = { count: 0, page, page_size: pageSize, retention_days: SYNC_TASK_KEEP_MS / 86_400_000, error_classes: errorClassList(), results: [] as Array<ReturnType<typeof publicTask>> };
  if (!Types.ObjectId.isValid(userId) || platformExcluded(query)) return base;
  const cut = new Date(Date.now() - SYNC_TASK_KEEP_MS);
  const created = createdRange(query) || {};
  if (!created.$gte || created.$gte < cut) created.$gte = cut;
  const filter: Record<string, unknown> = { createdAt: created };

  const requestId = str(query.request_id);
  if (requestId) {
    // The full id only: the 8-character one is for reading, and two requests can share it.
    if (!Types.ObjectId.isValid(requestId) || requestId.length !== 24) throw new RequestInputError("request_id must be the full request id");
    const f = await findRequest(userId, requestId, deps);
    if (!f) return base;
    await ensureRequestRows(f, deps).catch(() => undefined);
    // The request is this workspace's (just checked), so its rows are read by request alone — straight off its index.
    filter.requestId = requestId;
  } else {
    filter.userId = new Types.ObjectId(userId);
  }
  const orderId = str(query.order_id);
  if (orderId) filter.orderId = orderId.toUpperCase();
  const email = str(query.email);
  if (email) filter.email = email.toLowerCase();
  const taskType = str(query.task_type);
  if (taskType) filter.taskType = taskType;
  const source = str(query.source);
  if (source) filter.source = source;
  const status = str(query.status);
  if (status) {
    if (!REQUEST_TASK_STATUSES.includes(status as RequestTaskStatus)) return base;
    filter.status = status;
  }
  const errorCode = str(query.error_code);
  if (errorCode) filter.errorCode = errorCode;

  const [count, rows] = await Promise.all([
    deps.Tasks.countDocuments(filter).maxTimeMS(20_000),
    deps.Tasks.find(filter).sort({ createdAt: -1, _id: -1 }).skip(skip).limit(pageSize).maxTimeMS(20_000).lean(),
  ]);
  base.count = count;
  base.results = rows.map((t) => publicTask(t as TaskRow));
  return base;
}
