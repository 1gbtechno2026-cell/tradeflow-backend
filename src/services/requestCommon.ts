import os from "node:os";
import { Types, type Model } from "mongoose";
import { config } from "../config.js";
import { getRedis } from "../lib/redis.js";
import { OrderJobRun, type IOrderJobRun, type JobRunStatus, type JobRunTrigger, type JobRunType } from "../models/OrderJobRun.js";
import { countKeyOf, type RequestCounts, type RequestStatus, type RequestTaskStatus } from "../models/OrderInvoiceRequest.js";
import { OrderApiError, OrderApiSessionError } from "./orderApi.js";

/**
 * What the request kinds started from "Start a request" share — the invoice
 * batch (services/invoiceRequests.ts) and the coupon batch
 * (services/couponRequests.ts), and the body checks of all four triggers:
 *
 *   the 2000 rule            the reference's cap and its sentence, word for word
 *   error classes            the reference's list (code → the sentence the Error column shows)
 *   counts                   {total, queued, in_progress, success, failed, cancelled, aborted, timed_out}
 *                            (the task status behind `timed_out` is spelled `timedout`, as on the reference)
 *   worker heartbeat         a Redis key per queue, so the API can tell "no worker is
 *                            listening on this queue" from "the worker is busy"
 *   the ledger row           one order_job_runs row per request (refId = request id), written
 *                            by id — never through orderJobRuns' in-process map — so the API
 *                            that creates a request and the worker that runs it keep one row
 */

// ── the 2000 rule ───────────────────────────────────────────────────────────

/** The reference's "Multi Value Allowed" cap (the dashboard's MULTI_VALUE_MAX). */
export const REQUEST_VALUE_MAX = 2000;

/** The reference's sentence, word for word (the dashboard's multiValueLimitMessage). */
export function tooManyValuesMessage(label: string, count: number, max = REQUEST_VALUE_MAX): string {
  return `Too many values in "${label}": ${count} entered, but the maximum is ${max}. Please reduce it.`;
}

/** A body the trigger refuses: answered as `status` with `{ error, code }`. */
export class RequestInputError extends Error {
  constructor(
    message: string,
    public readonly status = 400,
    public readonly code = "invalid_request"
  ) {
    super(message);
  }
}

/** An array, or a typed list split on spaces / commas — the reference's inputs. */
export function splitValues(value: unknown): string[] {
  if (Array.isArray(value)) return value.map((item) => String(item ?? "").trim()).filter(Boolean);
  if (value == null || typeof value === "object") return [];
  return String(value)
    .split(/[\s,]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

export function distinct(values: string[]): string[] {
  return [...new Set(values)];
}

/** Throws the reference sentence when a list holds more than 2000 DISTINCT values (repeats do not count). */
export function assertValueLimit(label: string, values: string[], max = REQUEST_VALUE_MAX): void {
  const count = new Set(values).size;
  if (count > max) throw new RequestInputError(tooManyValuesMessage(label, count, max), 400, "too_many_values");
}

/** "Only Flipkart is connected" — the one platform Trade Flow reads. Missing = FLIPKART, as the routes always took it. */
export function assertFlipkart(platform: unknown): "FLIPKART" {
  const p = String(platform || "FLIPKART").trim().toUpperCase();
  if (p !== "FLIPKART") throw new RequestInputError("Only Flipkart is connected", 400, "platform_not_connected");
  return "FLIPKART";
}

// ── error classes ───────────────────────────────────────────────────────────

/** The reference's error classes: code → the sentence its Error column and filter show. */
export const ERROR_CLASSES = {
  not_logged_in: "Account is not logged in",
  account_blocked: "Account is blocked or needs verification",
  account_not_eligible: "Account can't be used for this",
  order_not_found: "Order not found on this account",
  not_allowed_now: "Platform does not allow this action for this order now",
  no_slots: "No delivery date available to reschedule to",
  invalid_coupon: "Coupon link is invalid or expired",
  nothing_available: "Nothing available yet",
  platform_unavailable: "Platform did not respond; try again later",
  platform_declined: "Platform declined the request",
  rate_limited: "Platform is limiting requests; try again later",
  unexpected_response: "Platform returned a response we could not read",
  partially_failed: "Some items in this task failed",
  invalid_request: "Request was missing required details",
  internal_error: "Something went wrong on our side",
  unknown: "Failed (no reason recorded)",
} as const;

export type ErrorClass = keyof typeof ERROR_CLASSES;

export interface ClassifiedError {
  errorClass: ErrorClass;
  /** The class's sentence. */
  error: string;
  /** The raw first line behind it, cut to 200 characters. */
  detail: string;
}

export function classified(errorClass: ErrorClass, detail = ""): ClassifiedError {
  return { errorClass, error: ERROR_CLASSES[errorClass], detail: String(detail || "").split("\n")[0].slice(0, 200) };
}

const NETWORK_RE = /ECONNRESET|ETIMEDOUT|ECONNREFUSED|EAI_AGAIN|ENOTFOUND|EPIPE|EHOSTUNREACH|ENETUNREACH|socket hang up|Timeout \d+ms exceeded|network error|fetch failed|net::ERR_/i;
const UNREADABLE_RE = /neither PDF nor zip|had no PDF|did not answer JSON|not a PDF or zip|carried neither/i;

/** The class of a Flipkart HTTP status, or null when the status says nothing by itself (2xx / 3xx). */
export function classOfHttpStatus(status: number): ErrorClass | null {
  if (status === 401) return "not_logged_in";
  if (status === 404 || status === 410) return "nothing_available";
  if (status === 429) return "rate_limited";
  if (status >= 500) return "platform_unavailable";
  if (status >= 400) return "platform_declined";
  return null;
}

/**
 * What went wrong on a call to Flipkart, as a reference error class:
 * a dead session → not_logged_in; 404 / 410 → nothing_available (Flipkart has
 * not issued it); 429 → rate_limited; 5xx, timeouts and dropped sockets →
 * platform_unavailable; any other 4xx → platform_declined; an answer that is
 * not the file or JSON expected → unexpected_response; everything else (our
 * storage, our database) → internal_error.
 */
export function classifyPlatformError(err: unknown): ClassifiedError {
  const message = err instanceof Error ? err.message : String(err ?? "");
  if (err instanceof OrderApiSessionError) return classified("not_logged_in", message);
  if (err instanceof OrderApiError) {
    const byStatus = classOfHttpStatus(err.status);
    if (byStatus) return classified(byStatus, message);
    return classified("unexpected_response", message);
  }
  if (UNREADABLE_RE.test(message)) return classified("unexpected_response", message);
  if (NETWORK_RE.test(message)) return classified("platform_unavailable", message);
  return classified(message ? "internal_error" : "unknown", message);
}

const OUR_SIDE_RE = /mongodb|Connection pool|<monitor>|MongoServer|MongoNetwork|not configured|INVOICE_S3_BUCKET|Redis|BullMQ|S3 (upload|put)/i;
const SESSION_RE = /not logged[ _-]?in|^Session:|session (has )?expired|re-?login this ID|no_session|API refused the session|no usable saved session|login page|Refresh this ID/i;
const BLOCKED_RE = /blocked|needs verification|too many attempts|account is locked/i;
const NO_ORDER_RE = /no such order|Not found in Order Units|unknown_order|no longer in Order Units|is it the account/i;
const NOT_ALLOWED_RE = /not eligible for cancellation|not_cancellable|This option is not available|does not allow a date change|cannot be cancelled/i;
const NO_SLOTS_RE = /did not offer|no dates? (recognised|available|offered)/i;
const HTTP_STATUS_RE = /\b(?:HTTP|status)\s*(\d{3})\b/i;

/**
 * The class of a failure kept only as a sentence — what cancel, reschedule,
 * update and fetch store (units[].message, accounts[].message, failedItems[].reason).
 * The sentence itself stays with the task as its `detail`; the class is read
 * beside it, never instead of it. `platformAction` = the task asks Flipkart to
 * change something (cancel, reschedule): a refusal nothing else explains is
 * then the platform's, not ours.
 */
export function classifyMessage(message: unknown, platformAction = false): ClassifiedError {
  const text = String(message ?? "").trim();
  if (!text) return classified("unknown");
  if (OUR_SIDE_RE.test(text)) return classified("internal_error", text);
  if (SESSION_RE.test(text)) return classified("not_logged_in", text);
  if (BLOCKED_RE.test(text)) return classified("account_blocked", text);
  if (NO_ORDER_RE.test(text)) return classified("order_not_found", text);
  if (NO_SLOTS_RE.test(text)) return classified("no_slots", text);
  if (NOT_ALLOWED_RE.test(text)) return classified("not_allowed_now", text);
  if (/\b429\b|too many requests|rate.?limit/i.test(text)) return classified("rate_limited", text);
  if (UNREADABLE_RE.test(text) || /gave no data|rendered nothing|could not read|unexpected (answer|response)/i.test(text)) return classified("unexpected_response", text);
  const status = Number(text.match(HTTP_STATUS_RE)?.[1] || 0);
  const byStatus = status ? classOfHttpStatus(status) : null;
  if (byStatus) return classified(byStatus, text);
  if (NETWORK_RE.test(text) || /timed? ?out|Timeout/i.test(text)) return classified("platform_unavailable", text);
  return classified(platformAction ? "platform_declined" : "internal_error", text);
}

// ── counts ──────────────────────────────────────────────────────────────────

export const CANCELLED_BEFORE_START = "Cancelled before it started";
export const ABORTED_WHILE_RUNNING = "Stopped while running: the request was cancelled";
/** The reference's sentence for a task whose worker went away, or that never came back. */
export const TIMED_OUT = "Timed out: the agent went offline or the task ran too long";
export const WORKER_NOT_PICKED_UP = "The worker has not picked this up — restart the worker";

/**
 * What a cancel / reschedule unit carries when Batch requests → Cancel closed
 * it. It starts with "Cleared by", as a Force-clear's note does, so the task
 * log reads it the same way: cancelled "Cancelled before it started" when the
 * unit had not been taken, aborted when it had.
 */
export const clearedByStop = (by: string) => `Cleared by ${by || "a user"} — the request was cancelled`;

export function emptyCounts(total = 0): RequestCounts {
  return { total, queued: total, in_progress: 0, success: 0, failed: 0, cancelled: 0, aborted: 0, timed_out: 0 };
}

export function countsOf(statuses: Iterable<RequestTaskStatus>): RequestCounts {
  const counts = emptyCounts(0);
  for (const s of statuses) {
    counts.total += 1;
    counts[countKeyOf(s)] += 1;
  }
  return counts;
}

/** Tasks that reached an end, whichever one — the reference's `finished_count`. */
export function finishedCount(c: RequestCounts): number {
  return c.success + c.failed + c.cancelled + c.aborted + c.timed_out;
}

/** The reference's per-request numbers derived from the counts and the three times. */
export function requestProgress(c: RequestCounts, startedAt: Date | null | undefined, finishedAt: Date | null | undefined) {
  const finished = finishedCount(c);
  const start = startedAt ? new Date(startedAt).getTime() : 0;
  const end = finishedAt ? new Date(finishedAt).getTime() : Date.now();
  const ran = c.success + c.failed + c.aborted + c.timed_out;
  const duration = start ? Math.max(0, (end - start) / 1000) : null;
  const avg = duration != null && ran ? duration / ran : null;
  const left = c.queued + c.in_progress;
  return {
    finished_count: finished,
    progress_percent: c.total ? Math.floor((finished / c.total) * 100) : 0,
    duration_seconds: duration == null ? null : Math.round(duration),
    avg_seconds_per_task: avg == null ? null : Math.round(avg * 100) / 100,
    eta_seconds: !finishedAt && avg != null && left ? Math.round(left * avg) : null,
  };
}

// ── worker heartbeat ────────────────────────────────────────────────────────

const HEARTBEAT_EVERY_MS = 20_000;
const HEARTBEAT_TTL_MS = 75_000;

function aliveKey(queueName: string) {
  return `tradeflow:worker-alive:${queueName}`;
}

/** Worker side: say "a worker that knows this queue is running" until the process ends. */
export function startWorkerHeartbeat(queueName: string): NodeJS.Timeout {
  const beat = () =>
    void getRedis()
      .set(aliveKey(queueName), JSON.stringify({ host: os.hostname(), pid: process.pid, at: new Date().toISOString() }), "PX", HEARTBEAT_TTL_MS)
      .catch(() => undefined);
  beat();
  const t = setInterval(beat, HEARTBEAT_EVERY_MS);
  t.unref();
  return t;
}

/** Is a worker listening on this queue? A Redis error counts as "yes": nothing is failed on a guess. */
export async function workerAlive(queueName: string): Promise<boolean> {
  try {
    return (await getRedis().exists(aliveKey(queueName))) === 1;
  } catch {
    return true;
  }
}

/**
 * For whoever shows an open request: is a worker behind it? A request keeps
 * `in_progress` when its worker dies (a restart resumes it), so its status
 * alone cannot say "running". `stalled` = still open with no worker listening
 * on its queue; a request that has ended is never stalled (`worker_alive` null).
 */
export interface RequestLiveness {
  worker_alive: boolean | null;
  stalled: boolean;
  stalled_message: string;
}

export function livenessOf(status: string, alive: boolean, left: number, unit: "order" | "account"): RequestLiveness {
  if (status !== "queued" && status !== "in_progress") return { worker_alive: null, stalled: false, stalled_message: "" };
  if (alive) return { worker_alive: true, stalled: false, stalled_message: "" };
  return {
    worker_alive: false,
    stalled: true,
    stalled_message: status === "queued" ? "No worker is listening on this queue — restart the worker" : `The worker stopped with ${left} ${unit}(s) left — restart the worker to resume`,
  };
}

/** livenessOf for a list of requests: one heartbeat read per queue, and none when every request has ended. */
export async function withLiveness<T extends { status: string; counts: RequestCounts }>(rows: Array<{ row: T; queue: string }>, unit: "order" | "account"): Promise<Array<T & RequestLiveness>> {
  const alive = new Map<string, boolean>();
  const out: Array<T & RequestLiveness> = [];
  for (const { row, queue } of rows) {
    const open = row.status === "queued" || row.status === "in_progress";
    if (open && !alive.has(queue)) alive.set(queue, await workerAlive(queue));
    out.push({ ...row, ...livenessOf(row.status, alive.get(queue) ?? true, row.counts.queued + row.counts.in_progress, unit) });
  }
  return out;
}

export async function clearWorkerHeartbeat(queueName: string): Promise<void> {
  await getRedis().del(aliveKey(queueName)).catch(() => undefined);
}

// ── the ledger row ──────────────────────────────────────────────────────────

export type LedgerModel = Model<IOrderJobRun>;

const FAILED_KEEP = 500;

/** The row a request shows as in "Sync Order Units From Platform": created `queued`, total known. */
export async function openRequestRun(
  input: { userId: string; type: JobRunType; refId: string; trigger?: JobRunTrigger; by?: string; total: number; message?: string; meta?: Record<string, unknown> },
  Ledger: LedgerModel = OrderJobRun
): Promise<string> {
  const run = await Ledger.create({
    userId: new Types.ObjectId(input.userId),
    type: input.type,
    trigger: input.trigger || "user",
    by: input.by || "",
    status: "queued",
    total: input.total,
    startedAt: null,
    message: input.message || "",
    refId: input.refId,
    meta: input.meta || {},
  });
  return String(run._id);
}

export interface RequestRunState {
  status: RequestStatus;
  counts: RequestCounts;
  startedAt: Date | null;
  finishedAt: Date | null;
  message?: string;
  /**
   * Set when nothing is running the request although it is not finished (no
   * worker took it, or the worker stopped mid-way): the row then reads
   * `failed` with this sentence instead of `running`, so nothing that waits
   * for "no invoice run in progress" waits on a row no process is behind.
   */
  parked?: string;
  failedItems?: Array<{ key: string; reason: string }>;
}

/** request status → the ledger's vocabulary. */
export function ledgerStatusOf(state: Pick<RequestRunState, "status" | "parked" | "counts">): JobRunStatus {
  if (state.parked) return "failed";
  if (state.status === "queued") return "queued";
  if (state.status === "in_progress") return "running";
  if (state.status === "cancelled") return "stopped";
  return "completed";
}

/** The $set that brings a request's ledger row in line with the request. Pure: the checks read it. */
export function ledgerPatchOf(state: RequestRunState): Record<string, unknown> {
  const c = state.counts;
  const processed = finishedCount(c);
  const ran = c.success + c.failed + c.aborted + c.timed_out;
  const start = state.startedAt ? new Date(state.startedAt).getTime() : 0;
  const end = state.finishedAt ? new Date(state.finishedAt).getTime() : Date.now();
  const status = ledgerStatusOf(state);
  const left = c.queued + c.in_progress;
  const message =
    state.parked ||
    state.message ||
    (status === "queued"
      ? `${c.total} task(s) waiting for the worker`
      : `${c.success} done, ${c.failed + c.timed_out} failed${c.cancelled + c.aborted ? `, ${c.cancelled + c.aborted} cancelled` : ""}${left ? `, ${left} left` : ""}`);
  return {
    status,
    total: c.total,
    processed,
    success: c.success,
    failed: c.failed + c.timed_out,
    cancelled: c.cancelled + c.aborted,
    startedAt: state.startedAt,
    finishedAt: status === "queued" || status === "running" ? null : state.finishedAt || new Date(),
    avgMsPerItem: start && ran ? Math.round(Math.max(0, end - start) / ran) : 0,
    message,
    ...(state.failedItems ? { failedItems: state.failedItems.slice(0, FAILED_KEEP) } : {}),
  };
}

/**
 * Write the row. Several jobs of one request write it, each from its own read
 * of the request, so a write can arrive after a newer one: it is applied only
 * when it does not go back — never over more finished tasks than it knows of,
 * and a write made while the request was running never over the row of a
 * request that has ended (`final` marks the writes made once it has).
 */
export async function writeRequestRun(
  key: { userId: string | Types.ObjectId; type: JobRunType; refId: string },
  state: RequestRunState,
  Ledger: LedgerModel = OrderJobRun,
  opts: { final?: boolean } = {}
): Promise<void> {
  const patch = ledgerPatchOf(state);
  await Ledger.updateOne(
    { userId: key.userId, type: key.type, refId: key.refId, processed: { $lte: patch.processed as number }, ...(opts.final ? {} : { status: { $nin: ["completed", "stopped"] } }) },
    { $set: patch }
  ).catch(() => undefined);
}

/** This machine, as its rows and the request log's checks name it: TRADEFLOW_HOST_ID, else the machine name when the process started (config.hostId). */
export function hostName(): string {
  return config.hostId;
}
