import { DelayedError, type Job, type Queue } from "bullmq";
import { Types, type Model } from "mongoose";
import type { APIRequestContext, Browser, BrowserContext, Page } from "playwright";
import { config } from "../config.js";
import { isDestroyedContext } from "../lib/evalOnPage.js";
import { Order } from "../models/Order.js";
import { OrderSyncSettings } from "../models/OrderSyncSettings.js";
import { demoteCircuitFor, forgetDemoteCircuit, markSessionEligibleForRelogin, SessionDeadError } from "./sessionEligibility.js";
import { OrderUpdateRequest, type IOrderUpdateRequest, type IUpdateAccount, type IUpdateSchedule, type UpdateSource } from "../models/OrderUpdateRequest.js";
import { isScheduleName, SCHEDULE_LABELS, type ScheduleName } from "../models/ScheduleLease.js";
import { getUpdateQueue, type UpdateJobData } from "../queue.js";
import { lockAccount } from "./accountLock.js";
import { blockFlipkartLogout, closeBrowser, confirmLoginLanding, flipkartLoginUrl, launchStealthContext, restoreFlipkartSession, sleep } from "./browser.js";
import { openOrderApiSession, OrderApiSessionError } from "./orderApi.js";
import { apiScrapeAndSave, scrapeAndSave, sessionExpiredMessage, type OrderAccount } from "./orderFetch.js";
import { resolveOrderFetchMode } from "./orderSyncSettings.js";
import { DEFAULT_UPDATE_STATUSES, stageOf } from "./orderLifecycle.js";
import { resolveTargets, type UpdateTriggerInput } from "./orderUpdate.js";
import { recordUnitChanges, snapshotUnit } from "./orderUnitEvents.js";
import { NO_SAVED_COOKIES, resolveLoggedInSession } from "./sessionStore.js";
import { noteDeliveredOrder, queueInvoicesAfterUpdate } from "./requestChains.js";
import { clockStamp, clockTime, releaseLease, renewLease, slotStamp, supersededBy, supersededText, type ScheduleClaim, type ScheduleRunner, type ScheduleRunResult } from "./scheduleClock.js";
import { retireLegacySchedulers } from "./scheduleSettings.js";
// startRequests makes its requests through this module; both sides only call each other inside functions.
import { STATUS_ALIASES, statusSelection, storedStatusKeys } from "./startRequests.js";
import { closeUpdateAccountRows, openRequestTasksQuietly, syncRequestTasksQuietly, taskLog, updateTaskKey } from "./syncTaskLog.js";

/**
 * "Update Order Units" on the worker.
 *
 *   createUpdateRequest   resolve the selection to units (same rules as the
 *                         in-API update: no selection = every active unit),
 *                         group by account, one order_update_requests doc,
 *                         one queue job per account
 *   runOrderUpdateJob     (worker) one account: its units re-read from
 *                         Flipkart through the order API, or the page when the
 *                         API refuses the session or a unit — exactly what the
 *                         in-API update did, with progress on the request doc
 *   runUpdateSweep        (worker, the shared schedule clock) a fixed time of
 *                         "every active unit" (every 6 h) or of the
 *                         out-for-delivery recheck (every 2 h) that this
 *                         machine took: one request made under that lease —
 *                         skipped while a sweep of that kind this machine
 *                         made is still open
 *   legacyStatusView      the shape the dashboard's status panel already polls
 *
 * The schedules (services/scheduleClock.ts): the two sweeps run on the one
 * clock every machine shares, not on a BullMQ scheduler per machine. A sweep's
 * request carries `schedule { name, slotAt, token }`; its end gives the lease
 * back, and an account job of a sweep another machine has taken over since
 * ends before it calls Flipkart.
 *
 * Why the worker: the run survives an API restart, shares the per-account
 * lock with checkout / cancel / sync, and scales with the update windows
 * setting instead of the API box's RAM.
 */

/** A unit stuck "running" this long belongs to a dead worker; the next job re-picks it. */
const STALE_ACCOUNT_MS = 20 * 60 * 1000;
const LOCK_RETRY_MS = 20_000;
const LOG_KEEP = 300;
/** Update reaper: how often it sweeps, and how long an open request must be quiet to count as stranded. */
const UPDATE_REAP_EVERY_MS = 60 * 1000;
const UPDATE_REAP_STALE_MS = 5 * 60 * 1000;
/**
 * Active-unit sweep / Out-for-delivery recheck periods (ms) as this machine's .env has them. The periods in use are
 * the clocks' (order_schedule_leases, written once by the first seed from these values); here they are only what
 * GET /update/settings answers for a workspace that has no clock yet.
 */
export const UPDATE_SWEEP_MS = Math.max(15 * 60 * 1000, Number(process.env.UPDATE_SWEEP_MS) || 6 * 60 * 60 * 1000);
export const UPDATE_OFD_SWEEP_MS = Math.max(15 * 60 * 1000, Number(process.env.UPDATE_OFD_SWEEP_MS) || 2 * 60 * 60 * 1000);
/** The statuses the 2-hour recheck covers, as the reference names them: the last mile, where the OTP and the delivery land. */
export const OFD_STATUSES = ["Out for delivery", "Dispatched"];
/** Those two as the page scraper writes them, wherever they stand in a status. */
const OFD_RE = /out for delivery|dispatched/i;

/**
 * Every spelling those statuses are stored under, lower-cased. The page scraper wrote "Out for delivery"; the order
 * API — what nearly every unit is read through since 2026-10 — writes "ofd". Which spellings are one status is kept
 * in ONE place, STATUS_ALIASES (startRequests.ts, what the Update button's status filter reads), so the recheck's
 * selection and the OTP pass below cannot fall behind it again.
 */
export function ofdStatusSpellings(): Set<string> {
  return new Set(OFD_STATUSES.flatMap((label) => STATUS_ALIASES[label.toLowerCase()] || [label]).map((s) => s.toLowerCase()));
}

/** Is a unit in the last mile — the only stage it can carry a delivery OTP in? */
export function isOutForDelivery(statusKey: string | null | undefined): boolean {
  const key = String(statusKey || "").trim();
  return Boolean(key) && (OFD_RE.test(key) || ofdStatusSpellings().has(key.toLowerCase()));
}

/**
 * The status_key values the out-for-delivery recheck selects in this workspace: the labels resolved exactly as a
 * status filter of the Update button is (STATUS_ALIASES, then the workspace's own spelling of each key).
 */
export async function ofdStatusKeys(userId: string): Promise<string[]> {
  const keys = statusSelection(OFD_STATUSES, await storedStatusKeys(userId)).keys;
  // Never empty: an update that names no status takes every active unit.
  return keys.length ? keys : [...OFD_STATUSES];
}
/**
 * HTTP "windows" = accounts refreshed in parallel. In API mode each window is a
 * light HTTP context (tens of MB), so the cap is high; Chrome is separately gated
 * below. Pure-Chrome runs should stay low — each account is a ~0.5 GB browser.
 */
export const WINDOW_LIMITS = { min: 1, max: 20 } as const;

/**
 * Global cap on concurrent Chrome contexts across ALL update windows, independent
 * of the HTTP window count. At 12 HTTP windows a burst of page fallbacks could try
 * to open 12 browsers (~6 GB); this baton-passing semaphore holds that to a few at
 * a time so RAM can't blow up. HTTP fetching stays fully parallel.
 */
const CHROME_GATE_MAX = Math.max(1, Math.floor(Number(process.env.UPDATE_CHROME_CONCURRENCY) || 4));
let chromeInUse = 0;
const chromeWaiters: Array<() => void> = [];
async function acquireChrome(): Promise<void> {
  if (chromeInUse < CHROME_GATE_MAX) {
    chromeInUse++;
    return;
  }
  await new Promise<void>((resolve) => chromeWaiters.push(resolve));
  // Resolved = a releaser handed us its slot; the in-use count was kept, not re-incremented.
}
function releaseChrome(): void {
  const next = chromeWaiters.shift();
  if (next) next(); // pass the slot straight to the next waiter (count unchanged)
  else chromeInUse--; // no one waiting: free the slot
}

// ── settings ────────────────────────────────────────────────────────────────

export async function resolveUpdateWindows(userId: string): Promise<{ windows: number; source: "ui" | "env"; changedBy: string; changedAt: Date | null }> {
  const row = Types.ObjectId.isValid(userId) ? await OrderSyncSettings.findOne({ userId }).lean().catch(() => null) : null;
  const ui = Math.floor(Number(row?.updateWindows || 0));
  if (ui >= WINDOW_LIMITS.min) return { windows: Math.min(WINDOW_LIMITS.max, ui), source: "ui", changedBy: row?.updateChangedBy || "", changedAt: row?.updateChangedAt ?? null };
  const env = Math.floor(Number(process.env.UPDATE_CONCURRENCY || 0));
  return { windows: env >= 1 ? Math.min(WINDOW_LIMITS.max, env) : 3, source: "env", changedBy: "", changedAt: null };
}

export async function setUpdateWindows(userId: string, windows: unknown, by: string) {
  const n = Math.floor(Number(windows));
  if (!Number.isFinite(n) || n < WINDOW_LIMITS.min || n > WINDOW_LIMITS.max) throw new Error(`windows must be ${WINDOW_LIMITS.min}–${WINDOW_LIMITS.max}`);
  await OrderSyncSettings.updateOne({ userId }, { $set: { updateWindows: n, updateChangedBy: by || "", updateChangedAt: new Date() }, $setOnInsert: { userId } }, { upsert: true });
  return resolveUpdateWindows(userId);
}

// ── request ─────────────────────────────────────────────────────────────────

async function pushLog(requestId: Types.ObjectId | string, level: "info" | "warn" | "error", message: string) {
  console.log(`[orders-update] ${message}`);
  await OrderUpdateRequest.updateOne({ _id: requestId }, { $push: { logs: { $each: [{ time: new Date(), level, message }], $slice: -LOG_KEEP } } }).catch(() => undefined);
}

/** One unit of an update that is already known — a retry takes the units of the request it retries, as that request stored them. */
export interface PreparedUpdateUnit {
  email: string;
  platformId: Types.ObjectId | null;
  orderId: string;
  itemId: string;
  unitId: string;
  orderUrl: string;
}

/**
 * `opts.units` (Batch requests → Retry): the request is made of exactly these
 * units and the selection is not resolved again. Whether each account is
 * still logged in is then the job's to find, as for any account that logs
 * out between a request and its turn: its units fail "Session: … — re-login
 * this ID".
 */
export async function createUpdateRequest(
  userId: string,
  input: UpdateTriggerInput,
  opts: {
    source?: UpdateSource;
    by?: string;
    params?: Record<string, unknown>;
    parentId?: string;
    units?: PreparedUpdateUnit[];
    /** A sweep of the shared clock: the fixed time and the lease claim it is made under (runUpdateSweep). */
    schedule?: IUpdateSchedule;
    /** The queue its account jobs go on — this machine's update queue unless a check passes its own. */
    queue?: Queue<UpdateJobData>;
  } = {}
) {
  const resolved = opts.units ? null : await resolveTargets(userId, input);
  const targets = opts.units
    ? opts.units.map((u) => ({ account: { email: u.email, id: u.platformId }, card: { orderId: u.orderId, itemId: u.itemId, unitId: u.unitId, orderUrl: u.orderUrl } }))
    : (resolved || []).map((t) => ({ account: { email: t.account.email, id: t.account.id as Types.ObjectId | null }, card: t.card }));
  if (!targets.length) throw new Error("No active order units to update. Fetch first, or paste a logged-in email + order id.");

  const byAccount = new Map<string, IUpdateAccount>();
  const taken = new Set<string>();
  for (const t of targets) {
    const key = t.account.email.toLowerCase();
    // resolveTargets never names a unit twice; a prepared list might.
    if (taken.has(`${key}|${t.card.unitId}`)) continue;
    taken.add(`${key}|${t.card.unitId}`);
    const acc = byAccount.get(key) || {
      email: key,
      platformId: t.account.id,
      units: [],
      total: 0,
      status: "queued" as const,
      done: 0,
      failed: 0,
      statusChanged: 0,
      otpNew: 0,
      via: "",
      message: "",
      startedAt: null,
      finishedAt: null,
    };
    acc.units.push({ orderId: t.card.orderId, itemId: t.card.itemId || "", unitId: t.card.unitId, orderUrl: t.card.orderUrl || "" });
    acc.total = acc.units.length;
    byAccount.set(key, acc);
  }
  const [mode, speed] = await Promise.all([resolveOrderFetchMode(userId), resolveUpdateWindows(userId)]);
  const request = await OrderUpdateRequest.create({
    userId: new Types.ObjectId(userId),
    source: opts.source || "manual",
    selectionMethod: input.selectionMethod || (input.orderIds?.length ? "order_ids" : input.emails?.length ? "emails" : input.csvRows?.length ? "csv" : input.orderStatus?.length ? "order_status" : "active"),
    params: opts.params || {},
    parentId: String(opts.parentId || "").trim(),
    systemId: config.systemId,
    ...(opts.schedule ? { schedule: opts.schedule } : {}),
    createdBy: String(opts.by || "").trim(),
    status: "queued",
    mode: mode.mode,
    windows: speed.windows,
    accounts: [...byAccount.values()],
    logs: [
      {
        time: new Date(),
        level: "info",
        message: `Update ${taken.size} unit(s) across ${byAccount.size} account(s) · ${speed.windows} window(s) · ${mode.mode === "api" ? "API mode (Chrome only on fallback)" : "Chrome"} · ${opts.source || "manual"}${opts.schedule && isScheduleName(opts.schedule.name) ? ` · ${SCHEDULE_LABELS[opts.schedule.name]}, the ${clockTime(opts.schedule.slotAt)} time, on ${config.hostId}` : ""}`,
      },
    ],
  });
  const requestId = String(request._id);
  // The task log: a row per unit, queued — written before a job can take one (services/syncTaskLog.ts).
  await openRequestTasksQuietly("update", request.toObject());
  await (opts.queue || getUpdateQueue()).addBulk([...byAccount.keys()].map((email) => ({ name: "update", data: { requestId, email }, opts: { jobId: `${requestId}-${email}` } })));
  console.log(`[orders-update] request ${requestId}: ${taken.size} unit(s) on ${byAccount.size} account(s) queued (${opts.source || "manual"})`);
  return publicUpdateRequest(request.toObject());
}

/**
 * Requests the system queues by itself after other work, many times a day:
 * the after-order update (services/afterOrderChain.ts) follows every checkout
 * burst. A control that names no request — Force kill, POST /update/stop
 * without an id, the sync panel's Update row, GET /update/status and the
 * Force clear that acts on its requestId — never means one of these while a
 * request a person or a schedule started is open, or was the last such.
 */
const AUTOMATIC_UPDATE_SOURCES: UpdateSource[] = ["system-order"];

/**
 * The update a control means when it names none: the newest OPEN request a
 * person or a schedule started; else the newest open one of any source (an
 * after-order update running alone); else, unless `openOnly`, the newest
 * request a person or a schedule started, then the newest of any. "" = none.
 */
export async function currentUpdateRequestId(userId: string, openOnly = false, Update: Model<IOrderUpdateRequest> = OrderUpdateRequest): Promise<string> {
  const open = { $in: ["queued", "running"] };
  const started = { $nin: AUTOMATIC_UPDATE_SOURCES };
  const tries: Array<Record<string, unknown>> = [{ userId, status: open, source: started }, { userId, status: open }, ...(openOnly ? [] : [{ userId, source: started }, { userId }])];
  for (const filter of tries) {
    const row = await Update.findOne(filter).sort({ createdAt: -1 }).select("_id").lean();
    if (row) return String(row._id);
  }
  return "";
}

export async function cancelUpdateRequest(userId: string, requestId?: string) {
  const current = requestId ? "" : await currentUpdateRequestId(userId, true);
  const row = requestId || current ? await OrderUpdateRequest.findOne({ _id: requestId || current, userId }) : null;
  if (!row) return null;
  const queue = getUpdateQueue();
  let removed = 0;
  for (const acc of row.accounts) {
    if (acc.status !== "queued") continue;
    const job = await queue.getJob(`${String(row._id)}-${acc.email}`).catch(() => null);
    if (job) {
      await job.remove().catch(() => undefined);
      removed++;
    }
  }
  await OrderUpdateRequest.updateOne(
    { _id: row._id },
    { $set: { status: "cancelled", finishedAt: new Date(), "accounts.$[a].status": "cancelled", "accounts.$[a].finishedAt": new Date(), "accounts.$[a].message": "Stopped before it started" } },
    { arrayFilters: [{ "a.status": "queued" }] }
  );
  await pushLog(row._id, "warn", `Stopped by request — ${removed} account(s) removed from the queue; accounts already running finish their current unit`);
  await syncRequestTasksQuietly("update", String(row._id));
  return { id: String(row._id), removed };
}

/**
 * A sweep of the shared clock that another machine has taken over since this
 * one made it (this machine slept, or lost its network, in the middle of it):
 * what has not started ends cancelled with who took over — "Superseded — Mac-2
 * took over the update of all active units at 18:15" — and leaves the queue;
 * an account being read finishes the unit in hand, as after a Stop (its job
 * reads the request before the next unit). The request itself reads cancelled.
 * False when it had already ended.
 */
export async function supersedeUpdateRequest(requestId: string, message: string, queue: Queue<UpdateJobData> = getUpdateQueue()): Promise<boolean> {
  if (!Types.ObjectId.isValid(requestId)) return false;
  const open = { $in: ["queued", "running"] };
  const row = await OrderUpdateRequest.findOne({ _id: requestId, status: open }).select("accounts.email accounts.status").lean();
  if (!row) return false;
  const now = new Date();
  const res = await OrderUpdateRequest.updateOne(
    { _id: requestId, status: open },
    { $set: { status: "cancelled", finishedAt: now, "accounts.$[a].status": "cancelled", "accounts.$[a].finishedAt": now, "accounts.$[a].message": message } },
    { arrayFilters: [{ "a.status": "queued" }] }
  );
  // Another job of the same request got here first: it has said so and cleared the queue.
  if (!res.modifiedCount) return false;
  const waiting = row.accounts.filter((a) => a.status === "queued");
  let removed = 0;
  for (const acc of waiting) {
    const job = await queue.getJob(`${requestId}-${acc.email}`).catch(() => null);
    // The job that noticed is one of these and cannot be removed while it runs: it ends by itself.
    if (job && (await job.remove().then(() => true, () => false))) removed++;
  }
  await pushLog(requestId, "warn", `${message} — ${waiting.length} account(s) that had not started are left to that machine (${removed} job(s) removed from the queue); an account being read finishes its current unit`);
  await syncRequestTasksQuietly("update", requestId);
  return true;
}

export async function getUpdateRequest(userId: string, requestId: string) {
  if (!Types.ObjectId.isValid(requestId)) return null;
  const row = await OrderUpdateRequest.findOne({ _id: requestId, userId }).lean();
  return row ? publicUpdateRequest(row) : null;
}

/**
 * Admin "Force clear" for a stranded update (the twin of the cancellation
 * Force-clear): drop any per-account jobs still on the queue, mark the
 * queued/running accounts terminal and close the request. An update only
 * re-reads Flipkart — it parks no Order rows — so there is nothing to free:
 * the user simply presses Update again. `mode` is kept for API symmetry with
 * the cancel abort; both just clear here.
 */
export async function abortUpdateRequest(userId: string, requestId: string, by: string, mode: "reset" | "abort" = "reset") {
  if (!Types.ObjectId.isValid(requestId)) return null;
  const row = await OrderUpdateRequest.findOne({ _id: requestId, userId }).lean();
  if (!row) return null;

  // 1) Drop any BullMQ jobs still carrying this request (one per account email).
  const emails = [...new Set((row.accounts || []).map((a) => a.email))];
  const queue = getUpdateQueue();
  let removed = 0;
  await Promise.all(
    emails.map(async (email) => {
      const job = await queue.getJob(`${requestId}-${email}`).catch(() => null);
      if (job) {
        await job.remove().catch(() => undefined);
        removed++;
      }
    })
  );

  // 2) Close the stuck accounts and the request (accounts already done stay as they are).
  const stuckAccounts = (row.accounts || []).filter((a) => a.status === "queued" || a.status === "running").length;
  const note = `Cleared by ${by || "admin"} — press Update to re-run`;
  await OrderUpdateRequest.updateOne(
    { _id: requestId },
    {
      $set: {
        "accounts.$[a].status": "cancelled",
        "accounts.$[a].message": note,
        "accounts.$[a].finishedAt": new Date(),
        status: "cancelled",
        finishedAt: new Date(),
      },
    },
    { arrayFilters: [{ "a.status": { $in: ["queued", "running"] } }] }
  );
  await pushLog(requestId, "warn", `Force-cleared by ${by || "admin"} — ${removed} queue job(s) dropped, ${stuckAccounts} stuck account(s) closed`);
  console.log(`[orders-update ${requestId}] force-cleared by ${by || "admin"} (mode=${mode}, ${removed} job(s) dropped, ${stuckAccounts} account(s) closed)`);
  await syncRequestTasksQuietly("update", requestId);
  const updated = await OrderUpdateRequest.findById(requestId).lean();
  return updated ? { ...publicUpdateRequest(updated), removed, stuckAccounts } : null;
}

export async function listUpdateRequests(userId: string, limit = 20) {
  const rows = await OrderUpdateRequest.find({ userId }).sort({ createdAt: -1 }).limit(Math.min(100, Math.max(1, limit))).select("-accounts.units -logs").lean();
  return rows.map((r) => publicUpdateRequest(r));
}

export function publicUpdateRequest(r: Partial<IOrderUpdateRequest> & { _id: unknown }) {
  const accounts = r.accounts || [];
  const unitsOf = (a: IUpdateAccount) => (a.total || a.units?.length || 0);
  const total = accounts.reduce((n, a) => n + unitsOf(a), 0);
  const done = accounts.reduce((n, a) => n + a.done, 0);
  const failed = accounts.reduce((n, a) => n + a.failed, 0);
  const statusChanged = accounts.reduce((n, a) => n + (a.statusChanged || 0), 0);
  const otpNew = accounts.reduce((n, a) => n + (a.otpNew || 0), 0);
  const count = (s: IUpdateAccount["status"]) => accounts.filter((a) => a.status === s).length;
  return {
    id: String(r._id),
    source: r.source || "manual",
    selectionMethod: r.selectionMethod || "",
    params: r.params || {},
    parentId: r.parentId || "",
    createdBy: r.createdBy || "",
    status: r.status || "queued",
    mode: r.mode || "api",
    windows: r.windows || 1,
    createdAt: r.createdAt ?? null,
    startedAt: r.startedAt ?? null,
    finishedAt: r.finishedAt ?? null,
    counts: {
      accounts: accounts.length,
      accountsQueued: count("queued"),
      accountsRunning: count("running"),
      accountsDone: count("done"),
      accountsFailed: count("failed"),
      accountsCancelled: count("cancelled"),
      units: total,
      done,
      failed,
      left: Math.max(0, total - done - failed - accounts.filter((a) => a.status === "cancelled").reduce((n, a) => n + unitsOf(a) - a.done - a.failed, 0)),
      statusChanged,
      otpNew,
    },
    accounts: accounts.map((a) => ({
      email: a.email,
      status: a.status,
      units: unitsOf(a),
      done: a.done,
      failed: a.failed,
      statusChanged: a.statusChanged || 0,
      otpNew: a.otpNew || 0,
      via: a.via || "",
      message: a.message || "",
      startedAt: a.startedAt ?? null,
      finishedAt: a.finishedAt ?? null,
    })),
    logs: (r.logs || []).map((l) => ({ time: l.time instanceof Date ? l.time.toISOString() : String(l.time), level: l.level, message: l.message })),
  };
}

/**
 * The dashboard's status panel polls GET /update/status and expects the old
 * in-process shape; this builds it from the request the panel follows
 * (currentUpdateRequestId — the newest, unless that is an automatic one while
 * another matters more) so nothing in that panel changes.
 */
export async function legacyStatusView(userId: string) {
  const current = await currentUpdateRequestId(userId);
  const row = current ? await OrderUpdateRequest.findById(current).lean() : null;
  const [mode, speed] = await Promise.all([resolveOrderFetchMode(userId).catch(() => null), resolveUpdateWindows(userId).catch(() => null)]);
  if (!row) {
    return { running: false, cancelled: false, currentEmail: null, logs: [], total: 0, done: 0, failed: 0, skipped: 0, savedOrders: 0, windows: speed?.windows || 1, startedAt: null, finishedAt: null, elapsedMs: 0, mode: mode?.mode || config.orderFetchMode, modeSource: mode?.source || "env", requestId: "", source: "" };
  }
  const pub = publicUpdateRequest(row);
  const running = row.status === "queued" || row.status === "running";
  const active = row.accounts.find((a) => a.status === "running");
  const currentEmail = active ? `${active.email} ${active.done + active.failed}/${active.total || active.units.length}` : running ? `${pub.counts.accountsQueued} account(s) waiting for a window` : null;
  const start = row.startedAt || row.createdAt;
  const end = row.finishedAt || new Date();
  return {
    running,
    cancelled: row.status === "cancelled",
    currentEmail,
    logs: pub.logs.slice(-250),
    total: pub.counts.units,
    done: pub.counts.done,
    failed: pub.counts.failed,
    skipped: 0,
    savedOrders: pub.counts.done,
    windows: row.windows,
    startedAt: start ? new Date(start).toISOString() : null,
    finishedAt: row.finishedAt ? new Date(row.finishedAt).toISOString() : null,
    elapsedMs: start ? Math.max(0, new Date(end).getTime() - new Date(start).getTime()) : 0,
    mode: running ? row.mode : mode?.mode || row.mode,
    modeSource: mode?.source || "env",
    requestId: pub.id,
    source: pub.source,
    statusChanged: pub.counts.statusChanged,
    otpNew: pub.counts.otpNew,
  };
}

// ── worker ──────────────────────────────────────────────────────────────────

async function setAccount(requestId: Types.ObjectId | string, email: string, patch: Record<string, unknown>) {
  const set: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(patch)) set[`accounts.$[a].${k}`] = v;
  await OrderUpdateRequest.updateOne({ _id: requestId }, { $set: set }, { arrayFilters: [{ "a.email": email }] });
}

async function bumpAccount(requestId: Types.ObjectId | string, email: string, inc: Record<string, number>) {
  const $inc: Record<string, number> = {};
  for (const [k, v] of Object.entries(inc)) $inc[`accounts.$[a].${k}`] = v;
  await OrderUpdateRequest.updateOne({ _id: requestId }, { $inc }, { arrayFilters: [{ "a.email": email }] });
}

/**
 * Done once every account is settled — the last job to finish closes the
 * request, and its dead-session circuit goes with it. A stopped request never
 * closes here: an account that stops on it drops the circuit once no other
 * account of it is still running.
 */
async function settleRequest(requestId: Types.ObjectId | string, stopped = false) {
  const res = await OrderUpdateRequest.updateOne(
    { _id: requestId, status: { $in: ["queued", "running"] }, "accounts.status": { $nin: ["queued", "running"] } },
    { $set: { status: "done", finishedAt: new Date() } }
  );
  if (!res.modifiedCount && stopped && !(await OrderUpdateRequest.exists({ _id: requestId, "accounts.status": "running" }).catch(() => true))) forgetDemoteCircuit(String(requestId));
  if (res.modifiedCount) {
    forgetDemoteCircuit(String(requestId));
    const row = await OrderUpdateRequest.findById(requestId).select("-accounts.units").lean();
    if (row) {
      const pub = publicUpdateRequest(row);
      await pushLog(requestId, "info", `Update finished. ok ${pub.counts.done}, failed ${pub.counts.failed}, status changed ${pub.counts.statusChanged}, new OTP ${pub.counts.otpNew}`);
      // A sweep of the shared clock: its lease is given back with how it went (the tick "finished — ok 926, failed 3").
      await releaseSweepLease(row, `ok ${pub.counts.done}, failed ${pub.counts.failed}${pub.counts.statusChanged ? `, status changed ${pub.counts.statusChanged}` : ""}${pub.counts.otpNew ? `, new OTP ${pub.counts.otpNew}` : ""}`);
    }
    // The request has ended: its task rows are brought in line with it once, whole.
    await syncRequestTasksQuietly("update", String(requestId));
    // The reference's "Delivered check": one invoice fetch for the orders this update saw arrive at Delivered.
    await queueInvoicesAfterUpdate(String(requestId)).catch((err) => console.warn(`[chain] invoice fetch after update ${String(requestId).slice(-8)} failed: ${err instanceof Error ? err.message.split("\n")[0] : err}`));
  }
}

/**
 * The request of a clock's sweep has ended: the lease it was made under is
 * released, with the sentence of its last tick. Nothing happens when the lease
 * is no longer this claim's (another machine took over; the clock had already
 * released a request that Stop or Force clear closed). A release the database
 * refuses is not lost: the clock's next look sees the closed request and
 * releases it itself.
 */
async function releaseSweepLease(row: { _id: unknown; userId: unknown; schedule?: IUpdateSchedule | null }, reason: string): Promise<void> {
  const s = row.schedule;
  if (!s?.token || !isScheduleName(s.name)) return;
  const id = String(row._id);
  await releaseLease({ userId: String(row.userId), name: s.name, token: s.token }, { outcome: "finished", reason, runRef: { kind: "update", id } }).catch((err) =>
    console.warn(`[orders-update] request ${id.slice(-8)}: its ${s.name} lease was not released (${err instanceof Error ? err.message.split("\n")[0] : err}) — the clock's next look releases it`)
  );
}

/** My Orders: where the update's Chrome checks that the account is logged in before it opens any order. */
const MY_ORDERS_URL = "https://www.flipkart.com/account/orders?link=home_orders";
/** A unit that needed the page after Flipkart had sent this ID to the login page twice: no Chrome is opened for it. */
export const PAGE_SKIPPED = "page skipped — Flipkart sent this ID to the login page";

/**
 * What one update job reaches outside Mongo. The worker passes nothing;
 * scripts/deadSessionFlowsCheck.ts passes a Chromium that answers every
 * request itself, a fake order API and no Redis lock.
 */
export interface UpdateJobDeps {
  launch: typeof launchStealthContext;
  lock: typeof lockAccount;
  /** Flipkart's order API (API mode): the session, and one unit's read. */
  openApi: typeof openOrderApiSession;
  apiRead: typeof apiScrapeAndSave;
  /** One unit read from its order page, on the account's tab: the page fallback, and the OTP pass of out-for-delivery units. */
  pageRead: typeof scrapeAndSave;
  /** This machine's update queue. */
  queue: () => Queue<UpdateJobData>;
  /**
   * A leftover job of the per-machine scheduler this queue had before the
   * shared clock has arrived: the worker makes its schedule clock look (it
   * runs a fixed time only when one is due and free). Nothing by default.
   */
  clockLook: () => Promise<void>;
}

const updateJobDefaults: UpdateJobDeps = {
  launch: launchStealthContext,
  lock: lockAccount,
  openApi: openOrderApiSession,
  apiRead: apiScrapeAndSave,
  pageRead: scrapeAndSave,
  queue: getUpdateQueue,
  clockLook: async () => undefined,
};

/**
 * One account of an update request.
 *
 * A dead session (sessionEligibility.ts): Chrome opens My Orders before any
 * order, and a login landing there is looked at once more with the cookies
 * attached again (confirmLoginLanding). A second landing marks the page dead —
 * no unit opens Chrome for this account again. If the account read nothing in
 * this run, it is handed off once (F1's rules decide whether the row moves to
 * Not logged in), its units end failed with one sentence and the account
 * stops. If it had read units already, nothing is handed off: only the page
 * work left is skipped.
 *
 * A sweep of the shared clock (the request carries `schedule`): when another
 * machine has taken that schedule over since — this one slept in the middle
 * of its sweep — the account, and with it everything the request had left,
 * ends "Superseded — …" before the account is locked, its session looked up
 * or Flipkart called.
 */
export async function runOrderUpdateJob(job: Job<UpdateJobData>, token: string | undefined, overrides: Partial<UpdateJobDeps> = {}): Promise<void> {
  const deps: UpdateJobDeps = { ...updateJobDefaults, ...overrides };
  if (job.name === "sweep") {
    // A job of the per-machine BullMQ scheduler this queue had before the shared clock: one that was already waiting
    // when the worker moved over, or whose scheduler could not be removed then. It never runs a sweep by itself — the
    // scheduler is removed again and the clock looks, which runs a fixed time only when it is due and free.
    console.log(`[orders-update] a leftover ${job.data.sweep === "ofd" ? "out-for-delivery" : "active-units"} scheduler job — no sweep is run from it, the shared clock decides`);
    await retireUpdateSchedulers(deps.queue());
    await deps.clockLook();
    return;
  }
  const { requestId, email } = job.data;
  const request = await OrderUpdateRequest.findById(requestId).lean();
  if (!request || request.status === "cancelled") return;
  const me = email.toLowerCase();
  const account = request.accounts.find((a) => a.email === me);
  if (!account) return;
  const stale = account.status === "running" && (!account.startedAt || Date.now() - new Date(account.startedAt).getTime() > STALE_ACCOUNT_MS);
  if (account.status !== "queued" && !stale) return;

  // One read of the lease, and only for a clock's sweep: has another machine taken this schedule since the request
  // was made? Then this is work left over on a machine that was away, and the other machine is doing it.
  const sweep = request.schedule;
  if (sweep?.token && isScheduleName(sweep.name)) {
    const taker = await supersededBy(String(request.userId), { name: sweep.name, token: sweep.token });
    if (taker) {
      const message = supersededText(sweep.name, taker, taker.at);
      await supersedeUpdateRequest(requestId, message, deps.queue());
      // An account an earlier worker left "running" was not waiting: closed the same way, here.
      if (account.status !== "queued") await setAccount(requestId, me, { status: "cancelled", finishedAt: new Date(), message });
      await closeUpdateAccountRows(requestId, me, { status: "cancelled", message });
      return;
    }
  }

  const unlock = await deps.lock(me, `update:${requestId}:${config.workerId}`);
  if (!unlock) {
    await job.moveToDelayed(Date.now() + LOCK_RETRY_MS, token);
    throw new DelayedError();
  }
  const userId = String(request.userId);
  const log = (level: "info" | "warn" | "error", msg: string) => pushLog(requestId, level, `${me}: ${msg}`);
  let launched: { browser: Browser; context: BrowserContext } | null = null;
  let chromeHeld = false; // true once this account has taken a slot from the Chrome gate
  // How this account ended, for the task log: the units it never got to end the same way.
  let ended: { status: string; message: string } | null = null;
  // Units this run read — whether a dead page is handed off, and how the account counts in the request's circuit.
  let read = 0;
  let circuitPushed = false;
  try {
    await OrderUpdateRequest.updateOne({ _id: requestId, status: "queued" }, { $set: { status: "running", startedAt: new Date() } });
    await setAccount(requestId, me, { status: "running", startedAt: new Date(), done: 0, failed: 0, statusChanged: 0, otpNew: 0, message: "" });

    const session = await resolveLoggedInSession(me);
    if (!session.ok) {
      ended = { status: "failed", message: `Session: ${session.reason} — re-login this ID` };
      await setAccount(requestId, me, { status: "failed", finishedAt: new Date(), message: `Session: ${session.reason} — re-login this ID` });
      // Only "no saved cookies" on a logged_in row is certain without a page; a row with no sessionSavedAt is already in the login pool.
      await markSessionEligibleForRelogin({
        userId: request.userId,
        platformId: account.platformId,
        email: me,
        reason: `Update: ${session.reason} — re-login this ID`,
        flow: "update",
        confirmed: session.reason === NO_SAVED_COOKIES,
        sessionSavedAt: session.sessionSavedAt ?? null,
        via: "worker",
        requestId,
      });
      await log("warn", `session: ${session.reason} — re-login this ID`);
      return;
    }
    const acct: OrderAccount = { id: new Types.ObjectId(session.platformId), email: session.email, cookies: session.cookies, sessionSavedAt: session.sessionSavedAt };
    const mode = request.mode;

    // Chrome, on the first unit that needs the page. `page` is kept only once My Orders opened on it.
    let page: Page | null = null;
    let tab: Page | null = null; // the account's one tab, kept or not
    // Flipkart sent this ID to the login page twice: no unit opens Chrome for it again.
    let pageDead = false;
    const openPage = async (): Promise<Page> => {
      if (pageDead) throw new SessionDeadError(PAGE_SKIPPED);
      if (page) return page;
      // One browser per account, and no more than CHROME_GATE_MAX across all windows.
      // Guard the acquire so a retry after a failed launch never takes a 2nd slot.
      if (!chromeHeld) {
        await acquireChrome();
        chromeHeld = true;
      }
      // One browser and one tab for the account, however many times the check below runs.
      const l = launched ?? (launched = await deps.launch({ headless: config.headless }));
      let p = tab;
      if (!p) {
        p = tab = await l.context.newPage();
        await blockFlipkartLogout(p);
      }
      await restoreFlipkartSession(p, acct.cookies);
      try {
        await p.goto(MY_ORDERS_URL, { waitUntil: "domcontentloaded", timeout: 30_000 });
      } catch (err) {
        // A My Orders load that timed out or failed says nothing about the login: the tab is kept, as it always
        // was, so the next unit goes straight to its own order page (scrapeAndSave's login checks still stop a
        // dead one). Only a tab sitting on the login page is not kept.
        if (!flipkartLoginUrl(p.url())) page = p;
        throw err;
      }
      if (flipkartLoginUrl(p.url())) {
        // Once more with the cookies attached again; a re-check that cannot run (it throws) confirms nothing.
        if (await confirmLoginLanding(p, acct.cookies).catch(() => false)) {
          pageDead = true;
          throw new SessionDeadError();
        }
        if (flipkartLoginUrl(p.url())) throw new Error("Flipkart session expired. Refresh this ID in platform id first.");
      }
      page = p;
      return p;
    };
    // The stored page was sent to the login page later (an order URL): looked at once more, once per account.
    let landingChecked = false;
    /** Is this failure Flipkart's login page, twice? Sets pageDead when it is. */
    const loginTwice = async (err: unknown, text: string): Promise<boolean> => {
      if (err instanceof SessionDeadError) return true;
      if (pageDead || landingChecked || !page || !/^Flipkart session expired/.test(text)) return false;
      landingChecked = true;
      pageDead = await confirmLoginLanding(page, acct.cookies).catch(() => false);
      return pageDead;
    };
    // The dead page has been dealt with: handed off, or (units already read) the rest of the page work skipped.
    let deadNoted = false;

    let api: APIRequestContext | null = null;
    if (mode === "api") {
      try {
        api = await deps.openApi(acct.cookies);
      } catch (err) {
        await log("warn", `API session unavailable (${err instanceof Error ? err.message : err}) — using the page`);
      }
    }
    let via = api ? "api" : "page";
    let noData = 0;
    // Part A — value-aware hybrid: units the HTTP read leaves without an OTP while
    // out for delivery are the only ones that genuinely need the page. Collect them
    // in Phase 1 and scrape just those in Phase 2, so HTTP does the bulk and Chrome
    // only opens for the OTP it alone can read.
    const needsScrape: typeof account.units = [];
    try {
      for (let n = 0; n < account.units.length; n++) {
        const fresh = await OrderUpdateRequest.findById(requestId).select("status").lean();
        if (!fresh || fresh.status === "cancelled") {
          ended = { status: "cancelled", message: `Stopped after ${n} unit(s)` };
          await setAccount(requestId, me, { status: "cancelled", finishedAt: new Date(), message: `Stopped after ${n} unit(s)` });
          return;
        }
        const u = account.units[n];
        const taskKey = updateTaskKey(me, u.unitId);
        taskLog.mark(requestId, taskKey, { status: "in_progress" });
        const card = { orderId: u.orderId, itemId: u.itemId, unitId: u.unitId, orderUrl: u.orderUrl, amount: "" };
        const before = await snapshotUnit(userId, u.unitId);
        try {
          let result: Awaited<ReturnType<typeof scrapeAndSave>>;
          let usedPage = false; // did this unit already go through Chrome this round?
          if (api) {
            try {
              result = await deps.apiRead(api, userId, acct, card, { applyCalendar: false });
            } catch (apiErr) {
              if (apiErr instanceof OrderApiSessionError) {
                await log("warn", `${apiErr.message} — ${pageDead ? "and the page is skipped: the rest of this account fails" : "using the page from here"}`);
                await api.dispose().catch(() => undefined);
                api = null;
                via = "page";
              } else {
                // "— page" alone is no failure yet (the page may read it); "— page skipped" is this unit's failure.
                await log("warn", `${u.orderId}/${u.unitId}: API read failed (${apiErr instanceof Error ? apiErr.message.slice(0, 140) : apiErr}) — ${pageDead ? "page skipped" : "page"}`);
              }
              result = await deps.pageRead(await openPage(), userId, acct, card, { applyCalendar: false });
              usedPage = true;
            }
          } else {
            result = await deps.pageRead(await openPage(), userId, acct, card, { applyCalendar: false });
            usedPage = true;
          }
          const after = await snapshotUnit(userId, u.unitId);
          // Read over HTTP, out for delivery (under either spelling: the API stores "ofd"), still no OTP → the OTP is
          // page-only here. Defer to Phase 2 rather than opening Chrome mid-loop.
          if (!usedPage && after && isOutForDelivery(after.status_key) && !after.delivery_otp) {
            needsScrape.push(u);
          }
          // Stage changes (not a vocabulary swap between the API's "Approved" and the page's
          // "Order Confirmed"), new OTPs and address changes become order_unit_events rows
          // — the dashboard's alerts read those.
          const changes = await recordUnitChanges(userId, u.unitId, before, after, requestId);
          if (changes.stageChanged && stageOf(after?.status_key) === "delivered") await noteDeliveredOrder(requestId, u.orderId);
          await bumpAccount(requestId, me, { done: 1, ...(changes.stageChanged ? { statusChanged: 1 } : {}), ...(changes.otpNew ? { otpNew: 1 } : {}) });
          read += 1;
          taskLog.mark(requestId, taskKey, { status: "success" });
          if (changes.stageChanged || changes.otpNew || changes.addressChanged) {
            await log(
              "info",
              `${u.orderId}/${u.unitId}: ${before?.status_key || "?"} → ${after?.status_key || result.statusKey || "?"}${changes.otpNew ? ` · OTP ${after?.delivery_otp}` : ""}${changes.addressChanged ? ` · ADDRESS CHANGED → ${after?.shipping_address?.text || "?"}` : ""}`
            );
          }
        } catch (err) {
          if (isDestroyedContext(err)) throw err;
          const text = err instanceof Error ? err.message.split("\n")[0] : String(err);
          const dead = await loginTwice(err, text);
          await bumpAccount(requestId, me, { failed: 1 });
          taskLog.mark(requestId, taskKey, { status: "failed", message: text });
          if (/gave no data/.test(text)) noData++;
          // A unit that only skipped the dead page adds nothing: the account's one line already said why.
          if (!(dead && deadNoted)) await log("warn", `${u.orderId}/${u.unitId}: ${text.slice(0, 200)}`);
          if (dead && !deadNoted) {
            deadNoted = true;
            if (read === 0) {
              // Nothing of this account read in this run, the order API refused it (or the run reads pages) and
              // Flipkart sent My Orders to the login page twice: handed off once; the account stops here.
              const circuit = demoteCircuitFor(String(requestId));
              const wasOpen = circuit.open;
              circuit.push("dead");
              circuitPushed = true;
              if (circuit.open && !wasOpen) await pushLog(requestId, "warn", `Dead sessions: ${circuit.describe()} — no ID is moved to Not logged in for the rest of this update`);
              const out = await markSessionEligibleForRelogin({
                userId: request.userId,
                platformId: acct.id,
                email: me,
                reason: `Update: ${mode === "api" ? "the order API refused this ID and " : ""}Flipkart sent /account/orders to the login page twice — re-login`,
                flow: "update",
                confirmed: true,
                sessionSavedAt: session.sessionSavedAt,
                circuitOpen: circuit.open,
                via: "worker",
                requestId,
              });
              const message = sessionExpiredMessage(out);
              // The units it never got to are failed with it (closeUpdateAccountRows gives their rows the same sentence).
              const untried = account.units.length - (n + 1);
              if (untried > 0) await bumpAccount(requestId, me, { failed: untried });
              ended = { status: "failed", message };
              await setAccount(requestId, me, { status: "failed", via, finishedAt: new Date(), message });
              await log("warn", `${message}${untried ? ` · ${untried} more unit(s) not tried` : ""}`);
              return;
            }
            await log("warn", `Flipkart sent this ID to the login page twice after ${read} unit(s) were read — not moved to Not logged in; the rest of its page work is skipped`);
          }
        }
      }
      // ── Phase 2 — surgical scrape ───────────────────────────────────────────
      // Only OFD units the API couldn't give an OTP for. One browser (gated), each
      // unit already counted as `done` in Phase 1, so here we pick up only the OTP
      // (and any stage move the page reveals) without re-counting. Not on a page
      // that is already dead. A unit whose page shows no OTP either (cash on
      // delivery often has none) is simply left as the API read it: no failure.
      if (needsScrape.length && !pageDead) {
        await log("info", `${needsScrape.length} out-for-delivery unit(s) without an API OTP — scraping those for OTP`);
        for (let k = 0; k < needsScrape.length; k++) {
          const u = needsScrape[k];
          const fresh = await OrderUpdateRequest.findById(requestId).select("status").lean();
          if (!fresh || fresh.status === "cancelled") break;
          const card = { orderId: u.orderId, itemId: u.itemId, unitId: u.unitId, orderUrl: u.orderUrl, amount: "" };
          try {
            const before = await snapshotUnit(userId, u.unitId);
            await deps.pageRead(await openPage(), userId, acct, card, { applyCalendar: false });
            const after = await snapshotUnit(userId, u.unitId);
            const changes = await recordUnitChanges(userId, u.unitId, before, after, requestId);
            if (changes.stageChanged && stageOf(after?.status_key) === "delivered") await noteDeliveredOrder(requestId, u.orderId);
            if (changes.otpNew || changes.stageChanged) {
              await bumpAccount(requestId, me, { ...(changes.stageChanged ? { statusChanged: 1 } : {}), ...(changes.otpNew ? { otpNew: 1 } : {}) });
            }
            if (changes.otpNew) {
              via = via === "api" ? "api+scrape" : via;
              await log("info", `${u.orderId}/${u.unitId}: OTP ${after?.delivery_otp} (page)`);
            }
          } catch (err) {
            if (isDestroyedContext(err)) throw err;
            const text = err instanceof Error ? err.message.split("\n")[0] : String(err);
            if (await loginTwice(err, text)) {
              // Every unit of this pass was read over the API in this run: no hand-off, the rest of the pass is skipped.
              deadNoted = true;
              await log("warn", `Flipkart sent this ID to the login page twice during the OTP pass — not moved to Not logged in (${read} unit(s) read); ${needsScrape.length - k} OTP scrape(s) skipped`);
              break;
            }
            await log("warn", `${u.orderId}/${u.unitId}: OTP scrape failed (${text.slice(0, 160)})`);
          }
        }
      }

      const final = (await OrderUpdateRequest.findById(requestId).select("accounts").lean())?.accounts.find((a) => a.email === me);
      // Every order page rendered nothing on a page that had passed the login check: the orders, not the login,
      // are the likely problem (a CSV that paired this ID with another one's orders). Nothing is handed off.
      if (noData > 0 && (final?.done ?? 0) === 0 && noData === account.units.length) {
        await log("warn", `every order page rendered nothing — the session passed the login check; these orders may belong to another Platform ID (${noData} unit(s))`);
      }
      ended = { status: "done", message: "" };
      await setAccount(requestId, me, {
        status: "done",
        via,
        finishedAt: new Date(),
        message: `${final?.done ?? 0} refreshed${final?.failed ? `, ${final.failed} failed` : ""}${final?.statusChanged ? `, ${final.statusChanged} status changed` : ""}${final?.otpNew ? `, ${final.otpNew} new OTP` : ""} · ${via}`,
      });
    } finally {
      await api?.dispose().catch(() => undefined);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message.split("\n")[0].slice(0, 300) : String(err);
    ended = { status: "failed", message };
    await setAccount(requestId, me, { status: "failed", finishedAt: new Date(), message });
    await log("error", `failed: ${message}`);
  } finally {
    // How this account counts in its request's dead-session circuit, unless its hand-off already counted it.
    if (!circuitPushed) demoteCircuitFor(String(requestId)).push(read > 0 ? "alive" : "other");
    if (launched) {
      const l = launched as { browser: Browser; context: BrowserContext };
      await closeBrowser(l.browser, l.context).catch(() => undefined);
    }
    if (chromeHeld) releaseChrome();
    await unlock();
    if (ended) await closeUpdateAccountRows(requestId, me, ended);
    await settleRequest(requestId, ended?.status === "cancelled");
    await sleep(300);
  }
}

// ── schedules ───────────────────────────────────────────────────────────────

/** "the 25 Nov 12:15 request 1a2b3c4d" needs both: a request by the last 8 characters of its id, as Batch requests shows it. */
const shortRequest = (id: unknown) => String(id).slice(-8);

/**
 * One fixed time of a sweep, taken by this machine's schedule clock (`claim`,
 * services/scheduleClock.ts) — "active": every active unit, every 6 h; "ofd":
 * the units out for delivery, every 2 h. Makes ONE update request, under the
 * claim: the request carries `schedule { name, slotAt, token }`, its params
 * say which schedule, which time and which machine, and the lease follows it
 * until it ends (settleRequest gives the lease back).
 *
 * What it answers is the time's tick:
 *   { runRef }   the request was made — "ran on Mac — 926 unit(s) on 402 account(s)"
 *   { done }     no unit to refresh — "ran on Mac — nothing to refresh"
 *   { skipped }  an earlier request of this kind that THIS machine made is
 *                still open (the update after a nightly fetch counts: it is a
 *                "schedule" request too), or the lease went to another machine
 *                before the request could be made
 *   throws       anything else — "failed — could not start: …"
 * A request of the same kind another machine left open does not hold it up:
 * this time could only be taken because that machine's lease had run out. It
 * is named in the tick and left to that machine, whose own jobs end it as
 * superseded when it comes back.
 */
export async function runUpdateSweep(kind: "active" | "ofd", claim: ScheduleClaim, overrides: { queue?: Queue<UpdateJobData> } = {}): Promise<ScheduleRunResult> {
  const userId = claim.userId;
  const name: ScheduleName = kind === "ofd" ? "update_ofd" : "update_active";
  const source: UpdateSource = kind === "ofd" ? "ofd" : "schedule";
  const open = await OrderUpdateRequest.find({ userId, source, status: { $in: ["queued", "running"] } })
    .select("_id createdAt systemId params.ran_on")
    .sort({ createdAt: 1 })
    .lean();
  const mine = open.find((r) => (r.systemId || "") === config.systemId);
  if (mine) {
    console.log(`[orders-update] ${kind} sweep skipped — request ${String(mine._id)} from ${mine.createdAt?.toISOString()} still open`);
    return { skipped: true, reason: `request ${shortRequest(mine._id)} from ${clockStamp(mine.createdAt)} is still open on this machine` };
  }
  // The claim once more, now that the reads are done: a machine that fell asleep between taking the time and this
  // line must not start a sweep another machine has taken over meanwhile.
  if (!(await renewLease(claim))) {
    console.log(`[orders-update] ${kind} sweep skipped — the lease went to another machine before the request was made`);
    return { skipped: true, reason: "the lease went to another machine before the sweep started" };
  }
  const left = open.map((r) => `${String((r.params as { ran_on?: unknown } | undefined)?.ran_on || r.systemId || "another machine")}'s request ${shortRequest(r._id)} from ${clockStamp(r.createdAt)} is still open and is left to that machine`);
  const withLeft = (text: string) => [text, ...left].join(" · ");
  try {
    const keys = kind === "ofd" ? await ofdStatusKeys(userId) : [];
    const input: UpdateTriggerInput = kind === "ofd" ? { orderStatus: keys, selectionMethod: "schedule_ofd" } : { selectionMethod: "schedule_active" };
    const req = await createUpdateRequest(userId, input, {
      source,
      by: "schedule",
      params: {
        platform: "FLIPKART",
        selection_method: input.selectionMethod,
        force: false,
        // "All active" names no status: it records the in-progress list it stands for — the schedule's own rule, which
        // leaves Returned out. The recheck records its two labels, and the keys they are stored under here.
        statuses_to_update: kind === "ofd" ? [...OFD_STATUSES] : DEFAULT_UPDATE_STATUSES.filter((s) => s !== "Returned"),
        status_keys: keys,
        email_ids_count: 0,
        order_ids_count: 0,
        order_email_pairs_count: 0,
        // Which schedule, which fixed time (India time) and which machine: Batch requests shows them as "Schedule".
        schedule_name: name,
        schedule_slot: slotStamp(claim.slotAt),
        ran_on: config.hostId,
        attempt: claim.attempt,
      },
      schedule: { name, slotAt: claim.slotAt, token: claim.token },
      queue: overrides.queue,
    });
    console.log(`[orders-update] ${kind} sweep (${clockTime(claim.slotAt)}) → request ${req.id}: ${req.counts.units} unit(s) on ${req.counts.accounts} account(s)`);
    return { runRef: { kind: "update", id: req.id }, reason: withLeft(`${req.counts.units} unit(s) on ${req.counts.accounts} account(s)`) };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (/No active order units/.test(msg)) {
      console.log(`[orders-update] ${kind} sweep (${clockTime(claim.slotAt)}): nothing to refresh`);
      return { done: true, reason: withLeft(kind === "ofd" ? "nothing out for delivery" : "nothing to refresh") };
    }
    console.warn(`[orders-update] ${kind} sweep failed: ${msg}`);
    throw err;
  }
}

/**
 * A sweep as the worker's schedule clock runs it. `supersede`: another machine
 * took the schedule over while this one was away — the request this machine
 * made for it stops (supersedeUpdateRequest). A request the clock no longer
 * knows of (the worker was restarted since) is caught by its own account jobs.
 */
export function updateSweepRunner(kind: "active" | "ofd", overrides: { queue?: Queue<UpdateJobData> } = {}): ScheduleRunner {
  return {
    run: (claim) => runUpdateSweep(kind, claim, overrides),
    supersede: async (info) => {
      if (info.runRef?.kind !== "update") return;
      await supersedeUpdateRequest(info.runRef.id, supersededText(info.name, info.by, info.at), overrides.queue);
    },
  };
}

/**
 * (worker, update role, at start) The two BullMQ job schedulers this queue had
 * before the shared clock — order-update-active and order-update-ofd — leave
 * this machine's Redis, each with its next delayed job. Always: whether a
 * sweep runs is the switch in Settings, and whether this machine may run it
 * its .env (services/scheduleSettings.ts); neither is this queue's business
 * any more.
 */
export async function retireUpdateSchedulers(queue?: Queue<UpdateJobData>): Promise<string[]> {
  return retireLegacySchedulers(["update_active", "update_ofd"], queue ? { update: () => queue, invoice: () => queue } : undefined);
}

// ── update reaper ─────────────────────────────────────────────────────────────
// The twin of the cancel reaper. Each account is one fire-and-forget job
// (attempts:1); a request whose jobs are lost — worker restarted mid-delay, jobs
// removed on a transient failure, or a request created against a Redis this
// worker no longer reads (e.g. submitted while the dashboard still pointed at a
// now-gone AWS) — sits "queued"/"running" forever at 0/N with nothing to pick it
// up. This sweep finds such stranded requests (open accounts, quiet a while, no
// live job) and re-enqueues the missing per-account jobs; runOrderUpdateJob
// re-reads the account's units from the request doc, so a half-done run resumes.
// It takes only this system's requests: see reapCandidatesFilter.

let reapingUpdates = false;

/**
 * The requests the update reaper may take: open, quiet for UPDATE_REAP_STALE_MS, and made on this system. A request's
 * jobs live in the Redis of the system that made it. The two Macs share Atlas but not Redis, so without the systemId
 * one Mac would re-enqueue the other's quiet request into its own queue and both would run it.
 */
export function reapCandidatesFilter(now = Date.now(), systemId = config.systemId) {
  return { status: { $in: ["queued", "running"] }, systemId, updatedAt: { $lt: new Date(now - UPDATE_REAP_STALE_MS) } };
}

export async function reapStuckUpdates(): Promise<number> {
  if (reapingUpdates) return 0;
  reapingUpdates = true;
  try {
    const open = await OrderUpdateRequest.find(reapCandidatesFilter())
      .select("_id accounts.email accounts.status accounts.startedAt")
      .lean();
    if (!open.length) return 0;

    const queue = getUpdateQueue();
    const isStaleRunning = (status: string, startedAt?: Date | null) =>
      status === "running" && (!startedAt || Date.now() - new Date(startedAt).getTime() > STALE_ACCOUNT_MS);

    let requeued = 0;
    for (const r of open) {
      const requestId = String(r._id);
      const emails = new Set<string>();
      for (const a of r.accounts || []) if (a.status === "queued" || isStaleRunning(a.status, a.startedAt)) emails.add(a.email);

      for (const email of emails) {
        const jobId = `${requestId}-${email}`;
        // A live job (waiting/active/delayed) means it is already being handled.
        const existing = await queue.getJob(jobId).catch(() => null);
        if (existing) continue;
        await queue.add("update", { requestId, email }, { jobId }).catch(() => undefined);
        requeued++;
        console.log(`[update-reaper] re-enqueued stranded ${requestId.slice(0, 8)} / ${email}`);
      }
    }
    if (requeued) console.log(`[update-reaper] re-enqueued ${requeued} stranded update job(s) across ${open.length} open request(s)`);
    return requeued;
  } finally {
    reapingUpdates = false;
  }
}

/** Start the periodic update reaper (worker, update role only). Returns the timer. */
export function startUpdateReaper() {
  const t = setInterval(() => {
    void reapStuckUpdates().catch((e) => console.error("[update-reaper]", e instanceof Error ? e.message : e));
  }, UPDATE_REAP_EVERY_MS);
  t.unref();
  console.log(`[update-reaper] on — re-enqueues stranded updates every ${Math.round(UPDATE_REAP_EVERY_MS / 1000)}s (stranded after ${Math.round(UPDATE_REAP_STALE_MS / 60000)}m quiet)`);
  return t;
}
