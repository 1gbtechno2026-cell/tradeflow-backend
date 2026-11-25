import { DelayedError, type Job } from "bullmq";
import { Types, type Model } from "mongoose";
import type { APIRequestContext, Browser, BrowserContext, Page } from "playwright";
import { config } from "../config.js";
import { isDestroyedContext } from "../lib/evalOnPage.js";
import { Order } from "../models/Order.js";
import { OrderSyncSettings } from "../models/OrderSyncSettings.js";
import { markSessionEligibleForRelogin } from "./sessionEligibility.js";
import { OrderUpdateRequest, type IOrderUpdateRequest, type IUpdateAccount, type UpdateSource } from "../models/OrderUpdateRequest.js";
import { getUpdateQueue, type UpdateJobData } from "../queue.js";
import { lockAccount } from "./accountLock.js";
import { blockFlipkartLogout, closeBrowser, flipkartLoginUrl, launchStealthContext, restoreFlipkartSession, sleep } from "./browser.js";
import { openOrderApiSession, OrderApiSessionError } from "./orderApi.js";
import { apiScrapeAndSave, scrapeAndSave, type OrderAccount } from "./orderFetch.js";
import { resolveOrderFetchMode } from "./orderSyncSettings.js";
import { DEFAULT_UPDATE_STATUSES, stageOf } from "./orderLifecycle.js";
import { resolveTargets, type UpdateTriggerInput } from "./orderUpdate.js";
import { recordUnitChanges, snapshotUnit } from "./orderUnitEvents.js";
import { resolveLoggedInSession, workspaceUserId } from "./sessionStore.js";
import { noteDeliveredOrder, queueInvoicesAfterUpdate } from "./requestChains.js";
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
 *   runUpdateSweep        (worker, scheduler tick) every 6 h all active units,
 *                         every 2 h the units Out for delivery — skipped while
 *                         a sweep of that kind is still open
 *   legacyStatusView      the shape the dashboard's status panel already polls
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
/** Active-unit sweep / Out-for-delivery recheck periods (ms). */
export const UPDATE_SWEEP_MS = Math.max(15 * 60 * 1000, Number(process.env.UPDATE_SWEEP_MS) || 6 * 60 * 60 * 1000);
export const UPDATE_OFD_SWEEP_MS = Math.max(15 * 60 * 1000, Number(process.env.UPDATE_OFD_SWEEP_MS) || 2 * 60 * 60 * 1000);
export const UPDATE_SCHEDULE_ENABLED = String(process.env.UPDATE_SCHEDULE ?? "1") !== "0";
/** Statuses the 2-hour recheck covers: the last mile, where the OTP and the delivery land. */
export const OFD_STATUSES = ["Out for delivery", "Dispatched"];
/** A unit can only carry a delivery OTP while it is in one of these stages. */
const OFD_RE = /out for delivery|dispatched/i;
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
  opts: { source?: UpdateSource; by?: string; params?: Record<string, unknown>; parentId?: string; units?: PreparedUpdateUnit[] } = {}
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
    createdBy: String(opts.by || "").trim(),
    status: "queued",
    mode: mode.mode,
    windows: speed.windows,
    accounts: [...byAccount.values()],
    logs: [{ time: new Date(), level: "info", message: `Update ${taken.size} unit(s) across ${byAccount.size} account(s) · ${speed.windows} window(s) · ${mode.mode === "api" ? "API mode (Chrome only on fallback)" : "Chrome"} · ${opts.source || "manual"}` }],
  });
  const requestId = String(request._id);
  // The task log: a row per unit, queued — written before a job can take one (services/syncTaskLog.ts).
  await openRequestTasksQuietly("update", request.toObject());
  await getUpdateQueue().addBulk([...byAccount.keys()].map((email) => ({ name: "update", data: { requestId, email }, opts: { jobId: `${requestId}-${email}` } })));
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

/** Done once every account is settled — the last job to finish closes the request. */
async function settleRequest(requestId: Types.ObjectId | string) {
  const res = await OrderUpdateRequest.updateOne(
    { _id: requestId, status: { $in: ["queued", "running"] }, "accounts.status": { $nin: ["queued", "running"] } },
    { $set: { status: "done", finishedAt: new Date() } }
  );
  if (res.modifiedCount) {
    const row = await OrderUpdateRequest.findById(requestId).select("-accounts.units").lean();
    if (row) {
      const pub = publicUpdateRequest(row);
      await pushLog(requestId, "info", `Update finished. ok ${pub.counts.done}, failed ${pub.counts.failed}, status changed ${pub.counts.statusChanged}, new OTP ${pub.counts.otpNew}`);
    }
    // The request has ended: its task rows are brought in line with it once, whole.
    await syncRequestTasksQuietly("update", String(requestId));
    // The reference's "Delivered check": one invoice fetch for the orders this update saw arrive at Delivered.
    await queueInvoicesAfterUpdate(String(requestId)).catch((err) => console.warn(`[chain] invoice fetch after update ${String(requestId).slice(-8)} failed: ${err instanceof Error ? err.message.split("\n")[0] : err}`));
  }
}

export async function runOrderUpdateJob(job: Job<UpdateJobData>, token: string | undefined): Promise<void> {
  if (job.name === "sweep") {
    await runUpdateSweep(job.data.sweep === "ofd" ? "ofd" : "active");
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

  const unlock = await lockAccount(me, `update:${requestId}:${config.workerId}`);
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
  try {
    await OrderUpdateRequest.updateOne({ _id: requestId, status: "queued" }, { $set: { status: "running", startedAt: new Date() } });
    await setAccount(requestId, me, { status: "running", startedAt: new Date(), done: 0, failed: 0, statusChanged: 0, otpNew: 0, message: "" });

    const session = await resolveLoggedInSession(me);
    if (!session.ok) {
      ended = { status: "failed", message: `Session: ${session.reason} — re-login this ID` };
      await setAccount(requestId, me, { status: "failed", finishedAt: new Date(), message: `Session: ${session.reason} — re-login this ID` });
      await markSessionEligibleForRelogin({ userId: request.userId, platformId: account.platformId, email: me, reason: `Update: ${session.reason} — re-login this ID`, via: "worker", requestId });
      await log("warn", `session: ${session.reason} — re-login this ID`);
      return;
    }
    const acct: OrderAccount = { id: new Types.ObjectId(session.platformId), email: session.email, cookies: session.cookies };
    const mode = request.mode;

    let page: Page | null = null;
    const openPage = async () => {
      if (page) return page;
      // One browser per account, and no more than CHROME_GATE_MAX across all windows.
      // Guard the acquire so a retry after a failed launch never takes a 2nd slot.
      if (!chromeHeld) {
        await acquireChrome();
        chromeHeld = true;
      }
      launched = await launchStealthContext({ headless: config.headless });
      page = await launched.context.newPage();
      await blockFlipkartLogout(page);
      await restoreFlipkartSession(page, acct.cookies);
      await page.goto("https://www.flipkart.com/account/orders?link=home_orders", { waitUntil: "domcontentloaded", timeout: 30_000 });
      if (flipkartLoginUrl(page.url())) throw new Error("Flipkart session expired. Refresh this ID in platform id first.");
      return page;
    };

    let api: APIRequestContext | null = null;
    if (mode === "api") {
      try {
        api = await openOrderApiSession(acct.cookies);
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
              result = await apiScrapeAndSave(api, userId, acct, card, { applyCalendar: false });
            } catch (apiErr) {
              if (apiErr instanceof OrderApiSessionError) {
                await log("warn", `${apiErr.message} — using the page from here`);
                await api.dispose().catch(() => undefined);
                api = null;
                via = "page";
              } else {
                await log("warn", `${u.orderId}/${u.unitId}: API read failed (${apiErr instanceof Error ? apiErr.message.slice(0, 140) : apiErr}) — page`);
              }
              result = await scrapeAndSave(await openPage(), userId, acct, card, { applyCalendar: false });
              usedPage = true;
            }
          } else {
            result = await scrapeAndSave(await openPage(), userId, acct, card, { applyCalendar: false });
            usedPage = true;
          }
          const after = await snapshotUnit(userId, u.unitId);
          // Read over HTTP, out for delivery, still no OTP → the OTP is page-only here.
          // Defer to Phase 2 rather than opening Chrome mid-loop.
          if (!usedPage && after && OFD_RE.test(after.status_key || "") && !after.delivery_otp) {
            needsScrape.push(u);
          }
          // Stage changes (not a vocabulary swap between the API's "Approved" and the page's
          // "Order Confirmed"), new OTPs and address changes become order_unit_events rows
          // — the dashboard's alerts read those.
          const changes = await recordUnitChanges(userId, u.unitId, before, after, requestId);
          if (changes.stageChanged && stageOf(after?.status_key) === "delivered") await noteDeliveredOrder(requestId, u.orderId);
          await bumpAccount(requestId, me, { done: 1, ...(changes.stageChanged ? { statusChanged: 1 } : {}), ...(changes.otpNew ? { otpNew: 1 } : {}) });
          taskLog.mark(requestId, taskKey, { status: "success" });
          if (changes.stageChanged || changes.otpNew || changes.addressChanged) {
            await log(
              "info",
              `${u.orderId}/${u.unitId}: ${before?.status_key || "?"} → ${after?.status_key || result.statusKey || "?"}${changes.otpNew ? ` · OTP ${after?.delivery_otp}` : ""}${changes.addressChanged ? ` · ADDRESS CHANGED → ${after?.shipping_address?.text || "?"}` : ""}`
            );
          }
        } catch (err) {
          if (isDestroyedContext(err)) throw err;
          await bumpAccount(requestId, me, { failed: 1 });
          const text = err instanceof Error ? err.message.split("\n")[0] : String(err);
          taskLog.mark(requestId, taskKey, { status: "failed", message: text });
          if (/gave no data/.test(text)) noData++;
          await log("warn", `${u.orderId}/${u.unitId}: ${text.slice(0, 200)}`);
        }
      }
      // ── Phase 2 — surgical scrape ───────────────────────────────────────────
      // Only OFD units the API couldn't give an OTP for. One browser (gated), each
      // unit already counted as `done` in Phase 1, so here we pick up only the OTP
      // (and any stage move the page reveals) without re-counting.
      if (needsScrape.length) {
        await log("info", `${needsScrape.length} out-for-delivery unit(s) without an API OTP — scraping those for OTP`);
        for (const u of needsScrape) {
          const fresh = await OrderUpdateRequest.findById(requestId).select("status").lean();
          if (!fresh || fresh.status === "cancelled") break;
          const card = { orderId: u.orderId, itemId: u.itemId, unitId: u.unitId, orderUrl: u.orderUrl, amount: "" };
          try {
            const before = await snapshotUnit(userId, u.unitId);
            await scrapeAndSave(await openPage(), userId, acct, card, { applyCalendar: false });
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
            await log("warn", `${u.orderId}/${u.unitId}: OTP scrape failed (${err instanceof Error ? err.message.split("\n")[0].slice(0, 160) : err})`);
          }
        }
      }

      const final = (await OrderUpdateRequest.findById(requestId).select("accounts").lean())?.accounts.find((a) => a.email === me);
      // Every unit's page rendered nothing and the API refused the session:
      // the login is the problem, not the orders. Mark the Platform ID's last
      // check as an error (Session Health → Suspect) and tell the dashboard.
      if (noData > 0 && (final?.done ?? 0) === 0 && noData === account.units.length) {
        const reason = `Update: ${noData} order page(s) rendered nothing and the API refused the session — re-login this ID`;
        await markSessionEligibleForRelogin({ userId: request.userId, platformId: acct.id, email: me, reason, via, requestId });
        await log("warn", `every order page rendered nothing (${noData}) — Platform ID set eligible for re-login`);
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
    if (launched) {
      const l = launched as { browser: Browser; context: BrowserContext };
      await closeBrowser(l.browser, l.context).catch(() => undefined);
    }
    if (chromeHeld) releaseChrome();
    await unlock();
    if (ended) await closeUpdateAccountRows(requestId, me, ended);
    await settleRequest(requestId);
    await sleep(300);
  }
}

// ── schedules ───────────────────────────────────────────────────────────────

/**
 * One tick of a schedule. The workspace is single (TRADE_FLOW_USER_ID); a tick
 * is skipped while an earlier request of the same source is still open, so a
 * slow night never stacks sweeps on top of each other.
 */
export async function runUpdateSweep(kind: "active" | "ofd"): Promise<void> {
  const userId = workspaceUserId();
  const source: UpdateSource = kind === "ofd" ? "ofd" : "schedule";
  const open = await OrderUpdateRequest.findOne({ userId, source, status: { $in: ["queued", "running"] } }).select("_id createdAt").lean();
  if (open) {
    console.log(`[orders-update] ${kind} sweep skipped — request ${String(open._id)} from ${open.createdAt?.toISOString()} still open`);
    return;
  }
  try {
    const input: UpdateTriggerInput = kind === "ofd" ? { orderStatus: OFD_STATUSES, selectionMethod: "schedule_ofd" } : { selectionMethod: "schedule_active" };
    const req = await createUpdateRequest(userId, input, {
      source,
      by: "schedule",
      // "All active" names no status: it records the in-progress list it stands for — the schedule's own rule, which leaves Returned out.
      params: { platform: "FLIPKART", selection_method: input.selectionMethod, force: false, statuses_to_update: input.orderStatus || DEFAULT_UPDATE_STATUSES.filter((s) => s !== "Returned"), status_keys: input.orderStatus || [], email_ids_count: 0, order_ids_count: 0, order_email_pairs_count: 0 },
    });
    console.log(`[orders-update] ${kind} sweep → request ${req.id}: ${req.counts.units} unit(s) on ${req.counts.accounts} account(s)`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (/No active order units/.test(msg)) console.log(`[orders-update] ${kind} sweep: nothing to refresh`);
    else console.warn(`[orders-update] ${kind} sweep failed: ${msg}`);
  }
}

/** Idempotent: one schedule each in Redis however many workers run. */
export async function ensureUpdateSchedules(): Promise<void> {
  const queue = getUpdateQueue();
  if (!UPDATE_SCHEDULE_ENABLED) {
    await queue.removeJobScheduler("order-update-active").catch(() => undefined);
    await queue.removeJobScheduler("order-update-ofd").catch(() => undefined);
    console.log("[orders-update] schedules disabled (UPDATE_SCHEDULE=0)");
    return;
  }
  await queue.upsertJobScheduler("order-update-active", { every: UPDATE_SWEEP_MS }, { name: "sweep", data: { requestId: "", email: "", sweep: "active" }, opts: { attempts: 1 } });
  await queue.upsertJobScheduler("order-update-ofd", { every: UPDATE_OFD_SWEEP_MS }, { name: "sweep", data: { requestId: "", email: "", sweep: "ofd" }, opts: { attempts: 1 } });
  console.log(`[orders-update] schedules: all active units every ${Math.round(UPDATE_SWEEP_MS / 60000)} min, Out for delivery every ${Math.round(UPDATE_OFD_SWEEP_MS / 60000)} min`);
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

let reapingUpdates = false;

export async function reapStuckUpdates(): Promise<number> {
  if (reapingUpdates) return 0;
  reapingUpdates = true;
  try {
    const staleBefore = new Date(Date.now() - UPDATE_REAP_STALE_MS);
    const open = await OrderUpdateRequest.find({ status: { $in: ["queued", "running"] }, updatedAt: { $lt: staleBefore } })
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
