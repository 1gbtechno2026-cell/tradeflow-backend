import { DelayedError, type Job } from "bullmq";
import { Types } from "mongoose";
import type { APIRequestContext, Browser, BrowserContext, Page } from "playwright";
import { config } from "../config.js";
import { isDestroyedContext } from "../lib/evalOnPage.js";
import { Order } from "../models/Order.js";
import { OrderSyncSettings } from "../models/OrderSyncSettings.js";
import { OrderUpdateRequest, type IOrderUpdateRequest, type IUpdateAccount, type UpdateSource } from "../models/OrderUpdateRequest.js";
import { getUpdateQueue, type UpdateJobData } from "../queue.js";
import { lockAccount } from "./accountLock.js";
import { blockFlipkartLogout, closeBrowser, flipkartLoginUrl, launchStealthContext, restoreFlipkartSession, sleep } from "./browser.js";
import { openOrderApiSession, OrderApiSessionError } from "./orderApi.js";
import { apiScrapeAndSave, scrapeAndSave, type OrderAccount } from "./orderFetch.js";
import { resolveOrderFetchMode } from "./orderSyncSettings.js";
import { resolveTargets, type UpdateTriggerInput } from "./orderUpdate.js";
import { resolveLoggedInSession, workspaceUserId } from "./sessionStore.js";

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
/** Active-unit sweep / Out-for-delivery recheck periods (ms). */
export const UPDATE_SWEEP_MS = Math.max(15 * 60 * 1000, Number(process.env.UPDATE_SWEEP_MS) || 6 * 60 * 60 * 1000);
export const UPDATE_OFD_SWEEP_MS = Math.max(15 * 60 * 1000, Number(process.env.UPDATE_OFD_SWEEP_MS) || 2 * 60 * 60 * 1000);
export const UPDATE_SCHEDULE_ENABLED = String(process.env.UPDATE_SCHEDULE ?? "1") !== "0";
/** Statuses the 2-hour recheck covers: the last mile, where the OTP and the delivery land. */
export const OFD_STATUSES = ["Out for delivery", "Dispatched"];
export const WINDOW_LIMITS = { min: 1, max: 6 } as const;

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

export async function createUpdateRequest(userId: string, input: UpdateTriggerInput, opts: { source?: UpdateSource; by?: string } = {}) {
  const targets = await resolveTargets(userId, input);
  if (!targets.length) throw new Error("No active order units to update. Fetch first, or paste a logged-in email + order id.");

  const byAccount = new Map<string, IUpdateAccount>();
  for (const t of targets) {
    const key = t.account.email.toLowerCase();
    const acc = byAccount.get(key) || {
      email: key,
      platformId: t.account.id,
      units: [],
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
    byAccount.set(key, acc);
  }
  const [mode, speed] = await Promise.all([resolveOrderFetchMode(userId), resolveUpdateWindows(userId)]);
  const request = await OrderUpdateRequest.create({
    userId: new Types.ObjectId(userId),
    source: opts.source || "manual",
    selectionMethod: input.selectionMethod || (input.orderIds?.length ? "order_ids" : input.emails?.length ? "emails" : input.csvRows?.length ? "csv" : input.orderStatus?.length ? "order_status" : "active"),
    createdBy: String(opts.by || "").trim(),
    status: "queued",
    mode: mode.mode,
    windows: speed.windows,
    accounts: [...byAccount.values()],
    logs: [{ time: new Date(), level: "info", message: `Update ${targets.length} unit(s) across ${byAccount.size} account(s) · ${speed.windows} window(s) · ${mode.mode === "api" ? "API mode (Chrome only on fallback)" : "Chrome"} · ${opts.source || "manual"}` }],
  });
  const requestId = String(request._id);
  await getUpdateQueue().addBulk([...byAccount.keys()].map((email) => ({ name: "update", data: { requestId, email }, opts: { jobId: `${requestId}-${email}` } })));
  console.log(`[orders-update] request ${requestId}: ${targets.length} unit(s) on ${byAccount.size} account(s) queued (${opts.source || "manual"})`);
  return publicUpdateRequest(request.toObject());
}

export async function cancelUpdateRequest(userId: string, requestId?: string) {
  const row = requestId
    ? await OrderUpdateRequest.findOne({ _id: requestId, userId })
    : await OrderUpdateRequest.findOne({ userId, status: { $in: ["queued", "running"] } }).sort({ createdAt: -1 });
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
  return { id: String(row._id), removed };
}

export async function getUpdateRequest(userId: string, requestId: string) {
  if (!Types.ObjectId.isValid(requestId)) return null;
  const row = await OrderUpdateRequest.findOne({ _id: requestId, userId }).lean();
  return row ? publicUpdateRequest(row) : null;
}

export async function listUpdateRequests(userId: string, limit = 20) {
  const rows = await OrderUpdateRequest.find({ userId }).sort({ createdAt: -1 }).limit(Math.min(100, Math.max(1, limit))).select("-accounts.units -logs").lean();
  return rows.map((r) => publicUpdateRequest(r));
}

export function publicUpdateRequest(r: Partial<IOrderUpdateRequest> & { _id: unknown }) {
  const accounts = r.accounts || [];
  const total = accounts.reduce((n, a) => n + (a.units?.length ?? 0), 0);
  const done = accounts.reduce((n, a) => n + a.done, 0);
  const failed = accounts.reduce((n, a) => n + a.failed, 0);
  const statusChanged = accounts.reduce((n, a) => n + (a.statusChanged || 0), 0);
  const otpNew = accounts.reduce((n, a) => n + (a.otpNew || 0), 0);
  const count = (s: IUpdateAccount["status"]) => accounts.filter((a) => a.status === s).length;
  return {
    id: String(r._id),
    source: r.source || "manual",
    selectionMethod: r.selectionMethod || "",
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
      left: Math.max(0, total - done - failed - accounts.filter((a) => a.status === "cancelled").reduce((n, a) => n + (a.units?.length ?? 0) - a.done - a.failed, 0)),
      statusChanged,
      otpNew,
    },
    accounts: accounts.map((a) => ({
      email: a.email,
      status: a.status,
      units: a.units?.length ?? 0,
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
 * in-process shape; this builds it from the newest request so nothing in
 * that panel changes.
 */
export async function legacyStatusView(userId: string) {
  const row = await OrderUpdateRequest.findOne({ userId }).sort({ createdAt: -1 }).lean();
  const [mode, speed] = await Promise.all([resolveOrderFetchMode(userId).catch(() => null), resolveUpdateWindows(userId).catch(() => null)]);
  if (!row) {
    return { running: false, cancelled: false, currentEmail: null, logs: [], total: 0, done: 0, failed: 0, skipped: 0, savedOrders: 0, windows: speed?.windows || 1, startedAt: null, finishedAt: null, elapsedMs: 0, mode: mode?.mode || config.orderFetchMode, modeSource: mode?.source || "env", requestId: "", source: "" };
  }
  const pub = publicUpdateRequest(row);
  const running = row.status === "queued" || row.status === "running";
  const active = row.accounts.find((a) => a.status === "running");
  const currentEmail = active ? `${active.email} ${active.done + active.failed}/${active.units.length}` : running ? `${pub.counts.accountsQueued} account(s) waiting for a window` : null;
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
  try {
    await OrderUpdateRequest.updateOne({ _id: requestId, status: "queued" }, { $set: { status: "running", startedAt: new Date() } });
    await setAccount(requestId, me, { status: "running", startedAt: new Date(), done: 0, failed: 0, statusChanged: 0, otpNew: 0, message: "" });

    const session = await resolveLoggedInSession(me);
    if (!session.ok) {
      await setAccount(requestId, me, { status: "failed", finishedAt: new Date(), message: `Session: ${session.reason} — re-login this ID` });
      await log("warn", `session: ${session.reason} — re-login this ID`);
      return;
    }
    const acct: OrderAccount = { id: new Types.ObjectId(session.platformId), email: session.email, cookies: session.cookies };
    const mode = request.mode;

    let page: Page | null = null;
    const openPage = async () => {
      if (page) return page;
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
    try {
      for (let n = 0; n < account.units.length; n++) {
        const fresh = await OrderUpdateRequest.findById(requestId).select("status").lean();
        if (!fresh || fresh.status === "cancelled") {
          await setAccount(requestId, me, { status: "cancelled", finishedAt: new Date(), message: `Stopped after ${n} unit(s)` });
          return;
        }
        const u = account.units[n];
        const card = { orderId: u.orderId, itemId: u.itemId, unitId: u.unitId, orderUrl: u.orderUrl, amount: "" };
        const before = await Order.findOne({ userId, unit_id: u.unitId }).select("status_key delivery_otp").lean();
        try {
          let result: Awaited<ReturnType<typeof scrapeAndSave>>;
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
            }
          } else {
            result = await scrapeAndSave(await openPage(), userId, acct, card, { applyCalendar: false });
          }
          const after = await Order.findOne({ userId, unit_id: u.unitId }).select("status_key delivery_otp").lean();
          const changed = Boolean(before && after && String(before.status_key || "") !== String(after.status_key || ""));
          const otp = Boolean(after?.delivery_otp && !before?.delivery_otp);
          await bumpAccount(requestId, me, { done: 1, ...(changed ? { statusChanged: 1 } : {}), ...(otp ? { otpNew: 1 } : {}) });
          if (changed || otp) {
            await log("info", `${u.orderId}/${u.unitId}: ${before?.status_key || "?"} → ${after?.status_key || result.statusKey || "?"}${otp ? ` · OTP ${after?.delivery_otp}` : ""}`);
          }
        } catch (err) {
          if (isDestroyedContext(err)) throw err;
          await bumpAccount(requestId, me, { failed: 1 });
          await log("warn", `${u.orderId}/${u.unitId}: ${err instanceof Error ? err.message.split("\n")[0].slice(0, 200) : err}`);
        }
      }
      const final = (await OrderUpdateRequest.findById(requestId).select("accounts").lean())?.accounts.find((a) => a.email === me);
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
    await setAccount(requestId, me, { status: "failed", finishedAt: new Date(), message });
    await log("error", `failed: ${message}`);
  } finally {
    if (launched) {
      const l = launched as { browser: Browser; context: BrowserContext };
      await closeBrowser(l.browser, l.context).catch(() => undefined);
    }
    await unlock();
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
    const req = await createUpdateRequest(userId, input, { source, by: "schedule" });
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
