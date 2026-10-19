import fs from "node:fs";
import path from "node:path";
import { DelayedError, type Job } from "bullmq";
import { Types } from "mongoose";
import { FlipkartCancel, itemIdOf, type CancelOutcome } from "../automation/FlipkartCancel.js";
import { fullOrderId } from "../automation/PaymentApiWatcher.js";
import { config } from "../config.js";
import { CancelRequest, type CancelUnitStatus, type ICancelUnit } from "../models/CancelRequest.js";
import { Order } from "../models/Order.js";
import type { APIRequestContext, Page } from "playwright";
import { OrderSyncSettings } from "../models/OrderSyncSettings.js";
import { getCancelQueue, type CancelJobData } from "../queue.js";
import { blockFlipkartLogout, desktopContext, launchStealthBrowser, restoreFlipkartSession } from "./browser.js";
import { lockAccount } from "./accountLock.js";
import { fetchOrderDetails, mapApiUnitToOrder, openOrderApiSession } from "./orderApi.js";
import { fallbackUnitId, saveMappedUnit, type OrderAccount } from "./orderFetch.js";
import { claimProxyForWorker } from "./proxyPool.js";
import { resolveLoggedInSession } from "./sessionStore.js";

/**
 * "Cancel Order Units" — the dashboard's popup (Unit IDs or Order IDs) and the
 * row-level Cancel both land here.
 *
 *   requestCancellation  resolve the typed ids to Order rows, reject what cannot
 *                        be cancelled, mark the rest queued, one job per account
 *   pullUnknownOrders    an Order ID that was never fetched, when the caller
 *                        names the Platform ID that placed it: read that one
 *                        order through Flipkart's order API (every unit) and
 *                        save it, so the cancel can go ahead without a Fetch
 *   runCancelJob         (worker) one browser per account, its units in turn,
 *                        each through FlipkartCancel, each result onto both the
 *                        CancelRequest and the Order row
 *
 * Eligibility is Flipkart's to decide (FlipkartCancel reads it from the order
 * page): we only refuse what our own record already shows as over — delivered,
 * cancelled, returned, refunded, failed — or already being cancelled.
 */

const TERMINAL = /deliver|cancel|return|refund|fail/i;
const IN_FLIGHT = new Set(["queued", "running"]);
/** Errors worth one more go in the same browser. */
const TRANSIENT = /timeout|timed out|net::|ECONN|ETIMEDOUT|EAI_AGAIN|socket hang up|proxy|Target (page|context|browser).*closed|has been closed|navigation/i;
/**
 * Hard cap on one unit in a tab. A renderer that stops answering (memory
 * pressure, a swapped-out Chrome) can make a Playwright call wait forever;
 * without this the tab, the job and one of the worker's slots hang for good —
 * which is how a 3 900-unit run ground to a halt on 2026-10-19 with six idle
 * Chromes and 750 units still queued.
 */
const UNIT_TIMEOUT_MS = 4 * 60 * 1000;
/** A unit "running" this long with no finish was abandoned by a hung or dead worker; pick it up again. */
const STALE_RUNNING_MS = 10 * 60 * 1000;

class UnitTimeoutError extends Error {}

function withTimeout<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout;
  return Promise.race([
    p.finally(() => clearTimeout(timer)),
    new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new UnitTimeoutError(`${what} did not finish in ${Math.round(ms / 60000)} min — page hung`)), ms);
    }),
  ]);
}
/** The account is busy (checkout, sync or another cancel) — try again after this. */
const LOCK_RETRY_MS = 20_000;
/** Unknown orders are read inside the request; keep that to seconds, not minutes. */
const MAX_PULLED_ORDERS = 50;

export interface CancelRequestInput {
  type: "unit" | "order";
  ids: string[];
  /** Platform ID (email) per Order ID, for orders never fetched into Order Units. */
  accounts?: Record<string, string>;
  /** Platform ID (email) for every unknown Order ID not named in `accounts`. */
  email?: string;
  /** Walk to REQUEST CANCELLATION and stop. For testing the path on a real unit. */
  dryRun?: boolean;
  /** Dashboard user submitting the request. */
  by?: string;
  /** The request this one retries the failed units of. */
  retryOf?: string;
}

/** Seconds one unit takes in the cancel browser, from measured runs. */
export const SECONDS_PER_UNIT = 6.5;
/** How many accounts the cancel worker drives at once when nothing is saved (worker.ts). */
export function cancelConcurrency() {
  const env = Number(process.env.CANCEL_CONCURRENCY);
  return Number.isFinite(env) && env > 0 ? Math.floor(env) : Math.max(1, config.workerConcurrency);
}

/**
 * Cancel speed. `windows` = Chrome windows at once = accounts cancelled in
 * parallel (the BullMQ concurrency; the worker follows changes within 10 s).
 * `tabs` = tabs per window = units of ONE account cancelled in parallel, each
 * tab its own FlipkartCancel on the same logged-in context. Units in flight =
 * windows × tabs; each window is a Chrome (~400 MB), so the caps are about
 * the machine, and tabs are capped low so one account never hammers Flipkart.
 */
export const SPEED_LIMITS = { windows: { min: 1, max: 6 }, tabs: { min: 1, max: 4 } } as const;

export interface CancelSpeed {
  windows: number;
  tabs: number;
  source: "ui" | "env";
  changedBy: string;
  changedAt: Date | null;
  limits: typeof SPEED_LIMITS;
  secondsPerUnit: number;
}

function clampSpeed(value: unknown, limit: { min: number; max: number }): number | null {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.max(limit.min, Math.min(limit.max, n));
}

export async function resolveCancelSpeed(userId: string): Promise<CancelSpeed> {
  const row = Types.ObjectId.isValid(userId) ? await OrderSyncSettings.findOne({ userId }).lean().catch(() => null) : null;
  const windows = clampSpeed(row?.cancelWindows, SPEED_LIMITS.windows);
  const tabs = clampSpeed(row?.cancelTabs, SPEED_LIMITS.tabs);
  return {
    windows: windows ?? Math.min(SPEED_LIMITS.windows.max, cancelConcurrency()),
    tabs: tabs ?? 1,
    source: windows || tabs ? "ui" : "env",
    changedBy: row?.cancelChangedBy || "",
    changedAt: row?.cancelChangedAt ?? null,
    limits: SPEED_LIMITS,
    secondsPerUnit: SECONDS_PER_UNIT,
  };
}

export async function setCancelSpeed(userId: string, input: { windows?: unknown; tabs?: unknown }, by: string): Promise<CancelSpeed> {
  const current = await resolveCancelSpeed(userId);
  const windows = input.windows === undefined ? current.windows : clampSpeed(input.windows, SPEED_LIMITS.windows);
  const tabs = input.tabs === undefined ? current.tabs : clampSpeed(input.tabs, SPEED_LIMITS.tabs);
  if (!windows || !tabs) throw new Error(`windows must be ${SPEED_LIMITS.windows.min}–${SPEED_LIMITS.windows.max} and tabs ${SPEED_LIMITS.tabs.min}–${SPEED_LIMITS.tabs.max}`);
  await OrderSyncSettings.updateOne(
    { userId },
    { $set: { cancelWindows: windows, cancelTabs: tabs, cancelChangedBy: by || "", cancelChangedAt: new Date() }, $setOnInsert: { userId } },
    { upsert: true }
  );
  console.log(`[cancel] speed ${current.windows}×${current.tabs} → ${windows}×${tabs} (windows × tabs) by ${by || "dashboard"}`);
  return resolveCancelSpeed(userId);
}

const NEEDS_EMAIL = "Not found in Order Units — add the Platform ID (email) that placed it, or fetch the account's orders first";

/**
 * Read the given orders from Flipkart through the named accounts and save
 * every unit of each, exactly as a Fetch would (same mapper, same row shape,
 * `applyCalendar: false` so the since-date window never drops one). Returns a
 * reason per order that could not be read; the rest are now in Order Units.
 */
async function pullUnknownOrders(userId: string, orderIds: string[], emailFor: (orderId: string) => string) {
  const errors = new Map<string, string>();
  const byEmail = new Map<string, string[]>();
  for (const orderId of orderIds) {
    const email = emailFor(orderId);
    if (!email) errors.set(orderId, NEEDS_EMAIL);
    else if (byEmail.size >= MAX_PULLED_ORDERS) errors.set(orderId, `At most ${MAX_PULLED_ORDERS} unfetched orders per request — fetch the account's orders instead`);
    else byEmail.set(email, [...(byEmail.get(email) || []), orderId]);
  }

  for (const [email, list] of byEmail) {
    const session = await resolveLoggedInSession(email);
    if (!session.ok) {
      for (const orderId of list) errors.set(orderId, `${email}: ${session.reason} — re-login this ID`);
      continue;
    }
    const account: OrderAccount = { id: new Types.ObjectId(session.platformId), email: session.email, cookies: session.cookies };
    let ctx: APIRequestContext | null = null;
    try {
      ctx = await openOrderApiSession(session.cookies);
      for (const orderId of list) {
        try {
          // The details page answers for any unit id of the order; the first
          // call (guessed id) tells us the real unit ids, then one call per unit
          // so each row carries its own tracking and money, as a Fetch does.
          const first = await fetchOrderDetails(ctx, orderId, fallbackUnitId(orderId));
          const unitIds = Object.keys(first.orderView.units || {});
          if (!unitIds.length) {
            errors.set(orderId, `${email}: Flipkart shows no units for this order on this Platform ID — is it the account that placed it?`);
            continue;
          }
          for (const unitId of unitIds) {
            const view = unitId === fallbackUnitId(orderId) ? first.orderView : (await fetchOrderDetails(ctx, orderId, unitId)).orderView;
            await saveMappedUnit(userId, account, mapApiUnitToOrder(view, unitId), { applyCalendar: false });
          }
          console.log(`[cancel] pulled ${orderId} (${unitIds.length} unit${unitIds.length === 1 ? "" : "s"}) via ${email} for cancellation`);
        } catch (err) {
          const raw = (err instanceof Error ? err.message : String(err)).split("\n")[0];
          // The details page comes back without an order view when this account
          // never placed that order — say that, not the widget names.
          const reason = /carried neither/.test(raw) ? "Flipkart shows no such order on this Platform ID — is it the account that placed it?" : raw.slice(0, 200);
          errors.set(orderId, `${email}: ${reason}`);
        }
      }
    } catch (err) {
      const reason = (err instanceof Error ? err.message : String(err)).split("\n")[0].slice(0, 200);
      for (const orderId of list) if (!errors.has(orderId)) errors.set(orderId, `${email}: ${reason}`);
    } finally {
      await ctx?.dispose().catch(() => undefined);
    }
  }
  return errors;
}

function normaliseIds(type: "unit" | "order", ids: string[]): string[] {
  const out = ids
    .map((raw) => String(raw || "").trim())
    .filter(Boolean)
    .map((id) => (type === "order" ? fullOrderId(id.toUpperCase()) : id.replace(/\D/g, "")))
    .filter(Boolean);
  return [...new Set(out)];
}

export async function requestCancellation(userId: string, input: CancelRequestInput) {
  const type = input.type === "unit" ? "unit" : "order";
  const ids = normaliseIds(type, input.ids);
  if (!ids.length) throw new Error(type === "unit" ? "Paste at least one Unit ID" : "Paste at least one Order ID");
  if (ids.length > 2000) throw new Error("At most 2000 ids per request");

  const select = "platform platform_email order_id unit_id item_id product_name status_key cancel_status";
  const find = (list: string[]) =>
    Order.find({ userId, ...(type === "unit" ? { unit_id: { $in: list } } : { order_id: { $in: list } }) })
      .select(select)
      .lean();
  let rows = await find(ids);

  // Order IDs we have never fetched: read them from Flipkart now when the
  // caller says which Platform ID placed them, then look again.
  const pullErrors = new Map<string, string>();
  if (type === "order") {
    const known = new Set(rows.map((r) => r.order_id));
    const unknown = ids.filter((id) => !known.has(id));
    if (unknown.length) {
      const named = new Map(Object.entries(input.accounts || {}).map(([k, v]) => [fullOrderId(String(k).trim().toUpperCase()), String(v || "").trim().toLowerCase()]));
      const fallback = String(input.email || "").trim().toLowerCase();
      const errors = await pullUnknownOrders(userId, unknown, (orderId) => named.get(orderId) || fallback);
      for (const [id, reason] of errors) pullErrors.set(id, reason);
      const pulled = unknown.filter((id) => !errors.has(id));
      if (pulled.length) rows = [...rows, ...(await find(pulled))];
    }
  }

  const { units, rejected } = classify(type, ids, rows, (id) => pullErrors.get(id));

  const request = await CancelRequest.create({
    userId: new Types.ObjectId(userId),
    type,
    input: ids,
    dryRun: Boolean(input.dryRun),
    createdBy: String(input.by || "").trim(),
    retryOf: String(input.retryOf || ""),
    status: units.length ? "queued" : "done",
    units,
    rejected,
    finishedAt: units.length ? null : new Date(),
  });
  const requestId = String(request._id);

  if (units.length) {
    await Order.updateMany(
      { userId, unit_id: { $in: units.map((u) => u.unitId) } },
      {
        $set: {
          cancel_status: "queued",
          cancel_request_id: requestId,
          cancel_requested_at: new Date(),
          cancel_requested_by: String(input.by || "").trim(),
          cancel_error: "",
        },
      }
    );
    const emails = [...new Set(units.map((u) => u.email))];
    const queue = getCancelQueue();
    await queue.addBulk(emails.map((email) => ({ name: "cancel", data: { requestId, email }, opts: { jobId: `${requestId}-${email}` } })));
  }

  return publicRequest(request.toObject());
}

type KnownRow = { platform?: string; platform_email: string; order_id: string; unit_id: string; item_id?: string; product_name?: string; status_key?: string; cancel_status?: string };

/** The typed ids against the Order rows: what will be queued and what is refused, with the reason. */
function classify(type: "unit" | "order", ids: string[], rows: KnownRow[], reasonFor: (id: string) => string | undefined) {
  const rejected: Array<{ id: string; reason: string }> = [];
  const units: ICancelUnit[] = [];
  for (const id of ids) {
    const matches = rows.filter((r) => (type === "unit" ? r.unit_id === id : r.order_id === id));
    if (!matches.length) {
      rejected.push({ id, reason: reasonFor(id) || (type === "order" ? NEEDS_EMAIL : "Not found in Order Units — fetch the account's orders first") });
      continue;
    }
    for (const r of matches) {
      const label = type === "order" ? `${id} / ${r.unit_id}` : id;
      if (String(r.platform || "FLIPKART").toUpperCase() !== "FLIPKART") {
        rejected.push({ id: label, reason: "Only Flipkart orders can be cancelled" });
      } else if (TERMINAL.test(String(r.status_key || ""))) {
        rejected.push({ id: label, reason: `Already ${r.status_key}` });
      } else if (IN_FLIGHT.has(String(r.cancel_status || ""))) {
        rejected.push({ id: label, reason: `Cancellation already ${r.cancel_status}` });
      } else {
        units.push({
          orderId: r.order_id,
          unitId: r.unit_id,
          itemId: String(r.item_id || "") || itemIdOf(r.unit_id),
          email: r.platform_email,
          productName: String(r.product_name || ""),
          status: "queued",
          message: "",
          startedAt: null,
          finishedAt: null,
        });
      }
    }
  }
  return { units, rejected };
}

/**
 * What a request would do, without doing it: nothing is queued and unknown
 * orders are not pulled. For the popup's confirmation line.
 */
export async function previewCancellation(userId: string, input: CancelRequestInput) {
  const type = input.type === "unit" ? "unit" : "order";
  const ids = normaliseIds(type, input.ids);
  if (!ids.length) throw new Error(type === "unit" ? "Paste at least one Unit ID" : "Paste at least one Order ID");
  if (ids.length > 2000) throw new Error("At most 2000 ids per request");
  const rows: KnownRow[] = await Order.find({ userId, ...(type === "unit" ? { unit_id: { $in: ids } } : { order_id: { $in: ids } }) })
    .select("platform platform_email order_id unit_id item_id product_name status_key cancel_status")
    .lean();
  const known = new Set(rows.map((r) => (type === "unit" ? r.unit_id : r.order_id)));
  const unknown = ids.filter((id) => !known.has(id));
  const named = new Map(Object.entries(input.accounts || {}).map(([k, v]) => [fullOrderId(String(k).trim().toUpperCase()), String(v || "").trim().toLowerCase()]));
  const fallback = String(input.email || "").trim().toLowerCase();
  const withEmail = type === "order" ? unknown.filter((id) => named.get(id) || fallback) : [];
  const { units, rejected } = classify(type, ids.filter((id) => known.has(id)), rows, () => undefined);
  const reasons: Record<string, number> = {};
  for (const r of rejected) reasons[r.reason] = (reasons[r.reason] || 0) + 1;
  const accounts = new Set(units.map((u) => u.email)).size;
  const speed = await resolveCancelSpeed(userId);
  return {
    type,
    ids: ids.length,
    units: units.length,
    orders: new Set(units.map((u) => u.orderId)).size,
    accounts,
    unknownWithEmail: withEmail.length,
    unknownNeedingEmail: unknown.length - withEmail.length,
    rejected: rejected.length,
    reasons,
    windows: speed.windows,
    tabs: speed.tabs,
    concurrency: speed.windows,
    // Accounts in parallel up to `windows`, each account's units in parallel up to `tabs`.
    estimateSeconds: Math.ceil((units.length * SECONDS_PER_UNIT) / Math.max(1, Math.min(accounts || 1, speed.windows) * speed.tabs)) + withEmail.length,
  };
}

/** A new request made of the failed units of an earlier one. Flipkart's refusals (not_cancellable) are not retried. */
export async function retryCancelRequest(userId: string, requestId: string, by: string) {
  if (!Types.ObjectId.isValid(requestId)) return null;
  const row = await CancelRequest.findOne({ _id: requestId, userId }).lean();
  if (!row) return null;
  const failed = row.units.filter((u) => u.status === "failed").map((u) => u.unitId);
  if (!failed.length) throw new Error("Nothing to retry — this request has no failed units");
  return requestCancellation(userId, { type: "unit", ids: failed, dryRun: row.dryRun, by, retryOf: requestId });
}

export async function getCancelRequest(userId: string, requestId: string) {
  if (!Types.ObjectId.isValid(requestId)) return null;
  const row = await CancelRequest.findOne({ _id: requestId, userId }).lean();
  return row ? publicRequest(row) : null;
}

export async function listCancelRequests(userId: string, limit = 20) {
  const rows = await CancelRequest.find({ userId }).sort({ createdAt: -1 }).limit(Math.min(100, Math.max(1, limit))).lean();
  return rows.map(publicRequest);
}

export function publicRequest(r: {
  _id: unknown;
  type: string;
  dryRun?: boolean;
  createdBy?: string;
  retryOf?: string;
  status: string;
  units: ICancelUnit[];
  rejected: Array<{ id: string; reason: string }>;
  createdAt?: Date;
  finishedAt?: Date | null;
}) {
  const count = (s: CancelUnitStatus) => r.units.filter((u) => u.status === s).length;
  return {
    id: String(r._id),
    type: r.type,
    dryRun: Boolean(r.dryRun),
    createdBy: r.createdBy || "",
    retryOf: r.retryOf || "",
    status: r.status,
    createdAt: r.createdAt ?? null,
    finishedAt: r.finishedAt ?? null,
    counts: {
      total: r.units.length,
      queued: count("queued"),
      running: count("running"),
      cancelled: count("cancelled"),
      notCancellable: count("not_cancellable"),
      failed: count("failed"),
      dryRun: count("dry_run"),
      rejected: r.rejected.length,
    },
    units: r.units,
    rejected: r.rejected,
  };
}

// ── worker side ──────────────────────────────────────────────────────────────

async function setUnit(requestId: string, unitId: string, patch: Partial<ICancelUnit>) {
  const set: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(patch)) set[`units.$[u].${k}`] = v;
  await CancelRequest.updateOne({ _id: requestId }, { $set: set }, { arrayFilters: [{ "u.unitId": unitId }] });
}

/** The Order row, in the shape the order sync writes a cancelled unit in, until the next sync refines it. */
async function applyToOrder(userId: Types.ObjectId, unitId: string, status: CancelUnitStatus, message: string) {
  if (status === "cancelled") {
    const row = await Order.findOne({ userId, unit_id: unitId }).select("status_key").lean();
    await Order.updateOne(
      { userId, unit_id: unitId },
      {
        $set: {
          cancel_status: "cancelled",
          cancel_error: "",
          status_key: "Cancelled",
          status_label: "Cancelled",
          status_reason: "Cancelled from the dashboard (Trade Flow)",
          cancelled_date: new Date(),
          cancelled_from_bae: true,
          cancelled_by_user: true,
          order_status_pre_cancellation: String(row?.status_key || "") || "Order Confirmed",
          delivery_otp: null,
        },
      }
    );
    return;
  }
  // A dry run leaves the row as it found it.
  const cancel_status = status === "dry_run" ? "" : status;
  await Order.updateOne({ userId, unit_id: unitId }, { $set: { cancel_status, cancel_error: status === "dry_run" ? "" : message } });
}

export async function runCancelJob(job: Job<CancelJobData>, token: string | undefined): Promise<void> {
  const { requestId, email } = job.data;
  const unlock = await lockAccount(email, `cancel:${requestId}:${config.workerId}`);
  if (!unlock) {
    // A checkout, sync or another cancel is on this account right now — come
    // back shortly rather than use the same login twice at once.
    await job.moveToDelayed(Date.now() + LOCK_RETRY_MS, token);
    throw new DelayedError();
  }

  const request = await CancelRequest.findById(requestId).lean();
  try {
    if (!request) return;
    // Queued units, plus ones a hung or dead worker left "running" long ago.
    const staleBefore = Date.now() - STALE_RUNNING_MS;
    const units = request.units.filter(
      (u) => u.email === email.toLowerCase() && (u.status === "queued" || (u.status === "running" && (!u.startedAt || new Date(u.startedAt).getTime() < staleBefore)))
    );
    if (!units.length) return;
    const stale = units.filter((u) => u.status === "running").length;
    if (stale) console.log(`[cancel ${requestId}] ${email} re-picking ${stale} unit(s) left running by an earlier worker`);
    await CancelRequest.updateOne({ _id: requestId, status: "queued" }, { $set: { status: "running" } });
    const log = (msg: string) => console.log(`[cancel ${requestId}] ${email} ${msg}`);

    const finish = async (u: ICancelUnit, status: CancelUnitStatus, message: string) => {
      await setUnit(requestId, u.unitId, { status, message, finishedAt: new Date() });
      await applyToOrder(request.userId, u.unitId, status, message);
      log(`${u.orderId}/${u.unitId} → ${status}${message ? `: ${message}` : ""}`);
    };

    const session = await resolveLoggedInSession(email);
    if (!session.ok) {
      for (const u of units) await finish(u, "failed", `Session: ${session.reason} — re-login this ID`);
      return;
    }

    const egress = await claimProxyForWorker(config.workerId).catch(() => null);
    const browser = await launchStealthBrowser({ headless: config.headless, proxy: egress?.proxy });
    const artifactDir = path.resolve("debug/cancel-runs", requestId);
    const speed = await resolveCancelSpeed(String(request.userId)).catch(() => null);
    const tabs = Math.max(1, Math.min(speed?.tabs || 1, units.length));
    try {
      const ctx = await desktopContext(browser);
      const first = await ctx.newPage();
      await blockFlipkartLogout(first);
      await restoreFlipkartSession(first, session.cookies);
      // More tabs share the context (and so the login); each runs its own flow.
      const pages: Page[] = [first];
      for (let i = 1; i < tabs; i++) {
        const p = await ctx.newPage();
        await blockFlipkartLogout(p);
        pages.push(p);
      }
      if (tabs > 1) log(`${units.length} units across ${tabs} tabs`);

      const pending = [...units];
      let sessionDead = false;
      const runTab = async (startPage: Page) => {
        let page = startPage;
        let flow = new FlipkartCancel(page, (_lvl, msg) => log(msg));
        for (;;) {
          const u = pending.shift();
          if (!u) return;
          if (sessionDead) {
            await finish(u, "failed", "Session expired — re-login this ID");
            continue;
          }
          await setUnit(requestId, u.unitId, { status: "running", startedAt: new Date() });
          await Order.updateOne({ userId: request.userId, unit_id: u.unitId }, { $set: { cancel_status: "running" } });
          let lastStep = "";
          try {
            // One automatic retry for network-class trouble (timeouts, proxy,
            // a closed target); never for an expired session or Flipkart's answer.
            // Every attempt is capped: a tab that stops answering fails the
            // unit instead of holding this slot forever.
            const attempt = () =>
              withTimeout(
                flow.cancelUnit({ orderId: u.orderId, unitId: u.unitId, itemId: u.itemId }, { dryRun: request.dryRun, onStep: async (name) => void (lastStep = name) }),
                UNIT_TIMEOUT_MS,
                `${u.orderId}/${u.unitId}`
              );
            let outcome: CancelOutcome;
            try {
              outcome = await attempt();
            } catch (first) {
              const text = first instanceof Error ? first.message : String(first);
              if (first instanceof UnitTimeoutError || !TRANSIENT.test(text) || /SESSION_EXPIRED/.test(text)) throw first;
              log(`${u.orderId}/${u.unitId} ${text.split("\n")[0].slice(0, 120)} — retrying once`);
              outcome = await attempt();
            }
            const note = "feeNote" in outcome && outcome.feeNote ? ` — fee: ${outcome.feeNote}` : "";
            if (outcome.kind === "cancelled") {
              await finish(u, "cancelled", `${outcome.message}${outcome.refundTat ? ` · ${outcome.refundTat}` : ""}${note}`);
            } else if (outcome.kind === "dry_run") {
              await finish(u, "dry_run", `${outcome.message}${note}`);
            } else {
              await finish(u, "not_cancellable", outcome.message);
            }
          } catch (err) {
            const message = err instanceof Error ? err.message.split("\n")[0] : String(err);
            // The page it broke on, for the next person reading this failure.
            fs.mkdirSync(artifactDir, { recursive: true });
            const base = path.join(artifactDir, `${u.unitId}-${lastStep || "start"}`);
            await withTimeout(page.screenshot({ path: `${base}.png`, fullPage: true }), 15_000, "screenshot").catch(() => undefined);
            await finish(u, "failed", message.slice(0, 500));
            // Every tab shares the login: once it is gone, the rest are failed, not attempted.
            if (/SESSION_EXPIRED/.test(message)) sessionDead = true;
            if (err instanceof UnitTimeoutError) {
              // The tab is not trusted any more: drop it and carry on in a fresh one.
              log(`${u.orderId}/${u.unitId} replacing the hung tab`);
              await withTimeout(page.close(), 10_000, "close tab").catch(() => undefined);
              page = await ctx.newPage();
              await blockFlipkartLogout(page);
              flow = new FlipkartCancel(page, (_lvl, msg) => log(msg));
            }
          }
        }
      };
      await Promise.all(pages.map((p) => runTab(p)));
    } finally {
      await browser.close().catch(() => undefined);
    }
  } finally {
    await unlock();
    // Done once no unit anywhere in the request is still waiting or running.
    await CancelRequest.updateOne(
      { _id: requestId, "units.status": { $nin: ["queued", "running"] } },
      { $set: { status: "done", finishedAt: new Date() } }
    );
  }
}
