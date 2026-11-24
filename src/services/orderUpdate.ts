import os from "node:os";
import type { Browser, BrowserContext, Page } from "playwright";
import type { Types } from "mongoose";
import { PlatformId } from "../models/PlatformId.js";
import { Order } from "../models/Order.js";
import { OrderIndex } from "../models/OrderIndex.js";
import { config } from "../config.js";
import { isDestroyedContext } from "../lib/evalOnPage.js";
import {
  blockFlipkartLogout,
  closeBrowser,
  flipkartLoginUrl,
  launchStealthContext,
  restoreFlipkartSession,
  sleep,
} from "./browser.js";
import { inProgressUpdateFilter, terminalStatusFilter } from "./orderLifecycle.js";
import { assertIdleForUpdate, markUpdateRunning } from "./orderSyncLock.js";
import {
  apiScrapeAndSave,
  cookiesFor,
  orderDetailsUrl,
  parseOrderId,
  fallbackUnitId,
  scrapeAndSave,
  type OrderAccount,
  type OrderUnitCard,
} from "./orderFetch.js";
import { openOrderApiSession, OrderApiSessionError } from "./orderApi.js";
import { beginTrace, endTrace } from "./orderTrace.js";
import { resolveOrderFetchMode } from "./orderSyncSettings.js";
import type { APIRequestContext } from "playwright";

export interface UpdateLog {
  time: string;
  level: "info" | "warn" | "error";
  message: string;
}

interface UpdateJobState {
  running: boolean;
  cancelled: boolean;
  currentEmail: string | null;
  logs: UpdateLog[];
  total: number;
  done: number;
  failed: number;
  skipped: number;
  savedOrders: number;
  windows: number;
  startedAt: string | null;
  finishedAt: string | null;
  elapsedMs: number;
  liveBrowsers: Array<{ browser: Browser; context: BrowserContext }>;
  /** Effective mode for THIS run, resolved at start (dashboard setting, else env). */
  mode: "scrape" | "api";
}

export interface UpdateTriggerInput {
  emails?: string[];
  orderIds?: string[];
  orderStatus?: string[];
  /** Statuses that are a condition on the unit, not a status_key (the reference's detailed cancellations — services/startRequests.ts); ORed with orderStatus. */
  statusConditions?: Array<Record<string, unknown>>;
  csvRows?: Array<{ email?: string; order_id?: string }>;
  selectionMethod?: string;
  /**
   * With no status named: also take units stored as Returned. The reference's
   * default set counts Returned as in progress (a returned unit still moves to
   * Refunded), so the Update button's selections ask for it; the worker's own
   * 6 h / 2 h schedules do not and keep the rule they always had.
   */
  withReturned?: boolean;
}

export type UpdateTarget = {
  account: OrderAccount;
  card: OrderUnitCard;
};

const jobs = new Map<string, UpdateJobState>();

function windowCount(n: number) {
  const gb = os.totalmem() / 1024 ** 3;
  const byRam = gb >= 28 ? 5 : gb >= 14 ? 3 : 2;
  const env = config.fetchWindowConcurrency || Number(process.env.FETCH_WINDOW_CONCURRENCY || 0);
  const cap = env > 0 ? env : byRam;
  return Math.max(1, Math.min(n, cap, env > 0 ? env : 4));
}

function tickElapsed(job: UpdateJobState) {
  if (!job.startedAt) {
    job.elapsedMs = 0;
    return;
  }
  const end = job.finishedAt ? new Date(job.finishedAt).getTime() : Date.now();
  job.elapsedMs = Math.max(0, end - new Date(job.startedAt).getTime());
}

function state(userId: string): UpdateJobState {
  if (!jobs.has(userId)) {
    jobs.set(userId, {
      running: false,
      cancelled: false,
      currentEmail: null,
      logs: [],
      total: 0,
      done: 0,
      failed: 0,
      skipped: 0,
      savedOrders: 0,
      windows: 1,
      startedAt: null,
      finishedAt: null,
      elapsedMs: 0,
      liveBrowsers: [],
      mode: config.orderFetchMode,
    });
  }
  return jobs.get(userId)!;
}

function log(userId: string, level: UpdateLog["level"], message: string) {
  const job = state(userId);
  job.logs.push({ time: new Date().toISOString(), level, message });
  if (job.logs.length > 250) job.logs.splice(0, job.logs.length - 250);
  console.log(`[orders-update] ${message}`);
}

export function getOrderUpdateJob(userId: string) {
  const job = state(userId);
  tickElapsed(job);
  const { liveBrowsers, ...rest } = job;
  return { ...rest, mode: job.mode || config.orderFetchMode, logs: [...job.logs] };
}

async function closeLiveBrowsers(userId: string) {
  const job = state(userId);
  const live = job.liveBrowsers.splice(0, job.liveBrowsers.length);
  await Promise.all(live.map(({ browser, context }) => closeBrowser(browser, context)));
}

export async function stopOrderUpdate(userId: string) {
  const job = state(userId);
  job.cancelled = true;
  job.running = false;
  job.currentEmail = null;
  markUpdateRunning(userId, false);
  log(userId, "warn", "Force kill: stopping order update and killing Chrome");
  await closeLiveBrowsers(userId);
}

/** Accounts looked up per query: a saved session is ~19 KB of cookies. */
const ACCOUNT_LOOKUP_BATCH = 200;

/**
 * The logged-in Platform ID with a usable saved session for each of these
 * e-mails, keyed by lower-cased e-mail — the newest saved session where an
 * e-mail has several rows; an e-mail whose newest row has no cookies is left
 * out. One query per 200 e-mails: looked up per unit, a 1,400-unit update
 * spent 93 s here before its trigger could answer.
 */
async function loggedInAccounts(userId: string, emails: Iterable<string | null | undefined>): Promise<Map<string, OrderAccount>> {
  const wanted = [...new Set([...emails].map((e) => String(e || "").toLowerCase()).filter(Boolean))];
  type SessionRow = { _id: unknown; email: string; sessionSavedAt?: Date | null; sessionCookies?: unknown[]; sessionState?: { cookies?: unknown[] } };
  const newest = new Map<string, SessionRow>();
  for (let at = 0; at < wanted.length; at += ACCOUNT_LOOKUP_BATCH) {
    const rows = await PlatformId.find({ userId, email: { $in: wanted.slice(at, at + ACCOUNT_LOOKUP_BATCH) }, status: "logged_in", sessionSavedAt: { $ne: null } })
      .select("email sessionSavedAt sessionCookies sessionState.cookies")
      .lean();
    for (const row of rows) {
      const key = String(row.email || "").toLowerCase();
      const held = newest.get(key);
      if (!held || new Date(row.sessionSavedAt || 0).getTime() > new Date(held.sessionSavedAt || 0).getTime()) newest.set(key, row as SessionRow);
    }
  }
  const out = new Map<string, OrderAccount>();
  for (const [key, row] of newest) {
    const cookies = cookiesFor(row);
    if (cookies.length) out.set(key, { id: row._id as Types.ObjectId, email: row.email, cookies });
  }
  return out;
}

function cardFromDoc(row: {
  order_id?: string;
  item_id?: string;
  unit_id?: string;
  order_url?: string;
}): OrderUnitCard | null {
  const orderId = parseOrderId(row.order_id || row.order_url || "") || row.order_id || "";
  if (!orderId) return null;
  const unitId = fallbackUnitId(orderId, row.item_id || "", row.unit_id || "");
  return {
    orderId,
    itemId: row.item_id || "",
    unitId,
    orderUrl: row.order_url || orderDetailsUrl(orderId, row.item_id || "", unitId),
    amount: "",
  };
}

/** Shared with the worker-side update (orderUpdateQueue.ts): the same selection rules, whoever runs the refresh. */
export async function resolveTargets(userId: string, input: UpdateTriggerInput): Promise<UpdateTarget[]> {
  const emails = [...new Set((input.emails || []).map((e) => e.toLowerCase()).filter(Boolean))];
  const orderIds = [...new Set((input.orderIds || []).map((id) => parseOrderId(id) || id.toUpperCase()).filter(Boolean))];
  const statuses = (input.orderStatus || []).map((s) => s.trim()).filter(Boolean);
  const csvRows = input.csvRows || [];
  const out: UpdateTarget[] = [];
  const seen = new Set<string>();

  const push = (account: OrderAccount, card: OrderUnitCard) => {
    const key = `${account.id}:${card.unitId}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push({ account, card });
  };

  if (csvRows.length) {
    const pairs = csvRows
      .map((row) => ({ email: String(row.email || "").toLowerCase(), orderId: parseOrderId(row.order_id || "") || String(row.order_id || "").toUpperCase() }))
      .filter((p) => p.email && p.orderId);
    const accounts = await loggedInAccounts(userId, pairs.map((p) => p.email));
    // Every listed order in one read, then matched to its own e-mail — a pair names the account as well as the order.
    const saved = pairs.length ? await Order.find({ userId, order_id: { $in: [...new Set(pairs.map((p) => p.orderId))] } }).lean() : [];
    const unitsOf = new Map<string, typeof saved>();
    for (const unit of saved) {
      const key = `${unit.platform_email} ${unit.order_id}`;
      const list = unitsOf.get(key);
      if (list) list.push(unit);
      else unitsOf.set(key, [unit]);
    }
    for (const { email, orderId } of pairs) {
      const account = accounts.get(email);
      if (!account) continue;
      const units = unitsOf.get(`${email} ${orderId}`) || [];
      if (units.length) {
        for (const unit of units) {
          const card = cardFromDoc(unit);
          if (card) push(account, card);
        }
      } else {
        push(account, {
          orderId,
          itemId: "",
          unitId: fallbackUnitId(orderId),
          orderUrl: orderDetailsUrl(orderId),
          amount: "",
        });
      }
    }
    return out;
  }

  if (orderIds.length) {
    const units = await Order.find({ userId, order_id: { $in: orderIds } }).lean();
    const indexRows = await OrderIndex.find({ userId, order_id: { $in: orderIds } }).lean();
    const byOrder = new Map<string, typeof units>();
    for (const unit of units) {
      const list = byOrder.get(unit.order_id) || [];
      list.push(unit);
      byOrder.set(unit.order_id, list);
    }
    const accounts = await loggedInAccounts(userId, [...indexRows.map((i) => i.platform_email), ...units.map((u) => u.platform_email)]);
    const savedUnitIds = new Set(units.map((u) => u.unit_id));
    for (const idx of indexRows) {
      if (savedUnitIds.has(idx.unit_id)) continue;
      const account = accounts.get(String(idx.platform_email || "").toLowerCase());
      if (!account) continue;
      const card = cardFromDoc(idx);
      if (card) push(account, card);
    }
    for (const unit of units) {
      const account = accounts.get(String(unit.platform_email || "").toLowerCase());
      if (!account) continue;
      const card = cardFromDoc(unit);
      if (card) push(account, card);
    }
    return out;
  }

  const query: Record<string, unknown> = { userId };
  if (emails.length) query.platform_email = { $in: emails };
  const conditions = input.statusConditions || [];
  if (conditions.length) query.$or = [...(statuses.length ? [{ status_key: { $in: statuses } }] : []), ...conditions];
  else if (statuses.length) query.status_key = { $in: statuses };
  else Object.assign(query, input.withReturned ? inProgressUpdateFilter() : terminalStatusFilter());

  // Only what a target is made of: a status like Delivered matches thousands of units.
  const units = await Order.find(query).select("platform_email order_id item_id unit_id order_url").lean();
  const accounts = await loggedInAccounts(userId, units.map((u) => u.platform_email));
  for (const unit of units) {
    const account = accounts.get(String(unit.platform_email || "").toLowerCase());
    if (!account) continue;
    const card = cardFromDoc(unit);
    if (card) push(account, card);
  }
  return out;
}

export async function startOrderUpdate(userId: string, input: UpdateTriggerInput) {
  assertIdleForUpdate(userId);
  const job = state(userId);
  if (job.running) throw new Error("Order update is already running");

  const targets = await resolveTargets(userId, input);
  if (!targets.length) {
    throw new Error("No active order units to update. Fetch first, or paste a logged-in email + order id.");
  }

  const accountCount = new Set(targets.map((t) => String(t.account.id))).size;
  job.running = true;
  job.cancelled = false;
  job.done = 0;
  job.failed = 0;
  job.skipped = 0;
  job.savedOrders = 0;
  job.total = targets.length;
  job.currentEmail = null;
  job.windows = windowCount(accountCount);
  job.liveBrowsers = [];
  job.logs = [];
  job.startedAt = new Date().toISOString();
  job.finishedAt = null;
  job.elapsedMs = 0;
  markUpdateRunning(userId, true);
  const resolved = await resolveOrderFetchMode(userId);
  job.mode = resolved.mode;
  beginTrace(userId, "update", { targets: targets.length, accounts: accountCount, selection: input.selectionMethod || "", mode: job.mode, modeSource: resolved.source });
  log(
    userId,
    "info",
    `Update ${targets.length} unit(s) across ${accountCount} account(s) · ${job.windows} lane(s)${job.mode === "api" ? " · API mode (Chrome only on fallback)" : " · Chrome"} · mode from ${resolved.source === "ui" ? "the dashboard switch" : ".env"} · active statuses only unless IDs were listed`
  );

  void run(userId, targets).finally(() => {
    job.running = false;
    job.currentEmail = null;
    if (!job.finishedAt) job.finishedAt = new Date().toISOString();
    tickElapsed(job);
    markUpdateRunning(userId, false);
  });
}

async function run(userId: string, targets: UpdateTarget[]) {
  const job = state(userId);
  const groups = new Map<string, UpdateTarget[]>();
  for (const target of targets) {
    const key = String(target.account.id);
    const list = groups.get(key) || [];
    list.push(target);
    groups.set(key, list);
  }
  const lanes = [...groups.values()];
  let next = 0;

  async function lane() {
    if (job.cancelled) return;
    // Chrome on demand: scrape mode needs it for every account, API mode only
    // when a unit (or a whole session) has to fall back to the page.
    let launched: { browser: Browser; context: BrowserContext } | null = null;
    let page: Page | null = null;
    const getPage = async () => {
      if (page) return page;
      launched = await launchStealthContext();
      job.liveBrowsers.push(launched);
      page = await launched.context.newPage();
      await blockFlipkartLogout(page);
      return page;
    };
    const openPageFor = async (account: OrderAccount) => {
      const p = await getPage();
      await restoreFlipkartSession(p, account.cookies);
      await p.goto("https://www.flipkart.com/account/orders?link=home_orders", { waitUntil: "domcontentloaded", timeout: 30000 });
      if (flipkartLoginUrl(p.url())) throw new Error("Flipkart session expired. Refresh this ID in platform id first.");
      return p;
    };
    try {
      while (!job.cancelled) {
        const i = next++;
        if (i >= lanes.length) break;
        const batch = lanes[i];
        const account = batch[0].account;
        try {
          let api: APIRequestContext | null = null;
          let pageForAccount: Page | null = null;
          if (job.mode === "api") {
            try {
              api = await openOrderApiSession(account.cookies);
            } catch (err) {
              log(userId, "warn", `${account.email}: API session unavailable (${err instanceof Error ? err.message : err}) — using the page`);
            }
          }
          try {
            if (!api) pageForAccount = await openPageFor(account);
            for (let n = 0; n < batch.length; n++) {
              if (job.cancelled) throw new Error("cancelled");
              const { card } = batch[n];
              job.currentEmail = `${account.email} ${n + 1}/${batch.length} ${card.orderId} ${card.unitId}`;
              try {
                let result: Awaited<ReturnType<typeof scrapeAndSave>>;
                if (api) {
                  try {
                    result = await apiScrapeAndSave(api, userId, account, card, { applyCalendar: false });
                  } catch (apiErr) {
                    if (apiErr instanceof OrderApiSessionError) {
                      // The whole session is refused: finish this account on the page.
                      log(userId, "warn", `${account.email}: ${apiErr.message} — using the page from here`);
                      await api.dispose().catch(() => undefined);
                      api = null;
                    } else {
                      log(userId, "warn", `${card.orderId} unit ${card.unitId}: API read failed (${apiErr instanceof Error ? apiErr.message.slice(0, 140) : apiErr}) — falling back to the page`);
                    }
                    if (!pageForAccount) pageForAccount = await openPageFor(account);
                    result = await scrapeAndSave(pageForAccount, userId, account, card, { applyCalendar: false });
                  }
                } else {
                  if (!pageForAccount) pageForAccount = await openPageFor(account);
                  result = await scrapeAndSave(pageForAccount, userId, account, card, { applyCalendar: false });
                }
                job.done += 1;
                if (result.statusKey) job.savedOrders += 1;
                log(userId, "info", `${card.orderId} unit ${card.unitId}: ${result.statusKey || "updated"} · ${result.productName || "product"}${api ? " · API" : ""}`);
              } catch (err) {
                if (isDestroyedContext(err)) throw err;
                job.failed += 1;
                log(userId, "warn", `${card.orderId} unit ${card.unitId}: ${err instanceof Error ? err.message : err}`);
              }
            }
          } finally {
            await api?.dispose().catch(() => undefined);
          }
        } catch (err) {
          if (job.cancelled || (err instanceof Error && err.message === "cancelled")) break;
          job.failed += batch.length;
          log(userId, "error", `${account.email} failed: ${err instanceof Error ? err.message : err}`);
        }
        await sleep(400);
      }
    } finally {
      if (launched) {
        const l = launched as { browser: Browser; context: BrowserContext };
        await closeBrowser(l.browser, l.context).catch(() => {});
        job.liveBrowsers = job.liveBrowsers.filter((row) => row.browser !== l.browser);
      }
    }
  }

  try {
    await Promise.all(Array.from({ length: job.windows }, () => lane()));
    if (!job.cancelled) log(userId, "info", `Update finished. ok ${job.done}, failed ${job.failed}`);
  } finally {
    endTrace(userId, { done: job.done, failed: job.failed, total: job.total, cancelled: job.cancelled });
    await closeLiveBrowsers(userId);
    if (!job.cancelled) log(userId, "info", "Chrome closed.");
  }
}
