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
import { terminalStatusFilter } from "./orderLifecycle.js";
import { assertIdleForUpdate, markUpdateRunning } from "./orderSyncLock.js";
import {
  cookiesFor,
  orderDetailsUrl,
  parseOrderId,
  fallbackUnitId,
  scrapeAndSave,
  type OrderAccount,
  type OrderUnitCard,
} from "./orderFetch.js";

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
}

export interface UpdateTriggerInput {
  emails?: string[];
  orderIds?: string[];
  orderStatus?: string[];
  csvRows?: Array<{ email?: string; order_id?: string }>;
  selectionMethod?: string;
}

type UpdateTarget = {
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
  return { ...rest, logs: [...job.logs] };
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

async function loggedInAccount(userId: string, email: string) {
  const row = await PlatformId.findOne({
    userId,
    email: email.toLowerCase(),
    status: "logged_in",
    sessionSavedAt: { $ne: null },
  }).sort({ sessionSavedAt: -1 });
  const cookies = cookiesFor(row);
  if (!row || !cookies.length) return null;
  return { id: row._id as Types.ObjectId, email: row.email, cookies };
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

async function resolveTargets(userId: string, input: UpdateTriggerInput): Promise<UpdateTarget[]> {
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
    for (const row of csvRows) {
      const email = String(row.email || "").toLowerCase();
      const orderId = parseOrderId(row.order_id || "") || String(row.order_id || "").toUpperCase();
      if (!email || !orderId) continue;
      const account = await loggedInAccount(userId, email);
      if (!account) continue;
      const units = await Order.find({ userId, platform_email: email, order_id: orderId }).lean();
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
    for (const idx of indexRows) {
      if (units.some((u) => u.unit_id === idx.unit_id)) continue;
      const account = await loggedInAccount(userId, idx.platform_email);
      if (!account) continue;
      const card = cardFromDoc(idx);
      if (card) push(account, card);
    }
    for (const unit of units) {
      const account = await loggedInAccount(userId, unit.platform_email);
      if (!account) continue;
      const card = cardFromDoc(unit);
      if (card) push(account, card);
    }
    return out;
  }

  const query: Record<string, unknown> = { userId };
  if (emails.length) query.platform_email = { $in: emails };
  if (statuses.length) query.status_key = { $in: statuses };
  else Object.assign(query, terminalStatusFilter());

  const units = await Order.find(query).lean();
  for (const unit of units) {
    const account = await loggedInAccount(userId, unit.platform_email);
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
  log(
    userId,
    "info",
    `Update ${targets.length} unit(s) across ${accountCount} account(s) · ${job.windows} Chrome window(s) · active statuses only unless IDs were listed`
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
    const launched = await launchStealthContext();
    job.liveBrowsers.push(launched);
    const page = await launched.context.newPage();
    await blockFlipkartLogout(page);
    try {
      while (!job.cancelled) {
        const i = next++;
        if (i >= lanes.length) break;
        const batch = lanes[i];
        const account = batch[0].account;
        try {
          await restoreFlipkartSession(page, account.cookies);
          await page.goto("https://www.flipkart.com/account/orders?link=home_orders", {
            waitUntil: "domcontentloaded",
            timeout: 30000,
          });
          if (flipkartLoginUrl(page.url())) {
            throw new Error("Flipkart session expired. Refresh this ID in platform id first.");
          }
          for (let n = 0; n < batch.length; n++) {
            if (job.cancelled) throw new Error("cancelled");
            const { card } = batch[n];
            job.currentEmail = `${account.email} ${n + 1}/${batch.length} ${card.orderId} ${card.unitId}`;
            try {
              const scraped = await scrapeAndSave(page, userId, account, card, { applyCalendar: false });
              job.done += 1;
              if (scraped.statusKey) job.savedOrders += 1;
              log(
                userId,
                "info",
                `${card.orderId} unit ${card.unitId}: ${scraped.statusKey || "updated"} · ${scraped.productName || "product"}`
              );
            } catch (err) {
              if (isDestroyedContext(err)) throw err;
              job.failed += 1;
              log(userId, "warn", `${card.orderId} unit ${card.unitId}: ${err instanceof Error ? err.message : err}`);
            }
          }
        } catch (err) {
          if (job.cancelled || (err instanceof Error && err.message === "cancelled")) break;
          job.failed += batch.length;
          log(userId, "error", `${account.email} failed: ${err instanceof Error ? err.message : err}`);
        }
        await sleep(400);
      }
    } finally {
      await closeBrowser(launched.browser, launched.context).catch(() => {});
      job.liveBrowsers = job.liveBrowsers.filter((row) => row.browser !== launched.browser);
    }
  }

  try {
    await Promise.all(Array.from({ length: job.windows }, () => lane()));
    if (!job.cancelled) log(userId, "info", `Update finished. ok ${job.done}, failed ${job.failed}`);
  } finally {
    await closeLiveBrowsers(userId);
    if (!job.cancelled) log(userId, "info", "Chrome closed.");
  }
}
