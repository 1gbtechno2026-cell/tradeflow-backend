import { CheckoutJob } from "../models/CheckoutJob.js";
import { Order } from "../models/Order.js";
import { createUpdateRequest } from "./orderUpdateQueue.js";
import { CHAIN_AFTER_ORDER } from "./requestChains.js";
import { REQUEST_VALUE_MAX } from "./requestCommon.js";
import { openTasksAmong } from "./syncTaskLog.js";

/**
 * "After order" — the reference queues an Update for an order as soon as it is
 * placed (source `system-order`). Here the order sync (services/orderSync.ts)
 * is what brings a placed order into Order Units; once it has, the order is
 * noted here and ONE update request is queued for everything noted in the
 * same burst — a 200-order batch is one request (one job per account), not
 * two hundred.
 *
 *   quiet     the request is queued when no new order has been noted for
 *             AFTER_ORDER_QUIET_MS (default 90 s)
 *   ceiling   and never later than AFTER_ORDER_MAX_WAIT_MS after the first
 *             order of the burst (default 5 min), nor with more than 2000 orders
 *   skip      an order that already has a queued or running update task in any
 *             request is left to that request
 *   checkout  an order whose account still has a checkout queued or running
 *             waits for the next flush (one quiet period on): an update job
 *             holds the account's lock for every unit it reads, and the
 *             checkout jobs behind it would be put back 15 s at a time. After
 *             AFTER_ORDER_DEFER_MAX_MS (default 30 min since it was noted) it
 *             is queued anyway
 *
 * The notes live in this process only: the worker that synced the order is the
 * one that queues its update, on its own Redis. A worker that stops with
 * notes waiting loses them — the orders are in Order Units already, and the
 * 2-hour and 6-hour update schedules read them like any other.
 *
 *   CHAIN_AFTER_ORDER=0   switches it off (services/requestChains.ts)
 */

export interface AfterOrderDeps {
  enabled: boolean;
  quietMs: number;
  maxWaitMs: number;
  maxOrders: number;
  /** Of these orders, the ones with an update task queued or running in any request. */
  ordersInOpenUpdate: (userId: string, orderIds: string[]) => Promise<Set<string>>;
  /** Of these orders, the ones whose account still has a checkout queued or running. */
  ordersCheckingOut: (userId: string, orderIds: string[]) => Promise<Set<string>>;
  /** How long an order may wait for its account's checkouts before its update is queued anyway. */
  deferMaxMs: number;
  /** Queue the one update request. Answers its id and how many units it took. */
  createUpdate: (userId: string, orderIds: string[], batchIds: string[]) => Promise<{ id: string; units: number }>;
  log: (line: string) => void;
}

export interface AfterOrderFlush {
  status: "queued" | "skipped" | "failed";
  request_id: string;
  orders: number;
  already_in_update: number;
  /** Left for the next flush: their account still has a checkout queued or running. */
  deferred: number;
  reason: string;
}

/** Checkout jobs that still hold, or will take, their account's lock. */
const CHECKOUT_OPEN = ["queued", "running"];

export const afterOrderDefaults: AfterOrderDeps = {
  enabled: CHAIN_AFTER_ORDER,
  quietMs: Math.max(1000, Number(process.env.AFTER_ORDER_QUIET_MS) || 90_000),
  maxWaitMs: Math.max(1000, Number(process.env.AFTER_ORDER_MAX_WAIT_MS) || 5 * 60_000),
  maxOrders: REQUEST_VALUE_MAX,
  ordersInOpenUpdate: async (userId, orderIds) => new Set((await openTasksAmong(userId, "update", "orderId", orderIds)).map((t) => t.orderId)),
  ordersCheckingOut: async (userId, orderIds) => {
    const units = await Order.find({ userId, order_id: { $in: orderIds } }).select("order_id platform_email").lean();
    const emails = [...new Set(units.map((u) => String(u.platform_email || "").toLowerCase()).filter(Boolean))];
    if (!emails.length) return new Set();
    const busy = new Set(((await CheckoutJob.distinct("email", { userId: String(userId), email: { $in: emails }, status: { $in: CHECKOUT_OPEN } })) as string[]).map((e) => String(e).toLowerCase()));
    return new Set(units.filter((u) => busy.has(String(u.platform_email || "").toLowerCase())).map((u) => String(u.order_id)));
  },
  deferMaxMs: Math.max(60_000, Number(process.env.AFTER_ORDER_DEFER_MAX_MS) || 30 * 60_000),
  createUpdate: async (userId, orderIds, batchIds) => {
    const request = await createUpdateRequest(
      userId,
      { orderIds, selectionMethod: "order_ids" },
      {
        source: "system-order",
        by: "",
        params: { platform: "FLIPKART", selection_method: "order_ids", follows: "order", force: false, statuses_to_update: [], status_keys: [], email_ids_count: 0, order_ids_count: orderIds.length, order_email_pairs_count: 0, ...(batchIds.length ? { checkout_batch_ids: batchIds.slice(0, 20) } : {}) },
      }
    );
    return { id: request.id, units: request.counts.units };
  },
  log: (line) => console.log(`[chain] ${line}`),
};

interface Noted {
  /** The checkout batch that placed it ("" = none). */
  batchId: string;
  /** When it was first noted — a deferred order keeps it. */
  at: number;
}

interface Burst {
  orders: Map<string, Noted>;
  firstAt: number;
  timer: NodeJS.Timeout | null;
}

export function makeAfterOrderChain(overrides: Partial<AfterOrderDeps> = {}) {
  const deps = { ...afterOrderDefaults, ...overrides };
  const bursts = new Map<string, Burst>();

  const flush = async (userId: string): Promise<AfterOrderFlush | null> => {
    const burst = bursts.get(userId);
    if (!burst) return null;
    bursts.delete(userId);
    if (burst.timer) clearTimeout(burst.timer);
    const orderIds = [...burst.orders.keys()];
    const out: AfterOrderFlush = { status: "skipped", request_id: "", orders: 0, already_in_update: 0, deferred: 0, reason: "" };
    const later: Array<[string, Noted]> = [];
    try {
      const held = await deps.ordersInOpenUpdate(userId, orderIds);
      const free = orderIds.filter((id) => !held.has(id));
      out.already_in_update = orderIds.length - free.length;
      // Accounts still checking out: their orders wait for the next flush — unless they have waited long enough.
      const busy = free.length ? await deps.ordersCheckingOut(userId, free) : new Set<string>();
      const now = Date.now();
      const wanted = free.filter((id) => !busy.has(id) || now - (burst.orders.get(id)?.at ?? now) >= deps.deferMaxMs);
      for (const id of free) if (!wanted.includes(id)) later.push([id, burst.orders.get(id) || { batchId: "", at: now }]);
      out.deferred = later.length;
      out.orders = wanted.length;
      if (!wanted.length) out.reason = later.length ? "every account is still checking out — tried again after the next quiet period" : "every order is already in an open update";
      else {
        const batchIds = [...new Set(wanted.map((id) => burst.orders.get(id)?.batchId || "").filter(Boolean))];
        const made = await deps.createUpdate(userId, wanted, batchIds);
        out.status = "queued";
        out.request_id = made.id;
      }
    } catch (err) {
      out.reason = err instanceof Error ? err.message.split("\n")[0] : String(err);
      // Units that are not in Order Units, or whose account has logged out since, are nothing to update.
      out.status = /No active order units/.test(out.reason) ? "skipped" : "failed";
    }
    deps.log(`after order: ${orderIds.length} placed order(s) → update ${out.status}${out.request_id ? ` (request ${out.request_id}, ${out.orders} order(s))` : ""}${out.already_in_update ? `, ${out.already_in_update} already in an open update` : ""}${out.deferred ? `, ${out.deferred} wait for their account's checkouts` : ""}${out.reason ? ` — ${out.reason}` : ""}`);
    // The orders that wait join whatever is noted next — or make a burst of their own, flushed after one quiet period.
    for (const [id, noted] of later) remember(userId, id, noted);
    return out;
  };

  /** Note one order in the user's burst and set the burst's timer. */
  const remember = (userId: string, id: string, noted: Noted) => {
    const now = Date.now();
    const burst = bursts.get(userId) || { orders: new Map<string, Noted>(), firstAt: now, timer: null };
    const had = burst.orders.get(id);
    burst.orders.set(id, { batchId: noted.batchId || had?.batchId || "", at: Math.min(noted.at, had?.at ?? noted.at) });
    bursts.set(userId, burst);
    if (burst.timer) clearTimeout(burst.timer);
    if (burst.orders.size >= deps.maxOrders) {
      void flush(userId);
      return;
    }
    // Quiet for a while, or the ceiling since the burst's first order — whichever comes first.
    const wait = Math.max(0, Math.min(deps.quietMs, burst.firstAt + deps.maxWaitMs - now));
    burst.timer = setTimeout(() => void flush(userId), wait);
    burst.timer.unref();
  };

  return {
    /** An order our checkout placed is now in Order Units. Returns at once; the request is queued when the burst goes quiet. */
    note(userId: string, orderId: string, batchId = ""): void {
      const id = String(orderId || "").trim().toUpperCase();
      if (!deps.enabled || !userId || !id) return;
      remember(userId, id, { batchId, at: Date.now() });
    },
    /** Queue what is waiting now (the checks, and a worker that is asked to stop). */
    flush,
    /** How many orders are waiting for the burst to go quiet. */
    pending: (userId: string) => bursts.get(userId)?.orders.size || 0,
  };
}

/** The chain of this process. */
export const afterOrderChain = makeAfterOrderChain();
