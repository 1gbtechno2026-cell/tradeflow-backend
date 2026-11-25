import type { Model } from "mongoose";
import { Order } from "../models/Order.js";
import { OrderUpdateRequest, type IOrderUpdateRequest } from "../models/OrderUpdateRequest.js";
import { createInvoiceRequest } from "./invoiceRequests.js";
import { distinct, REQUEST_VALUE_MAX, RequestInputError } from "./requestCommon.js";

/**
 * The requests nobody presses — the reference's "How the pieces chain":
 *
 *   1. nightly fetch → update        services/startRequests.ts queueUpdateAfterFetch (step 4)
 *   2. After order → update          services/afterOrderChain.ts — source "system-order"
 *   3. Delivered check → invoices    here — source "system-update"
 *   4. nightly invoice fetch         services/invoiceSweep.ts — every run of the 3-hourly
 *                                    sweep is a request with source "system-cron"
 *
 * "Order email" (source "system-email": an update when a platform e-mail about
 * an order arrives) has no counterpart here — nothing reads order e-mails. It
 * is a choice of the Source filter that matches no request.
 *
 * Each of 2-4 has its own switch, on unless set to 0:
 *
 *   CHAIN_AFTER_ORDER=0       no update after a placed order
 *   CHAIN_DELIVERED_CHECK=0   no invoice fetch when an update sees Delivered
 *   CHAIN_INVOICE_SWEEP=0     the sweep runs as it always did, outside the request log
 *
 * Two machines share this database and not their Redis. A chain only ever
 * reacts to work its own process did — the sync job this worker ran, the
 * update request this worker closed, the sweep this worker ran — and queues
 * on that process's own Redis, so the other machine never sees a request it
 * cannot run.
 */
const switchedOn = (name: string) => !["0", "off", "false", "no"].includes(String(process.env[name] ?? "1").trim().toLowerCase());

export const CHAIN_AFTER_ORDER = switchedOn("CHAIN_AFTER_ORDER");
export const CHAIN_DELIVERED_CHECK = switchedOn("CHAIN_DELIVERED_CHECK");
export const CHAIN_INVOICE_SWEEP = switchedOn("CHAIN_INVOICE_SWEEP");

// ── Delivered check: an update saw units arrive at Delivered → one invoice fetch ──

export interface DeliveredCheckDeps {
  enabled: boolean;
  Update: Model<IOrderUpdateRequest>;
  /** Of these orders, the ones that already have a stored invoice. */
  ordersWithInvoice: (userId: string, orderIds: string[]) => Promise<string[]>;
  createInvoice: typeof createInvoiceRequest;
}

export const deliveredCheckDefaults: DeliveredCheckDeps = {
  enabled: CHAIN_DELIVERED_CHECK,
  Update: OrderUpdateRequest,
  ordersWithInvoice: async (userId, orderIds) => (await Order.distinct("order_id", { userId, order_id: { $in: orderIds }, invoice_s3_key: { $nin: [null, ""] } })) as string[],
  createInvoice: createInvoiceRequest,
};

/**
 * (update worker) This unit's stage has just changed to Delivered: its order
 * is noted on the update request. Nothing is queued here — the request's end
 * queues one invoice fetch for all of them. Never fails the unit.
 */
export async function noteDeliveredOrder(requestId: string, orderId: string, overrides: Partial<DeliveredCheckDeps> = {}): Promise<void> {
  const deps = { ...deliveredCheckDefaults, ...overrides };
  if (!deps.enabled || !orderId) return;
  await deps.Update.updateOne({ _id: requestId }, { $addToSet: { deliveredOrderIds: orderId } }).catch((err) => console.warn(`[chain] delivered ${orderId} not noted on update ${requestId.slice(-8)}: ${err instanceof Error ? err.message.split("\n")[0] : err}`));
}

export interface DeliveredCheckOutcome {
  status: "off" | "nothing" | "queued" | "skipped" | "failed";
  /** The invoice requests made (one, unless the update saw more than 2000 orders delivered). */
  request_ids: string[];
  orders: number;
  already_stored: number;
  reason: string;
}

/**
 * (update worker) The update request has just finished: ONE invoice fetch for
 * the orders it saw arrive at Delivered that have no stored invoice — source
 * "system-update", parent = the update. Never one request per order. Orders
 * another open invoice request already holds are left to it (the invoice
 * request's own rule when it is not forced). What happened is written on the
 * update (`params.invoice_fetch`), and the mark is taken before anything is
 * queued, so two jobs ending the request together queue once.
 */
export async function queueInvoicesAfterUpdate(requestId: string, overrides: Partial<DeliveredCheckDeps> = {}): Promise<DeliveredCheckOutcome> {
  const deps = { ...deliveredCheckDefaults, ...overrides };
  const out: DeliveredCheckOutcome = { status: "nothing", request_ids: [], orders: 0, already_stored: 0, reason: "" };
  if (!deps.enabled) return { ...out, status: "off" };
  const row = await deps.Update.findById(requestId).select("userId status deliveredOrderIds").lean();
  const seen = distinct((row?.deliveredOrderIds || []).map((id) => String(id).toUpperCase()).filter(Boolean));
  if (!row || !seen.length) return out;
  // Only a request that ran to its end: one that was stopped leaves its orders to the sweep.
  if (row.status !== "done") return { ...out, status: "skipped", reason: "the update did not finish" };
  const claimed = await deps.Update.updateOne({ _id: requestId, "params.invoice_fetch": { $exists: false } }, { $set: { "params.invoice_fetch": { status: "queueing" } } });
  if (!claimed.modifiedCount) return { ...out, status: "skipped", reason: "already handled for this update" };

  const userId = String(row.userId);
  try {
    const stored = new Set(await deps.ordersWithInvoice(userId, seen));
    const wanted = seen.filter((id) => !stored.has(id));
    out.already_stored = seen.length - wanted.length;
    out.orders = wanted.length;
    if (!wanted.length) {
      out.status = "skipped";
      out.reason = "every order already has a stored invoice";
    }
    for (let at = 0; at < wanted.length; at += REQUEST_VALUE_MAX) {
      const made = await deps.createInvoice(userId, {
        orderIds: wanted.slice(at, at + REQUEST_VALUE_MAX),
        force: false,
        by: "",
        source: "system-update",
        parentId: requestId,
        params: { follows: "update", parent_id: requestId },
      });
      out.request_ids.push(made.request_id);
      out.status = "queued";
    }
  } catch (err) {
    out.reason = err instanceof Error ? err.message.split("\n")[0] : String(err);
    // "Already queued in another invoice request" and "storage is not configured" are answers, not breakages.
    if (!out.request_ids.length) out.status = err instanceof RequestInputError ? "skipped" : "failed";
  }
  await deps.Update.updateOne({ _id: requestId }, { $set: { "params.invoice_fetch": out } }).catch(() => undefined);
  console.log(`[chain] update ${requestId.slice(-8)} saw ${seen.length} order(s) delivered → invoice fetch ${out.status}${out.request_ids.length ? ` (${out.request_ids.join(", ")}, ${out.orders} order(s))` : ""}${out.reason ? ` — ${out.reason}` : ""}`);
  return out;
}
