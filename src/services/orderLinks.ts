import { CheckoutJob } from "../models/CheckoutJob.js";
import type { JobResultSnapshot } from "../types.js";

/**
 * The link between an order unit and the checkout job that placed it.
 *
 * BoB's export carries "BoB Order", the card name and both masked card
 * numbers on every row because BoB seeds its order fetch from its own
 * checkout records: an order it placed is known by id even when Flipkart's
 * My Orders hides it (payment never confirmed), and the card that paid is on
 * the record. tradeflowjobs holds the same facts; this module reads them.
 * Nothing here writes.
 */

export interface PlacedOrderRef {
  orderId: string;
  placedAt: Date | null;
  jobId: string;
}

/** Every order id a checkout job on this account produced. */
export async function checkoutOrderIdsFor(userId: string, email: string): Promise<PlacedOrderRef[]> {
  const rows = await CheckoutJob.find({
    userId,
    email: String(email || "").toLowerCase(),
    "result.flipkartOrderId": { $regex: /^OD\d{12,}/ },
  })
    .select("result.flipkartOrderId completedAt createdAt")
    .lean();
  const seen = new Map<string, PlacedOrderRef>();
  for (const r of rows) {
    const orderId = String(r.result?.flipkartOrderId || "").toUpperCase();
    if (!orderId || seen.has(orderId)) continue;
    seen.set(orderId, { orderId, placedAt: r.completedAt || r.createdAt || null, jobId: String(r._id) });
  }
  return [...seen.values()];
}

export interface OrderCardFields {
  card_name: string;
  parent_card_number: string;
  card_number: string;
  is_bae_order: boolean;
}

/**
 * The card that paid for this order, as the job recorded it: label plus the
 * MASKED numbers from the job's card list, matched by last 4. Empty object
 * when no job placed this order, so a spread into $set changes nothing.
 */
export async function cardFieldsFor(userId: string, email: string, orderId: string): Promise<Partial<OrderCardFields>> {
  const id = String(orderId || "").toUpperCase();
  if (!/^OD\d{12,}/.test(id)) return {};
  const job = await CheckoutJob.findOne({ userId, email: String(email || "").toLowerCase(), "result.flipkartOrderId": id })
    .sort({ completedAt: -1 })
    .select("result.cardName result.parentCardLast4 result.childCardLast4 result.cardLast4 request.cards request.paymentMode")
    .lean();
  if (!job) return {};
  const r = (job.result || {}) as Partial<JobResultSnapshot>;
  const cards = job.request?.cards || [];
  const last4 = (v: unknown) => String(v || "").replace(/\D/g, "").slice(-4);
  const childLast4 = last4(r.childCardLast4);
  const parentLast4 = last4(r.parentCardLast4 || r.cardLast4);
  const byChild = cards.find((c) => childLast4 && last4(c.cardNumberMasked) === childLast4);
  const byParent = cards.find((c) => parentLast4 && last4(c.parentCardNumberMasked) === parentLast4) || byChild;
  return {
    card_name: String(r.cardName || byChild?.name || ""),
    parent_card_number: String(byParent?.parentCardNumberMasked || (parentLast4 ? `XXXXXXXXXXXX${parentLast4}` : "")),
    card_number: String(byChild?.cardNumberMasked || (childLast4 ? `XXXXXXXXXXXX${childLast4}` : "")),
    is_bae_order: true,
  };
}
