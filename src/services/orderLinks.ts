import { CheckoutJob } from "../models/CheckoutJob.js";
import type { JobResultSnapshot } from "../types.js";
import { jobClassFor } from "./jobClass.js";

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
  payment_mode: string;
  transaction_amount: string;
  payment_fee: string;
  payment_fee_details: string;
  cash_on_delivery: boolean;
}

/**
 * How this order was paid, as the job recorded it: the card's label plus the
 * MASKED numbers from the job's card list, matched by last 4; the payment mode;
 * and what Flipkart charged with its fee (PaymentApiWatcher.placedOrderDetails).
 * Empty object when no job placed this order, so a spread into $set changes
 * nothing.
 *
 * cash_on_delivery is set only to true, and only for a COD job: Flipkart's own
 * order page is the other source of it, and a card job has nothing to add there.
 * Spread after the fetched fields, so a COD order reads as COD before Flipkart's
 * page or API says so.
 */
export async function cardFieldsFor(userId: string, email: string, orderId: string): Promise<Partial<OrderCardFields>> {
  const id = String(orderId || "").toUpperCase();
  if (!/^OD\d{12,}/.test(id)) return {};
  const job = await CheckoutJob.findOne({ userId, email: String(email || "").toLowerCase(), "result.flipkartOrderId": id })
    .sort({ completedAt: -1 })
    .select(
      "result.cardName result.parentCardLast4 result.childCardLast4 result.cardLast4 " +
        "result.transactionAmount result.paymentFee result.paymentFeeDetails request.cards request.paymentMode"
    )
    .lean();
  if (!job) return {};
  const r = (job.result || {}) as Partial<JobResultSnapshot>;
  // A job with an order id was paid, so it is one or the other; without an
  // authType jobClassFor never answers "otp-phone".
  const mode = jobClassFor({ paymentMode: job.request?.paymentMode }) === "cod" ? "cod" : "card";
  const payment = {
    payment_mode: mode,
    transaction_amount: String(r.transactionAmount || ""),
    payment_fee: String(r.paymentFee || ""),
    payment_fee_details: String(r.paymentFeeDetails || ""),
    ...(mode === "cod" ? { cash_on_delivery: true } : {}),
  };
  if (mode === "cod") {
    // No card, so no card columns — written empty rather than left out, the
    // same as a card job whose numbers did not match.
    return { card_name: "", parent_card_number: "", card_number: "", is_bae_order: true, ...payment };
  }
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
    ...payment,
  };
}
