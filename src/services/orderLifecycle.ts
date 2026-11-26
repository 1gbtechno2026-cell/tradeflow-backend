export type IndexScrapeStatus = "pending" | "scraped" | "skipped_old" | "failed";

const TERMINAL_RE = /^(delivered|cancelled|canceled|returned|refunded|failed)$/i;

export function isTerminalStatus(status?: string | null) {
  return TERMINAL_RE.test(String(status || "").trim());
}

export function isActiveStatus(status?: string | null) {
  const raw = String(status || "").trim();
  if (!raw) return true;
  return !isTerminalStatus(raw);
}

export function terminalStatusFilter() {
  return {
    $or: [
      { status_key: { $exists: false } },
      { status_key: "" },
      { status_key: { $not: TERMINAL_RE } },
    ],
  };
}

/**
 * What the reference refreshes when an update names no status — its own list,
 * in its order (measured: `params.statuses_to_update` of a default update
 * request). Here the same selection is "every unit that is not finished"
 * (terminalStatusFilter) plus Returned; the list is what that stands for and
 * is recorded on the request, so its summary can print it.
 */
export const DEFAULT_UPDATE_STATUSES = [
  "Out for delivery",
  "Shipped",
  "Order Pending",
  "Returned",
  "Approved",
  "Purchased",
  "Ready to ship",
  "Dispatched",
  "Cancellation requested",
  "Payment not received",
  "Return started",
];

/**
 * The Update button's selection when no status is named: every unit that is
 * not finished, and — as on the reference — units stored as Returned.
 * TERMINAL_RE itself is left alone: it also decides what a fetch enriches and
 * which OTPs are kept, and Returned stays finished there.
 */
export function inProgressUpdateFilter() {
  return { $or: [...terminalStatusFilter().$or, { status_key: /^returned$/i }] };
}

/**
 * The lifecycle stage behind a status key, whichever source wrote it. The
 * order API says "Approved" / "Ready to ship" / "Dispatched"; the page scraper
 * says "Order Confirmed" / "Shipped". A change of *stage* is what the update
 * worker counts and alerts on — a vocabulary swap between the two sources is
 * not a change.
 */
export type LifecycleStage = "placed" | "shipped" | "out_for_delivery" | "delivered" | "cancelled" | "returned" | "refunded" | "failed" | "unknown";

export function stageOf(status?: string | null): LifecycleStage {
  const s = String(status || "").trim().toLowerCase();
  if (!s) return "unknown";
  if (/^(delivered)$/.test(s)) return "delivered";
  if (/^(cancelled|canceled|cancellation requested)$/.test(s)) return "cancelled";
  if (/^returned$/.test(s)) return "returned";
  if (/^refund/.test(s)) return "refunded";
  if (/^failed$/.test(s)) return "failed";
  if (/out for delivery/.test(s)) return "out_for_delivery";
  if (/^(shipped|dispatched|in transit|reached)/.test(s)) return "shipped";
  if (/^(approved|order confirmed|order received|purchased|ready to ship|order pending|packed|processing)/.test(s)) return "placed";
  return "unknown";
}

// ── the refund watch (data-quality U12 + SO-5's clause = X5) ─────────────────

const DAY_MS = 24 * 60 * 60 * 1000;
const CANCELLED_KEY_RE = /^(cancelled|canceled)$/i;
/** Refund statuses Flipkart's refund card shows while the money is still on its way (sub-step 9's mapper). */
export const REFUND_OPEN_STATUSES = ["Processed", "Accepted", "Pending"] as const;

/**
 * The cancelled units the 6-hour sweep re-reads although Cancelled is terminal — the ONE refund watch (X5). Without it
 * a refund that completes after the unit's last read stays "Processed" forever, and the late credit-note look of
 * owner decision 4 (invoiceRefetch.ts) never sees its "refund completed" moment. Three clauses, each its own reason:
 *
 *   recent      cancelled ≤ 7 days ago, refund not completed, not cash on delivery (a COD order took no money)
 *   open        refund shows Processed / Accepted / Pending, cancelled ≤ 30 days ago
 *   bundled     an invoiced unit (cancelled after shipping, or Flipkart said the invoice was eligible) of an order
 *               whose bundle is stored (invoice_s3_key), cancelled ≤ 30 days ago, no refund yet, not cash on
 *               delivery — its credit notes follow the refund (sub-step 12, SO-5's clause). A COD unit has no
 *               refund to wait for: its one late look is the cancellation's (noted when it turned Cancelled).
 *
 * "Refund completed" is the detection's own rule (orderUnitEvents.ts refundIsCompleted): the refund status when the
 * unit carries one, else a DONE Refund stage (refundNotCompletedClause) — in every clause, so "recent" skips the units
 * the detection already counts as refunded. Units with no cancelled_date are never selected. Measured 2026-11-26
 * (read-only: npm run orders:refund-watch): recent 485, open 0, bundled 13 → 498 units on 53 accounts a sweep, inside
 * the plan's ~730. "open" grows once sub-step 9's refund status is live; the total falls as refunds complete.
 */
/**
 * "No completed refund" in Mongo — orderUnitEvents.ts refundIsCompleted's rule, the one detection (X6): not completed
 * by its refund status, nor — without a status (null, blank, or the page path's bare `refund` marker) — by a DONE
 * Refund stage. A `$nor`, so it sits beside the other conditions of a clause.
 */
export function refundNotCompletedClause(): { $nor: Array<Record<string, unknown>> } {
  return {
    $nor: [
      { refund_status: /complet/i },
      { refund_status: { $in: [null, /^\s*(refund)?\s*$/i] }, tracking: { $elemMatch: { status: /refund/i, stage: "DONE" } } },
    ],
  };
}

export function refundWatchClauses(now: Date = new Date()): Record<"recent" | "open" | "bundled", Record<string, unknown>> {
  const since = (days: number) => new Date(now.getTime() - days * DAY_MS);
  const notCod = {
    cash_on_delivery: { $ne: true },
    payment_mode: { $not: /^(cod|cash)/i },
    "api_extra.payment_mode": { $not: /cash on delivery/i },
  };
  return {
    recent: { status_key: CANCELLED_KEY_RE, cancelled_date: { $gte: since(7) }, ...notCod, ...refundNotCompletedClause() },
    open: { status_key: CANCELLED_KEY_RE, cancelled_date: { $gte: since(30) }, refund_status: { $in: REFUND_OPEN_STATUSES.map((s) => new RegExp(`^${s}$`, "i")) } },
    bundled: {
      status_key: CANCELLED_KEY_RE,
      cancelled_date: { $gte: since(30) },
      invoice_s3_key: { $nin: [null, ""] },
      ...notCod,
      $and: [
        { $or: [{ order_status_pre_cancellation: /ship|deliver|out for/i }, { "api_extra.invoice_eligible": true }] },
        // No refund yet, by the detection's rule (refundNotCompletedClause).
        refundNotCompletedClause(),
      ],
    },
  };
}

/** The refund watch as one selection: `{ $or: [recent, open, bundled] }` (refundWatchClauses). */
export function refundWatchFilter(now: Date = new Date()): { $or: Array<Record<string, unknown>> } {
  const c = refundWatchClauses(now);
  return { $or: [c.recent, c.open, c.bundled] };
}
