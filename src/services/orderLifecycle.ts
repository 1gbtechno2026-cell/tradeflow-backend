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
