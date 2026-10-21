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
