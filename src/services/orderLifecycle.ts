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
