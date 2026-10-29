/**
 * Query-string helpers shared by the Orders listings (routes/jobs.ts): the
 * date range, pagination and comma-or-repeated lists, in the shape the
 * dashboard's Orders tab sends them.
 *
 *   range_mode=relative&range_key=last_24h      the last N hours / days
 *   range_mode=absolute&start=2026-10-01&end=2026-10-28   IST calendar days
 *   page=2&page_size=50
 *   status=failed,cancelled   or   status=failed&status=cancelled
 */

type Query = Record<string, unknown>;

const IST_OFFSET_MS = 5.5 * 3600e3;

function str(v: unknown): string {
  if (Array.isArray(v)) return v.map(String).join(",");
  return v == null ? "" : String(v);
}

/** A list parameter: repeated keys and comma-separated values both work. */
export function listParam(q: Query, key: string): string[] {
  const raw = q[key];
  const parts = Array.isArray(raw) ? raw.map(String) : raw == null ? [] : [String(raw)];
  return parts
    .flatMap((p) => p.split(","))
    .map((s) => s.trim())
    .filter(Boolean);
}

export interface DateRange {
  from: Date | null;
  to: Date | null;
  /** How the range was described, echoed back for the UI. */
  mode: "relative" | "absolute" | "all";
  key: string;
}

/** `YYYY-MM-DD` as the start of that calendar day in IST. */
function istDayStart(day: string): Date | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(day.trim());
  if (!m) return null;
  const utcMidnight = Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  return new Date(utcMidnight - IST_OFFSET_MS);
}

const RELATIVE_MS: Record<string, number> = {
  last_1h: 3600e3,
  last_24h: 24 * 3600e3,
  last_7d: 7 * 864e5,
  last_30d: 30 * 864e5,
  last_60d: 60 * 864e5,
  last_90d: 90 * 864e5,
};

/**
 * Start/End dates win over a relative key when either is present (that is how
 * the reference UI behaves: picking a Start Date switches to absolute mode).
 * Without any of them the range is "all time".
 */
export function parseDateRange(q: Query): DateRange {
  const start = str(q.start || q.start_date);
  const end = str(q.end || q.end_date);
  const mode = str(q.range_mode);
  if (start || end) {
    const from = start ? istDayStart(start) : null;
    const endStart = end ? istDayStart(end) : null;
    const to = endStart ? new Date(endStart.getTime() + 864e5 - 1) : null;
    return { from, to, mode: "absolute", key: `${start || "…"}..${end || "…"}` };
  }
  const key = str(q.range_key);
  if (mode === "all" || key === "all") return { from: null, to: null, mode: "all", key: "all" };
  let ms = RELATIVE_MS[key];
  if (!ms) {
    const custom = /^last_(\d{1,4})d$/.exec(key);
    if (custom) ms = Number(custom[1]) * 864e5;
  }
  if (!ms) return { from: null, to: null, mode: "all", key: "all" };
  return { from: new Date(Date.now() - ms), to: null, mode: "relative", key };
}

/** Mongo filter fragment for `field` within the range, or nothing when unbounded. */
export function rangeFilter(range: DateRange): Record<string, Date> | null {
  const f: Record<string, Date> = {};
  if (range.from) f.$gte = range.from;
  if (range.to) f.$lte = range.to;
  return Object.keys(f).length ? f : null;
}

export interface Pagination {
  page: number;
  pageSize: number;
  skip: number;
}

export function parsePagination(q: Query, defaults: { pageSize: number; maxPageSize: number }): Pagination {
  const page = Math.max(1, Math.floor(Number(q.page)) || 1);
  const requested = Number(q.page_size ?? q.pageSize ?? q.limit);
  const pageSize = Math.min(defaults.maxPageSize, Math.max(1, Math.floor(requested) || defaults.pageSize));
  return { page, pageSize, skip: (page - 1) * pageSize };
}

export function paginationMeta(count: number, p: Pagination) {
  return {
    count,
    num_pages: Math.max(1, Math.ceil(count / p.pageSize)),
    current_page: p.page,
    page_size: p.pageSize,
  };
}

/** A user-typed search term as a safe case-insensitive regex. */
export function containsRegex(term: string): RegExp {
  return new RegExp(term.trim().replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i");
}
