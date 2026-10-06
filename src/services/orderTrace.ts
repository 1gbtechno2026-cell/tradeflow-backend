import fs from "node:fs";
import path from "node:path";
import { config } from "../config.js";

/**
 * Replayable trace of a real fetch / update run, on disk.
 *
 * Off unless ORDER_TRACE_DIR is set. When it is, every run gets
 * <dir>/<kind>-<timestamp>/ with:
 *   manifest.json                 mode, accounts, since-date, counters, the
 *                                 invariant line(s) and PASS/FAIL
 *   list-<email>-p<N>.json        the raw order-list response (API mode)
 *   unit-<orderId>-<unitId>.json  what the unit was built from — the raw
 *                                 details response + the mapped document in
 *                                 API mode, the page snapshot + tracking text
 *                                 in scrape mode — and what was done with it
 *
 * `npm run orders:replay -- <runDir>` re-maps every API unit file from its raw
 * JSON and diffs it against order_details, which is what makes the trace
 * replayable rather than just a dump. Traces hold real order and address
 * data; keep the directory under debug/ (gitignored).
 *
 * Every method is a no-op when tracing is off, so the fetch path can call
 * them unconditionally.
 */

export interface TraceUnitRecord {
  email: string;
  orderId: string;
  unitId: string;
  mode: "api" | "scrape";
  /** API mode: the whole page/fetch body. */
  raw?: unknown;
  /** API mode: the document mapApiUnitToOrder() produced. */
  mapped?: unknown;
  /** Scrape mode: what READ_PAGE returned and the "See all updates" text. */
  pageData?: unknown;
  trackingText?: string;
  url?: string;
  outcome?: "saved" | "skipped_old" | "no_date" | "failed" | "read";
  error?: string;
  ms?: number;
}

export interface TraceListRecord {
  email: string;
  page: number;
  request: unknown;
  response: unknown;
  ms: number;
}

export class OrderTrace {
  readonly dir: string;
  private manifestData: Record<string, unknown>;
  private unitCount = 0;
  private listCount = 0;
  private readonly unitSeen = new Map<string, number>();

  constructor(dir: string, kind: "fetch" | "update", meta: Record<string, unknown>) {
    this.dir = dir;
    fs.mkdirSync(dir, { recursive: true });
    this.manifestData = {
      kind,
      startedAt: new Date().toISOString(),
      finishedAt: null,
      mode: config.orderFetchMode,
      ...meta,
      accounts: [],
      invariants: [],
      units: 0,
      listPages: 0,
    };
    this.flush();
  }

  private flush() {
    try {
      fs.writeFileSync(path.join(this.dir, "manifest.json"), JSON.stringify(this.manifestData, null, 1));
    } catch {
      /* a trace must never fail the run it describes */
    }
  }

  private safe(s: string) {
    return String(s || "").replace(/[^A-Za-z0-9@._-]/g, "_").slice(0, 80);
  }

  list(rec: TraceListRecord) {
    try {
      fs.writeFileSync(path.join(this.dir, `list-${this.safe(rec.email)}-p${rec.page}.json`), JSON.stringify(rec, null, 1));
      this.listCount += 1;
      this.manifestData.listPages = this.listCount;
    } catch {
      /* same */
    }
  }

  unit(rec: TraceUnitRecord) {
    try {
      const key = `${rec.orderId}-${rec.unitId}`;
      const n = (this.unitSeen.get(key) || 0) + 1;
      this.unitSeen.set(key, n);
      const file = `unit-${this.safe(rec.orderId)}-${this.safe(rec.unitId)}${n > 1 ? `-${n}` : ""}.json`;
      fs.writeFileSync(path.join(this.dir, file), JSON.stringify({ at: new Date().toISOString(), ...rec }, null, 1));
      this.unitCount += 1;
      this.manifestData.units = this.unitCount;
    } catch {
      /* same */
    }
  }

  /** Append an account's invariant line (and its numbers) to the manifest. */
  invariant(line: Record<string, unknown>) {
    (this.manifestData.invariants as unknown[]).push(line);
    this.flush();
  }

  account(info: Record<string, unknown>) {
    (this.manifestData.accounts as unknown[]).push(info);
    this.flush();
  }

  finish(summary: Record<string, unknown>) {
    this.manifestData = { ...this.manifestData, ...summary, finishedAt: new Date().toISOString() };
    this.flush();
  }
}

const active = new Map<string, OrderTrace>();

/** Start a trace for this user's run, or return null when ORDER_TRACE_DIR is empty. */
export function beginTrace(userId: string, kind: "fetch" | "update", meta: Record<string, unknown>): OrderTrace | null {
  if (!config.orderTraceDir) return null;
  const dir = path.resolve(config.orderTraceDir, `${kind}-${new Date().toISOString().replace(/[:.]/g, "-")}`);
  try {
    const trace = new OrderTrace(dir, kind, meta);
    active.set(userId, trace);
    console.log(`[order-trace] ${kind} run → ${dir}`);
    return trace;
  } catch (err) {
    console.warn(`[order-trace] could not open ${dir}: ${err instanceof Error ? err.message : err}`);
    return null;
  }
}

export function traceFor(userId: string): OrderTrace | null {
  return active.get(userId) || null;
}

export function endTrace(userId: string, summary: Record<string, unknown>) {
  const trace = active.get(userId);
  if (!trace) return;
  trace.finish(summary);
  active.delete(userId);
  console.log(`[order-trace] written: ${trace.dir}`);
}
