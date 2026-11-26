/**
 * The one-off forced refresh (data-quality sub-step 10 U15, operations OPS-11 / OPS-12 / OPS-18). DRY RUN BY DEFAULT.
 *
 * Every stored Delivered and Cancelled unit (owner decision 5: back to January — nothing older is stored) is read
 * again through the update worker, in batches of ~200 accounts: each batch is ONE ordinary update request
 * (selection order_status_and_email_ids, force, quiet_events — sub-step 10 U10: its events are written already
 * processed, so no alert flood; no late re-download is noted). It shows in Order Units → Batch requests and stops
 * with its Stop button like any other.
 *
 *   npm run orders:forced-refresh                                    dry run: the batches, the accounts left out
 *   npm run orders:forced-refresh -- --canary 50 --go                OPS-11: 50 healthy accounts, field counts before/after
 *   npm run orders:forced-refresh -- --go [--from-batch N]           OPS-12: every batch (resume at N)
 *   npm run orders:forced-refresh -- --statuses Failed --go          owner question 3
 *   npm run orders:forced-refresh -- --never-stored-since 2026-01-01 --go     OPS-18: index-only units, by order id
 *   --accounts-per-batch 200 · --show-emails (the dry run masks addresses)
 *
 * The order: accounts by their newest read (last_fetch), newest first; those unread for more than 14 days last.
 * Left out and listed apart: accounts not logged in, and accounts that LOOK logged out — a not_logged_in update or
 * fetch task in the last 48 h (order_sync_tasks), or a session check after the session was saved that said
 * expired / not_logged_in. Re-login those and refresh them by hand.
 *
 * --go refuses unless an update worker is alive on THIS system (its heartbeat key in this Mac's Redis — the jobs go
 * to this Mac's queue; an unreachable Redis counts as "not alive"), waits while a schedule or out-for-delivery request
 * is open, and stops BEFORE the next batch when a batch ends with more than 5 % failed accounts, an account message
 * carries "[demotion cap reached" or "[circuit open", the request was cancelled (Stop), the worker's heartbeat is gone
 * when the next batch is due, or a batch cannot be created. Ctrl-C also stops before the next batch (the batch running
 * goes on: Stop it in Batch requests). Resume with --from-batch N. The canary (--canary N) waits one dashboard alert
 * poll (90 s) after its batch, prints the field counts and the OPS-11 gate, and exits 3 when it fails.
 *
 * Mongo: connected with autoIndex / autoCreate off; the dry run only reads. Never Flipkart from here — the worker
 * reads the units. Run it only in the window the plan names (OPS-10: switches off, the other Mac quiet, outside
 * 04:30–07:00 IST).
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import mongoose, { Types } from "mongoose";

const DAY_MS = 24 * 60 * 60 * 1000;
export const STALE_DAYS = 14;
export const FAILED_SHARE_STOP = 0.05;
export const STOP_NOTES = ["[demotion cap reached", "[circuit open"] as const;
export const FORCED_REFRESH_PURPOSE = "forced_refresh_2026";
export const FORCED_REFRESH_BY = "forced-refresh";
const ORDER_IDS_PER_REQUEST = 2000;

// ── planning (pure) ──────────────────────────────────────────────────────────

export interface RefreshAccount {
  email: string;
  units: number;
  orders: number;
  /** Newest last_fetch of its units in scope. */
  lastFetch: Date | null;
  /** Order ids (the never-stored variant). */
  orderIds?: string[];
}

export interface RefreshBatch {
  /** 1-based. */
  n: number;
  emails: string[];
  accounts: number;
  units: number;
  /** The never-stored variant: the order ids of this batch (≤ 2,000). */
  orderIds?: string[];
}

/** Freshest first; accounts unread for more than STALE_DAYS (or never) last, freshest first among them too. */
export function orderAccounts(accounts: RefreshAccount[], now: Date = new Date()): RefreshAccount[] {
  const staleBefore = now.getTime() - STALE_DAYS * DAY_MS;
  const t = (a: RefreshAccount) => (a.lastFetch ? new Date(a.lastFetch).getTime() : 0);
  const stale = (a: RefreshAccount) => t(a) < staleBefore;
  return [...accounts].sort((a, b) => Number(stale(a)) - Number(stale(b)) || t(b) - t(a) || a.email.localeCompare(b.email));
}

/** Batches of `perBatch` accounts in that order. */
export function planBatches(accounts: RefreshAccount[], opts: { perBatch: number; now?: Date }): RefreshBatch[] {
  const per = Math.max(1, Math.floor(opts.perBatch));
  const ordered = orderAccounts(accounts, opts.now);
  const out: RefreshBatch[] = [];
  for (let i = 0; i < ordered.length; i += per) {
    const slice = ordered.slice(i, i + per);
    out.push({ n: out.length + 1, emails: slice.map((a) => a.email), accounts: slice.length, units: slice.reduce((s, a) => s + a.units, 0) });
  }
  return out;
}

/** The never-stored variant: order-id batches of at most 2,000, cut at account boundaries (an account above 2,000 is split on its own). */
export function planOrderBatches(accounts: RefreshAccount[], opts: { max?: number; now?: Date } = {}): RefreshBatch[] {
  const max = Math.max(1, opts.max ?? ORDER_IDS_PER_REQUEST);
  const out: RefreshBatch[] = [];
  let cur: RefreshBatch | null = null;
  const close = () => {
    if (cur && cur.orderIds!.length) out.push(cur);
    cur = null;
  };
  for (const a of orderAccounts(accounts, opts.now)) {
    const ids = [...new Set(a.orderIds || [])];
    if (!ids.length) continue;
    if (ids.length > max) {
      close();
      for (let i = 0; i < ids.length; i += max) out.push({ n: 0, emails: [a.email], accounts: 1, units: 0, orderIds: ids.slice(i, i + max) });
      continue;
    }
    if (cur && cur.orderIds!.length + ids.length > max) close();
    if (!cur) cur = { n: 0, emails: [], accounts: 0, units: 0, orderIds: [] };
    const c: RefreshBatch = cur;
    c.emails.push(a.email);
    c.accounts++;
    c.units += a.units;
    c.orderIds!.push(...ids);
  }
  close();
  return out.map((b, i) => ({ ...b, n: i + 1 }));
}

/** The canary: `count` accounts read in the last day with at most 15 units in scope, those holding both Delivered and Cancelled first. */
export function pickCanary(accounts: Array<RefreshAccount & { statuses?: string[] }>, count: number, now: Date = new Date()): RefreshAccount[] {
  const fresh = accounts.filter((a) => a.lastFetch && now.getTime() - new Date(a.lastFetch).getTime() <= DAY_MS && a.units <= 15);
  const mixed = (a: { statuses?: string[] }) => Number(new Set(a.statuses || []).size > 1);
  return [...fresh].sort((a, b) => mixed(b) - mixed(a) || (b.lastFetch!.getTime() - a.lastFetch!.getTime()) || a.email.localeCompare(b.email)).slice(0, Math.max(0, count));
}

/** A platformids row as the classification reads it: status, session date and last check — never the cookies. */
export interface AccountRow {
  email?: string;
  status?: string;
  sessionSavedAt?: Date | null;
  sessionCheck?: { at?: Date | null; outcome?: string } | null;
}

/**
 * Which accounts the batches leave out. Pure.
 *  · notLoggedIn — no row of the e-mail is logged_in with a saved session (the worker's own rule, orderUpdate.ts
 *    loggedInAccounts: the newest logged-in row with a session is the one it reads with).
 *  · looksOut — logged in, but a not_logged_in update / fetch task in the last 48 h names it, or that newest row's
 *    session check, made after its session was saved, said expired / not_logged_in.
 */
export function classifyAccounts<A extends RefreshAccount>(accounts: A[], rows: AccountRow[], recentNotLoggedIn: Set<string>) {
  const newest = new Map<string, AccountRow>();
  for (const r of rows) {
    const key = String(r.email || "").toLowerCase();
    if (r.status !== "logged_in" || !r.sessionSavedAt) continue;
    const held = newest.get(key);
    if (!held || new Date(r.sessionSavedAt).getTime() > new Date(held.sessionSavedAt || 0).getTime()) newest.set(key, r);
  }
  const checkSaysOut = (email: string) => {
    const r = newest.get(email);
    const c = r?.sessionCheck;
    return Boolean(c && /^(expired|not_logged_in)$/.test(String(c.outcome || "")) && c.at && (!r?.sessionSavedAt || new Date(c.at) > new Date(r.sessionSavedAt)));
  };
  const notLoggedIn = accounts.filter((a) => !newest.has(a.email));
  const looksOut = accounts.filter((a) => newest.has(a.email) && (recentNotLoggedIn.has(a.email) || checkSaysOut(a.email)));
  const left = new Set([...notLoggedIn, ...looksOut].map((a) => a.email));
  return {
    notLoggedIn,
    looksOut,
    inScope: accounts.filter((a) => !left.has(a.email)),
    why: (email: string) => [recentNotLoggedIn.has(email) ? "task" : "", checkSaysOut(email) ? "check" : ""].filter(Boolean).join(" + "),
  };
}

/**
 * The never-stored variant's accounts: index rows (skipped_old) whose unit is not in order_details, by account; the
 * order ids are those of the units still missing — an order whose units are all stored is not sent again. Pure.
 */
export function neverStoredAccounts(rows: Array<{ email: string; orderId: string; unitId: string }>, stored: Set<string>): Array<RefreshAccount & { statuses: string[] }> {
  const by = new Map<string, { units: Set<string>; orders: Set<string> }>();
  for (const r of rows) {
    const email = String(r.email || "").toLowerCase();
    if (!email || !r.orderId || stored.has(r.unitId)) continue;
    const a = by.get(email) || { units: new Set<string>(), orders: new Set<string>() };
    a.units.add(r.unitId);
    a.orders.add(r.orderId);
    by.set(email, a);
  }
  return [...by].map(([email, a]) => ({ email, units: a.units.size, orders: a.orders.size, lastFetch: null, orderIds: [...a.orders], statuses: [] }));
}

// ── running (the dependencies are injected: test:forced-refresh drives it with fakes) ──

export interface PolledAccount {
  status: string;
  message?: string;
}
export interface PolledRequest {
  status: string;
  accounts: PolledAccount[];
}

export interface BatchOutcome {
  n: number;
  requestId: string;
  status: string;
  accounts: number;
  failed: number;
  notes: string[];
}

export interface RunDeps {
  create: (batch: RefreshBatch) => Promise<{ id: string }>;
  poll: (requestId: string) => Promise<PolledRequest | null>;
  /** Open schedule / out-for-delivery requests right now (the refresh waits while there is one). */
  openSchedules: () => Promise<number>;
  sleep: (ms: number) => Promise<void>;
  log: (line: string) => void;
  /** Ctrl-C: stop before the next batch. */
  stopRequested?: () => boolean;
  pollEveryMs?: number;
  waitEveryMs?: number;
  /** After a batch ended, before the stop rules (the canary's field counts). */
  afterBatch?: (batch: RefreshBatch, outcome: BatchOutcome) => Promise<void>;
  /**
   * Is the update worker on this system alive (its heartbeat)? Asked before each batch is created — none is created
   * while it is gone (the run stops, resume with --from-batch) — and while a batch runs, where a missing heartbeat is
   * only reported: a request keeps its state when the worker dies and the owner's restart resumes it.
   */
  workerAlive?: () => Promise<boolean>;
}

export interface RunResult {
  ran: BatchOutcome[];
  /** Why the run stopped before its last batch, or "" when every batch ran. */
  stopped: string;
  /** The batch to resume at (--from-batch), when it stopped. */
  resumeAt: number | null;
}

/** Why a finished batch stops the run, or "". Pure. */
export function stopReason(outcome: BatchOutcome): string {
  if (outcome.status === "cancelled") return `batch ${outcome.n} was stopped (request ${outcome.requestId})`;
  if (outcome.accounts && outcome.failed / outcome.accounts > FAILED_SHARE_STOP)
    return `batch ${outcome.n}: ${outcome.failed} of ${outcome.accounts} account(s) failed (${((100 * outcome.failed) / outcome.accounts).toFixed(1)} % > ${FAILED_SHARE_STOP * 100} %)`;
  if (outcome.notes.length) return `batch ${outcome.n}: ${outcome.notes.join(" · ")}`;
  return "";
}

/** What a finished request says about its accounts. Pure. */
export function outcomeOf(batch: RefreshBatch, requestId: string, req: PolledRequest): BatchOutcome {
  const notes = new Set<string>();
  for (const a of req.accounts) for (const note of STOP_NOTES) if (String(a.message || "").includes(note)) notes.add(`an account message carries "${note}"`);
  return { n: batch.n, requestId, status: req.status, accounts: req.accounts.length, failed: req.accounts.filter((a) => a.status === "failed").length, notes: [...notes] };
}

export async function runBatches(batches: RefreshBatch[], deps: RunDeps, opts: { fromBatch?: number } = {}): Promise<RunResult> {
  const pollEvery = deps.pollEveryMs ?? 10_000;
  const waitEvery = deps.waitEveryMs ?? 30_000;
  const from = Math.max(1, opts.fromBatch ?? 1);
  const ran: BatchOutcome[] = [];
  const todo = batches.filter((b) => b.n >= from);
  for (let i = 0; i < todo.length; i++) {
    const batch = todo[i];
    if (deps.stopRequested?.()) return { ran, stopped: "stopped by hand (Ctrl-C)", resumeAt: batch.n };
    // A sweep of the shared clock (6-hour or out-for-delivery) is open: wait until it ends — the two would read the
    // same accounts, and the account locks would make each wait for the other anyway.
    let waits = 0;
    while ((await deps.openSchedules()) > 0) {
      if (deps.stopRequested?.()) return { ran, stopped: "stopped by hand (Ctrl-C)", resumeAt: batch.n };
      if (waits++ === 0) deps.log(`batch ${batch.n}: a schedule request is open — waiting until it ends`);
      await deps.sleep(waitEvery);
    }
    if (deps.workerAlive && !(await deps.workerAlive())) return { ran, stopped: `batch ${batch.n} not created: no update worker is alive on this system`, resumeAt: batch.n };
    let id: string;
    try {
      ({ id } = await deps.create(batch));
    } catch (err) {
      // createUpdateRequest refuses a batch none of whose accounts has a usable session ("No active order units").
      return { ran, stopped: `batch ${batch.n} could not be created: ${err instanceof Error ? err.message : String(err)}`, resumeAt: batch.n };
    }
    deps.log(`batch ${batch.n}/${batches.length}: request ${id} — ${batch.orderIds ? `${batch.orderIds.length} order id(s), ` : ""}${batch.accounts} account(s), ${batch.units} unit(s)`);
    let req: PolledRequest | null = null;
    let workerGone = false;
    for (;;) {
      req = await deps.poll(id);
      if (!req || req.status === "done" || req.status === "cancelled") break;
      if (deps.workerAlive) {
        const alive = await deps.workerAlive();
        if (!alive && !workerGone) deps.log(`batch ${batch.n}: the update worker's heartbeat is gone — request ${id} waits for the worker's restart (or Stop it in Batch requests)`);
        else if (alive && workerGone) deps.log(`batch ${batch.n}: the update worker is back`);
        workerGone = !alive;
      }
      await deps.sleep(pollEvery);
    }
    const outcome = outcomeOf(batch, id, req || { status: "cancelled", accounts: [] });
    ran.push(outcome);
    deps.log(`batch ${batch.n}: ${outcome.status} — ${outcome.accounts - outcome.failed}/${outcome.accounts} account(s) ok, ${outcome.failed} failed${outcome.notes.length ? ` · ${outcome.notes.join(" · ")}` : ""}`);
    await deps.afterBatch?.(batch, outcome);
    const why = stopReason(outcome);
    if (why) return { ran, stopped: why, resumeAt: todo[i + 1]?.n ?? null };
  }
  return { ran, stopped: "", resumeAt: null };
}

// ── the canary's field counts ────────────────────────────────────────────────

/** Which alerts are the canary's (an `$or` for the dashboard's alerts): its orders', and its accounts' (account-level alerts carry the e-mail, no order id). Pure. */
export function canaryAlertScope(orderIds: string[], emails: string[]): Array<Record<string, unknown>> {
  return [{ orderId: { $in: orderIds.filter(Boolean) } }, { email: { $in: emails.map((e) => e.trim().toLowerCase()).filter(Boolean) } }];
}

export const CANARY_FIELDS = [
  "status_key", "product_name",
  "refund_status", "refund_amount", "refund_msg", "refund_date", "refund_kind",
  "delivery_date", "imei", "is_invoice_downloaded", "delivery_otp",
  "card_name", "card_number", "parent_card_number", "card_source",
] as const;

export interface FieldCount {
  changed: number;
  /** A value before, blank after — must stay 0 for imei / is_invoice_downloaded / delivery_otp. */
  blanked: number;
  /** Blank before, a value after (refunds filled, a delivery date found). */
  filled: number;
}

/** Per field: how many units changed, went blank, or were filled. Pure. */
export function fieldChanges(before: Map<string, Record<string, unknown>>, after: Map<string, Record<string, unknown>>): Record<string, FieldCount> {
  const out: Record<string, FieldCount> = {};
  const blank = (v: unknown) => v === null || v === undefined || v === "" || v === false;
  const same = (a: unknown, b: unknown) => JSON.stringify(a ?? null) === JSON.stringify(b ?? null);
  for (const f of CANARY_FIELDS) out[f] = { changed: 0, blanked: 0, filled: 0 };
  for (const [unit, b] of before) {
    const a = after.get(unit);
    if (!a) continue;
    for (const f of CANARY_FIELDS) {
      if (same(b[f], a[f])) continue;
      out[f].changed++;
      if (!blank(b[f]) && blank(a[f])) out[f].blanked++;
      if (blank(b[f]) && !blank(a[f])) out[f].filled++;
    }
  }
  return out;
}

/** OPS-11's gate on the canary's counts (plan sub-step 13). Pure: the lines to print and whether it failed. */
export function canaryVerdict(
  counts: Record<string, FieldCount>,
  o: { units: number; cancelledUnits: number; cancelledRefundFilled: number; newOpenAlerts: number }
): { lines: string[]; fail: boolean } {
  const lines: string[] = [];
  const blanked = ["imei", "is_invoice_downloaded", "delivery_otp"].filter((f) => counts[f].blanked > 0);
  const statusShare = o.units ? counts.status_key.changed / o.units : 0;
  lines.push(blanked.length ? `FAIL — went blank: ${blanked.join(", ")}` : "ok — no IMEI, invoice flag or OTP went blank");
  lines.push(`${o.newOpenAlerts ? "FAIL" : "ok"} — new open alerts on the canary's orders: ${o.newOpenAlerts}`);
  lines.push(`${statusShare > 0.01 ? "REVIEW" : "ok"} — status changed on ${counts.status_key.changed} of ${o.units} unit(s) (${(100 * statusShare).toFixed(1)} %; review above ~1 %)`);
  lines.push(`info — names changed on ${counts.product_name.changed} (the subtitle added) · refunds filled on ${o.cancelledRefundFilled} of ${o.cancelledUnits} Cancelled unit(s)`);
  return { lines, fail: blanked.length > 0 || o.newOpenAlerts > 0 };
}

// ── the script ───────────────────────────────────────────────────────────────

interface Args {
  go: boolean;
  statuses: string[];
  perBatch: number;
  canary: number;
  fromBatch: number;
  neverStoredSince: Date | null;
  showEmails: boolean;
}

function parseArgs(argv: string[]): Args {
  const a: Args = { go: false, statuses: ["Delivered", "Cancelled"], perBatch: 200, canary: 0, fromBatch: 1, neverStoredSince: null, showEmails: false };
  const value = (i: number, name: string) => {
    const v = argv[i + 1];
    if (v === undefined || v.startsWith("--")) throw new Error(`${name} needs a value`);
    return v;
  };
  for (let i = 0; i < argv.length; i++) {
    const x = argv[i];
    if (x === "--go") a.go = true;
    else if (x === "--dry-run") a.go = false;
    else if (x === "--show-emails") a.showEmails = true;
    else if (x === "--statuses") a.statuses = value(i++, x).split(",").map((s) => s.trim()).filter(Boolean);
    else if (x === "--accounts-per-batch") a.perBatch = Number(value(i++, x));
    else if (x === "--canary") a.canary = Number(value(i++, x));
    else if (x === "--from-batch") a.fromBatch = Number(value(i++, x));
    else if (x === "--never-stored-since") {
      const v = value(i++, x);
      a.neverStoredSince = new Date(/T/.test(v) ? v : `${v}T00:00:00+05:30`);
      if (Number.isNaN(a.neverStoredSince.getTime())) throw new Error(`--never-stored-since needs a date (got "${v}")`);
    } else throw new Error(`unknown argument "${x}"`);
  }
  if (!(a.perBatch >= 1)) throw new Error("--accounts-per-batch must be ≥ 1");
  if (!(a.fromBatch >= 1)) throw new Error("--from-batch must be ≥ 1");
  if (!a.statuses.length) throw new Error("--statuses is empty");
  return a;
}

/** An e-mail as the dry run prints it: two letters of the name, one of the domain, and its last label (the domain is often a business name). */
export function maskEmail(email: string): string {
  const at = email.lastIndexOf("@");
  if (at < 0) return `${email.slice(0, 2)}***`;
  const domain = email.slice(at + 1);
  const dot = domain.lastIndexOf(".");
  return `${email.slice(0, Math.min(2, at))}***@${domain.slice(0, 1)}***${dot > 0 ? domain.slice(dot) : ""}`;
}
const mask = maskEmail;
const ist = (d: Date | null | undefined) => (d ? `${new Date(d).toLocaleString("en-IN", { timeZone: "Asia/Kolkata", hour12: false })} IST` : "—");

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const { config } = await import("../config.js");
  if (!config.userId || !Types.ObjectId.isValid(config.userId)) throw new Error("TRADE_FLOW_USER_ID is required (og_user workspace id)");
  const userId = new Types.ObjectId(config.userId);
  const show = (email: string) => (args.showEmails ? email : mask(email));
  // Never the app's connectDb(): no index work, no collection creation.
  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false, serverSelectionTimeoutMS: 15_000 });
  const db = mongoose.connection.db!;
  let redisUsed = false;
  try {
    const now = new Date();
    console.log(`forced refresh · ${args.go ? "GO" : "dry run (nothing is sent; --go to send)"} · ${ist(now)} · this system "${config.systemId}" (${config.hostId})`);

    // ── scope: units by account ──
    let accounts: Array<RefreshAccount & { statuses: string[] }>;
    let scopeLine: string;
    if (args.neverStoredSince) {
      // Index-only units: listed by Flipkart since the date, skipped as old, never stored.
      const rows = (
        await db
          .collection("order_index")
          .find({ userId, scrape_status: "skipped_old", order_date: { $gte: args.neverStoredSince } }, { projection: { platform_email: 1, order_id: 1, unit_id: 1 } })
          .toArray()
      ).map((r) => ({ email: String(r.platform_email || ""), orderId: String(r.order_id || ""), unitId: String(r.unit_id || "") }));
      const stored = new Set<string>();
      const unitIds = [...new Set(rows.map((r) => r.unitId))];
      for (let i = 0; i < unitIds.length; i += 5000) {
        const found = await db.collection("order_details").find({ userId, unit_id: { $in: unitIds.slice(i, i + 5000) } }, { projection: { unit_id: 1 } }).toArray();
        for (const r of found) stored.add(String(r.unit_id));
      }
      accounts = neverStoredAccounts(rows, stored);
      // Staleness order like the main run: the account's newest read of any stored unit (none stored → never read).
      const reads = await db
        .collection("order_details")
        .aggregate<{ _id: string; lastFetch: Date | null }>([
          { $match: { userId, platform_email: { $in: accounts.map((a) => a.email) } } },
          { $group: { _id: "$platform_email", lastFetch: { $max: "$last_fetch" } } },
        ])
        .toArray();
      const readAt = new Map(reads.map((r) => [String(r._id || "").toLowerCase(), r.lastFetch || null]));
      for (const a of accounts) a.lastFetch = readAt.get(a.email) ?? null;
      scopeLine = `never stored since ${ist(args.neverStoredSince)}: ${accounts.reduce((s, a) => s + a.units, 0)} unit(s) · ${accounts.reduce((s, a) => s + a.orders, 0)} order(s) · ${accounts.length} account(s)`;
    } else {
      const rows = await db
        .collection("order_details")
        .aggregate<{ _id: string; units: number; orders: string[]; lastFetch: Date | null; statuses: string[] }>([
          { $match: { userId, status_key: { $in: args.statuses } } },
          { $group: { _id: "$platform_email", units: { $sum: 1 }, orders: { $addToSet: "$order_id" }, lastFetch: { $max: "$last_fetch" }, statuses: { $addToSet: "$status_key" } } },
        ])
        .toArray();
      accounts = rows.map((r) => ({ email: String(r._id || "").toLowerCase(), units: r.units, orders: r.orders.length, lastFetch: r.lastFetch || null, statuses: r.statuses })).filter((a) => a.email);
      scopeLine = `${args.statuses.join(" + ")}: ${accounts.reduce((s, a) => s + a.units, 0)} unit(s) · ${accounts.reduce((s, a) => s + a.orders, 0)} order(s) · ${accounts.length} account(s)`;
    }
    console.log(`\nscope — ${scopeLine}`);

    // ── accounts left out ──
    // The rows' status, session dates and last check only — never their cookies.
    const ids = (await db
      .collection("platformids")
      .find({ userId, email: { $in: accounts.map((a) => a.email) } }, { projection: { email: 1, status: 1, sessionSavedAt: 1, "sessionCheck.at": 1, "sessionCheck.outcome": 1 } })
      .toArray()) as AccountRow[];
    const recentFails = new Set(
      (
        await db
          .collection("order_sync_tasks")
          .distinct("email", { userId, kind: { $in: ["update", "fetch"] }, errorCode: "not_logged_in", createdAt: { $gte: new Date(now.getTime() - 2 * DAY_MS) } })
      ).map((e) => String(e || "").toLowerCase())
    );
    const { notLoggedIn, looksOut, inScope, why } = classifyAccounts(accounts, ids, recentFails);
    const sum = (list: RefreshAccount[]) => list.reduce((s, a) => s + a.units, 0);
    console.log(`left out, not logged in (no logged_in platformids row with a saved session): ${notLoggedIn.length} account(s), ${sum(notLoggedIn)} unit(s)${notLoggedIn.length ? `\n  ${notLoggedIn.map((a) => `${show(a.email)} (${a.units})`).join(" · ")}` : ""}`);
    console.log(
      `left out, LOOK logged out (a not_logged_in update / fetch task in 48 h, or a later session check said so): ${looksOut.length} account(s), ${sum(looksOut)} unit(s) — re-login, then refresh them by hand` +
        (looksOut.length ? `\n  ${looksOut.map((a) => `${show(a.email)} (${a.units}; ${why(a.email)})`).join(" · ")}` : "")
    );
    // The plan's "9 logged-out accounts" counted every account with such a task; the ones holding no unit of this
    // scope have nothing to refresh, so they are only counted here (measured 2026-11-26: 7 with a task, 4 in scope).
    const scopeEmails = new Set(accounts.map((a) => a.email.toLowerCase()));
    const outside = [...recentFails].filter((e) => e && !scopeEmails.has(e)).length;
    if (outside) console.log(`  (+ ${outside} other account(s) with a not_logged_in task in 48 h hold no unit of this scope — nothing to refresh)`);

    // ── batches ──
    const stale = inScope.filter((a) => !a.lastFetch || now.getTime() - new Date(a.lastFetch).getTime() > STALE_DAYS * DAY_MS).length;
    const canary = args.canary > 0 ? pickCanary(inScope, args.canary, now) : [];
    const batches = args.canary > 0
      ? [{ n: 1, emails: canary.map((a) => a.email), accounts: canary.length, units: sum(canary) }]
      : args.neverStoredSince
        ? planOrderBatches(inScope, { now })
        : planBatches(inScope, { perBatch: args.perBatch, now });
    console.log(`\n${args.canary > 0 ? `canary — ${canary.length} account(s) read in the last day with ≤ 15 units in scope (both statuses first)` : `batches — ${inScope.length} account(s), ${sum(inScope)} unit(s); ${stale} account(s) unread for more than ${STALE_DAYS} days go last`}`);
    for (const b of batches) {
      const first = inScope.find((a) => a.email === b.emails[0]);
      const last = inScope.find((a) => a.email === b.emails[b.emails.length - 1]);
      console.log(`  batch ${String(b.n).padStart(2)}: ${String(b.accounts).padStart(4)} account(s) · ${String(b.units).padStart(5)} unit(s)${b.orderIds ? ` · ${b.orderIds.length} order id(s)` : ""} · newest read ${ist(first?.lastFetch)} … ${ist(last?.lastFetch)}${b.n < args.fromBatch ? "  (skipped: --from-batch)" : ""}`);
    }

    // ── the gates --go applies ──
    const { OrderUpdateRequest } = await import("../models/OrderUpdateRequest.js");
    const openSchedules = () => OrderUpdateRequest.countDocuments({ userId, source: { $in: ["schedule", "ofd"] }, status: { $in: ["queued", "running"] } });
    const { workerAlive } = await import("../services/requestCommon.js");
    const { capabilitiesOf, updateCapabilitiesKey } = await import("../services/orderUpdateQueue.js");
    const { UPDATE_QUEUE } = await import("../queue.js");
    const { getRedis } = await import("../lib/redis.js");
    redisUsed = true;
    // Reads of the worker's heartbeat and capability keys in this Mac's Redis (PING, EXISTS, GET) — nothing is
    // written there. workerAlive() answers "alive" when Redis cannot be reached (so a request is never shown stalled by
    // mistake); this gate must fail closed instead, so Redis answers a PING first. And the worker must run code that
    // honours quiet_events (sub-step 10, stamped by the update worker): an older worker would write the refresh's
    // events unprocessed and the dashboard would raise the address alerts quiet mode exists to prevent.
    const updateWorkerState = async (): Promise<{ alive: boolean; quiet: boolean }> => {
      try {
        const pong = await Promise.race([getRedis().ping(), new Promise<string>((_, no) => setTimeout(() => no(new Error("timeout")), 5_000).unref())]);
        if (pong !== "PONG" || !(await workerAlive(UPDATE_QUEUE))) return { alive: false, quiet: false };
        return { alive: true, quiet: capabilitiesOf(await getRedis().get(updateCapabilitiesKey(UPDATE_QUEUE))).includes("quiet_events") };
      } catch {
        return { alive: false, quiet: false };
      }
    };
    const updateWorkerAlive = async () => {
      const w = await updateWorkerState();
      return w.alive && w.quiet;
    };
    const worker = await updateWorkerState();
    const alive = worker.alive && worker.quiet;
    const scheduleOpen = await openSchedules();
    console.log(
      `\ngates — update worker alive on this system: ${worker.alive ? "yes" : "NO (--go refuses)"} · it honours quiet_events: ${worker.alive ? (worker.quiet ? "yes" : "NO — older code, restart it with sub-step 10 (R3); --go refuses") : "—"} · open schedule / out-for-delivery requests: ${scheduleOpen}${scheduleOpen ? " (--go waits)" : ""}`
    );
    console.log(`stops before the next batch on: > ${FAILED_SHARE_STOP * 100} % failed accounts · ${STOP_NOTES.map((n) => `"${n}"`).join(" / ")} in an account message · a stopped request · no worker heartbeat · a batch that cannot be created · Ctrl-C`);
    if (!args.go) {
      console.log(`\ndry run — nothing sent. ${batches.length} request(s) would be made (source manual, by ${FORCED_REFRESH_BY}, force, quiet_events).`);
      return;
    }

    // ── --go ──
    if (!worker.alive) throw new Error(`refused: no update worker is alive on this system (${config.hostId}) — the requests' jobs go to this Mac's Redis; restart the worker with sub-step 10 first`);
    if (!alive) throw new Error(`refused: the update worker on this system (${config.hostId}) runs code without quiet_events — its events would raise alerts; restart it with sub-step 10 (R3) first`);
    if (!batches.length || !batches[0].accounts) {
      console.log("nothing to refresh");
      return;
    }
    const { createUpdateRequest } = await import("../services/orderUpdateQueue.js");
    const { Order } = await import("../models/Order.js");
    const statusesToUpdate = args.statuses;
    const create = async (b: RefreshBatch) => {
      const params = {
        platform: "FLIPKART",
        selection_method: b.orderIds ? "order_ids" : "order_status_and_email_ids",
        force: true,
        quiet_events: true,
        purpose: FORCED_REFRESH_PURPOSE,
        statuses_to_update: b.orderIds ? [] : statusesToUpdate,
        email_ids_count: b.orderIds ? 0 : b.emails.length,
        order_ids_count: b.orderIds ? b.orderIds.length : 0,
        batch: b.n,
        batches: batches.length,
        ...(args.canary > 0 ? { canary: true } : {}),
      };
      const input = b.orderIds
        ? { orderIds: b.orderIds, selectionMethod: "order_ids" }
        : { emails: b.emails, orderStatus: statusesToUpdate, selectionMethod: "order_status_and_email_ids" };
      const req = await createUpdateRequest(String(userId), input, { source: "manual", by: FORCED_REFRESH_BY, params });
      return { id: String(req.id) };
    };
    const poll = async (id: string) => (await OrderUpdateRequest.findById(id).select("status accounts.status accounts.message").lean()) as PolledRequest | null;
    let stop = false;
    process.on("SIGINT", () => {
      if (stop) process.exit(130);
      stop = true;
      console.log("\nCtrl-C — stopping before the next batch (the running request goes on: Stop it in Batch requests). Ctrl-C again quits now.");
    });

    // The canary's before/after counts.
    const snap = async (emails: string[]) => {
      const rows = await Order.collection.find({ userId, platform_email: { $in: emails }, status_key: { $in: statusesToUpdate } }, { projection: Object.fromEntries([["unit_id", 1], ["order_id", 1], ...CANARY_FIELDS.map((f) => [f, 1])]) }).toArray();
      return new Map(rows.map((r) => [String(r.unit_id), r as Record<string, unknown>]));
    };
    const before = args.canary > 0 ? await snap(batches[0].emails) : null;
    const canaryOrders = before ? [...new Set([...before.values()].map((r) => String(r.order_id || "")).filter(Boolean))] : [];
    const runStart = new Date();
    let canaryFailed = false;

    const result = await runBatches(batches, {
      create,
      poll,
      openSchedules,
      sleep: (ms) => new Promise((r) => setTimeout(r, ms)),
      log: (line) => console.log(`[${ist(new Date())}] ${line}`),
      stopRequested: () => stop,
      workerAlive: updateWorkerAlive,
      afterBatch: async (b) => {
        if (!before) return;
        // The dashboard raises alerts from unit events on its minute poll: wait for one full poll before counting.
        console.log(`[${ist(new Date())}] canary: waiting 90 s for the dashboard's alert poll`);
        await new Promise((r) => setTimeout(r, 90_000));
        // Units re-read before the status filter: a unit that moved status is still counted by its unit id.
        const rows = await Order.collection.find({ userId, unit_id: { $in: [...before.keys()] } }, { projection: Object.fromEntries([["unit_id", 1], ...CANARY_FIELDS.map((f) => [f, 1])]) }).toArray();
        const after = new Map(rows.map((r) => [String(r.unit_id), r as Record<string, unknown>]));
        const counts = fieldChanges(before, after);
        console.log(`\ncanary field counts over ${before.size} unit(s) of batch ${b.n} (changed / went blank / filled):`);
        for (const f of CANARY_FIELDS) console.log(`  ${f.padEnd(22)} ${String(counts[f].changed).padStart(5)} / ${counts[f].blanked} / ${counts[f].filled}`);
        const cancelled = [...before].filter(([, r]) => r.status_key === "Cancelled");
        const blank = (v: unknown) => v === null || v === undefined || v === "";
        const cancelledRefundFilled = cancelled.filter(([u, r]) => blank(r.refund_status) && !blank(after.get(u)?.refund_status)).length;
        // New open alerts of the canary: on its own orders, and on its accounts (an account-level alert such as
        // account.unreadable carries the e-mail and no order id).
        const newOpenAlerts = await db.collection("alerts").countDocuments({ userId, acknowledgedAt: null, createdAt: { $gte: runStart }, $or: canaryAlertScope(canaryOrders, b.emails) });
        const verdict = canaryVerdict(counts, { units: before.size, cancelledUnits: cancelled.length, cancelledRefundFilled, newOpenAlerts });
        for (const line of verdict.lines) console.log(`  ${line}`);
        if (verdict.fail) canaryFailed = true;
        console.log(verdict.fail ? "  CANARY FAIL — do not start OPS-12" : "  canary gate: pass (read the REVIEW line, if any, before OPS-12)");
      },
    });
    console.log(result.stopped ? `\nSTOPPED — ${result.stopped}.${result.resumeAt ? ` Resume with --from-batch ${result.resumeAt} once it is understood.` : ""}` : `\nall ${result.ran.length} batch(es) ran.`);
    if (result.stopped) process.exitCode = 2;
    if (canaryFailed) process.exitCode = 3;
  } finally {
    await mongoose.disconnect();
    if (redisUsed) {
      const { getRedis } = await import("../lib/redis.js");
      await getRedis().quit().catch(() => undefined);
    }
  }
}

const invokedAs = process.argv[1] ? path.resolve(process.argv[1]) : "";
if (invokedAs && fileURLToPath(import.meta.url) === invokedAs) {
  main()
    .then(() => process.exit(process.exitCode ?? 0))
    .catch((err) => {
      console.error(err instanceof Error ? err.message : err);
      process.exit(1);
    });
}
