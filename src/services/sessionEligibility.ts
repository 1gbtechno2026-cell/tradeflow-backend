import type { Types } from "mongoose";
import { OrderUnitEvent } from "../models/OrderUnitEvent.js";
import { PlatformId } from "../models/PlatformId.js";
import { SessionDemotion } from "../models/SessionDemotion.js";

/**
 * The one hand-off for a dead Flipkart session. Every Trade Flow flow that
 * finds an account logged out reports it here — update, fetch, checkout,
 * cancel and reschedule, which can confirm it in Chrome, and coupons and the
 * invoice batch, which cannot and only report — so all of them are held to the
 * same rules. A report is decided in this order:
 *
 *   1. the row is not logged_in (or is gone, or now holds another session
 *      than the one the flow tested): nothing is written and no event is
 *      raised — the row is already on the Not-logged-in list and keeps the
 *      reason that put it there
 *   2. not confirmed (an HTTP-only flow): status left unchanged
 *   3. the session was saved less than 24 h ago: status left unchanged — the
 *      dashboard's rule (sessionVerifyHttp.ts FRESH_SESSION_MS); it also stops a
 *      login-then-demote loop
 *   4. the caller's run circuit is open (DemoteCircuit below): status left
 *      unchanged
 *   5. SESSION_DEMOTE_CAP (default 10) Trade Flow demotions of this workspace
 *      in the last 6 h: status left unchanged. They are counted in their own
 *      Mongo record (models/SessionDemotion.ts), shared by both Macs, that a
 *      login or an Undo never clears; a slot is taken there before the row is
 *      written, so reports that run at once cannot pass the cap together
 *   6. otherwise demote: logged_in → pending, cookies kept, statusBeforeDemote
 *      "logged_in", demotedAt now, demotedBy "trade-flow:<flow>"
 *
 * Every write is filtered on { _id, status: "logged_in", sessionSavedAt: the
 * session tested }, so a login that lands in the meantime is never touched. A
 * refused demotion writes only lastChecked and sessionCheck (Session Health
 * shows the row as Suspect). Each report raises an `account_unreadable` event —
 * the dashboard's "Needs attention" alert — but an unconfirmed one at most once
 * per account per 24 h; a confirmed one always.
 *
 * Undo: demotedAt is set, so the dashboard's Session Health → Undo
 * (sessionVerifyHttp.ts undoDemotions, last 24 h) puts a demoted row back to
 * logged_in — as long as it still has its cookies and was not logged in again.
 * An undone demotion still counts against the cap for its 6 h.
 *
 * No login happens here. The dashboard owns the one login engine
 * (platformJob.ts → resolvePriorityLogin / startPlatformJobForIds); once this
 * row is pending, that engine logs it back in — on the Not-logged-in panel's
 * "Log in these", Session Health's Log in button, or the auto-login loop when
 * its switch is on. No flow writes platformids itself.
 */

/** The flows that report — a demotion records demotedBy "trade-flow:<flow>". "test" is the check scripts'. */
export type SessionFlow = "update" | "fetch" | "checkout" | "cancel" | "reschedule" | "coupon" | "invoice" | "remediation" | "test";

/** The default sentence of a SessionDeadError — read as "not logged in" by the task log (requestCommon.ts classifyMessage). */
export const SESSION_DEAD_MESSAGE = "Flipkart session expired — Flipkart sent this ID to the login page twice; re-login this ID";

/** A flow confirmed the account's session dead (two login landings): thrown to stop the account at once. */
export class SessionDeadError extends Error {
  constructor(message: string = SESSION_DEAD_MESSAGE) {
    super(message);
    this.name = "SessionDeadError";
  }
}

const OBJECT_ID_RE = /^[a-f0-9]{24}$/i;

/** A session saved more recently than this is never demoted (the dashboard's rule). */
export const FRESH_SESSION_MS = 24 * 60 * 60_000;
/** The demotion cap counts Trade Flow demotions over this rolling window. */
export const DEMOTE_WINDOW_MS = 6 * 60 * 60_000;
/** An unconfirmed report raises its event at most this often per account. */
export const UNCONFIRMED_EVENT_EVERY_MS = 24 * 60 * 60_000;
const DEFAULT_DEMOTE_CAP = 10;

/** SESSION_DEMOTE_CAP — Trade Flow demotions allowed per workspace in any rolling 6 h (default 10; 0 = never demote). Read on every call. */
export function sessionDemoteCap(): number {
  const raw = String(process.env.SESSION_DEMOTE_CAP ?? "").trim();
  const n = raw ? Number(raw) : NaN;
  return Number.isFinite(n) && n >= 0 ? Math.floor(n) : DEFAULT_DEMOTE_CAP;
}

/** What a report adds to its reason when the status was left as it is. A flow appends `note` to its own message. */
export const SESSION_NOTES = {
  unconfirmed: " [unconfirmed — status left unchanged]",
  fresh: " [session saved < 24 h ago — status left unchanged]",
  circuit: " [circuit open — status left unchanged]",
  cap: " [demotion cap reached — status left unchanged]",
  replaced: " [session replaced since the check — status left unchanged]",
  notFound: " [Platform ID not found — nothing written]",
  failed: " [could not be recorded — status left unchanged]",
  notLoggedIn: (status: string) => ` [status already "${status}" — nothing written]`,
} as const;

export interface DeadSessionReport {
  userId: Types.ObjectId | string;
  /** The row's _id when the flow has it; otherwise (or when it is not an ObjectId) the row is found by workspace + email. */
  platformId?: Types.ObjectId | string | null;
  email: string;
  /** What the flow saw, as one sentence ("Update: … — re-login"). The decision's note is appended to it. */
  reason: string;
  flow: SessionFlow;
  /**
   * true only when the flow confirmed it itself: Flipkart sent the page to the
   * login page, confirmLoginLanding (browser.ts — the cookies attached again)
   * landed there too, and the account read nothing in this run. HTTP-only
   * flows pass false: they report, they never demote.
   */
  confirmed: boolean;
  /** sessionSavedAt of the session the flow tested (resolveLoggedInSession), null when the row had none. */
  sessionSavedAt: Date | null;
  /** The caller's per-run circuit is open (DemoteCircuit.open). */
  circuitOpen?: boolean;
  /** How the flow read the account — "worker" (default), "api", "page", "manual: …". The event keeps it as given; sessionCheck.via is the dashboard's enum ("manual" | "worker"). */
  via?: string;
  requestId?: string;
}

export interface DeadSessionOutcome {
  /** The row went logged_in → pending (cookies kept; Undo restores it within 24 h). */
  demoted: boolean;
  /** Why it did not ("" when it did) — one of SESSION_NOTES, already appended to the recorded reason. */
  note: string;
}

/** The dashboard's sessionCheck.via is an enum: a manual run says so, everything else is a worker. */
const checkVia = (via: string): "manual" | "worker" => (/^manual/i.test(via) ? "manual" : "worker");

/**
 * Trade Flow demotions of this workspace in the last 6 h — what
 * SESSION_DEMOTE_CAP is held against. Read from their own record
 * (models/SessionDemotion.ts), not from the rows: a login or an Undo clears a
 * row's demotedAt, and the demotion still happened.
 */
export async function recentTradeFlowDemotions(userId: Types.ObjectId | string, now = new Date()): Promise<number> {
  return SessionDemotion.countDocuments({ userId, at: { $gte: new Date(now.getTime() - DEMOTE_WINDOW_MS) } });
}

/**
 * Rows still stamped by a Trade Flow demotion of the last 6 h. A Trade Flow
 * that records no slots yet — a worker not restarted since this change —
 * leaves only these behind, so they are a floor under the record's count.
 */
async function stampedTradeFlowDemotions(userId: Types.ObjectId | string, now: Date): Promise<number> {
  return PlatformId.countDocuments({ userId, demotedBy: /^trade-flow:/, demotedAt: { $gte: new Date(now.getTime() - DEMOTE_WINDOW_MS) } });
}

/** How often a report that met others taking slots at the same moment tries again (after a random pause). */
const SLOT_ATTEMPTS = 5;

/**
 * Take one of the cap's slots for a demotion, before the row is written — or
 * null when the window is full. The slot is counted again once it is written,
 * and kept only if the window, with it, is still within the cap: reports that
 * take one at the same moment (fetch lanes, update windows, the other Mac) each
 * see the others, so more than the cap never go through. When they all step
 * back at once, each tries again after a random pause. Stamped rows count too
 * when they are more (demotions made without a slot).
 */
async function reserveDemotionSlot(entry: { userId: Types.ObjectId | string; platformId: unknown; email: string; flow: SessionFlow; requestId: string; at: Date }): Promise<Types.ObjectId | null> {
  const cap = sessionDemoteCap();
  if (cap <= 0) return null;
  const stamped = await stampedTradeFlowDemotions(entry.userId, entry.at);
  for (let attempt = 1; attempt <= SLOT_ATTEMPTS; attempt++) {
    if (Math.max(await recentTradeFlowDemotions(entry.userId, entry.at), stamped) >= cap) return null;
    const slot = await SessionDemotion.create({ userId: entry.userId, platformId: entry.platformId ?? null, email: entry.email, flow: entry.flow, requestId: entry.requestId, at: entry.at });
    if (Math.max(await recentTradeFlowDemotions(entry.userId, entry.at), stamped + 1) <= cap) return slot._id;
    await giveBackDemotionSlot(slot._id);
    if (attempt < SLOT_ATTEMPTS) await new Promise((r) => setTimeout(r, 100 + Math.floor(Math.random() * 400)));
  }
  return null;
}

/** The demotion a slot was taken for did not happen. */
async function giveBackDemotionSlot(slot: Types.ObjectId): Promise<void> {
  await SessionDemotion.deleteOne({ _id: slot }).catch(() => undefined);
}

/**
 * Report a dead session (see the rules above). Never throws: a health write
 * must never break the run that found it.
 */
export async function markSessionEligibleForRelogin(opts: DeadSessionReport): Promise<DeadSessionOutcome> {
  const email = String(opts.email || "").trim().toLowerCase();
  const via = String(opts.via || "worker");
  const tested = opts.sessionSavedAt ? new Date(opts.sessionSavedAt) : null;
  const now = new Date();
  try {
    const id = opts.platformId != null && OBJECT_ID_RE.test(String(opts.platformId)) ? String(opts.platformId) : "";
    const row = await PlatformId.findOne(id ? { _id: id, userId: opts.userId } : { userId: opts.userId, platform: "flipkart", email })
      .select("email status sessionSavedAt")
      .lean();
    // 1. Already off the logged-in side, or gone: it keeps the reason that put it there.
    if (!row) return { demoted: false, note: SESSION_NOTES.notFound };
    if (row.status !== "logged_in") return { demoted: false, note: SESSION_NOTES.notLoggedIn(String(row.status)) };
    // The flow tested another session than the one stored now — a login landed after its read.
    const stored = row.sessionSavedAt ? new Date(row.sessionSavedAt).getTime() : null;
    if (stored !== (tested ? tested.getTime() : null)) return { demoted: false, note: SESSION_NOTES.replaced };

    let note = "";
    if (opts.confirmed !== true) note = SESSION_NOTES.unconfirmed;
    else if (stored !== null && now.getTime() - stored < FRESH_SESSION_MS) note = SESSION_NOTES.fresh;
    else if (opts.circuitOpen) note = SESSION_NOTES.circuit;
    // 5. The cap: a slot is taken before the row is written, and given back below if the write does not happen.
    let slot: Types.ObjectId | null = null;
    if (!note) {
      slot = await reserveDemotionSlot({ userId: opts.userId, platformId: row._id, email: row.email || email, flow: opts.flow, requestId: opts.requestId || "", at: now });
      if (!slot) note = SESSION_NOTES.cap;
    }
    const demote = !note;

    const reason = `${String(opts.reason || "").trim().slice(0, 300)}${note}`;
    const sessionCheck = { at: now, outcome: "not_logged_in", reason, via: checkVia(via) };
    let res: { matchedCount: number };
    try {
      res = await PlatformId.updateOne(
        { _id: row._id, status: "logged_in", sessionSavedAt: tested },
        {
          $set: demote
            ? { status: "pending", statusBeforeDemote: "logged_in", demotedAt: now, demotedBy: `trade-flow:${opts.flow}`, lastChecked: now, sessionCheck }
            : { lastChecked: now, sessionCheck },
        }
      );
    } catch (err) {
      if (slot) await giveBackDemotionSlot(slot);
      throw err;
    }
    // A login landed between the read above and this write: that session was not the one tested.
    if (!res.matchedCount) {
      if (slot) await giveBackDemotionSlot(slot);
      return { demoted: false, note: SESSION_NOTES.replaced };
    }
    if (demote) console.log(`[session] ${row.email || email}: moved to pending (trade-flow:${opts.flow}) — ${reason}`);

    // The dashboard's alert. An unconfirmed report repeats every run the account is in: one event a day is enough.
    const quiet =
      opts.confirmed !== true &&
      Boolean(
        await OrderUnitEvent.exists({ userId: opts.userId, unitId: "", type: "account_unreadable", email: row.email || email, at: { $gte: new Date(now.getTime() - UNCONFIRMED_EVENT_EVERY_MS) } }).catch(() => null)
      );
    if (!quiet) {
      await OrderUnitEvent.create({
        userId: opts.userId,
        orderId: "",
        unitId: "",
        email: row.email || email,
        productName: "",
        type: "account_unreadable",
        at: now,
        before: null,
        after: { reason, via, flow: opts.flow, demoted: demote, needsRelogin: true },
        requestId: opts.requestId || "",
        processed: false,
        processedAt: null,
      }).catch(() => undefined);
    }
    return { demoted: demote, note };
  } catch (err) {
    console.warn(`[session] ${email}: dead-session report not recorded — ${err instanceof Error ? err.message.split("\n")[0] : err}`);
    return { demoted: false, note: SESSION_NOTES.failed };
  }
}

// ── the per-run circuit ─────────────────────────────────────────────────────

/** The dashboard checker's numbers (sessionVerifyHttp.ts CIRCUIT_WINDOW, note()): the last 50 accounts… */
export const CIRCUIT_WINDOW = 50;
/** …judged once 20 are in… */
export const CIRCUIT_JUDGED_FROM = 20;
/** …open when more than 30 % of them were dead. */
export const CIRCUIT_DEAD_SHARE = 0.3;

/** How one account of a run ended: read something, confirmed dead, or neither (failed otherwise, skipped). */
export type CircuitOutcome = "alive" | "dead" | "other";

/**
 * One run's circuit. A run that finds too many accounts dead at once is more
 * likely seeing a Flipkart outage than many logouts: once open, the run demotes
 * nothing more (pass `circuitOpen: circuit.open`). It stays open for the rest of
 * the run, as the dashboard's does. Push each finished account first, then read
 * `open` for its own report — the dashboard's order.
 */
export class DemoteCircuit {
  private readonly recent: CircuitOutcome[] = [];
  private tripped = false;

  /** Record one finished account. Returns whether the circuit is open now. */
  push(outcome: CircuitOutcome): boolean {
    this.recent.push(outcome);
    if (this.recent.length > CIRCUIT_WINDOW) this.recent.shift();
    if (!this.tripped && this.recent.length >= CIRCUIT_JUDGED_FROM && this.deadShare > CIRCUIT_DEAD_SHARE) this.tripped = true;
    return this.tripped;
  }

  get open(): boolean {
    return this.tripped;
  }

  /** Accounts in the window (at most CIRCUIT_WINDOW). */
  get size(): number {
    return this.recent.length;
  }

  get dead(): number {
    return this.recent.filter((x) => x === "dead").length;
  }

  get deadShare(): number {
    return this.recent.length ? this.dead / this.recent.length : 0;
  }

  /** For a log line: "7 of the last 21 accounts dead (33 %) — circuit open". */
  describe(): string {
    return `${this.dead} of the last ${this.size} accounts dead (${Math.round(this.deadShare * 100)} %)${this.tripped ? " — circuit open" : ""}`;
  }
}

const circuits = new Map<string, DemoteCircuit>();

/** The circuit of one run (an update request id, a fetch run), made on first use. Forget it when the run ends. */
export function demoteCircuitFor(runKey: string): DemoteCircuit {
  let c = circuits.get(runKey);
  if (!c) {
    c = new DemoteCircuit();
    circuits.set(runKey, c);
  }
  return c;
}

export function forgetDemoteCircuit(runKey: string): void {
  circuits.delete(runKey);
}
