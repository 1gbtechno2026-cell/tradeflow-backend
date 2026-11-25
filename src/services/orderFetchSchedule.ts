import { Types } from "mongoose";
import { config } from "../config.js";
import { OrderJobRun } from "../models/OrderJobRun.js";
import { OrderSyncSettings } from "../models/OrderSyncSettings.js";
import { PlatformId } from "../models/PlatformId.js";
import { getOrderFetchJob, isOrderFetchPreparing, OrderFetchStoppedError, reserveOrderFetch, startOrderFetch, stopOrderFetch } from "./orderFetch.js";
import { createQueuedRun, failRunById, stopRunById } from "./orderJobRuns.js";
import {
  attachRun,
  claimManual,
  cronOf,
  markStarted,
  releaseLease,
  runRefState,
  scheduleDefaults,
  slotStamp,
  startScheduleClock,
  supersededText,
  type LeaseHandle,
  type ManualClaim,
  type RunState,
  type ScheduleClaim,
  type ScheduleClock,
  type ScheduleDeps,
  type ScheduleRunner,
} from "./scheduleClock.js";
import { eligibleHere, scheduleStatuses, seedSchedules, servedBy } from "./scheduleSettings.js";
// startRequests reads this module's look-back and account list; both sides only call each other inside functions.
import { queueUpdateAfterFetch } from "./startRequests.js";

/**
 * The scheduled Fetch — the reference tool's "system-cron" row, "nightly
 * fetch" in Settings.
 *
 * One Fetch over every logged-in Flipkart Platform ID of the workspace,
 * looking back `fetchSinceDays` (dashboard setting, else FETCH_SINCE_DAYS,
 * else 60), with trigger "system-cron" on the panel row and the ledger. It
 * uses the same startOrderFetch the button uses, so the dashboard's mode
 * switch, windows and invoice step apply unchanged. When the run ends an
 * update of the in-progress units is queued, as on the reference ("Follows a
 * nightly fetch").
 *
 * WHEN it runs is the shared schedule clock's (services/scheduleClock.ts), in
 * the API process, since a fetch lives in the API:
 *
 *   the switch       "Nightly fetch" in Settings → Scheduled fetch
 *                    (order_sync_settings.schedules.fetch), shared by every
 *                    machine. Off until somebody switches it on.
 *   the time         the clock's — daily at 05:00 India time unless the first
 *                    seed read another "M H * * *" from FETCH_SCHEDULE.
 *   which machine    one that MAY run it: FETCH_SCHEDULE in its .env is
 *                    anything but 0 / off / unset. FETCH_SCHEDULE=0 means
 *                    "this machine never runs the nightly fetch" — it does not
 *                    switch the schedule off, and no value switches it on.
 *   one run          the lease: a machine takes the night, renews its lease
 *                    while the fetch runs here, and gives it back when the
 *                    fetch ends. A night whose fetch died with its API (a
 *                    restart) or whose machine went quiet is started ONCE more
 *                    within 3 h of the time; never a third time.
 *   not twice        a tick is skipped while a fetch — anybody's — is running
 *                    in this process, or was seen running on another machine
 *                    in the last 10 minutes (its ledger row, which a fetch
 *                    touches once a minute).
 *
 * "Run the scheduled fetch now" takes the same lease by hand: it works
 * whatever the switch says and on a machine the schedule never uses, and is
 * refused (409) while another machine holds the lease.
 *
 *   FETCH_SINCE_DAYS=60         default look-back when no dashboard setting
 *   FETCH_SCHEDULE_INVOICES=0   default for the "download invoices" switch when no dashboard setting
 *
 * Invoices: the dashboard's Settings → Scheduled fetch toggle decides whether
 * the run fetches with invoices "missing" (every delivered unit of each ID
 * without a PDF, INVOICE_MAX_PER_RUN per ID per run) or "off". Stored on the
 * same order_sync_settings row as the look-back.
 */

const RAW = String(process.env.FETCH_SCHEDULE ?? "").trim();
/** FETCH_SCHEDULE as a cron line — only what GET /fetch/schedule answers for a workspace that has no clock yet. */
export const FETCH_SCHEDULE_CRON = /^\d{1,2}\s+\d{1,2}\s+\*\s+\*\s+\*$/.test(RAW) ? RAW : "0 5 * * *";
export const FETCH_TIMEZONE = process.env.CRON_TIMEZONE || "Asia/Kolkata";
const DEFAULT_SINCE_DAYS = Math.max(1, Number(process.env.FETCH_SINCE_DAYS) || 60);
const DEFAULT_INVOICES = ["1", "on", "true", "yes", "missing"].includes(String(process.env.FETCH_SCHEDULE_INVOICES || "0").trim().toLowerCase());

/** A run started by hand in a workspace that has no clock (nothing to write its tick on): the last one, in this process. */
let lastLocalTick: { at: Date; outcome: string } | null = null;

export async function resolveFetchSinceDays(userId: string): Promise<{ days: number; source: "ui" | "env" }> {
  const row = Types.ObjectId.isValid(userId) ? await OrderSyncSettings.findOne({ userId }).lean().catch(() => null) : null;
  const ui = Math.floor(Number(row?.fetchSinceDays || 0));
  return ui >= 1 ? { days: Math.min(365, ui), source: "ui" } : { days: DEFAULT_SINCE_DAYS, source: "env" };
}

export async function setFetchSinceDays(userId: string, days: unknown, by: string) {
  const n = Math.floor(Number(days));
  if (!Number.isFinite(n) || n < 1 || n > 365) throw new Error("fetchSinceDays must be 1–365");
  await OrderSyncSettings.updateOne({ userId }, { $set: { fetchSinceDays: n, fetchScheduleChangedBy: by || "", fetchScheduleChangedAt: new Date() }, $setOnInsert: { userId } }, { upsert: true });
  return resolveFetchSinceDays(userId);
}

export async function resolveScheduledInvoices(userId: string): Promise<{ enabled: boolean; source: "ui" | "env"; changedBy: string; changedAt: Date | null }> {
  const row = Types.ObjectId.isValid(userId) ? await OrderSyncSettings.findOne({ userId }).lean().catch(() => null) : null;
  if (typeof row?.scheduledInvoices === "boolean") return { enabled: row.scheduledInvoices, source: "ui", changedBy: row.scheduledInvoicesChangedBy || "", changedAt: row.scheduledInvoicesChangedAt || null };
  return { enabled: DEFAULT_INVOICES, source: "env", changedBy: "", changedAt: null };
}

export async function setScheduledInvoices(userId: string, enabled: unknown, by: string) {
  if (typeof enabled !== "boolean") throw new Error("scheduledInvoices must be true or false");
  await OrderSyncSettings.updateOne({ userId }, { $set: { scheduledInvoices: enabled, scheduledInvoicesChangedBy: by || "", scheduledInvoicesChangedAt: new Date() }, $setOnInsert: { userId } }, { upsert: true });
  console.log(`[fetch-schedule] invoices ${enabled ? "ON (all missing)" : "OFF"}${by ? ` by ${by}` : ""}`);
  return resolveScheduledInvoices(userId);
}

/** Logged-in Flipkart Platform IDs with a saved session. Cookie blobs are ~19 KB each,
 *  so the filter runs in the query (does the array have an element?) and only emails come back. */
const WITH_SESSION = { userId: "", platform: "flipkart", status: "logged_in", sessionSavedAt: { $ne: null }, $or: [{ "sessionState.cookies.0": { $exists: true } }, { "sessionCookies.0": { $exists: true } }] };

export async function scheduledFetchEmails(userId: string): Promise<string[]> {
  const rows = await PlatformId.find({ ...WITH_SESSION, userId }).select("email").sort({ createdAt: 1 }).lean();
  return rows.map((r) => r.email);
}

export async function scheduledFetchCount(userId: string): Promise<number> {
  return PlatformId.countDocuments({ ...WITH_SESSION, userId });
}

// ── the run ─────────────────────────────────────────────────────────────────

/** What a fetch another process of this machine was running reads once that process is gone (orderJobRuns.closeInterruptedFetchRuns). */
const FETCH_INTERRUPTED = "The API restarted while this fetch was running — start it again";
/** A fetch touches its ledger row this often; another machine counts it as running for FETCH_SEEN_MS after the last touch. */
export const FETCH_SEEN_EVERY_MS = 60_000;
export const FETCH_SEEN_MS = 10 * 60_000;

/**
 * What the scheduled fetch reaches outside Mongo and the lease. The API passes
 * nothing; scripts/fetchScheduleCheck.ts passes a fetch that opens no Chrome
 * and calls nothing, and throwaway schedule collections.
 */
export interface FetchScheduleDeps {
  /** Is a fetch running, or being prepared, in this process? */
  busy: (userId: string) => boolean;
  /** The workspace's one fetch slot, taken ahead of the start. */
  reserve: typeof reserveOrderFetch;
  start: typeof startOrderFetch;
  stop: typeof stopOrderFetch;
  /** The accounts a scheduled fetch reads. */
  emails: (userId: string) => Promise<string[]>;
  /** The update that follows a nightly fetch. */
  afterFetch: typeof queueUpdateAfterFetch;
  /** The clocks and leases, and the ledger (`Runs`). */
  schedule: ScheduleDeps;
}

export const fetchScheduleDefaults: FetchScheduleDeps = {
  busy: (userId) => getOrderFetchJob(userId).running || isOrderFetchPreparing(userId),
  reserve: (userId) => reserveOrderFetch(userId),
  start: (userId, emails, since, opts) => startOrderFetch(userId, emails, since, opts),
  stop: (userId, reason) => stopOrderFetch(userId, reason),
  emails: (userId) => scheduledFetchEmails(userId),
  afterFetch: (userId, fetch) => queueUpdateAfterFetch(userId, fetch),
  schedule: scheduleDefaults,
};

export interface ScheduledFetchResult {
  started: boolean;
  /** "started — …", "skipped — …", "could not start: …", or who holds the lease. */
  message: string;
  emails?: number;
  sinceDays?: number;
  invoices?: "missing" | "off";
  /** The ledger run of a fetch that started. */
  runId?: string;
  /** The bare reason of a skip or a failure — what the schedule's tick says. */
  reason?: string;
  /** It could not start (an error), as opposed to being skipped on purpose. */
  failed?: boolean;
  /** A run by hand the lease refused: another machine is running the schedule (held_elsewhere), or this one is. */
  code?: "held_elsewhere" | "running_here";
  holder?: Extract<ManualClaim, { ok: false }>["holder"];
}

/**
 * A fetch of another machine that is running now: an open fetch row that names
 * another host and was written in the last 10 minutes. A fetch of this code
 * touches its row once a minute (touchRunningFetch); a row nothing touches is
 * one whose process is gone, or a machine on older code — seen for the first
 * 10 minutes after its start only.
 */
export async function fetchRunningElsewhere(userId: string, deps: FetchScheduleDeps = fetchScheduleDefaults): Promise<{ runId: string; host: string; by: string } | null> {
  if (!Types.ObjectId.isValid(userId)) return null;
  const since = new Date(deps.schedule.now().getTime() - FETCH_SEEN_MS);
  const row = await deps.schedule.Runs.findOne({ userId, type: "fetch", status: { $in: ["queued", "running"] }, updatedAt: { $gte: since }, "meta.host": { $exists: true, $nin: [null, "", config.hostId] } })
    .sort({ updatedAt: -1 })
    .select("by trigger meta.host")
    .lean();
  if (!row) return null;
  return { runId: String(row._id), host: String((row.meta as { host?: unknown }).host), by: row.by || (row.trigger === "system-cron" ? "the schedule" : "a user") };
}

/**
 * (API, once a minute) A fetch running in this process — the nightly one, the
 * Fetch button's, a retry — says it is alive: `updatedAt` on its ledger row.
 * Nothing else writes that row between a fetch's start and its end, and it is
 * what another machine's nightly fetch looks at before it starts.
 */
export async function touchRunningFetch(userId: string, deps: FetchScheduleDeps = fetchScheduleDefaults): Promise<number> {
  if (!Types.ObjectId.isValid(userId) || !deps.busy(userId)) return 0;
  const res = await deps.schedule.Runs.updateMany(
    { userId, type: "fetch", status: { $in: ["queued", "running"] }, "meta.host": config.hostId, "meta.pid": process.pid },
    { $set: { updatedAt: deps.schedule.now() } },
    { timestamps: false }
  );
  return res.modifiedCount || 0;
}

/**
 * The scheduled fetch of ONE API process: what it is preparing or running
 * (per workspace), the run itself, and the runner the schedule clock calls.
 * The API has one (below); the check makes one per process it plays.
 */
export function createScheduledFetch(overrides: Partial<FetchScheduleDeps> = {}) {
  /** The leased fetch this process is preparing or running: its ledger run and the claim it runs under. */
  const current = new Map<string, { runId: string; token: string }>();
  const depsNow = (): FetchScheduleDeps => ({ ...fetchScheduleDefaults, ...overrides });

  /**
   * One scheduled run. With `claim` it is a fixed time the clock took: what
   * this answers becomes the time's tick, written by the clock. Without, it is
   * "Run the scheduled fetch now": the lease is taken by hand here (refused
   * while another holder has it) and the ticks are written here. In order:
   * the lease · a fetch running in this process · a fetch running on another
   * machine · the accounts · the start.
   */
  async function run(userId: string, by = "schedule", claim: ScheduleClaim | null = null): Promise<ScheduledFetchResult> {
    const deps = depsNow();
    const byHand = !claim;
    let lease: LeaseHandle | null = claim;
    if (!lease) {
      const taken = await claimManual(userId, "fetch", by, { deps: deps.schedule });
      if (taken.ok) lease = taken.claim;
      // A workspace Trade Flow has not set a clock up for has no lease to take: the run goes ahead as it always did.
      else if (taken.code !== "no_clock") {
        console.log(`[fetch-schedule] run now by ${by} refused — ${taken.message}`);
        return { started: false, message: taken.message, reason: taken.message, code: taken.code, holder: taken.holder };
      }
    }
    // A fixed time's tick is the clock's to write, from what this answers; a run by hand writes its own on the lease
    // it took — or, in a workspace without a clock, keeps it in this process.
    const note = async (write: (handle: LeaseHandle) => Promise<unknown>, outcome: string) => {
      if (!byHand) return;
      if (lease) await write(lease).catch((err) => console.warn(`[fetch-schedule] the lease was not written (${err instanceof Error ? err.message.split("\n")[0] : err}) — the clock's next look settles it`));
      else lastLocalTick = { at: new Date(), outcome };
    };
    const skip = async (reason: string): Promise<ScheduledFetchResult> => {
      const message = `skipped — ${reason}`;
      console.log(`[fetch-schedule] ${message}`);
      await note((h) => releaseLease(h, { outcome: "skipped", reason }, deps.schedule), message);
      return { started: false, message, reason };
    };

    if (deps.busy(userId)) return skip("a fetch is already running");
    const elsewhere = await fetchRunningElsewhere(userId, deps);
    if (elsewhere) return skip(`a fetch started by ${elsewhere.by} is running on ${elsewhere.host}`);
    const emails = await deps.emails(userId);
    if (!emails.length) return skip("no logged-in Platform ID");
    // The workspace's one fetch slot, from here until the start has taken over: the Fetch button cannot slip in between.
    let free: (() => void) | null = null;
    try {
      free = deps.reserve(userId);
    } catch {
      return skip("a fetch is already running");
    }
    let runId = "";
    try {
      const [{ days }, inv] = await Promise.all([resolveFetchSinceDays(userId), resolveScheduledInvoices(userId)]);
      const invoices = inv.enabled ? "missing" : "off";
      const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
      since.setHours(0, 0, 0, 0);
      // What this run was asked for, in the keys the Fetch button records (services/startRequests.ts planFetch).
      const params = {
        platform: "FLIPKART",
        selection_method: "all_accounts",
        email_ids_count: 0,
        accounts_count: emails.length,
        logged_in_count: emails.length,
        fetch_orders_after_date: since.toLocaleDateString("en-CA"),
        fetch_orders_after_date_source: "lookback",
        force_fetch: false,
        enable_login: false,
        run_update_after_fetch: true,
        invoices,
        // A fixed time of the schedule: which time, which machine, which start (Batch requests shows them as "Schedule").
        ...(claim ? { schedule_name: "fetch", schedule_slot: slotStamp(claim.slotAt), ran_on: config.hostId, attempt: claim.attempt } : {}),
      };
      // The ledger row first, queued: reading 21,000 accounts' saved units takes minutes, and a row that exists from
      // the first second is what the lease follows — an API restart in those minutes then reads as a fetch that
      // failed (and may be started once more), not as a night nobody started.
      runId = await createQueuedRun(userId, "fetch", {
        trigger: "system-cron",
        by,
        total: emails.length,
        message: `Preparing ${emails.length} account(s) — the fetch starts when their saved units are read`,
        meta: { platform: "FLIPKART", params, emails_count: emails.length, host: config.hostId, pid: process.pid },
      });
      if (lease) {
        current.set(userId, { runId, token: lease.token });
        await attachRun(lease, { kind: "fetch", id: runId }, deps.schedule).catch(() => undefined);
      }
      const holder = lease;
      const started = await deps.start(userId, emails, since, {
        invoices,
        trigger: "system-cron",
        by,
        runId,
        reserved: true,
        params,
        // When the run has ended and its ledger row is closed — stopped runs included.
        onFinished: async (info) => {
          if (current.get(userId)?.runId === info.runId) current.delete(userId);
          if (holder) {
            // The lease is given back with how the run ended: "finished — 641/21250 email(s) · …", "finished — stopped — …".
            const ended = await runRefState({ kind: "fetch", id: info.runId }, deps.schedule).catch(() => null);
            await releaseLease(holder, { outcome: ended && !ended.open && ended.outcome ? ended.outcome : "finished", reason: (ended && !ended.open && ended.reason) || (info.cancelled ? "stopped" : ""), runRef: { kind: "fetch", id: info.runId } }, deps.schedule).catch((err) =>
              console.warn(`[fetch-schedule] the lease was not released (${err instanceof Error ? err.message.split("\n")[0] : err}) — the clock's next look releases it`)
            );
          }
          // The reference's chain: when the nightly fetch ends, an update of the in-progress units is queued
          // (skipped when the fetch was stopped or an update request is already open — startRequests.queueUpdateAfterFetch).
          await deps.afterFetch(userId, { runId: info.runId, emails: info.emails, selection: "all_accounts", cancelled: info.cancelled, by, trigger: "system-cron" });
        },
      });
      if (started.runId && started.runId !== runId) {
        // The queued row was closed before the run could adopt it: the run wrote its own, and the lease follows that one.
        runId = started.runId;
        if (lease) {
          current.set(userId, { runId, token: lease.token });
          await attachRun(lease, { kind: "fetch", id: runId }, deps.schedule).catch(() => undefined);
        }
      }
      const size = `${emails.length} logged-in Platform ID(s), orders since ${since.toISOString().slice(0, 10)} (${days} d), invoices ${invoices === "missing" ? "on (all missing)" : "off"}`;
      const message = `started — ${size}`;
      console.log(`[fetch-schedule] ${message}`);
      await note((h) => markStarted(h, { runRef: { kind: "fetch", id: runId }, reason: `started by hand by ${by} — ${size}` }, deps.schedule), message);
      return { started: true, message, emails: emails.length, sinceDays: days, invoices, runId, reason: size };
    } catch (err) {
      const reason = err instanceof Error ? err.message.split("\n")[0] : String(err);
      if (current.get(userId)?.runId === runId) current.delete(userId);
      // Force kill while the account list was being prepared is a stop, not a failure.
      const stopped = err instanceof OrderFetchStoppedError;
      if (runId) await (stopped ? stopRunById(runId, reason) : failRunById(runId, `Could not start: ${reason}`));
      if (stopped) return skip(reason);
      const message = `could not start: ${reason}`;
      console.warn(`[fetch-schedule] ${message}`);
      await note((h) => releaseLease(h, { outcome: "failed", reason: message }, deps.schedule), message);
      return { started: false, failed: true, message, reason };
    } finally {
      free?.();
    }
  }

  /** The nightly fetch as the API's schedule clock runs it. */
  const runner = (userId: string): ScheduleRunner => ({
    async run(claim) {
      const out = await run(userId, "schedule", claim);
      if (out.started) return { runRef: { kind: "fetch", id: out.runId || "" }, reason: out.reason };
      if (out.failed) throw new Error(out.reason || out.message);
      return { skipped: true, reason: out.reason || out.message };
    },
    // A fetch has no document that is written while it runs: this process knows whether its own is going. A fetch
    // that was another process's of this machine died with that process — whether or not its row says so yet.
    async runState(runRef): Promise<RunState> {
      const deps = depsNow();
      if (current.get(userId)?.runId === runRef.id && deps.busy(userId)) return { open: true };
      const row = Types.ObjectId.isValid(runRef.id) ? await deps.schedule.Runs.findById(runRef.id).select("status meta.host meta.pid").lean() : null;
      const meta = (row?.meta || {}) as { host?: unknown; pid?: unknown };
      if (row && (row.status === "queued" || row.status === "running") && meta.host === config.hostId && meta.pid !== undefined && meta.pid !== process.pid) {
        await failRunById(runRef.id, FETCH_INTERRUPTED);
        return { open: false, outcome: "failed", reason: FETCH_INTERRUPTED };
      }
      return runRefState(runRef, deps.schedule);
    },
    // Another machine took the night over while this one was away: the fetch this process still has of it stops.
    async supersede(info) {
      const mine = current.get(userId);
      if (!mine || (mine.token !== info.token && mine.runId !== info.runRef?.id)) return;
      await depsNow().stop(userId, supersededText("fetch", info.by, info.at));
    },
  });

  return { run, runner, running: (userId: string) => current.get(userId)?.runId || "" };
}

/** This API process's. */
const own = createScheduledFetch();

/**
 * One scheduled run — the clock's (`claim`), or "Run the scheduled fetch now"
 * (no claim: `by` is who pressed it). Returns what it started, or why it did not.
 */
export const runScheduledFetch = own.run;
/** The runner this API's schedule clock calls for the nightly fetch (startFetchScheduleClock). */
export const scheduledFetchRunner = own.runner;

// ── the clock ───────────────────────────────────────────────────────────────

let clock: ScheduleClock | null = null;
let seen: NodeJS.Timeout | null = null;

/**
 * (API, at start) The nightly fetch joins the shared schedule clock. This
 * process serves the "fetch" clock whether or not it may run it: a machine
 * with FETCH_SCHEDULE=0 still writes its presence, records "switched off" on
 * time, keeps a lease a Run-now took renewed, and gives back one its own
 * earlier process left. Also starts the once-a-minute sign of life of any
 * fetch running here.
 */
export function startFetchScheduleClock(userId: string = config.userId): ScheduleClock | null {
  if (clock) return clock;
  if (!userId) {
    console.log("[fetch-schedule] TRADE_FLOW_USER_ID is not set — no nightly fetch and no schedule clock in this API");
    return null;
  }
  const serves = servedBy("api");
  const names = serves.filter((n) => eligibleHere(n));
  try {
    clock = startScheduleClock({ role: "api", userId, serves, names, runners: { fetch: scheduledFetchRunner(userId) }, seed: () => seedSchedules(userId) });
  } catch (err) {
    console.warn(`[fetch-schedule] the schedule clock could not start: ${err instanceof Error ? err.message.split("\n")[0] : err}`);
    return null;
  }
  seen = setInterval(() => void touchRunningFetch(userId).catch(() => undefined), FETCH_SEEN_EVERY_MS);
  seen.unref();
  console.log(
    `[fetch-schedule] on the shared clock — ${names.includes("fetch") ? "this machine may run the nightly fetch" : `this machine never runs the nightly fetch (FETCH_SCHEDULE=${RAW || "unset"} in its .env)`}; whether it runs at all is the switch in Settings → Scheduled fetch · look-back ${DEFAULT_SINCE_DAYS} d unless the dashboard sets fetchSinceDays`
  );
  return clock;
}

export function stopFetchScheduleClock(): void {
  clock?.stop();
  clock = null;
  if (seen) clearInterval(seen);
  seen = null;
}

/**
 * GET /fetch/schedule. The keys it always had, with two of them read from the
 * database now instead of this process: `enabled` is the Settings switch
 * (order_sync_settings.schedules.fetch — FETCH_SCHEDULE only says whether this
 * machine may run the fetch: `eligibleHere`), and `lastTick` is the clock's
 * (order_schedule_leases), so it is the same on every machine and survives a
 * restart. `cron` / `timezone` are the clock's time. New: `nextAt`, `lease`
 * (who is running it), `enabledSource`, `changedBy`, `changedAt`.
 */
export async function fetchScheduleStatus(userId: string) {
  const [since, invoices] = await Promise.all([resolveFetchSinceDays(userId), resolveScheduledInvoices(userId)]);
  const [lastRun, loggedInIds, schedule] = await Promise.all([
    OrderJobRun.findOne({ userId, type: "fetch", trigger: "system-cron" }).sort({ createdAt: -1 }).select("status startedAt finishedAt total success failed message").lean(),
    scheduledFetchCount(userId),
    scheduleStatuses(userId, ["fetch"]).then((all) => all.fetch),
  ]);
  // `outcome` stays the sentence it always was ("skipped — switched off"); the tick's own fields come with it.
  const t = schedule.lastTick;
  const clockTick = t ? { at: t.at, outcome: t.text, result: t.outcome, reason: t.reason, slotAt: t.slotAt, systemId: t.systemId, host: t.host, runRef: t.runRef } : null;
  return {
    enabled: schedule.on,
    enabledSource: schedule.source,
    changedBy: schedule.changedBy,
    changedAt: schedule.changedAt,
    eligibleHere: schedule.eligibleHere,
    cron: cronOf(schedule.clock) || FETCH_SCHEDULE_CRON,
    timezone: schedule.clock?.tz || FETCH_TIMEZONE,
    nextAt: schedule.nextAt,
    lease: schedule.lease,
    sinceDays: since.days,
    sinceSource: since.source,
    invoices,
    invoiceMaxPerRun: config.invoiceMaxPerRun,
    loggedInIds,
    windows: config.fetchWindowConcurrency || null,
    // Every tick is the clock's now; a Run-now in a workspace that has no clock is known to this process only.
    lastTick: clockTick || lastLocalTick,
    lastScheduledRun: lastRun,
    fetchRunning: getOrderFetchJob(userId).running,
  };
}
