import { randomUUID } from "node:crypto";
import os from "node:os";
import { Types, type Model } from "mongoose";
import { config } from "../config.js";
import { describeError, isTransientError, throttledWarn } from "../lib/resilience.js";
import { OrderJobRun, type IOrderJobRun } from "../models/OrderJobRun.js";
import { OrderSyncSettings, type IOrderSyncSettings } from "../models/OrderSyncSettings.js";
import { OrderUpdateRequest, type IOrderUpdateRequest } from "../models/OrderUpdateRequest.js";
import { ScheduleHost, type IScheduleHost } from "../models/ScheduleHost.js";
import {
  SCHEDULE_LABELS,
  SCHEDULE_NAMES,
  SCHEDULE_ROLE,
  ScheduleLease,
  TICKS_KEPT,
  type IScheduleClock,
  type IScheduleHolder,
  type IScheduleLease,
  type IScheduleRunRef,
  type IScheduleTick,
  type ScheduleName,
  type ScheduleRole,
  type TickOutcome,
} from "../models/ScheduleLease.js";

/**
 * One clock per schedule, kept in Mongo (models/ScheduleLease.ts), and the
 * lease that goes with it.
 *
 *   slot maths    nextSlot / slotAtOrBefore — fixed times in +05:30 (India has
 *                 no daylight saving), or UTC
 *   the poll      every 30 s (± 5 s) each process looks at the clocks its role
 *                 serves. A time that is due is dealt with by ONE
 *                 compare-and-set on `nextAt`, which does exactly one of:
 *                   take it   lease free or expired → this process holds it
 *                             for 5 minutes and starts the work
 *                   skipped   switched off, or the earlier run still holds a
 *                             live lease
 *                   missed    later than the clock's late limit
 *                 `nextAt` always moves to the next time after NOW: a machine
 *                 that slept ten hours deals with one time, not five.
 *   the lease     renewed on every poll while its run is open, released when
 *                 the run closes. A machine that goes quiet loses it after 5
 *                 minutes; the next time is then taken by another machine,
 *                 and the first one, when it wakes, is told (supersede) — or,
 *                 if nobody took over, simply carries on. A new process of
 *                 the same machine adopts the lease of a run that is still
 *                 open (a worker restart in the middle of a sweep).
 *   who           a machine takes a time only for a schedule it MAY run (the
 *                 caller's `names`: its .env, see scheduleSettings.ts). One it
 *                 may not run it never takes; it only records "switched off"
 *                 on time, and "missed" once the late limit has passed.
 *                 `clock.preferSystemId` runs a schedule when it is awake:
 *                 another machine waits `standbyGraceSec` (2 min) first, but
 *                 only while the preferred one was seen in the last 10 min.
 *   first start   a clock no machine has served yet (`armedAt` null) never
 *                 runs a time that is already due: the first machine that may
 *                 run it starts with the next fixed time.
 *   the fetch     alone may be started a second time for the same night: when
 *                 its run did not finish (its lease expired, or it ended
 *                 failed), within the late limit (3 h), at most 2 starts.
 *   by hand       claimManual — the Run-now buttons: refused while a live
 *                 lease is held, with who holds it.
 *
 * The switch is read at every tick (order_sync_settings.schedules.<name>.on).
 * A compare-and-set that matches nothing means somebody else got there first,
 * or the lease is lost; a database or network error is logged at most once a
 * minute and the next poll tries again — what a runner answered is kept until
 * the lease says so too, and a superseded runner is told until it has heard.
 *
 * What a tick looks like, per time: one of `ran` / `took_over` (with the run,
 * or "nothing to refresh"), `skipped`, `missed`, `failed` (could not start);
 * and, for a run that started, `finished` or `failed` when it ends. The tick
 * of a time that was taken is written when its runner has answered, so it
 * names the run.
 *
 * Machines are told apart by TRADEFLOW_SYSTEM_ID: two machines under one id
 * would count as one (each would carry on the other's runs, neither would take
 * over). The presence row shows it, and the clock says so in the log.
 */

export const POLL_MS = 30_000;
export const POLL_JITTER_MS = 5_000;
/** How long a lease is good for without a renewal. */
export const LEASE_MS = 5 * 60_000;
export const PRESENCE_EVERY_MS = 60_000;
/** A standby waits for the preferred machine only while that machine was seen this recently. */
export const PREFERRED_SEEN_MS = 10 * 60_000;
/** A run nothing has written to for this long counts as closed; so does a lease taken this long ago with no run attached. */
export const RUN_QUIET_MS = 30 * 60_000;
/** A runner that has not answered after this long no longer holds up the poll (its lease is renewed meanwhile). */
const RUN_START_WAIT_MS = 60_000;
const SEED_RETRY_MS = 5 * 60_000;

/** The pause before the next poll: 30 s ± 5 s, so two machines do not look in step for ever. */
export function pollDelayMs(random: number = Math.random()): number {
  return POLL_MS + Math.round((random * 2 - 1) * POLL_JITTER_MS);
}

/** How many times one fixed time may be started: the nightly fetch twice, the sweeps once (their next time comes soon enough). */
export const MAX_STARTS: Record<ScheduleName, number> = { fetch: 2, update_active: 1, update_ofd: 1, invoice_sweep: 1 };

// ── slot maths ──────────────────────────────────────────────────────────────

/** The only clocks the slot maths knows: fixed offsets, in minutes east of UTC. */
export const SCHEDULE_TIMEZONES: Record<string, number> = { "Asia/Kolkata": 330, UTC: 0 };

type ClockTimes = Pick<IScheduleClock, "kind" | "at" | "everyMin" | "phaseMin" | "tz">;

function offsetMs(tz: string): number {
  const minutes = SCHEDULE_TIMEZONES[tz];
  if (minutes === undefined) throw new Error(`Schedule timezone "${tz}" is not supported — use Asia/Kolkata or UTC`);
  return minutes * 60_000;
}

/** "05:00" → minutes after midnight; throws on anything else. */
export function minutesOfDay(at: string): number {
  const m = /^(\d{1,2}):(\d{2})$/.exec(String(at || "").trim());
  const h = m ? Number(m[1]) : NaN;
  const min = m ? Number(m[2]) : NaN;
  if (!(h >= 0 && h <= 23 && min >= 0 && min <= 59)) throw new Error(`Schedule time "${at}" is not HH:MM`);
  return h * 60 + min;
}

export function periodMs(clock: ClockTimes): number {
  if (clock.kind === "daily") return 24 * 60 * 60_000;
  if (!(clock.everyMin >= 1)) throw new Error(`Schedule period "${clock.everyMin}" min is not valid`);
  return Math.round(clock.everyMin) * 60_000;
}

/** The latest fixed time at or before `at`. */
export function slotAtOrBefore(clock: ClockTimes, at: Date): Date {
  const off = offsetMs(clock.tz);
  const period = periodMs(clock);
  // Wall-clock milliseconds: midnight in the clock's timezone is a multiple of a day.
  const local = at.getTime() + off;
  const phase = clock.kind === "daily" ? minutesOfDay(clock.at) * 60_000 : (((Math.round(clock.phaseMin) * 60_000) % period) + period) % period;
  return new Date(Math.floor((local - phase) / period) * period + phase - off);
}

/** The first fixed time strictly after `after`. */
export function nextSlot(clock: ClockTimes, after: Date): Date {
  return new Date(slotAtOrBefore(clock, after).getTime() + periodMs(clock));
}

/** How late a time may still be taken. Never under 5 minutes: a standby's grace and a poll must fit in it. */
export function lateLimitMs(clock: Pick<IScheduleClock, "lateMin">): number {
  return Math.max(5, Number(clock.lateMin) || 0) * 60_000;
}

function two(n: number) {
  return String(n).padStart(2, "0");
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

// The words below are for people (ticks, notes, the log): they never throw — a timezone the maths refuses reads as India's.
const wallClock = (at: Date, tz: string) => new Date(new Date(at).getTime() + (SCHEDULE_TIMEZONES[tz] ?? 330) * 60_000);

/** "12:15" in the clock's timezone. */
export function clockTime(at: Date | null | undefined, tz = "Asia/Kolkata"): string {
  if (!at) return "?";
  const d = wallClock(at, tz);
  return `${two(d.getUTCHours())}:${two(d.getUTCMinutes())}`;
}

/** "25 Nov 12:15" in the clock's timezone. */
export function clockStamp(at: Date | null | undefined, tz = "Asia/Kolkata"): string {
  if (!at) return "?";
  const d = wallClock(at, tz);
  return `${d.getUTCDate()} ${MONTHS[d.getUTCMonth()]} ${two(d.getUTCHours())}:${two(d.getUTCMinutes())}`;
}

/** A daily clock as the cron line the older fetch-schedule route always gave ("0 5 * * *"); null for any other clock. */
export function cronOf(clock: Pick<IScheduleClock, "kind" | "at"> | null | undefined): string | null {
  if (clock?.kind !== "daily" || !/^\d{1,2}:\d{2}$/.test(String(clock.at || "").trim())) return null;
  const [h, m] = clock.at.trim().split(":").map(Number);
  return h <= 23 && m <= 59 ? `${m} ${h} * * *` : null;
}

/** "every 6 h at 00:15 / 06:15 / 12:15 / 18:15", "every 2 h at :45", "daily at 05:00". */
export function describeClock(clock: ClockTimes): string {
  if (clock.kind === "daily") return `daily at ${cronOf(clock) ? `${two(Math.floor(minutesOfDay(clock.at) / 60))}:${two(minutesOfDay(clock.at) % 60)}` : clock.at || "?"}`;
  const every = Math.round(clock.everyMin);
  if (!(every >= 1)) return "?";
  const phase = ((Math.round(clock.phaseMin) % every) + every) % every;
  if (every % 60 !== 0) return `every ${every} min`;
  const perDay = 1440 / every;
  if (Number.isInteger(perDay) && perDay <= 4) {
    const times = Array.from({ length: perDay }, (_, i) => `${two(Math.floor((phase + i * every) / 60) % 24)}:${two((phase + i * every) % 60)}`);
    return `every ${every / 60} h at ${times.join(" / ")}`;
  }
  return `every ${every / 60} h at :${two(phase % 60)}`;
}

// ── what the clock reads and writes ─────────────────────────────────────────

export type LeaseDoc = IScheduleLease & { _id: Types.ObjectId };

/**
 * The collections behind the clock, and the time. The worker and the API pass
 * nothing; scripts/scheduleLeaseCheck.ts passes throwaway collections with the
 * real schemas and a clock it moves by hand.
 */
export interface ScheduleDeps {
  Leases: Model<IScheduleLease>;
  Hosts: Model<IScheduleHost>;
  Settings: Model<IOrderSyncSettings>;
  /** Where a lease's run lives: an update request, or a ledger run (fetch, invoice sweep). */
  Updates: Model<IOrderUpdateRequest>;
  Runs: Model<IOrderJobRun>;
  now: () => Date;
}

export const scheduleDefaults: ScheduleDeps = {
  Leases: ScheduleLease,
  Hosts: ScheduleHost,
  Settings: OrderSyncSettings,
  Updates: OrderUpdateRequest,
  Runs: OrderJobRun,
  now: () => new Date(),
};

/** A process as the lease and the presence rows name it. */
export interface ScheduleIdentity {
  systemId: string;
  host: string;
  role: ScheduleRole;
  pid: number;
  machine: string;
}

export function thisProcess(role: ScheduleRole): ScheduleIdentity {
  return { systemId: config.systemId, host: config.hostId, role, pid: process.pid, machine: os.hostname() };
}

/** What renews, starts and releases a lease — all a run has to carry (an update request keeps it as `schedule`). */
export interface LeaseHandle {
  userId: string;
  name: ScheduleName;
  token: string;
}

export interface ScheduleClaim extends LeaseHandle {
  /** The fixed time taken; null for a run started by hand. */
  slotAt: Date | null;
  attempt: number;
  manual: boolean;
  /** Who asked: "schedule", or the person behind a Run-now button. */
  by: string;
  takenAt: Date;
  holder: IScheduleHolder;
  /** How late the time was taken. */
  lateMs: number;
  /** Earlier times that passed with nobody awake, dealt with by this one write. */
  missedBefore: number;
  /** The holder whose lease had expired, when this claim took over from one. */
  replaced: { systemId: string; host: string; slotAt: Date | null; expiredAt: Date | null } | null;
  /** Set on the second start of a time: how the first one ended. */
  secondStartOf: string;
}

/** Only a stored `true` runs a schedule; `false`, null and a missing path do not. */
export function switchOn(row: Pick<IOrderSyncSettings, "schedules"> | null | undefined, name: ScheduleName): boolean {
  return row?.schedules?.[name]?.on === true;
}

/** Has anybody (the seed, a person) ever set this switch? */
export function switchSet(row: Pick<IOrderSyncSettings, "schedules"> | null | undefined, name: ScheduleName): boolean {
  return typeof row?.schedules?.[name]?.on === "boolean";
}

function objectId(userId: string): Types.ObjectId {
  if (!Types.ObjectId.isValid(userId)) throw new Error("A schedule needs the workspace id (TRADE_FLOW_USER_ID)");
  return new Types.ObjectId(userId);
}

function tickOf(outcome: TickOutcome, reason: string, who: { systemId: string; host: string }, slotAt: Date | null, at: Date, runRef: IScheduleRunRef | null = null): IScheduleTick {
  return { slotAt, at, outcome, reason: String(reason || "").split("\n")[0].slice(0, 300), systemId: who.systemId, host: who.host, runRef };
}

const pushTick = (tick: IScheduleTick) => ({ ticks: { $each: [tick], $slice: -TICKS_KEPT } });

/** A tick as one sentence: "ran on Mac", "skipped — switched off", "finished — ok 926, failed 3". */
export function tickText(tick: Pick<IScheduleTick, "outcome" | "reason" | "host"> | null | undefined): string {
  if (!tick) return "";
  const head = tick.outcome === "ran" ? `ran on ${tick.host || "?"}` : tick.outcome === "took_over" ? `took over on ${tick.host || "?"}` : tick.outcome;
  return tick.reason ? `${head} — ${tick.reason}` : head;
}

/** A schedule inside a sentence: "the nightly fetch", "the update of all active units". */
export const SCHEDULE_NOUNS: Record<ScheduleName, string> = {
  fetch: "the nightly fetch",
  update_active: "the update of all active units",
  update_ofd: "the out-for-delivery recheck",
  invoice_sweep: "the invoice sweep",
};

/**
 * What is written on work another machine has taken over — the account of an update sweep, a stopped fetch, a
 * stopped invoice sweep: "Superseded — Mac-2 took over the nightly fetch at 05:16".
 */
export function supersededText(name: ScheduleName, by: { host?: string; systemId?: string }, at: Date | null | undefined, tz = "Asia/Kolkata"): string {
  return `Superseded — ${by.host || by.systemId || "another machine"} took over ${SCHEDULE_NOUNS[name]} at ${clockTime(at, tz)}`;
}

/** "2026-11-25 18:15" on the clock's timezone: a fixed time as a request records it (params.schedule_slot). */
export function slotStamp(at: Date | null | undefined, tz = "Asia/Kolkata"): string {
  if (!at) return "";
  const d = wallClock(at, tz);
  return `${d.getUTCFullYear()}-${two(d.getUTCMonth() + 1)}-${two(d.getUTCDate())} ${two(d.getUTCHours())}:${two(d.getUTCMinutes())}`;
}

function leaseFilter(h: LeaseHandle) {
  return { userId: objectId(h.userId), name: h.name, "holder.token": h.token };
}

/** Who holds this claim, for the tick it is about to write: the claim itself knows, a bare handle is looked up. */
async function claimFacts(h: LeaseHandle | ScheduleClaim, deps: ScheduleDeps): Promise<{ who: { systemId: string; host: string }; slotAt: Date | null } | null> {
  if ("holder" in h) return { who: h.holder, slotAt: h.slotAt };
  const lease = await deps.Leases.findOne(leaseFilter(h)).select("holder slotAt").lean();
  return lease?.holder ? { who: lease.holder, slotAt: lease.slotAt ?? null } : null;
}

/**
 * Keep the lease for another 5 minutes: false = it is no longer this claim's (nothing is written). `carriedBy` is a
 * new process of the same machine taking over the renewals of an earlier one: the claim stays, the process changes.
 */
export async function renewLease(h: LeaseHandle, deps: ScheduleDeps = scheduleDefaults, carriedBy?: { pid: number; host: string }): Promise<boolean> {
  const now = deps.now();
  const res = await deps.Leases.updateOne(leaseFilter(h), { $set: { renewedAt: now, expiresAt: new Date(now.getTime() + LEASE_MS), ...(carriedBy ? { "holder.pid": carriedBy.pid, "holder.host": carriedBy.host } : {}) } });
  return res.matchedCount === 1;
}

/**
 * The work behind a claim has started: its tick (`ran`, or `took_over`), and
 * the run the lease now follows. False = the lease is no longer this claim's.
 */
export async function markStarted(
  h: LeaseHandle | ScheduleClaim,
  start: { runRef?: IScheduleRunRef | null; reason?: string; outcome?: "ran" | "took_over" } = {},
  deps: ScheduleDeps = scheduleDefaults
): Promise<boolean> {
  const facts = await claimFacts(h, deps);
  if (!facts) return false;
  const now = deps.now();
  const tick = tickOf(start.outcome || "ran", start.reason || "", facts.who, facts.slotAt, now, start.runRef ?? null);
  const res = await deps.Leases.updateOne(leaseFilter(h), {
    $set: { ...(start.runRef ? { runRef: start.runRef } : {}), renewedAt: now, expiresAt: new Date(now.getTime() + LEASE_MS), lastTick: tick },
    $push: pushTick(tick),
  });
  return res.matchedCount === 1;
}

/** A run that began after its claim's tick (a queued job) says which document it lives in. False = the lease is no longer this claim's. */
export async function attachRun(h: LeaseHandle, runRef: IScheduleRunRef, deps: ScheduleDeps = scheduleDefaults): Promise<boolean> {
  const now = deps.now();
  const res = await deps.Leases.updateOne(leaseFilter(h), { $set: { runRef, renewedAt: now, expiresAt: new Date(now.getTime() + LEASE_MS) } });
  return res.matchedCount === 1;
}

/**
 * Give the lease back, with the tick that says how it went:
 *   finished / failed   the run ended
 *   ran                 nothing to do ("nothing to refresh") — the time's only tick
 *   skipped             the runner decided not to run this time (the reason says why)
 * False = the lease was no longer this claim's (somebody took over): nothing is written.
 */
export async function releaseLease(
  h: LeaseHandle | ScheduleClaim,
  end: { outcome: "finished" | "failed" | "ran" | "skipped"; reason?: string; runRef?: IScheduleRunRef | null },
  deps: ScheduleDeps = scheduleDefaults
): Promise<boolean> {
  const facts = await claimFacts(h, deps);
  if (!facts) return false;
  const now = deps.now();
  const tick = tickOf(end.outcome, end.reason || "", facts.who, facts.slotAt, now, end.runRef ?? null);
  const res = await deps.Leases.updateOne(leaseFilter(h), {
    $set: { holder: null, expiresAt: null, finishedAt: now, endedAs: end.outcome, lastTick: tick, ...(end.runRef ? { runRef: end.runRef } : {}) },
    $push: pushTick(tick),
  });
  return res.matchedCount === 1;
}

/**
 * Has another MACHINE taken this schedule since this claim was made? For work
 * left over from an older claim — an account job of a sweep whose machine
 * slept — which must then end before it calls Flipkart. Null = no: the claim
 * still stands, or it was this machine itself that ran again.
 */
export async function supersededBy(
  userId: string,
  claim: { name: ScheduleName; token: string },
  systemId: string = config.systemId,
  deps: ScheduleDeps = scheduleDefaults
): Promise<{ systemId: string; host: string; at: Date | null; slotAt: Date | null } | null> {
  if (!claim.token) return null;
  const lease = await deps.Leases.findOne({ userId: objectId(userId), name: claim.name }).select("ranBy takenAt slotAt").lean();
  const taker = lease?.ranBy;
  if (!taker?.token || taker.token === claim.token || taker.systemId === systemId) return null;
  return { systemId: taker.systemId, host: taker.host || taker.systemId, at: lease?.takenAt ?? null, slotAt: lease?.slotAt ?? null };
}

export type ManualClaim =
  | { ok: true; claim: ScheduleClaim }
  | {
      ok: false;
      /** held_elsewhere = another machine has a live lease; running_here = this machine has; no_clock = the schedule was never seeded. */
      code: "held_elsewhere" | "running_here" | "no_clock";
      status: 409;
      message: string;
      holder: { systemId: string; host: string; since: Date | null; slotAt: Date | null; manual: boolean } | null;
    };

/**
 * The Run-now buttons: take the lease for a run a person asked for. It works
 * whatever the switch says, and takes no fixed time (`nextAt` stays). Refused
 * while a live lease is held — `held_elsewhere` is the 409 "…is running on
 * <host> since <time>". The caller then starts its work and calls markStarted
 * (or releaseLease when there was nothing to do); the clock of the process
 * kind that serves this schedule on this machine keeps the lease renewed and
 * releases it when the run closes.
 */
export async function claimManual(userId: string, name: ScheduleName, by: string, opts: { me?: Partial<ScheduleIdentity>; deps?: ScheduleDeps } = {}): Promise<ManualClaim> {
  const deps = opts.deps || scheduleDefaults;
  const me: ScheduleIdentity = { ...thisProcess(SCHEDULE_ROLE[name]), ...opts.me };
  const id = objectId(userId);
  const label = SCHEDULE_LABELS[name].toLowerCase();
  // The compare-and-set decides; when it matches nothing, the lease is read to say why. Twice at most: a lease
  // released between the two is simply taken on the second go.
  for (let attempt = 0; attempt < 2; attempt++) {
    const now = deps.now();
    const holder: IScheduleHolder = { systemId: me.systemId, host: me.host, role: me.role, pid: me.pid, token: randomUUID() };
    const before = await deps.Leases.findOneAndUpdate(
      { userId: id, name, $or: [{ holder: null }, { expiresAt: { $lte: now } }] },
      { $set: { holder, slotAt: null, attempt: 1, manual: true, takenAt: now, renewedAt: now, expiresAt: new Date(now.getTime() + LEASE_MS), runRef: null, finishedAt: null, endedAs: null, ranBy: { systemId: holder.systemId, host: holder.host, token: holder.token } } },
      { new: false }
    )
      .select("holder slotAt expiresAt")
      .lean();
    if (before) {
      // A holder still on the lease had let it run out: this claim takes over from it.
      const replaced = before.holder ? { systemId: before.holder.systemId, host: before.holder.host, slotAt: before.slotAt ?? null, expiredAt: before.expiresAt ?? null } : null;
      return { ok: true, claim: { userId, name, token: holder.token, slotAt: null, attempt: 1, manual: true, by: by || "", takenAt: now, holder, lateMs: 0, missedBefore: 0, replaced, secondStartOf: "" } };
    }
    const lease = await deps.Leases.findOne({ userId: id, name }).select("holder takenAt slotAt manual expiresAt clock").lean();
    if (!lease) return { ok: false, code: "no_clock", status: 409, message: `The ${label} has no clock yet — Trade Flow creates it when it starts`, holder: null };
    const held = lease.holder && lease.expiresAt && lease.expiresAt.getTime() > deps.now().getTime() ? lease.holder : null;
    if (!held) continue;
    const here = held.systemId === me.systemId;
    return {
      ok: false,
      code: here ? "running_here" : "held_elsewhere",
      status: 409,
      message: `The ${label} is running on ${here ? "this machine" : held.host || held.systemId} since ${clockTime(lease.takenAt, lease.clock.tz)}`,
      holder: { systemId: held.systemId, host: held.host, since: lease.takenAt ?? null, slotAt: lease.slotAt ?? null, manual: Boolean(lease.manual) },
    };
  }
  return { ok: false, code: "held_elsewhere", status: 409, message: `The ${label} is being taken by another machine right now — try again in a moment`, holder: null };
}

// ── is a run still open? ────────────────────────────────────────────────────

export interface RunState {
  open: boolean;
  /** How it ended, once closed. */
  outcome?: "finished" | "failed";
  reason?: string;
}

/**
 * The state of the document a lease's run lives in. Closed = the document
 * says so, or nothing has written to it for 30 minutes (its process is gone:
 * a lease must not be renewed for ever on the strength of a row left open).
 */
export async function runRefState(ref: IScheduleRunRef, deps: ScheduleDeps = scheduleDefaults, now: Date = deps.now()): Promise<RunState> {
  if (!Types.ObjectId.isValid(ref.id)) return { open: false, outcome: "failed", reason: "its run could not be found" };
  const quiet = (updatedAt: Date | null | undefined) => !updatedAt || now.getTime() - new Date(updatedAt).getTime() > RUN_QUIET_MS;
  const stale: RunState = { open: false, outcome: "failed", reason: `nothing was written to its run for ${RUN_QUIET_MS / 60_000} min` };
  if (ref.kind === "update") {
    const row = await deps.Updates.findById(ref.id).select("status updatedAt").lean();
    if (!row) return { open: false, outcome: "failed", reason: "its update request could not be found" };
    if (row.status === "done") return { open: false, outcome: "finished", reason: "" };
    if (row.status === "cancelled") return { open: false, outcome: "finished", reason: "the request was stopped" };
    return quiet(row.updatedAt) ? stale : { open: true };
  }
  const run = await deps.Runs.findById(ref.id).select("status message updatedAt").lean();
  if (!run) return { open: false, outcome: "failed", reason: "its run could not be found" };
  const message = String(run.message || "").split("\n")[0];
  if (run.status === "completed") return { open: false, outcome: "finished", reason: message };
  if (run.status === "stopped") return { open: false, outcome: "finished", reason: message ? `stopped — ${message}` : "stopped" };
  if (run.status === "failed") return { open: false, outcome: "failed", reason: message };
  return quiet(run.updatedAt) ? stale : { open: true };
}

// ── the poll ────────────────────────────────────────────────────────────────

/** What a runner answers when a time has been taken for it. */
export interface ScheduleRunResult {
  /** The work started and lives in this document: the lease follows it until it closes. */
  runRef?: IScheduleRunRef | null;
  /** Nothing to do ("nothing to refresh"): the lease is released at once and `reason` is the tick. */
  done?: boolean;
  /** The runner decided against this time (a fetch another machine started is running): released, tick `skipped`. */
  skipped?: boolean;
  reason?: string;
}

/**
 * The work of one schedule. `run` is called once per time this process took:
 * it starts the work and answers quickly. With neither `done` nor `skipped`
 * the lease is kept — with `runRef` when the work is a document already, or
 * without one when it has only been queued and will say so itself
 * (attachRun), which it must within 30 minutes.
 */
export interface ScheduleRunner {
  run(claim: ScheduleClaim): Promise<ScheduleRunResult>;
  /** Is the run still open? Default: runRefState (its document). The fetch answers from the process it runs in. */
  runState?(runRef: IScheduleRunRef, lease: LeaseDoc): Promise<RunState>;
  /** Another machine holds the lease now: stop what this one still has of that run. */
  supersede?(info: { name: ScheduleName; token: string; slotAt: Date | null; runRef: IScheduleRunRef | null; by: { systemId: string; host: string }; at: Date | null }): Promise<void> | void;
}

export interface ScheduleClockOptions {
  role: ScheduleRole;
  userId: string;
  /** The schedules this process may RUN: those its role serves, less what this machine's .env forbids (scheduleSettings.eligibleHere). */
  names: ScheduleName[];
  /** The schedules this process serves — default: all of its role. For one it may not run it still records "switched off" and "missed". */
  serves?: ScheduleName[];
  runners: Partial<Record<ScheduleName, ScheduleRunner>>;
  me?: Partial<ScheduleIdentity>;
  deps?: ScheduleDeps;
  /** Called (at most every 5 min) when a served clock or its switch does not exist yet: the caller's seed. */
  seed?: () => Promise<unknown>;
  log?: (line: string) => void;
  /** How long a poll waits for a runner to answer before it moves on (default 60 s). */
  startWaitMs?: number;
}

export interface ScheduleClock {
  readonly me: ScheduleIdentity;
  /** One look at every served clock. The timer calls it; a check, or a stray job that wants "now", may too. */
  pollOnce(): Promise<void>;
  start(): void;
  stop(): void;
  /** The lease this process holds for a schedule, if any. */
  holds(name: ScheduleName): LeaseHandle | null;
  /** Resolves once no runner is still answering a time this process took. */
  idle(): Promise<void>;
}

/** What a runner's answer came to: the run started (the lease is kept), or the time ended there and then (released). */
type Verdict = { started: boolean; outcome: "ran" | "took_over" | "skipped" | "failed"; reason: string; runRef: IScheduleRunRef | null };

interface Held {
  token: string;
  slotAt: Date | null;
  runRef: IScheduleRunRef | null;
  /** The runner has been called and has not answered yet. */
  starting: boolean;
  /** The claim behind a time this process took itself (not one it adopted). */
  claim: ScheduleClaim | null;
  /** The runner's answer, until the lease says so too: a write the database refused is made again on the next poll. */
  unwritten: Verdict | null;
  writing: boolean;
}

export function createScheduleClock(opts: ScheduleClockOptions): ScheduleClock {
  const deps = opts.deps || scheduleDefaults;
  const me: ScheduleIdentity = { ...thisProcess(opts.role), ...opts.me, role: opts.role };
  const uid = objectId(opts.userId);
  const serves = (opts.serves || SCHEDULE_NAMES.filter((n) => SCHEDULE_ROLE[n] === opts.role)).filter((n) => SCHEDULE_ROLE[n] === opts.role);
  const names = opts.names.filter((n) => serves.includes(n) && opts.runners[n]);
  const log = opts.log || ((line: string) => console.log(`[schedules] ${line}`));
  for (const n of opts.names) if (!names.includes(n)) log(`${n}: not served by this ${opts.role} process (no runner, or another role's) — it will not be run here`);

  const held = new Map<ScheduleName, Held>();
  /** Runs another machine took over, whose runner has not confirmed yet that it stopped what it had of them. */
  const toStop = new Map<ScheduleName, Parameters<NonNullable<ScheduleRunner["supersede"]>>[0]>();
  const starting = new Set<Promise<void>>();
  const startedAt = deps.now();
  let presenceAt = 0;
  let seedAt = 0;
  let polling = false;
  let started = false;
  let stopped = false;
  let timer: NodeJS.Timeout | null = null;

  // At most one line a minute: an outage as one line for the whole clock, anything else per place it happened.
  const report = (where: string, err: unknown) => {
    throttledWarn(isTransientError(err) ? `schedule-clock:${opts.role}` : `schedule-clock:${opts.role}:${where}`, `[schedules] ${where}: ${describeError(err)} — the next poll tries again`);
  };

  const wantSeed = () => {
    const now = Date.now();
    if (!opts.seed || now - seedAt < SEED_RETRY_MS) return;
    seedAt = now;
    void opts.seed().catch((err) => report("seed", err));
  };

  async function writePresence(now: Date) {
    const before = await deps.Hosts.findOneAndUpdate(
      { _id: `${me.systemId}:${me.role}` },
      { $set: { systemId: me.systemId, role: me.role, host: me.host, machine: me.machine, pid: me.pid, serves, eligible: names, startedAt, seenAt: now } },
      { upsert: true, new: false }
    )
      .select("machine seenAt")
      .lean();
    // The row was just written by a machine with another name: two machines share one TRADEFLOW_SYSTEM_ID. To the lease
    // they are one machine — each would carry on the other's runs and neither would ever take over. Said, loudly.
    if (before?.machine && me.machine && before.machine !== me.machine && now.getTime() - new Date(before.seenAt).getTime() < 2 * PRESENCE_EVERY_MS) {
      throttledWarn(`schedule-clock:same-id:${me.role}`, `[schedules] another machine ("${before.machine}") runs Trade Flow under this machine's TRADEFLOW_SYSTEM_ID "${me.systemId}" — give each machine its own in its .env: under one id two machines count as one, and neither takes over from the other`);
    }
  }

  /** The time that is due now: the latest fixed time, or `nextAt` itself when a changed clock left it off the grid. */
  function dueSlot(lease: LeaseDoc, now: Date): Date {
    const latest = slotAtOrBefore(lease.clock, now);
    return latest.getTime() >= lease.nextAt.getTime() ? latest : lease.nextAt;
  }

  /** A standby holds back while the machine that should run it is around and its grace has not passed. */
  async function waitForPreferred(lease: LeaseDoc, sinceMs: number, now: Date): Promise<boolean> {
    const prefer = lease.clock.preferSystemId;
    if (!prefer || prefer === me.systemId) return false;
    if (sinceMs >= Math.max(0, Number(lease.clock.standbyGraceSec ?? 120)) * 1000) return false;
    const row = await deps.Hosts.findById(`${prefer}:${me.role}`).lean();
    if (!row?.seenAt || now.getTime() - new Date(row.seenAt).getTime() > PREFERRED_SEEN_MS) return false;
    // Around, but its .env does not let it run this schedule: nothing to wait for.
    return !Array.isArray(row.eligible) || row.eligible.includes(lease.name);
  }

  async function stateOf(lease: LeaseDoc, now: Date): Promise<RunState> {
    if (!lease.runRef) {
      // Taken, no run attached yet: a start in flight (a queued job, a Run-now about to begin).
      const since = lease.takenAt ? now.getTime() - new Date(lease.takenAt).getTime() : Infinity;
      return since <= RUN_QUIET_MS ? { open: true } : { open: false, outcome: "failed", reason: `no run started within ${RUN_QUIET_MS / 60_000} min of taking it` };
    }
    const runner = opts.runners[lease.name];
    return runner?.runState ? runner.runState(lease.runRef, lease) : runRefState(lease.runRef, deps, now);
  }

  /**
   * A lease of this machine and process kind: renewed while its run is open
   * (adopted first when an earlier process took it), released when the run
   * has closed. One that went to another machine is reported once
   * (supersede). Answers the lease as it is afterwards.
   */
  async function keepHeld(read: LeaseDoc, now: Date): Promise<LeaseDoc> {
    let lease = read;
    const name = lease.name;
    const mine = held.get(name);
    if (mine?.unwritten && !mine.starting && !mine.writing && lease.holder?.token === mine.token) {
      // What the runner answered did not reach the lease last time (the database was away): written now.
      await writeVerdict(name, mine);
      lease = (await deps.Leases.findById(lease._id).select("-ticks").lean()) || lease;
    }
    if (mine && lease.holder?.token !== mine.token) {
      // Released by the run itself, or taken by somebody else while this machine was away.
      held.delete(name);
      const taker = lease.holder || (lease.ranBy && lease.ranBy.token !== mine.token ? lease.ranBy : null);
      if (taker && taker.token !== mine.token && taker.systemId !== me.systemId) {
        const tz = lease.clock.tz;
        const tick = tickOf("superseded", `${taker.host || taker.systemId} took over at ${clockTime(lease.takenAt, tz)}`, me, mine.slotAt, now, mine.runRef);
        await deps.Leases.updateOne({ _id: lease._id }, { $push: pushTick(tick) });
        log(`${name}: superseded — ${tick.reason}`);
        toStop.set(name, { name, token: mine.token, slotAt: mine.slotAt, runRef: mine.runRef, by: { systemId: taker.systemId, host: taker.host || taker.systemId }, at: lease.takenAt ?? null });
      }
    }
    // Told until it has answered: a stop that failed (the database was away) is asked for again on the next poll.
    const stop = toStop.get(name);
    if (stop) {
      await opts.runners[name]?.supersede?.(stop);
      toStop.delete(name);
    }
    const holder = lease.holder;
    if (!holder || holder.systemId !== me.systemId || holder.role !== me.role) return lease;
    const tracked = held.get(name);
    const handle: LeaseHandle = { userId: opts.userId, name, token: holder.token };
    // While its runner has not answered, a claim is only renewed — for 30 minutes at most, like any claim without a run.
    const stillStarting = Boolean(tracked?.starting) && now.getTime() - new Date(lease.takenAt || now).getTime() <= RUN_QUIET_MS;
    if (!stillStarting) {
      const state = await stateOf(lease, now);
      if (!state.open) {
        held.delete(name);
        const outcome = state.outcome || "finished";
        const done = await releaseLease(handle, { outcome, reason: state.reason || "", runRef: lease.runRef }, deps);
        if (!done) return lease;
        log(`${name}: ${outcome}${state.reason ? ` — ${state.reason}` : ""}`);
        return { ...lease, holder: null, expiresAt: null, finishedAt: now, endedAs: outcome, lastTick: tickOf(outcome, state.reason || "", holder, lease.slotAt ?? null, now, lease.runRef) };
      }
    }
    const adopt = holder.pid !== me.pid;
    if (!(await renewLease(handle, deps, adopt ? { pid: me.pid, host: me.host } : undefined))) {
      // Gone between the read and the write: the next poll sees who has it.
      held.delete(name);
      return lease;
    }
    const expiresAt = new Date(deps.now().getTime() + LEASE_MS);
    if (tracked) tracked.runRef = lease.runRef ?? tracked.runRef;
    else {
      held.set(name, { token: holder.token, slotAt: lease.slotAt ?? null, runRef: lease.runRef ?? null, starting: false, claim: null, unwritten: null, writing: false });
      if (adopt) log(`${name}: this process carries on the ${lease.manual ? "run started by hand" : `${clockTime(lease.slotAt, lease.clock.tz)} run`} an earlier one on ${me.host} took`);
    }
    return { ...lease, renewedAt: now, expiresAt, holder: { ...holder, pid: me.pid, host: adopt ? me.host : holder.host } };
  }

  /** The first machine that may run a schedule arms its clock; a time already due then is not run late. */
  async function arm(lease: LeaseDoc, now: Date): Promise<LeaseDoc> {
    const due = now.getTime() >= lease.nextAt.getTime();
    const set: Partial<IScheduleLease> = { armedAt: now };
    let tick: IScheduleTick | null = null;
    if (due) {
      const slot = dueSlot(lease, now);
      tick = tickOf("skipped", `the shared clock started on ${me.host} after this time — the first run is the next fixed time`, me, slot, now);
      Object.assign(set, { nextAt: nextSlot(lease.clock, now), lastSlotAt: slot, lastTick: tick });
    }
    const res = await deps.Leases.updateOne({ _id: lease._id, armedAt: null, nextAt: lease.nextAt }, { $set: set, ...(tick ? { $push: pushTick(tick) } : {}) });
    if (res.matchedCount) {
      log(`${lease.name}: this machine now serves the clock — next ${clockStamp(set.nextAt || lease.nextAt, lease.clock.tz)}${tick ? ` (${clockTime(tick.slotAt, lease.clock.tz)} was already due and is not run late)` : ""}`);
      return { ...lease, ...set };
    }
    return (await deps.Leases.findById(lease._id).select("-ticks").lean()) || lease;
  }

  /** May this fixed time be started once more? "expired" = its holder went quiet; "failed" = its run ended without finishing. */
  function secondStart(lease: LeaseDoc, now: Date): "expired" | "failed" | null {
    if (lease.manual || !lease.slotAt || lease.attempt < 1 || lease.attempt >= MAX_STARTS[lease.name]) return null;
    if (now.getTime() - new Date(lease.slotAt).getTime() > lateLimitMs(lease.clock)) return null;
    if (lease.holder) return lease.expiresAt && lease.expiresAt.getTime() <= now.getTime() ? "expired" : null;
    return lease.endedAs === "failed" ? "failed" : null;
  }

  function noteOf(claim: ScheduleClaim, tz: string): string {
    const parts: string[] = [];
    if (claim.secondStartOf) parts.push(`start ${claim.attempt} of ${MAX_STARTS[claim.name]} — the first ${claim.secondStartOf}`);
    if (claim.replaced) parts.push(`${claim.replaced.host || claim.replaced.systemId}'s lease ran out at ${clockTime(claim.replaced.expiredAt, tz)}${claim.replaced.slotAt ? ` and its ${clockTime(claim.replaced.slotAt, tz)} run did not finish` : ""}`);
    if (claim.lateMs > 90_000) parts.push(`started ${Math.round(claim.lateMs / 60_000)} min late`);
    if (claim.missedBefore > 0) parts.push(`${claim.missedBefore} earlier time(s) passed with no machine awake`);
    return parts.join("; ");
  }

  /**
   * Write a runner's answer to the lease: its tick, and the run or the release. Throws when the database is away —
   * the answer then stays on the entry and the next poll writes it.
   */
  async function writeVerdict(name: ScheduleName, entry: Held): Promise<void> {
    const v = entry.unwritten;
    if (!v || entry.writing) return;
    const handle: LeaseHandle | ScheduleClaim = entry.claim || { userId: opts.userId, name, token: entry.token };
    entry.writing = true;
    try {
      if (v.started) {
        const kept = await markStarted(handle, { outcome: v.outcome === "took_over" ? "took_over" : "ran", runRef: v.runRef, reason: v.reason }, deps);
        entry.unwritten = null;
        // Lost in the moment it took to start (this machine slept): the next poll's keepHeld tells the runner.
        if (!kept) log(`${name}: the lease was gone when its run had started — the next poll settles it`);
      } else {
        await releaseLease(handle, { outcome: v.outcome === "took_over" ? "ran" : v.outcome, reason: v.reason, runRef: v.runRef }, deps);
        entry.unwritten = null;
        if (held.get(name) === entry) held.delete(name);
      }
    } finally {
      entry.writing = false;
    }
  }

  /** Hand a taken time to its runner, and write what came of it. */
  async function startRun(claim: ScheduleClaim, runner: ScheduleRunner, tz: string): Promise<void> {
    const entry: Held = { token: claim.token, slotAt: claim.slotAt, runRef: null, starting: true, claim, unwritten: null, writing: false };
    held.set(claim.name, entry);
    const takeover = Boolean(claim.replaced || claim.secondStartOf);
    const note = noteOf(claim, tz);
    const join = (reason?: string) => [reason, note].filter(Boolean).join(" · ");
    const at = `${claim.name} ${claim.manual ? "" : `${clockTime(claim.slotAt, tz)} `}`.trimEnd();
    const work: Promise<void> = (async () => {
      let verdict: Verdict;
      try {
        const result = await runner.run(claim);
        if (result.skipped) verdict = { started: false, outcome: "skipped", reason: join(result.reason), runRef: result.runRef ?? null };
        else if (result.done) verdict = { started: false, outcome: "ran", reason: join(result.reason), runRef: result.runRef ?? null };
        else verdict = { started: true, outcome: takeover ? "took_over" : "ran", reason: join(result.reason), runRef: result.runRef ?? null };
      } catch (err) {
        verdict = { started: false, outcome: "failed", reason: join(`could not start: ${err instanceof Error ? err.message.split("\n")[0] : String(err)}`), runRef: null };
      }
      entry.runRef = verdict.runRef;
      entry.unwritten = verdict;
      entry.starting = false;
      const what = verdict.outcome === "ran" ? `ran on ${me.host}` : verdict.outcome === "took_over" ? `took over on ${me.host}` : verdict.outcome;
      log(`${at}: ${what}${verdict.runRef ? ` → ${verdict.runRef.kind} ${verdict.runRef.id}` : ""}${verdict.reason ? ` — ${verdict.reason}` : ""}`);
      await writeVerdict(claim.name, entry);
    })()
      .catch((err) => report(`start ${claim.name}`, err))
      .finally(() => {
        entry.starting = false;
        starting.delete(work);
      });
    starting.add(work);
    // A runner that is slow to answer does not hold up the poll: the lease is renewed while it starts.
    let giveUp: NodeJS.Timeout | null = null;
    await Promise.race([work, new Promise<void>((resolve) => (giveUp = setTimeout(resolve, opts.startWaitMs ?? RUN_START_WAIT_MS)).unref())]);
    if (giveUp) clearTimeout(giveUp);
  }

  async function serve(first: LeaseDoc, settings: () => Promise<Pick<IOrderSyncSettings, "schedules"> | null>): Promise<void> {
    const name = first.name;
    const tz = first.clock.tz;
    let now = deps.now();
    let lease = await keepHeld(first, now);
    const runner = opts.runners[name];
    const mayRun = names.includes(name) && Boolean(runner);

    if (mayRun && !lease.armedAt) lease = await arm(lease, now);

    // The same fixed time once more (the nightly fetch): its run did not finish and the late limit has not passed.
    const again = mayRun ? secondStart(lease, now) : null;
    if (again && switchOn(await settings(), name)) {
      // A holder that went quiet has been gone 5 minutes already; after a failed end a standby gives the preferred machine its grace.
      const wait = again === "failed" && (await waitForPreferred(lease, now.getTime() - new Date(lease.finishedAt || now).getTime(), now));
      if (!wait) {
        const holder: IScheduleHolder = { systemId: me.systemId, host: me.host, role: me.role, pid: me.pid, token: randomUUID() };
        const filter = again === "expired" ? { _id: lease._id, "holder.token": lease.holder?.token, expiresAt: { $lte: now } } : { _id: lease._id, holder: null, endedAs: "failed", slotAt: lease.slotAt, attempt: lease.attempt };
        const res = await deps.Leases.updateOne(filter, {
          $set: { holder, attempt: lease.attempt + 1, manual: false, takenAt: now, renewedAt: now, expiresAt: new Date(now.getTime() + LEASE_MS), runRef: null, finishedAt: null, endedAs: null, ranBy: { systemId: holder.systemId, host: holder.host, token: holder.token } },
        });
        if (res.matchedCount) {
          const how = again === "expired" ? `stopped answering on ${lease.holder?.host || lease.holder?.systemId || "its machine"}` : `ended: ${lease.lastTick?.reason || "failed"}`;
          await startRun({ userId: opts.userId, name, token: holder.token, slotAt: lease.slotAt, attempt: lease.attempt + 1, manual: false, by: "schedule", takenAt: now, holder, lateMs: now.getTime() - new Date(lease.slotAt as Date).getTime(), missedBefore: 0, replaced: null, secondStartOf: how }, runner as ScheduleRunner, tz);
          return;
        }
        lease = (await deps.Leases.findById(lease._id).select("-ticks").lean()) || lease;
      }
    }

    now = deps.now();
    if (now.getTime() < lease.nextAt.getTime()) return;

    // ── a fixed time is due: exactly one write deals with it ─────────────────
    const row = await settings();
    if (!switchSet(row, name)) {
      // Never set: the seed has not reached this workspace. Nothing is decided on a guess.
      wantSeed();
      return;
    }
    const slot = dueSlot(lease, now);
    const next = nextSlot(lease.clock, now);
    const lateMs = now.getTime() - slot.getTime();
    const tooLate = lateMs > lateLimitMs(lease.clock);
    const live = Boolean(lease.holder && lease.expiresAt && lease.expiresAt.getTime() > now.getTime());
    const untouched = { _id: lease._id, nextAt: lease.nextAt };
    const moveOn = { nextAt: next, lastSlotAt: slot };
    const record = async (outcome: "skipped" | "missed", reason: string) => {
      const tick = tickOf(outcome, reason, me, slot, now);
      const res = await deps.Leases.updateOne(untouched, { $set: { ...moveOn, lastTick: tick }, $push: pushTick(tick) });
      if (res.matchedCount) log(`${name} ${clockTime(slot, tz)}: ${outcome} — ${reason} · next ${clockStamp(next, tz)}`);
    };

    if (!switchOn(row, name)) return record("skipped", "switched off");
    if (live) {
      // The machine that holds it, or any that may run it, says so on time; one that may not run it only once the limit has passed.
      if (!mayRun && !tooLate) return;
      const h = lease.holder as IScheduleHolder;
      return record("skipped", lease.manual ? `a run started by hand is still going on ${h.host || h.systemId}` : `the ${clockTime(lease.slotAt, tz)} run is still going on ${h.host || h.systemId}`);
    }
    if (tooLate) return record("missed", `no machine that may run it was awake within ${lateLimitMs(lease.clock) / 60_000} min (looked at ${clockTime(now, tz)})`);
    if (!mayRun) return;
    if (await waitForPreferred(lease, lateMs, now)) return;

    const holder: IScheduleHolder = { systemId: me.systemId, host: me.host, role: me.role, pid: me.pid, token: randomUUID() };
    const res = await deps.Leases.updateOne(
      { ...untouched, $or: [{ holder: null }, { expiresAt: { $lte: now } }] },
      { $set: { ...moveOn, holder, slotAt: slot, attempt: 1, manual: false, takenAt: now, renewedAt: now, expiresAt: new Date(now.getTime() + LEASE_MS), runRef: null, finishedAt: null, endedAs: null, ranBy: { systemId: holder.systemId, host: holder.host, token: holder.token } } }
    );
    if (!res.matchedCount) return; // another machine took this time, or moved the clock on
    const replaced = lease.holder ? { systemId: lease.holder.systemId, host: lease.holder.host, slotAt: lease.slotAt ?? null, expiredAt: lease.expiresAt ?? null } : null;
    const missedBefore = Math.max(0, Math.round((slot.getTime() - lease.nextAt.getTime()) / periodMs(lease.clock)));
    await startRun({ userId: opts.userId, name, token: holder.token, slotAt: slot, attempt: 1, manual: false, by: "schedule", takenAt: now, holder, lateMs, missedBefore, replaced, secondStartOf: "" }, runner as ScheduleRunner, tz);
  }

  async function pollOnce(): Promise<void> {
    if (polling) return;
    polling = true;
    try {
      const now = deps.now();
      if (now.getTime() - presenceAt >= PRESENCE_EVERY_MS || now.getTime() < presenceAt) {
        presenceAt = now.getTime();
        await writePresence(now).catch((err) => report("presence", err));
      }
      if (!serves.length) return;
      const leases = (await deps.Leases.find({ userId: uid, name: { $in: serves } }).select("-ticks").lean()) as LeaseDoc[];
      if (leases.length < serves.length) wantSeed();
      let row: Promise<Pick<IOrderSyncSettings, "schedules"> | null> | null = null;
      const settings = () => (row ??= deps.Settings.findOne({ userId: uid }).select("schedules").lean().exec());
      for (const lease of leases) {
        try {
          await serve(lease, settings);
        } catch (err) {
          report(lease.name, err);
        }
      }
    } catch (err) {
      report("poll", err);
    } finally {
      polling = false;
    }
  }

  function arrange() {
    if (stopped) return;
    timer = setTimeout(() => void pollOnce().finally(arrange), pollDelayMs());
    timer.unref();
  }

  return {
    me,
    pollOnce,
    start() {
      if (started || stopped) return;
      started = true;
      log(`clock on ${me.host} (${me.role}) — serves ${serves.join(", ") || "nothing"}; may run ${names.join(", ") || "none of them"}`);
      void pollOnce().finally(arrange);
    },
    stop() {
      stopped = true;
      if (timer) clearTimeout(timer);
      timer = null;
    },
    holds(name) {
      const h = held.get(name);
      return h ? { userId: opts.userId, name, token: h.token } : null;
    },
    async idle() {
      while (starting.size) await Promise.all([...starting]);
    },
  };
}

/** Create the clock of this process and start polling. */
export function startScheduleClock(opts: ScheduleClockOptions): ScheduleClock {
  const clock = createScheduleClock(opts);
  clock.start();
  return clock;
}
