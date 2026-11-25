import { Types } from "mongoose";
import { config } from "../config.js";
import type { IScheduleSwitch } from "../models/OrderSyncSettings.js";
import type { IScheduleHost } from "../models/ScheduleHost.js";
import {
  isScheduleName,
  SCHEDULE_LABELS,
  SCHEDULE_NAMES,
  SCHEDULE_ROLE,
  type IScheduleClock,
  type IScheduleLease,
  type IScheduleRunRef,
  type IScheduleTick,
  type ScheduleName,
  type ScheduleRole,
} from "../models/ScheduleLease.js";
import { getInvoiceQueue, getUpdateQueue } from "../queue.js";
import { RequestInputError } from "./requestCommon.js";
import { clockStamp, clockTime, describeClock, nextSlot, scheduleDefaults, SCHEDULE_TIMEZONES, switchSet, tickText, type ScheduleDeps } from "./scheduleClock.js";

/**
 * The schedules' switches, what a machine's .env still decides, the one-time
 * seed, and what Settings reads.
 *
 *   switches   order_sync_settings.schedules.<name> = { on, changedBy,
 *              changedAt } — one per schedule, in the database, shared by
 *              every machine. The clock reads it at every tick; a change
 *              applies to the next time, never to a run in progress.
 *   the .env   only says whether THIS machine may run a schedule
 *              (eligibleHere) — it can never switch one on:
 *                UPDATE_SCHEDULE=0    never runs update_active / update_ofd
 *                INVOICE_SCHEDULE=0   never runs invoice_sweep
 *                FETCH_SCHEDULE       0, off or unset: never runs the fetch;
 *                                     any other value: it may
 *              WORKER_ROLES still decides which process serves which
 *              schedule (servedBy). UPDATE_SWEEP_MS, UPDATE_OFD_SWEEP_MS,
 *              INVOICE_SWEEP_MS, CRON_TIMEZONE and the time in
 *              FETCH_SCHEDULE are read by the first seed only.
 *   the seed   at the start of the API and of the worker: writes the switch
 *              paths and the clock documents that do not exist yet, nothing
 *              else. Each sweep's switch starts as this machine's .env has
 *              it; the nightly fetch always starts OFF (the owner switches it
 *              on in Settings). Clocks: fetch daily at the FETCH_SCHEDULE time
 *              (05:00), active units every 6 h at :15, out for delivery every
 *              2 h at :45, invoices every 3 h at :35, this machine preferred.
 *   reads      scheduleOverview (GET /api/orders/schedules) and
 *              scheduleStatuses (the older schedule routes).
 */

type Env = Record<string, string | undefined>;

const OFF_WORDS = new Set(["0", "off", "false", "no"]);
const word = (value: string | undefined) => String(value ?? "").trim().toLowerCase();

/** The .env key that decides whether a machine may run a schedule. */
export const ELIGIBILITY_ENV: Record<ScheduleName, string> = { fetch: "FETCH_SCHEDULE", update_active: "UPDATE_SCHEDULE", update_ofd: "UPDATE_SCHEDULE", invoice_sweep: "INVOICE_SCHEDULE" };

/** May this machine run the schedule? A hard off for this machine only; the switch in Settings decides whether it runs at all. */
export function eligibleHere(name: ScheduleName, env: Env = process.env): boolean {
  const value = word(env[ELIGIBILITY_ENV[name]]);
  // The sweeps: allowed unless the line says off. The fetch: allowed only when the line says something else than off.
  return name === "fetch" ? value !== "" && !OFF_WORDS.has(value) : !OFF_WORDS.has(value);
}

/** The schedules a process kind serves: the API the nightly fetch, a worker the sweeps of the roles it has (WORKER_ROLES). */
export function servedBy(role: ScheduleRole, workerRoles?: Iterable<string>): ScheduleName[] {
  const roles = workerRoles ? new Set(workerRoles) : null;
  return SCHEDULE_NAMES.filter((n) => SCHEDULE_ROLE[n] === role && (role === "api" || !roles || roles.has(n === "invoice_sweep" ? "invoice" : "update")));
}

function uid(userId: string): Types.ObjectId {
  if (!Types.ObjectId.isValid(userId)) throw new RequestInputError("userId is not a workspace id", 400, "invalid_request");
  return new Types.ObjectId(userId);
}

// ── the seed ────────────────────────────────────────────────────────────────

const TZ_ALIASES: Record<string, string> = { "asia/kolkata": "Asia/Kolkata", "asia/calcutta": "Asia/Kolkata", utc: "UTC", "etc/utc": "UTC" };

/** Minutes from a *_SWEEP_MS value, with the floor and default its old BullMQ schedule had. */
function periodMinutes(raw: string | undefined, defaultMin: number, floorMin: number): number {
  const ms = Number(raw);
  return Math.max(floorMin, Math.round((Number.isFinite(ms) && ms > 0 ? ms : defaultMin * 60_000) / 60_000));
}

/**
 * The four clocks a first seed writes, from this machine's .env. Throws on a
 * CRON_TIMEZONE the slot maths does not know.
 */
export function seedClocks(env: Env = process.env, systemId: string = config.systemId): { clocks: Record<ScheduleName, IScheduleClock>; notes: string[] } {
  const notes: string[] = [];
  const rawTz = String(env.CRON_TIMEZONE || "Asia/Kolkata").trim();
  const tz = TZ_ALIASES[rawTz.toLowerCase()];
  if (!tz || SCHEDULE_TIMEZONES[tz] === undefined) throw new Error(`CRON_TIMEZONE "${rawTz}" is not supported by the schedule clock — use Asia/Kolkata or UTC (or leave it out)`);

  // FETCH_SCHEDULE: "M H * * *" gives the time; 1 / on / true (and 0 / off / unset, which only say "not here") give 05:00.
  let at = "05:00";
  const rawFetch = String(env.FETCH_SCHEDULE ?? "").trim();
  const cron = /^(\d{1,2})\s+(\d{1,2})\s+\*\s+\*\s+\*$/.exec(rawFetch);
  if (cron && Number(cron[1]) <= 59 && Number(cron[2]) <= 23) at = `${cron[2].padStart(2, "0")}:${cron[1].padStart(2, "0")}`;
  else if (rawFetch && !OFF_WORDS.has(rawFetch.toLowerCase()) && !["1", "on", "true", "yes"].includes(rawFetch.toLowerCase())) notes.push(`FETCH_SCHEDULE "${rawFetch}" is not a daily "M H * * *" time — the nightly fetch clock is set to 05:00`);

  const base = { tz, preferSystemId: systemId, standbyGraceSec: 120, preferChangedBy: "", preferChangedAt: null };
  const every = (everyMin: number, phaseMin: number): IScheduleClock => ({ ...base, kind: "every", at: "", everyMin, phaseMin: phaseMin % everyMin, lateMin: Math.max(1, Math.floor(everyMin / 2)) });
  return {
    clocks: {
      fetch: { ...base, kind: "daily", at, everyMin: 0, phaseMin: 0, lateMin: 180 },
      update_active: every(periodMinutes(env.UPDATE_SWEEP_MS, 360, 15), 15),
      update_ofd: every(periodMinutes(env.UPDATE_OFD_SWEEP_MS, 120, 15), 45),
      invoice_sweep: every(periodMinutes(env.INVOICE_SWEEP_MS, 180, 30), 35),
    },
    notes,
  };
}

export interface SeedOptions {
  /** Default: this process's environment. */
  env?: Env;
  /** Default: this machine (TRADEFLOW_SYSTEM_ID / TRADEFLOW_HOST_ID). */
  me?: { systemId: string; host: string };
  deps?: ScheduleDeps;
}

export interface SeedPlan {
  userId: string;
  /** What a seeded switch's changedBy reads. */
  by: string;
  systemId: string;
  host: string;
  eligible: Record<ScheduleName, boolean>;
  switches: Array<{ name: ScheduleName; label: string; write: boolean; on: boolean; why: string; current: IScheduleSwitch | null }>;
  clocks: Array<{ name: ScheduleName; label: string; write: boolean; clock: IScheduleClock; text: string; nextAt: Date; current: { text: string; nextAt: Date | null; preferSystemId: string } | null }>;
  notes: string[];
}

/**
 * What a seed would write for this workspace, and what is already there —
 * read-only (scripts/scheduleSeed.ts --check prints it).
 */
export async function seedPlan(userId: string, opts: SeedOptions = {}): Promise<SeedPlan> {
  const deps = opts.deps || scheduleDefaults;
  const env = opts.env || process.env;
  const me = opts.me || { systemId: config.systemId, host: config.hostId };
  const id = uid(userId);
  const now = deps.now();
  const { clocks, notes } = seedClocks(env, me.systemId);
  const [row, leases] = await Promise.all([deps.Settings.findOne({ userId: id }).select("schedules").lean(), deps.Leases.find({ userId: id }).select("name clock nextAt").lean()]);
  const eligible = Object.fromEntries(SCHEDULE_NAMES.map((n) => [n, eligibleHere(n, env)])) as Record<ScheduleName, boolean>;
  return {
    userId,
    by: `seed:${me.host}`,
    systemId: me.systemId,
    host: me.host,
    eligible,
    switches: SCHEDULE_NAMES.map((name) => {
      const set = switchSet(row, name);
      // The nightly fetch starts OFF whatever FETCH_SCHEDULE says: the owner switches it on. The sweeps start as this machine runs them today.
      const on = name === "fetch" ? false : eligible[name];
      const why = name === "fetch" ? "the nightly fetch always starts off — it is switched on in Settings" : `${ELIGIBILITY_ENV[name]} in this machine's .env ${eligible[name] ? "lets it run" : "says off"}`;
      return { name, label: SCHEDULE_LABELS[name], write: !set, on: set ? row?.schedules?.[name]?.on === true : on, why: set ? "already set — left as it is" : why, current: set ? (row?.schedules?.[name] as IScheduleSwitch) : null };
    }),
    clocks: SCHEDULE_NAMES.map((name) => {
      const have = leases.find((l) => l.name === name);
      const clock = have?.clock || clocks[name];
      return {
        name,
        label: SCHEDULE_LABELS[name],
        write: !have,
        clock,
        text: describeClock(clock),
        nextAt: have?.nextAt || nextSlot(clock, now),
        current: have ? { text: describeClock(have.clock), nextAt: have.nextAt ?? null, preferSystemId: have.clock.preferSystemId || "" } : null,
      };
    }),
    notes,
  };
}

const isDuplicateKey = (err: unknown) => (err as { code?: number })?.code === 11000;

const indexed = new WeakSet<object>();

/**
 * The indexes of the two schedule collections — one clock per (workspace, schedule), and the 7-day life of a presence
 * row — built here, once per process and before the first insert. Their models never build them by themselves, so
 * that nothing but a seed creates these collections. Throws when the database refuses: without the unique index two
 * seeds at the same moment could both insert a clock.
 */
export async function ensureScheduleIndexes(deps: ScheduleDeps = scheduleDefaults): Promise<void> {
  for (const model of [deps.Leases, deps.Hosts] as Array<{ createIndexes(): Promise<unknown> }>) {
    if (indexed.has(model)) continue;
    await model.createIndexes();
    indexed.add(model);
  }
}

/**
 * Write the switches and clocks this workspace does not have yet — and only
 * those: a switch somebody set, or a clock that exists, is never touched, so
 * running it again (the other process, the other Mac, a restart) changes
 * nothing. Two seeds at the same moment are safe: the unique index makes one
 * of two inserts fail, and that failure means "it exists".
 */
export async function seedSchedules(userId: string, opts: SeedOptions = {}): Promise<{ plan: SeedPlan; switchesWritten: ScheduleName[]; clocksWritten: ScheduleName[] }> {
  const deps = opts.deps || scheduleDefaults;
  const id = uid(userId);
  const plan = await seedPlan(userId, opts);
  // Both collections and their indexes first: before a clock is inserted, and before a clock process writes its presence.
  await ensureScheduleIndexes(deps);
  const now = deps.now();
  const switchesWritten: ScheduleName[] = [];
  const clocksWritten: ScheduleName[] = [];

  const missing = plan.switches.filter((s) => s.write);
  if (missing.length) {
    // The row itself first (a workspace nothing has saved a setting for), then each switch only where none is set.
    await deps.Settings.updateOne({ userId: id }, { $setOnInsert: { userId: id } }, { upsert: true }).catch((err) => {
      if (!isDuplicateKey(err)) throw err;
    });
    for (const s of missing) {
      const res = await deps.Settings.updateOne({ userId: id, [`schedules.${s.name}.on`]: { $nin: [true, false] } }, { $set: { [`schedules.${s.name}`]: { on: s.on, changedBy: plan.by, changedAt: now } } });
      if (res.modifiedCount) switchesWritten.push(s.name);
    }
  }
  for (const c of plan.clocks.filter((x) => x.write)) {
    try {
      const res = await deps.Leases.updateOne(
        { userId: id, name: c.name },
        { $setOnInsert: { userId: id, name: c.name, clock: c.clock, nextAt: c.nextAt, lastSlotAt: null, armedAt: null, holder: null, attempt: 0, manual: false, ticks: [] } },
        { upsert: true }
      );
      if (res.upsertedCount) clocksWritten.push(c.name);
    } catch (err) {
      if (!isDuplicateKey(err)) throw err;
    }
  }
  return { plan, switchesWritten, clocksWritten };
}

/** The seed as the API and the worker call it at start: one line of what it wrote, and never a reason not to start. */
export async function seedSchedulesAtStart(userId: string, opts: SeedOptions = {}): Promise<void> {
  if (!userId) {
    console.log("[schedules] TRADE_FLOW_USER_ID is not set — no schedule switches or clocks");
    return;
  }
  try {
    const { plan, switchesWritten, clocksWritten } = await seedSchedules(userId, opts);
    for (const note of plan.notes) console.warn(`[schedules] ${note}`);
    if (!switchesWritten.length && !clocksWritten.length) {
      console.log("[schedules] switches and clocks are in place — nothing written");
      return;
    }
    const sw = plan.switches.filter((s) => switchesWritten.includes(s.name)).map((s) => `${s.name} ${s.on ? "ON" : "OFF"}`);
    const cl = plan.clocks.filter((c) => clocksWritten.includes(c.name)).map((c) => `${c.name} ${c.text} (next ${clockStamp(c.nextAt, c.clock.tz)})`);
    console.log(`[schedules] first start on this workspace (${plan.by})${sw.length ? ` — switches: ${sw.join(", ")}` : ""}${cl.length ? ` — clocks: ${cl.join("; ")}` : ""}`);
  } catch (err) {
    console.warn(`[schedules] could not write the schedule switches and clocks: ${err instanceof Error ? err.message.split("\n")[0] : err}`);
  }
}

// ── switches ────────────────────────────────────────────────────────────────

export interface ScheduleSwitchView {
  on: boolean;
  /** ui = a person set it; seed = still the value Trade Flow wrote at its first start; unset = nothing has set it (it does not run). */
  source: "ui" | "seed" | "unset";
  changedBy: string;
  changedAt: Date | null;
}

function switchView(sw: IScheduleSwitch | null | undefined): ScheduleSwitchView {
  if (typeof sw?.on !== "boolean") return { on: false, source: "unset", changedBy: "", changedAt: null };
  const changedBy = sw.changedBy || "";
  return { on: sw.on, source: changedBy.startsWith("seed:") ? "seed" : "ui", changedBy, changedAt: sw.changedAt || null };
}

/** The four switches of a workspace: one read. */
export async function resolveScheduleSwitches(userId: string, deps: ScheduleDeps = scheduleDefaults): Promise<Record<ScheduleName, ScheduleSwitchView>> {
  const row = Types.ObjectId.isValid(userId) ? await deps.Settings.findOne({ userId }).select("schedules").lean() : null;
  return Object.fromEntries(SCHEDULE_NAMES.map((n) => [n, switchView(row?.schedules?.[n])])) as Record<ScheduleName, ScheduleSwitchView>;
}

function scheduleName(name: unknown): ScheduleName {
  if (!isScheduleName(name)) throw new RequestInputError(`Unknown schedule "${String(name ?? "")}" — it is one of ${SCHEDULE_NAMES.join(", ")}`, 400, "unknown_schedule");
  return name;
}

/** Switch a schedule on or off. It changes the next time; a run in progress finishes (stop it in Batch requests). */
export async function setScheduleSwitch(userId: string, name: unknown, on: unknown, by: string, deps: ScheduleDeps = scheduleDefaults): Promise<ScheduleSwitchView> {
  const n = scheduleName(name);
  if (typeof on !== "boolean") throw new RequestInputError("on must be true or false", 400, "invalid_request");
  const id = uid(userId);
  const sw: IScheduleSwitch = { on, changedBy: String(by || "").trim(), changedAt: deps.now() };
  await deps.Settings.updateOne({ userId: id }, { $set: { [`schedules.${n}`]: sw }, $setOnInsert: { userId: id } }, { upsert: true });
  console.log(`[schedules] ${n} switched ${on ? "ON" : "OFF"}${sw.changedBy ? ` by ${sw.changedBy}` : ""}`);
  return switchView(sw);
}

/** Which machine runs a schedule when it is awake (the others take over after the grace). "" = whichever looks first. */
export async function setSchedulePreference(userId: string, name: unknown, systemId: unknown, by: string, deps: ScheduleDeps = scheduleDefaults): Promise<string> {
  const n = scheduleName(name);
  if (typeof systemId !== "string" || systemId.length > 100 || /\s/.test(systemId.trim())) throw new RequestInputError("preferSystemId must be a machine's system id (or empty for no preference)", 400, "invalid_request");
  const prefer = systemId.trim();
  const res = await deps.Leases.updateOne({ userId: uid(userId), name: n }, { $set: { "clock.preferSystemId": prefer, "clock.preferChangedBy": String(by || "").trim(), "clock.preferChangedAt": deps.now() } });
  if (!res.matchedCount) throw new RequestInputError(`The ${SCHEDULE_LABELS[n].toLowerCase()} has no clock yet — Trade Flow creates it when it starts`, 409, "no_clock");
  console.log(`[schedules] ${n} runs on ${prefer || "whichever machine looks first"}${by ? ` (set by ${by})` : ""}`);
  return prefer;
}

// ── what Settings reads ─────────────────────────────────────────────────────

/** The BullMQ job schedulers that ran the sweeps before the shared clock, per machine, in that machine's Redis. */
export const LEGACY_SCHEDULERS: Partial<Record<ScheduleName, { queue: "update" | "invoice"; id: string }>> = {
  update_active: { queue: "update", id: "order-update-active" },
  update_ofd: { queue: "update", id: "order-update-ofd" },
  invoice_sweep: { queue: "invoice", id: "order-invoice-sweep" },
};

export type LegacyClocks = Partial<Record<ScheduleName, { scheduler: string; everyMs: number | null; nextAt: Date | null }>>;

const LEGACY_READ_MS = 1500;

/** As much of a BullMQ queue as the question needs. */
interface SchedulerQueue {
  getJobScheduler(id: string): Promise<{ every?: number | string; next?: number | null } | undefined>;
}

/**
 * Which of those old schedulers this machine's Redis still holds — read-only.
 * A worker that has not been restarted onto the shared clock keeps running
 * them, whatever the switches say; Settings must be able to say so. Null =
 * Redis did not answer in time (the overview is not held up by it).
 */
export async function readLegacyClocks(queues: { update: () => SchedulerQueue; invoice: () => SchedulerQueue } = { update: getUpdateQueue, invoice: getInvoiceQueue }, waitMs: number = LEGACY_READ_MS): Promise<LegacyClocks | null> {
  const read = (async () => {
    const out: LegacyClocks = {};
    for (const name of SCHEDULE_NAMES) {
      const old = LEGACY_SCHEDULERS[name];
      if (!old) continue;
      const row = await queues[old.queue]().getJobScheduler(old.id);
      if (row) out[name] = { scheduler: old.id, everyMs: Number(row.every) || null, nextAt: row.next ? new Date(row.next) : null };
    }
    return out;
  })().catch(() => null);
  return Promise.race([read, new Promise<null>((resolve) => setTimeout(() => resolve(null), waitMs).unref())]);
}

/** As much of a BullMQ queue as removing a scheduler needs. */
interface RetiringQueue extends SchedulerQueue {
  removeJobScheduler(id: string): Promise<boolean>;
  getJob(id: string): Promise<{ remove(): Promise<unknown> } | undefined | null>;
}

/**
 * (worker, at start) Remove the old per-machine BullMQ schedulers of these
 * schedules from this machine's Redis. Always, whatever the .env or the
 * switches say: the shared clock is the only clock now.
 *
 * BullMQ's own removal takes the scheduler and its next job while that job is
 * still DELAYED. The job of a time that has just come is already waiting for a
 * worker and is not touched by it — so that one is removed here, by its id
 * (repeat:<scheduler>:<its time>). One a worker has already taken cannot be
 * removed; it runs no sweep any more (it only makes the clock look).
 *
 * Idempotent, and never a reason not to start: a scheduler that could not be
 * removed is said, and tried again when a job of it arrives. Answers the ids
 * that were there and are gone.
 */
export async function retireLegacySchedulers(
  names: readonly ScheduleName[],
  queues: { update: () => RetiringQueue; invoice: () => RetiringQueue } = { update: getUpdateQueue, invoice: getInvoiceQueue }
): Promise<string[]> {
  const removed: string[] = [];
  for (const name of names) {
    const old = LEGACY_SCHEDULERS[name];
    if (!old) continue;
    try {
      const queue = queues[old.queue]();
      const next = (await queue.getJobScheduler(old.id))?.next;
      if (await queue.removeJobScheduler(old.id)) removed.push(old.id);
      if (next) await (await queue.getJob(`repeat:${old.id}:${next}`))?.remove().catch(() => undefined);
    } catch (err) {
      console.warn(`[schedules] the old ${old.id} scheduler could not be removed from this machine's Redis (${err instanceof Error ? err.message.split("\n")[0] : err}) — a job it fires runs no sweep; it is tried again when one arrives, and at the next worker start`);
    }
  }
  if (removed.length) console.log(`[schedules] retired this machine's old BullMQ scheduler(s) ${removed.join(", ")} and their next jobs — the shared clock runs ${removed.length > 1 ? "them" : "it"} now`);
  return removed;
}

/** What the overview asks outside Mongo. The check answers it itself for the route cases, so they open no Redis. */
export const scheduleProbes: { legacyClocks: () => Promise<LegacyClocks | null> } = { legacyClocks: () => readLegacyClocks() };

/** A presence row counts as "up" for this long after it was last written (it is written once a minute). */
const MACHINE_UP_MS = 3 * 60_000;

/**
 * Is a process of THIS machine that serves the schedule on the shared clock up?
 * It is what keeps a lease taken by hand renewed and gives it back when its run
 * closes. False while this machine's worker still runs the code from before the
 * shared clock (it writes no presence row), or is not running.
 */
export async function clockProcessUp(name: ScheduleName, deps: ScheduleDeps = scheduleDefaults, systemId: string = config.systemId): Promise<boolean> {
  const row = await deps.Hosts.findById(`${systemId}:${SCHEDULE_ROLE[name]}`).select("seenAt serves").lean();
  return Boolean(row?.seenAt && deps.now().getTime() - new Date(row.seenAt).getTime() <= MACHINE_UP_MS && (row.serves || []).includes(name));
}

export interface TickView extends IScheduleTick {
  /** The tick as one sentence: "ran on Mac", "skipped — switched off". */
  text: string;
}

export interface LeaseView {
  systemId: string;
  host: string;
  role: ScheduleRole;
  pid: number;
  slotAt: Date | null;
  attempt: number;
  manual: boolean;
  takenAt: Date | null;
  renewedAt: Date | null;
  expiresAt: Date | null;
  /** False = its machine stopped renewing it: nothing may be running any more. */
  live: boolean;
  runRef: IScheduleRunRef | null;
}

export interface ScheduleStatus extends ScheduleSwitchView {
  name: ScheduleName;
  label: string;
  /** The process kind that runs it. */
  role: ScheduleRole;
  /** May the machine answering run it (its .env)? */
  eligibleHere: boolean;
  clock: (IScheduleClock & { text: string }) | null;
  nextAt: Date | null;
  /** The next time has passed and nothing has dealt with it yet. */
  overdue: boolean;
  armedAt: Date | null;
  /** Who holds the lease now; null = nothing is running. */
  lease: LeaseView | null;
  lastRun: { slotAt: Date | null; manual: boolean; attempt: number; systemId: string; host: string; takenAt: Date | null; finishedAt: Date | null; endedAs: string | null; runRef: IScheduleRunRef | null } | null;
  lastTick: TickView | null;
}

type LeaseRow = Omit<IScheduleLease, "ticks"> & { ticks?: IScheduleTick[] };

const tickView = (t: IScheduleTick | null | undefined): TickView | null => (t ? { slotAt: t.slotAt ?? null, at: t.at, outcome: t.outcome, reason: t.reason || "", systemId: t.systemId || "", host: t.host || "", runRef: t.runRef ? { kind: t.runRef.kind, id: t.runRef.id } : null, text: tickText(t) } : null);

function statusOf(name: ScheduleName, sw: ScheduleSwitchView, lease: LeaseRow | undefined, now: Date): ScheduleStatus {
  const holder = lease?.holder || null;
  return {
    name,
    label: SCHEDULE_LABELS[name],
    role: SCHEDULE_ROLE[name],
    ...sw,
    eligibleHere: eligibleHere(name),
    clock: lease ? { ...lease.clock, text: describeClock(lease.clock) } : null,
    nextAt: lease?.nextAt ?? null,
    overdue: Boolean(lease?.nextAt && lease.nextAt.getTime() <= now.getTime() - 90_000),
    armedAt: lease?.armedAt ?? null,
    // The token never leaves Trade Flow: it is what renews and releases the lease.
    lease: holder
      ? { systemId: holder.systemId, host: holder.host, role: holder.role, pid: holder.pid, slotAt: lease?.slotAt ?? null, attempt: lease?.attempt || 0, manual: Boolean(lease?.manual), takenAt: lease?.takenAt ?? null, renewedAt: lease?.renewedAt ?? null, expiresAt: lease?.expiresAt ?? null, live: Boolean(lease?.expiresAt && lease.expiresAt.getTime() > now.getTime()), runRef: lease?.runRef ?? null }
      : null,
    lastRun: lease?.takenAt
      ? { slotAt: lease.slotAt ?? null, manual: Boolean(lease.manual), attempt: lease.attempt || 0, systemId: lease.ranBy?.systemId || "", host: lease.ranBy?.host || "", takenAt: lease.takenAt, finishedAt: lease.finishedAt ?? null, endedAs: lease.endedAs ?? null, runRef: lease.runRef ?? null }
      : null,
    lastTick: tickView(lease?.lastTick),
  };
}

/** Switch, clock, next time, lease and last tick of some schedules: two reads. For the routes that had a shape before the clock. */
export async function scheduleStatuses<N extends ScheduleName>(userId: string, names: readonly N[], deps: ScheduleDeps = scheduleDefaults): Promise<Record<N, ScheduleStatus>> {
  const id = uid(userId);
  const [switches, leases] = await Promise.all([resolveScheduleSwitches(userId, deps), deps.Leases.find({ userId: id, name: { $in: names } }).select("-ticks").lean()]);
  const now = deps.now();
  return Object.fromEntries(names.map((n) => [n, statusOf(n, switches[n], leases.find((l) => l.name === n), now)])) as Record<N, ScheduleStatus>;
}

export interface MachineView {
  id: string;
  systemId: string;
  role: ScheduleRole;
  host: string;
  machine: string;
  pid: number;
  serves: string[];
  eligible: string[];
  startedAt: Date;
  seenAt: Date;
  /** Seen in the last 3 minutes. */
  up: boolean;
}

export interface ScheduleView extends ScheduleStatus {
  /** The newest 20, newest first. */
  ticks: TickView[];
  /** The machines up now whose process serves this schedule, and of them those that may run it. */
  servedOn: string[];
  runnableOn: string[];
  /** This machine's Redis still holds the sweep's old BullMQ scheduler: its worker runs it on its own clock, whatever the switch says. */
  legacy: { scheduler: string; everyMs: number | null; nextAt: Date | null } | null;
  /**
   * In one word, and one sentence:
   *   running      a machine holds a live lease
   *   own_clock    the worker here has not moved to the shared clock yet (see `legacy`)
   *   off          switched off
   *   no_machine   switched on, but no machine that may run it is up
   *   scheduled    switched on, and a machine that may run it is up
   *   not_seeded   Trade Flow has not created its clock or its switch yet
   */
  state: "running" | "own_clock" | "off" | "no_machine" | "scheduled" | "not_seeded";
  note: string;
}

export interface ScheduleOverview {
  timezone: string;
  now: Date;
  /** The machine answering (the API). */
  here: { systemId: string; host: string; eligible: ScheduleName[] };
  machines: MachineView[];
  schedules: ScheduleView[];
}

const TICKS_SHOWN = 20;

function machineView(h: IScheduleHost, now: Date): MachineView {
  return { id: String(h._id), systemId: h.systemId, role: h.role, host: h.host || h.systemId, machine: h.machine || "", pid: h.pid || 0, serves: h.serves || [], eligible: h.eligible || [], startedAt: h.startedAt, seenAt: h.seenAt, up: now.getTime() - new Date(h.seenAt).getTime() <= MACHINE_UP_MS };
}

/**
 * GET /api/orders/schedules — the four schedules as they are: the switch, the
 * clock and its next time, who holds the lease, the last ticks, the machines
 * seen, and for each schedule one word and one sentence for where it stands.
 * That sentence is the truth also while a worker still runs its old BullMQ
 * clock (`own_clock`): the switch does not reach that worker until it has been
 * restarted.
 */
export async function scheduleOverview(userId: string, deps: ScheduleDeps = scheduleDefaults, probes = scheduleProbes): Promise<ScheduleOverview> {
  const id = uid(userId);
  const [switches, leases, hosts, legacy] = await Promise.all([resolveScheduleSwitches(userId, deps), deps.Leases.find({ userId: id }).lean(), deps.Hosts.find({}).sort({ _id: 1 }).lean(), probes.legacyClocks().catch(() => null)]);
  const now = deps.now();
  const machines = hosts.map((h) => machineView(h, now));
  const tz = leases[0]?.clock.tz || "Asia/Kolkata";
  const schedules = SCHEDULE_NAMES.map((name): ScheduleView => {
    const lease = leases.find((l) => l.name === name);
    const st = statusOf(name, switches[name], lease, now);
    const zone = lease?.clock.tz || tz;
    const serving = machines.filter((m) => m.up && m.role === st.role && m.serves.includes(name));
    const runnable = serving.filter((m) => m.eligible.includes(name));
    const old = legacy?.[name] || null;
    const base = { ...st, ticks: (lease?.ticks || []).slice(-TICKS_SHOWN).reverse().map((t) => tickView(t) as TickView), servedOn: serving.map((m) => m.host), runnableOn: runnable.map((m) => m.host), legacy: old };
    const nextText = st.nextAt ? clockStamp(st.nextAt, zone) : "?";
    const kind = st.role === "worker" ? "worker" : "Trade Flow API";
    let state: ScheduleView["state"];
    let note: string;
    if (!lease || st.source === "unset") {
      state = "not_seeded";
      note = "Trade Flow has not set this schedule up yet — it does when its API or worker next starts";
    } else if (st.lease?.live) {
      state = "running";
      note = `Running on ${st.lease.host || st.lease.systemId} since ${clockTime(st.lease.takenAt, zone)}${st.lease.manual ? " (started by hand)" : ""}`;
    } else if (old && !st.armedAt) {
      state = "own_clock";
      note = `The worker on ${config.hostId} still runs this on its own clock${old.nextAt ? ` (next ${clockStamp(old.nextAt, zone)})` : ""} — the switch and the times here apply once that worker has been restarted`;
    } else if (!st.on) {
      state = "off";
      note = st.overdue ? "Switched off" : `Switched off — the ${nextText} time will be skipped`;
    } else if (!runnable.length) {
      state = "no_machine";
      note = serving.length
        ? `No machine that may run it is up — ${serving.map((m) => m.host).join(", ")} ${serving.length > 1 ? "have" : "has"} ${ELIGIBILITY_ENV[name]} off in the .env`
        : `No ${kind} on the shared clock is up${st.armedAt ? "" : " yet"} — nothing runs until one starts`;
      // Redis did not answer: whether a worker here still runs its own clock is not known, and is not claimed.
      if (!legacy && LEGACY_SCHEDULERS[name] && !st.armedAt) note += " (this machine's Redis did not answer, so a worker still on its own clock cannot be ruled out)";
    } else {
      state = "scheduled";
      const prefer = lease.clock.preferSystemId;
      const first = runnable.find((m) => m.systemId === prefer) || runnable[0];
      note = st.overdue ? `Due since ${nextText} — ${first.host} takes it on its next look` : `Next ${nextText} on ${first.host}${runnable.length > 1 ? ` · ${runnable.filter((m) => m !== first).map((m) => m.host).join(", ")} take${runnable.length > 2 ? "" : "s"} over after ${Math.round((lease.clock.standbyGraceSec ?? 120) / 60)} min` : ""}`;
    }
    // A lease nobody renews any more: its machine went quiet in the middle of a run (asleep, stopped). Not "running".
    if (st.lease && !st.lease.live) note += ` · ${st.lease.host || st.lease.systemId} has not renewed its lease since ${clockTime(st.lease.renewedAt, zone)} (it ran out at ${clockTime(st.lease.expiresAt, zone)}) — it carries on if it wakes first, else the next time is taken over`;
    // Both clocks at once is never right: say so wherever the old scheduler is still there.
    if (old && state !== "own_clock") note += ` · this machine's Redis still holds the old ${old.scheduler} scheduler — restart the worker`;
    return { ...base, state, note };
  });
  return { timezone: tz, now, here: { systemId: config.systemId, host: config.hostId, eligible: SCHEDULE_NAMES.filter((n) => eligibleHere(n)) }, machines, schedules };
}

/**
 * PUT /api/orders/schedules/:name — body { on?: boolean, preferSystemId?: string, by? }.
 * Everything is checked before anything is written. Answers the overview.
 */
export async function changeSchedule(userId: string, name: unknown, body: { on?: unknown; preferSystemId?: unknown; by?: unknown } | null | undefined, deps: ScheduleDeps = scheduleDefaults, probes = scheduleProbes): Promise<ScheduleOverview> {
  const n = scheduleName(name);
  uid(userId);
  const on = body?.on;
  const prefer = body?.preferSystemId;
  if (on === undefined && prefer === undefined) throw new RequestInputError("Nothing to save: send on and/or preferSystemId", 400, "invalid_request");
  if (on !== undefined && typeof on !== "boolean") throw new RequestInputError("on must be true or false", 400, "invalid_request");
  if (prefer !== undefined && (typeof prefer !== "string" || prefer.length > 100 || /\s/.test(prefer.trim()))) throw new RequestInputError("preferSystemId must be a machine's system id (or empty for no preference)", 400, "invalid_request");
  const by = String(body?.by || "").trim();
  // The preference first: it is the one that can still refuse (no clock yet), and then nothing has been written.
  if (prefer !== undefined) await setSchedulePreference(userId, n, prefer, by, deps);
  if (on !== undefined) await setScheduleSwitch(userId, n, on, by, deps);
  return scheduleOverview(userId, deps, probes);
}
