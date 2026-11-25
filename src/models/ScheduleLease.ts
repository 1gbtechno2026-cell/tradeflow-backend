import mongoose, { Schema, type Types } from "mongoose";

/**
 * One clock and one lease per schedule of a workspace (services/scheduleClock.ts).
 *
 * The clock is the document, not a machine: `nextAt` is the next fixed time,
 * and whichever machine that may run the schedule is awake moves it on with
 * one compare-and-set — it takes the time (`holder`, a 5-minute lease it
 * renews while its run is open), or records why nothing ran. Every machine on
 * this database reads the same row, so two Macs never both run a time, a
 * sleeping Mac's time is taken by the other, and Settings shows a real next
 * run and a real last outcome.
 *
 *   fetch          the nightly fetch                    (served by the API)
 *   update_active  every active unit, every 6 h         (served by the worker)
 *   update_ofd     the out-for-delivery recheck, 2 h    (served by the worker)
 *   invoice_sweep  the invoice sweep, every 3 h         (served by the worker)
 */
export const SCHEDULE_NAMES = ["fetch", "update_active", "update_ofd", "invoice_sweep"] as const;
export type ScheduleName = (typeof SCHEDULE_NAMES)[number];

/** The kind of process that runs a schedule's work: the fetch lives in the API, the sweeps on the worker's queues. */
export type ScheduleRole = "api" | "worker";
export const SCHEDULE_ROLE: Record<ScheduleName, ScheduleRole> = { fetch: "api", update_active: "worker", update_ofd: "worker", invoice_sweep: "worker" };

/** The four schedules as Settings → Scheduled fetch names them. */
export const SCHEDULE_LABELS: Record<ScheduleName, string> = {
  fetch: "Nightly fetch",
  update_active: "Update all active units",
  update_ofd: "Out-for-delivery recheck",
  invoice_sweep: "Invoice sweep",
};

export function isScheduleName(value: unknown): value is ScheduleName {
  return typeof value === "string" && (SCHEDULE_NAMES as readonly string[]).includes(value);
}

/**
 * What happened at a time, or to a run:
 *   ran         a machine took the time and started its work (or found nothing to do — the reason says so)
 *   took_over   the same, over a lease another holder let expire, or a second start of a run that did not finish
 *   skipped     nothing ran, on purpose: switched off, the earlier run still going, the clock just started
 *   missed      nothing ran: no machine that may run it was awake within the late limit
 *   finished    the run ended
 *   failed      the run could not start, or ended without finishing
 *   superseded  a machine found that another one had taken over what it was running
 */
export const TICK_OUTCOMES = ["ran", "skipped", "missed", "took_over", "superseded", "finished", "failed"] as const;
export type TickOutcome = (typeof TICK_OUTCOMES)[number];

/** The document a run lives in: an update request, or a ledger run (order_job_runs) for the fetch and the invoice sweep. */
export interface IScheduleRunRef {
  kind: "fetch" | "update" | "invoices";
  id: string;
}

export interface IScheduleTick {
  /** The fixed time this is about; null for a run somebody started by hand. */
  slotAt: Date | null;
  at: Date;
  outcome: TickOutcome;
  reason: string;
  systemId: string;
  host: string;
  runRef: IScheduleRunRef | null;
}

export interface IScheduleClock {
  /** daily = once a day at `at`; every = every `everyMin` minutes, `phaseMin` past the period (06:15 = every 360, phase 15). */
  kind: "daily" | "every";
  at: string;
  everyMin: number;
  phaseMin: number;
  /** Asia/Kolkata (fixed +05:30) or UTC — the slot maths knows no other. */
  tz: string;
  /** A time may still be taken this many minutes late; later than that it is recorded as missed. */
  lateMin: number;
  /** The machine that runs it when it is awake; another one waits `standbyGraceSec` before taking a time. "" = whoever polls first. */
  preferSystemId: string;
  standbyGraceSec: number;
  preferChangedBy: string;
  preferChangedAt: Date | null;
}

export interface IScheduleHolder {
  systemId: string;
  host: string;
  role: ScheduleRole;
  pid: number;
  /** The claim: what renews and releases the lease, and what a run started under it carries. Never sent to the dashboard. */
  token: string;
}

export interface IScheduleLease {
  userId: Types.ObjectId;
  name: ScheduleName;
  clock: IScheduleClock;
  /** The next fixed time. Authoritative: a machine's own clock only decides when it looks. */
  nextAt: Date;
  /** The last fixed time that was dealt with (taken, skipped or missed). */
  lastSlotAt: Date | null;
  /**
   * When a machine that may run this schedule first served this clock. Until
   * then a time that is already due is never run late: the first such machine
   * starts with the next fixed time (a worker restarted onto this clock must
   * not sweep at the restart).
   */
  armedAt: Date | null;
  /** Who holds the lease; null once released. Held and `expiresAt` past = its machine went quiet. */
  holder: IScheduleHolder | null;
  /** The run the lease is, or was last, held for. */
  slotAt: Date | null;
  /** 1 for the first start of a time, 2 for the nightly fetch's one second start. */
  attempt: number;
  /** Started by a person (Run now), not by a fixed time. */
  manual: boolean;
  takenAt: Date | null;
  renewedAt: Date | null;
  expiresAt: Date | null;
  runRef: IScheduleRunRef | null;
  finishedAt: Date | null;
  /** How that run ended, as its last tick's outcome; null while it is held. */
  endedAs: TickOutcome | null;
  /** Who took that run — kept after the release, so work left over from an older claim can tell that another machine has run since. */
  ranBy: { systemId: string; host: string; token: string } | null;
  /** The last decision or result (never a `superseded` note). */
  lastTick: IScheduleTick | null;
  /** The newest 50, oldest first. */
  ticks: IScheduleTick[];
  createdAt: Date;
  updatedAt: Date;
}

export const TICKS_KEPT = 50;

const RunRefSchema = new Schema<IScheduleRunRef>({ kind: { type: String, enum: ["fetch", "update", "invoices"], required: true }, id: { type: String, required: true } }, { _id: false });

const TickSchema = new Schema<IScheduleTick>(
  {
    slotAt: { type: Date, default: null },
    at: { type: Date, required: true },
    outcome: { type: String, enum: TICK_OUTCOMES, required: true },
    reason: { type: String, default: "" },
    systemId: { type: String, default: "" },
    host: { type: String, default: "" },
    runRef: { type: RunRefSchema, default: null },
  },
  { _id: false }
);

const ClockSchema = new Schema<IScheduleClock>(
  {
    kind: { type: String, enum: ["daily", "every"], required: true },
    at: { type: String, default: "" },
    everyMin: { type: Number, default: 0 },
    phaseMin: { type: Number, default: 0 },
    tz: { type: String, default: "Asia/Kolkata" },
    lateMin: { type: Number, default: 0 },
    preferSystemId: { type: String, default: "" },
    standbyGraceSec: { type: Number, default: 120 },
    preferChangedBy: { type: String, default: "" },
    preferChangedAt: { type: Date, default: null },
  },
  { _id: false }
);

const HolderSchema = new Schema<IScheduleHolder>(
  {
    systemId: { type: String, required: true },
    host: { type: String, default: "" },
    role: { type: String, enum: ["api", "worker"], required: true },
    pid: { type: Number, default: 0 },
    token: { type: String, required: true },
  },
  { _id: false }
);

const RanBySchema = new Schema<NonNullable<IScheduleLease["ranBy"]>>({ systemId: { type: String, default: "" }, host: { type: String, default: "" }, token: { type: String, default: "" } }, { _id: false });

const ScheduleLeaseSchema = new Schema<IScheduleLease>(
  {
    userId: { type: Schema.Types.ObjectId, required: true },
    name: { type: String, enum: SCHEDULE_NAMES, required: true },
    clock: { type: ClockSchema, required: true },
    nextAt: { type: Date, required: true },
    lastSlotAt: { type: Date, default: null },
    armedAt: { type: Date, default: null },
    holder: { type: HolderSchema, default: null },
    slotAt: { type: Date, default: null },
    attempt: { type: Number, default: 0 },
    manual: { type: Boolean, default: false },
    takenAt: { type: Date, default: null },
    renewedAt: { type: Date, default: null },
    expiresAt: { type: Date, default: null },
    runRef: { type: RunRefSchema, default: null },
    finishedAt: { type: Date, default: null },
    endedAs: { type: String, default: null },
    ranBy: { type: RanBySchema, default: null },
    lastTick: { type: TickSchema, default: null },
    ticks: { type: [TickSchema], default: [] },
  },
  // Never created or indexed just because a process imported this file and connected: several check scripts connect
  // with mongoose's defaults, and one of them must not be what first creates this collection in the live database.
  // The seed builds the index itself, before its first insert (services/scheduleSettings.ts ensureScheduleIndexes).
  { timestamps: true, collection: "order_schedule_leases", autoCreate: false, autoIndex: false }
);

// One clock per schedule of a workspace: the seed's upsert and every compare-and-set rely on it.
ScheduleLeaseSchema.index({ userId: 1, name: 1 }, { unique: true });

export const ScheduleLease = mongoose.model<IScheduleLease>("ScheduleLease", ScheduleLeaseSchema);
