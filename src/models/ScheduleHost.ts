import mongoose, { Schema } from "mongoose";
import type { ScheduleRole } from "./ScheduleLease.js";

/**
 * Presence: one row per process that runs the schedule clock
 * (services/scheduleClock.ts), written once a minute. It answers two
 * questions — "was the machine that should run this schedule seen lately?"
 * (a standby waits for it only then) and, for Settings, "which machines are
 * there, and which schedules may each run?".
 *
 * Not per workspace: a process serves the one workspace of its
 * TRADE_FLOW_USER_ID. A row nobody refreshes is dropped after 7 days.
 */
export interface IScheduleHost {
  /** "<systemId>:<role>" — a restart of the same process writes the same row. */
  _id: string;
  systemId: string;
  role: ScheduleRole;
  /** The machine as its rows name it (TRADEFLOW_HOST_ID). */
  host: string;
  /** os.hostname() — what the network calls it today. */
  machine: string;
  pid: number;
  /** The schedules this process serves (its role's, by WORKER_ROLES). */
  serves: string[];
  /** Of those, the ones this machine's .env lets it run. */
  eligible: string[];
  startedAt: Date;
  seenAt: Date;
}

export const SCHEDULE_HOST_KEEP_SEC = 7 * 24 * 60 * 60;

const ScheduleHostSchema = new Schema<IScheduleHost>(
  {
    _id: { type: String, required: true },
    systemId: { type: String, required: true },
    role: { type: String, enum: ["api", "worker"], required: true },
    host: { type: String, default: "" },
    machine: { type: String, default: "" },
    pid: { type: Number, default: 0 },
    serves: { type: [String], default: [] },
    eligible: { type: [String], default: [] },
    startedAt: { type: Date, required: true },
    seenAt: { type: Date, required: true },
  },
  // Like the lease collection, created and indexed by the seed only (services/scheduleSettings.ts ensureScheduleIndexes).
  { collection: "order_schedule_hosts", versionKey: false, autoCreate: false, autoIndex: false }
);

ScheduleHostSchema.index({ seenAt: 1 }, { expireAfterSeconds: SCHEDULE_HOST_KEEP_SEC });

export const ScheduleHost = mongoose.model<IScheduleHost>("ScheduleHost", ScheduleHostSchema);
