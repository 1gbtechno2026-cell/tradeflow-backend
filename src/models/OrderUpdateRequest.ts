import mongoose, { Schema, type Types } from "mongoose";
import type { ScheduleName } from "./ScheduleLease.js";

/**
 * One "Update Order Units" run: who asked (a person, the 6-hour schedule, the
 * 2-hour Out-for-delivery recheck, or the cancel popup), which units it
 * resolved to, and how each account's refresh went. The worker takes one job
 * per account (queue `order-update`) and keeps this document current, so the
 * dashboard's status panel and the morning email read it rather than a
 * process-local state that dies with the API.
 */
/**
 * Who asked. The first four are this app's own words (the request log reads
 * manual / cancel as "user", schedule / ofd as "system-cron"); the last two are
 * the reference's: "system-order" — queued by itself after our checkout placed
 * an order (services/requestChains.ts) — and "system-retry" — made by Batch
 * requests → Retry (services/syncActions.ts).
 */
export type UpdateSource = "manual" | "schedule" | "ofd" | "cancel" | "system-order" | "system-retry";
export const UPDATE_SOURCES: UpdateSource[] = ["manual", "schedule", "ofd", "cancel", "system-order", "system-retry"];
export type UpdateAccountStatus = "queued" | "running" | "done" | "failed" | "cancelled";

export interface IUpdateUnitCard {
  orderId: string;
  itemId: string;
  unitId: string;
  orderUrl: string;
}

export interface IUpdateAccount {
  email: string;
  platformId: Types.ObjectId | null;
  units: IUpdateUnitCard[];
  /** units.length, kept as a number so list views can leave the cards out. */
  total: number;
  status: UpdateAccountStatus;
  done: number;
  failed: number;
  /** Units whose status_key changed in this refresh. */
  statusChanged: number;
  /** Units that got a delivery OTP they did not have before. */
  otpNew: number;
  /** "api" | "page" — how this account was read in the end. */
  via: string;
  message: string;
  startedAt: Date | null;
  finishedAt: Date | null;
}

export interface IUpdateLog {
  time: Date;
  level: "info" | "warn" | "error";
  message: string;
}

/**
 * The fixed time of a schedule this request is the work of (services/scheduleClock.ts).
 * Only a sweep the shared clock started carries it: the update after a nightly fetch, a
 * retry and everything a person starts do not.
 */
export interface IUpdateSchedule {
  /** update_active (every active unit) or update_ofd (the out-for-delivery recheck). */
  name: ScheduleName;
  /** The fixed time that was taken. */
  slotAt: Date | null;
  /**
   * The lease claim the sweep was started under: what gives the lease back when the request ends, and what an
   * account job compares with the lease before it calls Flipkart — a sweep another machine has taken over since
   * ends there. Never sent to the dashboard.
   */
  token: string;
}

export interface IOrderUpdateRequest {
  userId: Types.ObjectId;
  source: UpdateSource;
  selectionMethod: string;
  /**
   * What was asked, kept so the batch list can show it later: platform,
   * selection_method, statuses_to_update (the labels as sent), status_keys
   * (what they matched here), email_ids_count, order_ids_count,
   * order_email_pairs_count. Empty on requests made before 2026-11-24 and on
   * the two schedules.
   */
  params: Record<string, unknown>;
  /** The request this one follows — the fetch's ledger run id for `after_fetch`. */
  parentId: string;
  /** The deployment whose Redis holds this request's jobs (config.systemId). "" on requests made before 2026-11-24 — the request log closes a request nothing is behind only when it is this deployment's. */
  systemId: string;
  /**
   * Orders this update saw arrive at Delivered (the stage changed to it during
   * this request). When the request finishes, ONE invoice fetch is queued for
   * those of them that have no stored invoice — the reference's "Delivered
   * check" (services/requestChains.ts).
   */
  deliveredOrderIds: string[];
  /** Set on a sweep of the shared clock only (IUpdateSchedule); missing on every other request. */
  schedule?: IUpdateSchedule | null;
  createdBy: string;
  status: "queued" | "running" | "done" | "cancelled";
  mode: "scrape" | "api";
  windows: number;
  accounts: IUpdateAccount[];
  logs: IUpdateLog[];
  createdAt: Date;
  updatedAt: Date;
  startedAt: Date | null;
  finishedAt: Date | null;
}

const UnitCardSchema = new Schema<IUpdateUnitCard>(
  {
    orderId: { type: String, required: true },
    itemId: { type: String, default: "" },
    unitId: { type: String, required: true },
    orderUrl: { type: String, default: "" },
  },
  { _id: false }
);

const AccountSchema = new Schema<IUpdateAccount>(
  {
    email: { type: String, required: true, lowercase: true },
    platformId: { type: Schema.Types.ObjectId, default: null },
    units: { type: [UnitCardSchema], default: [] },
    total: { type: Number, default: 0 },
    status: { type: String, enum: ["queued", "running", "done", "failed", "cancelled"], default: "queued" },
    done: { type: Number, default: 0 },
    failed: { type: Number, default: 0 },
    statusChanged: { type: Number, default: 0 },
    otpNew: { type: Number, default: 0 },
    via: { type: String, default: "" },
    message: { type: String, default: "" },
    startedAt: { type: Date, default: null },
    finishedAt: { type: Date, default: null },
  },
  { _id: false }
);

const LogSchema = new Schema<IUpdateLog>(
  {
    time: { type: Date, required: true },
    level: { type: String, enum: ["info", "warn", "error"], default: "info" },
    message: { type: String, default: "" },
  },
  { _id: false }
);

const UpdateScheduleSchema = new Schema<IUpdateSchedule>(
  {
    name: { type: String, required: true },
    slotAt: { type: Date, default: null },
    token: { type: String, default: "" },
  },
  { _id: false }
);

const OrderUpdateRequestSchema = new Schema<IOrderUpdateRequest>(
  {
    userId: { type: Schema.Types.ObjectId, required: true, index: true },
    source: { type: String, enum: UPDATE_SOURCES, default: "manual" },
    selectionMethod: { type: String, default: "" },
    params: { type: Schema.Types.Mixed, default: () => ({}) },
    parentId: { type: String, default: "" },
    systemId: { type: String, default: "" },
    deliveredOrderIds: { type: [String], default: [] },
    // No default: a request that is not a clock's sweep has no such path at all.
    schedule: { type: UpdateScheduleSchema, default: undefined },
    createdBy: { type: String, default: "" },
    status: { type: String, enum: ["queued", "running", "done", "cancelled"], default: "queued" },
    mode: { type: String, enum: ["scrape", "api"], default: "api" },
    windows: { type: Number, default: 1 },
    accounts: { type: [AccountSchema], default: [] },
    logs: { type: [LogSchema], default: [] },
    startedAt: { type: Date, default: null },
    finishedAt: { type: Date, default: null },
  },
  { timestamps: true, collection: "order_update_requests" }
);

OrderUpdateRequestSchema.index({ userId: 1, createdAt: -1 });
OrderUpdateRequestSchema.index({ userId: 1, status: 1 });

export const OrderUpdateRequest = mongoose.model<IOrderUpdateRequest>("OrderUpdateRequest", OrderUpdateRequestSchema);
