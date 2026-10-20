import mongoose, { Schema, type Types } from "mongoose";

/**
 * One "Update Order Units" run: who asked (a person, the 6-hour schedule, the
 * 2-hour Out-for-delivery recheck, or the cancel popup), which units it
 * resolved to, and how each account's refresh went. The worker takes one job
 * per account (queue `order-update`) and keeps this document current, so the
 * dashboard's status panel and the morning email read it rather than a
 * process-local state that dies with the API.
 */
export type UpdateSource = "manual" | "schedule" | "ofd" | "cancel";
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

export interface IOrderUpdateRequest {
  userId: Types.ObjectId;
  source: UpdateSource;
  selectionMethod: string;
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

const OrderUpdateRequestSchema = new Schema<IOrderUpdateRequest>(
  {
    userId: { type: Schema.Types.ObjectId, required: true, index: true },
    source: { type: String, enum: ["manual", "schedule", "ofd", "cancel"], default: "manual" },
    selectionMethod: { type: String, default: "" },
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
