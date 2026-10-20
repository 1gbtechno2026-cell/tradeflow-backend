import mongoose, { Schema, type Types } from "mongoose";

/**
 * One "Cancel Order Units" submission from the dashboard: what was typed, what
 * it resolved to, and how each unit ended. The Order rows carry the same
 * per-unit status (cancel_status); this is the request-level view the popup
 * polls, including the ids that were rejected before anything was queued.
 */
/** dry_run: walked to REQUEST CANCELLATION and stopped (test requests only). */
export type CancelUnitStatus = "queued" | "running" | "cancelled" | "not_cancellable" | "failed" | "dry_run";

export interface ICancelUnit {
  orderId: string;
  unitId: string;
  itemId: string;
  email: string;
  productName: string;
  status: CancelUnitStatus;
  message: string;
  startedAt: Date | null;
  finishedAt: Date | null;
}

/**
 * Urgent cancel: an Order ID never fetched, to be read in the Platform ID's
 * own Chrome by the worker (order_details page) and then cancelled unit by
 * unit — no API read at request time. `read` = units found and queued (they
 * appear in `units`); `failed` = the page gave no order (wrong account, dead
 * session, Flipkart error) with the reason in `message`.
 */
export type CancelOrderStatus = "queued" | "running" | "read" | "failed";

export interface ICancelOrder {
  orderId: string;
  email: string;
  status: CancelOrderStatus;
  message: string;
  unitsFound: number;
  startedAt: Date | null;
  finishedAt: Date | null;
}

export interface ICancelRequest {
  userId: Types.ObjectId;
  type: "unit" | "order";
  input: string[];
  dryRun: boolean;
  /** Urgent: unknown orders are read in Chrome by the worker, not through the API first. */
  urgent: boolean;
  /** Dashboard user who submitted it (shown in the Cancellations history). */
  createdBy: string;
  /** Set when this request was made by "Retry failed" on another one. */
  retryOf: string;
  status: "queued" | "running" | "done";
  units: ICancelUnit[];
  /** Urgent-mode orders still to be read (or that could not be read). */
  orders: ICancelOrder[];
  rejected: Array<{ id: string; reason: string }>;
  createdAt: Date;
  updatedAt: Date;
  finishedAt: Date | null;
}

const OrderSchema = new Schema<ICancelOrder>(
  {
    orderId: { type: String, required: true },
    email: { type: String, required: true, lowercase: true },
    status: { type: String, enum: ["queued", "running", "read", "failed"], default: "queued" },
    message: { type: String, default: "" },
    unitsFound: { type: Number, default: 0 },
    startedAt: { type: Date, default: null },
    finishedAt: { type: Date, default: null },
  },
  { _id: false }
);

const UnitSchema = new Schema<ICancelUnit>(
  {
    orderId: { type: String, required: true },
    unitId: { type: String, required: true },
    itemId: { type: String, default: "" },
    email: { type: String, required: true, lowercase: true },
    productName: { type: String, default: "" },
    status: { type: String, enum: ["queued", "running", "cancelled", "not_cancellable", "failed", "dry_run"], default: "queued" },
    message: { type: String, default: "" },
    startedAt: { type: Date, default: null },
    finishedAt: { type: Date, default: null },
  },
  { _id: false }
);

const CancelRequestSchema = new Schema<ICancelRequest>(
  {
    userId: { type: Schema.Types.ObjectId, required: true, index: true },
    type: { type: String, enum: ["unit", "order"], required: true },
    input: { type: [String], default: [] },
    dryRun: { type: Boolean, default: false },
    urgent: { type: Boolean, default: false },
    createdBy: { type: String, default: "" },
    retryOf: { type: String, default: "" },
    status: { type: String, enum: ["queued", "running", "done"], default: "queued" },
    units: { type: [UnitSchema], default: [] },
    orders: { type: [OrderSchema], default: [] },
    rejected: { type: [new Schema({ id: String, reason: String }, { _id: false })], default: [] },
    finishedAt: { type: Date, default: null },
  },
  { timestamps: true }
);

CancelRequestSchema.index({ userId: 1, createdAt: -1 });

export const CancelRequest = mongoose.model<ICancelRequest>("CancelRequest", CancelRequestSchema, "order_cancel_requests");
