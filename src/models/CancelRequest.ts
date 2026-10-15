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

export interface ICancelRequest {
  userId: Types.ObjectId;
  type: "unit" | "order";
  input: string[];
  dryRun: boolean;
  status: "queued" | "running" | "done";
  units: ICancelUnit[];
  rejected: Array<{ id: string; reason: string }>;
  createdAt: Date;
  updatedAt: Date;
  finishedAt: Date | null;
}

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
    status: { type: String, enum: ["queued", "running", "done"], default: "queued" },
    units: { type: [UnitSchema], default: [] },
    rejected: { type: [new Schema({ id: String, reason: String }, { _id: false })], default: [] },
    finishedAt: { type: Date, default: null },
  },
  { timestamps: true }
);

CancelRequestSchema.index({ userId: 1, createdAt: -1 });

export const CancelRequest = mongoose.model<ICancelRequest>("CancelRequest", CancelRequestSchema, "order_cancel_requests");
