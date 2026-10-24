import mongoose, { Schema, type Types } from "mongoose";

/**
 * One "Reschedule Order Units" submission from the dashboard: the typed ids,
 * the date asked for, what they resolved to, and how each unit ended. The
 * worker (order-reschedule queue) drives Flipkart's own "Change Date" per
 * unit in the Platform ID's Chrome (services/orderReschedule.ts).
 *
 *   queued        waiting for the account's job
 *   running       its order page is open
 *   not_available Flipkart's feasibility check refused ("This option is not available on this order")
 *   needs_review  Flipkart offered dates; the dialog was captured but RESCHEDULE_CONFIRM is off
 *   rescheduled   confirmed on Flipkart
 *   failed        session / page / timeout trouble — the message says what
 */
export type RescheduleUnitStatus = "queued" | "running" | "not_available" | "needs_review" | "rescheduled" | "failed";

export interface IRescheduleUnit {
  orderId: string;
  unitId: string;
  itemId: string;
  email: string;
  productName: string;
  status: RescheduleUnitStatus;
  message: string;
  /** Dates / slots Flipkart's dialog offered, as shown (captured when feasible). */
  offeredDates: string[];
  /** The date actually chosen on Flipkart, when confirmed. */
  chosen: string;
  /** Flipkart's workflow token from the feasibility answer (WF-…). */
  token: string;
  startedAt: Date | null;
  finishedAt: Date | null;
}

export interface IRescheduleRequest {
  userId: Types.ObjectId;
  type: "unit" | "order";
  input: string[];
  /** Requested delivery date, YYYY-MM-DD. */
  date: string;
  createdBy: string;
  retryOf: string;
  /** Whether the worker may press Flipkart's confirm (RESCHEDULE_CONFIRM at submit time). */
  confirm: boolean;
  status: "queued" | "running" | "done";
  units: IRescheduleUnit[];
  rejected: Array<{ id: string; reason: string }>;
  createdAt: Date;
  updatedAt: Date;
  finishedAt: Date | null;
}

const UnitSchema = new Schema<IRescheduleUnit>(
  {
    orderId: { type: String, required: true },
    unitId: { type: String, required: true },
    itemId: { type: String, default: "" },
    email: { type: String, required: true, lowercase: true },
    productName: { type: String, default: "" },
    status: { type: String, enum: ["queued", "running", "not_available", "needs_review", "rescheduled", "failed"], default: "queued" },
    message: { type: String, default: "" },
    offeredDates: { type: [String], default: [] },
    chosen: { type: String, default: "" },
    token: { type: String, default: "" },
    startedAt: { type: Date, default: null },
    finishedAt: { type: Date, default: null },
  },
  { _id: false }
);

const RescheduleRequestSchema = new Schema<IRescheduleRequest>(
  {
    userId: { type: Schema.Types.ObjectId, required: true, index: true },
    type: { type: String, enum: ["unit", "order"], required: true },
    input: { type: [String], default: [] },
    date: { type: String, required: true },
    createdBy: { type: String, default: "" },
    retryOf: { type: String, default: "" },
    confirm: { type: Boolean, default: false },
    status: { type: String, enum: ["queued", "running", "done"], default: "queued" },
    units: { type: [UnitSchema], default: [] },
    rejected: { type: [new Schema({ id: String, reason: String }, { _id: false })], default: [] },
    finishedAt: { type: Date, default: null },
  },
  { timestamps: true, collection: "order_reschedule_requests" }
);

RescheduleRequestSchema.index({ userId: 1, createdAt: -1 });

export const RescheduleRequest = mongoose.model<IRescheduleRequest>("RescheduleRequest", RescheduleRequestSchema);
