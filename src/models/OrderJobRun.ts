import mongoose, { Schema, type Types } from "mongoose";

/**
 * The job ledger behind "Sync Order Units From Platform": one row per run of
 * any order job, whoever started it, so the panel's rows survive restarts and
 * read the same for every job type. Update and cancel keep their own richer
 * request documents and are mapped into this shape at read time
 * (services/orderJobRuns.ts); fetch, coupons and reschedule write here.
 */
export type JobRunType = "fetch" | "update" | "invoices" | "coupons" | "reschedule" | "cancel";
export type JobRunTrigger = "user" | "system-cron";
export type JobRunStatus = "queued" | "running" | "completed" | "stopped" | "failed";

export interface IOrderJobRun {
  userId: Types.ObjectId;
  type: JobRunType;
  trigger: JobRunTrigger;
  by: string;
  status: JobRunStatus;
  total: number;
  processed: number;
  success: number;
  failed: number;
  cancelled: number;
  startedAt: Date | null;
  finishedAt: Date | null;
  avgMsPerItem: number;
  message: string;
  /** What failed and why — the panel's "View failed items". */
  failedItems: Array<{ key: string; reason: string }>;
  /** The detailed request behind this run (update / cancel request id …). */
  refId: string;
  meta: Record<string, unknown>;
  createdAt: Date;
  updatedAt: Date;
}

const OrderJobRunSchema = new Schema<IOrderJobRun>(
  {
    userId: { type: Schema.Types.ObjectId, required: true, index: true },
    type: { type: String, enum: ["fetch", "update", "invoices", "coupons", "reschedule", "cancel"], required: true },
    trigger: { type: String, enum: ["user", "system-cron"], default: "user" },
    by: { type: String, default: "" },
    status: { type: String, enum: ["queued", "running", "completed", "stopped", "failed"], default: "running" },
    total: { type: Number, default: 0 },
    processed: { type: Number, default: 0 },
    success: { type: Number, default: 0 },
    failed: { type: Number, default: 0 },
    cancelled: { type: Number, default: 0 },
    startedAt: { type: Date, default: null },
    finishedAt: { type: Date, default: null },
    avgMsPerItem: { type: Number, default: 0 },
    message: { type: String, default: "" },
    failedItems: { type: [new Schema({ key: String, reason: String }, { _id: false })], default: [] },
    refId: { type: String, default: "" },
    meta: { type: Schema.Types.Mixed, default: {} },
  },
  { timestamps: true, collection: "order_job_runs" }
);

OrderJobRunSchema.index({ userId: 1, type: 1, createdAt: -1 });

export const OrderJobRun = mongoose.model<IOrderJobRun>("OrderJobRun", OrderJobRunSchema);
