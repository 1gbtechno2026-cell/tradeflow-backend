import mongoose, { Schema, type Types } from "mongoose";
import { REQUEST_TASK_STATUSES, type RequestTaskStatus } from "./OrderInvoiceRequest.js";

/**
 * The task log behind "All tasks" — one row per task of every request kind,
 * in the reference platform's shape (GET /order/sync/tasks/):
 *
 *   fetch                  one account (no order)
 *   update                 one unit of one order
 *   invoice_fetch          one order
 *   company_coupon_apply   one account
 *   cancel                 one unit — or, urgent mode, one order page read in Chrome
 *   reschedule             one unit
 *
 * The request documents stay the source of truth for how a job runs
 * (order_job_runs, order_update_requests, order_invoice_requests,
 * order_coupon_requests / order_coupon_tasks, order_cancel_requests,
 * order_reschedule_requests). This collection is their mirror, written by
 * services/syncTaskLog.ts: rows are created with the request, moved by the
 * code that does the work, and brought back in line from the request document
 * whenever the two disagree — so it can always be rebuilt.
 *
 * `key` names a task inside its request (the account, the order, the unit);
 * (requestId, key) is unique, which makes every write repeatable.
 *
 * Rows are kept 7 days from `createdAt`, as on the reference ("Tasks are kept
 * for 7 days"). The TTL index and every other index here belong to this
 * collection alone.
 */
export type SyncTaskKind = "fetch" | "update" | "invoices" | "coupons" | "cancel" | "reschedule";
export type SyncTaskType = "fetch" | "update" | "invoice_fetch" | "reschedule" | "cancel" | "company_coupon_apply";

export const SYNC_TASK_KINDS: SyncTaskKind[] = ["fetch", "update", "invoices", "coupons", "cancel", "reschedule"];
export const SYNC_TASK_TYPE_OF: Record<SyncTaskKind, SyncTaskType> = {
  fetch: "fetch",
  update: "update",
  invoices: "invoice_fetch",
  coupons: "company_coupon_apply",
  cancel: "cancel",
  reschedule: "reschedule",
};

/** The reference keeps task rows for 7 days. */
export const SYNC_TASK_KEEP_MS = 7 * 24 * 60 * 60 * 1000;

export interface IOrderSyncTask {
  userId: Types.ObjectId;
  /** The request's id in its own collection (a 24-hex ObjectId string). */
  requestId: string;
  /** Which collection the request lives in. */
  kind: SyncTaskKind;
  /** The task inside the request: an e-mail, an order id, "u:<unit id>", "o:<order id>". */
  key: string;
  platform: string;
  taskType: SyncTaskType;
  /** user | system-cron | system-order | system-email | system-update | system-retry | system-repair */
  source: string;
  status: RequestTaskStatus;
  email: string;
  orderId: string;
  unitId: string;
  /** Who asked: the dashboard user's name. "" for the system's own requests. */
  triggeredBy: string;
  /** What the Error column shows: the class's sentence, or the stop / time-out sentence. */
  error: string;
  /** Reference error class (services/requestCommon.ts ERROR_CLASSES), "" unless failed. */
  errorCode: string;
  /** The raw reason behind `error`, for whoever debugs it. */
  detail: string;
  startedAt: Date | null;
  finishedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

const OrderSyncTaskSchema = new Schema<IOrderSyncTask>(
  {
    userId: { type: Schema.Types.ObjectId, required: true },
    requestId: { type: String, default: "" },
    kind: { type: String, enum: SYNC_TASK_KINDS, required: true },
    key: { type: String, required: true },
    platform: { type: String, default: "FLIPKART" },
    taskType: { type: String, required: true },
    source: { type: String, default: "user" },
    status: { type: String, enum: REQUEST_TASK_STATUSES, default: "queued" },
    email: { type: String, default: "" },
    orderId: { type: String, default: "" },
    unitId: { type: String, default: "" },
    triggeredBy: { type: String, default: "" },
    error: { type: String, default: "" },
    errorCode: { type: String, default: "" },
    detail: { type: String, default: "" },
    startedAt: { type: Date, default: null },
    finishedAt: { type: Date, default: null },
    // Written by the log itself: a task's createdAt is its request's, so history keeps its dates and its 7 days.
    createdAt: { type: Date, required: true },
    updatedAt: { type: Date, required: true },
  },
  { collection: "order_sync_tasks" }
);

// One task per (request, key): every write of the log is an upsert or an update by this pair.
OrderSyncTaskSchema.index({ requestId: 1, key: 1 }, { unique: true });
// "Tasks of request …" newest first, and a request's counts / issues by status.
OrderSyncTaskSchema.index({ requestId: 1, createdAt: -1, _id: -1 });
OrderSyncTaskSchema.index({ requestId: 1, status: 1, errorCode: 1 });
// All tasks, newest first — alone and behind each filter of the tab. Every list index ends in
// (createdAt, _id) so a page is read off the index in order, never sorted in memory.
OrderSyncTaskSchema.index({ userId: 1, createdAt: -1, _id: -1 });
OrderSyncTaskSchema.index({ userId: 1, orderId: 1, createdAt: -1, _id: -1 });
OrderSyncTaskSchema.index({ userId: 1, email: 1, createdAt: -1, _id: -1 });
OrderSyncTaskSchema.index({ userId: 1, status: 1, createdAt: -1, _id: -1 });
OrderSyncTaskSchema.index({ userId: 1, taskType: 1, createdAt: -1, _id: -1 });
OrderSyncTaskSchema.index({ userId: 1, source: 1, createdAt: -1, _id: -1 });
OrderSyncTaskSchema.index({ userId: 1, errorCode: 1, createdAt: -1, _id: -1 });
// 7-day retention, as on the reference.
OrderSyncTaskSchema.index({ createdAt: 1 }, { expireAfterSeconds: SYNC_TASK_KEEP_MS / 1000 });

export const OrderSyncTask = mongoose.model<IOrderSyncTask>("OrderSyncTask", OrderSyncTaskSchema);
