import mongoose, { Schema, type Types } from "mongoose";

/**
 * One "Download Order Invoices" batch (POST /api/orders/invoice/trigger): the
 * order ids that were asked for and how each one ended. One task per order;
 * the worker takes one job per (request, account) on the `order-invoice-batch`
 * queue (services/invoiceRequests.ts) and writes each order's result here as
 * it finishes, with the request-level counts kept in step by the same write.
 *
 * Statuses are the reference platform's: a request is queued → in_progress →
 * completed | cancelled; a task is queued → in_progress → success | failed,
 * or cancelled ("Cancelled before it started") when the request is stopped,
 * or aborted when it is stopped with no worker alive to finish it.
 *
 * "Timed out" is spelled two ways on the reference and both are kept: a
 * task's status VALUE is `timedout` (its status filter and `retry_statuses`
 * carry that), the KEY in a request's counts is `timed_out`. countKeyOf maps
 * one to the other; nothing else may assume they are the same word.
 */
export type RequestStatus = "queued" | "in_progress" | "completed" | "cancelled";
export type RequestTaskStatus = "queued" | "in_progress" | "success" | "failed" | "cancelled" | "aborted" | "timedout";

export const REQUEST_STATUSES: RequestStatus[] = ["queued", "in_progress", "completed", "cancelled"];
export const REQUEST_TASK_STATUSES: RequestTaskStatus[] = ["queued", "in_progress", "success", "failed", "cancelled", "aborted", "timedout"];

/** The reference's counts object, one number per task status (`timed_out` counts the `timedout` tasks). */
export interface RequestCounts {
  total: number;
  queued: number;
  in_progress: number;
  success: number;
  failed: number;
  cancelled: number;
  aborted: number;
  timed_out: number;
}

/** A task status → its key in the counts object. */
export function countKeyOf(status: RequestTaskStatus): Exclude<keyof RequestCounts, "total"> {
  return status === "timedout" ? "timed_out" : status;
}

export const RequestCountsSchema = new Schema<RequestCounts>(
  {
    total: { type: Number, default: 0 },
    queued: { type: Number, default: 0 },
    in_progress: { type: Number, default: 0 },
    success: { type: Number, default: 0 },
    failed: { type: Number, default: 0 },
    cancelled: { type: Number, default: 0 },
    aborted: { type: Number, default: 0 },
    timed_out: { type: Number, default: 0 },
  },
  { _id: false }
);

export interface IInvoiceTask {
  orderId: string;
  /** The Platform ID that owns the order ("" when the order is not in Order Units). */
  email: string;
  accountId: string;
  status: RequestTaskStatus;
  /** Reference error class (services/requestCommon.ts ERROR_CLASSES), "" unless failed. */
  errorClass: string;
  /** What the Error column shows: the class's sentence, or the stop / reaper sentence. */
  error: string;
  /** The raw reason behind `error` (HTTP status, exception text), for whoever debugs it. */
  detail: string;
  /** Invoice parts stored for the order ("product", "transport"…), filled on success. */
  parts: string[];
  sizeKb: number;
  startedAt: Date | null;
  finishedAt: Date | null;
}

export interface IOrderInvoiceRequest {
  userId: Types.ObjectId;
  platform: string;
  /**
   * "user" for the button. The reference's others: "system-update" (queued when
   * an update saw units arrive at Delivered), "system-cron" (one run of the
   * 3-hourly sweep, recorded as a request by services/invoiceSweep.ts) and
   * "system-retry" (Batch requests → Retry).
   */
  source: string;
  createdBy: string;
  status: RequestStatus;
  /** A request-level sentence: who stopped it, or why nobody ran it. */
  message: string;
  /**
   * What was asked. Beside the two the button sends: `follows` ("update" |
   * "retry"), `retry_of` + `retry_statuses` (+ `error_code`) on a retry, and on
   * a sweep run `sweep: true`, `capture_count`, `recapture_count`, `cap_per_phase`.
   */
  params: { order_ids_count: number; force: boolean } & Record<string, unknown>;
  tasks: IInvoiceTask[];
  counts: RequestCounts;
  /** Set when this request retries another one / follows another one (step 5). */
  retryOf: string;
  parentId: string;
  /** The BullMQ queue its jobs were put on and the deployment whose Redis holds them. */
  queue: string;
  systemId: string;
  host: string;
  /** The worker that last took one of its jobs. */
  workerHost: string;
  stoppedBy: string;
  createdAt: Date;
  updatedAt: Date;
  startedAt: Date | null;
  finishedAt: Date | null;
}

const TaskSchema = new Schema<IInvoiceTask>(
  {
    orderId: { type: String, required: true },
    email: { type: String, default: "", lowercase: true },
    accountId: { type: String, default: "" },
    status: { type: String, enum: REQUEST_TASK_STATUSES, default: "queued" },
    errorClass: { type: String, default: "" },
    error: { type: String, default: "" },
    detail: { type: String, default: "" },
    parts: { type: [String], default: [] },
    sizeKb: { type: Number, default: 0 },
    startedAt: { type: Date, default: null },
    finishedAt: { type: Date, default: null },
  },
  { _id: false }
);

const OrderInvoiceRequestSchema = new Schema<IOrderInvoiceRequest>(
  {
    userId: { type: Schema.Types.ObjectId, required: true },
    platform: { type: String, default: "FLIPKART" },
    source: { type: String, default: "user" },
    createdBy: { type: String, default: "" },
    status: { type: String, enum: REQUEST_STATUSES, default: "queued" },
    message: { type: String, default: "" },
    // Mixed since step 5: a retry and a sweep run record more than the button's two keys.
    params: { type: Schema.Types.Mixed, default: () => ({ order_ids_count: 0, force: false }) },
    tasks: { type: [TaskSchema], default: [] },
    counts: { type: RequestCountsSchema, default: () => ({}) },
    retryOf: { type: String, default: "" },
    parentId: { type: String, default: "" },
    queue: { type: String, default: "" },
    systemId: { type: String, default: "" },
    host: { type: String, default: "" },
    workerHost: { type: String, default: "" },
    stoppedBy: { type: String, default: "" },
    startedAt: { type: Date, default: null },
    finishedAt: { type: Date, default: null },
  },
  { timestamps: true, collection: "order_invoice_requests" }
);

OrderInvoiceRequestSchema.index({ userId: 1, createdAt: -1 });
OrderInvoiceRequestSchema.index({ status: 1, updatedAt: 1 });

export const OrderInvoiceRequest = mongoose.model<IOrderInvoiceRequest>("OrderInvoiceRequest", OrderInvoiceRequestSchema);
