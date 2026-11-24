import mongoose, { Schema, type Types } from "mongoose";
import { REQUEST_STATUSES, REQUEST_TASK_STATUSES, RequestCountsSchema, type RequestCounts, type RequestStatus, type RequestTaskStatus } from "./OrderInvoiceRequest.js";

/**
 * One "Apply Company Coupons" batch (POST /api/orders/company-coupon/trigger):
 * the coupon links and one task per Platform ID. The worker takes one job per
 * (request, account) on the `order-coupon-apply` queue and opens every link
 * with that account's saved session over plain HTTP (services/couponRequests.ts).
 *
 * How Flipkart activates a company coupon has never been observed, so a task
 * records what came back for each link — the hops, the final URL, the HTTP
 * status, whether Flipkart bounced to login, the page title — and claims no
 * more than that.
 *
 * The tasks are their own collection (order_coupon_tasks), not an array on
 * the request: "all logged-in accounts" is over twenty thousand Platform IDs,
 * which with a result per link does not fit one document.
 */
export interface IOrderCouponRequest {
  userId: Types.ObjectId;
  platform: string;
  source: string;
  createdBy: string;
  status: RequestStatus;
  message: string;
  links: string[];
  params: { coupon_links_count: number; email_ids_count: number; selection_method: "email_ids" | "all_accounts"; accounts_count: number };
  /** A snapshot recounted from the tasks while the request runs; exact once it has ended. */
  counts: RequestCounts;
  retryOf: string;
  parentId: string;
  queue: string;
  systemId: string;
  host: string;
  workerHost: string;
  stoppedBy: string;
  createdAt: Date;
  updatedAt: Date;
  startedAt: Date | null;
  finishedAt: Date | null;
}

const OrderCouponRequestSchema = new Schema<IOrderCouponRequest>(
  {
    userId: { type: Schema.Types.ObjectId, required: true },
    platform: { type: String, default: "FLIPKART" },
    source: { type: String, default: "user" },
    createdBy: { type: String, default: "" },
    status: { type: String, enum: REQUEST_STATUSES, default: "queued" },
    message: { type: String, default: "" },
    links: { type: [String], default: [] },
    params: { type: Schema.Types.Mixed, default: () => ({}) },
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
  { timestamps: true, collection: "order_coupon_requests" }
);

OrderCouponRequestSchema.index({ userId: 1, createdAt: -1 });
OrderCouponRequestSchema.index({ status: 1, updatedAt: 1 });

export const OrderCouponRequest = mongoose.model<IOrderCouponRequest>("OrderCouponRequest", OrderCouponRequestSchema);

/** What one coupon link answered for one account. */
export interface ICouponLinkResult {
  link: string;
  /** opened = the link answered a page with the account's session; failed = see errorClass; skipped = not tried (the session had already bounced to login). */
  outcome: "opened" | "failed" | "skipped";
  errorClass: string;
  message: string;
  finalUrl: string;
  httpStatus: number;
  loginBounce: boolean;
  /** Every response on the way: the redirects, then the page. */
  hops: Array<{ status: number; url: string }>;
  title: string;
  /** The first visible words of the page. */
  excerpt: string;
  ms: number;
}

export interface IOrderCouponTask {
  requestId: Types.ObjectId;
  userId: Types.ObjectId;
  email: string;
  accountId: string;
  status: RequestTaskStatus;
  errorClass: string;
  error: string;
  /** On success: what is known — "Link opened with the account's session (HTTP 200)". */
  note: string;
  results: ICouponLinkResult[];
  startedAt: Date | null;
  finishedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

const LinkResultSchema = new Schema<ICouponLinkResult>(
  {
    link: { type: String, default: "" },
    outcome: { type: String, enum: ["opened", "failed", "skipped"], default: "failed" },
    errorClass: { type: String, default: "" },
    message: { type: String, default: "" },
    finalUrl: { type: String, default: "" },
    httpStatus: { type: Number, default: 0 },
    loginBounce: { type: Boolean, default: false },
    hops: { type: [new Schema({ status: Number, url: String }, { _id: false })], default: [] },
    title: { type: String, default: "" },
    excerpt: { type: String, default: "" },
    ms: { type: Number, default: 0 },
  },
  { _id: false }
);

const OrderCouponTaskSchema = new Schema<IOrderCouponTask>(
  {
    requestId: { type: Schema.Types.ObjectId, required: true },
    userId: { type: Schema.Types.ObjectId, required: true },
    email: { type: String, required: true, lowercase: true },
    accountId: { type: String, default: "" },
    status: { type: String, enum: REQUEST_TASK_STATUSES, default: "queued" },
    errorClass: { type: String, default: "" },
    error: { type: String, default: "" },
    note: { type: String, default: "" },
    results: { type: [LinkResultSchema], default: [] },
    startedAt: { type: Date, default: null },
    finishedAt: { type: Date, default: null },
  },
  { timestamps: true, collection: "order_coupon_tasks" }
);

OrderCouponTaskSchema.index({ requestId: 1, email: 1 }, { unique: true });
OrderCouponTaskSchema.index({ requestId: 1, status: 1 });

export const OrderCouponTask = mongoose.model<IOrderCouponTask>("OrderCouponTask", OrderCouponTaskSchema);

export type { RequestCounts, RequestStatus, RequestTaskStatus };
