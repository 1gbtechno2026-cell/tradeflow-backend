import mongoose, { Schema, Types } from "mongoose";

export interface ITrackingProgress {
  date: string | null;
  remark: string;
  location: string | null;
}

export interface ITrackingStep {
  event: string;
  progress: ITrackingProgress[];
  step_text: string;
}

export interface ITrackingStage {
  date: string | null;
  stage: "DONE" | "PENDING";
  status: string;
  detailed_steps: ITrackingStep[];
}

export interface IOrder {
  userId: Types.ObjectId;
  platformAccountId: Types.ObjectId;
  id: number;
  platform_email: string;
  platform: string;
  order_id: string;
  item_id: string;
  order_url: string;
  order_date: Date | null;
  product_name: string;
  seller_name: string;
  unit_id: string;
  quantity: number;
  total_amount: string;
  unit_amount: string;
  delivery_date: Date | null;
  actual_delivery_date: Date | null;
  ewb_number: string;
  delivery_message: string;
  status_key: string;
  status_label: string;
  status_reason: string | null;
  business_name: string;
  business_gst_no: string;
  tracking_id: string;
  delivery_otp: string | null;
  tracking: ITrackingStage[];
  last_tracking_step: string;
  billing_phone_number: string | null;
  billing_address_name: string | null;
  billing_address_pincode: string | null;
  /** Delivery address as Flipkart shows it (services/orderApi.ts ShippingAddress) and its hash. */
  shipping_address?: Record<string, string> | null;
  shipping_address_hash?: string;
  /** Address bookkeeping, written by the update worker (services/orderUnitEvents.ts). */
  address_first_seen_at?: Date | null;
  address_changed_at?: Date | null;
  address_history?: Array<{ hash: string; address: Record<string, string> | null; seenAt: Date; until: Date }>;
  is_invoice_downloaded: boolean;
  invoice_path: string;
  invoice_s3_key: string;
  invoice_downloaded_at: Date | null;
  invoice_size: number;
  invoice_parts: Array<{ type: string; pages: number; invoiceNumber: string; bytes: number }>;
  api_extra?: Record<string, unknown>;
  supercoin_amount_applied: number;
  is_bae_order: boolean;
  cash_on_delivery: boolean;
  received: boolean;
  // Placeholders — nothing in this app currently records which vaulted card
  // (or ref_code) placed a given order; the real checkout flow stops before
  // a card is ever submitted (see checkoutRunner.ts). Populated externally
  // until a real linkage exists.
  card_name: string;
  parent_card_number: string;
  card_number: string;
  ref_code: string;
  /** From the checkout job that placed the order (services/orderLinks.ts):
   *  "cod" / "card", the amount Flipkart charged, and its handling fee —
   *  COD_FEE on cash on delivery, the card-type fee on a card. Blank on
   *  orders no job of ours placed. */
  payment_mode: string;
  transaction_amount: string;
  payment_fee: string;
  payment_fee_details: string;
  /** Cancellation requested from the dashboard (services/orderCancel.ts):
   *  "" | queued | running | cancelled | not_cancellable | failed. */
  cancel_status: string;
  /** Reschedule Order Units (services/orderReschedule.ts): queued | running | not_available | needs_review | rescheduled | failed. */
  reschedule_status: string;
  reschedule_request_id: string;
  reschedule_date: string;
  reschedule_message: string;
  cancel_request_id: string;
  cancel_requested_at: Date | null;
  /** Dashboard user who asked for the cancellation. */
  cancel_requested_by: string;
  cancel_error: string;
  refund_status: string | null;
  refund_amount: string | null;
  refund_msg: string | null;
  cancelled_date: Date | null;
  cancelled_from_bae: boolean;
  cancelled_by_user: boolean;
  /** Flipkart's fkCancelled flag: who cancelled — "flipkart" | "customer" | "". */
  cancelled_by: string;
  cancel_reason_text: string | null;
  order_status_pre_cancellation: string | null;
  imei: string | null;
  is_logged_in: boolean;
  last_fetch: Date | null;
  since_date: Date | null;
  last_error: string;
  /** Which reader wrote this document last: "scrape" (the pages) or "api"
   *  (Flipkart's order APIs). Blank on documents older than the field. */
  fetch_source?: string;
  /** Courier on the shipped step ("Ekart Logistics"). */
  shipment_partner_name?: string;
  createdAt: Date;
  updatedAt: Date;
}

const ProgressSchema = new Schema<ITrackingProgress>(
  {
    date: { type: String, default: null },
    remark: { type: String, default: "" },
    location: { type: String, default: null },
  },
  { _id: false }
);

const StepSchema = new Schema<ITrackingStep>(
  {
    event: { type: String, default: "" },
    progress: { type: [ProgressSchema], default: [] },
    step_text: { type: String, default: "" },
  },
  { _id: false }
);

const StageSchema = new Schema<ITrackingStage>(
  {
    date: { type: String, default: null },
    stage: { type: String, enum: ["DONE", "PENDING"], default: "DONE" },
    status: { type: String, default: "" },
    detailed_steps: { type: [StepSchema], default: [] },
  },
  { _id: false }
);

const OrderSchema = new Schema<IOrder>(
  {
    userId: { type: Schema.Types.ObjectId, ref: "OgUser", required: true, index: true },
    platformAccountId: { type: Schema.Types.ObjectId, ref: "PlatformId", required: true, index: true },
    // Indexed via the explicit unique+sparse OrderSchema.index() below, not
    // here — declaring it in both places is what caused Mongoose's
    // duplicate-index warning, and MongoDB can only keep one index per
    // exact key pattern, so the plain one from this field-level flag was
    // silently winning over the unique constraint the code actually wants.
    id: { type: Number, required: true },
    platform_email: { type: String, required: true, lowercase: true, trim: true },
    platform: { type: String, default: "FLIPKART" },
    order_id: { type: String, required: true, trim: true, uppercase: true },
    item_id: { type: String, default: "", trim: true },
    order_url: { type: String, default: "" },
    order_date: { type: Date, default: null },
    product_name: { type: String, default: "" },
    seller_name: { type: String, default: "" },
    unit_id: { type: String, required: true, trim: true },
    quantity: { type: Number, default: 1 },
    total_amount: { type: String, default: "" },
    unit_amount: { type: String, default: "" },
    delivery_date: { type: Date, default: null },
    actual_delivery_date: { type: Date, default: null },
    ewb_number: { type: String, default: "" },
    delivery_message: { type: String, default: "" },
    status_key: { type: String, default: "" },
    status_label: { type: String, default: "" },
    status_reason: { type: String, default: null },
    business_name: { type: String, default: "" },
    business_gst_no: { type: String, default: "" },
    tracking_id: { type: String, default: "" },
    delivery_otp: { type: String, default: null },
    tracking: { type: [StageSchema], default: [] },
    last_tracking_step: { type: String, default: "" },
    billing_phone_number: { type: String, default: null },
    billing_address_name: { type: String, default: null },
    billing_address_pincode: { type: String, default: null },
    shipping_address: { type: Schema.Types.Mixed, default: null },
    shipping_address_hash: { type: String, default: "" },
    address_first_seen_at: { type: Date, default: null },
    address_changed_at: { type: Date, default: null },
    address_history: { type: [new Schema({ hash: String, address: Schema.Types.Mixed, seenAt: Date, until: Date }, { _id: false })], default: [] },
    is_invoice_downloaded: { type: Boolean, default: false },
    invoice_path: { type: String, default: "" },
    /** S3 key of the stored invoice PDF (services/invoiceCapture.ts). */
    invoice_s3_key: { type: String, default: "" },
    invoice_downloaded_at: { type: Date, default: null },
    invoice_size: { type: Number, default: 0 },
    /** The PDFs inside Flipkart's bundle, in merged order (product first). */
    invoice_parts: { type: [new Schema({ type: String, pages: Number, invoiceNumber: String, bytes: Number }, { _id: false })], default: [] },
    /** API-only extras from the order view (fsn, listing_id, prices…) — kept
     *  so the dashboard's Product ID / Listing ID filters have something to match. */
    api_extra: { type: Schema.Types.Mixed, default: undefined },
    supercoin_amount_applied: { type: Number, default: 0 },
    is_bae_order: { type: Boolean, default: false },
    cash_on_delivery: { type: Boolean, default: false },
    received: { type: Boolean, default: false },
    card_name: { type: String, default: "" },
    parent_card_number: { type: String, default: "" },
    card_number: { type: String, default: "" },
    ref_code: { type: String, default: "" },
    payment_mode: { type: String, default: "" },
    transaction_amount: { type: String, default: "" },
    payment_fee: { type: String, default: "" },
    payment_fee_details: { type: String, default: "" },
    cancel_status: { type: String, default: "" },
    reschedule_status: { type: String, default: "" },
    reschedule_request_id: { type: String, default: "" },
    reschedule_date: { type: String, default: "" },
    reschedule_message: { type: String, default: "" },
    cancel_request_id: { type: String, default: "" },
    cancel_requested_at: { type: Date, default: null },
    cancel_requested_by: { type: String, default: "" },
    cancel_error: { type: String, default: "" },
    refund_status: { type: String, default: null },
    refund_amount: { type: String, default: null },
    refund_msg: { type: String, default: null },
    cancelled_date: { type: Date, default: null },
    cancelled_from_bae: { type: Boolean, default: false },
    cancelled_by_user: { type: Boolean, default: false },
    cancelled_by: { type: String, enum: ["", "flipkart", "customer"], default: "" },
    cancel_reason_text: { type: String, default: null },
    order_status_pre_cancellation: { type: String, default: null },
    imei: { type: String, default: null },
    is_logged_in: { type: Boolean, default: false },
    last_fetch: { type: Date, default: null },
    since_date: { type: Date, default: null },
    last_error: { type: String, default: "" },
    fetch_source: { type: String, default: "" },
    shipment_partner_name: { type: String, default: "" },
  },
  { timestamps: true }
);

OrderSchema.index({ userId: 1, unit_id: 1 }, { unique: true });
OrderSchema.index({ userId: 1, order_id: 1 });
OrderSchema.index({ userId: 1, order_date: -1 });
OrderSchema.index({ userId: 1, last_fetch: -1 });
OrderSchema.index({ id: 1 }, { unique: true, sparse: true });
// Order Units History filter bar — status and email are the two most
// selective single filters users apply alongside the date range, so each
// gets its own compound index rather than relying on the plain
// {userId, order_date} index plus a post-filter scan.
OrderSchema.index({ userId: 1, status_key: 1, order_date: -1 });
OrderSchema.index({ userId: 1, platform_email: 1, order_date: -1 });

// Explicit collection name: data lives in `order_details` after migration
// from `orders`. Same documents, same _ids — model name stays "Order" so
// any future ref: "Order" still resolves correctly.
export const Order = mongoose.model<IOrder>("Order", OrderSchema, "order_details");
