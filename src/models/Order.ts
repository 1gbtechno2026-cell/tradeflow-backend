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
  is_invoice_downloaded: boolean;
  invoice_path: string;
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
  refund_status: string | null;
  refund_amount: string | null;
  refund_msg: string | null;
  cancelled_date: Date | null;
  cancelled_from_bae: boolean;
  cancelled_by_user: boolean;
  order_status_pre_cancellation: string | null;
  imei: string | null;
  is_logged_in: boolean;
  last_fetch: Date | null;
  since_date: Date | null;
  last_error: string;
  /** Which reader wrote this document last: "scrape" (the pages) or "api"
   *  (Flipkart's order APIs). Blank on documents older than the field. */
  fetch_source?: string;
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
    is_invoice_downloaded: { type: Boolean, default: false },
    invoice_path: { type: String, default: "" },
    supercoin_amount_applied: { type: Number, default: 0 },
    is_bae_order: { type: Boolean, default: false },
    cash_on_delivery: { type: Boolean, default: false },
    received: { type: Boolean, default: false },
    card_name: { type: String, default: "" },
    parent_card_number: { type: String, default: "" },
    card_number: { type: String, default: "" },
    ref_code: { type: String, default: "" },
    refund_status: { type: String, default: null },
    refund_amount: { type: String, default: null },
    refund_msg: { type: String, default: null },
    cancelled_date: { type: Date, default: null },
    cancelled_from_bae: { type: Boolean, default: false },
    cancelled_by_user: { type: Boolean, default: false },
    order_status_pre_cancellation: { type: String, default: null },
    imei: { type: String, default: null },
    is_logged_in: { type: Boolean, default: false },
    last_fetch: { type: Date, default: null },
    since_date: { type: Date, default: null },
    last_error: { type: String, default: "" },
    fetch_source: { type: String, default: "" },
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
