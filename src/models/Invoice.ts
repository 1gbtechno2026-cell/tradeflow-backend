import mongoose, { Schema, Types } from "mongoose";

/**
 * Mirror of the dashboard's backend/src/models/Invoice.ts — same `invoices`
 * collection, same fields. Trade Flow only ever creates rows as `pending`
 * (services/invoiceCapture.ts → registerInvoice); the extract Lambda drives
 * every other status. Keep the two files in step.
 *
 * One stored invoice bundle = one S3 object (raw/<userId>/<orderId>.pdf) =
 * one document here; every unit of the order shares it.
 *
 *   pending ──claim──► processing ──► processed            (tag status=processed)
 *                          ├─ transient error ─► failed    (SQS retries)
 *                          └─ permanent / out of retries ─► needs_review   (tag status=failed)
 */

export const INVOICE_STATUSES = ["pending", "processing", "processed", "failed", "needs_review"] as const;
export type InvoiceStatus = (typeof INVOICE_STATUSES)[number];

export interface IInvoicePart {
  type: string;
  pages: number;
  invoiceNumber: string;
  bytes: number;
}

export interface IInvoice {
  _id: Types.ObjectId;
  userId: Types.ObjectId;
  platformAccountId: Types.ObjectId | null;
  order_id: string;
  s3_key: string;
  source_key: string;
  etag: string;
  size: number;
  parts: IInvoicePart[];
  processing_status: InvoiceStatus;
  retry_count: number;
  last_error: string;
  last_attempt_at: Date | null;
  processing_started_at: Date | null;
  processed_at: Date | null;
  retried_by: string;
  retried_at: Date | null;
  unit_ids: string[];
  summary: Record<string, unknown> | null;
  createdAt: Date;
  updatedAt: Date;
}

const InvoiceSchema = new Schema<IInvoice>(
  {
    userId: { type: Schema.Types.ObjectId, ref: "OgUser", required: true },
    platformAccountId: { type: Schema.Types.ObjectId, ref: "PlatformId", default: null },
    order_id: { type: String, required: true, trim: true, uppercase: true },
    s3_key: { type: String, required: true },
    source_key: { type: String, default: "" },
    etag: { type: String, default: "" },
    size: { type: Number, default: 0 },
    parts: { type: [new Schema<IInvoicePart>({ type: String, pages: Number, invoiceNumber: String, bytes: Number }, { _id: false })], default: [] },
    processing_status: { type: String, enum: INVOICE_STATUSES, default: "pending" },
    retry_count: { type: Number, default: 0 },
    last_error: { type: String, default: "" },
    last_attempt_at: { type: Date, default: null },
    processing_started_at: { type: Date, default: null },
    processed_at: { type: Date, default: null },
    retried_by: { type: String, default: "" },
    retried_at: { type: Date, default: null },
    unit_ids: { type: [String], default: [] },
    summary: { type: Schema.Types.Mixed, default: null },
  },
  { timestamps: true }
);

InvoiceSchema.index({ s3_key: 1 }, { unique: true });
InvoiceSchema.index({ userId: 1, order_id: 1 });
InvoiceSchema.index({ userId: 1, processing_status: 1, updatedAt: -1 });
InvoiceSchema.index({ processing_status: 1, processing_started_at: 1, updatedAt: 1 });

export const Invoice = mongoose.model<IInvoice>("Invoice", InvoiceSchema, "invoices");
