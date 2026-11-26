import mongoose, { Schema, Types } from "mongoose";

/**
 * Mirror of the dashboard's backend/src/models/Invoice.ts — same `invoices`
 * collection, same fields (the archive's zip_key / files are the dashboard's only). Trade Flow only ever creates rows as `pending`
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
  /** Time-settled completeness: settled after the +48 h re-capture (or at capture when the order was delivered > 48 h ago, parts already final). */
  settled: boolean;
  captured_at: Date | null;
  unit_ids: string[];
  /** Every tax invoice in the bundle (written by the Lambda; the dashboard's invoiceParse.ts InvoiceLine). */
  lines: Record<string, unknown>[];
  summary: Record<string, unknown> | null;
  /**
   * Written by registerInvoice (data-quality sub-step 12): the invoice_downloads id whose merged PDF is stored at
   * s3_key now ("" for a bundle stored before it), when the order was last downloaded, and how many downloads the
   * row has counted since. Flipkart's per-download file list lives in invoice_downloads, never in `parts`.
   */
  stored_download_id: string;
  last_download_at: Date | null;
  download_count: number;
  /** Written by the extract Lambda (dashboard sub-step 5): Flipkart's files the archive was built from, and that download's id. */
  originals: IInvoiceOriginal[];
  originals_download_id: string;
  /** Written by the extract Lambda (dashboard sub-step 5): "original" = the archive holds Flipkart's own files; "split" = cut from the merged PDF. */
  zip_source: "" | "original" | "split";
  /**
   * Late re-downloads (data-quality sub-step 12, SO-5 — services/invoiceRefetch.ts): one entry per (unit, reason),
   * noted when an invoiced unit of this order turns Cancelled / Returned or its refund completes; the invoice
   * sweep's third phase ("refetch") downloads the order again once the entry is due. Written by Trade Flow only.
   */
  refetch: IInvoiceRefetch[];
  /** The refetch phase's claim on this order (one sweep, one machine at a time): until when, and by which host. */
  refetch_claim_until: Date | null;
  refetch_claim_by: string;
  createdAt: Date;
  updatedAt: Date;
}

export const REFETCH_REASONS = ["cancelled", "returned", "refund"] as const;
export type RefetchReason = (typeof REFETCH_REASONS)[number];
/** How an entry was closed: downloaded again (new bytes / the same bytes), a later download already covered it, or 3 failed attempts. */
export const REFETCH_RESULTS = ["", "downloaded", "unchanged", "already_newer", "gave_up"] as const;
export type RefetchResult = (typeof REFETCH_RESULTS)[number];

/** One late re-download the order is due (SO-5). Open while `done_at` is null. */
export interface IInvoiceRefetch {
  unit_id: string;
  reason: RefetchReason;
  /** The moment on Flipkart's side: the cancellation, the return, or the refund. */
  event_at: Date;
  /** event_at + the reason's delay, as it applied when the entry was noted: the workspace's invoice setting (Settings → Invoice settings, `invoice_settings`), else INVOICE_REFETCH_*_DELAY_H, else the default. */
  due_at: Date;
  noted_at: Date;
  /** Who noted it: "update <request id>", "store <path>", … */
  by: string;
  /** Cash on delivery: no refund follows, so the cancellation look waits the longer delay. */
  cod: boolean;
  attempts: number;
  last_error: string;
  done_at: Date | null;
  result: RefetchResult;
}

/** Mirror of the dashboard's IInvoiceOriginal (sub-step 5): one of Flipkart's files of the download the archive was built from. */
export interface IInvoiceOriginal {
  name: string;
  key: string;
  sha256: string;
  bytes: number;
  pages: number;
  type: string;
  invoiceNumber: string;
  invoice_numbers: string[];
  merge_index: number;
  page_from: number;
  signed_at: string;
  signature_ok: boolean;
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
    settled: { type: Boolean, default: false },
    captured_at: { type: Date, default: null },
    unit_ids: { type: [String], default: [] },
    lines: { type: [new Schema<Record<string, unknown>>({}, { _id: false, strict: false })], default: [] },
    summary: { type: Schema.Types.Mixed, default: null },
    stored_download_id: { type: String, default: "" },
    last_download_at: { type: Date, default: null },
    download_count: { type: Number, default: 0 },
    originals: {
      type: [
        new Schema<IInvoiceOriginal>(
          { name: String, key: String, sha256: String, bytes: Number, pages: Number, type: String, invoiceNumber: String, invoice_numbers: [String], merge_index: Number, page_from: Number, signed_at: String, signature_ok: Boolean },
          { _id: false }
        ),
      ],
      default: [],
    },
    originals_download_id: { type: String, default: "" },
    zip_source: { type: String, enum: ["", "original", "split"], default: "" },
    refetch: {
      type: [
        new Schema<IInvoiceRefetch>(
          {
            unit_id: String,
            reason: { type: String, enum: REFETCH_REASONS },
            event_at: Date,
            due_at: Date,
            noted_at: Date,
            by: String,
            cod: Boolean,
            attempts: { type: Number, default: 0 },
            last_error: { type: String, default: "" },
            done_at: { type: Date, default: null },
            result: { type: String, enum: REFETCH_RESULTS, default: "" },
          },
          { _id: false }
        ),
      ],
      default: undefined,
    },
    refetch_claim_until: { type: Date, default: null },
    refetch_claim_by: { type: String, default: "" },
  },
  { timestamps: true }
);

InvoiceSchema.index({ s3_key: 1 }, { unique: true });
InvoiceSchema.index({ userId: 1, order_id: 1 });
InvoiceSchema.index({ userId: 1, processing_status: 1, updatedAt: -1 });
InvoiceSchema.index({ processing_status: 1, processing_started_at: 1, updatedAt: 1 });
InvoiceSchema.index({ userId: 1, settled: 1 });
// The refetch phase's selection (SO-5): open entries due by now. On `invoices` (5,000 rows), never on order_details.
InvoiceSchema.index({ userId: 1, "refetch.due_at": 1 });

export const Invoice = mongoose.model<IInvoice>("Invoice", InvoiceSchema, "invoices");
