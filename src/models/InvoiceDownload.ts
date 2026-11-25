import mongoose, { Schema } from "mongoose";

/**
 * One download of an order's invoice bundle from Flipkart — collection `invoice_downloads` (data-quality sub-step 12,
 * SO-1). Trade Flow writes it (services/invoiceOriginals.ts storeInvoiceDownload) and owns the collection and its
 * index; the dashboard and the extract Lambda only read it (dashboard backend/src/models/InvoiceDownload.ts, sub-step
 * 5): the merged raw PDF carries this document's id as S3 metadata `download-id`, and the Lambda's loadOriginals
 * reads the document to find Flipkart's own files under orig/ and where each one sits in the merged PDF.
 *
 * `_id` is the download id: a string (24 hex characters), never an ObjectId — the dashboard accepts either.
 * Documents are written once (plus `merged.etag` after the raw PUT, or `error` when that PUT failed); none is ever
 * deleted by the app.
 */

/** Which path made the download. */
export type InvoiceDownloadPath = "batch" | "sweep-capture" | "sweep-recapture" | "sweep-refetch" | "by-order-id" | "fetch";

export interface IInvoiceDownloadFile {
  /** Flipkart's own name of the file in its zip answer ("file_7.pdf"; "<order>.pdf" for a single-PDF answer). */
  name: string;
  /** orig/<userId>/<orderId>/<sha256>.pdf — content-addressed, written once. */
  key: string;
  /** Hex SHA-256 of the file's bytes, lowercase. */
  sha256: string;
  bytes: number;
  /** Pages of the file in the merged PDF; 0 when pdf-lib could not read it (then it is not in the merged PDF). */
  pages: number;
  /** The worker's classification (invoiceCapture.ts classifyInvoicePdf): product, protect_promise, flipkart_fee, transport, other. */
  type: string;
  invoice_number: string;
  /** 0-based position of the file in the merged PDF's order; -1 when it is not in it. */
  merge_index: number;
  /** 1-based first page of the file in the merged PDF; 0 when it is not in it. */
  page_from: number;
  /**
   * "new" = this download wrote it; "existing" = the same bytes were already stored; "failed" = S3 refused or failed
   * the PUT (e.g. no orig/* grant before D1): the download went on without it — `originals_error` says why, and the
   * raw PDF then carries no `download-id`, so the Lambda never looks for these files.
   */
  stored: "new" | "existing" | "failed";
}

export interface IInvoiceDownload {
  _id: string;
  userId: mongoose.Types.ObjectId;
  order_id: string;
  platformAccountId: mongoose.Types.ObjectId | null;
  at: Date;
  via: "api" | "page";
  path: InvoiceDownloadPath;
  request_id: string;
  host: string;
  system_id: string;
  /** Flipkart's answer itself: a zip, or a single PDF. */
  answer: { kind: "zip" | "pdf"; bytes: number; sha256: string };
  files: IInvoiceDownloadFile[];
  /** The merged raw PDF made from the files (raw/<userId>/<orderId>.pdf); etag "" when no raw PUT was made (unchanged). */
  merged: { key: string; bytes: number; pages: number; etag: string };
  /** The files equal the processed row's originals and nothing was re-uploaded (sweep re-capture / refetch only). */
  unchanged: boolean;
  /** Set only when the raw PUT failed after this record was written. */
  error?: string;
  /** Set only when one or more originals could not be stored (the first error); the download itself went on. */
  originals_error?: string;
}

const InvoiceDownloadSchema = new Schema<IInvoiceDownload>(
  {
    _id: { type: String },
    userId: { type: Schema.Types.ObjectId, required: true },
    order_id: { type: String, required: true },
    platformAccountId: { type: Schema.Types.ObjectId, default: null },
    at: { type: Date, required: true },
    via: { type: String, enum: ["api", "page"] },
    path: { type: String },
    request_id: { type: String, default: "" },
    host: { type: String, default: "" },
    system_id: { type: String, default: "" },
    answer: { type: Schema.Types.Mixed },
    files: { type: Schema.Types.Mixed, default: [] },
    merged: { type: Schema.Types.Mixed },
    unchanged: { type: Boolean, default: false },
    error: { type: String },
    originals_error: { type: String },
  },
  { collection: "invoice_downloads", versionKey: false, strict: false }
);

// A new collection, so it may carry its own index (plan rule 7 forbids new ones on order_details only).
InvoiceDownloadSchema.index({ userId: 1, order_id: 1, at: -1 });

export const InvoiceDownload = mongoose.model<IInvoiceDownload>("InvoiceDownload", InvoiceDownloadSchema, "invoice_downloads");
