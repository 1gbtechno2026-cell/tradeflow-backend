import { createHash } from "node:crypto";
import os from "node:os";
import { HeadObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { Types } from "mongoose";
import { config } from "../config.js";
import { Invoice } from "../models/Invoice.js";
import { InvoiceDownload, type IInvoiceDownloadFile, type InvoiceDownloadPath } from "../models/InvoiceDownload.js";
import { Order } from "../models/Order.js";
import { noteLateDocumentsForOrder } from "./invoiceRefetch.js";
import { invoiceS3, mergeInvoiceBundle, putInvoice, rawInvoiceKey, registerInvoice, type InvoiceAnswer, type InvoicePart } from "./invoiceCapture.js";

/**
 * Flipkart's own invoice files, kept at download (data-quality sub-step 12, SO-1).
 *
 * Flipkart answers an invoice download with a zip of signed PDFs, one per document (or one PDF). Until now only a
 * merged copy re-saved by pdf-lib was kept, so no stored file carried a signature that verifies. From here on every
 * download goes through ONE store helper, storeInvoiceDownload, used by every path that downloads an order's
 * invoices — the invoice batch, the Download-by-Order-ID modal, the 3-hourly sweep's phases and the fetch's capture:
 *
 *   1. originals      each file Flipkart sent, byte for byte, at orig/<userId>/<orderId>/<sha256>.pdf — PUT with
 *                     IfNoneMatch "*" and its SHA-256 checksum, 4 at a time: content-addressed, so a file is written
 *                     once and a second writer of the same bytes (the other Mac, a repeat download) is a no-op;
 *   2. invoice_downloads  one document per download (models/InvoiceDownload.ts): each file's name, key, sha256,
 *                     pages, type, invoice number and where it sits in the merged PDF (merge_index, page_from);
 *   3. the raw PDF    the merged copy, still the parser's input, to raw/<userId>/<orderId>.pdf with S3 metadata
 *                     `download-id` = that document's id (the extract Lambda reads it — dashboard sub-step 5);
 *   4. register       the `invoices` row (invoiceCapture.ts registerInvoice — T1: etag / size / parts only on insert;
 *                     T2: captured_at + settled by the delivery rule);
 *   5. units          the order's unit rows stamped (X3: the part list only on a unit that has none — the pipeline is
 *                     its one writer).
 *
 * At a bundle's first capture, a unit of the order that already ended gets its late re-download noted
 * (services/invoiceRefetch.ts, SO-5).
 *
 * `skipIfUnchanged` (the sweep's re-capture and the refetch phase only): when the download's files are exactly the
 * processed row's originals, nothing is re-uploaded — no raw PUT, so no Lambda run; the download is recorded with
 * `unchanged: true`. A batch and the modal always re-upload: they mean "extract again".
 *
 * Originals are BEST-EFFORT: a PUT under orig/ that S3 refuses (403 AccessDenied while the Writer policy has no
 * orig/* grant — it comes with D1, dashboard sub-step 5 — or any other S3 error) never fails the download. The file
 * is recorded `stored: "failed"` with `originals_error`, the merged PDF is stored WITHOUT `download-id` (the Lambda
 * then splits the merged copy, as before sub-step 5), and the row's `stored_download_id` is cleared, so the
 * originals backfill (SO-4) picks the order up again. Before this sub-step the paths needed raw/* rights only; they
 * still do.
 *
 * An error this helper throws (the merge, the record, the raw PUT, the row) is marked as OURS
 * (isInvoiceStoreError): the batch files it as internal_error, never as Flipkart's, and the sweep's failure memory
 * does not count it against the order.
 */

export const ORIG_PREFIX = "orig/";

const safe = (s: string) => String(s || "").replace(/[^A-Za-z0-9._-]/g, "_");

/** orig/<userId>/<orderId>/<sha256>.pdf — the same function as the dashboard's services/invoiceOriginals.ts (sub-step 5). */
export function originalKey(userId: string, orderId: string, sha256: string): string {
  return `${ORIG_PREFIX}${safe(userId)}/${safe(orderId)}/${safe(String(sha256 || "").toLowerCase())}.pdf`;
}

export function sha256Hex(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

const PUT_CONCURRENCY = 4;

const statusOf = (err: unknown) => Number((err as { $metadata?: { httpStatusCode?: number } })?.$metadata?.httpStatusCode || 0);
const nameOf = (err: unknown) => String((err as { name?: string; Code?: string })?.name || (err as { Code?: string })?.Code || "");
const firstLine = (err: unknown) => (err instanceof Error ? err.message : String(err)).split("\n")[0].slice(0, 200);

/** Is the object there? false on "not found"; any other answer is thrown. */
async function originalExists(key: string): Promise<boolean> {
  try {
    await invoiceS3().send(new HeadObjectCommand({ Bucket: config.invoiceBucket, Key: key }));
    return true;
  } catch (err) {
    if (statusOf(err) === 404 || /NotFound|NoSuchKey/.test(nameOf(err))) return false;
    throw err;
  }
}

/** Marks an error of the store (after Flipkart answered): ours, not Flipkart's. */
const STORE_STAGE = "invoiceStoreStage";
export function markStoreError(err: unknown): Error {
  const e = err instanceof Error ? err : new Error(String(err));
  (e as unknown as Record<string, unknown>)[STORE_STAGE] = true;
  return e;
}
/** Did this error come from storing the download (S3 / Mongo / the merge) rather than from Flipkart's answer? */
export function isInvoiceStoreError(err: unknown): boolean {
  return Boolean(err && typeof err === "object" && (err as Record<string, unknown>)[STORE_STAGE] === true);
}

export type OriginalOutcome = "new" | "existing" | "failed";

/**
 * Store one original: PUT only if absent (IfNoneMatch "*"), with its SHA-256 for S3 to verify. 412 = already stored
 * ("existing"). A store that refuses the condition (409 ConditionalRequestConflict while another writer is mid-PUT,
 * 501 NotImplemented, 400 on IfNoneMatch) falls back to HEAD, then a plain PUT: identical bytes under a
 * content-addressed key make a duplicate write harmless. Errors start "S3 put original …"; storeOriginals turns them
 * into stored: "failed" (best-effort), so they never fail a download.
 */
async function putOriginal(key: string, body: Buffer, sha256: string): Promise<"new" | "existing"> {
  const checksum = Buffer.from(sha256, "hex").toString("base64");
  const put = (conditional: boolean) =>
    invoiceS3().send(
      new PutObjectCommand({
        Bucket: config.invoiceBucket,
        Key: key,
        Body: body,
        ContentType: "application/pdf",
        ChecksumAlgorithm: "SHA256",
        ChecksumSHA256: checksum,
        ...(conditional ? { IfNoneMatch: "*" } : {}),
      })
    );
  try {
    await put(true);
    return "new";
  } catch (err) {
    const status = statusOf(err);
    if (status === 412 || /PreconditionFailed/.test(nameOf(err))) return "existing";
    if (!(status === 409 || status === 501 || status === 400 || /ConditionalRequestConflict|NotImplemented|InvalidArgument/.test(nameOf(err)))) {
      throw new Error(`S3 put original ${key}: ${firstLine(err)}`);
    }
  }
  try {
    if (await originalExists(key)) return "existing";
    await put(false);
    return "new";
  } catch (err) {
    throw new Error(`S3 put original ${key}: ${firstLine(err)}`);
  }
}

/**
 * Store every original of an order (4 at a time). Returns, per file in the same order, "new", "existing" or
 * "failed" (best-effort: a failed PUT never throws; its message goes to `errors` when given).
 */
export async function storeOriginals(userId: string, orderId: string, files: ReadonlyArray<{ bytes: Buffer; sha256: string }>, errors?: string[]): Promise<OriginalOutcome[]> {
  const out: OriginalOutcome[] = new Array(files.length);
  let next = 0;
  const lane = async () => {
    while (next < files.length) {
      const i = next++;
      try {
        out[i] = await putOriginal(originalKey(userId, orderId, files[i].sha256), files[i].bytes, files[i].sha256);
      } catch (err) {
        out[i] = "failed";
        errors?.push(firstLine(err));
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(PUT_CONCURRENCY, files.length) }, lane));
  return out;
}

/** What a stored download hands back: what the unit rows are stamped with, and what the callers report. */
export interface StoredInvoice {
  key: string;
  /** Size of the merged raw PDF (as stored now). */
  size: number;
  /** The worker's per-type list of the merged PDF (what a NEW Invoice row and a unit without a list get). */
  parts: InvoicePart[];
  downloadId: string;
  /** No raw PUT: the files equal the processed row's originals (skipIfUnchanged only). */
  unchanged: boolean;
  /** failed > 0: not every original was kept — the raw PDF went without download-id (originalsError: the first error). */
  originals: { new: number; existing: number; failed: number };
  originalsError?: string;
}

export interface StoreDownloadInput {
  userId: string;
  accountId: unknown;
  orderId: string;
  answer: InvoiceAnswer;
  /** Every unit of the order (the Invoice row's unit_ids). */
  unitIds: string[];
  /** The unit rows (order_details _id) to stamp now. */
  unitRowIds: unknown[];
  path: InvoiceDownloadPath;
  requestId?: string;
  skipIfUnchanged?: boolean;
}

/**
 * X3 — stamp the order's unit rows with the stored bundle. The key, the flag and the download time on every row
 * given; the size only when a raw PDF was stored; the part list ONLY on a row that has none: from sub-step 4 the
 * extract pipeline writes the one part list of the order on every unit (describeParts), and an unchanged download
 * (no Lambda run) must not put the worker's list back.
 */
export async function stampInvoiceUnits(unitRowIds: readonly unknown[], stored: Pick<StoredInvoice, "key" | "size" | "parts" | "unchanged">, now = new Date()): Promise<void> {
  if (!unitRowIds.length) return;
  const ids = [...unitRowIds];
  await Order.updateMany(
    { _id: { $in: ids } },
    { $set: { invoice_s3_key: stored.key, invoice_downloaded_at: now, is_invoice_downloaded: true, ...(stored.unchanged ? {} : { invoice_size: stored.size }) } }
  );
  await Order.updateMany({ _id: { $in: ids }, $or: [{ invoice_parts: null }, { invoice_parts: { $size: 0 } }] }, { $set: { invoice_parts: stored.parts } });
}

/** The processed row's originals equal this download's files (same sha256 set), and its archive was built from the download stored now. */
function sameAsProcessed(row: { processing_status?: string; originals?: Array<{ sha256?: string }>; stored_download_id?: string; originals_download_id?: string } | null, shas: readonly string[]): boolean {
  if (!row || row.processing_status !== "processed") return false;
  const kept = (row.originals || []).map((o) => String(o.sha256 || "").toLowerCase()).filter(Boolean);
  if (!kept.length) return false;
  if (row.stored_download_id && row.originals_download_id && row.stored_download_id !== row.originals_download_id) return false;
  const a = [...new Set(kept)].sort();
  const b = [...new Set(shas)].sort();
  return a.length === b.length && a.every((s, i) => s === b[i]);
}

/**
 * The one store helper (SO-1): Flipkart's answer → originals → invoice_downloads → raw PDF with `download-id` →
 * registerInvoice → units. Originals are best-effort (see the top of this file). Throws what S3 or Mongo threw on
 * the record, the raw PUT or the rows, marked as ours (isInvoiceStoreError); the originals and the record written
 * before a failure stay (content-addressed / a record of a download that was made), and the record then says what failed.
 */
export async function storeInvoiceDownload(input: StoreDownloadInput, now = new Date()): Promise<StoredInvoice> {
  try {
    return await storeDownload(input, now);
  } catch (err) {
    throw markStoreError(err);
  }
}

async function storeDownload(input: StoreDownloadInput, now: Date): Promise<StoredInvoice> {
  const { userId, orderId, answer } = input;
  if (!answer.files.length) throw new Error(`invoice answer for ${orderId} holds no PDF`);
  const key = rawInvoiceKey(userId, orderId);
  const files = answer.files.map((f) => ({ name: f.name, bytes: f.bytes, sha256: sha256Hex(f.bytes) }));
  const merged = await mergeInvoiceBundle(files.map((f) => f.bytes));

  const row = input.skipIfUnchanged ? await Invoice.findOne({ s3_key: key }).select("processing_status size originals.sha256 stored_download_id originals_download_id").lean() : null;
  const unchanged = sameAsProcessed(row, files.map((f) => f.sha256));

  // 1. Flipkart's own files — best-effort: a refused PUT is recorded, never fatal.
  const originalsErrors: string[] = [];
  const stored = await storeOriginals(userId, orderId, files, originalsErrors);
  const failedOriginals = stored.filter((s) => s === "failed").length;
  const originalsKept = failedOriginals === 0;
  if (!originalsKept) {
    console.warn(`[invoice-originals] ${orderId}: ${failedOriginals} of ${files.length} original(s) not kept (${originalsErrors[0]}) — the merged PDF is stored without download-id`);
  }

  // 2. The download, before the raw PUT that names it: the Lambda may read it the moment the object lands.
  const downloadId = new Types.ObjectId().toHexString();
  const account = input.accountId && Types.ObjectId.isValid(String(input.accountId)) ? new Types.ObjectId(String(input.accountId)) : null;
  const recordFiles: IInvoiceDownloadFile[] = files.map((f, i) => ({
    name: f.name,
    key: originalKey(userId, orderId, f.sha256),
    sha256: f.sha256,
    bytes: f.bytes.length,
    pages: merged.layout[i].pages,
    type: merged.layout[i].type,
    invoice_number: merged.layout[i].invoiceNumber,
    merge_index: merged.layout[i].mergeIndex,
    page_from: merged.layout[i].pageFrom,
    stored: stored[i],
  }));
  await InvoiceDownload.collection.insertOne({
    _id: downloadId,
    userId: new Types.ObjectId(userId),
    order_id: orderId,
    platformAccountId: account,
    at: now,
    via: answer.via,
    path: input.path,
    request_id: input.requestId || "",
    host: config.hostId || os.hostname(),
    system_id: config.systemId,
    answer: { kind: answer.kind, bytes: answer.bytes, sha256: answer.sha256 },
    files: recordFiles,
    merged: { key, bytes: merged.pdf.length, pages: merged.pages, etag: "" },
    unchanged,
    ...(originalsKept ? {} : { originals_error: originalsErrors[0] || "an original was not stored" }),
  } as never);

  // 3. The merged PDF — unless nothing changed and the caller asked to skip it.
  let size = unchanged ? Number(row?.size) || merged.pdf.length : merged.pdf.length;
  let etag = "";
  if (!unchanged) {
    try {
      // download-id only when every original is stored: the Lambda would otherwise look for files that are not there.
      const put = await putInvoice(key, merged.pdf, originalsKept ? { downloadId } : {});
      size = put.size;
      etag = put.etag;
    } catch (err) {
      await InvoiceDownload.collection.updateOne({ _id: downloadId } as never, { $set: { error: `raw PUT failed: ${firstLine(err)}` } }).catch(() => undefined);
      throw err;
    }
    await InvoiceDownload.collection.updateOne({ _id: downloadId } as never, { $set: { "merged.etag": etag } });
  }

  // 4. The invoices row, 5. the units.
  const registered = await registerInvoice({
    userId,
    platformAccountId: input.accountId,
    orderId,
    key,
    etag,
    size,
    parts: merged.parts,
    unitIds: input.unitIds,
    downloadId: originalsKept ? downloadId : "",
    originalsMissing: !originalsKept,
    unchanged,
    now,
  });
  // SO-5: the bundle's first capture — a unit of the order that already ended (cancelled / returned / refunded) gets
  // its late re-download noted now; the noting refuses a second (unit, reason) and never throws.
  if (registered.captured) await noteLateDocumentsForOrder(userId, orderId, `store ${input.path}`, now);
  const out: StoredInvoice = {
    key,
    size,
    parts: merged.parts,
    downloadId,
    unchanged,
    originals: { new: stored.filter((s) => s === "new").length, existing: stored.filter((s) => s === "existing").length, failed: failedOriginals },
    ...(originalsKept ? {} : { originalsError: originalsErrors[0] || "an original was not stored" }),
  };
  await stampInvoiceUnits(input.unitRowIds, out, now);
  return out;
}
