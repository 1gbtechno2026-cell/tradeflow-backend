import fs from "node:fs";
import type { APIRequestContext } from "playwright";
import { OrderApiError, romeCall } from "./orderApi.js";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import type { Page } from "playwright";
import { createRequire } from "node:module";
import { PDFDocument } from "pdf-lib";
import { unzipSync } from "fflate";
import { HeadObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { config } from "../config.js";
import { Types } from "mongoose";
import { Order } from "../models/Order.js";
import { Invoice, type IInvoicePart } from "../models/Invoice.js";
import { evalOnPage } from "../lib/evalOnPage.js";
import { sleep } from "./browser.js";
import { isTerminalStatus, stageOf } from "./orderLifecycle.js";
import { stampInvoiceUnits, storeInvoiceDownload, type StoredInvoice } from "./invoiceOriginals.js";

/**
 * Invoice capture: open a delivered unit's order page, press Flipkart's
 * "Download Invoice", catch the file, and put it in the private S3 bucket
 * under the key the extraction pipeline listens on:
 *
 *   raw/<workspace user id>/<order id>.pdf
 *
 * One object per order — Flipkart's download on ANY unit page returns the
 * whole order's bundle, so every unit of the order shares the key and the
 * pipeline picks each unit's own product invoice out of it. The key never
 * changes afterwards; status lives on an S3 tag (pending here, processed /
 * failed from the Lambda) and on the `invoices` row this file registers as
 * `pending`. S3 → SQS → Lambda does the extraction; nothing is parsed here.
 *
 * Since data-quality sub-step 12 every download — this file's fetch capture,
 * the batch, the modal, the sweep — is stored through ONE helper,
 * services/invoiceOriginals.ts storeInvoiceDownload: Flipkart's own files are
 * kept untouched under orig/<user>/<order>/<sha256>.pdf and recorded in
 * invoice_downloads before the merged copy goes to raw/ (docs/invoice-originals.md).
 *
 * Runs as a step AFTER enrichment, never inside it, so the fetch invariant
 * (listed − dropped − known + carried = queued = enriched) is untouched.
 * Switched per run from the dashboard: off | new (units enriched in this
 * run) | missing (every delivered unit of the account with no key yet, capped
 * by INVOICE_MAX_PER_RUN). Credentials come from the standard AWS chain.
 */

export type InvoiceMode = "off" | "new" | "missing";

const require = createRequire(import.meta.url);
const pdfParse = require("pdf-parse") as (data: Buffer) => Promise<{ text: string; numpages: number }>;

/** What one PDF inside Flipkart's invoice bundle is. */
export type InvoicePartType = "product" | "protect_promise" | "flipkart_fee" | "transport" | "other";
export interface InvoicePart {
  type: InvoicePartType;
  pages: number;
  invoiceNumber: string;
  bytes: number;
}

/** One pdf-parse read may take this long; then it is a failed read and the queue moves on (a stuck read must not stall every later classification of the process). */
export const PDF_PARSE_TIMEOUT_MS = 20_000;

/** pdf-parse, one read at a time in this process (see classifyInvoicePdf), each bounded by PDF_PARSE_TIMEOUT_MS. */
let pdfParseTail: Promise<unknown> = Promise.resolve();
function pdfParseOneAtATime(data: Buffer, timeoutMs = PDF_PARSE_TIMEOUT_MS): Promise<{ text: string; numpages: number }> {
  const run = pdfParseTail.then(() => {
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`pdf-parse read took over ${timeoutMs} ms`)), timeoutMs);
      timer.unref?.();
    });
    return Promise.race([pdfParse(data) as Promise<{ text: string; numpages: number }>, timeout]).finally(() => clearTimeout(timer));
  });
  pdfParseTail = run.catch(() => undefined);
  return run;
}

/** Classify a PDF of the bundle by its text; order decides what comes first in the merged file. */
export async function classifyInvoicePdf(buf: Buffer): Promise<{ type: InvoicePartType; invoiceNumber: string; text: string }> {
  // pdf.js 1.10 (inside pdf-parse) now and then fails a document it reads fine a moment later ("bad XRef entry"
  // right after another document was read — measured 2026-11-26 on the offline fixtures: 1 of 4 product invoices
  // of a 6-file answer came back "other", and a sequence of reads gave other, other, product, product…). Each try
  // reads its own copy of the bytes (never a slice of Node's 64 KB Buffer pool), and a failed read is tried again
  // up to twice before the file is called "other".
  //
  // The reads also go ONE AT A TIME in this process (pdfParseOneAtATime): pdf.js 1.10 runs in-process (its "fake
  // worker"), and reads that overlap can corrupt each other — measured 2026-11-26: six reads of the fixtures started
  // together failed 2 of 6 ("Command token too long"), read one after the other in the same script 0 of 18. Overlap is
  // what production has: a merge reads every file of an answer at once, and a batch / sweep stores 6–12 orders at once.
  // Serial reads still fail now and then inside the full check ("bad XRef entry", or an empty text), which the
  // retries below absorb: test:invoice-originals labels its 6-file answer right on every run with both in place.
  let text: string | null = null;
  for (let attempt = 0; attempt < 3 && text === null; attempt++) {
    try {
      text = (await pdfParseOneAtATime(ownBuffer(buf))).text.replace(/\s+/g, " ");
      // An empty text is a failed read too (measured: pdf.js returned "" for a product invoice it read in full the
      // next time); a real invoice always has text, and an image-only file is still "other" after the last try.
      if (!text.trim()) text = null;
    } catch {
      text = null;
    }
  }
  if (text === null) return { type: "other", invoiceNumber: "", text: "" };
  return { ...classifyInvoiceText(text), text };
}

/** The part label and invoice number of one PDF's text (whitespace already collapsed). */
export function classifyInvoiceText(text: string): { type: InvoicePartType; invoiceNumber: string } {
  const invoiceNumber = (text.match(/Invoice Number\s*#?\s*:?\s*([A-Z0-9]{8,})/i) || [])[1] || (text.match(/Tax Invoice Number\s*:\s*([A-Z0-9]{8,})/i) || [])[1] || "";
  let type: InvoicePartType = "other";
  if (/GOODS TRANSPORTED BY GTA|Goods Carriage/i.test(text) || /NB[A-Z0-9]{8,}\s*Tax Invoice Number/i.test(text) || /\b(?:GT|Delivery)\s*Charges\s*996\d/i.test(text)) type = "transport";
  else if (/Protect Promise Fee/i.test(text)) type = "protect_promise";
  else if (/Corporate Card Fee|Offer Handling Fee|Flipkart Internet Private Limited/i.test(text) && !/FSN:/i.test(text)) type = "flipkart_fee";
  else if (/FSN:|HSN\/SAC:\s*\d{4,}|Product\s*Title|Total items:/i.test(text)) type = "product";
  // T4 (data-quality sub-step 12, the dashboard parser's rule of sub-step 3): the sellers' own layouts are product
  // invoices too — a goods invoice ("HSN: <code>" with a goods code, service codes start with 99, and no FSN) and a
  // grocery "Tax Invoice/Bill of Supply" headed "HSN (Tax%)". Tested after every fee and freight rule, as there.
  else if (isGoodsInvoice(text) || isGroceryInvoice(text)) type = "product";
  return { type, invoiceNumber };
}

function isGoodsInvoice(text: string): boolean {
  return /Tax Invoice/i.test(text) && !/FSN:/i.test(text) && /\bHSN:\s*(?!99)\d{4,8}\b/.test(text);
}

function isGroceryInvoice(text: string): boolean {
  return /Tax Invoice\s*\/\s*Bill of Supply/i.test(text) && /HSN\s*\(Tax\s*%\)/i.test(text) && !/FSN:/i.test(text);
}

const PART_ORDER: Record<InvoicePartType, number> = { product: 0, protect_promise: 1, flipkart_fee: 2, transport: 3, other: 4 };

/** Where one file of the answer landed in the merged PDF (index = the file's position in the answer). */
export interface MergedFileLayout {
  type: InvoicePartType;
  invoiceNumber: string;
  /** Pages of the file in the merged PDF; 0 = pdf-lib could not read it, so it is not in the merged PDF. */
  pages: number;
  /** 0-based position in the merged PDF's order; -1 when the file is not in it. */
  mergeIndex: number;
  /** 1-based first page in the merged PDF; 0 when the file is not in it. */
  pageFrom: number;
}

/**
 * Merge the bundle into one PDF, product invoice first, and describe the parts. `layout` (one entry per input
 * file, in the input's order) says where each file went: what invoice_downloads records as merge_index / page_from,
 * so the dashboard can tie every parsed document back to Flipkart's own file (sub-step 5).
 */
export async function mergeInvoiceBundle(pdfs: Buffer[]): Promise<{ pdf: Buffer; parts: InvoicePart[]; layout: MergedFileLayout[]; pages: number }> {
  const classified = await Promise.all(pdfs.map(async (b, src) => ({ src, buf: b, ...(await classifyInvoicePdf(b)) })));
  classified.sort((a, b) => PART_ORDER[a.type] - PART_ORDER[b.type] || a.src - b.src);
  const layout: MergedFileLayout[] = classified
    .slice()
    .sort((a, b) => a.src - b.src)
    .map((c) => ({ type: c.type, invoiceNumber: c.invoiceNumber, pages: 0, mergeIndex: -1, pageFrom: 0 }));
  if (classified.length === 1) {
    const only = classified[0];
    let pages = 1;
    try {
      pages = (await PDFDocument.load(only.buf, { ignoreEncryption: true })).getPageCount();
    } catch {
      /* keep 1 */
    }
    layout[only.src] = { ...layout[only.src], pages, mergeIndex: 0, pageFrom: 1 };
    return { pdf: only.buf, parts: [{ type: only.type, pages, invoiceNumber: only.invoiceNumber, bytes: only.buf.length }], layout, pages };
  }
  const merged = await PDFDocument.create();
  const parts: InvoicePart[] = [];
  let nextPage = 1;
  let mergeIndex = 0;
  for (const c of classified) {
    try {
      const src = await PDFDocument.load(c.buf, { ignoreEncryption: true });
      const pages = await merged.copyPages(src, src.getPageIndices());
      for (const p of pages) merged.addPage(p);
      parts.push({ type: c.type, pages: pages.length, invoiceNumber: c.invoiceNumber, bytes: c.buf.length });
      if (pages.length) {
        layout[c.src] = { ...layout[c.src], pages: pages.length, mergeIndex: mergeIndex++, pageFrom: nextPage };
        nextPage += pages.length;
      }
    } catch {
      parts.push({ type: c.type, pages: 0, invoiceNumber: c.invoiceNumber, bytes: c.buf.length });
    }
  }
  return { pdf: Buffer.from(await merged.save()), parts, layout, pages: nextPage - 1 };
}

// ── Flipkart's answer, as it came ────────────────────────────────────────────

/** One file of Flipkart's answer: its own name in the zip ("file_7.pdf"; "<order>.pdf" for a single-PDF answer) and its bytes, untouched. */
export interface InvoiceAnswerFile {
  name: string;
  bytes: Buffer;
}

/** What one download of an order's invoices brought back. */
export interface InvoiceAnswer {
  kind: "zip" | "pdf";
  via: "api" | "page";
  /** The answer's own size and sha256 (the zip, or the single PDF). */
  bytes: number;
  sha256: string;
  /** Every PDF of the answer, in Flipkart's file order (numeric: file_7 before file_11). */
  files: InvoiceAnswerFile[];
}

/**
 * P9 — Flipkart's zip names in Flipkart's own order: numeric, so file_7 comes before file_11 (a text sort put
 * file_7 after file_32). Safe since the pipeline no longer reads position (conflict 6 of the data-quality plan);
 * the merged PDF's page order and Compile's _n numbering follow Flipkart's file order.
 */
export function sortInvoiceFileNames(names: readonly string[]): string[] {
  return [...names].sort((a, b) => a.localeCompare(b, "en", { numeric: true, sensitivity: "base" }) || (a < b ? -1 : a > b ? 1 : 0));
}

/** `bytes` in memory of its own (never a slice of Node's Buffer pool or of a larger buffer). */
export function ownBuffer(bytes: Uint8Array): Buffer {
  if (Buffer.isBuffer(bytes) && bytes.byteOffset === 0 && bytes.buffer.byteLength === bytes.byteLength) return bytes;
  const own = Buffer.alloc(bytes.byteLength);
  own.set(bytes);
  return own;
}

/** Flipkart's answer, unpacked: a single PDF, or a zip of PDFs. null when the body is neither; a zip without a PDF gives files: []. */
export function unpackInvoiceAnswer(body: Buffer, orderId: string, via: "api" | "page"): InvoiceAnswer | null {
  const sha256 = createHash("sha256").update(body).digest("hex");
  if (body.subarray(0, 4).toString("latin1") === "%PDF") return { kind: "pdf", via, bytes: body.length, sha256, files: [{ name: `${orderId}.pdf`, bytes: body }] };
  if (body[0] === 0x50 && body[1] === 0x4b) {
    const entries = unzipSync(new Uint8Array(body.buffer, body.byteOffset, body.byteLength));
    const names = sortInvoiceFileNames(Object.keys(entries).filter((n) => !n.endsWith("/") && n.toLowerCase().endsWith(".pdf")));
    return { kind: "zip", via, bytes: body.length, sha256, files: names.map((n) => ({ name: n, bytes: ownBuffer(entries[n]) })) };
  }
  return null;
}

export function parseInvoiceMode(v: unknown): InvoiceMode {
  const s = String(v || "").toLowerCase();
  return s === "new" || s === "missing" ? s : "off";
}

export function invoiceStoreEnabled(): boolean {
  return Boolean(config.invoiceBucket);
}

/** The one S3 call this file and services/invoiceOriginals.ts make: `send`. */
export type InvoiceS3 = Pick<S3Client, "send">;

let client: InvoiceS3 | null = null;
export function invoiceS3(): InvoiceS3 {
  if (!config.invoiceBucket) throw new Error("INVOICE_S3_BUCKET is not set");
  if (!client) client = new S3Client({ region: config.awsRegion });
  return client;
}
const s3 = invoiceS3;

/** Checks only (scripts/invoiceOriginalsCheck.ts): every S3 call of the invoice store goes to `fake` (an in-memory store); null restores the real client. */
export function useInvoiceS3ForChecks(fake: InvoiceS3 | null): void {
  client = fake;
}

export const RAW_PREFIX = "raw/";

/** raw/<workspace user id>/<order id>.pdf — fixed for the object's life. Same function as the dashboard's invoicePipeline.ts. */
export function rawInvoiceKey(userId: string, orderId: string): string {
  const safe = (s: string) => String(s || "").replace(/[^A-Za-z0-9._-]/g, "_");
  return `${RAW_PREFIX}${safe(userId)}/${safe(orderId)}.pdf`;
}

export async function invoiceExists(key: string): Promise<boolean> {
  try {
    await s3().send(new HeadObjectCommand({ Bucket: config.invoiceBucket, Key: key }));
    return true;
  } catch {
    return false;
  }
}

/**
 * Upload the bundle tagged `status=pending`; the ETag identifies these exact bytes to the pipeline. `downloadId`
 * (sub-step 12) goes on the object as metadata `download-id`: the invoice_downloads document of the download this
 * merged PDF was made from, which the extract Lambda reads from its HEAD to find Flipkart's own files (sub-step 5).
 */
export async function putInvoice(key: string, body: Buffer, opts: { downloadId?: string } = {}): Promise<{ size: number; etag: string }> {
  const res = await s3().send(
    new PutObjectCommand({
      Bucket: config.invoiceBucket,
      Key: key,
      Body: body,
      ContentType: "application/pdf",
      Tagging: "status=pending",
      ...(opts.downloadId ? { Metadata: { "download-id": opts.downloadId } } : {}),
    })
  );
  return { size: body.length, etag: String(res.ETag || "").replace(/"/g, "") };
}

/** INVOICE_SETTLE_HOURS (default 48): how long after the last delivery an order's invoice bundle is final. */
export function invoiceSettleMs(): number {
  return Math.max(1, Number(process.env.INVOICE_SETTLE_HOURS || 48)) * 3_600_000;
}

/** The fields of a unit the delivery rule reads. */
export interface SettleUnit {
  status_key?: string | null;
  actual_delivery_date?: Date | string | null;
  cancelled_date?: Date | string | null;
}

/**
 * The delivery rule (T2 / C11): an order's bundle is final — `settled` — when every unit of the order is finished
 * (Delivered, Cancelled, Returned, Refunded, Failed) and the last of its deliveries (a cancelled unit: its
 * cancellation) is older than INVOICE_SETTLE_HOURS. A unit still on its way, or a delivered unit whose delivery time
 * is not known, keeps the bundle open: the sweep's phase 2 then looks at it again. An order with no unit is not
 * settled.
 */
export function settledByDelivery(units: readonly SettleUnit[], now = new Date(), settleMs = invoiceSettleMs()): boolean {
  if (!units.length) return false;
  let last = 0;
  for (const u of units) {
    if (!isTerminalStatus(u.status_key)) return false;
    const stage = stageOf(u.status_key);
    const at = u.actual_delivery_date ? new Date(u.actual_delivery_date).getTime() : u.cancelled_date ? new Date(u.cancelled_date).getTime() : NaN;
    if (Number.isNaN(at)) {
      // A Failed unit was never placed, so never invoiced; any other finished unit without its time keeps the bundle open.
      if (stage === "failed") continue;
      return false;
    }
    last = Math.max(last, at);
  }
  return last > 0 && last <= now.getTime() - settleMs;
}

/** The delivery rule on the order's stored units. */
export async function orderSettledByDelivery(userId: string, orderId: string, now = new Date()): Promise<boolean> {
  const units = await Order.find({ userId, order_id: orderId }).select("status_key actual_delivery_date cancelled_date").lean();
  return settledByDelivery(units as SettleUnit[], now);
}

/**
 * Hand the object to the extraction pipeline: one `invoices` row per bundle,
 * created `pending` when new. An existing row keeps its status — the Lambda's
 * claim notices changed bytes by etag on its own — so a re-capture never
 * regresses a processed invoice; and if the S3 event beat us here, the
 * Lambda's own upsert already made the row and this only fills in what the
 * worker knows (units, account).
 *
 * T1 (data-quality sub-step 12): `etag`, `size` and `parts` are written ONLY when the row is new. On an existing
 * row the Lambda's claim owns the etag and describeParts owns the part list: before this, a re-download $set the
 * new etag here, before the S3 event's claim ("processed, etag differs") could see it, so a re-downloaded
 * processed bundle was never extracted again (70 of 70 in the 100-order test), and `parts` flipped to Flipkart's
 * per-file list. Flipkart's file list lives in invoice_downloads (one document per download), which
 * `stored_download_id` names: the download whose merged PDF is at `s3_key` now.
 *
 * T2: a row without `captured_at` (new, or stored by a path that never set it — the fetch, the batch) gets it now,
 * and `settled` by the delivery rule (settledByDelivery), so a bundle fetched before delivery gets the sweep's
 * second look after delivery, and one fetched long after it is final at once.
 *
 * `unchanged` (a sweep re-capture or a refetch whose files equal the processed row's originals — no raw PUT was
 * made): the download is counted, `stored_download_id` keeps naming the download whose bytes are stored.
 */
export async function registerInvoice(input: {
  userId: string;
  platformAccountId: unknown;
  orderId: string;
  key: string;
  etag: string;
  size: number;
  parts: IInvoicePart[];
  unitIds: string[];
  /** The invoice_downloads id of this download ("" from a path that records none). */
  downloadId?: string;
  /** Not every original of this download was stored: the raw PDF went without download-id, so no download names it. */
  originalsMissing?: boolean;
  unchanged?: boolean;
  now?: Date;
}): Promise<{ captured: boolean; settled: boolean | null }> {
  const now = input.now || new Date();
  const account = input.platformAccountId && Types.ObjectId.isValid(String(input.platformAccountId)) ? new Types.ObjectId(String(input.platformAccountId)) : null;
  const stored = input.downloadId && !input.unchanged ? { stored_download_id: input.downloadId } : {};
  // The bytes at s3_key now come from a download whose originals are not all kept: no download names them (the
  // originals backfill, SO-4, selects such a row again).
  const unset = input.originalsMissing && !input.unchanged ? { $unset: { stored_download_id: "" } } : {};
  await Invoice.updateOne(
    { s3_key: input.key },
    {
      $setOnInsert: {
        userId: new Types.ObjectId(input.userId),
        order_id: input.orderId,
        source_key: input.key,
        processing_status: "pending",
        retry_count: 0,
        last_error: "",
        createdAt: now,
        etag: input.etag,
        size: input.size,
        parts: input.parts,
      },
      $set: { updatedAt: now, last_download_at: now, ...stored, ...(account ? { platformAccountId: account } : {}) },
      $inc: { download_count: 1 },
      $addToSet: { unit_ids: { $each: input.unitIds } },
      ...unset,
    },
    { upsert: true, timestamps: false }
  );
  // The S3 event's claim can make the row first (its upsert sets parts: []): the worker's list then fills an EMPTY
  // list only — the pipeline (describeParts) stays the owner of every list it wrote.
  if (input.parts.length) {
    await Invoice.updateOne({ s3_key: input.key, $or: [{ parts: { $exists: false } }, { parts: null }, { parts: { $size: 0 } }] }, { $set: { parts: input.parts } }, { timestamps: false });
  }
  // T2 — the first capture this row has on record: when, and whether the bundle is already final.
  const settled = await orderSettledByDelivery(input.userId, input.orderId, now);
  const res = await Invoice.updateOne({ s3_key: input.key, captured_at: null }, { $set: { captured_at: now, settled } }, { timestamps: false });
  return res.modifiedCount ? { captured: true, settled } : { captured: false, settled: null };
}

// ---- the page work (ported from the dashboard's proven click path) ----------

const EXPAND_PRICE_DETAILS = `() => {
  const hasInvoice = Array.from(document.querySelectorAll("div, span, a, button")).some((el) => (el.textContent || "").trim() === "Download Invoice");
  if (hasInvoice) return "visible";
  const block = document.querySelector('[data-tat-id="OD_PRICE_BLOCK"]');
  if (block) { block.click(); return "od-price-block"; }
  for (const el of document.querySelectorAll('[role="button"]')) {
    if (/price details/i.test(el.textContent || "") && (el.textContent || "").length < 80) { el.click(); return "price-details"; }
  }
  for (const el of document.querySelectorAll("span, div")) {
    const t = (el.textContent || "").replace(/\\s+/g, " ").trim();
    if (/^Paid ₹[\\d,]+(?:\\.\\d+)? by /i.test(t) && t.length < 90) { (el.closest('[role="button"]') || el.parentElement || el).click(); return "paid-line"; }
  }
  return null;
}`;

const CLICK_DOWNLOAD_INVOICE = `() => {
  const els = document.querySelectorAll("div, span, a, button");
  for (const el of els) { const t = (el.textContent || "").trim(); if (t === "Download Invoice" && el.childElementCount <= 1) { el.click(); return true; } }
  for (const el of els) { if ((el.textContent || "").trim() === "Download Invoice") { el.click(); return true; } }
  return false;
}`;

async function invoiceVisible(page: Page): Promise<boolean> {
  try {
    if (await page.getByText("Download Invoice", { exact: true }).count()) return true;
  } catch {
    /* eval */
  }
  return Boolean(await evalOnPage<boolean>(page, `() => Array.from(document.querySelectorAll("div, span, a, button")).some((el) => (el.textContent || "").trim() === "Download Invoice")`));
}

async function revealDownloadInvoice(page: Page): Promise<boolean> {
  if (await invoiceVisible(page)) return true;
  await evalOnPage<string | null>(page, EXPAND_PRICE_DETAILS).catch(() => null);
  await sleep(900);
  if (await invoiceVisible(page)) return true;
  try {
    const paid = page.getByText(/Paid ₹[\d,]+ by /i).first();
    if (await paid.count()) {
      await paid.click({ timeout: 4000 });
      await sleep(900);
    }
  } catch {
    /* continue */
  }
  return invoiceVisible(page);
}

async function clickDownloadInvoice(page: Page): Promise<boolean> {
  try {
    const byText = page.getByText("Download Invoice", { exact: true }).first();
    if (await byText.count()) {
      await byText.click({ timeout: 4000 });
      return true;
    }
  } catch {
    /* eval */
  }
  return Boolean(await evalOnPage<boolean>(page, CLICK_DOWNLOAD_INVOICE));
}

/**
 * Open the order page (session already on the page) and bring back Flipkart's
 * answer as it came: a single PDF, or a zip bundling the product invoice with
 * the fee / transport invoices (one order = several tax invoices), each file
 * with its own name, in Flipkart's (numeric) file order.
 */
export async function captureInvoiceAnswer(page: Page, orderUrl: string, orderId: string): Promise<InvoiceAnswer> {
  await page.goto(orderUrl, { waitUntil: "domcontentloaded", timeout: 25000 });
  await sleep(900);
  await page
    .waitForFunction(() => /order_details|order_id=/i.test(location.href) && /price details|download invoice|delivered/i.test(document.body.innerText || ""), { timeout: 12000 })
    .catch(() => undefined);
  if (!(await revealDownloadInvoice(page))) throw new Error("Download Invoice not on page");
  const wait = page.waitForEvent("download", { timeout: 25000 }).catch(() => null);
  if (!(await clickDownloadInvoice(page))) throw new Error("Download Invoice could not be clicked");
  const download = await wait;
  if (!download) throw new Error("Invoice click did not start a download");
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "fk-inv-"));
  try {
    const raw = path.join(tmp, "invoice.download");
    await download.saveAs(raw);
    const answer = unpackInvoiceAnswer(fs.readFileSync(raw), orderId, "page");
    if (!answer) throw new Error("Downloaded file was not a PDF or zip");
    if (!answer.files.length) throw new Error("Invoice zip had no PDF");
    return answer;
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
}

/** Back-compat: the page's PDFs only. */
export async function captureInvoicePdfs(page: Page, orderUrl: string, orderId = "invoice"): Promise<Buffer[]> {
  return (await captureInvoiceAnswer(page, orderUrl, orderId)).files.map((f) => f.bytes);
}

/** Back-compat: the merged bundle as one PDF. */
export async function captureInvoicePdf(page: Page, orderUrl: string): Promise<Buffer> {
  return (await mergeInvoiceBundle(await captureInvoicePdfs(page, orderUrl))).pdf;
}

export interface InvoiceCaptureSummary {
  candidates: number;
  stored: number;
  failed: number;
  skippedCap: number;
}

/**
 * The after-enrichment step for one account. `justEnriched` is the set of unit
 * ids mapped in this run (what "new" means); "missing" ignores it.
 */
/**
 * The order's invoice bundle over Flipkart's API — the request the page's
 * "Download Invoice" button makes (recorded 2026-10-30 on OD339012711228057100:
 * GET <dc>.rome.api.flipkart.com/api/1/self-serve/invoice/download/<orderId>
 * → application/octet-stream, DC-switched like every rome call). The body is
 * either a single %PDF, or (multi-invoice orders — the common case) a PK zip
 * of one PDF per tax invoice — the same bundle the page's "Download Invoice"
 * button produces. Returns the answer with every PDF under Flipkart's own name,
 * untouched, in Flipkart's file order; the store helper keeps them and merges.
 */
export async function downloadInvoiceAnswerViaApi(ctx: APIRequestContext, orderId: string): Promise<InvoiceAnswer> {
  const { res } = await romeCall(ctx, (host) => ctx.get(`${host}/api/1/self-serve/invoice/download/${orderId}`, { timeout: 60_000 }));
  if (!res.ok()) throw new OrderApiError(`invoice download for ${orderId} answered HTTP ${res.status()}`, res.status(), "");
  const body = await res.body();
  const answer = unpackInvoiceAnswer(body, orderId, "api");
  if (!answer) throw new Error(`invoice download for ${orderId} is neither PDF nor zip (${res.headers()["content-type"] || "?"}, ${body.length} bytes)`);
  if (!answer.files.length) throw new Error(`invoice zip for ${orderId} had no PDF`);
  return answer;
}

/** Back-compat: the API answer's PDFs only, in Flipkart's file order. */
export async function downloadInvoiceViaApi(ctx: APIRequestContext, orderId: string): Promise<Buffer[]> {
  return (await downloadInvoiceAnswerViaApi(ctx, orderId)).files.map((f) => f.bytes);
}

export async function captureInvoicesForAccount(opts: {
  userId: string;
  platformAccountId: unknown;
  email: string;
  mode: InvoiceMode;
  justEnriched: Set<string>;
  /** API mode: the account's order-API session; the page is opened only when the API cannot deliver. */
  api?: APIRequestContext;
  getPage: () => Promise<Page>;
  cancelled: () => boolean;
  log: (level: "info" | "warn", message: string) => void;
}): Promise<InvoiceCaptureSummary> {
  const out: InvoiceCaptureSummary = { candidates: 0, stored: 0, failed: 0, skippedCap: 0 };
  if (opts.mode === "off") return out;
  if (!invoiceStoreEnabled()) {
    opts.log("warn", `${opts.email}: invoices requested but INVOICE_S3_BUCKET is not set — skipped`);
    return out;
  }
  const rows = await Order.find({
    userId: opts.userId,
    platformAccountId: opts.platformAccountId,
    status_key: /deliver/i,
    $or: [{ invoice_s3_key: { $in: [null, ""] } }, { invoice_s3_key: { $exists: false } }],
  })
    .select("order_id unit_id order_url order_date item_id")
    .sort({ order_date: -1 })
    .lean();
  const wanted = opts.mode === "new" ? rows.filter((r) => opts.justEnriched.has(String(r.unit_id))) : rows;
  out.candidates = wanted.length;
  if (!wanted.length) return out;
  const cap = config.invoiceMaxPerRun;
  const batch = wanted.slice(0, cap);
  out.skippedCap = wanted.length - batch.length;
  opts.log("info", `${opts.email}: invoices — ${wanted.length} delivered unit(s) without a PDF${out.skippedCap ? ` (doing ${cap}, ${out.skippedCap} left for the next run)` : ""}`);
  let page: Page | null = null;
  const pageOf = async () => (page ||= await opts.getPage());
  const answerOf = async (orderId: string, orderUrl: string): Promise<InvoiceAnswer> => {
    if (opts.api) {
      try {
        return await downloadInvoiceAnswerViaApi(opts.api, orderId);
      } catch (err) {
        opts.log("warn", `${orderId}: invoice over the API failed (${err instanceof Error ? err.message.split("\n")[0].slice(0, 120) : String(err)}) — the page`);
      }
    }
    return captureInvoiceAnswer(await pageOf(), orderUrl, orderId);
  };
  // One object per order, captured once per run however many of its units
  // are in the batch; each unit row then points at the shared key. Stored
  // through the one store helper every download path uses (invoiceOriginals.ts:
  // Flipkart's files → invoice_downloads → raw PDF → register → units).
  const doneOrders = new Map<string, StoredInvoice>();
  for (const r of batch) {
    if (opts.cancelled()) break;
    const orderId = String(r.order_id);
    try {
      let stamped = doneOrders.get(orderId);
      let fresh = false;
      if (!stamped) {
        const answer = await answerOf(orderId, String(r.order_url || ""));
        stamped = await storeInvoiceDownload({
          userId: opts.userId,
          accountId: opts.platformAccountId,
          orderId,
          answer,
          unitIds: rows.filter((x) => String(x.order_id) === orderId).map((x) => String(x.unit_id)),
          unitRowIds: [r._id],
          path: "fetch",
        });
        doneOrders.set(orderId, stamped);
        fresh = true;
        const productParts = stamped.parts.filter((p) => p.type === "product").length;
        if (!productParts) opts.log("warn", `${orderId} unit ${r.unit_id}: bundle has no product invoice (${stamped.parts.map((p) => p.type).join(", ")}) — the pipeline will park it as needs_review`);
      } else {
        await stampInvoiceUnits([r._id], stamped);
      }
      out.stored += 1;
      const summary = Object.entries(stamped.parts.reduce<Record<string, number>>((acc, p) => ((acc[p.type] = (acc[p.type] || 0) + 1), acc), {}))
        .map(([t, n]) => (n > 1 ? `${n}× ${t}` : t))
        .join(", ");
      opts.log("info", `${orderId} unit ${r.unit_id}: invoice ${fresh ? "stored" : "shares the order's bundle"} (${Math.round(stamped.size / 1024)} KB: ${summary})`);
    } catch (err) {
      out.failed += 1;
      opts.log("warn", `${r.order_id} unit ${r.unit_id}: invoice failed (${err instanceof Error ? err.message.split("\n")[0].slice(0, 160) : String(err)})`);
    }
    await sleep(600);
  }
  return out;
}
