import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import type { Page } from "playwright";
import { createRequire } from "node:module";
import { PDFDocument } from "pdf-lib";
import { HeadObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { config } from "../config.js";
import { Types } from "mongoose";
import { Order } from "../models/Order.js";
import { Invoice, type IInvoicePart } from "../models/Invoice.js";
import { evalOnPage } from "../lib/evalOnPage.js";
import { sleep } from "./browser.js";

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

/** Classify a PDF of the bundle by its text; order decides what comes first in the merged file. */
export async function classifyInvoicePdf(buf: Buffer): Promise<{ type: InvoicePartType; invoiceNumber: string; text: string }> {
  let text = "";
  try {
    text = (await pdfParse(buf)).text.replace(/\s+/g, " ");
  } catch {
    return { type: "other", invoiceNumber: "", text: "" };
  }
  const invoiceNumber = (text.match(/Invoice Number\s*#?\s*:?\s*([A-Z0-9]{8,})/i) || [])[1] || (text.match(/Tax Invoice Number\s*:\s*([A-Z0-9]{8,})/i) || [])[1] || "";
  let type: InvoicePartType = "other";
  if (/GT Charges|GOODS TRANSPORTED BY GTA|Goods Carriage/i.test(text)) type = "transport";
  else if (/Protect Promise Fee/i.test(text)) type = "protect_promise";
  else if (/Corporate Card Fee|Offer Handling Fee|Flipkart Internet Private Limited/i.test(text) && !/FSN:/i.test(text)) type = "flipkart_fee";
  else if (/FSN:|HSN\/SAC:\s*\d{4,}|Product\s*Title|Total items:/i.test(text)) type = "product";
  return { type, invoiceNumber, text };
}

const PART_ORDER: Record<InvoicePartType, number> = { product: 0, protect_promise: 1, flipkart_fee: 2, transport: 3, other: 4 };

/** Merge the bundle into one PDF, product invoice first, and describe the parts. */
export async function mergeInvoiceBundle(pdfs: Buffer[]): Promise<{ pdf: Buffer; parts: InvoicePart[] }> {
  const classified = await Promise.all(pdfs.map(async (b) => ({ buf: b, ...(await classifyInvoicePdf(b)) })));
  classified.sort((a, b) => PART_ORDER[a.type] - PART_ORDER[b.type]);
  if (classified.length === 1) {
    const only = classified[0];
    let pages = 1;
    try {
      pages = (await PDFDocument.load(only.buf, { ignoreEncryption: true })).getPageCount();
    } catch {
      /* keep 1 */
    }
    return { pdf: only.buf, parts: [{ type: only.type, pages, invoiceNumber: only.invoiceNumber, bytes: only.buf.length }] };
  }
  const merged = await PDFDocument.create();
  const parts: InvoicePart[] = [];
  for (const c of classified) {
    try {
      const src = await PDFDocument.load(c.buf, { ignoreEncryption: true });
      const pages = await merged.copyPages(src, src.getPageIndices());
      for (const p of pages) merged.addPage(p);
      parts.push({ type: c.type, pages: pages.length, invoiceNumber: c.invoiceNumber, bytes: c.buf.length });
    } catch {
      parts.push({ type: c.type, pages: 0, invoiceNumber: c.invoiceNumber, bytes: c.buf.length });
    }
  }
  return { pdf: Buffer.from(await merged.save()), parts };
}

export function parseInvoiceMode(v: unknown): InvoiceMode {
  const s = String(v || "").toLowerCase();
  return s === "new" || s === "missing" ? s : "off";
}

export function invoiceStoreEnabled(): boolean {
  return Boolean(config.invoiceBucket);
}

let client: S3Client | null = null;
function s3(): S3Client {
  if (!config.invoiceBucket) throw new Error("INVOICE_S3_BUCKET is not set");
  if (!client) client = new S3Client({ region: config.awsRegion });
  return client;
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

/** Upload the bundle tagged `status=pending`; the ETag identifies these exact bytes to the pipeline. */
export async function putInvoice(key: string, body: Buffer): Promise<{ size: number; etag: string }> {
  const res = await s3().send(
    new PutObjectCommand({ Bucket: config.invoiceBucket, Key: key, Body: body, ContentType: "application/pdf", Tagging: "status=pending" })
  );
  return { size: body.length, etag: String(res.ETag || "").replace(/"/g, "") };
}

/**
 * Hand the object to the extraction pipeline: one `invoices` row per bundle,
 * created `pending` when new. An existing row keeps its status — the Lambda's
 * claim notices changed bytes by etag on its own — so a re-capture never
 * regresses a processed invoice; and if the S3 event beat us here, the
 * Lambda's own upsert already made the row and this only fills in what the
 * worker knows (parts, units, account).
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
}): Promise<void> {
  const now = new Date();
  const account = input.platformAccountId && Types.ObjectId.isValid(String(input.platformAccountId)) ? new Types.ObjectId(String(input.platformAccountId)) : null;
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
      },
      $set: { etag: input.etag, size: input.size, parts: input.parts, updatedAt: now, ...(account ? { platformAccountId: account } : {}) },
      $addToSet: { unit_ids: { $each: input.unitIds } },
    },
    { upsert: true, timestamps: false }
  );
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
 * Open the order page (session already on the page) and bring back every PDF
 * Flipkart hands out for the unit: a single PDF, or a zip bundling the product
 * invoice with the fee / transport invoices (one order = several tax invoices).
 */
export async function captureInvoicePdfs(page: Page, orderUrl: string): Promise<Buffer[]> {
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
    const buf = fs.readFileSync(raw);
    if (buf.subarray(0, 4).toString("utf8") === "%PDF") return [buf];
    if (buf[0] === 0x50 && buf[1] === 0x4b) {
      const dir = path.join(tmp, "x");
      fs.mkdirSync(dir);
      execFileSync(process.platform === "win32" ? "tar" : "unzip", process.platform === "win32" ? ["-xf", raw, "-C", dir] : ["-o", raw, "-d", dir], { stdio: "ignore" });
      const pdfs = fs
        .readdirSync(dir, { recursive: true })
        .map((n) => path.join(dir, String(n)))
        .filter((f) => fs.statSync(f).isFile() && f.toLowerCase().endsWith(".pdf"))
        .sort()
        .map((f) => fs.readFileSync(f));
      if (!pdfs.length) throw new Error("Invoice zip had no PDF");
      return pdfs;
    }
    throw new Error("Downloaded file was not a PDF or zip");
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
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
export async function captureInvoicesForAccount(opts: {
  userId: string;
  platformAccountId: unknown;
  email: string;
  mode: InvoiceMode;
  justEnriched: Set<string>;
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
  const page = await opts.getPage();
  // One object per order, captured once per run however many of its units
  // are in the batch; each unit row then points at the shared key.
  const doneOrders = new Map<string, { key: string; size: number; parts: InvoicePart[] }>();
  for (const r of batch) {
    if (opts.cancelled()) break;
    const orderId = String(r.order_id);
    try {
      let stamped = doneOrders.get(orderId);
      let fresh = false;
      if (!stamped) {
        const pdfs = await captureInvoicePdfs(page, String(r.order_url || ""));
        const bundle = await mergeInvoiceBundle(pdfs);
        const productParts = bundle.parts.filter((p) => p.type === "product").length;
        const key = rawInvoiceKey(opts.userId, orderId);
        const put = await putInvoice(key, bundle.pdf);
        stamped = { key, size: put.size, parts: bundle.parts };
        doneOrders.set(orderId, stamped);
        fresh = true;
        await registerInvoice({
          userId: opts.userId,
          platformAccountId: opts.platformAccountId,
          orderId,
          key,
          etag: put.etag,
          size: put.size,
          parts: bundle.parts,
          unitIds: rows.filter((x) => String(x.order_id) === orderId).map((x) => String(x.unit_id)),
        });
        if (!productParts) opts.log("warn", `${orderId} unit ${r.unit_id}: bundle has no product invoice (${bundle.parts.map((p) => p.type).join(", ")}) — the pipeline will park it as needs_review`);
      }
      await Order.updateOne(
        { _id: r._id },
        { $set: { invoice_s3_key: stamped.key, invoice_downloaded_at: new Date(), invoice_size: stamped.size, is_invoice_downloaded: true, invoice_parts: stamped.parts } }
      );
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
