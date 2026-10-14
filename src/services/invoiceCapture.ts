import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import type { Page } from "playwright";
import { HeadObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { config } from "../config.js";
import { Order } from "../models/Order.js";
import { evalOnPage } from "../lib/evalOnPage.js";
import { sleep } from "./browser.js";

/**
 * Invoice capture: open a delivered unit's order page, press Flipkart's
 * "Download Invoice", catch the file, and put it in the private S3 bucket
 * under the same key layout the dashboard signs links for:
 *
 *   invoices/<workspace user id>/<order date YYYY-MM-DD>/<order id>.pdf
 *   (…/<order id>-<unit id>.pdf when one order has several invoices)
 *
 * Runs as a step AFTER enrichment, never inside it, so the fetch invariant
 * (listed − dropped − known + carried = queued = enriched) is untouched.
 * Switched per run from the dashboard: off | new (units enriched in this
 * run) | missing (every delivered unit of the account with no key yet, capped
 * by INVOICE_MAX_PER_RUN). Credentials come from the standard AWS chain.
 */

export type InvoiceMode = "off" | "new" | "missing";

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

export function invoiceKeyFor(userId: string, orderDate: Date | string | null | undefined, orderId: string, unitId = ""): string {
  const d = orderDate ? new Date(orderDate) : null;
  const day = d && !Number.isNaN(d.getTime()) ? d.toISOString().slice(0, 10) : "undated";
  const safe = (s: string) => String(s || "").replace(/[^A-Za-z0-9._-]/g, "_");
  const name = unitId && unitId !== orderId ? `${safe(orderId)}-${safe(unitId)}` : safe(orderId);
  return `invoices/${safe(userId)}/${day}/${name}.pdf`;
}

export async function invoiceExists(key: string): Promise<boolean> {
  try {
    await s3().send(new HeadObjectCommand({ Bucket: config.invoiceBucket, Key: key }));
    return true;
  } catch {
    return false;
  }
}

export async function putInvoice(key: string, body: Buffer): Promise<number> {
  await s3().send(new PutObjectCommand({ Bucket: config.invoiceBucket, Key: key, Body: body, ContentType: "application/pdf" }));
  return body.length;
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

/** Open the order page (session already on the page) and bring back the invoice PDF bytes. */
export async function captureInvoicePdf(page: Page, orderUrl: string): Promise<Buffer> {
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
    if (buf.subarray(0, 4).toString("utf8") === "%PDF") return buf;
    if (buf[0] === 0x50 && buf[1] === 0x4b) {
      // A zip: Flipkart bundles several PDFs for some orders — take the first.
      const dir = path.join(tmp, "x");
      fs.mkdirSync(dir);
      execFileSync(process.platform === "win32" ? "tar" : "unzip", process.platform === "win32" ? ["-xf", raw, "-C", dir] : ["-o", raw, "-d", dir], { stdio: "ignore" });
      const pdf = fs.readdirSync(dir, { recursive: true }).map((n) => path.join(dir, String(n))).find((f) => fs.statSync(f).isFile() && f.toLowerCase().endsWith(".pdf"));
      if (!pdf) throw new Error("Invoice zip had no PDF");
      return fs.readFileSync(pdf);
    }
    throw new Error("Downloaded file was not a PDF or zip");
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
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
  for (const r of batch) {
    if (opts.cancelled()) break;
    const key = invoiceKeyFor(opts.userId, r.order_date, String(r.order_id), String(r.unit_id));
    try {
      let size: number;
      if (await invoiceExists(key)) {
        // Already in the bucket (an earlier run that died before the row was stamped).
        size = 0;
      } else {
        const pdf = await captureInvoicePdf(page, String(r.order_url || ""));
        size = await putInvoice(key, pdf);
      }
      await Order.updateOne({ _id: r._id }, { $set: { invoice_s3_key: key, invoice_downloaded_at: new Date(), invoice_size: size, is_invoice_downloaded: true } });
      out.stored += 1;
      opts.log("info", `${r.order_id} unit ${r.unit_id}: invoice stored${size ? ` (${Math.round(size / 1024)} KB)` : " (already in bucket)"}`);
    } catch (err) {
      out.failed += 1;
      opts.log("warn", `${r.order_id} unit ${r.unit_id}: invoice failed (${err instanceof Error ? err.message.split("\n")[0].slice(0, 160) : String(err)})`);
    }
    await sleep(600);
  }
  return out;
}
