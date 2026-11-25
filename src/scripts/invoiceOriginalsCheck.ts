/**
 * Flipkart's files kept at download — data-quality sub-step 12 (SO-1, T1, T2, P9, X3, T4's part labels) — checked
 * WITHOUT Flipkart and WITHOUT S3:
 *
 *   · a fake order-API context answers the invoice URL with a PK zip built (fflate) from the dashboard's offline
 *     fixtures (infra/test-fixtures: the synthetic smoke product invoice and the two freight fixtures, read in place,
 *     plus three copies of the smoke invoice resized by an incremental update); an in-memory S3 takes every PUT / HEAD (it enforces IfNoneMatch and the SHA-256
 *     checksum the way S3 does). INVOICE_S3_BUCKET is set to a made-up name before anything is loaded, so a call
 *     that ever reached the real client would only hit a bucket that does not exist.
 *   · Mongo is real, rows are invented: a made-up workspace id (TRADE_FLOW_USER_ID), @verify.invalid e-mails, order
 *     ids ODVERIFY…; every invoices, invoice_downloads and order_details row of that workspace is deleted at the end
 *     and the leftovers are counted (must be 0). Connected with autoIndex / autoCreate off.
 *
 * Expect (the plan's Tests paragraph): one orig/ object per file whose sha256 equals the fixture's; a second run
 * stores 0 new objects; `download-id` on the raw PUT; each file's page_from adds up to the merged page count; numeric
 * merge order; captured_at / settled set by the delivery rule; skipIfUnchanged makes no raw PUT. Plus:
 *
 *   T1 proof — against the DASHBOARD's own claim rule (backend/src/services/invoicePipeline.ts claim(), read from that
 *   repo's file, read-only): a re-download of a processed bundle leaves the row's etag as the Lambda wrote it, so the
 *   S3 event's new etag differs → the row is claimed → re-extracted; and the old $set would have hidden it.
 *
 *   npm run test:invoice-originals
 *     DASHBOARD_REPO=<path>          the dashboard checkout (default ../../data_for_project/gmail-id-dashboard_mongodb)
 *     CLAIM_RULE_FILES=<a.ts,b.ts>   more invoicePipeline.ts files whose claim rule is checked too (e.g. a worktree)
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { zipSync } from "fflate";
import mongoose, { Types } from "mongoose";
import { PDFDocument, StandardFonts } from "pdf-lib";
import type { APIRequestContext } from "playwright";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const dashboardRepo = path.resolve(process.env.DASHBOARD_REPO || path.join(repoRoot, "../../data_for_project/gmail-id-dashboard_mongodb"));
const FIXTURES = path.join(dashboardRepo, "infra/test-fixtures");

let passed = 0;
const failures: string[] = [];
async function check(name: string, fn: () => void | Promise<void>) {
  try {
    await fn();
    passed++;
    console.log(`  ok    ${name}`);
  } catch (err) {
    failures.push(name);
    console.log(`  FAIL  ${name}\n        ${err instanceof Error ? err.message.split("\n").slice(0, 40).join("\n        ") : err}`);
  }
}

const sha = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");
const md5 = (b: Uint8Array) => createHash("md5").update(b).digest("hex");
const hoursAgo = (h: number) => new Date(Date.now() - h * 3_600_000);

// ── the in-memory S3 ─────────────────────────────────────────────────────────

type Stored = { body: Buffer; etag: string; metadata: Record<string, string>; tagging: string };
interface PutCall {
  key: string;
  ifNoneMatch?: string;
  checksum?: string;
  metadata?: Record<string, string>;
  outcome: "stored" | "412" | "refused";
}

class MemoryS3 {
  objects = new Map<string, Stored>();
  puts: PutCall[] = [];
  heads: string[] = [];
  /** Answer every conditional PUT with 501 NotImplemented (a store that refuses the condition). */
  refuseConditions = false;
  /** Fail every PUT of an orig/ key: "403" = AccessDenied (no orig/* grant — before D1), "503" = a storage outage. */
  failOriginals: false | "403" | "503" = false;
  /** Fail every PUT of a raw/ key with this network error (our storage, not Flipkart). */
  failRaw = false;
  constructor(readonly bucket: string) {}
  private err(name: string, status: number, message: string) {
    return Object.assign(new Error(message), { name, $metadata: { httpStatusCode: status } });
  }
  async send(cmd: { constructor: { name: string }; input: Record<string, unknown> }): Promise<unknown> {
    const input = cmd.input;
    assert.equal(input.Bucket, this.bucket, "a call went to a bucket other than the in-memory one");
    const key = String(input.Key);
    if (cmd.constructor.name === "PutObjectCommand") {
      const body = Buffer.from(input.Body as Uint8Array);
      const call: PutCall = { key, ifNoneMatch: input.IfNoneMatch as string | undefined, checksum: input.ChecksumSHA256 as string | undefined, metadata: input.Metadata as Record<string, string> | undefined, outcome: "stored" };
      this.puts.push(call);
      if (this.failOriginals && key.startsWith("orig/")) {
        call.outcome = "refused";
        if (this.failOriginals === "403") throw this.err("AccessDenied", 403, "User is not authorized to perform: s3:PutObject on resource orig/* (verify)");
        throw this.err("ServiceUnavailable", 503, "Please reduce your request rate.");
      }
      if (this.failRaw && key.startsWith("raw/")) {
        call.outcome = "refused";
        throw Object.assign(new Error("write ECONNRESET"), { name: "Error", code: "ECONNRESET" });
      }
      if (input.IfNoneMatch !== undefined) {
        if (this.refuseConditions) {
          call.outcome = "refused";
          throw this.err("NotImplemented", 501, "A header you provided implies functionality that is not implemented");
        }
        if (input.IfNoneMatch === "*" && this.objects.has(key)) {
          call.outcome = "412";
          throw this.err("PreconditionFailed", 412, "At least one of the pre-conditions you specified did not hold");
        }
      }
      if (input.ChecksumSHA256 !== undefined && input.ChecksumSHA256 !== createHash("sha256").update(body).digest("base64")) throw this.err("BadDigest", 400, "The SHA256 you specified did not match the calculated checksum.");
      const etag = md5(body);
      this.objects.set(key, { body, etag, metadata: { ...((input.Metadata as Record<string, string>) || {}) }, tagging: String(input.Tagging || "") });
      return { ETag: `"${etag}"` };
    }
    if (cmd.constructor.name === "HeadObjectCommand") {
      this.heads.push(key);
      const o = this.objects.get(key);
      if (!o) throw this.err("NotFound", 404, "NotFound");
      return { ETag: `"${o.etag}"`, ContentLength: o.body.length, Metadata: o.metadata };
    }
    throw new Error(`the in-memory S3 does not answer ${cmd.constructor.name}`);
  }
  putsSince(n: number) {
    return this.puts.slice(n);
  }
}

// ── fixtures ─────────────────────────────────────────────────────────────────

/** An invented document: `pages` pages of size width × height, one line of text each (no real data). */
async function inventedPdf(label: string, pages: number, width: number, height: number): Promise<Buffer> {
  const doc = await PDFDocument.create();
  const font = await doc.embedFont(StandardFonts.Helvetica);
  for (let i = 0; i < pages; i++) doc.addPage([width, height]).drawText(`${label} page ${i + 1} (invented)`, { x: 30, y: height - 40, size: 10, font });
  return Buffer.from(await doc.save({ useObjectStreams: false }));
}

/**
 * The fixture with its page resized by an incremental update (a new revision of the page object with its own
 * MediaBox, an xref section and a trailer with /Prev): a different file, readable by pdf.js and pdf-lib alike.
 */
function resizedPdf(src: Buffer, width: number, height: number, label: string): Buffer {
  const text = src.toString("latin1");
  const page = text.match(/(\d+) 0 obj\s*(<<\s*\/Type \/Page \/Parent[^>]*?)\s*>>/);
  const prev = text.match(/startxref\s+(\d+)\s+%%EOF\s*$/);
  const trailer = text.match(/trailer\s*<<([\s\S]*?)>>\s*startxref/);
  const size = trailer?.[1].match(/\/Size (\d+)/)?.[1];
  const root = trailer?.[1].match(/\/Root (\d+ \d+ R)/)?.[1];
  if (!page || !prev || !size || !root) throw new Error("the smoke fixture's layout is not the one this check knows — regenerate per its README or adjust resizedPdf");
  let out = text.endsWith("\n") ? text : `${text}\n`;
  const objAt = Buffer.byteLength(out, "latin1");
  out += `${page[1]} 0 obj\n${page[2]} /MediaBox [0 0 ${width} ${height}] >>\nendobj\n% ${label}\n`;
  const xrefAt = Buffer.byteLength(out, "latin1");
  // Its own /ID: pdf.js takes a document's fingerprint from it, and copies of one file must not share one.
  const id = createHash("md5").update(label).digest("hex");
  out += `xref\n0 1\n0000000000 65535 f \n${page[1]} 1\n${String(objAt).padStart(10, "0")} 00000 n \ntrailer\n<< /Size ${size} /Root ${root} /Prev ${prev[1]} /ID [<${id}> <${id}>] >>\nstartxref\n${xrefAt}\n%%EOF\n`;
  return Buffer.from(out, "latin1");
}

async function pageSizes(pdf: Buffer): Promise<string[]> {
  const doc = await PDFDocument.load(pdf, { ignoreEncryption: true });
  return doc.getPages().map((p) => `${Math.round(p.getWidth())}x${Math.round(p.getHeight())}`);
}

/** A fake order-API context: GET …/invoice/download/<order> answers `bodies[order]` (200) or 404. Nothing leaves the process. */
function fakeApi(bodies: Map<string, Buffer>, hits: string[]): APIRequestContext {
  const ctx = {
    async get(url: string) {
      hits.push(url);
      const orderId = decodeURIComponent(url.split("/").pop() || "");
      const body = bodies.get(orderId);
      return {
        ok: () => Boolean(body),
        status: () => (body ? 200 : 404),
        text: async () => (body ? "" : "not found"),
        body: async () => body || Buffer.alloc(0),
        headers: () => ({ "content-type": "application/octet-stream" }),
      };
    },
  };
  return ctx as unknown as APIRequestContext;
}

/** The dashboard's claim filter, read from its invoicePipeline.ts (read-only) and evaluated as the Lambda builds it. */
function claimFilterFrom(file: string): (key: string, etag: string, stale: Date) => Record<string, unknown> {
  const src = fs.readFileSync(file, "utf8");
  const at = src.indexOf("async function claim(");
  if (at < 0) throw new Error(`${file}: no claim() found`);
  const f = src.indexOf("const filter = {", at);
  if (f < 0) throw new Error(`${file}: claim() has no "const filter = {"`);
  const open = src.indexOf("{", f);
  let depth = 0;
  let end = -1;
  for (let i = open; i < src.length; i++) {
    if (src[i] === "{") depth++;
    else if (src[i] === "}" && --depth === 0) {
      end = i;
      break;
    }
  }
  const literal = src.slice(open, end + 1);
  if (!/etag/.test(literal) || !/processed/.test(literal)) throw new Error(`${file}: the claim filter no longer mentions etag / processed — re-read the rule`);
  return new Function("key", "etag", "stale", `"use strict"; return (${literal});`) as (key: string, etag: string, stale: Date) => Record<string, unknown>;
}

// ── the checks ───────────────────────────────────────────────────────────────

async function main() {
  // A workspace and a bucket that do not exist: set before config is read.
  const workspace = new Types.ObjectId();
  const userId = String(workspace);
  process.env.TRADE_FLOW_USER_ID = userId;
  const BUCKET = `verify-in-memory-${Date.now().toString(36)}`;
  process.env.INVOICE_S3_BUCKET = BUCKET;
  process.env.INVOICE_SETTLE_HOURS = "48";

  const { config } = await import("../config.js");
  assert.equal(config.invoiceBucket, BUCKET);
  delete process.env.INVOICE_RECAPTURE_GIVE_UP_DAYS; // after dotenv: the check runs on the default (30 days)
  const { Invoice } = await import("../models/Invoice.js");
  const { InvoiceDownload } = await import("../models/InvoiceDownload.js");
  const { Order } = await import("../models/Order.js");
  const capture = await import("../services/invoiceCapture.js");
  const originals = await import("../services/invoiceOriginals.js");
  const onDemand = await import("../services/invoiceOnDemand.js");
  const { classifyPlatformError } = await import("../services/requestCommon.js");
  const sweep = await import("../services/invoiceSweep.js");

  const s3 = new MemoryS3(BUCKET);
  capture.useInvoiceS3ForChecks(s3 as unknown as import("../services/invoiceCapture.js").InvoiceS3);

  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false, serverSelectionTimeoutMS: 15_000 });
  const account = new Types.ObjectId();
  const tag = Date.now().toString().slice(-8);
  const orderA = `ODVERIFY${tag}01`;
  const orderB = `ODVERIFY${tag}02`;
  const orderC = `ODVERIFY${tag}03`;
  const orderD = `ODVERIFY${tag}04`;
  const orderF = `ODVERIFY${tag}05`;
  const orderG = `ODVERIFY${tag}06`;
  let unitSeq = 0;
  const insertUnits = async (orderId: string, units: Array<Record<string, unknown>>) => {
    const docs = units.map((u) => ({
      userId: workspace,
      platformAccountId: account,
      platform_email: "s12@verify.invalid",
      id: 990_000_000 + ++unitSeq,
      order_id: orderId,
      unit_id: `${orderId}-U${unitSeq}`,
      item_id: `ITEM${unitSeq}`,
      invoice_s3_key: "",
      invoice_parts: [],
      is_invoice_downloaded: false,
      ...u,
    }));
    const res = await Order.collection.insertMany(docs as never[]);
    return docs.map((d, i) => ({ _id: res.insertedIds[i], unit_id: d.unit_id }));
  };

  const haveFixtures = ["smoke-invoice.pdf", "gt-old.pdf", "gt-new-nov2026.pdf"].every((f) => fs.existsSync(path.join(FIXTURES, f)));
  if (!haveFixtures) console.log(`  note  the dashboard's fixtures are not at ${FIXTURES} — only invented PDFs are used (set DASHBOARD_REPO)`);
  const product = haveFixtures ? fs.readFileSync(path.join(FIXTURES, "smoke-invoice.pdf")) : await inventedPdf("product", 1, 595, 842);
  const gtOld = haveFixtures ? fs.readFileSync(path.join(FIXTURES, "gt-old.pdf")) : await inventedPdf("gt-old", 1, 596, 842);
  const gtNew = haveFixtures ? fs.readFileSync(path.join(FIXTURES, "gt-new-nov2026.pdf")) : await inventedPdf("gt-new", 1, 597, 842);
  // Three more product documents, each with its own page size so the merged page order can be read back: the smoke
  // fixture with an incremental update that resizes its page (pdf.js 1.10 inside pdf-parse reads these; it cannot
  // read pdf-lib's output, and a failed read can make the next few reads fail too — so pdf-lib documents are used
  // only when the fixtures are missing).
  const otherA = haveFixtures ? resizedPdf(product, 401, 601, "verify A") : await inventedPdf("other A", 2, 401, 601);
  const otherB = haveFixtures ? resizedPdf(product, 402, 602, "verify B") : await inventedPdf("other B", 1, 402, 602);
  const otherC = haveFixtures ? resizedPdf(product, 403, 603, "verify C") : await inventedPdf("other C", 1, 403, 603);
  // Flipkart's names; a text sort would read file_1, file_11, file_12, file_2, file_32, file_7.
  const answerFiles: Record<string, Buffer> = { "file_1.pdf": gtOld, "file_2.pdf": otherA, "file_7.pdf": product, "file_11.pdf": gtNew, "file_12.pdf": otherB, "file_32.pdf": otherC };
  const zip = Buffer.from(zipSync(Object.fromEntries(Object.entries(answerFiles).map(([n, b]) => [n, new Uint8Array(b)])), { level: 0 }));
  /** The same answer plus one late document (its own bytes per call). */
  let lateSeq = 0;
  const zipWithLate = async () => {
    const label = `verify late ${tag} ${++lateSeq}`;
    const late = haveFixtures ? resizedPdf(product, 404, 604, label) : await inventedPdf(label, 1, 404, 604);
    return Buffer.from(zipSync(Object.fromEntries([...Object.entries(answerFiles), ["file_40.pdf", late] as const].map(([n, b]) => [n, new Uint8Array(b)])), { level: 0 }));
  };
  const bodies = new Map<string, Buffer>([
    [orderA, zip],
    [orderB, zip],
    [orderC, product],
    [orderD, zip],
  ]);
  const hits: string[] = [];
  const api = fakeApi(bodies, hits);
  const fixtureSha = new Map(Object.entries(answerFiles).map(([n, b]) => [n, sha(b)]));

  try {
    // ── pure pieces ──
    await check("P9: Flipkart's names sorted numerically — file_1…file_36 in number order; the product block 7, 11, 12, 17, 19, 22, 32 keeps that order", () => {
      const names = Array.from({ length: 36 }, (_, i) => `file_${i + 1}.pdf`);
      const shuffled = [...names].sort(() => Math.random() - 0.5);
      const sorted = capture.sortInvoiceFileNames(shuffled);
      assert.deepEqual(sorted, names);
      const productBlock = [7, 11, 12, 17, 19, 22, 32].map((n) => `file_${n}.pdf`);
      assert.deepEqual(sorted.filter((n) => productBlock.includes(n)), productBlock);
      assert.deepEqual([...names].sort().slice(0, 4), ["file_1.pdf", "file_10.pdf", "file_11.pdf", "file_12.pdf"], "the old text sort, for the record");
    });

    await check("T4: the goods invoice layout (\"HSN: <code>\", no FSN) and the grocery layout are labelled product; a service code (99…) is not; fee and freight rules still come first", () => {
      assert.equal(capture.classifyInvoiceText("Tax Invoice Invoice Number # FAVERIFY000001 Item | HSN: 85176290 | IGST: 18%").type, "product");
      assert.equal(capture.classifyInvoiceText("Tax Invoice/Bill of Supply HSN (Tax%) 1 Item 1904").type, "product");
      assert.equal(capture.classifyInvoiceText("Tax Invoice HSN: 998599 service").type, "other");
      assert.equal(capture.classifyInvoiceText("Tax Invoice HSN: 85176290 GT Charges996511").type, "transport");
      assert.equal(capture.classifyInvoiceText("Tax Invoice Protect Promise Fee HSN: 85176290").type, "protect_promise");
      assert.equal(capture.classifyInvoiceText("Tax Invoice FSN: ABC HSN/SAC: 85171300").type, "product");
    });

    await check("T2 rule (settledByDelivery): all finished and the last delivery older than INVOICE_SETTLE_HOURS → settled; a unit on its way, a recent delivery, or a delivered unit without its time → not", () => {
      const now = new Date();
      const r = (units: Array<Record<string, unknown>>) => capture.settledByDelivery(units as never, now, 48 * 3_600_000);
      assert.equal(r([{ status_key: "Delivered", actual_delivery_date: hoursAgo(72) }, { status_key: "Delivered", actual_delivery_date: hoursAgo(60) }]), true);
      assert.equal(r([{ status_key: "Delivered", actual_delivery_date: hoursAgo(72) }, { status_key: "Shipped" }]), false);
      assert.equal(r([{ status_key: "Delivered", actual_delivery_date: hoursAgo(72) }, { status_key: "Delivered", actual_delivery_date: hoursAgo(5) }]), false);
      assert.equal(r([{ status_key: "Delivered", actual_delivery_date: null }]), false);
      assert.equal(r([{ status_key: "Delivered", actual_delivery_date: hoursAgo(72) }, { status_key: "Cancelled", cancelled_date: hoursAgo(80) }, { status_key: "Failed" }]), true);
      assert.equal(r([{ status_key: "Cancelled", cancelled_date: hoursAgo(10) }]), false);
      assert.equal(r([]), false);
    });

    // ── the API download ──
    let answerA: Awaited<ReturnType<typeof capture.downloadInvoiceAnswerViaApi>>;
    await check("the API download keeps Flipkart's answer: kind zip, its sha256, every PDF under its own name in numeric order, bytes untouched", async () => {
      answerA = await capture.downloadInvoiceAnswerViaApi(api, orderA);
      assert.equal(answerA.kind, "zip");
      assert.equal(answerA.via, "api");
      assert.equal(answerA.sha256, sha(zip));
      assert.deepEqual(answerA.files.map((f) => f.name), ["file_1.pdf", "file_2.pdf", "file_7.pdf", "file_11.pdf", "file_12.pdf", "file_32.pdf"]);
      for (const f of answerA.files) assert.equal(sha(f.bytes), fixtureSha.get(f.name), `${f.name} changed`);
      assert.ok(hits.every((u) => u.includes(".rome.api.flipkart.com/") && u.endsWith(`/invoice/download/${orderA}`)), "the fake answered the usual URL");
      const single = await capture.downloadInvoiceAnswerViaApi(api, orderC);
      assert.equal(single.kind, "pdf");
      assert.deepEqual(single.files.map((f) => f.name), [`${orderC}.pdf`]);
    });

    // ── first store (a batch download of a new order) ──
    const unitsA = await insertUnits(orderA, [
      { status_key: "Delivered", actual_delivery_date: hoursAgo(96) },
      { status_key: "Delivered", actual_delivery_date: hoursAgo(80) },
    ]);
    const targetA = { userId, accountId: String(account), email: "s12@verify.invalid", orderId: orderA, unitIds: unitsA.map((u) => u.unit_id), rowIds: unitsA.map((u) => u._id) };
    const before1 = s3.puts.length;
    let first: Awaited<ReturnType<typeof onDemand.downloadAndStoreInvoice>>;
    await check("store #1 (batch path): one orig/ object per file at orig/<user>/<order>/<sha256>.pdf, IfNoneMatch * with the SHA-256 checksum, sha256 = the fixture's", async () => {
      first = await onDemand.downloadAndStoreInvoice(api, targetA, { path: "batch", requestId: "verify-request" });
      const origPuts = s3.putsSince(before1).filter((p) => p.key.startsWith("orig/"));
      assert.equal(origPuts.length, 6);
      assert.ok(origPuts.every((p) => p.ifNoneMatch === "*" && p.checksum && p.outcome === "stored"));
      for (const [name, digest] of fixtureSha) {
        const key = originals.originalKey(userId, orderA, digest);
        assert.equal(key, `orig/${userId}/${orderA}/${digest}.pdf`);
        const o = s3.objects.get(key);
        assert.ok(o, `${name} not stored`);
        assert.equal(sha(o.body), digest);
      }
    });

    await check("store #1: the raw PUT carries download-id = the invoice_downloads document, which records every file (name, key, sha256, pages, type, number, merge_index, page_from, stored:new)", async () => {
      const rawPuts = s3.putsSince(before1).filter((p) => p.key.startsWith("raw/"));
      assert.equal(rawPuts.length, 1);
      assert.equal(rawPuts[0].key, capture.rawInvoiceKey(userId, orderA));
      assert.equal(rawPuts[0].metadata?.["download-id"], first.downloadId);
      assert.equal(s3.objects.get(rawPuts[0].key)?.tagging, "status=pending");
      const doc = await InvoiceDownload.collection.findOne({ _id: first.downloadId } as never);
      assert.ok(doc, "no invoice_downloads document");
      assert.equal(String(doc.userId), userId);
      assert.equal(doc.order_id, orderA);
      assert.equal(doc.path, "batch");
      assert.equal(doc.request_id, "verify-request");
      assert.equal(doc.via, "api");
      assert.equal(doc.unchanged, false);
      assert.deepEqual(doc.answer, { kind: "zip", bytes: zip.length, sha256: sha(zip) });
      assert.deepEqual(
        doc.files.map((f: { name: string }) => f.name),
        ["file_1.pdf", "file_2.pdf", "file_7.pdf", "file_11.pdf", "file_12.pdf", "file_32.pdf"]
      );
      for (const f of doc.files) {
        assert.equal(f.sha256, fixtureSha.get(f.name));
        assert.equal(f.key, originals.originalKey(userId, orderA, f.sha256));
        assert.equal(f.bytes, answerFiles[f.name].length);
        assert.equal(f.stored, "new");
      }
      if (haveFixtures) {
        const types = Object.fromEntries(doc.files.map((f: { name: string; type: string }) => [f.name, f.type]));
        assert.deepEqual(types, { "file_1.pdf": "transport", "file_2.pdf": "product", "file_7.pdf": "product", "file_11.pdf": "transport", "file_12.pdf": "product", "file_32.pdf": "product" });
      }
      const raw = s3.objects.get(capture.rawInvoiceKey(userId, orderA))!;
      assert.equal(doc.merged.key, capture.rawInvoiceKey(userId, orderA));
      assert.equal(doc.merged.bytes, raw.body.length);
      assert.equal(doc.merged.etag, raw.etag);
    });

    await check("store #1: page_from tiles the merged PDF — the files' pages run 1…N with no gap, N = the raw PDF's page count; merge order = product, freight, other, each block in Flipkart's numeric order", async () => {
      const doc = await InvoiceDownload.collection.findOne({ _id: first.downloadId } as never);
      const raw = s3.objects.get(capture.rawInvoiceKey(userId, orderA))!.body;
      const total = (await PDFDocument.load(raw)).getPageCount();
      const inMerge = [...doc!.files].filter((f: { pages: number }) => f.pages > 0).sort((a: { page_from: number }, b: { page_from: number }) => a.page_from - b.page_from);
      let next = 1;
      for (const [i, f] of inMerge.entries()) {
        assert.equal(f.page_from, next, `${f.name} starts at ${f.page_from}, expected ${next}`);
        assert.equal(f.merge_index, i);
        next += f.pages;
      }
      assert.equal(next - 1, total, "the files' pages add up to the merged page count");
      assert.equal(doc!.merged.pages, total);
      const order = inMerge.map((f: { name: string }) => f.name);
      // product block in Flipkart's numeric order (a text sort would give 12, 2, 32, 7), then freight 1, 11.
      if (haveFixtures) assert.deepEqual(order, ["file_2.pdf", "file_7.pdf", "file_12.pdf", "file_32.pdf", "file_1.pdf", "file_11.pdf"]);
      else assert.deepEqual(order, ["file_1.pdf", "file_2.pdf", "file_7.pdf", "file_11.pdf", "file_12.pdf", "file_32.pdf"]);
      // The merged pages ARE those files' pages, in that order (each invented file has its own page size).
      const expected: string[] = [];
      for (const name of order) expected.push(...(await pageSizes(answerFiles[name])));
      assert.deepEqual(await pageSizes(raw), expected);
      // And it is exactly what mergeInvoiceBundle makes of the files in that (numeric) order.
      const again = await capture.mergeInvoiceBundle(answerA.files.map((f) => f.bytes));
      assert.deepEqual(await pageSizes(again.pdf), expected);
      assert.deepEqual(again.parts.map((p) => p.type), first.parts);
    });

    await check("store #1 → registerInvoice: a NEW row gets etag/size/parts on insert, stored_download_id, download_count 1, captured_at now and settled = true (delivered 80 h ago, all finished)", async () => {
      const row = await Invoice.findOne({ s3_key: capture.rawInvoiceKey(userId, orderA) }).lean();
      assert.ok(row);
      const raw = s3.objects.get(capture.rawInvoiceKey(userId, orderA))!;
      assert.equal(row.processing_status, "pending");
      assert.equal(row.etag, raw.etag);
      assert.equal(row.size, raw.body.length);
      assert.equal(row.parts.length, 6);
      assert.equal(row.stored_download_id, first.downloadId);
      assert.equal(row.download_count, 1);
      assert.ok(row.last_download_at && row.captured_at);
      assert.equal(row.settled, true);
      assert.deepEqual([...row.unit_ids].sort(), targetA.unitIds.slice().sort());
      assert.equal(String(row.platformAccountId), String(account));
    });

    await check("store #1 → units: key, flag, size and (empty before) the worker's part list on both units", async () => {
      const rows = await Order.find({ _id: { $in: targetA.rowIds } }).lean();
      assert.equal(rows.length, 2);
      for (const u of rows) {
        assert.equal(u.invoice_s3_key, capture.rawInvoiceKey(userId, orderA));
        assert.equal(u.is_invoice_downloaded, true);
        assert.equal(u.invoice_parts.length, 6);
      }
    });

    // ── second store: nothing new under orig/ ──
    const before2 = s3.puts.length;
    let second: Awaited<ReturnType<typeof onDemand.downloadAndStoreInvoice>>;
    await check("store #2 of the same answer stores 0 new originals (6 × 412 'existing'); the record says stored:existing; a batch still re-uploads the raw PDF (it means 'extract again')", async () => {
      const objectsBefore = [...s3.objects.keys()].filter((k) => k.startsWith("orig/")).length;
      second = await onDemand.downloadAndStoreInvoice(api, targetA, { path: "batch" });
      const objectsAfter = [...s3.objects.keys()].filter((k) => k.startsWith("orig/")).length;
      assert.equal(objectsAfter - objectsBefore, 0);
      const origPuts = s3.putsSince(before2).filter((p) => p.key.startsWith("orig/"));
      assert.equal(origPuts.length, 6);
      assert.ok(origPuts.every((p) => p.outcome === "412"));
      assert.deepEqual(second.originals, { new: 0, existing: 6, failed: 0 });
      const doc = await InvoiceDownload.collection.findOne({ _id: second.downloadId } as never);
      assert.ok(doc!.files.every((f: { stored: string }) => f.stored === "existing"));
      assert.equal(s3.putsSince(before2).filter((p) => p.key.startsWith("raw/")).length, 1);
      assert.equal(s3.objects.get(capture.rawInvoiceKey(userId, orderA))!.metadata["download-id"], second.downloadId);
    });

    // ── T1 proof against the dashboard's claim rule ──
    const claimFiles = [path.join(dashboardRepo, "backend/src/services/invoicePipeline.ts"), ...String(process.env.CLAIM_RULE_FILES || "").split(",").map((s) => s.trim()).filter(Boolean)];
    for (const claimFile of claimFiles) {
      const where = path.relative(path.resolve(repoRoot, ".."), claimFile);
      await check(`T1 proof (${where}): a re-download of a PROCESSED bundle keeps the Lambda's etag, size and parts → the S3 event's new etag differs → claimed (re-extracted); a duplicate event of the old bytes is not; the old $set would have hidden it`, async () => {
        const filterOf = claimFilterFrom(claimFile);
        const key = capture.rawInvoiceKey(userId, orderA);
        // The Lambda processed store #2: its claim wrote the etag it read, describeParts its own per-document list.
        const lambdaEtag = s3.objects.get(key)!.etag;
        const lambdaParts = [
          { type: "product", pages: 1, invoiceNumber: "VERIFYP1", bytes: 111 },
          { type: "transport", pages: 1, invoiceNumber: "VERIFYT1", bytes: 222 },
        ];
        await Invoice.updateOne(
          { s3_key: key },
          { $set: { processing_status: "processed", processed_at: new Date(), etag: lambdaEtag, size: 4242, parts: lambdaParts, processing_started_at: new Date() } },
          { timestamps: false }
        );
        const countBefore = (await Invoice.findOne({ s3_key: key }).lean())!.download_count;
        const stale = new Date(Date.now() - 10 * 60_000);
        assert.ok(await Invoice.collection.findOne(filterOf(key, lambdaEtag, stale) as never) === null, "a processed row is not claimed for the bytes it was processed from");

        // The re-download (the path OPS-15 uses: the batch), and Flipkart now also sends a late document (a credit
        // note issued after the first download — what decision 4's late re-downloads are for).
        bodies.set(orderA, await zipWithLate());
        let third: Awaited<ReturnType<typeof onDemand.downloadAndStoreInvoice>>;
        try {
          third = await onDemand.downloadAndStoreInvoice(api, targetA, { path: "batch" });
        } finally {
          bodies.set(orderA, zip);
        }
        const eventEtag = s3.objects.get(key)!.etag;
        assert.notEqual(eventEtag, lambdaEtag, "the re-download put new bytes");
        assert.equal(third.originals.new, 1, "only the late document is new under orig/");
        const row = await Invoice.findOne({ s3_key: key }).lean();
        assert.equal(row!.etag, lambdaEtag, "registerInvoice left the etag to the Lambda's claim");
        assert.equal(row!.size, 4242, "…and the size");
        assert.deepEqual(row!.parts.map((p) => ({ type: p.type, pages: p.pages, invoiceNumber: p.invoiceNumber, bytes: p.bytes })), lambdaParts, "…and describeParts' list (Flipkart's file list is in invoice_downloads)");
        assert.equal(row!.processing_status, "processed");
        assert.equal(row!.stored_download_id, third.downloadId);
        assert.equal(row!.download_count, countBefore + 1);

        const claimed = await Invoice.collection.findOne(filterOf(key, eventEtag, stale) as never);
        assert.ok(claimed, "the S3 event of the re-download is NOT claimed — T1 is not fixed");
        assert.equal(String(claimed._id), String(row!._id));
        assert.equal(await Invoice.collection.findOne(filterOf(key, lambdaEtag, stale) as never), null, "a duplicate event of the old bytes must not be claimed");

        // What the code before this sub-step did: $set the new etag at register time — then nothing claims it.
        await Invoice.collection.updateOne({ s3_key: key }, { $set: { etag: eventEtag } });
        assert.equal(await Invoice.collection.findOne(filterOf(key, eventEtag, stale) as never), null, "the old $set etag hides the new bytes from the claim (the 70 of 70 of the 100-order test)");
        await Invoice.collection.updateOne({ s3_key: key }, { $set: { etag: lambdaEtag } });
      });
    }

    // ── skipIfUnchanged ──
    await check("skipIfUnchanged (sweep re-capture / refetch): files equal to the processed row's originals → NO raw PUT, record unchanged:true, stored_download_id kept, download counted, units keep their size", async () => {
      const key = capture.rawInvoiceKey(userId, orderA);
      const row0 = await Invoice.findOne({ s3_key: key }).lean();
      // The Lambda (dashboard sub-step 5) built the archive from the stored download's originals.
      await Invoice.updateOne(
        { s3_key: key },
        { $set: { originals: [...fixtureSha.values()].map((d, i) => ({ name: `f${i}`, key: originals.originalKey(userId, orderA, d), sha256: d, bytes: 1, pages: 1, type: "other", invoiceNumber: "", invoice_numbers: [], merge_index: i, page_from: i + 1, signed_at: "", signature_ok: true })), originals_download_id: row0!.stored_download_id } },
        { timestamps: false }
      );
      await Order.updateMany({ _id: { $in: targetA.rowIds } }, { $set: { invoice_size: 777 } });
      const before = s3.puts.length;
      const rawBefore = s3.objects.get(key)!.etag;
      const r = await onDemand.downloadAndStoreInvoice(api, targetA, { path: "sweep-recapture", skipIfUnchanged: true });
      assert.equal(r.unchanged, true);
      assert.equal(s3.putsSince(before).filter((p) => p.key.startsWith("raw/")).length, 0, "a raw PUT was made");
      assert.equal(s3.objects.get(key)!.etag, rawBefore);
      const doc = await InvoiceDownload.collection.findOne({ _id: r.downloadId } as never);
      assert.equal(doc!.unchanged, true);
      assert.equal(doc!.path, "sweep-recapture");
      assert.equal(doc!.merged.etag, "");
      const row = await Invoice.findOne({ s3_key: key }).lean();
      assert.equal(row!.stored_download_id, row0!.stored_download_id);
      assert.equal(row!.download_count, row0!.download_count + 1);
      assert.ok(row!.last_download_at! > row0!.last_download_at!);
      for (const u of await Order.find({ _id: { $in: targetA.rowIds } }).lean()) assert.equal(u.invoice_size, 777);
      // A different file set is not "unchanged": one file fewer → the raw PDF is stored.
      const fewer = { ...answerA, files: answerA.files.slice(0, 5) };
      const b2 = s3.puts.length;
      const r2 = await originals.storeInvoiceDownload({ userId, accountId: account, orderId: orderA, answer: fewer, unitIds: targetA.unitIds, unitRowIds: [], path: "sweep-recapture", skipIfUnchanged: true });
      assert.equal(r2.unchanged, false);
      assert.equal(s3.putsSince(b2).filter((p) => p.key.startsWith("raw/")).length, 1);
      // Without the flag (a batch, the modal) the same files are always re-uploaded — asserted by store #2 above.
    });

    // ── X3 ──
    await check("X3: the worker writes its part list only on a unit that has none — a unit holding the pipeline's list keeps it; a new sibling gets the worker's", async () => {
      const pipelineList = [{ type: "product", pages: 1, invoiceNumber: "VERIFYP1", bytes: 111 }];
      const [kept, fresh] = await insertUnits(orderB, [
        { status_key: "Delivered", actual_delivery_date: hoursAgo(20), invoice_parts: pipelineList },
        { status_key: "Delivered", actual_delivery_date: hoursAgo(20) },
      ]);
      await onDemand.downloadAndStoreInvoice(api, { userId, accountId: String(account), email: "s12@verify.invalid", orderId: orderB, unitIds: [kept.unit_id, fresh.unit_id], rowIds: [kept._id, fresh._id] }, { path: "by-order-id" });
      const a = await Order.findById(kept._id).lean();
      const b = await Order.findById(fresh._id).lean();
      assert.deepEqual(a!.invoice_parts.map((p) => ({ type: p.type, pages: p.pages, invoiceNumber: p.invoiceNumber, bytes: p.bytes })), pipelineList);
      assert.equal(b!.invoice_parts.length, 6);
      assert.equal(a!.is_invoice_downloaded, true);
      assert.equal(a!.invoice_s3_key, capture.rawInvoiceKey(userId, orderB));
    });

    await check("T2 on a bundle fetched before delivery: captured_at set, settled false (a unit still Shipped); the record's path is by-order-id", async () => {
      const row = await Invoice.findOne({ s3_key: capture.rawInvoiceKey(userId, orderB) }).lean();
      assert.ok(row!.captured_at);
      assert.equal(row!.settled, false, "delivered 20 h ago is inside the 48 h window");
      const units = await insertUnits(orderD, [{ status_key: "Delivered", actual_delivery_date: hoursAgo(100) }, { status_key: "Shipped" }]);
      await onDemand.downloadAndStoreInvoice(api, { userId, accountId: String(account), email: "s12@verify.invalid", orderId: orderD, unitIds: units.map((u) => u.unit_id), rowIds: units.map((u) => u._id) });
      const rowD = await Invoice.findOne({ s3_key: capture.rawInvoiceKey(userId, orderD) }).lean();
      assert.ok(rowD!.captured_at);
      assert.equal(rowD!.settled, false);
      const doc = await InvoiceDownload.collection.findOne({ userId: workspace, order_id: orderD });
      assert.equal(doc!.path, "by-order-id");
      // A row that already has captured_at keeps it (a later download is not a first capture).
      const capturedAt = rowD!.captured_at!.getTime();
      await onDemand.downloadAndStoreInvoice(api, { userId, accountId: String(account), email: "s12@verify.invalid", orderId: orderD, unitIds: units.map((u) => u.unit_id), rowIds: units.map((u) => u._id) });
      assert.equal((await Invoice.findOne({ s3_key: capture.rawInvoiceKey(userId, orderD) }).lean())!.captured_at!.getTime(), capturedAt);
    });

    await check("T2 on a row stored before (captured_at null, settled false, processed — the 1,308): the next download sets captured_at and settles it by the rule; its etag stays", async () => {
      const key = capture.rawInvoiceKey(userId, orderC);
      const units = await insertUnits(orderC, [{ status_key: "Delivered", actual_delivery_date: hoursAgo(24 * 20) }]);
      await Invoice.collection.insertOne({ userId: workspace, order_id: orderC, s3_key: key, source_key: key, etag: "verify-old-etag", size: 1, parts: [], processing_status: "processed", processed_at: new Date(), settled: false, captured_at: null, unit_ids: [], createdAt: new Date(), updatedAt: new Date() } as never);
      const r = await onDemand.downloadAndStoreInvoice(api, { userId, accountId: String(account), email: "s12@verify.invalid", orderId: orderC, unitIds: units.map((u) => u.unit_id), rowIds: units.map((u) => u._id) }, { path: "batch" });
      const row = await Invoice.findOne({ s3_key: key }).lean();
      assert.ok(row!.captured_at);
      assert.equal(row!.settled, true);
      assert.equal(row!.etag, "verify-old-etag");
      assert.equal(row!.stored_download_id, r.downloadId);
      const doc = await InvoiceDownload.collection.findOne({ _id: r.downloadId } as never);
      assert.equal(doc!.answer.kind, "pdf");
      assert.deepEqual(doc!.files.map((f: { name: string; page_from: number; merge_index: number }) => [f.name, f.page_from, f.merge_index]), [[`${orderC}.pdf`, 1, 0]]);
      // A single-PDF answer is stored as it came: the raw PDF is Flipkart's own bytes.
      assert.equal(sha(s3.objects.get(key)!.body), sha(product));
    });

    await check("phase 2's settle (settleRecaptured): settled by the delivery rule — a delivered-long-ago order settles, one still on its way gets captured_at = now and stays open; a row older than 30 days settles anyway", async () => {
      const now = new Date();
      await Invoice.updateMany({ userId: workspace, order_id: { $in: [orderA, orderD] } }, { $set: { settled: false, captured_at: hoursAgo(60) } }, { timestamps: false });
      const n = await sweep.settleRecaptured(userId, [orderA, orderD], now);
      assert.equal(n, 1);
      const a = await Invoice.findOne({ userId: workspace, order_id: orderA }).lean();
      const d = await Invoice.findOne({ userId: workspace, order_id: orderD }).lean();
      assert.equal(a!.settled, true);
      assert.equal(d!.settled, false);
      assert.equal(d!.captured_at!.getTime(), now.getTime());
      await Invoice.collection.updateOne({ userId: workspace, order_id: orderD }, { $set: { createdAt: new Date(Date.now() - 31 * 86_400_000) } });
      assert.equal(await sweep.settleRecaptured(userId, [orderD], now), 1);
      assert.equal((await Invoice.findOne({ userId: workspace, order_id: orderD }).lean())!.settled, true);
    });

    // ── the fetch path ──
    await check("the fetch's capture (captureInvoicesForAccount, API session) stores through the same helper: path fetch, orig/ per order, one raw PUT per order, every delivered unit of the order stamped, the page never opened", async () => {
      bodies.set(orderF, zip);
      const units = await insertUnits(orderF, [{ status_key: "Delivered", actual_delivery_date: hoursAgo(3) }, { status_key: "Delivered", actual_delivery_date: hoursAgo(3) }]);
      const before = s3.puts.length;
      const logs: string[] = [];
      const summary = await capture.captureInvoicesForAccount({
        userId,
        platformAccountId: account,
        email: "s12@verify.invalid",
        mode: "missing",
        justEnriched: new Set(),
        api,
        getPage: async () => {
          throw new Error("the page must not be opened: the API answered");
        },
        cancelled: () => false,
        log: (_l, m) => logs.push(m),
      });
      assert.equal(summary.stored, 2);
      assert.equal(summary.failed, 0, logs.join("\n"));
      const puts = s3.putsSince(before);
      assert.equal(puts.filter((p) => p.key.startsWith(`orig/${userId}/${orderF}/`)).length, 6, "the orig/ prefix is per order");
      assert.equal(puts.filter((p) => p.key.startsWith("raw/")).length, 1, "one raw PUT per order");
      const doc = await InvoiceDownload.collection.findOne({ userId: workspace, order_id: orderF });
      assert.equal(doc!.path, "fetch");
      assert.equal(doc!.via, "api");
      for (const u of await Order.find({ _id: { $in: units.map((x) => x._id) } }).lean()) {
        assert.equal(u.invoice_s3_key, capture.rawInvoiceKey(userId, orderF));
        assert.equal(u.invoice_parts.length, 6);
      }
      const row = await Invoice.findOne({ userId: workspace, order_id: orderF }).lean();
      assert.ok(row!.captured_at);
      assert.equal(row!.settled, false, "delivered 3 h ago");
    });

    await check("the S3 event's claim made the row first (parts: [], its etag): the worker's list fills the EMPTY list; the claim's etag stays", async () => {
      const key = capture.rawInvoiceKey(userId, orderG);
      bodies.set(orderG, zip);
      const units = await insertUnits(orderG, [{ status_key: "Delivered", actual_delivery_date: hoursAgo(60) }]);
      await Invoice.collection.insertOne({ userId: workspace, order_id: orderG, s3_key: key, source_key: key, etag: "verify-claim-etag", size: 9, parts: [], processing_status: "processing", unit_ids: [], createdAt: new Date(), updatedAt: new Date() } as never);
      await onDemand.downloadAndStoreInvoice(api, { userId, accountId: String(account), email: "s12@verify.invalid", orderId: orderG, unitIds: units.map((u) => u.unit_id), rowIds: units.map((u) => u._id) }, { path: "by-order-id" });
      const row = await Invoice.findOne({ s3_key: key }).lean();
      assert.equal(row!.parts.length, 6);
      assert.equal(row!.etag, "verify-claim-etag");
      assert.equal(row!.size, 9);
    });

    // ── failures ──
    await check("a store that refuses the condition (501): HEAD, then a plain PUT only for what is missing — identical bytes, nothing written twice", async () => {
      s3.refuseConditions = true;
      try {
        const files = [{ bytes: otherA, sha256: sha(otherA) }, { bytes: Buffer.from("%PDF-1.4 verify new"), sha256: sha(Buffer.from("%PDF-1.4 verify new")) }];
        const before = s3.puts.length;
        const out = await originals.storeOriginals(userId, orderA, files);
        assert.deepEqual(out, ["existing", "new"]);
        const plain = s3.putsSince(before).filter((p) => p.ifNoneMatch === undefined);
        assert.equal(plain.length, 1);
        assert.equal(plain[0].key, originals.originalKey(userId, orderA, files[1].sha256));
      } finally {
        s3.refuseConditions = false;
      }
    });

    for (const mode of ["403", "503"] as const) {
      await check(`originals are best-effort (${mode === "403" ? "403 AccessDenied — the Writer has no orig/* grant before D1" : "503 — a storage outage"}): the download still stores the merged PDF, WITHOUT download-id; the record says stored:failed + originals_error; the row's stored_download_id is cleared (the backfill takes it again); units stamped`, async () => {
        const key = capture.rawInvoiceKey(userId, orderA);
        await Invoice.updateOne({ s3_key: key }, { $set: { stored_download_id: "f".repeat(24) } }, { timestamps: false });
        const warn = console.warn;
        const warned: string[] = [];
        console.warn = (...a: unknown[]) => void warned.push(a.join(" "));
        s3.failOriginals = mode;
        const before = s3.puts.length;
        let r: Awaited<ReturnType<typeof onDemand.downloadAndStoreInvoice>>;
        try {
          r = await onDemand.downloadAndStoreInvoice(api, targetA, { path: "batch" });
        } finally {
          s3.failOriginals = false;
          console.warn = warn;
        }
        assert.deepEqual(r.originals, { new: 0, existing: 0, failed: 6 });
        const rawPuts = s3.putsSince(before).filter((p) => p.key.startsWith("raw/"));
        assert.equal(rawPuts.length, 1, "the merged PDF was not stored");
        assert.equal(rawPuts[0].metadata?.["download-id"], undefined, "download-id on a raw PDF whose originals are missing");
        const doc = await InvoiceDownload.collection.findOne({ _id: r.downloadId } as never);
        assert.ok(doc!.files.every((f: { stored: string }) => f.stored === "failed"));
        assert.match(String(doc!.originals_error), mode === "403" ? /^S3 put original orig\/.*not authorized/ : /^S3 put original orig\//);
        assert.equal(doc!.merged.etag, s3.objects.get(key)!.etag);
        const row = await Invoice.findOne({ s3_key: key }).lean();
        assert.ok(!row!.stored_download_id, "stored_download_id still names a download");
        assert.ok(warned.some((w) => w.includes(orderA) && w.includes("without download-id")), "no warning was logged");
        for (const u of await Order.find({ _id: { $in: targetA.rowIds } }).lean()) assert.equal(u.invoice_s3_key, key);
      });
    }

    await check("an S3 failure of our own after Flipkart answered (the raw PUT, ECONNRESET) is marked as ours: the batch files it internal_error (classifyPlatformError alone would blame Flipkart: platform_unavailable); the sweep does not count it", async () => {
      s3.failRaw = true;
      try {
        await assert.rejects(onDemand.downloadAndStoreInvoice(api, targetA, { path: "batch" }), (err: Error) => {
          assert.match(err.message, /ECONNRESET/);
          assert.equal(originals.isInvoiceStoreError(err), true);
          assert.equal(classifyPlatformError(err).errorClass, "platform_unavailable", "for the record: why the mark is needed");
          return true;
        });
      } finally {
        s3.failRaw = false;
      }
      // A Flipkart-side failure is not marked.
      await assert.rejects(onDemand.downloadAndStoreInvoice(api, { ...targetA, orderId: "ODVERIFYNOSUCH" }, { path: "batch" }), (err: Error) => {
        assert.equal(originals.isInvoiceStoreError(err), false);
        assert.equal(classifyPlatformError(err).errorClass, "nothing_available");
        return true;
      });
    });
  } finally {
    capture.useInvoiceS3ForChecks(null);
    const removed = {
      invoices: (await Invoice.collection.deleteMany({ userId: workspace })).deletedCount,
      downloads: (await InvoiceDownload.collection.deleteMany({ userId: workspace })).deletedCount,
      units: (await Order.collection.deleteMany({ userId: workspace })).deletedCount,
    };
    const left =
      (await Invoice.collection.countDocuments({ userId: workspace })) +
      (await InvoiceDownload.collection.countDocuments({ userId: workspace })) +
      (await Order.collection.countDocuments({ userId: workspace })) +
      (await Invoice.collection.countDocuments({ order_id: { $regex: `^ODVERIFY${tag}` } })) +
      (await Order.collection.countDocuments({ order_id: { $regex: `^ODVERIFY${tag}` } }));
    console.log(`\ncleanup: ${removed.invoices} invoices, ${removed.downloads} invoice_downloads and ${removed.units} order_details row(s) of the invented workspace deleted — ${left} left; in-memory S3 held ${s3.objects.size} object(s), real S3 untouched`);
    if (left) failures.push("cleanup left rows behind");
    await mongoose.disconnect();
  }
  console.log(`\n${passed} passed, ${failures.length} failed`);
  if (failures.length) {
    for (const f of failures) console.log(`  - ${f}`);
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
  void mongoose.disconnect();
});
