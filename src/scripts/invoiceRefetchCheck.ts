/**
 * Late re-downloads (SO-5), the cancelled-after-shipping capture and phase 1's failure memory (SO-6), and the
 * originals backfill's pure rules (SO-4) — data-quality sub-step 12 — checked WITHOUT Flipkart and WITHOUT S3:
 *
 *   · every download is a function injected into the sweep (runInvoiceSweep's `download`), which records the call
 *     and answers as told; the one store through the real helper uses an in-memory S3. INVOICE_S3_BUCKET is set to
 *     a made-up name before anything is loaded.
 *   · Mongo is real, rows are invented: made-up workspace ids, @verify.invalid e-mails, order ids ODREFETCH…;
 *     every invoices, invoice_downloads, order_details and order_invoice_requests row of those workspaces is
 *     deleted at the end and the leftovers are counted (must be 0). Connected with autoIndex / autoCreate off.
 *
 *   npm run test:invoice-refetch
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import mongoose, { Types } from "mongoose";
import { PDFDocument, StandardFonts } from "pdf-lib";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const dashboardRepo = path.resolve(process.env.DASHBOARD_REPO || path.join(repoRoot, "../../data_for_project/gmail-id-dashboard_mongodb"));

let passed = 0;
const failures: string[] = [];
async function check(name: string, fn: () => void | Promise<void>) {
  try {
    await fn();
    passed++;
    console.log(`  ok    ${name}`);
  } catch (err) {
    failures.push(name);
    console.log(`  FAIL  ${name}\n        ${err instanceof Error ? err.message.split("\n").slice(0, 30).join("\n        ") : err}`);
  }
}

const H = 3_600_000;
const D = 24 * H;
const ago = (ms: number) => new Date(Date.now() - ms);
const sha = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

/** PUT / HEAD in memory (IfNoneMatch honoured); anything else is refused. */
class MemoryS3 {
  objects = new Map<string, Buffer>();
  constructor(readonly bucket: string) {}
  async send(cmd: { constructor: { name: string }; input: Record<string, unknown> }): Promise<unknown> {
    assert.equal(cmd.input.Bucket, this.bucket, "a call went to a bucket other than the in-memory one");
    const key = String(cmd.input.Key);
    if (cmd.constructor.name === "PutObjectCommand") {
      if (cmd.input.IfNoneMatch === "*" && this.objects.has(key)) throw Object.assign(new Error("PreconditionFailed"), { name: "PreconditionFailed", $metadata: { httpStatusCode: 412 } });
      const body = Buffer.from(cmd.input.Body as Uint8Array);
      this.objects.set(key, body);
      return { ETag: `"${createHash("md5").update(body).digest("hex")}"` };
    }
    if (cmd.constructor.name === "HeadObjectCommand") {
      if (!this.objects.has(key)) throw Object.assign(new Error("NotFound"), { name: "NotFound", $metadata: { httpStatusCode: 404 } });
      return { ContentLength: this.objects.get(key)!.length };
    }
    throw new Error(`the in-memory S3 does not answer ${cmd.constructor.name}`);
  }
}

async function main() {
  const wsNote = new Types.ObjectId(); // noting + first store
  const wsSweep = new Types.ObjectId(); // phase 3
  const wsCapture = new Types.ObjectId(); // phase 1 (SO-6, failure memory)
  const wsBackfill = new Types.ObjectId(); // SO-4
  const workspaces = [wsNote, wsSweep, wsCapture, wsBackfill];
  process.env.TRADE_FLOW_USER_ID = String(wsNote);
  const BUCKET = `verify-in-memory-${Date.now().toString(36)}`;
  process.env.INVOICE_S3_BUCKET = BUCKET;
  process.env.INVOICE_SETTLE_HOURS = "48";

  const { config } = await import("../config.js");
  // AFTER config.ts has run dotenv (it never runs again): whatever this Mac's .env sets for these, the check runs on
  // the defaults — deleting them before the import would only let dotenv read them back in.
  for (const k of ["INVOICE_REFETCH_CANCEL_DELAY_H", "INVOICE_REFETCH_REFUND_DELAY_H", "INVOICE_REFETCH_COD_DELAY_H", "INVOICE_CAPTURE_CANCELLED", "INVOICE_RECAPTURE_GIVE_UP_DAYS"]) delete process.env[k];
  assert.equal(config.invoiceBucket, BUCKET);
  const { Invoice } = await import("../models/Invoice.js");
  const { InvoiceDownload } = await import("../models/InvoiceDownload.js");
  const { Order } = await import("../models/Order.js");
  const { OrderInvoiceRequest } = await import("../models/OrderInvoiceRequest.js");
  const capture = await import("../services/invoiceCapture.js");
  const originals = await import("../services/invoiceOriginals.js");
  const refetch = await import("../services/invoiceRefetch.js");
  const sweep = await import("../services/invoiceSweep.js");
  const backfill = await import("./invoiceOriginalsBackfill.js");
  type DownloadFn = NonNullable<Parameters<typeof sweep.runInvoiceSweep>[1]>["download"];
  type DownloadOpts = Parameters<NonNullable<DownloadFn>>[0];

  const s3 = new MemoryS3(BUCKET);
  capture.useInvoiceS3ForChecks(s3 as unknown as import("../services/invoiceCapture.js").InvoiceS3);
  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false, serverSelectionTimeoutMS: 15_000 });

  const tag = Date.now().toString().slice(-8);
  const account = new Types.ObjectId();
  let seq = 0;
  const order = (n: number) => `ODREFETCH${tag}${String(n).padStart(2, "0")}`;
  const insertUnits = async (ws: Types.ObjectId, orderId: string, units: Array<Record<string, unknown>>) => {
    const docs = units.map((u) => ({
      userId: ws,
      platformAccountId: account,
      platform_email: "s12-refetch@verify.invalid",
      id: 991_000_000 + Number(tag.slice(-4)) * 100 + ++seq,
      order_id: orderId,
      unit_id: `${orderId}-U${seq}`,
      item_id: `ITEM${seq}`,
      invoice_s3_key: "",
      invoice_parts: [],
      is_invoice_downloaded: false,
      tracking: [],
      ...u,
    }));
    await Order.collection.insertMany(docs as never[]);
    return docs.map((d) => d.unit_id);
  };
  const insertInvoice = async (ws: Types.ObjectId, orderId: string, extra: Record<string, unknown> = {}) => {
    const now = new Date();
    await Invoice.collection.insertOne({
      userId: ws,
      order_id: orderId,
      s3_key: capture.rawInvoiceKey(String(ws), orderId),
      source_key: capture.rawInvoiceKey(String(ws), orderId),
      etag: "verify-etag",
      size: 1,
      parts: [],
      processing_status: "processed",
      settled: true,
      captured_at: ago(20 * D),
      unit_ids: [],
      createdAt: ago(20 * D),
      updatedAt: now,
      ...extra,
    } as never);
  };
  const rowOf = async (ws: Types.ObjectId, orderId: string) => (await Invoice.collection.findOne({ userId: ws, order_id: orderId })) as unknown as { refetch?: Array<Record<string, unknown>>; settled?: boolean; refetch_claim_until?: Date | null; refetch_claim_by?: string; last_download_at?: Date } | null;

  try {
    // ── the detection (pure) ──
    await check("refetchEventsOf: Shipped → Cancelled (invoiced) gives 'cancelled' at the cancellation, due 1 day later; Order Confirmed → Cancelled, Cancellation requested and Shipped give nothing", () => {
      const now = new Date();
      const cancelledAt = ago(2 * H);
      const base = { unit_id: "U", order_id: "O", tracking: [] };
      const m = refetch.refetchEventsOf({ status_key: "Shipped" }, { ...base, status_key: "Cancelled", order_status_pre_cancellation: "Shipped", cancelled_date: cancelledAt, payment_mode: "" }, now);
      assert.deepEqual(m.map((x) => [x.reason, x.event_at.getTime(), x.cod]), [["cancelled", cancelledAt.getTime(), false]]);
      assert.equal(refetch.refetchDueAt("cancelled", cancelledAt, false).getTime(), cancelledAt.getTime() + 24 * H);
      assert.deepEqual(refetch.refetchEventsOf({ status_key: "Order Confirmed" }, { ...base, status_key: "Cancelled", order_status_pre_cancellation: "Order Confirmed", cancelled_date: cancelledAt }, now), []);
      assert.deepEqual(refetch.refetchEventsOf({ status_key: "Shipped" }, { ...base, status_key: "Cancellation requested", order_status_pre_cancellation: "Shipped" }, now), []);
      assert.deepEqual(refetch.refetchEventsOf({ status_key: "Approved" }, { ...base, status_key: "Shipped" }, now), []);
      // Flipkart's invoice_eligible counts as invoiced; "Delivery Unsuccessful" before the cancellation too.
      assert.equal(refetch.refetchEventsOf(null, { ...base, status_key: "Cancelled", order_status_pre_cancellation: "Approved", invoice_eligible: true, cancelled_date: cancelledAt }, now).length, 1);
      assert.equal(refetch.refetchEventsOf(null, { ...base, status_key: "Cancelled", order_status_pre_cancellation: "Delivery Unsuccessful. Retrying soon.", cancelled_date: cancelledAt }, now).length, 1);
    });

    await check("refetchEventsOf: Delivered → Returned gives 'returned'; cash on delivery waits 8 days; a DONE Refund stage gives 'refund' due 8 days later (a stage dated ahead counts as now); a PENDING one and an already-cancelled unit's cancellation give nothing new", () => {
      const now = new Date();
      const base = { unit_id: "U", order_id: "O" };
      const r = refetch.refetchEventsOf({ status_key: "Delivered" }, { ...base, status_key: "Returned", tracking: [] }, now);
      assert.deepEqual(r.map((x) => x.reason), ["returned"]);
      const cod = refetch.refetchEventsOf({ status_key: "Shipped" }, { ...base, status_key: "Cancelled", order_status_pre_cancellation: "Shipped", cancelled_date: ago(H), payment_mode: "cod", tracking: [] }, now);
      assert.equal(cod[0].cod, true);
      assert.equal(refetch.refetchDueAt(cod[0].reason, cod[0].event_at, cod[0].cod).getTime(), cod[0].event_at.getTime() + 192 * H);
      // A unit as the order API stores it: payment_mode "" (only checkout jobs set it), cash_on_delivery true — or
      // only Flipkart's own text in api_extra.payment_mode.
      const apiRow = { ...base, status_key: "Cancelled", order_status_pre_cancellation: "Shipped", cancelled_date: ago(H), payment_mode: "", tracking: [] };
      assert.equal(refetch.refetchEventsOf({ status_key: "Shipped" }, { ...apiRow, cash_on_delivery: true }, now)[0].cod, true, "cash_on_delivery: true is COD");
      assert.equal(refetch.refetchEventsOf({ status_key: "Shipped" }, { ...apiRow, api_payment_mode: "Cash On Delivery" }, now)[0].cod, true, "api_extra.payment_mode 'Cash On Delivery' is COD");
      assert.equal(refetch.refetchEventsOf({ status_key: "Shipped" }, { ...apiRow, cash_on_delivery: false, api_payment_mode: "UPI" }, now)[0].cod, false, "a prepaid API row is not COD");
      assert.deepEqual(
        refetch.refetchStateOf({ unit_id: "U", order_id: "O", status_key: "Cancelled", payment_mode: "", cash_on_delivery: true, api_extra: { payment_mode: "Cash On Delivery", invoice_eligible: true } }),
        { unit_id: "U", order_id: "O", status_key: "Cancelled", refund_status: null, cancelled_date: null, order_status_pre_cancellation: null, payment_mode: "", cash_on_delivery: true, api_payment_mode: "Cash On Delivery", invoice_eligible: true, tracking: [] }
      );
      assert.match(refetch.REFETCH_UNIT_FIELDS, /\bcash_on_delivery\b/);
      assert.match(refetch.REFETCH_UNIT_FIELDS, /\bapi_extra\.payment_mode\b/);
      const refundAt = ago(3 * D);
      const ref = refetch.refetchEventsOf({ status_key: "Cancelled" }, { ...base, status_key: "Cancelled", order_status_pre_cancellation: "Shipped", cancelled_date: ago(5 * D), tracking: [{ status: "Refund", stage: "DONE", date: refundAt.toISOString() }] }, now);
      assert.deepEqual(ref.map((x) => [x.reason, x.event_at.getTime()]), [["refund", refundAt.getTime()]]);
      assert.equal(refetch.refetchDueAt("refund", refundAt, false).getTime(), refundAt.getTime() + 192 * H);
      const ahead = refetch.refetchEventsOf({ status_key: "Cancelled" }, { ...base, status_key: "Cancelled", order_status_pre_cancellation: "Shipped", tracking: [{ status: "Refund", stage: "DONE", date: new Date(Date.now() + 4 * D).toISOString() }] }, now);
      assert.equal(ahead[0].event_at.getTime(), now.getTime());
      assert.deepEqual(refetch.refetchEventsOf({ status_key: "Cancelled" }, { ...base, status_key: "Cancelled", order_status_pre_cancellation: "Shipped", tracking: [{ status: "Refund", stage: "PENDING", date: refundAt.toISOString() }] }, now), []);
      assert.deepEqual(refetch.refetchEventsOf({ status_key: "Cancelled" }, { ...base, status_key: "Cancelled", order_status_pre_cancellation: "Shipped", refund_status: "Refund completed", tracking: [] }, now).map((x) => x.reason), ["refund"]);
      // First sight (no before): the state alone — both moments.
      assert.deepEqual(refetch.refetchEventsOf(null, { ...base, status_key: "Cancelled", order_status_pre_cancellation: "Shipped", cancelled_date: ago(5 * D), tracking: [{ status: "Refund", stage: "DONE", date: refundAt.toISOString() }] }, now).map((x) => x.reason), ["cancelled", "refund"]);
    });

    await check("the delays are .env settings, read at call time: defaults 24 h / 192 h / 192 h (the plan's recommendation); INVOICE_REFETCH_CANCEL_DELAY_H=2 makes the cancellation look 2 h", () => {
      assert.deepEqual(refetch.refetchDelaysH(), { cancel: 24, refund: 192, cod: 192 });
      process.env.INVOICE_REFETCH_CANCEL_DELAY_H = "2";
      try {
        const at = new Date();
        assert.equal(refetch.refetchDueAt("cancelled", at, false).getTime(), at.getTime() + 2 * H);
        assert.equal(refetch.refetchDueAt("returned", at, true).getTime(), at.getTime() + 192 * H, "a cash-on-delivery return keeps the COD delay");
      } finally {
        delete process.env.INVOICE_REFETCH_CANCEL_DELAY_H;
      }
    });

    // ── the noting (Mongo, invented rows) ──
    await check("noteInvoiceRefetch: one entry per (unit, reason) — twice, and six at once, leave one; another reason adds one; an order without a bundle gets none", async () => {
      const o = order(1);
      await insertInvoice(wsNote, o);
      const moment = { reason: "cancelled" as const, event_at: ago(2 * H), cod: false };
      assert.equal(await refetch.noteInvoiceRefetch({ userId: wsNote, orderId: o, unitId: "U1", moment, by: "verify" }), true);
      assert.equal(await refetch.noteInvoiceRefetch({ userId: wsNote, orderId: o, unitId: "U1", moment, by: "verify" }), false);
      const racing = await Promise.all(Array.from({ length: 6 }, () => refetch.noteInvoiceRefetch({ userId: String(wsNote), orderId: o, unitId: "U2", moment, by: "verify both Macs" })));
      assert.equal(racing.filter(Boolean).length, 1);
      assert.equal(await refetch.noteInvoiceRefetch({ userId: wsNote, orderId: o, unitId: "U1", moment: { reason: "refund", event_at: ago(H), cod: false }, by: "verify" }), true);
      const row = await rowOf(wsNote, o);
      assert.deepEqual(row!.refetch!.map((e) => `${e.unit_id}:${e.reason}`), ["U1:cancelled", "U2:cancelled", "U1:refund"]);
      const e = row!.refetch![0];
      assert.equal(e.attempts, 0);
      assert.equal(e.done_at, null);
      assert.equal(new Date(e.due_at as Date).getTime(), moment.event_at.getTime() + 24 * H);
      assert.equal(await refetch.noteInvoiceRefetch({ userId: wsNote, orderId: order(99), unitId: "U1", moment, by: "verify" }), false);
    });

    await check("the update worker's call (noteLateDocumentsAfterRead): an unended unit costs no read and notes nothing; Shipped → Cancelled notes once; a repeat read notes nothing; a later DONE Refund stage notes 'refund'", async () => {
      const o = order(2);
      await insertInvoice(wsNote, o);
      const [u] = await insertUnits(wsNote, o, [{ status_key: "Cancelled", order_status_pre_cancellation: "Shipped", cancelled_date: ago(3 * H), payment_mode: "", invoice_s3_key: capture.rawInvoiceKey(String(wsNote), o) }]);
      assert.deepEqual(await refetch.noteLateDocumentsAfterRead(wsNote, `${o}-nothing`, { before: "Approved", after: "Shipped" }, "verify"), []);
      assert.deepEqual(await refetch.noteLateDocumentsAfterRead(wsNote, u, { before: "Shipped", after: "Cancelled" }, "update verify"), ["cancelled"]);
      assert.deepEqual(await refetch.noteLateDocumentsAfterRead(wsNote, u, { before: "Cancelled", after: "Cancelled" }, "update verify"), []);
      await Order.collection.updateOne({ userId: wsNote, unit_id: u }, { $set: { tracking: [{ status: "Refund", stage: "DONE", date: ago(H).toISOString(), detailed_steps: [] }] } });
      assert.deepEqual(await refetch.noteLateDocumentsAfterRead(wsNote, u, { before: "Cancelled", after: "Cancelled" }, "update verify"), ["refund"]);
      const row = await rowOf(wsNote, o);
      assert.deepEqual(row!.refetch!.map((e) => `${e.reason}:${e.by}`), ["cancelled:update verify", "refund:update verify"]);
    });

    await check("a cash-on-delivery unit as the order API stores it (cash_on_delivery true, no top-level payment_mode, api_extra.payment_mode 'Cash On Delivery') is noted cod:true, due 8 days after the cancellation — not the prepaid 1 day", async () => {
      const o = order(4);
      await insertInvoice(wsNote, o);
      const cancelledAt = ago(3 * H);
      const [u] = await insertUnits(wsNote, o, [{ status_key: "Cancelled", order_status_pre_cancellation: "Shipped", cancelled_date: cancelledAt, cash_on_delivery: true, api_extra: { payment_mode: "Cash On Delivery" }, invoice_s3_key: capture.rawInvoiceKey(String(wsNote), o) }]);
      assert.deepEqual(await refetch.noteLateDocumentsAfterRead(wsNote, u, { before: "Shipped", after: "Cancelled" }, "update verify"), ["cancelled"]);
      const e = (await rowOf(wsNote, o))!.refetch![0];
      assert.equal(e.cod, true);
      assert.equal(new Date(e.due_at as Date).getTime(), cancelledAt.getTime() + 192 * H);
    });

    await check("the first store of a bundle (storeInvoiceDownload) notes what its units already show: a unit cancelled after shipping → 'cancelled' by 'store batch'; a unit cancelled before shipping → nothing; a second store notes nothing more", async () => {
      const o = order(3);
      const fixture = path.join(dashboardRepo, "infra/test-fixtures/smoke-invoice.pdf");
      let pdf: Buffer;
      if (fs.existsSync(fixture)) pdf = fs.readFileSync(fixture);
      else {
        const doc = await PDFDocument.create();
        const font = await doc.embedFont(StandardFonts.Helvetica);
        doc.addPage([595, 842]).drawText("invented invoice (verify)", { x: 30, y: 800, size: 10, font });
        pdf = Buffer.from(await doc.save({ useObjectStreams: false }));
      }
      const unitIds = await insertUnits(wsNote, o, [
        { status_key: "Delivered", actual_delivery_date: ago(3 * D) },
        { status_key: "Cancelled", order_status_pre_cancellation: "Shipped", cancelled_date: ago(2 * D), payment_mode: "" },
        { status_key: "Cancelled", order_status_pre_cancellation: "Order Confirmed", cancelled_date: ago(2 * D), payment_mode: "" },
      ]);
      const rows = await Order.collection.find({ userId: wsNote, order_id: o }).project({ _id: 1 }).toArray();
      const answer = { kind: "pdf" as const, via: "api" as const, bytes: pdf.length, sha256: sha(pdf), files: [{ name: `${o}.pdf`, bytes: pdf }] };
      const input = { userId: String(wsNote), accountId: String(account), orderId: o, answer, unitIds, unitRowIds: rows.map((r) => r._id), path: "batch" as const };
      await originals.storeInvoiceDownload(input);
      let row = await rowOf(wsNote, o);
      assert.deepEqual(row!.refetch!.map((e) => `${e.unit_id}:${e.reason}:${e.by}`), [`${unitIds[1]}:cancelled:store batch`]);
      await originals.storeInvoiceDownload(input);
      row = await rowOf(wsNote, o);
      assert.equal(row!.refetch!.length, 1);
    });

    // ── phase 3 of the sweep ──
    const calls: Array<{ orderIds: string[]; path?: string; force: boolean; skipIfUnchanged?: boolean }> = [];
    const answers = new Map<string, "downloaded" | "unchanged" | "failed">();
    const fakeDownload = async (opts: DownloadOpts) => {
      calls.push({ orderIds: [...opts.orderIds], path: opts.path, force: opts.force, skipIfUnchanged: opts.skipIfUnchanged });
      const out = [];
      for (const orderId of opts.orderIds) {
        await opts.onStart?.(orderId);
        const a = answers.get(orderId) || "failed";
        if (a !== "failed") {
          // What the real store does to the row: registerInvoice's last_download_at.
          await Invoice.collection.updateOne({ userId: new Types.ObjectId(opts.userId), order_id: orderId }, { $set: { last_download_at: new Date() }, $inc: { download_count: 1 } });
          out.push({ orderId, status: "downloaded" as const, units: 1, ...(a === "unchanged" ? { unchanged: true } : {}) });
        } else out.push({ orderId, status: "failed" as const, units: 1, error: "invented failure" });
        await opts.onResult?.(out[out.length - 1]);
      }
      return out;
    };
    const entry = (unit: string, reason: string, dueAt: Date, extra: Record<string, unknown> = {}) => ({ unit_id: unit, reason, event_at: new Date(dueAt.getTime() - 24 * H), due_at: dueAt, noted_at: ago(2 * D), by: "verify", cod: false, attempts: 0, last_error: "", done_at: null, result: "", ...extra });
    const [R1, R2, R3, R4, R5] = [11, 12, 13, 14, 15].map(order);

    await check("phase 3: a due entry is downloaded once (force, path sweep-refetch, skipIfUnchanged); one a later download covers closes 'already_newer' with no call; one not yet due is left; one another sweep holds is left; settled stays true; claims are given back", async () => {
      await insertInvoice(wsSweep, R1, { last_download_at: ago(3 * D), refetch: [entry("A", "cancelled", ago(H))] });
      await insertInvoice(wsSweep, R2, { last_download_at: ago(H), refetch: [entry("B", "cancelled", ago(5 * H))] });
      await insertInvoice(wsSweep, R3, { last_download_at: ago(3 * D), refetch: [entry("C", "refund", ago(2 * H))] });
      await insertInvoice(wsSweep, R4, { last_download_at: ago(3 * D), refetch: [entry("D", "refund", new Date(Date.now() + 5 * D))] });
      await insertInvoice(wsSweep, R5, { last_download_at: ago(3 * D), refetch: [entry("E", "cancelled", ago(H))], refetch_claim_until: new Date(Date.now() + 10 * 60_000), refetch_claim_by: "the-other-mac" });
      for (const o of [R1, R2, R3, R4, R5]) await insertUnits(wsSweep, o, [{ status_key: "Cancelled", order_status_pre_cancellation: "Shipped", cancelled_date: ago(2 * D), invoice_s3_key: capture.rawInvoiceKey(String(wsSweep), o) }]);
      answers.set(R1, "downloaded");
      const phases: Array<[string, string[]]> = [];
      calls.length = 0;
      const result = await sweep.runInvoiceSweep(String(wsSweep), { download: fakeDownload as never, onPhase: (p, ids) => void phases.push([p, ids]) });
      assert.deepEqual(phases.map(([p, ids]) => [p, [...ids].sort()]), [["refetch", [R1, R3]]], "phases 1 and 2 have nothing; phase 3 takes R1 and R3");
      assert.deepEqual(calls.map((c) => ({ ...c, orderIds: [...c.orderIds].sort() })), [{ orderIds: [R1, R3], path: "sweep-refetch", force: true, skipIfUnchanged: true }]);
      assert.deepEqual({ ...result.phase3 }, { candidates: 3, refetched: 1, unchanged: 0, alreadyNewer: 1, failed: 1, gaveUp: 0, heldElsewhere: 1 });
      const r1 = await rowOf(wsSweep, R1);
      assert.equal(r1!.refetch![0].result, "downloaded");
      assert.ok(r1!.refetch![0].done_at);
      assert.equal(r1!.settled, true);
      assert.equal(r1!.refetch_claim_until, null);
      const r2 = await rowOf(wsSweep, R2);
      assert.equal(r2!.refetch![0].result, "already_newer");
      const r3 = await rowOf(wsSweep, R3);
      assert.deepEqual([r3!.refetch![0].attempts, r3!.refetch![0].done_at, r3!.refetch![0].last_error], [1, null, "failed: invented failure"]);
      assert.equal(r3!.settled, true);
      const r4 = await rowOf(wsSweep, R4);
      assert.deepEqual([r4!.refetch![0].attempts, r4!.refetch![0].done_at], [0, null]);
      const r5 = await rowOf(wsSweep, R5);
      assert.deepEqual([r5!.refetch![0].done_at, r5!.refetch_claim_by], [null, "the-other-mac"], "another sweep's claim is left alone");
      assert.equal(sweep.sweepWords(result), "captured 0/0, re-captured 0, late re-downloads 1/3, failed 1");
    });

    await check("phase 3: failures stop at 3 — the entry closes 'gave_up' on the third failed attempt and the next sweep makes no call for it", async () => {
      for (let i = 0; i < 2; i++) await sweep.runInvoiceSweep(String(wsSweep), { download: fakeDownload as never });
      const r3 = await rowOf(wsSweep, R3);
      assert.deepEqual([r3!.refetch![0].attempts, r3!.refetch![0].result], [3, "gave_up"]);
      calls.length = 0;
      const result = await sweep.runInvoiceSweep(String(wsSweep), { download: fakeDownload as never });
      assert.deepEqual(calls, [], "nothing left to download");
      assert.equal(result.phase3!.candidates, 0);
    });

    await check("phase 3: two sweeps at once (two Macs) download a due order once — the per-order claim; an unchanged download closes as 'unchanged'", async () => {
      const R6 = order(16);
      await insertInvoice(wsSweep, R6, { last_download_at: ago(3 * D), refetch: [entry("F", "refund", ago(H)), entry("G", "cancelled", ago(2 * H))] });
      answers.set(R6, "unchanged");
      calls.length = 0;
      const slow = async (opts: DownloadOpts) => {
        await new Promise((r) => setTimeout(r, 300));
        return fakeDownload(opts);
      };
      const [a, b] = await Promise.all([
        refetch.runRefetchPhase(String(wsSweep), { cap: 50, concurrency: 2, download: slow as never, claimBy: "mac-one" }),
        refetch.runRefetchPhase(String(wsSweep), { cap: 50, concurrency: 2, download: slow as never, claimBy: "mac-two" }),
      ]);
      assert.equal(calls.filter((c) => c.orderIds.includes(R6)).length, 1);
      assert.equal(a.refetched + b.refetched, 1);
      assert.equal(a.unchanged + b.unchanged, 1);
      const r6 = await rowOf(wsSweep, R6);
      assert.deepEqual(r6!.refetch!.map((e) => e.result), ["unchanged", "unchanged"], "a success closes every due entry of the order");
    });

    // ── phase 1: SO-6 and the failure memory ──
    const [Oa, Ob, Oc, Od, Oe, Of, Og, Oh, Oi] = [21, 22, 23, 24, 25, 26, 27, 28, 29].map(order);
    const shippedCancel = (days: number, extra: Record<string, unknown> = {}) => ({ status_key: "Cancelled", order_status_pre_cancellation: "Shipped", cancelled_date: ago(days * D), ...extra });
    await check("SO-6 selection: switched off (default) = today's selection exactly (delivered without a bundle); on = also cancelled after shipping ≥ 15 days ago with no bundle — not 10 days ago, not cancelled at Order Confirmed, not with a unit on its way, not with an invoices row; held back after 3 failures or a failure in the last 24 h", async () => {
      await insertUnits(wsCapture, Oa, [shippedCancel(16)]);
      await insertUnits(wsCapture, Ob, [shippedCancel(10)]);
      await insertUnits(wsCapture, Oc, [{ status_key: "Cancelled", order_status_pre_cancellation: "Order Confirmed", cancelled_date: ago(20 * D) }]);
      await insertUnits(wsCapture, Od, [shippedCancel(20, { invoice_capture_failures: 3, invoice_capture_failed_at: ago(5 * D) })]);
      await insertUnits(wsCapture, Oe, [shippedCancel(20, { invoice_capture_failures: 1, invoice_capture_failed_at: ago(2 * H) })]);
      await insertUnits(wsCapture, Of, [{ status_key: "Delivered", actual_delivery_date: ago(3 * D) }]);
      await insertUnits(wsCapture, Og, [{ status_key: "Delivered", actual_delivery_date: ago(3 * D), invoice_capture_failures: 1, invoice_capture_failed_at: ago(30 * H) }]);
      await insertUnits(wsCapture, Oh, [shippedCancel(20), { status_key: "Shipped" }]);
      await insertUnits(wsCapture, Oi, [shippedCancel(20)]);
      await insertInvoice(wsCapture, Oi);
      const old = ((await Order.distinct("order_id", { userId: wsCapture, status_key: /deliver/i, $or: [{ invoice_s3_key: { $in: [null, ""] } }, { invoice_s3_key: { $exists: false } }] })) as string[]).sort();
      const off = await sweep.captureCandidates(String(wsCapture), { cap: 200 });
      assert.equal(sweep.captureCancelledEnabled(), false);
      assert.deepEqual([...off.orderIds].sort(), old, "switch off: the same orders as before");
      assert.deepEqual([...off.orderIds].sort(), [Of, Og].sort());
      process.env.INVOICE_CAPTURE_CANCELLED = "1";
      try {
        const on = await sweep.captureCandidates(String(wsCapture), { cap: 200 });
        assert.deepEqual([...on.orderIds].sort(), [Oa, Of, Og].sort());
        assert.deepEqual([on.delivered, on.cancelled, on.heldBack], [2, 1, 2]);
      } finally {
        delete process.env.INVOICE_CAPTURE_CANCELLED;
      }
    });

    await check("failure memory: a failed phase-1 order gets invoice_capture_failures + 1 and invoice_capture_failed_at on every unit and is not taken again within 24 h; a 'no session' answer is not counted", async () => {
      const [second] = await insertUnits(wsCapture, Og, [{ status_key: "Delivered", actual_delivery_date: ago(3 * D) }]);
      calls.length = 0;
      const noSession = async (opts: DownloadOpts) => {
        calls.push({ orderIds: [...opts.orderIds], path: opts.path, force: opts.force });
        return opts.orderIds.map((orderId) => (orderId === Og ? { orderId, status: "no_session" as const } : { orderId, status: "failed" as const, error: "invented failure" }));
      };
      const r = await sweep.runInvoiceSweep(String(wsCapture), { download: noSession as never });
      assert.deepEqual(calls.map((c) => [c.path, c.force, [...c.orderIds].sort()]), [["sweep-capture", false, [Of, Og].sort()]]);
      assert.deepEqual([r.phase1.candidates, r.phase1.failed], [2, 1]);
      const f = await Order.collection.find({ userId: wsCapture, order_id: Of }).toArray();
      assert.ok(f.every((u) => u.invoice_capture_failures === 1 && u.invoice_capture_failed_at instanceof Date));
      const g = await Order.collection.find({ userId: wsCapture, order_id: Og }).toArray();
      assert.equal(g.find((u) => u.unit_id === second)?.invoice_capture_failures, undefined, "no session: not counted");
      const next = await sweep.captureCandidates(String(wsCapture), { cap: 200 });
      assert.deepEqual(next.orderIds, [Og]);
    });

    await check("failure memory counts Flipkart's side only: a failure of our own (ours — S3 / Mongo after Flipkart answered) is not counted; a later capture clears an order's memory", async () => {
      const [Oj, Ok] = [30, 31].map(order);
      await insertUnits(wsCapture, Oj, [{ status_key: "Delivered", actual_delivery_date: ago(3 * D) }]);
      await insertUnits(wsCapture, Ok, [{ status_key: "Delivered", actual_delivery_date: ago(3 * D), invoice_capture_failures: 2, invoice_capture_failed_at: ago(30 * H) }]);
      assert.deepEqual(
        sweep.failuresToRemember([
          { orderId: "X1", status: "failed", error: "invoice download for X1 answered HTTP 500" },
          { orderId: "X2", status: "failed", error: "S3 put …", ours: true },
          { orderId: "X3", status: "no_session" },
        ]),
        ["X1"]
      );
      const mixed = async (opts: DownloadOpts) =>
        opts.orderIds.map((orderId) =>
          orderId === Oj ? { orderId, status: "failed" as const, error: "raw PUT: ECONNRESET", ours: true } : orderId === Ok ? { orderId, status: "downloaded" as const, units: 1 } : { orderId, status: "no_session" as const }
        );
      const r = await sweep.runInvoiceSweep(String(wsCapture), { download: mixed as never });
      assert.ok(r.phase1.failed >= 1);
      for (const u of await Order.collection.find({ userId: wsCapture, order_id: Oj }).toArray()) assert.equal(u.invoice_capture_failures, undefined, "our own failure was counted");
      for (const u of await Order.collection.find({ userId: wsCapture, order_id: Ok }).toArray()) {
        assert.equal(u.invoice_capture_failures, undefined, "a capture did not clear the memory");
        assert.equal(u.invoice_capture_failed_at, undefined);
      }
    });

    // ── SO-4 ──
    await check("SO-4 chunker: invented lists of ~5,000 orders on ~1,800 accounts — no request over 2,000, no account in two requests, every order once; an account with 2,500 orders alone is split into requests of its own and named", () => {
      for (let round = 0; round < 5; round++) {
        const items: Array<{ orderId: string; accountId: string }> = [];
        for (let a = 0; a < 1800; a++) for (let k = 0, n = 1 + Math.floor(Math.random() * (round === 4 ? 3 : 5)); k < n; k++) items.push({ orderId: `O${a}-${k}`, accountId: `A${a}` });
        items.sort(() => Math.random() - 0.5);
        const { chunks, splitAccounts } = backfill.chunkOrdersByAccount(items);
        assert.ok(chunks.every((c) => c.length <= 2000 && c.length > 0));
        assert.deepEqual(splitAccounts, []);
        const owner = new Map(items.map((i) => [i.orderId, i.accountId]));
        const accountChunk = new Map<string, number>();
        chunks.forEach((c, i) => c.forEach((o) => {
          const a = owner.get(o)!;
          assert.ok(!accountChunk.has(a) || accountChunk.get(a) === i, `account ${a} in two requests`);
          accountChunk.set(a, i);
        }));
        assert.equal(chunks.flat().length, items.length);
        assert.equal(new Set(chunks.flat()).size, items.length);
      }
      const big = [...Array.from({ length: 2500 }, (_, i) => ({ orderId: `B${i}`, accountId: "BIG" })), { orderId: "S1", accountId: "SMALL" }];
      const out = backfill.chunkOrdersByAccount(big);
      assert.deepEqual(out.splitAccounts, ["BIG"]);
      assert.deepEqual(out.chunks.map((c) => c.length), [2000, 500, 1]);
    });

    await check("SO-4 canary rule: ok once an invoice_downloads document exists and the bundle's row names it (a warning when the Lambda built the archive not from the originals); fail on a failed task, on a download with no document (old worker), or after 3 minutes; wait otherwise", () => {
      const v = (s: Partial<import("./invoiceOriginalsBackfill.js").CanaryState>) => backfill.canaryVerdict({ taskStatus: "queued", download: null, row: null, elapsedMs: 10_000, waitMs: 180_000, ...s });
      assert.equal(v({}).state, "wait");
      assert.equal(v({ taskStatus: "in_progress", download: { _id: "d1" }, row: { stored_download_id: "" } }).state, "wait");
      const ok = v({ taskStatus: "success", download: { _id: "d1" }, row: { stored_download_id: "d1", processing_status: "pending" } });
      assert.deepEqual([ok.state, ok.warn], ["ok", undefined]);
      assert.equal(v({ taskStatus: "success", download: { _id: "d1" }, row: { stored_download_id: "d1", processing_status: "processed", originals_download_id: "d1", zip_source: "original" } }).warn, undefined);
      assert.match(v({ taskStatus: "success", download: { _id: "d1" }, row: { stored_download_id: "d1", processing_status: "processed", originals_download_id: "d1", zip_source: "split" } }).warn || "", /not deployed/);
      assert.equal(v({ taskStatus: "failed", taskError: "Not logged in" }).state, "fail");
      assert.match(v({ taskStatus: "success", download: null }).reason, /does not run sub-step 12/);
      assert.equal(v({ elapsedMs: 180_000 }).state, "fail");
      // The originals were refused (no orig/* grant before D1): fail at once, saying so.
      const denied = v({ taskStatus: "success", download: { _id: "d1", originals_error: "S3 put original orig/x: Access Denied" }, row: { stored_download_id: "" } });
      assert.equal(denied.state, "fail");
      assert.match(denied.reason, /originals were not stored.*orig\/\* grant/);
    });

    await check("SO-4 repeat gate (--compare): two downloads with the same sha256 set pass; a late document or a missing file does not", () => {
      const a = [{ sha256: "A".repeat(64) }, { sha256: "b".repeat(64) }];
      assert.deepEqual(backfill.compareDownloadShas(a, [{ sha256: "b".repeat(64) }, { sha256: "a".repeat(64) }]), { same: true, onlyFirst: 0, onlySecond: 0 });
      assert.deepEqual(backfill.compareDownloadShas(a, [...a, { sha256: "c".repeat(64) }]), { same: false, onlyFirst: 0, onlySecond: 1 });
      assert.deepEqual(backfill.compareDownloadShas(a, a.slice(0, 1)), { same: false, onlyFirst: 1, onlySecond: 0 });
      assert.equal(backfill.compareDownloadShas([], []).same, false, "two empty downloads prove nothing");
    });

    await check("SO-4 guard and selection (invented rows): an open request with purpose originals_backfill — from either Mac — is found, a finished one is not; the selection takes only rows without a stored download whose order has units", async () => {
      const ws = String(wsBackfill);
      const now = new Date();
      const base = { userId: wsBackfill, platform: "FLIPKART", source: "user", createdBy: "verify", message: "", tasks: [], counts: {}, queue: "verify-none", startedAt: null, finishedAt: null, createdAt: now, updatedAt: now };
      await OrderInvoiceRequest.collection.insertMany([
        { ...base, status: "in_progress", params: { purpose: "originals_backfill" }, systemId: "other-mac", host: "other-mac" },
        { ...base, status: "completed", params: { purpose: "originals_backfill" }, systemId: "this-mac", host: "this-mac" },
        { ...base, status: "queued", params: {}, systemId: "this-mac", host: "this-mac" },
      ] as never[]);
      const open = await backfill.openBackfillRequests(ws);
      assert.deepEqual(open.map((o) => o.host), ["other-mac"]);
      const [B1, B2, B3] = [31, 32, 33].map(order);
      await insertInvoice(wsBackfill, B1);
      await insertInvoice(wsBackfill, B2, { stored_download_id: "a".repeat(24) });
      await insertInvoice(wsBackfill, B3);
      await insertUnits(wsBackfill, B1, [{ status_key: "Delivered" }]);
      await insertUnits(wsBackfill, B2, [{ status_key: "Delivered" }]);
      const sel = await backfill.backfillSelection(ws);
      assert.deepEqual([sel.bundles, sel.stored, sel.withoutUnits], [3, 1, 1]);
      assert.deepEqual(sel.pending, [{ orderId: B1, accountId: String(account) }]);
    });
  } finally {
    capture.useInvoiceS3ForChecks(null);
    let removed = 0;
    for (const ws of workspaces) {
      removed += (await Invoice.collection.deleteMany({ userId: ws })).deletedCount;
      removed += (await InvoiceDownload.collection.deleteMany({ userId: ws })).deletedCount;
      removed += (await Order.collection.deleteMany({ userId: ws })).deletedCount;
      removed += (await OrderInvoiceRequest.collection.deleteMany({ userId: ws })).deletedCount;
    }
    let left = 0;
    for (const ws of workspaces) {
      left += await Invoice.collection.countDocuments({ userId: ws });
      left += await InvoiceDownload.collection.countDocuments({ userId: ws });
      left += await Order.collection.countDocuments({ userId: ws });
      left += await OrderInvoiceRequest.collection.countDocuments({ userId: ws });
    }
    left += await Invoice.collection.countDocuments({ order_id: { $regex: `^ODREFETCH${tag}` } });
    left += await Order.collection.countDocuments({ order_id: { $regex: `^ODREFETCH${tag}` } });
    console.log(`\ncleanup: ${removed} invented row(s) deleted (invoices, invoice_downloads, order_details, order_invoice_requests of ${workspaces.length} invented workspaces) — ${left} left; in-memory S3 held ${s3.objects.size} object(s), real S3 untouched`);
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
