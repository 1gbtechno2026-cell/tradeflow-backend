/**
 * A unit save keeps what other writers stored (data-quality plan sub-step 1), checked against the real database on rows
 * of an INVENTED workspace: a made-up TRADE_FLOW_USER_ID and @verify.invalid accounts, every row deleted at the end.
 * No Flipkart, no queue, no worker, no Redis.
 *
 *   · saveMappedUnit (the API path), with a mapped object built from a saved order trace (debug/order-traces, the
 *     newest Delivered API unit without an OTP or a refund; names, phones, addresses and ids replaced) or, when there
 *     is no trace, a realistic invented one:
 *       - a stored unit keeps its IMEI, downloaded flag, invoice path / key / id, delivery OTP and refund fields
 *       - a new unit gets null / false / ''
 *       - a non-blank OTP or refund value still replaces the stored one
 *       - our own COD checkout job still writes its blank card columns
 *   · scrapeAndSave (the page path) on a fake page: the same for a Delivered and a Cancelled page, a new unit, a new OTP
 *   · unitSetFor's table, the cancel worker's cancelled-unit write
 *   · the otp_new event table (recordUnitChanges; its event insert is captured in this process, nothing is written to
 *     order_unit_events) and the API diagnostics' comparison with a stored unit (diffAgainstScraper as saved)
 *   · the reapers: an open request of another system ("Mac-2"), quiet 10 minutes, is not taken on this machine, and
 *     this machine's is. On THROWAWAY collections with the real schemas (verify_us_<run>_…), dropped at the end: an
 *     open request in the real collections would be re-enqueued by a running worker on older code.
 *
 * Sub-step 10 (saving, events and watches), on the same invented workspace:
 *   · U6 Created By: invented tradeflowbatches / tradeflowjobs rows (createdBy "tester", a dry_run job) → cardFieldsFor
 *     gives created_by, a save writes it; a batch without a creator writes none; the x-requested-by header's reading
 *   · U7 the OTP event table (null → 111111, 111111 → 222222, 111111 → 111111)
 *   · U8 siblings: a stubbed APIRequestContext serving a saved 7-unit answer (debug/order-traces; the addresses and
 *     the customer's details replaced with invented ones, each unit's business / GST block and refund card removed,
 *     the seller block removed, the order, unit and item ids invented — Flipkart's own timeline sentences are kept) —
 *     an account holding 1 stored unit gets the other 6; a second call stores none; a sibling that fails is counted,
 *     never thrown
 *   · U9 the skipped-row cases through discoverUnit (order_index rows of the invented workspace) and the fetch invariant
 *   · U10 quiet events: inserted processed (after.backfill), and after the dashboard's minute poll (the dashboard API
 *     must be running — a control event of the invented workspace proves it drained) no alert exists for them;
 *     Processed → Completed gives one refund_completed event; the event's source / request kind (T10)
 *   · U12 + X5 the refund-watch selection table through resolveTargets (an invented logged_in platformids row with a
 *     made-up cookie; never read here)
 *
 * The sequence counter and the checkout-job lookup are replaced in this process only, so no real counter moves and no
 * real checkout job is read or written (the Created By case reads its own invented rows).
 *
 *   npm run test:unit-save          UNIT_SAVE_TRACE_DIR=<dir> reads the traces from another checkout
 *                                   UNIT_SAVE_NO_DASHBOARD=1 skips the 90-s wait for the dashboard (the quiet case
 *                                   then reports SKIPPED and the run fails)
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import mongoose, { Types } from "mongoose";
import type { Page } from "playwright";
import type { MappedOrderUnit, ShippingAddress } from "../services/orderApi.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

let passed = 0;
const failures: string[] = [];

async function check(name: string, fn: () => void | Promise<void>) {
  try {
    await fn();
    passed++;
    console.log(`  ok    ${name}`);
  } catch (err) {
    failures.push(name);
    console.log(`  FAIL  ${name}\n        ${err instanceof Error ? err.message.split("\n").slice(0, 8).join("\n        ") : err}`);
  }
}

const minutesAgo = (n: number) => new Date(Date.now() - n * 60_000);

/**
 * A saved 7-unit order answer (debug/order-traces), scrubbed: the addresses and the customer's details replaced with
 * invented ones, every unit's business / GST block and refund card removed, the seller block removed. The caller swaps
 * the order, unit and item ids for invented ones. Flipkart's timeline sentences stay (no personal field in them is read). Without a trace: an invented 7-unit answer in the same shape.
 */
function findSevenUnitAnswer(traceDir: string): { raw: unknown; orderId: string; unitIds: string[]; itemIds: string[]; source: string } {
  type View = { orderMetaData?: { orderId?: string }; units?: Record<string, Record<string, unknown>>; addresses?: unknown; customerInfo?: unknown };
  const viewOf = (raw: unknown): View | null => {
    const slots = (raw as { RESPONSE?: { slots?: Array<{ widget?: { type?: string; data?: { ssResponse?: { orderView?: View } } } }> } })?.RESPONSE?.slots || [];
    return slots.find((x) => x.widget?.type === "DUMMY_CARD_WIDGET")?.widget?.data?.ssResponse?.orderView || null;
  };
  const scrub = (raw: unknown) => {
    const copy = JSON.parse(JSON.stringify(raw));
    const v = viewOf(copy)!;
    const invented = { name: "Verify Unit", phoneNumber: "0000000000", pinCode: "000000", city: "Nowhere", state: "Nowhere", addressLine1: "1 Verify Street", addressLine2: "" };
    v.addresses = { SHIPPING: invented, BILLING: invented };
    v.customerInfo = { businessName: "", gstNumber: "", phoneNumber: "0000000000", emailId: "unit-save@verify.invalid" };
    delete (v as Record<string, unknown>).sellerDataBag;
    for (const u of Object.values(v.units || {})) {
      const meta = (u.metaData || {}) as Record<string, unknown>;
      meta.b2BInfo = null;
      u.metaData = meta;
      delete u.actionInfoDataBag;
    }
    // Only the order view goes on (the other slots are not read by the mapper).
    return { RESPONSE: { slots: [{ widget: { type: "DUMMY_CARD_WIDGET", data: { ssResponse: { orderView: v } } } }] } };
  };
  if (fs.existsSync(traceDir)) {
    const when = (d: string) => d.replace(/^[a-z]+-/, "");
    for (const r of fs.readdirSync(traceDir).filter((d) => /^(fetch|update)-/.test(d)).sort((a, b) => when(b).localeCompare(when(a)))) {
      for (const f of fs.readdirSync(path.join(traceDir, r)).filter((x) => x.startsWith("unit-") && x.endsWith(".json")).sort()) {
        let rec: { mode?: string; raw?: unknown };
        try {
          rec = JSON.parse(fs.readFileSync(path.join(traceDir, r, f), "utf8"));
        } catch {
          continue;
        }
        if (rec.mode !== "api" || !rec.raw) continue;
        const v = viewOf(rec.raw);
        const unitIds = Object.keys(v?.units || {});
        if (unitIds.length !== 7 || !v?.orderMetaData?.orderId) continue;
        const itemIds = [...new Set(Object.values(v.units || {}).map((x) => String(((x.metaData || {}) as Record<string, unknown>).itemId || "")).filter((x) => x.length >= 8))].sort((a, b) => b.length - a.length);
        return { raw: scrub(rec.raw), orderId: String(v.orderMetaData.orderId), unitIds: unitIds.sort((a, b) => b.length - a.length), itemIds, source: `the trace ${r}` };
      }
    }
  }
  const orderId = "OD000000000000000000";
  const unitIds = Array.from({ length: 7 }, (_, i) => `ITEMVERIFY0000${i}000`);
  const units = Object.fromEntries(
    unitIds.map((u) => [u, { metaData: { unitId: u, itemId: u.replace(/000$/, ""), title: "Verify Phone 5G", fsn: "VERIFYFSN0000001", quantity: 1, status: { key: "Delivered", text: "Delivered" } }, moneyDataBag: { amount: 12999 } }])
  );
  const view = { orderMetaData: { orderId, orderDate: Date.now() - 5 * 86_400_000 }, units, productDataBag: { VERIFYFSN0000001: { productBasicData: { title: "Verify Phone 5G", subTitle: "8 GB RAM" } } } };
  return { raw: scrub({ RESPONSE: { slots: [{ widget: { type: "DUMMY_CARD_WIDGET", data: { ssResponse: { orderView: view } } } }] } }), orderId, unitIds, itemIds: [] as string[], source: "an invented answer (no trace found)" };
}

async function main() {
  // A workspace that does not exist: set before config is read, so nothing here can act as the real one.
  const workspace = new Types.ObjectId();
  process.env.TRADE_FLOW_USER_ID = String(workspace);
  const { config } = await import("../config.js");
  const { Order } = await import("../models/Order.js");
  const { OrderIndex } = await import("../models/OrderIndex.js");
  const { OrderUnitEvent } = await import("../models/OrderUnitEvent.js");
  const { Counter } = await import("../models/Counter.js");
  const { CheckoutJob } = await import("../models/CheckoutJob.js");
  const { CheckoutBatch } = await import("../models/CheckoutBatch.js");
  const { PlatformId } = await import("../models/PlatformId.js");
  const { OrderUpdateRequest } = await import("../models/OrderUpdateRequest.js");
  const { CancelRequest } = await import("../models/CancelRequest.js");
  const { RescheduleRequest } = await import("../models/RescheduleRequest.js");
  const { KEEP_WHEN_BLANK, UNIT_INSERT_DEFAULTS, saveMappedUnit, scrapeAndSave, unitSetFor, requeueSkippedOld, setFetchWindowForChecks, discoverUnitForChecks, checkFetchInvariantForChecks } = await import("../services/orderFetch.js");
  const { addressHash, mapApiUnitToOrder, orderViewFromPageFetch } = await import("../services/orderApi.js");
  const { reapCandidatesFilter, saveMissingSiblings, eventOptionsFor, capabilitiesOf, updateCapabilitiesKey, UPDATE_WORKER_CAPABILITIES } = await import("../services/orderUpdateQueue.js");
  const { resolveTargets } = await import("../services/orderUpdate.js");
  const { refundWatchFilter } = await import("../services/orderLifecycle.js");
  const { cardFieldsFor, forgetBatchCreators } = await import("../services/orderLinks.js");
  const { requestedByHeader } = await import("../routes/jobs.js");
  const { cancelReapCandidatesFilter } = await import("../services/orderCancel.js");
  const { rescheduleReapCandidatesFilter } = await import("../services/orderReschedule.js");
  const { recordUnitChanges, snapshotUnit, updateSourceLabel } = await import("../services/orderUnitEvents.js");
  const { COMPARED_FIELDS, diffAgainstScraper } = await import("../services/orderApiTest.js");
  assert.equal(config.userId, String(workspace), "the made-up TRADE_FLOW_USER_ID is the one in use");
  // The other machine of the reaper case: "Mac-2", or "Mac" when this is Mac-2.
  const SECOND_MAC = config.systemId === "Mac-2" ? "Mac" : "Mac-2";

  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false, serverSelectionTimeoutMS: 15_000 });
  const userId = String(workspace);
  const run = new Types.ObjectId().toString().slice(-8);
  const throwaway: string[] = [];

  // nextSeq() must not move the real "order_fetch" counter: a new invented unit gets a negative id from here instead.
  let fakeSeq = -Date.now() * 100;
  let seqCalls = 0;
  (Counter as unknown as { findByIdAndUpdate: unknown }).findByIdAndUpdate = async () => {
    seqCalls++;
    return { seq: fakeSeq-- };
  };
  // cardFieldsFor() reads the checkout job that placed an order; here one invented COD job, no real read.
  const stamp = String(Date.now()).slice(-10);
  const orderId = (n: number) => `OD99${stamp}${String(n).padStart(6, "0")}`;
  const COD_ORDER = orderId(40);
  const codJob = { result: { transactionAmount: "12999", paymentFee: "", paymentFeeDetails: "" }, request: { paymentMode: "cod", cards: [] } };
  const realJobFindOne = CheckoutJob.findOne.bind(CheckoutJob);
  const CREATED_ORDERS = new Set([orderId(60), orderId(61)]);
  (CheckoutJob as unknown as { findOne: unknown }).findOne = (filter: Record<string, unknown>) => {
    // The Created By case reads its own invented job rows (userId = the invented workspace).
    if (CREATED_ORDERS.has(String(filter?.["result.flipkartOrderId"] || ""))) return realJobFindOne(filter as never);
    const value = filter?.["result.flipkartOrderId"] === COD_ORDER ? codJob : null;
    const q = { sort: () => q, select: () => q, lean: async () => value };
    return q;
  };

  const account = { id: new Types.ObjectId(), email: "unit-save@verify.invalid", cookies: [] as unknown[] };
  const ids = (n: number) => {
    const o = orderId(n);
    const itemId = `9${stamp}${String(n).padStart(5, "0")}`;
    return { orderId: o, itemId, unitId: `${itemId}000` };
  };
  const urlOf = (x: { orderId: string; itemId: string; unitId: string }) => `https://www.flipkart.com/order_details?order_id=${x.orderId}&item_id=${x.itemId}&unit_id=${x.unitId}`;
  const ADDRESS: ShippingAddress = { name: "Verify Unit", phone: "0000000000", pincode: "000000", city: "Nowhere", state: "", line1: "1 Verify Street", line2: "", text: "Verify Unit, 1 Verify Street, Nowhere 000000" };

  // ── The mapped object: from a saved trace when there is one ─────────────────────────────────────────────────────
  const traceDir = process.env.UNIT_SAVE_TRACE_DIR || path.resolve(repoRoot, config.orderTraceDir || "debug/order-traces");
  const fromTrace = ((): { mapped: MappedOrderUnit; source: string } | null => {
    if (!fs.existsSync(traceDir)) return null;
    const when = (d: string) => d.replace(/^[a-z]+-/, "");
    const runs = fs.readdirSync(traceDir).filter((d) => /^(fetch|update)-/.test(d)).sort((a, b) => when(b).localeCompare(when(a)));
    let read = 0;
    for (const r of runs) {
      const files = fs.readdirSync(path.join(traceDir, r)).filter((f) => f.startsWith("unit-") && f.endsWith(".json")).sort();
      for (let i = 0; i < files.length; i++) {
        if (++read > 400) return null;
        let rec: { mode?: string; unitId?: string; raw?: unknown };
        try {
          rec = JSON.parse(fs.readFileSync(path.join(traceDir, r, files[i]), "utf8"));
        } catch {
          continue;
        }
        if (rec.mode !== "api" || !rec.raw || !rec.unitId) continue;
        const view = orderViewFromPageFetch(rec.raw);
        if (!view) continue;
        const mapped = mapApiUnitToOrder(view, rec.unitId);
        if (mapped.status_key !== "Delivered" || mapped.delivery_otp || mapped.refund_status || mapped.refund_amount || mapped.refund_msg) continue;
        return { mapped, source: `${r}, unit file #${i + 1}` };
      }
    }
    return null;
  })();
  const invented: MappedOrderUnit = {
    order_id: ids(0).orderId,
    item_id: ids(0).itemId,
    unit_id: ids(0).unitId,
    order_url: "",
    order_date: new Date("2026-11-17T10:15:09.300+05:30"),
    product_name: "Verify Phone 5G (Black, 128 GB)",
    seller_name: "Verify Seller",
    quantity: 1,
    total_amount: "12999.00",
    unit_amount: "12999.00",
    delivery_date: new Date("2026-11-20T23:59:59+05:30"),
    actual_delivery_date: new Date("2026-11-20T18:00:00+05:30"),
    ewb_number: "",
    delivery_message: "Delivered on Nov 20",
    status_key: "Delivered",
    status_label: "Your item has been delivered",
    status_reason: null,
    business_name: "",
    business_gst_no: "",
    tracking_id: "",
    delivery_otp: null,
    tracking: [
      { date: "2026-11-17T10:15:39+05:30", stage: "DONE", status: "Order Confirmed", detailed_steps: [{ event: "paymentApprovalStep", progress: [{ date: "2026-11-17T10:15:39+05:30", remark: "Your Order has been placed.", location: null }], step_text: "Your Order has been placed." }] },
      { date: "2026-11-18T09:00:00+05:30", stage: "DONE", status: "Shipped", detailed_steps: [{ event: "itemShippedStep", progress: [{ date: "2026-11-18T09:00:00+05:30", remark: "Your item has been shipped.", location: null }], step_text: "Your item has been shipped." }] },
      { date: "2026-11-20T18:00:00+05:30", stage: "DONE", status: "Delivered", detailed_steps: [{ event: "itemDeliveredStep", progress: [{ date: "2026-11-20T18:00:00+05:30", remark: "Your item has been delivered", location: null }], step_text: "Your item has been delivered" }] },
    ],
    last_tracking_step: "Delivered · 2026-11-20T18:00:00+05:30",
    billing_phone_number: null,
    billing_address_name: null,
    billing_address_pincode: null,
    shipping_address: null,
    shipping_address_hash: "",
    supercoin_amount_applied: 0,
    cash_on_delivery: false,
    refund_status: null,
    refund_amount: null,
    refund_msg: null,
    cancelled_date: null,
    cancelled_by_user: false,
    cancelled_by: "",
    cancel_reason_text: null,
    order_status_pre_cancellation: null,
    shipment_partner_name: "Ekart Logistics",
    api_extra: { listing_price: "14999.00", selling_price: "12999.00", payment_mode: "PREPAID", adjustments: [], fsn: "VERIFYFSN0000001", listing_id: "", seller_id: "", invoice_eligible: true, customer_email: "" },
  };
  const base = fromTrace?.mapped || invented;
  /** The base unit under another identity, with every name, phone, address and free-text step replaced. */
  const mappedFor = (n: number, over: Partial<MappedOrderUnit> = {}): MappedOrderUnit => {
    const x = ids(n);
    return {
      ...base,
      order_id: x.orderId,
      item_id: x.itemId,
      unit_id: x.unitId,
      order_url: urlOf(x),
      business_name: "",
      business_gst_no: "",
      tracking_id: "",
      ewb_number: "",
      billing_phone_number: ADDRESS.phone,
      billing_address_name: ADDRESS.name,
      billing_address_pincode: ADDRESS.pincode,
      shipping_address: { ...ADDRESS },
      shipping_address_hash: addressHash(ADDRESS),
      tracking: base.tracking.map((s) => ({ ...s, detailed_steps: s.detailed_steps.map((st) => ({ ...st, step_text: s.status, progress: st.progress.map((p) => ({ ...p, remark: s.status, location: null })) })) })),
      api_extra: { ...base.api_extra, customer_email: account.email, seller_id: "", listing_id: "" },
      ...over,
    };
  };

  /** A unit as the invoice pipeline, the capture and an earlier OFD read left it. */
  const STORED = {
    imei: "999000000000017",
    is_invoice_downloaded: true,
    invoice_path: "/verify/invoices/unit.pdf",
    invoice_size: 123456,
    invoice_parts: [{ type: "product", pages: 1, invoiceNumber: "VERIFY-INV-1", bytes: 1000 }],
    delivery_otp: "123456",
    refund_status: "Processed",
    refund_amount: "1999.00",
    refund_msg: "Total refund - ₹1,999",
  };
  const insertStored = async (n: number, over: Record<string, unknown> = {}) => {
    const x = ids(n);
    const now = new Date();
    const doc = {
      _id: new Types.ObjectId(),
      userId: workspace,
      platformAccountId: account.id,
      id: fakeSeq--,
      platform_email: account.email,
      platform: "FLIPKART",
      order_id: x.orderId,
      item_id: x.itemId,
      unit_id: x.unitId,
      order_url: urlOf(x),
      product_name: "Stored name",
      status_key: "Out for delivery",
      status_label: "Your item is out for delivery",
      ...STORED,
      invoice_s3_key: `raw/${userId}/${x.orderId}.pdf`,
      invoice_downloaded_at: minutesAgo(600),
      // Written by the dashboard's pipeline; not declared in Trade Flow's model, so stored through the driver.
      invoice_id: new Types.ObjectId(),
      invoice_data: { imei: STORED.imei, product_count: 1 },
      invoice_extracted_at: minutesAgo(590),
      cancelled_from_bae: true,
      card_name: "Stored Card",
      card_number: "XXXXXXXXXXXX1111",
      parent_card_number: "XXXXXXXXXXXX2222",
      last_fetch: minutesAgo(900),
      createdAt: minutesAgo(1000),
      updatedAt: minutesAgo(900),
      ...over,
    };
    await Order.collection.insertOne(doc);
    return doc;
  };
  const row = async (n: number) => (await Order.collection.findOne({ userId: workspace, unit_id: ids(n).unitId })) as Record<string, unknown> | null;
  // Compared as JSON: an ObjectId as its hex, a Date as its ISO time.
  const keptAsStored = (got: Record<string, unknown> | null, stored: Record<string, unknown>, keys: string[]) => {
    assert.ok(got, "the unit is there");
    for (const k of keys) {
      assert.ok(stored[k] !== undefined, `${k} was stored`);
      assert.equal(JSON.stringify(got[k]), JSON.stringify(stored[k]), `${k} kept`);
    }
  };
  const PIPELINE_KEYS = ["imei", "is_invoice_downloaded", "invoice_path", "invoice_s3_key", "invoice_id", "invoice_data", "invoice_size", "invoice_parts", "invoice_downloaded_at"];
  const CAPTURED_KEYS = ["delivery_otp", "refund_status", "refund_amount", "refund_msg"];

  console.log(`\ninvented workspace ${userId} · system "${config.systemId}" · mapped unit from ${fromTrace ? `the trace ${fromTrace.source}` : "an invented object (no trace found)"}\n`);

  try {
    console.log("unitSetFor — the rule");
    await check("KEEP_WHEN_BLANK is the OTP and the three refund fields; the insert defaults are false / '' / null and frozen", () => {
      assert.deepEqual([...KEEP_WHEN_BLANK], ["delivery_otp", "refund_status", "refund_amount", "refund_msg"]);
      assert.deepEqual({ ...UNIT_INSERT_DEFAULTS }, { is_invoice_downloaded: false, invoice_path: "", imei: null });
      assert.ok(Object.isFrozen(UNIT_INSERT_DEFAULTS));
    });
    await check("null, '' and undefined are dropped for those four keys", () => {
      assert.deepEqual(unitSetFor({ delivery_otp: null, refund_status: "", refund_amount: undefined, refund_msg: null }), {});
      assert.deepEqual(unitSetFor({ delivery_otp: "", status_key: "Delivered" }), { status_key: "Delivered" });
    });
    await check("a non-blank value replaces ('0.00' and a code included)", () => {
      assert.deepEqual(unitSetFor({ delivery_otp: "654321", refund_status: "refund", refund_amount: "0.00", refund_msg: "Total refund - ₹0" }), { delivery_otp: "654321", refund_status: "refund", refund_amount: "0.00", refund_msg: "Total refund - ₹0" });
    });
    await check("every other key passes as it is, blanks included (a save may still clear its own fields)", () => {
      const fields = { status_reason: null, product_name: "", tracking: [], cancelled_date: null, card_name: "" };
      assert.deepEqual(unitSetFor(fields), fields);
    });
    await check("the input is not changed", () => {
      const fields = { delivery_otp: null, status_key: "Cancelled" };
      unitSetFor(fields);
      assert.deepEqual(fields, { delivery_otp: null, status_key: "Cancelled" });
    });

    console.log("\nsaveMappedUnit — the API path");
    const mappedA = mappedFor(1);
    await check("the mapped object carries none of the protected fields (no OTP, no refund, no IMEI / flag / path)", () => {
      assert.equal(mappedA.status_key, "Delivered");
      for (const k of CAPTURED_KEYS) assert.equal((mappedA as unknown as Record<string, unknown>)[k], null, `${k} is null`);
      for (const k of ["imei", "is_invoice_downloaded", "invoice_path"]) assert.ok(!(k in mappedA), `${k} is not on it`);
    });
    const storedA = await insertStored(1);
    await check("a stored unit keeps its IMEI, flag, invoice path / key / id, OTP and refund fields; the rest is written", async () => {
      await saveMappedUnit(userId, account, mappedA, { applyCalendar: false });
      const got = await row(1);
      keptAsStored(got, storedA, [...PIPELINE_KEYS, ...CAPTURED_KEYS, "cancelled_from_bae", "id"]);
      assert.equal(got!.status_key, "Delivered");
      assert.equal(got!.product_name, mappedA.product_name);
      assert.equal(got!.fetch_source, "api");
      assert.ok((got!.last_fetch as Date) > storedA.last_fetch, "last_fetch moved");
      assert.equal(seqCalls, 0, "an existing unit keeps its id");
    });
    await check("a non-blank OTP and refund replace the stored ones; the IMEI and flag still stay", async () => {
      await saveMappedUnit(userId, account, mappedFor(1, { delivery_otp: "654321", refund_status: "refund", refund_amount: "2499.00", refund_msg: "Total refund - ₹2,499" }), { applyCalendar: false });
      const got = await row(1);
      assert.deepEqual([got!.delivery_otp, got!.refund_status, got!.refund_amount, got!.refund_msg], ["654321", "refund", "2499.00", "Total refund - ₹2,499"]);
      keptAsStored(got, storedA, PIPELINE_KEYS);
    });
    await check("a new unit gets imei null, flag false, path '', no OTP / refund, cancelled_from_bae false — and an id from the (stubbed) counter", async () => {
      await saveMappedUnit(userId, account, mappedFor(2), { applyCalendar: false });
      const got = await row(2);
      assert.ok(got, "inserted");
      assert.deepEqual(
        [got.imei, got.is_invoice_downloaded, got.invoice_path, got.invoice_s3_key, got.delivery_otp, got.refund_status, got.refund_amount, got.refund_msg, got.cancelled_from_bae],
        [null, false, "", "", null, null, null, null, false]
      );
      assert.equal(got.status_key, "Delivered");
      assert.equal(seqCalls, 1, "one id taken");
      assert.ok((got.id as number) < 0, "the invented id, not the real counter's");
    });
    await check("our own COD job still writes its blank card columns (cardFieldsFor), the OTP and IMEI stay", async () => {
      const stored = await insertStored(3, { order_id: COD_ORDER });
      await saveMappedUnit(userId, account, mappedFor(3, { order_id: COD_ORDER }), { applyCalendar: false });
      const got = await row(3);
      assert.deepEqual([got!.card_name, got!.card_number, got!.parent_card_number], ["", "", ""]);
      assert.deepEqual([got!.is_bae_order, got!.payment_mode, got!.cash_on_delivery], [true, "cod", true]);
      keptAsStored(got, stored, ["imei", "is_invoice_downloaded", "delivery_otp", "refund_amount"]);
    });

    console.log("\nscrapeAndSave — the page path (a fake page, no browser)");
    // "See all updates" as the page reads it: stage, stage day, then each step's text and its time.
    const placed = "Order Confirmed Tue, 17th Nov '26 Your Order has been placed. Tue, 17th Nov '26 - 10:15 am ";
    const shipped = "Shipped Wed, 18th Nov '26 Your item has been shipped. Wed, 18th Nov '26 - 9:00 am ";
    const ofd = "Out For Delivery Fri, 20th Nov '26 Your item is out for delivery Fri, 20th Nov '26 - 8:00 am ";
    const tracks = {
      delivered: `${placed}${shipped}${ofd}Delivered Fri, 20th Nov '26 Your item has been delivered Fri, 20th Nov '26 - 6:00 pm`,
      cancelled: `${placed}Cancelled Wed, 18th Nov '26 Your order was cancelled as per your request. Wed, 18th Nov '26 - 9:00 am`,
      ofd: `${placed}${shipped}${ofd}`.trim(),
    };
    const pageFor = (n: number, kind: keyof typeof tracks, otp = "") => {
      const x = ids(n);
      const message = kind === "delivered" ? "Delivered on Nov 20" : kind === "ofd" ? "Out For Delivery" : "";
      const pageData = {
        url: urlOf(x), unitId: x.unitId, itemId: x.itemId,
        productName: "Verify Phone 5G (Black, 128 GB)", sellerName: "Verify Seller", gstSellerName: "", gstNumber: "", ewbNumber: "",
        totalAmount: "12999", quantity: 1, supercoin: 0, deliveryOtp: otp, billingPhone: "", billingName: "", pincode: "",
        deliveryAddressText: "", deliveryMessage: message, cashOnDelivery: false,
        pageText: `${message} Verify Phone 5G (Black, 128 GB) Seller: Verify Seller ${otp ? `OTP ${otp} ` : ""}Price details Total amount ₹12,999`,
      };
      const locator: Record<string, unknown> = {};
      Object.assign(locator, {
        first: () => locator, filter: () => locator, locator: () => locator,
        isVisible: async () => false, count: async () => 1,
        scrollIntoViewIfNeeded: async () => undefined, click: async () => undefined, waitFor: async () => undefined,
      });
      const page = {
        goto: async () => null,
        waitForFunction: async () => null,
        url: () => urlOf(x),
        getByText: () => locator,
        locator: () => locator,
        // evalOnPage sends each page function as source text: answer the three readers, accept the rest.
        evaluate: async (_fn: unknown, payload: { fnSource?: string }) => {
          const src = String(payload?.fnSource || "");
          if (src.includes("gstBlock")) return pageData;
          if (src.includes("status_display")) return null;
          if (src.includes("your order has been placed") && src.includes("role=")) return tracks[kind];
          return true;
        },
      };
      return { page: page as unknown as Page, card: { orderId: x.orderId, itemId: x.itemId, unitId: x.unitId, orderUrl: urlOf(x), amount: "" } };
    };
    await check("Delivered page: a stored unit keeps its IMEI, flag, path / key / id, OTP and refund fields", async () => {
      const stored = await insertStored(11);
      const { page, card } = pageFor(11, "delivered");
      const res = await scrapeAndSave(page, userId, account, card, { applyCalendar: false });
      assert.equal(res.statusKey, "Delivered");
      const got = await row(11);
      keptAsStored(got, stored, [...PIPELINE_KEYS, ...CAPTURED_KEYS, "cancelled_from_bae", "id"]);
      assert.deepEqual([got!.status_key, got!.fetch_source, got!.product_name], ["Delivered", "scrape", "Verify Phone 5G (Black, 128 GB)"]);
    });
    await check("Cancelled page (the page path writes null for its OTP): the captured OTP stays", async () => {
      const stored = await insertStored(12);
      const { page, card } = pageFor(12, "cancelled");
      await scrapeAndSave(page, userId, account, card, { applyCalendar: false });
      const got = await row(12);
      assert.equal(got!.status_key, "Cancelled");
      keptAsStored(got, stored, ["delivery_otp", "imei", "is_invoice_downloaded", "invoice_path", "refund_status"]);
    });
    await check("Out for delivery page with a new OTP: it replaces the stored one", async () => {
      await insertStored(13, { delivery_otp: "111111" });
      const { page, card } = pageFor(13, "ofd", "778899");
      await scrapeAndSave(page, userId, account, card, { applyCalendar: false });
      const got = await row(13);
      assert.deepEqual([got!.status_key, got!.delivery_otp, got!.imei], ["Out for delivery", "778899", STORED.imei]);
    });
    await check("a new unit from the page gets imei null, flag false, path '', no OTP / refund", async () => {
      const before = seqCalls;
      const { page, card } = pageFor(14, "delivered");
      await scrapeAndSave(page, userId, account, card, { applyCalendar: false });
      const got = await row(14);
      assert.ok(got, "inserted");
      assert.deepEqual([got.imei, got.is_invoice_downloaded, got.invoice_path, got.delivery_otp, got.refund_status, got.refund_amount, got.refund_msg, got.cancelled_from_bae], [null, false, "", null, null, null, null, false]);
      assert.equal(seqCalls, before + 1);
    });

    console.log("\nthe other writers");
    await check("the cancel worker's cancelled-unit write (orderCancel.ts applyToOrder) has no delivery_otp", () => {
      const src = fs.readFileSync(path.join(repoRoot, "src/services/orderCancel.ts"), "utf8");
      const start = src.indexOf("async function applyToOrder(");
      assert.ok(start >= 0, "applyToOrder is there");
      const body = src.slice(start, src.indexOf("\n}\n", start));
      assert.ok(/status_key: "Cancelled"/.test(body), "it is the cancelled-unit write");
      assert.ok(!/delivery_otp\s*:/.test(body), "no delivery_otp key in it");
    });

    console.log("\nthe otp_new event (orderUnitEvents.ts recordUnitChanges) — events captured here, none written");
    // The dashboard watches order_unit_events, so the insert is replaced in this process; no address hash, so no unit write.
    const captured: Array<Record<string, unknown>> = [];
    const realInsertMany = OrderUnitEvent.insertMany;
    (OrderUnitEvent as unknown as { insertMany: unknown }).insertMany = async (docs: Array<Record<string, unknown>>) => (captured.push(...docs), docs);
    try {
      const eventUnit = ids(80);
      const snap = (otp: string) => ({ orderId: eventUnit.orderId, email: account.email, productName: "Verify Phone", status_key: "Out for delivery", delivery_otp: otp, shipping_address: null, shipping_address_hash: "", address_first_seen_at: null });
      const table: Array<[string, ReturnType<typeof snap> | null, ReturnType<typeof snap>, boolean]> = [
        ["a first code on a new unit", null, snap("123456"), true],
        ["a first code after none", snap(""), snap("123456"), true],
        ["the same code (read again, or kept through a blank answer)", snap("123456"), snap("123456"), false],
        ["a different code (a second delivery attempt)", snap("123456"), snap("654321"), true],
        ["no code now", snap("123456"), snap(""), false],
      ];
      for (const [name, before, after, fires] of table) {
        await check(`${name} → ${fires ? "one otp_new with the new code" : "no event"}`, async () => {
          captured.length = 0;
          const changes = await recordUnitChanges(userId, eventUnit.unitId, before, after, "verify-request");
          assert.equal(changes.otpNew, fires);
          const otpEvents = captured.filter((e) => e.type === "otp_new");
          assert.equal(otpEvents.length, fires ? 1 : 0, "otp_new events");
          if (fires) assert.deepEqual(otpEvents[0].after, { otp: after.delivery_otp, status: after.status_key });
          assert.equal(captured.length, otpEvents.length, "no other event");
        });
      }
    } finally {
      (OrderUnitEvent as unknown as { insertMany: unknown }).insertMany = realInsertMany;
    }

    console.log("\nthe API diagnostics against a stored unit (orderApiTest.ts diffAgainstScraper: Test tab, orders:replay, orders:api-bench)");
    await check("a blank OTP / refund in the answer against a kept one: the same as saved, different between two mappings; another code still differs", () => {
      const storedRow: Record<string, unknown> = { ...Object.fromEntries(COMPARED_FIELDS.map((k) => [k, mappedA[k]])), delivery_otp: "123456", refund_status: "refund" };
      const differing = (asSaved: boolean, m = mappedA) => diffAgainstScraper(m, storedRow, asSaved).filter((d) => !d.same).map((d) => d.field).sort();
      assert.deepEqual(differing(true), []);
      assert.deepEqual(differing(false), ["delivery_otp", "refund_status"]);
      assert.deepEqual(differing(true, { ...mappedA, delivery_otp: "654321" }), ["delivery_otp"]);
    });

    console.log("\nthe reapers — only this system's quiet requests (throwaway collections)");
    const unit = ids(90);
    const kinds = [
      {
        kind: "update",
        model: OrderUpdateRequest,
        filter: reapCandidatesFilter,
        doc: (systemId: string, quietMinutes: number) => ({
          userId: workspace, source: "manual", selectionMethod: "unit_ids", params: {}, parentId: "", systemId, deliveredOrderIds: [], createdBy: "verify", status: "queued", mode: "api", windows: 1,
          accounts: [{ email: "reap@verify.invalid", platformId: null, units: [{ orderId: unit.orderId, itemId: unit.itemId, unitId: unit.unitId, orderUrl: "" }], total: 1, status: "queued", done: 0, failed: 0, statusChanged: 0, otpNew: 0, via: "", message: "", startedAt: null, finishedAt: null }],
          logs: [], startedAt: null, finishedAt: null, createdAt: minutesAgo(quietMinutes + 1), updatedAt: minutesAgo(quietMinutes),
        }),
      },
      {
        kind: "cancel",
        model: CancelRequest,
        filter: cancelReapCandidatesFilter,
        doc: (systemId: string, quietMinutes: number) => ({
          userId: workspace, type: "unit_ids", input: [unit.unitId], dryRun: false, urgent: false, createdBy: "verify", retryOf: "", source: "", systemId, stoppedBy: "", stoppedAt: null, status: "queued",
          units: [{ orderId: unit.orderId, unitId: unit.unitId, itemId: unit.itemId, email: "reap@verify.invalid", status: "queued", message: "", startedAt: null, finishedAt: null }], orders: [], rejected: [],
          finishedAt: null, createdAt: minutesAgo(quietMinutes + 1), updatedAt: minutesAgo(quietMinutes),
        }),
      },
      {
        kind: "reschedule",
        model: RescheduleRequest,
        filter: rescheduleReapCandidatesFilter,
        doc: (systemId: string, quietMinutes: number) => ({
          userId: workspace, type: "unit_ids", input: [unit.unitId], date: "2026-12-01", createdBy: "verify", retryOf: "", source: "", systemId, stoppedBy: "", stoppedAt: null, confirm: false, status: "queued",
          units: [{ orderId: unit.orderId, unitId: unit.unitId, itemId: unit.itemId, email: "reap@verify.invalid", status: "queued", message: "", startedAt: null, finishedAt: null }], rejected: [],
          finishedAt: null, createdAt: minutesAgo(quietMinutes + 1), updatedAt: minutesAgo(quietMinutes),
        }),
      },
    ] as const;
    for (const k of kinds) {
      await check(`${k.kind}: "${SECOND_MAC}"'s open request quiet 10 min is not taken on "${config.systemId}"; this machine's is; a fresh one and an unstamped one are not`, async () => {
        const name = `verify_us_${run}_${k.model.collection.collectionName}`;
        throwaway.push(name);
        const T = mongoose.model(`VerifyUs${run}${k.kind}`, k.model.schema, name);
        const otherMac = { _id: new Types.ObjectId(), ...k.doc(SECOND_MAC, 10) };
        const mine = { _id: new Types.ObjectId(), ...k.doc(config.systemId, 10) };
        const fresh = { _id: new Types.ObjectId(), ...k.doc(config.systemId, 1) };
        const unstamped = { _id: new Types.ObjectId(), ...k.doc("", 10) };
        await T.collection.insertMany([otherMac, mine, fresh, unstamped]);
        const filter = k.filter();
        assert.equal(filter.systemId, config.systemId, "the filter names this machine");
        const taken = (await T.find(filter).select("_id").lean()).map((r) => String(r._id));
        assert.deepEqual(taken, [String(mine._id)], `taken here: ${taken.length}`);
        const onSecondMac = (await T.find(k.filter(Date.now(), SECOND_MAC)).select("_id").lean()).map((r) => String(r._id));
        assert.deepEqual(onSecondMac, [String(otherMac._id)], "the second Mac takes its own");
      });
    }

    // ══ sub-step 10 ══════════════════════════════════════════════════════════════════════════════════════════════

    console.log("\nU6 — Created By (orderLinks.ts cardFieldsFor ← the batch's createdBy; routes/jobs.ts x-requested-by)");
    const BATCH_WITH = `verify-us-${run}-with`;
    const BATCH_WITHOUT = `verify-us-${run}-without`;
    await check("the x-requested-by header: a plain name, a percent-encoded one, blanks and control characters", () => {
      assert.equal(requestedByHeader("tester"), "tester");
      assert.equal(requestedByHeader(encodeURIComponent("Tëster Ünïcode")), "Tëster Ünïcode");
      assert.equal(requestedByHeader("  "), "");
      assert.equal(requestedByHeader(undefined), "");
      assert.equal(requestedByHeader("a%ZZb"), "a%ZZb", "not a valid encoding: kept as sent");
      assert.equal(requestedByHeader("x".repeat(300)).length, 120);
      assert.equal(requestedByHeader("bad\u0007name"), "badname");
    });
    await check("an invented batch (createdBy 'tester') and its job: cardFieldsFor gives created_by 'tester', a save writes it on the unit; a batch without a creator gives none", async () => {
      // A dry_run job and a completed batch: no worker, reaper or reconciler takes either. Both models key userId as a string.
      await CheckoutBatch.collection.insertMany([
        { batchId: BATCH_WITH, userId, systemId: "verify", batchStatus: "completed", createdBy: "tester", createdAt: new Date(), updatedAt: new Date() },
        { batchId: BATCH_WITHOUT, userId, systemId: "verify", batchStatus: "completed", createdBy: "", createdAt: new Date(), updatedAt: new Date() },
      ] as never[]);
      await CheckoutJob.collection.insertMany([
        { userId, batchId: BATCH_WITH, email: account.email, status: "dry_run", request: { paymentMode: "cod", cards: [] }, result: { flipkartOrderId: orderId(60) }, createdAt: new Date(), updatedAt: new Date() },
        { userId, batchId: BATCH_WITHOUT, email: account.email, status: "dry_run", request: { paymentMode: "cod", cards: [] }, result: { flipkartOrderId: orderId(61) }, createdAt: new Date(), updatedAt: new Date() },
      ] as never[]);
      forgetBatchCreators();
      const withCreator = await cardFieldsFor(userId, account.email, orderId(60));
      assert.equal(withCreator.created_by, "tester");
      assert.equal(withCreator.payment_mode, "cod", "the job's own columns are still there");
      const without = await cardFieldsFor(userId, account.email, orderId(61));
      assert.ok(!("created_by" in without), "no creator → no created_by key (a $set leaves the stored value)");
      await saveMappedUnit(userId, account, mappedFor(60, { order_id: orderId(60) }), { applyCalendar: false });
      await saveMappedUnit(userId, account, mappedFor(61, { order_id: orderId(61) }), { applyCalendar: false });
      assert.equal((await row(60))!.created_by, "tester");
      assert.ok(!(await row(61))!.created_by, "past orders stay blank");
    });

    console.log("\nU7 — the OTP event on every code change (the plan's table)");
    {
      const captured: Array<Record<string, unknown>> = [];
      const realInsertMany = OrderUnitEvent.insertMany;
      (OrderUnitEvent as unknown as { insertMany: unknown }).insertMany = async (docs: Array<Record<string, unknown>>) => (captured.push(...docs), docs);
      try {
        const u = ids(81);
        const snap = (otp: string | null) => ({ orderId: u.orderId, email: account.email, productName: "Verify Phone", status_key: "ofd", delivery_otp: otp || "", shipping_address: null, shipping_address_hash: "", address_first_seen_at: null });
        for (const [b, a, n] of [[null, "111111", 1], ["111111", "222222", 1], ["111111", "111111", 0]] as const) {
          await check(`${b ?? "null"} → ${a}: ${n} otp_new`, async () => {
            captured.length = 0;
            const changes = await recordUnitChanges(userId, u.unitId, b === null ? snap(null) : snap(b), snap(a), "verify-request");
            assert.equal(captured.filter((e) => e.type === "otp_new").length, n);
            assert.equal(changes.otpNew, n === 1);
          });
        }
      } finally {
        (OrderUnitEvent as unknown as { insertMany: unknown }).insertMany = realInsertMany;
      }
    }

    console.log("\nU10 + X6 — refund_completed and the one late-document detection (events captured here, none written)");
    {
      const captured: Array<Record<string, unknown>> = [];
      const realInsertMany = OrderUnitEvent.insertMany;
      (OrderUnitEvent as unknown as { insertMany: unknown }).insertMany = async (docs: Array<Record<string, unknown>>) => (captured.push(...docs), docs);
      try {
        const u = ids(82);
        const late = (over: Record<string, unknown>) => ({ unit_id: u.unitId, order_id: u.orderId, status_key: "Cancelled", order_status_pre_cancellation: "Shipped", cancelled_date: minutesAgo(3000), payment_mode: "", tracking: [] as Array<{ status: string; stage: string; date: string }>, ...over });
        const snap = (over: Record<string, unknown>, money: Record<string, unknown> = {}) => ({
          orderId: u.orderId, email: account.email, productName: "Verify Phone", status_key: "Cancelled", delivery_otp: "", shipping_address: null, shipping_address_hash: "", address_first_seen_at: null,
          refund_amount: null, refund_date: null, refund_kind: null, ...money, late: late(over),
        });
        const refundEvents = () => captured.filter((e) => e.type === "refund_completed");
        await check("Processed → Completed: one refund_completed event (amount, date, kind), refundCompleted true, one 'refund' moment", async () => {
          captured.length = 0;
          const paid = new Date("2026-11-20T10:00:00+05:30");
          const changes = await recordUnitChanges(userId, u.unitId, snap({ refund_status: "Processed" }), snap({ refund_status: "Completed" }, { refund_amount: "1999.00", refund_date: paid, refund_kind: "refund" }), "verify-request", { requestSource: "schedule", sourceLabel: "Update order details (6-hour sweep)" });
          assert.equal(changes.refundCompleted, true);
          assert.equal(refundEvents().length, 1);
          assert.deepEqual(refundEvents()[0].after, { amount: "1999.00", date: paid, kind: "refund" });
          assert.deepEqual(changes.lateDocuments.map((m) => m.reason), ["refund"], "the ending was seen before; the refund is the moment");
          assert.deepEqual([refundEvents()[0].source, refundEvents()[0].requestSource, refundEvents()[0].sourceLabel, refundEvents()[0].processed], ["update", "schedule", "Update order details (6-hour sweep)", false]);
        });
        await check("Completed → Completed: nothing; Processed with a DONE Refund stage: not completed (the card's status wins); a DONE stage on a unit with no refund status: completed", async () => {
          captured.length = 0;
          const done = [{ status: "Refund", stage: "DONE", date: "2026-11-20T10:00:00+05:30" }];
          let c = await recordUnitChanges(userId, u.unitId, snap({ refund_status: "Completed" }), snap({ refund_status: "Completed" }), "verify-request");
          assert.deepEqual([c.refundCompleted, c.lateDocuments.length, refundEvents().length], [false, 0, 0]);
          c = await recordUnitChanges(userId, u.unitId, snap({ refund_status: null }), snap({ refund_status: "Processed", tracking: done }), "verify-request");
          assert.deepEqual([c.refundCompleted, c.lateDocuments.length, refundEvents().length], [false, 0, 0]);
          c = await recordUnitChanges(userId, u.unitId, snap({ refund_status: null }), snap({ refund_status: null, tracking: done }), "verify-request");
          assert.deepEqual([c.refundCompleted, c.lateDocuments.map((m) => m.reason), refundEvents().length], [true, ["refund"], 1]);
        });
        await check("Shipped → Cancelled (invoiced): one 'cancelled' moment, no refund event; cancelled before shipping: none", async () => {
          captured.length = 0;
          const shipped = { ...snap({ status_key: "Shipped" }), status_key: "Shipped" };
          let c = await recordUnitChanges(userId, u.unitId, shipped, snap({}), "verify-request");
          assert.deepEqual([c.stageChanged, c.lateDocuments.map((m) => m.reason), refundEvents().length], [true, ["cancelled"], 0]);
          c = await recordUnitChanges(userId, u.unitId, { ...snap({ status_key: "Approved" }), status_key: "Approved" }, snap({ order_status_pre_cancellation: "Order Confirmed" }), "verify-request");
          assert.deepEqual(c.lateDocuments, []);
        });
        await check("one condition for the event and the moment: a first read (no before) with the refund already Completed gives the event AND the 'refund' moment", async () => {
          captured.length = 0;
          const c = await recordUnitChanges(userId, u.unitId, null, snap({ refund_status: "Completed" }, { refund_amount: "10.00" }), "verify-request");
          assert.equal(c.refundCompleted, true);
          assert.equal(refundEvents().length, 1);
          assert.ok(c.lateDocuments.some((m) => m.reason === "refund"));
          captured.length = 0;
          const d = await recordUnitChanges(userId, u.unitId, null, snap({ refund_status: "Processed" }), "verify-request");
          assert.deepEqual([d.refundCompleted, d.lateDocuments.some((m) => m.reason === "refund"), refundEvents().length], [false, false, 0]);
        });
        await check("the page path's bare 'refund' marker is no status: with a DONE Refund stage the refund is completed (event + moment), without one it is not", async () => {
          captured.length = 0;
          const done = [{ status: "Refund Completed", stage: "DONE", date: "2026-11-20T10:00:00+05:30" }];
          let c = await recordUnitChanges(userId, u.unitId, snap({ refund_status: null }), snap({ refund_status: "refund", tracking: done }), "verify-request");
          assert.deepEqual([c.refundCompleted, c.lateDocuments.map((m) => m.reason), refundEvents().length], [true, ["refund"], 1]);
          captured.length = 0;
          c = await recordUnitChanges(userId, u.unitId, snap({ refund_status: null }), snap({ refund_status: "refund" }), "verify-request");
          assert.deepEqual([c.refundCompleted, c.lateDocuments.length, refundEvents().length], [false, 0, 0]);
          captured.length = 0;
          c = await recordUnitChanges(userId, u.unitId, snap({ refund_status: "refund", tracking: done }), snap({ refund_status: "refund", tracking: done }), "verify-request");
          assert.deepEqual([c.refundCompleted, c.lateDocuments.length], [false, 0], "completed already before: no second moment");
        });
        await check("quiet: every event inserted processed with after.backfill; the refund is still recorded; no late-document moment", async () => {
          captured.length = 0;
          const c = await recordUnitChanges(userId, u.unitId, { ...snap({ status_key: "Shipped", refund_status: null }), status_key: "Shipped" }, snap({ refund_status: "Completed" }), "verify-request", { quiet: true });
          assert.deepEqual(c.lateDocuments, []);
          assert.equal(c.refundCompleted, true);
          assert.deepEqual(captured.map((e) => e.type).sort(), ["refund_completed", "stage_changed"]);
          for (const e of captured) {
            assert.equal(e.processed, true);
            assert.ok(e.processedAt instanceof Date);
            assert.equal((e.after as Record<string, unknown>).backfill, true);
          }
        });
        await check("T10: the label follows the request — manual, the 6-hour sweep, out-for-delivery, after an order, retry, the forced refresh", () => {
          assert.equal(updateSourceLabel("manual", {}), "Update order details");
          assert.equal(updateSourceLabel("schedule", { schedule_name: "update_active" }), "Update order details (6-hour sweep)");
          assert.equal(updateSourceLabel("schedule", { selection_method: "after_fetch" }), "Update order details (scheduled)");
          assert.equal(updateSourceLabel("ofd", {}), "Update order details (out-for-delivery recheck)");
          assert.equal(updateSourceLabel("system-order", {}), "Update order details (after an order)");
          assert.equal(updateSourceLabel("system-retry", {}), "Update order details (retry)");
          assert.equal(updateSourceLabel("manual", { purpose: "forced_refresh_2026" }), "Forced refresh");
          assert.ok(!/Fetch account details/.test(updateSourceLabel("manual", {})), "an update is never labelled a fetch");
        });
        await check("the worker's event options per request (eventOptionsFor, both recordUnitChanges calls): quiet only on params.quiet_events === true, the source and label of the request", () => {
          assert.deepEqual(eventOptionsFor({ source: "manual", params: { quiet_events: true, purpose: "forced_refresh_2026" } }), { quiet: true, requestSource: "manual", sourceLabel: "Forced refresh" });
          assert.deepEqual(eventOptionsFor({ source: "schedule", params: { schedule_name: "update_active" } }), { quiet: false, requestSource: "schedule", sourceLabel: "Update order details (6-hour sweep)" });
          assert.equal(eventOptionsFor({ source: "manual", params: { quiet_events: "true" } }).quiet, false, "only the boolean");
          assert.equal(eventOptionsFor({ source: "manual" }).quiet, false);
          const src = fs.readFileSync(path.join(repoRoot, "src/services/orderUpdateQueue.ts"), "utf8");
          assert.match(src, /const eventOpts = eventOptionsFor\(request\);/);
          assert.equal((src.match(/recordUnitChanges\(userId, u\.unitId, before, after, requestId, eventOpts\)/g) || []).length, 2, "both save paths pass them");
        });
        await check("the update worker's capability stamp says quiet_events (the forced refresh refuses a worker without it); an absent or unreadable stamp has none", () => {
          assert.deepEqual([...UPDATE_WORKER_CAPABILITIES], ["quiet_events"]);
          assert.equal(updateCapabilitiesKey("order-update"), "tradeflow:worker-caps:order-update");
          assert.deepEqual(capabilitiesOf(JSON.stringify({ caps: UPDATE_WORKER_CAPABILITIES, pid: 1 })), ["quiet_events"]);
          assert.deepEqual(capabilitiesOf(null), []);
          assert.deepEqual(capabilitiesOf("not json"), []);
          assert.deepEqual(capabilitiesOf(JSON.stringify({ host: "x", pid: 1 })), [], "the old heartbeat's shape");
        });
        await check("the event model takes refund_completed (insertMany would drop an unknown type) and the three source fields", () => {
          const doc = new OrderUnitEvent({ userId: workspace, type: "refund_completed", at: new Date(), source: "update", requestSource: "manual", sourceLabel: "Update order details" });
          assert.equal(doc.validateSync(), undefined);
          assert.deepEqual([doc.source, doc.requestSource, doc.sourceLabel], ["update", "manual", "Update order details"]);
        });
      } finally {
        (OrderUnitEvent as unknown as { insertMany: unknown }).insertMany = realInsertMany;
      }
    }

    console.log("\nU10 — a quiet insert raises no alert after the dashboard's minute poll (the dashboard API's event drain)");
    await check("quiet events of a unit with no address baseline and a changed address: processed at insert, no alert after the poll; a control event of the same workspace IS drained", async () => {
      if (process.env.UNIT_SAVE_NO_DASHBOARD === "1") throw new Error("SKIPPED (UNIT_SAVE_NO_DASHBOARD=1) — run it with the dashboard API up");
      const q = ids(83);
      const addr = (line1: string): ShippingAddress => ({ ...ADDRESS, line1, text: `${ADDRESS.name}, ${line1}, Nowhere 000000` });
      // A stored unit with an address hash and no baseline (as 12,302 scope units are), then a read that moves the address.
      await insertStored(83, { shipping_address: addr("1 Verify Street"), shipping_address_hash: addressHash(addr("1 Verify Street")), address_first_seen_at: null, status_key: "Shipped" });
      const before = await snapshotUnit(userId, q.unitId);
      await Order.collection.updateOne({ userId: workspace, unit_id: q.unitId }, { $set: { status_key: "Cancelled", shipping_address: addr("2 Other Street"), shipping_address_hash: addressHash(addr("2 Other Street")) } });
      const after = await snapshotUnit(userId, q.unitId);
      const changes = await recordUnitChanges(userId, q.unitId, before, after, "verify-quiet", { quiet: true, requestSource: "manual", sourceLabel: "Forced refresh" });
      assert.deepEqual(changes.events.sort(), ["address_changed", "address_seen", "stage_changed"]);
      assert.ok((await row(83))!.address_first_seen_at, "the baseline is still written");
      const quietRows = await OrderUnitEvent.find({ userId: workspace, unitId: q.unitId }).lean();
      assert.equal(quietRows.length, 3);
      assert.ok(quietRows.every((e) => e.processed === true), "inserted processed");
      // The control: an event the dashboard must claim (an unreadable account of the invented workspace).
      const control = await OrderUnitEvent.create({ userId: workspace, orderId: "", unitId: "", email: "control@verify.invalid", productName: "", type: "account_unreadable", at: new Date(), before: null, after: { reason: "verify control", flow: "verify" }, requestId: "verify-quiet", processed: false, processedAt: null });
      const until = Date.now() + 90_000;
      while (Date.now() < until && !(await OrderUnitEvent.exists({ _id: control._id, processed: true }))) await new Promise((r) => setTimeout(r, 2000));
      assert.ok(await OrderUnitEvent.exists({ _id: control._id, processed: true }), "the control event was not drained within 90 s — is the dashboard API running?");
      await new Promise((r) => setTimeout(r, 3000));
      const alerts = mongoose.connection.collection("alerts");
      assert.equal(await alerts.countDocuments({ userId: workspace, unitId: q.unitId }), 0, "no alert for the quiet unit");
      assert.equal(await alerts.countDocuments({ userId: workspace, kind: { $in: ["order.address_change", "order.address_mismatch"] } }), 0, "no address alert in the invented workspace");
      const stillQuiet = await OrderUnitEvent.find({ userId: workspace, unitId: q.unitId }).select("processedAt").lean();
      assert.ok(stillQuiet.every((e) => new Date(e.processedAt!).getTime() <= new Date(quietRows[0].processedAt!).getTime() + 1000), "no quiet event was claimed again");
      console.log(`        control drained; alerts in the invented workspace: ${await alerts.countDocuments({ userId: workspace })} (the control's own)`);
    });

    console.log("\nU8 — missing siblings through a stubbed order API (a saved 7-unit answer, scrubbed, ids invented)");
    await (async () => {
      const seven = findSevenUnitAnswer(traceDir);
      const SIB_ORDER = orderId(70);
      const SIB_ORDER_2 = orderId(71);
      const body = (target: string) => {
        const { raw, orderId: realOrder, unitIds } = seven;
        let text = JSON.stringify(raw);
        // Invented ids in place of the real ones (longest first: an item id is a unit id's prefix).
        unitIds.forEach((u, i) => (text = text.split(u).join(`9${stamp}${String(i).padStart(2, "0")}${target === SIB_ORDER ? "7" : "8"}000`)));
        // Then the order id (its digits can hold an item id), then the item ids (metaData.itemId; units of one item
        // share it): what is left of them stands alone now.
        text = text.split(realOrder).join(target);
        seven.itemIds.forEach((item, j) => (text = text.split(item).join(`8${stamp}${String(j).padStart(2, "0")}${target === SIB_ORDER ? "7" : "8"}`)));
        return text;
      };
      const sibIds = (target: string) => seven.unitIds.map((_, i) => `9${stamp}${String(i).padStart(2, "0")}${target === SIB_ORDER ? "7" : "8"}000`);
      const calls: string[] = [];
      const failing = new Set<string>();
      const ctxFor = (target: string) =>
        ({
          post: async (url: string, opts: { data?: { requestContext?: { unitId?: string } } }) => {
            const unitId = String(opts?.data?.requestContext?.unitId || "");
            calls.push(unitId);
            const known = sibIds(target).includes(unitId);
            const status = failing.has(unitId) ? 500 : known ? 200 : 404;
            return { status: () => status, ok: () => status === 200, url: () => url, text: async () => (status === 200 ? body(target) : "{}") };
          },
        }) as unknown as import("playwright").APIRequestContext;
      console.log(`        the answer: ${seven.source} (${seven.unitIds.length} units)`);
      await check("an account holding 1 stored unit of a 7-unit order: the other 6 are saved under the invented workspace; a second call (same job) stores none; a new job finds none missing", async () => {
        const ids7 = sibIds(SIB_ORDER);
        // The one stored unit, saved from the same answer.
        const { orderViewFromPageFetch: view, mapApiUnitToOrder: map } = await import("../services/orderApi.js");
        const v = view(JSON.parse(body(SIB_ORDER)))!;
        await saveMappedUnit(userId, account, map(v, ids7[0]), { applyCalendar: false });
        assert.equal(await Order.countDocuments({ userId: workspace, order_id: SIB_ORDER }), 1);
        calls.length = 0;
        const memo = new Set<string>();
        const r1 = await saveMissingSiblings(ctxFor(SIB_ORDER), userId, account, { orderId: SIB_ORDER, unitId: ids7[0] }, ids7, memo);
        assert.deepEqual(r1, { missing: 6, saved: 6, failed: 0 });
        assert.equal(calls.length, 6, "one read per missing sibling");
        assert.equal(await Order.countDocuments({ userId: workspace, order_id: SIB_ORDER }), 7);
        const stored = await Order.find({ userId: workspace, order_id: SIB_ORDER }).select("unit_id platform_email").lean();
        assert.deepEqual(stored.map((x) => x.unit_id).sort(), [...ids7].sort());
        assert.ok(stored.every((x) => x.platform_email === account.email));
        calls.length = 0;
        assert.deepEqual(await saveMissingSiblings(ctxFor(SIB_ORDER), userId, account, { orderId: SIB_ORDER, unitId: ids7[1] }, ids7, memo), { missing: 0, saved: 0, failed: 0 }, "once per order per job");
        assert.deepEqual(await saveMissingSiblings(ctxFor(SIB_ORDER), userId, account, { orderId: SIB_ORDER, unitId: ids7[1] }, ids7, new Set()), { missing: 0, saved: 0, failed: 0 }, "a new job: none missing");
        assert.equal(calls.length, 0, "no read when nothing is missing");
      });
      await check("a sibling that fails (HTTP 500) is counted and logged, never thrown; the request's own units (skip) are not read", async () => {
        const ids7 = sibIds(SIB_ORDER_2);
        failing.add(ids7[2]);
        const lines: string[] = [];
        calls.length = 0;
        const r = await saveMissingSiblings(ctxFor(SIB_ORDER_2), userId, account, { orderId: SIB_ORDER_2, unitId: ids7[0] }, ids7, new Set(), { skip: new Set([ids7[1]]), log: (m) => void lines.push(m) });
        assert.deepEqual(r, { missing: 5, saved: 4, failed: 1 });
        assert.ok(!calls.includes(ids7[1]), "the skipped unit is not read");
        assert.equal(lines.length, 1);
        assert.match(lines[0], /sibling unit not saved/);
        assert.equal(await Order.countDocuments({ userId: workspace, order_id: SIB_ORDER_2 }), 4);
      });
      await check("the update worker calls it after an API read only, with the request's units skipped, and adds '+N sibling unit(s) saved' to the account's message", () => {
        const src = fs.readFileSync(path.join(repoRoot, "src/services/orderUpdateQueue.ts"), "utf8");
        assert.match(src, /if \(api && !usedPage\) \{\s*const sib = await saveMissingSiblings\(api, userId, acct, \{ orderId: u\.orderId, unitId: u\.unitId \}, \(result as \{ siblingUnitIds\?: string\[\] \}\)\.siblingUnitIds \|\| \[\], siblingMemo, \{ skip: ownUnits, read: deps\.apiRead/);
        assert.match(src, /\+\$\{siblingsSaved\} sibling unit\(s\) saved/);
      });
    })();

    console.log("\nU9 — a skipped_old index row inside the fetch window goes back to pending (discoverUnit)");
    await check("requeueSkippedOld's table", () => {
      const since = new Date("2026-10-01T00:00:00+05:30");
      assert.equal(requeueSkippedOld("skipped_old", new Date("2026-10-05"), since, false), true);
      assert.equal(requeueSkippedOld("skipped_old", new Date("2026-09-20"), since, false), false, "before the window");
      assert.equal(requeueSkippedOld("skipped_old", new Date("2026-10-05"), since, true), false, "stored already");
      assert.equal(requeueSkippedOld("skipped_old", null, since, false), false, "no list date");
      assert.equal(requeueSkippedOld("scraped", new Date("2026-10-05"), since, false), false);
      // Skipped by the enrich step on the detail's placed date: that date decides, not the list card's (a status date).
      const placed = { lastError: "placed before 1 Oct 2026", placedDate: new Date("2026-08-20") };
      assert.equal(requeueSkippedOld("skipped_old", new Date("2026-10-12"), since, false, placed), false, "'placed before' with an old placed date stays, whatever the list date");
      assert.equal(requeueSkippedOld("skipped_old", new Date("2026-10-12"), since, false, { ...placed, placedDate: new Date("2026-10-03") }), true, "'placed before' with a placed date inside a wider window goes back");
      assert.equal(requeueSkippedOld("skipped_old", new Date("2026-10-12"), since, false, { lastError: "list date before 1 Oct 2026", placedDate: null }), true, "'list date before' is judged on the list date");
    });
    await check("discoverUnit on invented order_index rows: inside the window and not stored → pending, counted new; before the window, and stored, stay skipped_old (duplicate); the invariant has 0 unaccounted", async () => {
      const since = new Date(Date.now() - 60 * 86_400_000);
      setFetchWindowForChecks(userId, since);
      const inWindow = new Date(Date.now() - 10 * 86_400_000);
      const old = new Date(Date.now() - 90 * 86_400_000);
      const card = (n: number, at: Date) => ({ ...ids(n), orderUrl: urlOf(ids(n)), amount: "", orderDate: at });
      const idx = (n: number, at: Date) => ({ userId: workspace, platformAccountId: account.id, platform_email: account.email, platform: "FLIPKART", order_id: ids(n).orderId, item_id: ids(n).itemId, unit_id: ids(n).unitId, order_url: urlOf(ids(n)), scrape_status: "skipped_old", list_hint: "", status_key: "", order_date: at, first_seen_at: new Date(), last_seen_at: new Date(), last_error: "list date before …" });
      // 94: skipped by the enrich step (placed 90 days ago) while its list card shows a status date inside the window.
      await OrderIndex.collection.insertMany([idx(91, inWindow), idx(92, old), idx(93, inWindow), { ...idx(94, old), last_error: "placed before …" }] as never[]);
      await insertStored(93, { status_key: "Delivered" });
      const kinds = [await discoverUnitForChecks(userId, account, card(91, inWindow)), await discoverUnitForChecks(userId, account, card(92, old)), await discoverUnitForChecks(userId, account, card(93, inWindow)), await discoverUnitForChecks(userId, account, card(94, inWindow))];
      assert.deepEqual(kinds, ["new", "duplicate", "duplicate", "duplicate"]);
      assert.equal((await OrderIndex.collection.findOne({ userId: workspace, unit_id: ids(94).unitId }))!.scrape_status, "skipped_old", "a 'placed before' row is not re-queued on a status date");
      const st = async (n: number) => (await OrderIndex.collection.findOne({ userId: workspace, unit_id: ids(n).unitId }))!;
      assert.deepEqual([(await st(91)).scrape_status, (await st(91)).last_error], ["pending", ""]);
      assert.equal((await st(92)).scrape_status, "skipped_old");
      assert.equal((await st(93)).scrape_status, "skipped_old");
      // The enrich pass of the run takes 91 (pending); 92 was dropped by date at the list; 93 is known.
      const inv = await checkFetchInvariantForChecks(userId, account, "api", [ids(91).unitId, ids(92).unitId, ids(93).unitId], [ids(92).unitId], [ids(91).unitId], { enriched: 1, mapped: 1, oldAtEnrich: 0, failed: 0 });
      assert.deepEqual([inv.unaccounted, inv.pass], [0, true], inv.line);
    });

    console.log("\nU12 + X5 — the refund watch (orderLifecycle.ts refundWatchFilter) through resolveTargets");
    await check("selected: Cancelled 2 days ago, not COD, no refund (also with the page's bare 'refund' marker); refund Processed 20 days ago; an invoiced bundled unit cancelled 20 days ago without a refund — not: COD, Completed, Delivered, cancelled 20 days ago with nothing open, a recent or bundled one refunded by its DONE stage (with no status or the bare marker), a bundled cash-on-delivery one", async () => {
      const day = 86_400_000;
      const ago = (d: number) => new Date(Date.now() - d * day);
      const W = { recent: 101, processed: 102, bundled: 103, cod: 104, completed: 105, delivered: 106, oldPlain: 107, bundledRefunded: 108, bundledCod: 109, recentDone: 110, recentMarkerDone: 111, recentMarker: 112 };
      const key = (n: number) => `raw/${userId}/${ids(n).orderId}.pdf`;
      await insertStored(W.recent, { status_key: "Cancelled", cancelled_date: ago(2), refund_status: null, cash_on_delivery: false, payment_mode: "", invoice_s3_key: "" });
      await insertStored(W.processed, { status_key: "Cancelled", cancelled_date: ago(20), refund_status: "Processed", cash_on_delivery: false, invoice_s3_key: "" });
      await insertStored(W.bundled, { status_key: "Cancelled", cancelled_date: ago(20), refund_status: null, cash_on_delivery: false, order_status_pre_cancellation: "Shipped", invoice_s3_key: key(W.bundled), tracking: [] });
      await insertStored(W.bundledCod, { status_key: "Cancelled", cancelled_date: ago(20), refund_status: null, cash_on_delivery: true, order_status_pre_cancellation: "Shipped", invoice_s3_key: key(W.bundledCod), tracking: [] });
      await insertStored(W.cod, { status_key: "Cancelled", cancelled_date: ago(2), refund_status: null, cash_on_delivery: true, invoice_s3_key: "" });
      await insertStored(W.completed, { status_key: "Cancelled", cancelled_date: ago(2), refund_status: "Completed", cash_on_delivery: false, invoice_s3_key: "" });
      await insertStored(W.delivered, { status_key: "Delivered", cancelled_date: null, refund_status: null, invoice_s3_key: "" });
      await insertStored(W.oldPlain, { status_key: "Cancelled", cancelled_date: ago(20), refund_status: null, cash_on_delivery: false, invoice_s3_key: "" });
      await insertStored(W.bundledRefunded, { status_key: "Cancelled", cancelled_date: ago(20), refund_status: null, cash_on_delivery: false, order_status_pre_cancellation: "Shipped", invoice_s3_key: key(W.bundledRefunded), tracking: [{ status: "Refund", stage: "DONE", date: ago(5).toISOString(), detailed_steps: [] }] });
      // "recent" follows the detection's rule too: a DONE Refund stage without a status (or with the page's bare marker) is refunded.
      const doneStage = [{ status: "Refund Completed", stage: "DONE", date: ago(1).toISOString(), detailed_steps: [] }];
      await insertStored(W.recentDone, { status_key: "Cancelled", cancelled_date: ago(2), refund_status: null, cash_on_delivery: false, invoice_s3_key: "", tracking: doneStage });
      await insertStored(W.recentMarkerDone, { status_key: "Cancelled", cancelled_date: ago(2), refund_status: "refund", cash_on_delivery: false, invoice_s3_key: "", tracking: doneStage });
      await insertStored(W.recentMarker, { status_key: "Cancelled", cancelled_date: ago(2), refund_status: "refund", cash_on_delivery: false, invoice_s3_key: "", tracking: [] });
      // Every other invented unit of this run moves to another (not logged-in) e-mail: only the twelve above are asked about.
      const asked = Object.values(W).map((n) => ids(n).unitId);
      await Order.collection.updateMany({ userId: workspace, unit_id: { $nin: asked } }, { $set: { platform_email: "elsewhere@verify.invalid" } });
      // The account, logged in with a made-up cookie (resolveTargets reads it; this check never does).
      await PlatformId.collection.insertOne({ userId: workspace, platform: "FLIPKART", email: account.email, status: "logged_in", sessionSavedAt: new Date(), sessionCookies: [{ name: "verify", value: "invented", domain: ".verify.invalid", path: "/" }], createdAt: new Date(), updatedAt: new Date() } as never);
      const units = (await resolveTargets(userId, { emails: [account.email], withRefundWatch: true })).map((t) => t.card.unitId).sort();
      assert.deepEqual(units, [ids(W.recent).unitId, ids(W.processed).unitId, ids(W.bundled).unitId, ids(W.recentMarker).unitId].sort());
      const without = await resolveTargets(userId, { emails: [account.email] });
      assert.deepEqual(without.map((t) => t.card.unitId), [], "the active selection alone takes none of them (all are finished)");
      assert.equal(refundWatchFilter().$or.length, 3, "one filter, three clauses");
    });
    await check("the 6-hour active sweep asks for the watch (and records refund_watch); the out-for-delivery recheck does not", () => {
      const src = fs.readFileSync(path.join(repoRoot, "src/services/orderUpdateQueue.ts"), "utf8");
      assert.match(src, /kind === "ofd" \? \{ orderStatus: keys, selectionMethod: "schedule_ofd" \} : \{ selectionMethod: "schedule_active", withRefundWatch: true \}/);
      assert.match(src, /\.\.\.\(kind === "active" \? \{ refund_watch: true \} : \{\}\)/);
    });
  } finally {
    // Events first (the dashboard turns new ones into alerts), then the alerts it raised, then the rest.
    const events = (await OrderUnitEvent.deleteMany({ userId: workspace })).deletedCount;
    const db = mongoose.connection.db!;
    const removed = {
      units: (await Order.deleteMany({ userId: workspace })).deletedCount,
      index: (await OrderIndex.deleteMany({ userId: workspace })).deletedCount,
      events: events + (await OrderUnitEvent.deleteMany({ userId: workspace })).deletedCount,
      alerts: (await db.collection("alerts").deleteMany({ userId: workspace })).deletedCount,
      jobs: (await CheckoutJob.collection.deleteMany({ userId: { $in: [userId, workspace] as unknown[] } } as never)).deletedCount,
      batches: (await CheckoutBatch.collection.deleteMany({ userId: { $in: [userId, workspace] as unknown[] } } as never)).deletedCount,
      platformids: (await PlatformId.collection.deleteMany({ userId: workspace })).deletedCount,
    };
    for (const name of throwaway) await mongoose.connection.db!.dropCollection(name).catch(() => undefined);
    const left =
      (await Order.countDocuments({ userId: workspace })) +
      (await OrderIndex.countDocuments({ userId: workspace })) +
      (await OrderUnitEvent.countDocuments({ userId: workspace })) +
      (await db.collection("alerts").countDocuments({ userId: workspace })) +
      (await CheckoutJob.collection.countDocuments({ userId: { $in: [userId, workspace] as unknown[] } } as never)) +
      (await CheckoutBatch.collection.countDocuments({ userId: { $in: [userId, workspace] as unknown[] } } as never)) +
      (await PlatformId.collection.countDocuments({ userId: workspace }));
    const strays = (await mongoose.connection.db!.listCollections({ name: { $regex: `^verify_us_${run}_` } }).toArray()).length;
    console.log(
      `\ncleanup: ${removed.units} unit(s), ${removed.index} index row(s), ${removed.events} event(s), ${removed.alerts} alert(s), ${removed.jobs} job(s), ${removed.batches} batch(es), ${removed.platformids} platformids row(s) of the invented workspace deleted — ${left} left; ${throwaway.length} throwaway collection(s) dropped (${strays} left)`
    );
    if (left || strays) failures.push("cleanup");
    await mongoose.disconnect();
  }

  console.log(`\n${passed} passed, ${failures.length} failed${failures.length ? ` — ${failures.join("; ")}` : ""}`);
  process.exitCode = failures.length ? 1 : 0;
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
