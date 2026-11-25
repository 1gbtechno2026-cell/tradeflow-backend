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
 * The sequence counter and the checkout-job lookup are replaced in this process only, so no real counter moves and no
 * checkout job is read or written.
 *
 *   npm run test:unit-save          UNIT_SAVE_TRACE_DIR=<dir> reads the traces from another checkout
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
  const { OrderUpdateRequest } = await import("../models/OrderUpdateRequest.js");
  const { CancelRequest } = await import("../models/CancelRequest.js");
  const { RescheduleRequest } = await import("../models/RescheduleRequest.js");
  const { KEEP_WHEN_BLANK, UNIT_INSERT_DEFAULTS, saveMappedUnit, scrapeAndSave, unitSetFor } = await import("../services/orderFetch.js");
  const { addressHash, mapApiUnitToOrder, orderViewFromPageFetch } = await import("../services/orderApi.js");
  const { reapCandidatesFilter } = await import("../services/orderUpdateQueue.js");
  const { cancelReapCandidatesFilter } = await import("../services/orderCancel.js");
  const { rescheduleReapCandidatesFilter } = await import("../services/orderReschedule.js");
  const { recordUnitChanges } = await import("../services/orderUnitEvents.js");
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
  (CheckoutJob as unknown as { findOne: unknown }).findOne = (filter: Record<string, unknown>) => {
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
  } finally {
    const removed = {
      units: (await Order.deleteMany({ userId: workspace })).deletedCount,
      index: (await OrderIndex.deleteMany({ userId: workspace })).deletedCount,
      events: (await OrderUnitEvent.deleteMany({ userId: workspace })).deletedCount,
    };
    for (const name of throwaway) await mongoose.connection.db!.dropCollection(name).catch(() => undefined);
    const left =
      (await Order.countDocuments({ userId: workspace })) + (await OrderIndex.countDocuments({ userId: workspace })) + (await OrderUnitEvent.countDocuments({ userId: workspace }));
    const strays = (await mongoose.connection.db!.listCollections({ name: { $regex: `^verify_us_${run}_` } }).toArray()).length;
    console.log(`\ncleanup: ${removed.units} unit(s), ${removed.index} index row(s), ${removed.events} event(s) of the invented workspace deleted — ${left} left; ${throwaway.length} throwaway collection(s) dropped (${strays} left)`);
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
