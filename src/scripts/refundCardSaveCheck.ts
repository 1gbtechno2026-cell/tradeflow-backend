/**
 * Sub-step 9's save rules (data-quality U2 page path, U3 card), checked against the real database on rows of an
 * INVENTED workspace: a made-up TRADE_FLOW_USER_ID and @verify.invalid accounts, every row deleted at the end.
 * No Flipkart, no queue, no worker, no Redis.
 *
 *   · cardColumnsFor's table (pure)
 *   · saveMappedUnit (the API path):
 *       - no checkout job of ours + a refund card → card_name = bank, card_number = XXXXXXXXXXXX + last four,
 *         card_source "flipkart_refund", api_extra.refund_card; refund_date / refund_kind written
 *       - our own card job → the checkout card, card_source "checkout", no api_extra.refund_card
 *       - our own COD job → its blank card columns, card_source ""
 *       - a failed job lookup → the stored card columns stay
 *       - a later answer without a refund card → the refund fields, their date / kind and the card stay
 *   · scrapeAndSave (the page path, a fake page): a stored name that starts with the page's title stays; another
 *     stored name is replaced; a stored refund status (Flipkart's "Completed") is not replaced by the page's "refund"
 *
 * The sequence counter and the checkout-job lookup are replaced in this process only, so no real counter moves and no
 * checkout job is read or written.
 *
 *   npm run test:refund-card
 */
import assert from "node:assert/strict";
import mongoose, { Types } from "mongoose";
import type { Page } from "playwright";
import type { MappedOrderUnit } from "../services/orderApi.js";

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
  const { cardColumnsFor, saveMappedUnit, scrapeAndSave } = await import("../services/orderFetch.js");
  assert.equal(config.userId, String(workspace), "the made-up TRADE_FLOW_USER_ID is the one in use");

  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false, serverSelectionTimeoutMS: 15_000 });
  const userId = String(workspace);

  let fakeSeq = -Date.now() * 100 - 50_000_000;
  (Counter as unknown as { findByIdAndUpdate: unknown }).findByIdAndUpdate = async () => ({ seq: fakeSeq-- });

  const stamp = String(Date.now()).slice(-10);
  const ids = (n: number) => {
    const orderId = `OD98${stamp}${String(n).padStart(6, "0")}`;
    const itemId = `8${stamp}${String(n).padStart(5, "0")}`;
    return { orderId, itemId, unitId: `${itemId}000` };
  };
  const CARD_ORDER = ids(2).orderId;
  const COD_ORDER = ids(3).orderId;
  const BROKEN_ORDER = ids(4).orderId;
  const cardJob = {
    result: { cardName: "Verify Card", childCardLast4: "4444", parentCardLast4: "3333", transactionAmount: "12999", paymentFee: "", paymentFeeDetails: "" },
    request: { paymentMode: "card", cards: [{ name: "Verify Card", cardNumberMasked: "XXXXXXXXXXXX4444", parentCardNumberMasked: "XXXXXXXXXXXX3333" }] },
  };
  const codJob = { result: { transactionAmount: "12999", paymentFee: "", paymentFeeDetails: "" }, request: { paymentMode: "cod", cards: [] } };
  (CheckoutJob as unknown as { findOne: unknown }).findOne = (filter: Record<string, unknown>) => {
    const id = filter?.["result.flipkartOrderId"];
    const value = id === CARD_ORDER ? cardJob : id === COD_ORDER ? codJob : null;
    const q = {
      sort: () => q,
      select: () => q,
      lean: async () => {
        if (id === BROKEN_ORDER) throw new Error("lookup failed (verify)");
        return value;
      },
    };
    return q;
  };

  const account = { id: new Types.ObjectId(), email: "refund-card@verify.invalid", cookies: [] as unknown[] };
  const refundCard = { bank: "VERIFYBANK", network: "MASTERCARD", type: "Credit" as const, last4: "0000" };
  const mappedFor = (n: number, over: Partial<MappedOrderUnit> = {}): MappedOrderUnit => {
    const x = ids(n);
    return {
      order_id: x.orderId,
      item_id: x.itemId,
      unit_id: x.unitId,
      order_url: `https://www.flipkart.com/order_details?order_id=${x.orderId}&item_id=${x.itemId}&unit_id=${x.unitId}`,
      order_date: new Date("2026-11-17T10:15:09+05:30"),
      product_name: "Verify Phone 5G (Black, 128 GB) (8 GB RAM)",
      seller_name: "Verify Seller",
      quantity: 1,
      total_amount: "12999.00",
      unit_amount: "12999.00",
      delivery_date: new Date("2026-11-20T23:59:59+05:30"),
      actual_delivery_date: null,
      ewb_number: "",
      delivery_message: "",
      status_key: "Cancelled",
      status_label: "Your order was cancelled as per your request.",
      status_reason: null,
      business_name: "",
      business_gst_no: "",
      tracking_id: "",
      delivery_otp: null,
      tracking: [
        { date: "2026-11-17T10:15:39+05:30", stage: "DONE", status: "Order Confirmed", detailed_steps: [{ event: "paymentApprovalStep", progress: [{ date: "2026-11-17T10:15:39+05:30", remark: "Your Order has been placed.", location: null }], step_text: "Your Order has been placed." }] },
        { date: "2026-11-18T09:00:00+05:30", stage: "DONE", status: "Cancelled", detailed_steps: [{ event: "cancelStep", progress: [{ date: "2026-11-18T09:00:00+05:30", remark: "Your order was cancelled as per your request.", location: null }], step_text: "Your order was cancelled as per your request." }] },
      ],
      last_tracking_step: "2026-11-18T09:00:00+05:30 | Cancelled",
      billing_phone_number: null,
      billing_address_name: null,
      billing_address_pincode: null,
      shipping_address: null,
      shipping_address_hash: "",
      supercoin_amount_applied: 0,
      cash_on_delivery: false,
      refund_status: "Completed",
      refund_amount: "12999.00",
      refund_msg: "Refund was added to your VERIFYBANK MASTERCARD Credit Card ************0000 on Nov 19 03:15 PM.",
      refund_date: new Date("2026-11-19T15:15:00+05:30"),
      refund_kind: "refund",
      refund_card: refundCard,
      cancelled_date: new Date("2026-11-18T09:00:00+05:30"),
      cancelled_by_user: true,
      cancelled_by: "customer",
      cancel_reason_text: "Cancelled",
      order_status_pre_cancellation: "Order Confirmed",
      shipment_partner_name: "",
      api_extra: { listing_price: "14999.00", selling_price: "12999.00", payment_mode: "PREPAID", adjustments: [], fsn: "VERIFYFSN0000001", listing_id: "", seller_id: "", invoice_eligible: true, customer_email: account.email, product_title: "Verify Phone 5G (Black, 128 GB)", product_subtitle: "8 GB RAM", first_promised_date: "2026-11-20T23:59:59+05:30", delivered_stage_at: null },
      ...over,
    };
  };
  const row = async (n: number) => (await Order.collection.findOne({ userId: workspace, unit_id: ids(n).unitId })) as Record<string, unknown> | null;
  const cardOf = (r: Record<string, unknown> | null) => [r?.card_name, r?.card_number, r?.card_source];

  try {
    console.log("cardColumnsFor — the rule");
    await check("own job wins (card → 'checkout', COD → ''); no job + refund card → 'flipkart_refund'; a failed lookup or no card → nothing", () => {
      const own = { card_name: "Verify Card", card_number: "XXXXXXXXXXXX4444", payment_mode: "card" };
      assert.deepEqual(cardColumnsFor(own, refundCard), { ...own, card_source: "checkout" });
      assert.deepEqual(cardColumnsFor({ card_name: "", payment_mode: "cod" }, refundCard), { card_name: "", payment_mode: "cod", card_source: "" });
      assert.deepEqual(cardColumnsFor({}, refundCard), { card_name: "VERIFYBANK", card_number: "XXXXXXXXXXXX0000", card_source: "flipkart_refund" });
      assert.deepEqual(cardColumnsFor({}, null), {});
      assert.deepEqual(cardColumnsFor(null, refundCard), {});
    });

    console.log("\nsaveMappedUnit — the API path");
    await check("no job of ours: the refund sentence's card, marked 'flipkart_refund', api_extra.refund_card; refund date and kind written", async () => {
      await saveMappedUnit(userId, account, mappedFor(1), { applyCalendar: false });
      const got = await row(1);
      assert.deepEqual(cardOf(got), ["VERIFYBANK", "XXXXXXXXXXXX0000", "flipkart_refund"]);
      assert.deepEqual((got!.api_extra as Record<string, unknown>).refund_card, refundCard);
      assert.equal((got!.api_extra as Record<string, unknown>).product_title, "Verify Phone 5G (Black, 128 GB)");
      assert.deepEqual([got!.refund_status, got!.refund_amount, got!.refund_kind, (got!.refund_date as Date).toISOString()], ["Completed", "12999.00", "refund", new Date("2026-11-19T15:15:00+05:30").toISOString()]);
      assert.ok(!("refund_card" in got!), "refund_card is not a unit field");
      assert.equal(got!.parent_card_number, "", "parent_card_number left as it is");
    });
    await check("a later answer without a refund card: refund fields, date, kind, the card and api_extra.refund_card stay", async () => {
      await saveMappedUnit(userId, account, mappedFor(1, { refund_status: null, refund_amount: null, refund_msg: null, refund_date: null, refund_kind: null, refund_card: null }), { applyCalendar: false });
      const got = await row(1);
      assert.deepEqual(cardOf(got), ["VERIFYBANK", "XXXXXXXXXXXX0000", "flipkart_refund"]);
      assert.deepEqual((got!.api_extra as Record<string, unknown>).refund_card, refundCard, "the structured card is carried forward");
      assert.equal((got!.api_extra as Record<string, unknown>).product_title, "Verify Phone 5G (Black, 128 GB)", "the rest of api_extra is this answer's");
      assert.deepEqual([got!.refund_status, got!.refund_amount, got!.refund_kind, (got!.refund_date as Date).toISOString()], ["Completed", "12999.00", "refund", new Date("2026-11-19T15:15:00+05:30").toISOString()]);
    });
    await check("our own card job: the checkout card wins, card_source 'checkout', no api_extra.refund_card", async () => {
      await saveMappedUnit(userId, account, mappedFor(2), { applyCalendar: false });
      const got = await row(2);
      assert.deepEqual(cardOf(got), ["Verify Card", "XXXXXXXXXXXX4444", "checkout"]);
      assert.equal(got!.parent_card_number, "XXXXXXXXXXXX3333");
      assert.equal((got!.api_extra as Record<string, unknown>).refund_card, undefined);
    });
    await check("our own COD job: its blank card columns, card_source ''", async () => {
      await saveMappedUnit(userId, account, mappedFor(3), { applyCalendar: false });
      const got = await row(3);
      assert.deepEqual(cardOf(got), ["", "", ""]);
      assert.deepEqual([got!.payment_mode, got!.cash_on_delivery], ["cod", true]);
    });
    await check("a failed job lookup: the stored card columns stay (never replaced by Flipkart's)", async () => {
      const x = ids(4);
      await Order.collection.insertOne({ userId: workspace, platformAccountId: account.id, id: fakeSeq--, platform_email: account.email, platform: "FLIPKART", order_id: x.orderId, item_id: x.itemId, unit_id: x.unitId, card_name: "Stored Card", card_number: "XXXXXXXXXXXX1111", card_source: "checkout", createdAt: new Date(), updatedAt: new Date() });
      await saveMappedUnit(userId, account, mappedFor(4), { applyCalendar: false });
      assert.deepEqual(cardOf(await row(4)), ["Stored Card", "XXXXXXXXXXXX1111", "checkout"]);
    });

    console.log("\nscrapeAndSave — the page path keeps the fuller stored name (a fake page, no browser)");
    const pageFor = (n: number, extraText = "") => {
      const x = ids(n);
      const url = `https://www.flipkart.com/order_details?order_id=${x.orderId}&item_id=${x.itemId}&unit_id=${x.unitId}`;
      const pageData = {
        url, unitId: x.unitId, itemId: x.itemId,
        productName: "Verify Phone 5G (Black, 128 GB)", sellerName: "Verify Seller", gstSellerName: "", gstNumber: "", ewbNumber: "",
        totalAmount: "12999", quantity: 1, supercoin: 0, deliveryOtp: "", billingPhone: "", billingName: "", pincode: "",
        deliveryAddressText: "", deliveryMessage: "Delivered on Nov 20", cashOnDelivery: false,
        pageText: `Delivered on Nov 20 Verify Phone 5G (Black, 128 GB) Seller: Verify Seller Price details Total amount ₹12,999${extraText}`,
      };
      const track = "Order Confirmed Tue, 17th Nov '26 Your Order has been placed. Tue, 17th Nov '26 - 10:15 am Delivered Fri, 20th Nov '26 Your item has been delivered Fri, 20th Nov '26 - 6:00 pm";
      const locator: Record<string, unknown> = {};
      Object.assign(locator, {
        first: () => locator, filter: () => locator, locator: () => locator,
        isVisible: async () => false, count: async () => 1,
        scrollIntoViewIfNeeded: async () => undefined, click: async () => undefined, waitFor: async () => undefined,
      });
      const page = {
        goto: async () => null,
        waitForFunction: async () => null,
        url: () => url,
        getByText: () => locator,
        locator: () => locator,
        evaluate: async (_fn: unknown, payload: { fnSource?: string }) => {
          const src = String(payload?.fnSource || "");
          if (src.includes("gstBlock")) return pageData;
          if (src.includes("status_display")) return null;
          if (src.includes("your order has been placed") && src.includes("role=")) return track;
          return true;
        },
      };
      return { page: page as unknown as Page, card: { orderId: x.orderId, itemId: x.itemId, unitId: x.unitId, orderUrl: url, amount: "" } };
    };
    const insertNamed = async (n: number, name: string) => {
      const x = ids(n);
      await Order.collection.insertOne({ userId: workspace, platformAccountId: account.id, id: fakeSeq--, platform_email: account.email, platform: "FLIPKART", order_id: x.orderId, item_id: x.itemId, unit_id: x.unitId, product_name: name, createdAt: new Date(), updatedAt: new Date() });
    };
    await check("stored 'Title (…) (8 GB RAM)', page 'Title (…)' → the stored name stays; the reference's Last Tracking Step", async () => {
      await insertNamed(11, "Verify Phone 5G (Black, 128 GB) (8 GB RAM)");
      const { page, card } = pageFor(11);
      await scrapeAndSave(page, userId, account, card, { applyCalendar: false });
      const got = await row(11);
      assert.equal(got!.product_name, "Verify Phone 5G (Black, 128 GB) (8 GB RAM)");
      assert.equal(got!.status_key, "Delivered");
      assert.match(String(got!.last_tracking_step), /^\S+\+05:30 \| Delivered$/);
    });
    await check("another stored name (not starting with the page's title) → the page's name", async () => {
      await insertNamed(12, "Some Older Name");
      const { page, card } = pageFor(12);
      await scrapeAndSave(page, userId, account, card, { applyCalendar: false });
      assert.equal((await row(12))!.product_name, "Verify Phone 5G (Black, 128 GB)");
    });
    await check("the page's bare 'refund' marker never replaces Flipkart's refund status / sentence; a unit with none still gets it", async () => {
      const x = ids(13);
      await Order.collection.insertOne({ userId: workspace, platformAccountId: account.id, id: fakeSeq--, platform_email: account.email, platform: "FLIPKART", order_id: x.orderId, item_id: x.itemId, unit_id: x.unitId, product_name: "Verify Phone 5G (Black, 128 GB)", refund_status: "Completed", refund_msg: "Refund was added to your VERIFYBANK VISA Credit Card ************0000 on Nov 21 10:00 AM.", createdAt: new Date(), updatedAt: new Date() });
      const a = pageFor(13, " Total refund ₹12,999 refund completed");
      await scrapeAndSave(a.page, userId, account, a.card, { applyCalendar: false });
      const got = await row(13);
      assert.equal(got!.refund_status, "Completed");
      assert.ok(String(got!.refund_msg).startsWith("Refund was added to your"));
      await insertNamed(14, "Verify Phone 5G (Black, 128 GB)");
      const b = pageFor(14, " Total refund ₹12,999 refund completed");
      await scrapeAndSave(b.page, userId, account, b.card, { applyCalendar: false });
      assert.equal((await row(14))!.refund_status, "refund");
    });
  } finally {
    const removed = {
      units: (await Order.deleteMany({ userId: workspace })).deletedCount,
      index: (await OrderIndex.deleteMany({ userId: workspace })).deletedCount,
      events: (await OrderUnitEvent.deleteMany({ userId: workspace })).deletedCount,
    };
    const left =
      (await Order.countDocuments({ userId: workspace })) + (await OrderIndex.countDocuments({ userId: workspace })) + (await OrderUnitEvent.countDocuments({ userId: workspace }));
    console.log(`\ncleanup: ${removed.units} unit(s), ${removed.index} index row(s), ${removed.events} event(s) of the invented workspace deleted — ${left} left`);
    if (left) failures.push("cleanup");
    await mongoose.disconnect();
  }

  console.log(`\n${passed} passed, ${failures.length} failed${failures.length ? ` — ${failures.join("; ")}` : ""}`);
  process.exitCode = failures.length ? 1 : 0;
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
