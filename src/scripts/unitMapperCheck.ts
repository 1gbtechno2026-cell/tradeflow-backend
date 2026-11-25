/**
 * The unit mapper (data-quality plan sub-step 9: U2, U3, U4, U5, U16 and T8), checked with no network and no Mongo:
 *
 *   1. Unit cases on invented inputs — the product name rule, the refund card (the six masked sentence shapes,
 *      "Tomorrow", SuperCoins, a replacement, a two-widget card), the reference's Last Tracking Step, milliseconds in
 *      the timeline, and T8 (a dateless DONE "Delivered" node followed by Cancelled / Refund).
 *   2. Every saved trace under debug/order-traces re-mapped from Flipkart's raw answer (mapApiUnitToOrder) and
 *      compared with the object the run itself saved (the mapper of that day) — the plan's counts on the 24-11 nightly
 *      trace, the refund cards of every distinct traced unit, the compared order, and a per-field diff table.
 *
 * Prints counts, order ids and rule names only: never a name, phone, address, card digit or OTP.
 *
 *   npm run test:unit-mapper                       traces from ORDER_TRACE_DIR (env or .env) or ./debug/order-traces
 *                                                  (a worktree has no debug/ folder: pass --dir there)
 *   npm run test:unit-mapper -- --dir <traces>     traces from another checkout (read only)
 *   npm run test:unit-mapper -- --cases            the unit cases only
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { ITrackingStage } from "../models/Order.js";
import {
  fullProductName,
  mapApiUnitToOrder,
  orderViewFromPageFetch,
  referenceLastStep,
  refundCardIn,
  refundFromActionInfo,
  trackingFromApi,
  type ApiOrderView,
  type ApiUnit,
  type MappedOrderUnit,
} from "../services/orderApi.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const argv = process.argv.slice(2);
const argOf = (name: string) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : undefined;
};
/** ORDER_TRACE_DIR from the app's config (.env) when it loads; the check itself needs no setting from it. */
async function configuredTraceDir(): Promise<string> {
  try {
    const { config } = await import("../config.js");
    return config.orderTraceDir || "";
  } catch {
    return "";
  }
}
let traceRoot = "";
const casesOnly = argv.includes("--cases");

/** The nightly fetch of 24-11 (the plan's counts) and the order compared with the reference. */
const NIGHTLY = "fetch-2026-11-24T23-33-00-153Z";
const COMPARED = "OD338983063918894100";
/** The reference's Last Tracking Step (U5). */
const STEP_DATE = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{6})?\\+05:30";
const STEP_RE = new RegExp(`^${STEP_DATE} \\| [^|]+( \\| [^|]* \\| [^|]* \\| ${STEP_DATE})?$`);

let passed = 0;
const failures: string[] = [];
function check(name: string, fn: () => void) {
  try {
    fn();
    passed++;
    console.log(`  ok    ${name}`);
  } catch (err) {
    failures.push(name);
    console.log(`  FAIL  ${name}\n        ${err instanceof Error ? err.message.split("\n").slice(0, 6).join("\n        ") : err}`);
  }
}

// ── 1. Unit cases ────────────────────────────────────────────────────────────────────────────────────────────────────

const at = (s: string) => new Date(s).getTime();
const stage = (date: string | null, status: string, stepText = status, extra: Array<{ date: string | null; remark: string }> = []): ITrackingStage => ({
  date,
  stage: "DONE",
  status,
  detailed_steps: [{ event: "step", step_text: stepText, progress: [{ date, remark: stepText, location: null }, ...extra.map((e) => ({ ...e, location: null }))] }],
});
const widget = (status: string, subHeading: string, amount?: string) => ({ status, subHeading, heading: amount ? { contents: [{ text: amount }] } : null });
const block = (heading: string | null, widgets: Array<ReturnType<typeof widget>>) => ({ actionInfoDataBag: { heading: heading ? { contents: [{ text: heading }] } : null, subWidgets: widgets } });

/** An order view around one invented unit, as orderViewFromPageFetch returns it. */
function viewOf(unit: ApiUnit, extra: Partial<ApiOrderView> = {}): ApiOrderView {
  return {
    orderMetaData: { orderId: "OD999000000000000100", orderDate: at("2026-11-01T10:00:00+05:30") },
    units: { "999000000000000100000": unit },
    productDataBag: { VERIFYFSN0000001: { productBasicData: { title: "Verify Phone 5G (Black, 128 GB)", subTitle: "8 GB RAM" } } },
    ...extra,
  };
}
const node = (title: string, date: number | null, children: Array<{ text: string; at: number | null; infos?: Array<{ remark: string; at: number }> }>, status = "DONE") => ({
  nodeTitle: title,
  stepStatus: status,
  date,
  children: children.map((c) => ({
    eventName: "step",
    eventDate: c.at,
    stepState: { key: "COMPLETE", text: c.text },
    progressStepInfoList: (c.infos || []).map((i) => ({ remark: i.remark, updatedDate: i.at })),
  })),
});
function unitWith(nodes: ReturnType<typeof node>[], key: string, extra: Partial<ApiUnit> = {}): ApiUnit {
  return {
    metaData: { unitId: "999000000000000100000", fsn: "VERIFYFSN0000001", title: "Verify Phone 5G (Black, 128 GB)", quantity: 1, status: { key, text: key } },
    moneyDataBag: { amount: 12999 },
    deliveryDataBag: { promiseDataBag: { promisedDate: at("2026-11-05T23:59:59+05:30") } },
    orderUnitProgressStepsV1: { unitProgressSteps: nodes },
    ...extra,
  };
}

function unitCases() {
  console.log("U2 — product name (title + subTitle)");
  check("title + subtitle in brackets; a title that holds it stays; no subtitle stays; blanks", () => {
    assert.equal(fullProductName("MOTOROLA Edge 70 Fusion (Pantone SILHOUETTE, 128 GB)", "8 GB RAM"), "MOTOROLA Edge 70 Fusion (Pantone SILHOUETTE, 128 GB) (8 GB RAM)");
    assert.equal(fullProductName("Verify Phone (8 GB RAM, 128 GB)", "8 gb ram"), "Verify Phone (8 GB RAM, 128 GB)");
    assert.equal(fullProductName("Verify Phone (Black, 128 GB)", ""), "Verify Phone (Black, 128 GB)");
    assert.equal(fullProductName("Verify Phone", null), "Verify Phone");
    assert.equal(fullProductName("  Verify Strap ", " Black Strap, Free Size "), "Verify Strap (Black Strap, Free Size)");
    assert.equal(fullProductName("", "8 GB RAM"), "", "no name is made from a subtitle alone");
  });
  check("the mapper writes product_name and keeps title / subtitle in api_extra", () => {
    const m = mapApiUnitToOrder(viewOf(unitWith([], "Shipped")), "999000000000000100000");
    assert.equal(m.product_name, "Verify Phone 5G (Black, 128 GB) (8 GB RAM)");
    assert.equal(m.api_extra.product_title, "Verify Phone 5G (Black, 128 GB)");
    assert.equal(m.api_extra.product_subtitle, "8 GB RAM");
  });

  console.log("\nU3 — refund card (masked sentences: invented banks' digits)");
  const now = new Date("2026-11-25T12:00:00+05:30");
  const refundStage = [stage("2026-10-20T10:00:00+05:30", "Cancelled", "Your order was cancelled."), stage("2026-10-21T15:00:00+05:30", "Refund", "Refund completed")];
  check("'Refund was added to your … Card ****NNNN on Mon DD HH:MM PM' — Completed, amount, date (IST, the Refund stage's year), card", () => {
    const r = refundFromActionInfo(block("Total refund - %E2%82%B912,999", [widget("Completed", "Refund was added to your  VERIFYBANK MASTERCARD Credit Card ************0000 on Oct 21 03:15 PM.If you can’t see the refund in your bank statement, contact your bank.")]), refundStage, now)!;
    assert.equal(r.refund_status, "Completed");
    assert.equal(r.refund_amount, "12999.00");
    assert.equal(r.refund_kind, "refund");
    assert.equal(r.refund_date?.toISOString(), new Date("2026-10-21T15:15:00+05:30").toISOString());
    assert.deepEqual(r.card, { bank: "VERIFYBANK", network: "MASTERCARD", type: "Credit", last4: "0000" });
    assert.ok(r.refund_msg?.startsWith("Refund was added to your"));
  });
  check("'… charged should have been reversed to your … Card' is a reversal, not money paid back; no date", () => {
    const r = refundFromActionInfo(block("Total refund - ₹9,999", [widget("Completed", "₹9999.0 charged should have been reversed to your VERIFY BANK VISA Credit Card ************0001 statement. There should not be any credit/debit entry.")]), [], now)!;
    assert.equal(r.refund_kind, "reversal");
    assert.equal(r.refund_amount, "9999.00");
    assert.equal(r.refund_date, null, "no sentence date and no Refund stage");
    assert.deepEqual(r.card, { bank: "VERIFY BANK", network: "VISA", type: "Credit", last4: "0001" });
  });
  check("'… will be refunded to your … on May DD YYYY, HH:MM AM' — the sentence's own year", () => {
    const r = refundFromActionInfo(block("Total refund - %E2%82%B91,499", [widget("Completed", "%E2%82%B91499.0 will be refunded to your VERIFYBANK RUPAY Debit Card ************0002 on May 05 2026, 09:05 AM. If you can’t see the refund, share refund reference number 000000000000.")]), [], now)!;
    assert.equal(r.refund_date?.toISOString(), new Date("2026-05-05T09:05:00+05:30").toISOString());
    assert.deepEqual(r.card, { bank: "VERIFYBANK", network: "RUPAY", type: "Debit", last4: "0002" });
    assert.ok(r.refund_msg?.startsWith("₹1499.0 will be refunded"), "the URL-encoded rupee sign is decoded");
  });
  check("'… processed … by Tomorrow, HH:MM PM' — Processed, the date of the Refund stage", () => {
    const r = refundFromActionInfo(block("Total refund - ₹2,000", [widget("Processed", "Refund is successfully processed and will be available in your  VERIFYBANK MASTERCARD Credit Card ************0003 by Tomorrow, 11:59 PM. Once the refund is credited…")]), refundStage, now)!;
    assert.equal(r.refund_status, "Processed");
    assert.equal(r.refund_date?.toISOString(), new Date("2026-10-21T15:00:00+05:30").toISOString());
    assert.equal(r.card?.last4, "0003");
  });
  check("SuperCoins: kind supercoins, the heading's number, no card", () => {
    const r = refundFromActionInfo(block("Total refund - 25", [widget("Completed", "The refund was added to your SuperCoins balance on by Nov 02 04:10 PM.")]), [], now)!;
    assert.equal(r.refund_kind, "supercoins");
    assert.equal(r.refund_amount, "25.00");
    assert.equal(r.card, null);
    assert.equal(r.refund_date?.toISOString(), new Date("2026-11-02T16:10:00+05:30").toISOString());
  });
  check("a date in December read in January takes the year before (never after now + 1 day)", () => {
    const r = refundFromActionInfo(block("Total refund - ₹10", [widget("Completed", "Refund was added to your VERIFYBANK VISA Credit Card ************0004 on Dec 30 10:00 AM.")]), [], new Date("2027-01-02T10:00:00+05:30"))!;
    assert.equal(r.refund_date?.toISOString(), new Date("2026-12-30T10:00:00+05:30").toISOString());
  });
  check("'by Nov 23' read on Nov 18 with a Refund stage on Nov 23 takes the stage's year, not the year before (no now + 1 day cap then)", () => {
    const stageNov23 = [stage("2026-11-17T10:00:00+05:30", "Cancelled", "Your order was cancelled."), stage("2026-11-23T12:02:00+05:30", "Refund", "Refund completed")];
    const r = refundFromActionInfo(block("Total refund - ₹10", [widget("Completed", "Refund is successfully processed and will be available in your VERIFYBANK VISA Credit Card ************0007 by Nov 23 12:02 PM.")]), stageNov23, new Date("2026-11-18T09:00:00+05:30"))!;
    assert.equal(r.refund_date?.toISOString(), new Date("2026-11-23T12:02:00+05:30").toISOString());
  });
  check("no heading: the movements' own amounts are summed; Accepted wins over Completed", () => {
    const r = refundFromActionInfo(block(null, [widget("Completed", "Refund was added to your VERIFYBANK VISA Credit Card ************0005 on Oct 01 10:00 AM.", "%E2%82%B9100"), widget("Accepted", "Refund was added to your VERIFYBANK VISA Credit Card ************0005 on Oct 02 10:00 AM.", "₹50.5")]), [], now)!;
    assert.equal(r.refund_amount, "150.50");
    assert.equal(r.refund_status, "Accepted");
    assert.equal(r.refund_date?.toISOString(), new Date("2026-10-02T10:00:00+05:30").toISOString(), "the latest movement");
    assert.equal(r.refund_msg?.split(" | ").length, 2);
  });
  check("'We have resent this product' is a replacement: null; no sub-widgets: null; the mapper keeps the sentence in api_extra", () => {
    const resent = block(null, [widget("Completed", "We have resent this product. Due to shipment-related issues, we could not deliver this product via the original order.")]);
    assert.equal(refundFromActionInfo(resent, [], now), null);
    assert.equal(refundFromActionInfo({ actionInfoDataBag: { heading: null, subWidgets: [] } }, [], now), null);
    assert.equal(refundFromActionInfo({}, [], now), null);
    const m = mapApiUnitToOrder(viewOf(unitWith([], "Cancelled", resent as Partial<ApiUnit>)), "999000000000000100000");
    assert.deepEqual([m.refund_status, m.refund_amount, m.refund_msg, m.refund_kind, m.refund_card], [null, null, null, null, null]);
    assert.ok(m.api_extra.action_info?.startsWith("We have resent this product"));
  });
  check("the card regex: bank of several words, X-masked digits", () => {
    assert.deepEqual(refundCardIn("added to your VERIFY SMALL BANK MASTERCARD Debit Card XXXXXXXX0006 on"), { bank: "VERIFY SMALL BANK", network: "MASTERCARD", type: "Debit", last4: "0006" });
    assert.equal(refundCardIn("added to your SuperCoins balance"), null);
  });

  console.log("\nU4 / U16 — current promise, Flipkart's hand-over time");
  check("delivery_date = currentSlotEndDate ?? newPromisedDate ?? promisedDate; the first promise in api_extra", () => {
    const base = unitWith([], "Shipped");
    const p = (bag: Record<string, number | null>) => mapApiUnitToOrder(viewOf({ ...base, deliveryDataBag: { promiseDataBag: bag } }), "999000000000000100000");
    const first = at("2026-11-05T23:59:59+05:30");
    const moved = at("2026-11-06T23:59:59+05:30");
    const slot = at("2026-11-06T11:30:00+05:30");
    assert.equal(p({ promisedDate: first, newPromisedDate: moved, currentSlotEndDate: slot }).delivery_date?.getTime(), slot);
    assert.equal(p({ promisedDate: first, newPromisedDate: moved }).delivery_date?.getTime(), moved);
    assert.equal(p({ promisedDate: first }).delivery_date?.getTime(), first);
    assert.equal(p({ promisedDate: first, currentSlotEndDate: slot }).api_extra.first_promised_date, "2026-11-05T23:59:59+05:30");
  });
  check("actual_delivery_date = actualDeliveredDate, else the Delivered stage; the stage time kept in api_extra", () => {
    const nodes = [node("Order Confirmed", at("2026-11-01T10:00:00+05:30"), [{ text: "Your Order has been placed.", at: at("2026-11-01T10:00:30+05:30") }]), node("Delivered", at("2026-11-04T18:00:00+05:30"), [{ text: "Your item has been delivered", at: at("2026-11-04T18:00:00+05:30") }])];
    const handOver = at("2026-11-04T15:10:00+05:30");
    const withScan = mapApiUnitToOrder(viewOf(unitWith(nodes, "Delivered", { deliveryDataBag: { promiseDataBag: { promisedDate: at("2026-11-05T23:59:59+05:30"), actualDeliveredDate: handOver } } })), "999000000000000100000");
    assert.equal(withScan.actual_delivery_date?.getTime(), handOver);
    assert.equal(withScan.api_extra.delivered_stage_at, "2026-11-04T18:00:00+05:30");
    const noScan = mapApiUnitToOrder(viewOf(unitWith(nodes, "Delivered")), "999000000000000100000");
    assert.equal(noScan.actual_delivery_date?.getTime(), at("2026-11-04T18:00:00+05:30"));
  });

  console.log("\nU5 — cancelled label, status reason, Last Tracking Step, milliseconds");
  const cancelledNodes = [
    node("Order Confirmed", at("2026-11-01T10:00:00+05:30"), [{ text: "Your Order has been placed.", at: at("2026-11-01T10:00:30+05:30") }]),
    node("Cancelled", at("2026-11-02T09:00:00+05:30"), [{ text: "Your order was cancelled as per your request.", at: at("2026-11-02T09:00:00.250+05:30") }]),
    node("Refund", at("2026-11-03T12:00:00+05:30"), [{ text: "Refund completed", at: at("2026-11-03T12:00:00+05:30"), infos: [{ remark: "Refund completed", at: at("2026-11-03T12:00:00+05:30") }, { remark: "₹12,999 added to your card", at: at("2026-11-03T12:05:00.5+05:30") }] }]),
  ];
  check("a cancelled unit reads the cancel step's sentence (not 'Refund'); status_reason only when Flipkart sends one", () => {
    const m = mapApiUnitToOrder(viewOf(unitWith(cancelledNodes, "Cancelled")), "999000000000000100000");
    assert.equal(m.status_key, "Cancelled");
    assert.equal(m.status_label, "Your order was cancelled as per your request.");
    assert.equal(m.status_reason, null);
    assert.equal(m.cancel_reason_text, "Cancelled");
    const withReason = mapApiUnitToOrder(viewOf(unitWith(cancelledNodes, "Cancelled", { metaData: { ...unitWith([], "Cancelled").metaData, statusReason: "Seller could not ship" } })), "999000000000000100000");
    assert.equal(withReason.status_reason, "Seller could not ship");
  });
  check("timeline dates keep non-zero milliseconds; whole seconds print as before", () => {
    const t = trackingFromApi(unitWith(cancelledNodes, "Cancelled"));
    assert.equal(t[1].date, "2026-11-02T09:00:00.250+05:30");
    assert.equal(t[0].date, "2026-11-01T10:00:30+05:30");
  });
  check("Last Tracking Step: 'date | stage', and '| step | last remark | its date' when Flipkart added remarks; ASCII, 6-digit fraction", () => {
    const m = mapApiUnitToOrder(viewOf(unitWith(cancelledNodes, "Cancelled")), "999000000000000100000");
    assert.equal(m.last_tracking_step, "2026-11-03T12:00:00+05:30 | Refund | Refund completed | Rs.12,999 added to your card | 2026-11-03T12:05:00.500000+05:30");
    assert.match(m.last_tracking_step, STEP_RE);
    assert.equal(referenceLastStep([stage("2026-10-27T21:55:34+05:30", "Delivered", "Your item has been delivered")]), "2026-10-27T21:55:34+05:30 | Delivered");
    assert.equal(referenceLastStep([stage("2026-10-27T21:55:34.120+05:30", "Out for delivery · today", "x|y")]), "2026-10-27T21:55:34.120000+05:30 | Out for delivery - today");
    assert.equal(referenceLastStep([]), "");
    assert.equal(referenceLastStep([{ ...stage(null, "Shipped"), stage: "PENDING" }]), "");
  });

  console.log("\nT8 — Delivered only from Flipkart's key or a dated Delivered stage; a later Cancelled / Refund wins");
  const placed = node("Order Confirmed", at("2026-11-01T10:00:00+05:30"), [{ text: "Your Order has been placed.", at: at("2026-11-01T10:00:30+05:30") }]);
  const datelessDelivered = node("Delivered", null, [{ text: "Your item has been delivered", at: null }]);
  const cancelledNode = node("Cancelled", at("2026-11-03T09:00:00+05:30"), [{ text: "Your order was cancelled.", at: at("2026-11-03T09:00:00+05:30") }]);
  const refundNode = node("Refund", at("2026-11-04T09:00:00+05:30"), [{ text: "Refund completed", at: at("2026-11-04T09:00:00+05:30") }]);
  check("a dateless DONE Delivered node followed by Cancelled + Refund → Cancelled (T8's shape)", () => {
    const m = mapApiUnitToOrder(viewOf(unitWith([placed, datelessDelivered, cancelledNode, refundNode], "Cancelled")), "999000000000000100000");
    assert.equal(m.status_key, "Cancelled");
    assert.equal(m.actual_delivery_date, null);
    assert.equal(m.status_label, "Your order was cancelled.");
  });
  check("a dateless Delivered node alone, Flipkart's key not Delivered → not Delivered", () => {
    const m = mapApiUnitToOrder(viewOf(unitWith([placed, datelessDelivered], "Shipped")), "999000000000000100000");
    assert.notEqual(m.status_key, "Delivered");
  });
  check("Flipkart's key Delivered with a Refund stage after the Delivered one → Cancelled; without one → Delivered", () => {
    const dated = node("Delivered", at("2026-11-02T18:00:00+05:30"), [{ text: "Your item has been delivered", at: at("2026-11-02T18:00:00+05:30") }]);
    assert.equal(mapApiUnitToOrder(viewOf(unitWith([placed, dated, refundNode], "Delivered")), "999000000000000100000").status_key, "Cancelled");
    assert.equal(mapApiUnitToOrder(viewOf(unitWith([placed, dated], "Delivered")), "999000000000000100000").status_key, "Delivered");
    assert.equal(mapApiUnitToOrder(viewOf(unitWith([placed, dated], "Purchased")), "999000000000000100000").status_key, "Delivered", "a dated stage still makes it Delivered");
    assert.equal(mapApiUnitToOrder(viewOf(unitWith([placed], "Delivered")), "999000000000000100000").status_key, "Delivered", "the key alone still does");
  });
  // OD339115273746442100's stored shape: … Delivery Unsuccessful > Delivered (DONE, no date) > Refund (DONE), no Cancelled stage.
  const shipped = node("Shipped", at("2026-11-02T08:00:00+05:30"), [{ text: "Your item has been shipped.", at: at("2026-11-02T08:00:00+05:30") }]);
  const unsuccessful = node("Delivery Unsuccessful", at("2026-11-03T18:00:00+05:30"), [{ text: "Delivery attempt was unsuccessful", at: at("2026-11-03T18:00:00+05:30") }]);
  for (const key of ["Cancelled", "Delivered"]) {
    check(`the real T8 shape (dateless Delivered, then Refund, no Cancelled stage), key ${key} → Cancelled, the Refund step's sentence, cancelled_date = the Refund stage`, () => {
      const m = mapApiUnitToOrder(viewOf(unitWith([placed, shipped, unsuccessful, datelessDelivered, refundNode], key)), "999000000000000100000");
      assert.equal(m.status_key, "Cancelled");
      assert.equal(m.status_label, "Refund completed", "not 'Delivered' (a stage title)");
      assert.equal(m.cancelled_date?.toISOString(), new Date("2026-11-04T09:00:00+05:30").toISOString(), "not the order date");
      assert.equal(m.actual_delivery_date, null);
    });
  }
  check("return / refund keys after a dated Delivered stage and a Refund stage: Returned → Returned, Refund* → Refunded, an open return stays Delivered", () => {
    const dated = node("Delivered", at("2026-11-02T18:00:00+05:30"), [{ text: "Your item has been delivered", at: at("2026-11-02T18:00:00+05:30") }]);
    const st = (key: string) => mapApiUnitToOrder(viewOf(unitWith([placed, dated, refundNode], key)), "999000000000000100000").status_key;
    assert.equal(st("Returned"), "Returned");
    assert.equal(st("Refund Completed"), "Refunded");
    assert.equal(st("REFUNDED"), "Refunded");
    assert.equal(st("Return requested"), "Delivered");
    assert.equal(st("Return approved"), "Delivered");
  });
}

// ── 2. The saved traces ──────────────────────────────────────────────────────────────────────────────────────────────

interface TraceRecord {
  at?: string;
  orderId: string;
  unitId: string;
  mode?: string;
  raw?: unknown;
  mapped?: Record<string, unknown>;
  outcome?: string;
}

/** The status the mapper gave before T8 (any DONE Delivered stage, dated or not, made the unit Delivered). */
function statusBeforeT8(unit: ApiUnit): string {
  const tracking = trackingFromApi(unit);
  const statusKey = String(unit.metaData?.status?.key || "");
  if (/not placed/i.test(statusKey)) return "Failed";
  const delivered = /^delivered$/i.test(statusKey) || tracking.some((s) => /^delivered$/i.test(s.status) && s.stage === "DONE");
  if (delivered) return "Delivered";
  if (/^cancelled$/i.test(statusKey) || tracking.some((s) => /^cancelled$/i.test(s.status) && s.stage === "DONE")) return "Cancelled";
  return "(other)";
}
const json = (v: unknown) => JSON.stringify(v ?? null);
const istDay = (d: Date | null) => (d ? new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Kolkata" }).format(d) : "");

function traces() {
  if (!fs.existsSync(traceRoot)) {
    failures.push("traces");
    console.log(`\n  FAIL  no trace directory at ${traceRoot} (pass --dir <debug/order-traces>)`);
    return;
  }
  const runs = fs
    .readdirSync(traceRoot)
    .filter((d) => fs.existsSync(path.join(traceRoot, d, "manifest.json")))
    .sort();
  console.log(`\ntraces: ${runs.length} runs under ${path.relative(process.cwd(), traceRoot) || "."}`);

  const n = { files: 0, unreadable: 0, api: 0, noView: 0, notApi: 0 };
  const fieldDiff = new Map<string, number>();
  const nightlyDiff = new Map<string, number>();
  const stageTitles = new Set<string>();
  /** Every distinct traced unit, its newest API file. */
  const newest = new Map<string, { at: number; unit: ApiUnit; mapped: MappedOrderUnit; old: Record<string, unknown> | undefined; status: string }>();
  const t8: Record<string, number> = {};
  const nightly = {
    units: 0,
    nameGain: 0,
    nameAlready: 0,
    nameNone: 0,
    nameOther: 0,
    subRam: 0,
    subEmpty: 0,
    subOther: 0,
    failed: 0,
    slotPresent: 0,
    dueEqualsSlot: 0,
    dueMoved: 0,
    dueMovedNewPromise: 0,
    newPromiseLater: 0,
    newPromiseLaterDelivered: 0,
    newPromiseLaterCancelled: 0,
    delivered: 0,
    deliveredWithScan: 0,
    adEqualsScan: 0,
    adOtherDay: 0,
    stepBad: 0,
    stepNonAscii: 0,
    stepEmpty: 0,
    labelIsStageTitle: 0,
    cancelled: 0,
    cancelledLabelWasRefund: 0,
    tracksWithFraction: 0,
    refundBlocks: 0,
    replacements: 0,
    cancelledDateMoved: 0,
    cancelledDateSubSecond: 0,
    stepWithRemark: 0,
  };
  const comparedUnits = new Map<string, MappedOrderUnit>();
  const allSteps = { units: 0, bad: 0, nonAscii: 0, empty: 0 };
  const cancelledLabels: Array<{ label: string; unit: string }> = [];

  for (const run of runs) {
    const dir = path.join(traceRoot, run);
    for (const f of fs.readdirSync(dir).filter((x) => x.startsWith("unit-") && x.endsWith(".json")).sort()) {
      n.files++;
      let rec: TraceRecord;
      try {
        rec = JSON.parse(fs.readFileSync(path.join(dir, f), "utf8"));
      } catch {
        n.unreadable++;
        continue;
      }
      if (rec.mode !== "api" || !rec.raw) {
        n.notApi++;
        continue;
      }
      const view = orderViewFromPageFetch(rec.raw);
      const unit = view?.units?.[rec.unitId];
      if (!view || !unit) {
        n.noView++;
        continue;
      }
      n.api++;
      const when = rec.at ? new Date(rec.at) : new Date();
      const m = mapApiUnitToOrder(view, rec.unitId, null, when);
      for (const node of unit.orderUnitProgressStepsV1?.unitProgressSteps || []) if (node.nodeTitle) stageTitles.add(String(node.nodeTitle).trim());
      for (const s of m.tracking) stageTitles.add(s.status);

      // Per-field diff against what the run saved (the mapper of that day).
      if (rec.mapped) {
        for (const [k, v] of Object.entries(m)) {
          if (k === "refund_card") continue;
          const was = rec.mapped[k];
          const same = k === "api_extra" ? json(Object.fromEntries(Object.entries(v as object).filter(([kk]) => kk in ((was as object) || {})))) === json(was) : json(v) === json(was);
          if (!same) {
            fieldDiff.set(k, (fieldDiff.get(k) || 0) + 1);
            if (run === NIGHTLY) nightlyDiff.set(k, (nightlyDiff.get(k) || 0) + 1);
          }
        }
      }

      // T8: what the rule before it said.
      const before = statusBeforeT8(unit);
      const nowStatus = m.status_key === "Delivered" || m.status_key === "Cancelled" || m.status_key === "Failed" ? m.status_key : "(other)";
      if (before !== nowStatus) {
        const deliveredNode = (unit.orderUnitProgressStepsV1?.unitProgressSteps || []).find((x) => /^delivered$/i.test(String(x.nodeTitle || "")) && x.stepStatus === "DONE");
        const why = `${before} → ${nowStatus} (key ${unit.metaData?.status?.key}, Delivered node ${deliveredNode ? (deliveredNode.date ? "dated" : "dateless") : "none"})`;
        t8[why] = (t8[why] || 0) + 1;
      }

      // Last Tracking Step on every traced unit.
      allSteps.units++;
      if (!m.last_tracking_step) allSteps.empty++;
      else {
        if (!STEP_RE.test(m.last_tracking_step)) allSteps.bad++;
        if (/[^\x00-\x7F]/.test(m.last_tracking_step)) allSteps.nonAscii++;
      }
      if (m.status_key === "Cancelled") cancelledLabels.push({ label: m.status_label, unit: rec.unitId });

      const prev = newest.get(rec.unitId);
      if (!prev || when.getTime() >= prev.at) newest.set(rec.unitId, { at: when.getTime(), unit, mapped: m, old: rec.mapped, status: m.status_key });
      if (rec.orderId === COMPARED) comparedUnits.set(rec.unitId, m);

      if (run !== NIGHTLY) continue;
      // ── The plan's counts on the 24-11 nightly trace ──
      const N = nightly;
      N.units++;
      const product = view.productDataBag?.[String(unit.metaData?.fsn || "")]?.productBasicData || view.productDataBag?.[String(unit.metaData?.listingId || "")]?.productBasicData;
      const sub = String(product?.subTitle || "").trim();
      const title = String(unit.metaData?.title || product?.title || "").trim();
      const oldName = String(rec.mapped?.product_name ?? "");
      if (!sub) N.nameNone++;
      else if (title.toLowerCase().includes(sub.toLowerCase())) N.nameAlready++;
      if (sub && m.product_name === `${oldName} (${sub})`) N.nameGain++;
      else if (m.product_name !== oldName) N.nameOther++;
      if (!sub) N.subEmpty++;
      else if (/^\d+\s*GB RAM$/i.test(sub)) N.subRam++;
      else N.subOther++;

      const promise = unit.deliveryDataBag?.promiseDataBag || {};
      const failed = m.status_key === "Failed";
      if (failed) N.failed++;
      if (promise.currentSlotEndDate) N.slotPresent++;
      const due = m.delivery_date?.getTime() ?? null;
      if (!failed && due === (promise.currentSlotEndDate ?? null)) N.dueEqualsSlot++;
      const oldDue = rec.mapped?.delivery_date ? new Date(String(rec.mapped.delivery_date)).getTime() : null;
      const later = promise.newPromisedDate != null && promise.promisedDate != null && promise.newPromisedDate > promise.promisedDate;
      if (later) {
        N.newPromiseLater++;
        if (m.status_key === "Delivered") N.newPromiseLaterDelivered++;
        if (m.status_key === "Cancelled") N.newPromiseLaterCancelled++;
      }
      if (due !== oldDue) {
        N.dueMoved++;
        if (later) N.dueMovedNewPromise++;
      }
      if (m.status_key === "Delivered") {
        N.delivered++;
        if (promise.actualDeliveredDate) {
          N.deliveredWithScan++;
          if (m.actual_delivery_date?.getTime() === promise.actualDeliveredDate) N.adEqualsScan++;
          const stageAt = m.api_extra.delivered_stage_at ? new Date(m.api_extra.delivered_stage_at) : null;
          if (stageAt && istDay(stageAt) !== istDay(m.actual_delivery_date)) N.adOtherDay++;
        }
      }
      if (!m.last_tracking_step) N.stepEmpty++;
      else {
        if (!STEP_RE.test(m.last_tracking_step)) N.stepBad++;
        if (/[^\x00-\x7F]/.test(m.last_tracking_step)) N.stepNonAscii++;
      }
      if (/ \| .* \| .* \| /.test(m.last_tracking_step)) N.stepWithRemark++;
      const oldCancelled = rec.mapped?.cancelled_date ? new Date(String(rec.mapped.cancelled_date)).getTime() : null;
      if ((m.cancelled_date?.getTime() ?? null) !== oldCancelled) {
        N.cancelledDateMoved++;
        if (oldCancelled != null && m.cancelled_date && Math.abs(m.cancelled_date.getTime() - oldCancelled) < 1000) N.cancelledDateSubSecond++;
      }
      if (m.status_key === "Cancelled") {
        N.cancelled++;
        if (String(rec.mapped?.status_label || "") === "Refund") N.cancelledLabelWasRefund++;
      }
      const widgets = unit.actionInfoDataBag?.subWidgets || [];
      if (widgets.length) {
        N.refundBlocks++;
        if (widgets.every((w) => /we have resent this product/i.test(String(w?.subHeading || "")))) N.replacements++;
      }
      if (m.tracking.some((s) => s.detailed_steps.some((st) => st.progress.some((p) => /\.\d{3}\+05:30$/.test(String(p.date || "")))))) N.tracksWithFraction++;
    }
  }

  console.log(`  unit files ${n.files} · re-mapped (api with a raw answer) ${n.api} · page snapshots / failed reads ${n.notApi} · no order view ${n.noView} · unreadable ${n.unreadable} · distinct units ${newest.size}`);

  // ── Refund cards over every distinct traced unit (newest answer) ──
  const R = { blocks: 0, replacements: 0, money: 0, withHeading: 0, headingMatches: 0, noHeadingSummed: 0, statuses: {} as Record<string, number>, kinds: {} as Record<string, number>, cards: 0, cardLines: 0, cardTypes: {} as Record<string, number>, networks: {} as Record<string, number>, dated: 0, datedWithStage: 0, dateFarFromStage: [] as string[], unitsWithCardLine: 0, cardMatches: 0, deliveredWithRefund: 0, multi: 0, byStatus: {} as Record<string, number>, statusAsSent: 0 };
  for (const [unitKey, u] of newest) {
    const widgets = u.unit.actionInfoDataBag?.subWidgets || [];
    if (!widgets.length) {
      continue;
    }
    R.blocks++;
    if (widgets.length > 1) R.multi++;
    const sentences = widgets.map((w) => String(w?.subHeading || ""));
    if (sentences.every((t) => /we have resent this product/i.test(t))) {
      R.replacements++;
      continue;
    }
    R.money++;
    R.byStatus[u.status] = (R.byStatus[u.status] || 0) + 1;
    if (u.status === "Delivered") R.deliveredWithRefund++;
    const heading = (u.unit.actionInfoDataBag?.heading?.contents || []).map((c) => String(c?.text ?? "")).join(" ");
    const hm = heading.replace(/%E2%82%B9/gi, "₹").match(/total refund\s*-\s*₹?\s*([\d,]+(?:\.\d+)?)/i);
    if (hm) {
      R.withHeading++;
      if (u.mapped.refund_amount === Number(hm[1].replace(/,/g, "")).toFixed(2)) R.headingMatches++;
    } else if (u.mapped.refund_amount != null) R.noHeadingSummed++;
    const sent = widgets.map((w) => String(w?.status || "")).filter(Boolean);
    const expected = sent.every((s) => s === "Completed") ? "Completed" : sent.find((s) => s !== "Completed");
    if (u.mapped.refund_status === expected) R.statusAsSent++;
    for (const s of sent) R.statuses[s] = (R.statuses[s] || 0) + 1;
    R.kinds[String(u.mapped.refund_kind)] = (R.kinds[String(u.mapped.refund_kind)] || 0) + 1;
    if (u.mapped.refund_date) R.dated++;
    for (const t of sentences) if (refundCardIn(t)) R.cardLines++;
    // The mapper's card is the first card sentence's, on every unit that has one.
    const firstCard = sentences.map((t) => refundCardIn(t.replace(/%E2%82%B9/gi, "₹"))).find(Boolean) || null;
    if (firstCard) {
      R.unitsWithCardLine++;
      if (json(u.mapped.refund_card) === json(firstCard)) R.cardMatches++;
    }
    // A sentence date far from its own Refund stage is a wrong year.
    const refundStageAt = [...u.mapped.tracking].reverse().find((s) => /^refund/i.test(s.status) && s.stage === "DONE" && s.date)?.date;
    if (u.mapped.refund_date && refundStageAt) {
      R.datedWithStage++;
      if (Math.abs(u.mapped.refund_date.getTime() - new Date(refundStageAt).getTime()) > 60 * 86400_000) R.dateFarFromStage.push(unitKey);
    }
    if (u.mapped.refund_card) {
      R.cards++;
      R.cardTypes[u.mapped.refund_card.type] = (R.cardTypes[u.mapped.refund_card.type] || 0) + 1;
      R.networks[u.mapped.refund_card.network] = (R.networks[u.mapped.refund_card.network] || 0) + 1;
    }
  }
  let refundWithoutBlock = 0;
  let replacementWithRefund = 0;
  for (const [, u] of newest) {
    const widgets = u.unit.actionInfoDataBag?.subWidgets || [];
    const has = u.mapped.refund_status != null || u.mapped.refund_amount != null || u.mapped.refund_msg != null || u.mapped.refund_card != null;
    if (!widgets.length && has) refundWithoutBlock++;
    if (widgets.length && widgets.every((w) => /we have resent this product/i.test(String(w?.subHeading || ""))) && has) replacementWithRefund++;
  }

  const N = nightly;
  console.log(`\nU2 — product name, ${NIGHTLY} (${N.units} re-mapped units)`);
  check(`2,725 names gain ' (subTitle)', 20 already hold it, 1,185 without one stay — got ${N.nameGain} / ${N.nameAlready} / ${N.nameNone}, ${N.nameOther} other changes`, () => {
    assert.deepEqual([N.nameGain, N.nameAlready, N.nameNone, N.nameOther], [2725, 20, 1185, 0]);
  });
  console.log(`        subTitle: 'N GB RAM' ${N.subRam} · other text ${N.subOther} · empty ${N.subEmpty}`);
  const compared = [...comparedUnits.values()];
  check(`the compared order ${COMPARED}: 7 units, each ending '(8 GB RAM)' — got ${compared.length}, ${compared.filter((u) => u.product_name.endsWith("(8 GB RAM)")).length}`, () => {
    assert.equal(compared.length, 7);
    assert.ok(compared.every((u) => u.product_name.endsWith("(8 GB RAM)")));
  });

  console.log(`\nU3 — refund cards, ${newest.size} distinct traced units (newest answer of each)`);
  check(`314 units carry a refund card — got ${R.blocks} (${R.money} money, ${R.replacements} replacements, ${R.multi} with more than one movement); the nightly's 7 replacements — got ${N.replacements} (${N.refundBlocks} cards there)`, () => {
    assert.equal(R.blocks, 314);
    assert.equal(N.replacements, 7);
  });
  check(`every 'Total refund - ₹N' heading gives refund_amount — ${R.headingMatches} of ${R.withHeading}; ${R.noHeadingSummed} cards without a heading take the movements' sum`, () => {
    assert.equal(R.headingMatches, R.withHeading);
    assert.equal(R.withHeading + R.noHeadingSummed, R.money);
  });
  check(`statuses come through as sent — ${R.statusAsSent} of ${R.money} (${JSON.stringify(R.statuses)}); kinds ${JSON.stringify(R.kinds)}; dated ${R.dated}`, () => {
    assert.equal(R.statusAsSent, R.money);
  });
  check(`268 card lines become card fields — ${R.cardLines} card sentences, ${R.cards} units with a card (${JSON.stringify(R.cardTypes)}, ${JSON.stringify(R.networks)})`, () => {
    assert.equal(R.cardLines, 268);
    assert.equal(R.cards, 256);
  });
  check(`the mapped card is the first card sentence's on every unit with one — ${R.cardMatches} of ${R.unitsWithCardLine}`, () => {
    assert.equal(R.unitsWithCardLine, R.cards);
    assert.equal(R.cardMatches, R.unitsWithCardLine);
  });
  check(`no refund_date lies more than 60 days from its own Refund stage (a wrong year) — ${R.dateFarFromStage.length} of ${R.datedWithStage}${R.dateFarFromStage.length ? `: ${R.dateFarFromStage.slice(0, 5).join(", ")}` : ""}`, () => {
    assert.equal(R.dateFarFromStage.length, 0);
  });
  check(`the 7 replacements and units without a card keep null; 0 Delivered units get a refund — got ${replacementWithRefund} / ${refundWithoutBlock} / ${R.deliveredWithRefund}; by status ${JSON.stringify(R.byStatus)}`, () => {
    assert.deepEqual([replacementWithRefund, refundWithoutBlock, R.deliveredWithRefund], [0, 0, 0]);
  });

  console.log(`\nU4 — current promise, ${NIGHTLY}`);
  check(`delivery_date = currentSlotEndDate on every unit that has a date (${N.dueEqualsSlot} of ${N.units - N.failed}; ${N.failed} failed stay null; currentSlotEndDate present on ${N.slotPresent} of ${N.units})`, () => {
    assert.equal(N.slotPresent, N.units);
    assert.equal(N.dueEqualsSlot, N.units - N.failed);
  });
  check(`it moves only on the 164 with a later promise — moved ${N.dueMoved}, of them with a later newPromisedDate ${N.dueMovedNewPromise}; later promises ${N.newPromiseLater} (${N.newPromiseLaterDelivered} Delivered, ${N.newPromiseLaterCancelled} Cancelled)`, () => {
    assert.equal(N.newPromiseLater, 164);
    assert.equal(N.dueMovedNewPromise, 164);
    assert.equal(N.dueMoved, N.dueMovedNewPromise, "it moved on no other unit");
  });
  const comparedDue = [...new Set(compared.map((u) => (u.delivery_date ? new Intl.DateTimeFormat("sv-SE", { timeZone: "Asia/Kolkata", dateStyle: "short", timeStyle: "medium" }).format(u.delivery_date).replace(" ", "T") + "+05:30" : "")))];
  check(`the compared order reads 2026-11-03T11:30:00+05:30 — got ${comparedDue.join(", ")}`, () => {
    assert.deepEqual(comparedDue, ["2026-11-03T11:30:00+05:30"]);
  });

  console.log("\nU5 — labels and Last Tracking Step");
  const titleLabels = cancelledLabels.filter((c) => stageTitles.has(c.label));
  check(`0 cancelled labels equal a stage title — ${titleLabels.length} of ${cancelledLabels.length} re-mapped cancelled answers (${stageTitles.size} stage titles seen); the nightly's ${N.cancelledLabelWasRefund} 'Refund' labels of ${N.cancelled} cancelled units are gone`, () => {
    assert.equal(titleLabels.length, 0, `e.g. ${titleLabels.slice(0, 3).map((c) => `${c.unit} '${c.label}'`).join(", ")}`);
  });
  check(`every Last Tracking Step matches the reference's pattern and is ASCII — all traces: ${allSteps.units - allSteps.empty - allSteps.bad} of ${allSteps.units - allSteps.empty} (${allSteps.empty} with no DONE stage, ${allSteps.nonAscii} non-ASCII); nightly ${N.units - N.stepEmpty - N.stepBad} of ${N.units - N.stepEmpty}`, () => {
    assert.deepEqual([allSteps.bad, allSteps.nonAscii, N.stepBad, N.stepNonAscii], [0, 0, 0, 0]);
  });
  const comparedDelivered = compared.filter((u) => u.status_key === "Delivered");
  const asStage = comparedDelivered.filter((u) => u.last_tracking_step === `${u.tracking.filter((s) => s.stage === "DONE").slice(-1)[0]?.date} | Delivered`);
  const unit2 = comparedUnits.get("338983063918894100002")?.last_tracking_step;
  check(`the compared order's Delivered cells read '<Delivered stage time> | Delivered' — ${asStage.length} of ${comparedDelivered.length}; unit …002 (the plan's example) '${unit2}'`, () => {
    assert.equal(comparedDelivered.length, 5);
    assert.equal(asStage.length, comparedDelivered.length);
    assert.equal(unit2, "2026-10-27T21:55:34+05:30 | Delivered");
  });
  console.log(`        nightly: timelines with a millisecond fraction now ${N.tracksWithFraction} of ${N.units}; steps with Flipkart's remark part ${N.stepWithRemark}`);
  check(`cancelled_date only gains Flipkart's milliseconds (toIstIso) — moved on ${N.cancelledDateMoved}, all by under 1 s: ${N.cancelledDateSubSecond}`, () => {
    assert.equal(N.cancelledDateSubSecond, N.cancelledDateMoved);
  });

  console.log(`\nU16 — Actual Delivery Date = Flipkart's actualDeliveredDate, ${NIGHTLY}`);
  // The plan's study counted 1,841 units (stage = scan 186, later < 1 h 368, 1–4 h 1,068, > 4 h 219). No unit set of
  // this trace gives 1,841: it holds 1,838 units with actualDeliveredDate (1,835 Delivered + 3 Cancelled), and the
  // Delivered stage time (node or child event) splits the 1,835 as 187 / 368 / 1,063 / 217. The 212 day changes agree
  // exactly. The measured 1,835 is the gate number here (sub-step 9 review, 2026-11-26).
  check(`equals actualDeliveredDate on every Delivered unit that has one — ${N.adEqualsScan} of ${N.deliveredWithScan} (${N.delivered} Delivered; measured 1,835, the plan's study 1,841); another IST day than the stage on ${N.adOtherDay} (plan: 212)`, () => {
    assert.equal(N.adEqualsScan, N.deliveredWithScan);
    assert.equal(N.deliveredWithScan, 1835);
    assert.equal(N.adOtherDay, 212);
  });

  console.log("\nT8 — status changes against the rule before it, every re-mapped answer");
  const t8Total = Object.values(t8).reduce((a, b) => a + b, 0);
  check(`0 statuses change on the traces (no dateless Delivered node, no Cancelled / Refund after a Delivered stage) — got ${t8Total}${t8Total ? `: ${JSON.stringify(t8)}` : ""}`, () => {
    assert.equal(t8Total, 0);
  });

  const table = (m: Map<string, number>) => [...m].sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k} ${v}`).join(" · ");
  console.log(`\nper-field differences against each file's saved mapped object (api_extra: the keys the file had)`);
  console.log(`  all ${n.api} answers: ${table(fieldDiff) || "none"}`);
  console.log(`  nightly ${N.units}: ${table(nightlyDiff) || "none"}`);
}

async function main() {
  traceRoot = path.resolve(repoRoot, argOf("--dir") || process.env.UNIT_MAPPER_TRACE_DIR || process.env.ORDER_TRACE_DIR || (casesOnly ? "" : await configuredTraceDir()) || "debug/order-traces");
  console.log("unit mapper — sub-step 9 (no network, no Mongo)\n");
  unitCases();
  if (!casesOnly) traces();
  console.log(`\n${passed} passed, ${failures.length} failed${failures.length ? `: ${failures.join(" | ")}` : ""}`);
  process.exitCode = failures.length ? 1 : 0;
}

void main();
