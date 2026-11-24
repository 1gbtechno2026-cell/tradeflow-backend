/**
 * "Start a request" — the pure parts, checked against the real database
 * READ-ONLY (nothing is written, nothing is queued, Flipkart is not called):
 *
 *   · every reference body of Fetch and Update → the selection it makes
 *   · the 2000 cap and its sentence
 *   · "no emails" = every logged-in Flipkart account, with the count
 *   · reference status labels → how units store them (aliases, slugs)
 *   · error classes, the ledger mapping, coupon link checks and page reading
 *
 *   npm run test:start-requests
 */
import assert from "node:assert/strict";
import mongoose from "mongoose";
import { config } from "../config.js";
import { Order } from "../models/Order.js";
import { countKeyOf, OrderInvoiceRequest, REQUEST_TASK_STATUSES } from "../models/OrderInvoiceRequest.js";
import { OrderCouponRequest } from "../models/OrderCouponRequest.js";
import { PlatformId } from "../models/PlatformId.js";
import type { APIRequestContext } from "playwright";
import { assertCouponLinks, couponTaskOutcome, createCouponRequest, LINK_OPENED, openCouponLink, pageTitleAndExcerpt, parseCouponLinks } from "../services/couponRequests.js";
import { createInvoiceRequest, parseInvoiceOrderIds } from "../services/invoiceRequests.js";
import { OrderApiError, OrderApiSessionError } from "../services/orderApi.js";
import { isOrderFetchPreparing, OrderFetchStoppedError, reserveOrderFetch, startOrderFetch, stopOrderFetch } from "../services/orderFetch.js";
import { scheduledFetchCount } from "../services/orderFetchSchedule.js";
import { DEFAULT_UPDATE_STATUSES, inProgressUpdateFilter } from "../services/orderLifecycle.js";
import { resolveTargets } from "../services/orderUpdate.js";
import { classifyPlatformError, countsOf, emptyCounts, ERROR_CLASSES, ledgerPatchOf, livenessOf, RequestInputError, requestProgress, tooManyValuesMessage } from "../services/requestCommon.js";
import { parseFetchBody, parseUpdateBody, planFetch, planUpdate, queueUpdateAfterFetch, statusSelection, storedStatusKeys } from "../services/startRequests.js";
import { OrderJobRun } from "../models/OrderJobRun.js";
import { OrderUpdateRequest } from "../models/OrderUpdateRequest.js";

let passed = 0;
const failures: string[] = [];

async function check(name: string, fn: () => void | Promise<void>) {
  try {
    await fn();
    passed++;
    console.log(`  ok    ${name}`);
  } catch (err) {
    failures.push(name);
    console.log(`  FAIL  ${name}\n        ${err instanceof Error ? err.message.split("\n").slice(0, 6).join("\n        ") : err}`);
  }
}

function refuses(fn: () => unknown, message: string | RegExp, status = 400) {
  try {
    fn();
  } catch (err) {
    assert.ok(err instanceof RequestInputError, `expected a RequestInputError, got ${err}`);
    if (typeof message === "string") assert.equal(err.message, message);
    else assert.match(err.message, message);
    assert.equal(err.status, status);
    return;
  }
  assert.fail("expected a refusal");
}

async function refusesAsync(fn: () => Promise<unknown>, message: string | RegExp, status = 400) {
  try {
    await fn();
  } catch (err) {
    assert.ok(err instanceof RequestInputError, `expected a RequestInputError, got ${err}`);
    if (typeof message === "string") assert.equal(err.message, message);
    else assert.match(err.message, message);
    assert.equal(err.status, status);
    return;
  }
  assert.fail("expected a refusal");
}

const many = (n: number, make: (i: number) => string) => Array.from({ length: n }, (_, i) => make(i));

async function main() {
  await mongoose.connect(config.mongoUri, { serverSelectionTimeoutMS: 15_000 });
  const userId = config.userId;
  assert.ok(userId, "TRADE_FLOW_USER_ID is not set");

  // What the database holds now, so the expectations below are measured, not assumed.
  const before = {
    invoiceRequests: await OrderInvoiceRequest.countDocuments({}).maxTimeMS(20_000),
    couponRequests: await OrderCouponRequest.countDocuments({}).maxTimeMS(20_000),
    updateRequests: await OrderUpdateRequest.countDocuments({}).maxTimeMS(20_000),
    ledgerRuns: await OrderJobRun.countDocuments({}).maxTimeMS(20_000),
  };
  const stored = await storedStatusKeys(userId);
  console.log(`\nstatus keys stored in order_details: ${stored.join(" · ")}\n`);

  console.log("FETCH — bodies");
  await check("the reference body (emails, date, the three flags)", () => {
    const b = parseFetchBody({ platform: "FLIPKART", email_ids: ["A@x.in", "b@x.in", "a@x.in"], enable_login: true, fetch_orders_after_date: "2026-11-23", force_fetch: true, run_update_after_fetch: true });
    assert.deepEqual(b, { platform: "FLIPKART", emails: ["a@x.in", "b@x.in"], afterDate: "2026-11-23", enableLogin: true, forceFetch: true, runUpdateAfterFetch: true, invoices: "off", trigger: "user", by: "", parentId: "", follows: "" });
  });
  await check("a fetch that follows another (the dashboard's Enable login): parent_id + follows are read", () => {
    const b = parseFetchBody({ platform: "FLIPKART", email_ids: ["a@x.in"], parent_id: " 6b05f37c0a11d2e4b5a6c7d8 ", follows: "enable_login" });
    assert.deepEqual([b.parentId, b.follows], ["6b05f37c0a11d2e4b5a6c7d8", "enable_login"]);
  });
  await check("the body this route always took (emails as a typed list, invoices, by)", () => {
    const b = parseFetchBody({ platform: "flipkart", emails: "a@x.in, b@x.in  c@x.in", fetch_orders_after_date: "2026-11-01", invoices: "missing", by: "user1" });
    assert.deepEqual(b.emails, ["a@x.in", "b@x.in", "c@x.in"]);
    assert.equal(b.invoices, "missing");
    assert.equal(b.by, "user1");
    assert.equal(b.runUpdateAfterFetch, false);
  });
  await check("email_ids omitted = no emails (the UI leaves the key out when empty)", () => {
    assert.deepEqual(parseFetchBody({ platform: "FLIPKART", enable_login: false, fetch_orders_after_date: "2026-11-23", force_fetch: true, run_update_after_fetch: true }).emails, []);
    assert.deepEqual(parseFetchBody({ platform: "FLIPKART", email_ids: [] }).emails, []);
  });
  await check('another platform → 400 "Only Flipkart is connected"', () => {
    for (const platform of ["SHOPSY", "AMAZON", "samsung"]) refuses(() => parseFetchBody({ platform, email_ids: ["a@x.in"] }), "Only Flipkart is connected");
    refuses(() => parseUpdateBody({ platform: "AMAZON" }), "Only Flipkart is connected");
  });
  await check("2001 emails → the reference sentence; 2000 pass", () => {
    refuses(() => parseFetchBody({ platform: "FLIPKART", email_ids: many(2001, (i) => `u${i}@x.in`) }), 'Too many values in "Email Addresses": 2001 entered, but the maximum is 2000. Please reduce it.');
    assert.equal(parseFetchBody({ platform: "FLIPKART", email_ids: many(2000, (i) => `u${i}@x.in`) }).emails.length, 2000);
    assert.equal(tooManyValuesMessage("Order ID", 2501), 'Too many values in "Order ID": 2501 entered, but the maximum is 2000. Please reduce it.');
  });

  console.log("\nFETCH — who it selects (read from platformids)");
  const loggedIn = await scheduledFetchCount(userId);
  const sample = (await PlatformId.find({ userId, platform: "flipkart", status: "logged_in", sessionSavedAt: { $ne: null }, "sessionState.cookies.0": { $exists: true } }).select("email").limit(2).maxTimeMS(20_000).lean()).map((r) => r.email);
  await check(`no emails = every logged-in Flipkart account: ${loggedIn}`, async () => {
    const plan = await planFetch(userId, parseFetchBody({ platform: "FLIPKART", fetch_orders_after_date: "2026-11-23", force_fetch: true, run_update_after_fetch: true }));
    assert.equal(plan.selection, "all_accounts");
    assert.equal(plan.emails.length, loggedIn);
    assert.equal(plan.loggedIn, loggedIn);
    assert.equal(new Set(plan.emails).size, plan.emails.length, "an account is listed twice");
    assert.deepEqual(plan.params, {
      platform: "FLIPKART",
      selection_method: "all_accounts",
      email_ids_count: 0,
      accounts_count: loggedIn,
      logged_in_count: loggedIn,
      fetch_orders_after_date: "2026-11-23",
      fetch_orders_after_date_source: "request",
      force_fetch: true,
      enable_login: false,
      run_update_after_fetch: true,
      invoices: "off",
    });
    assert.equal(plan.sinceDate.toISOString(), "2026-11-22T18:30:00.000Z", "the date is the IST day");
  });
  await check("emails given: selection email_ids, logged-in ones counted, the rest still listed", async () => {
    assert.equal(sample.length, 2, "need two logged-in Platform IDs to check this");
    const plan = await planFetch(userId, parseFetchBody({ platform: "FLIPKART", email_ids: [...sample, "nobody-here@verify.invalid"], fetch_orders_after_date: "2026-11-23" }));
    assert.equal(plan.selection, "email_ids");
    assert.equal(plan.emails.length, 3);
    assert.equal(plan.loggedIn, 2);
    assert.equal(plan.params.email_ids_count, 3);
    assert.equal(plan.params.force_fetch, false);
    assert.ok(!("parent_id" in plan.params) && !("follows" in plan.params), "an ordinary fetch carries no parent");
    // The follow-up of Enable login: the mark is kept in params, which the ledger row stores (meta.params).
    const follow = await planFetch(userId, parseFetchBody({ platform: "FLIPKART", email_ids: sample, fetch_orders_after_date: "2026-11-23", parent_id: "6b05f37c0a11d2e4b5a6c7d8", follows: "enable_login" }));
    assert.deepEqual([follow.params.parent_id, follow.params.follows], ["6b05f37c0a11d2e4b5a6c7d8", "enable_login"]);
  });
  await check("no date = the workspace's look-back, said so", async () => {
    const plan = await planFetch(userId, parseFetchBody({ platform: "FLIPKART", email_ids: sample }));
    assert.equal(plan.afterDateSource, "lookback");
    assert.match(plan.afterDate, /^\d{4}-\d{2}-\d{2}$/);
    assert.ok(plan.sinceDate.getTime() < Date.now() - 24 * 3600e3);
    assert.equal(plan.params.fetch_orders_after_date_source, "lookback");
  });

  console.log("\nFETCH — the update that follows it (run_update_after_fetch)");
  await check("a stopped fetch is not followed by an update", async () => {
    const out = await queueUpdateAfterFetch(String(new mongoose.Types.ObjectId()), { runId: "", emails: ["a@x.in"], selection: "email_ids", cancelled: true, by: "verify", trigger: "user" });
    assert.deepEqual(out, { status: "skipped", reason: "the fetch was stopped" });
  });
  await check("nothing in progress on the fetched accounts: skipped through the real createUpdateRequest, nothing written or queued", async () => {
    // An invented workspace has no units, so the existing path answers "No active order units" before it creates anything.
    const out = await queueUpdateAfterFetch(String(new mongoose.Types.ObjectId()), { runId: "", emails: ["a@x.in"], selection: "email_ids", cancelled: false, by: "verify", trigger: "user" });
    assert.equal(out.status, "skipped");
    assert.match(String(out.reason), /^No active order units to update/);
    assert.equal(await OrderUpdateRequest.countDocuments({}).maxTimeMS(20_000), before.updateRequests);
  });

  console.log("\nUPDATE — every reference body → the existing selection");
  const emails = ["a@x.in", "b@x.in"];
  const REF_DEFAULT_STATUSES = ["Out for delivery", "Shipped", "Order Pending", "Returned", "Approved", "Purchased", "Ready to ship", "Dispatched", "Cancellation requested", "Payment not received", "Return started"];
  const cases: Array<{ name: string; body: Record<string, unknown>; selection: string; input: Record<string, unknown>; params: Record<string, unknown> }> = [
    // The reference's own record of a default update (full-order_sync_batches_.json): the eleven in-progress statuses, force false.
    { name: "{platform}", body: { platform: "FLIPKART" }, selection: "default", input: { emails: [], orderIds: [], orderStatus: [], csvRows: [], withReturned: true }, params: { force: false, statuses_to_update: REF_DEFAULT_STATUSES, email_ids_count: 0, order_ids_count: 0, order_email_pairs_count: 0 } },
    { name: "{platform, order_status}", body: { platform: "FLIPKART", order_status: ["Shipped", "Out for delivery"] }, selection: "order_status", input: { emails: [], orderIds: [], orderStatus: ["Shipped", "Out for delivery", "ofd"], csvRows: [], withReturned: false }, params: { force: false, statuses_to_update: ["Shipped", "Out for delivery"], email_ids_count: 0 } },
    { name: "{platform, email_ids}", body: { platform: "FLIPKART", email_ids: emails }, selection: "email_ids", input: { emails, orderIds: [], orderStatus: [], csvRows: [], withReturned: true }, params: { force: false, statuses_to_update: REF_DEFAULT_STATUSES, email_ids_count: 2 } },
    { name: "{platform, order_status, email_ids}", body: { platform: "FLIPKART", order_status: ["Approved"], email_ids: emails }, selection: "order_status_and_email_ids", input: { emails, orderIds: [], orderStatus: ["Approved", "Order Confirmed"], csvRows: [], withReturned: false }, params: { statuses_to_update: ["Approved"], email_ids_count: 2 } },
    { name: "{platform, order_ids}", body: { platform: "FLIPKART", order_ids: ["od123456", "OD789012", "OD123456"] }, selection: "order_ids", input: { emails: [], orderIds: ["OD123456", "OD789012"], orderStatus: [], csvRows: [], withReturned: false }, params: { force: false, statuses_to_update: [], order_ids_count: 2 } },
    {
      name: "{platform, order_email_pairs}",
      body: { platform: "FLIPKART", order_email_pairs: [{ email: "A@x.in", order_id: "OD1" }, { email: "b@x.in", order_id: "OD2" }, { email: "a@x.in", order_id: "od1" }] },
      selection: "order_email_pairs",
      input: { emails: [], orderIds: [], orderStatus: [], csvRows: [{ email: "a@x.in", order_id: "OD1" }, { email: "b@x.in", order_id: "OD2" }] },
      params: { order_email_pairs_count: 2 },
    },
  ];
  for (const c of cases) {
    await check(`${c.name} → ${c.selection}`, async () => {
      const plan = await planUpdate(userId, parseUpdateBody(c.body), stored);
      assert.equal(plan.selection, c.selection);
      assert.equal(plan.input.selectionMethod, c.selection);
      assert.equal(plan.params.selection_method, c.selection);
      assert.equal(plan.params.platform, "FLIPKART");
      for (const [k, v] of Object.entries(c.input)) assert.deepEqual((plan.input as Record<string, unknown>)[k], v, `input.${k}`);
      for (const [k, v] of Object.entries(c.params)) assert.deepEqual(plan.params[k], v, `params.${k}`);
    });
  }
  await check("the body this route always took (emails, orderIds, csv_rows, source, by)", async () => {
    const a = parseUpdateBody({ platform: "FLIPKART", emails: "A@x.in b@x.in", selection_method: "email", by: "user1" });
    assert.equal(a.selection, "email_ids");
    assert.deepEqual(a.emails, emails);
    assert.equal(a.by, "user1");
    assert.equal(a.source, "manual");
    const b = parseUpdateBody({ platform: "FLIPKART", orderIds: "OD1, OD2", selection_method: "order_ids", source: "cancel" });
    assert.equal(b.selection, "order_ids");
    assert.equal(b.source, "cancel");
    const c = parseUpdateBody({ csv_rows: [{ email: "a@x.in", orderId: "OD9" }] });
    assert.equal(c.selection, "order_email_pairs");
    assert.deepEqual(c.pairs, [{ email: "a@x.in", order_id: "OD9" }]);
  });
  await check("pairs win over order ids, order ids over statuses and emails — as the update itself applies them", async () => {
    const both = await planUpdate(userId, parseUpdateBody({ platform: "FLIPKART", order_ids: ["OD1"], order_status: ["Shipped"], email_ids: emails }), stored);
    assert.equal(both.selection, "order_ids");
    assert.deepEqual(both.input.emails, []);
    assert.deepEqual(both.input.orderStatus, []);
    assert.deepEqual(both.params.statuses_to_update, []);
    const csv = await planUpdate(userId, parseUpdateBody({ platform: "FLIPKART", order_ids: ["OD1"], order_email_pairs: [{ email: "a@x.in", order_id: "OD2" }] }), stored);
    assert.equal(csv.selection, "order_email_pairs");
    assert.deepEqual(csv.input.orderIds, []);
  });
  await check("the 2000 cap on order ids, emails and pairs — the reference sentence", () => {
    refuses(() => parseUpdateBody({ platform: "FLIPKART", order_ids: many(2001, (i) => `OD${i}`) }), 'Too many values in "Order IDs": 2001 entered, but the maximum is 2000. Please reduce it.');
    refuses(() => parseUpdateBody({ platform: "FLIPKART", email_ids: many(2500, (i) => `u${i}@x.in`) }), 'Too many values in "Email Addresses": 2500 entered, but the maximum is 2000. Please reduce it.');
    refuses(() => parseUpdateBody({ platform: "FLIPKART", order_email_pairs: many(2001, (i) => `OD${i}`).map((order_id) => ({ email: "a@x.in", order_id })) }), 'Too many values in "CSV email + order ID pairs": 2001 entered, but the maximum is 2000. Please reduce it.');
    assert.equal(parseUpdateBody({ platform: "FLIPKART", order_ids: many(2000, (i) => `OD${i}`) }).orderIds.length, 2000);
    // Repeats do not count, as on the filter bar.
    assert.equal(parseUpdateBody({ platform: "FLIPKART", order_ids: [...many(2000, (i) => `OD${i}`), "od5", "OD7"] }).orderIds.length, 2000);
  });

  console.log("\nSTATUS LABELS → stored keys");
  await check('"Out for delivery" ↔ ofd, "Approved" ↔ Order Confirmed — spellings of one status; "Cancelled" is the Cancelled key alone, "Failed" its own status', () => {
    assert.deepEqual(statusSelection(["Out for delivery"], stored).keys, ["Out for delivery", "ofd"]);
    assert.deepEqual(statusSelection(["Approved"], stored).keys, ["Approved", "Order Confirmed"]);
    // An update calls Flipkart once per unit: "Cancelled" must not pull in the failed-payment units (the list filter's alias does, the update never did).
    assert.deepEqual(statusSelection(["Cancelled"], stored).keys, ["Cancelled"]);
    assert.deepEqual(statusSelection(["Failed"], stored).keys, ["Failed"]);
    assert.deepEqual(statusSelection(["Cancelled", "Failed"], stored).keys, ["Cancelled", "Failed"]);
  });
  await check("no status named = the reference's eleven in-progress statuses; the selection behind it also takes Returned, the schedules' own rule does not", async () => {
    assert.deepEqual(DEFAULT_UPDATE_STATUSES, REF_DEFAULT_STATUSES);
    const filter = inProgressUpdateFilter();
    assert.ok(filter.$or.some((c) => "status_key" in c && c.status_key instanceof RegExp && c.status_key.test("Returned") && !c.status_key.test("Refunded")), "Returned is in, Refunded is not");
    // Read-only: today's numbers behind the two rules.
    const [withReturned, without] = await Promise.all([Order.countDocuments({ userId, ...filter }).maxTimeMS(30_000), Order.countDocuments({ userId, status_key: /^returned$/i }).maxTimeMS(30_000)]);
    console.log(`        ${withReturned} unit(s) in progress for a default update, ${without} of them stored as Returned`);
  });
  await check("a label is matched to the stored spelling whatever its case; an unknown one stays as typed", () => {
    assert.deepEqual(statusSelection(["OUT FOR DELIVERY", "delivered", "SHIPPED"], stored).keys, ["Out for delivery", "ofd", "Delivered", "Shipped"]);
    assert.deepEqual(statusSelection(["Return started"], stored).keys, ["Return started"]);
  });
  await check("detailed cancellations (slug or label) are conditions on a cancelled unit; beside plain Cancelled they add nothing", () => {
    const slug = statusSelection(["cancelled_approved", "cancelled_by_user_shipped"], stored);
    assert.deepEqual(slug.keys, []);
    assert.equal(slug.conditions.length, 2);
    assert.equal(slug.conditions[0].status_key, "Cancelled");
    assert.deepEqual(slug.conditions[1].cancelled_by_user, true);
    const label = statusSelection(["Cancelled - Order shipped"], stored);
    assert.equal(label.conditions.length, 1);
    assert.ok((label.conditions[0].order_status_pre_cancellation as RegExp).test("Shipped"));
    assert.deepEqual(statusSelection(["Cancelled", "cancelled_shipped"], stored), { keys: ["Cancelled"], conditions: [] });
    assert.deepEqual(statusSelection(["cancelled_something_new"], stored).keys, ["Cancelled"]);
  });
  await check("the conditions are valid queries on order_details (counted, read-only)", async () => {
    const all = statusSelection(["cancelled_approved", "cancelled_shipped", "cancelled_received", "cancelled_by_user_approved", "cancelled_by_user_shipped"], stored);
    const counts: number[] = [];
    for (const where of all.conditions) counts.push(await Order.countDocuments({ userId, ...where }).maxTimeMS(30_000));
    const cancelled = await Order.countDocuments({ userId, status_key: "Cancelled" }).maxTimeMS(30_000);
    console.log(`        cancelled_approved ${counts[0]} · cancelled_shipped ${counts[1]} · cancelled_received ${counts[2]} · by_user_approved ${counts[3]} · by_user_shipped ${counts[4]} · of ${cancelled} Cancelled`);
    assert.ok(counts.reduce((a, b) => a + b, 0) <= cancelled);
  });
  await check('an update of "Out for delivery" resolves to units stored under either spelling (resolveTargets, read-only)', async () => {
    const plan = await planUpdate(userId, parseUpdateBody({ platform: "FLIPKART", order_status: ["Out for delivery"] }));
    const inDb = await Order.countDocuments({ userId, status_key: { $in: plan.input.orderStatus } }).maxTimeMS(30_000);
    const targets = await resolveTargets(userId, plan.input);
    const units = await Order.find({ userId, unit_id: { $in: targets.map((t) => t.card.unitId) } }).select("status_key").maxTimeMS(30_000).lean();
    console.log(`        ${inDb} unit(s) stored as ${JSON.stringify(plan.input.orderStatus)} → ${targets.length} on logged-in accounts`);
    assert.ok(targets.length > 0 && targets.length <= inDb);
    assert.ok(units.every((u) => (plan.input.orderStatus || []).includes(u.status_key)));
  });

  await check("accounts are looked up once per e-mail, not once per unit: the default selection resolves in seconds and matches an independent count (read-only)", async () => {
    const plan = await planUpdate(userId, parseUpdateBody({ platform: "FLIPKART" }), stored);
    const t0 = Date.now();
    const targets = await resolveTargets(userId, plan.input);
    const ms = Date.now() - t0;
    // Independently: in-progress units whose account is logged in with a saved session.
    const units = await Order.find({ userId, ...inProgressUpdateFilter() }).select("platform_email unit_id").maxTimeMS(60_000).lean();
    const emailsOfUnits = [...new Set(units.map((u) => String(u.platform_email || "").toLowerCase()).filter(Boolean))];
    const usable = new Set<string>();
    for (let i = 0; i < emailsOfUnits.length; i += 500) {
      const rows = (await PlatformId.distinct("email", { userId, email: { $in: emailsOfUnits.slice(i, i + 500) }, status: "logged_in", sessionSavedAt: { $ne: null }, $or: [{ "sessionState.cookies.0": { $exists: true } }, { "sessionCookies.0": { $exists: true } }] })) as string[];
      for (const e of rows) usable.add(e.toLowerCase());
    }
    const expected = units.filter((u) => usable.has(String(u.platform_email || "").toLowerCase())).length;
    const accounts = new Set(targets.map((t) => t.account.email.toLowerCase()));
    console.log(`        ${targets.length} unit(s) on ${accounts.size} account(s) resolved in ${ms} ms (was ≈66 ms per unit); independent count ${expected}`);
    assert.equal(targets.length, expected);
    assert.ok(targets.every((t) => t.account.cookies.length > 0 && usable.has(t.account.email.toLowerCase())));
    assert.ok(ms < 30_000, `resolving took ${ms} ms`);
  });
  await check("order ids and CSV pairs resolve through the same per-e-mail lookup (read-only, on real units)", async () => {
    const some = await Order.find({ userId, ...inProgressUpdateFilter() }).select("platform_email order_id unit_id").limit(40).maxTimeMS(30_000).lean();
    if (!some.length) return;
    const byIds = await resolveTargets(userId, { orderIds: [...new Set(some.map((u) => u.order_id))], selectionMethod: "order_ids" });
    const byPairs = await resolveTargets(userId, { csvRows: some.map((u) => ({ email: u.platform_email, order_id: u.order_id })), selectionMethod: "order_email_pairs" });
    const ids = (list: typeof byIds) => new Set(list.map((t) => t.card.unitId));
    // Every pair names a real unit's own account, so both routes reach the same units (those on logged-in accounts).
    const fromIds = ids(byIds);
    assert.ok([...ids(byPairs)].every((unitId) => fromIds.has(unitId)), "a unit reached by its pair is reached by its order id too");
    assert.ok(byPairs.length > 0 && byPairs.length <= byIds.length);
    // A pair that names another account than the order's own selects nothing saved: it becomes the bare order on that account.
    const other = await resolveTargets(userId, { csvRows: [{ email: byPairs[0].account.email, order_id: "ODVERIFY000000000000" }], selectionMethod: "order_email_pairs" });
    assert.deepEqual(other.map((t) => [t.account.email, t.card.orderId, t.card.itemId]), [[byPairs[0].account.email, "ODVERIFY000000000000", ""]]);
    assert.deepEqual(await resolveTargets(userId, { csvRows: [{ email: "nobody-here@verify.invalid", order_id: some[0].order_id }], selectionMethod: "order_email_pairs" }), []);
  });

  console.log("\nFETCH — one slot per workspace, and a stop while the account list is prepared");
  await check("the slot is taken before the trigger plans: a second trigger, or a start by anyone else, is refused until it is given back", async () => {
    const ws = String(new mongoose.Types.ObjectId());
    assert.equal(isOrderFetchPreparing(ws), false);
    const release = reserveOrderFetch(ws);
    assert.equal(isOrderFetchPreparing(ws), true, "/fetch/status reads preparing while the trigger plans");
    assert.throws(() => reserveOrderFetch(ws), /^Error: Order fetch is already running$/);
    await assert.rejects(() => startOrderFetch(ws, ["a@verify.invalid"], new Date("2026-11-23")), /^Error: Order fetch is already running$/);
    release();
    release();
    assert.equal(isOrderFetchPreparing(ws), false);
    reserveOrderFetch(ws)();
  });
  await check("Force kill while the account list is prepared: the start gives up before it reads an account, and the fetcher is free at once", async () => {
    const ws = String(new mongoose.Types.ObjectId());
    const release = reserveOrderFetch(ws);
    await stopOrderFetch(ws);
    const runsBefore = await OrderJobRun.countDocuments({}).maxTimeMS(20_000);
    await assert.rejects(
      () => startOrderFetch(ws, many(500, (i) => `u${i}@verify.invalid`), new Date("2026-11-23"), { reserved: true }),
      (err) => err instanceof OrderFetchStoppedError && /^Stopped while the account list was being prepared/.test(err.message)
    );
    release();
    assert.equal(isOrderFetchPreparing(ws), false);
    assert.equal(await OrderJobRun.countDocuments({}).maxTimeMS(20_000), runsBefore, "no run was written");
    // The stop was for that start only: the next one is not stopped by it (an invented workspace has no account, so it ends on that).
    await assert.rejects(() => startOrderFetch(ws, ["a@verify.invalid"], new Date("2026-11-23")), /^Error: None of the 1 email\(s\) are logged in/);
  });

  console.log("\nTASK STATUSES");
  await check("'Timed out' is `timedout` as a task status and `timed_out` as a counts key — the reference's two spellings", () => {
    assert.deepEqual(REQUEST_TASK_STATUSES, ["queued", "in_progress", "success", "failed", "cancelled", "aborted", "timedout"]);
    assert.equal(countKeyOf("timedout"), "timed_out");
    assert.equal(countKeyOf("in_progress"), "in_progress");
    assert.deepEqual(countsOf(["timedout", "timedout", "failed", "success"]), { total: 4, queued: 0, in_progress: 0, success: 1, failed: 1, cancelled: 0, aborted: 0, timed_out: 2 });
  });
  await check("an open request with no worker behind it reads stalled, with the sentence; a finished one never does", () => {
    assert.deepEqual(livenessOf("in_progress", true, 5, "order"), { worker_alive: true, stalled: false, stalled_message: "" });
    assert.deepEqual(livenessOf("in_progress", false, 5, "order"), { worker_alive: false, stalled: true, stalled_message: "The worker stopped with 5 order(s) left — restart the worker to resume" });
    assert.deepEqual(livenessOf("in_progress", false, 9, "account"), { worker_alive: false, stalled: true, stalled_message: "The worker stopped with 9 account(s) left — restart the worker to resume" });
    assert.deepEqual(livenessOf("queued", false, 5, "order"), { worker_alive: false, stalled: true, stalled_message: "No worker is listening on this queue — restart the worker" });
    assert.deepEqual(livenessOf("completed", false, 0, "order"), { worker_alive: null, stalled: false, stalled_message: "" });
    assert.deepEqual(livenessOf("cancelled", false, 0, "order"), { worker_alive: null, stalled: false, stalled_message: "" });
  });

  console.log("\nINVOICES — input");
  await check("order ids: split on spaces / commas, upper-cased, de-duplicated", () => {
    assert.deepEqual(parseInvoiceOrderIds("od123456 406-1664516-7286725,OD123456\nOD9"), ["OD123456", "406-1664516-7286725", "OD9"]);
    assert.deepEqual(parseInvoiceOrderIds(["od1", " OD2 ", "od1"]), ["OD1", "OD2"]);
  });
  await check("2001 distinct order ids → the reference sentence, nothing created; none → refused", async () => {
    await refusesAsync(() => createInvoiceRequest(userId, { orderIds: many(2001, (i) => `ODVERIFY${i}`) }), 'Too many values in "Order IDs": 2001 entered, but the maximum is 2000. Please reduce it.');
    await refusesAsync(() => createInvoiceRequest(userId, { orderIds: " , " }), "Order IDs are required");
    assert.equal(await OrderInvoiceRequest.countDocuments({}).maxTimeMS(20_000), before.invoiceRequests);
  });

  console.log("\nERROR CLASSES");
  await check("Flipkart answers → the reference's classes", () => {
    const cls = (err: unknown) => classifyPlatformError(err).errorClass;
    assert.equal(cls(new OrderApiError("invoice download for OD1 answered HTTP 404", 404)), "nothing_available");
    assert.equal(cls(new OrderApiError("answered HTTP 410", 410)), "nothing_available");
    assert.equal(cls(new OrderApiError("answered HTTP 401", 401)), "not_logged_in");
    assert.equal(cls(new OrderApiSessionError("did not answer JSON — the session is probably not logged in")), "not_logged_in");
    assert.equal(cls(new OrderApiError("answered HTTP 429", 429)), "rate_limited");
    assert.equal(cls(new OrderApiError("answered HTTP 500", 500)), "platform_unavailable");
    assert.equal(cls(new OrderApiError("answered HTTP 503", 503)), "platform_unavailable");
    assert.equal(cls(new OrderApiError("answered HTTP 403", 403)), "platform_declined");
    assert.equal(cls(new OrderApiError("answered HTTP 400", 400)), "platform_declined");
    assert.equal(cls(new Error("apiRequestContext.get: Timeout 60000ms exceeded.")), "platform_unavailable");
    assert.equal(cls(new Error("read ECONNRESET")), "platform_unavailable");
    assert.equal(cls(new Error("invoice download for OD1 is neither PDF nor zip (text/html, 812 bytes)")), "unexpected_response");
    assert.equal(cls(new Error("invoice zip for OD1 had no PDF")), "unexpected_response");
    assert.equal(cls(new Error("AccessDenied: s3:PutObject")), "internal_error");
    assert.equal(cls(undefined), "unknown");
    const c = classifyPlatformError(new OrderApiError("invoice download for OD1 answered HTTP 404", 404));
    assert.deepEqual(c, { errorClass: "nothing_available", error: "Nothing available yet", detail: "invoice download for OD1 answered HTTP 404" });
    assert.equal(Object.keys(ERROR_CLASSES).length, 16);
    assert.equal(ERROR_CLASSES.order_not_found, "Order not found on this account");
    assert.equal(ERROR_CLASSES.not_logged_in, "Account is not logged in");
    assert.equal(ERROR_CLASSES.invalid_coupon, "Coupon link is invalid or expired");
  });

  console.log("\nLEDGER — a request's row");
  await check("queued → running → completed / stopped; parked reads failed; counts carried", () => {
    const t0 = new Date("2026-11-24T10:00:00Z");
    const t1 = new Date("2026-11-24T10:01:40Z");
    assert.deepEqual(ledgerPatchOf({ status: "queued", counts: emptyCounts(10), startedAt: null, finishedAt: null }), { status: "queued", total: 10, processed: 0, success: 0, failed: 0, cancelled: 0, startedAt: null, finishedAt: null, avgMsPerItem: 0, message: "10 task(s) waiting for the worker" });
    const running = ledgerPatchOf({ status: "in_progress", counts: { total: 10, queued: 4, in_progress: 2, success: 3, failed: 1, cancelled: 0, aborted: 0, timed_out: 0 }, startedAt: t0, finishedAt: null });
    assert.equal(running.status, "running");
    assert.equal(running.finishedAt, null);
    assert.equal(running.processed, 4);
    assert.equal(running.message, "3 done, 1 failed, 6 left");
    const done = ledgerPatchOf({ status: "completed", counts: { total: 10, queued: 0, in_progress: 0, success: 8, failed: 1, cancelled: 0, aborted: 0, timed_out: 1 }, startedAt: t0, finishedAt: t1 });
    assert.deepEqual({ status: done.status, processed: done.processed, success: done.success, failed: done.failed, finishedAt: done.finishedAt, avgMsPerItem: done.avgMsPerItem }, { status: "completed", processed: 10, success: 8, failed: 2, finishedAt: t1, avgMsPerItem: 10_000 });
    const stopped = ledgerPatchOf({ status: "cancelled", counts: { total: 10, queued: 0, in_progress: 0, success: 3, failed: 0, cancelled: 6, aborted: 1, timed_out: 0 }, startedAt: t0, finishedAt: t1, message: "Stopped by user1" });
    assert.deepEqual({ status: stopped.status, cancelled: stopped.cancelled, message: stopped.message }, { status: "stopped", cancelled: 7, message: "Stopped by user1" });
    const parked = ledgerPatchOf({ status: "in_progress", counts: { total: 10, queued: 6, in_progress: 1, success: 3, failed: 0, cancelled: 0, aborted: 0, timed_out: 0 }, startedAt: t0, finishedAt: null, parked: "The worker stopped" });
    assert.equal(parked.status, "failed");
    assert.ok(parked.finishedAt instanceof Date, "a parked row is not left open");
    assert.deepEqual(requestProgress({ total: 10, queued: 0, in_progress: 0, success: 8, failed: 2, cancelled: 0, aborted: 0, timed_out: 0 }, t0, t1), { finished_count: 10, progress_percent: 100, duration_seconds: 100, avg_seconds_per_task: 10, eta_seconds: null });
  });

  console.log("\nCOUPONS — links and pages");
  await check("coupon_links: one string split on spaces or commas; https + a flipkart.com host only", () => {
    assert.deepEqual(parseCouponLinks("https://www.flipkart.com/a?x=1 https://dl.flipkart.com/b,https://www.flipkart.com/a?x=1"), ["https://www.flipkart.com/a?x=1", "https://dl.flipkart.com/b"]);
    assertCouponLinks(["https://www.flipkart.com/a", "https://dl.flipkart.com/s/abc", "https://flipkart.com/x"]);
    for (const bad of ["http://www.flipkart.com/a", "https://evilflipkart.com/a", "https://flipkart.com.evil.in/a", "https://fkrt.it/abc", "www.flipkart.com/a", "javascript:alert(1)"]) {
      refuses(() => assertCouponLinks([bad]), /^Not a Flipkart coupon link: /);
    }
    refuses(() => assertCouponLinks([]), "Coupon links are required");
    refuses(() => assertCouponLinks(many(51, (i) => `https://www.flipkart.com/c/${i}`)), 'Too many values in "Coupon links": 51 entered, but the maximum is 50. Please reduce it.');
  });
  await check("a bad link or too many emails is refused before anything is created", async () => {
    await refusesAsync(() => createCouponRequest(userId, { links: "https://www.flipkart.com/ok http://www.flipkart.com/plain" }), /^Not a Flipkart coupon link: http:\/\/www\.flipkart\.com\/plain/);
    await refusesAsync(() => createCouponRequest(userId, { links: "https://www.flipkart.com/ok", emails: many(2001, (i) => `u${i}@x.in`) }), 'Too many values in "Email addresses": 2001 entered, but the maximum is 2000. Please reduce it.');
    assert.equal(await OrderCouponRequest.countDocuments({}).maxTimeMS(20_000), before.couponRequests);
  });
  await check("a link is the login page by its PATH only: /login in the query is opened like any link; a redirect to /account/login is a bounce and is not followed", async () => {
    const asked: string[] = [];
    const ctx = (answers: Record<string, { status: number; location?: string; body?: string }>) =>
      ({
        get: async (url: string) => {
          asked.push(url);
          const a = answers[url] || { status: 200, body: "<title>Offer</title><p>Coupon applied</p>" };
          return { status: () => a.status, headers: () => (a.location ? { location: a.location } : {}), text: async () => a.body || "" };
        },
      }) as unknown as APIRequestContext;
    const inQuery = "https://www.flipkart.com/x?ret=/account/login";
    assertCouponLinks([inQuery]);
    const opened = await openCouponLink(ctx({}), inQuery);
    assert.deepEqual([opened.outcome, opened.loginBounce, opened.httpStatus, asked], ["opened", false, 200, [inQuery]]);
    asked.length = 0;
    const hop = await openCouponLink(ctx({ "https://www.flipkart.com/c/1": { status: 302, location: "/y?next=/login" } }), "https://www.flipkart.com/c/1");
    assert.deepEqual([hop.outcome, hop.loginBounce, asked], ["opened", false, ["https://www.flipkart.com/c/1", "https://www.flipkart.com/y?next=/login"]]);
    asked.length = 0;
    const bounce = await openCouponLink(ctx({ "https://www.flipkart.com/c/2": { status: 302, location: "/account/login?ret=/c/2" } }), "https://www.flipkart.com/c/2");
    assert.deepEqual([bounce.outcome, bounce.errorClass, bounce.loginBounce, asked], ["failed", "not_logged_in", true, ["https://www.flipkart.com/c/2"]]);
  });
  await check("a page's title and first visible words (scripts and styles left out)", () => {
    const page = pageTitleAndExcerpt(`<!doctype html><html><head><title> Coupon &amp; Offers  | Flipkart </title><style>.a{color:red}</style></head><body><script>var invalid = "expired";</script><h1>Your coupon is ready</h1><p>Use it at checkout.</p></body></html>`);
    assert.equal(page.title, "Coupon & Offers | Flipkart");
    assert.equal(page.excerpt, "Your coupon is ready Use it at checkout.");
    assert.equal(pageTitleAndExcerpt('{"error":"nope"}').excerpt, '{"error":"nope"}');
  });
  await check("an account's outcome from its link results — and it claims no more than a link opening", () => {
    const r = (outcome: "opened" | "failed" | "skipped", errorClass = "", httpStatus = 200) => ({ link: "l", outcome, errorClass, message: outcome === "opened" ? LINK_OPENED(httpStatus) : "", finalUrl: "", httpStatus, loginBounce: false, hops: [], title: "", excerpt: "", ms: 1 });
    assert.deepEqual(couponTaskOutcome([r("opened")]), { status: "success", errorClass: "", error: "", note: "Link opened with the account's session (HTTP 200)" });
    assert.equal(couponTaskOutcome([r("opened"), r("opened")]).note, "2 links opened with the account's session (HTTP 200)");
    assert.deepEqual(couponTaskOutcome([r("failed", "invalid_coupon", 404)]), { status: "failed", errorClass: "invalid_coupon", error: "Coupon link is invalid or expired", note: "" });
    assert.deepEqual(couponTaskOutcome([r("opened"), r("failed", "invalid_coupon", 404)]), { status: "failed", errorClass: "partially_failed", error: "Some items in this task failed", note: "1 of 2 link(s) opened" });
    assert.equal(couponTaskOutcome([r("opened"), r("failed", "not_logged_in", 302), r("skipped")]).errorClass, "not_logged_in");
    assert.equal(couponTaskOutcome([r("failed", "platform_unavailable", 500), r("failed", "invalid_coupon", 404)]).errorClass, "partially_failed");
  });

  // The routes themselves, on the API that is already running — refusals only: every call below
  // is answered before anything is created, queued or sent to Flipkart.
  const base = `http://localhost:${config.port}/api/orders`;
  const call = async (method: "GET" | "POST", path: string, body?: unknown) => {
    const res = await fetch(`${base}${path}`, { method, headers: { "x-api-key": config.apiKey, "content-type": "application/json" }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(30_000) });
    return { status: res.status, body: (await res.json().catch(() => ({}))) as Record<string, unknown> };
  };
  const up = await fetch(`http://localhost:${config.port}/api/health`, { signal: AbortSignal.timeout(3000) }).then((r) => r.ok, () => false);
  if (!up) {
    console.log(`\nLIVE API — skipped: nothing answers on port ${config.port}`);
  } else {
    console.log(`\nLIVE API (port ${config.port}) — the routes refuse what they should, and start nothing`);
    const fetchIdle = !((await call("GET", "/fetch/status")).body.running as boolean);
    await check("POST /fetch/trigger: another platform, 2001 emails, emails that are not logged in", async () => {
      assert.deepEqual(await call("POST", "/fetch/trigger", { platform: "SHOPSY", email_ids: ["a@x.in"] }), { status: 400, body: { error: "Only Flipkart is connected", code: "platform_not_connected" } });
      assert.deepEqual(await call("POST", "/fetch/trigger", { platform: "FLIPKART", email_ids: many(2001, (i) => `u${i}@x.in`) }), { status: 400, body: { error: 'Too many values in "Email Addresses": 2001 entered, but the maximum is 2000. Please reduce it.', code: "too_many_values" } });
      if (fetchIdle) {
        assert.deepEqual(await call("POST", "/fetch/trigger", { platform: "FLIPKART", email_ids: ["nobody-here@verify.invalid"], enable_login: false, fetch_orders_after_date: "2026-11-23", force_fetch: true, run_update_after_fetch: true }), { status: 400, body: { error: "None of the 1 email(s) are logged in. Login them in platform id first." } });
        assert.equal((await call("GET", "/fetch/status")).body.running, false);
      }
    });
    await check("POST /update/trigger: another platform, 2001 order ids, a selection that matches no unit", async () => {
      assert.deepEqual(await call("POST", "/update/trigger", { platform: "AMAZON" }), { status: 400, body: { error: "Only Flipkart is connected", code: "platform_not_connected" } });
      assert.deepEqual(await call("POST", "/update/trigger", { platform: "FLIPKART", order_ids: many(2001, (i) => `OD${i}`) }), { status: 400, body: { error: 'Too many values in "Order IDs": 2001 entered, but the maximum is 2000. Please reduce it.', code: "too_many_values" } });
      const none = { status: 400, body: { error: "No active order units to update. Fetch first, or paste a logged-in email + order id." } };
      assert.deepEqual(await call("POST", "/update/trigger", { platform: "FLIPKART", order_ids: ["ODVERIFY-NOT-AN-ORDER"] }), none);
      assert.deepEqual(await call("POST", "/update/trigger", { platform: "FLIPKART", order_email_pairs: [{ email: "nobody-here@verify.invalid", order_id: "ODVERIFY-NOT-AN-ORDER" }] }), none);
      assert.deepEqual(await call("POST", "/update/trigger", { platform: "FLIPKART", order_status: ["Return started"], email_ids: ["nobody-here@verify.invalid"] }), none);
    });
    await check("POST /invoice/trigger: another platform, no ids, 2001 ids; the reads answer; the inline download keeps its own limit", async () => {
      assert.deepEqual(await call("POST", "/invoice/trigger", { platform: "AMAZON", order_ids: ["406-1664516-7286725"] }), { status: 400, body: { error: "Only Flipkart is connected", code: "platform_not_connected" } });
      assert.deepEqual(await call("POST", "/invoice/trigger", { platform: "FLIPKART", order_ids: [] }), { status: 400, body: { error: "Order IDs are required", code: "invalid_request" } });
      assert.deepEqual(await call("POST", "/invoice/trigger", { platform: "FLIPKART", order_ids: many(2001, (i) => `ODVERIFY${i}`) }), { status: 400, body: { error: 'Too many values in "Order IDs": 2001 entered, but the maximum is 2000. Please reduce it.', code: "too_many_values" } });
      const list = await call("GET", "/invoice/requests");
      assert.equal(list.status, 200);
      assert.ok(Array.isArray(list.body.requests));
      const nobody = String(new mongoose.Types.ObjectId());
      assert.deepEqual(await call("GET", `/invoice/requests/${nobody}`), { status: 404, body: { error: "Invoice request not found" } });
      assert.deepEqual(await call("POST", `/invoice/requests/${nobody}/stop`, { by: "verify" }), { status: 404, body: { error: "Invoice request not found" } });
      assert.deepEqual(await call("POST", "/invoices/download", { orderIds: [] }), { status: 400, body: { error: "No order IDs given" } });
      assert.deepEqual(await call("POST", "/invoices/download", { orderIds: many(101, (i) => `ODVERIFY${i}`) }), { status: 400, body: { error: "Too many order IDs (101); download at most 100 at once here" } });
    });
    await check("POST /company-coupon/trigger: no links, a link that is not https on flipkart.com; the reads answer", async () => {
      assert.deepEqual(await call("POST", "/company-coupon/trigger", {}), { status: 400, body: { error: "Coupon links are required", code: "invalid_request" } });
      const bad = await call("POST", "/company-coupon/trigger", { coupon_links: "https://www.flipkart.com/ok http://www.flipkart.com/plain" });
      assert.equal(bad.status, 400);
      assert.equal(bad.body.code, "invalid_coupon_link");
      assert.match(String(bad.body.error), /^Not a Flipkart coupon link: http:\/\/www\.flipkart\.com\/plain/);
      assert.equal((await call("POST", "/company-coupon/trigger", { coupon_links: "https://flipkart.com.evil.in/x" })).status, 400);
      const list = await call("GET", "/company-coupon/requests");
      assert.equal(list.status, 200);
      assert.ok(Array.isArray(list.body.requests));
      assert.deepEqual(await call("GET", `/company-coupon/requests/${String(new mongoose.Types.ObjectId())}`), { status: 404, body: { error: "Coupon request not found" } });
    });
  }

  await check("nothing was written by this run", async () => {
    assert.equal(await OrderInvoiceRequest.countDocuments({}).maxTimeMS(20_000), before.invoiceRequests);
    assert.equal(await OrderCouponRequest.countDocuments({}).maxTimeMS(20_000), before.couponRequests);
    assert.equal(await OrderUpdateRequest.countDocuments({}).maxTimeMS(20_000), before.updateRequests);
    assert.equal(await OrderJobRun.countDocuments({}).maxTimeMS(20_000), before.ledgerRuns);
  });

  await mongoose.disconnect();
  console.log(`\n${passed} passed, ${failures.length} failed`);
  if (failures.length) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
