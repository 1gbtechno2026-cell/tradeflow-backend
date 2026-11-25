/**
 * The dead-session fix in the update worker and the fetch (F2, F3) — WITHOUT
 * Flipkart, without a worker, without a queue, without Redis:
 *
 *   UPDATE   runOrderUpdateJob, one account at a time, a sealed Chromium in
 *            place of Chrome and no account lock:
 *     · a dead session (My Orders goes to the login page): exactly 2
 *       navigations to /account/orders and no order page opened, the account
 *       failed after unit 1 in < 15 s, its 5 task rows failed not_logged_in and
 *       equal to the request's counters, the row pending with demotedBy
 *       "trade-flow:update", one event
 *     · no saved cookies on a logged_in row: handed off confirmed, no Chrome;
 *       a logged_in row that never stored a session: reported, not moved
 *     · a live session: units read, nothing reported
 *     · an empty order page (a CSV that paired the ID with another one's
 *       orders): the unit fails "gave no data" after the 30 s wait, the row
 *       stays logged_in and the log says the orders may belong to another ID
 *     · a session that dies after unit 1: no hand-off, unit 2 fails at once,
 *       unit 3 opens no Chrome, one log line
 *     · My Orders times out once on a live session: the tab is kept, the next
 *       unit goes straight to its order page
 *     · API mode (what nearly every live update runs), with a fake order API:
 *       the API refuses the session and the page lands on the login page twice
 *       → handed off with "the order API refused this ID and …"; the API reads
 *       units first and the page dies on an HTTP 500's fallback → no hand-off,
 *       "— page skipped" for the next 500, the out-for-delivery pass skipped;
 *       the page dies in the out-for-delivery pass itself → no hand-off, the
 *       rest of the pass skipped
 *     · 25 accounts, 10 dead, SESSION_DEMOTE_CAP=100: the 7th dead opens the
 *       request's circuit — 6 moved, none after
 *   FETCH    startOrderFetch with the same Chromium and, in API mode, a fake
 *            order API:
 *     · page mode, a dead and a live account: one task failed "Session
 *       expired …" (not_logged_in), its row pending (trade-flow:fetch), one
 *       event; the other succeeded and is untouched
 *     · API mode, the list answers and every unit's API read fails: the page
 *       fallback lands on the login page and is confirmed once, the later
 *       units open no Chrome ("page fallback skipped …"), the account succeeds,
 *       the row stays logged_in, nothing reported
 *     · 13 live then 8 dead in one lane: the run's circuit opens at the 7th
 *       dead — 6 moved, 2 not
 *   scrapeAndSave   a login landing fails at once, not after the 30 s wait
 *
 * The browser is plain Chromium in which no host name resolves: every request
 * is answered here (My Orders, an order page, Flipkart's login page, and the
 * login redirect — made by the page's own script, since a fulfilled 302 is
 * followed outside the route) or aborted, so nothing can leave the machine.
 * The fake tells the invented accounts apart by their invented cookie
 * (T=verify-invalid-<kind>-<label>), as Flipkart does by the session.
 *
 * What is real: Mongo — platformids, order_update_requests, order_sync_tasks,
 * order_job_runs, order_details, order_index, order_unit_events, alerts and
 * session_demotions of an INVENTED workspace with @verify.invalid e-mails (each
 * carrying this run's id), all deleted at the end (the script prints what was
 * left). This process works for the invented
 * workspace whatever TRADE_FLOW_USER_ID says; no update is created through the
 * real function (it would enqueue on the live queue) — the requests are
 * written here and their jobs run in this process.
 *
 *   npm run test:dead-session-flows
 */
import assert from "node:assert/strict";
import type { Job } from "bullmq";
import mongoose, { Types } from "mongoose";
import { chromium, type APIRequestContext, type Browser, type Page, type Route } from "playwright";
import { config } from "../config.js";
import { Order } from "../models/Order.js";
import { OrderIndex } from "../models/OrderIndex.js";
import { OrderJobRun } from "../models/OrderJobRun.js";
import { OrderSyncTask } from "../models/OrderSyncTask.js";
import { OrderUnitEvent } from "../models/OrderUnitEvent.js";
import { OrderUpdateRequest, type IOrderUpdateRequest } from "../models/OrderUpdateRequest.js";
import { PlatformId } from "../models/PlatformId.js";
import type { UpdateJobData } from "../queue.js";
import { OrderApiError, OrderApiSessionError } from "../services/orderApi.js";
import type { ApiListWalk } from "../services/orderApiSync.js";
import { getOrderFetchJob, PAGE_FALLBACK_SKIPPED, scrapeAndSave, startOrderFetch, type FetchDeps } from "../services/orderFetch.js";
import { PAGE_SKIPPED, runOrderUpdateJob, type UpdateJobDeps } from "../services/orderUpdateQueue.js";
import { deliveredCheckDefaults } from "../services/requestChains.js";
import { classifyMessage, emptyCounts } from "../services/requestCommon.js";
import { SESSION_DEAD_MESSAGE, SESSION_NOTES } from "../services/sessionEligibility.js";
import { countsOfUpdate, logCounts, openRequestTasksQuietly, taskLog } from "../services/syncTaskLog.js";

const USER = new Types.ObjectId();
const RUN = Date.now().toString(36);
/** The fetch's since-date, and the invented sessions' save date (older than 24 h: F1 may move them). */
const SINCE = new Date("2026-11-01T00:00:00+05:30");
const SAVED = new Date("2026-08-01T00:00:00Z");
const MIN = 60_000;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const near = (d: unknown, ms = 10 * MIN) => d instanceof Date && Math.abs(Date.now() - d.getTime()) < ms;
/** The run in every e-mail: the dashboard folds an event into any open alert of the same e-mail and day, so two runs at once (another Mac, a re-run) must not share one. */
const mail = (label: string) => `${label}-${RUN}@verify.invalid`;

let passed = 0;
const failures: string[] = [];

async function check(name: string, fn: () => void | Promise<void>) {
  const started = Date.now();
  try {
    await fn();
    passed++;
    console.log(`  ok    ${name} (${((Date.now() - started) / 1000).toFixed(1)} s)`);
  } catch (err) {
    failures.push(name);
    console.log(`  FAIL  ${name}\n        ${err instanceof Error ? err.message.split("\n").slice(0, 10).join("\n        ") : err}`);
  }
}

// ── the fake Flipkart (a sealed Chromium) ───────────────────────────────────

/**
 * How the fake treats an account, by its invented cookie: dead (My Orders and
 * every order page go to the login page), live (they render), empty (My Orders
 * renders, an order page renders nothing — another account's order), dies
 * (live for the first order page, logged out from the second on), flaky (live,
 * but the first My Orders load fails — a network error, as a slow spell's
 * timeout would).
 */
type Kind = "dead" | "live" | "empty" | "dies" | "flaky";

const LOGIN_PAGE = "<html><head><title>Login</title></head><body>Login</body></html>";
const toLogin = (ret: string) => `<html><head><script>location.replace(${JSON.stringify(`https://www.flipkart.com/login?ret=${encodeURIComponent(ret)}`)})</script></head><body></body></html>`;
/** One card, delivered long before SINCE: the fetch lists it, indexes it as older and stops scrolling after a few rounds. */
const MY_ORDERS = `<html><head><title>My Orders</title></head><body><div class="card"><a href="/order_details?order_id=OD900000000000001&item_id=VI9001&unit_id=VU9001">Verify Item (8 GB RAM)</a><div>Delivered on 1 Jan 2024</div></div></body></html>`;
const ORDER_PAGE = `<html><head><title>Order details</title></head><body><h1>Verify Test Phone (8 GB RAM, 128 GB)</h1><div>Seller: Verify Retail</div><div>Price details</div></body></html>`;
const EMPTY_ORDER_PAGE = "<html><head><title>Flipkart</title></head><body></body></html>";

type Seen = { orders: number; details: number; login: number };
const seen = new Map<string, Seen>();
const seenOf = (label: string): Seen => {
  let s = seen.get(label);
  if (!s) seen.set(label, (s = { orders: 0, details: 0, login: 0 }));
  return s;
};
/** Requests that went round the route (the sinkhole stopped them). Must stay empty. */
const unanswered: string[] = [];
let aborted = 0;
let launches = 0;
const browsers = new Set<Browser>();

async function answer(route: Route) {
  const req = route.request();
  let url: URL;
  try {
    url = new URL(req.url());
  } catch {
    aborted++;
    return route.abort();
  }
  if (url.hostname !== "www.flipkart.com") {
    aborted++;
    return route.abort();
  }
  const m = ((await req.allHeaders()).cookie || "").match(/(?:^|;\s*)T=verify-invalid-([a-z]+)-([a-z0-9]+)/);
  const kind = (m?.[1] || "none") as Kind | "none";
  const st = seenOf(m?.[2] || "-");
  const nav = req.isNavigationRequest();
  const html = (body: string) => route.fulfill({ status: 200, contentType: "text/html; charset=utf-8", body });
  if (url.pathname.startsWith("/account/orders")) {
    if (nav) st.orders++;
    if (kind === "flaky" && nav && st.orders === 1) {
      aborted++;
      return route.abort("failed");
    }
    return html(kind === "dead" || (kind === "dies" && st.details >= 2) ? toLogin("/account/orders") : MY_ORDERS);
  }
  if (url.pathname.startsWith("/order_details")) {
    if (nav) st.details++;
    if (kind === "dead" || (kind === "dies" && st.details >= 2)) return html(toLogin(`${url.pathname}${url.search}`));
    return html(kind === "empty" ? EMPTY_ORDER_PAGE : ORDER_PAGE);
  }
  if (url.pathname === "/login") {
    if (nav) st.login++;
    return html(LOGIN_PAGE);
  }
  aborted++;
  return route.abort();
}

/** What the update and the fetch launch instead of Chrome. */
const fakeLaunch = async () => {
  launches++;
  const browser = await chromium.launch({ headless: true, args: ["--host-resolver-rules=MAP * ~NOTFOUND"] });
  browsers.add(browser);
  browser.on("disconnected", () => browsers.delete(browser));
  const context = await browser.newContext();
  context.on("requestfailed", (r) => {
    if (/ERR_NAME_NOT_RESOLVED/.test(r.failure()?.errorText || "")) unanswered.push(r.url());
  });
  await context.route("**/*", answer);
  return { browser, context };
};

const noLock: UpdateJobDeps["lock"] = async () => async () => undefined;
const updateDeps: Partial<UpdateJobDeps> = { launch: fakeLaunch, lock: noLock };

// ── invented rows ───────────────────────────────────────────────────────────

/** Invented cookies under Flipkart's auth names (restoreFlipkartSession's attach check passes); only the fake above ever sees them. */
const cookiesOf = (kind: Kind, label: string) =>
  [
    ["T", `verify-invalid-${kind}-${label}`],
    ["SN", "verify-invalid-sn"],
    ["at", "verify-invalid-at"],
  ].map(([name, value]) => ({ name, value, domain: ".flipkart.com", path: "/", httpOnly: true, secure: true, sameSite: "Lax" as const }));

async function insertRow(label: string, kind: Kind, opts: { savedAt?: Date | null; cookies?: boolean } = {}) {
  const _id = new Types.ObjectId();
  const cookies = opts.cookies === false ? [] : cookiesOf(kind, label);
  const now = new Date();
  await PlatformId.collection.insertOne({
    _id,
    userId: USER,
    onboardingId: new Types.ObjectId(),
    parentId: null,
    parentEmail: "",
    domain: "verify.invalid",
    platform: "flipkart",
    email: mail(label),
    status: "logged_in",
    lastError: "",
    retryable: true,
    sessionCookies: cookies,
    sessionState: { cookies, origins: [] },
    sessionSavedAt: opts.savedAt === undefined ? SAVED : opts.savedAt,
    lastChecked: null,
    sessionCheck: null,
    statusBeforeDemote: "",
    demotedAt: null,
    demotedBy: "",
    createdAt: now,
    updatedAt: now,
  });
  return _id;
}

type RowView = {
  status: string;
  sessionSavedAt: Date | null;
  statusBeforeDemote?: string;
  demotedAt?: Date | null;
  demotedBy?: string;
  lastChecked: Date | null;
  sessionCheck?: { at: Date; outcome: string; reason: string; via: string } | null;
};

/** The fields under test — never the cookies. */
async function rowOf(label: string): Promise<RowView> {
  const row = await PlatformId.findOne({ userId: USER, email: mail(label) }).select("status sessionSavedAt statusBeforeDemote demotedAt demotedBy lastChecked sessionCheck").lean();
  assert.ok(row, `no row ${label}`);
  return row as unknown as RowView;
}

const eventsOf = (label: string) => OrderUnitEvent.find({ userId: USER, email: mail(label) }).sort({ at: 1, _id: 1 }).lean();

function assertDemoted(row: RowView, flow: string, reason: string) {
  assert.equal(row.status, "pending");
  assert.equal(row.statusBeforeDemote, "logged_in");
  assert.equal(row.demotedBy, `trade-flow:${flow}`);
  assert.ok(near(row.demotedAt), "demotedAt");
  assert.equal(row.sessionCheck?.outcome, "not_logged_in");
  assert.equal(row.sessionCheck?.reason, reason);
  assert.equal(row.sessionSavedAt?.getTime(), SAVED.getTime(), "the session itself is not touched");
}

function assertUntouched(row: RowView) {
  assert.equal(row.status, "logged_in");
  assert.equal(row.sessionCheck ?? null, null);
  assert.equal(row.lastChecked ?? null, null);
  assert.equal(row.demotedAt ?? null, null);
  assert.equal(row.demotedBy || "", "");
}

// ── update requests, written here (never through createUpdateRequest, which enqueues) ──

type Unit = { orderId: string; itemId: string; unitId: string; orderUrl: string };
let unitSeq = 0;
const orderIdBase = Math.floor(Date.now() / 1000) * 1000;

/** Units with an order row each (snapshotUnit reads it; its own negative `id` keeps the real order counter untouched). */
async function unitsFor(label: string, accountId: Types.ObjectId, n: number): Promise<Unit[]> {
  const units: Unit[] = [];
  for (let i = 0; i < n; i++) {
    unitSeq += 1;
    const orderId = `OD7${String(unitSeq).padStart(14, "0")}`;
    const itemId = `VI${label}${i}`;
    const unitId = `VU${label}${i}`;
    const orderUrl = `https://www.flipkart.com/order_details?order_id=${orderId}&item_id=${itemId}&unit_id=${unitId}`;
    units.push({ orderId, itemId, unitId, orderUrl });
    await Order.collection.insertOne({
      userId: USER,
      platformAccountId: accountId,
      id: -(orderIdBase + unitSeq),
      platform_email: mail(label),
      platform: "FLIPKART",
      order_id: orderId,
      item_id: itemId,
      unit_id: unitId,
      order_url: orderUrl,
      status_key: "",
      product_name: "",
      delivery_otp: null,
      shipping_address: null,
      shipping_address_hash: "",
      createdAt: new Date(),
      updatedAt: new Date(),
    });
  }
  return units;
}

const updateIds: string[] = [];

async function updateRequest(accounts: Array<{ label: string; id: Types.ObjectId; units: Unit[] }>, mode: "scrape" | "api" = "scrape"): Promise<string> {
  const _id = new Types.ObjectId();
  const now = new Date();
  const doc = {
    _id,
    userId: USER,
    source: "manual",
    selectionMethod: "order_ids",
    params: {},
    parentId: "",
    // Another deployment's id: the request log's keeper never takes this request for one of its own.
    systemId: "verify-dsf",
    deliveredOrderIds: [],
    createdBy: "verify",
    status: "queued",
    mode,
    windows: 1,
    accounts: accounts.map((a) => ({ email: mail(a.label), platformId: a.id, units: a.units, total: a.units.length, status: "queued", done: 0, failed: 0, statusChanged: 0, otpNew: 0, via: "", message: "", startedAt: null, finishedAt: null })),
    logs: [{ time: now, level: "info", message: `verify ${RUN}` }],
    startedAt: null,
    finishedAt: null,
    createdAt: now,
    updatedAt: now,
  };
  await OrderUpdateRequest.collection.insertOne(doc);
  // The task log: a row per unit, queued — as createUpdateRequest writes them.
  await openRequestTasksQuietly("update", doc as unknown as Partial<IOrderUpdateRequest> & { _id: unknown });
  updateIds.push(String(_id));
  return String(_id);
}

const fakeJob = (data: UpdateJobData) =>
  ({
    name: "update",
    data,
    moveToDelayed: async () => {
      throw new Error("verify: there is no queue here");
    },
  }) as unknown as Job<UpdateJobData>;

async function runAccount(requestId: string, label: string): Promise<number> {
  const started = Date.now();
  await runOrderUpdateJob(fakeJob({ requestId, email: mail(label) }), undefined, updateDeps);
  return Date.now() - started;
}

async function requestOf(requestId: string) {
  const doc = await OrderUpdateRequest.findById(requestId).lean();
  assert.ok(doc, "request gone");
  return doc;
}

const accountOf = async (requestId: string, label: string) => {
  const a = (await requestOf(requestId)).accounts.find((x) => x.email === mail(label));
  assert.ok(a, `no account ${label}`);
  return a;
};

/** The request's own counters equal its task rows, and no row is open. */
async function updateInStep(requestId: string) {
  await taskLog.flush();
  const doc = await requestOf(requestId);
  assert.equal(doc.status, "done", "the request settled");
  const rows = await logCounts(requestId);
  assert.deepEqual({ ...emptyCounts(0), ...rows }, { ...emptyCounts(0), ...countsOfUpdate(doc) }, "task rows ≠ the request's counters");
  assert.equal(rows.queued + rows.in_progress, 0, "a task row is still open");
}

const rowsOf = (requestId: string) => OrderSyncTask.find({ requestId }).sort({ key: 1 }).select("key email unitId status errorCode error detail").lean();
const logsOf = async (requestId: string) => (await requestOf(requestId)).logs.map((l) => l.message);

// ── fetch runs ──────────────────────────────────────────────────────────────

const fetchRunIds: string[] = [];

async function runFetch(labels: string[], mode: "scrape" | "api", lanes: number, deps: Partial<FetchDeps>) {
  config.orderFetchMode = mode;
  config.fetchWindowConcurrency = lanes;
  let finished!: (info: { runId: string; cancelled: boolean }) => void;
  const ended = new Promise<{ runId: string; cancelled: boolean }>((r) => (finished = r));
  const started = Date.now();
  const { runId } = await startOrderFetch(String(USER), labels.map(mail), SINCE, { trigger: "user", by: "verify", deps, onFinished: (info) => finished(info) });
  fetchRunIds.push(runId);
  const info = await Promise.race([ended, sleep(300_000).then(() => null)]);
  assert.ok(info, "the fetch did not finish within 5 minutes");
  assert.equal(info.cancelled, false);
  // The rows' last write may land just after the ledger row is closed.
  const run = await OrderJobRun.findById(runId).lean();
  assert.ok(run, "ledger row gone");
  const want = (run.meta as { task_counts?: Record<string, number> })?.task_counts;
  assert.ok(want, "the run kept its task counts");
  for (let i = 0; i < 20; i++) {
    await taskLog.flush();
    const rows = await logCounts(runId);
    if (rows.queued + rows.in_progress === 0 && JSON.stringify({ ...emptyCounts(0), ...rows }) === JSON.stringify({ ...emptyCounts(0), ...want })) break;
    await sleep(500);
  }
  return { runId, run, ms: Date.now() - started, logs: getOrderFetchJob(String(USER)).logs.map((l) => l.message) };
}

async function fetchInStep(runId: string) {
  const run = await OrderJobRun.findById(runId).lean();
  assert.ok(run);
  assert.equal(run.status, "completed");
  const rows = await logCounts(runId);
  assert.deepEqual({ ...emptyCounts(0), ...rows }, { ...emptyCounts(0), ...((run.meta as { task_counts?: object }).task_counts || {}) }, "task rows ≠ the run's counts");
  assert.equal(rows.queued + rows.in_progress, 0, "a task row is still open");
}

const fetchRowOf = async (runId: string, label: string) => {
  const r = await OrderSyncTask.findOne({ requestId: runId, email: mail(label) }).select("status errorCode error detail").lean();
  assert.ok(r, `no fetch task for ${label}`);
  return r;
};

/** A fake order API context: it only carries the account's kind; nothing is ever sent through it. */
const fakeApi = (cookies: unknown[]) => {
  const t = (cookies as Array<{ name?: string; value?: string }>).find((c) => c.name === "T")?.value || "";
  return { kind: t.split("-")[2] || "none", dispose: async () => undefined } as unknown as APIRequestContext;
};
const kindOfApi = (ctx: APIRequestContext) => (ctx as unknown as { kind: string }).kind;
const emptyWalk = (): ApiListWalk => ({ units: [], pages: 1, calls: 1, stoppedAtSince: false });

/**
 * The update's fake order API (UpdateJobDeps): what one unit's read does, by
 * the unit's place in its account — "session" (OrderApiSessionError: the API
 * refuses the session), "500" (an HTTP 500), "read" (answered) or "ofd"
 * (answered, out for delivery, no OTP: the unit waits for the page's OTP pass).
 * An answered read writes the order row the way the real one would.
 */
type ApiAnswer = "session" | "500" | "read" | "ofd";
function updateApi(plan: (unitIndex: number) => ApiAnswer, reads: string[]): Partial<UpdateJobDeps> {
  return {
    launch: fakeLaunch,
    lock: noLock,
    openApi: async (cookies) => fakeApi(cookies),
    apiRead: async (_ctx, _userId, _account, card) => {
      const index = Number(String(card.unitId).slice(-1));
      const answer = plan(index);
      reads.push(`${card.unitId}:${answer}`);
      if (answer === "session") throw new OrderApiSessionError("order details did not answer JSON — the session is probably not logged in (verify)");
      if (answer === "500") throw new OrderApiError("https://2.rome.api.flipkart.com/api/5/order/details answered HTTP 500 (verify)", 500);
      const statusKey = answer === "ofd" ? "Out for delivery" : "Order Confirmed";
      await Order.collection.updateOne({ userId: USER, unit_id: card.unitId }, { $set: { status_key: statusKey, updatedAt: new Date() } });
      return { lastFetch: new Date(), productName: "", totalAmount: "", statusKey, gstNumber: "", trackingCount: 0, invoiceDownloaded: false, imei: null, orderDate: null, skippedOld: false, siblingUnitIds: [] } as unknown as Awaited<ReturnType<UpdateJobDeps["apiRead"]>>;
    },
  };
}
const unreadableOf = async (label: string) => (await eventsOf(label)).filter((e) => e.type === "account_unreadable");

// ── the run ─────────────────────────────────────────────────────────────────

async function main() {
  // This process works for the invented workspace only, whatever .env says.
  config.userId = String(USER);
  process.env.TRADE_FLOW_USER_ID = String(USER);
  process.env.SESSION_DEMOTE_CAP = "10";
  // No trace files, and no invoice request queued after an update (none of these units reaches Delivered anyway).
  config.orderTraceDir = "";
  deliveredCheckDefaults.enabled = false;
  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false });
  console.log(`invented workspace ${USER} · @verify.invalid rows · run ${RUN}\n`);

  try {
    // ── update: one account at a time ──────────────────────────────────────
    console.log("UPDATE (runOrderUpdateJob · sealed Chromium · no lock)");

    await check("dead session: 2 /account/orders navigations, no order page, failed after unit 1 in < 15 s; 5 rows not_logged_in = counters; row pending (trade-flow:update); one event", async () => {
      const id = await insertRow("ud1", "dead");
      const requestId = await updateRequest([{ label: "ud1", id, units: await unitsFor("ud1", id, 5) }]);
      const before = launches;
      const ms = await runAccount(requestId, "ud1");
      assert.ok(ms < 15_000, `took ${ms} ms`);
      assert.deepEqual(seenOf("ud1"), { orders: 2, details: 0, login: 2 }, "navigations");
      assert.equal(launches - before, 1, "one Chrome");
      const a = await accountOf(requestId, "ud1");
      assert.equal(a.status, "failed");
      assert.equal(a.message, "Session expired — Flipkart logged this ID out; moved to Not logged in for re-login");
      assert.deepEqual([a.done, a.failed], [0, 5]);
      const rows = await rowsOf(requestId);
      assert.equal(rows.length, 5);
      for (const r of rows) assert.deepEqual([r.status, r.errorCode], ["failed", "not_logged_in"], r.key);
      assert.equal(rows.filter((r) => r.detail === SESSION_DEAD_MESSAGE).length, 1, "unit 1 says what it met");
      assert.equal(rows.filter((r) => r.detail === a.message).length, 4, "the untried units carry the account's sentence");
      await updateInStep(requestId);
      assertDemoted(await rowOf("ud1"), "update", "Update: Flipkart sent /account/orders to the login page twice — re-login");
      const ev = await eventsOf("ud1");
      assert.equal(ev.length, 1);
      assert.equal(ev[0].type, "account_unreadable");
      assert.deepEqual(ev[0].after, { reason: "Update: Flipkart sent /account/orders to the login page twice — re-login", via: "worker", flow: "update", demoted: true, needsRelogin: true });
      assert.equal(ev[0].requestId, requestId);
      const logs = await logsOf(requestId);
      assert.ok(logs.includes(`${mail("ud1")}: Session expired — Flipkart logged this ID out; moved to Not logged in for re-login · 4 more unit(s) not tried`), logs.join("\n"));
    });

    await check("no saved cookies on a logged_in row: confirmed without a page — pending (trade-flow:update), no Chrome", async () => {
      const id = await insertRow("un1", "dead", { cookies: false });
      const requestId = await updateRequest([{ label: "un1", id, units: await unitsFor("un1", id, 2) }]);
      const before = launches;
      await runAccount(requestId, "un1");
      assert.equal(launches, before, "no Chrome");
      const a = await accountOf(requestId, "un1");
      assert.equal(a.status, "failed");
      assert.equal(a.message, "Session: No saved session cookies — re-login this ID");
      assertDemoted(await rowOf("un1"), "update", "Update: No saved session cookies — re-login this ID");
      assert.equal((await eventsOf("un1")).length, 1);
      await updateInStep(requestId);
      for (const r of await rowsOf(requestId)) assert.deepEqual([r.status, r.errorCode], ["failed", "not_logged_in"]);
    });

    await check("a logged_in row that never stored a session: reported unconfirmed, the row stays logged_in", async () => {
      const id = await insertRow("us1", "dead", { savedAt: null });
      const requestId = await updateRequest([{ label: "us1", id, units: await unitsFor("us1", id, 1) }]);
      await runAccount(requestId, "us1");
      const row = await rowOf("us1");
      assert.equal(row.status, "logged_in");
      assert.equal(row.sessionCheck?.reason, `Update: No sessionSavedAt — session was never stored — re-login this ID${SESSION_NOTES.unconfirmed}`);
      assert.equal(row.demotedAt ?? null, null);
      const ev = await eventsOf("us1");
      assert.equal(ev.length, 1);
      assert.equal(ev[0].after?.demoted, false);
      await updateInStep(requestId);
    });

    await check("live session (My Orders and the order pages render): units read, no demotion, no event", async () => {
      const id = await insertRow("ul1", "live");
      const requestId = await updateRequest([{ label: "ul1", id, units: await unitsFor("ul1", id, 2) }]);
      await runAccount(requestId, "ul1");
      assert.deepEqual(seenOf("ul1"), { orders: 1, details: 2, login: 0 });
      const a = await accountOf(requestId, "ul1");
      assert.equal(a.status, "done");
      assert.deepEqual([a.done, a.failed], [2, 0]);
      assert.equal(a.message, "2 refreshed · page");
      for (const r of await rowsOf(requestId)) assert.equal(r.status, "success");
      await updateInStep(requestId);
      assertUntouched(await rowOf("ul1"));
      assert.equal((await eventsOf("ul1")).length, 0);
    });

    await check("empty order page (the CSV wrong-owner case): 'gave no data' after the 30 s wait, the row stays logged_in, the log names another Platform ID", async () => {
      const id = await insertRow("ue1", "empty");
      const requestId = await updateRequest([{ label: "ue1", id, units: await unitsFor("ue1", id, 1) }]);
      const ms = await runAccount(requestId, "ue1");
      assert.ok(ms >= 29_000 && ms < 60_000, `took ${ms} ms — the wait for an order to render stays 30 s`);
      assert.deepEqual(seenOf("ue1"), { orders: 1, details: 1, login: 0 });
      const a = await accountOf(requestId, "ue1");
      assert.equal(a.status, "done");
      assert.deepEqual([a.done, a.failed], [0, 1]);
      const [r] = await rowsOf(requestId);
      assert.equal(r.status, "failed");
      assert.equal(r.errorCode, "unexpected_response");
      assert.match(String(r.detail), /gave no data/);
      await updateInStep(requestId);
      assertUntouched(await rowOf("ue1"));
      assert.equal((await eventsOf("ue1")).length, 0);
      const logs = await logsOf(requestId);
      assert.ok(logs.includes(`${mail("ue1")}: every order page rendered nothing — the session passed the login check; these orders may belong to another Platform ID (1 unit(s))`), logs.join("\n"));
    });

    await check("a session that dies after unit 1: no hand-off; unit 2 fails at once, unit 3 opens no Chrome; one log line; row untouched", async () => {
      const id = await insertRow("uk1", "dies");
      const requestId = await updateRequest([{ label: "uk1", id, units: await unitsFor("uk1", id, 3) }]);
      const ms = await runAccount(requestId, "uk1");
      assert.ok(ms < 30_000, `took ${ms} ms`);
      // My Orders (open) + My Orders (the re-check); order pages for units 1 and 2 only.
      assert.deepEqual(seenOf("uk1"), { orders: 2, details: 2, login: 2 });
      const a = await accountOf(requestId, "uk1");
      assert.equal(a.status, "done");
      assert.deepEqual([a.done, a.failed], [1, 2]);
      const rows = await rowsOf(requestId);
      assert.deepEqual(rows.map((r) => [r.status, r.errorCode || "", r.detail || ""]), [
        ["success", "", ""],
        ["failed", "not_logged_in", "Flipkart session expired"],
        ["failed", "not_logged_in", PAGE_SKIPPED],
      ]);
      await updateInStep(requestId);
      assertUntouched(await rowOf("uk1"));
      assert.equal((await eventsOf("uk1")).length, 0);
      const logs = (await logsOf(requestId)).filter((l) => l.startsWith(mail("uk1")));
      assert.equal(logs.filter((l) => /login page twice/.test(l)).length, 1, logs.join("\n"));
      assert.ok(logs.includes(`${mail("uk1")}: Flipkart sent this ID to the login page twice after 1 unit(s) were read — not moved to Not logged in; the rest of its page work is skipped`));
      assert.equal(logs.filter((l) => l.includes(PAGE_SKIPPED)).length, 0, "the skipped unit has no line of its own");
    });

    await check("My Orders fails to load once on a live session: unit 1 fails, the tab is kept — units 2 and 3 go straight to their order pages", async () => {
      const id = await insertRow("uf1", "flaky");
      const requestId = await updateRequest([{ label: "uf1", id, units: await unitsFor("uf1", id, 3) }]);
      await runAccount(requestId, "uf1");
      // No second My Orders: the tab that met the failed load is the page from then on.
      assert.deepEqual(seenOf("uf1"), { orders: 1, details: 2, login: 0 });
      const a = await accountOf(requestId, "uf1");
      assert.equal(a.status, "done");
      assert.deepEqual([a.done, a.failed], [2, 1]);
      const rows = await rowsOf(requestId);
      assert.deepEqual(rows.map((r) => r.status), ["failed", "success", "success"]);
      assert.match(String(rows[0].detail), /ERR_FAILED/);
      await updateInStep(requestId);
      assertUntouched(await rowOf("uf1"));
      assert.equal((await eventsOf("uf1")).length, 0);
    });

    console.log("\nUPDATE IN API MODE (fake order API)");
    await check("API mode, the API refuses the session and the page lands on the login page twice: handed off with 'the order API refused this ID and …'; 4 rows not_logged_in = counters", async () => {
      const id = await insertRow("ua1", "dead");
      const requestId = await updateRequest([{ label: "ua1", id, units: await unitsFor("ua1", id, 4) }], "api");
      const reads: string[] = [];
      await runOrderUpdateJob(fakeJob({ requestId, email: mail("ua1") }), undefined, updateApi(() => "session", reads));
      assert.equal(reads.length, 1, `the API is asked once: ${reads.join(", ")}`);
      assert.deepEqual(seenOf("ua1"), { orders: 2, details: 0, login: 2 });
      const reason = "Update: the order API refused this ID and Flipkart sent /account/orders to the login page twice — re-login";
      const a = await accountOf(requestId, "ua1");
      assert.equal(a.status, "failed");
      assert.equal(a.message, "Session expired — Flipkart logged this ID out; moved to Not logged in for re-login");
      assert.deepEqual([a.done, a.failed], [0, 4]);
      for (const r of await rowsOf(requestId)) assert.deepEqual([r.status, r.errorCode], ["failed", "not_logged_in"], r.key);
      await updateInStep(requestId);
      assertDemoted(await rowOf("ua1"), "update", reason);
      const ev = await unreadableOf("ua1");
      assert.equal(ev.length, 1);
      assert.deepEqual(ev[0].after, { reason, via: "worker", flow: "update", demoted: true, needsRelogin: true });
      const logs = await logsOf(requestId);
      assert.ok(logs.some((l) => l.startsWith(`${mail("ua1")}: order details did not answer JSON`) && l.endsWith("— using the page from here")), logs.join("\n"));
    });

    await check("API mode, units read over the API, then an HTTP 500's page fallback lands on the login page twice: no hand-off; the next 500 says '— page skipped'; the out-for-delivery pass is skipped", async () => {
      const id = await insertRow("ub1", "dead");
      const requestId = await updateRequest([{ label: "ub1", id, units: await unitsFor("ub1", id, 4) }], "api");
      const reads: string[] = [];
      // unit 0 read (out for delivery, no OTP), unit 1 a 500 → the page (dead), unit 2 a 500 → no page, unit 3 read (out for delivery).
      await runOrderUpdateJob(fakeJob({ requestId, email: mail("ub1") }), undefined, updateApi((i) => (i === 1 || i === 2 ? "500" : "ofd"), reads));
      assert.equal(reads.length, 4);
      // My Orders, its re-check — and nothing more: neither unit 2 nor the out-for-delivery pass opens a page.
      assert.deepEqual(seenOf("ub1"), { orders: 2, details: 0, login: 2 });
      const a = await accountOf(requestId, "ub1");
      assert.equal(a.status, "done");
      assert.deepEqual([a.done, a.failed], [2, 2]);
      const rows = await rowsOf(requestId);
      assert.deepEqual(rows.map((r) => [r.status, r.errorCode || "", r.detail || ""]), [
        ["success", "", ""],
        ["failed", "not_logged_in", SESSION_DEAD_MESSAGE],
        ["failed", "not_logged_in", PAGE_SKIPPED],
        ["success", "", ""],
      ]);
      await updateInStep(requestId);
      const row = await rowOf("ub1");
      assert.equal(row.status, "logged_in");
      assert.equal(row.sessionCheck ?? null, null);
      assert.equal(row.demotedAt ?? null, null);
      assert.equal((await unreadableOf("ub1")).length, 0);
      const logs = (await logsOf(requestId)).filter((l) => l.startsWith(mail("ub1")));
      assert.ok(logs.includes(`${mail("ub1")}: Flipkart sent this ID to the login page twice after 1 unit(s) were read — not moved to Not logged in; the rest of its page work is skipped`), logs.join("\n"));
      assert.equal(logs.filter((l) => /API read failed .* — page skipped$/.test(l)).length, 1, logs.join("\n"));
      assert.equal(logs.filter((l) => /scraping those for OTP/.test(l)).length, 0, "the out-for-delivery pass ran on a dead page");
    });

    await check("API mode, every unit read but out for delivery without an OTP; the OTP pass meets the login page twice: no hand-off, the rest of the pass skipped", async () => {
      const id = await insertRow("uo1", "dead");
      const requestId = await updateRequest([{ label: "uo1", id, units: await unitsFor("uo1", id, 2) }], "api");
      const reads: string[] = [];
      await runOrderUpdateJob(fakeJob({ requestId, email: mail("uo1") }), undefined, updateApi(() => "ofd", reads));
      assert.equal(reads.length, 2);
      assert.deepEqual(seenOf("uo1"), { orders: 2, details: 0, login: 2 });
      const a = await accountOf(requestId, "uo1");
      assert.equal(a.status, "done");
      assert.deepEqual([a.done, a.failed], [2, 0]);
      for (const r of await rowsOf(requestId)) assert.equal(r.status, "success");
      await updateInStep(requestId);
      const row = await rowOf("uo1");
      assert.equal(row.status, "logged_in");
      assert.equal(row.sessionCheck ?? null, null);
      assert.equal((await unreadableOf("uo1")).length, 0);
      const logs = (await logsOf(requestId)).filter((l) => l.startsWith(mail("uo1")));
      assert.ok(logs.includes(`${mail("uo1")}: Flipkart sent this ID to the login page twice during the OTP pass — not moved to Not logged in (2 unit(s) read); 2 OTP scrape(s) skipped`), logs.join("\n"));
    });

    await check("25 accounts, 10 dead, SESSION_DEMOTE_CAP=100: the 7th dead opens the request's circuit — 6 moved, none after; rows = counters", async () => {
      process.env.SESSION_DEMOTE_CAP = "100";
      try {
        // 13 live, 7 dead, 1 live, 3 dead, 1 live — run in this order, one at a time.
        const plan: Kind[] = [...Array(13).fill("live"), ...Array(7).fill("dead"), "live", ...Array(3).fill("dead"), "live"];
        const accounts: Array<{ label: string; id: Types.ObjectId; units: Unit[] }> = [];
        for (let i = 0; i < plan.length; i++) {
          const label = `uc${String(i + 1).padStart(2, "0")}`;
          const id = await insertRow(label, plan[i]);
          accounts.push({ label, id, units: await unitsFor(label, id, 1) });
        }
        const requestId = await updateRequest(accounts);
        for (const a of accounts) await runAccount(requestId, a.label);
        const dead = accounts.filter((_, i) => plan[i] === "dead").map((a) => a.label);
        const live = accounts.filter((_, i) => plan[i] === "live").map((a) => a.label);
        const reason = "Update: Flipkart sent /account/orders to the login page twice — re-login";
        for (const label of dead.slice(0, 6)) assertDemoted(await rowOf(label), "update", reason);
        for (const label of dead.slice(6)) {
          const row = await rowOf(label);
          assert.equal(row.status, "logged_in", label);
          assert.equal(row.sessionCheck?.reason, `${reason}${SESSION_NOTES.circuit}`, label);
          assert.equal((await accountOf(requestId, label)).message, `Session expired — Flipkart logged this ID out${SESSION_NOTES.circuit} — re-login this ID`);
        }
        for (const label of live) assertUntouched(await rowOf(label));
        for (const label of dead) assert.equal((await eventsOf(label)).length, 1, `${label}: a confirmed report always raises its event`);
        const logs = await logsOf(requestId);
        assert.ok(logs.includes("Dead sessions: 7 of the last 20 accounts dead (35 %) — circuit open — no ID is moved to Not logged in for the rest of this update"), logs.filter((l) => /circuit/.test(l)).join("\n"));
        const rows = await rowsOf(requestId);
        assert.equal(rows.filter((r) => r.status === "success").length, 15);
        assert.equal(rows.filter((r) => r.status === "failed" && r.errorCode === "not_logged_in").length, 10);
        await updateInStep(requestId);
      } finally {
        process.env.SESSION_DEMOTE_CAP = "10";
      }
    });

    // ── scrapeAndSave on its own ───────────────────────────────────────────
    console.log("\nSCRAPE AND SAVE");
    await check("an order page sent to the login page fails at once ('Flipkart session expired'), not after the 30 s wait", async () => {
      const { browser, context } = await fakeLaunch();
      try {
        await context.addCookies(cookiesOf("dead", "sx1"));
        const page: Page = await context.newPage();
        const started = Date.now();
        await assert.rejects(
          scrapeAndSave(page, String(USER), { id: new Types.ObjectId(), email: mail("sx1"), cookies: [] }, { orderId: "OD700000000000099", itemId: "VIsx", unitId: "VUsx", orderUrl: "https://www.flipkart.com/order_details?order_id=OD700000000000099&item_id=VIsx&unit_id=VUsx", amount: "" }, { applyCalendar: false }),
          /^Error: Flipkart session expired$/
        );
        assert.ok(Date.now() - started < 5_000, `took ${Date.now() - started} ms`);
        assert.deepEqual(seenOf("sx1"), { orders: 0, details: 1, login: 1 });
      } finally {
        await context.close().catch(() => undefined);
        await browser.close().catch(() => undefined);
      }
    });

    // ── fetch ──────────────────────────────────────────────────────────────
    console.log("\nFETCH (startOrderFetch · sealed Chromium · fake order API in API mode)");

    await check("page mode, a dead and a live account: one task failed 'Session expired …' (not_logged_in), row pending (trade-flow:fetch), one event; the other succeeded, untouched", async () => {
      await insertRow("fd1", "dead");
      await insertRow("fl1", "live");
      const { runId, run } = await runFetch(["fd1", "fl1"], "scrape", 2, { launch: fakeLaunch });
      await fetchInStep(runId);
      assert.deepEqual(seenOf("fd1"), { orders: 2, details: 0, login: 2 });
      const dead = await fetchRowOf(runId, "fd1");
      assert.equal(dead.status, "failed");
      assert.equal(dead.errorCode, "not_logged_in");
      assert.equal(dead.detail, "Session expired — Flipkart logged this ID out; moved to Not logged in for re-login");
      assert.deepEqual(run.failedItems.map((f) => [f.key, f.reason]), [[mail("fd1"), "Session expired — Flipkart logged this ID out; moved to Not logged in for re-login"]]);
      assertDemoted(await rowOf("fd1"), "fetch", "Fetch: Flipkart sent /account/orders to the login page twice — re-login");
      const ev = await eventsOf("fd1");
      assert.equal(ev.length, 1);
      assert.deepEqual(ev[0].after, { reason: "Fetch: Flipkart sent /account/orders to the login page twice — re-login", via: "page", flow: "fetch", demoted: true, needsRelogin: true });
      assert.equal(ev[0].requestId, runId);
      const live = await fetchRowOf(runId, "fl1");
      assert.equal(live.status, "success");
      assert.equal(seenOf("fl1").orders, 1);
      assertUntouched(await rowOf("fl1"));
      assert.equal((await eventsOf("fl1")).length, 0);
    });

    await check("API mode, the list answers, every unit's API read fails: the fallback page is confirmed once, later units skip Chrome; the account succeeds; row logged_in, no event", async () => {
      const id = await insertRow("fa1", "dead");
      const listed = [1, 2, 3].map((i) => {
        const orderId = `OD6${String(i).padStart(14, "0")}`;
        return { orderId, itemId: `VIfa${i}`, unitId: `VUfa${i}`, orderUrl: `https://www.flipkart.com/order_details?order_id=${orderId}&item_id=VIfa${i}&unit_id=VUfa${i}`, orderDate: new Date(), status: "Order Confirmed", statusText: "", amount: "", productName: "", unit: {} };
      });
      let apiReads = 0;
      const { runId, ms, logs } = await runFetch(["fa1"], "api", 1, {
        launch: fakeLaunch,
        openApi: async (cookies) => fakeApi(cookies),
        walkList: async () => ({ ...emptyWalk(), units: listed }),
        apiRead: async () => {
          apiReads++;
          throw new OrderApiError("https://2.rome.api.flipkart.com/api/5/order/details answered HTTP 500 (verify)", 500);
        },
      });
      await fetchInStep(runId);
      assert.ok(ms < 30_000, `took ${ms} ms`);
      assert.equal(apiReads, 3);
      // One order page (unit 1) and one My Orders (its re-check): units 2 and 3 open nothing.
      assert.deepEqual(seenOf("fa1"), { orders: 1, details: 1, login: 2 });
      assert.equal((await fetchRowOf(runId, "fa1")).status, "success");
      const idx = await OrderIndex.find({ userId: USER, platformAccountId: id }).sort({ unit_id: 1 }).select("unit_id scrape_status last_error").lean();
      assert.deepEqual(idx.map((r) => [r.unit_id, r.scrape_status, r.last_error]), [
        ["VUfa1", "failed", "Flipkart session expired"],
        ["VUfa2", "failed", PAGE_FALLBACK_SKIPPED],
        ["VUfa3", "failed", PAGE_FALLBACK_SKIPPED],
      ]);
      assert.equal(classifyMessage(PAGE_FALLBACK_SKIPPED).errorClass, "not_logged_in");
      assertUntouched(await rowOf("fa1"));
      assert.equal((await eventsOf("fa1")).length, 0);
      assert.equal(logs.filter((l) => /landed on Flipkart's login page twice/.test(l)).length, 1, logs.join("\n"));
    });

    await check("13 live then 8 dead in one lane, SESSION_DEMOTE_CAP=100: the run's circuit opens at the 7th dead — 6 moved, 2 not", async () => {
      process.env.SESSION_DEMOTE_CAP = "100";
      try {
        const plan: Kind[] = [...Array(13).fill("live"), ...Array(8).fill("dead")];
        const labels: string[] = [];
        for (let i = 0; i < plan.length; i++) {
          const label = `fc${String(i + 1).padStart(2, "0")}`;
          await insertRow(label, plan[i]);
          labels.push(label);
        }
        // API mode: a live account's list answers (nothing listed); a dead one's is refused, so it goes to the page.
        const { runId, logs } = await runFetch(labels, "api", 1, {
          launch: fakeLaunch,
          openApi: async (cookies) => fakeApi(cookies),
          walkList: async (ctx) => {
            if (kindOfApi(ctx) === "dead") throw new OrderApiSessionError("order list did not answer JSON — the session is probably not logged in (verify)");
            return emptyWalk();
          },
          apiRead: async () => {
            throw new Error("verify: nothing is listed");
          },
        });
        await fetchInStep(runId);
        const dead = labels.filter((_, i) => plan[i] === "dead");
        const reason = "Fetch: Flipkart sent /account/orders to the login page twice — re-login";
        for (const label of dead.slice(0, 6)) assertDemoted(await rowOf(label), "fetch", reason);
        for (const label of dead.slice(6)) {
          const row = await rowOf(label);
          assert.equal(row.status, "logged_in", label);
          assert.equal(row.sessionCheck?.reason, `${reason}${SESSION_NOTES.circuit}`, label);
          assert.equal((await fetchRowOf(runId, label)).detail, `Session expired — Flipkart logged this ID out${SESSION_NOTES.circuit} — re-login this ID`);
        }
        for (const label of labels.filter((_, i) => plan[i] === "live")) {
          assertUntouched(await rowOf(label));
          assert.equal((await fetchRowOf(runId, label)).status, "success");
        }
        for (const label of dead) assert.equal((await fetchRowOf(runId, label)).errorCode, "not_logged_in");
        assert.ok(logs.includes("Dead sessions: 7 of the last 20 accounts dead (35 %) — circuit open — no ID is moved to Not logged in for the rest of this fetch"), logs.filter((l) => /circuit/.test(l)).join("\n"));
      } finally {
        process.env.SESSION_DEMOTE_CAP = "10";
      }
    });

    console.log("\nTHE FAKE");
    await check("nothing went round the route: every request was answered here or aborted", () => {
      assert.deepEqual(unanswered, []);
      assert.ok(launches > 0);
      console.log(`        ${launches} Chromium launch(es) · ${aborted} request(s) aborted · ${[...seen.values()].reduce((n, s) => n + s.orders + s.details + s.login, 0)} page(s) answered`);
    });
  } finally {
    for (const b of [...browsers]) await b.close().catch(() => undefined);
    await cleanUp();
  }
  console.log(`\n${passed} passed, ${failures.length} failed${failures.length ? `: ${failures.join(" · ")}` : ""}`);
  await mongoose.disconnect();
  process.exit(failures.length ? 1 : 0);
}

/**
 * Everything this run wrote, by the invented workspace id. The dashboard API
 * turns the events into alerts as they arrive: the events go first, so no new
 * one is taken, then the alerts, then a recount.
 */
async function cleanUp() {
  const db = mongoose.connection.db!;
  // An update left open would be taken for stranded by a worker's reaper: none stays open, even on a failure.
  await OrderUpdateRequest.updateMany({ userId: USER, status: { $in: ["queued", "running"] } }, { $set: { status: "cancelled" } }).catch(() => undefined);
  const events = (await OrderUnitEvent.deleteMany({ userId: USER })).deletedCount;
  await sleep(3000);
  const del = {
    platformids: (await PlatformId.deleteMany({ userId: USER })).deletedCount,
    order_unit_events: events + (await OrderUnitEvent.deleteMany({ userId: USER })).deletedCount,
    alerts: (await db.collection("alerts").deleteMany({ userId: USER })).deletedCount,
    order_update_requests: (await OrderUpdateRequest.deleteMany({ userId: USER })).deletedCount,
    order_job_runs: (await OrderJobRun.deleteMany({ userId: USER })).deletedCount,
    order_sync_tasks: (await OrderSyncTask.deleteMany({ $or: [{ userId: USER }, { requestId: { $in: [...updateIds, ...fetchRunIds] } }] })).deletedCount,
    order_details: (await Order.deleteMany({ userId: USER })).deletedCount,
    order_index: (await OrderIndex.deleteMany({ userId: USER })).deletedCount,
    session_demotions: (await db.collection("session_demotions").deleteMany({ userId: USER })).deletedCount,
  };
  await sleep(2000);
  await db.collection("alerts").deleteMany({ userId: USER });
  const left: Record<string, number> = {};
  for (const name of ["platformids", "order_unit_events", "alerts", "order_update_requests", "order_job_runs", "order_sync_tasks", "order_details", "order_index", "session_demotions", "order_invoice_requests", "order_coupon_requests", "tradeflowjobs", "batches"]) {
    left[name] = await db.collection(name).countDocuments({ userId: USER });
  }
  left.order_sync_tasks += await OrderSyncTask.countDocuments({ requestId: { $in: [...updateIds, ...fetchRunIds] } });
  left["*@verify.invalid platformids of this run"] = await PlatformId.countDocuments({ userId: USER, email: /@verify\.invalid$/ });
  console.log(`\ncleaned up: ${Object.entries(del).map(([k, v]) => `${k} ${v}`).join(" · ")}`);
  console.log(`left behind: ${Object.entries(left).map(([k, v]) => `${k} ${v}`).join(" · ")}${Object.values(left).every((n) => n === 0) ? " — nothing" : " — NOT CLEAN"}`);
  if (!Object.values(left).every((n) => n === 0)) failures.push("clean-up");
}

main().catch(async (err) => {
  console.error(err);
  for (const b of [...browsers]) await b.close().catch(() => undefined);
  await cleanUp().catch((e) => console.error("clean-up failed:", e));
  process.exit(1);
});
