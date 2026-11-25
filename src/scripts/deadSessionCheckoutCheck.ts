/**
 * F4 + F5 of the dead-session fixes — checkout, cancel and reschedule hand a
 * session they CONFIRMED dead to the one hand-off (services/sessionEligibility.ts)
 * — WITHOUT Flipkart, without a worker, without a queue:
 *
 *   CHECKOUT (F4)
 *   · the /account pre-flight (FlipkartCheckout.fetchAccountMobile): Flipkart's
 *     /login?ret=%2faccount — and the old /account/login — throw
 *     PREFLIGHT_LOGIN_MESSAGE (SESSION_EXPIRED); a real /account still gives
 *     its mobile
 *   · the runner's hand-off on the still-open page (checkoutRunner.ts
 *     loginLandingFailure): the login page again on /account/orders → the row
 *     pending (trade-flow:checkout), one event, the job's error ending "— moved
 *     to Not logged in", not re-queued; My Orders on the re-check → nothing
 *     written, and re-queued as before; a session saved an hour ago →
 *     reported, not moved, not re-queued
 *   · the session-check exit releases the account lock (it used to hold it
 *     for ACCOUNT_LOCK_MS)
 *   · no retry: an invented batch holds one job for an e-mail that has no
 *     Platform ID; runCheckoutJob stops at the session check and the batch
 *     still holds exactly that one job (a retry of the same e-mail used to be
 *     queued, an attempt each time)
 *   CANCEL (dry run) + RESCHEDULE (F5), real jobs on invented requests
 *   · the login page twice and nothing done → the units read "Session expired
 *     — re-login this ID — moved to Not logged in" (the unit that met the page
 *     keeps its own words before that), the row pending, one event, one
 *     re-check per job — also with two tabs, and for an urgent cancel's order
 *     pages
 *   · Flipkart answered for a unit first (refused / not available) → no hand-off
 *   · the re-check opens My Orders → no hand-off
 *   · every request's task log closes all its rows and equals its counters
 *
 * Nothing can leave the machine: the Chromium resolves no host name, and every
 * request is answered here or aborted (a login redirect is the page's own
 * script — a fulfilled 302 would be followed on the network, see
 * deadSessionCheck.ts launchSealed). BullMQ's add / addBulk refuse in this
 * process. The cancel and reschedule jobs get no proxy, no API pass and no
 * Redis lock.
 *
 * What is real: Mongo — platformids, order_unit_events, alerts,
 * order_cancel_requests, order_reschedule_requests, order_sync_tasks,
 * order_sync_settings, session_demotions, tradeflowjobs and tradeflowbatches
 * rows of an INVENTED workspace id with @verify.invalid e-mails, all deleted at
 * the end (the script prints what was left) — and one Redis key, the invented
 * e-mail's checkout lock (account:lock:k-norow-<run>@verify.invalid), which the
 * job must release itself (deleted at the end whatever happened). This process
 * uses the invented workspace id whatever TRADE_FLOW_USER_ID says. Screenshots
 * go to a temp folder, removed at the end.
 *
 *   npm run test:dead-session-checkout
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Queue, type Job } from "bullmq";
import mongoose, { Types } from "mongoose";
import { chromium, type Browser, type BrowserContext, type Route } from "playwright";
import { FlipkartCheckout, PREFLIGHT_LOGIN_MESSAGE } from "../automation/FlipkartCheckout.js";
import { config } from "../config.js";
import { getRedis } from "../lib/redis.js";
import { CancelRequest, type ICancelOrder, type ICancelUnit } from "../models/CancelRequest.js";
import { CheckoutBatch } from "../models/CheckoutBatch.js";
import { CheckoutJob } from "../models/CheckoutJob.js";
import { OrderSyncTask } from "../models/OrderSyncTask.js";
import { OrderUnitEvent } from "../models/OrderUnitEvent.js";
import { PlatformId } from "../models/PlatformId.js";
import { RescheduleRequest, type IRescheduleUnit } from "../models/RescheduleRequest.js";
import type { CancelJobData, RescheduleJobData } from "../queue.js";
import { CHECKOUT_ERRORS } from "../services/checkoutErrors.js";
import { loginLandingFailure, MOVED_TO_NOT_LOGGED_IN, runCheckoutJob } from "../services/checkoutRunner.js";
import { runCancelJob, type CancelJobDeps } from "../services/orderCancel.js";
import { runRescheduleJob, type RescheduleJobDeps } from "../services/orderReschedule.js";
import { SESSION_NOTES } from "../services/sessionEligibility.js";
import { resolveLoggedInSession } from "../services/sessionStore.js";
import { countsOfCancel, countsOfReschedule, logCounts, openRequestTasksQuietly, taskLog } from "../services/syncTaskLog.js";
import type { CheckoutJobData, LogLevel } from "../types.js";

// ── nothing is queued from this process ─────────────────────────────────────
// Every Queue in this process — the checkout retry, the order sync — refuses,
// and the attempt is recorded, so a check below can say none was made.
const enqueued: string[] = [];
for (const method of ["add", "addBulk"] as const) {
  (Queue.prototype as unknown as Record<string, unknown>)[method] = async function (this: { name: string }) {
    enqueued.push(`${this.name}.${method}`);
    throw new Error(`verify: refused to ${method} onto "${this.name}" — this check never enqueues`);
  };
}

const USER = new Types.ObjectId();
const RUN = Date.now().toString(36);
const DIGITS = String(Date.now()).slice(-9);
const MIN = 60_000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;
const SYSTEM = "verify-dsc";
const ARTIFACTS = fs.mkdtempSync(path.join(os.tmpdir(), "verify-dsc-"));
const ago = (ms: number) => new Date(Date.now() - ms);
const near = (d: unknown, ms = 5 * MIN) => d instanceof Date && Math.abs(Date.now() - d.getTime()) < ms;
/** The run in every e-mail: the dashboard folds an event into any open alert of the same e-mail and day, so two runs at once (another Mac, a re-run) must not share one. */
const mail = (name: string) => `${name}-${RUN}@verify.invalid`;
/** Invented ids in Flipkart's shapes: OD + 18 digits, a unit id whose item id is all but its last 3 digits. */
const orderId = (n: number) => `OD999${DIGITS}${String(n).padStart(6, "0")}`;
const unitId = (n: number, k: number) => `999${DIGITS}${String(n).padStart(4, "0")}${k}000`;
const itemOf = (unit: string) => unit.slice(0, -3);

const SESSION_SENTENCE = "Session expired — re-login this ID";
const CANCEL_LOGIN = "SESSION_EXPIRED: Flipkart showed the login page";

let passed = 0;
const failures: string[] = [];
const requestIds: string[] = [];

async function check(name: string, fn: () => void | Promise<void>) {
  try {
    await fn();
    passed++;
    console.log(`  ok    ${name}`);
  } catch (err) {
    failures.push(name);
    console.log(`  FAIL  ${name}\n        ${err instanceof Error ? err.message.split("\n").slice(0, 10).join("\n        ") : err}`);
  }
}

// ── invented Platform IDs ───────────────────────────────────────────────────

/** Invented cookies under Flipkart's auth names, so restoreFlipkartSession's check passes. Only the routes below ever see them. */
const FAKE_COOKIES = ["T", "SN", "at"].map((name) => ({ name, value: `verify-invalid-${name}`, domain: ".flipkart.com", path: "/", httpOnly: true, secure: true, sameSite: "Lax" }));
const FAKE_MOBILE = "0000012345";

async function insertRow(email: string, savedAgoMs: number) {
  const _id = new Types.ObjectId();
  const now = new Date();
  await PlatformId.collection.insertOne({
    _id,
    userId: USER,
    onboardingId: new Types.ObjectId(),
    parentId: null,
    parentEmail: "",
    domain: "verify.invalid",
    platform: "flipkart",
    email,
    status: "logged_in",
    lastError: "",
    retryable: true,
    sessionCookies: FAKE_COOKIES,
    sessionState: { cookies: FAKE_COOKIES, origins: [] },
    sessionSavedAt: ago(savedAgoMs),
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
  email: string;
  status: string;
  sessionSavedAt: Date | null;
  statusBeforeDemote?: string;
  demotedAt?: Date | null;
  demotedBy?: string;
  lastChecked: Date | null;
  sessionCheck?: { at: Date; outcome: string; reason: string; via: string } | null;
};

/** The fields under test — never the cookies. */
async function rowOf(email: string): Promise<RowView> {
  const row = await PlatformId.findOne({ userId: USER, email }).select("email status sessionSavedAt statusBeforeDemote demotedAt demotedBy lastChecked sessionCheck").lean();
  assert.ok(row, `no row ${email}`);
  return row as unknown as RowView;
}

const eventsOf = (email: string) => OrderUnitEvent.find({ userId: USER, email, type: "account_unreadable" }).sort({ at: 1, _id: 1 }).lean();

async function sessionOf(email: string) {
  const s = await resolveLoggedInSession(email);
  assert.ok(s.ok, `${email}: ${s.ok ? "" : s.reason}`);
  return s;
}

/** Moved by the hand-off: pending, the demotion fields set, one event that says so. */
async function assertMoved(email: string, flow: string, reason: string, requestId: string) {
  const row = await rowOf(email);
  assert.equal(row.status, "pending");
  assert.equal(row.statusBeforeDemote, "logged_in");
  assert.ok(near(row.demotedAt), "demotedAt");
  assert.equal(row.demotedBy, `trade-flow:${flow}`);
  assert.equal(row.sessionCheck?.outcome, "not_logged_in");
  assert.equal(row.sessionCheck?.reason, reason);
  assert.equal(row.sessionCheck?.via, "worker");
  const ev = await eventsOf(email);
  assert.equal(ev.length, 1, `${ev.length} events`);
  assert.deepEqual(ev[0].after, { reason, via: "worker", flow, demoted: true, needsRelogin: true });
  assert.equal(ev[0].requestId, requestId);
  assert.equal(await PlatformId.countDocuments({ userId: USER, email, "sessionState.cookies.0": { $exists: true } }), 1, "cookies kept");
}

/** Never reported: still logged_in, nothing written on it, no event. */
async function assertUntouched(email: string) {
  const row = await rowOf(email);
  assert.equal(row.status, "logged_in");
  assert.equal(row.sessionCheck ?? null, null, "sessionCheck written");
  assert.equal(row.lastChecked ?? null, null, "lastChecked written");
  assert.equal(row.demotedAt ?? null, null);
  assert.equal((await eventsOf(email)).length, 0, "an event was raised");
}

// ── a fake Flipkart for Chromium ────────────────────────────────────────────

/**
 * Plain Chromium in which no host name resolves: whatever the routes below do
 * not answer fails on this machine (net::ERR_NAME_NOT_RESOLVED) instead of
 * reaching anyone.
 */
const launchSealed = () => chromium.launch({ headless: true, args: ["--host-resolver-rules=MAP * ~NOTFOUND"] });

const loginFor = (ret: string) => `https://www.flipkart.com/login?ret=${ret.replace(/\//g, "%2f")}`;
const html = (title: string, body = "", script = "") => `<html><head><title>${title}</title>${script ? `<script>${script}</script>` : ""}</head><body>${body}</body></html>`;
/** Flipkart's login redirect, made by the page's own script: a new navigation the route answers. */
const redirectTo = (url: string) => html("…", "", `location.replace(${JSON.stringify(url)})`);

interface Scene {
  /** /account/orders — the re-check: Flipkart's login page again, or My Orders. */
  orders: "login" | "orders";
  /** /order_details, by unit id: the login page, an order page whose data refuses the cancel, or a rendered order page. */
  details?: (unit: string) => "login" | "refuse" | "page";
  /** /account: today's login page, the old /account/login, or the profile with its mobile. */
  account?: "login" | "old-login" | "mobile";
}

interface Seen {
  ordersNavigations: number;
  routed: string[];
  aborted: string[];
  /** Requests that went round the routes — the sinkhole stopped them; must stay empty. */
  unanswered: string[];
}

const newSeen = (): Seen => ({ ordersNavigations: 0, routed: [], aborted: [], unanswered: [] });

async function answer(route: Route, scene: Scene, seen: Seen) {
  const req = route.request();
  const url = new URL(req.url());
  const where = `${req.method()} ${url.host}${url.pathname}`;
  const fk = url.host === "www.flipkart.com";
  const fulfil = (body: string, contentType = "text/html", headers: Record<string, string> = {}) => {
    seen.routed.push(where);
    return route.fulfill({ status: 200, contentType, headers, body });
  };
  if (fk && url.pathname === "/account/orders") {
    if (req.isNavigationRequest()) seen.ordersNavigations++;
    return fulfil(scene.orders === "login" ? redirectTo(loginFor("/account/orders")) : html("My Orders", "<a href='/order_details?order_id=OD1'>order_id=OD1</a> Price details"));
  }
  if (fk && (url.pathname === "/login" || url.pathname === "/account/login")) return fulfil(html("Login", "Login · Enter Email/Mobile number · Request OTP"));
  if (fk && url.pathname === "/account" && scene.account) {
    if (scene.account === "login") return fulfil(redirectTo(loginFor("/account")));
    if (scene.account === "old-login") return fulfil(redirectTo("https://www.flipkart.com/account/login?ret=%2faccount"));
    return fulfil(html("My Profile", `Personal Information <input name="mobileNumber" value="${FAKE_MOBILE}">`));
  }
  if (fk && url.pathname === "/order_details" && scene.details) {
    const kind = scene.details(url.searchParams.get("unit_id") || "");
    if (kind === "login") return fulfil(redirectTo(loginFor("/order_details")));
    if (kind === "refuse") {
      // The order page asks for its own data, as Flipkart's does (CX_ORDER_DETAIL_PAGE); the answer refuses the cancel.
      return fulfil(html("Order Details", "Order Details", `fetch("/api/4/page/fetch", { method: "POST", body: JSON.stringify({ pageUri: "/order_details", pageContext: { pageId: "CX_ORDER_DETAIL_PAGE" } }) })`));
    }
    return fulfil(html("Order Details", `Order Details · ${"Delivery expected by Mon, Dec 1 · Deliver to Verify Invalid · ".repeat(40)}`));
  }
  if (fk && url.pathname === "/api/4/page/fetch" && req.method() === "POST") {
    return fulfil(JSON.stringify({ RESPONSE: { slots: [{ widget: { data: { actions: { CANCEL: { eligible: false, eligibilityReason: "Verify: this unit has shipped" } } } } }] } }), "application/json");
  }
  if (url.host === "2.rome.api.flipkart.com" && url.pathname.startsWith("/api/1/self-serve/rfr/feasible/")) {
    return fulfil(
      JSON.stringify({ RESPONSE: { feasibilityStatus: false, feasibilityFailureMessageTitle: "Verify", feasibilityFailureMessage: ["Delivery date can't be changed on this unit"], token: "" } }),
      "application/json",
      { "access-control-allow-origin": "https://www.flipkart.com", "access-control-allow-credentials": "true" }
    );
  }
  seen.aborted.push(where);
  return route.abort();
}

/** A context of the sealed browser in which every request is answered by `answer` or aborted. */
function sealedContext(scene: Scene, seen: Seen) {
  return async (browser: Browser): Promise<BrowserContext> => {
    const ctx = await browser.newContext();
    await ctx.route("**/*", (route) => answer(route, scene, seen));
    ctx.on("requestfailed", (r) => {
      if (/ERR_NAME_NOT_RESOLVED/.test(r.failure()?.errorText || "")) seen.unanswered.push(r.url());
    });
    return ctx;
  };
}

/** Only the fake pages were served, and nothing went round the routes. */
function assertSealed(seen: Seen) {
  assert.deepEqual(seen.unanswered, [], "a request went round the routes");
  for (const r of seen.routed) assert.match(r, /^(GET|POST) (www\.flipkart\.com|2\.rome\.api\.flipkart\.com)\//, r);
}

// ── cancel / reschedule jobs on invented requests ───────────────────────────

const fakeJob = <T>(data: T) =>
  ({
    data,
    moveToDelayed: async () => {
      throw new Error("verify: there is no queue here");
    },
  }) as unknown as Job<T>;

/** Everything the jobs reach beyond Mongo, swapped: the sealed browser, no proxy, no API pass, no Redis lock. */
const chromeOnly = (scene: Scene, seen: Seen) => ({
  lock: async () => async () => undefined,
  egress: async () => ({ egress: null, release: async () => undefined }),
  launch: () => launchSealed(),
  context: sealedContext(scene, seen),
  artifactDir: (requestId: string) => path.join(ARTIFACTS, requestId),
});
const cancelDeps = (scene: Scene, seen: Seen): Partial<CancelJobDeps> => ({ ...chromeOnly(scene, seen), fetchMode: async () => ({ mode: "scrape", source: "env" }) });
const rescheduleDeps = (scene: Scene, seen: Seen): Partial<RescheduleJobDeps> => chromeOnly(scene, seen);

const cancelUnit = (email: string, n: number, k: number): ICancelUnit => {
  const unit = unitId(n, k);
  return { orderId: orderId(n), unitId: unit, itemId: itemOf(unit), email, productName: "Verify product", status: "queued", message: "", startedAt: null, finishedAt: null };
};

async function cancelRequest(email: string, opts: { units?: ICancelUnit[]; orders?: string[] }) {
  const units = opts.units || [];
  const orders: ICancelOrder[] = (opts.orders || []).map((id) => ({ orderId: id, email, status: "queued", message: "", unitsFound: 0, startedAt: null, finishedAt: null }));
  const request = await CancelRequest.create({
    userId: USER,
    type: orders.length ? "order" : "unit",
    input: orders.length ? orders.map((o) => o.orderId) : units.map((u) => u.unitId),
    dryRun: true,
    urgent: orders.length > 0,
    createdBy: "verify",
    systemId: SYSTEM,
    status: "queued",
    units,
    orders,
    rejected: [],
  });
  await openRequestTasksQuietly("cancel", request.toObject());
  requestIds.push(String(request._id));
  return String(request._id);
}

async function rescheduleRequest(email: string, n: number, count: number) {
  const units: IRescheduleUnit[] = Array.from({ length: count }, (_, k) => {
    const unit = unitId(n, k);
    return { orderId: orderId(n), unitId: unit, itemId: itemOf(unit), email, productName: "Verify product", status: "queued", message: "", offeredDates: [], chosen: "", token: "", startedAt: null, finishedAt: null };
  });
  const request = await RescheduleRequest.create({
    userId: USER,
    type: "unit",
    input: units.map((u) => u.unitId),
    date: "2026-12-01",
    createdBy: "verify",
    systemId: SYSTEM,
    confirm: false,
    status: "queued",
    units,
    rejected: [],
  });
  await openRequestTasksQuietly("reschedule", request.toObject());
  requestIds.push(String(request._id));
  return String(request._id);
}

/** The request ended, its task rows equal its own counters, none is left open — and how the given units' rows read. */
async function taskLogInStep(kind: "cancel" | "reschedule", requestId: string) {
  await taskLog.flush();
  const doc = kind === "cancel" ? await CancelRequest.findById(requestId).lean() : await RescheduleRequest.findById(requestId).lean();
  assert.ok(doc, "request gone");
  assert.equal(doc.status, "done", "request not closed");
  const rows = await logCounts(requestId);
  const own = kind === "cancel" ? countsOfCancel(doc as Parameters<typeof countsOfCancel>[0]) : countsOfReschedule(doc as Parameters<typeof countsOfReschedule>[0]);
  assert.deepEqual(rows, own, "task rows ≠ the request's counters");
  assert.equal(rows.queued + rows.in_progress, 0, "a task row is still open");
  return OrderSyncTask.find({ requestId }).select("key status errorCode detail -_id").sort({ key: 1 }).lean();
}

const unitsOf = async (requestId: string) => (await CancelRequest.findById(requestId).lean())!;
const rescheduleUnitsOf = async (requestId: string) => (await RescheduleRequest.findById(requestId).lean())!;

// ── the run ─────────────────────────────────────────────────────────────────

async function main() {
  // This process works for the invented workspace only, whatever .env says.
  config.userId = String(USER);
  process.env.TRADE_FLOW_USER_ID = String(USER);
  process.env.SESSION_DEMOTE_CAP = "10";
  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false });
  console.log(`invented workspace ${USER} · rows k-*, c-*, r-* @verify.invalid · screenshots ${ARTIFACTS}\n`);

  const lockKey = `account:lock:${mail("k-norow")}`;
  let browser: Browser | null = null;
  try {
    // ── checkout: the pre-flight and the runner's hand-off ─────────────────
    console.log("CHECKOUT (sealed Chromium: no host name resolves; every request answered here or aborted)");
    browser = await launchSealed();
    await check("the sealed browser: a request the routes do not answer cannot leave the machine", async () => {
      const ctx = await browser!.newContext();
      try {
        await assert.rejects((await ctx.newPage()).goto("https://www.example.invalid/", { timeout: 15_000 }), /ERR_NAME_NOT_RESOLVED/);
      } finally {
        await ctx.close();
      }
    });

    const preflight = async (account: Scene["account"]) => {
      const seen = newSeen();
      const ctx = await sealedContext({ orders: "orders", account }, seen)(browser!);
      try {
        const page = await ctx.newPage();
        const lines: string[] = [];
        const checkout = new FlipkartCheckout(page, "https://www.flipkart.com/verify-invalid/p/itm0?pid=VERIFY", (level, message) => void lines.push(`${level} ${message}`));
        const out = await checkout.fetchAccountMobile().then((v) => ({ value: v, error: null as Error | null }), (e: Error) => ({ value: null, error: e }));
        assertSealed(seen);
        return { ...out, url: page.url(), lines };
      } finally {
        await ctx.close();
      }
    };
    await check("pre-flight: /account sent to /login?ret=%2faccount (what Flipkart does) → throws PREFLIGHT_LOGIN_MESSAGE → SESSION_EXPIRED (a verdict once the runner confirms it)", async () => {
      const out = await preflight("login");
      assert.equal(out.url, "https://www.flipkart.com/login?ret=%2faccount");
      assert.equal(out.error?.message, PREFLIGHT_LOGIN_MESSAGE);
      assert.ok(out.lines.some((l) => /^error \[preflight\] \/account redirected to https:\/\/www\.flipkart\.com\/login\?ret=%2faccount/.test(l)), out.lines.join(" | "));
      assert.equal(CHECKOUT_ERRORS.SESSION_EXPIRED.noRetry, true);
    });
    await check("pre-flight: the old /account/login?ret=… → the same", async () => {
      const out = await preflight("old-login");
      assert.equal(out.error?.message, PREFLIGHT_LOGIN_MESSAGE);
    });
    await check("pre-flight: a real /account still gives the account's mobile", async () => {
      const out = await preflight("mobile");
      assert.equal(out.error, null);
      assert.equal(out.value, FAKE_MOBILE);
    });

    await insertRow(mail("k-dead"), 30 * DAY);
    await insertRow(mail("k-alive"), 30 * DAY);
    await insertRow(mail("k-fresh"), 1 * HOUR);
    const landing = async (email: string, where: "viewcart" | "/account", orders: Scene["orders"]) => {
      const seen = newSeen();
      const ctx = await sealedContext({ orders }, seen)(browser!);
      try {
        const page = await ctx.newPage();
        const lines: string[] = [];
        const jobId = `verify-job-${RUN}-${email.split("@")[0]}`;
        const failure = await loginLandingFailure(where, { page, session: await sessionOf(email), userId: String(USER), jobId, log: (level: LogLevel, message: string) => void lines.push(`${level} ${message}`) });
        assertSealed(seen);
        return { failure, lines, seen, jobId };
      } finally {
        await ctx.close();
      }
    };
    await check("runner hand-off: login page on /account, again on /account/orders → pending (trade-flow:checkout), one event, the error ends '— moved to Not logged in'", async () => {
      const { failure, seen, jobId, lines } = await landing(mail("k-dead"), "/account", "login");
      assert.equal(failure.code, "SESSION_EXPIRED");
      assert.equal(failure.noRetry, true);
      assert.equal(failure.details, `${PREFLIGHT_LOGIN_MESSAGE}${MOVED_TO_NOT_LOGGED_IN}`);
      assert.equal(seen.ordersNavigations, 1, "one re-check");
      await assertMoved(mail("k-dead"), "checkout", "Checkout: Flipkart sent this ID to the login page (/account, then /account/orders) — re-login", jobId);
      assert.ok(lines.some((l) => /moved to Not logged in for re-login/.test(l)), lines.join(" | "));
    });
    await check("runner hand-off: login page on viewcart, but /account/orders opens My Orders → SESSION_EXPIRED as before, re-queued as before, nothing written", async () => {
      const { failure, seen, lines } = await landing(mail("k-alive"), "viewcart", "orders");
      assert.equal(failure.code, "SESSION_EXPIRED");
      assert.equal(failure.details, "Session expired: Flipkart showed the login page on viewcart");
      assert.equal(failure.noRetry, false, "an unconfirmed landing is no verdict on the ID: it is re-queued");
      assert.equal(seen.ordersNavigations, 1);
      await assertUntouched(mail("k-alive"));
      assert.ok(lines.some((l) => /not handed off/.test(l)), lines.join(" | "));
    });
    await check("runner hand-off: a session saved 1 h ago → reported with the 24 h note, not moved; the error stays as it was (confirmed: not re-queued)", async () => {
      const { failure } = await landing(mail("k-fresh"), "/account", "login");
      assert.equal(failure.details, PREFLIGHT_LOGIN_MESSAGE);
      assert.equal(failure.noRetry, true);
      const row = await rowOf(mail("k-fresh"));
      assert.equal(row.status, "logged_in");
      assert.equal(row.sessionCheck?.reason, `Checkout: Flipkart sent this ID to the login page (/account, then /account/orders) — re-login${SESSION_NOTES.fresh}`);
      const ev = await eventsOf(mail("k-fresh"));
      assert.equal(ev.length, 1);
      assert.equal(ev[0].after?.demoted, false);
    });
    await browser.close();
    browser = null;

    // ── checkout: no retry on SESSION_EXPIRED ──────────────────────────────
    console.log("\nCHECKOUT — NO RETRY (no Chrome: the job stops at the session check)");
    await check("an invented batch, one job for an e-mail with no Platform ID → the job fails SESSION_EXPIRED and the batch still holds 1 job; nothing queued", async () => {
      const batchId = `verify-dsc-${RUN}`;
      const productUrl = "https://www.flipkart.com/verify-invalid/p/itm0?pid=VERIFY";
      const address = { name: "Verify Invalid", mobile: "", pincode: "000000", locality: "", addressLine1: "verify.invalid", city: "Nowhere", state: "Nowhere", addressType: "Home" as const, gstNumber: "", companyName: "" };
      await CheckoutBatch.create({ batchId, userId: String(USER), systemId: SYSTEM, batchStatus: "running", totalQuantity: 1, quantityPerOrder: 1, totalAttempts: 3, productUrl, paymentMode: "cod", createdBy: "verify" });
      const job = await CheckoutJob.create({
        batchId,
        userId: String(USER),
        systemId: SYSTEM,
        email: mail("k-norow"),
        productUrl,
        quantity: 1,
        totalQuantity: 1,
        quantityPerOrder: 1,
        totalAttempts: 3,
        address,
        status: "queued",
        step: "queued",
        logs: [{ at: new Date(), level: "info", step: "queued", message: "verify: an invented job" }],
      });
      const data: CheckoutJobData = { jobId: String(job._id), batchId, email: mail("k-norow"), productUrl, quantity: 1, totalQuantity: 1, quantityPerOrder: 1, totalAttempts: 3, address, paymentMode: "cod" };
      await runCheckoutJob(data);
      assert.deepEqual(enqueued, [], "something was queued");
      assert.equal(await CheckoutJob.countDocuments({ batchId }), 1, "a retry job was made");
      const after = await CheckoutJob.findById(job._id).select("status errorCode errorDetails logs").lean();
      assert.equal(after?.status, "failed");
      assert.equal(after?.errorCode, "SESSION_EXPIRED");
      assert.equal(after?.errorDetails, "Platform ID not found");
      assert.ok(after?.logs.some((l) => l.message === "SESSION_EXPIRED is a verdict on this platform ID — not re-queued; the batch continues with the next ID"), "no 'not re-queued' line");
      assert.ok(!after?.logs.some((l) => /retry queued/i.test(l.message)));
      const batch = await CheckoutBatch.findOne({ batchId }).select("attemptsUsed purchasedQuantity batchStatus").lean();
      assert.deepEqual({ attemptsUsed: batch?.attemptsUsed, purchasedQuantity: batch?.purchasedQuantity, batchStatus: batch?.batchStatus }, { attemptsUsed: 1, purchasedQuantity: 0, batchStatus: "running" }, "one attempt spent, the reservation given back");
      // That exit used to keep account:lock:<email> for ACCOUNT_LOCK_MS (15 min) — the dashboard's login of this ID waited it out.
      assert.equal(await getRedis().exists(lockKey), 0, "the account lock is still held after the session check");
    });

    // ── cancel ─────────────────────────────────────────────────────────────
    console.log("\nCANCEL, dry run (real runCancelJob; sealed Chromium; no proxy, no API pass, no lock)");
    const cancel = async (email: string, scene: Scene, request: string) => {
      const seen = newSeen();
      await runCancelJob(fakeJob<CancelJobData>({ requestId: request, email }), undefined, cancelDeps(scene, seen));
      assertSealed(seen);
      return seen;
    };

    await insertRow(mail("c-dead"), 30 * DAY);
    await check("the login page twice, nothing done → both units say so and '— moved to Not logged in'; row pending (trade-flow:cancel); one event; one re-check", async () => {
      const request = await cancelRequest(mail("c-dead"), { units: [cancelUnit(mail("c-dead"), 1, 1), cancelUnit(mail("c-dead"), 1, 2)] });
      const seen = await cancel(mail("c-dead"), { orders: "login", details: () => "login" }, request);
      const doc = await unitsOf(request);
      assert.deepEqual(
        doc.units.map((u) => [u.status, u.message]),
        [
          ["failed", `${CANCEL_LOGIN}${MOVED_TO_NOT_LOGGED_IN}`],
          ["failed", `${SESSION_SENTENCE}${MOVED_TO_NOT_LOGGED_IN}`],
        ]
      );
      assert.equal(seen.ordersNavigations, 1, "one re-check per job");
      await assertMoved(mail("c-dead"), "cancel", "Cancel: Flipkart showed the login page twice — re-login", request);
      const rows = await taskLogInStep("cancel", request);
      assert.deepEqual(rows.map((r) => [r.status, r.errorCode]), [["failed", "not_logged_in"], ["failed", "not_logged_in"]]);
    });

    await insertRow(mail("c-urgent"), 30 * DAY);
    await check("urgent cancel: the order page sent to the login page twice → both orders '… — moved to Not logged in'; row pending; one event", async () => {
      const request = await cancelRequest(mail("c-urgent"), { orders: [orderId(2), orderId(3)] });
      const seen = await cancel(mail("c-urgent"), { orders: "login", details: () => "login" }, request);
      const doc = await unitsOf(request);
      assert.deepEqual(doc.orders.map((o) => [o.status, o.message]), [
        ["failed", `${SESSION_SENTENCE}${MOVED_TO_NOT_LOGGED_IN}`],
        ["failed", `${SESSION_SENTENCE}${MOVED_TO_NOT_LOGGED_IN}`],
      ]);
      assert.equal(doc.units.length, 0);
      assert.equal(seen.ordersNavigations, 1);
      await assertMoved(mail("c-urgent"), "cancel", "Cancel: Flipkart showed the login page twice — re-login", request);
      const rows = await taskLogInStep("cancel", request);
      assert.deepEqual(rows.map((r) => [r.status, r.errorCode]), [["failed", "not_logged_in"], ["failed", "not_logged_in"]]);
    });

    await insertRow(mail("c-tabs"), 30 * DAY);
    await check("two tabs meet the login page together → still one re-check and one event; all three units end with the note", async () => {
      await mongoose.connection.collection("order_sync_settings").insertOne({ userId: USER, cancelWindows: 1, cancelTabs: 2, createdAt: new Date(), updatedAt: new Date() });
      try {
        const request = await cancelRequest(mail("c-tabs"), { units: [cancelUnit(mail("c-tabs"), 4, 1), cancelUnit(mail("c-tabs"), 4, 2), cancelUnit(mail("c-tabs"), 4, 3)] });
        const seen = await cancel(mail("c-tabs"), { orders: "login", details: () => "login" }, request);
        const doc = await unitsOf(request);
        assert.ok(doc.units.every((u) => u.status === "failed" && u.message.endsWith(MOVED_TO_NOT_LOGGED_IN)), JSON.stringify(doc.units.map((u) => u.message)));
        assert.ok(doc.units.filter((u) => u.message === `${CANCEL_LOGIN}${MOVED_TO_NOT_LOGGED_IN}`).length >= 1, "no unit kept the page's own words");
        assert.equal(seen.ordersNavigations, 1, "one re-check per job, whatever the tabs");
        await assertMoved(mail("c-tabs"), "cancel", "Cancel: Flipkart showed the login page twice — re-login", request);
        await taskLogInStep("cancel", request);
      } finally {
        await mongoose.connection.collection("order_sync_settings").deleteMany({ userId: USER });
      }
    });

    await insertRow(mail("c-alive"), 30 * DAY);
    await check("Flipkart refused a unit first (not_cancellable), then the login page → no hand-off: row untouched, no event, no re-check", async () => {
      const first = cancelUnit(mail("c-alive"), 5, 1);
      const second = cancelUnit(mail("c-alive"), 5, 2);
      const request = await cancelRequest(mail("c-alive"), { units: [first, second] });
      const seen = await cancel(mail("c-alive"), { orders: "login", details: (unit) => (unit === first.unitId ? "refuse" : "login") }, request);
      const doc = await unitsOf(request);
      assert.deepEqual(doc.units.map((u) => [u.status, u.message]), [
        ["not_cancellable", "Flipkart: Verify: this unit has shipped"],
        ["failed", CANCEL_LOGIN],
      ]);
      assert.equal(seen.ordersNavigations, 0, "re-checked although a unit went through");
      await assertUntouched(mail("c-alive"));
      const rows = await taskLogInStep("cancel", request);
      assert.deepEqual(rows.map((r) => [r.status, r.errorCode]), [["failed", "not_allowed_now"], ["failed", "not_logged_in"]]);
    });

    await insertRow(mail("c-recheck"), 30 * DAY);
    await check("the login page, but the re-check opens My Orders → no hand-off: the unit fails as before, row untouched, no event", async () => {
      const request = await cancelRequest(mail("c-recheck"), { units: [cancelUnit(mail("c-recheck"), 6, 1)] });
      const seen = await cancel(mail("c-recheck"), { orders: "orders", details: () => "login" }, request);
      const doc = await unitsOf(request);
      assert.deepEqual(doc.units.map((u) => [u.status, u.message]), [["failed", CANCEL_LOGIN]]);
      assert.equal(seen.ordersNavigations, 1);
      await assertUntouched(mail("c-recheck"));
      await taskLogInStep("cancel", request);
    });

    // ── reschedule ─────────────────────────────────────────────────────────
    console.log("\nRESCHEDULE (real runRescheduleJob; sealed Chromium; no proxy, no lock)");
    const reschedule = async (email: string, scene: Scene, request: string) => {
      const seen = newSeen();
      await runRescheduleJob(fakeJob<RescheduleJobData>({ requestId: request, email }), undefined, rescheduleDeps(scene, seen));
      assertSealed(seen);
      return seen;
    };

    await insertRow(mail("r-dead"), 30 * DAY);
    await check("the login page twice, nothing done → both units 'Session expired — re-login this ID — moved to Not logged in'; row pending (trade-flow:reschedule); one event", async () => {
      const request = await rescheduleRequest(mail("r-dead"), 7, 2);
      const seen = await reschedule(mail("r-dead"), { orders: "login", details: () => "login" }, request);
      const doc = await rescheduleUnitsOf(request);
      assert.deepEqual(doc.units.map((u) => [u.status, u.message]), [
        ["failed", `${SESSION_SENTENCE}${MOVED_TO_NOT_LOGGED_IN}`],
        ["failed", `${SESSION_SENTENCE}${MOVED_TO_NOT_LOGGED_IN}`],
      ]);
      assert.equal(seen.ordersNavigations, 1);
      await assertMoved(mail("r-dead"), "reschedule", "Reschedule: Flipkart showed the login page twice — re-login", request);
      const rows = await taskLogInStep("reschedule", request);
      assert.deepEqual(rows.map((r) => [r.status, r.errorCode]), [["failed", "not_logged_in"], ["failed", "not_logged_in"]]);
    });

    await insertRow(mail("r-alive"), 30 * DAY);
    await check("Flipkart answered for a unit first (date change not available), then the login page → no hand-off: row untouched, no event", async () => {
      const request = await rescheduleRequest(mail("r-alive"), 8, 2);
      const units = (await rescheduleUnitsOf(request)).units;
      const seen = await reschedule(mail("r-alive"), { orders: "login", details: (unit) => (unit === units[0].unitId ? "page" : "login") }, request);
      const doc = await rescheduleUnitsOf(request);
      assert.deepEqual(doc.units.map((u) => u.status), ["not_available", "failed"]);
      assert.match(doc.units[0].message, /Delivery date can't be changed on this unit/);
      assert.equal(doc.units[1].message, SESSION_SENTENCE);
      assert.equal(seen.ordersNavigations, 0);
      await assertUntouched(mail("r-alive"));
      await taskLogInStep("reschedule", request);
    });

    // ── what the dashboard makes of it ─────────────────────────────────────
    console.log("\nTHE DASHBOARD'S ALERTS (the dashboard API, which must be running, turns each event into an alert)");
    await check("each event became its account's alert, in the event's own words", async () => {
      const until = Date.now() + 90_000;
      while ((await OrderUnitEvent.countDocuments({ userId: USER, processed: { $ne: true } })) > 0 && Date.now() < until) await new Promise((r) => setTimeout(r, 1000));
      const open = await OrderUnitEvent.countDocuments({ userId: USER, processed: { $ne: true } });
      assert.equal(open, 0, `${open} event(s) not taken by the dashboard API within 90 s — is it running?`);
      await new Promise((r) => setTimeout(r, 3000));
      const alerts = await mongoose.connection.collection("alerts").find({ userId: USER, kind: "account.unreadable" }).toArray();
      const events = await OrderUnitEvent.find({ userId: USER, type: "account_unreadable" }).sort({ at: 1, _id: 1 }).lean();
      // Any dashboard API on this database may take an event; one on older code writes the old words.
      const olderWatcher = alerts.filter((a) => /every order page rendered nothing and the order API refused the session/.test(String(a.message)));
      assert.equal(olderWatcher.length, 0, `${olderWatcher.length} alert(s) in the old words — a dashboard API on older code (another machine on this database?) took those events, not this one: ${olderWatcher.map((a) => a.email).join(", ")}`);
      assert.equal(alerts.length, events.length, `${alerts.length} alert(s) for ${events.length} event(s)`);
      for (const e of events) {
        const a = alerts.find((x) => x.email === e.email);
        assert.ok(a, `no alert for ${e.email}`);
        assert.equal(a.message, `${e.email}: ${String(e.after?.reason)}`);
        assert.equal(a.source, `Trade Flow ${String(e.after?.flow)}`);
      }
      console.log(`        e.g. ${alerts[0]?.source} · ${alerts[0]?.message}`);
    });
  } finally {
    await browser?.close().catch(() => undefined);
    await cleanUp(lockKey);
  }
  console.log(`\n${passed} passed, ${failures.length} failed${failures.length ? `: ${failures.join(" · ")}` : ""}`);
  await mongoose.disconnect();
  process.exit(failures.length ? 1 : 0);
}

/**
 * Everything this run wrote, by the invented workspace id. The dashboard API
 * turns events into alerts as they arrive: the events go first, so no new one
 * is taken, then the alerts, then a recount.
 */
async function cleanUp(lockKey: string) {
  const db = mongoose.connection.db!;
  const uid = String(USER);
  const events = (await OrderUnitEvent.deleteMany({ userId: USER })).deletedCount;
  await new Promise((r) => setTimeout(r, 3000));
  // The checkout lock of the invented e-mail: the job releases it itself now (the no-retry check asserts it); deleted here whatever happened.
  const redis = getRedis();
  const lockLeft = await redis.get(lockKey).catch(() => null);
  const lockDeleted = await redis.del(lockKey).catch(() => 0);
  const del = {
    platformids: (await PlatformId.deleteMany({ userId: USER })).deletedCount,
    order_unit_events: events + (await OrderUnitEvent.deleteMany({ userId: USER })).deletedCount,
    alerts: (await db.collection("alerts").deleteMany({ userId: USER })).deletedCount,
    order_cancel_requests: (await CancelRequest.deleteMany({ userId: USER })).deletedCount,
    order_reschedule_requests: (await RescheduleRequest.deleteMany({ userId: USER })).deletedCount,
    order_sync_tasks: (await OrderSyncTask.deleteMany({ $or: [{ userId: USER }, { requestId: { $in: requestIds } }] })).deletedCount,
    order_sync_settings: (await db.collection("order_sync_settings").deleteMany({ userId: USER })).deletedCount,
    session_demotions: (await db.collection("session_demotions").deleteMany({ userId: USER })).deletedCount,
    tradeflowjobs: (await CheckoutJob.deleteMany({ userId: uid })).deletedCount,
    tradeflowbatches: (await CheckoutBatch.deleteMany({ userId: uid })).deletedCount,
    [`redis ${lockKey}`]: lockDeleted,
  };
  await new Promise((r) => setTimeout(r, 2000));
  await db.collection("alerts").deleteMany({ userId: USER });
  fs.rmSync(ARTIFACTS, { recursive: true, force: true });
  const left: Record<string, number> = {};
  for (const name of ["platformids", "order_unit_events", "alerts", "order_cancel_requests", "order_reschedule_requests", "order_sync_tasks", "order_sync_settings", "session_demotions", "order_details", "order_update_requests", "order_job_runs"]) {
    left[name] = await db.collection(name).countDocuments({ userId: USER });
  }
  left.order_sync_tasks += await OrderSyncTask.countDocuments({ requestId: { $in: requestIds } });
  for (const name of ["tradeflowjobs", "tradeflowbatches"]) left[name] = await db.collection(name).countDocuments({ $or: [{ userId: uid }, { userId: USER }] });
  left["*@verify.invalid platformids of this run"] = await PlatformId.countDocuments({ userId: USER, email: /@verify\.invalid$/ });
  left[`redis ${lockKey}`] = await redis.exists(lockKey).catch(() => 0);
  left["screenshot folder"] = fs.existsSync(ARTIFACTS) ? 1 : 0;
  await redis.quit().catch(() => undefined);
  if (lockLeft) console.log(`\nnote: ${lockKey} was still held at the end — deleted`);
  console.log(`\ncleaned up: ${Object.entries(del).map(([k, v]) => `${k} ${v}`).join(" · ")}`);
  console.log(`left behind: ${Object.entries(left).map(([k, v]) => `${k} ${v}`).join(" · ")}${Object.values(left).every((n) => n === 0) ? " — nothing" : " — NOT CLEAN"}`);
  if (!Object.values(left).every((n) => n === 0)) failures.push("clean-up");
}

main().catch(async (err) => {
  console.error(err);
  await cleanUp(`account:lock:${mail("k-norow")}`).catch((e) => console.error("clean-up failed:", e));
  process.exit(1);
});
