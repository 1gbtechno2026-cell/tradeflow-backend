/**
 * The dead-session hand-off (services/sessionEligibility.ts) — WITHOUT
 * Flipkart, without a worker, without a queue:
 *
 *   · markSessionEligibleForRelogin, every branch, on invented Platform IDs
 *     x1..x6@verify.invalid: confirmed on an old session → pending, with
 *     statusBeforeDemote, demotedAt, demotedBy "trade-flow:test" and one event;
 *     a session saved 1 h ago → the 24 h note; the session replaced between the
 *     flow's read and its report → untouched; unconfirmed → only lastChecked +
 *     sessionCheck, and one event a day however often it is reported;
 *     SESSION_DEMOTE_CAP=2 → the third stays logged_in with the cap note, and
 *     an Undo (which clears a row's demotedAt) gives no slot back, and a
 *     demotion made without a slot (an older Trade Flow) still counts; the
 *     circuit open → the circuit note; a row already pending → nothing
 *     written; a missing row → nothing; reports that run at the same moment
 *     never pass the cap together
 *   · resolveLoggedInSession hands over sessionSavedAt; SessionDeadError reads
 *     as "not logged in" in the task log; SESSION_DEMOTE_CAP parsing
 *   · the per-run circuit's numbers: window 50, judged from 20, open above 30 %
 *   · confirmLoginLanding (browser.ts) in plain Chromium in which no host name
 *     resolves and every request is answered here (a login redirect made by
 *     the page's own script, a login page, a My Orders page) or aborted —
 *     nothing can leave the machine (see launchSealed: a fulfilled 302 would)
 *   · F6: a coupon run (rewards API 401; a link sent to /login) and the invoice
 *     batch (a download refused as not logged in) report once per account,
 *     unconfirmed — the row stays logged_in; an HTTP 500 reports nothing; the
 *     task log still closes every task and matches the request's counters
 *   · the dashboard (its backend folder: --dashboard=<dir>, else
 *     DASHBOARD_BACKEND_DIR, else ~/Desktop/data_for_project/
 *     gmail-id-dashboard_mongodb/backend — not found is a FAILED check, never
 *     a skipped one): its Session Health → Undo (dry run) lists the rows
 *     demoted here, and its running API's alert watcher raised the events with
 *     their own words
 *
 * What is real: Mongo — platformids, order_unit_events, alerts,
 * order_coupon_requests / order_coupon_tasks, order_invoice_requests,
 * order_sync_tasks and session_demotions rows of an INVENTED workspace id with
 * @verify.invalid e-mails (each carrying this run's id), all deleted at the end
 * (the script prints what was left). The ledger goes to a throwaway
 * collection, dropped at the end. This process uses the invented workspace id
 * whatever TRADE_FLOW_USER_ID says. Coupons and invoices talk to a fake on
 * 127.0.0.1 only.
 *
 *   npm run test:dead-session
 *   npm run test:dead-session -- --dashboard=/path/to/gmail-id-dashboard_mongodb/backend
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import type { AddressInfo } from "node:net";
import os from "node:os";
import path from "node:path";
import type { Job } from "bullmq";
import mongoose, { Types } from "mongoose";
import { chromium, request as playwrightRequest, type Browser, type Page, type Route } from "playwright";
import { config } from "../config.js";
import { OrderCouponRequest, OrderCouponTask } from "../models/OrderCouponRequest.js";
import { OrderInvoiceRequest } from "../models/OrderInvoiceRequest.js";
import { OrderJobRun, type IOrderJobRun } from "../models/OrderJobRun.js";
import { OrderSyncTask } from "../models/OrderSyncTask.js";
import { OrderUnitEvent } from "../models/OrderUnitEvent.js";
import { PlatformId } from "../models/PlatformId.js";
import type { CouponJobData, InvoiceBatchJobData } from "../queue.js";
import { confirmLoginLanding } from "../services/browser.js";
import { runCouponJob, type CouponDeps } from "../services/couponRequests.js";
import { runInvoiceBatchJob, type InvoiceBatchDeps } from "../services/invoiceRequests.js";
import { OrderApiError, OrderApiSessionError } from "../services/orderApi.js";
import { classifyMessage, emptyCounts } from "../services/requestCommon.js";
import {
  CIRCUIT_DEAD_SHARE,
  CIRCUIT_JUDGED_FROM,
  CIRCUIT_WINDOW,
  DemoteCircuit,
  demoteCircuitFor,
  forgetDemoteCircuit,
  markSessionEligibleForRelogin,
  recentTradeFlowDemotions,
  SESSION_DEAD_MESSAGE,
  SESSION_NOTES,
  SessionDeadError,
  sessionDemoteCap,
} from "../services/sessionEligibility.js";
import { NO_SAVED_COOKIES, resolveLoggedInSession } from "../services/sessionStore.js";
import { logCounts, openRequestTasksQuietly, taskLog } from "../services/syncTaskLog.js";

/** The dashboard's backend folder: --dashboard=<dir>, else DASHBOARD_BACKEND_DIR, else where it lives on the owner's Macs. */
const DASHBOARD =
  (process.argv.find((a) => a.startsWith("--dashboard=")) || "").split("=").slice(1).join("=") ||
  process.env.DASHBOARD_BACKEND_DIR ||
  path.join(os.homedir(), "Desktop", "data_for_project", "gmail-id-dashboard_mongodb", "backend");
const DASHBOARD_FOUND = fs.existsSync(path.join(DASHBOARD, "src", "scripts", "deadSessionCheck.ts"));

const USER = new Types.ObjectId();
const RUN = Date.now().toString(36);
const MIN = 60_000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;
const ago = (ms: number) => new Date(Date.now() - ms);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const near = (d: unknown, ms = 2 * MIN) => d instanceof Date && Math.abs(Date.now() - d.getTime()) < ms;
/** What a dashboard API from before F7 writes on every account.unreadable alert, whatever the event says. */
const OLD_ALERT_WORDS = /every order page rendered nothing and the order API refused the session/;
/** The run in every e-mail: the dashboard folds an event into any open alert of the same e-mail and day, so two runs at once (another Mac, a re-run) must not share one. */
const mail = (name: string) => `${name}-${RUN}@verify.invalid`;

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

// ── invented Platform IDs ───────────────────────────────────────────────────

/**
 * Invented cookies under Flipkart's auth names, so restoreFlipkartSession's
 * attach check passes. Nothing ever sends them: every page request here is
 * answered by this file or aborted, and the HTTP calls go to 127.0.0.1.
 */
const FAKE_COOKIES = ["T", "SN", "at"].map((name) => ({ name, value: `verify-invalid-${name}`, domain: ".flipkart.com", path: "/", httpOnly: true, secure: true, sameSite: "Lax" }));

async function insertRow(email: string, savedAgoMs: number | null, opts: { status?: string; cookies?: boolean } = {}) {
  const _id = new Types.ObjectId();
  const cookies = opts.cookies === false ? [] : FAKE_COOKIES;
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
    status: opts.status || "logged_in",
    lastError: "",
    retryable: true,
    sessionCookies: cookies,
    sessionState: { cookies, origins: [] },
    sessionSavedAt: savedAgoMs == null ? null : ago(savedAgoMs),
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
  updatedAt: Date;
};

/** The fields under test — never the cookies. */
async function rowOf(email: string): Promise<RowView> {
  const row = await PlatformId.findOne({ userId: USER, email }).select("email status sessionSavedAt statusBeforeDemote demotedAt demotedBy lastChecked sessionCheck updatedAt").lean();
  assert.ok(row, `no row ${email}`);
  return row as unknown as RowView;
}

const eventsOf = (email: string) => OrderUnitEvent.find({ userId: USER, email, type: "account_unreadable" }).sort({ at: 1, _id: 1 }).lean();

async function session(email: string) {
  const s = await resolveLoggedInSession(email);
  assert.ok(s.ok, `${email}: ${s.ok ? "" : s.reason}`);
  return s;
}

// ── a fake Flipkart for the page (Chromium, routed) ─────────────────────────

const LOGIN_URL = "https://www.flipkart.com/login?ret=%2faccount%2forders";

/**
 * Plain Chromium in which no host name resolves: a request the route below does
 * not answer fails on this machine (net::ERR_NAME_NOT_RESOLVED) instead of
 * reaching anyone. That matters because a route.fulfill() 302 is followed
 * OUTSIDE the route — Chromium sends the redirect target to the network — so
 * the login redirect here is the page's own script (location.replace), a new
 * navigation that the route does answer; page.goto still resolves on the login
 * URL, as with Flipkart's server redirect.
 */
const launchSealed = () => chromium.launch({ headless: true, args: ["--host-resolver-rules=MAP * ~NOTFOUND"] });

async function routedPage(browser: Browser, orders: "login" | "orders") {
  const context = await browser.newContext();
  const seen = { ordersNavigations: 0, aborted: 0, unanswered: [] as string[] };
  await context.route("**/*", async (route: Route) => {
    const req = route.request();
    const url = new URL(req.url());
    if (url.hostname === "www.flipkart.com" && url.pathname.startsWith("/account/orders")) {
      if (req.isNavigationRequest()) seen.ordersNavigations++;
      if (orders === "login") return route.fulfill({ status: 200, contentType: "text/html", body: `<html><head><script>location.replace(${JSON.stringify(LOGIN_URL)})</script></head><body></body></html>` });
      return route.fulfill({ status: 200, contentType: "text/html", body: "<html><head><title>My Orders</title></head><body><a href='/order_details?order_id=OD1'>order_id=OD1</a> Price details</body></html>" });
    }
    if (url.hostname === "www.flipkart.com" && url.pathname === "/login") {
      return route.fulfill({ status: 200, contentType: "text/html", body: "<html><head><title>Login</title></head><body>Login</body></html>" });
    }
    seen.aborted++;
    return route.abort();
  });
  const page: Page = await context.newPage();
  // Anything that went round the route: the sinkhole stopped it, and the check fails.
  page.on("requestfailed", (r) => {
    if (/ERR_NAME_NOT_RESOLVED/.test(r.failure()?.errorText || "")) seen.unanswered.push(r.url());
  });
  return { context, page, seen };
}

// ── a fake Flipkart for the HTTP flows (127.0.0.1) ──────────────────────────

function startFake(): Promise<{ origin: string; close: () => Promise<void>; hits: string[] }> {
  const hits: string[] = [];
  const server = http.createServer((req, res) => {
    const path = (req.url || "/").split("?")[0];
    hits.push(path);
    const send = (status: number, body: string, headers: Record<string, string> = {}) => {
      res.writeHead(status, { "content-type": "application/json", ...headers });
      res.end(body);
    };
    if (path === "/rewards401") return send(401, JSON.stringify({ STATUS_CODE: 401 }));
    if (path === "/rewards500") return send(500, JSON.stringify({ STATUS_CODE: 500 }));
    if (path === "/rewards200") return send(200, JSON.stringify({ RESPONSE: { liveOffers: [] } }));
    if (path === "/coupon/bounce") return send(302, "", { location: "/account/login?ret=%2fcoupon%2fbounce" });
    if (path === "/coupon/down") return send(500, "<html><body>Something went wrong</body></html>", { "content-type": "text/html" });
    if (path === "/account/login") return send(200, "<html><body>Login</body></html>", { "content-type": "text/html" });
    return send(404, "{}");
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address() as AddressInfo;
      resolve({ origin: `http://127.0.0.1:${port}`, hits, close: () => new Promise((r) => server.close(() => r())) });
    });
  });
}

const fakeJob = <T>(data: T) =>
  ({
    data,
    moveToDelayed: async () => {
      throw new Error("verify: there is no queue here");
    },
  }) as unknown as Job<T>;

const requestIds: string[] = [];

async function couponRequest(email: string, accountId: string, links: string[]): Promise<string> {
  const request = await OrderCouponRequest.create({
    userId: USER,
    createdBy: "verify",
    status: "queued",
    links,
    params: { coupon_links_count: links.length, email_ids_count: 1, selection_method: "email_ids", accounts_count: 1 },
    counts: emptyCounts(1),
    queue: "verify-ds-none",
    systemId: "verify-ds",
    host: "verify-ds",
  });
  const task = await OrderCouponTask.create({ requestId: request._id, userId: USER, email, accountId, status: "queued" });
  await openRequestTasksQuietly("coupons", request.toObject(), { couponTasks: [task.toObject()] });
  requestIds.push(String(request._id));
  return String(request._id);
}

async function invoiceRequest(email: string, accountId: string, orderIds: string[]): Promise<string> {
  const request = await OrderInvoiceRequest.create({
    userId: USER,
    createdBy: "verify",
    status: "queued",
    params: { order_ids_count: orderIds.length, force: true },
    tasks: orderIds.map((orderId) => ({ orderId, email, accountId, status: "queued" })),
    counts: emptyCounts(orderIds.length),
    queue: "verify-ds-none",
    systemId: "verify-ds",
    host: "verify-ds",
  });
  await openRequestTasksQuietly("invoices", request.toObject());
  requestIds.push(String(request._id));
  return String(request._id);
}

/** The request's own counters equal its task rows, and no row is left open. */
async function taskLogInStep(kind: "coupons" | "invoices", requestId: string) {
  await taskLog.flush();
  const doc = kind === "coupons" ? await OrderCouponRequest.findById(requestId).select("status counts").lean() : await OrderInvoiceRequest.findById(requestId).select("status counts").lean();
  assert.ok(doc, "request gone");
  assert.equal(doc.status, "completed");
  const rows = await logCounts(requestId);
  assert.deepEqual({ ...emptyCounts(0), ...rows }, { ...emptyCounts(0), ...(doc.counts as object) }, "task rows ≠ the request's counters");
  assert.equal(rows.queued + rows.in_progress, 0, "a task row is still open");
}

// ── the run ─────────────────────────────────────────────────────────────────

async function main() {
  // This process works for the invented workspace only, whatever .env says.
  config.userId = String(USER);
  process.env.TRADE_FLOW_USER_ID = String(USER);
  process.env.SESSION_DEMOTE_CAP = "10";
  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false });
  const Ledger = mongoose.model<IOrderJobRun>(`VerifyDsLedger${RUN}`, OrderJobRun.schema, `verify_ds_${RUN}_ledger`);
  console.log(`invented workspace ${USER} · rows x1..x6, c1..c3, i1..i2 @verify.invalid · ledger verify_ds_${RUN}_ledger\n`);

  const reason = "Test: Flipkart sent /account/orders to the login page twice — re-login";
  let browser: Browser | null = null;
  let fake: Awaited<ReturnType<typeof startFake>> | null = null;
  try {
    // ── pure ────────────────────────────────────────────────────────────────
    console.log("PURE");
    await check("SessionDeadError: its default sentence reads as 'not logged in' in the task log", () => {
      const err = new SessionDeadError();
      assert.ok(err instanceof Error);
      assert.equal(err.name, "SessionDeadError");
      assert.equal(err.message, SESSION_DEAD_MESSAGE);
      assert.equal(classifyMessage(err.message).errorClass, "not_logged_in");
      assert.equal(new SessionDeadError("Session expired — x").message, "Session expired — x");
    });
    await check("SESSION_DEMOTE_CAP: default 10, a number, 0 = never; empty or junk = the default", () => {
      const keep = process.env.SESSION_DEMOTE_CAP;
      try {
        for (const [raw, want] of [[undefined, 10], ["", 10], ["2", 2], ["0", 0], ["abc", 10], ["-3", 10], ["7.9", 7]] as Array<[string | undefined, number]>) {
          if (raw === undefined) delete process.env.SESSION_DEMOTE_CAP;
          else process.env.SESSION_DEMOTE_CAP = raw;
          assert.equal(sessionDemoteCap(), want, `SESSION_DEMOTE_CAP=${raw}`);
        }
      } finally {
        process.env.SESSION_DEMOTE_CAP = keep;
      }
    });
    await check(`circuit: ${CIRCUIT_WINDOW} wide, judged from ${CIRCUIT_JUDGED_FROM}, open above ${CIRCUIT_DEAD_SHARE * 100} % dead`, () => {
      assert.deepEqual([CIRCUIT_WINDOW, CIRCUIT_JUDGED_FROM, CIRCUIT_DEAD_SHARE], [50, 20, 0.3]);
      // 19 dead is not judged yet; the 20th is.
      const a = new DemoteCircuit();
      for (let i = 0; i < 19; i++) assert.equal(a.push("dead"), false);
      assert.equal(a.push("dead"), true);
      assert.equal(a.open, true);
      // 6 of 20 = 30 % stays closed; 7 of 21 = 33 % opens.
      const b = new DemoteCircuit();
      for (let i = 0; i < 14; i++) b.push("alive");
      for (let i = 0; i < 6; i++) b.push("dead");
      assert.equal(b.open, false, b.describe());
      assert.equal(b.push("dead"), true, b.describe());
      assert.equal(b.describe(), "7 of the last 21 accounts dead (33 %) — circuit open");
      // 'other' counts in the window, not as dead.
      const c = new DemoteCircuit();
      for (let i = 0; i < 14; i++) c.push("other");
      for (let i = 0; i < 6; i++) c.push("dead");
      assert.equal(c.open, false);
      // The window is the last 50: 50 alive then 15 dead = 15 of 50 (30 %) closed, the 16th opens.
      const d = new DemoteCircuit();
      for (let i = 0; i < 50; i++) d.push("alive");
      for (let i = 0; i < 15; i++) d.push("dead");
      assert.equal(d.size, 50);
      assert.equal(d.dead, 15);
      assert.equal(d.open, false, d.describe());
      assert.equal(d.push("dead"), true, d.describe());
      // Once open it stays open for the run.
      for (let i = 0; i < 100; i++) d.push("alive");
      assert.equal(d.open, true);
      assert.equal(d.dead, 0);
      // One circuit per run key.
      const k = `verify-${RUN}`;
      assert.equal(demoteCircuitFor(k), demoteCircuitFor(k));
      demoteCircuitFor(k).push("dead");
      forgetDemoteCircuit(k);
      assert.equal(demoteCircuitFor(k).size, 0);
      forgetDemoteCircuit(k);
    });

    // ── the page re-check ──────────────────────────────────────────────────
    console.log("\nCONFIRM LOGIN LANDING (Chromium: no host name resolves; every request answered here or aborted)");
    browser = await launchSealed();
    await check("the sealed browser: a request the route does not answer cannot leave the machine", async () => {
      const context = await browser!.newContext();
      try {
        const page = await context.newPage();
        await assert.rejects(page.goto("https://www.example.invalid/", { timeout: 15_000 }), /ERR_NAME_NOT_RESOLVED/);
      } finally {
        await context.close();
      }
    });
    await check("a login redirect with the cookies attached again → true, after exactly one /account/orders navigation", async () => {
      const { context, page, seen } = await routedPage(browser!, "login");
      try {
        const started = Date.now();
        assert.equal(await confirmLoginLanding(page, FAKE_COOKIES), true);
        assert.ok(Date.now() - started >= 2500, "the 2.5 s wait");
        assert.equal(seen.ordersNavigations, 1);
        assert.match(page.url(), /^https:\/\/www\.flipkart\.com\/login\?ret=/);
        const names = (await context.cookies("https://www.flipkart.com")).map((c) => c.name).sort();
        assert.deepEqual(names, ["SN", "T", "at"], "only the invented cookies — nothing came back from a real server");
        assert.deepEqual(seen.unanswered, []);
      } finally {
        await context.close();
      }
    });
    await check("My Orders renders → false", async () => {
      const { context, page, seen } = await routedPage(browser!, "orders");
      try {
        assert.equal(await confirmLoginLanding(page, FAKE_COOKIES), false);
        assert.equal(seen.ordersNavigations, 1);
        assert.deepEqual(seen.unanswered, []);
      } finally {
        await context.close();
      }
    });
    await check("no cookies to attach → it throws (a caller reads that as 'not confirmed')", async () => {
      const { context, page, seen } = await routedPage(browser!, "login");
      try {
        await assert.rejects(confirmLoginLanding(page, []), /no usable cookies/);
        assert.equal(seen.ordersNavigations, 0);
      } finally {
        await context.close();
      }
    });

    // ── the decision ───────────────────────────────────────────────────────
    console.log("\nMARK SESSION ELIGIBLE FOR RE-LOGIN (invented rows)");
    await insertRow(mail("x1"), 30 * DAY);
    await insertRow(mail("x2"), 1 * HOUR);
    await insertRow(mail("x3"), 30 * DAY);
    await insertRow(mail("x4"), 30 * DAY);
    await insertRow(mail("x5"), 30 * DAY);
    await insertRow(mail("x6"), 30 * DAY);
    await insertRow(mail("nocookies"), 30 * DAY, { cookies: false });

    await check("resolveLoggedInSession hands over sessionSavedAt (and a refusal carries it)", async () => {
      const s = await session(mail("x1"));
      const row = await rowOf(mail("x1"));
      assert.ok(s.sessionSavedAt instanceof Date);
      assert.equal(s.sessionSavedAt.getTime(), row.sessionSavedAt!.getTime());
      const none = await resolveLoggedInSession(mail("nocookies"));
      assert.equal(none.ok, false);
      assert.equal(!none.ok && none.reason, NO_SAVED_COOKIES);
      assert.equal(!none.ok && none.sessionSavedAt?.getTime(), (await rowOf(mail("nocookies"))).sessionSavedAt!.getTime());
    });

    await check("confirmed, session saved 30 days ago → pending, statusBeforeDemote, demotedAt, demotedBy trade-flow:test, one event", async () => {
      const s = await session(mail("x1"));
      const out = await markSessionEligibleForRelogin({ userId: USER, platformId: s.platformId, email: s.email, reason, flow: "test", confirmed: true, sessionSavedAt: s.sessionSavedAt, via: "worker", requestId: `verify-${RUN}` });
      assert.deepEqual(out, { demoted: true, note: "" });
      const row = await rowOf(mail("x1"));
      assert.equal(row.status, "pending");
      assert.equal(row.statusBeforeDemote, "logged_in");
      assert.ok(near(row.demotedAt), "demotedAt");
      assert.equal(row.demotedBy, "trade-flow:test");
      assert.ok(near(row.lastChecked), "lastChecked");
      assert.equal(row.sessionCheck?.outcome, "not_logged_in");
      assert.equal(row.sessionCheck?.reason, reason);
      assert.equal(row.sessionCheck?.via, "worker");
      assert.equal(row.sessionSavedAt?.getTime(), s.sessionSavedAt.getTime(), "the session itself is not touched");
      const ev = await eventsOf(mail("x1"));
      assert.equal(ev.length, 1);
      assert.deepEqual(ev[0].after, { reason, via: "worker", flow: "test", demoted: true, needsRelogin: true });
      assert.equal(ev[0].requestId, `verify-${RUN}`);
      assert.equal(ev[0].unitId, "");
      assert.equal(await PlatformId.countDocuments({ _id: s.platformId, $or: [{ "sessionState.cookies.0": { $exists: true } }, { "sessionCookies.0": { $exists: true } }] }), 1, "cookies kept");
    });

    await check("confirmed, session saved 1 h ago → stays logged_in with the 24 h note (event kept, sessionCheck.via from 'page' → worker)", async () => {
      const s = await session(mail("x2"));
      const out = await markSessionEligibleForRelogin({ userId: USER, platformId: s.platformId, email: s.email, reason, flow: "test", confirmed: true, sessionSavedAt: s.sessionSavedAt, via: "page" });
      assert.deepEqual(out, { demoted: false, note: SESSION_NOTES.fresh });
      const row = await rowOf(mail("x2"));
      assert.equal(row.status, "logged_in");
      assert.equal(row.sessionCheck?.reason, `${reason}${SESSION_NOTES.fresh}`);
      assert.equal(row.sessionCheck?.via, "worker");
      assert.ok(near(row.lastChecked));
      assert.equal(row.statusBeforeDemote, "");
      assert.equal(row.demotedAt, null);
      assert.equal(row.demotedBy, "");
      const ev = await eventsOf(mail("x2"));
      assert.equal(ev.length, 1);
      assert.deepEqual(ev[0].after, { reason: `${reason}${SESSION_NOTES.fresh}`, via: "page", flow: "test", demoted: false, needsRelogin: true });
    });

    await check("the session was replaced between the flow's read and its report → untouched, no event", async () => {
      const s = await session(mail("x3"));
      // A login lands after the flow read the row.
      await PlatformId.collection.updateOne({ userId: USER, email: mail("x3") }, { $set: { sessionSavedAt: ago(2 * DAY) } });
      const before = await rowOf(mail("x3"));
      const out = await markSessionEligibleForRelogin({ userId: USER, platformId: s.platformId, email: s.email, reason, flow: "test", confirmed: true, sessionSavedAt: s.sessionSavedAt });
      assert.deepEqual(out, { demoted: false, note: SESSION_NOTES.replaced });
      const after = await rowOf(mail("x3"));
      assert.deepEqual(after, before);
      assert.equal((await eventsOf(mail("x3"))).length, 0);
    });

    await check("unconfirmed → only lastChecked + sessionCheck (via manual: … → manual); asked again → sessionCheck again, still one event", async () => {
      const s = await session(mail("x4"));
      const out = await markSessionEligibleForRelogin({ userId: USER, email: s.email, reason, flow: "test", confirmed: false, sessionSavedAt: s.sessionSavedAt, via: "manual: verify" });
      assert.deepEqual(out, { demoted: false, note: SESSION_NOTES.unconfirmed });
      const row = await rowOf(mail("x4"));
      assert.equal(row.status, "logged_in");
      assert.equal(row.sessionCheck?.outcome, "not_logged_in");
      assert.equal(row.sessionCheck?.reason, `${reason}${SESSION_NOTES.unconfirmed}`);
      assert.equal(row.sessionCheck?.via, "manual");
      assert.ok(near(row.lastChecked));
      assert.equal(row.statusBeforeDemote, "");
      assert.equal(row.demotedAt, null);
      let ev = await eventsOf(mail("x4"));
      assert.equal(ev.length, 1);
      assert.deepEqual(ev[0].after, { reason: `${reason}${SESSION_NOTES.unconfirmed}`, via: "manual: verify", flow: "test", demoted: false, needsRelogin: true });
      await sleep(20);
      const again = await markSessionEligibleForRelogin({ userId: USER, email: s.email, reason: "Test: a second run", flow: "test", confirmed: false, sessionSavedAt: s.sessionSavedAt });
      assert.deepEqual(again, { demoted: false, note: SESSION_NOTES.unconfirmed });
      const row2 = await rowOf(mail("x4"));
      assert.equal(row2.sessionCheck?.reason, `Test: a second run${SESSION_NOTES.unconfirmed}`);
      assert.ok(row2.sessionCheck!.at.getTime() > row.sessionCheck!.at.getTime());
      ev = await eventsOf(mail("x4"));
      assert.equal(ev.length, 1, "an unconfirmed report raises one event a day");
    });

    await check("SESSION_DEMOTE_CAP=2 with 3 rows: x1 and x5 demoted, the third (x6) stays logged_in with the cap note", async () => {
      process.env.SESSION_DEMOTE_CAP = "2";
      try {
        assert.equal(await recentTradeFlowDemotions(USER), 1);
        const s5 = await session(mail("x5"));
        assert.deepEqual(await markSessionEligibleForRelogin({ userId: USER, platformId: s5.platformId, email: s5.email, reason, flow: "test", confirmed: true, sessionSavedAt: s5.sessionSavedAt }), { demoted: true, note: "" });
        assert.equal(await recentTradeFlowDemotions(USER), 2);
        const s6 = await session(mail("x6"));
        const out = await markSessionEligibleForRelogin({ userId: USER, platformId: s6.platformId, email: s6.email, reason, flow: "test", confirmed: true, sessionSavedAt: s6.sessionSavedAt });
        assert.deepEqual(out, { demoted: false, note: SESSION_NOTES.cap });
        const row = await rowOf(mail("x6"));
        assert.equal(row.status, "logged_in");
        assert.equal(row.sessionCheck?.reason, `${reason}${SESSION_NOTES.cap}`);
        assert.equal(row.demotedAt, null);
        assert.equal((await rowOf(mail("x5"))).status, "pending");
        const ev = await eventsOf(mail("x6"));
        assert.equal(ev.length, 1, "a confirmed report always raises its event");
        assert.equal(ev[0].after?.demoted, false);
        assert.equal(await recentTradeFlowDemotions(USER), 2);
      } finally {
        process.env.SESSION_DEMOTE_CAP = "10";
      }
    });

    await check("an Undo (or a login) clears x5's demotedAt, not its slot: with the cap at 2, x7 still stays logged_in with the cap note", async () => {
      process.env.SESSION_DEMOTE_CAP = "2";
      const x5 = await PlatformId.collection.findOne({ userId: USER, email: mail("x5") }, { projection: { status: 1, statusBeforeDemote: 1, demotedAt: 1, demotedBy: 1 } });
      assert.ok(x5);
      try {
        // What Session Health → Undo writes (sessionVerifyHttp.ts undoDemotions), and a login (saveFlipkartSession) clears the same.
        await PlatformId.collection.updateOne({ _id: x5._id }, { $set: { status: "logged_in", demotedAt: null, demotedBy: "", statusBeforeDemote: "" } });
        assert.equal(await PlatformId.countDocuments({ userId: USER, demotedBy: /^trade-flow:/, demotedAt: { $gte: ago(6 * HOUR) } }), 1, "one row still carries the stamp");
        assert.equal(await recentTradeFlowDemotions(USER), 2, "the demotion still happened");
        await insertRow(mail("x7"), 30 * DAY);
        const s7 = await session(mail("x7"));
        const out = await markSessionEligibleForRelogin({ userId: USER, platformId: s7.platformId, email: s7.email, reason, flow: "test", confirmed: true, sessionSavedAt: s7.sessionSavedAt });
        assert.deepEqual(out, { demoted: false, note: SESSION_NOTES.cap });
        assert.equal((await rowOf(mail("x7"))).status, "logged_in");
        assert.equal(await recentTradeFlowDemotions(USER), 2);
      } finally {
        // x5 back as it was: the dashboard's Undo listing below expects it.
        await PlatformId.collection.updateOne({ _id: x5._id }, { $set: { status: x5.status, statusBeforeDemote: x5.statusBeforeDemote, demotedAt: x5.demotedAt, demotedBy: x5.demotedBy } });
        process.env.SESSION_DEMOTE_CAP = "10";
      }
    });

    await check("a demotion made without a slot (a worker not restarted since) still counts: with 2 slots and 3 stamped rows, the cap at 3 refuses x8", async () => {
      // z1: demoted by a Trade Flow that records no slots — only its row's stamp shows it.
      const z1 = await insertRow(mail("z1"), 30 * DAY, { status: "pending" });
      await PlatformId.collection.updateOne({ _id: z1 }, { $set: { statusBeforeDemote: "logged_in", demotedAt: new Date(), demotedBy: "trade-flow:update" } });
      process.env.SESSION_DEMOTE_CAP = "3";
      try {
        assert.equal(await recentTradeFlowDemotions(USER), 2, "two slots");
        await insertRow(mail("x8"), 30 * DAY);
        const s8 = await session(mail("x8"));
        const out = await markSessionEligibleForRelogin({ userId: USER, platformId: s8.platformId, email: s8.email, reason, flow: "test", confirmed: true, sessionSavedAt: s8.sessionSavedAt });
        assert.deepEqual(out, { demoted: false, note: SESSION_NOTES.cap });
        assert.equal((await rowOf(mail("x8"))).status, "logged_in");
        assert.equal(await recentTradeFlowDemotions(USER), 2);
      } finally {
        // Gone again: the dashboard's Undo listing below expects x1 and x5 only.
        await PlatformId.collection.deleteOne({ _id: z1 });
        process.env.SESSION_DEMOTE_CAP = "10";
      }
    });

    await check("circuit open (cap back at 10) → x6 stays logged_in with the circuit note", async () => {
      const s6 = await session(mail("x6"));
      const out = await markSessionEligibleForRelogin({ userId: USER, platformId: s6.platformId, email: s6.email, reason, flow: "test", confirmed: true, sessionSavedAt: s6.sessionSavedAt, circuitOpen: true });
      assert.deepEqual(out, { demoted: false, note: SESSION_NOTES.circuit });
      const row = await rowOf(mail("x6"));
      assert.equal(row.status, "logged_in");
      assert.equal(row.sessionCheck?.reason, `${reason}${SESSION_NOTES.circuit}`);
      assert.equal((await eventsOf(mail("x6"))).length, 2);
    });

    await check("a row already pending (x1) → nothing written, no event; a missing row → nothing", async () => {
      const before = await rowOf(mail("x1"));
      const out = await markSessionEligibleForRelogin({ userId: USER, email: mail("x1"), reason: "Test: again", flow: "test", confirmed: true, sessionSavedAt: before.sessionSavedAt });
      assert.deepEqual(out, { demoted: false, note: SESSION_NOTES.notLoggedIn("pending") });
      assert.deepEqual(await rowOf(mail("x1")), before);
      assert.equal((await eventsOf(mail("x1"))).length, 1);
      const none = await markSessionEligibleForRelogin({ userId: USER, platformId: new Types.ObjectId(), email: mail("nobody"), reason, flow: "test", confirmed: true, sessionSavedAt: null });
      assert.deepEqual(none, { demoted: false, note: SESSION_NOTES.notFound });
      assert.equal((await eventsOf(mail("nobody"))).length, 0);
      assert.equal(await recentTradeFlowDemotions(USER), 2);
    });

    // ── F6: the HTTP-only flows report, never demote ───────────────────────
    console.log("\nCOUPONS + INVOICE BATCH (HTTP to 127.0.0.1 only)");
    fake = await startFake();
    const origin = fake.origin;
    const ids = {
      c1: String(await insertRow(mail("c1"), 30 * DAY)),
      c2: String(await insertRow(mail("c2"), 30 * DAY)),
      c3: String(await insertRow(mail("c3"), 30 * DAY)),
      i1: String(await insertRow(mail("i1"), 30 * DAY)),
      i2: String(await insertRow(mail("i2"), 30 * DAY)),
    };
    const couponDeps = (rewards: string): Partial<CouponDeps> => ({
      lock: async () => async () => undefined,
      ledger: Ledger,
      queueName: "verify-ds-none",
      verify: true,
      verifyPauseMs: 0,
      linkPauseMs: 0,
      accountPauseMs: 0,
      lockRetryMs: 1000,
      rewardsUrl: () => `${origin}${rewards}`,
      resolveUrl: (link) => `${origin}${new URL(link).pathname}`,
      allowHost: (host) => host === "127.0.0.1",
    });
    const invoiceDeps = (email: string, accountId: string, fail: Error): Partial<InvoiceBatchDeps> => ({
      resolveOrders: async (userId, orderIds) => new Map(orderIds.map((orderId) => [orderId, { userId, accountId, email, orderId, unitIds: [], rowIds: [] }])),
      openSession: async () => playwrightRequest.newContext(),
      download: async () => {
        throw fail;
      },
      storeReady: () => true,
      ledger: Ledger,
      queueName: "verify-ds-none",
      pauseMs: 0,
    });

    await check("coupons, the rewards API answers 401 → task not_logged_in; row stays logged_in, sessionCheck '[unconfirmed', one event (flow coupon)", async () => {
      const requestId = await couponRequest(mail("c1"), ids.c1, ["https://www.flipkart.com/coupon/bounce"]);
      await runCouponJob(fakeJob<CouponJobData>({ requestId, email: mail("c1") }), undefined, couponDeps("/rewards401"));
      const task = await OrderCouponTask.findOne({ requestId, email: mail("c1") }).lean();
      assert.equal(task?.status, "failed");
      assert.equal(task?.errorClass, "not_logged_in");
      const row = await rowOf(mail("c1"));
      assert.equal(row.status, "logged_in");
      assert.equal(row.sessionCheck?.outcome, "not_logged_in");
      assert.equal(row.sessionCheck?.reason, `Coupons: the coupons API answered HTTP 401 — the session is not logged in${SESSION_NOTES.unconfirmed}`);
      assert.ok(near(row.lastChecked));
      assert.equal(row.demotedAt, null);
      const ev = await eventsOf(mail("c1"));
      assert.equal(ev.length, 1);
      assert.equal(ev[0].after?.flow, "coupon");
      assert.equal(ev[0].after?.demoted, false);
      assert.equal(ev[0].requestId, requestId);
      await taskLogInStep("coupons", requestId);
    });

    await check("coupons, the same account in a second run → sessionCheck again, still one event", async () => {
      const first = (await rowOf(mail("c1"))).sessionCheck!.at;
      const requestId = await couponRequest(mail("c1"), ids.c1, ["https://www.flipkart.com/coupon/bounce"]);
      await runCouponJob(fakeJob<CouponJobData>({ requestId, email: mail("c1") }), undefined, couponDeps("/rewards401"));
      assert.ok((await rowOf(mail("c1"))).sessionCheck!.at.getTime() > first.getTime());
      assert.equal((await eventsOf(mail("c1"))).length, 1);
      await taskLogInStep("coupons", requestId);
    });

    await check("coupons, a link sent to /login → reported once (the next link is not tried); row stays logged_in", async () => {
      const requestId = await couponRequest(mail("c2"), ids.c2, ["https://www.flipkart.com/coupon/bounce", "https://www.flipkart.com/coupon/down"]);
      await runCouponJob(fakeJob<CouponJobData>({ requestId, email: mail("c2") }), undefined, couponDeps("/rewards200"));
      const task = await OrderCouponTask.findOne({ requestId, email: mail("c2") }).lean();
      assert.equal(task?.errorClass, "not_logged_in");
      assert.equal(task?.results[0]?.loginBounce, true);
      assert.equal(task?.results[1]?.outcome, "skipped");
      const row = await rowOf(mail("c2"));
      assert.equal(row.status, "logged_in");
      assert.equal(row.sessionCheck?.reason, `Coupons: a coupon link sent this session to Flipkart's login page${SESSION_NOTES.unconfirmed}`);
      const ev = await eventsOf(mail("c2"));
      assert.equal(ev.length, 1);
      assert.equal(ev[0].after?.flow, "coupon");
      await taskLogInStep("coupons", requestId);
    });

    await check("coupons, HTTP 500 everywhere → the task fails platform_unavailable and nothing is reported", async () => {
      const requestId = await couponRequest(mail("c3"), ids.c3, ["https://www.flipkart.com/coupon/down"]);
      await runCouponJob(fakeJob<CouponJobData>({ requestId, email: mail("c3") }), undefined, couponDeps("/rewards500"));
      const task = await OrderCouponTask.findOne({ requestId, email: mail("c3") }).lean();
      assert.equal(task?.status, "failed");
      assert.equal(task?.errorClass, "platform_unavailable");
      const row = await rowOf(mail("c3"));
      assert.equal(row.sessionCheck, null);
      assert.equal(row.lastChecked, null);
      assert.equal((await eventsOf(mail("c3"))).length, 0);
      await taskLogInStep("coupons", requestId);
    });

    await check("invoice batch, the download is refused as not logged in → both orders failed, reported once, row stays logged_in", async () => {
      const requestId = await invoiceRequest(mail("i1"), ids.i1, ["ODV-I1-1", "ODV-I1-2"]);
      await runInvoiceBatchJob(
        fakeJob<InvoiceBatchJobData>({ requestId, accountId: ids.i1 }),
        invoiceDeps(mail("i1"), ids.i1, new OrderApiSessionError("https://2.rome.api.flipkart.com/api/1/invoice did not answer JSON — the session is probably not logged in"))
      );
      const doc = await OrderInvoiceRequest.findById(requestId).lean();
      assert.deepEqual(doc?.tasks.map((t) => [t.status, t.errorClass]), [["failed", "not_logged_in"], ["failed", "not_logged_in"]]);
      const row = await rowOf(mail("i1"));
      assert.equal(row.status, "logged_in");
      assert.equal(row.sessionCheck?.reason, `Invoices: https://2.rome.api.flipkart.com/api/1/invoice did not answer JSON — the session is probably not logged in${SESSION_NOTES.unconfirmed}`);
      assert.equal(row.demotedAt, null);
      const ev = await eventsOf(mail("i1"));
      assert.equal(ev.length, 1);
      assert.equal(ev[0].after?.flow, "invoice");
      assert.equal(ev[0].requestId, requestId);
      await taskLogInStep("invoices", requestId);
    });

    await check("invoice batch, HTTP 500 → platform_unavailable, nothing reported", async () => {
      const requestId = await invoiceRequest(mail("i2"), ids.i2, ["ODV-I2-1"]);
      await runInvoiceBatchJob(fakeJob<InvoiceBatchJobData>({ requestId, accountId: ids.i2 }), invoiceDeps(mail("i2"), ids.i2, new OrderApiError("https://2.rome.api.flipkart.com/api/1/invoice answered HTTP 500", 500)));
      const doc = await OrderInvoiceRequest.findById(requestId).lean();
      assert.equal(doc?.tasks[0]?.errorClass, "platform_unavailable");
      const row = await rowOf(mail("i2"));
      assert.equal(row.sessionCheck, null);
      assert.equal((await eventsOf(mail("i2"))).length, 0);
      await taskLogInStep("invoices", requestId);
    });

    await check("the fake was the only thing the HTTP flows talked to", () => {
      assert.ok(fake!.hits.length > 0);
      assert.ok(fake!.hits.every((p) => /^\/(rewards\d{3}|coupon\/(bounce|down)|account\/login)$/.test(p)), fake!.hits.join(", "));
    });

    // ── the dashboard's side ───────────────────────────────────────────────
    if (!DASHBOARD_FOUND) {
      console.log(`\nTHE DASHBOARD — not found at ${DASHBOARD}`);
      await check("the dashboard's Undo listing and alert words", () => {
        throw new Error(`no dashboard backend at ${DASHBOARD} — pass --dashboard=<its backend folder> or set DASHBOARD_BACKEND_DIR`);
      });
    } else {
      console.log(`\nTHE DASHBOARD (${DASHBOARD})`);
      await check("Session Health → Undo (dry run, last hour) lists exactly the rows demoted here", () => {
        const out = spawnSync("npx", ["tsx", "src/scripts/deadSessionCheck.ts", `--undo-list=${USER}`], { cwd: DASHBOARD, encoding: "utf8", timeout: 180_000 });
        const line = String(out.stdout || "").trim().split("\n").reverse().find((l) => l.startsWith("{"));
        assert.ok(line, `no answer from the dashboard script (exit ${out.status}): ${String(out.stderr || "").slice(0, 400)}`);
        const res = JSON.parse(line) as { matched: number; restored: number; emails: string[] };
        assert.deepEqual([...res.emails].sort(), [mail("x1"), mail("x5")]);
        assert.equal(res.matched, 2);
        assert.equal(res.restored, 0, "a dry run restores nothing");
        console.log(`        undoDemotions(${USER}, 3600e3, true) → ${line}`);
      });
      await check("the dashboard API raised each account's first event as its alert, in the event's own words", async () => {
        const until = Date.now() + 90_000;
        while ((await OrderUnitEvent.countDocuments({ userId: USER, processed: { $ne: true } })) > 0 && Date.now() < until) await sleep(1000);
        assert.equal(await OrderUnitEvent.countDocuments({ userId: USER, processed: { $ne: true } }), 0, "events not taken by the dashboard API within 90 s — is it running?");
        await sleep(3000);
        const alerts = await mongoose.connection.collection("alerts").find({ userId: USER, kind: "account.unreadable" }).toArray();
        const events = await OrderUnitEvent.find({ userId: USER, type: "account_unreadable" }).sort({ at: 1, _id: 1 }).lean();
        const firstByEmail = new Map<string, (typeof events)[number]>();
        for (const e of events) if (!firstByEmail.has(e.email)) firstByEmail.set(e.email, e);
        // Any dashboard API on this database may take an event; one on older code writes the old words.
        const olderWatcher = alerts.filter((a) => OLD_ALERT_WORDS.test(String(a.message)));
        assert.equal(olderWatcher.length, 0, `${olderWatcher.length} alert(s) in the old words — a dashboard API on older code (another machine on this database?) took those events, not this one: ${olderWatcher.map((a) => a.email).join(", ")}`);
        assert.equal(alerts.length, firstByEmail.size, `${alerts.length} alert(s) for ${firstByEmail.size} account(s)`);
        for (const [email, e] of firstByEmail) {
          const a = alerts.find((x) => x.email === email);
          assert.ok(a, `no alert for ${email}`);
          assert.equal(a.message, `${email}: ${String(e.after?.reason)}`);
          assert.equal(a.source, `Trade Flow ${String(e.after?.flow)}`);
        }
        console.log(`        e.g. ${alerts[0]?.source} · ${alerts[0]?.message}`);
      });
    }

    // ── the cap under reports that run at once ─────────────────────────────
    // After the dashboard's listing, which expects only x1 and x5 demoted.
    console.log("\nTHE CAP, REPORTS AT THE SAME MOMENT");
    await check("8 confirmed reports at once with 3 slots left: at most 3 demoted, and one slot per demotion", async () => {
      const used = await recentTradeFlowDemotions(USER);
      process.env.SESSION_DEMOTE_CAP = String(used + 3);
      try {
        const names = Array.from({ length: 8 }, (_, i) => `y${i + 1}`);
        for (const n of names) await insertRow(mail(n), 30 * DAY);
        const sessions = await Promise.all(names.map((n) => session(mail(n))));
        const outs = await Promise.all(
          sessions.map((s) => markSessionEligibleForRelogin({ userId: USER, platformId: s.platformId, email: s.email, reason, flow: "test", confirmed: true, sessionSavedAt: s.sessionSavedAt }))
        );
        const demoted = outs.filter((o) => o.demoted).length;
        assert.ok(demoted >= 1 && demoted <= 3, `${demoted} demoted with 3 slots left`);
        for (const o of outs) if (!o.demoted) assert.equal(o.note, SESSION_NOTES.cap);
        assert.equal(await PlatformId.countDocuments({ userId: USER, email: { $in: names.map(mail) }, status: "pending" }), demoted);
        assert.equal(await recentTradeFlowDemotions(USER), used + demoted, "a slot for each demotion, and none for a refusal");
        console.log(`        ${demoted} of 8 demoted (3 slots were left)`);
      } finally {
        process.env.SESSION_DEMOTE_CAP = "10";
      }
    });
  } finally {
    await browser?.close().catch(() => undefined);
    await fake?.close().catch(() => undefined);
    await cleanUp(`verify_ds_${RUN}_ledger`);
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
async function cleanUp(ledgerName: string) {
  const db = mongoose.connection.db!;
  const events = (await OrderUnitEvent.deleteMany({ userId: USER })).deletedCount;
  await sleep(3000);
  const del = {
    platformids: (await PlatformId.deleteMany({ userId: USER })).deletedCount,
    order_unit_events: events + (await OrderUnitEvent.deleteMany({ userId: USER })).deletedCount,
    alerts: (await db.collection("alerts").deleteMany({ userId: USER })).deletedCount,
    order_coupon_requests: (await OrderCouponRequest.deleteMany({ userId: USER })).deletedCount,
    order_coupon_tasks: (await OrderCouponTask.deleteMany({ userId: USER })).deletedCount,
    order_invoice_requests: (await OrderInvoiceRequest.deleteMany({ userId: USER })).deletedCount,
    order_sync_tasks: (await OrderSyncTask.deleteMany({ $or: [{ userId: USER }, { requestId: { $in: requestIds } }] })).deletedCount,
    session_demotions: (await db.collection("session_demotions").deleteMany({ userId: USER })).deletedCount,
  };
  await db.dropCollection(ledgerName).catch(() => undefined);
  await sleep(2000);
  await db.collection("alerts").deleteMany({ userId: USER });
  const left: Record<string, number> = {};
  for (const name of ["platformids", "order_unit_events", "alerts", "order_coupon_requests", "order_coupon_tasks", "order_invoice_requests", "order_sync_tasks", "session_demotions", "order_update_requests", "order_job_runs", "tradeflowjobs", "batches"]) {
    left[name] = await db.collection(name).countDocuments({ userId: USER });
  }
  left.order_sync_tasks += await OrderSyncTask.countDocuments({ requestId: { $in: requestIds } });
  left[ledgerName] = (await db.listCollections({ name: ledgerName }).toArray()).length;
  left["*@verify.invalid platformids of this run"] = await PlatformId.countDocuments({ userId: USER, email: /@verify\.invalid$/ });
  console.log(`\ncleaned up: ${Object.entries(del).map(([k, v]) => `${k} ${v}`).join(" · ")}`);
  console.log(`left behind: ${Object.entries(left).map(([k, v]) => `${k} ${v}`).join(" · ")}${Object.values(left).every((n) => n === 0) ? " — nothing" : " — NOT CLEAN"}`);
  if (!Object.values(left).every((n) => n === 0)) failures.push("clean-up");
}

main().catch(async (err) => {
  console.error(err);
  await cleanUp(`verify_ds_${RUN}_ledger`).catch((e) => console.error("clean-up failed:", e));
  process.exit(1);
});
