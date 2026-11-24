/**
 * "Apply Company Coupons" end to end — WITHOUT Flipkart and without a login.
 *
 * A local fake plays Flipkart's part: a coupon page that answers 200 to a
 * live session and 302 → /account/login to a dead one, a redirect chain, a
 * 404, a page that says "expired", a redirect off the site, a 503 and a
 * redirect loop. The worker (a separate process) opens the links through the
 * REAL request context the order API uses (openOrderApiSession) and the real
 * runner, queue, per-account lock, counts, ledger row, stop and resume.
 *
 * What is real: Mongo (the new order_coupon_requests / order_coupon_tasks
 * collections, rows of an INVENTED workspace id, deleted at the end), Redis /
 * BullMQ (a THROWAWAY queue name, obliterated at the end). What is stubbed:
 * the account list and the saved sessions (invented accounts whose cookie is
 * for 127.0.0.1), and where a flipkart.com link is really requested (the
 * fake). The ledger goes to a throwaway collection with the real schema.
 *
 * What this cannot show: that opening a real coupon link activates a coupon.
 *
 *   npm run test:coupons
 */
import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { fileURLToPath } from "node:url";
import mongoose, { Types } from "mongoose";
import { config } from "../config.js";
import { getRedis } from "../lib/redis.js";
import { OrderCouponRequest, OrderCouponTask } from "../models/OrderCouponRequest.js";
import { OrderJobRun, type IOrderJobRun } from "../models/OrderJobRun.js";
import { createCouponWorker, getCouponQueue } from "../queue.js";
import { lockAccount } from "../services/accountLock.js";
import {
  createCouponRequest,
  failUnclaimedCouponRequests,
  getCouponRequest,
  listCouponRequests,
  runCouponJob,
  startCouponRequestReaper,
  stopCouponRequest,
  type CouponDeps,
} from "../services/couponRequests.js";
import { clearWorkerHeartbeat, RequestInputError, startWorkerHeartbeat, WORKER_NOT_PICKED_UP, workerAlive } from "../services/requestCommon.js";

const MODE = process.argv.includes("--worker") ? "worker" : process.argv.includes("--switched-off") ? "switched-off" : "driver";
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const SITE = "https://www.flipkart.com";

// ── the stubs (both processes) ──────────────────────────────────────────────

/** good… and dead… are logged-in Platform IDs (a dead one's session bounces to login); nosess… lost its session after the request was made; anything else is not an account. */
const kindOf = (email: string) => (email.match(/^(good|dead|nosess)\d+@verify\.invalid$/) || [])[1] || "";

function stubs(queueName: string, ledgerCollection: string, origin: string, all: string[], linkPauseMs = 0): Partial<CouponDeps> {
  const ledger = (mongoose.models.OrderJobRunVerify as mongoose.Model<IOrderJobRun>) || mongoose.model<IOrderJobRun>("OrderJobRunVerify", OrderJobRun.schema, ledgerCollection);
  return {
    queueName,
    ledger,
    linkPauseMs,
    accountPauseMs: 0,
    lockRetryMs: 1000,
    // The real lock on an invented email, held 3 s at most so a killed worker's hold lapses within the check.
    lock: (email, holder) => lockAccount(email, holder, 3000),
    listAccounts: async (_userId, emails) => (emails || all).filter((e) => kindOf(e)).map((email) => ({ email, accountId: `verify-${email.split("@")[0]}` })),
    loadSession: async (email) =>
      kindOf(email) === "nosess"
        ? { ok: false as const, email, reason: 'Status is "pending", not logged_in' }
        : { ok: true as const, email, platformId: `verify-${email.split("@")[0]}`, cookies: [{ name: "T", value: `${kindOf(email)}:${email}`, domain: "127.0.0.1", path: "/", httpOnly: true, secure: false, sameSite: "Lax" }] },
    // openContext is NOT stubbed: the links are opened through openOrderApiSession, as in production.
    resolveUrl: (link) => (link.startsWith(SITE) ? origin + link.slice(SITE.length) : link),
    allowHost: (host) => host === "127.0.0.1",
  };
}

// ── the worker process ──────────────────────────────────────────────────────

async function workerMain() {
  const queueName = String(process.env.VERIFY_QUEUE);
  await mongoose.connect(config.mongoUri, { serverSelectionTimeoutMS: 15_000 });
  const deps = stubs(queueName, String(process.env.VERIFY_LEDGER), String(process.env.VERIFY_ORIGIN), [], Number(process.env.VERIFY_LINK_PAUSE) || 0);
  const worker = createCouponWorker(Number(process.env.VERIFY_CONCURRENCY) || 4, (job, token) => runCouponJob(job, token, deps), { queueName, lockDuration: 4000, stalledInterval: 2000 });
  worker.on("failed", (job, err) => {
    if (err.name !== "DelayedError") console.log(`job ${job?.id} failed: ${err.message}`);
  });
  startWorkerHeartbeat(queueName);
  startCouponRequestReaper(queueName);
  await worker.waitUntilReady();
  console.log("READY");
}

/** COMPANY_COUPONS_ENABLED=0 is read when the module loads, so the switch is checked in a process started with it. */
async function switchedOffMain() {
  try {
    await createCouponRequest(String(new Types.ObjectId()), { links: `${SITE}/coupon/ok` });
    console.log("CREATED");
  } catch (err) {
    console.log(JSON.stringify({ refused: err instanceof RequestInputError, status: (err as RequestInputError).status, code: (err as RequestInputError).code, message: (err as Error).message }));
  }
}

// ── the checks ──────────────────────────────────────────────────────────────

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

async function waitFor<T>(label: string, read: () => Promise<T | null | undefined | false>, timeoutMs = 30_000, everyMs = 100): Promise<T> {
  const until = Date.now() + timeoutMs;
  for (;;) {
    const value = await read();
    if (value) return value;
    if (Date.now() > until) throw new Error(`timed out waiting for: ${label}`);
    await sleep(everyMs);
  }
}

async function driverMain() {
  const tag = `${Date.now().toString(36)}${Math.floor(Math.random() * 1e4)}`;
  const queueName = `verify-coupon-apply-${tag}`;
  const ledgerCollection = `order_job_runs_verify_${tag}`;
  const userId = String(new Types.ObjectId()); // a workspace that does not exist
  await mongoose.connect(config.mongoUri, { serverSelectionTimeoutMS: 15_000 });

  // The fake Flipkart.
  const hits: Array<{ path: string; email: string; at: number; ua: string; accept: string }> = [];
  let slowMs = 300;
  let inFlight = 0;
  let maxInFlight = 0;
  const page = (title: string, body: string) => `<!doctype html><html><head><title>${title}</title><script>var state = {"invalid":false,"expired":0};</script></head><body>${body}</body></html>`;
  const server = http.createServer((req, res) => {
    const url = new URL(req.url || "/", "http://127.0.0.1");
    const cookie = /(?:^|;\s*)T=([^;]+)/.exec(String(req.headers.cookie || ""))?.[1] || "";
    const [session, email = ""] = decodeURIComponent(cookie).split(":");
    hits.push({ path: url.pathname, email, at: Date.now(), ua: String(req.headers["user-agent"] || ""), accept: String(req.headers.accept || "") });
    const send = (status: number, headers: Record<string, string>, body = "") => res.writeHead(status, headers).end(body);
    const html = { "content-type": "text/html; charset=utf-8" };
    // Every coupon page needs a live session: anything else is sent to login, as Flipkart does.
    if (url.pathname.startsWith("/coupon/") && session !== "good") return send(302, { location: `/account/login?ret=${encodeURIComponent(url.pathname)}` });
    switch (url.pathname) {
      case "/coupon/ok":
        return send(200, html, page("Company coupon | Flipkart", "<h1>Coupon added to your account</h1><p>Use it at checkout.</p>"));
      case "/coupon/slow":
        inFlight++;
        maxInFlight = Math.max(maxInFlight, inFlight);
        setTimeout(() => {
          inFlight--;
          send(200, html, page("Company coupon | Flipkart", "<h1>Coupon added to your account</h1>"));
        }, slowMs);
        return;
      case "/coupon/hop":
        return send(302, { location: "/coupon/hop2" });
      case "/coupon/hop2":
        return send(301, { location: `${`http://${req.headers.host}`}/coupon/ok` });
      case "/coupon/gone":
        return send(404, html, page("Page not found", "<h1>Sorry, this page isn't available</h1>"));
      case "/coupon/expired":
        return send(200, html, page("Offers | Flipkart", "<h1>This coupon has expired</h1>"));
      case "/coupon/away":
        return send(302, { location: "https://example.org/elsewhere" });
      case "/coupon/boom":
        return send(503, html, "<html><body>Service Unavailable</body></html>");
      case "/coupon/loop":
        return send(302, { location: "/coupon/loop" });
      case "/account/login":
        return send(200, html, page("Login", "<h1>Login</h1>"));
      default:
        return send(404, html, page("Page not found", ""));
    }
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

  const six = [1, 2, 3, 4, 5, 6].map((n) => `good0${n}@verify.invalid`);
  let all: string[] = six;
  const deps = stubs(queueName, ledgerCollection, origin, []);
  deps.listAccounts = async (_userId, emails) => (emails || all).filter((e) => kindOf(e)).map((email) => ({ email, accountId: `verify-${email.split("@")[0]}` }));
  const Ledger = deps.ledger!;
  const queue = getCouponQueue(queueName);
  const reqOf = async (id: string) => (await OrderCouponRequest.findById(id).lean())!;
  const tasksOf = async (id: string) => OrderCouponTask.find({ requestId: new Types.ObjectId(id) }).sort({ email: 1 }).lean();
  const taskOf = async (id: string, email: string) => (await OrderCouponTask.findOne({ requestId: new Types.ObjectId(id), email }).lean())!;
  const ledgerOf = async (id: string) => (await Ledger.findOne({ userId, type: "coupons", refId: id }).lean())!;
  const hitsOf = (email: string, path?: string) => hits.filter((h) => h.email === email && (!path || h.path === path));
  const link = (name: string) => `${SITE}/coupon/${name}`;

  let child: ChildProcess | null = null;
  const startWorker = async (concurrency: number, linkPauseMs = 0) => {
    const p = spawn(process.execPath, [...process.execArgv, fileURLToPath(import.meta.url), "--worker"], {
      env: { ...process.env, VERIFY_QUEUE: queueName, VERIFY_LEDGER: ledgerCollection, VERIFY_ORIGIN: origin, VERIFY_CONCURRENCY: String(concurrency), VERIFY_LINK_PAUSE: String(linkPauseMs) },
      stdio: ["ignore", "pipe", "pipe"],
    });
    child = p;
    let ready = false;
    p.stdout!.on("data", (d) => {
      if (String(d).includes("READY")) ready = true;
    });
    p.stderr!.on("data", (d) => {
      const text = String(d);
      if (!/DEP0205|trace-deprecation/.test(text)) process.stderr.write(`        [worker] ${text}`);
    });
    await waitFor("the worker process to be ready", async () => ready, 60_000);
  };
  const killWorker = async () => {
    const p = child;
    if (!p || p.exitCode != null) return;
    const gone = new Promise((resolve) => p.once("exit", resolve));
    p.kill("SIGKILL");
    await gone;
    child = null;
  };
  const completed = (id: string, timeoutMs = 60_000) =>
    waitFor(`request ${id.slice(-6)} to complete`, async () => {
      const r = await reqOf(id);
      return r.status === "completed" ? r : null;
    }, timeoutMs);
  const ledgerClosed = (id: string) =>
    waitFor("the ledger row to close", async () => {
      const row = await ledgerOf(id);
      return row.status === "running" || row.status === "queued" ? null : row;
    }, 5000);

  try {
    console.log(`\nworkspace ${userId} (invented) · queue ${queueName} · ledger ${ledgerCollection} · fake Flipkart ${origin}\n`);

    // ── A: creation, and nobody listening ────────────────────────────────────
    console.log("A — the request, and no worker on the queue");
    const a = await createCouponRequest(userId, { links: `${link("ok")}, ${link("hop")} ${link("ok")}`, emails: "GOOD01@verify.invalid good02@verify.invalid stranger@verify.invalid", by: "verify" }, deps);
    await check("one task per account; an email that is not a logged-in Platform ID gets a failed task, not a job", async () => {
      assert.deepEqual({ status: a.status, task_type: a.task_type, platform: a.platform, source: a.source, triggered_by: a.triggered_by }, { status: "queued", task_type: "company_coupon_apply", platform: "FLIPKART", source: "user", triggered_by: "verify" });
      assert.deepEqual(a.coupon_links, [link("ok"), link("hop")]);
      assert.deepEqual(a.params, { coupon_links_count: 2, email_ids_count: 3, selection_method: "email_ids", accounts_count: 2 });
      assert.deepEqual(a.counts, { total: 3, queued: 2, in_progress: 0, success: 0, failed: 1, cancelled: 0, aborted: 0, timed_out: 0 });
      assert.deepEqual({ tasks_created: a.tasks_created, accounts: a.accounts, not_logged_in: a.not_logged_in }, { tasks_created: 3, accounts: 2, not_logged_in: 1 });
      assert.equal(a.activation_verified, false);
      const stranger = await taskOf(a.request_id, "stranger@verify.invalid");
      assert.deepEqual({ status: stranger.status, errorClass: stranger.errorClass, error: stranger.error }, { status: "failed", errorClass: "not_logged_in", error: "Account is not logged in" });
      assert.equal(await queue.getWaitingCount(), 2);
      const l = await ledgerOf(a.request_id);
      assert.deepEqual({ status: l.status, total: l.total, by: l.by, trigger: l.trigger }, { status: "queued", total: 3, by: "verify", trigger: "user" });
      assert.equal((l.meta as { activation_verified?: boolean }).activation_verified, false);
    });
    await check("no emails = every logged-in Flipkart account", async () => {
      const r = await createCouponRequest(userId, { links: [link("ok")], by: "verify" }, deps);
      assert.deepEqual(r.params, { coupon_links_count: 1, email_ids_count: 0, selection_method: "all_accounts", accounts_count: 6 });
      assert.equal(r.counts.queued, 6);
      assert.deepEqual((await tasksOf(r.request_id)).map((t) => t.email), six);
      const stopped = (await stopCouponRequest(userId, r.request_id, "verify", deps))!;
      assert.deepEqual({ status: stopped.status, removed: stopped.removed, cancelled: stopped.counts.cancelled, message: stopped.message }, { status: "cancelled", removed: 6, cancelled: 6, message: "Stopped by verify" });
      assert.ok((await tasksOf(r.request_id)).every((t) => t.status === "cancelled" && t.error === "Cancelled before it started"));
      assert.equal((await ledgerOf(r.request_id)).status, "stopped");
    });
    await check("refused before anything exists: no account at all, and the switch off (COMPANY_COUPONS_ENABLED=0)", async () => {
      all = [];
      await assert.rejects(() => createCouponRequest(userId, { links: link("ok") }, deps), (err: unknown) => err instanceof RequestInputError && err.message === "No logged-in Flipkart account in this workspace. Login a Platform ID first.");
      all = six;
      const off = spawnSync(process.execPath, [...process.execArgv, fileURLToPath(import.meta.url), "--switched-off"], { env: { ...process.env, COMPANY_COUPONS_ENABLED: "0" }, encoding: "utf8" });
      const line = off.stdout.split("\n").find((l) => l.startsWith("{")) || off.stdout;
      assert.deepEqual(JSON.parse(line), { refused: true, status: 503, code: "coupons_disabled", message: "Company Coupons is switched off on this server (COMPANY_COUPONS_ENABLED=0)" });
    });
    await check("a worker heartbeat = only waiting; none and past the pick-up time → failed with the restart sentence", async () => {
      const beat = startWorkerHeartbeat(queueName);
      clearInterval(beat);
      await waitFor("the heartbeat key", () => workerAlive(queueName), 5000);
      assert.deepEqual(await failUnclaimedCouponRequests({ pickupMs: 0, queueName, ledger: Ledger }), { failed: 0, parked: 0 });
      await clearWorkerHeartbeat(queueName);
      assert.deepEqual(await failUnclaimedCouponRequests({ queueName, ledger: Ledger }), { failed: 0, parked: 0 }, "younger than the pick-up time");
      assert.deepEqual(await failUnclaimedCouponRequests({ pickupMs: 0, queueName, ledger: Ledger }), { failed: 1, parked: 0 });
      const r = await reqOf(a.request_id);
      assert.deepEqual({ status: r.status, message: r.message }, { status: "completed", message: WORKER_NOT_PICKED_UP });
      assert.deepEqual({ ...r.counts }, { total: 3, queued: 0, in_progress: 0, success: 0, failed: 3, cancelled: 0, aborted: 0, timed_out: 0 });
      const t = await taskOf(a.request_id, "good01@verify.invalid");
      assert.deepEqual({ status: t.status, errorClass: t.errorClass, error: t.error }, { status: "failed", errorClass: "internal_error", error: WORKER_NOT_PICKED_UP });
      const l = await ledgerOf(a.request_id);
      assert.deepEqual({ status: l.status, message: l.message, failed: l.failed }, { status: "failed", message: WORKER_NOT_PICKED_UP, failed: 3 });
      const jobs = await queue.getJobCounts("waiting", "active", "delayed");
      assert.equal(jobs.waiting + jobs.active + jobs.delayed, 0);
      assert.equal(hits.length, 0, "nothing was requested");
    });

    // ── B: what each kind of answer becomes ─────────────────────────────────
    console.log("\nB — a worker process opens the links (4 accounts at once, 150 ms between an account's links)");
    await startWorker(4, 150);
    const names = ["ok", "hop", "gone", "expired", "away", "boom", "loop"];
    const b = await createCouponRequest(userId, { links: names.map(link), emails: ["good01@verify.invalid", "good02@verify.invalid", "dead01@verify.invalid", "nosess01@verify.invalid"], by: "verify" }, deps);
    await completed(b.request_id);
    const good = await taskOf(b.request_id, "good01@verify.invalid");
    const by = (name: string) => good.results.find((r) => r.link === link(name))!;
    await check("200 with the session → opened, and it says no more than that", () => {
      const r = by("ok");
      assert.deepEqual(
        { outcome: r.outcome, errorClass: r.errorClass, message: r.message, httpStatus: r.httpStatus, loginBounce: r.loginBounce, finalUrl: r.finalUrl, title: r.title, excerpt: r.excerpt },
        { outcome: "opened", errorClass: "", message: "Link opened with the account's session (HTTP 200)", httpStatus: 200, loginBounce: false, finalUrl: `${origin}/coupon/ok`, title: "Company coupon | Flipkart", excerpt: "Coupon added to your account Use it at checkout." }
      );
      assert.deepEqual(r.hops.map((h) => ({ status: h.status, url: h.url })), [{ status: 200, url: `${origin}/coupon/ok` }]);
    });
    await check("redirects are followed and every hop is on record", () => {
      const r = by("hop");
      assert.equal(r.outcome, "opened");
      assert.equal(r.finalUrl, `${origin}/coupon/ok`);
      assert.deepEqual(r.hops.map((h) => `${h.status} ${h.url.replace(origin, "")}`), ["302 /coupon/hop", "301 /coupon/hop2", "200 /coupon/ok"]);
    });
    await check('404 and a page saying "expired" → invalid_coupon "Coupon link is invalid or expired"', () => {
      for (const [name, status] of [["gone", 404], ["expired", 200]] as const) {
        const r = by(name);
        assert.deepEqual({ outcome: r.outcome, errorClass: r.errorClass, message: r.message, httpStatus: r.httpStatus }, { outcome: "failed", errorClass: "invalid_coupon", message: "Coupon link is invalid or expired", httpStatus: status }, name);
      }
      assert.equal(by("expired").excerpt, "This coupon has expired");
      assert.equal(by("ok").outcome, "opened", 'the word "expired" inside a script does not make a page an expired coupon');
    });
    await check("503 → platform_unavailable; a redirect off Flipkart is not followed; a redirect loop is given up", () => {
      assert.deepEqual({ errorClass: by("boom").errorClass, httpStatus: by("boom").httpStatus, message: by("boom").message }, { errorClass: "platform_unavailable", httpStatus: 503, message: "Platform did not respond; try again later" });
      const away = by("away");
      assert.deepEqual({ outcome: away.outcome, errorClass: away.errorClass, finalUrl: away.finalUrl, excerpt: away.excerpt }, { outcome: "failed", errorClass: "unexpected_response", finalUrl: "https://example.org/elsewhere", excerpt: "The link left Flipkart: example.org" });
      assert.deepEqual(away.hops.map((h) => h.status), [302]);
      const loop = by("loop");
      assert.deepEqual({ outcome: loop.outcome, errorClass: loop.errorClass, hops: loop.hops.length }, { outcome: "failed", errorClass: "unexpected_response", hops: 11 });
    });
    await check("the account's task: some links opened, some failed → failed partially_failed", () => {
      assert.deepEqual({ status: good.status, errorClass: good.errorClass, error: good.error, note: good.note, results: good.results.length }, { status: "failed", errorClass: "partially_failed", error: "Some items in this task failed", note: "2 of 7 link(s) opened", results: 7 });
      assert.ok(good.startedAt instanceof Date && good.finishedAt instanceof Date);
    });
    await check("a dead session: 302 → login is the answer — failed not_logged_in, the login page never loaded, the other links not tried", async () => {
      const t = await taskOf(b.request_id, "dead01@verify.invalid");
      assert.deepEqual({ status: t.status, errorClass: t.errorClass, error: t.error }, { status: "failed", errorClass: "not_logged_in", error: "Account is not logged in" });
      const first = t.results[0];
      assert.deepEqual({ outcome: first.outcome, errorClass: first.errorClass, loginBounce: first.loginBounce, httpStatus: first.httpStatus, finalUrl: first.finalUrl }, { outcome: "failed", errorClass: "not_logged_in", loginBounce: true, httpStatus: 302, finalUrl: `${origin}/account/login?ret=%2Fcoupon%2Fok` });
      assert.deepEqual(first.hops.map((h) => `${h.status} ${h.url.replace(origin, "")}`), ["302 /coupon/ok"]);
      assert.deepEqual(t.results.slice(1).map((r) => r.outcome), ["skipped", "skipped", "skipped", "skipped", "skipped", "skipped"]);
      assert.equal(hitsOf("dead01@verify.invalid").length, 1);
      assert.equal(hits.filter((h) => h.path === "/account/login").length, 0);
    });
    await check("an account with no saved session: failed not_logged_in, nothing requested", async () => {
      const t = await taskOf(b.request_id, "nosess01@verify.invalid");
      assert.deepEqual({ status: t.status, errorClass: t.errorClass, error: t.error, note: t.note, results: t.results.length }, { status: "failed", errorClass: "not_logged_in", error: "Account is not logged in", note: 'Status is "pending", not logged_in', results: 0 });
      assert.equal(hitsOf("nosess01@verify.invalid").length, 0);
    });
    await check("the session's cookie and a desktop User-Agent go out; links of one account are paced", () => {
      const mine = hitsOf("good01@verify.invalid");
      assert.ok(mine.length >= 7);
      assert.match(mine[0].ua, /^Mozilla\/5\.0 \(Windows NT 10\.0; Win64; x64\).*Chrome\/\d+/);
      assert.match(mine[0].accept, /^text\/html/);
      const firstOf = (name: string) => mine.find((h) => h.path === `/coupon/${name}`)!.at;
      assert.ok(firstOf("hop") - firstOf("ok") >= 140, `second link ${firstOf("hop") - firstOf("ok")} ms after the first`);
      assert.ok(firstOf("gone") - firstOf("hop") >= 140);
    });
    await check("counts, the ledger row and the reference shape (GET …/company-coupon/requests/:id)", async () => {
      const r = await reqOf(b.request_id);
      assert.deepEqual({ ...r.counts }, { total: 4, queued: 0, in_progress: 0, success: 0, failed: 4, cancelled: 0, aborted: 0, timed_out: 0 });
      assert.ok(r.startedAt instanceof Date && r.finishedAt instanceof Date && r.workerHost);
      const l = await ledgerClosed(b.request_id);
      assert.deepEqual({ status: l.status, total: l.total, processed: l.processed, success: l.success, failed: l.failed }, { status: "completed", total: 4, processed: 4, success: 0, failed: 4 });
      assert.equal(l.failedItems.length, 4);
      const pub = (await getCouponRequest(userId, b.request_id, { status: "failed", limit: 2 }))!;
      assert.deepEqual({ status: pub.status, finished_count: pub.finished_count, progress_percent: pub.progress_percent, can_cancel: pub.can_cancel, activation_verified: pub.activation_verified }, { status: "completed", finished_count: 4, progress_percent: 100, can_cancel: false, activation_verified: false });
      assert.deepEqual(pub.tasks_page, { limit: 2, skip: 0, returned: 2 });
      const t = pub.tasks[0];
      assert.deepEqual({ task_type: t.task_type, platform: t.platform, source: t.source, triggered_by: t.triggered_by, order_id: t.order_id }, { task_type: "company_coupon_apply", platform: "FLIPKART", source: "user", triggered_by: "verify", order_id: null });
      assert.ok(t.error_code && t.error && Array.isArray(t.results));
      assert.ok((await listCouponRequests(userId, 10)).some((x) => x.request_id === b.request_id));
      assert.equal(await getCouponRequest(String(new Types.ObjectId()), b.request_id), null, "another workspace cannot read it");
    });
    const b2 = await createCouponRequest(userId, { links: [link("ok"), link("hop")], by: "verify" }, deps);
    await completed(b2.request_id);
    const one = await createCouponRequest(userId, { links: link("ok"), emails: ["good03@verify.invalid"], by: "verify" }, deps);
    await completed(one.request_id);
    await check("every link opened → the task succeeds, worded as what is known", async () => {
      const tasks = await tasksOf(b2.request_id);
      assert.equal(tasks.length, 6);
      for (const t of tasks) assert.deepEqual({ status: t.status, errorClass: t.errorClass, error: t.error, note: t.note }, { status: "success", errorClass: "", error: "", note: "2 links opened with the account's session (HTTP 200)" }, t.email);
      assert.deepEqual({ ...(await reqOf(b2.request_id)).counts }, { total: 6, queued: 0, in_progress: 0, success: 6, failed: 0, cancelled: 0, aborted: 0, timed_out: 0 });
      const l = await ledgerClosed(b2.request_id);
      assert.deepEqual({ status: l.status, success: l.success, failed: l.failed }, { status: "completed", success: 6, failed: 0 });
      assert.equal((await taskOf(one.request_id, "good03@verify.invalid")).note, "Link opened with the account's session (HTTP 200)");
    });
    await killWorker();

    // ── C: stop ─────────────────────────────────────────────────────────────
    console.log("\nC — stop while it runs (20 accounts, 4 at once)");
    await clearWorkerHeartbeat(queueName);
    await startWorker(4);
    all = Array.from({ length: 20 }, (_, i) => `good${String(i + 10)}@verify.invalid`);
    slowMs = 350;
    hits.length = 0;
    maxInFlight = 0;
    const c = await createCouponRequest(userId, { links: link("slow"), by: "verify" }, deps);
    await waitFor("four accounts of C to finish", async () => ((await OrderCouponTask.countDocuments({ requestId: new Types.ObjectId(c.request_id), status: "success" })) >= 4 ? true : null), 30_000, 50);
    const stopped = (await stopCouponRequest(userId, c.request_id, "verify", deps))!;
    await waitFor("the accounts in flight to finish", async () => ((await OrderCouponTask.countDocuments({ requestId: new Types.ObjectId(c.request_id), status: "in_progress" })) === 0 ? true : null));
    await sleep(600);
    await check('queued accounts → cancelled "Cancelled before it started"; the ones in flight finished; never more than 4 at once', async () => {
      const tasks = await tasksOf(c.request_id);
      const n = (s: string) => tasks.filter((t) => t.status === s).length;
      assert.equal(n("success") + n("cancelled"), 20, JSON.stringify(tasks.map((t) => t.status)));
      assert.ok(n("success") >= 4 && n("cancelled") >= 8, `success ${n("success")}, cancelled ${n("cancelled")}`);
      assert.ok(stopped.removed >= 8, `removed ${stopped.removed}`);
      for (const t of tasks) {
        if (t.status === "cancelled") assert.deepEqual([t.error, t.startedAt, hitsOf(t.email).length], ["Cancelled before it started", null, 0], t.email);
        else assert.equal(hitsOf(t.email).length, 1, t.email);
      }
      assert.ok(maxInFlight >= 2 && maxInFlight <= 4, `${maxInFlight} at once`);
      const r = await reqOf(c.request_id);
      assert.deepEqual({ status: r.status, message: r.message, stoppedBy: r.stoppedBy }, { status: "cancelled", message: "Stopped by verify", stoppedBy: "verify" });
      assert.deepEqual({ ...r.counts }, { total: 20, queued: 0, in_progress: 0, success: n("success"), failed: 0, cancelled: n("cancelled"), aborted: 0, timed_out: 0 });
      const l = await ledgerOf(c.request_id);
      assert.deepEqual({ status: l.status, message: l.message, success: l.success, cancelled: l.cancelled }, { status: "stopped", message: "Stopped by verify", success: n("success"), cancelled: n("cancelled") });
      const jobs = await queue.getJobCounts("waiting", "active", "delayed");
      assert.equal(jobs.waiting + jobs.active + jobs.delayed, 0);
    });

    // ── D: the worker dies, then comes back ─────────────────────────────────
    console.log("\nD — the worker is killed mid-batch, then restarted");
    all = Array.from({ length: 12 }, (_, i) => `good${String(i + 40)}@verify.invalid`);
    hits.length = 0;
    const d = await createCouponRequest(userId, { links: [link("slow"), link("ok")], by: "verify" }, deps);
    await waitFor("four accounts of D to finish", async () => ((await OrderCouponTask.countDocuments({ requestId: new Types.ObjectId(d.request_id), status: "success" })) >= 4 ? true : null), 30_000, 50);
    await killWorker();
    await sleep(500);
    const doneBefore = new Map((await tasksOf(d.request_id)).filter((t) => t.status === "success").map((t) => [t.email, t.finishedAt!.getTime()]));
    await check("with the worker gone the request stays open and resumable, and its ledger row stops reading 'running'", async () => {
      const open = await OrderCouponTask.countDocuments({ requestId: new Types.ObjectId(d.request_id), status: { $in: ["queued", "in_progress"] } });
      assert.ok(open > 0 && doneBefore.size >= 4 && doneBefore.size < 12);
      assert.equal((await reqOf(d.request_id)).status, "in_progress");
      await clearWorkerHeartbeat(queueName); // what its 75 s expiry does
      await sleep(50);
      assert.deepEqual(await failUnclaimedCouponRequests({ pickupMs: 0, queueName, ledger: Ledger }), { failed: 0, parked: 1 });
      const l = await ledgerOf(d.request_id);
      assert.equal(l.status, "failed");
      assert.match(l.message, /^The worker stopped with \d+ account\(s\) left — restart the worker to resume$/);
      assert.equal((await reqOf(d.request_id)).status, "in_progress");
      // The request's own reads say so too — the dashboard's Stop for it is offered from this.
      const one = (await getCouponRequest(userId, d.request_id))!;
      assert.deepEqual([one.status, one.worker_alive, one.stalled], ["in_progress", false, true]);
      assert.match(one.stalled_message, /^The worker stopped with \d+ account\(s\) left — restart the worker to resume$/);
      const listed = (await listCouponRequests(userId, 50)).find((x) => x.request_id === d.request_id)!;
      assert.deepEqual([listed.worker_alive, listed.stalled], [false, true]);
    });
    await startWorker(4);
    await completed(d.request_id, 90_000);
    await check("a restarted worker finishes it — past the dead worker's account locks — and redoes no finished account", async () => {
      const tasks = await tasksOf(d.request_id);
      assert.ok(tasks.every((t) => t.status === "success" && t.results.length === 2), JSON.stringify(tasks.map((t) => [t.email, t.status])));
      for (const [email, finishedAt] of doneBefore) {
        assert.equal(tasks.find((t) => t.email === email)!.finishedAt!.getTime(), finishedAt, `${email} was written again`);
        assert.equal(hitsOf(email, "/coupon/slow").length, 1, `${email} was opened again`);
      }
      assert.deepEqual({ ...(await reqOf(d.request_id)).counts }, { total: 12, queued: 0, in_progress: 0, success: 12, failed: 0, cancelled: 0, aborted: 0, timed_out: 0 });
      const l = await ledgerClosed(d.request_id);
      assert.deepEqual({ status: l.status, success: l.success, processed: l.processed }, { status: "completed", success: 12, processed: 12 });
      assert.equal(await Ledger.countDocuments({ userId, status: { $in: ["queued", "running"] } }), 0, "no ledger row is left open");
    });
  } finally {
    // Everything this run made: the invented workspace's requests and tasks, the throwaway ledger and queue, the invented accounts' locks.
    await killWorker();
    server.close();
    const uid = new Types.ObjectId(userId);
    const removedTasks = await OrderCouponTask.deleteMany({ userId: uid });
    const removed = await OrderCouponRequest.deleteMany({ userId: uid });
    await mongoose.connection.db!.dropCollection(ledgerCollection).catch(() => undefined);
    await clearWorkerHeartbeat(queueName);
    await queue.obliterate({ force: true }).catch(() => undefined);
    await queue.close().catch(() => undefined);
    const locks = await getRedis().keys("account:lock:*@verify.invalid");
    if (locks.length) await getRedis().del(...locks);
    const left = (await OrderCouponRequest.countDocuments({ userId: uid })) + (await OrderCouponTask.countDocuments({ userId: uid }));
    const stray = (await getRedis().keys(`bull:${queueName}:*`)).length;
    console.log(`\ncleanup: ${removed.deletedCount} request(s) and ${removedTasks.deletedCount} task(s) of the invented workspace deleted (${left} left), ledger collection dropped, queue obliterated (${stray} key(s) left), ${locks.length} lock(s) of invented accounts released`);
    if (left || stray) failures.push("cleanup");
    await mongoose.disconnect();
  }
  console.log(`${passed} passed, ${failures.length} failed`);
  process.exit(failures.length ? 1 : 0);
}

(MODE === "worker" ? workerMain() : MODE === "switched-off" ? switchedOffMain().then(() => process.exit(0)) : driverMain()).catch((err) => {
  console.error(err);
  process.exit(1);
});
