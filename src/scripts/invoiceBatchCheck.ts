/**
 * The invoice batch end to end — WITHOUT Flipkart, without S3, without a login:
 *
 *   · the queue, the worker (a separate process), per-order results, counts,
 *     the ledger row, stop, and resume after the worker is killed
 *   · "no worker took it" → failed with the restart sentence
 *
 * What is real: Mongo (the new order_invoice_requests collection, rows of an
 * INVENTED workspace id, deleted at the end), Redis / BullMQ (a THROWAWAY
 * queue name, obliterated at the end — nothing touches the live queues), the
 * whole of services/invoiceRequests.ts. What is stubbed: order → account
 * (invented accounts), the session (a bare HTTP context), and the download —
 * it asks a local fake Flipkart that plays 200 PDF / 404 / 429 / 500 / 401 /
 * an HTML page, and stores nothing. The ledger is written to a throwaway
 * collection with the real schema, dropped at the end.
 *
 *   npm run test:invoice-batch
 */
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { fileURLToPath } from "node:url";
import mongoose, { Types } from "mongoose";
import { request as playwrightRequest } from "playwright";
import { config } from "../config.js";
import { getRedis } from "../lib/redis.js";
import { OrderInvoiceRequest } from "../models/OrderInvoiceRequest.js";
import { OrderJobRun, type IOrderJobRun } from "../models/OrderJobRun.js";
import { createInvoiceBatchWorker, getInvoiceBatchQueue } from "../queue.js";
import {
  createInvoiceRequest,
  failUnclaimedInvoiceRequests,
  getInvoiceRequest,
  listInvoiceRequests,
  runInvoiceBatchJob,
  startInvoiceRequestReaper,
  stopInvoiceRequest,
  type InvoiceBatchDeps,
} from "../services/invoiceRequests.js";
import type { InvoiceTarget } from "../services/invoiceOnDemand.js";
import { OrderApiError } from "../services/orderApi.js";
import { clearWorkerHeartbeat, RequestInputError, startWorkerHeartbeat, WORKER_NOT_PICKED_UP, workerAlive } from "../services/requestCommon.js";

const IS_WORKER = process.argv.includes("--worker");
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ── the stubs (both processes) ──────────────────────────────────────────────

/** ODV-<ACCOUNT>-<n>-<KIND> belongs to an invented account; anything else is not in "Order Units". */
function inventedTarget(userId: string, orderId: string): InvoiceTarget | null {
  const m = orderId.match(/^ODV-([A-Z0-9]+)-/);
  if (!m) return null;
  return { userId, accountId: `verify-acct-${m[1].toLowerCase()}`, email: `${m[1].toLowerCase()}@verify.invalid`, orderId, unitIds: [`${orderId}-U1`], rowIds: [] };
}

function stubs(queueName: string, ledgerCollection: string, origin: string): Partial<InvoiceBatchDeps> {
  const ledger = (mongoose.models.OrderJobRunVerify as mongoose.Model<IOrderJobRun>) || mongoose.model<IOrderJobRun>("OrderJobRunVerify", OrderJobRun.schema, ledgerCollection);
  return {
    queueName,
    ledger,
    pauseMs: 0,
    storeReady: () => true,
    resolveOrders: async (userId, orderIds) => {
      const out = new Map<string, InvoiceTarget>();
      for (const id of orderIds) {
        const t = inventedTarget(userId, id);
        if (t) out.set(id, t);
      }
      return out;
    },
    openSession: async (accountId) => (accountId === "verify-acct-nosession" ? null : playwrightRequest.newContext()),
    // The real download's own checks (invoiceCapture.downloadInvoiceViaApi), against the fake; nothing is stored.
    download: async (ctx, target) => {
      const res = await ctx.get(`${origin}/api/1/self-serve/invoice/download/${target.orderId}`, { timeout: 20_000 });
      if (!res.ok()) throw new OrderApiError(`invoice download for ${target.orderId} answered HTTP ${res.status()}`, res.status(), "");
      const body = await res.body();
      if (body.subarray(0, 4).toString("latin1") !== "%PDF") throw new Error(`invoice download for ${target.orderId} is neither PDF nor zip (${res.headers()["content-type"] || "?"}, ${body.length} bytes)`);
      return { size: body.length, parts: ["product"] };
    },
  };
}

// ── the worker process ──────────────────────────────────────────────────────

async function workerMain() {
  const queueName = String(process.env.VERIFY_QUEUE);
  await mongoose.connect(config.mongoUri, { serverSelectionTimeoutMS: 15_000 });
  const deps = stubs(queueName, String(process.env.VERIFY_LEDGER), String(process.env.VERIFY_ORIGIN));
  const worker = createInvoiceBatchWorker(Number(process.env.VERIFY_CONCURRENCY) || 4, (job) => runInvoiceBatchJob(job, deps), { queueName, lockDuration: 4000, stalledInterval: 2000 });
  worker.on("failed", (job, err) => console.log(`job ${job?.id} failed: ${err.message}`));
  startWorkerHeartbeat(queueName);
  startInvoiceRequestReaper(queueName);
  await worker.waitUntilReady();
  console.log("READY");
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
  const queueName = `verify-invoice-batch-${tag}`;
  const ledgerCollection = `order_job_runs_verify_${tag}`;
  const userId = String(new Types.ObjectId()); // a workspace that does not exist
  await mongoose.connect(config.mongoUri, { serverSelectionTimeoutMS: 15_000 });

  // The fake Flipkart: the invoice download, answering by the order id's suffix.
  const hits = new Map<string, number>();
  let delayMs = 30;
  const server = http.createServer((req, res) => {
    const orderId = decodeURIComponent((req.url || "").split("/").pop() || "");
    hits.set(orderId, (hits.get(orderId) || 0) + 1);
    const kind = orderId.split("-").pop();
    setTimeout(() => {
      if (kind === "NA") res.writeHead(404, { "content-type": "application/json" }).end('{"ERROR_MESSAGE":"Invoice not found"}');
      else if (kind === "RL") res.writeHead(429).end("slow down");
      else if (kind === "DOWN") res.writeHead(500).end("oops");
      else if (kind === "DEAD") res.writeHead(401).end("unauthorised");
      else if (kind === "HTML") res.writeHead(200, { "content-type": "text/html" }).end("<html><body>Login</body></html>");
      else res.writeHead(200, { "content-type": "application/octet-stream" }).end(Buffer.from(`%PDF-1.4 verify ${orderId}`));
    }, delayMs);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

  const deps = stubs(queueName, ledgerCollection, origin);
  const Ledger = deps.ledger!;
  const queue = getInvoiceBatchQueue(queueName);
  const reqOf = async (id: string) => (await OrderInvoiceRequest.findById(id).lean())!;
  const ledgerOf = async (id: string) => (await Ledger.findOne({ userId, type: "invoices", refId: id }).lean())!;
  /** The 3-hourly sweep's single-flight check, word for word (services/invoiceSweep.ts), on the throwaway ledger. */
  const sweepWouldSkip = async () => Boolean(await Ledger.findOne({ userId, type: "invoices", status: "running" }).select("_id").lean());
  const taskOf = (r: Awaited<ReturnType<typeof reqOf>>, orderId: string) => r.tasks.find((t) => t.orderId === orderId)!;

  let child: ChildProcess | null = null;
  const startWorker = async (concurrency: number) => {
    const p = spawn(process.execPath, [...process.execArgv, fileURLToPath(import.meta.url), "--worker"], {
      env: { ...process.env, VERIFY_QUEUE: queueName, VERIFY_LEDGER: ledgerCollection, VERIFY_ORIGIN: origin, VERIFY_CONCURRENCY: String(concurrency) },
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
    return p;
  };
  const killWorker = async () => {
    const p = child;
    if (!p || p.exitCode != null) return;
    const gone = new Promise((resolve) => p.once("exit", resolve));
    p.kill("SIGKILL");
    await gone;
    child = null;
  };

  try {
    console.log(`\nworkspace ${userId} (invented) · queue ${queueName} · ledger ${ledgerCollection} · fake Flipkart ${origin}\n`);

    // ── A: nobody is listening ───────────────────────────────────────────────
    console.log("A — no worker on the queue");
    const known = ["ODV-A1-1-OK", "ODV-A1-2-OK", "ODV-A1-3-OK", "ODV-A2-1-OK", "ODV-A2-2-OK"];
    const a = await createInvoiceRequest(userId, { orderIds: [...known, "odx-unknown-1", "ODV-A1-1-OK"].join(" "), by: "verify" }, deps);
    await check("a request is created queued: one task per distinct order, an unknown order failed at once, one job per account", async () => {
      assert.equal(a.status, "queued");
      assert.equal(a.task_type, "invoice_fetch");
      assert.equal(a.platform, "FLIPKART");
      assert.equal(a.source, "user");
      assert.equal(a.triggered_by, "verify");
      assert.deepEqual(a.params, { order_ids_count: 6, force: false });
      assert.deepEqual(a.counts, { total: 6, queued: 5, in_progress: 0, success: 0, failed: 1, cancelled: 0, aborted: 0, timed_out: 0 });
      assert.deepEqual({ tasks_created: a.tasks_created, accounts: a.accounts, not_found: a.not_found, already_queued: a.already_queued }, { tasks_created: 6, accounts: 2, not_found: 1, already_queued: 0 });
      const r = await reqOf(a.request_id);
      const unknown = taskOf(r, "ODX-UNKNOWN-1");
      assert.deepEqual({ status: unknown.status, errorClass: unknown.errorClass, error: unknown.error, email: unknown.email }, { status: "failed", errorClass: "order_not_found", error: "Order not found on this account", email: "" });
      assert.equal(taskOf(r, "ODV-A2-1-OK").email, "a2@verify.invalid");
      assert.equal(r.startedAt, null);
      assert.equal(await queue.getWaitingCount(), 2);
      const l = await ledgerOf(a.request_id);
      assert.deepEqual({ status: l.status, total: l.total, trigger: l.trigger, by: l.by, startedAt: l.startedAt, finishedAt: l.finishedAt }, { status: "queued", total: 6, trigger: "user", by: "verify", startedAt: null, finishedAt: null });
      assert.deepEqual((l.meta as { params?: unknown }).params, { order_ids_count: 6, force: false });
      assert.equal(await sweepWouldSkip(), false, "a queued request must not hold the sweep");
    });
    let extraId = "";
    let forcedId = "";
    await check("without force, orders another open request holds are left to it; with force they are queued again", async () => {
      await assert.rejects(
        () => createInvoiceRequest(userId, { orderIds: known }, deps),
        (err: unknown) => err instanceof RequestInputError && err.status === 409 && err.message === "All 5 order(s) are already queued in another invoice request"
      );
      const extra = await createInvoiceRequest(userId, { orderIds: [...known, "ODV-A3-1-OK"] }, deps);
      extraId = extra.request_id;
      assert.deepEqual({ tasks_created: extra.tasks_created, already_queued: extra.already_queued, total: extra.counts.total }, { tasks_created: 1, already_queued: 5, total: 1 });
      assert.deepEqual([...extra.already_queued_order_ids].sort(), [...known].sort());
      const forced = await createInvoiceRequest(userId, { orderIds: known, force: true }, deps);
      forcedId = forced.request_id;
      assert.deepEqual({ tasks_created: forced.tasks_created, already_queued: forced.already_queued, force: forced.params.force }, { tasks_created: 5, already_queued: 0, force: true });
    });
    await check("a worker heartbeat on the queue = the request is only waiting: nothing is failed", async () => {
      const beat = startWorkerHeartbeat(queueName);
      clearInterval(beat);
      await waitFor("the heartbeat key", () => workerAlive(queueName), 5000);
      assert.deepEqual(await failUnclaimedInvoiceRequests({ pickupMs: 0, queueName, ledger: Ledger }), { failed: 0, parked: 0 });
      assert.equal((await reqOf(a.request_id)).status, "queued");
      await clearWorkerHeartbeat(queueName);
      assert.equal(await workerAlive(queueName), false);
    });
    await check("no heartbeat, but the request is younger than the pick-up time: still left alone", async () => {
      assert.deepEqual(await failUnclaimedInvoiceRequests({ queueName, ledger: Ledger }), { failed: 0, parked: 0 });
      assert.equal((await reqOf(a.request_id)).status, "queued");
    });
    await check(`no heartbeat and past the pick-up time → failed: "${WORKER_NOT_PICKED_UP}"`, async () => {
      assert.deepEqual(await failUnclaimedInvoiceRequests({ pickupMs: 0, queueName, ledger: Ledger }), { failed: 3, parked: 0 });
      const r = await reqOf(a.request_id);
      assert.equal(r.status, "completed");
      assert.equal(r.message, WORKER_NOT_PICKED_UP);
      assert.ok(r.finishedAt instanceof Date);
      assert.equal(r.startedAt, null, "it never started");
      assert.deepEqual({ ...r.counts }, { total: 6, queued: 0, in_progress: 0, success: 0, failed: 6, cancelled: 0, aborted: 0, timed_out: 0 });
      for (const id of known) {
        const t = taskOf(r, id);
        assert.deepEqual({ status: t.status, errorClass: t.errorClass, error: t.error }, { status: "failed", errorClass: "internal_error", error: WORKER_NOT_PICKED_UP });
        assert.ok(t.finishedAt instanceof Date);
      }
      assert.equal(taskOf(r, "ODX-UNKNOWN-1").errorClass, "order_not_found", "an order already answered keeps its own reason");
      const l = await ledgerOf(a.request_id);
      assert.deepEqual({ status: l.status, message: l.message, failed: l.failed, processed: l.processed }, { status: "failed", message: WORKER_NOT_PICKED_UP, failed: 6, processed: 6 });
      assert.ok(l.finishedAt instanceof Date);
      assert.equal(l.failedItems.length, 6);
      assert.equal((await reqOf(extraId)).status, "completed");
      assert.equal((await reqOf(forcedId)).status, "completed");
      const jobs = await queue.getJobCounts("waiting", "active", "delayed");
      assert.deepEqual({ waiting: jobs.waiting, active: jobs.active, delayed: jobs.delayed }, { waiting: 0, active: 0, delayed: 0 }, "its jobs are gone: a later restart runs nothing already answered");
      assert.equal(await Ledger.countDocuments({ userId, status: { $in: ["queued", "running"] } }), 0, "no ledger row is left open");
    });
    await check("a request whose orders are all unknown ends at once, with nothing queued", async () => {
      const r = await createInvoiceRequest(userId, { orderIds: "406-1664516-7286725 ODX-2" }, deps);
      assert.equal(r.status, "completed");
      assert.equal(r.message, "None of these orders is in Order Units");
      assert.deepEqual({ failed: r.counts.failed, queued: r.counts.queued, accounts: r.accounts }, { failed: 2, queued: 0, accounts: 0 });
      assert.equal((await ledgerOf(r.request_id)).status, "completed");
      assert.equal(await queue.getWaitingCount(), 0);
    });
    await check("stopping a queued request with no worker: every order cancelled, its jobs removed", async () => {
      const r = await createInvoiceRequest(userId, { orderIds: ["ODV-A1-1-OK", "ODV-A2-1-OK"], by: "verify" }, deps);
      const stopped = await stopInvoiceRequest(userId, r.request_id, "verify", deps);
      assert.deepEqual({ status: stopped?.status, removed: stopped?.removed, cancelled: stopped?.counts.cancelled, message: stopped?.message }, { status: "cancelled", removed: 2, cancelled: 2, message: "Stopped by verify" });
      const row = await reqOf(r.request_id);
      assert.ok(row.tasks.every((t) => t.status === "cancelled" && t.error === "Cancelled before it started"));
      assert.equal(row.stoppedBy, "verify");
      assert.equal((await ledgerOf(r.request_id)).status, "stopped");
      assert.equal(await stopInvoiceRequest(userId, String(new Types.ObjectId()), "verify", deps), null);
    });

    // ── B: a normal run ─────────────────────────────────────────────────────
    console.log("\nB — a worker process takes the batch");
    await startWorker(4);
    const ids = [
      "ODV-A1-1-OK", "ODV-A1-2-OK", "ODV-A1-3-OK", "ODV-A1-4-OK",
      "ODV-A2-1-OK", "ODV-A2-2-NA", "ODV-A2-3-OK", "ODV-A2-4-OK",
      "ODV-A3-1-OK", "ODV-A3-2-DOWN", "ODV-A3-3-HTML", "ODV-A3-4-OK",
      "ODV-A4-1-OK", "ODV-A4-2-DEAD", "ODV-A4-3-OK", "ODV-A4-4-OK",
      "ODV-A5-1-RL", "ODV-A5-2-OK",
      "ODV-NOSESSION-1-OK", "ODV-NOSESSION-2-OK",
      "ODX-NOT-OURS-1", "ODX-NOT-OURS-2",
    ];
    hits.clear();
    const b = await createInvoiceRequest(userId, { orderIds: ids, by: "verify" }, deps);
    let sawRunning = false;
    let sawSweepHeld = false;
    const done = await waitFor(
      "request B to complete",
      async () => {
        const l = await ledgerOf(b.request_id);
        if (l.status === "running") {
          sawRunning = true;
          if (await sweepWouldSkip()) sawSweepHeld = true;
        }
        const r = await reqOf(b.request_id);
        return r.status === "completed" ? r : null;
      },
      60_000
    );
    await check("every order ends with its own result and error class", () => {
      const expect: Record<string, [string, string, string]> = {
        "ODV-A1-1-OK": ["success", "", ""],
        "ODV-A2-2-NA": ["failed", "nothing_available", "Nothing available yet"],
        "ODV-A2-3-OK": ["success", "", ""],
        "ODV-A3-2-DOWN": ["failed", "platform_unavailable", "Platform did not respond; try again later"],
        "ODV-A3-3-HTML": ["failed", "unexpected_response", "Platform returned a response we could not read"],
        "ODV-A3-4-OK": ["success", "", ""],
        "ODV-A4-1-OK": ["success", "", ""],
        "ODV-A4-2-DEAD": ["failed", "not_logged_in", "Account is not logged in"],
        "ODV-A4-3-OK": ["failed", "not_logged_in", "Account is not logged in"],
        "ODV-A4-4-OK": ["failed", "not_logged_in", "Account is not logged in"],
        "ODV-A5-1-RL": ["failed", "rate_limited", "Platform is limiting requests; try again later"],
        "ODV-A5-2-OK": ["success", "", ""],
        "ODV-NOSESSION-1-OK": ["failed", "not_logged_in", "Account is not logged in"],
        "ODV-NOSESSION-2-OK": ["failed", "not_logged_in", "Account is not logged in"],
        "ODX-NOT-OURS-1": ["failed", "order_not_found", "Order not found on this account"],
      };
      for (const [orderId, [status, errorClass, error]] of Object.entries(expect)) {
        const t = taskOf(done, orderId);
        assert.deepEqual([t.status, t.errorClass, t.error], [status, errorClass, error], orderId);
        assert.ok(t.finishedAt instanceof Date, `${orderId} has no finish time`);
      }
      const ok = taskOf(done, "ODV-A1-1-OK");
      assert.deepEqual(ok.parts, ["product"]);
      assert.ok(ok.startedAt instanceof Date && ok.finishedAt! >= ok.startedAt);
      assert.equal(taskOf(done, "ODV-A2-2-NA").detail, "invoice download for ODV-A2-2-NA answered HTTP 404");
      assert.match(taskOf(done, "ODV-A3-3-HTML").detail, /neither PDF nor zip \(text\/html/);
    });
    await check("the counts match the tasks: 11 downloaded, 11 failed, nothing left", () => {
      assert.deepEqual({ ...done.counts }, { total: 22, queued: 0, in_progress: 0, success: 11, failed: 11, cancelled: 0, aborted: 0, timed_out: 0 });
      const byStatus: Record<string, number> = {};
      for (const t of done.tasks) byStatus[t.status] = (byStatus[t.status] || 0) + 1;
      assert.deepEqual(byStatus, { success: 11, failed: 11 });
      assert.ok(done.startedAt instanceof Date && done.finishedAt instanceof Date && done.finishedAt >= done.startedAt);
      assert.ok(done.workerHost);
    });
    await check("Flipkart is asked once per order — and not at all once the session is known dead, or has no session", () => {
      for (const id of ["ODV-A1-1-OK", "ODV-A2-2-NA", "ODV-A3-2-DOWN", "ODV-A4-2-DEAD", "ODV-A5-1-RL"]) assert.equal(hits.get(id), 1, id);
      for (const id of ["ODV-A4-3-OK", "ODV-A4-4-OK", "ODV-NOSESSION-1-OK", "ODX-NOT-OURS-1"]) assert.equal(hits.get(id) || 0, 0, id);
    });
    await check("the ledger row: running only while it ran (the sweep waits on that), then completed with the failed orders listed", async () => {
      assert.ok(sawRunning, "the row was never seen running");
      assert.ok(sawSweepHeld, "the sweep's check did not see the running batch");
      // The row is closed by the write after the one that completes the request.
      const l = await waitFor("the ledger row of B to close", async () => {
        const row = await ledgerOf(b.request_id);
        return row.status === "running" ? null : row;
      }, 5000);
      assert.deepEqual({ status: l.status, total: l.total, processed: l.processed, success: l.success, failed: l.failed, cancelled: l.cancelled }, { status: "completed", total: 22, processed: 22, success: 11, failed: 11, cancelled: 0 });
      assert.ok(l.startedAt instanceof Date && l.finishedAt instanceof Date);
      assert.ok(l.avgMsPerItem > 0);
      assert.equal(l.failedItems.length, 11);
      assert.ok(l.failedItems.some((f) => f.key === "ODV-A2-2-NA · a2@verify.invalid" && f.reason === "Nothing available yet"));
      assert.equal(await sweepWouldSkip(), false, "a finished batch must not hold the sweep");
    });
    await check("read back in the reference's shape (GET …/invoice/requests/:id and the list)", async () => {
      const pub = (await getInvoiceRequest(userId, b.request_id))!;
      assert.deepEqual(
        Object.keys(pub).sort(),
        ["avg_seconds_per_task", "can_cancel", "counts", "created_at", "duration_seconds", "eta_seconds", "finished_at", "finished_count", "host", "message", "params", "parent_id", "platform", "progress_percent", "request_id", "retry_of", "source", "stalled", "stalled_message", "started_at", "status", "task_type", "tasks", "triggered_by", "worker_alive", "worker_host"].sort()
      );
      assert.deepEqual([pub.worker_alive, pub.stalled, pub.stalled_message], [null, false, ""], "a finished request is never stalled");
      assert.deepEqual({ status: pub.status, finished_count: pub.finished_count, progress_percent: pub.progress_percent, can_cancel: pub.can_cancel, eta_seconds: pub.eta_seconds }, { status: "completed", finished_count: 22, progress_percent: 100, can_cancel: false, eta_seconds: null });
      const t = pub.tasks!.find((x) => x.order_id === "ODV-A2-2-NA")!;
      assert.deepEqual({ status: t.status, error: t.error, error_code: t.error_code, email: t.email, task_type: t.task_type, platform: t.platform, source: t.source, triggered_by: t.triggered_by }, { status: "failed", error: "Nothing available yet", error_code: "nothing_available", email: "a2@verify.invalid", task_type: "invoice_fetch", platform: "FLIPKART", source: "user", triggered_by: "verify" });
      const list = await listInvoiceRequests(userId, 50);
      assert.ok(list.length >= 5 && list[0].request_id === b.request_id && !("tasks" in list[0]));
      assert.equal(await getInvoiceRequest(String(new Types.ObjectId()), b.request_id), null, "another workspace cannot read it");
    });

    // ── C: stop ─────────────────────────────────────────────────────────────
    console.log("\nC — stop while it runs (6 accounts, 4 at once)");
    delayMs = 350;
    hits.clear();
    const stopIds = ["S1", "S2", "S3", "S4", "S5", "S6"].flatMap((acct) => [1, 2, 3, 4, 5].map((n) => `ODV-${acct}-${n}-OK`));
    const c = await createInvoiceRequest(userId, { orderIds: stopIds, by: "verify" }, deps);
    await waitFor("three orders of C to finish", async () => ((await reqOf(c.request_id)).counts.success >= 3 ? true : null), 30_000, 50);
    const stopped = (await stopInvoiceRequest(userId, c.request_id, "verify", deps))!;
    const afterStop = await waitFor("the orders in flight to finish", async () => {
      const r = await reqOf(c.request_id);
      return r.counts.in_progress === 0 ? r : null;
    });
    await check("queued orders → cancelled \"Cancelled before it started\"; the ones being downloaded finished", () => {
      assert.equal(afterStop.status, "cancelled");
      assert.equal(afterStop.message, "Stopped by verify");
      assert.equal(stopped.removed, 2, "the two accounts no worker had taken yet leave the queue");
      const c2 = afterStop.counts;
      assert.equal(c2.queued, 0);
      assert.equal(c2.failed + c2.aborted + c2.timed_out, 0);
      assert.equal(c2.success + c2.cancelled, 30);
      assert.ok(c2.cancelled >= 10 && c2.success >= 3, `success ${c2.success}, cancelled ${c2.cancelled}`);
      for (const t of afterStop.tasks) {
        if (t.status === "cancelled") {
          assert.deepEqual([t.error, t.startedAt, hits.get(t.orderId) || 0], ["Cancelled before it started", null, 0], t.orderId);
        } else {
          assert.equal(t.status, "success", t.orderId);
          assert.equal(hits.get(t.orderId), 1, t.orderId);
        }
      }
    });
    await check("after a stop: the ledger reads stopped, the queue is empty, and a second stop changes nothing", async () => {
      await waitFor("the queue to drain", async () => {
        const n = await queue.getJobCounts("waiting", "active", "delayed");
        return n.waiting + n.active + n.delayed === 0;
      });
      const l = await waitFor("the ledger row to settle", async () => {
        const row = await ledgerOf(c.request_id);
        return row.success === afterStop.counts.success ? row : null;
      });
      assert.deepEqual({ status: l.status, message: l.message, cancelled: l.cancelled, processed: l.processed }, { status: "stopped", message: "Stopped by verify", cancelled: afterStop.counts.cancelled, processed: 30 });
      assert.equal(await sweepWouldSkip(), false);
      const again = (await stopInvoiceRequest(userId, c.request_id, "someone else", deps))!;
      assert.deepEqual({ status: again.status, removed: again.removed, message: again.message }, { status: "cancelled", removed: 0, message: "Stopped by verify" });
      assert.deepEqual({ ...(await reqOf(c.request_id)).counts }, { ...afterStop.counts });
    });

    // ── D: the worker dies, then comes back ─────────────────────────────────
    console.log("\nD — the worker is killed mid-batch, then restarted");
    delayMs = 300;
    hits.clear();
    const resumeIds = ["R1", "R2", "R3"].flatMap((acct) => [1, 2, 3, 4, 5, 6, 7, 8].map((n) => `ODV-${acct}-${n}-OK`));
    const d = await createInvoiceRequest(userId, { orderIds: resumeIds, by: "verify" }, deps);
    await waitFor("five orders of D to finish", async () => ((await reqOf(d.request_id)).counts.success >= 5 ? true : null), 30_000, 50);
    await killWorker();
    await sleep(500);
    const atKill = await reqOf(d.request_id);
    const doneBefore = new Map(atKill.tasks.filter((t) => t.status === "success").map((t) => [t.orderId, t.finishedAt!.getTime()]));
    await check("the kill leaves the request open, orders still running or queued, and the ledger row still 'running'", async () => {
      assert.equal(atKill.status, "in_progress");
      assert.ok(atKill.counts.queued + atKill.counts.in_progress > 0);
      assert.ok(doneBefore.size >= 5 && doneBefore.size < 24);
      assert.equal((await ledgerOf(d.request_id)).status, "running");
    });
    await check("with the worker gone, the row stops reading 'running' (the sweep is free) and the request stays resumable", async () => {
      await clearWorkerHeartbeat(queueName); // what its 75 s expiry does
      assert.deepEqual(await failUnclaimedInvoiceRequests({ pickupMs: 0, queueName, ledger: Ledger }), { failed: 0, parked: 1 });
      const l = await ledgerOf(d.request_id);
      assert.equal(l.status, "failed");
      assert.match(l.message, /^The worker stopped with \d+ order\(s\) left — restart the worker to resume$/);
      assert.ok(l.finishedAt instanceof Date);
      assert.equal(await sweepWouldSkip(), false);
      const r = await reqOf(d.request_id);
      assert.equal(r.status, "in_progress");
      assert.equal(r.counts.success, doneBefore.size, "nothing was touched");
      // Whoever shows the request can tell it from a running one: both reads say no worker is behind it.
      const left = r.counts.queued + r.counts.in_progress;
      const stalled = { status: "in_progress", worker_alive: false, stalled: true, stalled_message: `The worker stopped with ${left} order(s) left — restart the worker to resume` };
      const one = (await getInvoiceRequest(userId, d.request_id))!;
      assert.deepEqual({ status: one.status, worker_alive: one.worker_alive, stalled: one.stalled, stalled_message: one.stalled_message }, stalled);
      const listed = (await listInvoiceRequests(userId, 50)).find((x) => x.request_id === d.request_id)!;
      assert.deepEqual({ status: listed.status, worker_alive: listed.worker_alive, stalled: listed.stalled, stalled_message: listed.stalled_message }, stalled);
      assert.deepEqual(await failUnclaimedInvoiceRequests({ pickupMs: 0, queueName, ledger: Ledger }), { failed: 0, parked: 0 }, "parked once, not every tick");
    });
    await startWorker(4);
    const resumed = await waitFor("request D to complete after the restart", async () => {
      const r = await reqOf(d.request_id);
      return r.status === "completed" ? r : null;
    }, 90_000);
    await check("a restarted worker finishes it: every order downloaded, the finished ones not redone", async () => {
      assert.deepEqual({ ...resumed.counts }, { total: 24, queued: 0, in_progress: 0, success: 24, failed: 0, cancelled: 0, aborted: 0, timed_out: 0 });
      for (const [orderId, finishedAt] of doneBefore) {
        assert.equal(taskOf(resumed, orderId).finishedAt!.getTime(), finishedAt, `${orderId} was written again`);
        assert.equal(hits.get(orderId), 1, `${orderId} was downloaded again`);
      }
      const total = [...hits.values()].reduce((x, y) => x + y, 0);
      // Only an order that was in flight when the worker died can be asked for twice: at most one per account.
      assert.ok(total >= 24 && total <= 27, `${total} downloads for 24 orders`);
      const l = await waitFor("the ledger row of D to close", async () => {
        const row = await ledgerOf(d.request_id);
        return row.status === "running" ? null : row;
      }, 5000);
      assert.deepEqual({ status: l.status, success: l.success, failed: l.failed, processed: l.processed }, { status: "completed", success: 24, failed: 0, processed: 24 });
      assert.equal(await Ledger.countDocuments({ userId, status: { $in: ["queued", "running"] } }), 0, "no ledger row is left open");
    });
  } finally {
    // Everything this run made: the invented workspace's requests, the throwaway ledger, the throwaway queue.
    await killWorker();
    server.close();
    const removed = await OrderInvoiceRequest.deleteMany({ userId: new Types.ObjectId(userId) });
    await mongoose.connection.db!.dropCollection(ledgerCollection).catch(() => undefined);
    await clearWorkerHeartbeat(queueName);
    await queue.obliterate({ force: true }).catch(() => undefined);
    await queue.close().catch(() => undefined);
    const left = await OrderInvoiceRequest.countDocuments({ userId: new Types.ObjectId(userId) });
    const stray = (await getRedis().keys(`bull:${queueName}:*`)).length;
    console.log(`\ncleanup: ${removed.deletedCount} request(s) of the invented workspace deleted (${left} left), ledger collection dropped, queue obliterated (${stray} key(s) left)`);
    if (left || stray) failures.push("cleanup");
    await mongoose.disconnect();
  }
  console.log(`${passed} passed, ${failures.length} failed`);
  process.exit(failures.length ? 1 : 0);
}

(IS_WORKER ? workerMain() : driverMain()).catch((err) => {
  console.error(err);
  process.exit(1);
});
