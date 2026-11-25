/**
 * The request log — Batch requests and All tasks — WITHOUT Flipkart, without a
 * worker, without a queue:
 *
 *   · every task_type and every task status, written the three ways the log is
 *     written (a fetch's own rows, marks, rows brought in line from a document)
 *   · a request's own counters = its task rows, for every kind
 *   · GET /sync/batches: the reference's keys, newest first across the six
 *     kinds, every filter, paging, India-time days
 *   · GET /sync/batches/:id (settings as labels, the Request-cell line) and
 *     /issues (grouped by error class)
 *   · GET /sync/tasks: every filter, paging, the full-id rule, the 7-day cut
 *   · ETA from the recent rate; a request nothing is behind is closed and its
 *     tasks time out — only this machine's
 *
 * What is real: Mongo. Every collection this run writes is a THROWAWAY one
 * (verify_rl_<run>_…) with the real schemas and, for the task log, the real
 * indexes; all of them are dropped at the end. The rows belong to invented
 * workspace ids. The real collections are not written.
 *
 *   npm run test:request-log
 *   npm run test:request-log -- --real             also: read-only timing and plans of the two lists on the real collections
 *   npm run test:request-log -- --volume=150000    also: the task list over that many throwaway rows (timing + plans)
 */
import assert from "node:assert/strict";
import mongoose, { Types, type Model } from "mongoose";
import { config } from "../config.js";
import { CancelRequest } from "../models/CancelRequest.js";
import { OrderCouponRequest, OrderCouponTask } from "../models/OrderCouponRequest.js";
import { OrderInvoiceRequest, REQUEST_TASK_STATUSES, type RequestCounts } from "../models/OrderInvoiceRequest.js";
import { OrderJobRun } from "../models/OrderJobRun.js";
import { OrderSyncTask, SYNC_TASK_KEEP_MS, SYNC_TASK_KINDS, type SyncTaskKind } from "../models/OrderSyncTask.js";
import { OrderUpdateRequest } from "../models/OrderUpdateRequest.js";
import { RescheduleRequest } from "../models/RescheduleRequest.js";
import { closeTimedOut, keeperPass } from "../services/requestLogKeeper.js";
import { ABORTED_WHILE_RUNNING, CANCELLED_BEFORE_START, classifyMessage, countsOf, emptyCounts, ERROR_CLASSES, RequestInputError, TIMED_OUT } from "../services/requestCommon.js";
import { batchIssues, errorClassList, ETA_MODE, findRequest, getBatch, istDayBound, listBatches, listTasks, livenessOfRequest, noteProgress, progressOf, QueueWatch, recentRate, shortId, type LiveProbes } from "../services/syncBatches.js";
import { closeUpdateAccountRows, countsOfCancel, countsOfReschedule, countsOfUpdate, loadFacts, logCounts, logDefaults, openFetchTasks, sameCounts, seedsOfInvoice, syncRequestTasks, syncRequestTasksQuietly, syncRequestTasksSoon, taskLog, updateTaskKey, type LogDeps } from "../services/syncTaskLog.js";
import { workspaceUserId } from "../services/sessionStore.js";

const REAL = process.argv.includes("--real");
const VOLUME = Number((process.argv.find((a) => a.startsWith("--volume=")) || "").split("=")[1]) || 0;

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

const oid = () => new Types.ObjectId();
const MIN = 60_000;
const DAY = 24 * 60 * MIN;
const ago = (ms: number) => new Date(Date.now() - ms);

// ── throwaway collections ───────────────────────────────────────────────────

const run = Date.now().toString(36);
const names: string[] = [];

function throwaway<T>(real: Model<T>, name: string): Model<T> {
  const coll = `verify_rl_${run}_${name}`;
  names.push(coll);
  return mongoose.model<T>(`VerifyRl${name}${run}`, real.schema, coll) as Model<T>;
}

function makeDeps(): LogDeps {
  return {
    Tasks: throwaway(OrderSyncTask, "tasks"),
    Ledger: throwaway(OrderJobRun, "ledger"),
    Update: throwaway(OrderUpdateRequest, "update"),
    Invoice: throwaway(OrderInvoiceRequest, "invoice"),
    Coupon: throwaway(OrderCouponRequest, "coupon"),
    CouponTask: throwaway(OrderCouponTask, "coupontask"),
    Cancel: throwaway(CancelRequest, "cancel"),
    Reschedule: throwaway(RescheduleRequest, "reschedule"),
  };
}

const HOST = "verify-host";
const PID = 4242;
const SYSTEM = "verify-system";

function probes(aliveQueues: string[] = [], fetchLive = false): LiveProbes {
  return { queueHasWorker: async (q) => aliveQueues.includes(q), fetchLive: () => fetchLive, host: HOST, pid: PID, systemId: SYSTEM };
}

const BATCH_KEYS = ["request_id", "platform", "task_type", "source", "status", "triggered_by", "parent_id", "child_ids", "params", "created_at", "started_at", "finished_at", "counts", "finished_count", "progress_percent", "avg_seconds_per_task", "eta_seconds", "duration_seconds", "can_cancel", "can_retry"];
const TASK_KEYS = ["id", "request_id", "platform", "task_type", "source", "status", "email", "order_id", "unit_id", "triggered_by", "created_at", "started_at", "finished_at", "duration_seconds", "error", "error_code"];
const COUNT_KEYS = ["total", "queued", "in_progress", "success", "failed", "cancelled", "aborted", "timed_out"];

async function main() {
  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false });
  const deps = makeDeps();
  const userId = String(oid());
  const other = String(oid());
  const uid = new Types.ObjectId(userId);
  const ids: Record<string, string> = {};
  console.log(`invented workspace ${userId} · throwaway collections verify_rl_${run}_*\n`);

  try {
    // The task log gets its real indexes (the unique pair, the list indexes, the 7-day TTL).
    await deps.Tasks.createCollection();
    await deps.Tasks.createIndexes();

    // ── pure ────────────────────────────────────────────────────────────────
    console.log("PURE");
    await check("the 16 error classes, with the reference's labels", () => {
      const list = errorClassList();
      assert.equal(list.length, 16);
      assert.deepEqual(list[0], { code: "not_logged_in", label: "Account is not logged in" });
      assert.deepEqual(list[15], { code: "unknown", label: "Failed (no reason recorded)" });
      assert.equal(list.find((c) => c.code === "no_slots")?.label, "No delivery date available to reschedule to");
    });
    await check("stored sentences → error class (messages taken from the real request documents)", () => {
      const cases: Array<[string, boolean, string]> = [
        ["Session: Status is \"failed\", not logged_in — re-login this ID", false, "not_logged_in"],
        ["Flipkart session expired. Refresh this ID in platform id first.", false, "not_logged_in"],
        ["Session expired — re-login this ID", true, "not_logged_in"],
        ["Flipkart shows no such order on this Platform ID — is it the account that placed it?", true, "order_not_found"],
        ["cancel/data for OD339153026712514100/339153026712514100001 answered HTTP 500", true, "platform_unavailable"],
        ["page.goto: Timeout 30000ms exceeded.", false, "platform_unavailable"],
        ["order page gave no data for OD339180087105307100/339180087105307100000 (no status, timeline or product) — row left as it was", false, "unexpected_response"],
        ["getaddrinfo ENOTFOUND ac-ovgv7vf-shard-00-00.mfu7vu1.mongodb.net", false, "internal_error"],
        ["Flipkart did not offer 2026-10-25; offered: no dates recognised", true, "no_slots"],
        ["https://x answered HTTP 429", false, "rate_limited"],
        ["something Flipkart said", true, "platform_declined"],
        ["something we said", false, "internal_error"],
        ["", false, "unknown"],
      ];
      for (const [text, platform, code] of cases) assert.equal(classifyMessage(text, platform).errorClass, code, text);
      assert.equal(classifyMessage("page.goto: Timeout 30000ms exceeded.").error, ERROR_CLASSES.platform_unavailable);
    });
    await check("the three sentences of a task that did not run to its end", () => {
      assert.equal(CANCELLED_BEFORE_START, "Cancelled before it started");
      assert.equal(ABORTED_WHILE_RUNNING, "Stopped while running: the request was cancelled");
      assert.equal(TIMED_OUT, "Timed out: the agent went offline or the task ran too long");
    });
    await check("progress numbers are the reference's arithmetic (its own captured rows)", () => {
      const at = (iso: string) => new Date(iso);
      const c = (total: number, o: Partial<RequestCounts>) => ({ ...emptyCounts(0), total, ...o });
      // update a3cb9995: 466 tasks, 92 s → 0.2 s / task
      assert.deepEqual(pick(progressOf(c(466, { success: 466 }), at("2026-11-24T02:27:18.986Z"), at("2026-11-24T02:28:50.644Z")), ["finished_count", "progress_percent", "avg_seconds_per_task", "eta_seconds", "duration_seconds"]), { finished_count: 466, progress_percent: 100, avg_seconds_per_task: 0.2, eta_seconds: 0, duration_seconds: 92 });
      // invoice 5038520c, stopped: 88 done · 617 cancelled · 57 aborted in 48 s → 0.1
      assert.equal(progressOf(c(762, { success: 88, cancelled: 617, aborted: 57 }), at("2026-11-21T15:11:43.588Z"), at("2026-11-21T15:12:31.850Z")).avg_seconds_per_task, 0.1);
      // the running update 5e6c5433: 2537 of 3922 → 64.7 %
      const now = at("2026-11-24T05:51:34.000Z").getTime();
      const running = progressOf(c(3922, { queued: 1258, in_progress: 127, success: 2537 }), at("2026-11-24T05:48:51.015Z"), null, { now });
      assert.equal(running.progress_percent, 64.7);
      assert.equal(running.duration_seconds, 163);
      assert.deepEqual([running.eta_basis, running.eta_seconds], ["reference_rounded_average", 138], "the default ETA: the reference's own (its API answered 138 for this row)");
      assert.equal(progressOf(c(3922, { queued: 1258, in_progress: 127, success: 2537 }), at("2026-11-24T05:48:51.015Z"), null, { now, mode: "recent" }).eta_basis, "lifetime_average");
    });
    await check("ETA, BATCH_ETA=recent (ours): the recent rate once two readings exist, the lifetime average before", () => {
      const id = `eta-${run}`;
      const t0 = 1_800_000_000_000;
      const counts = (done: number) => ({ ...emptyCounts(0), total: 21166, success: done, in_progress: 100, queued: 21166 - done - 100 });
      // One reading: nothing recent to measure.
      noteProgress(id, 26, t0 + 13_000);
      assert.equal(recentRate(id, t0 + 13_000), null);
      const first = progressOf(counts(26), new Date(t0), null, { now: t0 + 13_000, rate: recentRate(id, t0 + 13_000), mode: "recent" });
      assert.equal(first.eta_basis, "lifetime_average");
      assert.equal(first.eta_seconds, Math.round((21166 - 26) / (26 / 13)));
      // Fifty seconds later 442 more have finished: 8.84 a second, not the lifetime 7.43.
      noteProgress(id, 468, t0 + 63_000);
      const rate = recentRate(id, t0 + 63_000);
      assert.ok(rate && Math.abs(rate - 442 / 50) < 1e-9, `rate ${rate}`);
      const second = progressOf(counts(468), new Date(t0), null, { now: t0 + 63_000, rate, mode: "recent" });
      assert.equal(second.eta_basis, "recent_rate");
      assert.equal(second.eta_seconds, Math.round((21166 - 468) / (442 / 50)));
      // A batch that has stalled gives no recent rate: back to the lifetime average.
      noteProgress(id, 468, t0 + 80_000);
      noteProgress(id, 468, t0 + 140_000);
      assert.equal(recentRate(id, t0 + 140_000), null);
      // An ended batch has no ETA.
      assert.equal(progressOf({ ...emptyCounts(0), total: 5, success: 5 }, new Date(t0), new Date(t0 + 5000)).eta_seconds, 0);
    });
    await check("ETA, the default (BATCH_ETA unset): the reference's own arithmetic — its three captured ETAs to the second, whatever rate was measured", () => {
      assert.equal(ETA_MODE, "reference");
      const t0 = 1_800_000_000_000;
      // A measured rate is passed as the list passes it: the default does not use it.
      const eta = (total: number, done: number, running: number, seconds: number) => progressOf({ ...emptyCounts(0), total, success: done, in_progress: running, queued: total - done - running }, new Date(t0), null, { now: t0 + seconds * 1000, rate: 8.84 }).eta_seconds;
      assert.equal(eta(21166, 26, 128, 13), 10570, "'~2h 56m left', 13 s in");
      assert.equal(eta(21166, 468, 89, 63), 2070, "'~34m 30s left', a minute later");
      assert.equal(eta(3922, 2537, 127, 163), 138, "eta_seconds 138 of the running update");
    });
    await check("Created from / Created to are India-time days", () => {
      assert.equal(istDayBound("2026-11-24", false).toISOString(), "2026-11-23T18:30:00.000Z");
      assert.equal(istDayBound("2026-11-24", true).toISOString(), "2026-11-24T18:29:59.999Z");
      assert.throws(() => istDayBound("24-11-2026", false), RequestInputError);
    });
    await check("short_id is the LAST 8 characters (neighbours share the first 8)", () => {
      const a = "6b0559003fb74e3ba92f71ea";
      const b = "6b0559003fb74e3ba92f71eb";
      assert.equal(a.slice(0, 8), b.slice(0, 8));
      assert.notEqual(shortId(a), shortId(b));
      assert.equal(shortId(a), "a92f71ea");
    });

    // ── fixtures: one request of every kind, every task status ───────────────
    console.log("\nWRITING THE LOG");
    const raw = <T>(m: Model<T>) => m.collection;
    const ledgerBase = { userId: uid, type: "fetch", trigger: "user", by: "verify@1gb.com", total: 0, processed: 0, success: 0, failed: 0, cancelled: 0, avgMsPerItem: 0, message: "", failedItems: [], refId: "", meta: {} };

    // 1. A fetch of this version, stopped while running: its own rows.
    {
      const _id = oid();
      ids.fetch = String(_id);
      const createdAt = ago(40 * MIN);
      await raw(deps.Ledger).insertOne({ ...ledgerBase, _id, status: "running", total: 5, startedAt: createdAt, finishedAt: null, createdAt, updatedAt: createdAt, meta: { host: HOST, pid: PID, platform: "FLIPKART", params: { platform: "FLIPKART", selection_method: "email_ids", email_ids_count: 6, fetch_orders_after_date: "2026-11-22", force_fetch: true, enable_login: false, run_update_after_fetch: true } } });
    }
    await check("fetch: rows opened with the run, marked per account, closed when it is stopped", async () => {
      const emails = ["f1", "f2", "f3", "f4", "f5"].map((n) => `${n}@verify.invalid`);
      const t = openFetchTasks({ userId, runId: ids.fetch, source: "user", by: "verify@1gb.com", emails, skipped: ["f6@verify.invalid"], createdAt: ago(40 * MIN) }, deps);
      t.started(emails[0]);
      t.succeeded(emails[0]);
      t.started(emails[1]);
      t.failed(emails[1], "Flipkart session expired. Refresh this ID in platform id first.");
      t.started(emails[2]);
      const { counts, written } = t.finish(true);
      await written;
      assert.deepEqual(counts, { total: 6, queued: 0, in_progress: 0, success: 1, failed: 2, cancelled: 2, aborted: 1, timed_out: 0 });
      await deps.Ledger.updateOne({ _id: ids.fetch }, { $set: { status: "stopped", finishedAt: new Date() } });
      const row = await deps.Ledger.findById(ids.fetch).lean();
      assert.deepEqual((row?.meta as { task_counts: RequestCounts }).task_counts, counts, "the counts are kept on the ledger row");
      assert.ok(sameCounts(await logCounts(ids.fetch, deps), counts), "rows = counts");
      const rows = await deps.Tasks.find({ requestId: ids.fetch }).lean();
      const by = Object.fromEntries(rows.map((r) => [r.email, r]));
      assert.deepEqual([by[emails[0]].status, by[emails[1]].status, by[emails[2]].status, by[emails[3]].status, by["f6@verify.invalid"].status], ["success", "failed", "aborted", "cancelled", "failed"]);
      assert.deepEqual([by[emails[1]].errorCode, by[emails[1]].error], ["not_logged_in", "Account is not logged in"]);
      assert.equal(by[emails[2]].error, ABORTED_WHILE_RUNNING);
      assert.equal(by[emails[3]].error, CANCELLED_BEFORE_START);
      assert.equal(by["f6@verify.invalid"].errorCode, "not_logged_in");
      assert.ok(rows.every((r) => r.orderId === "" && r.unitId === "" && r.taskType === "fetch" && r.kind === "fetch"), "a fetch task is one account, no order");
      assert.ok(by[emails[0]].startedAt && by[emails[0]].finishedAt, "taken and finished are both on the row though they were one write");
    });

    // 2. A fetch that ran before the log: its report and failed items are all there is.
    {
      const _id = oid();
      ids.fetchOld = String(_id);
      const createdAt = ago(2 * DAY);
      await raw(deps.Ledger).insertOne({ ...ledgerBase, _id, status: "completed", total: 341, processed: 341, success: 245, startedAt: createdAt, finishedAt: new Date(createdAt.getTime() + 40_000), createdAt, updatedAt: createdAt, failedItems: [{ key: "old1@verify.invalid", reason: "not logged in — skipped" }, { key: "old2@verify.invalid", reason: "page.goto: Timeout 30000ms exceeded." }], meta: { report: { accountsQueued: 8, accountsDone: 7 } } });
    }
    // 3. A fetch whose process is long gone and names no machine — the shape of the real row 6b02e9623fb74e3ba92c70c9.
    {
      const _id = oid();
      ids.fetchStale = String(_id);
      const createdAt = ago(2 * DAY);
      await raw(deps.Ledger).insertOne({ ...ledgerBase, _id, status: "running", total: 3, startedAt: createdAt, finishedAt: null, createdAt, updatedAt: createdAt, meta: { since: "2026-09-30T18:30:00.000Z", emails: ["s1@verify.invalid", "s2@verify.invalid", "s3@verify.invalid"], windows: 3, mode: "api" } });
    }
    // 4. A fetch still preparing its account list, on this machine and process.
    {
      const _id = oid();
      ids.fetchPreparing = String(_id);
      const createdAt = ago(5000);
      await raw(deps.Ledger).insertOne({ ...ledgerBase, _id, trigger: "system-cron", by: "schedule", status: "queued", total: 21251, startedAt: null, finishedAt: null, createdAt, updatedAt: createdAt, message: "Preparing 21251 account(s)", meta: { host: HOST, pid: PID, emails_count: 21251, params: { selection_method: "all_accounts", fetch_orders_after_date: "2026-11-23", force_fetch: false } } });
    }

    // 5. An update run by a worker that writes no rows: every account ending there is.
    const unit = (n: number) => ({ orderId: `ODV${run.toUpperCase()}${n}`, itemId: "", unitId: `9${String(n).padStart(6, "0")}`, orderUrl: "" });
    const account = (email: string, units: number[], status: string, o: Record<string, unknown> = {}) => ({ email, platformId: null, units: units.map(unit), total: units.length, status, done: 0, failed: 0, statusChanged: 0, otpNew: 0, via: "", message: "", startedAt: null, finishedAt: null, ...o });
    {
      const _id = oid();
      ids.update = String(_id);
      const createdAt = ago(30 * MIN);
      await raw(deps.Update).insertOne({
        _id, userId: uid, source: "manual", selectionMethod: "order_status", createdBy: "verify@1gb.com", status: "cancelled", mode: "api", windows: 3, parentId: "", systemId: SYSTEM,
        params: { platform: "FLIPKART", selection_method: "order_status", force: false, statuses_to_update: ["Shipped", "Out for delivery"], status_keys: ["Shipped", "ofd"], email_ids_count: 0, order_ids_count: 0, order_email_pairs_count: 0 },
        accounts: [
          account("u-done@verify.invalid", [1, 2, 3], "done", { done: 2, failed: 1, startedAt: ago(29 * MIN), finishedAt: ago(28 * MIN), message: "2 refreshed, 1 failed · api" }),
          account("u-session@verify.invalid", [4, 5], "failed", { startedAt: ago(29 * MIN), finishedAt: ago(29 * MIN), message: "Session: Status is \"pending\", not logged_in — re-login this ID" }),
          account("u-stopped@verify.invalid", [6, 7], "cancelled", { finishedAt: ago(27 * MIN), message: "Stopped before it started" }),
          account("u-cleared@verify.invalid", [8, 9, 10], "cancelled", { done: 1, startedAt: ago(28 * MIN), finishedAt: ago(27 * MIN), message: "Cleared by admin — press Update to re-run" }),
          account("u-lost@verify.invalid", [11, 12, 13, 14], "done", { done: 2, failed: 2, startedAt: ago(29 * MIN), finishedAt: ago(28 * MIN) }),
        ],
        logs: [
          { time: ago(29 * MIN), level: "warn", message: `u-done@verify.invalid: ${unit(2).orderId}/${unit(2).unitId}: API read failed (https://2.rome.api.flipkart.com/api/4/page/fetch answered HTTP 500) — page` },
          { time: ago(29 * MIN), level: "warn", message: `u-done@verify.invalid: ${unit(2).orderId}/${unit(2).unitId}: page.goto: Timeout 30000ms exceeded.` },
          { time: ago(29 * MIN), level: "warn", message: `u-lost@verify.invalid: ${unit(11).orderId}/${unit(11).unitId}: order page gave no data for ${unit(11).orderId}/${unit(11).unitId} (no status, timeline or product) — row left as it was` },
        ],
        startedAt: ago(29 * MIN), finishedAt: ago(27 * MIN), createdAt, updatedAt: ago(27 * MIN),
      });
    }
    // 6. The update a nightly fetch queued when it ended — still running, with a worker that DOES mark its units.
    {
      const fetchId = oid();
      ids.fetchNightly = String(fetchId);
      await raw(deps.Ledger).insertOne({ ...ledgerBase, _id: fetchId, trigger: "system-cron", by: "schedule", status: "completed", total: 2, processed: 2, success: 2, startedAt: ago(3 * 60 * MIN), finishedAt: ago(2 * 60 * MIN), createdAt: ago(3 * 60 * MIN), updatedAt: ago(2 * 60 * MIN), meta: { host: HOST, pid: 1, task_counts: { total: 2, queued: 0, in_progress: 0, success: 2, failed: 0, cancelled: 0, aborted: 0, timed_out: 0 }, params: { selection_method: "all_accounts", fetch_orders_after_date: "2026-11-22", force_fetch: false, run_update_after_fetch: true } } });
      const _id = oid();
      ids.updateAfter = String(_id);
      await raw(deps.Ledger).updateOne({ _id: fetchId }, { $set: { "meta.child_ids": [String(_id)] } });
      const createdAt = ago(2 * 60 * MIN);
      await raw(deps.Update).insertOne({
        _id, userId: uid, source: "schedule", selectionMethod: "after_fetch", createdBy: "schedule", status: "running", mode: "api", windows: 3, parentId: String(fetchId), systemId: SYSTEM,
        params: { platform: "FLIPKART", selection_method: "after_fetch", follows: "fetch", parent_id: String(fetchId), force: false, email_ids_count: 0, statuses_to_update: ["Out for delivery", "Shipped"], status_keys: [], order_ids_count: 0, order_email_pairs_count: 0 },
        accounts: [account("a-run@verify.invalid", [21, 22, 23], "running", { done: 1, startedAt: ago(MIN) }), account("a-wait@verify.invalid", [24], "queued")],
        logs: [], startedAt: ago(MIN), finishedAt: null, createdAt, updatedAt: new Date(),
      });
    }

    // 7. An invoice fetch with every task status.
    const invTask = (orderId: string, status: string, o: Record<string, unknown> = {}) => ({ orderId, email: "inv@verify.invalid", accountId: "acc1", status, errorClass: "", error: "", detail: "", parts: [], sizeKb: 0, startedAt: null, finishedAt: null, ...o });
    {
      const _id = oid();
      ids.invoice = String(_id);
      const createdAt = ago(20 * MIN);
      const tasks = [
        invTask("ODINV1", "success", { parts: ["product"], sizeKb: 120, startedAt: ago(19 * MIN), finishedAt: new Date(ago(19 * MIN).getTime() + 3970) }),
        invTask("ODINV2", "failed", { errorClass: "nothing_available", error: ERROR_CLASSES.nothing_available, detail: "HTTP 404", startedAt: ago(19 * MIN), finishedAt: ago(19 * MIN) }),
        invTask("ODINV3", "failed", { errorClass: "nothing_available", error: ERROR_CLASSES.nothing_available, detail: "HTTP 404", startedAt: ago(19 * MIN), finishedAt: ago(19 * MIN) }),
        invTask("ODINV4", "failed", { email: "", accountId: "", errorClass: "order_not_found", error: ERROR_CLASSES.order_not_found, finishedAt: createdAt }),
        invTask("ODINV5", "cancelled", { error: CANCELLED_BEFORE_START, finishedAt: ago(18 * MIN) }),
        invTask("ODINV6", "aborted", { error: ABORTED_WHILE_RUNNING, startedAt: ago(19 * MIN), finishedAt: ago(18 * MIN) }),
        invTask("ODINV7", "timedout", { error: TIMED_OUT, startedAt: ago(19 * MIN), finishedAt: ago(18 * MIN) }),
      ];
      await raw(deps.Invoice).insertOne({ _id, userId: uid, platform: "FLIPKART", source: "user", createdBy: "verify@1gb.com", status: "cancelled", message: "Stopped by verify", params: { order_ids_count: 763, force: false }, tasks, counts: countsOf(tasks.map((t) => t.status as never)), retryOf: "", parentId: "", queue: "order-invoice-batch", systemId: SYSTEM, host: HOST, workerHost: "", stoppedBy: "verify", startedAt: ago(19 * MIN), finishedAt: ago(18 * MIN), createdAt, updatedAt: ago(18 * MIN) });
      // …and its retry, queued and waiting: an open request of this deployment.
      const retry = oid();
      ids.invoiceRetry = String(retry);
      const again = [invTask("ODINV2", "queued"), invTask("ODINV3", "in_progress", { startedAt: ago(40 * MIN) })];
      await raw(deps.Invoice).insertOne({ _id: retry, userId: uid, platform: "FLIPKART", source: "user", createdBy: "verify@1gb.com", status: "in_progress", message: "", params: { order_ids_count: 2, force: false, retry_of: String(_id), retry_statuses: ["failed"] }, tasks: again, counts: countsOf(again.map((t) => t.status as never)), retryOf: String(_id), parentId: String(_id), queue: "order-invoice-batch", systemId: SYSTEM, host: HOST, workerHost: HOST, stoppedBy: "", startedAt: ago(40 * MIN), finishedAt: null, createdAt: ago(41 * MIN), updatedAt: ago(40 * MIN) });
    }

    // 8. Company coupons: the request and its own task collection.
    {
      const _id = oid();
      ids.coupon = String(_id);
      const createdAt = ago(15 * MIN);
      const t = (email: string, status: string, o: Record<string, unknown> = {}) => ({ requestId: _id, userId: uid, email, accountId: "a", status, errorClass: "", error: "", note: "", results: [], startedAt: null, finishedAt: null, createdAt, updatedAt: createdAt, ...o });
      const tasks = [
        t("c1@verify.invalid", "success", { note: "Link opened with the account's session (HTTP 200)", startedAt: ago(14 * MIN), finishedAt: ago(14 * MIN) }),
        t("c2@verify.invalid", "failed", { errorClass: "invalid_coupon", error: ERROR_CLASSES.invalid_coupon, startedAt: ago(14 * MIN), finishedAt: ago(14 * MIN) }),
        t("c3@verify.invalid", "queued"),
      ];
      await raw(deps.CouponTask).insertMany(tasks);
      await raw(deps.Coupon).insertOne({ _id, userId: uid, platform: "FLIPKART", source: "user", createdBy: "verify@1gb.com", status: "in_progress", message: "", links: ["https://www.flipkart.com/x"], params: { coupon_links_count: 1, email_ids_count: 3, selection_method: "email_ids", accounts_count: 3 }, counts: countsOf(tasks.map((x) => x.status as never)), retryOf: "", parentId: "", queue: "order-coupon-apply", systemId: "another-deployment", host: "elsewhere", workerHost: "", stoppedBy: "", startedAt: ago(14 * MIN), finishedAt: null, createdAt, updatedAt: ago(45 * MIN) });
    }

    // 9. Cancel units — units and urgent order pages, one force-cleared — and the retry of it.
    const cu = (n: number, status: string, o: Record<string, unknown> = {}) => ({ orderId: `ODCAN${n}`, unitId: `7${String(n).padStart(6, "0")}`, itemId: "", email: "can@verify.invalid", productName: "P", status, message: "", startedAt: null, finishedAt: null, ...o });
    {
      const _id = oid();
      ids.cancel = String(_id);
      const createdAt = ago(10 * MIN);
      await raw(deps.Cancel).insertOne({
        _id, userId: uid, type: "order", input: ["ODCAN1", "ODCAN2", "ODCAN9"], dryRun: false, urgent: true, createdBy: "verify@1gb.com", retryOf: "", status: "done",
        units: [
          cu(1, "cancelled", { message: "Cancellation requested on Flipkart (API)", startedAt: ago(9 * MIN), finishedAt: ago(9 * MIN) }),
          cu(2, "not_cancellable", { message: "Flipkart: Not eligible for cancellation", startedAt: ago(9 * MIN), finishedAt: ago(9 * MIN) }),
          cu(3, "failed", { message: "cancel/data for ODCAN3/7000003 answered HTTP 500", startedAt: ago(9 * MIN), finishedAt: ago(9 * MIN) }),
          cu(4, "failed", { message: "Cleared by admin — reset for re-run", finishedAt: ago(8 * MIN) }),
          cu(5, "failed", { message: "Cleared by admin — reset for re-run", startedAt: ago(9 * MIN), finishedAt: ago(8 * MIN) }),
          cu(6, "dry_run", { message: "stopped at REQUEST CANCELLATION", startedAt: ago(9 * MIN), finishedAt: ago(9 * MIN) }),
        ],
        orders: [
          { orderId: "ODCAN8", email: "can@verify.invalid", status: "read", message: "2 unit(s) queued", unitsFound: 2, startedAt: ago(9 * MIN), finishedAt: ago(9 * MIN) },
          { orderId: "ODCAN9", email: "can@verify.invalid", status: "failed", message: "Session expired — re-login this ID", unitsFound: 0, startedAt: ago(9 * MIN), finishedAt: ago(9 * MIN) },
        ],
        rejected: [{ id: "ODCAN7", reason: "already cancelled" }],
        finishedAt: ago(8 * MIN), createdAt, updatedAt: ago(8 * MIN),
      });
      const retry = oid();
      ids.cancelRetry = String(retry);
      await raw(deps.Cancel).insertOne({ _id: retry, userId: uid, type: "unit", input: ["7000003"], dryRun: false, urgent: false, createdBy: "verify@1gb.com", retryOf: String(_id), systemId: SYSTEM, status: "running", units: [cu(3, "running", { startedAt: ago(50 * MIN) }), cu(31, "queued")], orders: [], rejected: [], finishedAt: null, createdAt: ago(51 * MIN), updatedAt: ago(50 * MIN) });
    }

    // 10. Reschedule.
    {
      const _id = oid();
      ids.reschedule = String(_id);
      const createdAt = ago(8 * MIN);
      const ru = (n: number, status: string, message: string) => ({ ...cu(n, status, { message, startedAt: ago(7 * MIN), finishedAt: ago(7 * MIN) }), offeredDates: [], chosen: "", token: "" });
      await raw(deps.Reschedule).insertOne({
        _id, userId: uid, type: "unit", input: ["1", "2", "3", "4"], date: "2026-11-28", createdBy: "verify@1gb.com", retryOf: "", confirm: false, status: "done",
        units: [
          ru(41, "rescheduled", "Rescheduled to 28 Nov"),
          ru(42, "not_available", "Please call the delivery executive! — This option is not available on this order. (no Change Date button on the page)"),
          ru(43, "needs_review", "Flipkart offers a date change (27 Nov · 28 Nov). Confirm is off (RESCHEDULE_CONFIRM) — nothing was changed."),
          ru(44, "failed", "Flipkart did not offer 2026-11-28; offered: 26 Nov · 27 Nov"),
        ],
        rejected: [], finishedAt: ago(7 * MIN), createdAt, updatedAt: ago(7 * MIN),
      });
    }

    // 11. A request older than the log keeps, and another workspace's request.
    {
      const _id = oid();
      ids.updateAncient = String(_id);
      const createdAt = ago(8 * DAY);
      await raw(deps.Update).insertOne({ _id, userId: uid, source: "manual", selectionMethod: "order_ids", createdBy: "verify@1gb.com", status: "done", mode: "api", windows: 1, parentId: "", params: {}, accounts: [account("old@verify.invalid", [91, 92], "done", { done: 2, finishedAt: createdAt })], logs: [], startedAt: createdAt, finishedAt: createdAt, createdAt, updatedAt: createdAt });
      const foreign = oid();
      ids.foreign = String(foreign);
      await raw(deps.Update).insertOne({ _id: foreign, userId: new Types.ObjectId(other), source: "manual", selectionMethod: "order_ids", createdBy: "someone-else", status: "done", mode: "api", windows: 1, parentId: "", params: {}, accounts: [account("else@verify.invalid", [95], "done", { done: 1, finishedAt: ago(MIN) })], logs: [], startedAt: ago(2 * MIN), finishedAt: ago(MIN), createdAt: ago(2 * MIN), updatedAt: ago(MIN) });
    }

    const kindOf: Record<string, SyncTaskKind> = { fetch: "fetch", fetchOld: "fetch", fetchStale: "fetch", fetchPreparing: "fetch", fetchNightly: "fetch", update: "update", updateAfter: "update", updateAncient: "update", foreign: "update", invoice: "invoices", invoiceRetry: "invoices", coupon: "coupons", cancel: "cancel", cancelRetry: "cancel", reschedule: "reschedule" };
    const factsOf = async (name: string) => (await loadFacts(kindOf[name], [ids[name]], deps)).get(ids[name])!;

    await check("rows brought in line from the request document, for every kind (a worker that writes none)", async () => {
      for (const name of ["update", "updateAfter", "invoice", "invoiceRetry", "coupon", "cancel", "cancelRetry", "reschedule", "updateAncient", "foreign", "fetchOld", "fetchStale"]) {
        const res = await syncRequestTasks(kindOf[name], ids[name], deps);
        assert.ok(res.found && res.inserted === res.seeds, `${name}: ${res.inserted} of ${res.seeds} row(s) inserted`);
        // A second pass has nothing left to do.
        const again = await syncRequestTasks(kindOf[name], ids[name], deps);
        assert.deepEqual([again.inserted, again.moved], [0, 0], `${name}: second pass`);
      }
    });
    await check("a request's own counters = its task rows (update, invoice, coupons, cancel, reschedule, fetch)", async () => {
      for (const name of ["fetch", "update", "updateAfter", "invoice", "invoiceRetry", "coupon", "cancel", "cancelRetry", "reschedule", "fetchNightly"]) {
        if (name === "fetchNightly") continue; // its rows were never written in this fixture; it stands for a parent only
        const f = await factsOf(name);
        const rows = await logCounts(ids[name], deps);
        assert.deepEqual(rows, f.counts, `${name}: rows ${JSON.stringify(rows)} ≠ counters ${JSON.stringify(f.counts)}`);
        assert.equal(Object.keys(f.counts).join(), COUNT_KEYS.join());
      }
    });
    await check("update: a task per unit, with the ending its account implies", async () => {
      const f = await factsOf("update");
      assert.deepEqual(f.counts, { total: 14, queued: 0, in_progress: 0, success: 5, failed: 5, cancelled: 3, aborted: 1, timed_out: 0 });
      const rows = await deps.Tasks.find({ requestId: ids.update }).lean();
      const at = (email: string, n: number) => rows.find((r) => r.key === updateTaskKey(email, unit(n).unitId))!;
      // The unit the log names as failed is the failed one — not the last in line; "API read failed … — page" alone is not a failure.
      assert.deepEqual([at("u-done@verify.invalid", 1).status, at("u-done@verify.invalid", 2).status, at("u-done@verify.invalid", 3).status], ["success", "failed", "success"]);
      assert.deepEqual([at("u-done@verify.invalid", 2).errorCode, at("u-done@verify.invalid", 2).detail], ["platform_unavailable", "page.goto: Timeout 30000ms exceeded."]);
      assert.deepEqual([at("u-session@verify.invalid", 4).status, at("u-session@verify.invalid", 4).errorCode], ["failed", "not_logged_in"]);
      assert.deepEqual([at("u-stopped@verify.invalid", 6).status, at("u-stopped@verify.invalid", 6).error], ["cancelled", CANCELLED_BEFORE_START]);
      // Force-cleared while running: one done, the unit in flight cut off, the last never started.
      assert.deepEqual([8, 9, 10].map((n) => at("u-cleared@verify.invalid", n).status), ["success", "aborted", "cancelled"]);
      assert.equal(at("u-cleared@verify.invalid", 9).error, ABORTED_WHILE_RUNNING);
      // Two failed, the log names one: the other is placed and says it is not recorded which.
      const lost = [11, 12, 13, 14].map((n) => at("u-lost@verify.invalid", n));
      assert.deepEqual(lost.map((r) => r.status), ["failed", "success", "success", "failed"]);
      assert.equal(lost[0].errorCode, "unexpected_response");
      assert.deepEqual([lost[3].errorCode, lost[3].error], ["unknown", "Failed (no reason recorded)"]);
      assert.match(lost[3].detail, /no longer says which/);
      assert.ok(rows.every((r) => r.orderId && r.unitId && r.taskType === "update"), "an update task carries its order and unit");
    });
    await check("cancel / reschedule: counted in the database as they are in code", async () => {
      const cancelDoc = await deps.Cancel.findById(ids.cancel).lean();
      const f = await factsOf("cancel");
      assert.deepEqual(f.counts, countsOfCancel(cancelDoc!), "aggregation = code");
      assert.deepEqual(f.counts, { total: 8, queued: 0, in_progress: 0, success: 3, failed: 3, cancelled: 1, aborted: 1, timed_out: 0 });
      assert.equal(f.startedAt?.getTime(), Math.min(...[...cancelDoc!.units, ...cancelDoc!.orders].map((u) => (u.startedAt ? new Date(u.startedAt).getTime() : Infinity))), "started = the first unit taken");
      const rf = await factsOf("reschedule");
      assert.deepEqual(rf.counts, countsOfReschedule((await deps.Reschedule.findById(ids.reschedule).lean())!));
      assert.deepEqual(rf.counts, { total: 4, queued: 0, in_progress: 0, success: 1, failed: 3, cancelled: 0, aborted: 0, timed_out: 0 });
      const rows = await deps.Tasks.find({ requestId: { $in: [ids.cancel, ids.reschedule] } }).lean();
      const code = (key: string) => rows.find((r) => r.key === key)?.errorCode;
      assert.deepEqual([code("u:7000002"), code("u:7000003"), code("o:ODCAN9"), code("u:7000042"), code("u:7000043"), code("u:7000044")], ["not_allowed_now", "platform_unavailable", "not_logged_in", "not_allowed_now", "internal_error", "no_slots"]);
      const page = rows.find((r) => r.key === "o:ODCAN8")!;
      assert.deepEqual([page.status, page.orderId, page.unitId], ["success", "ODCAN8", ""], "an urgent order page is a task with an order and no unit");
    });
    await check("marks: buffered, one write for two marks, and they win over the document", async () => {
      // The worker marks unit 22 failed although the document (done: 1) would have guessed it queued.
      const key = updateTaskKey("a-run@verify.invalid", unit(22).unitId);
      taskLog.mark(ids.updateAfter, key, { status: "in_progress" }, deps);
      taskLog.mark(ids.updateAfter, key, { status: "failed", message: "order page gave no data for X (no status, timeline or product)" }, deps);
      await taskLog.flush();
      const row = await deps.Tasks.findOne({ requestId: ids.updateAfter, key }).lean();
      assert.deepEqual([row?.status, row?.errorCode], ["failed", "unexpected_response"]);
      assert.ok(row?.startedAt && row?.finishedAt);
      await deps.Update.updateOne({ _id: ids.updateAfter }, { $set: { "accounts.0.done": 1, "accounts.0.failed": 1 } });
      const res = await syncRequestTasks("update", ids.updateAfter, deps);
      const after = await deps.Tasks.findOne({ requestId: ids.updateAfter, key }).lean();
      assert.equal(after?.status, "failed", "a row that has ended is not moved by a pass");
      assert.ok(sameCounts(await logCounts(ids.updateAfter, deps), (await factsOf("updateAfter")).counts), `rows = counters after the pass (${res.moved} moved)`);
    });
    await check("an update as the worker of this version writes it: units marked as they go, an account's ending closing the rest", async () => {
      const _id = oid();
      const id = String(_id);
      const createdAt = ago(MIN);
      await raw(deps.Update).insertOne({
        _id, userId: new Types.ObjectId(other), source: "manual", selectionMethod: "order_ids", createdBy: "someone-else", status: "queued", mode: "api", windows: 2, parentId: "", systemId: SYSTEM, params: {},
        accounts: [account("w-a@verify.invalid", [51, 52, 53], "queued"), account("w-b@verify.invalid", [54, 55, 56], "queued")],
        logs: [], startedAt: null, finishedAt: null, createdAt, updatedAt: createdAt,
      });
      // createUpdateRequest: the rows are there before a job can take one.
      await syncRequestTasksQuietly("update", id, deps);
      assert.equal((await logCounts(id, deps)).queued, 6);
      const key = (email: string, n: number) => updateTaskKey(email, unit(n).unitId);
      // Account A: one unit read, one failed, and the browser died under the third.
      taskLog.mark(id, key("w-a@verify.invalid", 51), { status: "in_progress" }, deps);
      taskLog.mark(id, key("w-a@verify.invalid", 51), { status: "success" }, deps);
      taskLog.mark(id, key("w-a@verify.invalid", 52), { status: "in_progress" }, deps);
      taskLog.mark(id, key("w-a@verify.invalid", 52), { status: "failed", message: "page.goto: Timeout 30000ms exceeded." }, deps);
      taskLog.mark(id, key("w-a@verify.invalid", 53), { status: "in_progress" }, deps);
      await deps.Update.updateOne({ _id }, { $set: { status: "running", "accounts.0.status": "failed", "accounts.0.done": 1, "accounts.0.failed": 1, "accounts.0.message": "Target page, context or browser has been closed", "accounts.0.finishedAt": new Date() } });
      await closeUpdateAccountRows(id, "w-a@verify.invalid", { status: "failed", message: "Target page, context or browser has been closed" }, deps);
      const mid = await deps.Tasks.find({ requestId: id }).lean();
      const st = (email: string, n: number) => mid.find((r) => r.key === key(email, n))!;
      assert.deepEqual([51, 52, 53].map((n) => st("w-a@verify.invalid", n).status), ["success", "failed", "failed"]);
      assert.equal(st("w-a@verify.invalid", 52).errorCode, "platform_unavailable");
      assert.equal(st("w-a@verify.invalid", 53).detail, "Target page, context or browser has been closed");
      assert.deepEqual([54, 55, 56].map((n) => st("w-b@verify.invalid", n).status), ["queued", "queued", "queued"], "the other account's units are not touched");
      // Account B: its session is dead — nothing is read, all three fail the same way.
      await deps.Update.updateOne({ _id }, { $set: { status: "done", finishedAt: new Date(), "accounts.1.status": "failed", "accounts.1.message": "Session: Status is \"pending\", not logged_in — re-login this ID", "accounts.1.finishedAt": new Date() } });
      await closeUpdateAccountRows(id, "w-b@verify.invalid", { status: "failed", message: "Session: Status is \"pending\", not logged_in — re-login this ID" }, deps);
      // settleRequest: the request has ended — one whole pass, which finds nothing left to move.
      const last = await syncRequestTasks("update", id, deps);
      assert.deepEqual([last.inserted, last.moved], [0, 0]);
      const f = (await loadFacts("update", [id], deps)).get(id)!;
      assert.deepEqual(f.counts, { total: 6, queued: 0, in_progress: 0, success: 1, failed: 5, cancelled: 0, aborted: 0, timed_out: 0 });
      assert.deepEqual(await logCounts(id, deps), f.counts);
      assert.equal((await deps.Tasks.countDocuments({ requestId: id, errorCode: "not_logged_in" })), 3);
      // A stopped account: what it never got to is cancelled.
      await deps.Tasks.updateMany({ requestId: id, email: "w-b@verify.invalid" }, { $set: { status: "queued", error: "", errorCode: "", finishedAt: null } });
      await closeUpdateAccountRows(id, "w-b@verify.invalid", { status: "cancelled", message: "Stopped after 0 unit(s)" }, deps);
      assert.deepEqual(await deps.Tasks.distinct("error", { requestId: id, email: "w-b@verify.invalid" }), [CANCELLED_BEFORE_START]);
      await deps.Tasks.deleteMany({ requestId: id });
      await deps.Update.deleteOne({ _id });
    });
    await check("a job that ends one account brings the rows in line at most every few seconds — and at once when it ended the request", async () => {
      const _id = oid();
      const id = String(_id);
      const createdAt = ago(MIN);
      const t = (n: number, status: string) => invTask(`ODSOON${n}`, status);
      await raw(deps.Invoice).insertOne({ _id, userId: new Types.ObjectId(other), platform: "FLIPKART", source: "user", createdBy: "x", status: "in_progress", message: "", params: { order_ids_count: 3, force: false }, tasks: [t(1, "queued"), t(2, "queued"), t(3, "queued")], counts: { total: 3, queued: 3, in_progress: 0, success: 0, failed: 0, cancelled: 0, aborted: 0, timed_out: 0 }, retryOf: "", parentId: "", queue: "order-invoice-batch", systemId: SYSTEM, host: HOST, workerHost: "", stoppedBy: "", startedAt: createdAt, finishedAt: null, createdAt, updatedAt: createdAt });
      await syncRequestTasksSoon("invoices", id, false, deps);
      assert.equal((await logCounts(id, deps)).queued, 3);
      await deps.Invoice.updateOne({ _id }, { $set: { "tasks.0.status": "success", "counts.queued": 2, "counts.success": 1 } });
      await syncRequestTasksSoon("invoices", id, false, deps);
      assert.equal((await logCounts(id, deps)).success, 0, "a second account ending a moment later does not read the whole request again");
      await deps.Invoice.updateOne({ _id }, { $set: { status: "completed", "tasks.1.status": "success", "tasks.2.status": "success", "counts.queued": 0, "counts.success": 3 } });
      await syncRequestTasksSoon("invoices", id, true, deps);
      assert.equal((await logCounts(id, deps)).success, 3, "the account that ended the request does");
      await deps.Tasks.deleteMany({ requestId: id });
      await deps.Invoice.deleteOne({ _id });
    });
    await check("every task status and every task type is in the log", async () => {
      const statuses = await deps.Tasks.distinct("status", { userId: uid });
      assert.ok(REQUEST_TASK_STATUSES.every((s) => statuses.includes(s)), `statuses in the log: ${statuses.join(", ")}`);
      const types = await deps.Tasks.distinct("taskType", { userId: uid });
      assert.deepEqual([...types].sort(), ["cancel", "company_coupon_apply", "fetch", "invoice_fetch", "reschedule", "update"]);
    });

    // ── GET /sync/batches ───────────────────────────────────────────────────
    console.log("\nBATCH REQUESTS");
    const P = probes(["order-update"], true);
    const all = await listBatches(userId, { page_size: 100 }, deps, P);
    const row = (name: string) => all.results.find((r) => r.request_id === ids[name])!;
    await check("the reference's shape: { count, page, page_size, task_types, results[…20 keys…] }", () => {
      assert.deepEqual(Object.keys(all).slice(0, 5), ["count", "page", "page_size", "task_types", "results"]);
      assert.deepEqual(all.task_types, ["fetch", "update", "invoice_fetch", "reschedule", "cancel", "company_coupon_apply", "review"]);
      assert.equal(all.count, 14, "14 requests of this workspace (the other workspace's is not counted)");
      for (const r of all.results) {
        assert.deepEqual(BATCH_KEYS.filter((k) => !(k in r)), [], "every key of the reference's row");
        assert.deepEqual(Object.keys(r.counts), COUNT_KEYS);
        assert.ok(["queued", "in_progress", "completed", "cancelled"].includes(r.status));
        assert.equal(r.short_id, r.request_id.slice(-8));
      }
      assert.ok(!all.results.some((r) => r.request_id === ids.foreign));
    });
    await check("default page: 25 a page, newest first across the six kinds", async () => {
      const first = await listBatches(userId, {}, deps, P);
      assert.equal(first.page_size, 25);
      const times = all.results.map((r) => new Date(r.created_at).getTime());
      assert.deepEqual(times, [...times].sort((a, b) => b - a));
      assert.deepEqual([...new Set(all.results.map((r) => r.task_type))].sort(), ["cancel", "company_coupon_apply", "fetch", "invoice_fetch", "reschedule", "update"]);
    });
    await check("paging is cut in the database: pages of 4 are disjoint and add up to the list", async () => {
      const seen: string[] = [];
      for (let page = 1; page <= 4; page++) {
        const p = await listBatches(userId, { page, page_size: 4 }, deps, P);
        assert.equal(p.count, 14);
        assert.equal(p.results.length, page < 4 ? 4 : 2);
        seen.push(...p.results.map((r) => r.request_id));
      }
      assert.deepEqual(seen, all.results.map((r) => r.request_id));
      assert.equal((await listBatches(userId, { page: 9, page_size: 4 }, deps, P)).results.length, 0);
    });
    await check("statuses: preparing fetch = queued, stopped = cancelled, done = completed", () => {
      assert.equal(row("fetchPreparing").status, "queued");
      assert.deepEqual(row("fetchPreparing").counts, { total: 21251, queued: 21251, in_progress: 0, success: 0, failed: 0, cancelled: 0, aborted: 0, timed_out: 0 });
      assert.equal(row("fetch").status, "cancelled");
      assert.equal(row("update").status, "cancelled");
      assert.equal(row("invoice").status, "cancelled");
      assert.equal(row("cancel").status, "completed");
      assert.equal(row("updateAfter").status, "in_progress");
      assert.equal(row("fetchOld").status, "completed");
    });
    await check("counts, can_cancel, can_retry, finished_at", () => {
      assert.deepEqual(row("fetchOld").counts, { total: 9, queued: 0, in_progress: 0, success: 7, failed: 2, cancelled: 0, aborted: 0, timed_out: 0 }, "an older fetch counts accounts (8 + 1 skipped), not its 341 unit ids");
      assert.equal(row("fetchOld").tasks_recorded, "named");
      assert.deepEqual([row("updateAfter").can_cancel, row("updateAfter").can_retry, row("updateAfter").finished_at], [true, false, null]);
      assert.deepEqual([row("invoice").can_cancel, row("invoice").can_retry], [false, true]);
      assert.deepEqual([row("fetchNightly").can_cancel, row("fetchNightly").can_retry], [false, false]);
      assert.equal(row("invoice").finished_count, 7);
      assert.equal(row("invoice").progress_percent, 100);
      assert.equal(row("invoice").eta_seconds, 0);
    });
    await check("source, triggered_by, parent_id, child_ids", () => {
      assert.deepEqual([row("update").source, row("update").triggered_by], ["user", { id: null, name: "verify@1gb.com", email: "verify@1gb.com" }]);
      assert.deepEqual([row("updateAfter").source, row("updateAfter").triggered_by], ["system-cron", null]);
      assert.deepEqual([row("updateAfter").parent_id, row("fetchNightly").child_ids], [ids.fetchNightly, [ids.updateAfter]]);
      // A retry is the user's request; it names the one it retries, which lists it — as the reference does.
      assert.deepEqual([row("invoiceRetry").source, row("invoiceRetry").parent_id, row("invoice").child_ids], ["user", ids.invoice, [ids.invoiceRetry]]);
      assert.deepEqual([row("cancelRetry").parent_id, row("cancel").child_ids, row("cancelRetry").params.retry_of], [ids.cancel, [ids.cancelRetry], ids.cancel]);
    });
    await check("the Request cell line, as the reference writes it", () => {
      assert.equal(row("updateAfter").summary, "Statuses: Out for delivery, Shipped · After nightly fetch");
      assert.equal(row("updateAfter").follows?.label, "Follows a nightly fetch");
      assert.equal(row("updateAfter").params.after_fetch, true);
      assert.equal(row("fetch").summary, "Emails: 6 · After: 2026-11-22 · Force");
      assert.equal(row("fetchNightly").summary, "After: 2026-11-22");
      assert.equal(row("invoice").summary, "Orders: 763");
      assert.equal(row("cancel").summary, "Units: 8");
      assert.equal(row("update").summary, "Statuses: Shipped, Out for delivery");
      assert.equal(row("fetchStale").summary, "After: 2026-10-01", "an older row's date is read in India time");
    });
    await check("filter: task_type (each of the seven)", async () => {
      const n = async (task_type: string) => (await listBatches(userId, { task_type }, deps, P)).count;
      assert.deepEqual([await n("fetch"), await n("update"), await n("invoice_fetch"), await n("company_coupon_apply"), await n("cancel"), await n("reschedule"), await n("review")], [5, 3, 2, 1, 2, 1, 0]);
    });
    await check("filter: source", async () => {
      const got = async (source: string) => (await listBatches(userId, { source, page_size: 100 }, deps, P)).results.map((r) => r.request_id).sort();
      assert.deepEqual(await got("system-cron"), [ids.fetchPreparing, ids.fetchNightly, ids.updateAfter].sort());
      assert.equal((await got("user")).length, 11);
      for (const s of ["system-order", "system-email", "system-update", "system-retry", "system-repair"]) assert.equal((await got(s)).length, 0, s);
    });
    await check("filter: status (queued, in_progress, completed, cancelled)", async () => {
      const got = async (status: string) => (await listBatches(userId, { status, page_size: 100 }, deps, P)).results.map((r) => r.request_id).sort();
      assert.deepEqual(await got("queued"), [ids.fetchPreparing]);
      assert.deepEqual(await got("in_progress"), [ids.fetchStale, ids.updateAfter, ids.invoiceRetry, ids.coupon, ids.cancelRetry].sort());
      assert.deepEqual(await got("cancelled"), [ids.fetch, ids.update, ids.invoice].sort());
      assert.deepEqual(await got("completed"), [ids.fetchOld, ids.fetchNightly, ids.cancel, ids.reschedule, ids.updateAncient].sort());
      assert.equal((await got("nonsense")).length, 0);
    });
    await check("filter: platform, created_from / created_to (India days), and all of them together", async () => {
      assert.equal((await listBatches(userId, { platform: "FLIPKART" }, deps, P)).count, 14);
      assert.equal((await listBatches(userId, { platform: "AMAZON" }, deps, P)).count, 0);
      assert.equal((await listBatches(userId, { platform: "FLIPKART,SHOPSY,AMAZON" }, deps, P)).count, 14);
      const ancientDay = istDayOf(ago(8 * DAY));
      const only = await listBatches(userId, { created_from: ancientDay, created_to: ancientDay }, deps, P);
      assert.deepEqual(only.results.map((r) => r.request_id), [ids.updateAncient]);
      const today = istDayOf(new Date());
      const combo = await listBatches(userId, { platform: "FLIPKART", task_type: "invoice_fetch", source: "user", status: "cancelled", created_from: istDayOf(ago(DAY)), created_to: today }, deps, P);
      assert.deepEqual(combo.results.map((r) => r.request_id), [ids.invoice]);
      await assert.rejects(listBatches(userId, { created_from: "yesterday" }, deps, P), RequestInputError);
    });
    await check("a request with nothing behind it reads stalled, never plain in progress", () => {
      // Its worker is connected: alive.
      assert.deepEqual([row("updateAfter").worker_alive, row("updateAfter").stalled], [true, false]);
      // No worker on the invoice queue.
      assert.equal(row("invoiceRetry").stalled, true);
      assert.match(row("invoiceRetry").stalled_message, /The worker stopped with 2 order\(s\) left/);
      // Another deployment's request: this API cannot tell, and says nothing.
      assert.deepEqual([row("coupon").worker_alive, row("coupon").stalled], [null, false]);
      assert.equal(row("fetchStale").stalled, true);
    });

    // ── one request ─────────────────────────────────────────────────────────
    console.log("\nONE REQUEST");
    await check("GET /sync/batches/:id — SETTINGS under the reference's labels", async () => {
      const u = (await getBatch(userId, ids.update, deps, P))!;
      assert.deepEqual(u.settings, [
        { label: "Selected by", value: "Status filter" },
        { label: "Statuses", value: "Shipped, Out for delivery" },
        { label: "Force (skip in-progress check)", value: "No" },
      ]);
      const a = (await getBatch(userId, ids.updateAfter, deps, P))!;
      assert.deepEqual(a.settings, [
        { label: "Queued after nightly fetch", value: "Yes" },
        { label: "Statuses", value: "Out for delivery, Shipped" },
      ]);
      assert.deepEqual(a.follows, { request_id: ids.fetchNightly, short_id: ids.fetchNightly.slice(-8), what: "nightly fetch", label: "Follows a nightly fetch" });
      const f = (await getBatch(userId, ids.fetch, deps, P))!;
      assert.deepEqual(f.settings.slice(0, 4), [
        { label: "Selected by", value: "Email accounts" },
        { label: "Emails", value: "6" },
        { label: "After", value: "2026-11-22" },
        { label: "Force (skip in-progress check)", value: "Yes" },
      ]);
      const n = (await getBatch(userId, ids.fetchNightly, deps, P))!;
      assert.equal(n.settings[0].value, "All eligible");
      const i = (await getBatch(userId, ids.invoice, deps, P))!;
      assert.deepEqual(i.settings, [
        { label: "Orders", value: "763" },
        { label: "Force (skip in-progress check)", value: "No" },
      ]);
      const c = (await getBatch(userId, ids.cancel, deps, P))!;
      assert.deepEqual(c.settings.slice(0, 2), [
        { label: "Selected by", value: "Explicit order IDs" },
        { label: "Units", value: "8" },
      ]);
      assert.deepEqual((await getBatch(userId, ids.updateAncient, deps, P))!.settings, [{ label: "Selected by", value: "Explicit order IDs" }]);
      assert.deepEqual((await getBatch(userId, ids.fetchOld, deps, P))!.settings, [], "nothing recorded → the modal's \"No settings were recorded for this request.\"");
    });
    await check("GET /sync/batches/:id — OUTCOME and ISSUES", async () => {
      const i = (await getBatch(userId, ids.invoice, deps, P))!;
      assert.deepEqual(i.outcome, { tasks: 7, completed: 1, failed: 3, timed_out: 1, stopped: 1, cancelled: 1, still_queued: 0, still_running: 0, avg_seconds_per_task: i.avg_seconds_per_task });
      assert.equal(i.incomplete_count, 6);
      assert.deepEqual(
        i.issues.map((x) => [x.status, x.error_code, x.label, x.count]),
        [
          ["failed", "nothing_available", "Nothing available yet", 2],
          ["failed", "order_not_found", "Order not found on this account", 1],
          ["timedout", null, TIMED_OUT, 1],
          ["aborted", null, ABORTED_WHILE_RUNNING, 1],
          ["cancelled", null, CANCELLED_BEFORE_START, 1],
        ]
      );
      assert.equal(i.tasks_in_step, true);
      assert.equal(await getBatch(userId, ids.foreign, deps, P), null, "another workspace's request is not found");
      assert.equal(await getBatch(userId, ids.invoice.slice(-8), deps, P), null, "the 8-character id is not a lookup key");
      assert.equal(await findRequest(userId, "not-an-id", deps), null);
    });
    await check("GET /sync/batches/:id/issues?status=failed — grouped by error class, with the items for Copy all", async () => {
      const out = (await batchIssues(userId, ids.invoice, { status: "failed" }, deps))!;
      assert.equal(out.count, 3);
      assert.deepEqual(out.groups.map((g) => [g.error_code, g.label, g.count, g.items.map((x) => x.order_id), g.copy_text]), [
        ["nothing_available", "Nothing available yet", 2, ["ODINV2", "ODINV3"], "ODINV2\nODINV3"],
        ["order_not_found", "Order not found on this account", 1, ["ODINV4"], "ODINV4"],
      ]);
      assert.equal(out.copy_text, "ODINV2\nODINV3\nODINV4");
      const cancelled = (await batchIssues(userId, ids.update, { status: "cancelled" }, deps))!;
      assert.deepEqual([cancelled.count, cancelled.groups.length, cancelled.groups[0].label, cancelled.groups[0].error_code], [3, 1, CANCELLED_BEFORE_START, null]);
      // The default is failed; account tasks copy their e-mails.
      const fetch = (await batchIssues(userId, ids.fetch, {}, deps))!;
      assert.deepEqual([fetch.status, fetch.count, fetch.copy_text.split("\n").sort()], ["failed", 2, ["f2@verify.invalid", "f6@verify.invalid"]]);
      await assert.rejects(batchIssues(userId, ids.invoice, { status: "success" }, deps), RequestInputError);
      assert.equal(await batchIssues(userId, ids.foreign, {}, deps), null);
    });

    // ── GET /sync/tasks ─────────────────────────────────────────────────────
    console.log("\nALL TASKS");
    const allTasks = await listTasks(userId, { page_size: 200 }, deps);
    await check("the reference's shape: { count, error_classes[16], results[…16 keys…] }, 50 a page", async () => {
      const first = await listTasks(userId, {}, deps);
      assert.equal(first.page_size, 50);
      assert.equal(first.results.length, 50);
      assert.equal(first.error_classes.length, 16);
      assert.equal(first.retention_days, 7);
      for (const t of first.results) {
        assert.deepEqual(Object.keys(t).filter((k) => TASK_KEYS.includes(k)), TASK_KEYS);
        assert.ok(REQUEST_TASK_STATUSES.includes(t.status));
      }
      const times = allTasks.results.map((t) => new Date(t.created_at).getTime());
      assert.deepEqual(times, [...times].sort((a, b) => b - a), "newest first");
      const second = await listTasks(userId, { page: 2 }, deps);
      assert.equal(second.results.length, allTasks.count - 50);
      assert.equal(new Set([...first.results, ...second.results].map((t) => t.id)).size, allTasks.count, "two pages, no task twice");
    });
    await check("the 7-day cut: a task older than that is not listed, even before the TTL has removed its row", async () => {
      // The TTL thread removes a row within a minute or so of its 7 days; a row 30 s past them is still there — and already out.
      const at = ago(SYNC_TASK_KEEP_MS + 30_000);
      await deps.Tasks.collection.insertOne({ userId: uid, requestId: ids.update, kind: "update", key: "past-the-cut", platform: "FLIPKART", taskType: "update", source: "user", status: "success", email: "cut@verify.invalid", orderId: "ODCUT1", unitId: "1", triggeredBy: "", error: "", errorCode: "", detail: "", startedAt: at, finishedAt: at, createdAt: at, updatedAt: at });
      const there = await deps.Tasks.countDocuments({ orderId: "ODCUT1" });
      assert.equal((await listTasks(userId, { order_id: "ODCUT1" }, deps)).count, 0);
      assert.equal(there, 1, "the row was in the collection when it was asked for");
      await deps.Tasks.deleteOne({ orderId: "ODCUT1" });
      assert.equal((await listTasks(userId, { request_id: ids.updateAncient }, deps)).count, 0);
      assert.ok(!allTasks.results.some((t) => t.request_id === ids.updateAncient));
      assert.equal((await listTasks(userId, { created_from: istDayOf(ago(30 * DAY)) }, deps)).count, allTasks.count, "an older Created from does not reach behind the cut");
      const ttl = (await deps.Tasks.collection.indexes()).find((i) => i.expireAfterSeconds !== undefined);
      assert.deepEqual([ttl?.key, ttl?.expireAfterSeconds], [{ createdAt: 1 }, SYNC_TASK_KEEP_MS / 1000]);
      assert.equal(SYNC_TASK_KEEP_MS / 1000, 604800);
    });
    await check("another workspace's tasks are never listed", async () => {
      assert.ok(!allTasks.results.some((t) => t.request_id === ids.foreign));
      assert.equal((await listTasks(userId, { request_id: ids.foreign }, deps)).count, 0);
      assert.equal((await listTasks(other, {}, deps)).count, 1);
    });
    await check("filter: request_id — the full id only", async () => {
      const mine = await listTasks(userId, { request_id: ids.invoice }, deps);
      assert.equal(mine.count, 7);
      assert.ok(mine.results.every((t) => t.request_id === ids.invoice && t.short_request_id === ids.invoice.slice(-8) && t.task_type === "invoice_fetch"));
      await assert.rejects(listTasks(userId, { request_id: ids.invoice.slice(-8) }, deps), /full request id/);
      await assert.rejects(listTasks(userId, { request_id: ids.invoice.slice(0, 8) }, deps), RequestInputError);
    });
    await check("filter: order_id, email, platform", async () => {
      const byOrder = await listTasks(userId, { order_id: "odinv2" }, deps);
      assert.deepEqual(byOrder.results.map((t) => [t.order_id, t.request_id]).sort(), [["ODINV2", ids.invoice], ["ODINV2", ids.invoiceRetry]].sort());
      const byEmail = await listTasks(userId, { email: "CAN@verify.invalid" }, deps);
      assert.equal(byEmail.count, 14);
      assert.ok(byEmail.results.every((t) => t.email === "can@verify.invalid"));
      assert.equal((await listTasks(userId, { order_id: "ODINV2", email: "nobody@verify.invalid" }, deps)).count, 0);
      assert.equal((await listTasks(userId, { platform: "FLIPKART" }, deps)).count, allTasks.count);
      assert.equal((await listTasks(userId, { platform: "SAMSUNG" }, deps)).count, 0);
    });
    await check("filter: task_type and source", async () => {
      const n = async (q: Record<string, string>) => (await listTasks(userId, q, deps)).count;
      const types = { fetch: 6 + 2 + 3, update: 14 + 4, invoice_fetch: 9, company_coupon_apply: 3, cancel: 10, reschedule: 4, review: 0 };
      for (const [task_type, want] of Object.entries(types)) assert.equal(await n({ task_type }), want, task_type);
      assert.equal(Object.values(types).reduce((a, b) => a + b, 0), allTasks.count);
      assert.equal(await n({ source: "system-cron" }), 4);
      assert.equal(await n({ source: "user" }), allTasks.count - 4);
      assert.equal(await n({ source: "system-update" }), 0);
    });
    await check("filter: status (each of the seven) and error_code", async () => {
      let total = 0;
      for (const status of REQUEST_TASK_STATUSES) {
        const out = await listTasks(userId, { status, page_size: 200 }, deps);
        assert.ok(out.count > 0, `no ${status} task`);
        assert.ok(out.results.every((t) => t.status === status));
        total += out.count;
      }
      assert.equal(total, allTasks.count);
      assert.equal((await listTasks(userId, { status: "timed_out" }, deps)).count, 0, "the task status is spelled timedout, as on the reference");
      const notLoggedIn = await listTasks(userId, { error_code: "not_logged_in" }, deps);
      assert.ok(notLoggedIn.count >= 5 && notLoggedIn.results.every((t) => t.error_code === "not_logged_in" && t.error === "Account is not logged in" && t.status === "failed"));
      assert.equal((await listTasks(userId, { error_code: "invalid_coupon", task_type: "company_coupon_apply" }, deps)).count, 1);
      assert.equal((await listTasks(userId, { status: "failed", error_code: "nothing_available", request_id: ids.invoice }, deps)).count, 2);
    });
    await check("a task row: the reference's values", async () => {
      const t = (await listTasks(userId, { request_id: ids.invoice, page_size: 200 }, deps)).results;
      const of = (orderId: string) => t.find((x) => x.order_id === orderId)!;
      assert.deepEqual(pick(of("ODINV1"), ["status", "email", "unit_id", "duration_seconds", "error", "error_code"]), { status: "success", email: "inv@verify.invalid", unit_id: null, duration_seconds: 4, error: null, error_code: null });
      assert.deepEqual(pick(of("ODINV5"), ["status", "started_at", "duration_seconds", "error", "error_code"]), { status: "cancelled", started_at: null, duration_seconds: null, error: "Cancelled before it started", error_code: null });
      assert.deepEqual(pick(of("ODINV4"), ["status", "email", "error", "error_code"]), { status: "failed", email: null, error: "Order not found on this account", error_code: "order_not_found" });
      assert.deepEqual(of("ODINV1").triggered_by, { id: null, name: "verify@1gb.com", email: "verify@1gb.com" });
      const cron = (await listTasks(userId, { request_id: ids.updateAfter }, deps)).results[0];
      assert.deepEqual([cron.source, cron.triggered_by], ["system-cron", null]);
      const fetch = (await listTasks(userId, { request_id: ids.fetch }, deps)).results[0];
      assert.deepEqual([fetch.order_id, fetch.unit_id], [null, null]);
    });
    await check("the list is read off an index in order — no collection scan, no sort in memory", async () => {
      const cut = ago(SYNC_TASK_KEEP_MS);
      const plans: Array<[string, Record<string, unknown>]> = [
        ["all", { userId: uid, createdAt: { $gte: cut } }],
        ["status", { userId: uid, createdAt: { $gte: cut }, status: "failed" }],
        ["error_code", { userId: uid, createdAt: { $gte: cut }, errorCode: "not_logged_in" }],
        ["order_id", { userId: uid, createdAt: { $gte: cut }, orderId: "ODINV2" }],
        ["email", { userId: uid, createdAt: { $gte: cut }, email: "can@verify.invalid" }],
        ["task_type", { userId: uid, createdAt: { $gte: cut }, taskType: "update" }],
        ["source", { userId: uid, createdAt: { $gte: cut }, source: "system-cron" }],
        ["request_id", { requestId: ids.update, createdAt: { $gte: cut } }],
      ];
      for (const [name, filter] of plans) {
        // The winning plan only: the plans it beat (which do sort) are listed beside it.
        const explained = (await deps.Tasks.find(filter).sort({ createdAt: -1, _id: -1 }).limit(50).explain("queryPlanner")) as { queryPlanner?: { winningPlan?: unknown } };
        const plan = JSON.stringify(explained.queryPlanner?.winningPlan);
        assert.ok(plan.includes('"IXSCAN"') && !plan.includes('"COLLSCAN"'), `${name}: not an index scan — ${plan.slice(0, 200)}`);
        assert.ok(!/"stage":"SORT"/.test(plan), `${name}: sorted in memory — ${plan.slice(0, 200)}`);
      }
    });

    // ── what a request that has ended still offers, and what it says it did ──
    console.log("\nENDED REQUESTS — a stopped update's unfinished units, a pre-log fetch's retry, an invoice's parts");
    // A workspace of their own, so the lists and the keeper above see none of these.
    const third = String(oid());
    const thirdId = new Types.ObjectId(third);
    await check("a stopped update: the account left queued never starts; the one left running finishes its unit in hand — the request reads in progress (Cancelling) until it has, in the In progress filter; aggregation = code, rows = counters", async () => {
      const _id = oid();
      const id = String(_id);
      const u = (n: number) => ({ orderId: `ODST${n}`, itemId: "", unitId: `8${String(n).padStart(6, "0")}`, orderUrl: "" });
      const acc = (email: string, units: number[], status: string, o: Record<string, unknown> = {}) => ({ email, platformId: null, units: units.map(u), total: units.length, status, done: 0, failed: 0, statusChanged: 0, otpNew: 0, via: "", message: "", startedAt: null, finishedAt: null, ...o });
      await raw(deps.Update).insertOne({
        _id, userId: thirdId, source: "manual", selectionMethod: "default", createdBy: "verify@1gb.com", status: "cancelled", mode: "api", windows: 3, parentId: "", systemId: SYSTEM, params: { platform: "FLIPKART", selection_method: "default", force: false },
        accounts: [acc("st-done@verify.invalid", [31, 32], "done", { done: 2, startedAt: ago(5 * MIN), finishedAt: ago(4 * MIN) }), acc("st-run@verify.invalid", [33, 34, 35], "running", { done: 1, startedAt: ago(3 * MIN) }), acc("st-wait@verify.invalid", [36, 37], "queued")],
        logs: [], startedAt: ago(5 * MIN), finishedAt: ago(2 * MIN), createdAt: ago(6 * MIN), updatedAt: ago(2 * MIN),
      });
      const f = (await loadFacts("update", [id], deps)).get(id)!;
      assert.deepEqual([f.status, f.stopRequested, f.counts], ["in_progress", true, { total: 7, queued: 0, in_progress: 1, success: 3, failed: 0, cancelled: 3, aborted: 0, timed_out: 0 }], "the unit in hand is still being read; nothing is queued any more");
      assert.deepEqual(f.counts, countsOfUpdate((await deps.Update.findById(id).lean())!), "aggregation = code");
      const status = async (s: string) => (await listBatches(third, { status: s }, deps, P)).results.map((r) => r.request_id);
      assert.deepEqual([await status("in_progress"), await status("cancelled")], [[id], []]);
      const row = (await getBatch(third, id, deps, P))!;
      assert.deepEqual([row.can_cancel, row.cancel_note, row.can_retry, row.finished_at], [false, "Cancel was already pressed — the account being worked is finishing", false, null]);
      await syncRequestTasks("update", id, deps);
      const rows = Object.fromEntries((await deps.Tasks.find({ requestId: id }).lean()).map((r) => [r.unitId, r.status]));
      assert.deepEqual([u(34), u(35), u(36), u(37)].map((x) => rows[x.unitId]), ["in_progress", "cancelled", "cancelled", "cancelled"]);
      assert.deepEqual(await logCounts(id, deps), f.counts);
      // Twenty minutes on, the account still reads running: its worker is gone. The unit in hand was cut off, the request has ended.
      await deps.Update.updateOne({ _id }, { $set: { finishedAt: ago(25 * MIN) } });
      const dead = (await loadFacts("update", [id], deps)).get(id)!;
      assert.deepEqual([dead.status, dead.stopRequested, dead.counts], ["cancelled", false, { total: 7, queued: 0, in_progress: 0, success: 3, failed: 0, cancelled: 3, aborted: 1, timed_out: 0 }]);
      assert.deepEqual(dead.counts, countsOfUpdate((await deps.Update.findById(id).lean())!), "aggregation = code");
      assert.deepEqual([await status("in_progress"), await status("cancelled")], [[], [id]]);
      await syncRequestTasks("update", id, deps);
      assert.equal((await deps.Tasks.findOne({ requestId: id, unitId: u(34).unitId }).lean())?.status, "aborted");
      assert.deepEqual(await logCounts(id, deps), dead.counts);
      const after = (await getBatch(third, id, deps, P))!;
      assert.deepEqual([after.status, after.can_retry, after.progress_percent], ["cancelled", true, 100]);
    });
    await check("a fetch from before the log offers to retry what its rows hold — the accounts it names — not its account totals", async () => {
      const _id = oid();
      const id = String(_id);
      const createdAt = ago(DAY);
      // 750 accounts, 3 fetched, 4 failed by name, the rest never reached when its process went away; six e-mails kept.
      await raw(deps.Ledger).insertOne({
        ...ledgerBase, userId: thirdId, _id, status: "failed", message: "The API restarted while this fetch was running — start it again", total: 9000, processed: 30, success: 30, startedAt: createdAt, finishedAt: new Date(createdAt.getTime() + 600_000), createdAt, updatedAt: createdAt,
        failedItems: ["n1", "n2", "n3", "n4"].map((n) => ({ key: `${n}@verify.invalid`, reason: "page.goto: Timeout 30000ms exceeded." })),
        meta: { report: { accountsQueued: 750, accountsDone: 3 }, emails: ["e1", "e2", "e3", "e4", "e5", "e6"].map((n) => `${n}@verify.invalid`) },
      });
      await syncRequestTasks("fetch", id, deps);
      assert.deepEqual(await logCounts(id, deps), { total: 10, queued: 0, in_progress: 0, success: 3, failed: 4, cancelled: 0, aborted: 0, timed_out: 3 }, "its rows: the four named failures and the six e-mails it kept");
      const listed = (await listBatches(third, { task_type: "fetch" }, deps, P)).results.find((r) => r.request_id === id)!;
      assert.deepEqual([listed.tasks_recorded, listed.counts.failed, listed.counts.timed_out, listed.can_retry, listed.retryable_count, listed.retryable_failed_count], ["named", 4, 743, true, 7, 4], "the row keeps its account totals; Retry offers the rows");
      const one = (await getBatch(third, id, deps, P))!;
      assert.deepEqual([one.retryable_count, one.incomplete_count], [7, 747]);
      // What a retry takes is exactly that: the rows.
      assert.equal(await deps.Tasks.countDocuments({ requestId: id, status: { $in: ["failed", "timedout", "aborted", "cancelled"] } }), one.retryable_count);
    });
    await check("an invoice task that downloaded says which parts it stored", async () => {
      const row = await deps.Tasks.findOne({ requestId: ids.invoice, key: "ODINV1" }).lean();
      assert.deepEqual([row?.status, row?.detail], ["success", "Downloaded: product"]);
      const listed = await listTasks(userId, { request_id: ids.invoice, status: "success" }, deps);
      assert.deepEqual(listed.results.map((t) => [t.order_id, t.detail]), [["ODINV1", "Downloaded: product"]]);
      // A task that already says something (the sweep's "Already stored — left as it is") keeps its own words.
      assert.equal(seedsOfInvoice({ _id: oid(), tasks: [{ orderId: "ODX", email: "", accountId: "", status: "success", errorClass: "", error: "", detail: "Already stored — left as it is", parts: [], sizeKb: 0, startedAt: null, finishedAt: null }] })[0].detail, "Already stored — left as it is");
      assert.equal(seedsOfInvoice({ _id: oid(), tasks: [{ orderId: "ODY", email: "", accountId: "", status: "success", errorClass: "", error: "", detail: "", parts: ["product", "transport"], sizeKb: 1, startedAt: null, finishedAt: null }] })[0].detail, "Downloaded: product, transport");
    });

    // ── the keeper ──────────────────────────────────────────────────────────
    console.log("\nKEEPER — rows in step, and requests nothing is behind");
    await check("stale: which open requests may be closed — only this machine's", async () => {
      const now = Date.now();
      // `noWorkerSince`: when the keeper's watch first found the queue without a worker (45 minutes ago, unless said).
      const ctx = (queueAlive: boolean, p = probes([], false), noWorkerSince: number | null = now - 45 * MIN) => ({ now, probes: p, queueAlive, noWorkerSince });
      const inv = await factsOf("invoiceRetry"); // this deployment, quiet 40 min
      assert.equal(livenessOfRequest(inv, ctx(true)).stale, "", "a worker is connected");
      assert.match(livenessOfRequest(inv, ctx(false)).stale, /30 minutes/);
      assert.equal(livenessOfRequest({ ...inv, updatedAt: ago(10 * MIN) }, ctx(false)).stale, "", "silent for 10 minutes only: stalled, not closed");
      assert.equal(livenessOfRequest({ ...inv, updatedAt: ago(10 * MIN) }, ctx(false)).stalled, true);
      assert.equal(livenessOfRequest({ ...inv, systemId: "another-deployment" }, ctx(false)).stale, "", "another deployment's is never closed from here");
      assert.equal(livenessOfRequest({ ...inv, systemId: "" }, ctx(false)).stale, "", "one that names no deployment: not after 40 minutes…");
      assert.match(livenessOfRequest({ ...inv, systemId: "", updatedAt: ago(25 * 60 * MIN) }, ctx(false, probes([], false), now - 25 * 60 * MIN)).stale, /a day/, "…after a day");
      // A worker restart, or an API that starts before its worker: the first reading without one. Stalled, never closed —
      // the request's own 40 minutes of waiting in the queue are not silence.
      const restart = livenessOfRequest(inv, ctx(false, probes([], false), now));
      assert.deepEqual([restart.stalled, restart.stale], [true, ""], "no worker since this very reading");
      assert.equal(livenessOfRequest(inv, ctx(false, probes([], false), now - 10 * MIN)).stale, "", "a worker was there 10 minutes ago");
      assert.equal(livenessOfRequest(inv, ctx(false, probes([], false), null)).stale, "", "a reader that does not watch the queue (a list) never finds one stale");
      const stale = await factsOf("fetchStale"); // names no machine, silent 2 days
      assert.match(livenessOfRequest(stale, ctx(true)).stale, /names no machine/);
      assert.equal(livenessOfRequest({ ...stale, updatedAt: ago(2 * 60 * MIN) }, ctx(true)).stale, "");
      const prep = await factsOf("fetchPreparing"); // this machine, this process, fetch live
      assert.equal(livenessOfRequest(prep, ctx(true, probes([], true))).stalled, false);
      assert.match(livenessOfRequest({ ...prep, pid: 1 }, ctx(true, probes([], true))).stale, /process that ran it is gone/);
      assert.equal(livenessOfRequest({ ...prep, host: "another-mac", pid: 1 }, ctx(true)).stale, "", "another machine's fetch is never closed from here");
    });
    await check("a request whose worker writes no rows is brought in line by the pass", async () => {
      const everyQueue = probes(["order-update", "order-invoice-batch", "order-cancel", "order-coupon-apply"], true);
      // A pass while everything is in step. Every worker queue has a worker, so no worker request is closed —
      // only the fetch that names no machine and has been silent for two days.
      const inStep = await keeperPass({ deps, probes: everyQueue, since: ago(MIN), userId });
      assert.deepEqual(inStep.closed, [ids.fetchStale]);
      assert.deepEqual([...(inStep.open.coupons || [])], [ids.coupon], "what is open is handed to the next pass");
      // The coupons worker finished its last account; nobody marked the row.
      await deps.CouponTask.updateOne({ requestId: new Types.ObjectId(ids.coupon), email: "c3@verify.invalid" }, { $set: { status: "success", note: "Link opened with the account's session (HTTP 200)", startedAt: new Date(), finishedAt: new Date() } });
      await deps.Coupon.updateOne({ _id: ids.coupon }, { $set: { status: "completed", finishedAt: new Date(), counts: { total: 3, queued: 0, in_progress: 0, success: 2, failed: 1, cancelled: 0, aborted: 0, timed_out: 0 } } });
      // …and the running update moved on: its second account was taken.
      await deps.Update.updateOne({ _id: ids.updateAfter }, { $set: { "accounts.1.status": "running", "accounts.1.startedAt": new Date() } });
      const before = await logCounts(ids.coupon, deps);
      assert.equal(before.queued, 1);
      // The request that ended since is no longer open, and was created long before: it is looked at because it WAS open.
      const res = await keeperPass({ deps, probes: everyQueue, since: ago(MIN), also: inStep.open, userId });
      assert.ok(res.synced >= 2, `${res.synced} request(s) brought in line`);
      assert.deepEqual(res.closed, []);
      assert.ok(!(res.open.coupons || []).includes(ids.coupon));
      for (const name of ["coupon", "updateAfter"]) assert.deepEqual(await logCounts(ids[name], deps), (await factsOf(name)).counts, name);
      const again = await keeperPass({ deps, probes: everyQueue, since: ago(MIN), also: res.open, userId });
      assert.deepEqual([again.synced, again.rows], [0, 0], "a second pass finds everything in step");
      // Between full passes an open fetch / cancel / reschedule is looked for among the last days' requests only; update, invoice and coupon requests of any age.
      const light = await keeperPass({ deps, probes: everyQueue, since: ago(MIN), everyOpen: false, userId, close: false });
      assert.deepEqual([light.open.update, light.open.invoices, light.open.cancel, light.open.fetch], [[ids.updateAfter], [ids.invoiceRetry], [ids.cancelRetry], [ids.fetchPreparing]]);
    });
    await check("the watch: a stretch without a worker starts at its first reading, readings far apart start it again, a worker seen ends it", () => {
      const now = Date.now();
      const w = new QueueWatch(2 * MIN);
      assert.equal(w.note("q", false, now - 45 * MIN), now - 45 * MIN);
      assert.equal(w.note("q", false, now - 44 * MIN), now - 45 * MIN, "the next reading, a minute on: the same stretch");
      assert.equal(w.note("q", false, now), now, "43 minutes unseen: it starts again");
      assert.equal(w.note("q", true, now + 1000), null);
      assert.equal(w.note("q", false, now + 2000), now + 2000, "a worker was seen: a new stretch");
    });
    await check("no worker on a queue is not enough to close: not on the first pass (a restart, an API that starts before its worker), and never while the request's jobs still sit on its queue", async () => {
      // A fresh watch: the invoice and cancel requests have been silent 40 minutes, but their queues were only now found without a worker.
      const first = await keeperPass({ deps, probes: probes(["order-update"], true), since: ago(MIN), userId, watch: new QueueWatch() });
      assert.deepEqual([first.closed, first.stale], [[], []]);
      // 45 minutes without a worker — but the invoice request's jobs are still on the queue: the worker that comes back runs them.
      const watch = new QueueWatch(Number.POSITIVE_INFINITY);
      for (const q of ["order-invoice-batch", "order-cancel"]) watch.note(q, false, Date.now() - 45 * MIN);
      const waiting = { ...probes(["order-update"], true), jobsWaiting: async (f: { head: { requestId: string } }) => f.head.requestId === ids.invoiceRetry };
      const res = await keeperPass({ deps, probes: waiting, since: ago(MIN), userId, watch, close: false });
      assert.deepEqual([res.closed, res.waiting, res.stale.map((x) => x.split(" — ")[0])], [[], [`invoices ${ids.invoiceRetry}`], [`cancel ${ids.cancelRetry}`]]);
      // A probe that cannot say (Redis not answering) keeps it too.
      const unsure = await keeperPass({ deps, probes: { ...probes(["order-update"], true), jobsWaiting: async () => Promise.reject(new Error("Redis down")) }, since: ago(MIN), userId, watch, close: false });
      assert.deepEqual([unsure.stale, unsure.waiting.length], [[], 2]);
    });
    await check("the pass closes them: unfinished tasks time out, the request ends, counters = rows", async () => {
      const closedAfter: string[] = [];
      // No worker on the invoice and cancel queues for 45 minutes, pass after pass, and none of their jobs left on them; the update queue has one; this process runs the preparing fetch.
      const watch = new QueueWatch(Number.POSITIVE_INFINITY);
      for (const q of ["order-invoice-batch", "order-cancel"]) watch.note(q, false, Date.now() - 45 * MIN);
      const res = await keeperPass({ deps, probes: probes(["order-update"], true), since: ago(MIN), userId, watch, afterClose: async (f) => void closedAfter.push(f.head.requestId) });
      assert.deepEqual([...res.closed].sort(), [ids.invoiceRetry, ids.cancelRetry].sort());
      assert.deepEqual([...closedAfter].sort(), [...res.closed].sort(), "the after-close step ran for each");

      const stale = (await getBatch(userId, ids.fetchStale, deps, P))!;
      assert.deepEqual([stale.status, stale.counts.timed_out, stale.counts.total, stale.can_cancel, stale.can_retry, stale.stalled], ["completed", 3, 3, false, true, false]);
      assert.ok(stale.finished_at, "it has an end now");
      const ledger = await deps.Ledger.findById(ids.fetchStale).lean();
      assert.deepEqual([ledger?.status, ledger?.message], ["failed", TIMED_OUT]);
      const staleTasks = await listTasks(userId, { request_id: ids.fetchStale }, deps);
      assert.deepEqual([staleTasks.count, [...new Set(staleTasks.results.map((t) => `${t.status}|${t.error}`))]], [3, [`timedout|${TIMED_OUT}`]]);

      const inv = (await getBatch(userId, ids.invoiceRetry, deps, P))!;
      assert.deepEqual([inv.status, inv.counts], ["completed", { total: 2, queued: 0, in_progress: 0, success: 0, failed: 0, cancelled: 0, aborted: 0, timed_out: 2 }]);
      assert.deepEqual(await logCounts(ids.invoiceRetry, deps), inv.counts);
      const doc = await deps.Invoice.findById(ids.invoiceRetry).lean();
      assert.deepEqual(doc?.tasks.map((t) => [t.status, t.error]), [["timedout", TIMED_OUT], ["timedout", TIMED_OUT]]);

      const can = (await getBatch(userId, ids.cancelRetry, deps, P))!;
      assert.deepEqual([can.status, can.counts.timed_out, can.counts.total], ["completed", 2, 2]);
      assert.deepEqual(await logCounts(ids.cancelRetry, deps), can.counts);
      assert.deepEqual(can.issues.map((x) => [x.status, x.count]), [["timedout", 2]]);

      // Left alone: the other deployment's coupons, the update whose worker is connected, the fetch this process is preparing.
      assert.equal((await factsOf("updateAfter")).status, "in_progress");
      assert.equal((await factsOf("fetchPreparing")).status, "queued");
      const left = await listBatches(userId, { status: "in_progress" }, deps, P);
      assert.deepEqual(left.results.map((r) => r.request_id), [ids.updateAfter]);
    });
    await check("closing by kind: update, reschedule and coupons too", async () => {
      const f = await factsOf("updateAfter");
      assert.equal(await closeTimedOut(f, deps), true);
      assert.equal(await closeTimedOut(f, deps), false, "a request that has ended is not closed twice");
      await syncRequestTasks("update", ids.updateAfter, deps);
      const after = await factsOf("updateAfter");
      assert.deepEqual([after.status, after.counts.timed_out > 0, after.counts.queued + after.counts.in_progress], ["completed", true, 0]);
      assert.deepEqual(await logCounts(ids.updateAfter, deps), after.counts);

      await deps.Coupon.updateOne({ _id: ids.coupon }, { $set: { status: "in_progress", finishedAt: null } });
      await deps.CouponTask.updateOne({ requestId: new Types.ObjectId(ids.coupon), email: "c3@verify.invalid" }, { $set: { status: "in_progress", finishedAt: null } });
      assert.equal(await closeTimedOut(await factsOf("coupon"), deps), true);
      const c = await factsOf("coupon");
      assert.deepEqual([c.status, c.counts], ["completed", { total: 3, queued: 0, in_progress: 0, success: 1, failed: 1, cancelled: 0, aborted: 0, timed_out: 1 }]);
      assert.equal((await deps.CouponTask.findOne({ requestId: new Types.ObjectId(ids.coupon), email: "c3@verify.invalid" }).lean())?.error, TIMED_OUT);

      await deps.Reschedule.updateOne({ _id: ids.reschedule }, { $set: { status: "running", finishedAt: null, "units.0.status": "running", "units.0.finishedAt": null } });
      assert.equal(await closeTimedOut(await factsOf("reschedule"), deps), true);
      assert.deepEqual((await factsOf("reschedule")).counts, { total: 4, queued: 0, in_progress: 0, success: 0, failed: 3, cancelled: 0, aborted: 0, timed_out: 1 });
    });
    await check("a fetch whose process died with rows open: closed at the next pass, counters follow the rows", async () => {
      const _id = oid();
      const createdAt = ago(3 * MIN);
      await raw(deps.Ledger).insertOne({ ...ledgerBase, _id, status: "running", total: 3, startedAt: createdAt, finishedAt: null, createdAt, updatedAt: createdAt, meta: { host: HOST, pid: 999, params: {} } });
      const t = openFetchTasks({ userId, runId: String(_id), source: "user", by: "verify@1gb.com", emails: ["d1@verify.invalid", "d2@verify.invalid", "d3@verify.invalid"], skipped: [], createdAt }, deps);
      t.started("d1@verify.invalid");
      t.succeeded("d1@verify.invalid");
      t.started("d2@verify.invalid");
      await taskLog.flush();
      await new Promise((r) => setTimeout(r, 2300)); // the run's counts reach its ledger row
      const res = await keeperPass({ deps, probes: probes([], false), since: ago(MIN), userId });
      assert.deepEqual(res.closed, [String(_id)]);
      const f = (await loadFacts("fetch", [String(_id)], deps)).get(String(_id))!;
      assert.deepEqual([f.status, f.counts], ["completed", { total: 3, queued: 0, in_progress: 0, success: 1, failed: 0, cancelled: 0, aborted: 0, timed_out: 2 }]);
      assert.deepEqual(await logCounts(String(_id), deps), f.counts);
      // Had that process only been slow, its own ending would come now: rows that have ended, and the counts of a closed row, are not written over.
      await t.finish(false).written;
      assert.deepEqual(await logCounts(String(_id), deps), f.counts);
      assert.deepEqual(((await deps.Ledger.findById(_id).lean())?.meta as { task_counts: RequestCounts }).task_counts, f.counts);
    });

    if (VOLUME) await volume(deps, VOLUME);
  } finally {
    for (const name of names) await mongoose.connection.db!.dropCollection(name).catch(() => undefined);
    const left = (await mongoose.connection.db!.listCollections({ name: { $regex: `^verify_rl_${run}_` } }).toArray()).length;
    const stray = await OrderSyncTask.countDocuments({ userId: { $in: [uid, new Types.ObjectId(other)] } }).catch(() => -1);
    console.log(`\ncleanup: ${names.length} throwaway collection(s) dropped (${left} left) · rows of the invented workspaces in the real order_sync_tasks: ${stray}`);
    if (left || stray > 0) failures.push("cleanup");
  }

  if (REAL) await real();
  await mongoose.disconnect();
  console.log(`\n${passed} passed, ${failures.length} failed`);
  process.exit(failures.length ? 1 : 0);
}

function pick<T extends Record<string, unknown>>(o: T, keys: string[]) {
  return Object.fromEntries(keys.map((k) => [k, o[k]]));
}

function istDayOf(at: Date) {
  return new Date(at.getTime() + 330 * MIN).toISOString().slice(0, 10);
}

/** How a query is answered: the stages of its winning plan, the index it reads, and what it examined. */
async function planOf(q: { explain: (v: "executionStats") => Promise<unknown> }): Promise<string> {
  const e = (await q.explain("executionStats")) as { queryPlanner?: { winningPlan?: unknown }; executionStats?: { totalKeysExamined?: number; totalDocsExamined?: number; nReturned?: number; executionTimeMillis?: number } };
  const stages: string[] = [];
  const walk = (p: unknown) => {
    if (!p || typeof p !== "object") return;
    const s = p as { stage?: string; indexName?: string; inputStage?: unknown; inputStages?: unknown[]; queryPlan?: unknown };
    if (s.stage) stages.push(s.indexName ? `${s.stage}(${s.indexName})` : s.stage);
    walk(s.inputStage);
    walk(s.queryPlan);
    for (const c of s.inputStages || []) walk(c);
  };
  walk(e.queryPlanner?.winningPlan);
  const x = e.executionStats || {};
  return `${stages.join(" ← ")} · keys ${x.totalKeysExamined} · docs ${x.totalDocsExamined} · returned ${x.nReturned} · ${x.executionTimeMillis} ms in the server`;
}

async function timed<T>(fn: () => Promise<T>): Promise<{ ms: number; out: T }> {
  const t = Date.now();
  const out = await fn();
  return { ms: Date.now() - t, out };
}

/** The task list over a realistic number of rows, in a throwaway collection with the real indexes. */
async function volume(deps: LogDeps, n: number) {
  console.log(`\nVOLUME — ${n} task rows in the throwaway log`);
  const userId = new Types.ObjectId();
  const requests = Array.from({ length: 60 }, () => String(new Types.ObjectId()));
  const statuses = ["success", "success", "success", "success", "success", "success", "failed", "cancelled", "aborted", "timedout", "queued", "in_progress"];
  const codes = ["not_logged_in", "nothing_available", "platform_unavailable", "unexpected_response"];
  const types = ["update", "update", "update", "fetch", "invoice_fetch", "cancel"];
  const t0 = Date.now();
  for (let i = 0; i < n; i += 5000) {
    const rows = Array.from({ length: Math.min(5000, n - i) }, (_, j) => {
      const k = i + j;
      const status = statuses[k % statuses.length];
      const createdAt = new Date(Date.now() - ((k % 60) * 6 * 60 + (k % 97)) * 60_000 * 0.25);
      return { userId, requestId: requests[k % 60], kind: "update", key: `k${k}`, platform: "FLIPKART", taskType: types[k % types.length], source: k % 9 ? "user" : "system-cron", status, email: `acc${k % 2100}@verify.invalid`, orderId: `ODVOL${k % 40000}`, unitId: `${k}`, triggeredBy: "verify", error: status === "failed" ? "x" : "", errorCode: status === "failed" ? codes[k % codes.length] : "", detail: "", startedAt: createdAt, finishedAt: createdAt, createdAt, updatedAt: createdAt };
    });
    await deps.Tasks.collection.insertMany(rows, { ordered: false });
  }
  console.log(`        inserted in ${Math.round((Date.now() - t0) / 1000)} s (${Math.round(n / ((Date.now() - t0) / 1000))} rows / s, 11 indexes)`);
  const uid = String(userId);
  // The requests do not exist as documents here, so request_id is timed straight on the collection below.
  const cases: Array<[string, Record<string, unknown>]> = [
    ["no filter", {}],
    ["page 40", { page: 40 }],
    ["status=failed", { status: "failed" }],
    ["error_code=not_logged_in", { error_code: "not_logged_in" }],
    ["task_type=fetch", { task_type: "fetch" }],
    ["source=system-cron", { source: "system-cron" }],
    ["order_id", { order_id: "ODVOL123" }],
    ["email", { email: "acc77@verify.invalid" }],
    ["status + task_type", { status: "failed", task_type: "update" }],
  ];
  for (const [name, q] of cases) {
    const { ms, out } = await timed(() => listTasks(uid, q, deps));
    console.log(`        ${name.padEnd(26)} ${String(ms).padStart(5)} ms   count ${out.count}, ${out.results.length} row(s)`);
  }
  const cut = new Date(Date.now() - SYNC_TASK_KEEP_MS);
  const byRequest = { requestId: requests[3], createdAt: { $gte: cut } };
  const r = await timed(() => Promise.all([deps.Tasks.countDocuments(byRequest), deps.Tasks.find(byRequest).sort({ createdAt: -1, _id: -1 }).limit(50).lean()]));
  console.log(`        ${"request_id".padEnd(26)} ${String(r.ms).padStart(5)} ms   count ${r.out[0]}, ${r.out[1].length} row(s)`);
  console.log(`        plan, no filter:   ${await planOf(deps.Tasks.find({ userId, createdAt: { $gte: cut } }).sort({ createdAt: -1, _id: -1 }).limit(50))}`);
  console.log(`        plan, status:      ${await planOf(deps.Tasks.find({ userId, createdAt: { $gte: cut }, status: "failed" }).sort({ createdAt: -1, _id: -1 }).limit(50))}`);
  console.log(`        plan, request_id:  ${await planOf(deps.Tasks.find(byRequest).sort({ createdAt: -1, _id: -1 }).limit(50))}`);
  const c = (await deps.Tasks.collection.aggregate([{ $match: { userId, createdAt: { $gte: cut } } }, { $count: "n" }]).explain("executionStats")) as { stages?: Array<{ $cursor?: { executionStats?: { totalKeysExamined?: number; totalDocsExamined?: number; executionTimeMillis?: number } } }>; executionStats?: { totalKeysExamined?: number; totalDocsExamined?: number; executionTimeMillis?: number } };
  const cs = c.stages?.[0]?.$cursor?.executionStats || c.executionStats || {};
  console.log(`        plan, the count:   keys ${cs.totalKeysExamined} · docs ${cs.totalDocsExamined} · ${cs.executionTimeMillis} ms in the server`);
}

/** READ-ONLY, on the real collections: how long the two lists take and how they are answered. */
async function real() {
  const userId = workspaceUserId();
  const uid = new Types.ObjectId(userId);
  console.log(`\nREAL DATABASE — read only (workspace ${userId.slice(-6)})`);
  const sizes = await Promise.all(SYNC_TASK_KINDS.map(async (k) => `${k} ${await (k === "fetch" ? OrderJobRun.countDocuments({ userId: uid, type: "fetch" }) : (k === "update" ? OrderUpdateRequest : k === "invoices" ? OrderInvoiceRequest : k === "coupons" ? OrderCouponRequest : k === "cancel" ? CancelRequest : RescheduleRequest).countDocuments({ userId: uid }))}`));
  console.log(`        requests: ${sizes.join(" · ")} · task rows ${await OrderSyncTask.estimatedDocumentCount()}`);
  for (const [name, q] of [
    ["batches, page 1", {}],
    ["batches, page 1 again", {}],
    ["batches, page 5", { page: 5 }],
    ["batches, update only", { task_type: "update" }],
    ["batches, cancel only", { task_type: "cancel" }],
    ["batches, completed + user", { status: "completed", source: "user" }],
    ["batches, last 3 days", { created_from: istDayOf(ago(3 * DAY)) }],
  ] as Array<[string, Record<string, unknown>]>) {
    const { ms, out } = await timed(() => listBatches(userId, q, logDefaults));
    console.log(`        ${name.padEnd(28)} ${String(ms).padStart(5)} ms   count ${out.count}, ${out.results.length} row(s)`);
  }
  for (const [name, model, match] of [
    ["order_job_runs", OrderJobRun, { userId: uid, type: "fetch" }],
    ["order_update_requests", OrderUpdateRequest, { userId: uid }],
    ["order_cancel_requests", CancelRequest, { userId: uid }],
  ] as Array<[string, Model<unknown>, Record<string, unknown>]>) {
    console.log(`        plan, ${name} ids: ${await planOf(model.find(match).sort({ createdAt: -1 }).limit(25).select("_id createdAt") as never)}`);
  }
  for (const [name, q] of [
    ["tasks, page 1", {}],
    ["tasks, page 1 again", {}],
    ["tasks, status=failed", { status: "failed" }],
    ["tasks, task_type=update", { task_type: "update" }],
    ["tasks, page 100", { page: 100 }],
  ] as Array<[string, Record<string, unknown>]>) {
    const { ms, out } = await timed(() => listTasks(userId, q, logDefaults));
    console.log(`        ${name.padEnd(28)} ${String(ms).padStart(5)} ms   count ${out.count}, ${out.results.length} row(s)`);
  }
  const cut = new Date(Date.now() - SYNC_TASK_KEEP_MS);
  console.log(`        plan, tasks:       ${await planOf(OrderSyncTask.find({ userId: uid, createdAt: { $gte: cut } }).sort({ createdAt: -1, _id: -1 }).limit(50))}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
