import { downloadInvoicesByOrderId, parseOrderIds } from "../services/invoiceOnDemand.js";
import { queueManualSweep } from "../services/invoiceSweep.js";
import { Router, type Response } from "express";
import { config } from "../config.js";
import { workspaceUserId } from "../services/sessionStore.js";
import { resolveOrderFetchMode, setOrderFetchMode } from "../services/orderSyncSettings.js";
import { getOrderFetchJob, isOrderFetchPreparing, stopOrderFetch } from "../services/orderFetch.js";
import { triggerFetch } from "../services/fetchTrigger.js";
import { createInvoiceRequest, getInvoiceRequest, listInvoiceRequests, stopInvoiceRequest } from "../services/invoiceRequests.js";
import { createCouponRequest, getCouponRequest, listCouponRequests, stopCouponRequest } from "../services/couponRequests.js";
import { assertFlipkart, RequestInputError } from "../services/requestCommon.js";
import { holdUpdatePreparation, parseFetchBody, parseUpdateBody, planUpdate, UPDATE_PREPARING_MESSAGE } from "../services/startRequests.js";
import { resolveCheckoutSettings, setCheckoutSettings } from "../services/checkoutSettings.js";
import { abortCancelRequest, getCancelRequest, listCancelRequests, previewCancellation, requestCancellation, resolveCancelSpeed, retryCancelRequest, setCancelSpeed } from "../services/orderCancel.js";
import { abortRescheduleRequest, getRescheduleRequest, listRescheduleRequests, previewReschedule, requestReschedule, retryRescheduleRequest } from "../services/orderReschedule.js";
import { proxyModeStatus, setProxyMode } from "../services/proxyMode.js";
import { finishRun, listRuns, startRun, syncPanel } from "../services/orderJobRuns.js";
import { fetchScheduleStatus, runScheduledFetch, setFetchSinceDays, setScheduledInvoices } from "../services/orderFetchSchedule.js";
import { changeSchedule, eligibleHere, scheduleOverview, scheduleStatuses } from "../services/scheduleSettings.js";
import { invoiceSettingsView, setInvoiceSettings } from "../services/invoiceSettings.js";
import { Types } from "mongoose";
import { liveProbes } from "../services/requestLogLive.js";
import { retryBatch, stopBatch } from "../services/syncActions.js";
import { batchIssues, getBatch, listBatches, listTasks } from "../services/syncBatches.js";
import { logDefaults } from "../services/syncTaskLog.js";
import {
  abortUpdateRequest,
  cancelUpdateRequest,
  createUpdateRequest,
  getUpdateRequest,
  legacyStatusView,
  listUpdateRequests,
  resolveUpdateWindows,
  setUpdateWindows,
  UPDATE_OFD_SWEEP_MS,
  UPDATE_SWEEP_MS,
  WINDOW_LIMITS,
} from "../services/orderUpdateQueue.js";

export const orderFetchRouter = Router();

function parseIds(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean);
  }
  return String(value || "")
    .split(/[\s,]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function userIdFrom(req: { body?: { userId?: unknown } }) {
  const fromBody = String(req.body?.userId || "").trim();
  return fromBody || workspaceUserId();
}

/** A body the trigger refuses answers with its own status and code; anything else is the route's usual 400. */
function sendError(res: Response, err: unknown, fallback: string, status = 400) {
  if (err instanceof RequestInputError) {
    res.status(err.status).json({ error: err.message, code: err.code });
    return;
  }
  res.status(status).json({ error: err instanceof Error ? err.message : fallback });
}

/**
 * Fetch Order Units. Body — the reference's, and the one this route always took:
 *   { platform, email_ids? | emails?, fetch_orders_after_date?, enable_login?,
 *     force_fetch?, run_update_after_fetch?, invoices?, by? }
 * No emails = every logged-in Flipkart account of the workspace. No date = the
 * workspace's look-back. Answers with `request_id` (the ledger run id). What
 * was asked is kept on that run (meta.params). `enable_login` and
 * `force_fetch` are recorded only: an account that is not logged in is
 * skipped and listed, as before, and one fetch runs at a time. The start
 * itself is services/fetchTrigger.ts — Batch requests → Retry uses it too.
 */
orderFetchRouter.post("/fetch/trigger", async (req, res) => {
  try {
    res.json(await triggerFetch(userIdFrom(req), parseFetchBody(req.body)));
  } catch (err) {
    sendError(res, err, "Could not start fetch");
  }
});

orderFetchRouter.post("/fetch/stop", async (req, res) => {
  try {
    const userId = userIdFrom(req);
    await stopOrderFetch(userId);
    res.json({ message: "Order fetch stopped. Chrome killed." });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Could not stop" });
  }
});

orderFetchRouter.get("/fetch/status", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  const job = getOrderFetchJob(userId);
  // While idle, report the mode the NEXT run would use (the dashboard switch,
  // else .env); while running, the mode this run was started with.
  const resolved = await resolveOrderFetchMode(userId).catch(() => null);
  // `preparing`: a fetch has been asked for and its account list is still being read — it is not `running` yet.
  res.json({ ...job, mode: job.running ? job.mode : resolved?.mode || job.mode, modeSource: resolved?.source || "env", preparing: isOrderFetchPreparing(userId) });
});

/** The fetch/update mode switch — what the dashboard pill shows and flips. */
orderFetchRouter.get("/fetch/mode", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  try {
    const resolved = await resolveOrderFetchMode(userId);
    res.json({ ...resolved, env_default: config.orderFetchMode });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Could not read the fetch mode" });
  }
});

orderFetchRouter.put("/fetch/mode", async (req, res) => {
  const userId = userIdFrom(req);
  const mode = String(req.body?.mode || "").trim().toLowerCase();
  if (mode !== "scrape" && mode !== "api") {
    res.status(400).json({ error: 'mode must be "scrape" or "api"' });
    return;
  }
  // Never flip under a run: the mode is read once at a run's start, and a
  // half-API half-page run would be unexplainable.
  if (getOrderFetchJob(userId).running || (await legacyStatusView(userId)).running) {
    res.status(409).json({ error: "A fetch or update is running — stop it or wait for it to finish, then switch" });
    return;
  }
  try {
    const resolved = await setOrderFetchMode(userId, mode, String(req.body?.changedBy || "dashboard"));
    res.json({ ...resolved, env_default: config.orderFetchMode });
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not set the fetch mode" });
  }
});

/**
 * Refresh Order Units Status & OTP. Body — the reference's six, and the one this route always took:
 *   { platform } | { platform, order_status } | { platform, email_ids | emails }
 *   | { platform, order_status, email_ids } | { platform, order_ids }
 *   | { platform, order_email_pairs | csv_rows: [{ email, order_id }] }   (+ source, by, parent_id)
 * Status labels are the reference's and are matched to how units store them
 * (services/startRequests.ts). At most 2000 order ids / emails / pairs.
 */
orderFetchRouter.post("/update/trigger", async (req, res) => {
  // The workspace's update-preparation slot (services/startRequests.ts): a second press while a request is being
  // put together must not queue the same update twice. Batch requests → Retry of an update takes the same slot.
  let release: (() => void) | null = null;
  try {
    const body = parseUpdateBody(req.body);
    const userId = userIdFrom(req);
    release = holdUpdatePreparation(userId);
    if (!release) {
      res.status(409).json({ error: UPDATE_PREPARING_MESSAGE, code: "update_preparing" });
      return;
    }
    const plan = await planUpdate(userId, body);
    // Queued on the worker (services/orderUpdateQueue.ts): one job per account.
    const request = await createUpdateRequest(userId, plan.input, { source: body.source, by: body.by, params: plan.params, parentId: body.parentId });
    res.json({
      message: "Order update queued on the worker",
      platform: body.platform,
      selection_method: plan.selection,
      tasks_created: request.counts.units,
      accounts: request.counts.accounts,
      windows: request.windows,
      request_id: request.id,
      params: plan.params,
      timestamp: Math.floor(Date.now() / 1000),
    });
  } catch (err) {
    sendError(res, err, "Could not start update");
  } finally {
    release?.();
  }
});

/** Stops the newest open request (or `requestId`): queued accounts are removed, running ones finish their current unit. */
/** The scheduled fetch: on/off, cron, look-back, how many IDs it would take, last tick, last scheduled run. */
orderFetchRouter.get("/fetch/schedule", async (req, res) => {
  try {
    const userId = String(req.query.userId || "").trim() || workspaceUserId();
    res.json(await fetchScheduleStatus(userId));
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Could not read the fetch schedule" });
  }
});

/** PUT body: { fetchSinceDays?, scheduledInvoices?, by? } — either or both. */
orderFetchRouter.put("/fetch/schedule", async (req, res) => {
  try {
    const by = String(req.body?.by || "").trim();
    if (req.body?.fetchSinceDays === undefined && req.body?.scheduledInvoices === undefined) throw new Error("Nothing to save: send fetchSinceDays and/or scheduledInvoices");
    if (req.body?.fetchSinceDays !== undefined) await setFetchSinceDays(userIdFrom(req), req.body.fetchSinceDays, by);
    if (req.body?.scheduledInvoices !== undefined) await setScheduledInvoices(userIdFrom(req), req.body.scheduledInvoices, by);
    res.json(await fetchScheduleStatus(userIdFrom(req)));
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not save the fetch schedule" });
  }
});

/**
 * Run the scheduled fetch now (same selection and look-back; the row shows system-cron). Body: { by? }
 * It takes the nightly fetch's lease by hand, so it works whatever the switch says and on a machine the schedule never
 * uses. 202 { started: true, message, emails, sinceDays, invoices, runId }. 409 { started: false, message, error }
 * when nothing was started: a fetch is already running here or on another machine, no logged-in Platform ID, it could
 * not start — or another machine holds the lease (`code: "held_elsewhere"`, `holder`): "The nightly fetch is running
 * on <host> since <time>". `error` repeats `message`: it is the key the dashboard's proxy reads on a refusal.
 */
orderFetchRouter.post("/fetch/schedule/run", async (req, res) => {
  try {
    const out = await runScheduledFetch(userIdFrom(req), String(req.body?.by || "").trim() || "run now");
    res.status(out.started ? 202 : 409).json(out.started ? out : { ...out, error: out.message });
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not run the scheduled fetch" });
  }
});

/**
 * The four schedules — nightly fetch, update of all active units, out-for-delivery recheck, invoice sweep:
 *   GET /schedules → { timezone, now, here: { systemId, host, eligible }, machines: [{ id, systemId, role, host,
 *       serves, eligible, seenAt, up }], schedules: [{ name, label, role, on, source, changedBy, changedAt,
 *       eligibleHere, clock, nextAt, overdue, armedAt, lease, lastRun, lastTick, ticks (newest 20), servedOn,
 *       runnableOn, legacy, state, note }] }
 * `on` is the switch in the database, shared by every machine; a machine's .env only says whether it MAY run a
 * schedule (`here.eligible`, each machine's `eligible`). `state` / `note` say where a schedule stands in one word and
 * one sentence. Built by services/scheduleSettings.ts.
 */
orderFetchRouter.get("/schedules", async (req, res) => {
  try {
    const userId = String(req.query.userId || "").trim() || workspaceUserId();
    res.json(await scheduleOverview(userId));
  } catch (err) {
    sendError(res, err, "Could not read the schedules", 500);
  }
});

/**
 * Switch a schedule on or off, and/or choose the machine that runs it when it is awake. Body:
 *   { on?: boolean, preferSystemId?: string ("" = no preference), by? }
 * A switch changes the next time; a run in progress finishes. 400 for a name that is not a schedule, an `on` that is
 * not true / false, or nothing to save — before anything is written. Answers what GET /schedules answers.
 */
orderFetchRouter.put("/schedules/:name", async (req, res) => {
  try {
    res.json(await changeSchedule(userIdFrom(req), req.params.name, req.body));
  } catch (err) {
    sendError(res, err, "Could not save the schedule");
  }
});

/**
 * Invoice settings (Settings → Invoice settings in the dashboard; services/invoiceSettings.ts). One document per
 * workspace; a field it does not hold follows the .env, then the default.
 *   GET /invoice-settings?userId= → { userId, settings (the document's fields only), effective (all six, what applies
 *       now), sources ({ field: "db" | "env" | "default" }), history (newest first, ≤ 50: { field, from, to, by, at }),
 *       changedBy, changedAt, limits ({ field: { type, default, min?, max?, unit? } }) }
 *   PUT /invoice-settings { userId, by, ...fields } → the same as GET, after the save. 400 for an unknown field, a value
 *       out of its range (whole numbers; booleans true / false), or nothing to save — before anything is written.
 * A save reaches the worker within 30 s (its cache), with no restart.
 */
orderFetchRouter.get("/invoice-settings", async (req, res) => {
  try {
    const userId = String(req.query.userId || "").trim() || workspaceUserId();
    res.json(await invoiceSettingsView(userId));
  } catch (err) {
    sendError(res, err, "Could not read the invoice settings", 500);
  }
});

orderFetchRouter.put("/invoice-settings", async (req, res) => {
  try {
    const body = req.body && typeof req.body === "object" && !Array.isArray(req.body) ? { ...req.body, userId: userIdFrom(req) } : req.body;
    res.json(await setInvoiceSettings(body));
  } catch (err) {
    sendError(res, err, "Could not save the invoice settings", 500);
  }
});

/** "Sync Order Units From Platform": one row per job type (fetch, update, invoices placeholder, coupons, reschedule, cancel). */
orderFetchRouter.get("/sync/panel", async (req, res) => {
  try {
    const userId = String(req.query.userId || "").trim() || workspaceUserId();
    res.json(await syncPanel(userId));
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Could not build the sync panel" });
  }
});

orderFetchRouter.get("/sync/runs", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  const type = String(req.query.type || "") as "fetch" | "update" | "invoices" | "coupons" | "reschedule" | "cancel" | "";
  res.json({ runs: await listRuns(userId, type || undefined, Number(req.query.limit) || 20) });
});

/**
 * Batch requests — every request of every kind (fetch, update, invoice fetch, coupon apply, cancel,
 * reschedule), newest first, in the reference's shape:
 *   GET /sync/batches?page=&page_size=25&platform=&task_type=&source=&status=&created_from=&created_to=
 *   → { count, page, page_size, task_types, results: [{ request_id, platform, task_type, source, status,
 *       triggered_by, parent_id, child_ids, params, created_at, started_at, finished_at, counts, finished_count,
 *       progress_percent, avg_seconds_per_task, eta_seconds, duration_seconds, can_cancel, can_retry, … }] }
 * created_from / created_to are India-time days. Built by services/syncBatches.ts.
 */
orderFetchRouter.get("/sync/batches", async (req, res) => {
  try {
    const userId = String(req.query.userId || "").trim() || workspaceUserId();
    res.json(await listBatches(userId, req.query as Record<string, unknown>, logDefaults, liveProbes));
  } catch (err) {
    sendError(res, err, "Could not list the batch requests", 500);
  }
});

/** The tasks of one request that ended one way (?status=failed | timedout | aborted | cancelled), grouped by error class, with the items for "Copy all". */
orderFetchRouter.get("/sync/batches/:id/issues", async (req, res) => {
  try {
    const userId = String(req.query.userId || "").trim() || workspaceUserId();
    const out = await batchIssues(userId, req.params.id, req.query as Record<string, unknown>);
    if (!out) {
      res.status(404).json({ error: "Request not found" });
      return;
    }
    res.json(out);
  } catch (err) {
    sendError(res, err, "Could not read the request's issues", 500);
  }
});

/** One request: its batch row, SETTINGS as labels, OUTCOME, and the ISSUES summary. The full id only. */
orderFetchRouter.get("/sync/batches/:id", async (req, res) => {
  try {
    const userId = String(req.query.userId || "").trim() || workspaceUserId();
    const out = await getBatch(userId, req.params.id, logDefaults, liveProbes);
    if (!out) {
      res.status(404).json({ error: "Request not found" });
      return;
    }
    res.json(out);
  } catch (err) {
    sendError(res, err, "Could not read the request", 500);
  }
});

/**
 * Cancel one request, whatever its kind — the reference's row action "Cancel" (ours was "Stop"; the stop routes of
 * each kind still work and this calls them). Body: { by? }. What is queued ends "Cancelled before it started", what
 * is running finishes or ends "Stopped while running: the request was cancelled", and the request reads `cancelled`
 * with its tally kept. Answers the request as GET /sync/batches/:id does, with `message` and `stop`. 409 when it has
 * already ended (`not_open`), is another machine's to stop (`other_machine`) or is already being cancelled.
 */
orderFetchRouter.post("/sync/batches/:id/cancel", async (req, res) => {
  try {
    const out = await stopBatch(userIdFrom(req), req.params.id, String(req.body?.by || "").trim());
    if (!out) {
      res.status(404).json({ error: "Request not found" });
      return;
    }
    res.json(out);
  } catch (err) {
    sendError(res, err, "Could not cancel the request", 500);
  }
});

/**
 * Retry the unsuccessful tasks of a request that has ended, as ONE new request of the same kind and settings
 * (source `system-retry`, parent = this request). Body — the reference's:
 *   { statuses: ["failed"] }                      the tasks that ended that way (failed | timedout | aborted | cancelled)
 *   { statuses: ["failed"], error_code: "…" }     one error class of them
 *   { all_incomplete: true }                      failed + timed out + aborted + cancelled
 * (+ by). Items already queued or running in any request are skipped. 202 with the new `request_id`, what was
 * retried and what was skipped. 409: still open, tasks no longer kept (7 days), nothing to retry, everything
 * already queued; the kind's own refusals ("Order fetch is already running", …) are answered in its words.
 */
orderFetchRouter.post("/sync/batches/:id/retry", async (req, res) => {
  try {
    const out = await retryBatch(userIdFrom(req), req.params.id, req.body, String(req.body?.by || "").trim());
    if (!out) {
      res.status(404).json({ error: "Request not found" });
      return;
    }
    res.status(202).json(out);
  } catch (err) {
    sendError(res, err, "Could not retry the request");
  }
});

/**
 * All tasks — the task log (order_sync_tasks), kept 7 days:
 *   GET /sync/tasks?page=&page_size=50&order_id=&email=&platform=&task_type=&source=&status=&error_code=&request_id=&created_from=&created_to=
 *   → { count, error_classes: [{ code, label }], results: [{ id, request_id, platform, task_type, source, status,
 *       email, order_id, unit_id, triggered_by, created_at, started_at, finished_at, duration_seconds, error, error_code }] }
 * request_id is the full id (the 8-character one is refused).
 */
orderFetchRouter.get("/sync/tasks", async (req, res) => {
  try {
    const userId = String(req.query.userId || "").trim() || workspaceUserId();
    res.json(await listTasks(userId, req.query as Record<string, unknown>));
  } catch (err) {
    sendError(res, err, "Could not list the tasks", 500);
  }
});

orderFetchRouter.post("/update/stop", async (req, res) => {
  try {
    const userId = userIdFrom(req);
    const out = await cancelUpdateRequest(userId, String(req.body?.requestId || "") || undefined);
    res.json({ message: out ? `Order update stopped — ${out.removed} account(s) removed from the queue` : "No order update is running", ...(out || {}) });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Could not stop" });
  }
});

/**
 * On-demand invoice download by order ID (the "Download Order Invoices" modal).
 * Body: { orderIds: string[] | "id id,id", force?: boolean }. Resolves each
 * order's account, downloads the bundle over the API, and lets the pipeline
 * re-extract. Inline for a modest batch; larger sets belong on the invoice job.
 */
const MAX_INLINE_INVOICE_IDS = 100;
orderFetchRouter.post("/invoices/download", async (req, res) => {
  try {
    const userId = userIdFrom(req);
    const orderIds = parseOrderIds(req.body?.orderIds);
    const force = Boolean(req.body?.force);
    if (!orderIds.length) {
      res.status(400).json({ error: "No order IDs given" });
      return;
    }
    if (orderIds.length > MAX_INLINE_INVOICE_IDS) {
      res.status(400).json({ error: `Too many order IDs (${orderIds.length}); download at most ${MAX_INLINE_INVOICE_IDS} at once here` });
      return;
    }
    // Record it as an `invoices` run so the Sync panel / running indicator shows it.
    await startRun(userId, "invoices", {
      trigger: "user",
      by: String(req.body?.by || "").trim(),
      total: orderIds.length,
      message: `Downloading ${orderIds.length} invoice(s)…`,
      meta: { request: "invoice_fetch", platform: "FLIPKART", inline: true, params: { order_ids_count: orderIds.length, force } },
    });
    try {
      const results = await downloadInvoicesByOrderId({ userId, orderIds, force, concurrency: 6 });
      const summary = results.reduce<Record<string, number>>((a, r) => ((a[r.status] = (a[r.status] || 0) + 1), a), {});
      const success = results.filter((r) => r.status === "downloaded" || r.status === "already").length;
      const failed = results.filter((r) => r.status === "failed").length;
      // The ledger row counts every order that did not get its invoice and names each one; the answer below is unchanged.
      const notDone = results.filter((r) => r.status !== "downloaded" && r.status !== "already");
      const failedItems = notDone.map((r) => ({ key: r.orderId, reason: r.status === "unknown_order" ? "Order not found on this account" : r.status === "no_session" ? "Account is not logged in" : r.error || "failed" }));
      await finishRun(userId, "invoices", { status: "completed", total: results.length, processed: results.length, success, failed: notDone.length, cancelled: 0, failedItems, message: `Downloaded ${summary.downloaded || 0}, already ${summary.already || 0}, failed ${failed}${notDone.length > failed ? `, not found or not logged in ${notDone.length - failed}` : ""}` });
      res.json({ results, summary, total: results.length });
    } catch (err) {
      await finishRun(userId, "invoices", { status: "failed", total: orderIds.length, processed: 0, success: 0, failed: orderIds.length, cancelled: 0, message: err instanceof Error ? err.message : "download failed" });
      throw err;
    }
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Could not download invoices" });
  }
});

/**
 * Start the time-settled invoice sweep ("Capture newly delivered now"). Body: { by? }. Enqueues one job; the worker
 * runs it and the Sync panel shows it. It takes the invoice sweep's lease by hand, so it works whatever the switch
 * says. 409 { error, code: "held_elsewhere", holder } while another machine holds the lease: "The invoice sweep is
 * running on <host> since <time>". While this machine is sweeping, the job waits behind that sweep, as it always did.
 */
orderFetchRouter.post("/invoices/sweep", async (req, res) => {
  try {
    const out = await queueManualSweep(userIdFrom(req), String(req.body?.by || "").trim());
    if (!out.ok) {
      res.status(out.status).json({ error: out.message, code: out.code, holder: out.holder });
      return;
    }
    res.json({ message: out.message });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Could not start the invoice sweep" });
  }
});

/**
 * Download Order Invoices as a batch — the reference's "Invoice fetch".
 * Body: { platform, order_ids: string[] | "id id,id", force?, by? } — up to
 * 2000 distinct order ids. Answers 202 at once with the request; the worker
 * downloads on the order-invoice-batch queue (services/invoiceRequests.ts).
 * Every order is downloaded again, stored or not. `force` skips the one
 * check there is: without it an order another open request already holds is
 * left to that request.
 */
orderFetchRouter.post("/invoice/trigger", async (req, res) => {
  try {
    assertFlipkart(req.body?.platform);
    const result = await createInvoiceRequest(userIdFrom(req), { orderIds: req.body?.order_ids ?? req.body?.orderIds, force: req.body?.force === true, by: String(req.body?.by || "").trim() });
    // `message` is the request's own sentence when it has one ("None of these orders is in Order Units").
    res.status(202).json({ selection_method: "order_ids", ...result, message: result.message || "Invoice fetch queued", timestamp: Math.floor(Date.now() / 1000) });
  } catch (err) {
    sendError(res, err, "Could not queue the invoice fetch");
  }
});

/** ?limit= (20), and to narrow: ?source=user,system-retry (comma list) and ?sweep=0 | 1 (the 3-hourly sweep's runs, or none of them). */
orderFetchRouter.get("/invoice/requests", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  const sources = String(req.query.source || "").split(",").map((x) => x.trim()).filter(Boolean);
  const sweep = String(req.query.sweep ?? "").trim();
  res.json({ requests: await listInvoiceRequests(userId, Number(req.query.limit) || 20, { sources, sweep: sweep === "0" ? false : sweep === "1" ? true : undefined }) });
});

/** One request with its per-order tasks. */
orderFetchRouter.get("/invoice/requests/:id", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  const row = await getInvoiceRequest(userId, req.params.id);
  if (!row) {
    res.status(404).json({ error: "Invoice request not found" });
    return;
  }
  res.json(row);
});

/** Stop: queued orders become cancelled, an order being downloaded finishes. Body: { by? }. */
orderFetchRouter.post("/invoice/requests/:id/stop", async (req, res) => {
  try {
    const row = await stopInvoiceRequest(userIdFrom(req), req.params.id, String(req.body?.by || "").trim());
    if (!row) {
      res.status(404).json({ error: "Invoice request not found" });
      return;
    }
    res.json(row);
  } catch (err) {
    sendError(res, err, "Could not stop the invoice fetch", 500);
  }
});

/**
 * Apply Company Coupons — the reference's "Coupon apply". Body:
 * { coupon_links: "url url" | string[], email_ids? | emails?, by? }. No emails
 * = every logged-in Flipkart account. Answers 202 with the request; the
 * worker opens each link with each account's session on the
 * order-coupon-apply queue and records what Flipkart answered
 * (services/couponRequests.ts — activation itself is unverified).
 */
orderFetchRouter.post("/company-coupon/trigger", async (req, res) => {
  try {
    assertFlipkart(req.body?.platform);
    const result = await createCouponRequest(userIdFrom(req), { links: req.body?.coupon_links, emails: req.body?.email_ids ?? req.body?.emails, by: String(req.body?.by || "").trim() });
    res.status(202).json({ ...result, message: result.message || "Coupon apply queued", timestamp: Math.floor(Date.now() / 1000) });
  } catch (err) {
    sendError(res, err, "Could not queue the coupon apply");
  }
});

orderFetchRouter.get("/company-coupon/requests", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  res.json({ requests: await listCouponRequests(userId, Number(req.query.limit) || 20) });
});

/** One request with a page of its per-account tasks: ?status=&limit= (≤500, default 200)&skip=. */
orderFetchRouter.get("/company-coupon/requests/:id", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  const row = await getCouponRequest(userId, req.params.id, { status: String(req.query.status || "").trim() || undefined, limit: Number(req.query.limit) || undefined, skip: Number(req.query.skip) || undefined });
  if (!row) {
    res.status(404).json({ error: "Coupon request not found" });
    return;
  }
  res.json(row);
});

orderFetchRouter.post("/company-coupon/requests/:id/stop", async (req, res) => {
  try {
    const row = await stopCouponRequest(userIdFrom(req), req.params.id, String(req.body?.by || "").trim());
    if (!row) {
      res.status(404).json({ error: "Coupon request not found" });
      return;
    }
    res.json(row);
  } catch (err) {
    sendError(res, err, "Could not stop the coupon apply", 500);
  }
});

/** The dashboard's status panel: same shape as the old in-process job, built from the newest request. */
orderFetchRouter.get("/update/status", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  res.json(await legacyStatusView(userId));
});

orderFetchRouter.get("/update/requests", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  res.json({ requests: await listUpdateRequests(userId, Number(req.query.limit) || 20) });
});

orderFetchRouter.get("/update/requests/:id", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  const row = await getUpdateRequest(userId, req.params.id);
  if (!row) {
    res.status(404).json({ error: "Update request not found" });
    return;
  }
  res.json(row);
});

/**
 * Admin "Force clear" for a stranded update — the twin of /cancel/:id/abort.
 * Drops any per-account jobs still on the queue and closes the stuck request so
 * the panel is clean; an update parks no Order rows, so re-running is just the
 * Update button again. Body: { by?, mode? } (mode kept for API symmetry).
 */
orderFetchRouter.post("/update/requests/:id/abort", async (req, res) => {
  try {
    const mode = String(req.body?.mode || "reset").toLowerCase() === "abort" ? "abort" : "reset";
    const result = await abortUpdateRequest(userIdFrom(req), req.params.id, String(req.body?.by || "").trim(), mode);
    if (!result) {
      res.status(404).json({ error: "Update request not found" });
      return;
    }
    res.status(200).json(result);
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not clear the update" });
  }
});

/**
 * Update speed and schedules. PUT body: { windows, by? } — applies to the next job, no restart.
 * `schedule.enabled` is the "Update all active units" switch and `ofdEnabled` the out-for-delivery one (GET /schedules
 * has both in full); the periods and next times are the clocks', and `eligibleHere` says whether this machine's .env
 * lets it run them.
 * It answers 200 whenever the speed card can be drawn, as it did before the clock: for a userId that is not a workspace
 * id, or when the schedule read fails, the schedule part reads off with the .env periods and no next times.
 */
orderFetchRouter.get("/update/settings", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  const [speed, sched] = await Promise.all([
    resolveUpdateWindows(userId),
    Types.ObjectId.isValid(userId) ? scheduleStatuses(userId, ["update_active", "update_ofd"]).catch(() => null) : Promise.resolve(null),
  ]);
  const everyMs = (name: "update_active" | "update_ofd", fallback: number) => {
    const clock = sched?.[name].clock;
    return clock?.kind === "every" ? clock.everyMin * 60_000 : fallback;
  };
  res.json({
    ...speed,
    limits: WINDOW_LIMITS,
    schedule: {
      enabled: sched?.update_active.on ?? false,
      ofdEnabled: sched?.update_ofd.on ?? false,
      activeEveryMs: everyMs("update_active", UPDATE_SWEEP_MS),
      ofdEveryMs: everyMs("update_ofd", UPDATE_OFD_SWEEP_MS),
      activeNextAt: sched?.update_active.nextAt ?? null,
      ofdNextAt: sched?.update_ofd.nextAt ?? null,
      eligibleHere: sched?.update_active.eligibleHere ?? eligibleHere("update_active"),
    },
  });
});

orderFetchRouter.put("/update/settings", async (req, res) => {
  try {
    const speed = await setUpdateWindows(userIdFrom(req), req.body?.windows, String(req.body?.by || "").trim());
    res.json({ ...speed, limits: WINDOW_LIMITS });
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not save the update speed" });
  }
});

orderFetchRouter.post("/sync/stop", async (req, res) => {
  try {
    const userId = userIdFrom(req);
    await stopOrderFetch(userId);
    await cancelUpdateRequest(userId);
    res.json({ message: "Fetch and update stopped. Chrome killed." });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Could not stop" });
  }
});

/**
 * Cancel Order Units. Body: { type: "unit" | "order", ids: "id id,id" | string[],
 * accounts?: { [orderId]: email }, email?: string, dryRun?: boolean }. Answers
 * at once with what was queued and what was refused; the units are cancelled by
 * the worker (order-cancel queue), one account at a time per account, accounts
 * in parallel. An Order ID never fetched into Order Units is read from Flipkart
 * first through the Platform ID named for it (`accounts`, else `email`).
 */
orderFetchRouter.post("/cancel", async (req, res) => {
  try {
    const type = String(req.body?.type || "").toLowerCase();
    if (type !== "unit" && type !== "order") {
      res.status(400).json({ error: 'type must be "unit" or "order"' });
      return;
    }
    const accounts: Record<string, string> = {};
    if (req.body?.accounts && typeof req.body.accounts === "object") {
      for (const [orderId, email] of Object.entries(req.body.accounts as Record<string, unknown>)) {
        if (String(email || "").trim()) accounts[orderId] = String(email).trim().toLowerCase();
      }
    }
    const result = await requestCancellation(userIdFrom(req), {
      type,
      ids: parseIds(req.body?.ids),
      accounts,
      email: String(req.body?.email || "").trim().toLowerCase(),
      urgent: req.body?.urgent === true,
      dryRun: req.body?.dryRun === true,
      by: String(req.body?.by || "").trim(),
    });
    res.status(202).json(result);
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not queue the cancellation" });
  }
});

/** Same body as POST /cancel; answers what it would queue / refuse, with an ETA. Nothing is written. */
orderFetchRouter.post("/cancel/preview", async (req, res) => {
  try {
    const type = String(req.body?.type || "").toLowerCase();
    if (type !== "unit" && type !== "order") {
      res.status(400).json({ error: 'type must be "unit" or "order"' });
      return;
    }
    const accounts: Record<string, string> = {};
    if (req.body?.accounts && typeof req.body.accounts === "object") {
      for (const [orderId, email] of Object.entries(req.body.accounts as Record<string, unknown>)) {
        if (String(email || "").trim()) accounts[orderId] = String(email).trim().toLowerCase();
      }
    }
    res.json(await previewCancellation(userIdFrom(req), { type, ids: parseIds(req.body?.ids), accounts, email: String(req.body?.email || "").trim().toLowerCase(), urgent: req.body?.urgent === true }));
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not preview the cancellation" });
  }
});

/** A new request from the failed units of :id. Body: { by?: string, userId?: string }. */
orderFetchRouter.post("/cancel/:id/retry", async (req, res) => {
  try {
    const result = await retryCancelRequest(userIdFrom(req), req.params.id, String(req.body?.by || "").trim());
    if (!result) {
      res.status(404).json({ error: "Cancel request not found" });
      return;
    }
    res.status(202).json(result);
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not retry the cancellation" });
  }
});

/**
 * Force-clear a stuck cancel request (SuperAdmin — the dashboard gates the role).
 * Body: { by?: string, userId?: string, mode?: "reset" | "abort" }. Default
 * "reset" frees the stuck units so they can be re-cancelled; "abort" just closes it.
 */
orderFetchRouter.post("/cancel/:id/abort", async (req, res) => {
  try {
    const mode = String(req.body?.mode || "reset").toLowerCase() === "abort" ? "abort" : "reset";
    const result = await abortCancelRequest(userIdFrom(req), req.params.id, String(req.body?.by || "").trim(), mode);
    if (!result) {
      res.status(404).json({ error: "Cancel request not found" });
      return;
    }
    res.status(200).json(result);
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not clear the cancellation" });
  }
});

/**
 * Reschedule Order Units — Flipkart's "Change Date" per unit, in the Platform
 * ID's Chrome (order-reschedule queue). Body: { type: "unit"|"order", ids,
 * date: "YYYY-MM-DD", by? }. Answers at once with what was queued / refused.
 */
orderFetchRouter.post("/reschedule", async (req, res) => {
  try {
    const type = String(req.body?.type || "").toLowerCase();
    if (type !== "unit" && type !== "order") {
      res.status(400).json({ error: 'type must be "unit" or "order"' });
      return;
    }
    res.status(202).json(await requestReschedule(userIdFrom(req), { type, ids: parseIds(req.body?.ids), date: String(req.body?.date || ""), by: String(req.body?.by || "").trim() }));
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not queue the reschedule" });
  }
});

orderFetchRouter.post("/reschedule/preview", async (req, res) => {
  try {
    const type = String(req.body?.type || "").toLowerCase();
    if (type !== "unit" && type !== "order") {
      res.status(400).json({ error: 'type must be "unit" or "order"' });
      return;
    }
    res.json(await previewReschedule(userIdFrom(req), { type, ids: parseIds(req.body?.ids), date: String(req.body?.date || "") }));
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not preview the reschedule" });
  }
});

orderFetchRouter.post("/reschedule/:id/retry", async (req, res) => {
  try {
    const result = await retryRescheduleRequest(userIdFrom(req), req.params.id, String(req.body?.by || "").trim());
    if (!result) {
      res.status(404).json({ error: "Reschedule request not found" });
      return;
    }
    res.status(202).json(result);
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not retry the reschedule" });
  }
});

/**
 * Admin "Force clear" for a stranded reschedule — the twin of the cancel /
 * update abort. "reset" frees its units for a re-run; "abort" just closes it.
 */
orderFetchRouter.post("/reschedule/:id/abort", async (req, res) => {
  try {
    const mode = String(req.body?.mode || "reset").toLowerCase() === "abort" ? "abort" : "reset";
    const result = await abortRescheduleRequest(userIdFrom(req), req.params.id, String(req.body?.by || "").trim(), mode);
    if (!result) {
      res.status(404).json({ error: "Reschedule request not found" });
      return;
    }
    res.status(200).json(result);
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not clear the reschedule" });
  }
});

orderFetchRouter.get("/reschedule", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  res.json({ requests: await listRescheduleRequests(userId, Number(req.query.limit) || 20) });
});

orderFetchRouter.get("/reschedule/:id", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  const row = await getRescheduleRequest(userId, req.params.id);
  if (!row) {
    res.status(404).json({ error: "Reschedule request not found" });
    return;
  }
  res.json(row);
});

/** Proxy mode per environment — Settings → Environments. GET → status; PUT { env: local|production, mode: off|preferred|required, by? }. */
orderFetchRouter.get("/proxy-mode", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  res.json(await proxyModeStatus(userId));
});

orderFetchRouter.put("/proxy-mode", async (req, res) => {
  try {
    res.json(await setProxyMode(userIdFrom(req), req.body?.env, req.body?.mode, String(req.body?.by || "").trim()));
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not set the proxy mode" });
  }
});

orderFetchRouter.get("/cancel", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  res.json({ requests: await listCancelRequests(userId, Number(req.query.limit) || 20) });
});

/** Cancel speed: { windows, tabs, source, changedBy, changedAt, limits }. PUT body: { windows?, tabs?, by? }. Applies to the next job, no restart. */
/** HTTP checkout engine switches (services/checkoutSettings.ts). PUT body: { httpVerifyWaitSec?, by? }. */
orderFetchRouter.get("/checkout-settings", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  res.json(await resolveCheckoutSettings(userId));
});
orderFetchRouter.put("/checkout-settings", async (req, res) => {
  try {
    res.json(await setCheckoutSettings(userIdFrom(req), { httpVerifyWaitSec: req.body?.httpVerifyWaitSec }, String(req.body?.by || "").trim()));
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not save the checkout settings" });
  }
});

orderFetchRouter.get("/cancel/settings", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  res.json(await resolveCancelSpeed(userId));
});

orderFetchRouter.put("/cancel/settings", async (req, res) => {
  try {
    res.json(await setCancelSpeed(userIdFrom(req), { windows: req.body?.windows, tabs: req.body?.tabs }, String(req.body?.by || "").trim()));
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not save the cancel speed" });
  }
});

orderFetchRouter.get("/cancel/:id", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  const row = await getCancelRequest(userId, req.params.id);
  if (!row) {
    res.status(404).json({ error: "Cancel request not found" });
    return;
  }
  res.json(row);
});
