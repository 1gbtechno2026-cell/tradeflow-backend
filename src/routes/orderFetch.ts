import { downloadInvoicesByOrderId, parseOrderIds } from "../services/invoiceOnDemand.js";
import { getInvoiceQueue } from "../queue.js";
import { Router, type Response } from "express";
import { config } from "../config.js";
import { workspaceUserId } from "../services/sessionStore.js";
import { resolveOrderFetchMode, setOrderFetchMode } from "../services/orderSyncSettings.js";
import {
  getOrderFetchJob,
  isOrderFetchPreparing,
  OrderFetchStoppedError,
  reserveOrderFetch,
  startOrderFetch,
  stopOrderFetch,
  type OrderFetchStartOptions,
} from "../services/orderFetch.js";
import { createInvoiceRequest, getInvoiceRequest, listInvoiceRequests, stopInvoiceRequest } from "../services/invoiceRequests.js";
import { createCouponRequest, getCouponRequest, listCouponRequests, stopCouponRequest } from "../services/couponRequests.js";
import { assertFlipkart, hostName, RequestInputError } from "../services/requestCommon.js";
import { parseFetchBody, parseUpdateBody, planFetch, planUpdate, queueUpdateAfterFetch } from "../services/startRequests.js";
import { resolveCheckoutSettings, setCheckoutSettings } from "../services/checkoutSettings.js";
import { abortCancelRequest, getCancelRequest, listCancelRequests, previewCancellation, requestCancellation, resolveCancelSpeed, retryCancelRequest, setCancelSpeed } from "../services/orderCancel.js";
import { abortRescheduleRequest, getRescheduleRequest, listRescheduleRequests, previewReschedule, requestReschedule, retryRescheduleRequest } from "../services/orderReschedule.js";
import { proxyModeStatus, setProxyMode } from "../services/proxyMode.js";
import { createQueuedRun, failRunById, finishRun, listRuns, startRun, stopRunById, syncPanel } from "../services/orderJobRuns.js";
import { fetchScheduleStatus, runScheduledFetch, setFetchSinceDays, setScheduledInvoices } from "../services/orderFetchSchedule.js";
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
  UPDATE_SCHEDULE_ENABLED,
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

/** Up to this many accounts the trigger answers once the run is going; a longer list is prepared in the background (≈0.1 s per account). */
const FETCH_INLINE_START_MAX = 100;

/**
 * Fetch Order Units. Body — the reference's, and the one this route always took:
 *   { platform, email_ids? | emails?, fetch_orders_after_date?, enable_login?,
 *     force_fetch?, run_update_after_fetch?, invoices?, by? }
 * No emails = every logged-in Flipkart account of the workspace. No date = the
 * workspace's look-back. Answers with `request_id` (the ledger run id). What
 * was asked is kept on that run (meta.params). `enable_login` and
 * `force_fetch` are recorded only: an account that is not logged in is
 * skipped and listed, as before, and one fetch runs at a time.
 */
orderFetchRouter.post("/fetch/trigger", async (req, res) => {
  // The workspace's one fetch slot, held from the first check until startOrderFetch has taken over:
  // working out who to fetch takes a second or two, and a second trigger in that window is refused, not answered.
  let release: (() => void) | undefined;
  try {
    const body = parseFetchBody(req.body);
    const userId = userIdFrom(req);
    release = reserveOrderFetch(userId);
    const plan = await planFetch(userId, body);
    if (!plan.loggedIn) throw new Error(`None of the ${plan.emails.length} email(s) are logged in. Login them in platform id first.`);
    const opts: OrderFetchStartOptions = {
      invoices: body.invoices,
      trigger: body.trigger,
      by: body.by,
      params: plan.params,
      reserved: true,
      onFinished: body.runUpdateAfterFetch
        ? async (info) => {
            await queueUpdateAfterFetch(userId, { runId: info.runId, emails: info.emails, selection: plan.selection, cancelled: info.cancelled, by: body.by, trigger: body.trigger });
          }
        : undefined,
    };
    const preparing = plan.emails.length > FETCH_INLINE_START_MAX;
    let requestId = "";
    if (preparing) {
      // The id is handed out now; the run adopts this row when its account list is ready.
      requestId = await createQueuedRun(userId, "fetch", {
        trigger: body.trigger,
        by: body.by,
        total: plan.loggedIn,
        message: `Preparing ${plan.loggedIn} account(s) — the fetch starts when their saved units are read`,
        meta: { platform: body.platform, params: plan.params, emails_count: plan.loggedIn, host: hostName(), pid: process.pid },
      });
      void startOrderFetch(userId, plan.emails, plan.sinceDate, { ...opts, runId: requestId }).catch((err) => {
        const reason = err instanceof Error ? err.message : String(err);
        console.warn(`[orders-fetch] fetch ${requestId} could not start: ${reason}`);
        // Force kill during the preparation is a stop, not a failure.
        return err instanceof OrderFetchStoppedError ? stopRunById(requestId, reason) : failRunById(requestId, `Could not start: ${reason}`);
      });
    } else {
      requestId = (await startOrderFetch(userId, plan.emails, plan.sinceDate, opts)).runId;
    }
    const job = getOrderFetchJob(userId);
    const queued = preparing ? plan.loggedIn : job.total;
    res.json({
      message: "Order fetch triggered successfully",
      platform: body.platform,
      selection_method: plan.selection,
      total_accounts_found: plan.emails.length,
      valid_accounts_queued: queued,
      tasks_created: queued,
      windows: preparing ? null : job.windows,
      fetch_orders_after_date: plan.afterDate,
      fetch_orders_after_date_source: plan.afterDateSource,
      invoices: body.invoices,
      request_id: requestId,
      params: plan.params,
      preparing,
      timestamp: Math.floor(Date.now() / 1000),
    });
  } catch (err) {
    sendError(res, err, "Could not start fetch");
  } finally {
    // startOrderFetch marks the workspace as preparing before its first await, so nothing slips in between.
    release?.();
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
/**
 * Workspaces whose update request is being put together right now. Reading the
 * units and their accounts normally takes 1-4 s, but has been seen to take
 * minutes on a slow database; a second press (or a retry after the dashboard
 * gave up waiting) must not queue the same update twice.
 */
const updatesBeingPrepared = new Set<string>();

orderFetchRouter.post("/update/trigger", async (req, res) => {
  let held = "";
  try {
    const body = parseUpdateBody(req.body);
    const userId = userIdFrom(req);
    if (updatesBeingPrepared.has(userId)) {
      res.status(409).json({ error: "An update request is already being prepared — it will appear in a moment", code: "update_preparing" });
      return;
    }
    updatesBeingPrepared.add(userId);
    held = userId;
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
    if (held) updatesBeingPrepared.delete(held);
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

/** Run the scheduled fetch now (same selection and look-back; the row shows system-cron). Body: { by? } */
orderFetchRouter.post("/fetch/schedule/run", async (req, res) => {
  try {
    const out = await runScheduledFetch(userIdFrom(req), String(req.body?.by || "").trim() || "run now");
    res.status(out.started ? 202 : 409).json(out);
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not run the scheduled fetch" });
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

/** Start the time-settled invoice sweep (the "Invoices" button). Enqueues one job; the worker runs it and the Sync panel shows it. */
orderFetchRouter.post("/invoices/sweep", async (req, res) => {
  try {
    const by = String(req.body?.by || "").trim();
    await getInvoiceQueue().add("sweep", { sweep: "manual", by }, { attempts: 1 });
    res.json({ message: "Invoice sweep queued" });
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

orderFetchRouter.get("/invoice/requests", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  res.json({ requests: await listInvoiceRequests(userId, Number(req.query.limit) || 20) });
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

/** Update speed and schedules. PUT body: { windows, by? } — applies to the next job, no restart. */
orderFetchRouter.get("/update/settings", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  const speed = await resolveUpdateWindows(userId);
  res.json({ ...speed, limits: WINDOW_LIMITS, schedule: { enabled: UPDATE_SCHEDULE_ENABLED, activeEveryMs: UPDATE_SWEEP_MS, ofdEveryMs: UPDATE_OFD_SWEEP_MS } });
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
