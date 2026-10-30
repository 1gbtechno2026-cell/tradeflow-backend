import { parseInvoiceMode } from "../services/invoiceCapture.js";
import { Router } from "express";
import { config } from "../config.js";
import { workspaceUserId } from "../services/sessionStore.js";
import { resolveOrderFetchMode, setOrderFetchMode } from "../services/orderSyncSettings.js";
import {
  getOrderFetchJob,
  parseSinceDate,
  startOrderFetch,
  stopOrderFetch,
} from "../services/orderFetch.js";
import { resolveCheckoutSettings, setCheckoutSettings } from "../services/checkoutSettings.js";
import { getCancelRequest, listCancelRequests, previewCancellation, requestCancellation, resolveCancelSpeed, retryCancelRequest, setCancelSpeed } from "../services/orderCancel.js";
import { getRescheduleRequest, listRescheduleRequests, previewReschedule, requestReschedule, retryRescheduleRequest } from "../services/orderReschedule.js";
import { proxyModeStatus, setProxyMode } from "../services/proxyMode.js";
import { listRuns, syncPanel } from "../services/orderJobRuns.js";
import { fetchScheduleStatus, runScheduledFetch, setFetchSinceDays, setScheduledInvoices } from "../services/orderFetchSchedule.js";
import {
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

function parseEmails(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim().toLowerCase()).filter(Boolean);
  }
  return String(value || "")
    .split(/[\s,]+/)
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);
}

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

orderFetchRouter.post("/fetch/trigger", async (req, res) => {
  try {
    const platform = String(req.body?.platform || "FLIPKART").toUpperCase();
    if (platform !== "FLIPKART") {
      res.status(400).json({ error: "Only FLIPKART fetch is supported" });
      return;
    }
    const emails = parseEmails(req.body?.emails);
    if (!emails.length) {
      res.status(400).json({ error: "Paste Flipkart emails in Fetch (comma or space separated)" });
      return;
    }
    const after = String(req.body?.fetch_orders_after_date || "").trim();
    if (!after) {
      res.status(400).json({ error: "Pick Fetch Orders After date" });
      return;
    }
    const sinceDate = parseSinceDate(after);
    const userId = userIdFrom(req);
    await startOrderFetch(userId, emails, sinceDate, { invoices: parseInvoiceMode(req.body?.invoices), trigger: req.body?.trigger === "system-cron" ? "system-cron" : "user", by: String(req.body?.by || "").trim() });
    const job = getOrderFetchJob(userId);
    res.json({
      message: "Order fetch triggered successfully",
      platform,
      selection_method: "email",
      total_accounts_found: emails.length,
      valid_accounts_queued: job.total,
      tasks_created: job.total,
      windows: job.windows,
      fetch_orders_after_date: after,
      invoices: parseInvoiceMode(req.body?.invoices),
      timestamp: Math.floor(Date.now() / 1000),
    });
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not start fetch" });
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
  res.json({ ...job, mode: job.running ? job.mode : resolved?.mode || job.mode, modeSource: resolved?.source || "env" });
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

function parseCsvRows(value: unknown): Array<{ email: string; order_id: string }> {
  if (!Array.isArray(value)) return [];
  return value
    .map((row) => {
      const rec = row && typeof row === "object" ? (row as Record<string, unknown>) : {};
      return {
        email: String(rec.email || "").trim().toLowerCase(),
        order_id: String(rec.order_id || rec.orderId || "").trim(),
      };
    })
    .filter((row) => row.email && row.order_id);
}

orderFetchRouter.post("/update/trigger", async (req, res) => {
  try {
    const platform = String(req.body?.platform || "FLIPKART").toUpperCase();
    if (platform !== "FLIPKART") {
      res.status(400).json({ error: "Only FLIPKART update is supported" });
      return;
    }
    const userId = userIdFrom(req);
    const emails = parseEmails(req.body?.emails);
    const orderIds = parseIds(req.body?.order_ids || req.body?.orderIds);
    const orderStatus = Array.isArray(req.body?.order_status)
      ? req.body.order_status.map((item: unknown) => String(item || "").trim()).filter(Boolean)
      : [];
    const csvRows = parseCsvRows(req.body?.csv_rows || req.body?.csvRows);
    const source = String(req.body?.source || "") === "cancel" ? "cancel" : "manual";
    // Queued on the worker (services/orderUpdateQueue.ts): one job per account.
    const request = await createUpdateRequest(
      userId,
      { emails, orderIds, orderStatus, csvRows, selectionMethod: String(req.body?.selection_method || "") },
      { source, by: String(req.body?.by || "").trim() }
    );
    res.json({
      message: "Order update queued on the worker",
      platform,
      selection_method: req.body?.selection_method || "default",
      tasks_created: request.counts.units,
      accounts: request.counts.accounts,
      windows: request.windows,
      request_id: request.id,
      timestamp: Math.floor(Date.now() / 1000),
    });
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not start update" });
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
