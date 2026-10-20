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
import { getCancelRequest, listCancelRequests, previewCancellation, requestCancellation, resolveCancelSpeed, retryCancelRequest, setCancelSpeed } from "../services/orderCancel.js";
import {
  getOrderUpdateJob,
  startOrderUpdate,
  stopOrderUpdate,
} from "../services/orderUpdate.js";

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
    await startOrderFetch(userId, emails, sinceDate, { invoices: parseInvoiceMode(req.body?.invoices) });
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
  if (getOrderFetchJob(userId).running || getOrderUpdateJob(userId).running) {
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
    await startOrderUpdate(userId, {
      emails,
      orderIds,
      orderStatus,
      csvRows,
      selectionMethod: String(req.body?.selection_method || ""),
    });
    const job = getOrderUpdateJob(userId);
    res.json({
      message: "Order update triggered successfully",
      platform,
      selection_method: req.body?.selection_method || "default",
      tasks_created: job.total,
      windows: job.windows,
      timestamp: Math.floor(Date.now() / 1000),
    });
  } catch (err) {
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not start update" });
  }
});

orderFetchRouter.post("/update/stop", async (req, res) => {
  try {
    const userId = userIdFrom(req);
    await stopOrderUpdate(userId);
    res.json({ message: "Order update stopped. Chrome killed." });
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Could not stop" });
  }
});

orderFetchRouter.get("/update/status", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  const job = getOrderUpdateJob(userId);
  const resolved = await resolveOrderFetchMode(userId).catch(() => null);
  res.json({ ...job, mode: job.running ? job.mode : resolved?.mode || job.mode, modeSource: resolved?.source || "env" });
});

orderFetchRouter.post("/sync/stop", async (req, res) => {
  try {
    const userId = userIdFrom(req);
    await stopOrderFetch(userId);
    await stopOrderUpdate(userId);
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

orderFetchRouter.get("/cancel", async (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  res.json({ requests: await listCancelRequests(userId, Number(req.query.limit) || 20) });
});

/** Cancel speed: { windows, tabs, source, changedBy, changedAt, limits }. PUT body: { windows?, tabs?, by? }. Applies to the next job, no restart. */
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
