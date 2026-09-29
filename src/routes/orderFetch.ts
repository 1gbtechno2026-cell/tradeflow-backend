import { Router } from "express";
import { workspaceUserId } from "../services/sessionStore.js";
import {
  getOrderFetchJob,
  parseSinceDate,
  startOrderFetch,
  stopOrderFetch,
} from "../services/orderFetch.js";
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
    await startOrderFetch(userId, emails, sinceDate);
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

orderFetchRouter.get("/fetch/status", (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  res.json(getOrderFetchJob(userId));
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

orderFetchRouter.get("/update/status", (req, res) => {
  const userId = String(req.query.userId || "").trim() || workspaceUserId();
  res.json(getOrderUpdateJob(userId));
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
