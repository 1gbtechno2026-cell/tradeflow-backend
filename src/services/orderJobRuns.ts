import { Types } from "mongoose";
import { CancelRequest } from "../models/CancelRequest.js";
import { OrderJobRun, type IOrderJobRun, type JobRunStatus, type JobRunTrigger, type JobRunType } from "../models/OrderJobRun.js";
import { OrderUpdateRequest } from "../models/OrderUpdateRequest.js";
import { getOrderFetchJob } from "./orderFetch.js";
import { publicUpdateRequest } from "./orderUpdateQueue.js";

/**
 * "Sync Order Units From Platform" — the rows.
 *
 * Every job type ends up as the same PanelRow, so the dashboard draws six
 * identical rows (status chip, trigger chip, done/total, bar, success /
 * failed / cancelled, started, completed, ETA, average, pending, failed
 * items) without knowing where each comes from:
 *
 *   fetch       order_job_runs (written by startOrderFetch / its finally),
 *               merged with the live in-process job while it runs
 *   update      the newest order_update_requests document
 *   cancel      the newest order_cancel_requests document
 *   coupons,    order_job_runs when those jobs exist; "idle" until then
 *   reschedule
 *   invoices    the dashboard adds this row from its Invoice documents
 */

export interface PanelRow {
  type: JobRunType;
  title: string;
  description: string;
  status: "idle" | JobRunStatus;
  trigger: JobRunTrigger | "";
  by: string;
  total: number;
  processed: number;
  success: number;
  failed: number;
  cancelled: number;
  pending: number;
  startedAt: Date | string | null;
  finishedAt: Date | string | null;
  avgMs: number;
  etaSeconds: number | null;
  currentLabel: string;
  extras: Array<{ label: string; value: number }>;
  failedItems: Array<{ key: string; reason: string }>;
  refId: string;
}

const TITLES: Record<JobRunType, { title: string; description: string }> = {
  fetch: { title: "Fetch Order Units", description: "Downloads order history from the selected platform and brings new order units in the system." },
  update: { title: "Refresh Order Units Status & OTP", description: "Refreshes order statuses for existing order units on the selected platform. Flipkart also refreshes delivery OTP." },
  invoices: { title: "Download Order Invoices", description: "Downloads invoice PDFs from Flipkart for the specified order IDs and extracts invoice data." },
  coupons: { title: "Apply Company Coupons", description: "Activates Flipkart company coupon links for logged-in accounts using each account session." },
  reschedule: { title: "Reschedule Order Units", description: "Queues delivery reschedule on Flipkart for selected order units; progress reflects each unit task." },
  cancel: { title: "Cancel Order Units", description: "Queues cancellation on Flipkart for selected order units; progress reflects each unit task." },
};

const FAILED_KEEP = 500;

export function idleRow(type: JobRunType): PanelRow {
  return { type, ...TITLES[type], status: "idle", trigger: "", by: "", total: 0, processed: 0, success: 0, failed: 0, cancelled: 0, pending: 0, startedAt: null, finishedAt: null, avgMs: 0, etaSeconds: null, currentLabel: "", extras: [], failedItems: [], refId: "" };
}

function rowFromRun(r: IOrderJobRun): PanelRow {
  const pending = r.status === "running" || r.status === "queued" ? Math.max(0, r.total - r.processed) : 0;
  return {
    type: r.type,
    ...TITLES[r.type],
    status: r.status,
    trigger: r.trigger,
    by: r.by,
    total: r.total,
    processed: r.processed,
    success: r.success,
    failed: r.failed,
    cancelled: r.cancelled,
    pending,
    startedAt: r.startedAt,
    finishedAt: r.finishedAt,
    avgMs: r.avgMsPerItem,
    etaSeconds: pending && r.avgMsPerItem ? Math.round((pending * r.avgMsPerItem) / 1000) : null,
    currentLabel: r.message,
    extras: [],
    failedItems: r.failedItems || [],
    refId: r.refId || String((r as unknown as { _id: unknown })._id || ""),
  };
}

// ── ledger writes (fetch / coupons / reschedule) ────────────────────────────

const active = new Map<string, Types.ObjectId>(); // `${userId}:${type}` → run _id

export async function startRun(userId: string, type: JobRunType, input: { trigger?: JobRunTrigger; by?: string; total?: number; message?: string; meta?: Record<string, unknown>; failedItems?: Array<{ key: string; reason: string }> }) {
  const run = await OrderJobRun.create({
    userId: new Types.ObjectId(userId),
    type,
    trigger: input.trigger || "user",
    by: input.by || "",
    status: "running",
    total: input.total || 0,
    startedAt: new Date(),
    message: input.message || "",
    meta: input.meta || {},
    failedItems: (input.failedItems || []).slice(0, FAILED_KEEP),
  });
  active.set(`${userId}:${type}`, run._id as Types.ObjectId);
  return String(run._id);
}

export async function progressRun(userId: string, type: JobRunType, patch: Partial<Pick<IOrderJobRun, "total" | "processed" | "success" | "failed" | "cancelled" | "message">>) {
  const id = active.get(`${userId}:${type}`);
  if (!id) return;
  await OrderJobRun.updateOne({ _id: id }, { $set: patch }).catch(() => undefined);
}

export async function finishRun(
  userId: string,
  type: JobRunType,
  result: { status: JobRunStatus; total: number; processed: number; success: number; failed: number; cancelled: number; message?: string; failedItems?: Array<{ key: string; reason: string }>; meta?: Record<string, unknown> }
) {
  const id = active.get(`${userId}:${type}`);
  active.delete(`${userId}:${type}`);
  if (!id) return;
  const run = await OrderJobRun.findById(id).select("startedAt failedItems").lean();
  const finishedAt = new Date();
  const elapsed = run?.startedAt ? finishedAt.getTime() - new Date(run.startedAt).getTime() : 0;
  const base = result.processed || result.success + result.failed;
  await OrderJobRun.updateOne(
    { _id: id },
    {
      $set: {
        status: result.status,
        total: result.total,
        processed: result.processed,
        success: result.success,
        failed: result.failed,
        cancelled: result.cancelled,
        finishedAt,
        avgMsPerItem: base ? Math.round(elapsed / base) : 0,
        message: result.message || "",
        ...(result.failedItems ? { failedItems: [...(run?.failedItems || []), ...result.failedItems].slice(0, FAILED_KEEP) } : {}),
        ...(result.meta ? { meta: result.meta } : {}),
      },
    }
  ).catch(() => undefined);
}

// ── rows ────────────────────────────────────────────────────────────────────

async function latestRun(userId: string, type: JobRunType) {
  return OrderJobRun.findOne({ userId, type }).sort({ createdAt: -1 }).lean();
}

/** Fetch: the ledger row, overlaid with the live in-process job while it runs. */
async function fetchRow(userId: string): Promise<PanelRow> {
  const run = await latestRun(userId, "fetch");
  const row = run ? rowFromRun(run as IOrderJobRun) : idleRow("fetch");
  const live = getOrderFetchJob(userId);
  if (live.running) {
    const rep = live.report;
    const discovering = live.phase !== "enrich" && !rep.enrichQueued && !rep.idsOpened;
    const processed = discovering ? live.done : rep.idsOpened;
    const total = discovering ? live.total : rep.enrichQueued || rep.idsOpened;
    const elapsed = rep.startedAt ? Date.now() - new Date(rep.startedAt).getTime() : 0;
    const avgMs = processed ? elapsed / processed : 0;
    const pending = Math.max(0, total - processed);
    return {
      ...row,
      status: "running",
      trigger: row.trigger || "user",
      total,
      processed,
      success: rep.idsScraped,
      failed: rep.idsFailed,
      cancelled: 0,
      pending,
      startedAt: rep.startedAt,
      finishedAt: null,
      avgMs,
      etaSeconds: pending && avgMs ? Math.round((pending * avgMs) / 1000) : null,
      currentLabel: `${discovering ? "Discovering unit IDs" : "Enriching new units"}${live.currentEmail ? ` · ${live.currentEmail}` : ""}${live.sinceLabel ? ` · from ${live.sinceLabel}` : ""}`,
      extras: [
        { label: "Emails", value: live.total },
        { label: "Not logged in", value: live.skipped },
        { label: "Skipped old", value: rep.idsSkippedOld },
      ],
    };
  }
  return row;
}

async function updateRow(userId: string): Promise<PanelRow> {
  const doc = await OrderUpdateRequest.findOne({ userId }).sort({ createdAt: -1 }).lean();
  if (!doc) return idleRow("update");
  const p = publicUpdateRequest(doc);
  const running = p.status === "queued" || p.status === "running";
  const processed = p.counts.done + p.counts.failed;
  const start = p.startedAt || p.createdAt;
  const elapsed = start ? (p.finishedAt ? new Date(p.finishedAt).getTime() : Date.now()) - new Date(start).getTime() : 0;
  const avgMs = processed ? elapsed / processed : 0;
  const cancelledUnits = p.status === "cancelled" ? Math.max(0, p.counts.units - processed) : 0;
  const pending = running ? Math.max(0, p.counts.units - processed) : 0;
  const active = p.accounts.find((a) => a.status === "running");
  return {
    ...idleRow("update"),
    status: p.status === "cancelled" ? "stopped" : p.status === "done" ? "completed" : running ? "running" : "idle",
    trigger: p.source === "schedule" || p.source === "ofd" ? "system-cron" : "user",
    by: p.createdBy || (p.source === "cancel" ? "cancel popup" : p.source),
    total: p.counts.units,
    processed,
    success: p.counts.done,
    failed: p.counts.failed,
    cancelled: cancelledUnits,
    pending,
    startedAt: start,
    finishedAt: p.finishedAt,
    avgMs,
    etaSeconds: pending && avgMs ? Math.round((pending * avgMs) / 1000) : null,
    currentLabel: active ? `${active.email} ${active.done + active.failed}/${active.units}` : running ? `${p.counts.accountsQueued} account(s) waiting` : "",
    extras: [
      { label: "Accounts", value: p.counts.accounts },
      { label: "Status changed", value: p.counts.statusChanged },
      { label: "New OTP", value: p.counts.otpNew },
    ],
    failedItems: p.accounts.filter((a) => a.status === "failed" || a.failed > 0).slice(0, FAILED_KEEP).map((a) => ({ key: a.email, reason: a.message || `${a.failed} unit(s) failed` })),
    refId: p.id,
  };
}

async function cancelRow(userId: string): Promise<PanelRow> {
  const r = await CancelRequest.findOne({ userId }).sort({ createdAt: -1 }).lean();
  if (!r) return idleRow("cancel");
  const units = r.units || [];
  const count = (s: string) => units.filter((u) => u.status === s).length;
  const orders = r.orders || [];
  const ordersOpen = orders.filter((o) => o.status === "queued" || o.status === "running").length;
  const total = units.length + ordersOpen;
  const success = count("cancelled") + count("dry_run");
  const failed = count("failed") + orders.filter((o) => o.status === "failed").length;
  const notCancellable = count("not_cancellable");
  const processed = success + failed + notCancellable;
  const running = r.status !== "done";
  const finished = units.filter((u) => u.finishedAt).map((u) => new Date(u.finishedAt as Date).getTime());
  const start = r.createdAt;
  const end = r.finishedAt ? new Date(r.finishedAt).getTime() : finished.length ? Math.max(...finished) : Date.now();
  const elapsed = start ? end - new Date(start).getTime() : 0;
  const avgMs = processed ? elapsed / processed : 0;
  const pending = running ? Math.max(0, total - processed) : 0;
  const runningUnit = units.find((u) => u.status === "running");
  return {
    ...idleRow("cancel"),
    status: running ? "running" : "completed",
    trigger: "user",
    by: r.createdBy || "",
    total,
    processed,
    success,
    failed,
    cancelled: 0,
    pending,
    startedAt: start,
    finishedAt: r.finishedAt,
    avgMs,
    etaSeconds: pending && avgMs ? Math.round((pending * avgMs) / 1000) : null,
    currentLabel: runningUnit ? `${runningUnit.email} ${runningUnit.orderId}/${runningUnit.unitId}` : ordersOpen ? `${ordersOpen} order page(s) to read` : "",
    extras: [
      { label: "Not cancellable", value: notCancellable },
      { label: "Refused", value: (r.rejected || []).length },
      ...(r.urgent ? [{ label: "Urgent order pages", value: orders.length }] : []),
    ],
    failedItems: [
      ...units.filter((u) => u.status === "failed").map((u) => ({ key: `${u.email} ${u.orderId}/${u.unitId}`, reason: u.message || "failed" })),
      ...orders.filter((o) => o.status === "failed").map((o) => ({ key: `${o.email} ${o.orderId}`, reason: o.message || "order page failed" })),
    ].slice(0, FAILED_KEEP),
    refId: String(r._id),
  };
}

async function ledgerRow(userId: string, type: JobRunType): Promise<PanelRow> {
  const run = await latestRun(userId, type);
  return run ? rowFromRun(run as IOrderJobRun) : idleRow(type);
}

export async function syncPanel(userId: string): Promise<{ rows: PanelRow[]; at: Date }> {
  const [fetch, update, cancel, coupons, reschedule] = await Promise.all([fetchRow(userId), updateRow(userId), cancelRow(userId), ledgerRow(userId, "coupons"), ledgerRow(userId, "reschedule")]);
  // The invoices row is the dashboard's (its Invoice documents); a placeholder keeps the order stable.
  return { rows: [fetch, update, idleRow("invoices"), coupons, reschedule, cancel], at: new Date() };
}

export async function listRuns(userId: string, type: JobRunType | undefined, limit = 20) {
  return OrderJobRun.find({ userId, ...(type ? { type } : {}) }).sort({ createdAt: -1 }).limit(Math.min(100, Math.max(1, limit))).lean();
}
