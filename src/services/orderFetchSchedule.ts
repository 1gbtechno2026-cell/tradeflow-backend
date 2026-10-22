import cron, { type ScheduledTask } from "node-cron";
import { Types } from "mongoose";
import { config } from "../config.js";
import { OrderJobRun } from "../models/OrderJobRun.js";
import { OrderSyncSettings } from "../models/OrderSyncSettings.js";
import { PlatformId } from "../models/PlatformId.js";
import { getOrderFetchJob, startOrderFetch } from "./orderFetch.js";
import { workspaceUserId } from "./sessionStore.js";

/**
 * The scheduled Fetch — the reference tool's "system-cron" row.
 *
 * At FETCH_SCHEDULE (default 05:00 Asia/Kolkata) the API runs one Fetch over
 * every logged-in Flipkart Platform ID of the workspace, looking back
 * `fetchSinceDays` (dashboard setting, else FETCH_SINCE_DAYS, else 60), with
 * trigger "system-cron" on the panel row and the ledger. It uses the same
 * startOrderFetch the button uses, so the dashboard's mode switch, windows
 * and invoice step apply unchanged; a tick is skipped while a fetch is
 * already running.
 *
 *   FETCH_SCHEDULE=0            off (development)
 *   FETCH_SCHEDULE="0 5 * * *"  cron expression, CRON_TIMEZONE or Asia/Kolkata
 *   FETCH_SINCE_DAYS=60         default look-back when no dashboard setting
 *   FETCH_SCHEDULE_INVOICES=0   default for the "download invoices" switch when no dashboard setting
 *
 * Invoices: the dashboard's Settings → Scheduled fetch toggle decides whether
 * the run fetches with invoices "missing" (every delivered unit of each ID
 * without a PDF, INVOICE_MAX_PER_RUN per ID per run) or "off". Stored on the
 * same order_sync_settings row as the look-back.
 */

const RAW = String(process.env.FETCH_SCHEDULE ?? "0").trim();
export const FETCH_SCHEDULE_ENABLED = RAW !== "" && RAW !== "0" && RAW.toLowerCase() !== "off" && RAW.toLowerCase() !== "false";
export const FETCH_SCHEDULE_CRON = FETCH_SCHEDULE_ENABLED && RAW !== "1" && RAW.toLowerCase() !== "on" && RAW.toLowerCase() !== "true" ? RAW : "0 5 * * *";
export const FETCH_TIMEZONE = process.env.CRON_TIMEZONE || "Asia/Kolkata";
const DEFAULT_SINCE_DAYS = Math.max(1, Number(process.env.FETCH_SINCE_DAYS) || 60);
const DEFAULT_INVOICES = ["1", "on", "true", "yes", "missing"].includes(String(process.env.FETCH_SCHEDULE_INVOICES || "0").trim().toLowerCase());

let task: ScheduledTask | null = null;
let lastTick: { at: Date; outcome: string } | null = null;

export async function resolveFetchSinceDays(userId: string): Promise<{ days: number; source: "ui" | "env" }> {
  const row = Types.ObjectId.isValid(userId) ? await OrderSyncSettings.findOne({ userId }).lean().catch(() => null) : null;
  const ui = Math.floor(Number(row?.fetchSinceDays || 0));
  return ui >= 1 ? { days: Math.min(365, ui), source: "ui" } : { days: DEFAULT_SINCE_DAYS, source: "env" };
}

export async function setFetchSinceDays(userId: string, days: unknown, by: string) {
  const n = Math.floor(Number(days));
  if (!Number.isFinite(n) || n < 1 || n > 365) throw new Error("fetchSinceDays must be 1–365");
  await OrderSyncSettings.updateOne({ userId }, { $set: { fetchSinceDays: n, fetchScheduleChangedBy: by || "", fetchScheduleChangedAt: new Date() }, $setOnInsert: { userId } }, { upsert: true });
  return resolveFetchSinceDays(userId);
}

export async function resolveScheduledInvoices(userId: string): Promise<{ enabled: boolean; source: "ui" | "env"; changedBy: string; changedAt: Date | null }> {
  const row = Types.ObjectId.isValid(userId) ? await OrderSyncSettings.findOne({ userId }).lean().catch(() => null) : null;
  if (typeof row?.scheduledInvoices === "boolean") return { enabled: row.scheduledInvoices, source: "ui", changedBy: row.scheduledInvoicesChangedBy || "", changedAt: row.scheduledInvoicesChangedAt || null };
  return { enabled: DEFAULT_INVOICES, source: "env", changedBy: "", changedAt: null };
}

export async function setScheduledInvoices(userId: string, enabled: unknown, by: string) {
  if (typeof enabled !== "boolean") throw new Error("scheduledInvoices must be true or false");
  await OrderSyncSettings.updateOne({ userId }, { $set: { scheduledInvoices: enabled, scheduledInvoicesChangedBy: by || "", scheduledInvoicesChangedAt: new Date() }, $setOnInsert: { userId } }, { upsert: true });
  console.log(`[fetch-schedule] invoices ${enabled ? "ON (all missing)" : "OFF"}${by ? ` by ${by}` : ""}`);
  return resolveScheduledInvoices(userId);
}

/** Logged-in Flipkart Platform IDs with a saved session. Cookie blobs are ~19 KB each,
 *  so the filter runs in the query (does the array have an element?) and only emails come back. */
const WITH_SESSION = { userId: "", platform: "flipkart", status: "logged_in", sessionSavedAt: { $ne: null }, $or: [{ "sessionState.cookies.0": { $exists: true } }, { "sessionCookies.0": { $exists: true } }] };

export async function scheduledFetchEmails(userId: string): Promise<string[]> {
  const rows = await PlatformId.find({ ...WITH_SESSION, userId }).select("email").sort({ createdAt: 1 }).lean();
  return rows.map((r) => r.email);
}

export async function scheduledFetchCount(userId: string): Promise<number> {
  return PlatformId.countDocuments({ ...WITH_SESSION, userId });
}

/**
 * One scheduled run (also what "Run the scheduled fetch now" does). Returns
 * what it started, or why it did not.
 */
export async function runScheduledFetch(userId: string, by = "schedule"): Promise<{ started: boolean; message: string; emails?: number; sinceDays?: number; invoices?: "missing" | "off" }> {
  if (getOrderFetchJob(userId).running) {
    const message = "skipped — a fetch is already running";
    lastTick = { at: new Date(), outcome: message };
    console.log(`[fetch-schedule] ${message}`);
    return { started: false, message };
  }
  const emails = await scheduledFetchEmails(userId);
  if (!emails.length) {
    const message = "skipped — no logged-in Platform ID";
    lastTick = { at: new Date(), outcome: message };
    console.log(`[fetch-schedule] ${message}`);
    return { started: false, message };
  }
  const [{ days }, inv] = await Promise.all([resolveFetchSinceDays(userId), resolveScheduledInvoices(userId)]);
  const invoices = inv.enabled ? "missing" : "off";
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
  since.setHours(0, 0, 0, 0);
  try {
    await startOrderFetch(userId, emails, since, { invoices, trigger: "system-cron", by });
    const message = `started — ${emails.length} logged-in Platform ID(s), orders since ${since.toISOString().slice(0, 10)} (${days} d), invoices ${invoices === "missing" ? "on (all missing)" : "off"}`;
    lastTick = { at: new Date(), outcome: message };
    console.log(`[fetch-schedule] ${message}`);
    return { started: true, message, emails: emails.length, sinceDays: days, invoices };
  } catch (err) {
    const message = `could not start: ${err instanceof Error ? err.message : String(err)}`;
    lastTick = { at: new Date(), outcome: message };
    console.warn(`[fetch-schedule] ${message}`);
    return { started: false, message };
  }
}

export function startFetchSchedule(): void {
  if (task) return;
  if (!FETCH_SCHEDULE_ENABLED) {
    console.log("[fetch-schedule] off (FETCH_SCHEDULE=0) — the Fetch button only");
    return;
  }
  if (!cron.validate(FETCH_SCHEDULE_CRON)) {
    console.warn(`[fetch-schedule] FETCH_SCHEDULE "${FETCH_SCHEDULE_CRON}" is not a valid cron expression — not started`);
    return;
  }
  task = cron.schedule(FETCH_SCHEDULE_CRON, () => void runScheduledFetch(workspaceUserId(), "schedule"), { timezone: FETCH_TIMEZONE });
  console.log(`[fetch-schedule] "${FETCH_SCHEDULE_CRON}" ${FETCH_TIMEZONE} — every logged-in Platform ID, look-back ${DEFAULT_SINCE_DAYS} d unless the dashboard sets fetchSinceDays`);
}

export function stopFetchSchedule(): void {
  task?.stop();
  task = null;
}

export async function fetchScheduleStatus(userId: string) {
  const [since, invoices] = await Promise.all([resolveFetchSinceDays(userId), resolveScheduledInvoices(userId)]);
  const [lastRun, loggedInIds] = await Promise.all([
    OrderJobRun.findOne({ userId, type: "fetch", trigger: "system-cron" }).sort({ createdAt: -1 }).select("status startedAt finishedAt total success failed message").lean(),
    scheduledFetchCount(userId),
  ]);
  return {
    enabled: FETCH_SCHEDULE_ENABLED,
    cron: FETCH_SCHEDULE_CRON,
    timezone: FETCH_TIMEZONE,
    sinceDays: since.days,
    sinceSource: since.source,
    invoices,
    invoiceMaxPerRun: config.invoiceMaxPerRun,
    loggedInIds,
    windows: config.fetchWindowConcurrency || null,
    lastTick,
    lastScheduledRun: lastRun,
    fetchRunning: getOrderFetchJob(userId).running,
  };
}
