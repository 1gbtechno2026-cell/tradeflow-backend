import fs from "node:fs";
import path from "node:path";
import { DelayedError, type Job } from "bullmq";
import { Types } from "mongoose";
import type { Browser, BrowserContext, Page } from "playwright";
import { itemIdOf } from "../automation/FlipkartCancel.js";
import { fullOrderId } from "../automation/PaymentApiWatcher.js";
import { config } from "../config.js";
import { Order } from "../models/Order.js";
import { RescheduleRequest, type IRescheduleRequest, type IRescheduleUnit, type RescheduleUnitStatus } from "../models/RescheduleRequest.js";
import { getRescheduleQueue, type RescheduleJobData } from "../queue.js";
import { lockAccount } from "./accountLock.js";
import {
  blockFlipkartLogout,
  confirmLoginLanding,
  desktopContext,
  flipkartLoginUrl,
  launchStealthBrowser,
  restoreFlipkartSession,
  sleep,
  type LaunchOptions,
} from "./browser.js";
import { claimEgress } from "./proxyPool.js";
import { resolveLoggedInSession } from "./sessionStore.js";
import { markSessionEligibleForRelogin } from "./sessionEligibility.js";
import { clearedByStop } from "./requestCommon.js";
import { openRequestTasksQuietly, syncRequestTasksQuietly, syncRequestTasksSoon } from "./syncTaskLog.js";

/**
 * Reschedule Order Units — Flipkart's "Change Date", driven per unit in the
 * Platform ID's own Chrome.
 *
 * What Flipkart does (read on real orders, 2026-10-23/24): the order page
 * lists the unit's actions; RFR ("Change Date") shows a button, and pressing
 * it calls
 *   GET https://2.rome.api.flipkart.com/api/1/self-serve/rfr/feasible/<orderId>?unitId=<unitId>
 * which answers { feasibilityStatus, feasibilityFailureMessage[], …, token }.
 * Not feasible → the page shows "Delivery date can't be changed" and that is
 * the end (status not_available, with Flipkart's own sentence). Feasible → a
 * date dialog opens. Every unit seen so far was refused, so the dialog has
 * not been observed yet: the worker therefore CAPTURES it (its text, its
 * options, a screenshot under debug/reschedule-runs/<request>/) and, unless
 * RESCHEDULE_CONFIRM=1, stops there with status needs_review. With the switch
 * on it picks the option that names the requested date and presses the
 * confirm button, then reads Flipkart's answer.
 *
 * One job per (request, account); accounts in parallel up to the worker's
 * reschedule concurrency; the per-account Redis lock keeps a cancel / update
 * / checkout off the same login meanwhile.
 */

const TERMINAL = /deliver|cancel|return|refund|fail/i;
const IN_FLIGHT = new Set(["queued", "running"]);
const LOCK_RETRY_MS = 30_000;
const STALE_RUNNING_MS = 10 * 60 * 1000;
/** Reschedule reaper: sweep period, and how long an open request must be quiet to count as stranded. */
const REAP_EVERY_MS = 60 * 1000;
const REAP_STALE_MS = 5 * 60 * 1000;
const PAGE_TIMEOUT_MS = 60_000;
const UNIT_TIMEOUT_MS = 3 * 60 * 1000;
export const SECONDS_PER_UNIT = 12;

export function rescheduleConfirmEnabled(): boolean {
  return ["1", "true", "on", "yes"].includes(String(process.env.RESCHEDULE_CONFIRM || "").trim().toLowerCase());
}

export function rescheduleConcurrency() {
  const n = Number(process.env.RESCHEDULE_CONCURRENCY);
  return Number.isFinite(n) && n >= 0 ? Math.floor(n) : 1;
}

export interface RescheduleRequestInput {
  type: "unit" | "order";
  ids: string[];
  date: string;
  by?: string;
  retryOf?: string;
  failIfEmpty?: boolean;
  /** Internal: "system-retry" when Batch requests → Retry makes the request ("" = the user's own). */
  source?: string;
}

function normaliseIds(type: "unit" | "order", ids: string[]): string[] {
  const out = ids.map((s) => String(s || "").trim()).filter(Boolean);
  return [...new Set(type === "order" ? out.map((id) => fullOrderId(id.toUpperCase())) : out)];
}

function validDate(raw: string): string {
  const s = String(raw || "").trim().slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) throw new Error("Reschedule date must be YYYY-MM-DD");
  const d = new Date(`${s}T00:00:00`);
  if (Number.isNaN(d.getTime())) throw new Error("Reschedule date is not a real date");
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  if (d.getTime() < today.getTime()) throw new Error("Reschedule date must be today or later");
  return s;
}

type KnownRow = { platform?: string; platform_email: string; order_id: string; unit_id: string; item_id?: string; product_name?: string; status_key?: string; reschedule_status?: string };

function classify(type: "unit" | "order", ids: string[], rows: KnownRow[]) {
  const rejected: Array<{ id: string; reason: string }> = [];
  const units: IRescheduleUnit[] = [];
  for (const id of ids) {
    const matches = rows.filter((r) => (type === "unit" ? r.unit_id === id : r.order_id === id));
    if (!matches.length) {
      rejected.push({ id, reason: "Not found in Order Units — fetch the account's orders first" });
      continue;
    }
    for (const r of matches) {
      const label = type === "order" ? `${id} / ${r.unit_id}` : id;
      if (String(r.platform || "FLIPKART").toUpperCase() !== "FLIPKART") rejected.push({ id: label, reason: "Only Flipkart orders can be rescheduled" });
      else if (TERMINAL.test(String(r.status_key || ""))) rejected.push({ id: label, reason: `Already ${r.status_key}` });
      else if (IN_FLIGHT.has(String(r.reschedule_status || ""))) rejected.push({ id: label, reason: `Reschedule already ${r.reschedule_status}` });
      else
        units.push({
          orderId: r.order_id,
          unitId: r.unit_id,
          itemId: String(r.item_id || "") || itemIdOf(r.unit_id),
          email: r.platform_email,
          productName: String(r.product_name || ""),
          status: "queued",
          message: "",
          offeredDates: [],
          chosen: "",
          token: "",
          startedAt: null,
          finishedAt: null,
        });
    }
  }
  return { units, rejected };
}

async function knownRows(userId: string, type: "unit" | "order", ids: string[]): Promise<KnownRow[]> {
  return Order.find({ userId, ...(type === "unit" ? { unit_id: { $in: ids } } : { order_id: { $in: ids } }) })
    .select("platform platform_email order_id unit_id item_id product_name status_key reschedule_status")
    .lean<KnownRow[]>();
}

export async function previewReschedule(userId: string, input: RescheduleRequestInput) {
  const type = input.type === "unit" ? "unit" : "order";
  const ids = normaliseIds(type, input.ids);
  if (!ids.length) throw new Error(type === "unit" ? "Paste at least one Unit ID" : "Paste at least one Order ID");
  if (ids.length > 2000) throw new Error("At most 2000 ids per request");
  const date = validDate(input.date);
  const { units, rejected } = classify(type, ids, await knownRows(userId, type, ids));
  const reasons: Record<string, number> = {};
  for (const r of rejected) reasons[r.reason] = (reasons[r.reason] || 0) + 1;
  const accounts = new Set(units.map((u) => u.email)).size;
  const windows = Math.max(1, rescheduleConcurrency());
  return {
    type,
    date,
    ids: ids.length,
    units: units.length,
    orders: new Set(units.map((u) => u.orderId)).size,
    accounts,
    rejected: rejected.length,
    reasons,
    confirmEnabled: rescheduleConfirmEnabled(),
    windows,
    secondsPerUnit: SECONDS_PER_UNIT,
    estimateSeconds: Math.ceil((units.length * SECONDS_PER_UNIT) / Math.max(1, Math.min(accounts || 1, windows))),
  };
}

export async function requestReschedule(userId: string, input: RescheduleRequestInput) {
  const type = input.type === "unit" ? "unit" : "order";
  const ids = normaliseIds(type, input.ids);
  if (!ids.length) throw new Error(type === "unit" ? "Paste at least one Unit ID" : "Paste at least one Order ID");
  if (ids.length > 2000) throw new Error("At most 2000 ids per request");
  const date = validDate(input.date);
  const { units, rejected } = classify(type, ids, await knownRows(userId, type, ids));
  if (!units.length && input.failIfEmpty) throw new Error(`Nothing to reschedule: ${rejected.map((r) => r.reason)[0] || "no units"}`);
  const open = units.length > 0;
  const request = await RescheduleRequest.create({
    userId,
    type,
    input: ids,
    date,
    createdBy: String(input.by || "").trim(),
    retryOf: String(input.retryOf || ""),
    source: String(input.source || ""),
    systemId: config.systemId,
    confirm: rescheduleConfirmEnabled(),
    status: open ? "queued" : "done",
    units,
    rejected,
    finishedAt: open ? null : new Date(),
  });
  const requestId = String(request._id);
  // The task log: a row per unit, written before a job can take one.
  await openRequestTasksQuietly("reschedule", request.toObject());
  if (open) {
    await Order.updateMany(
      { userId, unit_id: { $in: units.map((u) => u.unitId) } },
      { $set: { reschedule_status: "queued", reschedule_request_id: requestId, reschedule_date: date, reschedule_message: "" } }
    );
    const emails = [...new Set(units.map((u) => u.email))];
    await getRescheduleQueue().addBulk(emails.map((email) => ({ name: "reschedule", data: { requestId, email }, opts: { jobId: `${requestId}-${email}` } })));
    console.log(`[reschedule ${requestId}] ${units.length} unit(s) on ${emails.length} account(s) → ${date}${request.confirm ? "" : " (capture only — RESCHEDULE_CONFIRM is off)"}`);
  }
  return publicRequest(request.toObject());
}

export async function retryRescheduleRequest(userId: string, requestId: string, by: string) {
  if (!Types.ObjectId.isValid(requestId)) return null;
  const row = await RescheduleRequest.findOne({ _id: requestId, userId }).lean();
  if (!row) return null;
  const failed = row.units.filter((u) => u.status === "failed").map((u) => u.unitId);
  if (!failed.length) throw new Error("Nothing to retry — this request has no failed units");
  return requestReschedule(userId, { type: "unit", ids: failed, date: row.date, by, retryOf: requestId, failIfEmpty: true });
}

export async function getRescheduleRequest(userId: string, requestId: string) {
  if (!Types.ObjectId.isValid(requestId)) return null;
  const row = await RescheduleRequest.findOne({ _id: requestId, userId }).lean();
  return row ? publicRequest(row) : null;
}

/**
 * Admin "Force clear" for a stranded reschedule (the twin of the cancellation /
 * update Force-clear): drop any per-account jobs still on the queue, mark the
 * queued/running units terminal and close the request. "reset" (default) also
 * frees the Order rows this request parked (reschedule_status queued/running)
 * so the units can be re-submitted cleanly; "abort" just closes it.
 */
export async function abortRescheduleRequest(userId: string, requestId: string, by: string, mode: "reset" | "abort" = "reset") {
  if (!Types.ObjectId.isValid(requestId)) return null;
  const row = await RescheduleRequest.findOne({ _id: requestId, userId }).lean();
  if (!row) return null;

  // 1) Drop any BullMQ jobs still carrying this request (one per account email).
  const emails = [...new Set((row.units || []).map((u) => u.email))];
  const queue = getRescheduleQueue();
  await Promise.all(emails.map((email) => queue.remove(`${requestId}-${email}`).catch(() => undefined)));

  // 2) Free the Order rows this request parked in-flight, so they re-schedule cleanly.
  let freedRows = 0;
  if (mode === "reset") {
    const res = await Order.updateMany(
      { userId, reschedule_request_id: requestId, reschedule_status: { $in: ["queued", "running"] } },
      { $set: { reschedule_status: "", reschedule_request_id: "", reschedule_date: "", reschedule_message: "" } }
    );
    freedRows = res.modifiedCount || 0;
  }

  // 3) Mark the stuck units terminal and close the request (settled units stay as they are).
  const note = `Cleared by ${by || "admin"}${mode === "reset" ? " — reset for re-run" : ""}`;
  await RescheduleRequest.updateOne(
    { _id: requestId },
    { $set: { "units.$[u].status": "failed", "units.$[u].message": note, "units.$[u].finishedAt": new Date(), status: "done", finishedAt: new Date() } },
    { arrayFilters: [{ "u.status": { $in: ["queued", "running"] } }] }
  );

  console.log(`[reschedule ${requestId}] force-cleared by ${by || "admin"} (mode=${mode}, freed ${freedRows} order row(s))`);
  await syncRequestTasksQuietly("reschedule", requestId);
  const updated = await RescheduleRequest.findById(requestId).lean();
  return updated ? { ...publicRequest(updated), freedRows } : null;
}

export async function listRescheduleRequests(userId: string, limit = 20) {
  const rows = await RescheduleRequest.find({ userId }).sort({ createdAt: -1 }).limit(Math.min(100, Math.max(1, limit))).lean();
  return rows.map(publicRequest);
}

export function publicRequest(r: { _id: unknown; type: string; date: string; createdBy?: string; retryOf?: string; confirm?: boolean; status: string; units: IRescheduleUnit[]; rejected: Array<{ id: string; reason: string }>; createdAt?: Date; finishedAt?: Date | null }) {
  const count = (s: RescheduleUnitStatus) => r.units.filter((u) => u.status === s).length;
  return {
    id: String(r._id),
    type: r.type,
    date: r.date,
    createdBy: r.createdBy || "",
    retryOf: r.retryOf || "",
    confirm: Boolean(r.confirm),
    status: r.status,
    createdAt: r.createdAt ?? null,
    finishedAt: r.finishedAt ?? null,
    counts: {
      total: r.units.length,
      queued: count("queued"),
      running: count("running"),
      rescheduled: count("rescheduled"),
      notAvailable: count("not_available"),
      needsReview: count("needs_review"),
      failed: count("failed"),
      rejected: r.rejected.length,
    },
    units: r.units,
    rejected: r.rejected,
  };
}

// ── worker side ──────────────────────────────────────────────────────────────

async function setUnit(requestId: string, unitId: string, patch: Partial<IRescheduleUnit>) {
  const set: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(patch)) set[`units.$[u].${k}`] = v;
  await RescheduleRequest.updateOne({ _id: requestId }, { $set: set }, { arrayFilters: [{ "u.unitId": unitId }] });
}

async function applyToOrder(userId: Types.ObjectId, unitId: string, status: RescheduleUnitStatus, message: string, chosen: string) {
  const set: Record<string, unknown> = { reschedule_status: status, reschedule_message: message };
  if (status === "rescheduled" && chosen) set.delivery_message = `Rescheduled to ${chosen}`;
  await Order.updateOne({ userId, unit_id: unitId }, { $set: set });
}

interface Feasibility {
  ok: boolean;
  status: number;
  feasible: boolean;
  messages: string[];
  title: string;
  token: string;
  raw: string;
}

/** The same GET the Change Date button makes, from the page's own origin and cookies. */
async function feasibility(page: Page, orderId: string, unitId: string): Promise<Feasibility> {
  const out = await page.evaluate(
    async (o) => {
      try {
        const r = await fetch(`https://2.rome.api.flipkart.com/api/1/self-serve/rfr/feasible/${o.orderId}?unitId=${o.unitId}`, {
          credentials: "include",
          headers: { "X-User-Agent": navigator.userAgent + " FKUA/website/42/website/Desktop" },
        });
        return { status: r.status, body: await r.text() };
      } catch (e) {
        return { status: 0, body: String(e) };
      }
    },
    { orderId, unitId }
  );
  let res: Record<string, unknown> | null = null;
  try {
    res = (JSON.parse(out.body) as { RESPONSE?: Record<string, unknown> }).RESPONSE || null;
  } catch {
    res = null;
  }
  return {
    ok: out.status === 200 && Boolean(res),
    status: out.status,
    feasible: Boolean(res?.feasibilityStatus),
    messages: Array.isArray(res?.feasibilityFailureMessage) ? (res!.feasibilityFailureMessage as unknown[]).map(String) : [],
    title: String(res?.feasibilityFailureMessageTitle || ""),
    token: String(res?.token || ""),
    raw: out.body.slice(0, 4000),
  };
}

/** "2026-10-27" → the strings a Flipkart date option might show: "27 Oct", "Oct 27", "Mon", "27". */
function dateNeedles(ymd: string): RegExp[] {
  const d = new Date(`${ymd}T00:00:00`);
  const day = d.getDate();
  const mon = d.toLocaleString("en-IN", { month: "short" });
  const monLong = d.toLocaleString("en-IN", { month: "long" });
  const dow = d.toLocaleString("en-IN", { weekday: "short" });
  return [new RegExp(`\\b${day}(st|nd|rd|th)?\\s+${mon}`, "i"), new RegExp(`\\b${mon}\\w*\\s+${day}\\b`, "i"), new RegExp(`\\b${day}(st|nd|rd|th)?\\s+${monLong}`, "i"), new RegExp(`\\b${dow}\\w*,?\\s+${day}\\b`, "i")];
}

/** What every unit of an account ends with once its session is gone. */
const SESSION_SENTENCE = "Session expired — re-login this ID";
/** Appended to it when the dead-session hand-off moved the ID to Not logged in. */
const MOVED_TO_NOT_LOGGED_IN = " — moved to Not logged in";

/**
 * What a reschedule job reaches beyond Mongo. The worker runs on
 * `rescheduleJobDefaults`; the checks (scripts/deadSessionCheckoutCheck.ts) swap
 * in a sealed Chromium whose every request is answered locally, no proxy and
 * no Redis lock.
 */
export interface RescheduleJobDeps {
  /** The per-account lock (accountLock.ts). */
  lock: typeof lockAccount;
  /** This account's proxy from the pool. */
  egress: typeof claimEgress;
  /** The account's Chrome, and its desktop context. */
  launch: (options: LaunchOptions) => Promise<Browser>;
  context: (browser: Browser) => Promise<BrowserContext>;
  /** Where a unit's page captures go. */
  artifactDir: (requestId: string) => string;
}

export const rescheduleJobDefaults: RescheduleJobDeps = {
  lock: lockAccount,
  egress: claimEgress,
  launch: launchStealthBrowser,
  context: desktopContext,
  artifactDir: (requestId) => path.resolve("debug/reschedule-runs", requestId),
};

export async function runRescheduleJob(job: Job<RescheduleJobData>, token: string | undefined, overrides: Partial<RescheduleJobDeps> = {}): Promise<void> {
  const deps: RescheduleJobDeps = { ...rescheduleJobDefaults, ...overrides };
  const { requestId, email } = job.data;
  const unlock = await deps.lock(email, `reschedule:${requestId}:${config.workerId}`);
  if (!unlock) {
    await job.moveToDelayed(Date.now() + LOCK_RETRY_MS, token);
    throw new DelayedError();
  }
  const request = await RescheduleRequest.findById(requestId).lean<IRescheduleRequest & { _id: Types.ObjectId }>();
  try {
    if (!request) return;
    const me = email.toLowerCase();
    const staleBefore = Date.now() - STALE_RUNNING_MS;
    const units = request.units.filter((u) => u.email === me && (u.status === "queued" || (u.status === "running" && (!u.startedAt || new Date(u.startedAt).getTime() < staleBefore))));
    if (!units.length) return;
    await RescheduleRequest.updateOne({ _id: requestId, status: "queued" }, { $set: { status: "running" } });
    const log = (msg: string) => console.log(`[reschedule ${requestId}] ${email} ${msg}`);
    // Units of this account Flipkart answered for in this job — any of them
    // proves the session worked: no dead-session hand-off then.
    let wentThrough = 0;
    const finish = async (u: IRescheduleUnit, status: RescheduleUnitStatus, message: string, extra: Partial<IRescheduleUnit> = {}) => {
      if (status === "rescheduled" || status === "not_available" || status === "needs_review") wentThrough++;
      await setUnit(requestId, u.unitId, { status, message, finishedAt: new Date(), ...extra });
      await applyToOrder(request.userId, u.unitId, status, message, extra.chosen || "");
      log(`${u.orderId}/${u.unitId} → ${status}${message ? `: ${message}` : ""}`);
    };

    const session = await resolveLoggedInSession(email);
    if (!session.ok) {
      for (const u of units) await finish(u, "failed", `Session: ${session.reason} — re-login this ID`);
      return;
    }
    const lease = await deps.egress(`reschedule:${requestId}:${email}`).catch(() => ({ egress: null, release: async () => undefined }));
    const egress = lease.egress;
    const browser = await deps.launch({ headless: config.headless, proxy: egress?.proxy });
    const artifactDir = deps.artifactDir(requestId);
    fs.mkdirSync(artifactDir, { recursive: true });
    try {
      const ctx = await deps.context(browser);
      const page = await ctx.newPage();
      await blockFlipkartLogout(page);
      await restoreFlipkartSession(page, session.cookies);
      let sessionDead = false;
      const needles = dateNeedles(request.date);

      // The dead-session hand-off (sessionEligibility.ts), at most once per
      // job, at the first login page — and only when Flipkart answered for no
      // unit of this account in this job (`wentThrough`). The landing is first
      // confirmed on this page: the saved cookies attached again, My Orders
      // opened once more (browser.ts confirmLoginLanding). The hand-off's own
      // rules (24 h, same session, the cap) then decide whether the ID moves to
      // Not logged in. Resolves to what this job's session sentences get appended.
      let handOff: Promise<string> | null = null;
      const sessionGone = (): Promise<string> =>
        (handOff ??= (async () => {
          if (wentThrough) {
            log(`login page after Flipkart answered for ${wentThrough} unit(s) in this job — the session worked, not handed off`);
            return "";
          }
          let confirmed = false;
          try {
            confirmed = await confirmLoginLanding(page, session.cookies);
            if (!confirmed) log("login page, but /account/orders opened with the cookies attached again — not handed off");
          } catch (err) {
            log(`login page; the re-check on /account/orders did not finish (${err instanceof Error ? err.message.split("\n")[0].slice(0, 120) : String(err)}) — not handed off`);
          }
          if (!confirmed) return "";
          const out = await markSessionEligibleForRelogin({
            userId: request.userId,
            platformId: session.platformId,
            email: session.email,
            reason: "Reschedule: Flipkart showed the login page twice — re-login",
            flow: "reschedule",
            confirmed: true,
            sessionSavedAt: session.sessionSavedAt,
            via: "worker",
            requestId,
          });
          log(`Flipkart showed the login page twice — ${out.demoted ? "moved to Not logged in for re-login" : `not moved${out.note}`}`);
          return out.demoted ? MOVED_TO_NOT_LOGGED_IN : "";
        })());

      for (const u of units) {
        // Batch requests → Cancel (services/syncActions.ts) marks the request; a unit this job has not started is
        // left as it is on Flipkart, closed "Cleared by …" and its Order row freed.
        const mark = await RescheduleRequest.findById(requestId).select("stoppedAt stoppedBy").lean();
        if (mark?.stoppedAt) {
          await setUnit(requestId, u.unitId, { status: "failed", message: clearedByStop(mark.stoppedBy || ""), finishedAt: new Date() });
          await Order.updateOne(
            { userId: request.userId, unit_id: u.unitId, reschedule_request_id: requestId, reschedule_status: { $in: ["queued", "running"] } },
            { $set: { reschedule_status: "", reschedule_request_id: "", reschedule_date: "", reschedule_message: "" } }
          );
          log(`${u.orderId}/${u.unitId} left as it is — the request was cancelled`);
          continue;
        }
        if (sessionDead) {
          await finish(u, "failed", `${SESSION_SENTENCE}${(await handOff) ?? ""}`);
          continue;
        }
        await setUnit(requestId, u.unitId, { status: "running", startedAt: new Date() });
        await Order.updateOne({ userId: request.userId, unit_id: u.unitId }, { $set: { reschedule_status: "running" } });
        const base = path.join(artifactDir, u.unitId);
        const deadline = Date.now() + UNIT_TIMEOUT_MS;
        try {
          const q = new URLSearchParams({ order_id: u.orderId, unit_id: u.unitId, item_id: u.itemId || itemIdOf(u.unitId) });
          await page.goto(`https://www.flipkart.com/order_details?${q.toString()}`, { waitUntil: "domcontentloaded", timeout: PAGE_TIMEOUT_MS });
          if (flipkartLoginUrl(page.url())) {
            sessionDead = true;
            await finish(u, "failed", `${SESSION_SENTENCE}${await sessionGone()}`);
            continue;
          }
          // The page is client-rendered; wait until the order card is there (or give up on a blank page).
          let text = "";
          for (let i = 0; i < 20 && Date.now() < deadline; i++) {
            await sleep(1500);
            text = await page.evaluate(() => document.body?.innerText || "");
            if (text.length > 1500 && /Cancel|Change Date|Deliver/i.test(text)) break;
          }
          if (text.length < 500) {
            await page.screenshot({ path: `${base}-blank.png`, fullPage: true }).catch(() => undefined);
            await finish(u, "failed", "order page gave no data (blank page) — try again later");
            continue;
          }
          const hasButton = /\bChange Date\b/.test(text);

          // 1. Flipkart's own feasibility answer decides.
          const f = await feasibility(page, u.orderId, u.unitId);
          if (!f.ok) {
            await finish(u, "failed", `feasibility check failed (HTTP ${f.status})`);
            continue;
          }
          if (!f.feasible) {
            await finish(u, "not_available", `${f.title ? `${f.title} — ` : ""}${f.messages.join(" ") || "Flipkart does not allow a date change on this unit now"}${hasButton ? "" : " (no Change Date button on the page)"}`, { token: f.token });
            continue;
          }

          // 2. Feasible: open the dialog and capture it.
          if (!hasButton) {
            await finish(u, "failed", "Flipkart says a date change is feasible but the page shows no Change Date button", { token: f.token });
            continue;
          }
          const before = new Set(text.split("\n"));
          await page.getByText(/^\s*Change Date\s*$/i).first().click({ timeout: 10_000 });
          await sleep(5000);
          const after = await page.evaluate(() => document.body?.innerText || "");
          const dialogLines = after.split("\n").map((l) => l.trim()).filter((l) => l && !before.has(l));
          const options = await page.evaluate(() =>
            Array.from(document.querySelectorAll("button,label,[role=button],[role=radio],li,input[type=radio]"))
              .map((e) => (e.textContent || (e as HTMLInputElement).value || "").trim())
              .filter((t) => t && t.length < 80)
              .filter((v, i, a) => a.indexOf(v) === i)
          );
          await page.screenshot({ path: `${base}-dialog.png`, fullPage: true }).catch(() => undefined);
          fs.writeFileSync(`${base}-dialog.json`, JSON.stringify({ feasibility: f.raw, dialogLines, options }, null, 2));
          const offered = options.filter((o) => needles.some(() => /\b(mon|tue|wed|thu|fri|sat|sun)\b|\b\d{1,2}\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)|\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+\d{1,2}\b/i.test(o)));
          const match = offered.find((o) => needles.some((re) => re.test(o)));
          log(`${u.orderId}/${u.unitId} dialog captured — ${offered.length} date option(s)${match ? `, "${match}" matches ${request.date}` : `, none matches ${request.date}`}`);

          if (!request.confirm) {
            await finish(u, "needs_review", `Flipkart offers a date change (${offered.length ? offered.slice(0, 6).join(" · ") : "see dialog capture"}). Confirm is off (RESCHEDULE_CONFIRM) — nothing was changed.`, { offeredDates: offered, token: f.token });
            continue;
          }
          if (!match) {
            await finish(u, "failed", `Flipkart did not offer ${request.date}; offered: ${offered.join(" · ") || "no dates recognised"}`, { offeredDates: offered, token: f.token });
            continue;
          }
          // 3. Choose the date and confirm.
          await page.getByText(match, { exact: false }).first().click({ timeout: 10_000 });
          await sleep(1500);
          const confirmBtn = page.locator("button").filter({ hasText: /confirm|reschedule|change date|submit|update|proceed/i }).filter({ hasNotText: /cancel|ok$/i }).last();
          if (!(await confirmBtn.count())) throw new Error("date chosen but no confirm button found — see the dialog capture");
          await confirmBtn.click({ timeout: 10_000 });
          await sleep(6000);
          const result = await page.evaluate(() => document.body?.innerText || "");
          await page.screenshot({ path: `${base}-result.png`, fullPage: true }).catch(() => undefined);
          const line = result.split("\n").map((l) => l.trim()).find((l) => l && l.length < 200 && /reschedul|changed|updated|confirm|new delivery|will be delivered/i.test(l)) || "";
          if (/reschedul|has been changed|updated|will be delivered/i.test(line)) await finish(u, "rescheduled", line, { offeredDates: offered, chosen: match, token: f.token });
          else await finish(u, "failed", `confirm pressed but Flipkart's answer was not recognised${line ? `: "${line}"` : ""} — see ${path.basename(base)}-result.png`, { offeredDates: offered, token: f.token });
        } catch (err) {
          const message = err instanceof Error ? err.message.split("\n")[0] : String(err);
          await page.screenshot({ path: `${base}-error.png`, fullPage: true }).catch(() => undefined);
          const loggedOut = /SESSION_EXPIRED/.test(message);
          if (loggedOut) sessionDead = true;
          await finish(u, "failed", `${message.slice(0, 500)}${loggedOut ? await sessionGone() : ""}`);
        }
      }
    } finally {
      await browser.close().catch(() => undefined);
      await lease.release().catch(() => undefined);
    }
  } finally {
    await unlock();
    const fresh = await RescheduleRequest.findById(requestId).select("units status").lean();
    const done = Boolean(fresh && !fresh.units.some((u) => u.status === "queued" || u.status === "running"));
    if (done) {
      await RescheduleRequest.updateOne({ _id: requestId }, { $set: { status: "done", finishedAt: new Date() } });
      console.log(`[reschedule ${requestId}] done`);
    }
    // The task log follows what this account's job wrote on the request — at once when that ended the request.
    await syncRequestTasksSoon("reschedule", requestId, done);
  }
}

// ── reschedule reaper ─────────────────────────────────────────────────────────
// The twin of the cancel / update reaper. Each account is one fire-and-forget
// job; a request whose jobs are lost — worker restarted mid-delay, jobs removed
// on a transient failure, or a request created against a Redis this worker no
// longer reads — sits "queued"/"running" forever with nothing to pick it up.
// This sweep finds such stranded requests (open units, quiet a while, no live
// job) and re-enqueues the missing per-account jobs; runRescheduleJob re-picks
// the account's queued / stale-running units from the doc.
// It takes only this system's requests: see rescheduleReapCandidatesFilter.

let reapingReschedules = false;

/**
 * The requests the reschedule reaper may take: open, quiet for REAP_STALE_MS, and made on this system. Their jobs live
 * in this machine's Redis; another Mac's quiet request is that Mac's to re-enqueue (the Macs share Atlas, not Redis).
 */
export function rescheduleReapCandidatesFilter(now = Date.now(), systemId = config.systemId) {
  return { status: { $in: ["queued", "running"] }, systemId, updatedAt: { $lt: new Date(now - REAP_STALE_MS) } };
}

export async function reapStuckReschedules(): Promise<number> {
  if (reapingReschedules) return 0;
  reapingReschedules = true;
  try {
    const open = await RescheduleRequest.find(rescheduleReapCandidatesFilter())
      .select("_id units.email units.status units.startedAt")
      .lean();
    if (!open.length) return 0;

    const queue = getRescheduleQueue();
    const isStaleRunning = (status: string, startedAt?: Date | null) =>
      status === "running" && (!startedAt || Date.now() - new Date(startedAt).getTime() > STALE_RUNNING_MS);

    let requeued = 0;
    for (const r of open) {
      const requestId = String(r._id);
      const emails = new Set<string>();
      for (const u of r.units || []) if (u.status === "queued" || isStaleRunning(u.status, u.startedAt)) emails.add(u.email);

      for (const email of emails) {
        const jobId = `${requestId}-${email}`;
        // A live job (waiting/active/delayed) means it is already being handled.
        const existing = await queue.getJob(jobId).catch(() => null);
        if (existing) continue;
        await queue.add("reschedule", { requestId, email }, { jobId }).catch(() => undefined);
        requeued++;
        console.log(`[reschedule-reaper] re-enqueued stranded ${requestId.slice(0, 8)} / ${email}`);
      }
    }
    if (requeued) console.log(`[reschedule-reaper] re-enqueued ${requeued} stranded reschedule job(s) across ${open.length} open request(s)`);
    return requeued;
  } finally {
    reapingReschedules = false;
  }
}

/** Start the periodic reschedule reaper (worker, reschedule role only). Returns the timer. */
export function startRescheduleReaper() {
  const t = setInterval(() => {
    void reapStuckReschedules().catch((e) => console.error("[reschedule-reaper]", e instanceof Error ? e.message : e));
  }, REAP_EVERY_MS);
  t.unref();
  console.log(`[reschedule-reaper] on — re-enqueues stranded reschedules every ${Math.round(REAP_EVERY_MS / 1000)}s (stranded after ${Math.round(REAP_STALE_MS / 60000)}m quiet)`);
  return t;
}
