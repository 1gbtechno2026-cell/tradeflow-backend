import fs from "node:fs";
import path from "node:path";
import { DelayedError, type Job } from "bullmq";
import { Types } from "mongoose";
import type { Page } from "playwright";
import { itemIdOf } from "../automation/FlipkartCancel.js";
import { fullOrderId } from "../automation/PaymentApiWatcher.js";
import { config } from "../config.js";
import { Order } from "../models/Order.js";
import { RescheduleRequest, type IRescheduleRequest, type IRescheduleUnit, type RescheduleUnitStatus } from "../models/RescheduleRequest.js";
import { getRescheduleQueue, type RescheduleJobData } from "../queue.js";
import { lockAccount } from "./accountLock.js";
import { blockFlipkartLogout, desktopContext, flipkartLoginUrl, launchStealthBrowser, restoreFlipkartSession, sleep } from "./browser.js";
import { claimEgress } from "./proxyPool.js";
import { resolveLoggedInSession } from "./sessionStore.js";

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
    confirm: rescheduleConfirmEnabled(),
    status: open ? "queued" : "done",
    units,
    rejected,
    finishedAt: open ? null : new Date(),
  });
  const requestId = String(request._id);
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

export async function runRescheduleJob(job: Job<RescheduleJobData>, token: string | undefined): Promise<void> {
  const { requestId, email } = job.data;
  const unlock = await lockAccount(email, `reschedule:${requestId}:${config.workerId}`);
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
    const finish = async (u: IRescheduleUnit, status: RescheduleUnitStatus, message: string, extra: Partial<IRescheduleUnit> = {}) => {
      await setUnit(requestId, u.unitId, { status, message, finishedAt: new Date(), ...extra });
      await applyToOrder(request.userId, u.unitId, status, message, extra.chosen || "");
      log(`${u.orderId}/${u.unitId} → ${status}${message ? `: ${message}` : ""}`);
    };

    const session = await resolveLoggedInSession(email);
    if (!session.ok) {
      for (const u of units) await finish(u, "failed", `Session: ${session.reason} — re-login this ID`);
      return;
    }
    const lease = await claimEgress(`reschedule:${requestId}:${email}`).catch(() => ({ egress: null, release: async () => undefined }));
    const egress = lease.egress;
    const browser = await launchStealthBrowser({ headless: config.headless, proxy: egress?.proxy });
    const artifactDir = path.resolve("debug/reschedule-runs", requestId);
    fs.mkdirSync(artifactDir, { recursive: true });
    try {
      const ctx = await desktopContext(browser);
      const page = await ctx.newPage();
      await blockFlipkartLogout(page);
      await restoreFlipkartSession(page, session.cookies);
      let sessionDead = false;
      const needles = dateNeedles(request.date);

      for (const u of units) {
        if (sessionDead) {
          await finish(u, "failed", "Session expired — re-login this ID");
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
            await finish(u, "failed", "Session expired — re-login this ID");
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
          await finish(u, "failed", message.slice(0, 500));
          if (/SESSION_EXPIRED/.test(message)) sessionDead = true;
        }
      }
    } finally {
      await browser.close().catch(() => undefined);
      await lease.release().catch(() => undefined);
    }
  } finally {
    await unlock();
    const fresh = await RescheduleRequest.findById(requestId).select("units status").lean();
    if (fresh && !fresh.units.some((u) => u.status === "queued" || u.status === "running")) {
      await RescheduleRequest.updateOne({ _id: requestId }, { $set: { status: "done", finishedAt: new Date() } });
      console.log(`[reschedule ${requestId}] done`);
    }
  }
}
