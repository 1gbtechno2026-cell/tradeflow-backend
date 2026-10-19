import fs from "node:fs";
import path from "node:path";
import { DelayedError, type Job } from "bullmq";
import { Types } from "mongoose";
import { FlipkartCancel, itemIdOf, type CancelOutcome } from "../automation/FlipkartCancel.js";
import { fullOrderId } from "../automation/PaymentApiWatcher.js";
import { config } from "../config.js";
import { CancelRequest, type CancelUnitStatus, type ICancelUnit } from "../models/CancelRequest.js";
import { Order } from "../models/Order.js";
import type { APIRequestContext } from "playwright";
import { getCancelQueue, type CancelJobData } from "../queue.js";
import { blockFlipkartLogout, desktopContext, launchStealthBrowser, restoreFlipkartSession } from "./browser.js";
import { lockAccount } from "./accountLock.js";
import { fetchOrderDetails, mapApiUnitToOrder, openOrderApiSession } from "./orderApi.js";
import { fallbackUnitId, saveMappedUnit, type OrderAccount } from "./orderFetch.js";
import { claimProxyForWorker } from "./proxyPool.js";
import { resolveLoggedInSession } from "./sessionStore.js";

/**
 * "Cancel Order Units" — the dashboard's popup (Unit IDs or Order IDs) and the
 * row-level Cancel both land here.
 *
 *   requestCancellation  resolve the typed ids to Order rows, reject what cannot
 *                        be cancelled, mark the rest queued, one job per account
 *   pullUnknownOrders    an Order ID that was never fetched, when the caller
 *                        names the Platform ID that placed it: read that one
 *                        order through Flipkart's order API (every unit) and
 *                        save it, so the cancel can go ahead without a Fetch
 *   runCancelJob         (worker) one browser per account, its units in turn,
 *                        each through FlipkartCancel, each result onto both the
 *                        CancelRequest and the Order row
 *
 * Eligibility is Flipkart's to decide (FlipkartCancel reads it from the order
 * page): we only refuse what our own record already shows as over — delivered,
 * cancelled, returned, refunded, failed — or already being cancelled.
 */

const TERMINAL = /deliver|cancel|return|refund|fail/i;
const IN_FLIGHT = new Set(["queued", "running"]);
/** The account is busy (checkout, sync or another cancel) — try again after this. */
const LOCK_RETRY_MS = 20_000;
/** Unknown orders are read inside the request; keep that to seconds, not minutes. */
const MAX_PULLED_ORDERS = 50;

export interface CancelRequestInput {
  type: "unit" | "order";
  ids: string[];
  /** Platform ID (email) per Order ID, for orders never fetched into Order Units. */
  accounts?: Record<string, string>;
  /** Platform ID (email) for every unknown Order ID not named in `accounts`. */
  email?: string;
  /** Walk to REQUEST CANCELLATION and stop. For testing the path on a real unit. */
  dryRun?: boolean;
}

const NEEDS_EMAIL = "Not found in Order Units — add the Platform ID (email) that placed it, or fetch the account's orders first";

/**
 * Read the given orders from Flipkart through the named accounts and save
 * every unit of each, exactly as a Fetch would (same mapper, same row shape,
 * `applyCalendar: false` so the since-date window never drops one). Returns a
 * reason per order that could not be read; the rest are now in Order Units.
 */
async function pullUnknownOrders(userId: string, orderIds: string[], emailFor: (orderId: string) => string) {
  const errors = new Map<string, string>();
  const byEmail = new Map<string, string[]>();
  for (const orderId of orderIds) {
    const email = emailFor(orderId);
    if (!email) errors.set(orderId, NEEDS_EMAIL);
    else if (byEmail.size >= MAX_PULLED_ORDERS) errors.set(orderId, `At most ${MAX_PULLED_ORDERS} unfetched orders per request — fetch the account's orders instead`);
    else byEmail.set(email, [...(byEmail.get(email) || []), orderId]);
  }

  for (const [email, list] of byEmail) {
    const session = await resolveLoggedInSession(email);
    if (!session.ok) {
      for (const orderId of list) errors.set(orderId, `${email}: ${session.reason} — re-login this ID`);
      continue;
    }
    const account: OrderAccount = { id: new Types.ObjectId(session.platformId), email: session.email, cookies: session.cookies };
    let ctx: APIRequestContext | null = null;
    try {
      ctx = await openOrderApiSession(session.cookies);
      for (const orderId of list) {
        try {
          // The details page answers for any unit id of the order; the first
          // call (guessed id) tells us the real unit ids, then one call per unit
          // so each row carries its own tracking and money, as a Fetch does.
          const first = await fetchOrderDetails(ctx, orderId, fallbackUnitId(orderId));
          const unitIds = Object.keys(first.orderView.units || {});
          if (!unitIds.length) {
            errors.set(orderId, `${email}: Flipkart shows no units for this order on this Platform ID — is it the account that placed it?`);
            continue;
          }
          for (const unitId of unitIds) {
            const view = unitId === fallbackUnitId(orderId) ? first.orderView : (await fetchOrderDetails(ctx, orderId, unitId)).orderView;
            await saveMappedUnit(userId, account, mapApiUnitToOrder(view, unitId), { applyCalendar: false });
          }
          console.log(`[cancel] pulled ${orderId} (${unitIds.length} unit${unitIds.length === 1 ? "" : "s"}) via ${email} for cancellation`);
        } catch (err) {
          const raw = (err instanceof Error ? err.message : String(err)).split("\n")[0];
          // The details page comes back without an order view when this account
          // never placed that order — say that, not the widget names.
          const reason = /carried neither/.test(raw) ? "Flipkart shows no such order on this Platform ID — is it the account that placed it?" : raw.slice(0, 200);
          errors.set(orderId, `${email}: ${reason}`);
        }
      }
    } catch (err) {
      const reason = (err instanceof Error ? err.message : String(err)).split("\n")[0].slice(0, 200);
      for (const orderId of list) if (!errors.has(orderId)) errors.set(orderId, `${email}: ${reason}`);
    } finally {
      await ctx?.dispose().catch(() => undefined);
    }
  }
  return errors;
}

function normaliseIds(type: "unit" | "order", ids: string[]): string[] {
  const out = ids
    .map((raw) => String(raw || "").trim())
    .filter(Boolean)
    .map((id) => (type === "order" ? fullOrderId(id.toUpperCase()) : id.replace(/\D/g, "")))
    .filter(Boolean);
  return [...new Set(out)];
}

export async function requestCancellation(userId: string, input: CancelRequestInput) {
  const type = input.type === "unit" ? "unit" : "order";
  const ids = normaliseIds(type, input.ids);
  if (!ids.length) throw new Error(type === "unit" ? "Paste at least one Unit ID" : "Paste at least one Order ID");
  if (ids.length > 2000) throw new Error("At most 2000 ids per request");

  const select = "platform platform_email order_id unit_id item_id product_name status_key cancel_status";
  const find = (list: string[]) =>
    Order.find({ userId, ...(type === "unit" ? { unit_id: { $in: list } } : { order_id: { $in: list } }) })
      .select(select)
      .lean();
  let rows = await find(ids);

  // Order IDs we have never fetched: read them from Flipkart now when the
  // caller says which Platform ID placed them, then look again.
  const pullErrors = new Map<string, string>();
  if (type === "order") {
    const known = new Set(rows.map((r) => r.order_id));
    const unknown = ids.filter((id) => !known.has(id));
    if (unknown.length) {
      const named = new Map(Object.entries(input.accounts || {}).map(([k, v]) => [fullOrderId(String(k).trim().toUpperCase()), String(v || "").trim().toLowerCase()]));
      const fallback = String(input.email || "").trim().toLowerCase();
      const errors = await pullUnknownOrders(userId, unknown, (orderId) => named.get(orderId) || fallback);
      for (const [id, reason] of errors) pullErrors.set(id, reason);
      const pulled = unknown.filter((id) => !errors.has(id));
      if (pulled.length) rows = [...rows, ...(await find(pulled))];
    }
  }

  const rejected: Array<{ id: string; reason: string }> = [];
  const units: ICancelUnit[] = [];
  for (const id of ids) {
    const matches = rows.filter((r) => (type === "unit" ? r.unit_id === id : r.order_id === id));
    if (!matches.length) {
      rejected.push({ id, reason: pullErrors.get(id) || (type === "order" ? NEEDS_EMAIL : "Not found in Order Units — fetch the account's orders first") });
      continue;
    }
    for (const r of matches) {
      const label = type === "order" ? `${id} / ${r.unit_id}` : id;
      if (String(r.platform || "FLIPKART").toUpperCase() !== "FLIPKART") {
        rejected.push({ id: label, reason: "Only Flipkart orders can be cancelled" });
      } else if (TERMINAL.test(String(r.status_key || ""))) {
        rejected.push({ id: label, reason: `Already ${r.status_key}` });
      } else if (IN_FLIGHT.has(String(r.cancel_status || ""))) {
        rejected.push({ id: label, reason: `Cancellation already ${r.cancel_status}` });
      } else {
        units.push({
          orderId: r.order_id,
          unitId: r.unit_id,
          itemId: String(r.item_id || "") || itemIdOf(r.unit_id),
          email: r.platform_email,
          productName: String(r.product_name || ""),
          status: "queued",
          message: "",
          startedAt: null,
          finishedAt: null,
        });
      }
    }
  }

  const request = await CancelRequest.create({
    userId: new Types.ObjectId(userId),
    type,
    input: ids,
    dryRun: Boolean(input.dryRun),
    status: units.length ? "queued" : "done",
    units,
    rejected,
    finishedAt: units.length ? null : new Date(),
  });
  const requestId = String(request._id);

  if (units.length) {
    await Order.updateMany(
      { userId, unit_id: { $in: units.map((u) => u.unitId) } },
      { $set: { cancel_status: "queued", cancel_request_id: requestId, cancel_requested_at: new Date(), cancel_error: "" } }
    );
    const emails = [...new Set(units.map((u) => u.email))];
    const queue = getCancelQueue();
    await queue.addBulk(emails.map((email) => ({ name: "cancel", data: { requestId, email }, opts: { jobId: `${requestId}-${email}` } })));
  }

  return publicRequest(request.toObject());
}

export async function getCancelRequest(userId: string, requestId: string) {
  if (!Types.ObjectId.isValid(requestId)) return null;
  const row = await CancelRequest.findOne({ _id: requestId, userId }).lean();
  return row ? publicRequest(row) : null;
}

export async function listCancelRequests(userId: string, limit = 20) {
  const rows = await CancelRequest.find({ userId }).sort({ createdAt: -1 }).limit(Math.min(100, Math.max(1, limit))).lean();
  return rows.map(publicRequest);
}

function publicRequest(r: {
  _id: unknown;
  type: string;
  dryRun?: boolean;
  status: string;
  units: ICancelUnit[];
  rejected: Array<{ id: string; reason: string }>;
  createdAt?: Date;
  finishedAt?: Date | null;
}) {
  const count = (s: CancelUnitStatus) => r.units.filter((u) => u.status === s).length;
  return {
    id: String(r._id),
    type: r.type,
    dryRun: Boolean(r.dryRun),
    status: r.status,
    createdAt: r.createdAt ?? null,
    finishedAt: r.finishedAt ?? null,
    counts: {
      total: r.units.length,
      queued: count("queued"),
      running: count("running"),
      cancelled: count("cancelled"),
      notCancellable: count("not_cancellable"),
      failed: count("failed"),
      dryRun: count("dry_run"),
      rejected: r.rejected.length,
    },
    units: r.units,
    rejected: r.rejected,
  };
}

// ── worker side ──────────────────────────────────────────────────────────────

async function setUnit(requestId: string, unitId: string, patch: Partial<ICancelUnit>) {
  const set: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(patch)) set[`units.$[u].${k}`] = v;
  await CancelRequest.updateOne({ _id: requestId }, { $set: set }, { arrayFilters: [{ "u.unitId": unitId }] });
}

/** The Order row, in the shape the order sync writes a cancelled unit in, until the next sync refines it. */
async function applyToOrder(userId: Types.ObjectId, unitId: string, status: CancelUnitStatus, message: string) {
  if (status === "cancelled") {
    const row = await Order.findOne({ userId, unit_id: unitId }).select("status_key").lean();
    await Order.updateOne(
      { userId, unit_id: unitId },
      {
        $set: {
          cancel_status: "cancelled",
          cancel_error: "",
          status_key: "Cancelled",
          status_label: "Cancelled",
          status_reason: "Cancelled from the dashboard (Trade Flow)",
          cancelled_date: new Date(),
          cancelled_from_bae: true,
          cancelled_by_user: true,
          order_status_pre_cancellation: String(row?.status_key || "") || "Order Confirmed",
          delivery_otp: null,
        },
      }
    );
    return;
  }
  // A dry run leaves the row as it found it.
  const cancel_status = status === "dry_run" ? "" : status;
  await Order.updateOne({ userId, unit_id: unitId }, { $set: { cancel_status, cancel_error: status === "dry_run" ? "" : message } });
}

export async function runCancelJob(job: Job<CancelJobData>, token: string | undefined): Promise<void> {
  const { requestId, email } = job.data;
  const unlock = await lockAccount(email, `cancel:${requestId}:${config.workerId}`);
  if (!unlock) {
    // A checkout, sync or another cancel is on this account right now — come
    // back shortly rather than use the same login twice at once.
    await job.moveToDelayed(Date.now() + LOCK_RETRY_MS, token);
    throw new DelayedError();
  }

  const request = await CancelRequest.findById(requestId).lean();
  try {
    if (!request) return;
    const units = request.units.filter((u) => u.email === email.toLowerCase() && u.status === "queued");
    if (!units.length) return;
    await CancelRequest.updateOne({ _id: requestId, status: "queued" }, { $set: { status: "running" } });
    const log = (msg: string) => console.log(`[cancel ${requestId}] ${email} ${msg}`);

    const finish = async (u: ICancelUnit, status: CancelUnitStatus, message: string) => {
      await setUnit(requestId, u.unitId, { status, message, finishedAt: new Date() });
      await applyToOrder(request.userId, u.unitId, status, message);
      log(`${u.orderId}/${u.unitId} → ${status}${message ? `: ${message}` : ""}`);
    };

    const session = await resolveLoggedInSession(email);
    if (!session.ok) {
      for (const u of units) await finish(u, "failed", `Session: ${session.reason} — re-login this ID`);
      return;
    }

    const egress = await claimProxyForWorker(config.workerId).catch(() => null);
    const browser = await launchStealthBrowser({ headless: config.headless, proxy: egress?.proxy });
    const artifactDir = path.resolve("debug/cancel-runs", requestId);
    try {
      const ctx = await desktopContext(browser);
      const page = await ctx.newPage();
      await blockFlipkartLogout(page);
      await restoreFlipkartSession(page, session.cookies);
      const flow = new FlipkartCancel(page, (_lvl, msg) => log(msg));

      for (const u of units) {
        await setUnit(requestId, u.unitId, { status: "running", startedAt: new Date() });
        await Order.updateOne({ userId: request.userId, unit_id: u.unitId }, { $set: { cancel_status: "running" } });
        let lastStep = "";
        try {
          const outcome: CancelOutcome = await flow.cancelUnit(
            { orderId: u.orderId, unitId: u.unitId, itemId: u.itemId },
            { dryRun: request.dryRun, onStep: async (name) => void (lastStep = name) }
          );
          const note = "feeNote" in outcome && outcome.feeNote ? ` — fee: ${outcome.feeNote}` : "";
          if (outcome.kind === "cancelled") {
            await finish(u, "cancelled", `${outcome.message}${outcome.refundTat ? ` · ${outcome.refundTat}` : ""}${note}`);
          } else if (outcome.kind === "dry_run") {
            await finish(u, "dry_run", `${outcome.message}${note}`);
          } else {
            await finish(u, "not_cancellable", outcome.message);
          }
        } catch (err) {
          const message = err instanceof Error ? err.message.split("\n")[0] : String(err);
          // The page it broke on, for the next person reading this failure.
          fs.mkdirSync(artifactDir, { recursive: true });
          const base = path.join(artifactDir, `${u.unitId}-${lastStep || "start"}`);
          await page.screenshot({ path: `${base}.png`, fullPage: true }).catch(() => undefined);
          await finish(u, "failed", message.slice(0, 500));
          if (/SESSION_EXPIRED/.test(message)) {
            for (const rest of units.slice(units.indexOf(u) + 1)) await finish(rest, "failed", "Session expired — re-login this ID");
            break;
          }
        }
      }
    } finally {
      await browser.close().catch(() => undefined);
    }
  } finally {
    await unlock();
    // Done once no unit anywhere in the request is still waiting or running.
    await CancelRequest.updateOne(
      { _id: requestId, "units.status": { $nin: ["queued", "running"] } },
      { $set: { status: "done", finishedAt: new Date() } }
    );
  }
}
