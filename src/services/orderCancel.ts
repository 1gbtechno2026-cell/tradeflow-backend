import fs from "node:fs";
import path from "node:path";
import { DelayedError, type Job } from "bullmq";
import { Types } from "mongoose";
import { FlipkartCancel, itemIdOf, type CancelOutcome } from "../automation/FlipkartCancel.js";
import { fullOrderId } from "../automation/PaymentApiWatcher.js";
import { config } from "../config.js";
import { CancelRequest, type CancelUnitStatus, type ICancelUnit } from "../models/CancelRequest.js";
import { Order } from "../models/Order.js";
import { getCancelQueue, type CancelJobData } from "../queue.js";
import { blockFlipkartLogout, desktopContext, launchStealthBrowser, restoreFlipkartSession } from "./browser.js";
import { lockAccount } from "./accountLock.js";
import { claimProxyForWorker } from "./proxyPool.js";
import { resolveLoggedInSession } from "./sessionStore.js";

/**
 * "Cancel Order Units" — the dashboard's popup (Unit IDs or Order IDs) and the
 * row-level Cancel both land here.
 *
 *   requestCancellation  resolve the typed ids to Order rows, reject what cannot
 *                        be cancelled, mark the rest queued, one job per account
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

export interface CancelRequestInput {
  type: "unit" | "order";
  ids: string[];
  /** Walk to REQUEST CANCELLATION and stop. For testing the path on a real unit. */
  dryRun?: boolean;
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

  const rows = await Order.find({
    userId,
    ...(type === "unit" ? { unit_id: { $in: ids } } : { order_id: { $in: ids } }),
  })
    .select("platform platform_email order_id unit_id item_id product_name status_key cancel_status")
    .lean();

  const rejected: Array<{ id: string; reason: string }> = [];
  const units: ICancelUnit[] = [];
  for (const id of ids) {
    const matches = rows.filter((r) => (type === "unit" ? r.unit_id === id : r.order_id === id));
    if (!matches.length) {
      rejected.push({ id, reason: "Not found in Order Units — fetch the account's orders first" });
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
