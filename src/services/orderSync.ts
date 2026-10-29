import { DelayedError, type Job } from "bullmq";
import { Types } from "mongoose";
import { config } from "../config.js";
import { CheckoutJob } from "../models/CheckoutJob.js";
import { getSyncQueue, SYNC_QUEUE, type SyncJobData } from "../queue.js";
import { lockAccount } from "./accountLock.js";
import { openOrderApiSession } from "./orderApi.js";
import { releaseBatchReservation } from "./batchCounters.js";
import { walkOrderList } from "./orderApiSync.js";
import { apiScrapeAndSave, fallbackUnitId, orderDetailsUrl, type OrderAccount, type OrderUnitCard } from "./orderFetch.js";
import { claimProxyForWorker } from "./proxyPool.js";
import { resolveLoggedInSession } from "./sessionStore.js";

/**
 * Every finished checkout's order, into Order Units — without anyone running a
 * fetch, and without the checkout waiting for it.
 *
 *   enqueueOrderSync   the checkout's last act: a few ms, one queue message,
 *                      keyed by the checkout job so it can only exist once
 *   runOrderSyncJob    (sync worker) Flipkart's order API over HTTP — no
 *                      browser — through the pool, under the account lock
 *   sweepOrderSyncs    the outbox sweep: any finished job whose sync is still
 *                      unset or pending after SYNC_SWEEP_GRACE_MS is queued
 *                      again, so a lost message or a dead worker is caught
 *
 * The CheckoutJob is the source of truth (orderSync.status); the queue only
 * carries the work. What gets synced:
 *
 *   order id known (paid)           that order's units, siblings included
 *   Pay pressed, no order id        the account's list from just before Pay;
 *   (failed / needs_reconciliation) units of this product nobody else claims
 *                                   are saved, and a single such order is
 *                                   written back as the job's order id
 *   never reached Pay               nothing exists on Flipkart — "skipped"
 */

const TERMINAL = ["paid", "failed", "needs_reconciliation"];
/** Listing-pending orders (result.listed === false): how often and how many times the sync asks My Orders before giving up (default every 20 min, 25 times ≈ 8 h). */
const LISTING_RETRY_MS = Number(process.env.SYNC_LISTING_RETRY_MS || 20 * 60 * 1000);
const LISTING_ATTEMPTS = Number(process.env.SYNC_LISTING_ATTEMPTS || 25);
/** The account is busy with a checkout, cancel or another sync. */
const LOCK_RETRY_MS = 30_000;
const SWEEP_GRACE_MS = Number(process.env.SYNC_SWEEP_GRACE_MS || 5 * 60 * 1000);
/** Do not chase jobs older than this; the regular fetch/update owns them. */
const SWEEP_MAX_AGE_MS = 3 * 24 * 3600 * 1000;
/** List-walk window starts this long before Pay was pressed. */
const DISCOVER_SLACK_MS = 5 * 60 * 1000;

type SyncableJob = {
  _id: Types.ObjectId;
  userId: string;
  email: string;
  status: string;
  productUrl: string;
  paymentStartedAt?: Date | null;
  startedAt?: Date | null;
  batchId?: string;
  quantity?: number;
  result?: { flipkartOrderId?: string; listed?: boolean };
};

function syncKind(job: SyncableJob): "order" | "discover" | "skip" {
  if (/^OD\d{12,}/.test(String(job.result?.flipkartOrderId || ""))) return "order";
  if (job.paymentStartedAt) return "discover";
  return "skip";
}

/** Best-effort, never throws: the checkout has finished whatever this does. */
export async function enqueueOrderSync(checkoutJobId: string): Promise<void> {
  try {
    const job = await CheckoutJob.findById(checkoutJobId)
      .select("status paymentStartedAt result.flipkartOrderId result.listed orderSync.status")
      .lean<SyncableJob & { orderSync?: { status?: string } }>();
    if (!job || !TERMINAL.includes(job.status) || job.orderSync?.status === "done") return;
    // An order Flipkart confirmed but had not listed when the engine checked can
    // take hours to show up in My Orders (2026-10-28: 171 of 173 did). Keep
    // asking every LISTING_RETRY_MS; only the final failure parks the job.
    const opts = job.result?.listed === false ? { attempts: LISTING_ATTEMPTS, backoff: { type: "fixed" as const, delay: LISTING_RETRY_MS } } : {};
    if (syncKind(job) === "skip") {
      await CheckoutJob.updateOne(
        { _id: checkoutJobId },
        { $set: { orderSync: { status: "skipped", attempts: 0, lastError: "", orderIds: [], units: 0, queuedAt: null, syncedAt: new Date() } } }
      );
      return;
    }
    await CheckoutJob.updateOne(
      { _id: checkoutJobId, "orderSync.status": { $ne: "done" } },
      { $set: { "orderSync.status": "pending", "orderSync.queuedAt": new Date() } }
    );
    // jobId = the checkout job: while one sync for it is queued, delayed or
    // running, adding it again does nothing.
    await getSyncQueue().add("sync", { checkoutJobId }, { jobId: `sync-${checkoutJobId}`, ...opts });
  } catch (err) {
    // The sweep picks it up.
    console.warn(`[order-sync] could not queue ${checkoutJobId}: ${err instanceof Error ? err.message : String(err)}`);
  }
}

function pidOf(productUrl: string): string {
  try {
    return new URL(productUrl).searchParams.get("pid") || "";
  } catch {
    return "";
  }
}

export async function runOrderSyncJob(job: Job<SyncJobData>, token: string | undefined): Promise<void> {
  const { checkoutJobId } = job.data;
  const cj = await CheckoutJob.findById(checkoutJobId)
    .select("userId email status productUrl paymentStartedAt startedAt batchId quantity result.flipkartOrderId result.listed orderSync")
    .lean<SyncableJob>();
  if (!cj) return;
  const kind = syncKind(cj);
  if (kind === "skip") return;
  const listingPending = kind === "order" && cj.result?.listed === false && cj.status === "paid";

  const unlock = await lockAccount(cj.email, `sync:${checkoutJobId}:${config.workerId}`, 5 * 60 * 1000);
  if (!unlock) {
    await job.moveToDelayed(Date.now() + LOCK_RETRY_MS, token);
    throw new DelayedError();
  }

  const attempt = job.attemptsMade + 1;
  await CheckoutJob.updateOne({ _id: checkoutJobId }, { $set: { "orderSync.attempts": attempt } });
  const tag = `[order-sync ${checkoutJobId}] ${cj.email}`;
  try {
    const session = await resolveLoggedInSession(cj.email);
    // A dead session will not heal by retrying; say so and stop.
    if (!session.ok) {
      await markFailed(checkoutJobId, `Session: ${session.reason} — re-login this ID; the next fetch will pick the order up`);
      return;
    }
    const account: OrderAccount = { id: new Types.ObjectId(session.platformId), email: session.email, cookies: session.cookies };
    const egress = await claimProxyForWorker(config.workerId).catch(() => null);
    const ctx = await openOrderApiSession(session.cookies, { proxy: egress?.proxy });
    try {
      const saved = new Set<string>();
      const orderIds = new Set<string>();
      let listedStatus = "";
      const syncOrder = async (card: OrderUnitCard) => {
        const first = await apiScrapeAndSave(ctx, cj.userId, account, card, { applyCalendar: false });
        saved.add(card.unitId);
        orderIds.add(card.orderId);
        if (!listedStatus && first.statusKey && first.statusKey !== "unknown") listedStatus = first.statusKey;
        // The order view lists every unit of the order: a multi-unit order is
        // saved whole from one known unit.
        for (const unitId of first.siblingUnitIds || []) {
          if (saved.has(unitId)) continue;
          await apiScrapeAndSave(ctx, cj.userId, account, { ...card, unitId, itemId: "", orderUrl: orderDetailsUrl(card.orderId, "", unitId) }, { applyCalendar: false });
          saved.add(unitId);
        }
      };

      if (kind === "order") {
        const orderId = String(cj.result!.flipkartOrderId).toUpperCase();
        await syncOrder({ orderId, itemId: "", unitId: fallbackUnitId(orderId), orderUrl: orderDetailsUrl(orderId), amount: "" });
      } else {
        // Pay was pressed and no order id came back. Flipkart may still have
        // created one (PAYMENT_NOT_CONFIRMED). Look at what the account ordered
        // from just before Pay, for this product, that no other job owns.
        const since = new Date((cj.paymentStartedAt || cj.startedAt || new Date()).getTime() - DISCOVER_SLACK_MS);
        const pid = pidOf(cj.productUrl);
        const walk = await walkOrderList(ctx, since, { maxPages: 3 });
        const recent = walk.units.filter(
          (u) => u.orderDate && u.orderDate.getTime() >= since.getTime() && (!pid || JSON.stringify(u.unit).includes(pid))
        );
        const claimed = new Set(
          (
            await CheckoutJob.find({ "result.flipkartOrderId": { $in: recent.map((u) => u.orderId) }, _id: { $ne: cj._id } })
              .select("result.flipkartOrderId")
              .lean()
          ).map((r) => String(r.result?.flipkartOrderId || ""))
        );
        const mine = recent.filter((u) => !claimed.has(u.orderId));
        for (const u of mine) {
          if (saved.has(u.unitId)) continue;
          await syncOrder({ orderId: u.orderId, itemId: u.itemId, unitId: u.unitId, orderUrl: u.orderUrl, amount: u.amount });
        }
        if (orderIds.size === 1) {
          const found = [...orderIds][0];
          await CheckoutJob.updateOne(
            { _id: checkoutJobId, "result.flipkartOrderId": { $in: [null, ""] } },
            {
              $set: { "result.flipkartOrderId": found },
              $push: {
                logs: {
                  at: new Date(),
                  level: "warn",
                  step: "order-sync",
                  message: `Order sync found ${found} on Flipkart for this attempt (placed after Pay, not confirmed to us) — status on Flipkart is in Order Units`,
                },
              },
            }
          );
        } else if (orderIds.size > 1) {
          console.warn(`${tag} ${orderIds.size} candidate orders after Pay (${[...orderIds].join(", ")}) — saved all, linked none`);
        }
      }

      await CheckoutJob.updateOne(
        { _id: checkoutJobId },
        {
          $set: {
            "orderSync.status": "done",
            "orderSync.lastError": "",
            "orderSync.orderIds": [...orderIds],
            "orderSync.units": saved.size,
            "orderSync.syncedAt": new Date(),
            ...(listingPending ? { "result.listed": true, "result.listedStatus": listedStatus || "listed", "result.listingCheckedAt": new Date() } : {}),
          },
          ...(listingPending
            ? { $push: { logs: { at: new Date(), level: "info", step: "order-sync", message: `My Orders now lists ${cj.result?.flipkartOrderId} (${listedStatus || "listed"}) — ${Math.round((Date.now() - new Date(cj.paymentStartedAt || cj.startedAt || Date.now()).getTime()) / 60000)} min after Pay` } } }
            : {}),
        }
      );
      console.log(`${tag} synced ${saved.size} unit(s)${orderIds.size ? ` of ${[...orderIds].join(", ")}` : " — no order on Flipkart for this attempt"}${listingPending ? " · listing confirmed" : ""}`);
    } finally {
      await ctx.dispose().catch(() => undefined);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message.split("\n")[0].slice(0, 400) : String(err);
    const final = attempt >= (job.opts.attempts || 1);
    await CheckoutJob.updateOne(
      { _id: checkoutJobId },
      { $set: { "orderSync.status": final ? "failed" : "pending", "orderSync.lastError": message } }
    );
    if (final && listingPending) {
      // Hours of asking and My Orders still has nothing: now it counts as dropped.
      // The units come off the batch here, not at placement time.
      const hours = Math.round((Date.now() - new Date(cj.paymentStartedAt || cj.startedAt || Date.now()).getTime()) / 36e5);
      await CheckoutJob.updateOne(
        { _id: checkoutJobId, status: "paid" },
        {
          $set: {
            status: "needs_reconciliation",
            step: "needs_reconciliation",
            error: `Flipkart answered SUCCESS with ${cj.result?.flipkartOrderId} but My Orders still does not list it after ${hours} h (${attempt} checks) — order dropped by Flipkart; not counted as purchased`,
            errorCode: "ORDER_NOT_LISTED",
            errorCodeDisplay: "Order not listed by Flipkart",
            "result.listedStatus": "not_listed",
            "result.listingCheckedAt": new Date(),
          },
          $push: { logs: { at: new Date(), level: "error", step: "order-sync", message: `Not in My Orders after ${hours} h and ${attempt} checks — parked as needs_reconciliation, ${cj.quantity || 1} unit(s) returned to the batch` } },
        }
      );
      if (cj.batchId) await releaseBatchReservation(cj.batchId, cj.quantity || 1).catch(() => undefined);
      console.warn(`${tag} listing never appeared after ${attempt} checks — parked as needs_reconciliation`);
    }
    console.warn(`${tag} attempt ${attempt} failed${final ? " (giving up)" : ", will retry"}: ${message}`);
    throw err;
  } finally {
    await unlock();
  }
}

async function markFailed(checkoutJobId: string, reason: string) {
  await CheckoutJob.updateOne({ _id: checkoutJobId }, { $set: { "orderSync.status": "failed", "orderSync.lastError": reason } });
  console.warn(`[order-sync ${checkoutJobId}] ${reason}`);
}

/**
 * The outbox sweep. Finished jobs whose sync never got going — the enqueue
 * failed, Redis lost the message, the worker died mid-job — are queued again.
 * Jobs a sync gave up on stay "failed" until someone asks (POST
 * /api/jobs/:id/order-sync) — re-trying a dead session every ten minutes would
 * only hammer it.
 */
export async function sweepOrderSyncs(): Promise<number> {
  const now = Date.now();
  const rows = await CheckoutJob.find({
    status: { $in: TERMINAL },
    // Only this deployment's jobs (rows from before systemId existed count as ours).
    systemId: { $in: [config.systemId, "", null] },
    completedAt: { $lt: new Date(now - SWEEP_GRACE_MS), $gt: new Date(now - SWEEP_MAX_AGE_MS) },
    $or: [{ orderSync: { $exists: false } }, { orderSync: null }, { "orderSync.status": "pending" }],
  })
    .select("_id")
    .limit(500)
    .lean();
  const queue = getSyncQueue();
  let queued = 0;
  for (const r of rows) {
    const id = String(r._id);
    // A pending sync that is genuinely waiting (delayed for a retry or the
    // lock) is still in the queue; leave it alone.
    const existing = await queue.getJob(`sync-${id}`);
    if (existing && !(await existing.isFailed())) continue;
    if (existing) await existing.remove().catch(() => undefined);
    await enqueueOrderSync(id);
    queued += 1;
  }
  if (queued) console.log(`[order-sync] sweep re-queued ${queued} job(s)`);
  return queued;
}

/** Manual re-run for one job, from the API. */
export async function resyncCheckoutJob(checkoutJobId: string) {
  const existing = await getSyncQueue().getJob(`sync-${checkoutJobId}`);
  if (existing && (await existing.isFailed())) await existing.remove().catch(() => undefined);
  await CheckoutJob.updateOne({ _id: checkoutJobId }, { $unset: { orderSync: "" } });
  await enqueueOrderSync(checkoutJobId);
}

export { SYNC_QUEUE };
