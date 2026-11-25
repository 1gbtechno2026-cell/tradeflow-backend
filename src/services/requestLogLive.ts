import type { Queue } from "bullmq";
import { Types } from "mongoose";
import { config } from "../config.js";
import { CancelRequest } from "../models/CancelRequest.js";
import { OrderCouponRequest, OrderCouponTask } from "../models/OrderCouponRequest.js";
import { OrderInvoiceRequest } from "../models/OrderInvoiceRequest.js";
import { Order } from "../models/Order.js";
import { OrderJobRun } from "../models/OrderJobRun.js";
import { OrderUpdateRequest } from "../models/OrderUpdateRequest.js";
import { RescheduleRequest } from "../models/RescheduleRequest.js";
import { CANCEL_QUEUE, COUPON_QUEUE, getCancelQueue, getCouponQueue, getInvoiceBatchQueue, getInvoiceQueue, getRescheduleQueue, getUpdateQueue, INVOICE_BATCH_QUEUE, INVOICE_QUEUE, RESCHEDULE_QUEUE, UPDATE_QUEUE } from "../queue.js";
import { getOrderFetchJob, isOrderFetchPreparing } from "./orderFetch.js";
import { emptyCounts, hostName, TIMED_OUT, workerAlive, writeRequestRun } from "./requestCommon.js";
import type { LiveProbes } from "./syncBatches.js";
import type { RequestFacts } from "./syncTaskLog.js";

/**
 * What the request log asks the running system, and what it does there after
 * closing a request nothing is behind (services/requestLogKeeper.ts).
 */

function queueOf(name: string): Queue | null {
  if (name === UPDATE_QUEUE) return getUpdateQueue() as unknown as Queue;
  if (name === CANCEL_QUEUE) return getCancelQueue() as unknown as Queue;
  if (name === RESCHEDULE_QUEUE) return getRescheduleQueue() as unknown as Queue;
  if (name === INVOICE_BATCH_QUEUE) return getInvoiceBatchQueue() as unknown as Queue;
  if (name === COUPON_QUEUE) return getCouponQueue() as unknown as Queue;
  // A sweep run recorded as a request (services/invoiceSweepRequest.ts) names the sweep's own queue.
  if (name === INVOICE_QUEUE) return getInvoiceQueue() as unknown as Queue;
  // Any other name is a check's throwaway queue: this API has no business on it.
  return null;
}

/**
 * Is a worker connected to this queue? BullMQ's own list of the queue's
 * workers — which a worker of any version is on — and then the heartbeat key
 * a worker of this version keeps on every queue it serves (75 s; it outlives a
 * restart, when BullMQ's list is empty for the seconds the new worker takes to
 * start). Redis not answering counts as yes.
 */
async function queueHasWorker(name: string): Promise<boolean> {
  const queue = queueOf(name);
  if (!queue) return true;
  try {
    if ((await queue.getWorkersCount()) > 0) return true;
  } catch {
    return true;
  }
  return workerAlive(name);
}

/** The job ids a request has on its queue: one per account (per account id, for an invoice batch). Read from the request's own document. */
async function jobIdsOf(f: RequestFacts): Promise<string[]> {
  const id = f.head.requestId;
  if (f.head.kind === "update") {
    const row = await OrderUpdateRequest.findById(id).select("accounts.email").lean();
    return (row?.accounts || []).map((a) => `${id}-${a.email}`);
  }
  if (f.head.kind === "cancel") {
    const row = await CancelRequest.findById(id).select("units.email orders.email").lean();
    return [...new Set([...(row?.units || []).map((u) => u.email), ...(row?.orders || []).map((o) => o.email)])].map((email) => `${id}-${email}`);
  }
  if (f.head.kind === "reschedule") {
    const row = await RescheduleRequest.findById(id).select("units.email").lean();
    return [...new Set((row?.units || []).map((u) => u.email))].map((email) => `${id}-${email}`);
  }
  if (f.head.kind === "invoices") {
    const row = await OrderInvoiceRequest.findById(id).select("tasks.accountId").lean();
    return [...new Set((row?.tasks || []).map((t) => t.accountId).filter(Boolean))].map((accountId) => `${id}-${accountId}`);
  }
  if (f.head.kind === "coupons") {
    const emails = await OrderCouponTask.distinct("email", { requestId: new Types.ObjectId(id) });
    return emails.map((email) => `${id}-${email}`);
  }
  return [];
}

/** A job in one of these states is still the queue's: a worker that comes back runs it (an active one a dead worker held is handed on). */
const WAITING_STATES = new Set(["waiting", "delayed", "active", "prioritized", "waiting-children"]);

/** Does a job of this request still sit on its queue? Unknown (Redis not answering) counts as yes: nothing is closed on a guess. */
async function jobsWaiting(f: RequestFacts): Promise<boolean> {
  const queue = queueOf(f.queue);
  if (!queue) return false;
  try {
    const ids = await jobIdsOf(f);
    for (let i = 0; i < ids.length; i += 50) {
      const states = await Promise.all(ids.slice(i, i + 50).map(async (jobId) => {
        const job = await queue.getJob(jobId);
        return job ? job.getState() : "missing";
      }));
      if (states.some((state) => WAITING_STATES.has(state))) return true;
    }
    return false;
  } catch {
    return true;
  }
}

export const liveProbes: LiveProbes = {
  queueHasWorker,
  fetchLive: (userId) => getOrderFetchJob(userId).running || isOrderFetchPreparing(userId),
  jobsWaiting,
  host: hostName(),
  pid: process.pid,
  systemId: config.systemId,
};

async function dropJobs(queueName: string, jobIds: string[]) {
  const queue = queueOf(queueName);
  if (!queue) return;
  for (let i = 0; i < jobIds.length; i += 50) await Promise.all(jobIds.slice(i, i + 50).map((id) => queue.remove(id).catch(() => 0)));
}

/**
 * A request has just been closed as timed out. Its jobs leave the queue (a
 * worker that comes back must not run a request that has been answered), the
 * Order rows a cancel or reschedule parked are freed so the units can be
 * submitted again — as their Force-clear does — and the ledger row of an
 * invoice or coupon batch is closed with it.
 */
export async function afterTimedOut(f: RequestFacts): Promise<void> {
  const id = f.head.requestId;
  const userId = f.head.userId;
  await dropJobs(f.queue, await jobIdsOf(f));
  if (f.head.kind === "cancel") {
    await Order.updateMany({ userId, cancel_request_id: id, cancel_status: { $in: ["queued", "running"] } }, { $set: { cancel_status: "", cancel_request_id: "", cancel_requested_at: null, cancel_requested_by: "", cancel_error: "" } });
  } else if (f.head.kind === "reschedule") {
    await Order.updateMany({ userId, reschedule_request_id: id, reschedule_status: { $in: ["queued", "running"] } }, { $set: { reschedule_status: "", reschedule_request_id: "", reschedule_date: "", reschedule_message: "" } });
  } else if (f.head.kind === "invoices") {
    const row = await OrderInvoiceRequest.findById(id).select("status counts startedAt finishedAt").lean();
    if (row) await writeRequestRun({ userId, type: "invoices", refId: id }, { status: row.status, counts: { ...emptyCounts(0), ...row.counts }, startedAt: row.startedAt, finishedAt: row.finishedAt, message: TIMED_OUT }, OrderJobRun, { final: true });
  } else if (f.head.kind === "coupons") {
    const row = await OrderCouponRequest.findById(id).select("status counts startedAt finishedAt").lean();
    if (row) await writeRequestRun({ userId, type: "coupons", refId: id }, { status: row.status, counts: { ...emptyCounts(0), ...row.counts }, startedAt: row.startedAt, finishedAt: row.finishedAt, message: TIMED_OUT }, OrderJobRun, { final: true });
  }
}
