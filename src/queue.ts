import { Queue, Worker, type Job, type JobsOptions } from "bullmq";
import { config } from "./config.js";
import { JOB_CLASSES, jobClassFor, type JobClass } from "./services/jobClass.js";
import type { CheckoutJobData } from "./types.js";

/**
 * One queue per concurrency class (see jobClass.ts), named `checkout-<class>`.
 * A worker process subscribes to the classes it is configured for, each with
 * its own concurrency, so a shortage of handsets for `otp-phone` never holds
 * up `card` or `cod`, and each class can be scaled to its own cap.
 *
 * The original single queue, `checkout`, is still drained (as the "legacy"
 * name) so jobs queued before this split are not stranded.
 */
export const LEGACY_QUEUE = "checkout";

/** BullMQ forbids ":" in a queue name (it is its own key separator). */
export function queueNameFor(cls: JobClass): string {
  return `checkout-${cls}`;
}

function redisConnection() {
  const u = new URL(config.redisUrl);
  return {
    host: u.hostname,
    port: Number(u.port || 6376),
    password: u.password || undefined,
    maxRetriesPerRequest: null as null,
  };
}

/** Read-only handle on any queue by name, for depth metrics (services/queueMetrics.ts). */
const metricQueues = new Map<string, Queue>();
export function metricsQueue(name: string): Queue {
  let q = metricQueues.get(name);
  if (!q) {
    q = new Queue(name, { connection: redisConnection() });
    metricQueues.set(name, q);
  }
  return q;
}

const queues = new Map<string, Queue<CheckoutJobData>>();

function queueByName(name: string): Queue<CheckoutJobData> {
  let q = queues.get(name);
  if (!q) {
    q = new Queue<CheckoutJobData>(name, {
      connection: redisConnection(),
      defaultJobOptions: {
        attempts: 1,
        // Dropped the moment the job settles: every status, log line, failure
        // code and result lives on the CheckoutJob document in Mongo, not here.
        removeOnComplete: true,
        removeOnFail: true,
      } satisfies JobsOptions,
    });
    queues.set(name, q);
  }
  return q;
}

export function getCheckoutQueue(cls: JobClass): Queue<CheckoutJobData> {
  return queueByName(queueNameFor(cls));
}

/**
 * The HTTP checkout engine's queue. COD jobs submitted as "High Speed" go here
 * instead of checkout-cod: no Chrome per job, so its concurrency is tens per
 * process (HTTP_CHECKOUT_CONCURRENCY), not the RAM-bound 1–8 of the Chrome
 * queues. A job that the engine hands back ("fallback") still runs Chrome in
 * the same process, capped by HTTP_FALLBACK_CHROME_MAX (checkoutRunner.ts).
 */
export const HTTP_QUEUE = "checkout-http";

export function getHttpCheckoutQueue(): Queue<CheckoutJobData> {
  return queueByName(HTTP_QUEUE);
}

/** Which queue a checkout payload belongs on — class queue, or the HTTP one. */
export function checkoutQueueFor(data: CheckoutJobData): Queue<CheckoutJobData> {
  return data.executionVersion === "high_speed" && jobClassFor(data) === "cod" ? getHttpCheckoutQueue() : getCheckoutQueue(jobClassFor(data));
}

/** Waiting + delayed + active counts per class — the gauges the Orders tab shows. */
export async function queueDepths(): Promise<Record<string, { waiting: number; delayed: number; active: number }>> {
  const out: Record<string, { waiting: number; delayed: number; active: number }> = {};
  for (const cls of JOB_CLASSES) {
    const q = getCheckoutQueue(cls);
    const [waiting, delayed, active] = await Promise.all([q.getWaitingCount(), q.getDelayedCount(), q.getActiveCount()]);
    out[cls] = { waiting, delayed, active };
  }
  {
    const q = getHttpCheckoutQueue();
    const [waiting, delayed, active] = await Promise.all([q.getWaitingCount(), q.getDelayedCount(), q.getActiveCount()]);
    out.http = { waiting, delayed, active };
  }
  return out;
}

export function createCheckoutWorker(
  queueName: string,
  concurrency: number,
  processor: (data: CheckoutJobData) => Promise<void>
) {
  return new Worker<CheckoutJobData>(
    queueName,
    async (job) => {
      await processor(job.data);
    },
    {
      connection: redisConnection(),
      concurrency: Math.max(1, concurrency),
      lockDuration: 10 * 60 * 1000,
      stalledInterval: 60 * 1000,
      maxStalledCount: 2,
    }
  );
}

// ── Order cancellation ─────────────────────────────────────────────────────
// One job per (cancel request, account): every unit the request holds for that
// account is cancelled in one browser, one after another. Different accounts are
// separate jobs, so they run in parallel up to the worker's cancel concurrency.

export const CANCEL_QUEUE = "order-cancel";

export interface CancelJobData {
  requestId: string;
  email: string;
}

let cancelQueue: Queue<CancelJobData> | null = null;

export function getCancelQueue(): Queue<CancelJobData> {
  if (!cancelQueue) {
    cancelQueue = new Queue<CancelJobData>(CANCEL_QUEUE, {
      connection: redisConnection(),
      defaultJobOptions: { attempts: 1, removeOnComplete: true, removeOnFail: true } satisfies JobsOptions,
    });
  }
  return cancelQueue;
}

export function createCancelWorker(
  concurrency: number,
  processor: (job: Job<CancelJobData>, token: string | undefined) => Promise<void>
) {
  return new Worker<CancelJobData>(CANCEL_QUEUE, processor, {
    connection: redisConnection(),
    concurrency: Math.max(1, concurrency),
    lockDuration: 10 * 60 * 1000,
    stalledInterval: 60 * 1000,
    maxStalledCount: 1,
  });
}

// ── Order reschedule ───────────────────────────────────────────────────────
// One job per (reschedule request, account): every unit the request holds for
// that account has its "Change Date" driven in one browser (services/orderReschedule.ts).

export const RESCHEDULE_QUEUE = "order-reschedule";

export interface RescheduleJobData {
  requestId: string;
  email: string;
}

let rescheduleQueue: Queue<RescheduleJobData> | null = null;

export function getRescheduleQueue(): Queue<RescheduleJobData> {
  if (!rescheduleQueue) {
    rescheduleQueue = new Queue<RescheduleJobData>(RESCHEDULE_QUEUE, {
      connection: redisConnection(),
      defaultJobOptions: { attempts: 1, removeOnComplete: true, removeOnFail: true } satisfies JobsOptions,
    });
  }
  return rescheduleQueue;
}

export function createRescheduleWorker(concurrency: number, processor: (job: Job<RescheduleJobData>, token: string | undefined) => Promise<void>) {
  return new Worker<RescheduleJobData>(RESCHEDULE_QUEUE, processor, {
    connection: redisConnection(),
    concurrency: Math.max(1, concurrency),
    lockDuration: 10 * 60 * 1000,
    stalledInterval: 60 * 1000,
    maxStalledCount: 1,
  });
}

// ── Order sync ─────────────────────────────────────────────────────────────
// After a checkout ends, its order goes into Order Units without anyone running
// a fetch (services/orderSync.ts). One job per checkout job, keyed by its id, so
// a second enqueue of the same attempt is a no-op. Retries back off from 30s;
// the sweep (every SYNC_SWEEP_MS) re-queues anything the queue lost.

export const SYNC_QUEUE = "order-sync";

export interface SyncJobData {
  checkoutJobId: string;
}

let syncQueue: Queue<SyncJobData> | null = null;

export function getSyncQueue(): Queue<SyncJobData> {
  if (!syncQueue) {
    syncQueue = new Queue<SyncJobData>(SYNC_QUEUE, {
      connection: redisConnection(),
      defaultJobOptions: {
        attempts: 6,
        backoff: { type: "exponential", delay: 30_000 },
        removeOnComplete: true,
        // Kept a week for inspection; the outcome itself is on the CheckoutJob.
        removeOnFail: { age: 7 * 24 * 3600 },
      } satisfies JobsOptions,
    });
  }
  return syncQueue;
}

export function createSyncWorker(
  concurrency: number,
  processor: (job: Job<SyncJobData>, token: string | undefined) => Promise<void>
) {
  return new Worker<SyncJobData>(SYNC_QUEUE, processor, {
    connection: redisConnection(),
    concurrency: Math.max(1, concurrency),
    lockDuration: 2 * 60 * 1000,
    stalledInterval: 60 * 1000,
    maxStalledCount: 2,
  });
}

// ── Order update ───────────────────────────────────────────────────────────
// "Update Order Units" (services/orderUpdateQueue.ts): one job per (update
// request, account), accounts in parallel up to the worker's update windows.
// The two schedules — every 6 h for all active units, every 2 h for units Out
// for delivery — are BullMQ job schedulers on this same queue (name "sweep").

export const UPDATE_QUEUE = "order-update";

export interface UpdateJobData {
  requestId: string;
  email: string;
  /** Scheduler ticks: "active" (6 h, every active unit) or "ofd" (2 h, Out for delivery). */
  sweep?: "active" | "ofd";
}

let updateQueue: Queue<UpdateJobData> | null = null;

export function getUpdateQueue(): Queue<UpdateJobData> {
  if (!updateQueue) {
    updateQueue = new Queue<UpdateJobData>(UPDATE_QUEUE, {
      connection: redisConnection(),
      defaultJobOptions: { attempts: 1, removeOnComplete: true, removeOnFail: true } satisfies JobsOptions,
    });
  }
  return updateQueue;
}

export function createUpdateWorker(
  concurrency: number,
  processor: (job: Job<UpdateJobData>, token: string | undefined) => Promise<void>
) {
  return new Worker<UpdateJobData>(UPDATE_QUEUE, processor, {
    connection: redisConnection(),
    concurrency: Math.max(1, concurrency),
    lockDuration: 10 * 60 * 1000,
    stalledInterval: 60 * 1000,
    maxStalledCount: 1,
  });
}
