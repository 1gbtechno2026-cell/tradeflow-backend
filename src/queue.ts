import { Queue, Worker, type Job, type JobsOptions } from "bullmq";
import { config } from "./config.js";
import { JOB_CLASSES, type JobClass } from "./services/jobClass.js";
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

/** Waiting + delayed + active counts per class — the gauges the Orders tab shows. */
export async function queueDepths(): Promise<Record<string, { waiting: number; delayed: number; active: number }>> {
  const out: Record<string, { waiting: number; delayed: number; active: number }> = {};
  for (const cls of JOB_CLASSES) {
    const q = getCheckoutQueue(cls);
    const [waiting, delayed, active] = await Promise.all([q.getWaitingCount(), q.getDelayedCount(), q.getActiveCount()]);
    out[cls] = { waiting, delayed, active };
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
