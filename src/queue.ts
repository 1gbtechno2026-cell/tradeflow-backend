import { Queue, Worker, type JobsOptions } from "bullmq";
import { config } from "./config.js";
import { JOB_CLASSES, type JobClass } from "./services/jobClass.js";
import type { CheckoutJobData } from "./types.js";

/**
 * One queue per concurrency class (see jobClass.ts), named `checkout:<class>`.
 * A worker process subscribes to the classes it is configured for, each with
 * its own concurrency, so a shortage of handsets for `otp-phone` never holds
 * up `card` or `cod`, and each class can be scaled to its own cap.
 *
 * The original single queue, `checkout`, is still drained (as the "legacy"
 * name) so jobs queued before this split are not stranded.
 */
export const LEGACY_QUEUE = "checkout";

export function queueNameFor(cls: JobClass): string {
  return `checkout:${cls}`;
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
