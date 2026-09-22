import { Queue, Worker, type JobsOptions } from "bullmq";
import { config } from "./config.js";
import type { CheckoutJobData } from "./types.js";

export const CHECKOUT_QUEUE = "checkout";

function redisConnection() {
  const u = new URL(config.redisUrl);
  return {
    host: u.hostname,
    port: Number(u.port || 6376),
    password: u.password || undefined,
    maxRetriesPerRequest: null as null,
  };
}

let queue: Queue<CheckoutJobData> | null = null;

export function getCheckoutQueue() {
  if (!queue) {
    queue = new Queue<CheckoutJobData>(CHECKOUT_QUEUE, {
      connection: redisConnection(),
      defaultJobOptions: {
        attempts: 1,
        removeOnComplete: { age: 60 * 60 * 24 * 7, count: 1000 },
        removeOnFail: { age: 60 * 60 * 24 * 14 },
      } satisfies JobsOptions,
    });
  }
  return queue;
}

export function createCheckoutWorker(
  processor: (data: CheckoutJobData) => Promise<void>
) {
  return new Worker<CheckoutJobData>(
    CHECKOUT_QUEUE,
    async (job) => {
      await processor(job.data);
    },
    {
      connection: redisConnection(),
      concurrency: config.workerConcurrency,
      lockDuration: 10 * 60 * 1000,
      stalledInterval: 60 * 1000,
      maxStalledCount: 2,
    }
  );
}
