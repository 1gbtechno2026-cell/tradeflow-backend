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
        // Dropped the moment the job settles, rather than retained for 7/14
        // days, because the payload now carries unmasked card data (see
        // CheckoutJobData.cards). Retention cost nothing before and costs a lot
        // now: a failed job used to sit in Redis for two weeks holding a PAN and
        // CVV. Nothing is lost — every status, log line, failure code and result
        // this UI reads lives on the CheckoutJob document in Mongo, not here.
        // Revisit only once cards are a reference instead of the secret itself.
        removeOnComplete: true,
        removeOnFail: true,
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
