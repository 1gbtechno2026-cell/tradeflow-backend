import { connectDb } from "./db.js";
import { config } from "./config.js";
import { createCheckoutWorker } from "./queue.js";
import { runCheckoutJob } from "./services/checkoutRunner.js";

async function main() {
  await connectDb();
  const worker = createCheckoutWorker(async (data) => {
    console.log(`[worker] PICKED job=${data.jobId} email=${data.email}`);
    await runCheckoutJob(data);
  });
  worker.on("failed", (job, err) => {
    console.error(`[worker] FAILED job=${job?.id}:`, err.message);
  });
  worker.on("completed", (job) => {
    console.log(`[worker] DONE job=${job?.id}`);
  });
  worker.on("error", (err) => {
    console.error("[worker] Redis/worker error:", err.message);
  });
  console.log(
    `[worker] listening on Redis queue 'checkout' — waiting for jobs (concurrency=${config.workerConcurrency}, lock=10m)`
  );
}

main().catch((err) => {
  console.error("Failed to start worker:", err);
  process.exit(1);
});
