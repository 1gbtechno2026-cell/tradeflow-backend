import { connectDb } from "./db.js";
import { config } from "./config.js";
import { createCheckoutWorker, LEGACY_QUEUE, queueNameFor } from "./queue.js";
import { isJobClass, JOB_CLASSES, type JobClass } from "./services/jobClass.js";
import { runCheckoutJob } from "./services/checkoutRunner.js";
import { claimProxyForWorker } from "./services/proxyPool.js";

/**
 * Which classes this process serves, and how many jobs of each at once.
 *
 *   WORKER_CLASSES=otp-phone,card,cod        default: all three
 *   WORKER_CONCURRENCY=2                      default per class
 *   WORKER_CONCURRENCY_OTP_PHONE=1            per-class override
 *
 * Every job is two Chromes (~0.5 GB), so per-class concurrency is a RAM
 * number for this machine. The CLASS caps — phones online, the 200/300
 * policy — are enforced where the job is admitted, not by counting processes.
 */
function classesFromEnv(): JobClass[] {
  const raw = String(process.env.WORKER_CLASSES || "").trim();
  if (!raw) return [...JOB_CLASSES];
  const picked = raw
    .split(",")
    .map((s) => s.trim())
    .filter(isJobClass);
  return picked.length ? picked : [...JOB_CLASSES];
}

function concurrencyFor(cls: JobClass): number {
  const key = `WORKER_CONCURRENCY_${cls.toUpperCase().replace(/-/g, "_")}`;
  const n = Number(process.env[key]);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : config.workerConcurrency;
}

async function main() {
  await connectDb();

  // Egress first. In `required` mode a worker with nothing to leave from must
  // not take a single job — it would place orders from this machine's own
  // address. The binding is re-read at every job start (claimProxyForWorker),
  // so this is the gate, not the only read.
  if (config.proxyMode !== "off") {
    const egress = await claimProxyForWorker(config.workerId);
    if (egress) {
      console.log(`[proxy] worker ${config.workerId} bound to ${egress.label} (mode ${config.proxyMode})`);
    } else if (config.proxyMode === "required") {
      console.error(
        `[proxy] worker ${config.workerId}: no free proxy in the pool and PROXY_MODE=required — ` +
          `add proxies in the dashboard's Proxy Pool tab (or assign one to "${config.workerId}") and start again`
      );
      process.exit(3);
    } else {
      console.warn(`[proxy] worker ${config.workerId}: no free proxy — running on the local connection (PROXY_MODE=preferred)`);
    }
  }

  const classes = classesFromEnv();
  const names = [...classes.map((c) => [queueNameFor(c), concurrencyFor(c)] as const), [LEGACY_QUEUE, 1] as const];
  for (const [name, concurrency] of names) {
    const worker = createCheckoutWorker(name, concurrency, async (data) => {
      console.log(`[worker] PICKED ${name} job=${data.jobId} email=${data.email}`);
      await runCheckoutJob(data);
    });
    worker.on("failed", (job, err) => {
      console.error(`[worker] FAILED ${name} job=${job?.id}:`, err.message);
    });
    worker.on("completed", (job) => {
      console.log(`[worker] DONE ${name} job=${job?.id}`);
    });
    worker.on("error", (err) => {
      console.error(`[worker] Redis/worker error (${name}):`, err.message);
    });
  }
  console.log(
    `[worker] listening on ${names.map(([n, c]) => `${n}×${c}`).join(", ")} — waiting for jobs (lock=10m)`
  );
}

main().catch((err) => {
  console.error("Failed to start worker:", err);
  process.exit(1);
});
