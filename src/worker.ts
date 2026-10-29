import { connectDb } from "./db.js";
import { config } from "./config.js";
import { CANCEL_QUEUE, createCancelWorker, createCheckoutWorker, createRescheduleWorker, createSyncWorker, createUpdateWorker, getSyncQueue, HTTP_QUEUE, LEGACY_QUEUE, queueNameFor, RESCHEDULE_QUEUE, SYNC_QUEUE, UPDATE_QUEUE } from "./queue.js";
import { isJobClass, JOB_CLASSES, type JobClass } from "./services/jobClass.js";
import { runCheckoutJob } from "./services/checkoutRunner.js";
import { claimProxyForWorker } from "./services/proxyPool.js";
import { currentProxyMode } from "./services/proxyMode.js";
import { resolveCancelSpeed, runCancelJob } from "./services/orderCancel.js";
import { rescheduleConcurrency, runRescheduleJob } from "./services/orderReschedule.js";
import { runOrderSyncJob, sweepOrderSyncs } from "./services/orderSync.js";
import { ensureUpdateSchedules, resolveUpdateWindows, runOrderUpdateJob } from "./services/orderUpdateQueue.js";
import { describeError, isTransientError, throttledWarn } from "./lib/resilience.js";

/**
 * What this process does. One image, three roles, so each can be its own
 * deployment (AWS: the checkout workers run checkout,cancel; one small
 * tradeflow-sync service runs sync — no Chrome, HTTP only):
 *
 *   WORKER_ROLES=checkout,cancel,sync,update,reschedule   default: all five (local dev)
 *   SYNC_CONCURRENCY=4                          order syncs at once (default 4)
 *   SYNC_SWEEP_MS=600000                        outbox sweep period (default 10 min)
 *   UPDATE_CONCURRENCY=3                        Update Order Units: accounts at once
 *                                               (the dashboard's update windows win)
 *   UPDATE_SWEEP_MS / UPDATE_OFD_SWEEP_MS       every active unit (6 h) / Out for
 *                                               delivery (2 h); UPDATE_SCHEDULE=0 off
 *
 * Which classes this process serves, and how many jobs of each at once.
 *
 *   WORKER_CLASSES=otp-phone,card,cod        default: all three
 *   WORKER_CONCURRENCY=2                      default per class
 *   WORKER_CONCURRENCY_OTP_PHONE=1            per-class override
 *   CANCEL_CONCURRENCY=2                      order cancellations (one per account
 *                                             at a time); default WORKER_CONCURRENCY,
 *                                             0 = this process takes none
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

type Role = "checkout" | "cancel" | "sync" | "update" | "reschedule";

function rolesFromEnv(): Set<Role> {
  const raw = String(process.env.WORKER_ROLES || "").trim();
  const all: Role[] = ["checkout", "cancel", "sync", "update", "reschedule"];
  if (!raw) return new Set(all);
  const picked = raw.split(",").map((s) => s.trim()).filter((r): r is Role => (all as string[]).includes(r));
  return new Set(picked.length ? picked : all);
}

function concurrencyFor(cls: JobClass): number {
  const key = `WORKER_CONCURRENCY_${cls.toUpperCase().replace(/-/g, "_")}`;
  const n = Number(process.env[key]);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : config.workerConcurrency;
}

// A network / Atlas / Redis outage is not a bug: log it once a minute and keep
// running — the driver reconnects, queues resume. Real bugs still end the process.
process.on("unhandledRejection", (reason) => {
  if (isTransientError(reason)) {
    throttledWarn("guard:transient", `[guard] outage, process stays up: ${describeError(reason)}`);
    return;
  }
  console.error("[guard] UNHANDLED REJECTION — exiting:", reason);
  process.exit(1);
});
process.on("uncaughtException", (err) => {
  if (isTransientError(err)) {
    throttledWarn("guard:transient", `[guard] outage (sync), process stays up: ${describeError(err)}`);
    return;
  }
  console.error("[guard] UNCAUGHT EXCEPTION — exiting:", err);
  process.exit(1);
});

async function main() {
  await connectDb();

  // Egress. In worker-lease mode a process binds one proxy for its life and,
  // in `required` mode, must not take a job without one. In session-lease
  // mode (default) every job leases its own proxy, so nothing is bound here;
  // a job that finds the pool empty under `required` waits in the queue.
  const mode = await currentProxyMode();
  console.log(`[proxy] env ${config.envName} · mode ${mode} · lease per ${config.proxyLease}${config.proxyLease === "session" ? ` (rest ${Math.round(config.proxyRestMs / 1000)} s)` : ""}`);
  if (mode !== "off" && config.proxyLease === "worker") {
    const egress = await claimProxyForWorker(config.workerId);
    if (egress) console.log(`[proxy] worker ${config.workerId} bound to ${egress.label}`);
    else if (mode === "required") console.error(`[proxy] worker ${config.workerId}: no free proxy and the mode is required — jobs will wait until the Proxy Pool has a free active proxy`);
    else console.warn(`[proxy] worker ${config.workerId}: no free proxy — running on the local connection (preferred)`);
  }

  const roles = rolesFromEnv();
  const classes = classesFromEnv();
  // The HTTP engine's queue (High Speed COD): no Chrome per job, so tens at
  // once per process. HTTP_CHECKOUT_CONCURRENCY=0 means this process takes none.
  const httpEnv = Number(process.env.HTTP_CHECKOUT_CONCURRENCY);
  const httpConcurrency = Number.isFinite(httpEnv) && httpEnv >= 0 ? Math.floor(httpEnv) : 25;
  const names: Array<readonly [string, number]> = roles.has("checkout")
    ? [
        ...classes.map((c) => [queueNameFor(c), concurrencyFor(c)] as const),
        [LEGACY_QUEUE, 1] as const,
        ...(httpConcurrency > 0 && classes.includes("cod") ? [[HTTP_QUEUE, httpConcurrency] as const] : []),
      ]
    : [];
  for (const [name, concurrency] of [...names]) {
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
  // Order cancellations: one job per account, accounts in parallel up to the cap.
  const cancelEnv = Number(process.env.CANCEL_CONCURRENCY);
  const cancelConcurrency = Number.isFinite(cancelEnv) && cancelEnv >= 0 ? Math.floor(cancelEnv) : config.workerConcurrency;
  if (roles.has("cancel") && cancelConcurrency > 0) {
    const cancelWorker = createCancelWorker(cancelConcurrency, async (job, token) => {
      console.log(`[worker] PICKED ${CANCEL_QUEUE} request=${job.data.requestId} email=${job.data.email}`);
      await runCancelJob(job, token);
    });
    cancelWorker.on("failed", (job, err) => {
      if (err.name === "DelayedError") return;
      console.error(`[worker] FAILED ${CANCEL_QUEUE} job=${job?.id}:`, err.message);
    });
    cancelWorker.on("error", (err) => console.error(`[worker] Redis/worker error (${CANCEL_QUEUE}):`, err.message));
    // The dashboard's cancel speed (windows = accounts at once) applies to the
    // next job without a restart; tabs per window are read per job.
    let windows = cancelConcurrency;
    const follow = async () => {
      const speed = await resolveCancelSpeed(config.userId).catch(() => null);
      if (speed && speed.windows !== windows) {
        console.log(`[worker] cancel windows ${windows} → ${speed.windows} (${speed.source}${speed.changedBy ? `, ${speed.changedBy}` : ""})`);
        cancelWorker.concurrency = speed.windows;
        windows = speed.windows;
      }
    };
    void follow();
    setInterval(() => void follow(), 10_000).unref();
    names.push([CANCEL_QUEUE, cancelConcurrency]);
  }
  // Order reschedules: one job per account; RESCHEDULE_CONCURRENCY accounts at once (default 1).
  const rescheduleWindows = rescheduleConcurrency();
  if (roles.has("reschedule") && rescheduleWindows > 0) {
    const rescheduleWorker = createRescheduleWorker(rescheduleWindows, async (job, token) => {
      console.log(`[worker] PICKED ${RESCHEDULE_QUEUE} request=${job.data.requestId} email=${job.data.email}`);
      await runRescheduleJob(job, token);
    });
    rescheduleWorker.on("failed", (job, err) => {
      if (err.name === "DelayedError") return;
      console.error(`[worker] FAILED ${RESCHEDULE_QUEUE} job=${job?.id}:`, err.message);
    });
    rescheduleWorker.on("error", (err) => console.error(`[worker] Redis/worker error (${RESCHEDULE_QUEUE}):`, err.message));
    names.push([RESCHEDULE_QUEUE, rescheduleWindows]);
  }
  // Order sync: finished checkouts' orders into Order Units, plus the sweep.
  if (roles.has("sync")) {
    const syncEnv = Number(process.env.SYNC_CONCURRENCY);
    const syncConcurrency = Number.isFinite(syncEnv) && syncEnv > 0 ? Math.floor(syncEnv) : 4;
    const syncWorker = createSyncWorker(syncConcurrency, async (job, token) => {
      if (job.name === "sweep") {
        await sweepOrderSyncs();
        return;
      }
      await runOrderSyncJob(job, token);
    });
    syncWorker.on("failed", (job, err) => {
      if (err.name === "DelayedError") return;
      console.error(`[worker] FAILED ${SYNC_QUEUE} job=${job?.id} (attempt ${job?.attemptsMade}):`, err.message);
    });
    syncWorker.on("error", (err) => console.error(`[worker] Redis/worker error (${SYNC_QUEUE}):`, err.message));
    // One schedule in Redis however many sync workers run: upsert is idempotent.
    const sweepMs = Math.max(60_000, Number(process.env.SYNC_SWEEP_MS) || 10 * 60 * 1000);
    await getSyncQueue().upsertJobScheduler("order-sync-sweep", { every: sweepMs }, { name: "sweep", data: { checkoutJobId: "" }, opts: { attempts: 1 } });
    names.push([SYNC_QUEUE, syncConcurrency]);
  }

  // Update Order Units: one job per account, plus the two schedules.
  if (roles.has("update")) {
    const initial = await resolveUpdateWindows(config.userId).catch(() => null);
    let windows = initial?.windows || 3;
    const updateWorker = createUpdateWorker(windows, async (job, token) => {
      if (job.name !== "sweep") console.log(`[worker] PICKED ${UPDATE_QUEUE} request=${job.data.requestId} email=${job.data.email}`);
      await runOrderUpdateJob(job, token);
    });
    updateWorker.on("failed", (job, err) => {
      if (err.name === "DelayedError") return;
      console.error(`[worker] FAILED ${UPDATE_QUEUE} job=${job?.id}:`, err.message);
    });
    updateWorker.on("error", (err) => console.error(`[worker] Redis/worker error (${UPDATE_QUEUE}):`, err.message));
    const followUpdate = async () => {
      const speed = await resolveUpdateWindows(config.userId).catch(() => null);
      if (speed && speed.windows !== windows) {
        console.log(`[worker] update windows ${windows} → ${speed.windows} (${speed.source}${speed.changedBy ? `, ${speed.changedBy}` : ""})`);
        updateWorker.concurrency = speed.windows;
        windows = speed.windows;
      }
    };
    setInterval(() => void followUpdate(), 10_000).unref();
    await ensureUpdateSchedules();
    names.push([UPDATE_QUEUE, windows]);
  }

  if (!names.length) {
    console.error("[worker] WORKER_ROLES selected nothing to run");
    process.exit(2);
  }
  console.log(
    `[worker] roles ${[...roles].join(",")} — listening on ${names.map(([n, c]) => `${n}×${c}`).join(", ")} — waiting for jobs (lock=10m)`
  );
}

main().catch((err) => {
  console.error("Failed to start worker:", err);
  process.exit(1);
});
