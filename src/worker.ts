import { connectDb } from "./db.js";
import { config } from "./config.js";
import { CANCEL_QUEUE, COUPON_QUEUE, createCancelWorker, createCheckoutWorker, createCouponWorker, createInvoiceBatchWorker, createInvoiceWorker, createRescheduleWorker, createSyncWorker, createUpdateWorker, getSyncQueue, HTTP_QUEUE, INVOICE_BATCH_QUEUE, INVOICE_QUEUE, LEGACY_QUEUE, queueNameFor, RESCHEDULE_QUEUE, SYNC_QUEUE, UPDATE_QUEUE } from "./queue.js";
import { isJobClass, JOB_CLASSES, type JobClass } from "./services/jobClass.js";
import { runCheckoutJob } from "./services/checkoutRunner.js";
import { claimProxyForWorker } from "./services/proxyPool.js";
import { currentProxyMode } from "./services/proxyMode.js";
import { resolveCancelSpeed, runCancelJob, startCancelReaper } from "./services/orderCancel.js";
import { rescheduleConcurrency, runRescheduleJob, startRescheduleReaper } from "./services/orderReschedule.js";
import { runOrderSyncJob, sweepOrderSyncs } from "./services/orderSync.js";
import { resolveUpdateWindows, retireUpdateSchedulers, runOrderUpdateJob, startUpdateReaper, updateSweepRunner } from "./services/orderUpdateQueue.js";
import { invoiceSweepRunner, runSweepJob, type SweepJobData } from "./services/invoiceSweep.js";
import { closeInterruptedSweepRequests } from "./services/invoiceSweepRequest.js";
import { CHAIN_AFTER_ORDER, CHAIN_DELIVERED_CHECK, CHAIN_INVOICE_SWEEP } from "./services/requestChains.js";
import { INVOICE_BATCH_CONCURRENCY, runInvoiceBatchJob, startInvoiceRequestReaper } from "./services/invoiceRequests.js";
import { COUPON_CONCURRENCY, runCouponJob, startCouponRequestReaper } from "./services/couponRequests.js";
import { startWorkerHeartbeat } from "./services/requestCommon.js";
import { startScheduleClock, type ScheduleClock } from "./services/scheduleClock.js";
import { eligibleHere, retireLegacySchedulers, seedSchedules, seedSchedulesAtStart, servedBy } from "./services/scheduleSettings.js";
import { describeError, isTransientError, throttledWarn } from "./lib/resilience.js";

/**
 * What this process does. One image, three roles, so each can be its own
 * deployment (AWS: the checkout workers run checkout,cancel; one small
 * tradeflow-sync service runs sync — no Chrome, HTTP only):
 *
 *   WORKER_ROLES=checkout,cancel,sync,update,reschedule,invoice,invoice-batch,coupon
 *                                               default: all of them (local dev)
 *   INVOICE_BATCH_CONCURRENCY=12                "Download Order Invoices" batches: accounts at once
 *   COUPON_CONCURRENCY=4                        "Apply Company Coupons": accounts at once
 *   SYNC_CONCURRENCY=4                          order syncs at once (default 4)
 *   SYNC_SWEEP_MS=600000                        outbox sweep period (default 10 min)
 *   UPDATE_CONCURRENCY=3                        Update Order Units: accounts at once
 *                                               (the dashboard's update windows win)
 *
 * The three sweeps — every active unit (6 h), out for delivery (2 h), invoices
 * (3 h) — run on the schedule clock every machine shares, not on a scheduler
 * in this machine's Redis (services/scheduleClock.ts). A worker with the
 * `update` role serves the two update clocks, one with `invoice` the invoice
 * clock. Whether a sweep runs is its switch in Settings → Scheduled fetch;
 * this machine's .env only says whether it MAY be the one that runs it:
 *
 *   UPDATE_SCHEDULE=0     this machine never runs the two update sweeps
 *   INVOICE_SCHEDULE=0    this machine never runs the invoice sweep
 *   (UPDATE_SWEEP_MS, UPDATE_OFD_SWEEP_MS, INVOICE_SWEEP_MS, CRON_TIMEZONE were
 *    read once, by the first seed of the clocks; changing them now does nothing)
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

type Role = "checkout" | "cancel" | "sync" | "update" | "reschedule" | "invoice" | "invoice-batch" | "coupon";

function rolesFromEnv(): Set<Role> {
  const raw = String(process.env.WORKER_ROLES || "").trim();
  const all: Role[] = ["checkout", "cancel", "sync", "update", "reschedule", "invoice", "invoice-batch", "coupon"];
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
  // The schedules' switches and clocks: written once per workspace, by whichever of the API and the worker starts
  // first; nothing that exists is touched (services/scheduleSettings.ts).
  await seedSchedulesAtStart(config.userId);

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
    // Re-enqueue cancel requests whose jobs were lost, so none sits stuck at 0/N.
    startCancelReaper();
    // The API's request log reads this key beside BullMQ's list of workers: it outlives a restart (services/requestLogLive.ts).
    startWorkerHeartbeat(CANCEL_QUEUE);
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
    startRescheduleReaper();
    startWorkerHeartbeat(RESCHEDULE_QUEUE);
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

  // The shared schedule clock of this process, started once every role is set up (below). A leftover job of an old
  // per-machine scheduler only makes it look: the clock runs a fixed time when one is due and free, never twice.
  let scheduleClock: ScheduleClock | null = null;
  const clockLook = async () => {
    await scheduleClock?.pollOnce();
  };

  // Update Order Units: one job per account. Its two sweeps run on the shared clock.
  if (roles.has("update")) {
    // The BullMQ schedulers this queue had before the shared clock leave this machine's Redis, with their next jobs —
    // before the worker below can take one.
    await retireUpdateSchedulers();
    const initial = await resolveUpdateWindows(config.userId).catch(() => null);
    let windows = initial?.windows || 3;
    const updateWorker = createUpdateWorker(windows, async (job, token) => {
      if (job.name !== "sweep") console.log(`[worker] PICKED ${UPDATE_QUEUE} request=${job.data.requestId} email=${job.data.email}`);
      await runOrderUpdateJob(job, token, { clockLook });
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
    startUpdateReaper();
    startWorkerHeartbeat(UPDATE_QUEUE);
    names.push([UPDATE_QUEUE, windows]);
  }

  // Invoice capture: the time-settled sweep (services/invoiceSweep.ts). One job
  // at a time (the sweep pools accounts itself); the shared clock queues the
  // scheduled ones, the "Invoices" button the manual ones.
  if (roles.has("invoice")) {
    // The old per-machine scheduler of the sweep leaves this machine's Redis, with its next job.
    await retireLegacySchedulers(["invoice_sweep"]);
    const invoiceWorker = createInvoiceWorker(1, async (job) => {
      if (job.name === "sweep") {
        const out = await runSweepJob(config.userId, job.data as SweepJobData, { clockLook });
        if (out.skipped) console.log(`[worker] ${INVOICE_QUEUE} sweep skipped — ${out.reason}`);
      }
    });
    invoiceWorker.on("failed", (job, err) => {
      if (err.name !== "DelayedError") console.error(`[worker] FAILED ${INVOICE_QUEUE} job=${job?.id}:`, err.message);
    });
    invoiceWorker.on("error", (err) => console.error(`[worker] Redis/worker error (${INVOICE_QUEUE}):`, err.message));
    // Each sweep run is also a request of the log (services/invoiceSweepRequest.ts). The heartbeat tells the API a
    // worker is behind an open one (its Cancel then lets the order in flight finish); one an earlier worker of this
    // machine left open is closed now — a sweep cannot be resumed.
    startWorkerHeartbeat(INVOICE_QUEUE);
    await closeInterruptedSweepRequests().catch((err) => console.warn("[worker] could not close interrupted sweep requests:", err instanceof Error ? err.message : err));
    names.push([INVOICE_QUEUE, 1]);
  }

  // Invoice batches ("Download Order Invoices" by order id): one job per account of a request,
  // on a queue of their own (see queue.ts). The heartbeat tells the API a worker that knows
  // this queue is up; without it the API fails a waiting request with "restart the worker".
  if (roles.has("invoice-batch")) {
    const batchWorker = createInvoiceBatchWorker(INVOICE_BATCH_CONCURRENCY, async (job) => {
      console.log(`[worker] PICKED ${INVOICE_BATCH_QUEUE} request=${job.data.requestId} account=${job.data.accountId}`);
      await runInvoiceBatchJob(job);
    });
    batchWorker.on("failed", (job, err) => {
      if (err.name !== "DelayedError") console.error(`[worker] FAILED ${INVOICE_BATCH_QUEUE} job=${job?.id}:`, err.message);
    });
    batchWorker.on("error", (err) => console.error(`[worker] Redis/worker error (${INVOICE_BATCH_QUEUE}):`, err.message));
    startWorkerHeartbeat(INVOICE_BATCH_QUEUE);
    // Requests a dead worker left half-done get their account jobs back; finished orders are not redone.
    startInvoiceRequestReaper();
    names.push([INVOICE_BATCH_QUEUE, INVOICE_BATCH_CONCURRENCY]);
  }

  // Company coupons: one job per account of a request; each coupon link opened with the account's session over HTTP.
  if (roles.has("coupon")) {
    const couponWorker = createCouponWorker(COUPON_CONCURRENCY, async (job, token) => {
      console.log(`[worker] PICKED ${COUPON_QUEUE} request=${job.data.requestId} email=${job.data.email}`);
      await runCouponJob(job, token);
    });
    couponWorker.on("failed", (job, err) => {
      if (err.name !== "DelayedError") console.error(`[worker] FAILED ${COUPON_QUEUE} job=${job?.id}:`, err.message);
    });
    couponWorker.on("error", (err) => console.error(`[worker] Redis/worker error (${COUPON_QUEUE}):`, err.message));
    startWorkerHeartbeat(COUPON_QUEUE);
    startCouponRequestReaper();
    names.push([COUPON_QUEUE, COUPON_CONCURRENCY]);
  }

  if (!names.length) {
    console.error("[worker] WORKER_ROLES selected nothing to run");
    process.exit(2);
  }

  // The sweeps, on the schedule clock every machine shares (services/scheduleClock.ts). This process serves the
  // clocks of its roles — it writes its presence, records "switched off" and "missed", renews the leases of this
  // machine — and RUNS a fixed time only for the schedules this machine's .env lets it (eligibleHere), when the
  // switch in Settings is on and no other machine has the time. The first time it serves a clock it starts with the
  // next fixed time: nothing sweeps because a worker was restarted.
  const serves = servedBy("worker", roles);
  if (serves.length && config.userId) {
    try {
      scheduleClock = startScheduleClock({
        role: "worker",
        userId: config.userId,
        serves,
        names: serves.filter((n) => eligibleHere(n)),
        runners: { update_active: updateSweepRunner("active"), update_ofd: updateSweepRunner("ofd"), invoice_sweep: invoiceSweepRunner() },
        seed: () => seedSchedules(config.userId),
      });
    } catch (err) {
      console.warn(`[worker] the schedule clock could not start — no sweep runs from this worker: ${err instanceof Error ? err.message.split("\n")[0] : err}`);
    }
  } else if (serves.length) {
    console.log("[worker] TRADE_FLOW_USER_ID is not set — no schedule clock, no sweeps");
  }
  // The requests nobody presses (services/requestChains.ts): each reacts only to work this process did.
  console.log(
    `[worker] chained requests — after order → update: ${CHAIN_AFTER_ORDER && roles.has("sync") ? "on" : "off"} · delivered → invoice fetch: ${CHAIN_DELIVERED_CHECK && roles.has("update") ? "on" : "off"} · invoice sweep as a request: ${CHAIN_INVOICE_SWEEP && roles.has("invoice") ? "on" : "off"}`
  );
  console.log(
    `[worker] roles ${[...roles].join(",")} — listening on ${names.map(([n, c]) => `${n}×${c}`).join(", ")} — waiting for jobs (lock=10m)`
  );
}

main().catch((err) => {
  console.error("Failed to start worker:", err);
  process.exit(1);
});
