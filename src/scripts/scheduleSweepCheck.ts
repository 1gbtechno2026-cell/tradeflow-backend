/**
 * The sweeps on the shared schedule clock (data-quality plan sub-step 7: C3, C4 and finding T7) — WITHOUT Flipkart,
 * without Chrome, without a worker, without a job on a live queue, and with a clock this script moves by hand.
 *
 *   UPDATE SWEEPS (C3)
 *   · a fixed time of "every active unit" → ONE update request made under the claim: `schedule {name, slotAt, token}`,
 *     params that say which schedule, which time and which machine, the lease following the request, its account jobs
 *     on the queue; the claim's token in no public view; the "Schedule" line of Batch requests → Details
 *   · the lease is renewed while the request is open and given back when its last account ends ("finished — ok 3,
 *     failed 0"); a request Stop / Force clear / the keeper closed is released by the clock's next look
 *   · nothing to refresh → one tick, no request; an open request THIS machine made holds the sweep up; one another
 *     machine left open under a lease that ran out does not — the sweep goes ahead and the tick names it
 *   · a sweep another machine has taken over: an account job of it ends "Superseded — …" before the account lock, the
 *     session lookup and any call — and with it everything the request had left; the clock's own look does the same
 *   · a leftover job of the old per-machine BullMQ scheduler runs no sweep; retireUpdateSchedulers empties the queue
 *     of both schedulers and their next jobs
 *   THE OUT-FOR-DELIVERY SPELLING (T7)
 *   · the recheck selects a unit stored as "ofd" (the order API's spelling) beside "Out for delivery" / "Dispatched"
 *   · an "ofd" unit whose API answer has no OTP gets the page pass — the page read injected, a tab that is no browser;
 *     a unit whose page shows no OTP either (cash on delivery) is no failure, no event, no alert
 *   INVOICE SWEEP (C4)
 *   · two clocks on one fixed time queue exactly one job; the job runs under the lease — its ledger run becomes the
 *     lease's run, the end gives the lease back with the counts; a job whose lease is gone runs nothing; a leftover
 *     job of the old scheduler runs no sweep
 *   · "Capture newly delivered now" through the real route: 409 with who holds it while another machine has the
 *     lease; taken when free; queued behind a sweep of this machine; and, while this machine's worker is not on the
 *     shared clock yet, released at once with a tick that says so
 *   · a takeover stops this machine's sweep between two orders — and, when the run is a request of the log, that
 *     request, the way Batch requests → Cancel does; such a request says which schedule, time and machine
 *
 * What is real: Mongo and Redis db 15. The clocks, switches and presence rows live in THROWAWAY collections
 * (verify_ss_<run>_…, dropped at the end). The requests, units, accounts, ledger runs and task rows are written to
 * their real collections for an INVENTED workspace (this process's TRADE_FLOW_USER_ID is that id, its system id and
 * host name are invented too, so no worker's reaper and no keeper takes them for its own) with @verify.invalid
 * e-mails, and deleted at the end — what was left is printed. Every queue is a throwaway one in Redis db 15
 * (verify-ss-…-<run>), obliterated at the end; the live queues (db 0) are only ASKED, at the end, whether they hold a
 * job of this run. The route is called on a listener of this process (127.0.0.1, a random port).
 *
 * A run that is stopped (Ctrl-C, kill) still cleans up; one that is killed outright leaves its rows, collections and
 * queue keys, and the next run removes them once they are 30 minutes old (scripts/scheduleCheckCleanup.ts) and says so.
 *
 * Run it alone — not next to test:start-requests, whose last check compares whole-collection counts of
 * order_update_requests and fails while this run's invented requests are there (neither check is wrong).
 *
 *   npm run test:schedule-sweep
 */
import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import type { Job } from "bullmq";
import mongoose, { Types, type Model } from "mongoose";
import type { IScheduleLease, ScheduleName } from "../models/ScheduleLease.js";
import type { UpdateJobData } from "../queue.js";
import type { InvoiceSweepDeps, InvoiceSweepResult, SweepJobData } from "../services/invoiceSweep.js";
import type { UpdateJobDeps } from "../services/orderUpdateQueue.js";
import type { ScheduleClaim, ScheduleClock, ScheduleDeps, ScheduleRunner, ScheduleRunResult } from "../services/scheduleClock.js";
import { describeCleanup, interruptedCleanup, isInterrupted, onInterrupt, registerRunIds, removeStaleRuns, standStill, type RunCleanup } from "./scheduleCheckCleanup.js";

let passed = 0;
const failures: string[] = [];

async function check(name: string, fn: () => void | Promise<void>) {
  // Stopped from outside: no further step, and no verdict on the one that was cut short.
  if (isInterrupted()) await standStill();
  try {
    await fn();
    if (isInterrupted()) await standStill();
    passed++;
    console.log(`  ok    ${name}`);
  } catch (err) {
    if (isInterrupted()) await standStill();
    failures.push(name);
    console.log(`  FAIL  ${name}\n        ${err instanceof Error ? err.message.split("\n").slice(0, 12).join("\n        ") : err}`);
  }
}

/** "2026-11-02 12:15" (or with seconds) on India's clock. */
const ist = (s: string) => new Date(`${s.replace(" ", "T")}${s.length <= 16 ? ":00" : ""}+05:30`);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  // A workspace and a machine that do not exist — set before config is read, so nothing here can act as the real ones.
  const run = Date.now().toString(36);
  const invented = new Types.ObjectId();
  process.env.TRADE_FLOW_USER_ID = String(invented);
  process.env.TRADEFLOW_SYSTEM_ID = `verify-ss-${run}`;
  process.env.TRADEFLOW_HOST_ID = "Mac-check";
  const { config } = await import("../config.js");
  const { Order } = await import("../models/Order.js");
  const { OrderInvoiceRequest } = await import("../models/OrderInvoiceRequest.js");
  const { OrderJobRun } = await import("../models/OrderJobRun.js");
  const { OrderSyncSettings } = await import("../models/OrderSyncSettings.js");
  const { OrderSyncTask } = await import("../models/OrderSyncTask.js");
  const { OrderUnitEvent } = await import("../models/OrderUnitEvent.js");
  const { OrderUpdateRequest } = await import("../models/OrderUpdateRequest.js");
  const { PlatformId } = await import("../models/PlatformId.js");
  const { ScheduleHost } = await import("../models/ScheduleHost.js");
  const { ScheduleLease } = await import("../models/ScheduleLease.js");
  const { claimManual, clockStamp, clockTime, createScheduleClock, LEASE_MS, scheduleDefaults, supersededBy, supersededText, tickText } = await import("../services/scheduleClock.js");
  const { clockProcessUp, retireLegacySchedulers, seedSchedules } = await import("../services/scheduleSettings.js");
  const { createUpdateRequest, getUpdateRequest, isOutForDelivery, listUpdateRequests, OFD_STATUSES, ofdStatusKeys, ofdStatusSpellings, publicUpdateRequest, retireUpdateSchedulers, runOrderUpdateJob, runUpdateSweep, supersedeUpdateRequest, updateSweepRunner } = await import("../services/orderUpdateQueue.js");
  const { invoiceSweepDefaults, invoiceSweepRunner, runSweepJob, sweepWords } = await import("../services/invoiceSweep.js");
  const { deliveredCheckDefaults } = await import("../services/requestChains.js");
  const { STATUS_ALIASES } = await import("../services/startRequests.js");
  const { settingsOf } = await import("../services/syncBatches.js");
  const { factsOfInvoice, factsOfUpdate, taskLog } = await import("../services/syncTaskLog.js");
  const { Queue } = await import("bullmq");
  const { Redis } = await import("ioredis");

  const U = String(invented);
  assert.equal(config.userId, U, "the invented TRADE_FLOW_USER_ID is the one in use");
  assert.equal(config.systemId, `verify-ss-${run}`, "an invented system id: no worker's reaper takes these requests for its own");
  // The live workspace reads Flipkart through the order API; so does this run (its fake).
  config.orderFetchMode = "api";
  // No chained invoice fetch from this run (it would go on a live queue): nothing here reaches Delivered anyway.
  deliveredCheckDefaults.enabled = false;

  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false, serverSelectionTimeoutMS: 15_000 });
  const db = mongoose.connection.db!;
  const realCollections = async () => (await db.listCollections({ name: { $in: ["order_schedule_leases", "order_schedule_hosts"] } }, { nameOnly: true }).toArray()).map((c) => c.name).sort();
  const realBefore = await realCollections();
  const dropList: string[] = [];
  const throwaway = <T>(real: Model<T>, name: string): Model<T> => {
    const coll = `verify_ss_${run}_${name}`;
    dropList.push(coll);
    return mongoose.model<T>(`VerifySs${name}${run}`, real.schema, coll) as Model<T>;
  };

  // The clock this script moves by hand. It stands in the PAST: everything the services write with the real time is
  // then "just written" to it, never 30 minutes quiet.
  let clockMs = ist("2026-11-02 10:00").getTime();
  const at = (s: string) => {
    clockMs = ist(s).getTime();
  };
  const realDefaults = { ...scheduleDefaults };
  // Clocks, switches and presence in throwaway collections; update requests and ledger runs are the real collections'
  // (the services under test write them there), for the invented workspace only.
  const deps: ScheduleDeps = { ...scheduleDefaults, Leases: throwaway(ScheduleLease, "leases"), Hosts: throwaway(ScheduleHost, "hosts"), Settings: throwaway(OrderSyncSettings, "settings"), now: () => new Date(clockMs) };
  Object.assign(scheduleDefaults, deps);

  // Redis db 15, throwaway queue names: nothing of this run can reach a live queue.
  const redisUrl = new URL(config.redisUrl);
  const connection = { host: redisUrl.hostname, port: Number(redisUrl.port || 6376), password: redisUrl.password || undefined, db: 15, maxRetriesPerRequest: null as null };
  // What an earlier run that was killed left behind goes first; then this run writes down the workspace it invents,
  // before a row of it reaches a real collection.
  const ROW_COLLECTIONS = ["order_update_requests", "order_details", "platformids", "order_job_runs", "order_sync_tasks", "order_unit_events", "order_invoice_requests", "alerts"];
  const mine: RunCleanup = { db, tag: "ss", rowCollections: ROW_COLLECTIONS, redis: { host: connection.host, port: connection.port, password: connection.password } };
  for (const stale of await removeStaleRuns({ ...mine, thisRun: run })) {
    console.log(`leftovers of an earlier run that never cleaned up — removed now · ${describeCleanup(stale)}`);
    if (stale.refused.length) failures.push("leftovers of an earlier run");
  }
  await registerRunIds(db, "ss", run, [invented]);
  const jobOptions = { attempts: 1, removeOnComplete: true, removeOnFail: true };
  const Q = new Queue<UpdateJobData>(`verify-ss-update-${run}`, { connection, defaultJobOptions: jobOptions });
  const QI = new Queue<SweepJobData>(`verify-ss-invoice-${run}`, { connection, defaultJobOptions: jobOptions });
  const invoiceQueue = (() => QI) as unknown as InvoiceSweepDeps["queue"];
  const realInvoiceDefaults = { ...invoiceSweepDefaults };
  const liveJobIds = { update: [] as string[], invoice: [] as string[] };

  console.log(`invented workspace ${U} on invented machine ${config.systemId} ("${config.hostId}") · throwaway collections verify_ss_${run}_* · throwaway queues ${Q.name}, ${QI.name} (Redis db 15) · the clock starts at ${clockStamp(deps.now())} IST\n`);

  // ── helpers ───────────────────────────────────────────────────────────────
  const THIS = { systemId: config.systemId, host: config.hostId };
  const MAC2 = { systemId: `mac2-${run}`, host: "Mac-2" };
  const ALL_ON = { UPDATE_SCHEDULE: "1", FETCH_SCHEDULE: "0 5 * * *" };
  const logs: string[] = [];
  let pidSeq = 100;
  const requestIds: string[] = [];

  /** Everything of the invented workspace back to nothing, then its 4 clocks and switches seeded at `when` (no preferred machine). */
  const reset = async (when: string) => {
    at(when);
    await taskLog.flush();
    await OrderUpdateRequest.updateMany({ userId: invented, status: { $in: ["queued", "running"] } }, { $set: { status: "cancelled" } });
    await OrderJobRun.updateMany({ userId: invented, status: { $in: ["queued", "running"] } }, { $set: { status: "failed", message: "verify: reset" } });
    await OrderInvoiceRequest.deleteMany({ userId: invented });
    await Promise.all([deps.Leases.deleteMany({ userId: invented }), deps.Settings.deleteMany({ userId: invented }), deps.Hosts.deleteMany({}), Order.deleteMany({ userId: invented }), PlatformId.deleteMany({ userId: invented })]);
    await Q.drain(true);
    await QI.drain(true);
    await seedSchedules(U, { env: ALL_ON, me: { systemId: "", host: "seed-host" }, deps });
  };
  const leaseOf = async (name: ScheduleName) => {
    const lease = (await deps.Leases.findOne({ userId: invented, name }).lean()) as (IScheduleLease & { _id: Types.ObjectId }) | null;
    assert.ok(lease, `the ${name} clock exists`);
    return lease;
  };
  const ticksOf = (lease: IScheduleLease) => lease.ticks.map((t) => `${t.outcome}@${clockTime(t.slotAt)}`);
  /** A runner that records what it was asked, and answers what the case says (another machine's, or a stand-in). */
  const recorder = (answer: (claim: ScheduleClaim) => ScheduleRunResult | Promise<ScheduleRunResult>) => {
    const calls: ScheduleClaim[] = [];
    const runner: ScheduleRunner = {
      run: async (claim) => {
        calls.push(claim);
        return answer(claim);
      },
    };
    return { calls, runner };
  };
  /** A worker's clock: `who` is this machine or another, `runners` what it runs. */
  const workerClock = (who: { systemId: string; host: string }, runners: Partial<Record<ScheduleName, ScheduleRunner>>, names?: ScheduleName[]): ScheduleClock =>
    createScheduleClock({ role: "worker", userId: U, names: names || (Object.keys(runners) as ScheduleName[]), serves: ["update_active", "update_ofd", "invoice_sweep"], runners, me: { ...who, pid: ++pidSeq, machine: `${who.host}.local` }, deps, log: (line) => logs.push(line), startWaitMs: 20_000 });
  const sweepRunners = () => ({ update_active: updateSweepRunner("active", { queue: Q }), update_ofd: updateSweepRunner("ofd", { queue: Q }) });

  /** Invented cookies under Flipkart's auth names: only the stand-in tab below ever sees them. */
  const cookiesOf = (label: string) => ["T", "SN", "at"].map((name) => ({ name, value: `verify-invalid-${label}`, domain: ".flipkart.com", path: "/", httpOnly: true, secure: true, sameSite: "Lax" as const }));
  const mail = (label: string) => `${label}-${run}@verify.invalid`;
  type Acc = { id: Types.ObjectId; email: string; label: string };
  async function account(label: string): Promise<Acc> {
    const _id = new Types.ObjectId();
    const cookies = cookiesOf(label);
    const now = new Date();
    await PlatformId.collection.insertOne({ _id, userId: invented, onboardingId: new Types.ObjectId(), parentId: null, parentEmail: "", domain: "verify.invalid", platform: "flipkart", email: mail(label), status: "logged_in", lastError: "", retryable: true, sessionCookies: cookies, sessionState: { cookies, origins: [] }, sessionSavedAt: new Date("2026-08-01T00:00:00Z"), lastChecked: null, sessionCheck: null, statusBeforeDemote: "", demotedAt: null, demotedBy: "", createdAt: now, updatedAt: now });
    return { id: _id, email: mail(label), label };
  }
  let unitSeq = 0;
  const idBase = Math.floor(Date.now() / 1000) * 1000;
  /** One order unit of an invented account (its own negative `id`: the real order counter is not touched). */
  async function unit(acc: Acc, status: string, otp: string | null = null) {
    unitSeq += 1;
    const orderId = `OD8${String(unitSeq).padStart(14, "0")}`;
    const itemId = `VI${acc.label}${unitSeq}`;
    const unitId = `VU${acc.label}${unitSeq}`;
    const orderUrl = `https://www.flipkart.com/order_details?order_id=${orderId}&item_id=${itemId}&unit_id=${unitId}`;
    await Order.collection.insertOne({ userId: invented, platformAccountId: acc.id, id: -(idBase + unitSeq), platform_email: acc.email, platform: "FLIPKART", order_id: orderId, item_id: itemId, unit_id: unitId, order_url: orderUrl, status_key: status, product_name: "", delivery_otp: otp, shipping_address: null, shipping_address_hash: "", createdAt: new Date(), updatedAt: new Date() });
    return { orderId, itemId, unitId, orderUrl, status };
  }
  /** An update request written here — another machine's, or one that was open before the clock. */
  async function rawRequest(o: { systemId: string; source: "schedule" | "ofd"; ranOn?: string; status?: string; createdAt: Date; token?: string; name?: ScheduleName }) {
    const _id = new Types.ObjectId();
    await OrderUpdateRequest.collection.insertOne({ _id, userId: invented, source: o.source, selectionMethod: o.source === "ofd" ? "schedule_ofd" : "schedule_active", params: o.ranOn ? { ran_on: o.ranOn } : {}, parentId: "", systemId: o.systemId, deliveredOrderIds: [], ...(o.token ? { schedule: { name: o.name || "update_active", slotAt: o.createdAt, token: o.token } } : {}), createdBy: "schedule", status: o.status || "running", mode: "api", windows: 1, accounts: [{ email: mail("elsewhere"), platformId: null, units: [{ orderId: "OD800000000000999", itemId: "", unitId: "VUelsewhere", orderUrl: "" }], total: 1, status: "queued", done: 0, failed: 0, statusChanged: 0, otpNew: 0, via: "", message: "", startedAt: null, finishedAt: null }], logs: [], startedAt: o.createdAt, finishedAt: null, createdAt: o.createdAt, updatedAt: o.createdAt });
    requestIds.push(String(_id));
    return String(_id);
  }
  const requestsOf = async (source: "schedule" | "ofd") => {
    const rows = await OrderUpdateRequest.find({ userId: invented, source }).sort({ createdAt: 1, _id: 1 }).lean();
    for (const r of rows) if (!requestIds.includes(String(r._id))) requestIds.push(String(r._id));
    return rows;
  };
  /** The requests of a schedule that are waiting for their accounts — what a sweep has just made. */
  const queuedOf = async (source: "schedule" | "ofd") => (await requestsOf(source)).filter((r) => r.status === "queued");
  const requestOf = async (id: string) => {
    const doc = await OrderUpdateRequest.findById(id).lean();
    assert.ok(doc, "request gone");
    return doc;
  };
  /** Jobs a queue still holds for a worker: [delayed, waiting]. */
  const pending = async (queue: { getJobCounts: (...types: Array<"delayed" | "waiting">) => Promise<Record<string, number>> }) => {
    const n = await queue.getJobCounts("delayed", "waiting");
    return [n.delayed, n.waiting];
  };
  const jobIdsIn = async (queue: { getJobs: (types: Array<"waiting" | "delayed" | "active" | "paused" | "prioritized">) => Promise<Array<{ id?: string }>> }) => (await queue.getJobs(["waiting", "delayed", "active", "paused", "prioritized"])).map((j) => String(j.id)).sort();

  // What an account job reaches instead of Flipkart, Chrome and the account lock.
  const touched = { lock: 0, api: 0, tabs: 0, apiReads: [] as string[], pageReads: [] as string[] };
  const untouched = () => Object.assign(touched, { lock: 0, api: 0, tabs: 0, apiReads: [], pageReads: [] });
  /** The account's tab — an object, not a browser: enough for the update's own "open My Orders" step, and nothing more. */
  const tab: UpdateJobDeps["launch"] = async () => {
    touched.tabs++;
    const jar: Array<Record<string, unknown>> = [];
    let url = "about:blank";
    const context = { route: async () => undefined, cookies: async () => jar, clearCookies: async () => void (jar.length = 0), addCookies: async (c: Array<Record<string, unknown>>) => void jar.push(...c), newPage: async () => page, close: async () => undefined };
    const page = { context: () => context, url: () => url, goto: async (to: string) => void (url = to) };
    return { browser: { close: async () => undefined }, context } as unknown as Awaited<ReturnType<UpdateJobDeps["launch"]>>;
  };
  const answered = (statusKey: string) => ({ lastFetch: new Date(), productName: "", totalAmount: "", statusKey, gstNumber: "", trackingCount: 0, invoiceDownloaded: false, imei: null, orderDate: null, skippedOld: false, siblingUnitIds: [] }) as unknown as Awaited<ReturnType<UpdateJobDeps["apiRead"]>>;
  /** A live account: the order API answers every unit as it is stored; the page read (when asked for) writes `pageOtp`. */
  const jobDeps = (pageOtp: Record<string, string> = {}): Partial<UpdateJobDeps> => ({
    lock: async () => {
      touched.lock++;
      return async () => undefined;
    },
    openApi: async () => {
      touched.api++;
      return { dispose: async () => undefined } as unknown as Awaited<ReturnType<UpdateJobDeps["openApi"]>>;
    },
    apiRead: async (_ctx, _userId, _account, card) => {
      touched.apiReads.push(card.unitId);
      const row = await Order.findOne({ userId: invented, unit_id: card.unitId }).select("status_key").lean();
      return answered(String(row?.status_key || ""));
    },
    launch: tab,
    pageRead: async (_page, _userId, _account, card) => {
      touched.pageReads.push(card.unitId);
      if (pageOtp[card.unitId]) await Order.collection.updateOne({ userId: invented, unit_id: card.unitId }, { $set: { delivery_otp: pageOtp[card.unitId] } });
      const row = await Order.findOne({ userId: invented, unit_id: card.unitId }).select("status_key").lean();
      return answered(String(row?.status_key || "")) as unknown as Awaited<ReturnType<UpdateJobDeps["pageRead"]>>;
    },
    queue: () => Q,
  });
  /** Deps that fail the check when anything behind the account lock is reached. */
  const forbidden = (): Partial<UpdateJobDeps> => {
    const no = (what: string) => async () => {
      throw new Error(`verify: ${what} was reached`);
    };
    return { ...jobDeps(), openApi: no("the order API") as unknown as UpdateJobDeps["openApi"], apiRead: no("an API read") as unknown as UpdateJobDeps["apiRead"], launch: no("Chrome") as unknown as UpdateJobDeps["launch"], pageRead: no("a page read") as unknown as UpdateJobDeps["pageRead"] };
  };
  const fakeJob = (data: UpdateJobData, name = "update") =>
    ({
      name,
      data,
      moveToDelayed: async () => {
        throw new Error("verify: there is no queue behind this job");
      },
    }) as unknown as Job<UpdateJobData>;
  const runJob = (requestId: string, email: string, overrides: Partial<UpdateJobDeps> = jobDeps()) => runOrderUpdateJob(fakeJob({ requestId, email }), undefined, overrides);
  const tasksOf = (requestId: string) => OrderSyncTask.find({ requestId }).sort({ key: 1 }).select("key status error detail").lean();

  let server: import("node:http").Server | null = null;
  // What the services say for themselves is kept, not printed between the checks.
  const said = { log: console.log, warn: console.warn };
  const serviceLines: string[] = [];
  const own = (print: (...a: unknown[]) => void) => (...a: unknown[]) => (typeof a[0] === "string" && /^\[(schedules|orders-update|invoice-sweep|worker|task-log|session|chain)/.test(a[0]) ? void serviceLines.push(a[0]) : print(...a));
  console.log = own(said.log);
  console.warn = own(said.warn);
  // Stopped from outside (Ctrl-C, kill): what this run made goes before the process does.
  onInterrupt(() =>
    interruptedCleanup(config.mongoUri, { tag: mine.tag, rowCollections: mine.rowCollections, redis: mine.redis }, run, async () => {
      console.log = said.log;
      console.warn = said.warn;
      server?.close();
      await Promise.all([Q.close(), QI.close()]);
    })
  );

  try {
    await deps.Settings.createCollection();
    await deps.Settings.createIndexes();

    // ── update sweeps ────────────────────────────────────────────────────────
    console.log("UPDATE SWEEPS — one request per fixed time, made under the lease");
    let firstRequest = "";
    let accA!: Acc;
    let accB!: Acc;
    await check("a fixed time of 'every active unit' → ONE request under the claim: schedule {name, slot, token}, params say schedule / time / machine, the lease follows it, its jobs are queued", async () => {
      await reset("2026-11-02 10:00");
      accA = await account("a");
      accB = await account("b");
      await unit(accA, "Shipped");
      await unit(accA, "Approved");
      await unit(accB, "ofd", "111111");
      await unit(accB, "Delivered");
      const c = workerClock(THIS, sweepRunners());
      at("2026-11-02 12:14");
      await c.pollOnce();
      assert.equal((await queuedOf("schedule")).length, 0, "not due: nothing is made");
      at("2026-11-02 12:15:05");
      await c.pollOnce();
      const lease = await leaseOf("update_active");
      const rows = await queuedOf("schedule");
      assert.equal(rows.length, 1, "one request");
      const r = rows[0];
      firstRequest = String(r._id);
      assert.deepEqual([r.selectionMethod, r.systemId, r.createdBy, r.status, r.mode], ["schedule_active", config.systemId, "schedule", "queued", "api"]);
      assert.ok(lease.holder?.token, "the lease is held");
      assert.deepEqual(r.schedule, { name: "update_active", slotAt: ist("2026-11-02 12:15"), token: lease.holder?.token });
      const p = r.params as Record<string, unknown>;
      assert.deepEqual([p.schedule_name, p.schedule_slot, p.ran_on, p.attempt, p.selection_method, p.status_keys], ["update_active", "2026-11-02 12:15", "Mac-check", 1, "schedule_active", []]);
      assert.deepEqual(r.accounts.map((a) => [a.email, a.units.length, a.status]).sort(), [[accA.email, 2, "queued"], [accB.email, 1, "queued"]].sort(), "the three units that are not finished, on their two accounts");
      assert.deepEqual(lease.runRef, { kind: "update", id: firstRequest });
      assert.deepEqual([ticksOf(lease), lease.lastTick?.reason, tickText(lease.lastTick), lease.lastTick?.runRef], [["ran@12:15"], "3 unit(s) on 2 account(s)", "ran on Mac-check — 3 unit(s) on 2 account(s)", { kind: "update", id: firstRequest }]);
      assert.deepEqual([clockStamp(lease.nextAt), lease.attempt, lease.holder?.systemId, lease.holder?.role], ["2 Nov 18:15", 1, config.systemId, "worker"]);
      const wanted = [`${firstRequest}-${accA.email}`, `${firstRequest}-${accB.email}`].sort();
      assert.deepEqual(await jobIdsIn(Q), wanted, "one job per account, on the throwaway queue");
      liveJobIds.update.push(...wanted);
      // The claim never leaves Trade Flow: no public view of the request carries it.
      const views = JSON.stringify([publicUpdateRequest(r), await getUpdateRequest(U, firstRequest), await listUpdateRequests(U)]);
      assert.ok(!views.includes(String(lease.holder?.token)) && !/"token"|"schedule":/.test(views), "the token is in no public view");
      assert.ok(r.logs[0].message.endsWith("· schedule · Update all active units, the 12:15 time, on Mac-check"), r.logs[0].message);
      // Batch requests → Details → SETTINGS.
      const settings = settingsOf(factsOfUpdate(r));
      assert.deepEqual(settings.find((s) => s.label === "Schedule"), { label: "Schedule", value: "Update all active units · 2026-11-02 12:15 · ran on Mac-check" });
      assert.deepEqual(settings.find((s) => s.label === "Selected by"), { label: "Selected by", value: "All eligible" });
      // A request nobody's schedule made — the Update button's — has no such line, and no `schedule` path at all.
      const pressed = await createUpdateRequest(U, { emails: [accA.email], selectionMethod: "email_ids" }, { by: "asha", params: { platform: "FLIPKART", selection_method: "email_ids", email_ids_count: 1 }, queue: Q });
      requestIds.push(pressed.id);
      liveJobIds.update.push(`${pressed.id}-${accA.email}`);
      const stored = await requestOf(pressed.id);
      assert.deepEqual(["schedule" in stored, settingsOf(factsOfUpdate(stored)).some((s) => s.label === "Schedule"), stored.source], [false, false, "manual"]);
      await OrderUpdateRequest.updateOne({ _id: pressed.id }, { $set: { status: "cancelled" } });
      await (await Q.getJob(`${pressed.id}-${accA.email}`))?.remove();
    });

    await check("the lease is renewed while the request is open, and given back when its last account ends: 'finished — ok 3, failed 0'", async () => {
      assert.ok(firstRequest, "the request of the check above");
      const c = workerClock(THIS, sweepRunners());
      // Another process of this machine (the worker was restarted): it carries the same claim on.
      const token = (await leaseOf("update_active")).holder?.token;
      for (const t of ["12:15:40", "12:19:00", "12:23:30"]) {
        at(`2026-11-02 ${t}`);
        await c.pollOnce();
        const l = await leaseOf("update_active");
        assert.deepEqual([l.holder?.token, l.expiresAt?.getTime()], [token, clockMs + LEASE_MS], `renewed at ${t}`);
      }
      untouched();
      await runJob(firstRequest, accA.email);
      assert.deepEqual([(await requestOf(firstRequest)).status, (await leaseOf("update_active")).holder?.token], ["running", token], "one account left: still held");
      await runJob(firstRequest, accB.email);
      const r = await requestOf(firstRequest);
      assert.deepEqual([r.status, r.accounts.map((a) => [a.email, a.status, a.done, a.failed]).sort()], ["done", [[accA.email, "done", 2, 0], [accB.email, "done", 1, 0]].sort()]);
      assert.deepEqual([touched.lock, touched.api, touched.apiReads.length, touched.pageReads, touched.tabs], [2, 2, 3, [], 0], "read over the fake order API; the 'ofd' unit has its OTP, so no page pass");
      let lease = await leaseOf("update_active");
      assert.deepEqual([lease.holder, lease.endedAs, tickText(lease.lastTick), lease.lastTick?.runRef, ticksOf(lease)], [null, "finished", "finished — ok 3, failed 0", { kind: "update", id: firstRequest }, ["ran@12:15", "finished@12:15"]]);
      at("2026-11-02 12:24:30");
      await c.pollOnce();
      lease = await leaseOf("update_active");
      assert.deepEqual([ticksOf(lease), c.holds("update_active")], [["ran@12:15", "finished@12:15"], null], "the clock's next look adds nothing");
      assert.equal(await supersededBy(U, { name: "update_active", token: String(token) }), null, "nobody took over");
    });

    await check("a request closed by Stop, Force clear or the keeper is released by the clock's next look; nothing to refresh → one tick and no request", async () => {
      await reset("2026-11-02 10:00");
      const a = await account("s");
      await unit(a, "Shipped");
      const c = workerClock(THIS, sweepRunners());
      at("2026-11-02 12:14");
      await c.pollOnce();
      at("2026-11-02 12:15:05");
      await c.pollOnce();
      const [r] = await queuedOf("schedule");
      assert.ok(r && (await leaseOf("update_active")).holder, "taken");
      // What POST /update/stop writes (the route itself would also clear this machine's real queue).
      await OrderUpdateRequest.updateOne({ _id: r._id }, { $set: { status: "cancelled", finishedAt: new Date(), "accounts.$[].status": "cancelled" } });
      at("2026-11-02 12:15:40");
      await c.pollOnce();
      let lease = await leaseOf("update_active");
      assert.deepEqual([lease.holder, tickText(lease.lastTick), ticksOf(lease)], [null, "finished — the request was stopped", ["ran@12:15", "finished@12:15"]]);
      // 18:15 — the unit has been delivered meanwhile: nothing is in progress.
      await Order.updateMany({ userId: invented }, { $set: { status_key: "Delivered" } });
      at("2026-11-02 18:15:05");
      await c.pollOnce();
      lease = await leaseOf("update_active");
      assert.deepEqual([(await queuedOf("schedule")).length, lease.holder, lease.endedAs, tickText(lease.lastTick), lease.runRef], [0, null, "ran", "ran on Mac-check — nothing to refresh", null]);
      // The out-for-delivery recheck of 18:45 the same.
      at("2026-11-02 18:45:05");
      await c.pollOnce();
      const ofd = await leaseOf("update_ofd");
      assert.deepEqual([(await queuedOf("ofd")).length, ofd.holder, tickText(ofd.lastTick)], [0, null, "ran on Mac-check — nothing out for delivery"]);
    });

    await check("an open request THIS machine made holds the sweep up (skipped, lease given back); one another machine left open under a lease that ran out does not — the sweep goes ahead and the tick names it", async () => {
      await reset("2026-11-02 10:00");
      const a = await account("g");
      await unit(a, "Shipped");
      // This machine's own: say, the update that followed a nightly fetch — a "schedule" request with no lease.
      const mine = await rawRequest({ systemId: config.systemId, source: "schedule", createdAt: ist("2026-11-02 11:50") });
      const c = workerClock(THIS, sweepRunners());
      at("2026-11-02 12:14");
      await c.pollOnce();
      at("2026-11-02 12:15:05");
      await c.pollOnce();
      let lease = await leaseOf("update_active");
      assert.deepEqual([(await queuedOf("schedule")).length, lease.holder, lease.endedAs, tickText(lease.lastTick)], [0, null, "skipped", `skipped — request ${mine.slice(-8)} from 2 Nov 11:50 is still open on this machine`]);
      await OrderUpdateRequest.updateOne({ _id: mine }, { $set: { status: "done" } });

      // Mac-2 took 18:15, made its request and went to sleep: its lease runs out at 18:20, its request stays open.
      const two = recorder(() => ({ runRef: { kind: "update", id: theirs } }));
      const c2 = workerClock(MAC2, { update_active: two.runner });
      at("2026-11-02 18:15:05");
      const theirs = await rawRequest({ systemId: MAC2.systemId, source: "schedule", ranOn: "Mac-2", createdAt: ist("2026-11-02 18:15:05") });
      await c2.pollOnce();
      assert.deepEqual([(await leaseOf("update_active")).holder?.systemId, two.calls.length], [MAC2.systemId, 1]);
      await OrderUpdateRequest.collection.updateOne({ _id: new Types.ObjectId(theirs) }, { $set: { "schedule.token": two.calls[0].token, "schedule.name": "update_active", "schedule.slotAt": ist("2026-11-02 18:15") } });
      // 00:15 — this machine is awake, Mac-2 is not.
      at("2026-11-03 00:15:05");
      await c.pollOnce();
      lease = await leaseOf("update_active");
      const made = (await queuedOf("schedule")).filter((r) => r.systemId === config.systemId);
      assert.equal(made.length, 1, "the sweep went ahead");
      assert.deepEqual([lease.holder?.systemId, lease.runRef, ticksOf(lease).slice(-1)], [config.systemId, { kind: "update", id: String(made[0]._id) }, ["took_over@00:15"]]);
      assert.equal(lease.lastTick?.reason, `1 unit(s) on 1 account(s) · Mac-2's request ${theirs.slice(-8)} from 2 Nov 18:15 is still open and is left to that machine · Mac-2's lease ran out at 18:20 and its 18:15 run did not finish`);
      const left = await requestOf(theirs);
      assert.deepEqual([left.status, left.accounts[0].status], ["running", "queued"], "Mac-2's request is not touched from here");
      liveJobIds.update.push(...(await jobIdsIn(Q)));
    });

    console.log("\nA SWEEP ANOTHER MACHINE TOOK OVER — what is left of it here ends before any call");
    await check("an account job of a superseded sweep ends 'Superseded — Mac-2 took over …' before the account lock, the session lookup and any call — and everything the request had left ends with it", async () => {
      await reset("2026-11-02 10:00");
      const [a, b, d] = [await account("x"), await account("y"), await account("z")];
      for (const acc of [a, b, d]) await unit(acc, "Shipped");
      const c = workerClock(THIS, sweepRunners());
      at("2026-11-02 12:14");
      await c.pollOnce();
      at("2026-11-02 12:15:05");
      await c.pollOnce();
      const [r] = await queuedOf("schedule");
      assert.ok(r);
      const id = String(r._id);
      const jobs = [a, b, d].map((acc) => `${id}-${acc.email}`).sort();
      assert.deepEqual(await jobIdsIn(Q), jobs);
      liveJobIds.update.push(...jobs);
      // Account x is read; then this machine falls asleep with y and z still queued.
      untouched();
      await runJob(id, a.email);
      assert.equal(touched.apiReads.length, 1);
      // 18:15 — Mac-2 takes the schedule over (this machine's lease ran out at 12:20), finds nothing left and is done.
      const two = recorder(() => ({ done: true, reason: "nothing to refresh" }));
      at("2026-11-02 18:15:05");
      await workerClock(MAC2, { update_active: two.runner }).pollOnce();
      assert.equal(two.calls.length, 1);
      assert.deepEqual(await supersededBy(U, { name: "update_active", token: String(r.schedule?.token) }), { systemId: MAC2.systemId, host: "Mac-2", at: ist("2026-11-02 18:15:05"), slotAt: ist("2026-11-02 18:15") });
      // This machine wakes; the reaper puts y's job back and the worker takes it — before its clock has looked.
      at("2026-11-02 18:20");
      untouched();
      await runJob(id, b.email, forbidden());
      assert.deepEqual([touched.lock, touched.api, touched.tabs, touched.apiReads, touched.pageReads], [0, 0, 0, [], []], "ended before the account lock — so before the session lookup and any call");
      const after = await requestOf(id);
      const message = "Superseded — Mac-2 took over the update of all active units at 18:15";
      assert.equal(message, supersededText("update_active", MAC2, ist("2026-11-02 18:15:05")));
      assert.equal(after.status, "cancelled");
      assert.deepEqual(after.accounts.map((x) => [x.email, x.status, x.message]).sort(), [[a.email, "done", "1 refreshed · api"], [b.email, "cancelled", message], [d.email, "cancelled", message]].sort());
      assert.ok(after.accounts.filter((x) => x.status === "cancelled").every((x) => x.finishedAt instanceof Date));
      assert.deepEqual(await jobIdsIn(Q), [`${id}-${a.email}`], "the jobs of the accounts that had not started left the queue (x's is the one this script ran by hand)");
      assert.ok(after.logs.some((l) => l.level === "warn" && l.message === `${message} — 2 account(s) that had not started are left to that machine (2 job(s) removed from the queue); an account being read finishes its current unit`), after.logs.map((l) => l.message).join("\n"));
      await taskLog.flush();
      assert.deepEqual((await tasksOf(id)).map((t) => [t.key.split("|")[0], t.status]).sort(), [[a.email, "success"], [b.email, "cancelled"], [d.email, "cancelled"]].sort(), "the task log says the same");
      // z's job, had it been taken as well: the request has ended, nothing more happens.
      await runJob(id, d.email, forbidden());
      assert.equal(touched.lock, 0);
      assert.equal(JSON.stringify(await requestOf(id)), JSON.stringify(after), "a second job changes nothing");
      // A sweep that only gets to start now, under the claim this machine took at 12:15 (it slept between taking the
      // time and making the request): the lease is another claim's — no request is made, and the lease is not touched.
      const stale: ScheduleClaim = { userId: U, name: "update_active", token: String(r.schedule?.token), slotAt: ist("2026-11-02 12:15"), attempt: 1, manual: false, by: "schedule", takenAt: ist("2026-11-02 12:15:05"), holder: { systemId: config.systemId, host: config.hostId, role: "worker", pid: 1, token: String(r.schedule?.token) }, lateMs: 0, missedBefore: 0, replaced: null, secondStartOf: "" };
      await Order.updateMany({ userId: invented }, { $set: { status_key: "Shipped" } });
      const requestsBefore = (await requestsOf("schedule")).length;
      const leaseBefore = JSON.stringify(await leaseOf("update_active"));
      assert.deepEqual(await runUpdateSweep("active", stale, { queue: Q }), { skipped: true, reason: "the lease went to another machine before the sweep started" });
      assert.deepEqual([(await requestsOf("schedule")).length, JSON.stringify(await leaseOf("update_active"))], [requestsBefore, leaseBefore]);
      // The lease is Mac-2's story: this machine's request closing wrote nothing on it.
      const lease = await leaseOf("update_active");
      assert.deepEqual([ticksOf(lease), lease.ranBy?.systemId, lease.holder], [["ran@12:15", "ran@18:15"], MAC2.systemId, null]);
      assert.equal(await OrderUnitEvent.countDocuments({ userId: invented }), 0, "no event, so no alert");
    });

    await check("the clock's own look does the same: superseded → the request this machine made stops, its queued accounts leave the queue; an account already read keeps its result", async () => {
      await reset("2026-11-02 10:00");
      const [a, b] = [await account("p"), await account("q")];
      for (const acc of [a, b]) await unit(acc, "Shipped");
      const c = workerClock(THIS, sweepRunners());
      at("2026-11-02 12:14");
      await c.pollOnce();
      at("2026-11-02 12:15:05");
      await c.pollOnce();
      const [r] = await queuedOf("schedule");
      assert.ok(r);
      const id = String(r._id);
      liveJobIds.update.push(...(await jobIdsIn(Q)));
      const two = recorder(() => ({ done: true, reason: "nothing to refresh" }));
      at("2026-11-02 18:15:05");
      await workerClock(MAC2, { update_active: two.runner }).pollOnce();
      at("2026-11-02 18:21");
      await c.pollOnce();
      const after = await requestOf(id);
      assert.deepEqual([after.status, after.accounts.map((x) => x.status), after.accounts[0].message], ["cancelled", ["cancelled", "cancelled"], "Superseded — Mac-2 took over the update of all active units at 18:15"]);
      assert.deepEqual(await jobIdsIn(Q), [], "both account jobs left the queue");
      const lease = await leaseOf("update_active");
      assert.deepEqual(ticksOf(lease), ["ran@12:15", "ran@18:15", "superseded@12:15"]);
      assert.equal(await supersedeUpdateRequest(id, "again", Q), false, "a request that has ended is not stopped twice");
      // This machine ran again itself (no other machine since): its own older claim is not superseded by that.
      at("2026-11-03 00:15:05");
      await Order.updateMany({ userId: invented }, { $set: { status_key: "Delivered" } });
      await c.pollOnce();
      assert.equal((await leaseOf("update_active")).ranBy?.systemId, config.systemId);
      assert.equal(await supersededBy(U, { name: "update_active", token: String(r.schedule?.token) }), null);
    });

    await check("a leftover job of the old per-machine scheduler runs no sweep — the scheduler is removed and the clock looks; retireUpdateSchedulers empties the queue of both schedulers and their next jobs", async () => {
      await reset("2026-11-02 10:00");
      const a = await account("l");
      await unit(a, "Shipped");
      // As the worker installed them before the shared clock.
      await Q.upsertJobScheduler("order-update-active", { every: 6 * 60 * 60_000 }, { name: "sweep", data: { requestId: "", email: "", sweep: "active" }, opts: { attempts: 1 } });
      await Q.upsertJobScheduler("order-update-ofd", { every: 2 * 60 * 60_000 }, { name: "sweep", data: { requestId: "", email: "", sweep: "ofd" }, opts: { attempts: 1 } });
      assert.equal((await Q.getJobSchedulers()).length, 2);
      const [delayed, waiting] = await pending(Q);
      assert.ok(delayed + waiting >= 2, "each has its next job");
      // One of those jobs reaches the worker after it moved to the shared clock (12:10 — no fixed time of this sweep is due).
      at("2026-11-02 12:10");
      let looks = 0;
      const c = workerClock(THIS, sweepRunners());
      await c.pollOnce();
      await runOrderUpdateJob(fakeJob({ requestId: "", email: "", sweep: "active" }, "sweep"), undefined, { ...forbidden(), clockLook: async () => void (looks++, await c.pollOnce()) });
      assert.deepEqual([looks, (await queuedOf("schedule")).length, (await Q.getJobSchedulers()).length], [1, 0, 0], "no request, the clock was asked once, both schedulers are gone");
      assert.deepEqual(await pending(Q), [0, 0], "and the jobs they had waiting with them");
      assert.deepEqual((await leaseOf("update_active")).ticks.length, 0, "no tick: nothing was due");
      // The function the worker calls at start, on its own.
      await Q.upsertJobScheduler("order-update-active", { every: 6 * 60 * 60_000 }, { name: "sweep", data: { requestId: "", email: "", sweep: "active" }, opts: { attempts: 1 } });
      assert.deepEqual(await retireUpdateSchedulers(Q), ["order-update-active"]);
      assert.deepEqual(await retireUpdateSchedulers(Q), [], "a second call finds nothing");
      assert.deepEqual([(await Q.getJobSchedulers()).length, await pending(Q)], [0, [0, 0]]);
      // A leftover job that arrives exactly when a time is due: the clock runs that time — once.
      at("2026-11-02 18:15:05");
      await runOrderUpdateJob(fakeJob({ requestId: "", email: "", sweep: "active" }, "sweep"), undefined, { ...forbidden(), clockLook: () => c.pollOnce() });
      await runOrderUpdateJob(fakeJob({ requestId: "", email: "", sweep: "active" }, "sweep"), undefined, { ...forbidden(), clockLook: () => c.pollOnce() });
      const made = await queuedOf("schedule");
      assert.deepEqual([made.length, ticksOf(await leaseOf("update_active"))], [1, ["ran@18:15"]], "one request for the time, however many leftover jobs arrive");
      await runJob(String(made[0]._id), a.email);
      assert.equal(tickText((await leaseOf("update_active")).lastTick), "finished — ok 1, failed 0");
      liveJobIds.update.push(...(await jobIdsIn(Q)));
    });

    // ── T7 ───────────────────────────────────────────────────────────────────
    console.log("\nTHE OUT-FOR-DELIVERY SPELLING (T7) — the order API stores \"ofd\"");
    await check("one source for the spellings: STATUS_ALIASES — 'ofd', 'Out for delivery' and 'Dispatched' are out for delivery; 'Shipped' is not", async () => {
      assert.deepEqual([...ofdStatusSpellings()].sort(), [...new Set([...STATUS_ALIASES["out for delivery"], "Dispatched"].map((s) => s.toLowerCase()))].sort());
      assert.ok(STATUS_ALIASES["out for delivery"].includes("ofd"), "the alias list knows the API's spelling");
      for (const s of ["ofd", "OFD", " ofd ", "Out for delivery", "Out For Delivery", "Dispatched"]) assert.equal(isOutForDelivery(s), true, s);
      for (const s of ["Shipped", "Approved", "Delivered", "Cancelled", "", null, undefined, "offd"]) assert.equal(isOutForDelivery(s), false, String(s));
      assert.deepEqual(OFD_STATUSES, ["Out for delivery", "Dispatched"]);
    });

    let ofdRequest = "";
    let ofdAccount!: Acc;
    const ofdUnits: Record<string, string> = {};
    await check("the 2-hourly recheck selects a unit stored as 'ofd' (and the page spellings), and nothing else", async () => {
      await reset("2026-11-02 10:00");
      ofdAccount = await account("o");
      const other = await account("n");
      ofdUnits.ofd = (await unit(ofdAccount, "ofd")).unitId; // the API's spelling, no OTP: cash on delivery
      ofdUnits.ofdWithOtp = (await unit(ofdAccount, "ofd", "654321")).unitId;
      ofdUnits.page = (await unit(ofdAccount, "Out for delivery")).unitId;
      ofdUnits.dispatched = (await unit(ofdAccount, "Dispatched", "222222")).unitId;
      ofdUnits.shipped = (await unit(ofdAccount, "Shipped")).unitId;
      await unit(other, "Shipped");
      await unit(other, "Delivered");
      assert.deepEqual((await ofdStatusKeys(U)).sort(), ["Dispatched", "Out for delivery", "ofd"]);
      const c = workerClock(THIS, sweepRunners());
      at("2026-11-02 10:44");
      await c.pollOnce();
      at("2026-11-02 10:45:05");
      await c.pollOnce();
      const rows = await queuedOf("ofd");
      assert.equal(rows.length, 1, "one out-for-delivery request");
      const r = rows[0];
      ofdRequest = String(r._id);
      assert.deepEqual(r.accounts.map((a) => a.email), [ofdAccount.email], "only the account that has units out for delivery");
      assert.deepEqual(r.accounts[0].units.map((u) => u.unitId).sort(), [ofdUnits.ofd, ofdUnits.ofdWithOtp, ofdUnits.page, ofdUnits.dispatched].sort(), "the two 'ofd' units, the 'Out for delivery' one and the 'Dispatched' one — not the Shipped ones");
      const p = r.params as Record<string, unknown>;
      assert.deepEqual([p.statuses_to_update, (p.status_keys as string[]).slice().sort(), p.schedule_name, p.schedule_slot, r.selectionMethod, r.source], [["Out for delivery", "Dispatched"], ["Dispatched", "Out for delivery", "ofd"], "update_ofd", "2026-11-02 10:45", "schedule_ofd", "ofd"]);
      const lease = await leaseOf("update_ofd");
      assert.deepEqual([tickText(lease.lastTick), lease.runRef, r.schedule?.token], ["ran on Mac-check — 4 unit(s) on 1 account(s)", { kind: "update", id: ofdRequest }, lease.holder?.token]);
      assert.deepEqual(settingsOf(factsOfUpdate(r)).find((s) => s.label === "Schedule"), { label: "Schedule", value: "Out-for-delivery recheck · 2026-11-02 10:45 · ran on Mac-check" });
      liveJobIds.update.push(...(await jobIdsIn(Q)));
    });

    await check("an 'ofd' unit whose API answer has no OTP gets the page pass (no Chrome: the page read is injected); one whose page shows none either — cash on delivery — is no failure, no event, no alert", async () => {
      assert.ok(ofdRequest, "the request of the check above");
      const eventsBefore = await OrderUnitEvent.countDocuments({ userId: invented });
      untouched();
      // The page shows an OTP for the 'Out for delivery' unit only; the 'ofd' one is cash on delivery and has none.
      await runJob(ofdRequest, ofdAccount.email, jobDeps({ [ofdUnits.page]: "909090" }));
      assert.equal(touched.apiReads.length, 4, "all four read over the API first");
      assert.deepEqual(touched.pageReads.slice().sort(), [ofdUnits.ofd, ofdUnits.page].sort(), "the page pass: the two out-for-delivery units the API gave no OTP for — the 'ofd' one among them; not the two that have their OTP");
      assert.equal(touched.tabs, 1, "one tab for the account");
      const r = await requestOf(ofdRequest);
      const a = r.accounts[0];
      assert.deepEqual([r.status, a.status, a.done, a.failed, a.otpNew, a.via, a.message], ["done", "done", 4, 0, 1, "api+scrape", "4 refreshed, 1 new OTP · api+scrape"]);
      const lines = r.logs.map((l) => `${l.level}: ${l.message}`);
      assert.ok(lines.includes(`info: ${ofdAccount.email}: 2 out-for-delivery unit(s) without an API OTP — scraping those for OTP`), lines.join("\n"));
      assert.deepEqual(lines.filter((l) => /^(warn|error):/.test(l)), [], "no warning, no error — a unit without an OTP is not one");
      const stored = await Order.find({ userId: invented, unit_id: { $in: [ofdUnits.ofd, ofdUnits.page] } }).select("unit_id delivery_otp status_key").lean();
      assert.deepEqual(stored.map((u) => [u.unit_id, u.status_key, u.delivery_otp || null]).sort(), [[ofdUnits.ofd, "ofd", null], [ofdUnits.page, "Out for delivery", "909090"]].sort());
      await taskLog.flush();
      assert.deepEqual((await tasksOf(ofdRequest)).map((t) => t.status), ["success", "success", "success", "success"]);
      // Only the OTP the page gave is an event; the cash-on-delivery unit raised nothing.
      const events = await OrderUnitEvent.find({ userId: invented }).select("type unitId").lean();
      assert.deepEqual(events.slice(eventsBefore).map((e) => [e.type, e.unitId]), [["otp_new", ofdUnits.page]]);
      assert.equal(await OrderUnitEvent.countDocuments({ userId: invented, unitId: ofdUnits.ofd }), 0);
      await sleep(1500);
      assert.equal(await db.collection("alerts").countDocuments({ userId: invented }), 0, "no alert");
      const lease = await leaseOf("update_ofd");
      assert.deepEqual([lease.holder, tickText(lease.lastTick)], [null, "finished — ok 4, failed 0, new OTP 1"]);
    });

    // ── invoice sweep ────────────────────────────────────────────────────────
    console.log("\nINVOICE SWEEP — one job per fixed time, run under the lease");
    const result = (c1: number, d1: number, c2: number, r2: number, f = 0): InvoiceSweepResult => ({ phase1: { candidates: c1, downloaded: d1, failed: 0 }, phase2: { candidates: c2, recaptured: r2, settled: r2, failed: f } });
    const sweeps: Array<{ userId: string; cancelled: () => boolean }> = [];
    /** The sweep, standing in: it downloads nothing and answers what the case says. */
    const sweepOf = (answer: InvoiceSweepResult | (() => Promise<InvoiceSweepResult>)): InvoiceSweepDeps["sweep"] => async (userId, opts = {}) => {
      sweeps.push({ userId, cancelled: opts.cancelled || (() => false) });
      return typeof answer === "function" ? answer() : answer;
    };
    const invoiceDeps = (answer: InvoiceSweepResult | (() => Promise<InvoiceSweepResult>) = result(0, 0, 0, 0)): Partial<InvoiceSweepDeps> => ({ queue: invoiceQueue, sweep: sweepOf(answer), storeEnabled: () => true, asRequest: false });
    const invoiceJobs = async () => (await QI.getJobs(["waiting", "delayed", "active", "paused", "prioritized"])).map((j) => ({ id: String(j.id), name: j.name, data: j.data }));
    const ledgerOf = (id: string) => OrderJobRun.findById(id).lean();

    await check("two clocks on one fixed time queue exactly ONE job — { sweep: 'scheduled', slotAt, token }, id sched-invoice-<time> — and the lease waits for it without a run", async () => {
      assert.equal(sweepWords(result(3, 3, 2, 1, 1)), "captured 3/3, re-captured 1/2, failed 1");
      assert.equal(sweepWords(result(3, 3, 2, 2)), "captured 3/3, re-captured 2");
      assert.equal(sweepWords(result(0, 0, 0, 0)), "nothing to do");
      await reset("2026-11-02 10:00");
      const runner = invoiceSweepRunner(invoiceDeps());
      const [c1, c2] = [workerClock(THIS, { invoice_sweep: runner }), workerClock(MAC2, { invoice_sweep: runner })];
      at("2026-11-02 12:34");
      await Promise.all([c1.pollOnce(), c2.pollOnce()]);
      at("2026-11-02 12:35:05");
      await Promise.all([c1.pollOnce(), c2.pollOnce()]);
      const lease = await leaseOf("invoice_sweep");
      const jobs = await invoiceJobs();
      const slot = ist("2026-11-02 12:35");
      assert.deepEqual(jobs, [{ id: `sched-invoice-${slot.getTime()}`, name: "sweep", data: { sweep: "scheduled", slotAt: slot.toISOString(), token: lease.holder?.token } }]);
      liveJobIds.invoice.push(jobs[0].id);
      assert.deepEqual([ticksOf(lease), lease.runRef, tickText(lease.lastTick), [c1, c2].filter((c) => c.holds("invoice_sweep")).length], [["ran@12:35"], null, `ran on ${lease.holder?.host}`, 1]);
      // Two processes that both believed they had the time: the job id lets one job through.
      const claim: ScheduleClaim = { userId: U, name: "invoice_sweep", token: String(lease.holder?.token), slotAt: slot, attempt: 1, manual: false, by: "schedule", takenAt: deps.now(), holder: lease.holder!, lateMs: 0, missedBefore: 0, replaced: null, secondStartOf: "" };
      assert.deepEqual(await runner.run(claim), {});
      assert.equal((await invoiceJobs()).length, 1);
      // Nothing is queued when this machine cannot store an invoice, or an invoice run is already going.
      assert.deepEqual(await invoiceSweepRunner({ ...invoiceDeps(), storeEnabled: () => false }).run(claim), { skipped: true, reason: "INVOICE_S3_BUCKET is not set on this machine" });
      const going = await OrderJobRun.create({ userId: invented, type: "invoices", trigger: "user", by: "asha", status: "running", total: 0, startedAt: new Date(), message: "Invoice download running…", meta: {} });
      await QI.drain(true);
      assert.deepEqual(await runner.run(claim), { skipped: true, reason: "an invoice run is already in progress" });
      assert.equal((await invoiceJobs()).length, 0, "nothing queued behind a run that is going");
      await OrderJobRun.deleteOne({ _id: going._id });
      // (the job of this time, for the next check)
      assert.deepEqual(await runner.run(claim), {});
      assert.equal((await invoiceJobs()).length, 1);
    });

    let sweepRun = "";
    await check("the job runs under the lease: its ledger run becomes the lease's run, the clock renews it meanwhile, and the end gives the lease back with the counts", async () => {
      const [job] = await invoiceJobs();
      assert.ok(job, "the job of the check above");
      const token = String(job.data.token);
      let finish: (r: InvoiceSweepResult) => void = () => undefined;
      const going = new Promise<InvoiceSweepResult>((resolve) => (finish = resolve));
      // The worker of the machine that took the time (either of the two above may have): another process of it.
      const taker = (await leaseOf("invoice_sweep")).holder;
      assert.ok(taker);
      const c = workerClock({ systemId: taker.systemId, host: taker.host }, { invoice_sweep: invoiceSweepRunner(invoiceDeps()) });
      at("2026-11-02 12:35:20");
      const before = sweeps.length;
      const done = runSweepJob(U, job.data, invoiceDeps(() => going));
      for (let i = 0; i < 100 && sweeps.length === before; i++) await sleep(50);
      assert.equal(sweeps.length, before + 1, "the sweep was started");
      let lease = await leaseOf("invoice_sweep");
      assert.ok(lease.runRef?.kind === "invoices" && lease.holder?.token === token, "the run is attached to the same claim");
      sweepRun = String(lease.runRef?.id);
      const row = await ledgerOf(sweepRun);
      assert.deepEqual([row?.status, row?.trigger, row?.by, (row?.meta as { params?: Record<string, unknown> })?.params], ["running", "system-cron", "schedule", { force: false, cap_per_phase: config.invoiceMaxPerRun, schedule_name: "invoice_sweep", schedule_slot: "2026-11-02 12:35", ran_on: "Mac-check" }]);
      at("2026-11-02 12:39:30");
      await c.pollOnce();
      lease = await leaseOf("invoice_sweep");
      assert.deepEqual([lease.holder?.token, lease.expiresAt?.getTime()], [token, clockMs + LEASE_MS], "renewed while the ledger run is open");
      finish(result(3, 3, 2, 1, 1));
      const out = await done;
      assert.equal(out.skipped, false);
      lease = await leaseOf("invoice_sweep");
      assert.deepEqual([lease.holder, lease.endedAs, tickText(lease.lastTick), lease.lastTick?.runRef, ticksOf(lease)], [null, "finished", "finished — captured 3/3, re-captured 1/2, failed 1", { kind: "invoices", id: sweepRun }, ["ran@12:35", "finished@12:35"]]);
      const closed = await ledgerOf(sweepRun);
      assert.deepEqual([closed?.status, closed?.message, closed?.success, closed?.failed], ["completed", "Captured 3/3, re-captured 1/2 (settled 1)", 4, 1]);
      // 15:35 — nothing to capture: one job, one ledger run, "ran — nothing to do" (C4's wording).
      await QI.drain(true);
      at("2026-11-02 15:35:05");
      await c.pollOnce();
      const [next] = await invoiceJobs();
      liveJobIds.invoice.push(next.id);
      assert.equal((await runSweepJob(U, next.data, invoiceDeps(result(0, 0, 0, 0)))).skipped, false);
      lease = await leaseOf("invoice_sweep");
      assert.deepEqual([lease.holder, tickText(lease.lastTick), ticksOf(lease).slice(-2)], [null, `ran on ${taker.host} — nothing to do`, ["ran@15:35", "ran@15:35"]]);
    });

    await check("a job whose lease is gone runs nothing; a sweep that breaks gives the lease back as failed; a leftover job of the old scheduler runs no sweep", async () => {
      await reset("2026-11-02 10:00");
      const c = workerClock(THIS, { invoice_sweep: invoiceSweepRunner(invoiceDeps()) });
      at("2026-11-02 12:34");
      await c.pollOnce();
      at("2026-11-02 12:35:05");
      await c.pollOnce();
      const [job] = await invoiceJobs();
      liveJobIds.invoice.push(job.id);
      // The job waits (this machine slept); at 15:35 Mac-2 takes the sweep over and holds it.
      const two = recorder(() => ({}));
      at("2026-11-02 15:35:05");
      await workerClock(MAC2, { invoice_sweep: two.runner }).pollOnce();
      assert.equal((await leaseOf("invoice_sweep")).holder?.systemId, MAC2.systemId);
      const runsBefore = await OrderJobRun.countDocuments({ userId: invented, type: "invoices" });
      const before = sweeps.length;
      assert.deepEqual(await runSweepJob(U, job.data, invoiceDeps(result(5, 5, 0, 0))), { skipped: true, reason: "its lease was taken over, or given up, before the sweep's turn came" });
      assert.deepEqual([sweeps.length, await OrderJobRun.countDocuments({ userId: invented, type: "invoices" })], [before, runsBefore], "no sweep, no ledger run");
      assert.equal((await leaseOf("invoice_sweep")).holder?.token, two.calls[0].token, "Mac-2's lease is as it was");

      // The job's turn comes while another invoice run (a download somebody started) is going: no sweep, and the
      // lease is given back at once — "skipped", with why.
      await reset("2026-11-02 10:00");
      const c1 = workerClock(THIS, { invoice_sweep: invoiceSweepRunner(invoiceDeps()) });
      at("2026-11-02 12:34");
      await c1.pollOnce();
      at("2026-11-02 12:35:05");
      await c1.pollOnce();
      const [waiting] = await invoiceJobs();
      const other = await OrderJobRun.create({ userId: invented, type: "invoices", trigger: "user", by: "asha", status: "running", total: 0, startedAt: new Date(), message: "Invoice download running…", meta: {} });
      const sweepsBefore = sweeps.length;
      assert.deepEqual(await runSweepJob(U, waiting.data, invoiceDeps(result(5, 5, 0, 0))), { skipped: true, reason: "an invoice run is already in progress" });
      const given = await leaseOf("invoice_sweep");
      assert.deepEqual([sweeps.length, given.holder, given.endedAs, tickText(given.lastTick), ticksOf(given)], [sweepsBefore, null, "skipped", "skipped — an invoice run is already in progress", ["ran@12:35", "skipped@12:35"]]);
      await OrderJobRun.deleteOne({ _id: other._id });

      // A sweep that throws: the ledger run and the lease both say failed.
      await reset("2026-11-02 10:00");
      const c2 = workerClock(THIS, { invoice_sweep: invoiceSweepRunner(invoiceDeps()) });
      at("2026-11-02 12:34");
      await c2.pollOnce();
      at("2026-11-02 12:35:05");
      await c2.pollOnce();
      const [breaking] = await invoiceJobs();
      await assert.rejects(() => runSweepJob(U, breaking.data, invoiceDeps(async () => Promise.reject(new Error("S3 said no\nstack")))), /S3 said no/);
      const lease = await leaseOf("invoice_sweep");
      assert.deepEqual([lease.holder, lease.endedAs, tickText(lease.lastTick)], [null, "failed", "failed — S3 said no"]);
      assert.equal((await ledgerOf(String(lease.runRef?.id)))?.status, "failed");

      // The old scheduler's job: no token. It runs no sweep; its scheduler leaves the queue; the clock is asked to look.
      await QI.drain(true);
      await QI.upsertJobScheduler("order-invoice-sweep", { every: 3 * 60 * 60_000 }, { name: "sweep", data: { sweep: "scheduled" }, opts: { attempts: 1 } });
      assert.equal((await QI.getJobSchedulers()).length, 1);
      let looks = 0;
      const n = sweeps.length;
      const out = await runSweepJob(U, { sweep: "scheduled" }, { ...invoiceDeps(result(5, 5, 0, 0)), clockLook: async () => void looks++ });
      assert.deepEqual([out.skipped, sweeps.length, looks, (await QI.getJobSchedulers()).length, await pending(QI)], [true, n, 1, 0, [0, 0]], "no sweep; the scheduler and the job it had waiting are gone");
      assert.deepEqual(await retireLegacySchedulers(["invoice_sweep"], { update: invoiceQueue, invoice: invoiceQueue }), [], "nothing left to remove");
    });

    console.log("\nCAPTURE NEWLY DELIVERED NOW — the real route, on a listener of this process");
    Object.assign(invoiceSweepDefaults, { queue: invoiceQueue, storeEnabled: () => true, asRequest: false });
    const { default: express } = await import("express");
    const { orderFetchRouter } = await import("../routes/orderFetch.js");
    const app = express();
    app.use(express.json());
    app.use("/api/orders", orderFetchRouter);
    server = await new Promise<import("node:http").Server>((resolve) => {
      const s = app.listen(0, "127.0.0.1", () => resolve(s));
    });
    const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}/api/orders`;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    type Json = Record<string, any>;
    const post = async (path: string, body: unknown): Promise<{ status: number; body: Json }> => {
      const res = await fetch(`${base}${path}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body), signal: AbortSignal.timeout(30_000) });
      return { status: res.status, body: (await res.json().catch(() => ({}))) as Json };
    };

    await check("while another machine holds the lease: 409 'The invoice sweep is running on Mac-2 since 12:35', with who holds it — and nothing is queued", async () => {
      await reset("2026-11-02 10:00");
      const two = recorder(() => ({}));
      const c2 = workerClock(MAC2, { invoice_sweep: two.runner });
      at("2026-11-02 12:34");
      await c2.pollOnce();
      at("2026-11-02 12:35:05");
      await c2.pollOnce();
      at("2026-11-02 12:37");
      const before = JSON.stringify(await leaseOf("invoice_sweep"));
      const res = await post("/invoices/sweep", { by: "asha", userId: U });
      assert.deepEqual(res, { status: 409, body: { error: "The invoice sweep is running on Mac-2 since 12:35", code: "held_elsewhere", holder: { systemId: MAC2.systemId, host: "Mac-2", since: ist("2026-11-02 12:35:05").toISOString(), slotAt: ist("2026-11-02 12:35").toISOString(), manual: false } } });
      assert.deepEqual([(await invoiceJobs()).length, JSON.stringify(await leaseOf("invoice_sweep"))], [0, before], "nothing queued, the lease untouched");
      // A lease Mac-2 let run out is no reason to refuse.
      at("2026-11-02 12:45");
      assert.equal((await post("/invoices/sweep", { by: "asha", userId: U })).status, 200);
    });

    await check("free, and this machine's worker is on the shared clock: 200, one job carrying the claim; the worker's job runs under it — 'ran — started by hand by asha', then 'finished — …'", async () => {
      await reset("2026-11-02 10:00");
      const c = workerClock(THIS, { invoice_sweep: invoiceSweepRunner(invoiceDeps()) });
      at("2026-11-02 10:01");
      await c.pollOnce(); // the worker's presence
      assert.equal(await clockProcessUp("invoice_sweep"), true);
      at("2026-11-02 10:02");
      const res = await post("/invoices/sweep", { by: "asha", userId: U });
      assert.deepEqual(res, { status: 200, body: { message: "Invoice sweep queued" } });
      let lease = await leaseOf("invoice_sweep");
      const jobs = await invoiceJobs();
      assert.deepEqual(jobs.map((j) => [j.name, j.data]), [["sweep", { sweep: "manual", by: "asha", token: lease.holder?.token }]]);
      assert.deepEqual([lease.manual, lease.slotAt, lease.holder?.systemId, lease.holder?.role, clockStamp(lease.nextAt), lease.ticks.length], [true, null, config.systemId, "worker", "2 Nov 12:35", 0], "taken by hand: no fixed time is used up");
      // A second press while it waits: this machine has the lease — queued behind, to take the lease itself at its turn.
      const again = await post("/invoices/sweep", { by: "ravi", userId: U });
      assert.deepEqual(again, { status: 200, body: { message: "Invoice sweep queued — it starts when the sweep running on this machine has finished" } });
      assert.deepEqual((await invoiceJobs()).map((j) => j.data).filter((d) => !d.token), [{ sweep: "manual", by: "ravi" }]);
      // The worker's clock keeps the claim alive until the job starts.
      at("2026-11-02 10:02:40");
      await c.pollOnce();
      assert.equal((await leaseOf("invoice_sweep")).expiresAt?.getTime(), clockMs + LEASE_MS);
      const out = await runSweepJob(U, jobs[0].data, invoiceDeps(result(2, 2, 0, 0)));
      assert.equal(out.skipped, false);
      lease = await leaseOf("invoice_sweep");
      assert.deepEqual([lease.holder, ticksOf(lease), lease.ticks[0].reason, tickText(lease.lastTick)], [null, ["ran@?", "finished@?"], "started by hand by asha", "finished — captured 2/2, re-captured 0"]);
      const row = await ledgerOf(String(lease.runRef?.id));
      assert.deepEqual([row?.trigger, row?.by, row?.status, (row?.meta as { params?: Record<string, unknown> })?.params], ["user", "asha", "completed", { force: false, cap_per_phase: config.invoiceMaxPerRun }]);
      // The second job's turn: it takes the lease itself and runs.
      const n = sweeps.length;
      assert.equal((await runSweepJob(U, { sweep: "manual", by: "ravi" }, invoiceDeps(result(0, 0, 0, 0)))).skipped, false);
      lease = await leaseOf("invoice_sweep");
      assert.deepEqual([sweeps.length, lease.holder, ticksOf(lease).length, lease.ticks[2].reason, tickText(lease.lastTick)], [n + 1, null, 4, "started by hand by ravi", "ran on Mac-check — nothing to do"]);
    });

    await check("in between — this machine's worker is not on the shared clock (old code, or not running): the lease is not kept; a tick says so; the job goes out as it always did", async () => {
      await reset("2026-11-02 10:00");
      assert.equal(await clockProcessUp("invoice_sweep"), false, "no presence row of a worker");
      at("2026-11-02 10:02");
      const res = await post("/invoices/sweep", { by: "asha", userId: U });
      assert.deepEqual(res, { status: 200, body: { message: "Invoice sweep queued" } });
      const lease = await leaseOf("invoice_sweep");
      assert.deepEqual((await invoiceJobs()).map((j) => j.data), [{ sweep: "manual", by: "asha" }], "no claim on the job: a worker from before the clock runs it as it always did");
      assert.deepEqual([lease.holder, lease.armedAt, lease.endedAs, clockStamp(lease.nextAt), tickText(lease.lastTick)], [null, null, "ran", "2 Nov 12:35", "ran on Mac-check — started by hand by asha — queued for the worker on Mac-check, which is not on the shared clock, so no lease is held for it"]);
      // An older worker's presence (seen 4 minutes ago) does not count as up either.
      const c = workerClock(THIS, { invoice_sweep: invoiceSweepRunner(invoiceDeps()) });
      await c.pollOnce();
      at("2026-11-02 10:06:30");
      assert.equal(await clockProcessUp("invoice_sweep"), false);
      // The same job at a worker that IS on the clock: it takes the lease itself — and is refused while another machine has it.
      await claimManual(U, "invoice_sweep", "ravi", { me: { ...MAC2, pid: 7 }, deps });
      const n = sweeps.length;
      assert.deepEqual(await runSweepJob(U, { sweep: "manual", by: "asha" }, invoiceDeps(result(1, 1, 0, 0))), { skipped: true, reason: "The invoice sweep is running on Mac-2 since 10:06" });
      assert.equal(sweeps.length, n);
    });

    await check("another machine takes the sweep over while this one is in the middle of it: this machine's sweep stops between two orders, and writes nothing on the lease", async () => {
      await reset("2026-11-02 10:00");
      const runner = invoiceSweepRunner(invoiceDeps());
      const c = workerClock(THIS, { invoice_sweep: runner });
      at("2026-11-02 12:34");
      await c.pollOnce();
      at("2026-11-02 12:35:05");
      await c.pollOnce();
      const [job] = await invoiceJobs();
      liveJobIds.invoice.push(job.id);
      let orders = 0;
      let stoppedAt = -1;
      const before = sweeps.length;
      // A sweep of 50 orders that looks at `cancelled` between two of them, as the real one does.
      const long = async (): Promise<InvoiceSweepResult> => {
        const mine = sweeps[sweeps.length - 1];
        for (orders = 0; orders < 50; orders++) {
          if (mine.cancelled()) {
            stoppedAt = orders;
            break;
          }
          await sleep(40);
        }
        return result(50, orders, 0, 0);
      };
      const done = runSweepJob(U, job.data, invoiceDeps(long));
      for (let i = 0; i < 100 && (sweeps.length === before || orders < 2); i++) await sleep(20);
      const token = String(job.data.token);
      const runId = String((await leaseOf("invoice_sweep")).runRef?.id);
      // This machine "sleeps"; Mac-2 takes 15:35 over; this machine's clock then looks.
      const two = recorder(() => ({}));
      at("2026-11-02 15:35:05");
      await workerClock(MAC2, { invoice_sweep: two.runner }).pollOnce();
      at("2026-11-02 15:36");
      await c.pollOnce();
      const out = await done;
      assert.ok(stoppedAt >= 2 && stoppedAt < 50, `stopped between two orders (after ${stoppedAt})`);
      assert.equal(out.skipped, false);
      const row = await ledgerOf(runId);
      assert.deepEqual([row?.status, row?.message], ["stopped", `Superseded — Mac-2 took over the invoice sweep at 15:35 — Captured ${stoppedAt}/50, re-captured 0/0 (settled 0)`]);
      const lease = await leaseOf("invoice_sweep");
      assert.deepEqual([lease.holder?.token, lease.holder?.systemId, ticksOf(lease)], [two.calls[0].token, MAC2.systemId, ["ran@12:35", "took_over@15:35", "superseded@12:35"]], "the lease is Mac-2's; this machine's end wrote nothing on it");
      assert.notEqual(token, two.calls[0].token);
    });
    await check("a sweep of the clock that has orders is a request of the log saying which schedule, time and machine; a takeover stops this machine's sweep AND that request, the way Batch requests → Cancel does", async () => {
      await reset("2026-11-02 10:00");
      const acc = await account("i");
      const [o1, o2] = [await unit(acc, "Delivered"), await unit(acc, "Delivered")];
      const stops: Array<[string, string, string]> = [];
      // What stands in for Batch requests → Cancel: it is asked, and says what it was asked.
      const withRequest = (sweep: InvoiceSweepDeps["sweep"]): Partial<InvoiceSweepDeps> => ({ queue: invoiceQueue, sweep, storeEnabled: () => true, asRequest: true, stopRequest: async (userId, requestId, by) => void stops.push([userId, requestId, by]) });
      let release: () => void = () => undefined;
      const held = new Promise<void>((resolve) => (release = resolve));
      const taken: string[] = [];
      // The sweep as the real one behaves towards whoever records it: its orders before the phase starts, each order
      // as it is taken and answered, and a look at `cancelled` between two orders.
      const sweep: InvoiceSweepDeps["sweep"] = async (_userId, opts = {}) => {
        await opts.onPhase?.("capture", [o1.orderId, o2.orderId]);
        for (const orderId of [o1.orderId, o2.orderId]) {
          if (opts.cancelled?.()) break;
          await opts.onStart?.(orderId);
          taken.push(orderId);
          await held;
          await opts.onResult?.({ orderId, status: "downloaded", units: 1, parts: ["product"], sizeKb: 12 });
        }
        return result(2, taken.length, 0, 0);
      };
      const deps1 = withRequest(sweep);
      const c = workerClock(THIS, { invoice_sweep: invoiceSweepRunner(deps1) });
      at("2026-11-02 12:34");
      await c.pollOnce();
      at("2026-11-02 12:35:05");
      await c.pollOnce();
      const [job] = await invoiceJobs();
      liveJobIds.invoice.push(job.id);
      const done = runSweepJob(U, job.data, deps1);
      const sweepRequest = () => OrderInvoiceRequest.findOne({ userId: invented, "params.sweep": true }).lean();
      let request: Awaited<ReturnType<typeof sweepRequest>> = null;
      // Until the request says both: its schedule line, and that the first order has been taken.
      for (let i = 0; i < 200 && !(request?.params?.schedule_name && request.tasks.some((t) => t.status === "in_progress")); i++) {
        await sleep(50);
        request = await sweepRequest();
      }
      assert.ok(request, "the run is a request of the log");
      const requestId = String(request._id);
      requestIds.push(requestId);
      assert.deepEqual([request.source, request.status, request.systemId, request.tasks.map((t) => [t.orderId, t.status])], ["system-cron", "in_progress", config.systemId, [[o1.orderId, "in_progress"], [o2.orderId, "queued"]]]);
      assert.deepEqual([request.params.sweep, request.params.schedule_name, request.params.schedule_slot, request.params.ran_on], [true, "invoice_sweep", "2026-11-02 12:35", "Mac-check"]);
      // Batch requests → Details → SETTINGS.
      const settings = settingsOf(factsOfInvoice(request));
      assert.deepEqual(settings.find((x) => x.label === "Schedule"), { label: "Schedule", value: "Invoice sweep · 2026-11-02 12:35 · ran on Mac-check" });
      assert.deepEqual(settings.find((x) => x.label === "Capture (delivered, no invoice yet)"), { label: "Capture (delivered, no invoice yet)", value: "2" });
      const lease = await leaseOf("invoice_sweep");
      const ledger = await ledgerOf(String(lease.runRef?.id));
      assert.deepEqual([ledger?.refId, (ledger?.meta as { params?: Record<string, unknown> })?.params?.schedule_slot], [requestId, "2026-11-02 12:35"], "the lease's run is the ledger row, which names the request");
      // This machine "sleeps" with the first order in hand; Mac-2 takes 15:35 over; this machine's clock then looks.
      const two = recorder(() => ({}));
      at("2026-11-02 15:35:05");
      await workerClock(MAC2, { invoice_sweep: two.runner }).pollOnce();
      at("2026-11-02 15:36");
      await c.pollOnce();
      assert.deepEqual(stops, [[U, requestId, "the schedule clock: Mac-2 took over the invoice sweep at 15:35"]], "the request is stopped once, by name");
      release();
      const out = await done;
      assert.deepEqual([out.skipped, taken], [false, [o1.orderId]], "the order in hand finished; the second was never taken");
      const closed = await ledgerOf(String(lease.runRef?.id));
      assert.deepEqual([closed?.status, closed?.message], ["stopped", "Superseded — Mac-2 took over the invoice sweep at 15:35 — Captured 1/2, re-captured 0/0 (settled 0)"]);
      const after = await leaseOf("invoice_sweep");
      assert.deepEqual([after.holder?.systemId, ticksOf(after)], [MAC2.systemId, ["ran@12:35", "took_over@15:35", "superseded@12:35"]], "the lease is Mac-2's; this machine's end wrote nothing on it");
      await c.pollOnce();
      assert.equal(stops.length, 1, "told once");
      // A sweep started by hand is nobody's fixed time: its request carries no schedule line.
      await reset("2026-11-02 10:00");
      const acc2 = await account("j");
      const o3 = await unit(acc2, "Delivered");
      const byHand: InvoiceSweepDeps["sweep"] = async (_userId, opts = {}) => {
        await opts.onPhase?.("capture", [o3.orderId]);
        await opts.onStart?.(o3.orderId);
        await opts.onResult?.({ orderId: o3.orderId, status: "downloaded", units: 1, parts: ["product"], sizeKb: 12 });
        return result(1, 1, 0, 0);
      };
      assert.equal((await runSweepJob(U, { sweep: "manual", by: "asha" }, withRequest(byHand))).skipped, false);
      const pressed = await OrderInvoiceRequest.findOne({ userId: invented, source: "user" }).lean();
      assert.ok(pressed);
      requestIds.push(String(pressed._id));
      assert.deepEqual([pressed.createdBy, pressed.status, "schedule_name" in pressed.params, settingsOf(factsOfInvoice(pressed)).some((x) => x.label === "Schedule")], ["asha", "completed", false, false]);
      assert.equal(tickText((await leaseOf("invoice_sweep")).lastTick), "finished — captured 1/1, re-captured 0");
    });
  } finally {
    // Stopped from outside: the interrupt removes what this run made (onInterrupt above) and ends the process.
    if (isInterrupted()) await standStill();
    console.log = said.log;
    console.warn = said.warn;
    Object.assign(scheduleDefaults, realDefaults);
    Object.assign(invoiceSweepDefaults, realInvoiceDefaults);
    if (server) await new Promise<void>((resolve) => (server as import("node:http").Server).close(() => resolve()));

    // Redis: the throwaway queues go; the live queues are asked whether they hold a job of this run.
    await Q.obliterate({ force: true }).catch(() => undefined);
    await QI.obliterate({ force: true }).catch(() => undefined);
    await Q.close();
    await QI.close();
    const db15 = new Redis({ host: connection.host, port: connection.port, password: connection.password, db: 15, maxRetriesPerRequest: 2 });
    const leftKeys = (await db15.keys(`bull:verify-ss-*-${run}:*`)).length;
    await db15.quit();
    const db0 = new Redis({ host: connection.host, port: connection.port, password: connection.password, db: 0, maxRetriesPerRequest: 2 });
    let inLive = 0;
    for (const id of [...new Set(liveJobIds.update)]) inLive += await db0.exists(`bull:order-update:${id}`);
    for (const id of [...new Set(liveJobIds.invoice)]) inLive += await db0.exists(`bull:order-invoice:${id}`);
    await db0.quit();
    console.log(`\nredis: 2 throwaway queues obliterated (${leftKeys} key(s) left in db 15) · jobs of this run found in the LIVE queues (db 0, ${new Set(liveJobIds.update).size} update + ${new Set(liveJobIds.invoice).size} invoice job id(s) asked): ${inLive}`);
    if (leftKeys || inLive) failures.push("redis");

    // Mongo: everything of the invented workspace, then a recount.
    await taskLog.flush().catch(() => undefined);
    const ids = [...new Set([...requestIds, ...(await OrderUpdateRequest.find({ userId: invented }).select("_id").lean()).map((r) => String(r._id))])];
    const events = (await OrderUnitEvent.deleteMany({ userId: invented })).deletedCount;
    await sleep(2500);
    const del = {
      order_update_requests: (await OrderUpdateRequest.deleteMany({ userId: invented })).deletedCount,
      order_details: (await Order.deleteMany({ userId: invented })).deletedCount,
      platformids: (await PlatformId.deleteMany({ userId: invented })).deletedCount,
      order_job_runs: (await OrderJobRun.deleteMany({ userId: invented })).deletedCount,
      order_sync_tasks: (await OrderSyncTask.deleteMany({ $or: [{ userId: invented }, { requestId: { $in: ids } }] })).deletedCount,
      order_unit_events: events + (await OrderUnitEvent.deleteMany({ userId: invented })).deletedCount,
      order_invoice_requests: (await OrderInvoiceRequest.deleteMany({ userId: invented })).deletedCount,
      alerts: (await db.collection("alerts").deleteMany({ userId: invented })).deletedCount,
    };
    for (const name of dropList) await db.dropCollection(name).catch(() => undefined);
    await sleep(1500);
    await db.collection("alerts").deleteMany({ userId: invented });
    const left: Record<string, number> = {};
    for (const name of ["order_update_requests", "order_details", "platformids", "order_job_runs", "order_sync_tasks", "order_unit_events", "order_invoice_requests", "alerts", "order_sync_settings", "order_index", "session_demotions"]) left[name] = await db.collection(name).countDocuments({ userId: invented });
    left.order_sync_tasks += await OrderSyncTask.countDocuments({ requestId: { $in: ids } });
    left["@verify.invalid platformids of this run"] = await PlatformId.countDocuments({ email: { $regex: `-${run}@verify\\.invalid$` } });
    // The ids this run wrote down go last, and only when no row of them is left: else the next run takes it up.
    if (Object.values(left).every((n) => n === 0)) await db.dropCollection(`verify_ss_${run}_ids`).catch(() => undefined);
    left[`verify_ss_${run}_* collections`] = (await db.listCollections({ name: { $regex: `^verify_ss_${run}_` } }, { nameOnly: true }).toArray()).length;
    const realAfter = await realCollections();
    const created = realAfter.filter((n) => !realBefore.includes(n));
    console.log(`mongo: deleted ${Object.entries(del).map(([k, v]) => `${k} ${v}`).join(" · ")} · ${dropList.length} throwaway collection(s) dropped`);
    console.log(`left behind: ${Object.entries(left).map(([k, v]) => `${k} ${v}`).join(" · ")}${Object.values(left).every((n) => n === 0) ? " — nothing" : " — NOT CLEAN"} · real schedule collections created by this run: ${created.length} (there before: ${realBefore.length ? realBefore.join(", ") : "none"}) · listener ${server ? "closed" : "never opened"}`);
    if (!Object.values(left).every((n) => n === 0) || created.length) failures.push("cleanup");
    await mongoose.disconnect();
  }

  console.log(`\n${passed} passed, ${failures.length} failed${failures.length ? ` — ${failures.join("; ")}` : ""}`);
  process.exit(failures.length ? 1 : 0);
}

main().catch((err) => {
  if (isInterrupted()) return;
  console.error(err);
  process.exit(1);
});
