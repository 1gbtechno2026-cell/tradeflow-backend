/**
 * The nightly fetch on the shared schedule clock (data-quality plan sub-step 7, C5) — with the fetch itself INJECTED:
 * no Chrome, no Flipkart, no account is read, and a clock this script moves by hand.
 *
 *   · the seed leaves the nightly fetch OFF whatever FETCH_SCHEDULE says: 05:00 is "skipped — switched off"
 *   · switched on: the API's clock takes 05:00 — the ledger row exists (queued) before the account list is read and is
 *     the lease's run from the first second; polls renew the lease as long as the fetch runs in this process (a fetch
 *     writes no document while it runs); its end gives the lease back ("finished — …") and asks for the update after
 *   · not twice: a fetch running in this process, or seen running on another machine in the last 10 minutes (its
 *     ledger row, which a fetch touches once a minute), skips the tick with that reason
 *   · the API restarts in the middle of the fetch: the row reads "The API restarted…", the lease is given back as
 *     failed and the SAME night is started once more — also when the dead process's row still says running, and when
 *     the restart came while the account list was still being read; it dies again → never a third start
 *   · another machine takes the night over while this one is away: this process's fetch is stopped with
 *     "Superseded — Mac-2 took over the nightly fetch at 05:30", and its end writes nothing on the lease
 *   · "Run the scheduled fetch now" through the real route: 409 with who holds it while another machine has the lease;
 *     202 with the switch off and on a machine the schedule never uses — the lease is taken by hand, this API's clock
 *     keeps it renewed and the end gives it back; 409 while a fetch runs here, with no logged-in ID, when it cannot
 *     start; a workspace without a clock runs as it always did
 *   · the clock as the API starts it: it serves the fetch whatever the .env says and may run it only when FETCH_SCHEDULE
 *     lets this machine
 *   · stopOrderFetch's reason is the line the fetch log ends on
 *   · what a killed run of this check left is removed by the next one — and never a row of a workspace that is real
 *
 * What is real: Mongo. The clocks, switches and presence rows live in THROWAWAY collections (verify_fs_<run>_…, dropped
 * at the end). The fetch's ledger rows are written to order_job_runs for an INVENTED workspace (this process's
 * TRADE_FLOW_USER_ID is that id; its system id and host name are invented too, so no API's start and no keeper takes
 * them for its own) and deleted at the end — what was left is printed. No Redis, no queue. The route is called on a
 * listener of this process (127.0.0.1, a random port).
 *
 * A run that is stopped (Ctrl-C, kill) still cleans up; one that is killed outright leaves its rows and collections,
 * and the next run removes them once they are 30 minutes old (scripts/scheduleCheckCleanup.ts) and says so.
 *
 *   npm run test:fetch-schedule
 */
import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import mongoose, { Types, type Model } from "mongoose";
import type { IScheduleLease } from "../models/ScheduleLease.js";
import type { OrderFetchStartOptions } from "../services/orderFetch.js";
import type { FetchScheduleDeps } from "../services/orderFetchSchedule.js";
import type { ScheduleClaim, ScheduleClock, ScheduleDeps, ScheduleRunner, ScheduleRunResult } from "../services/scheduleClock.js";
import { cleanRun, describeCleanup, envWorkspaceId, interruptedCleanup, isInterrupted, onInterrupt, refusalFor, registerRunIds, removeStaleRuns, runAgeMs, STALE_RUN_MS, standStill, type RunCleanup } from "./scheduleCheckCleanup.js";

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

/** "2026-11-03 05:00" (or with seconds) on India's clock. */
const ist = (s: string) => new Date(`${s.replace(" ", "T")}${s.length <= 16 ? ":00" : ""}+05:30`);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  // A workspace and a machine that do not exist — set before config is read, so nothing here can act as the real ones.
  const run = Date.now().toString(36);
  const invented = new Types.ObjectId();
  process.env.TRADE_FLOW_USER_ID = String(invented);
  process.env.TRADEFLOW_SYSTEM_ID = `verify-fs-${run}`;
  process.env.TRADEFLOW_HOST_ID = `Mac-check-${run}`;
  const { config } = await import("../config.js");
  const { OrderJobRun } = await import("../models/OrderJobRun.js");
  const { OrderSyncSettings } = await import("../models/OrderSyncSettings.js");
  const { ScheduleHost } = await import("../models/ScheduleHost.js");
  const { ScheduleLease } = await import("../models/ScheduleLease.js");
  const { getOrderFetchJob, OrderFetchStoppedError, stopOrderFetch } = await import("../services/orderFetch.js");
  const { createScheduledFetch, FETCH_SEEN_MS, fetchRunningElsewhere, fetchScheduleDefaults, scheduledFetchRunner, startFetchScheduleClock, stopFetchScheduleClock, touchRunningFetch } = await import("../services/orderFetchSchedule.js");
  const { claimManual, clockStamp, clockTime, createScheduleClock, LEASE_MS, scheduleDefaults, tickText } = await import("../services/scheduleClock.js");
  const { seedSchedules, setScheduleSwitch } = await import("../services/scheduleSettings.js");

  const U = String(invented);
  const HOST = config.hostId;
  assert.equal(config.userId, U, "the invented TRADE_FLOW_USER_ID is the one in use");
  assert.equal(config.systemId, `verify-fs-${run}`, "an invented system id");

  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false, serverSelectionTimeoutMS: 15_000 });
  const db = mongoose.connection.db!;
  const realCollections = async () => (await db.listCollections({ name: { $in: ["order_schedule_leases", "order_schedule_hosts"] } }, { nameOnly: true }).toArray()).map((c) => c.name).sort();
  const realBefore = await realCollections();
  // What an earlier run that was killed left behind goes first; then this run writes down the workspace it invents,
  // before a row of it reaches a real collection.
  const mine: RunCleanup = { db, tag: "fs", rowCollections: ["order_job_runs"] };
  for (const stale of await removeStaleRuns({ ...mine, thisRun: run })) {
    console.log(`leftovers of an earlier run that never cleaned up — removed now · ${describeCleanup(stale)}`);
    if (stale.refused.length) failures.push("leftovers of an earlier run");
  }
  await registerRunIds(db, "fs", run, [invented]);
  const dropList: string[] = [];
  const throwaway = <T>(real: Model<T>, name: string): Model<T> => {
    const coll = `verify_fs_${run}_${name}`;
    dropList.push(coll);
    return mongoose.model<T>(`VerifyFs${name}${run}`, real.schema, coll) as Model<T>;
  };

  // The clock this script moves by hand. It stands in the PAST: a ledger row the services write with the real time
  // is then "just written" to it; a row that must look old is given its time by hand.
  let clockMs = ist("2026-11-02 10:00").getTime();
  const at = (s: string) => {
    clockMs = ist(s).getTime();
  };
  const realDefaults = { ...scheduleDefaults };
  const realFetchDefaults = { ...fetchScheduleDefaults };
  const deps: ScheduleDeps = { ...scheduleDefaults, Leases: throwaway(ScheduleLease, "leases"), Hosts: throwaway(ScheduleHost, "hosts"), Settings: throwaway(OrderSyncSettings, "settings"), now: () => new Date(clockMs) };
  Object.assign(scheduleDefaults, deps);
  console.log(`invented workspace ${U} on invented machine ${config.systemId} ("${HOST}") · throwaway collections verify_fs_${run}_* · the clock starts at ${clockStamp(deps.now())} IST\n`);

  // ── helpers ───────────────────────────────────────────────────────────────
  const THIS = { systemId: config.systemId, host: HOST };
  const MAC2 = { systemId: `mac2-${run}`, host: "Mac-2" };
  const INTERRUPTED = "The API restarted while this fetch was running — start it again";
  const logs: string[] = [];
  let pidSeq = 100;

  type Started = { userId: string; emails: string[]; since: Date; opts: OrderFetchStartOptions; runId: string };
  /**
   * A fetch that opens nothing: it adopts the queued ledger row, as the real one does, and "runs" until the case ends
   * it. One per API process this script plays.
   */
  const fakeFetch = (o: { emails?: string[]; failStart?: Error; hang?: boolean } = {}) => {
    const st = { reserved: false, running: false, busyElsewise: false, starts: [] as Started[], stops: [] as string[], after: [] as Array<Record<string, unknown>> };
    const finish = async (status: "completed" | "stopped", message = "3/3 email(s) · 7 unit IDs found, 2 new, 2 saved, 0 failed, 0 older than 3 Sep") => {
      const s = st.starts[st.starts.length - 1];
      st.running = false;
      await OrderJobRun.updateOne({ _id: s.runId }, { $set: { status, finishedAt: new Date(), message } });
      await s.opts.onFinished?.({ runId: s.runId, cancelled: status === "stopped", emails: s.emails });
    };
    const d: Partial<FetchScheduleDeps> = {
      busy: () => st.reserved || st.running || st.busyElsewise,
      reserve: () => {
        if (st.reserved || st.running || st.busyElsewise) throw new Error("Order fetch is already running");
        st.reserved = true;
        return () => void (st.reserved = false);
      },
      start: async (userId, emails, since, opts = {}) => {
        if (o.failStart) throw o.failStart;
        if (o.hang) await new Promise(() => undefined); // the account list is still being read when the process "dies"
        const runId = String(opts.runId);
        const res = await OrderJobRun.updateOne({ _id: runId, status: "queued" }, { $set: { status: "running", startedAt: new Date(), "meta.params": opts.params || {} } });
        assert.equal(res.modifiedCount, 1, "the fake adopts the queued row");
        st.running = true;
        st.starts.push({ userId, emails, since, opts, runId });
        return { runId };
      },
      stop: async (_userId, reason) => {
        st.stops.push(reason || "");
        if (st.running) await finish("stopped");
      },
      emails: async () => o.emails ?? [`f1-${run}@verify.invalid`, `f2-${run}@verify.invalid`, `f3-${run}@verify.invalid`],
      afterFetch: async (_userId, fetch) => {
        st.after.push(fetch as unknown as Record<string, unknown>);
        return { status: "skipped" as const, reason: "verify" };
      },
      schedule: deps,
    };
    return { st, deps: d, finish };
  };
  /** One API process: its fetch, its scheduled-fetch state and its clock. `mayRun` = FETCH_SCHEDULE lets this machine run the nightly fetch. */
  const apiProcess = (who: { systemId: string; host: string }, o: Parameters<typeof fakeFetch>[0] & { mayRun?: boolean } = {}) => {
    const fake = fakeFetch(o);
    const sf = createScheduledFetch(fake.deps);
    // A start that hangs must not hold the poll up for the clock's usual minute.
    const clock: ScheduleClock = createScheduleClock({ role: "api", userId: U, names: o.mayRun === false ? [] : ["fetch"], serves: ["fetch"], runners: { fetch: sf.runner(U) }, me: { ...who, pid: ++pidSeq, machine: `${who.host}.local` }, deps, log: (line) => logs.push(line), ...(o.hang ? { startWaitMs: 300 } : {}) });
    return { ...fake, sf, clock };
  };
  /** Another machine's clock, with a runner that only records. */
  const recorder = (answer: (claim: ScheduleClaim) => ScheduleRunResult | Promise<ScheduleRunResult>) => {
    const calls: ScheduleClaim[] = [];
    const runner: ScheduleRunner = {
      run: async (claim) => {
        calls.push(claim);
        return answer(claim);
      },
    };
    const clock = createScheduleClock({ role: "api", userId: U, names: ["fetch"], serves: ["fetch"], runners: { fetch: runner }, me: { ...MAC2, pid: ++pidSeq, machine: "Mac-2.local" }, deps, log: (line) => logs.push(line) });
    return { calls, clock };
  };
  const otherWorkspaces: Types.ObjectId[] = [];
  /** The invented workspace back to nothing; its clocks seeded at `when` from a .env that WOULD run the fetch; the switch as the case wants it. */
  const reset = async (when: string, on: boolean) => {
    at(when);
    await OrderJobRun.deleteMany({ userId: invented });
    await Promise.all([deps.Leases.deleteMany({ userId: invented }), deps.Settings.deleteMany({ userId: invented }), deps.Hosts.deleteMany({})]);
    await seedSchedules(U, { env: { FETCH_SCHEDULE: "0 5 * * *" }, me: { systemId: "", host: "seed-host" }, deps });
    if (on) await setScheduleSwitch(U, "fetch", true, "asha", deps);
  };
  const lease = async (userId = U) => {
    const doc = (await deps.Leases.findOne({ userId, name: "fetch" }).lean()) as (IScheduleLease & { _id: Types.ObjectId }) | null;
    assert.ok(doc, "the fetch clock exists");
    return doc;
  };
  const ticksOf = (l: IScheduleLease) => l.ticks.map((t) => `${t.outcome}@${clockTime(t.slotAt)}`);
  const rowOf = async (id: string) => {
    const row = await OrderJobRun.findById(id).lean();
    assert.ok(row, "ledger row gone");
    return row;
  };
  const rowsNow = () => OrderJobRun.countDocuments({ userId: invented, type: "fetch" });
  /** Wait for something a start that is still going writes. */
  const until = async (what: string, ok: () => Promise<boolean>) => {
    for (let i = 0; i < 100; i++) {
      if (await ok()) return;
      await sleep(100);
    }
    assert.fail(`${what} — not within 10 s`);
  };
  /** A fetch row written here: another machine's, or an old one. */
  const rawRow = async (o: { host?: string; pid?: number; status?: string; by?: string; trigger?: string; updatedAt?: Date }) => {
    const _id = new Types.ObjectId();
    const now = o.updatedAt || deps.now();
    await OrderJobRun.collection.insertOne({ _id, userId: invented, type: "fetch", trigger: o.trigger || "user", by: o.by ?? "ravi", status: o.status || "running", total: 3, processed: 0, success: 0, failed: 0, cancelled: 0, startedAt: now, finishedAt: null, avgMsPerItem: 0, message: "", failedItems: [], refId: "", meta: o.host === undefined ? {} : { host: o.host, pid: o.pid ?? 4242 }, createdAt: now, updatedAt: now });
    return String(_id);
  };
  /** The process that ran a fetch is gone: nothing of it runs any more, and its row is closed as the next API's start closes it. */
  const die = async (p: { st: { running: boolean; reserved: boolean } }, runId: string, closeRow = true) => {
    p.st.running = false;
    p.st.reserved = false;
    if (closeRow) await OrderJobRun.updateOne({ _id: runId, status: { $in: ["queued", "running"] } }, { $set: { status: "failed", finishedAt: new Date(), message: INTERRUPTED } });
  };

  let server: import("node:http").Server | null = null;
  const said = { log: console.log, warn: console.warn };
  const serviceLines: string[] = [];
  const own = (print: (...a: unknown[]) => void) => (...a: unknown[]) => (typeof a[0] === "string" && /^\[(schedules|fetch-schedule|orders-fetch)/.test(a[0]) ? void serviceLines.push(a[0]) : print(...a));
  console.log = own(said.log);
  console.warn = own(said.warn);
  // Stopped from outside (Ctrl-C, kill): what this run made goes before the process does.
  onInterrupt(() =>
    interruptedCleanup(config.mongoUri, { tag: mine.tag, rowCollections: mine.rowCollections }, run, () => {
      console.log = said.log;
      console.warn = said.warn;
      server?.close();
    })
  );

  try {
    await deps.Settings.createCollection();
    await deps.Settings.createIndexes();

    console.log("THE NIGHTLY FETCH ON THE CLOCK");
    await check("the seed leaves the nightly fetch OFF whatever FETCH_SCHEDULE says: 05:00 is 'skipped — switched off' and nothing starts", async () => {
      await reset("2026-11-02 10:00", false);
      const p = apiProcess(THIS);
      at("2026-11-03 04:59");
      await p.clock.pollOnce();
      at("2026-11-03 05:00:10");
      await p.clock.pollOnce();
      const l = await lease();
      assert.deepEqual([p.st.starts.length, await rowsNow(), ticksOf(l), tickText(l.lastTick), l.holder, clockStamp(l.nextAt)], [0, 0, ["skipped@05:00"], "skipped — switched off", null, "4 Nov 05:00"]);
    });

    await check("switched on: 05:00 is taken — the ledger row exists before the account list is read and is the lease's run; the lease is renewed as long as the fetch runs here; its end gives it back and asks for the update after", async () => {
      await reset("2026-11-02 10:00", true);
      const p = apiProcess(THIS);
      at("2026-11-03 04:59");
      await p.clock.pollOnce();
      at("2026-11-03 05:00:10");
      await p.clock.pollOnce();
      assert.equal(p.st.starts.length, 1, "started once");
      const s = p.st.starts[0];
      let l = await lease();
      assert.deepEqual([l.holder?.systemId, l.holder?.role, l.attempt, l.manual, clockTime(l.slotAt), clockStamp(l.nextAt)], [config.systemId, "api", 1, false, "05:00", "4 Nov 05:00"]);
      assert.deepEqual(l.runRef, { kind: "fetch", id: s.runId });
      assert.deepEqual([ticksOf(l), l.lastTick?.runRef], [["ran@05:00"], { kind: "fetch", id: s.runId }]);
      assert.match(tickText(l.lastTick), new RegExp(`^ran on ${HOST} — 3 logged-in Platform ID\\(s\\), orders since \\d{4}-\\d{2}-\\d{2} \\(\\d+ d\\), invoices (off|on \\(all missing\\))$`));
      // What the fetch was started with.
      assert.deepEqual([s.userId, s.emails.length, s.opts.trigger, s.opts.by, s.opts.reserved, s.opts.runId], [U, 3, "system-cron", "schedule", true, s.runId]);
      const params = s.opts.params as Record<string, unknown>;
      assert.deepEqual([params.schedule_name, params.schedule_slot, params.ran_on, params.attempt, params.selection_method, params.run_update_after_fetch, params.logged_in_count], ["fetch", "2026-11-03 05:00", HOST, 1, "all_accounts", true, 3]);
      const row = await rowOf(s.runId);
      const meta = row.meta as { host?: string; pid?: number; emails_count?: number };
      assert.deepEqual([row.status, row.trigger, row.by, row.total, meta.host, meta.pid, meta.emails_count], ["running", "system-cron", "schedule", 3, HOST, process.pid, 3], "the row was made queued by the schedule, with this machine and process on it, and adopted by the fetch");
      assert.equal(p.sf.running(U), s.runId);
      for (const t of ["05:00:40", "05:01:10"]) {
        at(`2026-11-03 ${t}`);
        await p.clock.pollOnce();
        assert.equal((await lease()).expiresAt?.getTime(), clockMs + LEASE_MS, `renewed at ${t}`);
      }
      // Hours on, and nothing has written the row since 05:00: a fetch has no document that is written while it runs —
      // the process itself knows it is going.
      await OrderJobRun.collection.updateOne({ _id: new Types.ObjectId(s.runId) }, { $set: { updatedAt: ist("2026-11-03 05:00:10") } });
      at("2026-11-03 07:30");
      await p.clock.pollOnce();
      l = await lease();
      assert.deepEqual([l.holder?.token, l.expiresAt?.getTime(), ticksOf(l)], [p.clock.holds("fetch")?.token, clockMs + LEASE_MS, ["ran@05:00"]], "still held 2½ h in");
      // The next 05:00 comes while it runs (the real one takes ~37 h): skipped, said why.
      at("2026-11-04 05:00:10");
      await p.clock.pollOnce();
      l = await lease();
      assert.deepEqual([ticksOf(l), l.lastTick?.reason, p.st.starts.length], [["ran@05:00", "skipped@05:00"], `the 05:00 run is still going on ${HOST}`, 1]);
      // It ends.
      at("2026-11-04 18:00");
      await p.finish("completed");
      l = await lease();
      assert.deepEqual([l.holder, l.endedAs, tickText(l.lastTick), l.lastTick?.runRef, p.sf.running(U)], [null, "finished", "finished — 3/3 email(s) · 7 unit IDs found, 2 new, 2 saved, 0 failed, 0 older than 3 Sep", { kind: "fetch", id: s.runId }, ""]);
      assert.deepEqual(p.st.after, [{ runId: s.runId, emails: s.emails, selection: "all_accounts", cancelled: false, by: "schedule", trigger: "system-cron" }], "the update after the nightly fetch is asked for, once");
      assert.equal(p.st.reserved, false, "the fetch slot was given back");
    });

    await check("not twice: a fetch seen running on Mac-2 in the last 10 minutes skips the tick with that reason; a row nothing has touched for 11 minutes does not; a fetch running in this process skips it too", async () => {
      await reset("2026-11-02 10:00", true);
      const p = apiProcess(THIS);
      at("2026-11-03 04:59");
      await p.clock.pollOnce();
      at("2026-11-03 05:00:10");
      const theirs = await rawRow({ host: "Mac-2", by: "ravi" });
      await p.clock.pollOnce();
      let l = await lease();
      assert.deepEqual([p.st.starts.length, l.holder, l.endedAs, tickText(l.lastTick), await rowsNow()], [0, null, "skipped", "skipped — a fetch started by ravi is running on Mac-2", 1], "nothing started, no row made");
      // The next night: the same row, not touched since — its machine is gone (or runs older code). It does not hold the night up.
      await OrderJobRun.collection.updateOne({ _id: new Types.ObjectId(theirs) }, { $set: { updatedAt: new Date(ist("2026-11-04 05:00:10").getTime() - FETCH_SEEN_MS - 60_000) } });
      at("2026-11-04 05:00:10");
      await p.clock.pollOnce();
      assert.equal(p.st.starts.length, 1, "started");
      await p.finish("completed");
      // The night after: a fetch — the Fetch button's — is running in this very process. It is what the tick says,
      // also while one is seen on Mac-2 as well: this process's own fetch is looked at first.
      p.st.busyElsewise = true;
      at("2026-11-05 05:00:10");
      await OrderJobRun.collection.updateOne({ _id: new Types.ObjectId(theirs) }, { $set: { updatedAt: deps.now() } });
      await p.clock.pollOnce();
      l = await lease();
      assert.deepEqual([p.st.starts.length, l.holder, tickText(l.lastTick)], [1, null, "skipped — a fetch is already running"]);
      await OrderJobRun.collection.updateOne({ _id: new Types.ObjectId(theirs) }, { $set: { status: "completed" } });
      // And with no logged-in Platform ID at all.
      const none = apiProcess(THIS, { emails: [] });
      at("2026-11-06 05:00:10");
      await none.clock.pollOnce();
      assert.deepEqual([none.st.starts.length, tickText((await lease()).lastTick)], [0, "skipped — no logged-in Platform ID"]);
    });

    await check("what counts as running elsewhere, and the sign of life: only an open row of ANOTHER host written in the last 10 min; a fetch running here touches its own row", async () => {
      await reset("2026-11-02 10:00", true);
      at("2026-11-03 05:00");
      const d = { ...fetchScheduleDefaults, schedule: deps, busy: () => true };
      const mineRow = await rawRow({ host: HOST, pid: process.pid, by: "asha" });
      assert.equal(await fetchRunningElsewhere(U, d), null, "this machine's own fetch is the in-process check's");
      const noHost = await rawRow({});
      assert.equal(await fetchRunningElsewhere(U, d), null, "a row that names no machine (older code) is not guessed at");
      const done = await rawRow({ host: "Mac-2", status: "completed" });
      assert.equal(await fetchRunningElsewhere(U, d), null, "a finished one is not running");
      const old = await rawRow({ host: "Mac-2", updatedAt: new Date(clockMs - FETCH_SEEN_MS - 1000) });
      assert.equal(await fetchRunningElsewhere(U, d), null, "10 minutes without a sign of life");
      const fresh = await rawRow({ host: "Mac-2", by: "", trigger: "system-cron", updatedAt: new Date(clockMs - FETCH_SEEN_MS + 5000) });
      assert.deepEqual(await fetchRunningElsewhere(U, d), { runId: fresh, host: "Mac-2", by: "the schedule" });
      const queued = await rawRow({ host: "Mac-3", status: "queued", by: "ravi" });
      assert.deepEqual(await fetchRunningElsewhere(U, d), { runId: queued, host: "Mac-3", by: "ravi" }, "one that is still preparing counts");
      // The sign of life: this process's open rows only, and only while a fetch runs here.
      at("2026-11-03 05:07");
      assert.equal(await touchRunningFetch(U, { ...d, busy: () => false }), 0);
      assert.equal(await touchRunningFetch(U, d), 1);
      const stamp = async (id: string) => (await rowOf(id)).updatedAt.getTime();
      assert.deepEqual([await stamp(mineRow), await stamp(old), await stamp(noHost), await stamp(done)], [clockMs, ist("2026-11-03 05:00").getTime() - FETCH_SEEN_MS - 1000, ist("2026-11-03 05:00").getTime(), ist("2026-11-03 05:00").getTime()], "only this process's row was touched");
    });

    console.log("\nA RESTART IN THE MIDDLE — the same night once more, never a third time");
    await check("the API restarts mid-fetch: the row reads 'The API restarted…', the lease is given back as failed, and the next process starts the same night once more (took_over, start 2 of 2); it dies again → no third start", async () => {
      await reset("2026-11-02 10:00", true);
      const a = apiProcess(THIS);
      at("2026-11-03 04:59");
      await a.clock.pollOnce();
      at("2026-11-03 05:00:10");
      await a.clock.pollOnce();
      const first = a.st.starts[0].runId;
      // 06:10 — the API is restarted (a file was saved): the fetch dies with it; the new process closes its row at start.
      at("2026-11-03 06:10");
      await die(a, first);
      const b = apiProcess(THIS);
      at("2026-11-03 06:10:30");
      await b.clock.pollOnce();
      let l = await lease();
      assert.equal(b.st.starts.length, 1, "started again");
      const second = b.st.starts[0].runId;
      assert.notEqual(second, first);
      assert.deepEqual([ticksOf(l), l.ticks[1].reason, l.attempt, clockTime(l.slotAt), clockStamp(l.nextAt), l.runRef], [["ran@05:00", "failed@05:00", "took_over@05:00"], INTERRUPTED, 2, "05:00", "4 Nov 05:00", { kind: "fetch", id: second }]);
      assert.match(String(l.lastTick?.reason), new RegExp(`^3 logged-in Platform ID\\(s\\), .* · start 2 of 2 — the first ended: ${INTERRUPTED}; started 71 min late$`));
      assert.deepEqual([(b.st.starts[0].opts.params as Record<string, unknown>).attempt, (b.st.starts[0].opts.params as Record<string, unknown>).schedule_slot], [2, "2026-11-03 05:00"]);
      // 07:00 — it dies again.
      at("2026-11-03 07:00");
      await die(b, second);
      const c = apiProcess(THIS);
      await c.clock.pollOnce();
      at("2026-11-03 07:00:30");
      await c.clock.pollOnce();
      l = await lease();
      assert.deepEqual([c.st.starts.length, l.attempt, l.holder, l.endedAs, ticksOf(l).slice(-1), await rowsNow()], [0, 2, null, "failed", ["failed@05:00"], 2], "no third start, no third row");
    });

    await check("the dead process's row still says running (nothing closed it): it names this host and another process — closed as interrupted, and the night is started once more", async () => {
      await reset("2026-11-02 10:00", true);
      const a = apiProcess(THIS);
      at("2026-11-03 04:59");
      await a.clock.pollOnce();
      at("2026-11-03 05:00:10");
      await a.clock.pollOnce();
      const first = a.st.starts[0].runId;
      at("2026-11-03 05:40");
      await die(a, first, false);
      // As if another process of this machine had written it.
      await OrderJobRun.collection.updateOne({ _id: new Types.ObjectId(first) }, { $set: { "meta.pid": process.pid + 1 } });
      const b = apiProcess(THIS);
      await b.clock.pollOnce();
      const row = await rowOf(first);
      const l = await lease();
      assert.deepEqual([row.status, row.message, b.st.starts.length, l.attempt, ticksOf(l)], ["failed", INTERRUPTED, 1, 2, ["ran@05:00", "failed@05:00", "took_over@05:00"]]);
      // A row of this very process that is still going is never taken for dead.
      at("2026-11-03 05:41");
      await b.clock.pollOnce();
      assert.deepEqual([(await rowOf(b.st.starts[0].runId)).status, (await lease()).holder?.token], ["running", b.clock.holds("fetch")?.token]);
    });

    await check("a restart while the account list was still being read: the queued row makes it a fetch that failed — started again at once, not a night nobody started", async () => {
      await reset("2026-11-02 10:00", true);
      const a = apiProcess(THIS, { hang: true });
      at("2026-11-03 04:59");
      await a.clock.pollOnce();
      at("2026-11-03 05:00:10");
      await a.clock.pollOnce(); // comes back although the fetch has not started: the poll does not wait for it
      await until("the queued row is attached to the lease", async () => Boolean((await lease()).runRef));
      let l = await lease();
      assert.ok(l.runRef?.kind === "fetch" && l.holder, "the lease already follows the queued row");
      const queued = String(l.runRef?.id);
      assert.deepEqual([(await rowOf(queued)).status, a.st.starts.length, l.ticks.length, a.sf.running(U)], ["queued", 0, 0, queued]);
      at("2026-11-03 05:00:40");
      await a.clock.pollOnce();
      assert.equal((await lease()).expiresAt?.getTime(), clockMs + LEASE_MS, "renewed while it prepares");
      // 05:02 — the restart.
      at("2026-11-03 05:02");
      await die(a, queued);
      const b = apiProcess(THIS);
      await b.clock.pollOnce();
      l = await lease();
      assert.deepEqual([b.st.starts.length, l.attempt, ticksOf(l), l.ticks[0].reason], [1, 2, ["failed@05:00", "took_over@05:00"], INTERRUPTED]);
      assert.notEqual(b.st.starts[0].runId, queued);
    });

    await check("another machine takes the night over while this one is away: this process's fetch is stopped 'Superseded — Mac-2 took over the nightly fetch at 05:30'; its end writes nothing on the lease; a fetch that is not the leased one is left alone", async () => {
      await reset("2026-11-02 10:00", true);
      const a = apiProcess(THIS);
      at("2026-11-03 04:59");
      await a.clock.pollOnce();
      at("2026-11-03 05:00:10");
      await a.clock.pollOnce();
      const mine = a.st.starts[0].runId;
      // A takeover of some other run is not this fetch's business: it is never stopped by one.
      await a.sf.runner(U).supersede?.({ name: "fetch", token: "another-claim", slotAt: null, runRef: { kind: "fetch", id: String(new Types.ObjectId()) }, by: MAC2, at: deps.now() });
      assert.deepEqual([a.st.stops.length, a.st.running], [0, true]);
      // This machine sleeps (its fetch frozen with it); at 05:30 Mac-2 — which may run the fetch — takes the night over.
      const theirs = await rawRow({ host: "Mac-2", by: "schedule", trigger: "system-cron", updatedAt: ist("2026-11-03 05:30") });
      const two = recorder(() => ({ runRef: { kind: "fetch", id: theirs } }));
      at("2026-11-03 05:30");
      await two.clock.pollOnce();
      let l = await lease();
      assert.deepEqual([two.calls.length, two.calls[0]?.attempt, l.holder?.systemId, ticksOf(l)], [1, 2, MAC2.systemId, ["ran@05:00", "took_over@05:00"]]);
      // 05:40 — this machine wakes.
      at("2026-11-03 05:40");
      await a.clock.pollOnce();
      assert.deepEqual(a.st.stops, ["Superseded — Mac-2 took over the nightly fetch at 05:30"]);
      const row = await rowOf(mine);
      l = await lease();
      assert.deepEqual([row.status, a.st.running, a.sf.running(U), l.holder?.token, l.runRef, ticksOf(l)], ["stopped", false, "", two.calls[0].token, { kind: "fetch", id: theirs }, ["ran@05:00", "took_over@05:00", "superseded@05:00"]], "the lease is Mac-2's and stays as it was");
      assert.deepEqual(a.st.after.map((f) => [f.runId, f.cancelled]), [[mine, true]], "a stopped fetch: the update after it is told so (it then queues nothing)");
      await a.clock.pollOnce();
      assert.equal(a.st.stops.length, 1, "told once");
    });

    // ── the route ────────────────────────────────────────────────────────────
    console.log("\nRUN THE SCHEDULED FETCH NOW — the real route, on a listener of this process");
    // The route's own scheduled fetch (this API's), with a fetch that opens nothing behind it.
    let api = fakeFetch();
    const route = (o: Parameters<typeof fakeFetch>[0] = {}) => {
      api = fakeFetch(o);
      Object.assign(fetchScheduleDefaults, api.deps);
      return api;
    };
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
    const call = async (method: "GET" | "POST", path: string, body?: unknown): Promise<{ status: number; body: Json }> => {
      const res = await fetch(`${base}${path}`, { method, headers: { "content-type": "application/json" }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(30_000) });
      return { status: res.status, body: (await res.json().catch(() => ({}))) as Json };
    };
    /** This API's clock — the runner is the route's own; `mayRun` false = FETCH_SCHEDULE=0 on this machine. */
    const apiClock = (mayRun: boolean) => createScheduleClock({ role: "api", userId: U, names: mayRun ? ["fetch"] : [], serves: ["fetch"], runners: { fetch: scheduledFetchRunner(U) }, me: { ...THIS, pid: process.pid, machine: `${HOST}.local` }, deps, log: (line) => logs.push(line) });

    await check("while another machine holds the lease: 409 'The nightly fetch is running on Mac-2 since 05:00' with who holds it — nothing is started, no row is made", async () => {
      await reset("2026-11-02 10:00", true);
      route();
      const theirs = await rawRow({ host: "Mac-2", by: "schedule", trigger: "system-cron" });
      const two = recorder(() => ({ runRef: { kind: "fetch", id: theirs } }));
      at("2026-11-03 04:59");
      await two.clock.pollOnce();
      at("2026-11-03 05:00:10");
      await two.clock.pollOnce();
      at("2026-11-03 05:03");
      const before = JSON.stringify(await lease());
      const rows = await rowsNow();
      const res = await call("POST", "/fetch/schedule/run", { by: "asha", userId: U });
      const sentence = "The nightly fetch is running on Mac-2 since 05:00";
      assert.equal(res.status, 409);
      assert.deepEqual(res.body, { started: false, message: sentence, reason: sentence, code: "held_elsewhere", holder: { systemId: MAC2.systemId, host: "Mac-2", since: ist("2026-11-03 05:00:10").toISOString(), slotAt: ist("2026-11-03 05:00").toISOString(), manual: false }, error: sentence });
      assert.deepEqual([api.st.starts.length, await rowsNow(), JSON.stringify(await lease())], [0, rows, before], "nothing started, no row, the lease untouched");
    });

    await check("with the switch OFF, on a machine the schedule never uses: 202 — the lease is taken by hand, this API's clock keeps it renewed, the 05:00 time meanwhile is skipped, and the end gives the lease back", async () => {
      await reset("2026-11-02 10:00", false);
      route();
      const clock = apiClock(false);
      at("2026-11-02 22:00");
      await clock.pollOnce();
      const res = await call("POST", "/fetch/schedule/run", { by: "asha", userId: U });
      assert.equal(res.status, 202);
      assert.equal(api.st.starts.length, 1);
      const s = api.st.starts[0];
      assert.deepEqual([res.body.started, res.body.runId, res.body.emails, typeof res.body.sinceDays, "error" in res.body], [true, s.runId, 3, "number", false]);
      assert.match(String(res.body.message), /^started — 3 logged-in Platform ID\(s\), orders since /);
      assert.deepEqual([s.opts.trigger, s.opts.by, "schedule_name" in (s.opts.params as object)], ["system-cron", "asha", false], "the row shows system-cron, by who pressed it; no fixed time is recorded");
      let l = await lease();
      assert.deepEqual([l.manual, l.slotAt, l.holder?.systemId, l.holder?.role, l.holder?.pid, l.runRef, clockStamp(l.nextAt), ticksOf(l), l.armedAt], [true, null, config.systemId, "api", process.pid, { kind: "fetch", id: s.runId }, "3 Nov 05:00", ["ran@?"], null], "taken by hand: no fixed time is used up, and the clock is not armed by it");
      assert.match(tickText(l.lastTick), new RegExp(`^ran on ${HOST} — started by hand by asha — 3 logged-in Platform ID\\(s\\)`));
      at("2026-11-02 22:04");
      await clock.pollOnce();
      at("2026-11-02 22:08:30");
      await clock.pollOnce();
      l = await lease();
      assert.deepEqual([l.holder?.pid, l.expiresAt?.getTime()], [process.pid, clockMs + LEASE_MS], "renewed past the first 5 minutes by a clock that may not run the schedule itself");
      // Another press while it runs: this machine has it.
      const again = await call("POST", "/fetch/schedule/run", { by: "ravi", userId: U });
      assert.deepEqual([again.status, again.body.code, again.body.error, api.st.starts.length], [409, "running_here", "The nightly fetch is running on this machine since 22:00", 1]);
      // 05:00 — switched off: said so (the hand-started run is not what skips it).
      at("2026-11-03 05:00:10");
      await clock.pollOnce();
      l = await lease();
      assert.deepEqual([ticksOf(l), l.lastTick?.reason, l.holder?.pid, api.st.starts.length], [["ran@?", "skipped@05:00"], "switched off", process.pid, 1]);
      // GET /fetch/schedule shows the clock's state.
      const status = (await call("GET", `/fetch/schedule?userId=${U}`)).body;
      assert.deepEqual([status.enabled, status.lease.host, status.lease.manual, status.lease.live, status.lastTick.outcome, status.nextAt], [false, HOST, true, true, "skipped — switched off", ist("2026-11-04 05:00").toISOString()]);
      at("2026-11-03 06:00");
      await api.finish("completed");
      l = await lease();
      assert.deepEqual([l.holder, l.endedAs, tickText(l.lastTick)], [null, "finished", "finished — 3/3 email(s) · 7 unit IDs found, 2 new, 2 saved, 0 failed, 0 older than 3 Sep"]);
      assert.deepEqual(api.st.after.map((f) => [f.runId, f.cancelled, f.by]), [[s.runId, false, "asha"]]);
      assert.equal((await call("GET", `/fetch/schedule?userId=${U}`)).body.lastTick.outcome, "finished — 3/3 email(s) · 7 unit IDs found, 2 new, 2 saved, 0 failed, 0 older than 3 Sep");
    });

    await check("nothing to start: 409 with the sentence under `error` too — a fetch already running here, no logged-in Platform ID, a start that fails — and the lease taken for the try is given back each time", async () => {
      await reset("2026-11-02 10:00", false);
      const busy = route();
      busy.st.busyElsewise = true;
      at("2026-11-02 22:00");
      let res = await call("POST", "/fetch/schedule/run", { by: "asha", userId: U });
      assert.deepEqual([res.status, res.body], [409, { started: false, message: "skipped — a fetch is already running", reason: "a fetch is already running", error: "skipped — a fetch is already running" }]);
      let l = await lease();
      assert.deepEqual([l.holder, tickText(l.lastTick), await rowsNow()], [null, "skipped — a fetch is already running", 0]);
      route({ emails: [] });
      res = await call("POST", "/fetch/schedule/run", { by: "asha", userId: U });
      assert.deepEqual([res.status, res.body.error, (await lease()).holder], [409, "skipped — no logged-in Platform ID", null]);
      // The start itself refuses (the in-API update lock, the database): the queued row is closed as failed, the tick says failed.
      const broken = route({ failStart: new Error("Order update is already running. Wait or force kill first.\nstack") });
      res = await call("POST", "/fetch/schedule/run", { by: "asha", userId: U });
      assert.deepEqual([res.status, res.body.error, res.body.failed], [409, "could not start: Order update is already running. Wait or force kill first.", true]);
      l = await lease();
      const row = await OrderJobRun.findOne({ userId: invented, type: "fetch" }).sort({ createdAt: -1 }).lean();
      assert.deepEqual([l.holder, l.endedAs, tickText(l.lastTick), row?.status, row?.message, broken.st.reserved], [null, "failed", "failed — could not start: Order update is already running. Wait or force kill first.", "failed", "Could not start: Order update is already running. Wait or force kill first.", false]);
      // Force kill while the account list was being read is a stop, not a failure.
      const stopped = route({ failStart: new OrderFetchStoppedError() });
      res = await call("POST", "/fetch/schedule/run", { by: "asha", userId: U });
      l = await lease();
      const last = await OrderJobRun.findOne({ userId: invented, type: "fetch" }).sort({ createdAt: -1 }).lean();
      assert.deepEqual([res.status, res.body.failed, l.endedAs, last?.status, stopped.st.reserved], [409, undefined, "skipped", "stopped", false]);
      assert.match(tickText(l.lastTick), /^skipped — Stopped while the account list was being prepared/);
    });

    await check("a workspace Trade Flow has set no clock up for: Run now goes ahead as it always did, and its tick is this process's", async () => {
      const otherId = new Types.ObjectId();
      otherWorkspaces.push(otherId);
      await registerRunIds(db, "fs", run, [otherId]);
      const other = String(otherId);
      route();
      at("2026-11-02 23:00");
      const res = await call("POST", "/fetch/schedule/run", { by: "asha", userId: other });
      assert.deepEqual([res.status, res.body.started, api.st.starts.length, await deps.Leases.countDocuments({ userId: other })], [202, true, 1, 0]);
      const status = (await call("GET", `/fetch/schedule?userId=${other}`)).body;
      assert.match(String(status.lastTick?.outcome), /^started — 3 logged-in Platform ID\(s\)/);
      assert.deepEqual([status.enabled, status.nextAt, status.lease], [false, null, null]);
      await api.finish("completed");
      assert.deepEqual(api.st.after.map((f) => f.cancelled), [false]);
    });

    await check("the clock as the API starts it (index.ts → startFetchScheduleClock): it serves the fetch whatever the .env says, may run it only when FETCH_SCHEDULE lets this machine, seeds a workspace that has no clock, and stops", async () => {
      // A workspace nothing has seeded, and a fetch that opens nothing behind the clock's runner.
      const otherId = new Types.ObjectId();
      otherWorkspaces.push(otherId);
      await registerRunIds(db, "fs", run, [otherId]);
      const other = String(otherId);
      route();
      await deps.Hosts.deleteMany({});
      const had = process.env.FETCH_SCHEDULE;
      const presence = async () => {
        for (let i = 0; i < 100; i++) {
          const row = await deps.Hosts.findById(`${config.systemId}:api`).lean();
          if (row) return row;
          await sleep(100);
        }
        return null;
      };
      try {
        // FETCH_SCHEDULE=0: this machine never runs the nightly fetch — and still serves its clock.
        process.env.FETCH_SCHEDULE = "0";
        at("2026-11-02 23:10");
        const clock = startFetchScheduleClock(other);
        assert.ok(clock, "started");
        assert.equal(startFetchScheduleClock(other), clock, "a second start is the same clock");
        let row = await presence();
        assert.deepEqual([row?.role, row?.host, row?.pid, row?.serves, row?.eligible], ["api", HOST, process.pid, ["fetch"], []]);
        // The poll found no clock for this workspace and asked for the seed: four clocks, the fetch switched off.
        await until("the seed the clock asked for", async () => (await deps.Leases.countDocuments({ userId: otherId })) === 4);
        const seeded = await lease(other);
        assert.deepEqual([clockStamp(seeded.nextAt), seeded.armedAt, seeded.holder, (await deps.Settings.findOne({ userId: otherId }).lean())?.schedules?.fetch?.on], ["3 Nov 05:00", null, null, false]);
        stopFetchScheduleClock();
        // A time such as "0 5 * * *": it may.
        await deps.Hosts.deleteMany({});
        process.env.FETCH_SCHEDULE = "0 5 * * *";
        const second = startFetchScheduleClock(other);
        assert.ok(second && second !== clock, "a new clock after the stop");
        row = await presence();
        assert.deepEqual([row?.serves, row?.eligible], [["fetch"], ["fetch"]]);
        // Switched off (the seed's value): nothing was started by either clock.
        assert.equal(api.st.starts.length, 0);
      } finally {
        stopFetchScheduleClock();
        if (had === undefined) delete process.env.FETCH_SCHEDULE;
        else process.env.FETCH_SCHEDULE = had;
        await Promise.all([deps.Leases.deleteMany({ userId: otherId }), deps.Settings.deleteMany({ userId: otherId }), deps.Hosts.deleteMany({})]);
      }
    });

    await check("stopOrderFetch(userId, reason): the reason is the line the fetch log ends on; without one it reads Force kill, as before", async () => {
      const u = String(new Types.ObjectId());
      await stopOrderFetch(u, "Superseded — Mac-2 took over the nightly fetch at 05:30");
      assert.equal(getOrderFetchJob(u).logs.at(-1)?.message, "Superseded — Mac-2 took over the nightly fetch at 05:30: stopping order fetch and killing Chrome");
      await stopOrderFetch(u);
      assert.equal(getOrderFetchJob(u).logs.at(-1)?.message, "Force kill: stopping order fetch and killing Chrome");
      assert.deepEqual([getOrderFetchJob(u).running, getOrderFetchJob(u).cancelled], [false, true]);
    });

    await check("Run now by a person on another machine (Mac-2's API) while this one holds a run by hand: refused there with this machine's name", async () => {
      await reset("2026-11-02 10:00", false);
      route();
      at("2026-11-02 22:00");
      assert.equal((await call("POST", "/fetch/schedule/run", { by: "asha", userId: U })).status, 202);
      const refused = await claimManual(U, "fetch", "ravi", { me: { ...MAC2, pid: 9 }, deps });
      assert.deepEqual([refused.ok, !refused.ok && refused.code, !refused.ok && refused.message], [false, "held_elsewhere", `The nightly fetch is running on ${HOST} since 22:00`]);
      await api.finish("stopped", "1/3 email(s) · 2 unit IDs found");
      assert.equal(tickText((await lease()).lastTick), "finished — stopped — 1/3 email(s) · 2 unit IDs found");
    });

    console.log("\nA RUN THAT WAS KILLED — the next run removes what it left (scripts/scheduleCheckCleanup.ts)");
    await check("an earlier run's rows and throwaway collections go once it is 30 minutes old — found by the ids it wrote down and by its throwaway clocks; a younger run is left alone; a workspace that is not invented is refused", async () => {
      const now = Date.now();
      const old = (now - STALE_RUN_MS - 60_000).toString(36);
      const young = (now - 5 * 60_000).toString(36);
      assert.ok(runAgeMs(old, now) >= STALE_RUN_MS && runAgeMs(young, now) < STALE_RUN_MS && Number.isNaN(runAgeMs("not a run")));
      const [byIds, byClock, ofYoung] = [new Types.ObjectId(), new Types.ObjectId(), new Types.ObjectId()];
      // The killed run: one workspace it wrote down, one only its throwaway clock names; ledger rows of each.
      await registerRunIds(db, "fs", old, [byIds]);
      await db.collection(`verify_fs_${old}_leases`).insertOne({ userId: byClock, name: "fetch" });
      await registerRunIds(db, "fs", young, [ofYoung]);
      const row = (userId: Types.ObjectId, host: string) => ({ userId, type: "fetch", trigger: "system-cron", by: "schedule", status: "running", total: 0, message: "", meta: { host, pid: 1 }, createdAt: new Date(), updatedAt: new Date() });
      await OrderJobRun.collection.insertMany([row(byIds, `Mac-check-${old}`), row(byIds, "Mac-2"), row(byClock, `Mac-check-${old}`), row(ofYoung, `Mac-check-${young}`)]);
      const rowsOf = (ids: Types.ObjectId[]) => db.collection("order_job_runs").countDocuments({ userId: { $in: ids } });
      const collsOf = async (r: string) => (await db.listCollections({ name: { $regex: `^verify_fs_${r}_` } }, { nameOnly: true }).toArray()).length;
      try {
        const removed = await removeStaleRuns({ ...mine, thisRun: run, now });
        const ofOld = removed.filter((r) => r.run === old);
        assert.deepEqual(ofOld.map((r) => [r.rows, r.collections, r.refused]), [[{ order_job_runs: 3 }, 2, []]]);
        assert.ok(!removed.some((r) => r.run === young || r.run === run), "neither the young run nor this one");
        assert.deepEqual([await rowsOf([byIds, byClock]), await collsOf(old), await rowsOf([ofYoung]), await collsOf(young)], [0, 0, 1, 1]);
        assert.match(describeCleanup(ofOld[0]), new RegExp(`^run ${old} \\(started .* IST\\): rows deleted — order_job_runs 3 · 2 throwaway collection\\(s\\) dropped · 0 Redis key\\(s\\) deleted in db 15$`));
        // The young run, cleaned by name: its row and its collection of ids.
        const ofYoungRun = await cleanRun(mine, young);
        assert.deepEqual([ofYoungRun.rows, ofYoungRun.collections, ofYoungRun.refused], [{ order_job_runs: 1 }, 1, []]);
      } finally {
        // What this check planted is gone whatever happened above — removed here directly, not through the cleanup
        // it is checking.
        await db.collection("order_job_runs").deleteMany({ userId: { $in: [byIds, byClock, ofYoung] } });
        for (const r of [old, young]) for (const name of ["ids", "leases"]) await db.dropCollection(`verify_fs_${r}_${name}`).catch(() => undefined);
      }
      assert.deepEqual([await rowsOf([byIds, byClock, ofYoung]), await collsOf(old), await collsOf(young)], [0, 0, 0]);
      // The workspace of this machine's .env is never one whose rows are deleted — nor is any that owns a real account.
      const real = envWorkspaceId();
      assert.ok(Types.ObjectId.isValid(real) && real !== U, "the .env names a workspace, and it is not this run's");
      assert.equal(await refusalFor(db, new Types.ObjectId(real)), "it is the workspace of this machine's .env");
      assert.equal(await refusalFor(db, new Types.ObjectId(real), ""), "it owns a Platform ID that is not an @verify.invalid one");
      assert.equal(await refusalFor(db, invented), null);
    });
  } finally {
    // Stopped from outside: the interrupt removes what this run made (onInterrupt above) and ends the process.
    if (isInterrupted()) await standStill();
    console.log = said.log;
    console.warn = said.warn;
    Object.assign(scheduleDefaults, realDefaults);
    Object.assign(fetchScheduleDefaults, realFetchDefaults);
    if (server) await new Promise<void>((resolve) => (server as import("node:http").Server).close(() => resolve()));
    const deleted = (await OrderJobRun.deleteMany({ userId: { $in: [invented, ...otherWorkspaces] } })).deletedCount;
    for (const name of dropList) await db.dropCollection(name).catch(() => undefined);
    const left: Record<string, number> = {};
    for (const name of ["order_job_runs", "order_sync_tasks", "order_update_requests", "order_sync_settings", "platformids", "order_details"]) left[name] = await db.collection(name).countDocuments({ userId: { $in: [invented, ...otherWorkspaces] } });
    left[`order_job_runs naming host ${HOST}`] = await db.collection("order_job_runs").countDocuments({ "meta.host": HOST });
    // The ids this run wrote down go last, and only when no row of them is left: else the next run takes it up.
    if (Object.values(left).every((n) => n === 0)) await db.dropCollection(`verify_fs_${run}_ids`).catch(() => undefined);
    left[`verify_fs_${run}_* collections`] = (await db.listCollections({ name: { $regex: `^verify_fs_${run}_` } }, { nameOnly: true }).toArray()).length;
    const realAfter = await realCollections();
    const created = realAfter.filter((n) => !realBefore.includes(n));
    console.log(`\nmongo: ${deleted} ledger row(s) of the invented workspace deleted · ${dropList.length} throwaway collection(s) dropped`);
    console.log(`left behind: ${Object.entries(left).map(([k, v]) => `${k} ${v}`).join(" · ")}${Object.values(left).every((n) => n === 0) ? " — nothing" : " — NOT CLEAN"} · real schedule collections created by this run: ${created.length} (there before: ${realBefore.length ? realBefore.join(", ") : "none"}) · listener ${server ? "closed" : "never opened"} · no Redis, no queue`);
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
