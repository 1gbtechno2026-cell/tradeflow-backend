/**
 * One clock per schedule: the lease, the switches, the seed and the routes (data-quality plan sub-step 7, C1 + C2) —
 * WITHOUT Flipkart, without a worker, without a live queue, and with a clock this script moves by hand.
 *
 *   · slot maths: 00:15 / 06:15 / 12:15 / 18:15, every 2 h at :45, every 3 h at :35, daily at 05:00, in fixed +05:30
 *   · the .env as a hard off for one machine (eligibleHere), which process kind serves which schedule, the seed's clocks
 *   · the seed: 4 switches and 4 clocks from a stubbed .env — the nightly fetch OFF whatever it says; a second seed
 *     with another .env changes nothing; a switch a person set is kept; two seeds at once leave 4 clocks; the
 *     scheduleSeed.ts command refuses without a flag, prints with --check and writes nothing
 *   · the clock: 50 machines on one time → 1 run; switched off → one skipped tick; still running → skipped; a lease
 *     that ran out → the next time is taken over and the old holder is told; a new process of the same machine
 *     adopts an open run; past the late limit → missed; a machine that may not run it never takes a time; the
 *     standby's 2-minute grace; the nightly fetch's one second start within 3 h; no backlog after ten hours asleep;
 *     the first start waits for the next fixed time; a Run-now is refused while another machine holds the lease
 *   · the routes, through the real router on a listener of this process (127.0.0.1, a random port, closed at the
 *     end): GET / PUT /api/orders/schedules, and the two older routes with the keys they always had
 *   · the one question the overview asks Redis — does this machine still hold a sweep's old BullMQ scheduler? —
 *     against two THROWAWAY queues in Redis db 15
 *
 * What is real: Mongo and, for that last question only, Redis. Every collection this run writes is a THROWAWAY one
 * (verify_sl_<run>_…) with the real schemas and indexes, dropped at the end — order_schedule_leases,
 * order_schedule_hosts and order_sync_settings are not written (the first two are not even created), and no row of a
 * real workspace is touched. The rows belong to invented workspace ids; this process runs as an invented
 * TRADE_FLOW_USER_ID. The old routes also READ three real collections for an invented workspace (they find nothing).
 * The two queues (verify-sl-…-<run>, db 15 — the live queues are in db 0 under other names) are obliterated at the end.
 *
 *   npm run test:schedule-lease
 */
import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import mongoose, { Types, type Model } from "mongoose";
import type { IScheduleLease, ScheduleName } from "../models/ScheduleLease.js";
import type { ScheduleClaim, ScheduleClockOptions, ScheduleDeps, ScheduleRunner, ScheduleRunResult } from "../services/scheduleClock.js";
import { describeCleanup, interruptedCleanup, isInterrupted, onInterrupt, removeStaleRuns, standStill, type RunCleanup } from "./scheduleCheckCleanup.js";

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
    console.log(`  FAIL  ${name}\n        ${err instanceof Error ? err.message.split("\n").slice(0, 10).join("\n        ") : err}`);
  }
}

/** "2026-12-01 12:15" (or with seconds) on India's clock. */
const ist = (s: string) => new Date(`${s.replace(" ", "T")}${s.length <= 16 ? ":00" : ""}+05:30`);
const oid = () => new Types.ObjectId();

async function main() {
  // A workspace that does not exist: set before config is read, so nothing here can act as the real one.
  const invented = oid();
  process.env.TRADE_FLOW_USER_ID = String(invented);
  const { config } = await import("../config.js");
  const { OrderJobRun } = await import("../models/OrderJobRun.js");
  const { OrderSyncSettings } = await import("../models/OrderSyncSettings.js");
  const { OrderUpdateRequest } = await import("../models/OrderUpdateRequest.js");
  const { ScheduleHost } = await import("../models/ScheduleHost.js");
  const { SCHEDULE_NAMES, SCHEDULE_ROLE, ScheduleLease, TICKS_KEPT } = await import("../models/ScheduleLease.js");
  const clockLib = await import("../services/scheduleClock.js");
  const { attachRun, claimManual, clockStamp, clockTime, createScheduleClock, describeClock, LEASE_MS, markStarted, MAX_STARTS, nextSlot, pollDelayMs, releaseLease, renewLease, runRefState, scheduleDefaults, slotAtOrBefore, supersededBy, switchOn, tickText } = clockLib;
  const settingsLib = await import("../services/scheduleSettings.js");
  const { changeSchedule, eligibleHere, LEGACY_SCHEDULERS, readLegacyClocks, resolveScheduleSwitches, scheduleProbes, seedClocks, seedPlan, seedSchedules, seedSchedulesAtStart, servedBy, setSchedulePreference, setScheduleSwitch } = settingsLib;
  const { runScheduleSeedCli } = await import("./scheduleSeed.js");
  const { UPDATE_OFD_SWEEP_MS, UPDATE_SWEEP_MS } = await import("../services/orderUpdateQueue.js");
  assert.equal(config.userId, String(invented), "the invented TRADE_FLOW_USER_ID is the one in use");

  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false, serverSelectionTimeoutMS: 15_000 });
  const db = mongoose.connection.db!;
  const run = Date.now().toString(36);
  const realCollections = async () => (await db.listCollections({ name: { $in: ["order_schedule_leases", "order_schedule_hosts"] } }, { nameOnly: true }).toArray()).map((c) => c.name).sort();
  const realBefore = await realCollections();
  // What an earlier run that was killed left behind (its throwaway collections, its two queues in Redis db 15) goes
  // first — once it is 30 minutes old (scripts/scheduleCheckCleanup.ts). This check writes no row to a real collection.
  const redisAt = new URL(config.redisUrl);
  const mine: Omit<RunCleanup, "db"> = { tag: "sl", redis: { host: redisAt.hostname, port: Number(redisAt.port || 6376), password: redisAt.password || undefined } };
  for (const stale of await removeStaleRuns({ ...mine, db, thisRun: run })) console.log(`leftovers of an earlier run that never cleaned up — removed now · ${describeCleanup(stale)}`);
  const dropList: string[] = [];
  const throwaway = <T>(real: Model<T>, name: string): Model<T> => {
    const coll = `verify_sl_${run}_${name}`;
    dropList.push(coll);
    return mongoose.model<T>(`VerifySl${name}${run}`, real.schema, coll) as Model<T>;
  };

  // The clock this script moves by hand.
  let clockMs = ist("2026-12-01 10:00").getTime();
  const at = (s: string) => {
    clockMs = ist(s).getTime();
  };
  const deps: ScheduleDeps = {
    Leases: throwaway(ScheduleLease, "leases"),
    Hosts: throwaway(ScheduleHost, "hosts"),
    Settings: throwaway(OrderSyncSettings, "settings"),
    Updates: throwaway(OrderUpdateRequest, "updates"),
    Runs: throwaway(OrderJobRun, "runs"),
    now: () => new Date(clockMs),
  };
  console.log(`invented workspaces · throwaway collections verify_sl_${run}_* · the clock starts at ${clockStamp(deps.now())} IST\n`);

  // ── helpers ───────────────────────────────────────────────────────────────
  const ALL_ON = { UPDATE_SCHEDULE: "1", FETCH_SCHEDULE: "0 5 * * *" };
  let serial = 0;
  /** A machine no other case uses: presence rows are per machine, not per workspace. */
  const machine = (tag: string) => {
    const n = ++serial;
    return { systemId: `v${n}-${tag}`, host: `host-${tag}` };
  };
  /** A workspace with its 4 clocks and switches, seeded at the current time. `prefer` "" = no preferred machine. */
  const workspace = async (opts: { prefer?: string; on?: Partial<Record<ScheduleName, boolean>> } = {}) => {
    const userId = String(oid());
    await seedSchedules(userId, { env: ALL_ON, me: { systemId: opts.prefer ?? "", host: "seed-host" }, deps });
    for (const [name, on] of Object.entries(opts.on || {})) await setScheduleSwitch(userId, name, on, "check", deps);
    return userId;
  };
  const leaseOf = async (userId: string, name: ScheduleName) => (await deps.Leases.findOne({ userId, name }).lean()) as (IScheduleLease & { _id: Types.ObjectId }) | null;
  const must = async (userId: string, name: ScheduleName) => {
    const lease = await leaseOf(userId, name);
    assert.ok(lease, `the ${name} clock exists`);
    return lease;
  };
  const ticksOf = (lease: IScheduleLease) => lease.ticks.map((t) => `${t.outcome}@${clockTime(t.slotAt)}`);
  /** An update request (throwaway collection) that is open and was written just now. */
  const openUpdate = async (userId: string) => {
    const _id = oid();
    await deps.Updates.collection.insertOne({ _id, userId: new Types.ObjectId(userId), source: "schedule", status: "running", accounts: [], logs: [], createdAt: deps.now(), updatedAt: deps.now() });
    return { kind: "update" as const, id: String(_id) };
  };
  /** A ledger run (throwaway collection), open and written just now. */
  const openRun = async (userId: string, type: "fetch" | "invoices") => {
    const _id = oid();
    await deps.Runs.collection.insertOne({ _id, userId: new Types.ObjectId(userId), type, trigger: "system-cron", status: "running", message: "", createdAt: deps.now(), updatedAt: deps.now() });
    return { kind: type, id: String(_id) };
  };
  /** The run was written to just now — what an account job's progress does. */
  const touch = async (ref: { kind: string; id: string }) => {
    await (ref.kind === "update" ? deps.Updates : deps.Runs).collection.updateOne({ _id: new Types.ObjectId(ref.id) }, { $set: { updatedAt: deps.now() } });
  };
  const closeRun = async (ref: { kind: string; id: string }, status: string, message = "") => {
    await (ref.kind === "update" ? deps.Updates : deps.Runs).collection.updateOne({ _id: new Types.ObjectId(ref.id) }, { $set: { status, message, updatedAt: deps.now() } });
  };
  /** A runner that records what it was asked, and answers what the case says. */
  const recorder = (answer: (claim: ScheduleClaim) => ScheduleRunResult | Promise<ScheduleRunResult>) => {
    const calls: ScheduleClaim[] = [];
    const superseded: Array<Parameters<NonNullable<ScheduleRunner["supersede"]>>[0]> = [];
    const runner: ScheduleRunner = {
      run: async (claim) => {
        calls.push(claim);
        return answer(claim);
      },
      supersede: (info) => {
        superseded.push(info);
      },
    };
    return { calls, superseded, runner };
  };
  const logs: string[] = [];
  const clockFor = (userId: string, who: { systemId: string; host: string; pid?: number }, o: Partial<ScheduleClockOptions> & { name?: ScheduleName; runner?: ScheduleRunner }) => {
    const name = o.name || "update_ofd";
    return createScheduleClock({
      role: o.role || SCHEDULE_ROLE[name],
      userId,
      names: o.names || [name],
      serves: o.serves || [name],
      runners: o.runners || (o.runner ? { [name]: o.runner } : {}),
      me: { systemId: who.systemId, host: who.host, pid: who.pid ?? 1000 + serial, machine: `${who.host}.local` },
      deps,
      seed: o.seed,
      log: (line) => logs.push(line),
    });
  };

  let server: import("node:http").Server | null = null;
  const realDefaults = { ...scheduleDefaults };
  const realProbe = scheduleProbes.legacyClocks;
  // What the services say for themselves ("[schedules] fetch switched ON by asha") is kept, not printed between the checks.
  const said = { log: console.log, warn: console.warn };
  const serviceLines: string[] = [];
  const own = (print: (...a: unknown[]) => void) => (...a: unknown[]) => (typeof a[0] === "string" && a[0].startsWith("[schedules]") ? void serviceLines.push(a[0]) : print(...a));
  console.log = own(said.log);
  console.warn = own(said.warn);
  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
  // Stopped from outside (Ctrl-C, kill): what this run made goes before the process does.
  onInterrupt(() =>
    interruptedCleanup(config.mongoUri, mine, run, () => {
      console.log = said.log;
      console.warn = said.warn;
      server?.close();
    })
  );

  try {
    // The settings collection as it has long been in the database: one row per workspace. The two schedule
    // collections are NOT prepared: the seed has to create them and their indexes itself.
    await deps.Settings.createCollection();
    await deps.Settings.createIndexes();

    // ── slot maths ───────────────────────────────────────────────────────────
    console.log("SLOT MATHS (fixed +05:30)");
    const { clocks: CLOCKS } = seedClocks({}, "Mac");
    await check("update of all active units: 00:15 / 06:15 / 12:15 / 18:15", () => {
      const c = CLOCKS.update_active;
      assert.deepEqual([c.kind, c.everyMin, c.phaseMin, c.lateMin, c.tz], ["every", 360, 15, 180, "Asia/Kolkata"]);
      const day = ["2026-12-01 00:15", "2026-12-01 06:15", "2026-12-01 12:15", "2026-12-01 18:15", "2026-12-02 00:15"];
      let t = ist("2026-12-01 00:00");
      for (const want of day) {
        t = nextSlot(c, t);
        assert.equal(t.toISOString(), ist(want).toISOString());
      }
      assert.equal(nextSlot(c, ist("2026-12-01 12:15")).toISOString(), ist("2026-12-01 18:15").toISOString(), "strictly after: a time is never its own next");
      assert.equal(slotAtOrBefore(c, ist("2026-12-01 12:15")).toISOString(), ist("2026-12-01 12:15").toISOString());
      assert.equal(slotAtOrBefore(c, ist("2026-12-01 12:14:59")).toISOString(), ist("2026-12-01 06:15").toISOString());
      assert.equal(describeClock(c), "every 6 h at 00:15 / 06:15 / 12:15 / 18:15");
    });
    await check("out-for-delivery recheck: every 2 h at :45 — invoice sweep: every 3 h at :35", () => {
      const ofd = CLOCKS.update_ofd;
      assert.deepEqual([ofd.everyMin, ofd.phaseMin, ofd.lateMin], [120, 45, 60]);
      assert.equal(nextSlot(ofd, ist("2026-12-01 10:00")).toISOString(), ist("2026-12-01 10:45").toISOString());
      assert.equal(nextSlot(ofd, ist("2026-12-01 10:45")).toISOString(), ist("2026-12-01 12:45").toISOString());
      assert.equal(nextSlot(ofd, ist("2026-12-01 23:50")).toISOString(), ist("2026-12-02 00:45").toISOString());
      assert.equal(describeClock(ofd), "every 2 h at :45");
      const inv = CLOCKS.invoice_sweep;
      assert.deepEqual([inv.everyMin, inv.phaseMin, inv.lateMin], [180, 35, 90]);
      const got: string[] = [];
      let t = ist("2026-12-01 00:00");
      for (let i = 0; i < 8; i++) got.push(clockTime((t = nextSlot(inv, t))));
      assert.deepEqual(got, ["00:35", "03:35", "06:35", "09:35", "12:35", "15:35", "18:35", "21:35"]);
      assert.equal(describeClock(inv), "every 3 h at :35");
    });
    await check("nightly fetch: daily at 05:00, 3 h late limit; UTC is the only other clock", () => {
      const f = CLOCKS.fetch;
      assert.deepEqual([f.kind, f.at, f.lateMin, f.preferSystemId, f.standbyGraceSec], ["daily", "05:00", 180, "Mac", 120]);
      assert.equal(nextSlot(f, ist("2026-12-01 04:59")).toISOString(), ist("2026-12-01 05:00").toISOString());
      assert.equal(nextSlot(f, ist("2026-12-01 05:00")).toISOString(), ist("2026-12-02 05:00").toISOString());
      assert.equal(nextSlot(f, ist("2026-12-01 23:00")).toISOString(), ist("2026-12-02 05:00").toISOString());
      assert.equal(describeClock(f), "daily at 05:00");
      assert.equal(nextSlot({ ...f, tz: "UTC" }, new Date("2026-12-01T04:59:00Z")).toISOString(), "2026-12-01T05:00:00.000Z");
      assert.throws(() => nextSlot({ ...f, tz: "Europe/Berlin" }, new Date()), /not supported/);
      assert.throws(() => nextSlot({ ...f, at: "5 o'clock" }, new Date()), /not HH:MM/);
    });
    await check("a period that does not divide a day still gives equal gaps (7 h)", () => {
      const c = { kind: "every" as const, at: "", everyMin: 420, phaseMin: 15, tz: "Asia/Kolkata" };
      let t = nextSlot(c, ist("2026-12-01 00:00"));
      for (let i = 0; i < 6; i++) {
        const n = nextSlot(c, t);
        assert.equal(n.getTime() - t.getTime(), 420 * 60_000);
        t = n;
      }
    });

    // ── the .env ─────────────────────────────────────────────────────────────
    console.log("\nTHE .ENV — whether a machine MAY run a schedule, nothing more");
    await check("UPDATE_SCHEDULE=0 → neither update schedule; INVOICE_SCHEDULE=0 → no invoice sweep; unset → both allowed", () => {
      assert.deepEqual(SCHEDULE_NAMES.map((n) => eligibleHere(n, {})), [false, true, true, true]);
      assert.deepEqual(SCHEDULE_NAMES.map((n) => eligibleHere(n, { UPDATE_SCHEDULE: "0" })), [false, false, false, true]);
      assert.deepEqual(SCHEDULE_NAMES.map((n) => eligibleHere(n, { INVOICE_SCHEDULE: "0" })), [false, true, true, false]);
      // dotenv hands "1" for the line "UPDATE_SCHEDULE=1 "; a value typed with the space still means on.
      assert.equal(eligibleHere("update_active", { UPDATE_SCHEDULE: "1 " }), true);
      assert.equal(eligibleHere("update_active", { UPDATE_SCHEDULE: " 0 " }), false);
      assert.equal(eligibleHere("update_ofd", { UPDATE_SCHEDULE: "off" }), false);
    });
    await check("FETCH_SCHEDULE 0, off or unset → never the fetch; any other value → it may", () => {
      for (const v of [undefined, "", "0", "off", "OFF", "false", " 0 "]) assert.equal(eligibleHere("fetch", { FETCH_SCHEDULE: v }), false, `FETCH_SCHEDULE=${JSON.stringify(v)}`);
      for (const v of ["1", "on", "true", "0 5 * * *", "30 4 * * *"]) assert.equal(eligibleHere("fetch", { FETCH_SCHEDULE: v }), true, `FETCH_SCHEDULE=${JSON.stringify(v)}`);
    });
    await check("WORKER_ROLES decides which process serves which schedule: the API the fetch, a worker its roles' sweeps", () => {
      assert.deepEqual(servedBy("api"), ["fetch"]);
      assert.deepEqual(servedBy("worker"), ["update_active", "update_ofd", "invoice_sweep"]);
      assert.deepEqual(servedBy("worker", ["checkout", "cancel", "sync", "update"]), ["update_active", "update_ofd"]);
      assert.deepEqual(servedBy("worker", ["invoice"]), ["invoice_sweep"]);
      assert.deepEqual(servedBy("worker", ["checkout"]), []);
      assert.deepEqual(servedBy("api", ["update", "invoice"]), ["fetch"]);
    });
    await check("the seed's clocks follow the .env once: the *_SWEEP_MS periods, the time in FETCH_SCHEDULE, CRON_TIMEZONE", () => {
      const custom = seedClocks({ UPDATE_SWEEP_MS: "3600000", UPDATE_OFD_SWEEP_MS: "1800000", INVOICE_SWEEP_MS: "60000", FETCH_SCHEDULE: "30 4 * * *", CRON_TIMEZONE: "UTC" }, "Mac-2");
      assert.deepEqual([custom.clocks.update_active.everyMin, custom.clocks.update_active.phaseMin, custom.clocks.update_active.lateMin], [60, 15, 30]);
      assert.deepEqual([custom.clocks.update_ofd.everyMin, custom.clocks.update_ofd.phaseMin], [30, 15], "every 30 min: :45 is :15 of the period");
      assert.equal(custom.clocks.invoice_sweep.everyMin, 30, "the invoice sweep's floor of 30 min is kept");
      assert.deepEqual([custom.clocks.fetch.at, custom.clocks.fetch.tz, custom.clocks.fetch.preferSystemId], ["04:30", "UTC", "Mac-2"]);
      assert.equal(seedClocks({ FETCH_SCHEDULE: "0" }).clocks.fetch.at, "05:00");
      assert.equal(seedClocks({ FETCH_SCHEDULE: "1" }).clocks.fetch.at, "05:00");
      assert.match(seedClocks({ FETCH_SCHEDULE: "0 5 * * 1-5" }).notes.join(" "), /not a daily/);
      assert.equal(seedClocks({ CRON_TIMEZONE: "Asia/Calcutta" }).clocks.fetch.tz, "Asia/Kolkata");
      assert.throws(() => seedClocks({ CRON_TIMEZONE: "Europe/Berlin" }), /not supported by the schedule clock/);
    });

    // ── the seed ─────────────────────────────────────────────────────────────
    console.log("\nTHE SEED — only what is missing; the nightly fetch always starts off");
    const seeded = String(oid());
    const A = { systemId: "Mac", host: "Mac" };
    const indexesOf = async (model: Model<never>) => (await db.listCollections({ name: model.collection.collectionName }, { nameOnly: true }).toArray()).length ? (await model.collection.indexes()).map((i) => `${i.name}${i.unique ? " unique" : ""}${i.expireAfterSeconds ? ` ttl ${i.expireAfterSeconds / 86_400} d` : ""}`) : null;
    await check("the two schedule collections are created by the seed alone, with their indexes, before its first insert — importing their models and connecting creates nothing", async () => {
      assert.deepEqual([ScheduleLease.schema.get("autoCreate"), ScheduleLease.schema.get("autoIndex"), ScheduleHost.schema.get("autoCreate"), ScheduleHost.schema.get("autoIndex")], [false, false, false, false], "the models never create or index by themselves");
      assert.deepEqual([await indexesOf(deps.Leases as unknown as Model<never>), await indexesOf(deps.Hosts as unknown as Model<never>)], [null, null], "not there before a seed");
      // A read of what a seed would do creates nothing either.
      await seedPlan(String(oid()), { env: ALL_ON, me: A, deps });
      assert.equal(await indexesOf(deps.Leases as unknown as Model<never>), null);
      at("2026-12-01 10:00");
      await seedSchedules(String(oid()), { env: ALL_ON, me: A, deps });
      assert.deepEqual(await indexesOf(deps.Leases as unknown as Model<never>), ["_id_", "userId_1_name_1 unique"]);
      assert.deepEqual(await indexesOf(deps.Hosts as unknown as Model<never>), ["_id_", "seenAt_1 ttl 7 d"]);
    });
    await check("a .env with every schedule allowed (FETCH_SCHEDULE='0 5 * * *', UPDATE_SCHEDULE=1): 4 switches — fetch OFF, the three sweeps ON — and 4 clocks", async () => {
      at("2026-12-01 10:00");
      const out = await seedSchedules(seeded, { env: ALL_ON, me: A, deps });
      assert.deepEqual(out.switchesWritten, [...SCHEDULE_NAMES]);
      assert.deepEqual(out.clocksWritten, [...SCHEDULE_NAMES]);
      const sw = await resolveScheduleSwitches(seeded, deps);
      assert.deepEqual(SCHEDULE_NAMES.map((n) => sw[n].on), [false, true, true, true], "fetch off although this .env would run it");
      for (const n of SCHEDULE_NAMES) assert.deepEqual([sw[n].source, sw[n].changedBy, sw[n].changedAt?.toISOString()], ["seed", "seed:Mac", ist("2026-12-01 10:00").toISOString()]);
      const leases = await deps.Leases.find({ userId: seeded }).lean();
      assert.equal(leases.length, 4);
      const next = Object.fromEntries(leases.map((l) => [l.name, clockStamp(l.nextAt)]));
      assert.deepEqual(next, { fetch: "2 Dec 05:00", update_active: "1 Dec 12:15", update_ofd: "1 Dec 10:45", invoice_sweep: "1 Dec 12:35" });
      for (const l of leases) assert.deepEqual([l.clock.preferSystemId, l.clock.standbyGraceSec, l.holder, l.armedAt, l.lastSlotAt, l.ticks.length], ["Mac", 120, null, null, null, 0], l.name);
    });
    await check("a second seed with another .env, on another machine and later: nothing changes", async () => {
      const before = JSON.stringify([await deps.Settings.find({ userId: seeded }).lean(), await deps.Leases.find({ userId: seeded }).sort({ name: 1 }).lean()]);
      at("2026-12-01 16:00");
      const out = await seedSchedules(seeded, { env: { UPDATE_SCHEDULE: "0", INVOICE_SCHEDULE: "0", FETCH_SCHEDULE: "0", UPDATE_SWEEP_MS: "3600000", CRON_TIMEZONE: "UTC" }, me: { systemId: "Mac-2", host: "Mac-2" }, deps });
      assert.deepEqual([out.switchesWritten, out.clocksWritten], [[], []]);
      assert.equal(JSON.stringify([await deps.Settings.find({ userId: seeded }).lean(), await deps.Leases.find({ userId: seeded }).sort({ name: 1 }).lean()]), before);
      assert.ok(out.plan.switches.every((s) => !s.write) && out.plan.clocks.every((c) => !c.write));
      at("2026-12-01 10:00");
    });
    await check("a machine with UPDATE_SCHEDULE=0 and FETCH_SCHEDULE=0 seeds the update switches OFF, the invoice sweep ON, the fetch OFF", async () => {
      const u = String(oid());
      await seedSchedules(u, { env: { UPDATE_SCHEDULE: "0", FETCH_SCHEDULE: "0" }, me: A, deps });
      const sw = await resolveScheduleSwitches(u, deps);
      assert.deepEqual(SCHEDULE_NAMES.map((n) => sw[n].on), [false, false, false, true]);
    });
    await check("a switch a person set before the seed is kept, and the row's other settings are not touched", async () => {
      const u = String(oid());
      await deps.Settings.collection.insertOne({ userId: new Types.ObjectId(u), fetchMode: "api", fetchSinceDays: 60, scheduledInvoices: true });
      await setScheduleSwitch(u, "update_ofd", false, "asha", deps);
      const out = await seedSchedules(u, { env: ALL_ON, me: A, deps });
      assert.deepEqual(out.switchesWritten, ["fetch", "update_active", "invoice_sweep"]);
      const sw = await resolveScheduleSwitches(u, deps);
      assert.deepEqual([sw.update_ofd.on, sw.update_ofd.source, sw.update_ofd.changedBy], [false, "ui", "asha"]);
      const row = await deps.Settings.findOne({ userId: u }).lean();
      assert.deepEqual([row?.fetchMode, row?.fetchSinceDays, row?.scheduledInvoices], ["api", 60, true]);
      assert.equal(await deps.Settings.countDocuments({ userId: u }), 1);
    });
    await check("a row another setting's save created has no switch: none reads as set, and the seed fills all four", async () => {
      const u = String(oid());
      await deps.Settings.updateOne({ userId: new Types.ObjectId(u) }, { $set: { updateWindows: 5 }, $setOnInsert: { userId: new Types.ObjectId(u) } }, { upsert: true });
      const sw = await resolveScheduleSwitches(u, deps);
      assert.deepEqual(SCHEDULE_NAMES.map((n) => [sw[n].on, sw[n].source]), SCHEDULE_NAMES.map(() => [false, "unset"]));
      assert.equal(switchOn(await deps.Settings.findOne({ userId: u }).lean(), "update_active"), false);
      assert.deepEqual((await seedSchedules(u, { env: ALL_ON, me: A, deps })).switchesWritten, [...SCHEDULE_NAMES]);
    });
    await check("two seeds at the same moment: still 4 clocks and one settings row, no error", async () => {
      const u = String(oid());
      const outs = await Promise.all([seedSchedules(u, { env: ALL_ON, me: A, deps }), seedSchedules(u, { env: ALL_ON, me: { systemId: "Mac-2", host: "Mac-2" }, deps }), seedSchedules(u, { env: ALL_ON, me: A, deps })]);
      assert.equal(await deps.Leases.countDocuments({ userId: u }), 4);
      assert.equal(await deps.Settings.countDocuments({ userId: u }), 1);
      assert.deepEqual(outs.flatMap((o) => o.clocksWritten).sort(), [...SCHEDULE_NAMES].sort(), "each clock written by exactly one of them");
      assert.deepEqual(outs.flatMap((o) => o.switchesWritten).sort(), [...SCHEDULE_NAMES].sort(), "each switch written by exactly one of them");
    });
    await check("the seed as the API and the worker call it at start: one line of what it wrote, 'nothing written' the next time, and never a reason not to start", async () => {
      at("2026-12-01 10:00");
      const u = String(oid());
      const from = serviceLines.length;
      await seedSchedulesAtStart(u, { env: { UPDATE_SCHEDULE: "0", FETCH_SCHEDULE: "0" }, me: A, deps });
      await seedSchedulesAtStart(u, { env: ALL_ON, me: { systemId: "Mac-2", host: "Mac-2" }, deps });
      await seedSchedulesAtStart("", { env: ALL_ON, me: A, deps });
      // A .env the clock cannot be built from: said, nothing written, and the caller carries on.
      const other = String(oid());
      await seedSchedulesAtStart(other, { env: { CRON_TIMEZONE: "Europe/Berlin" }, me: A, deps });
      assert.deepEqual(serviceLines.slice(from), [
        "[schedules] first start on this workspace (seed:Mac) — switches: fetch OFF, update_active OFF, update_ofd OFF, invoice_sweep ON — clocks: fetch daily at 05:00 (next 2 Dec 05:00); update_active every 6 h at 00:15 / 06:15 / 12:15 / 18:15 (next 1 Dec 12:15); update_ofd every 2 h at :45 (next 1 Dec 10:45); invoice_sweep every 3 h at :35 (next 1 Dec 12:35)",
        "[schedules] switches and clocks are in place — nothing written",
        "[schedules] TRADE_FLOW_USER_ID is not set — no schedule switches or clocks",
        '[schedules] could not write the schedule switches and clocks: CRON_TIMEZONE "Europe/Berlin" is not supported by the schedule clock — use Asia/Kolkata or UTC (or leave it out)',
      ]);
      assert.deepEqual([await deps.Leases.countDocuments({ userId: u }), await deps.Leases.countDocuments({ userId: other }), await deps.Settings.countDocuments({ userId: other })], [4, 0, 0]);
    });
    await check("scheduleSeed.ts: no flag → refuses (exit 2); --check prints the 4 switches and 4 clocks and writes nothing; --go writes them", async () => {
      const u = String(oid());
      const count = async () => [await deps.Leases.countDocuments({ userId: u }), await deps.Settings.countDocuments({ userId: u })];
      const lines: string[] = [];
      const io = { userId: u, out: (l: string) => lines.push(l), seed: { env: ALL_ON, me: A, deps } };
      assert.equal(await runScheduleSeedCli([], io), 2);
      assert.match(lines.join("\n"), /A flag is required/);
      assert.equal(await runScheduleSeedCli(["--check", "--go"], io), 2);
      assert.deepEqual(await count(), [0, 0]);
      lines.length = 0;
      assert.equal(await runScheduleSeedCli(["--check"], io), 0);
      const text = lines.join("\n");
      assert.equal(lines.filter((l) => /^\s+\S+\s+would write (ON|OFF)/.test(l)).length, 4, text);
      assert.equal(lines.filter((l) => /would create/.test(l)).length, 4, text);
      assert.match(text, /fetch\s+would write OFF/);
      assert.match(text, /update_active\s+would write ON/);
      assert.match(text, /would write 4 switch\(es\) and 4 clock\(s\); nothing was written\./);
      assert.deepEqual(await count(), [0, 0], "--check wrote nothing");
      lines.length = 0;
      assert.equal(await runScheduleSeedCli(["--go"], io), 0);
      assert.match(lines.join("\n"), /wrote 4 switch\(es\) \(fetch, update_active, update_ofd, invoice_sweep\) and 4 clock\(s\)/);
      assert.deepEqual(await count(), [4, 1]);
      lines.length = 0;
      assert.equal(await runScheduleSeedCli(["--check"], io), 0);
      assert.match(lines.join("\n"), /would write 0 switch\(es\) and 0 clock\(s\)/);
      const plan = await seedPlan(u, { env: ALL_ON, me: A, deps });
      assert.ok(plan.switches.every((s) => !s.write && s.current?.changedBy === "seed:Mac"));
    });

    // ── the clock ────────────────────────────────────────────────────────────
    console.log("\nTHE CLOCK — one compare-and-set per due time");
    await check("50 machines look at one due time at once → exactly 1 runs it; the clock moves on once", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const ref = await openUpdate(u);
      const rec = recorder(() => ({ runRef: ref }));
      const clocks = Array.from({ length: 50 }, (_, i) => clockFor(u, { systemId: `v50-${run}-${i}`, host: `host-${i}`, pid: 5000 + i }, { runner: rec.runner }));
      at("2026-12-01 10:44");
      await Promise.all(clocks.map((c) => c.pollOnce()));
      const armed = await must(u, "update_ofd");
      assert.ok(armed.armedAt, "the first machine that may run it arms the clock");
      assert.deepEqual([armed.ticks.length, clockStamp(armed.nextAt)], [0, "1 Dec 10:45"], "not due yet: nothing happens");
      at("2026-12-01 10:45:05");
      await Promise.all(clocks.map((c) => c.pollOnce()));
      const lease = await must(u, "update_ofd");
      assert.equal(rec.calls.length, 1, "runner calls");
      assert.deepEqual(ticksOf(lease), ["ran@10:45"]);
      assert.deepEqual([clockStamp(lease.nextAt), clockStamp(lease.lastSlotAt), clockTime(lease.slotAt), lease.attempt, lease.manual], ["1 Dec 12:45", "1 Dec 10:45", "10:45", 1, false]);
      assert.equal(lease.holder?.token, rec.calls[0].token);
      assert.equal(lease.holder?.systemId, rec.calls[0].holder.systemId);
      assert.deepEqual(lease.runRef, ref);
      assert.deepEqual(lease.lastTick?.runRef, ref, "the tick names the run");
      assert.equal(clocks.filter((c) => c.holds("update_ofd")).length, 1, "one machine holds the lease");
      assert.equal(lease.expiresAt?.getTime(), ist("2026-12-01 10:45:05").getTime() + LEASE_MS);
      assert.equal(tickText(lease.lastTick), `ran on ${rec.calls[0].holder.host}`);
    });

    await check("switched off → one skipped tick per time, and the clock moves on once", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "", on: { update_ofd: false } });
      const rec = recorder(() => ({ done: true }));
      const c = clockFor(u, machine("off"), { runner: rec.runner });
      at("2026-12-01 10:44");
      await c.pollOnce();
      at("2026-12-01 10:45:10");
      await c.pollOnce();
      let lease = await must(u, "update_ofd");
      assert.deepEqual([ticksOf(lease), lease.lastTick?.reason, clockStamp(lease.nextAt), lease.holder], [["skipped@10:45"], "switched off", "1 Dec 12:45", null]);
      at("2026-12-01 10:45:40");
      await c.pollOnce();
      at("2026-12-01 11:30");
      await c.pollOnce();
      assert.deepEqual(ticksOf(await must(u, "update_ofd")), ["skipped@10:45"], "the same time is not skipped twice");
      at("2026-12-01 12:45:05");
      await c.pollOnce();
      lease = await must(u, "update_ofd");
      assert.deepEqual([ticksOf(lease), clockStamp(lease.nextAt)], [["skipped@10:45", "skipped@12:45"], "1 Dec 14:45"]);
      assert.equal(rec.calls.length, 0, "nothing ran");
      assert.equal(tickText(lease.lastTick), "skipped — switched off");
      // Switched on again: the next time runs.
      await setScheduleSwitch(u, "update_ofd", true, "asha", deps);
      at("2026-12-01 14:45:05");
      await c.pollOnce();
      assert.equal(rec.calls.length, 1);
    });

    await check("the earlier run still holds a live lease → the next time is skipped — still running; the lease is renewed while its run is open", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const ref = await openUpdate(u);
      const rec = recorder(() => ({ runRef: ref }));
      const who = machine("run");
      const c = clockFor(u, who, { name: "update_active", runner: rec.runner });
      at("2026-12-01 12:14");
      await c.pollOnce();
      at("2026-12-01 12:15:05");
      await c.pollOnce();
      assert.equal(rec.calls.length, 1);
      // 20 minutes of polls: the lease never runs out while the run is being written to.
      for (const t of ["12:19:30", "12:24:00", "12:28:30", "12:33:00"]) {
        at(`2026-12-01 ${t}`);
        await touch(ref);
        await c.pollOnce();
        const l = await must(u, "update_active");
        assert.equal(l.expiresAt?.getTime(), clockMs + LEASE_MS, `renewed at ${t}`);
      }
      // The machine sleeps until just before the next time: nobody took over, so it carries on with the same claim.
      at("2026-12-01 18:14");
      await touch(ref);
      await c.pollOnce();
      assert.equal((await must(u, "update_active")).holder?.token, rec.calls[0].token);
      at("2026-12-01 18:15:05");
      await touch(ref);
      await c.pollOnce();
      const lease = await must(u, "update_active");
      assert.deepEqual(ticksOf(lease), ["ran@12:15", "skipped@18:15"]);
      assert.equal(lease.lastTick?.reason, `the 12:15 run is still going on ${who.host}`);
      assert.deepEqual([clockStamp(lease.nextAt), lease.holder?.systemId, rec.calls.length], ["2 Dec 00:15", who.systemId, 1]);
    });

    await check("renewals stop → after 5 min another machine takes the NEXT time (took_over); the old holder's renew is lost and its supersede fires once", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const [ma, mb] = [machine("A"), machine("B")];
      const refA = await openUpdate(u);
      const refB = await openUpdate(u);
      const a = recorder(() => ({ runRef: refA }));
      const b = recorder(() => ({ runRef: refB }));
      const ca = clockFor(u, ma, { name: "update_active", runner: a.runner });
      const cb = clockFor(u, mb, { name: "update_active", runner: b.runner });
      at("2026-12-01 12:14");
      await ca.pollOnce();
      await cb.pollOnce();
      at("2026-12-01 12:15:05");
      await ca.pollOnce();
      assert.equal(a.calls.length, 1);
      // A goes to sleep. B looks before the 5 minutes are over: the lease is live, nothing to do.
      at("2026-12-01 12:19");
      await cb.pollOnce();
      assert.equal((await must(u, "update_active")).holder?.systemId, ma.systemId);
      assert.equal(await renewLease({ userId: u, name: "update_active", token: "not-the-token" }, deps), false, "a renew with another token matches nothing");
      // The next fixed time, A still asleep: its lease ran out at 12:20:05.
      at("2026-12-01 18:15:05");
      await cb.pollOnce();
      let lease = await must(u, "update_active");
      assert.equal(b.calls.length, 1);
      assert.deepEqual([lease.holder?.systemId, clockTime(lease.slotAt), lease.attempt], [mb.systemId, "18:15", 1]);
      assert.deepEqual(ticksOf(lease), ["ran@12:15", "took_over@18:15"]);
      assert.equal(lease.lastTick?.reason, `${ma.host}'s lease ran out at 12:20 and its 12:15 run did not finish`);
      assert.deepEqual(b.calls[0].replaced, { systemId: ma.systemId, host: ma.host, slotAt: ist("2026-12-01 12:15"), expiredAt: new Date(ist("2026-12-01 12:15:05").getTime() + LEASE_MS) });
      // A wakes.
      at("2026-12-01 18:20");
      assert.equal(await renewLease({ userId: u, name: "update_active", token: a.calls[0].token }, deps), false, "the old holder's renew reports lost");
      await ca.pollOnce();
      assert.equal(a.superseded.length, 1);
      assert.deepEqual([a.superseded[0].by, a.superseded[0].token, clockTime(a.superseded[0].slotAt), a.superseded[0].runRef, clockTime(a.superseded[0].at)], [{ systemId: mb.systemId, host: mb.host }, a.calls[0].token, "12:15", refA, "18:15"]);
      assert.equal(ca.holds("update_active"), null);
      await ca.pollOnce();
      assert.equal(a.superseded.length, 1, "told once");
      lease = await must(u, "update_active");
      assert.deepEqual(ticksOf(lease), ["ran@12:15", "took_over@18:15", "superseded@12:15"]);
      assert.equal(lease.ticks[2].reason, `${mb.host} took over at 18:15`);
      assert.equal(lease.lastTick?.outcome, "took_over", "the last tick stays the run's, not the note of the machine that lost it");
      assert.equal(lease.holder?.token, b.calls[0].token, "B still holds it");
      // What an account job of A's old sweep asks before it calls Flipkart.
      assert.deepEqual(await supersededBy(u, { name: "update_active", token: a.calls[0].token }, ma.systemId, deps), { systemId: mb.systemId, host: mb.host, at: ist("2026-12-01 18:15:05"), slotAt: ist("2026-12-01 18:15") });
      assert.equal(await supersededBy(u, { name: "update_active", token: b.calls[0].token }, mb.systemId, deps), null, "B's own claim stands");
      assert.equal(await supersededBy(u, { name: "update_active", token: "" }, ma.systemId, deps), null, "a request without a claim is not a schedule's");
      // After B's run has finished and released, A's leftovers are still superseded.
      await releaseLease(b.calls[0], { outcome: "finished", reason: "ok 12, failed 0", runRef: refB }, deps);
      assert.equal((await supersededBy(u, { name: "update_active", token: a.calls[0].token }, ma.systemId, deps))?.host, mb.host);
      lease = await must(u, "update_active");
      assert.deepEqual([lease.holder, lease.endedAs, tickText(lease.lastTick), lease.lastTick?.runRef], [null, "finished", "finished — ok 12, failed 0", refB]);
    });

    await check("a new process of the same machine adopts the lease of a run that is still open, and releases it when the run closes", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const who = machine("same");
      const ref = await openUpdate(u);
      const first = recorder(() => ({ runRef: ref }));
      const second = recorder(() => ({ runRef: ref }));
      const p1 = clockFor(u, { ...who, pid: 111 }, { name: "update_active", runner: first.runner });
      at("2026-12-01 12:14");
      await p1.pollOnce();
      at("2026-12-01 12:15:05");
      await p1.pollOnce();
      const token = first.calls[0].token;
      // Another machine never adopts it.
      const other = recorder(() => ({ runRef: ref }));
      const stranger = clockFor(u, machine("other"), { name: "update_active", runner: other.runner });
      at("2026-12-01 12:15:40");
      await stranger.pollOnce();
      assert.deepEqual([(await must(u, "update_active")).holder?.pid, stranger.holds("update_active")], [111, null]);
      // The worker is restarted in the middle of the sweep.
      const p2 = clockFor(u, { ...who, pid: 222 }, { name: "update_active", runner: second.runner });
      at("2026-12-01 12:16");
      await touch(ref);
      await p2.pollOnce();
      let lease = await must(u, "update_active");
      assert.deepEqual([lease.holder?.pid, lease.holder?.token, lease.holder?.systemId, p2.holds("update_active")?.token], [222, token, who.systemId, token], "same claim, new process");
      assert.equal(lease.expiresAt?.getTime(), clockMs + LEASE_MS);
      assert.deepEqual([ticksOf(lease), second.calls.length], [["ran@12:15"], 0], "nothing is started again");
      // Past the first process's 5 minutes: still held.
      at("2026-12-01 12:25");
      await touch(ref);
      await p2.pollOnce();
      assert.equal((await must(u, "update_active")).holder?.token, token);
      at("2026-12-01 12:30");
      await closeRun(ref, "done");
      await p2.pollOnce();
      lease = await must(u, "update_active");
      assert.deepEqual([lease.holder, lease.endedAs, ticksOf(lease), lease.finishedAt?.toISOString(), p2.holds("update_active")], [null, "finished", ["ran@12:15", "finished@12:15"], ist("2026-12-01 12:30").toISOString(), null]);
      assert.deepEqual(lease.lastTick?.runRef, ref);
    });
    await check("an earlier process's lease whose run has already closed is released, not adopted; a run nothing wrote to for 30 min counts as closed", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const who = machine("gone");
      const ref = await openUpdate(u);
      const rec = recorder(() => ({ runRef: ref }));
      at("2026-12-01 12:14");
      const p1 = clockFor(u, { ...who, pid: 1 }, { name: "update_active", runner: rec.runner });
      await p1.pollOnce();
      at("2026-12-01 12:15:05");
      await p1.pollOnce();
      at("2026-12-01 12:17");
      await closeRun(ref, "cancelled");
      const p2 = clockFor(u, { ...who, pid: 2 }, { name: "update_active", runner: rec.runner });
      await p2.pollOnce();
      const lease = await must(u, "update_active");
      assert.deepEqual([lease.holder, tickText(lease.lastTick), p2.holds("update_active")], [null, "finished — the request was stopped", null]);
      // The 30-minute rule, on its own.
      at("2026-12-01 13:00");
      const quiet = await openUpdate(u);
      assert.deepEqual(await runRefState(quiet, deps), { open: true });
      at("2026-12-01 13:29");
      assert.deepEqual(await runRefState(quiet, deps), { open: true });
      at("2026-12-01 13:31");
      assert.deepEqual(await runRefState(quiet, deps), { open: false, outcome: "failed", reason: "nothing was written to its run for 30 min" });
      assert.equal((await runRefState({ kind: "update", id: String(oid()) }, deps)).open, false, "a request that is gone is closed");
      const fetchRun = await openRun(u, "fetch");
      await closeRun(fetchRun, "failed", "The API restarted while this fetch was running — start it again");
      assert.deepEqual(await runRefState(fetchRun, deps), { open: false, outcome: "failed", reason: "The API restarted while this fetch was running — start it again" });
    });

    await check("a time nobody dealt with within the late limit is missed, not run; inside the limit it still runs, late", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const rec = recorder(() => ({ done: true, reason: "nothing out for delivery" }));
      const c = clockFor(u, machine("late"), { runner: rec.runner });
      at("2026-12-01 10:44");
      await c.pollOnce();
      // Every machine asleep until 65 minutes after 10:45; the limit for the 2-hourly recheck is 60.
      at("2026-12-01 11:50");
      await c.pollOnce();
      let lease = await must(u, "update_ofd");
      assert.deepEqual([ticksOf(lease), clockStamp(lease.nextAt), rec.calls.length, lease.holder], [["missed@10:45"], "1 Dec 12:45", 0, null]);
      assert.equal(lease.lastTick?.reason, "no machine that may run it was awake within 60 min (looked at 11:50)");
      // 55 minutes late is inside it.
      at("2026-12-01 13:40");
      await c.pollOnce();
      lease = await must(u, "update_ofd");
      assert.deepEqual([ticksOf(lease), rec.calls.length, rec.calls[0]?.lateMs], [["missed@10:45", "ran@12:45"], 1, 55 * 60_000]);
      assert.equal(lease.lastTick?.reason, "nothing out for delivery · started 55 min late");
    });

    await check("a machine that may not run a schedule never takes a time: nothing on time, missed once the limit has passed, switched off said on time", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const rec = recorder(() => ({ done: true }));
      // Serves the schedule (its role), but its .env says no: `names` is empty.
      const c = clockFor(u, machine("env0"), { names: [], serves: ["update_ofd"], runner: rec.runner });
      at("2026-12-01 10:44");
      await c.pollOnce();
      at("2026-12-01 10:45:10");
      await c.pollOnce();
      at("2026-12-01 11:40");
      await c.pollOnce();
      let lease = await must(u, "update_ofd");
      assert.deepEqual([lease.ticks.length, clockStamp(lease.nextAt), lease.armedAt, lease.holder], [0, "1 Dec 10:45", null, null], "on time and inside the limit it writes nothing");
      at("2026-12-01 11:46");
      await c.pollOnce();
      lease = await must(u, "update_ofd");
      assert.deepEqual([ticksOf(lease), clockStamp(lease.nextAt)], [["missed@10:45"], "1 Dec 12:45"]);
      await setScheduleSwitch(u, "update_ofd", false, "asha", deps);
      at("2026-12-01 12:45:10");
      await c.pollOnce();
      lease = await must(u, "update_ofd");
      assert.deepEqual([ticksOf(lease), lease.lastTick?.reason, clockStamp(lease.nextAt)], [["missed@10:45", "skipped@12:45"], "switched off", "1 Dec 14:45"]);
      assert.deepEqual([rec.calls.length, lease.armedAt, lease.holder, c.holds("update_ofd")], [0, null, null, null], "it never ran anything and never armed the clock");
    });

    await check("the standby waits out the 2-minute grace while the preferred machine was seen lately — and does not wait when it has been away over 10 min", async () => {
      // The preferred machine is around but does not take the time (say, its poll is slow).
      at("2026-12-01 10:00");
      const [p, s] = [machine("pref"), machine("standby")];
      let u = await workspace({ prefer: p.systemId });
      const pr = recorder(() => ({ done: true }));
      const sr = recorder(() => ({ done: true }));
      let cp = clockFor(u, p, { runner: pr.runner });
      let cs = clockFor(u, s, { runner: sr.runner });
      at("2026-12-01 10:44");
      await cp.pollOnce();
      await cs.pollOnce();
      at("2026-12-01 10:45:10");
      await cs.pollOnce();
      at("2026-12-01 10:46:50");
      await cs.pollOnce();
      assert.deepEqual([sr.calls.length, clockStamp((await must(u, "update_ofd")).nextAt)], [0, "1 Dec 10:45"], "110 s after the time the standby still waits");
      at("2026-12-01 10:47:05");
      await cs.pollOnce();
      let lease = await must(u, "update_ofd");
      assert.deepEqual([sr.calls.length, pr.calls.length, lease.lastTick?.host, clockStamp(lease.nextAt)], [1, 0, s.host, "1 Dec 12:45"], "125 s after it, the standby runs it");
      // The preferred machine itself never waits: seen a minute ago, it takes the next time although the standby looked first.
      at("2026-12-01 12:44");
      await cp.pollOnce();
      at("2026-12-01 12:45:05");
      await cs.pollOnce();
      await cp.pollOnce();
      lease = await must(u, "update_ofd");
      assert.deepEqual([pr.calls.length, sr.calls.length, lease.lastTick?.host], [1, 1, p.host]);

      // Away for more than 10 minutes: no wait.
      at("2026-12-01 10:00");
      const [p2, s2] = [machine("pref2"), machine("standby2")];
      u = await workspace({ prefer: p2.systemId });
      const pr2 = recorder(() => ({ done: true }));
      const sr2 = recorder(() => ({ done: true }));
      cp = clockFor(u, p2, { runner: pr2.runner });
      cs = clockFor(u, s2, { runner: sr2.runner });
      at("2026-12-01 10:30");
      await cp.pollOnce();
      at("2026-12-01 10:45:10");
      await cs.pollOnce();
      assert.deepEqual([sr2.calls.length, (await must(u, "update_ofd")).lastTick?.host], [1, s2.host], "the preferred machine was last seen 15 min ago");

      // Around, but its .env does not let it run this schedule: nothing to wait for.
      at("2026-12-01 10:00");
      const [p3, s3] = [machine("pref3"), machine("standby3")];
      u = await workspace({ prefer: p3.systemId });
      const sr3 = recorder(() => ({ done: true }));
      cp = clockFor(u, p3, { names: [], serves: ["update_ofd"], runner: pr.runner });
      cs = clockFor(u, s3, { runner: sr3.runner });
      at("2026-12-01 10:44:30");
      await cp.pollOnce();
      await cs.pollOnce();
      at("2026-12-01 10:45:10");
      await cs.pollOnce();
      assert.equal(sr3.calls.length, 1);
    });

    await check(`the nightly fetch may start a second time for the same night — within 3 h, at most ${MAX_STARTS.fetch} starts; no sweep ever does`, async () => {
      assert.deepEqual(MAX_STARTS, { fetch: 2, update_active: 1, update_ofd: 1, invoice_sweep: 1 });
      at("2026-12-01 10:00");
      const who = machine("api");
      const u = await workspace({ prefer: who.systemId, on: { fetch: true } });
      const run1 = await openRun(u, "fetch");
      const one = recorder(() => ({ runRef: run1 }));
      const api1 = clockFor(u, { ...who, pid: 1 }, { name: "fetch", runner: one.runner });
      at("2026-12-02 04:59");
      await api1.pollOnce();
      at("2026-12-02 05:00:10");
      await api1.pollOnce();
      assert.deepEqual([one.calls.length, one.calls[0].attempt], [1, 1]);
      // 06:10 — the API restarts; the fetch dies with it and its ledger row is closed.
      at("2026-12-02 06:10");
      await closeRun(run1, "failed", "The API restarted while this fetch was running — start it again");
      const run2 = await openRun(u, "fetch");
      const two = recorder(() => ({ runRef: run2 }));
      const api2 = clockFor(u, { ...who, pid: 2 }, { name: "fetch", runner: two.runner });
      at("2026-12-02 06:10:30");
      await api2.pollOnce();
      let lease = await must(u, "fetch");
      assert.deepEqual(ticksOf(lease), ["ran@05:00", "failed@05:00", "took_over@05:00"]);
      assert.equal(lease.ticks[1].reason, "The API restarted while this fetch was running — start it again");
      assert.equal(lease.lastTick?.reason, "start 2 of 2 — the first ended: The API restarted while this fetch was running — start it again; started 71 min late");
      assert.deepEqual([two.calls.length, two.calls[0].attempt, lease.attempt, lease.holder?.pid, clockTime(lease.slotAt), clockStamp(lease.nextAt), lease.runRef], [1, 2, 2, 2, "05:00", "3 Dec 05:00", run2]);
      // 07:00 — it dies again: there is no third start.
      at("2026-12-02 07:00");
      await closeRun(run2, "failed", "The API restarted while this fetch was running — start it again");
      const three = recorder(() => ({ runRef: run2 }));
      const api3 = clockFor(u, { ...who, pid: 3 }, { name: "fetch", runner: three.runner });
      await api3.pollOnce();
      at("2026-12-02 07:00:30");
      await api3.pollOnce();
      lease = await must(u, "fetch");
      assert.deepEqual([three.calls.length, lease.attempt, lease.holder, lease.endedAs, ticksOf(lease).slice(-1)], [0, 2, null, "failed", ["failed@05:00"]]);

      // Later than 3 h after 05:00: no second start either.
      at("2026-12-01 10:00");
      const who2 = machine("api2");
      const u2 = await workspace({ prefer: who2.systemId, on: { fetch: true } });
      const r1 = await openRun(u2, "fetch");
      const a = recorder(() => ({ runRef: r1 }));
      const b = recorder(() => ({ runRef: r1 }));
      const first = clockFor(u2, { ...who2, pid: 1 }, { name: "fetch", runner: a.runner });
      at("2026-12-02 04:59");
      await first.pollOnce();
      at("2026-12-02 05:00:10");
      await first.pollOnce();
      at("2026-12-02 08:05");
      await closeRun(r1, "failed", "The API restarted while this fetch was running — start it again");
      const later = clockFor(u2, { ...who2, pid: 2 }, { name: "fetch", runner: b.runner });
      await later.pollOnce();
      assert.deepEqual([b.calls.length, (await must(u2, "fetch")).attempt, (await must(u2, "fetch")).holder], [0, 1, null]);

      // Switched off after the failure: no second start.
      at("2026-12-01 10:00");
      const who3 = machine("api3");
      const u3 = await workspace({ prefer: who3.systemId, on: { fetch: true } });
      const r3 = await openRun(u3, "fetch");
      const c1 = recorder(() => ({ runRef: r3 }));
      const c2 = recorder(() => ({ runRef: r3 }));
      const p1 = clockFor(u3, { ...who3, pid: 1 }, { name: "fetch", runner: c1.runner });
      at("2026-12-02 04:59");
      await p1.pollOnce();
      at("2026-12-02 05:00:10");
      await p1.pollOnce();
      at("2026-12-02 05:30");
      await closeRun(r3, "failed", "The API restarted while this fetch was running — start it again");
      await setScheduleSwitch(u3, "fetch", false, "asha", deps);
      await clockFor(u3, { ...who3, pid: 2 }, { name: "fetch", runner: c2.runner }).pollOnce();
      assert.equal(c2.calls.length, 0);

      // An update sweep whose run failed is not started again: its next time comes soon enough.
      at("2026-12-01 10:00");
      const u4 = await workspace({ prefer: "" });
      let n = 0;
      const sweep = recorder(() => {
        n++;
        throw new Error("Atlas is unreachable\nstack…");
      });
      const w = clockFor(u4, machine("w"), { runner: sweep.runner });
      at("2026-12-01 10:44");
      await w.pollOnce();
      at("2026-12-01 10:45:05");
      await w.pollOnce();
      at("2026-12-01 10:45:40");
      await w.pollOnce();
      const failed = await must(u4, "update_ofd");
      assert.deepEqual([n, ticksOf(failed), failed.lastTick?.reason, failed.holder, failed.endedAs], [1, ["failed@10:45"], "could not start: Atlas is unreachable", null, "failed"]);
    });
    await check("the fetch's machine goes quiet → within 3 h another machine that may run it takes the same night over, and the first is told when it wakes", async () => {
      at("2026-12-01 10:00");
      const [f, g] = [machine("F"), machine("G")];
      const u = await workspace({ prefer: f.systemId, on: { fetch: true } });
      const runF = await openRun(u, "fetch");
      const runG = await openRun(u, "fetch");
      const fr = recorder(() => ({ runRef: runF }));
      const gr = recorder(() => ({ runRef: runG }));
      const cf = clockFor(u, f, { name: "fetch", runner: fr.runner });
      const cg = clockFor(u, g, { name: "fetch", runner: gr.runner });
      at("2026-12-02 04:59");
      await cf.pollOnce();
      await cg.pollOnce();
      at("2026-12-02 05:00:10");
      await cf.pollOnce();
      // G looks while F's lease is live: nothing.
      at("2026-12-02 05:03");
      await cg.pollOnce();
      assert.equal(gr.calls.length, 0);
      at("2026-12-02 05:30");
      await cg.pollOnce();
      const lease = await must(u, "fetch");
      assert.deepEqual([gr.calls.length, gr.calls[0].attempt, lease.holder?.systemId, lease.attempt, clockTime(lease.slotAt), clockStamp(lease.nextAt)], [1, 2, g.systemId, 2, "05:00", "3 Dec 05:00"]);
      assert.deepEqual(ticksOf(lease), ["ran@05:00", "took_over@05:00"]);
      assert.equal(lease.lastTick?.reason, `start 2 of 2 — the first stopped answering on ${f.host}; started 30 min late`);
      at("2026-12-02 05:40");
      await cf.pollOnce();
      assert.deepEqual([fr.superseded.length, fr.superseded[0]?.by.host, fr.superseded[0]?.runRef], [1, g.host, runF]);
    });

    await check("ten hours asleep: one time is dealt with, not five, and the next one is in the future", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const rec = recorder(() => ({ done: true, reason: "nothing out for delivery" }));
      const c = clockFor(u, machine("sleep"), { runner: rec.runner });
      at("2026-12-01 10:44");
      await c.pollOnce();
      at("2026-12-01 10:45:05");
      await c.pollOnce();
      // 12:45, 14:45, 16:45 and 18:45 pass; awake 5 minutes after 20:45.
      at("2026-12-01 20:50");
      await c.pollOnce();
      let lease = await must(u, "update_ofd");
      assert.deepEqual([ticksOf(lease), rec.calls.length, rec.calls[1].missedBefore, clockStamp(lease.nextAt)], [["ran@10:45", "ran@20:45"], 2, 4, "1 Dec 22:45"]);
      assert.equal(lease.lastTick?.reason, "nothing out for delivery · started 5 min late; 4 earlier time(s) passed with no machine awake");
      assert.ok(lease.nextAt.getTime() > clockMs, "no backlog");
      await c.pollOnce();
      assert.equal((await must(u, "update_ofd")).ticks.length, 2, "another look right after does nothing");
      // Asleep again, awake 75 minutes after the newest time: that one is missed — still one tick.
      at("2026-12-02 08:00");
      await c.pollOnce();
      lease = await must(u, "update_ofd");
      assert.deepEqual([ticksOf(lease), rec.calls.length, clockStamp(lease.nextAt)], [["ran@10:45", "ran@20:45", "missed@06:45"], 2, "2 Dec 08:45"]);
    });

    await check("first start: a time that was already due when the first machine that may run it started is not run late — its first run is the next fixed time", async () => {
      // The API seeded the clock at 10:00; the worker runs old code until it is restarted at 13:00 (12:35 has passed).
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const ref = await openRun(u, "invoices");
      const rec = recorder(() => ({ runRef: ref }));
      const who = machine("W");
      at("2026-12-01 13:00");
      const before = await must(u, "invoice_sweep");
      assert.deepEqual([clockStamp(before.nextAt), before.armedAt, before.ticks.length], ["1 Dec 12:35", null, 0], "nobody served it: the clock stands where the seed left it");
      const c = clockFor(u, who, { name: "invoice_sweep", runner: rec.runner });
      await c.pollOnce();
      let lease = await must(u, "invoice_sweep");
      assert.deepEqual([rec.calls.length, ticksOf(lease), clockStamp(lease.nextAt), lease.armedAt?.toISOString(), lease.holder], [0, ["skipped@12:35"], "1 Dec 15:35", ist("2026-12-01 13:00").toISOString(), null]);
      assert.equal(lease.lastTick?.reason, `the shared clock started on ${who.host} after this time — the first run is the next fixed time`);
      at("2026-12-01 15:35:05");
      await c.pollOnce();
      lease = await must(u, "invoice_sweep");
      assert.deepEqual([rec.calls.length, ticksOf(lease), lease.runRef], [1, ["skipped@12:35", "ran@15:35"], ref]);
      // Once armed, a time missed while every machine slept IS taken late (that is what the late limit is for).
      await releaseLease(rec.calls[0], { outcome: "finished", reason: "captured 3/3, re-captured 0", runRef: ref }, deps);
      at("2026-12-01 19:30");
      await clockFor(u, { ...who, pid: 77 }, { name: "invoice_sweep", runner: rec.runner }).pollOnce();
      assert.deepEqual([rec.calls.length, ticksOf(await must(u, "invoice_sweep")).slice(-1)], [2, ["ran@18:35"]]);
      // Work left from the 15:35 claim: this same machine ran again since — that supersedes nothing; seen from another machine it would.
      assert.notEqual(rec.calls[0].token, rec.calls[1].token);
      assert.equal(await supersededBy(u, { name: "invoice_sweep", token: rec.calls[0].token }, who.systemId, deps), null);
      assert.equal((await supersededBy(u, { name: "invoice_sweep", token: rec.calls[0].token }, "another-machine", deps))?.host, who.host);
    });

    await check("a process only serves its own kind's schedules: the API's clock leaves the sweeps alone, a worker's the fetch", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "", on: { fetch: true } });
      const rec = recorder(() => ({ done: true }));
      const all = { fetch: rec.runner, update_active: rec.runner, update_ofd: rec.runner, invoice_sweep: rec.runner };
      const who = machine("mix");
      const api = createScheduleClock({ role: "api", userId: u, names: [...SCHEDULE_NAMES], runners: all, me: { ...who, pid: 1, machine: `${who.host}.local` }, deps, log: (l) => logs.push(l) });
      const worker = createScheduleClock({ role: "worker", userId: u, names: [...SCHEDULE_NAMES], runners: all, me: { ...who, pid: 2, machine: `${who.host}.local` }, deps, log: (l) => logs.push(l) });
      // 13:00 — both update times and the invoice time are due; the API looks first.
      at("2026-12-01 13:00");
      await api.pollOnce();
      for (const n of ["update_active", "update_ofd", "invoice_sweep"] as const) {
        const l = await must(u, n);
        assert.deepEqual([l.ticks.length, l.armedAt, l.holder], [0, null, null], `${n} untouched by the API`);
      }
      assert.ok((await must(u, "fetch")).armedAt, "the API armed the fetch");
      await worker.pollOnce();
      assert.deepEqual([(await must(u, "update_ofd")).ticks.length, (await must(u, "fetch")).ticks.length], [1, 0]);
      const hosts = await deps.Hosts.find({ systemId: who.systemId }).sort({ _id: 1 }).lean();
      assert.deepEqual(hosts.map((h) => [h._id, h.role, h.pid, h.serves, h.eligible, h.host, h.machine]), [
        [`${who.systemId}:api`, "api", 1, ["fetch"], ["fetch"], who.host, `${who.host}.local`],
        [`${who.systemId}:worker`, "worker", 2, ["update_active", "update_ofd", "invoice_sweep"], ["update_active", "update_ofd", "invoice_sweep"], who.host, `${who.host}.local`],
      ]);
      // Presence once a minute.
      const seen = hosts[1].seenAt.getTime();
      at("2026-12-01 13:00:30");
      await worker.pollOnce();
      assert.equal((await deps.Hosts.findById(`${who.systemId}:worker`).lean())?.seenAt.getTime(), seen);
      at("2026-12-01 13:01:05");
      await worker.pollOnce();
      assert.equal((await deps.Hosts.findById(`${who.systemId}:worker`).lean())?.seenAt.getTime(), clockMs);
    });

    await check("Run now: the lease is taken by hand; another machine is refused with who holds it; a due time meanwhile is skipped; the release frees it", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const [x, y] = [machine("X"), machine("Y")];
      const first = await claimManual(u, "invoice_sweep", "asha", { me: { ...x, pid: 9 }, deps });
      assert.ok(first.ok);
      if (!first.ok) return;
      let lease = await must(u, "invoice_sweep");
      assert.deepEqual([lease.manual, lease.slotAt, lease.holder?.systemId, lease.holder?.role, lease.holder?.pid, lease.attempt, clockStamp(lease.nextAt), lease.ticks.length], [true, null, x.systemId, "worker", 9, 1, "1 Dec 12:35", 0], "it takes no fixed time");
      const refused = await claimManual(u, "invoice_sweep", "ravi", { me: y, deps });
      assert.deepEqual(refused, { ok: false, code: "held_elsewhere", status: 409, message: `The invoice sweep is running on ${x.host} since 10:00`, holder: { systemId: x.systemId, host: x.host, since: ist("2026-12-01 10:00"), slotAt: null, manual: true } });
      const again = await claimManual(u, "invoice_sweep", "asha", { me: x, deps });
      assert.deepEqual([again.ok, !again.ok && again.code, !again.ok && again.message], [false, "running_here", "The invoice sweep is running on this machine since 10:00"]);
      // The worker's clock on that machine keeps it renewed until the run is attached and closes.
      const ref = await openRun(u, "invoices");
      const rec = recorder(() => ({ done: true }));
      const c = clockFor(u, { ...x, pid: 10 }, { name: "invoice_sweep", runner: rec.runner });
      at("2026-12-01 10:01");
      await c.pollOnce();
      assert.equal(c.holds("invoice_sweep")?.token, first.claim.token, "adopted while no run is attached yet");
      assert.equal(await markStarted(first.claim, { runRef: ref, reason: "run now by asha" }, deps), true);
      lease = await must(u, "invoice_sweep");
      assert.deepEqual([ticksOf(lease), tickText(lease.lastTick), lease.runRef], [["ran@?"], `ran on ${x.host} — run now by asha`, ref]);
      at("2026-12-01 12:35:05");
      await touch(ref);
      await c.pollOnce();
      lease = await must(u, "invoice_sweep");
      assert.deepEqual([ticksOf(lease), lease.lastTick?.reason, rec.calls.length, clockStamp(lease.nextAt)], [["ran@?", "skipped@12:35"], `a run started by hand is still going on ${x.host}`, 0, "1 Dec 15:35"]);
      assert.equal(await releaseLease(first.claim, { outcome: "finished", reason: "captured 3/3, re-captured 0", runRef: ref }, deps), true);
      assert.equal(await releaseLease(first.claim, { outcome: "finished" }, deps), false, "a second release matches nothing");
      const next = await claimManual(u, "invoice_sweep", "ravi", { me: y, deps });
      assert.equal(next.ok, true);
      // A lease left to run out is no reason to refuse.
      at("2026-12-01 12:45");
      const third = await claimManual(u, "invoice_sweep", "asha", { me: x, deps });
      assert.deepEqual([third.ok, third.ok && third.claim.replaced?.host], [true, y.host]);
      // A workspace Trade Flow has not seeded has no lease to take.
      const none = await claimManual(String(oid()), "fetch", "asha", { me: x, deps });
      assert.deepEqual([none.ok, !none.ok && none.code, !none.ok && none.status], [false, "no_clock", 409]);
    });

    await check("what a runner answers decides the time's tick: nothing to do, skipped, could not start, queued and attached later, never started", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const ref = await openUpdate(u);
      const answers: Record<string, () => ScheduleRunResult> = {
        "10:45": () => ({ done: true, reason: "nothing out for delivery" }),
        "12:45": () => ({ skipped: true, reason: "a fetch started by asha is running on Mac-2" }),
        "14:45": () => ({}),
        "16:45": () => ({}),
      };
      const rec = recorder((claim) => answers[clockTime(claim.slotAt)]());
      const who = machine("r");
      const c = clockFor(u, who, { runner: rec.runner });
      at("2026-12-01 10:44");
      await c.pollOnce();
      at("2026-12-01 10:45:05");
      await c.pollOnce();
      let lease = await must(u, "update_ofd");
      assert.deepEqual([tickText(lease.lastTick), lease.holder, lease.endedAs, c.holds("update_ofd")], [`ran on ${who.host} — nothing out for delivery`, null, "ran", null]);
      at("2026-12-01 12:45:05");
      await c.pollOnce();
      lease = await must(u, "update_ofd");
      assert.deepEqual([tickText(lease.lastTick), lease.holder, lease.endedAs], ["skipped — a fetch started by asha is running on Mac-2", null, "skipped"]);
      // Queued: the lease is kept without a run; the job says which document it lives in when it starts.
      at("2026-12-01 14:45:05");
      await c.pollOnce();
      lease = await must(u, "update_ofd");
      assert.deepEqual([tickText(lease.lastTick), lease.runRef, lease.holder?.token], [`ran on ${who.host}`, null, rec.calls[2].token]);
      at("2026-12-01 14:50");
      await c.pollOnce();
      assert.equal((await must(u, "update_ofd")).expiresAt?.getTime(), clockMs + LEASE_MS, "renewed while it waits for its turn");
      await touch(ref);
      assert.equal(await attachRun({ userId: u, name: "update_ofd", token: rec.calls[2].token }, ref, deps), true);
      assert.equal(await attachRun({ userId: u, name: "update_ofd", token: "stale" }, ref, deps), false);
      at("2026-12-01 14:52");
      await closeRun(ref, "done");
      await c.pollOnce();
      lease = await must(u, "update_ofd");
      assert.deepEqual([ticksOf(lease).slice(-2), lease.holder, lease.lastTick?.runRef], [["ran@14:45", "finished@14:45"], null, ref]);
      // Queued and never started: given up after 30 minutes.
      at("2026-12-01 16:45:05");
      await c.pollOnce();
      at("2026-12-01 17:10");
      await c.pollOnce();
      assert.equal((await must(u, "update_ofd")).holder?.token, rec.calls[3].token);
      at("2026-12-01 17:16");
      await c.pollOnce();
      lease = await must(u, "update_ofd");
      assert.deepEqual([tickText(lease.lastTick), lease.holder, lease.endedAs], ["failed — no run started within 30 min of taking it", null, "failed"]);
    });

    await check("a runner that is slow to answer does not hold up the poll: its lease is renewed meanwhile, and the tick is written once it has answered", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const ref = await openUpdate(u);
      let answer: (r: ScheduleRunResult) => void = () => undefined;
      const rec = recorder(() => new Promise<ScheduleRunResult>((resolve) => (answer = resolve)));
      const who = machine("slow");
      const c = createScheduleClock({ role: "worker", userId: u, names: ["update_ofd"], serves: ["update_ofd"], runners: { update_ofd: rec.runner }, me: { ...who, pid: 1 }, deps, log: (l) => logs.push(l), startWaitMs: 30 });
      at("2026-12-01 10:44");
      await c.pollOnce();
      at("2026-12-01 10:45:05");
      await c.pollOnce(); // comes back after 30 ms although the runner has not answered
      let lease = await must(u, "update_ofd");
      assert.deepEqual([rec.calls.length, lease.holder?.token, lease.ticks.length, clockStamp(lease.nextAt)], [1, rec.calls[0].token, 0, "1 Dec 12:45"]);
      at("2026-12-01 10:49:30");
      await c.pollOnce();
      assert.equal((await must(u, "update_ofd")).expiresAt?.getTime(), clockMs + LEASE_MS, "renewed while the runner starts");
      await touch(ref);
      answer({ runRef: ref });
      await c.idle();
      lease = await must(u, "update_ofd");
      assert.deepEqual([ticksOf(lease), lease.runRef, c.holds("update_ofd")?.token, lease.lastTick?.at.toISOString()], [["ran@10:45"], ref, rec.calls[0].token, ist("2026-12-01 10:49:30").toISOString()]);
      at("2026-12-01 10:50");
      await closeRun(ref, "done");
      await c.pollOnce();
      assert.equal((await must(u, "update_ofd")).holder, null);
      // A runner that never answers is given up after 30 minutes, like any claim without a run.
      const never = recorder(() => new Promise<ScheduleRunResult>(() => undefined));
      const c2 = createScheduleClock({ role: "worker", userId: u, names: ["update_ofd"], serves: ["update_ofd"], runners: { update_ofd: never.runner }, me: { ...who, pid: 2 }, deps, log: (l) => logs.push(l), startWaitMs: 30 });
      at("2026-12-01 12:45:05");
      await c2.pollOnce();
      at("2026-12-01 13:10");
      await c2.pollOnce();
      assert.equal((await must(u, "update_ofd")).holder?.token, never.calls[0].token);
      at("2026-12-01 13:16");
      await c2.pollOnce();
      lease = await must(u, "update_ofd");
      assert.deepEqual([lease.holder, tickText(lease.lastTick), c2.holds("update_ofd")], [null, "failed — no run started within 30 min of taking it", null]);
    });

    await check("a runner's answer the database would not take is written on the next poll — for a time with nothing to do, and for a run that started", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const ref = await openUpdate(u);
      const rec = recorder((claim) => (clockTime(claim.slotAt) === "10:45" ? { done: true, reason: "nothing out for delivery" } : { runRef: ref }));
      const who = machine("flaky");
      const c = clockFor(u, who, { runner: rec.runner });
      // The next write of a tick fails as a dropped connection does; the claim itself (no tick) goes through.
      let failTicks = 0;
      const model = deps.Leases as unknown as { updateOne: (...args: unknown[]) => unknown };
      const inherited = model.updateOne;
      model.updateOne = function (this: unknown, ...args: unknown[]) {
        if (failTicks > 0 && args[1] && typeof args[1] === "object" && "$push" in (args[1] as object)) {
          failTicks--;
          return Promise.reject(new Error("MongoNetworkError: connection 4 to the cluster closed"));
        }
        return inherited.apply(this, args);
      };
      try {
        at("2026-12-01 10:44");
        await c.pollOnce();
        failTicks = 1;
        at("2026-12-01 10:45:05");
        await c.pollOnce();
        let lease = await must(u, "update_ofd");
        assert.deepEqual([rec.calls.length, lease.ticks.length, lease.holder?.token, clockStamp(lease.nextAt), failTicks], [1, 0, rec.calls[0].token, "1 Dec 12:45", 0], "taken, the runner answered, the release did not get through");
        at("2026-12-01 10:45:35");
        await c.pollOnce();
        lease = await must(u, "update_ofd");
        assert.deepEqual([rec.calls.length, ticksOf(lease), tickText(lease.lastTick), lease.holder, c.holds("update_ofd")], [1, ["ran@10:45"], `ran on ${who.host} — nothing out for delivery`, null, null], "written on the next poll; the runner is not asked twice");
        failTicks = 1;
        at("2026-12-01 12:45:05");
        await c.pollOnce();
        lease = await must(u, "update_ofd");
        assert.deepEqual([rec.calls.length, lease.ticks.length, lease.runRef, lease.holder?.token], [2, 1, null, rec.calls[1].token]);
        at("2026-12-01 12:45:35");
        await touch(ref);
        await c.pollOnce();
        lease = await must(u, "update_ofd");
        assert.deepEqual([rec.calls.length, ticksOf(lease), lease.runRef, lease.lastTick?.runRef, lease.holder?.token], [2, ["ran@10:45", "ran@12:45"], ref, ref, rec.calls[1].token]);
        assert.ok(serviceLines.some((l) => /MongoNetworkError: connection 4 to the cluster closed — the next poll tries again/.test(l)), "the outage is said");
      } finally {
        delete (model as { updateOne?: unknown }).updateOne;
      }
      assert.equal(deps.Leases.updateOne, inherited, "the model is as it was");
    });

    await check("a supersede that fails is asked for again on the next poll; one schedule's error does not stop the others of the same poll", async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const [ma, mb] = [machine("A2"), machine("B2")];
      const refA = await openUpdate(u);
      let tries = 0;
      const sweep: ScheduleRunner = {
        run: async () => ({ runRef: refA }),
        supersede: () => {
          if (++tries === 1) throw new Error("the sweep's stop was refused");
        },
      };
      const ofd = recorder(() => ({ done: true, reason: "nothing out for delivery" }));
      const ca = createScheduleClock({ role: "worker", userId: u, names: ["update_active", "update_ofd"], serves: ["update_active", "update_ofd"], runners: { update_active: sweep, update_ofd: ofd.runner }, me: { ...ma, pid: 1 }, deps, log: (l) => logs.push(l) });
      const b = recorder(() => ({ done: true, reason: "nothing to refresh" }));
      const cb = clockFor(u, mb, { name: "update_active", runner: b.runner });
      at("2026-12-01 12:14");
      await ca.pollOnce();
      at("2026-12-01 12:15:05");
      await ca.pollOnce();
      assert.equal(ca.holds("update_active")?.name, "update_active");
      // A sleeps; B takes 18:15 over, finds nothing to do and releases — the lease is free again when A wakes.
      at("2026-12-01 18:15:05");
      await cb.pollOnce();
      assert.deepEqual([b.calls.length, (await must(u, "update_active")).holder], [1, null]);
      at("2026-12-01 18:46");
      const before = ofd.calls.length;
      await ca.pollOnce();
      assert.equal(tries, 1, "told that B ran since, although B no longer holds the lease");
      assert.equal(ofd.calls.length, before + 1, "the out-for-delivery time of the same poll was still dealt with");
      assert.equal(serviceLines.filter((l) => /update_active: .*the sweep's stop was refused — the next poll tries again/.test(l)).length, 1, "the failure is said, once");
      at("2026-12-01 18:46:30");
      await ca.pollOnce();
      assert.equal(tries, 2, "asked again");
      at("2026-12-01 18:47");
      await ca.pollOnce();
      assert.equal(tries, 2, "and not a third time");
      assert.deepEqual(ticksOf(await must(u, "update_active")), ["ran@12:15", "ran@18:15", "superseded@12:15"], "one superseded note");
    });

    await check("the poll: every 30 s ± 5 s; start() looks at once and only once, stop() ends it", async () => {
      assert.deepEqual([pollDelayMs(0), pollDelayMs(0.5), pollDelayMs(1)], [25_000, 30_000, 35_000]);
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "" });
      const who = machine("timer");
      const c = createScheduleClock({ role: "worker", userId: u, names: [], runners: {}, me: { ...who, pid: 4242, machine: `${who.host}.local` }, deps, log: (l) => logs.push(l) });
      c.start();
      c.start();
      let row: { pid: number; serves: string[]; eligible: string[] } | null = null;
      for (let i = 0; i < 100 && !row; i++) {
        await sleep(50);
        row = await deps.Hosts.findById(`${who.systemId}:worker`).lean();
      }
      c.stop();
      assert.deepEqual([row?.pid, row?.serves, row?.eligible], [4242, ["update_active", "update_ofd", "invoice_sweep"], []]);
      assert.equal(logs.filter((l) => l.startsWith(`clock on ${who.host} (worker)`)).length, 1);
      // A restart of the same machine (another pid, the same name) is nothing to remark on.
      const before = serviceLines.length;
      await createScheduleClock({ role: "worker", userId: u, names: [], runners: {}, me: { ...who, pid: 5151, machine: `${who.host}.local` }, deps, log: (l) => logs.push(l) }).pollOnce();
      assert.equal(serviceLines.length, before);
      // Two machines under one TRADEFLOW_SYSTEM_ID would count as one machine: the second to write its presence says so.
      await createScheduleClock({ role: "worker", userId: u, names: [], runners: {}, me: { ...who, pid: 6161, machine: "another-mac.local" }, deps, log: (l) => logs.push(l) }).pollOnce();
      assert.deepEqual(serviceLines.slice(before).map((l) => l.replace(/"[^"]*"/g, "…")), ["[schedules] another machine (…) runs Trade Flow under this machine's TRADEFLOW_SYSTEM_ID … — give each machine its own in its .env: under one id two machines count as one, and neither takes over from the other"]);
    });

    await check(`the newest ${TICKS_KEPT} ticks are kept; a switch nothing has set decides nothing and asks for the seed`, async () => {
      at("2026-12-01 10:00");
      const u = await workspace({ prefer: "", on: { update_ofd: false } });
      const rec = recorder(() => ({ done: true }));
      const c = clockFor(u, machine("many"), { runner: rec.runner });
      let t = ist("2026-12-01 10:45:05").getTime();
      for (let i = 0; i < TICKS_KEPT + 4; i++, t += 2 * 60 * 60_000) {
        clockMs = t;
        await c.pollOnce();
      }
      const lease = await must(u, "update_ofd");
      assert.equal(lease.ticks.length, TICKS_KEPT);
      assert.deepEqual([lease.ticks[TICKS_KEPT - 1].slotAt?.getTime(), lease.lastTick?.slotAt?.getTime()], [t - 2 * 60 * 60_000 - 5000, t - 2 * 60 * 60_000 - 5000], "the newest is last");
      assert.equal(lease.ticks[0].slotAt?.getTime(), ist("2026-12-01 10:45").getTime() + 4 * 2 * 60 * 60_000, "the oldest four are gone");

      at("2026-12-01 10:00");
      const u2 = await workspace({ prefer: "" });
      await deps.Settings.updateOne({ userId: u2 }, { $unset: { "schedules.update_ofd": 1 } });
      let asked = 0;
      const c2 = clockFor(u2, machine("unset"), { runner: rec.runner, seed: async () => void asked++ });
      at("2026-12-01 10:44");
      await c2.pollOnce();
      at("2026-12-01 10:45:05");
      await c2.pollOnce();
      const l2 = await must(u2, "update_ofd");
      assert.deepEqual([l2.ticks.length, clockStamp(l2.nextAt), l2.holder, rec.calls.length, asked], [0, "1 Dec 10:45", null, 0, 1]);
      // A clock document that is not there at all: the seed is asked for, too.
      await deps.Leases.deleteOne({ userId: u2, name: "update_ofd" });
      let askedAgain = 0;
      await clockFor(u2, machine("gone"), { runner: rec.runner, seed: async () => void askedAgain++ }).pollOnce();
      assert.equal(askedAgain, 1);
    });

    // ── the routes ───────────────────────────────────────────────────────────
    console.log("\nTHE ROUTES — the real router, on a listener of this process");
    // From here the route handlers' own defaults are the throwaway collections and the hand-moved clock, and the
    // question "does this machine's Redis still hold an old scheduler?" is answered here: no Redis is opened.
    Object.assign(scheduleDefaults, deps);
    let legacy: Awaited<ReturnType<typeof scheduleProbes.legacyClocks>> = {};
    let probed = 0;
    scheduleProbes.legacyClocks = async () => {
      probed++;
      return legacy;
    };
    await deps.Hosts.deleteMany({});
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
    const call = async (method: "GET" | "PUT", path: string, body?: unknown): Promise<{ status: number; body: Json }> => {
      const res = await fetch(`${base}${path}`, { method, headers: { "content-type": "application/json" }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(30_000) });
      return { status: res.status, body: (await res.json().catch(() => ({}))) as Json };
    };

    at("2026-12-01 10:00");
    const mac = { systemId: `mac-${run}`, host: "Mac" };
    const U = String(oid());
    // This Mac's .env as it is when the API first runs this code: update sweeps not allowed here, the invoice sweep allowed.
    await seedSchedules(U, { env: { UPDATE_SCHEDULE: "0", FETCH_SCHEDULE: "0" }, me: mac, deps });
    const scheduleOf = (b: Json, name: ScheduleName): Json => (b.schedules as Json[]).find((s) => s.name === name) as Json;

    await check("GET /schedules: the four schedules in order, each with switch, clock, next time and state; here.eligible is this machine's .env; no token anywhere", async () => {
      const { status, body } = await call("GET", `/schedules?userId=${U}`);
      assert.equal(status, 200);
      assert.deepEqual(Object.keys(body).sort(), ["here", "machines", "now", "schedules", "timezone"]);
      assert.deepEqual([body.timezone, body.here.systemId, body.here.host, body.machines], ["Asia/Kolkata", config.systemId, config.hostId, []]);
      assert.deepEqual(body.here.eligible, SCHEDULE_NAMES.filter((n) => eligibleHere(n)));
      assert.deepEqual((body.schedules as Json[]).map((s) => [s.name, s.label, s.role, s.on, s.source, s.changedBy]), [
        ["fetch", "Nightly fetch", "api", false, "seed", "seed:Mac"],
        ["update_active", "Update all active units", "worker", false, "seed", "seed:Mac"],
        ["update_ofd", "Out-for-delivery recheck", "worker", false, "seed", "seed:Mac"],
        ["invoice_sweep", "Invoice sweep", "worker", true, "seed", "seed:Mac"],
      ]);
      const keys = ["armedAt", "changedAt", "changedBy", "clock", "eligibleHere", "label", "lastRun", "lastTick", "lease", "legacy", "name", "nextAt", "note", "on", "overdue", "role", "runnableOn", "servedOn", "source", "state", "ticks"];
      for (const s of body.schedules as Json[]) assert.deepEqual(Object.keys(s).sort(), keys, s.name);
      const f = scheduleOf(body, "fetch");
      assert.deepEqual([f.clock.text, f.clock.preferSystemId, f.nextAt, f.lease, f.lastTick, f.ticks, f.state], ["daily at 05:00", mac.systemId, ist("2026-12-02 05:00").toISOString(), null, null, [], "off"]);
      assert.equal(scheduleOf(body, "update_active").clock.text, "every 6 h at 00:15 / 06:15 / 12:15 / 18:15");
    });

    await check("PUT /schedules/:name then GET round-trips: on, who, when; the machine preference too", async () => {
      at("2026-12-01 10:05");
      const put = await call("PUT", "/schedules/update_ofd", { on: true, by: "asha", userId: U });
      assert.equal(put.status, 200);
      assert.deepEqual([scheduleOf(put.body, "update_ofd").on, scheduleOf(put.body, "update_ofd").source], [true, "ui"], "the answer is the overview");
      const got = scheduleOf((await call("GET", `/schedules?userId=${U}`)).body, "update_ofd");
      assert.deepEqual([got.on, got.source, got.changedBy, got.changedAt], [true, "ui", "asha", ist("2026-12-01 10:05").toISOString()]);
      assert.equal((await resolveScheduleSwitches(U, deps)).update_ofd.on, true);
      assert.equal(scheduleOf((await call("PUT", "/schedules/update_ofd", { on: false, by: "ravi", userId: U })).body, "update_ofd").changedBy, "ravi");
      const pref = await call("PUT", "/schedules/fetch", { preferSystemId: "Mac-2", by: "asha", userId: U });
      assert.deepEqual([pref.status, scheduleOf(pref.body, "fetch").clock.preferSystemId, scheduleOf(pref.body, "fetch").clock.preferChangedBy, scheduleOf(pref.body, "fetch").on], [200, "Mac-2", "asha", false]);
      const both = await call("PUT", "/schedules/invoice_sweep", { on: false, preferSystemId: "", by: "asha", userId: U });
      assert.deepEqual([both.status, scheduleOf(both.body, "invoice_sweep").on, scheduleOf(both.body, "invoice_sweep").clock.preferSystemId], [200, false, ""]);
      await call("PUT", "/schedules/invoice_sweep", { on: true, preferSystemId: mac.systemId, by: "asha", userId: U });
    });

    await check("PUT refuses before it writes: an unknown name, on:'yes', nothing to save, a preference that is not a system id → 400", async () => {
      const before = JSON.stringify([await deps.Settings.find({ userId: U }).lean(), await deps.Leases.find({ userId: U }).sort({ name: 1 }).lean()]);
      assert.deepEqual(await call("PUT", "/schedules/nightly", { on: true, userId: U }), { status: 400, body: { error: 'Unknown schedule "nightly" — it is one of fetch, update_active, update_ofd, invoice_sweep', code: "unknown_schedule" } });
      assert.deepEqual(await call("PUT", "/schedules/fetch", { on: "yes", userId: U }), { status: 400, body: { error: "on must be true or false", code: "invalid_request" } });
      assert.deepEqual(await call("PUT", "/schedules/fetch", { on: 1, userId: U }), { status: 400, body: { error: "on must be true or false", code: "invalid_request" } });
      assert.deepEqual(await call("PUT", "/schedules/fetch", { by: "asha", userId: U }), { status: 400, body: { error: "Nothing to save: send on and/or preferSystemId", code: "invalid_request" } });
      assert.equal((await call("PUT", "/schedules/fetch", { preferSystemId: 5, userId: U })).status, 400);
      assert.equal((await call("PUT", "/schedules/fetch", { on: true, preferSystemId: "two words", userId: U })).status, 400, "a bad preference refuses the whole save, the switch included");
      assert.equal((await call("PUT", "/schedules/fetch", { on: true, userId: "not-an-id" })).status, 400);
      assert.equal(JSON.stringify([await deps.Settings.find({ userId: U }).lean(), await deps.Leases.find({ userId: U }).sort({ name: 1 }).lean()]), before, "nothing was written");
      // A workspace without clocks: the preference has nothing to be set on, and the switch sent with it is not written.
      const fresh = String(oid());
      const none = await call("PUT", "/schedules/fetch", { on: true, preferSystemId: "Mac-2", userId: fresh });
      assert.deepEqual([none.status, none.body.code], [409, "no_clock"]);
      assert.equal(await deps.Settings.countDocuments({ userId: fresh }), 0);
      assert.equal(await setSchedulePreference(U, "fetch", mac.systemId, "check", deps), mac.systemId);
      await assert.rejects(() => changeSchedule(U, "fetch", null, deps), /Nothing to save/);
    });

    await check("in between — the API on this code, the worker not restarted yet: the sweep its old BullMQ clock still runs reads own_clock, and says the switch does not reach it", async () => {
      // 13:00: the API has been up since 10:00 (its presence row), no worker has started the shared clock, 12:35 has
      // passed, and this machine's Redis still holds the invoice sweep's scheduler (as measured on 2026-11-25).
      at("2026-12-01 13:00");
      const apiClock = createScheduleClock({ role: "api", userId: U, names: [], runners: {}, me: { ...mac, pid: 1 }, deps, log: (l) => logs.push(l) });
      await apiClock.pollOnce();
      legacy = { invoice_sweep: { scheduler: "order-invoice-sweep", everyMs: 10_800_000, nextAt: ist("2026-12-01 15:18:53") } };
      const { body } = await call("GET", `/schedules?userId=${U}`);
      assert.deepEqual((body.machines as Json[]).map((m) => [m.id, m.role, m.host, m.serves, m.eligible, m.up]), [[`${mac.systemId}:api`, "api", "Mac", ["fetch"], [], true]]);
      const inv = scheduleOf(body, "invoice_sweep");
      assert.deepEqual([inv.state, inv.on, inv.armedAt, inv.overdue, inv.nextAt, inv.servedOn, inv.lease, inv.ticks.length], ["own_clock", true, null, true, ist("2026-12-01 12:35").toISOString(), [], null, 0]);
      assert.deepEqual(inv.legacy, { scheduler: "order-invoice-sweep", everyMs: 10_800_000, nextAt: ist("2026-12-01 15:18:53").toISOString() });
      assert.equal(inv.note, `The worker on ${config.hostId} still runs this on its own clock (next 1 Dec 15:18) — the switch and the times here apply once that worker has been restarted`);
      // The update sweeps: switched off by the seed (UPDATE_SCHEDULE=0 here) and no old scheduler in Redis.
      const act = scheduleOf(body, "update_active");
      assert.deepEqual([act.state, act.on, act.legacy, act.note], ["off", false, null, "Switched off"]);
      // The fetch: the API serves it; switched off → the 05:00 time will be skipped.
      const f = scheduleOf(body, "fetch");
      assert.deepEqual([f.state, f.servedOn, f.runnableOn, f.note], ["off", ["Mac"], [], "Switched off — the 2 Dec 05:00 time will be skipped"]);
      // Switched on with no machine that may run it: said as it is.
      await setScheduleSwitch(U, "fetch", true, "asha", deps);
      const on = scheduleOf((await call("GET", `/schedules?userId=${U}`)).body, "fetch");
      assert.deepEqual([on.state, on.note], ["no_machine", "No machine that may run it is up — Mac has FETCH_SCHEDULE off in the .env"]);
      await setScheduleSwitch(U, "fetch", false, "asha", deps);
      // Nothing of the worker's was touched by the API's clock.
      const lease = await must(U, "invoice_sweep");
      assert.deepEqual([lease.ticks.length, lease.armedAt, lease.holder, clockStamp(lease.nextAt)], [0, null, null, "1 Dec 12:35"]);
      // Redis not answering is said, not guessed.
      legacy = null;
      await setScheduleSwitch(U, "update_active", true, "asha", deps);
      assert.match(scheduleOf((await call("GET", `/schedules?userId=${U}`)).body, "update_active").note, /^No worker on the shared clock is up yet — nothing runs until one starts \(this machine's Redis did not answer/);
      await setScheduleSwitch(U, "update_active", false, "asha", deps);
    });

    await check("after the worker restart: its clock arms the sweeps for the next fixed time, the old scheduler is gone → scheduled; then running, then the last tick", async () => {
      at("2026-12-01 13:30");
      legacy = {};
      const ref = await openRun(U, "invoices");
      const rec = recorder(() => ({ runRef: ref }));
      // The worker on this Mac: serves the three sweeps, may run only the invoice sweep (UPDATE_SCHEDULE=0).
      const worker = createScheduleClock({ role: "worker", userId: U, names: ["invoice_sweep"], runners: { invoice_sweep: rec.runner, update_active: rec.runner, update_ofd: rec.runner }, me: { ...mac, pid: 2 }, deps, log: (l) => logs.push(l) });
      await worker.pollOnce();
      let body = (await call("GET", `/schedules?userId=${U}`)).body;
      let inv = scheduleOf(body, "invoice_sweep");
      assert.deepEqual([inv.state, inv.note, inv.overdue, inv.nextAt, inv.servedOn, inv.runnableOn, inv.legacy], ["scheduled", "Next 1 Dec 15:35 on Mac", false, ist("2026-12-01 15:35").toISOString(), ["Mac"], ["Mac"], null]);
      assert.deepEqual([inv.lastTick.outcome, inv.lastTick.text, rec.calls.length], ["skipped", "skipped — the shared clock started on Mac after this time — the first run is the next fixed time", 0], "no sweep at the restart");
      // The update sweeps are off; this worker also records that, on time, for the times that were due.
      const ofd = scheduleOf(body, "update_ofd");
      assert.deepEqual([ofd.state, ofd.lastTick.text, ofd.servedOn, ofd.runnableOn, ofd.armedAt], ["off", "skipped — switched off", ["Mac"], [], null]);
      for (const s of body.schedules as Json[]) assert.ok(new Date(s.nextAt).getTime() > clockMs, `${s.name}: next time in the future`);
      // 15:35 — it runs.
      at("2026-12-01 15:35:05");
      await worker.pollOnce();
      body = (await call("GET", `/schedules?userId=${U}`)).body;
      inv = scheduleOf(body, "invoice_sweep");
      assert.deepEqual([inv.state, inv.note, inv.lease.host, inv.lease.live, inv.lease.runRef, inv.lease.slotAt, inv.lastTick.text], ["running", "Running on Mac since 15:35", "Mac", true, ref, ist("2026-12-01 15:35").toISOString(), "ran on Mac"]);
      const text = JSON.stringify(body);
      assert.ok(!text.includes(rec.calls[0].token) && !/"token"/.test(text), "the claim's token never leaves Trade Flow");
      // The worker goes quiet (the Mac sleeps): six minutes on, its lease has run out and it has not been seen for
      // over 3 minutes — that is not "running" any more, and no machine is up to run the next time.
      at("2026-12-01 15:41:30");
      const quiet = (await call("GET", `/schedules?userId=${U}`)).body;
      inv = scheduleOf(quiet, "invoice_sweep");
      assert.deepEqual([inv.state, inv.lease.live, inv.lease.host, inv.servedOn, (quiet.machines as Json[]).map((m) => [m.role, m.up])], ["no_machine", false, "Mac", [], [["api", false], ["worker", false]]]);
      assert.equal(inv.note, "No worker on the shared clock is up — nothing runs until one starts · Mac has not renewed its lease since 15:35 (it ran out at 15:40) — it carries on if it wakes first, else the next time is taken over");
      // It wakes: its next poll renews the lease (nobody took over) and it is running again.
      await touch(ref);
      await worker.pollOnce();
      inv = scheduleOf((await call("GET", `/schedules?userId=${U}`)).body, "invoice_sweep");
      assert.deepEqual([inv.state, inv.lease.live, inv.note], ["running", true, "Running on Mac since 15:35"]);
      await releaseLease(rec.calls[0], { outcome: "finished", reason: "captured 3/3, re-captured 0", runRef: ref }, deps);
      inv = scheduleOf((await call("GET", `/schedules?userId=${U}`)).body, "invoice_sweep");
      assert.deepEqual([inv.state, inv.lease, inv.lastTick.text, inv.lastTick.runRef, inv.lastRun.endedAs, inv.lastRun.host], ["scheduled", null, "finished — captured 3/3, re-captured 0", ref, "finished", "Mac"]);
      assert.deepEqual((inv.ticks as Json[]).map((t) => t.outcome), ["finished", "ran", "skipped"], "newest first");
      // Both clocks at once is never right: an old scheduler that is still there is said.
      legacy = { invoice_sweep: { scheduler: "order-invoice-sweep", everyMs: 10_800_000, nextAt: null } };
      assert.match(scheduleOf((await call("GET", `/schedules?userId=${U}`)).body, "invoice_sweep").note, /still holds the old order-invoice-sweep scheduler — restart the worker$/);
      legacy = {};
      assert.ok(probed > 0);
    });

    await check("GET /fetch/schedule keeps its keys: enabled is now the switch, lastTick the clock's; eligibleHere, nextAt and lease are new", async () => {
      at("2026-12-01 16:00");
      const { status, body } = await call("GET", `/fetch/schedule?userId=${U}`);
      assert.equal(status, 200);
      const old = ["enabled", "cron", "timezone", "sinceDays", "sinceSource", "invoices", "invoiceMaxPerRun", "loggedInIds", "windows", "lastTick", "lastScheduledRun", "fetchRunning"];
      for (const k of old) assert.ok(k in body, `key ${k}`);
      assert.deepEqual([body.enabled, body.enabledSource, body.cron, body.timezone, body.eligibleHere, body.nextAt, body.lease, body.lastTick, body.fetchRunning, body.loggedInIds], [false, "ui", "0 5 * * *", "Asia/Kolkata", eligibleHere("fetch"), ist("2026-12-02 05:00").toISOString(), null, null, false, 0]);
      assert.deepEqual([typeof body.sinceDays, typeof body.sinceSource, typeof body.invoices.enabled, typeof body.invoiceMaxPerRun], ["number", "string", "boolean", "number"]);
      // 05:00 with the switch off: the API's clock (this machine may not run the fetch) says so on time, and the route shows it.
      const apiClock = createScheduleClock({ role: "api", userId: U, names: [], runners: {}, me: { ...mac, pid: 1 }, deps, log: (l) => logs.push(l) });
      at("2026-12-02 05:00:15");
      await apiClock.pollOnce();
      const after = (await call("GET", `/fetch/schedule?userId=${U}`)).body;
      assert.deepEqual([after.lastTick.outcome, after.lastTick.result, after.lastTick.at, after.lastTick.slotAt, after.nextAt], ["skipped — switched off", "skipped", ist("2026-12-02 05:00:15").toISOString(), ist("2026-12-02 05:00").toISOString(), ist("2026-12-03 05:00").toISOString()]);
      // The switch, flipped through the new route, is what `enabled` reads.
      await call("PUT", "/schedules/fetch", { on: true, by: "asha", userId: U });
      assert.equal((await call("GET", `/fetch/schedule?userId=${U}`)).body.enabled, true);
      // A clock at another time reads as that cron.
      await deps.Leases.updateOne({ userId: U, name: "fetch" }, { $set: { "clock.at": "04:30" } });
      assert.equal((await call("GET", `/fetch/schedule?userId=${U}`)).body.cron, "30 4 * * *");
    });

    await check("GET /update/settings keeps its keys: schedule.enabled is the active-units switch; ofdEnabled and the next times are new", async () => {
      const { status, body } = await call("GET", `/update/settings?userId=${U}`);
      assert.equal(status, 200);
      for (const k of ["windows", "source", "changedBy", "changedAt", "limits", "schedule"]) assert.ok(k in body, `key ${k}`);
      assert.deepEqual(body.limits, { min: 1, max: 20 });
      // The next times are the clocks' as the worker last left them (its last look was 15:35 on 1 Dec).
      assert.deepEqual(body.schedule, { enabled: false, ofdEnabled: false, activeEveryMs: 21_600_000, ofdEveryMs: 7_200_000, activeNextAt: ist("2026-12-01 18:15").toISOString(), ofdNextAt: ist("2026-12-01 16:45").toISOString(), eligibleHere: eligibleHere("update_active") });
      await call("PUT", "/schedules/update_active", { on: true, by: "asha", userId: U });
      const on = (await call("GET", `/update/settings?userId=${U}`)).body.schedule;
      assert.deepEqual([on.enabled, on.ofdEnabled], [true, false]);
    });

    await check("a workspace Trade Flow has not seeded reads not_seeded on every schedule, and the older routes still answer", async () => {
      const fresh = String(oid());
      const { status, body } = await call("GET", `/schedules?userId=${fresh}`);
      assert.equal(status, 200);
      assert.deepEqual((body.schedules as Json[]).map((s) => [s.state, s.on, s.source, s.clock, s.nextAt]), SCHEDULE_NAMES.map(() => ["not_seeded", false, "unset", null, null]));
      const fs = await call("GET", `/fetch/schedule?userId=${fresh}`);
      assert.deepEqual([fs.status, fs.body.enabled, fs.body.nextAt, fs.body.lease, typeof fs.body.cron], [200, false, null, null, "string"]);
      const us = await call("GET", `/update/settings?userId=${fresh}`);
      assert.deepEqual([us.status, us.body.schedule.enabled, us.body.schedule.activeNextAt, typeof us.body.schedule.activeEveryMs], [200, false, null, "number"]);
      assert.equal((await call("GET", "/schedules?userId=not-an-id")).status, 400);
    });

    await check("GET /update/settings still answers 200 when the schedule part cannot be read — a userId that is no workspace id, or a database error — with the schedule off and the .env periods, as before the clock", async () => {
      const fallback = { enabled: false, ofdEnabled: false, activeEveryMs: UPDATE_SWEEP_MS, ofdEveryMs: UPDATE_OFD_SWEEP_MS, activeNextAt: null, ofdNextAt: null, eligibleHere: eligibleHere("update_active") };
      const bad = await call("GET", "/update/settings?userId=abc");
      assert.equal(bad.status, 200);
      for (const k of ["windows", "source", "changedBy", "changedAt", "limits", "schedule"]) assert.ok(k in bad.body, `key ${k}`);
      assert.deepEqual(bad.body.schedule, fallback);
      // The lease read breaks (an Atlas outage): the speed card is still drawn.
      const leases = scheduleDefaults.Leases;
      scheduleDefaults.Leases = { find: () => { throw new Error("verify: the database does not answer"); } } as unknown as typeof leases;
      try {
        const down = await call("GET", `/update/settings?userId=${U}`);
        assert.deepEqual([down.status, down.body.schedule], [200, fallback]);
      } finally {
        scheduleDefaults.Leases = leases;
      }
      assert.equal((await call("GET", `/update/settings?userId=${U}`)).body.schedule.enabled, true, "the switch reads again once the database answers");
    });
    // ── the old schedulers ───────────────────────────────────────────────────
    console.log("\nTHE OLD SCHEDULERS — the one question to Redis, on throwaway queues in db 15");
    const { Queue } = await import("bullmq");
    const redis = new URL(config.redisUrl);
    const connection = { host: redis.hostname, port: Number(redis.port || 6376), password: redis.password || undefined, db: 15, maxRetriesPerRequest: null as null };
    const oldUpdate = new Queue(`verify-sl-update-${run}`, { connection });
    const oldInvoice = new Queue(`verify-sl-invoice-${run}`, { connection });
    const queues = { update: () => oldUpdate, invoice: () => oldInvoice };
    try {
      await check("a sweep's old scheduler still in Redis is named with its period and next time; one that was removed is not; a Redis that does not answer reads as unknown", async () => {
        assert.deepEqual(LEGACY_SCHEDULERS, { update_active: { queue: "update", id: "order-update-active" }, update_ofd: { queue: "update", id: "order-update-ofd" }, invoice_sweep: { queue: "invoice", id: "order-invoice-sweep" } });
        assert.deepEqual(await readLegacyClocks(queues), {}, "none there");
        // As the worker installed them before the shared clock (worker.ts, orderUpdateQueue.ts ensureUpdateSchedules).
        const began = Date.now();
        await oldUpdate.upsertJobScheduler("order-update-active", { every: 6 * 60 * 60_000 }, { name: "sweep", data: { requestId: "", email: "", sweep: "active" }, opts: { attempts: 1 } });
        await oldInvoice.upsertJobScheduler("order-invoice-sweep", { every: 3 * 60 * 60_000 }, { name: "sweep", data: { sweep: "scheduled" }, opts: { attempts: 1 } });
        const held = await readLegacyClocks(queues);
        assert.deepEqual(Object.keys(held || {}).sort(), ["invoice_sweep", "update_active"]);
        assert.deepEqual([held?.update_active?.scheduler, held?.update_active?.everyMs, held?.invoice_sweep?.scheduler, held?.invoice_sweep?.everyMs], ["order-update-active", 21_600_000, "order-invoice-sweep", 10_800_000]);
        const next = held?.invoice_sweep?.nextAt?.getTime() || 0;
        assert.ok(next >= began - 1000 && next <= Date.now() + 3 * 60 * 60_000 + 1000, `a real next time (${held?.invoice_sweep?.nextAt?.toISOString()})`);
        // What the worker does when it moves to the shared clock.
        assert.equal(await oldUpdate.removeJobScheduler("order-update-active"), true);
        assert.deepEqual(Object.keys((await readLegacyClocks(queues)) || {}), ["invoice_sweep"]);
        // Redis away: the commands never come back; the overview gets "unknown" after the wait, not a hang and not "none".
        const silent = { getJobScheduler: () => new Promise<undefined>(() => undefined) };
        const asked = Date.now();
        assert.equal(await readLegacyClocks({ update: () => silent, invoice: () => silent }, 60), null);
        assert.ok(Date.now() - asked < 1000);
        assert.equal(await readLegacyClocks({ update: () => ({ getJobScheduler: () => Promise.reject(new Error("Connection is closed.")) }), invoice: () => silent }, 60), null);
      });
    } finally {
      // Stopped from outside: the interrupt removes what this run made (onInterrupt above) and ends the process.
      if (isInterrupted()) await standStill();
      await oldUpdate.obliterate({ force: true }).catch(() => undefined);
      await oldInvoice.obliterate({ force: true }).catch(() => undefined);
      await oldUpdate.close();
      await oldInvoice.close();
      const { Redis } = await import("ioredis");
      const db15 = new Redis({ host: connection.host, port: connection.port, password: connection.password, db: 15, maxRetriesPerRequest: 2 });
      const left = (await db15.keys(`bull:verify-sl-*-${run}:*`)).length;
      await db15.quit();
      console.log(`\nredis db 15: 2 throwaway queues obliterated (${left} key(s) left)`);
      if (left) failures.push("redis cleanup");
    }
  } finally {
    if (isInterrupted()) await standStill();
    console.log = said.log;
    console.warn = said.warn;
    Object.assign(scheduleDefaults, realDefaults);
    scheduleProbes.legacyClocks = realProbe;
    if (server) await new Promise<void>((resolve) => (server as import("node:http").Server).close(() => resolve()));
    for (const name of dropList) await db.dropCollection(name).catch(() => undefined);
    const strays = (await db.listCollections({ name: { $regex: `^verify_sl_${run}_` } }, { nameOnly: true }).toArray()).length;
    // The real collections: neither created nor written by this run, and no row of the invented workspace in the settings.
    const realAfter = await realCollections();
    const created = realAfter.filter((n) => !realBefore.includes(n));
    const realRows = (realAfter.includes("order_schedule_leases") ? await db.collection("order_schedule_leases").countDocuments({ userId: invented }) : 0) + (realAfter.includes("order_schedule_hosts") ? await db.collection("order_schedule_hosts").countDocuments({ systemId: { $regex: run } }) : 0);
    const settingsRows = await db.collection("order_sync_settings").countDocuments({ userId: invented });
    console.log(`cleanup: ${dropList.length} throwaway collection(s) dropped (${strays} left) · real schedule collections created by this run: ${created.length} (there before: ${realBefore.length ? realBefore.join(", ") : "none"}) · rows of this run in them: ${realRows} · in order_sync_settings: ${settingsRows} · listener ${server ? "closed" : "never opened"}`);
    if (strays || created.length || realRows || settingsRows) failures.push("cleanup");
    await mongoose.disconnect();
  }

  console.log(`\n${passed} passed, ${failures.length} failed${failures.length ? ` — ${failures.join("; ")}` : ""}`);
  process.exitCode = failures.length ? 1 : 0;
}

main().catch((err) => {
  if (isInterrupted()) return;
  console.error(err);
  process.exitCode = 1;
});
