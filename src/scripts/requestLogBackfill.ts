/**
 * The request log's history — what the API's keeper does on its first pass
 * (services/requestLogKeeper.ts), run by hand:
 *
 *   npm run request-log:backfill             DRY RUN. Reads the request collections (read-only) and writes the task rows
 *                                            of the last 7 days into a THROWAWAY collection with the real indexes; prints
 *                                            what would be written, times the task list over it, and drops it. Nothing
 *                                            real is written and no request is closed.
 *   npm run request-log:backfill -- --write  Writes the missing rows into order_sync_tasks (rows only — closing a request
 *                                            nothing is behind is the keeper's, in the running API).
 *   npm run request-log:backfill -- --check  READ ONLY, on the real order_sync_tasks: what it holds, every request's
 *                                            counters against its rows, and how long the task list takes.
 *
 * Requests older than 7 days are out: the log keeps a task 7 days from its
 * request's creation, as the reference does.
 */
import mongoose, { type Model } from "mongoose";
import { config } from "../config.js";
import { OrderSyncTask, SYNC_TASK_KEEP_MS, type IOrderSyncTask } from "../models/OrderSyncTask.js";
import { keeperPass } from "../services/requestLogKeeper.js";
import { workspaceUserId } from "../services/sessionStore.js";
import { listTasks, noProbes } from "../services/syncBatches.js";
import { SYNC_TASK_KINDS } from "../models/OrderSyncTask.js";
import { loadFacts, logCounts, logDefaults, sameCounts, type LogDeps } from "../services/syncTaskLog.js";

const CHECK = process.argv.includes("--check");
const WRITE = process.argv.includes("--write") && !CHECK;

async function main() {
  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false });
  const name = `verify_rl_backfill_${Date.now().toString(36)}`;
  let Tasks: Model<IOrderSyncTask> = OrderSyncTask;
  if (!WRITE && !CHECK) {
    Tasks = mongoose.model<IOrderSyncTask>("OrderSyncTaskDryRun", OrderSyncTask.schema, name);
    await Tasks.createCollection();
    await Tasks.createIndexes();
  }
  const deps: LogDeps = { ...logDefaults, Tasks };
  const since = new Date(Date.now() - SYNC_TASK_KEEP_MS);
  console.log(`${CHECK ? "READ ONLY — order_sync_tasks as it is" : WRITE ? "WRITING order_sync_tasks" : `DRY RUN into ${name}`} — requests written since ${since.toISOString()}`);
  try {
    if (!CHECK) {
      const t0 = Date.now();
      const res = await keeperPass({ deps, probes: noProbes, since, close: false, log: (l) => console.log(`  ${l}`) });
      console.log(`${res.checked} request(s) checked, ${res.synced} given rows, ${res.rows} task row(s) written — ${Math.round((Date.now() - t0) / 1000)} s`);
      if (res.stale.length) console.log(`open with nothing behind them (the keeper closes these; this script does not):\n  ${res.stale.join("\n  ")}`);
    }
    const by = await Tasks.aggregate<{ _id: { t: string; s: string }; n: number }>([{ $group: { _id: { t: "$taskType", s: "$status" }, n: { $sum: 1 } } }, { $sort: { "_id.t": 1, "_id.s": 1 } }]);
    const types = new Map<string, string[]>();
    for (const r of by) types.set(r._id.t, [...(types.get(r._id.t) || []), `${r._id.s} ${r.n}`]);
    for (const [type, parts] of types) console.log(`  ${type.padEnd(22)} ${parts.join(" · ")}`);
    console.log(`  rows in the collection: ${await Tasks.countDocuments({})} across ${(await Tasks.distinct("requestId")).length} request(s)`);
    if (!CHECK) {
      const again = await keeperPass({ deps, probes: noProbes, since, close: false });
      console.log(`a second pass: ${again.synced} request(s) given rows, ${again.rows} row(s) — ${again.rows ? "NOT in step" : "nothing left to write"}`);
    }

    // Every request of the window: its own counters against its rows.
    let agree = 0;
    let named = 0;
    const differ: string[] = [];
    for (const kind of SYNC_TASK_KINDS) {
      const model = (kind === "fetch" ? deps.Ledger : kind === "update" ? deps.Update : kind === "invoices" ? deps.Invoice : kind === "coupons" ? deps.Coupon : kind === "cancel" ? deps.Cancel : deps.Reschedule) as unknown as Model<Record<string, unknown>>;
      const found = await model.find({ createdAt: { $gte: since }, ...(kind === "fetch" ? { type: "fetch" } : {}) }).select("_id").lean();
      for (const f of (await loadFacts(kind, found.map((r) => String(r._id)), deps)).values()) {
        const rows = await logCounts(f.head.requestId, deps);
        if (f.tasksRecorded === "named") {
          named += 1;
          if (rows.total !== f.counts.total) differ.push(`  ${kind} ${f.head.requestId}: counts ${f.counts.total} account(s), ${rows.total} named in its record (ran before the log)`);
        } else if (sameCounts(rows, f.counts)) agree += 1;
        else differ.push(`  ${kind} ${f.head.requestId}: counters ${JSON.stringify(f.counts)} ≠ rows ${JSON.stringify(rows)}`);
      }
    }
    console.log(`counters = rows for ${agree} request(s); ${named} older fetch run(s) list only the accounts their record names${differ.length ? `; ${differ.length} differ:\n${differ.join("\n")}` : ""}`);

    const userId = workspaceUserId();
    console.log("the task list over these rows:");
    for (const [label, q] of [
      ["page 1", {}],
      ["page 1 again", {}],
      ["page 200", { page: 200 }],
      ["status=failed", { status: "failed" }],
      ["task_type=update", { task_type: "update" }],
      ["task_type=cancel", { task_type: "cancel" }],
      ["error_code=not_logged_in", { error_code: "not_logged_in" }],
      ["source=user", { source: "user" }],
    ] as Array<[string, Record<string, unknown>]>) {
      const t = Date.now();
      const out = await listTasks(userId, q, deps);
      console.log(`  ${label.padEnd(26)} ${String(Date.now() - t).padStart(5)} ms   count ${out.count}, ${out.results.length} row(s)`);
    }
  } finally {
    if (!WRITE && !CHECK) await mongoose.connection.db!.dropCollection(name).catch(() => undefined);
    await mongoose.disconnect();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
