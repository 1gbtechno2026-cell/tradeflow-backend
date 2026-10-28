import { connectDb } from "../db.js";
import { CheckoutJob } from "../models/CheckoutJob.js";
import { enqueueOrderSync } from "../services/orderSync.js";

/**
 * Re-queue the order sync of every PAID checkout job whose sync is failed or
 * stuck pending — after a fix to the sync path, so Order Units catches up.
 *   npm run orders:resync-recent -- --hours 6 [--dry-run]
 * Needs the worker running with the fixed code (sync role).
 */
async function main() {
  const argv = process.argv.slice(2);
  const hours = Number(argv[argv.indexOf("--hours") + 1]) || 6;
  const dry = argv.includes("--dry-run");
  await connectDb();
  const since = new Date(Date.now() - hours * 3600e3);
  const jobs = await CheckoutJob.find({ status: "paid", completedAt: { $gte: since }, "orderSync.status": { $in: ["failed", "pending"] } }).select("_id email orderSync.status orderSync.lastError").lean();
  console.log(`${jobs.length} paid job(s) in the last ${hours} h with sync ${dry ? "(dry run)" : "to re-queue"}`);
  const byErr = new Map<string, number>();
  for (const j of jobs as Array<{ _id: unknown; orderSync?: { lastError?: string } }>) {
    const k = String(j.orderSync?.lastError || "").replace(/OD\d+\/\d+/g, "OD…").slice(0, 70);
    byErr.set(k, (byErr.get(k) || 0) + 1);
  }
  for (const [k, n] of byErr) console.log(`  ${String(n).padStart(4)}  ${k}`);
  if (dry) process.exit(0);
  let n = 0;
  for (const j of jobs) {
    await CheckoutJob.updateOne({ _id: j._id }, { $set: { "orderSync.status": "pending", "orderSync.attempts": 0, "orderSync.lastError": "" } });
    await enqueueOrderSync(String(j._id));
    n += 1;
  }
  console.log(`re-queued ${n}`);
  process.exit(0);
}

main().catch((e) => {
  console.error("ERR", e instanceof Error ? e.message : String(e));
  process.exit(1);
});
