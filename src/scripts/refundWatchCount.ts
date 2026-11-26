/**
 * How many units the refund watch adds to the 6-hour active sweep (data-quality U12 + X5) — READ-ONLY.
 *
 *   npm run orders:refund-watch
 *
 * Counts, on the workspace (TRADE_FLOW_USER_ID), each clause of orderLifecycle.ts refundWatchClauses, their union
 * (refundWatchFilter — what resolveTargets ORs into the sweep's selection), the part of it that is new to the sweep
 * (the active selection — terminalStatusFilter — already takes the rest: none, since every clause is Cancelled) and
 * the part on logged-in accounts (resolveTargets leaves the others out). Prints counts only. Mongo connected with
 * autoIndex / autoCreate off; no write.
 */
import mongoose, { Types } from "mongoose";
import { config } from "../config.js";
import { refundWatchClauses, refundWatchFilter, terminalStatusFilter } from "../services/orderLifecycle.js";

async function main() {
  if (!config.userId || !Types.ObjectId.isValid(config.userId)) throw new Error("TRADE_FLOW_USER_ID is required");
  const userId = new Types.ObjectId(config.userId);
  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false, serverSelectionTimeoutMS: 15_000 });
  try {
    const units = mongoose.connection.db!.collection("order_details");
    const now = new Date();
    const clauses = refundWatchClauses(now);
    console.log(`refund watch — ${now.toISOString()} (read-only)`);
    for (const [name, q] of Object.entries(clauses)) console.log(`  ${name.padEnd(8)} ${await units.countDocuments({ userId, ...q })}`);
    const watch = { userId, ...refundWatchFilter(now) };
    const all = await units.countDocuments(watch);
    const active = await units.countDocuments({ userId, ...terminalStatusFilter() });
    const both = await units.countDocuments({ userId, $and: [refundWatchFilter(now), terminalStatusFilter()] });
    const emails = (await units.distinct("platform_email", watch)).map((e) => String(e || "").toLowerCase());
    const loggedIn = new Set(
      (await mongoose.connection.db!.collection("platformids").find({ userId, email: { $in: emails }, status: "logged_in" }, { projection: { email: 1 } }).toArray()).map((r) => String(r.email || "").toLowerCase())
    );
    const onLoggedIn = await units.countDocuments({ ...watch, platform_email: { $in: [...loggedIn] } });
    console.log(`  union    ${all} unit(s) on ${emails.length} account(s) — ${onLoggedIn} on logged-in accounts (what a sweep reads)`);
    console.log(`  the active selection today: ${active} unit(s); already in it: ${both} → the watch adds ${all - both} unit(s) a sweep`);
    const cancelled = { userId, status_key: /^(cancelled|canceled)$/i };
    const since = (d: number) => new Date(now.getTime() - d * 86_400_000);
    const notCod = { cash_on_delivery: { $ne: true }, payment_mode: { $not: /^(cod|cash)/i }, "api_extra.payment_mode": { $not: /cash on delivery/i } };
    console.log(
      `  context: cancelled in 3 days ${await units.countDocuments({ ...cancelled, cancelled_date: { $gte: since(3) } })} (not COD ${await units.countDocuments({ ...cancelled, ...notCod, cancelled_date: { $gte: since(3) } })})` +
        ` · in 7 days ${await units.countDocuments({ ...cancelled, cancelled_date: { $gte: since(7) } })} (not COD ${await units.countDocuments({ ...cancelled, ...notCod, cancelled_date: { $gte: since(7) } })})`
    );
  } finally {
    await mongoose.disconnect();
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exitCode = 1;
});
