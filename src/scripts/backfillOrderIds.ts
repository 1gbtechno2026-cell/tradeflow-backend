/**
 * One-off: jobs placed before 2026-10-01 stored Flipkart's SHORT order id
 * (the confirmation URL's reference_id, "OD" + 16 digits). The full id the
 * rest of the system keys on is the same digits plus "00". Rewrites those,
 * keeping the short form in result.flipkartReferenceId.
 *
 *   npm run backfill:order-ids            report only
 *   npm run backfill:order-ids -- --apply write
 */
import mongoose from "mongoose";
import { connectDb } from "../db.js";
import { CheckoutJob } from "../models/CheckoutJob.js";
import { fullOrderId } from "../automation/PaymentApiWatcher.js";

async function main() {
  const apply = process.argv.includes("--apply");
  await connectDb();
  const jobs = await CheckoutJob.find({ "result.flipkartOrderId": /^OD\d{16}$/ })
    .select("result.flipkartOrderId")
    .lean();
  console.log(`${jobs.length} job(s) hold a short order id`);
  for (const j of jobs) {
    const short = String(j.result?.flipkartOrderId || "");
    const full = fullOrderId(short);
    console.log(`  ${String(j._id)}  ${short} -> ${full}${apply ? "" : "  (dry run)"}`);
    if (apply) {
      await CheckoutJob.updateOne(
        { _id: j._id },
        { $set: { "result.flipkartOrderId": full, "result.flipkartReferenceId": short } }
      );
    }
  }
  await mongoose.disconnect();
}

main().catch(async (err) => {
  console.error(err instanceof Error ? err.message : err);
  await mongoose.disconnect().catch(() => {});
  process.exit(1);
});
