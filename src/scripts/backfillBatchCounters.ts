import mongoose from "mongoose";
import { connectDb } from "../db.js";
import { CheckoutBatch } from "../models/CheckoutBatch.js";
import { CheckoutJob } from "../models/CheckoutJob.js";

/**
 * One-off for the move of batch counters from Redis to the batch document
 * (services/batchCounters.ts, 2026-10-19): fill purchasedQuantity /
 * attemptsUsed / batchStatus on every batch from its jobs, so the first
 * reservation after the switch sees the truth and cannot overbuy.
 *
 *   purchasedQuantity = Σ quantity of paid jobs (+ units of attempts still in flight)
 *   attemptsUsed      = jobs that reached Flipkart: paid, failed, running, reached_payment, needs_reconciliation
 *   batchStatus       = running only while something is open; else completed / exhausted / (kept if cancelled)
 *
 *   npm run batches:backfill -- [--dry-run]
 */
const DRY = process.argv.includes("--dry-run");
const ATTEMPT = ["paid", "failed", "running", "reached_payment", "needs_reconciliation"];
const OPEN = ["queued", "running"];

async function main() {
  await connectDb();
  const batches = await CheckoutBatch.find({}).lean();
  let changed = 0;
  for (const b of batches) {
    const agg = await CheckoutJob.aggregate<{ _id: string; n: number; units: number }>([{ $match: { batchId: b.batchId } }, { $group: { _id: "$status", n: { $sum: 1 }, units: { $sum: "$quantity" } } }]);
    const units = (s: string) => agg.find((a) => a._id === s)?.units || 0;
    const count = (s: string) => agg.find((a) => a._id === s)?.n || 0;
    const purchased = units("paid") + units("running") + units("reached_payment") + units("needs_reconciliation");
    const attempts = ATTEMPT.reduce((s, st) => s + count(st), 0);
    const open = OPEN.reduce((s, st) => s + count(st), 0);
    let status = b.batchStatus;
    if (status === "running" || status === "filtered") {
      if (open > 0) status = "running";
      else if (purchased >= b.totalQuantity) status = "completed";
      else if (attempts >= b.totalAttempts) status = "exhausted";
      else if (count("cancelled") > 0) status = "cancelled"; // stopped by an operator before either limit
      else status = agg.length ? "exhausted" : "running"; // finished without reaching either limit — nothing can restart it
    }
    const first = await CheckoutJob.findOne({ batchId: b.batchId }).select("productUrl product.model request.paymentMode").lean();
    const set: Record<string, unknown> = {
      purchasedQuantity: purchased,
      attemptsUsed: attempts,
      batchStatus: status,
      productUrl: b.productUrl || first?.productUrl || "",
      productName: b.productName || (first as { product?: { model?: string } } | null)?.product?.model || "",
      paymentMode: b.paymentMode || (first as { request?: { paymentMode?: string } } | null)?.request?.paymentMode || "",
      completedAt: b.completedAt || (status !== "running" ? b.updatedAt : null),
      ...(status === "cancelled" && !b.cancelledAt ? { cancelledAt: b.updatedAt, cancelReason: b.cancelReason || "backfill: remaining attempts were cancelled" } : {}),
    };
    const diff = set.purchasedQuantity !== b.purchasedQuantity || set.attemptsUsed !== b.attemptsUsed || set.batchStatus !== b.batchStatus || !b.productUrl;
    if (!diff) continue;
    changed += 1;
    console.log(`${b.batchId.slice(0, 8)} ${b.createdAt.toISOString().slice(0, 16)} ${b.batchStatus}→${status} purchased ${b.purchasedQuantity}→${purchased}/${b.totalQuantity} attempts ${b.attemptsUsed}→${attempts}/${b.totalAttempts} open=${open}`);
    if (!DRY) await CheckoutBatch.updateOne({ _id: b._id }, { $set: set });
  }
  console.log(`${DRY ? "would update" : "updated"} ${changed}/${batches.length} batch document(s)`);
  await mongoose.disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
