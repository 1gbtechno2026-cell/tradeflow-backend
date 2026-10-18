import { CheckoutBatch, type BatchRecordStatus } from "../models/CheckoutBatch.js";

/**
 * Batch-wide counters, kept on the batch's own Mongo document
 * (tradeflowbatches) and mutated only with atomic, guarded updates — never
 * read-then-write in JS. Atlas is a replica set, so one findOneAndUpdate is
 * as atomic as the Lua script this replaced, and the numbers are the same
 * from every machine, survive a Redis restart, and feed the change stream
 * the dashboard listens to.
 *
 * Semantics (unchanged from the Redis version):
 *   purchasedQuantity  units reserved by attempts in flight + units bought;
 *                      a failed attempt gives its reservation back
 *   attemptsUsed       attempts that reached Flipkart; refunded only when OUR
 *                      plumbing failed before Flipkart saw anything
 *   batchStatus        running → completed (target met) | exhausted (budget
 *                      spent) | cancelling → cancelled (operator)
 *
 * There is no "filtered" state any more. A batch used to be stopped the moment
 * one job saw out-of-stock / not-deliverable; now every selected platform ID is
 * attempted and reports its own reason, because a product refused to one
 * account may be sold to another. The only things that end a batch early are
 * reaching the quantity target, running out of attempts, and cancellation.
 */

export type BatchStatus = BatchRecordStatus;

export interface BatchProgress {
  status: BatchStatus;
  purchasedQuantity: number;
  attemptsUsed: number;
}

export type ReserveResult =
  | { kind: "ok"; purchasedQuantity: number; attemptsUsed: number }
  | { kind: "skip_target"; purchasedQuantity: number; attemptsUsed: number }
  | { kind: "skip_exhausted"; purchasedQuantity: number; attemptsUsed: number }
  | { kind: "skip_cancelled"; purchasedQuantity: number; attemptsUsed: number };

const CANCELLED: BatchStatus[] = ["cancelling", "cancelled"];

type Counters = { batchStatus: BatchStatus; purchasedQuantity: number; attemptsUsed: number };

async function readCounters(batchId: string): Promise<Counters | null> {
  return CheckoutBatch.findOne({ batchId }).select("batchStatus purchasedQuantity attemptsUsed").lean<Counters>();
}

/** The batch document is created by the jobs route; nothing to prepare any more. Kept for callers. */
export async function initBatchCounters(_batchId: string): Promise<void> {
  /* no-op */
}

/**
 * Take one attempt and `quantityPerOrder` units for an attempt that is about
 * to open Chrome, or say why it must not: target already met, budget spent,
 * batch cancelled. One guarded update; two workers cannot both take the last
 * slot.
 */
export async function reserveBatchSlot(input: { batchId: string; quantityPerOrder: number; totalQuantity: number; totalAttempts: number }): Promise<ReserveResult> {
  const { batchId, quantityPerOrder, totalQuantity, totalAttempts } = input;
  const taken = await CheckoutBatch.findOneAndUpdate(
    {
      batchId,
      batchStatus: "running",
      purchasedQuantity: { $lte: totalQuantity - quantityPerOrder },
      attemptsUsed: { $lt: totalAttempts },
    },
    { $inc: { purchasedQuantity: quantityPerOrder, attemptsUsed: 1 }, $set: { lastProgressAt: new Date() } },
    { new: true }
  )
    .select("purchasedQuantity attemptsUsed")
    .lean<Counters>();
  if (taken) return { kind: "ok", purchasedQuantity: taken.purchasedQuantity, attemptsUsed: taken.attemptsUsed };

  // Refused: say why, and settle the batch status the way the attempt would have.
  const now = await readCounters(batchId);
  const purchasedQuantity = now?.purchasedQuantity ?? 0;
  const attemptsUsed = now?.attemptsUsed ?? 0;
  if (!now) return { kind: "skip_target", purchasedQuantity, attemptsUsed };
  if (CANCELLED.includes(now.batchStatus)) return { kind: "skip_cancelled", purchasedQuantity, attemptsUsed };
  if (now.batchStatus === "completed") return { kind: "skip_target", purchasedQuantity, attemptsUsed };
  if (now.batchStatus === "exhausted") return { kind: "skip_exhausted", purchasedQuantity, attemptsUsed };
  if (purchasedQuantity >= totalQuantity) {
    await CheckoutBatch.updateOne({ batchId, batchStatus: "running" }, { $set: { batchStatus: "completed", completedAt: new Date() } });
    return { kind: "skip_target", purchasedQuantity, attemptsUsed };
  }
  if (attemptsUsed >= totalAttempts) {
    await CheckoutBatch.updateOne({ batchId, batchStatus: "running" }, { $set: { batchStatus: "exhausted", completedAt: new Date() } });
    return { kind: "skip_exhausted", purchasedQuantity, attemptsUsed };
  }
  // Room left, but less than one full order: not a completed batch yet, just no slot for this attempt.
  return { kind: "skip_target", purchasedQuantity, attemptsUsed };
}

/**
 * Gives an attempt back. For a job that never reached Flipkart because OUR
 * plumbing failed (the proxy), so the batch's attempt budget is spent only on
 * Flipkart's answers. Clears an `exhausted` mark the refunded attempt caused.
 */
export async function refundBatchAttempt(batchId: string): Promise<number> {
  const doc = await CheckoutBatch.findOneAndUpdate(
    { batchId },
    [
      {
        $set: {
          attemptsUsed: { $max: [0, { $subtract: ["$attemptsUsed", 1] }] },
          batchStatus: { $cond: [{ $eq: ["$batchStatus", "exhausted"] }, "running", "$batchStatus"] },
          completedAt: { $cond: [{ $eq: ["$batchStatus", "exhausted"] }, null, "$completedAt"] },
        },
      },
    ],
    { new: true }
  )
    .select("attemptsUsed")
    .lean<Counters>();
  return doc?.attemptsUsed ?? 0;
}

/** An attempt that reserved units and did not buy them gives them back (never below zero). */
export async function releaseBatchReservation(batchId: string, quantityPerOrder: number): Promise<number> {
  const doc = await CheckoutBatch.findOneAndUpdate(
    { batchId },
    [{ $set: { purchasedQuantity: { $max: [0, { $subtract: ["$purchasedQuantity", quantityPerOrder] }] } } }],
    { new: true }
  )
    .select("purchasedQuantity")
    .lean<Counters>();
  return doc?.purchasedQuantity ?? 0;
}

/** Current numbers, settling `completed` / `exhausted` if the counters already say so. */
export async function readBatchProgress(batchId: string, totalQuantity: number, totalAttempts: number): Promise<BatchProgress> {
  const now = await readCounters(batchId);
  if (!now) return { status: "running", purchasedQuantity: 0, attemptsUsed: 0 };
  let status = now.batchStatus;
  if (status === "running" && now.purchasedQuantity >= totalQuantity) {
    await CheckoutBatch.updateOne({ batchId, batchStatus: "running" }, { $set: { batchStatus: "completed", completedAt: new Date() } });
    status = "completed";
  } else if (status === "running" && now.attemptsUsed >= totalAttempts) {
    await CheckoutBatch.updateOne({ batchId, batchStatus: "running" }, { $set: { batchStatus: "exhausted", completedAt: new Date() } });
    status = "exhausted";
  }
  return { status, purchasedQuantity: now.purchasedQuantity, attemptsUsed: now.attemptsUsed };
}

export function isBatchCancelled(status: BatchStatus | string | undefined): boolean {
  return CANCELLED.includes(status as BatchStatus);
}
