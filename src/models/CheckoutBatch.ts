import mongoose, { Schema } from "mongoose";

/**
 * One bulk order = one batch. The single source of truth for its progress
 * (services/batchCounters.ts mutates the counters atomically) and the unit
 * the operator cancels (routes/jobs.ts POST /batch/:batchId/cancel).
 *
 *   running     attempts may still be made
 *   completed   totalQuantity bought
 *   exhausted   totalAttempts spent
 *   cancelling  operator asked to stop; attempts already past Pay are finishing
 *   cancelled   nothing open any more
 *   filtered    legacy (pre-2026-09) — no longer produced
 *
 * `systemId` is the machine / deployment that owns the batch (its queue
 * lives in that system's Redis); other systems list it but never consume
 * or sweep it.
 */
export type BatchRecordStatus = "running" | "completed" | "exhausted" | "cancelling" | "cancelled" | "filtered";
export const BATCH_STATUSES: readonly BatchRecordStatus[] = ["running", "completed", "exhausted", "cancelling", "cancelled", "filtered"];

export interface ICheckoutBatch {
  batchId: string;
  userId: string;
  systemId: string;
  batchStatus: BatchRecordStatus;
  filterReason: string | null;
  filteredCount: number;
  /** Units reserved by attempts in flight + units bought (see batchCounters.ts). */
  purchasedQuantity: number;
  attemptsUsed: number;
  totalQuantity: number;
  quantityPerOrder: number;
  totalAttempts: number;
  /** What the batch is for — copied from the request so a list needs no job lookup. */
  productUrl: string;
  productName: string;
  paymentMode: string;
  createdBy: string;
  lastProgressAt: Date | null;
  completedAt: Date | null;
  cancelRequestedAt: Date | null;
  cancelRequestedBy: string;
  cancelReason: string;
  cancelledAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

const CheckoutBatchSchema = new Schema<ICheckoutBatch>(
  {
    batchId: { type: String, required: true, unique: true, index: true },
    userId: { type: String, required: true, index: true },
    systemId: { type: String, default: "", index: true },
    batchStatus: { type: String, enum: BATCH_STATUSES, default: "running", index: true },
    filterReason: { type: String, default: null },
    filteredCount: { type: Number, default: 0 },
    purchasedQuantity: { type: Number, default: 0 },
    attemptsUsed: { type: Number, default: 0 },
    totalQuantity: { type: Number, required: true },
    quantityPerOrder: { type: Number, required: true },
    totalAttempts: { type: Number, required: true },
    productUrl: { type: String, default: "" },
    productName: { type: String, default: "" },
    paymentMode: { type: String, default: "" },
    createdBy: { type: String, default: "" },
    lastProgressAt: { type: Date, default: null },
    completedAt: { type: Date, default: null },
    cancelRequestedAt: { type: Date, default: null },
    cancelRequestedBy: { type: String, default: "" },
    cancelReason: { type: String, default: "" },
    cancelledAt: { type: Date, default: null },
  },
  { timestamps: true, collection: "tradeflowbatches" }
);
// Backs the Orders tab's Bulk Orders listing: a workspace's batches newest first within a date range.
CheckoutBatchSchema.index({ userId: 1, createdAt: -1 });

export const CheckoutBatch = mongoose.model<ICheckoutBatch>("CheckoutBatch", CheckoutBatchSchema);
