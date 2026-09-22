import mongoose, { Schema } from "mongoose";

export type BatchRecordStatus = "running" | "completed" | "exhausted" | "filtered";

export interface ICheckoutBatch {
  batchId: string;
  userId: string;
  batchStatus: BatchRecordStatus;
  filterReason: string | null;
  filteredCount: number;
  purchasedQuantity: number;
  attemptsUsed: number;
  totalQuantity: number;
  quantityPerOrder: number;
  totalAttempts: number;
  createdAt: Date;
  updatedAt: Date;
}

const CheckoutBatchSchema = new Schema<ICheckoutBatch>(
  {
    batchId: { type: String, required: true, unique: true, index: true },
    userId: { type: String, required: true, index: true },
    batchStatus: {
      type: String,
      enum: ["running", "completed", "exhausted", "filtered"],
      default: "running",
      index: true,
    },
    filterReason: { type: String, default: null },
    filteredCount: { type: Number, default: 0 },
    purchasedQuantity: { type: Number, default: 0 },
    attemptsUsed: { type: Number, default: 0 },
    totalQuantity: { type: Number, required: true },
    quantityPerOrder: { type: Number, required: true },
    totalAttempts: { type: Number, required: true },
  },
  { timestamps: true, collection: "tradeflowbatches" }
);

export const CheckoutBatch = mongoose.model<ICheckoutBatch>("CheckoutBatch", CheckoutBatchSchema);
