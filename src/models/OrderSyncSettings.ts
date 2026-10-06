import mongoose, { Schema, Types } from "mongoose";

/**
 * Per-workspace switches for order fetch / update that an operator flips in
 * the dashboard, without an env edit or a restart. One document per user.
 * ORDER_FETCH_MODE in .env is only the fallback when no document exists.
 */
export interface IOrderSyncSettings {
  userId: Types.ObjectId;
  fetchMode: "scrape" | "api";
  changedBy: string;
  changedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

const OrderSyncSettingsSchema = new Schema<IOrderSyncSettings>(
  {
    userId: { type: Schema.Types.ObjectId, required: true, unique: true },
    fetchMode: { type: String, enum: ["scrape", "api"], required: true },
    changedBy: { type: String, default: "" },
    changedAt: { type: Date, default: null },
  },
  { timestamps: true, collection: "order_sync_settings" }
);

export const OrderSyncSettings = mongoose.model<IOrderSyncSettings>("OrderSyncSettings", OrderSyncSettingsSchema);
