import mongoose, { Schema, Types } from "mongoose";
import type { IndexScrapeStatus } from "../services/orderLifecycle.js";

export interface IOrderIndex {
  userId: Types.ObjectId;
  platformAccountId: Types.ObjectId;
  platform_email: string;
  platform: string;
  order_id: string;
  item_id: string;
  unit_id: string;
  order_url: string;
  scrape_status: IndexScrapeStatus;
  list_hint: string;
  status_key: string;
  order_date: Date | null;
  shipped_date: Date | null;
  first_seen_at: Date;
  last_seen_at: Date;
  last_error: string;
  createdAt: Date;
  updatedAt: Date;
}

const OrderIndexSchema = new Schema<IOrderIndex>(
  {
    userId: { type: Schema.Types.ObjectId, required: true, index: true },
    platformAccountId: { type: Schema.Types.ObjectId, required: true, index: true },
    platform_email: { type: String, default: "", lowercase: true, trim: true },
    platform: { type: String, default: "FLIPKART" },
    order_id: { type: String, default: "", index: true },
    item_id: { type: String, default: "" },
    unit_id: { type: String, required: true },
    order_url: { type: String, default: "" },
    scrape_status: {
      type: String,
      enum: ["pending", "scraped", "skipped_old", "failed"],
      default: "pending",
      index: true,
    },
    list_hint: { type: String, default: "" },
    status_key: { type: String, default: "" },
    order_date: { type: Date, default: null },
    shipped_date: { type: Date, default: null },
    first_seen_at: { type: Date, default: Date.now },
    last_seen_at: { type: Date, default: Date.now },
    last_error: { type: String, default: "" },
  },
  { timestamps: true }
);

OrderIndexSchema.index({ userId: 1, platformAccountId: 1, unit_id: 1 }, { unique: true });
OrderIndexSchema.index({ userId: 1, order_id: 1 });
OrderIndexSchema.index({ userId: 1, scrape_status: 1, last_seen_at: -1 });

export const OrderIndex = mongoose.model<IOrderIndex>("OrderIndex", OrderIndexSchema, "order_index");
