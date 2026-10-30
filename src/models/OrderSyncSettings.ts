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
  /** Cancel speed (services/orderCancel.ts): Chrome windows at once (accounts in parallel) and tabs per window (units of one account in parallel). 0 = not set, env/default applies. */
  cancelWindows: number;
  cancelTabs: number;
  cancelChangedBy: string;
  cancelChangedAt: Date | null;
  /** Update speed (services/orderUpdateQueue.ts): accounts refreshed in parallel by the worker. 0 = UPDATE_CONCURRENCY / default. */
  updateWindows: number;
  updateChangedBy: string;
  updateChangedAt: Date | null;
  /** Scheduled fetch (services/orderFetchSchedule.ts): how far back the 05:00 run looks. 0 = FETCH_SINCE_DAYS / default 60. */
  fetchSinceDays: number;
  fetchScheduleChangedBy: string;
  fetchScheduleChangedAt: Date | null;
  /** Scheduled fetch downloads invoices ("All missing": every delivered unit without a PDF, capped per run). Dashboard Settings toggle; null = FETCH_SCHEDULE_INVOICES / off. */
  /** Proxy mode per environment (services/proxyMode.ts): off | preferred | required; null = that process's PROXY_MODE env. */
  /** HTTP checkout engine: accept a saved address by its Match Phrase; seconds to wait for My Orders after Pay. null = default. */
  addressMatchPhrase: boolean | null;
  httpVerifyWaitSec: number | null;
  checkoutChangedBy: string;
  checkoutChangedAt: Date | null;
  proxyModeLocal: string | null;
  proxyModeProduction: string | null;
  proxyChangedBy: string;
  proxyChangedAt: Date | null;
  scheduledInvoices: boolean | null;
  scheduledInvoicesChangedBy: string;
  scheduledInvoicesChangedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

const OrderSyncSettingsSchema = new Schema<IOrderSyncSettings>(
  {
    userId: { type: Schema.Types.ObjectId, required: true, unique: true },
    fetchMode: { type: String, enum: ["scrape", "api"] },
    changedBy: { type: String, default: "" },
    changedAt: { type: Date, default: null },
    cancelWindows: { type: Number, default: 0 },
    cancelTabs: { type: Number, default: 0 },
    cancelChangedBy: { type: String, default: "" },
    cancelChangedAt: { type: Date, default: null },
    updateWindows: { type: Number, default: 0 },
    updateChangedBy: { type: String, default: "" },
    updateChangedAt: { type: Date, default: null },
    fetchSinceDays: { type: Number, default: 0 },
    fetchScheduleChangedBy: { type: String, default: "" },
    fetchScheduleChangedAt: { type: Date, default: null },
    proxyModeLocal: { type: String, default: null },
    proxyModeProduction: { type: String, default: null },
    proxyChangedBy: { type: String, default: "" },
    proxyChangedAt: { type: Date, default: null },
    scheduledInvoices: { type: Boolean, default: null },
    // HTTP checkout engine switches (services/checkoutSettings.ts); null = default.
    addressMatchPhrase: { type: Boolean, default: null },
    httpVerifyWaitSec: { type: Number, default: null },
    checkoutChangedBy: { type: String, default: "" },
    checkoutChangedAt: { type: Date, default: null },
    scheduledInvoicesChangedBy: { type: String, default: "" },
    scheduledInvoicesChangedAt: { type: Date, default: null },
  },
  { timestamps: true, collection: "order_sync_settings" }
);

export const OrderSyncSettings = mongoose.model<IOrderSyncSettings>("OrderSyncSettings", OrderSyncSettingsSchema);
