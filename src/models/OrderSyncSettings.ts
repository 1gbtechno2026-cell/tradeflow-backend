import mongoose, { Schema, Types } from "mongoose";
import type { ScheduleName } from "./ScheduleLease.js";

/**
 * One schedule's switch (Settings → Scheduled fetch), shared by every machine on this database. `on` null or the
 * path missing = never set: nothing runs until the seed or a person sets it (services/scheduleSettings.ts).
 */
export interface IScheduleSwitch {
  on: boolean | null;
  /** A username, or "seed:<host>" for the value Trade Flow wrote at its first start. */
  changedBy: string;
  changedAt: Date | null;
}

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
  /** HTTP checkout engine: seconds to wait for My Orders after Pay. null = default. */
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
  /** The four schedules' switches: schedules.fetch, .update_active, .update_ofd, .invoice_sweep. The .env only says whether a machine MAY run one. */
  schedules?: Partial<Record<ScheduleName, IScheduleSwitch>>;
  createdAt: Date;
  updatedAt: Date;
}

// No default on the four paths: a row made by any other setting's upsert must not look as if its switches had been set.
const ScheduleSwitchSchema = new Schema<IScheduleSwitch>({ on: { type: Boolean, default: null }, changedBy: { type: String, default: "" }, changedAt: { type: Date, default: null } }, { _id: false });

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
    // HTTP checkout engine (services/checkoutSettings.ts); null = default.
    httpVerifyWaitSec: { type: Number, default: null },
    checkoutChangedBy: { type: String, default: "" },
    checkoutChangedAt: { type: Date, default: null },
    scheduledInvoicesChangedBy: { type: String, default: "" },
    scheduledInvoicesChangedAt: { type: Date, default: null },
    schedules: {
      fetch: { type: ScheduleSwitchSchema, default: undefined },
      update_active: { type: ScheduleSwitchSchema, default: undefined },
      update_ofd: { type: ScheduleSwitchSchema, default: undefined },
      invoice_sweep: { type: ScheduleSwitchSchema, default: undefined },
    },
  },
  { timestamps: true, collection: "order_sync_settings" }
);

export const OrderSyncSettings = mongoose.model<IOrderSyncSettings>("OrderSyncSettings", OrderSyncSettingsSchema);
