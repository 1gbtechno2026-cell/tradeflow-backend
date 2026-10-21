import { Types } from "mongoose";
import { config } from "../config.js";
import { OrderSyncSettings } from "../models/OrderSyncSettings.js";

/**
 * The effective fetch mode for a run: the dashboard's setting when one has
 * been saved, else ORDER_FETCH_MODE from .env. Resolved at the START of every
 * fetch/update, never cached across runs, so a flip applies to the next run
 * with no restart.
 */
export type OrderFetchMode = "scrape" | "api";

export interface ResolvedFetchMode {
  mode: OrderFetchMode;
  source: "ui" | "env";
  changedBy?: string;
  changedAt?: Date | null;
}

export async function resolveOrderFetchMode(userId: string): Promise<ResolvedFetchMode> {
  if (!Types.ObjectId.isValid(userId)) return { mode: config.orderFetchMode, source: "env" };
  const row = await OrderSyncSettings.findOne({ userId }).lean().catch(() => null);
  if (row?.fetchMode === "api" || row?.fetchMode === "scrape") {
    return { mode: row.fetchMode, source: "ui", changedBy: row.changedBy, changedAt: row.changedAt };
  }
  return { mode: config.orderFetchMode, source: "env" };
}

export async function setOrderFetchMode(userId: string, mode: OrderFetchMode, changedBy: string): Promise<ResolvedFetchMode> {
  if (mode !== "api" && mode !== "scrape") throw new Error('mode must be "scrape" or "api"');
  const before = await resolveOrderFetchMode(userId);
  const now = new Date();
  await OrderSyncSettings.updateOne(
    { userId },
    { $set: { fetchMode: mode, changedBy: changedBy || "", changedAt: now }, $setOnInsert: { userId } },
    { upsert: true }
  );
  console.log(`[order-sync] fetch mode ${before.mode} (${before.source}) → ${mode} (ui) by ${changedBy || "dashboard"}`);
  return { mode, source: "ui", changedBy, changedAt: now };
}
