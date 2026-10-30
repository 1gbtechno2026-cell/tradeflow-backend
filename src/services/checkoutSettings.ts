/**
 * Operator setting for the HTTP checkout engine, set in the dashboard's
 * Settings tab and read by the worker per job (cached 30 s), no restart:
 *
 *   httpVerifyWaitSec    how long the engine waits after Pay for My Orders to
 *                        list the order. 15 s by default: of 733 paid orders
 *                        none was listed within 90 s; the order sync keeps
 *                        checking for hours, so a long wait only holds the
 *                        lane. Floor 5 s, cap 300 s.
 */
import { Types } from "mongoose";
import { OrderSyncSettings } from "../models/OrderSyncSettings.js";
import { config } from "../config.js";

export interface CheckoutSettings {
  httpVerifyWaitSec: number;
  source: "ui" | "default";
  changedBy: string;
  changedAt: Date | null;
}

const DEFAULT_WAIT_SEC = Math.max(5, Math.min(300, Math.round((Number(process.env.HTTP_VERIFY_WAIT_MS) || 15_000) / 1000)));
const CACHE_MS = 30_000;
let cached: { at: number; userId: string; value: CheckoutSettings } | null = null;

export async function resolveCheckoutSettings(userId: string): Promise<CheckoutSettings> {
  if (cached && cached.userId === userId && Date.now() - cached.at < CACHE_MS) return cached.value;
  let value: CheckoutSettings = { httpVerifyWaitSec: DEFAULT_WAIT_SEC, source: "default", changedBy: "", changedAt: null };
  if (Types.ObjectId.isValid(userId)) {
    const row = await OrderSyncSettings.findOne({ userId }).select("httpVerifyWaitSec checkoutChangedBy checkoutChangedAt").lean().catch(() => null);
    if (row && row.httpVerifyWaitSec != null) {
      value = {
        httpVerifyWaitSec: Math.max(5, Math.min(300, Number(row.httpVerifyWaitSec) || DEFAULT_WAIT_SEC)),
        source: "ui",
        changedBy: row.checkoutChangedBy || "",
        changedAt: row.checkoutChangedAt || null,
      };
    }
  }
  cached = { at: Date.now(), userId, value };
  return value;
}

export async function setCheckoutSettings(userId: string, input: { httpVerifyWaitSec?: unknown }, by: string): Promise<CheckoutSettings> {
  const n = Number(input.httpVerifyWaitSec);
  if (!Number.isFinite(n) || n < 5 || n > 300) throw new Error("httpVerifyWaitSec must be between 5 and 300");
  const set = { httpVerifyWaitSec: Math.round(n), checkoutChangedBy: by || "", checkoutChangedAt: new Date() };
  await OrderSyncSettings.updateOne({ userId: new Types.ObjectId(userId) }, { $set: set, $setOnInsert: { userId: new Types.ObjectId(userId) } }, { upsert: true });
  cached = null;
  console.log(`[checkout-settings] listing wait ${set.httpVerifyWaitSec} s by ${by || "?"} (applies to the next job on ${config.workerId})`);
  return resolveCheckoutSettings(userId);
}
