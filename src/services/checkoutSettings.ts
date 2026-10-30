/**
 * Operator switches for the HTTP checkout engine, flipped in the dashboard's
 * Settings tab and read by the worker per job (cached 30 s), no restart:
 *
 *   addressMatchPhrase   accept a saved address when its text contains the
 *                        address's Match Phrase (BulkOrderBuddy's rule), not
 *                        only when the contact name is identical. Off by
 *                        default: on 2,831 attempts (Oct 27–30) the engine
 *                        never needed it — the book is edited only when the
 *                        saved pincode differs, where no phrase can match.
 *   httpVerifyWaitSec    how long the engine waits after Pay for My Orders to
 *                        list the order. 15 s by default: of 733 paid orders
 *                        none was listed within 90 s; the order sync now
 *                        keeps checking for 8 h, so a long wait only holds the
 *                        lane. Floor 5 s, cap 300 s.
 */
import { Types } from "mongoose";
import { OrderSyncSettings } from "../models/OrderSyncSettings.js";
import { config } from "../config.js";

export interface CheckoutSettings {
  addressMatchPhrase: boolean;
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
  let value: CheckoutSettings = { addressMatchPhrase: false, httpVerifyWaitSec: DEFAULT_WAIT_SEC, source: "default", changedBy: "", changedAt: null };
  if (Types.ObjectId.isValid(userId)) {
    const row = await OrderSyncSettings.findOne({ userId }).select("addressMatchPhrase httpVerifyWaitSec checkoutChangedBy checkoutChangedAt").lean().catch(() => null);
    if (row && (row.addressMatchPhrase != null || row.httpVerifyWaitSec != null)) {
      value = {
        addressMatchPhrase: row.addressMatchPhrase === true,
        httpVerifyWaitSec: row.httpVerifyWaitSec != null ? Math.max(5, Math.min(300, Number(row.httpVerifyWaitSec) || DEFAULT_WAIT_SEC)) : DEFAULT_WAIT_SEC,
        source: "ui",
        changedBy: row.checkoutChangedBy || "",
        changedAt: row.checkoutChangedAt || null,
      };
    }
  }
  cached = { at: Date.now(), userId, value };
  return value;
}

export async function setCheckoutSettings(userId: string, input: { addressMatchPhrase?: unknown; httpVerifyWaitSec?: unknown }, by: string): Promise<CheckoutSettings> {
  const set: Record<string, unknown> = { checkoutChangedBy: by || "", checkoutChangedAt: new Date() };
  if (input.addressMatchPhrase !== undefined) set.addressMatchPhrase = input.addressMatchPhrase === true || input.addressMatchPhrase === "true";
  if (input.httpVerifyWaitSec !== undefined) {
    const n = Number(input.httpVerifyWaitSec);
    if (!Number.isFinite(n) || n < 5 || n > 300) throw new Error("httpVerifyWaitSec must be between 5 and 300");
    set.httpVerifyWaitSec = Math.round(n);
  }
  await OrderSyncSettings.updateOne({ userId: new Types.ObjectId(userId) }, { $set: set, $setOnInsert: { userId: new Types.ObjectId(userId) } }, { upsert: true });
  cached = null;
  console.log(`[checkout-settings] ${JSON.stringify(set)} by ${by || "?"} (applies to the next job on ${config.workerId})`);
  return resolveCheckoutSettings(userId);
}
