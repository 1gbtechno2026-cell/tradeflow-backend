import type { Types } from "mongoose";
import { Order } from "../models/Order.js";
import { OrderUnitEvent, type UnitEventType } from "../models/OrderUnitEvent.js";
import { stageOf } from "./orderLifecycle.js";

/**
 * Before/after of one unit around a refresh, and the events the difference
 * produces (models/OrderUnitEvent.ts). The update worker takes a snapshot,
 * lets the mapper rewrite the row, takes another, and calls recordUnitChanges.
 *
 * Address bookkeeping lives here too: the first hash seen becomes the
 * baseline (`address_first_seen_at`, an `address_seen` event); a different
 * hash later is an `address_changed` event, with the previous address pushed
 * onto `address_history` and `address_changed_at` set.
 */

export interface UnitSnapshot {
  orderId: string;
  email: string;
  productName: string;
  status_key: string;
  delivery_otp: string;
  shipping_address: Record<string, string> | null;
  shipping_address_hash: string;
  address_first_seen_at: Date | null;
}

const ADDRESS_HISTORY_KEEP = 8;

export async function snapshotUnit(userId: Types.ObjectId | string, unitId: string): Promise<UnitSnapshot | null> {
  const row = await Order.findOne({ userId, unit_id: unitId })
    .select("order_id platform_email product_name status_key delivery_otp shipping_address shipping_address_hash address_first_seen_at")
    .lean();
  if (!row) return null;
  return {
    orderId: String(row.order_id || ""),
    email: String(row.platform_email || ""),
    productName: String(row.product_name || ""),
    status_key: String(row.status_key || ""),
    delivery_otp: String(row.delivery_otp || ""),
    shipping_address: (row.shipping_address as Record<string, string> | null) || null,
    shipping_address_hash: String(row.shipping_address_hash || ""),
    address_first_seen_at: (row.address_first_seen_at as Date | null) || null,
  };
}

export interface UnitChanges {
  stageChanged: boolean;
  otpNew: boolean;
  addressSeen: boolean;
  addressChanged: boolean;
  events: UnitEventType[];
}

export async function recordUnitChanges(
  userId: Types.ObjectId | string,
  unitId: string,
  before: UnitSnapshot | null,
  after: UnitSnapshot | null,
  requestId: string
): Promise<UnitChanges> {
  const out: UnitChanges = { stageChanged: false, otpNew: false, addressSeen: false, addressChanged: false, events: [] };
  if (!after) return out;
  const now = new Date();
  const base = { userId, orderId: after.orderId, unitId, email: after.email, productName: after.productName, at: now, requestId, processed: false, processedAt: null };
  const events: Array<Record<string, unknown>> = [];

  if (before && stageOf(before.status_key) !== stageOf(after.status_key)) {
    out.stageChanged = true;
    events.push({ ...base, type: "stage_changed", before: { status: before.status_key, stage: stageOf(before.status_key) }, after: { status: after.status_key, stage: stageOf(after.status_key) } });
  }
  if (after.delivery_otp && !(before?.delivery_otp)) {
    out.otpNew = true;
    events.push({ ...base, type: "otp_new", before: null, after: { otp: after.delivery_otp, status: after.status_key } });
  }

  const prevHash = before?.shipping_address_hash || "";
  const nextHash = after.shipping_address_hash || "";
  if (nextHash) {
    if (!prevHash || !before?.address_first_seen_at) {
      // Baseline: the first address we record for this unit (a row written by
      // a Fetch already carries a hash but no first-seen stamp — same thing).
      if (!before?.address_first_seen_at) {
        await Order.updateOne({ userId, unit_id: unitId, address_first_seen_at: null }, { $set: { address_first_seen_at: now } });
        out.addressSeen = true;
        events.push({ ...base, type: "address_seen", before: null, after: { address: after.shipping_address, hash: nextHash } });
      }
      if (prevHash && prevHash !== nextHash) {
        out.addressChanged = true;
        await Order.updateOne(
          { userId, unit_id: unitId },
          { $set: { address_changed_at: now }, $push: { address_history: { $each: [{ hash: prevHash, address: before?.shipping_address || null, seenAt: before?.address_first_seen_at || now, until: now }], $slice: -ADDRESS_HISTORY_KEEP } } }
        );
        events.push({ ...base, type: "address_changed", before: { address: before?.shipping_address || null, hash: prevHash }, after: { address: after.shipping_address, hash: nextHash } });
      }
    } else if (prevHash !== nextHash) {
      out.addressChanged = true;
      await Order.updateOne(
        { userId, unit_id: unitId },
        { $set: { address_changed_at: now }, $push: { address_history: { $each: [{ hash: prevHash, address: before?.shipping_address || null, seenAt: before?.address_first_seen_at || now, until: now }], $slice: -ADDRESS_HISTORY_KEEP } } }
      );
      events.push({ ...base, type: "address_changed", before: { address: before?.shipping_address || null, hash: prevHash }, after: { address: after.shipping_address, hash: nextHash } });
    }
  }

  if (events.length) {
    await OrderUnitEvent.insertMany(events, { ordered: false }).catch((err) => console.warn(`[unit-events] insert failed for ${unitId}: ${err instanceof Error ? err.message : err}`));
    out.events = events.map((e) => e.type as UnitEventType);
  }
  return out;
}
