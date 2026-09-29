import { Types } from "mongoose";
import { CardType } from "../models/CardType.js";
import { EmployeePhone } from "../models/EmployeePhone.js";
import { normalizePhone } from "../lib/phone.js";
import { workspaceUserId } from "./sessionStore.js";

/**
 * Claims an employee's OTP phone for the duration of one checkout.
 *
 * Picking a phone is not the same as claiming one. resolveOtpPhone used to take
 * "the first ONLINE row" sorted by id — deterministic, so two jobs starting
 * seconds apart got the SAME handset. Both triggered an OTP to it, and the
 * second SMS overwrote the first in the phone-scoped Redis key. One job then
 * typed the other's code and the bank rejected it, while the logs showed the
 * SMS arriving, the phone online and the code read. Nothing looked wrong.
 *
 * The lease also does something the old picker could not: it lets the SMS
 * webhook answer "which run does this code belong to?", because the handset
 * records who is holding it. That is what makes per-run delivery possible.
 *
 * Schema note: leasedBy/leasedUntil and their index are DEFINED by the UI
 * backend, which owns employeephones. This service only $sets them — consistent
 * with the model's autoIndex:false / autoCreate:false stance.
 */

/** 3 minutes — deliberately shorter than the OTP's own ~5 minute lifetime, so a
 *  lapsed lease can never outlive the code it was guarding. */
export const LEASE_TTL_MS = Number(process.env.OTP_LEASE_TTL_MS || 3 * 60 * 1000);

export interface LeasedOtpPhone {
  phoneId: number;
  phoneNumber: string;
  employeeId: string;
  corporateId: string;
  cardTypeId: number;
  cardTypeName: string;
  leasedUntil: Date;
}

export class NoPhoneAvailableError extends Error {}

/**
 * Atomically claim a free phone for this run.
 *
 * findOneAndUpdate is one atomic operation, so two workers racing the identical
 * filter cannot both win: the loser gets a different free handset, or null.
 * Throws rather than returning null so a caller can never proceed phoneless.
 */
export async function leaseOtpPhone(opts: {
  cardTypeName: string;
  runId: string;
  corporateId?: string;
  employeeId?: string;
  ttlMs?: number;
}): Promise<LeasedOtpPhone> {
  const userId = new Types.ObjectId(workspaceUserId());
  const name = String(opts.cardTypeName || "").trim().toUpperCase();

  const cardType = await CardType.findOne({ userId, cardTypeName: name }).lean();
  if (!cardType) throw new Error(`Card type "${name}" not found in cardtypes for this workspace`);

  const now = new Date();
  const until = new Date(now.getTime() + (opts.ttlMs ?? LEASE_TTL_MS));

  const filter: Record<string, unknown> = {
    userId,
    cardTypeId: cardType.id,
    isActive: { $ne: false },
    // Claimable when never leased OR the hold has lapsed.
    // { leasedUntil: { $lt: now } } ALONE MATCHES NOTHING: MongoDB brackets
    // comparisons by BSON type, so $lt against a Date never matches null — and
    // every phone starts null, so the first claim would fail forever.
    $or: [{ leasedUntil: null }, { leasedUntil: { $exists: false } }, { leasedUntil: { $lt: now } }],
  };
  if (opts.corporateId) filter.corporateId = String(opts.corporateId).toUpperCase();
  if (opts.employeeId) filter.employeeId = String(opts.employeeId).toUpperCase();

  const row = await EmployeePhone.findOneAndUpdate(
    filter,
    { $set: { leasedBy: opts.runId, leasedUntil: until } },
    // Prefer a reachable handset, then the one idle longest — spreading usage
    // instead of hammering the lowest id, which is what made the old picker
    // collide in the first place.
    { new: true, sort: { connectionStatus: 1, leasedUntil: 1, id: 1 } }
  );

  if (!row) {
    throw new NoPhoneAvailableError(
      `No free OTP phone for "${name}"${opts.corporateId ? ` / ${opts.corporateId}` : ""} — ` +
        `every handset is leased to a running checkout. Wait, or add more under Employee Phones.`
    );
  }

  const phoneNumber = normalizePhone(row.rawPhoneNumber);
  if (!phoneNumber) {
    // Never hold a handset we cannot actually receive on.
    await releaseOtpPhone(row.id, opts.runId);
    throw new Error(`Employee ${row.employeeId} has an unusable phone number`);
  }

  return {
    phoneId: row.id,
    phoneNumber,
    employeeId: row.employeeId,
    corporateId: row.corporateId,
    cardTypeId: cardType.id,
    cardTypeName: cardType.cardTypeName,
    leasedUntil: until,
  };
}

/**
 * Release once the checkout finishes, so the employee ID is free again
 * immediately rather than after the TTL. Guarded by runId: a worker whose lease
 * already lapsed and was re-taken must not clear the new holder's claim.
 */
export async function releaseOtpPhone(phoneId: number, runId: string): Promise<boolean> {
  const userId = new Types.ObjectId(workspaceUserId());
  const res = await EmployeePhone.updateOne(
    { userId, id: phoneId, leasedBy: runId },
    { $set: { leasedBy: null, leasedUntil: null } }
  );
  return res.modifiedCount === 1;
}

/** Extend a hold that is still ours, for a payment page running long. False
 *  means the lease lapsed and someone else took the handset — stop, rather than
 *  keep typing into a phone you no longer own. */
export async function renewOtpPhone(phoneId: number, runId: string, ttlMs = LEASE_TTL_MS): Promise<boolean> {
  const userId = new Types.ObjectId(workspaceUserId());
  const res = await EmployeePhone.updateOne(
    { userId, id: phoneId, leasedBy: runId, leasedUntil: { $gt: new Date() } },
    { $set: { leasedUntil: new Date(Date.now() + ttlMs) } }
  );
  return res.modifiedCount === 1;
}

/**
 * Which run owns the handset an SMS just arrived on — the lookup that turns
 * "phone 8796701297 got a code" into "this belongs to run X".
 *
 * Trailing-digit match because forwarders report numbers with country codes and
 * separators ("+91 87967 01297").
 */
export async function findRunHoldingPhone(
  phoneNumber: string
): Promise<{ runId: string; phoneId: number; employeeId: string; corporateId: string } | null> {
  const digits = normalizePhone(phoneNumber);
  if (digits.length !== 10) return null;

  const userId = new Types.ObjectId(workspaceUserId());
  const row = await EmployeePhone.findOne({
    userId,
    rawPhoneNumber: { $regex: `${digits}$` },
    leasedBy: { $nin: [null, ""] },
    leasedUntil: { $gt: new Date() },
  }).lean();

  return row?.leasedBy
    ? {
        runId: String(row.leasedBy),
        phoneId: row.id,
        employeeId: row.employeeId,
        corporateId: row.corporateId,
      }
    : null;
}
