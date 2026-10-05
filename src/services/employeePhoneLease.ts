import { Types } from "mongoose";
import { CardType } from "../models/CardType.js";
import { EmployeePhone } from "../models/EmployeePhone.js";
import { EmployeePhoneSettings } from "../models/EmployeePhoneSettings.js";
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

/** 2 minutes — the payment budget (OTP_TIMEOUT_MS). A run renews its hold
 *  every 60s while it is alive, so a healthy payment keeps the handset for as
 *  long as it needs; a run that has gone quiet loses it on the same clock the
 *  order itself is failed on. Shorter than the OTP's own lifetime, so a lapsed
 *  lease can never outlive the code it was guarding. */
export const LEASE_TTL_MS = Number(process.env.OTP_LEASE_TTL_MS || 2 * 60 * 1000);

/**
 * May a handset be claimed when nothing has confirmed it is reachable?
 *
 * Defaults to true when the settings row is missing, matching the dashboard, so
 * a settings outage cannot silently stop every OTP order. Not cached: an
 * operator flipping the switch mid-batch expects the next lease to obey it.
 */
async function assumeAllOnline(userId: Types.ObjectId): Promise<boolean> {
  const doc = await EmployeePhoneSettings.findOne({ userId }).lean();
  return doc?.assumeAllOnline ?? true;
}

/**
 * Only the handsets that could actually receive an OTP right now.
 *
 * Shared by the lease and by capacity reporting so the two can never disagree
 * about what "available" means — the number an autoscaler caps worker count on
 * has to be the same number the lease will actually hand out.
 */
function claimableFilter(opts: {
  userId: Types.ObjectId;
  cardTypeId: number;
  now: Date;
  online: boolean;
  corporateId?: string;
  employeeId?: string;
}): Record<string, unknown> {
  const filter: Record<string, unknown> = {
    userId: opts.userId,
    cardTypeId: opts.cardTypeId,
    isActive: { $ne: false },
    // Claimable when never leased OR the hold has lapsed.
    // { leasedUntil: { $lt: now } } ALONE MATCHES NOTHING: MongoDB brackets
    // comparisons by BSON type, so $lt against a Date never matches null — and
    // every phone starts null, so the first claim would fail forever.
    $or: [{ leasedUntil: null }, { leasedUntil: { $exists: false } }, { leasedUntil: { $lt: opts.now } }],
  };
  // With the switch off, an unreachable handset must not be claimed at all.
  // Leaving this out is what let 76 OFFLINE phones be handed to workers whose
  // OTP could never arrive — the order then died on a timeout having burnt an
  // attempt, for a reason that looked nothing like "the phone was off".
  if (opts.online) filter.connectionStatus = "ONLINE";
  if (opts.corporateId) filter.corporateId = String(opts.corporateId).toUpperCase();
  if (opts.employeeId) filter.employeeId = String(opts.employeeId).toUpperCase();
  return filter;
}

/**
 * How many handsets could be claimed right now.
 *
 * This is the real concurrency ceiling for any otp-auth card type, and the
 * number Fargate task count should be capped at — NOT the total ever onboarded.
 * Today those differ by more than 2x: 144 rows exist, 67 are ONLINE and active.
 */
export async function availablePhoneCapacity(opts: {
  cardTypeName: string;
  corporateId?: string;
}): Promise<{ claimable: number; onlineOnly: boolean; totalOnboarded: number }> {
  const userId = new Types.ObjectId(workspaceUserId());
  const name = String(opts.cardTypeName || "").trim().toUpperCase();
  const cardType = await CardType.findOne({ userId, cardTypeName: name }).lean();
  if (!cardType) throw new Error(`Card type "${name}" not found in cardtypes for this workspace`);

  const online = !(await assumeAllOnline(userId));
  const [claimable, totalOnboarded] = await Promise.all([
    EmployeePhone.countDocuments(
      claimableFilter({ userId, cardTypeId: cardType.id, now: new Date(), online, corporateId: opts.corporateId })
    ),
    EmployeePhone.countDocuments({ userId, cardTypeId: cardType.id }),
  ]);
  return { claimable, onlineOnly: online, totalOnboarded };
}

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
  const online = !(await assumeAllOnline(userId));

  const filter = claimableFilter({
    userId,
    cardTypeId: cardType.id,
    now,
    online,
    corporateId: opts.corporateId,
    employeeId: opts.employeeId,
  });

  const row = await EmployeePhone.findOneAndUpdate(
    filter,
    { $set: { leasedBy: opts.runId, leasedUntil: until } },
    // Idle longest first, so usage spreads instead of hammering the lowest id —
    // which is what made the old picker collide in the first place.
    //
    // connectionStatus is deliberately NOT in this sort any more. It used to be,
    // ascending, described as "prefer a reachable handset" — but these are
    // strings, and "OFFLINE" < "ONLINE", so ascending order preferred exactly
    // the phones that cannot receive anything. Reachability is a filter now
    // (claimableFilter), which is where a hard requirement belongs; leaving it
    // in the sort would only re-introduce an alphabet dependency.
    { new: true, sort: { leasedUntil: 1, id: 1 } }
  );

  if (!row) {
    // Say WHICH scarcity this is. "No free phone" reads as "they are all busy",
    // but with the switch off it is far more often "almost none are reachable" —
    // 67 of 144 here — and those two need completely different actions.
    const capacity = await availablePhoneCapacity({
      cardTypeName: name,
      corporateId: opts.corporateId,
    }).catch(() => null);
    const detail = capacity
      ? ` (${capacity.claimable} claimable of ${capacity.totalOnboarded} onboarded` +
        `${capacity.onlineOnly ? ", ONLINE-only because 'assume all online' is off" : ""})`
      : "";
    throw new NoPhoneAvailableError(
      `No free OTP phone for "${name}"${opts.corporateId ? ` / ${opts.corporateId}` : ""}${detail} — ` +
        `every claimable handset is leased to a running checkout. Wait, or add more under Employee Phones.`
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
  // leasedUntil is set to "just now" rather than cleared. The picker sorts by
  // leasedUntil ascending to take the handset idle longest — but a cleared
  // field sorts as null, every released phone tied at null, and the tie-break
  // on id handed DMPL001 every single lease (every ICICI order from 1 to 5 Oct
  // went through 87****1297 while 49 other handsets sat untouched). A lapsed
  // timestamp keeps the phone claimable (the filter accepts leasedUntil < now)
  // and makes "released earliest" what the sort actually sees; never-leased
  // phones (null) still sort first.
  const res = await EmployeePhone.updateOne(
    { userId, id: phoneId, leasedBy: runId },
    { $set: { leasedBy: null, leasedUntil: new Date(Date.now() - 1) } }
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
