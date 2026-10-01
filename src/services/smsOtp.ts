import mongoose, { Types } from "mongoose";
import { CardType } from "../models/CardType.js";
import { CheckoutJob } from "../models/CheckoutJob.js";
import { EmployeePhone } from "../models/EmployeePhone.js";
import { OtpMessage } from "../models/OtpMessage.js";
import { normalizePhone } from "../lib/phone.js";
import { getRedis } from "../lib/redis.js";
import { findRunHoldingPhone } from "./employeePhoneLease.js";
import { extractCardLast4, extractOtp } from "./otpParse.js";
import { sleep } from "./browser.js";
import { workspaceUserId } from "./sessionStore.js";

// Dash after Incoming/Outgoing can be a hyphen, en-dash or em-dash depending on the forwarder app.
const FORWARDER_PATTERN = /(?:Incoming|Outgoing)(?:\s+MMS)?\s*[-–—]\s*(.*?)\s*\(.*?Message:\s*:?\s*(.*)$/is;
// The device/SIM that received the SMS is in `subject` ("...at the number 8796723446") — that is
// the employee phone to match, not the bank sender parsed out of `message`.
// Digits may be grouped with spaces/dashes ("+91 87967 01297"); normalizePhone strips them.
const SUBJECT_PHONE_PATTERN = /number\s*(\+?\d[\d\s-]{6,}\d)/i;

const PHONE_OTP_TTL_SECONDS = 300;
const OTP_POLL_INTERVAL_MS = 2000;
const OTP_TIMEOUT_MS = 5 * 60 * 1000;

/**
 * Parsing lives in otpParse.ts, with a regression test per bank
 * (scripts/otpParseTest.ts). It moved out of here because the patterns that used
 * to sit inline got two of five real messages wrong: Pine Labs' masked PAN
 * ("817546****7845") yielded no card at all, and IndusInd's "One Time Password
 * (OTP)" yielded no OTP. Re-exported so existing callers are unaffected.
 */
export { extractOtp, extractCardLast4, extractAmount } from "./otpParse.js";

export class SmsPayloadError extends Error {}
export class OtpTimeoutError extends Error {}
export class OtpJobCancelledError extends Error {}

export interface ParsedSms {
  recipientNumber: string;
  senderNumber: string;
  message: string;
  otp: string | null;
  cardLast4: string | null;
  completeMessage: string;
}

function phoneKey(phone: string) {
  return `otp:phone:${phone}`;
}

function jobKey(jobId: string) {
  return `otp:job:${jobId}`;
}

/** Per-RUN mailbox. The phone key is SETEX — one slot per handset — so two jobs
 *  on one phone overwrite each other. This one cannot collide, because a lease
 *  gives each run its own key. */
function runKey(runId: string) {
  return `otp:run:${runId}`;
}


/**
 * Accepts `{ sender, message }` or the SMS-forwarder app shape
 * `{ message: "Incoming - <from> (SIM1)<br/>Message: <sms> (<date>)", subject: "...at the number <device>" }`.
 */
export function parseForwarderPayload(body: unknown): ParsedSms {
  const raw = (body && typeof body === "object" ? body : {}) as Record<string, unknown>;

  if (typeof raw.sender === "string" && typeof raw.message === "string") {
    return {
      recipientNumber: raw.sender,
      senderNumber: raw.sender,
      message: raw.message.trim(),
      otp: extractOtp(raw.message),
      cardLast4: extractCardLast4(raw.message),
      completeMessage: typeof raw.complete_message === "string" ? raw.complete_message : JSON.stringify(raw),
    };
  }

  const wrapped = typeof raw.message === "string" ? raw.message : "";
  const hit = wrapped.match(FORWARDER_PATTERN);
  if (!hit) {
    throw new SmsPayloadError('Unrecognized SMS payload shape: no Incoming/Outgoing "message" field found');
  }
  const senderNumber = hit[1].trim();
  const message = hit[2].trim();
  const subjectHit = typeof raw.subject === "string" ? raw.subject.match(SUBJECT_PHONE_PATTERN) : null;

  return {
    recipientNumber: subjectHit ? subjectHit[1] : senderNumber,
    senderNumber,
    message,
    otp: extractOtp(message),
    cardLast4: extractCardLast4(message),
    completeMessage: JSON.stringify(raw),
  };
}

function employeePhoneFilter(userId: string, phone: string) {
  return {
    userId: new Types.ObjectId(userId),
    isActive: { $ne: false },
    rawPhoneNumber: { $regex: `${phone}$` },
  };
}

/**
 * Records an incoming SMS. When it carries an OTP: publishes the phone-scoped Redis key (works for
 * any phone) and stamps lastOtp/lastOtpTime/lastCardLast4/ONLINE on the workspace's matching active
 * employeephones rows. The audit row and the employee update commit in one transaction.
 */
export async function recordIncomingSms(rawBody: unknown) {
  const parsed = parseForwarderPayload(rawBody);
  const phone = normalizePhone(parsed.recipientNumber);
  const receivedAt = new Date();
  const userId = workspaceUserId();

  // Which run is holding this handset? Only a lease can answer that — without
  // it the code can only be delivered phone-scoped, where a second SMS
  // overwrites the first.
  let lease: Awaited<ReturnType<typeof findRunHoldingPhone>> = null;
  if (parsed.otp && phone) {
    try {
      lease = await findRunHoldingPhone(phone);
    } catch (err) {
      console.warn(`[sms-otp] lease lookup failed for ${phone}: ${err instanceof Error ? err.message : err}`);
    }
  }

  if (parsed.otp && phone) {
    const payload = JSON.stringify({
      otp: parsed.otp,
      cardLast4: parsed.cardLast4,
      receivedAt: receivedAt.toISOString(),
    });
    try {
      const redis = getRedis();
      // Keep the phone key: it is the fallback when no lease is held (a manual
      // test, or an SMS arriving after the hold lapsed).
      await redis.setex(phoneKey(phone), PHONE_OTP_TTL_SECONDS, payload);
      if (lease) {
        // LPUSH + BLPOP, not BRPOP: BRPOP pops the tail, so on a retry the
        // waiter would receive the OLDEST code. Newest must win.
        await redis.lpush(runKey(lease.runId), payload);
        await redis.expire(runKey(lease.runId), PHONE_OTP_TTL_SECONDS);
      }
    } catch (err) {
      console.warn(`[sms-otp] Redis write failed for ${phone}: ${err instanceof Error ? err.message : err}`);
    }
  }

  let matched = false;
  const session = await mongoose.startSession();
  try {
    await session.withTransaction(async () => {
      matched = false;
      if (parsed.otp && phone) {
        const updated = await EmployeePhone.updateMany(
          employeePhoneFilter(userId, phone),
          {
            $set: {
              lastOtp: parsed.otp,
              lastOtpTime: receivedAt,
              lastCardLast4: parsed.cardLast4,
              connectionStatus: "ONLINE",
            },
          },
          { session }
        );
        matched = updated.matchedCount > 0;
      }
      await OtpMessage.create(
        [
          {
            sender: parsed.senderNumber || "unknown",
            recipient: phone,
            message: parsed.message || "(empty)",
            otp: parsed.otp,
            card_last4: parsed.cardLast4,
            matched,
            complete_message: parsed.completeMessage,
            received_at: receivedAt,
          },
        ],
        { session }
      );
    });
  } finally {
    await session.endSession();
  }

  // Never log the code itself — only the fields needed to trace a delivery.
  console.log(
    `[sms-otp] SMS to ${phone || "?"} otp=${parsed.otp ? "yes" : "no"} card=${parsed.cardLast4 || "-"} ` +
      `matched=${matched} run=${lease?.runId ? lease.runId.slice(0, 8) : "-"}`
  );
  if (parsed.otp && !matched) {
    console.warn(`[sms-otp] OTP for ${phone || parsed.recipientNumber} — no active employeephones row for this workspace`);
  }
  return { matched, receivedAt, hasOtp: Boolean(parsed.otp) };
}

export async function getLatestOtp() {
  const latest = await OtpMessage.findOne({ otp: { $ne: null } }).sort({ received_at: -1 }).lean();
  return latest
    ? { otp: latest.otp, cardLast4: latest.card_last4, phone: latest.recipient, receivedAt: latest.received_at }
    : { otp: null };
}

/**
 * Picks the active employee phone for a card type (cardtypes.cardTypeName, e.g. "ICICI_CORP_VIRTUAL").
 * Prefers ONLINE rows; pass employeeId to pin a specific employee.
 */
export async function resolveOtpPhone(cardTypeName: string, employeeId?: string) {
  const userId = new Types.ObjectId(workspaceUserId());
  const name = String(cardTypeName || "").trim().toUpperCase();
  const cardType = await CardType.findOne({ userId, cardTypeName: name }).lean();
  if (!cardType) {
    throw new Error(`Card type "${name}" not found in cardtypes for this workspace`);
  }
  const rows = await EmployeePhone.find({
    userId,
    cardTypeId: cardType.id,
    isActive: { $ne: false },
    ...(employeeId ? { employeeId } : {}),
  })
    .sort({ id: 1 })
    .lean();
  const row = rows.find((r) => r.connectionStatus === "ONLINE" && normalizePhone(r.rawPhoneNumber)) ||
    rows.find((r) => normalizePhone(r.rawPhoneNumber));
  if (!row) {
    throw new Error(`No active employee phone for card type "${name}". Add one under Employee Phones first.`);
  }
  return {
    phoneNumber: normalizePhone(row.rawPhoneNumber),
    corporateId: row.corporateId || "",
    employeeId: row.employeeId || "",
    cardTypeId: cardType.id,
    cardTypeName: cardType.cardTypeName,
  };
}

/**
 * Accepts a stored code only if it is provably this order's.
 *
 * Three factors, all cheap:
 *   receivedAt >= notBefore   a code that predates the submit is a leftover
 *   cardLast4 matches         the SMS names the parent card ("Card XX8002"),
 *                             so a stale code for a different card is rejected
 *                             instead of typed and blamed on the bank
 *   (the lease decides WHOSE code it is; these two prove it)
 */
function parseStoredOtp(raw: string | null, notBefore: Date, expectCardLast4?: string | null): string | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as { otp?: string; receivedAt?: string; cardLast4?: string | null };
    if (!parsed?.otp) return null;
    if (parsed.receivedAt && new Date(parsed.receivedAt).getTime() < notBefore.getTime()) return null;
    if (expectCardLast4 && parsed.cardLast4 && parsed.cardLast4 !== expectCardLast4) return null;
    return String(parsed.otp);
  } catch {
    // Legacy bare-string payload: no metadata to verify, so only usable when
    // the caller is not asking for card verification.
    return expectCardLast4 ? null : raw;
  }
}

/** Manual fallback: lets an operator hand an OTP to a waiting job. */
export async function setJobOtp(jobId: string, otp: string) {
  await getRedis().setex(
    jobKey(jobId),
    PHONE_OTP_TTL_SECONDS,
    JSON.stringify({ otp, receivedAt: new Date().toISOString() })
  );
}

/**
 * Waits for a payment OTP, checking every 2s in this order:
 *  1. Redis `otp:phone:<phone>` (SMS forwarder fast path)
 *  2. employeephones.lastOtp for that phone
 *  3. Redis `otp:job:<jobId>` (manual)
 * OTPs received before `notBefore` (default: when waiting started) are ignored, so a previous
 * transaction's OTP is never reused. Throws OtpJobCancelledError if the checkout job is cancelled.
 */
export async function waitForPaymentOtp(opts: {
  jobId: string;
  phoneNumber?: string | null;
  timeoutMs?: number;
  notBefore?: Date | null;
  /** runId holding the phone lease — enables the per-run mailbox, which cannot
   *  be overwritten by a second job on the same handset. */
  runId?: string | null;
  /** Last 4 of the parent card being paid with. When set, a code naming a
   *  different card is rejected rather than typed. */
  cardLast4?: string | null;
}): Promise<string> {
  const { jobId, timeoutMs = OTP_TIMEOUT_MS } = opts;
  const start = Date.now();
  const since = opts.notBefore || new Date(start);
  const phone = opts.phoneNumber ? normalizePhone(opts.phoneNumber) : "";
  const runId = opts.runId || "";
  const expectCard = opts.cardLast4 || null;
  const userId = workspaceUserId();
  const redis = getRedis();

  // The cancellation check is by Mongo id. The test harness waits with its run
  // id (a UUID) — findById on that threw "Cast to ObjectId failed" the moment
  // the wait began, so the first ICICI run requested the OTP, released the
  // handset and died before a single poll. No job id means nothing to cancel.
  const checkCancel = Types.ObjectId.isValid(jobId);

  while (Date.now() - start < timeoutMs) {
    if (checkCancel) {
      const job = await CheckoutJob.findById(jobId).select("status").lean();
      if (job?.status === "cancelled") throw new OtpJobCancelledError(`Job ${jobId} cancelled while waiting for OTP`);
    }

    // Per-run mailbox first. Non-blocking LPOP rather than BLPOP: a blocking
    // read would occupy this connection for its whole timeout, and the loop
    // still has to check for cancellation every couple of seconds.
    if (runId) {
      const fromRun = parseStoredOtp(await redis.lpop(runKey(runId)), since, expectCard);
      if (fromRun) return fromRun;
    }

    if (phone) {
      const fromRedis = parseStoredOtp(await redis.get(phoneKey(phone)), since, expectCard);
      if (fromRedis) return fromRedis;

      const row = await EmployeePhone.findOne({
        ...employeePhoneFilter(userId, phone),
        lastOtpTime: { $gte: since },
        lastOtp: { $nin: [null, ""] },
        // Must be card-verified like the Redis paths above. Without this the
        // DB fallback happily returns a code sent for a DIFFERENT parent card,
        // which is then typed and blamed on the bank.
        ...(expectCard ? { lastCardLast4: expectCard } : {}),
      })
        .sort({ lastOtpTime: -1 })
        .lean();
      if (row?.lastOtp) return String(row.lastOtp);
    }

    // Manual override is deliberately NOT card-verified: an operator handing a
    // code over has already decided it is the right one.
    const manual = parseStoredOtp(await redis.get(jobKey(jobId)), since);
    if (manual) return manual;

    await sleep(OTP_POLL_INTERVAL_MS);
  }

  throw new OtpTimeoutError(
    `OTP timeout: no OTP received within ${Math.round(timeoutMs / 1000)}s${phone ? ` for phone ${phone}` : ""}`
  );
}
