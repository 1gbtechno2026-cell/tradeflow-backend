import {
  leaseOtpPhone,
  releaseOtpPhone,
  renewOtpPhone,
  type LeasedOtpPhone,
} from "../../services/employeePhoneLease.js";
import { waitForPaymentOtp } from "../../services/smsOtp.js";
import { parentCardLast4, type PaymentContext } from "../types.js";

/**
 * OTP mechanics shared by every bank that authenticates with one.
 *
 * Extracted because the hard part is identical everywhere — lease a handset,
 * request the code, prove the code is ours, release — and only the selectors
 * differ. A new OTP bank should be a file of selectors, not a re-implementation
 * of correlation.
 *
 * Two shapes, because the phone comes from different places:
 *   corporate  -> an onboarded employee handset, claimed by lease
 *   otherwise  -> the otp_phone_number column of the Cards CSV
 */

const LEASE_RENEW_MS = 60_000;

export interface OtpRound {
  /** Do the bank-page work that causes the OTP to be sent. */
  requestOtp: () => Promise<void>;
  /** Type the code and confirm. Must never log it. */
  submitOtp: (otp: string) => Promise<void>;
}

/**
 * Corporate flow: claim an employee handset for this run, request, wait,
 * release. The lease is what lets the SMS webhook answer "which run does this
 * code belong to" — without it, two jobs on one handset overwrite each other's
 * code and each types the other's.
 */
export async function withLeasedPhone<T>(
  ctx: PaymentContext,
  cardTypeName: string,
  body: (lease: LeasedOtpPhone, cardLast4: string) => Promise<T>
): Promise<T> {
  if (!ctx.corporateId) {
    throw new Error(`${cardTypeName} is a corporate card — a Corporate ID is required`);
  }
  const cardLast4 = parentCardLast4(ctx.card.parentCardNumber);
  if (cardLast4.length !== 4) {
    throw new Error("parentCardNumber is required — its last 4 digits verify the incoming OTP");
  }

  // Claimed BEFORE anything is submitted. A bank can deliver in ~5s, and an SMS
  // arriving before the lease exists has no run to be attributed to.
  const lease = await leaseOtpPhone({
    cardTypeName,
    runId: ctx.runId,
    corporateId: ctx.corporateId,
    employeeId: ctx.employeeId || undefined,
  });
  ctx.log(
    "info",
    `[pay] leased ${lease.employeeId} (${lease.phoneNumber.slice(0, 2)}****${lease.phoneNumber.slice(-4)}) ` +
      `under ${lease.corporateId}`
  );

  // A payment page running longer than the lease TTL would otherwise let the
  // hold lapse mid-flight and hand the handset to another worker.
  const renew = setInterval(() => {
    void renewOtpPhone(lease.phoneId, ctx.runId).then((ok) => {
      if (!ok) ctx.log("warn", `[pay] lease lapsed and was re-taken — ${lease.employeeId}`);
    });
  }, LEASE_RENEW_MS);
  renew.unref?.();

  try {
    return await body(lease, cardLast4);
  } finally {
    clearInterval(renew);
    const released = await releaseOtpPhone(lease.phoneId, ctx.runId).catch(() => false);
    ctx.log("info", `[pay] ${released ? "released" : "lease already lapsed"} ${lease.employeeId}`);
  }
}

/**
 * Request the code, then wait for one that is provably this order's.
 *
 * submittedAt is stamped BEFORE requesting, so a code that predates the request
 * can never be mistaken for the answer to it. cardLast4 rejects a code sent for
 * a different parent card — which is what turns "probably right" into "provably
 * right" when several orders share a handset over time.
 */
export async function requestAndAwaitOtp(
  ctx: PaymentContext,
  round: OtpRound,
  opts: { phoneNumber: string; cardLast4: string }
): Promise<string> {
  const submittedAt = new Date();
  if (!ctx.dryRun) await round.requestOtp();
  ctx.log("info", `[pay] OTP requested for card ****${opts.cardLast4}`);

  const otp = await waitForPaymentOtp({
    jobId: ctx.jobId,
    runId: ctx.runId,
    phoneNumber: opts.phoneNumber,
    cardLast4: opts.cardLast4,
    notBefore: submittedAt,
    timeoutMs: ctx.otpTimeoutMs,
  });

  // Never log the code.
  ctx.log("info", `[pay] OTP received for ****${opts.cardLast4} (${otp.length} digits)`);
  if (!ctx.dryRun) await round.submitOtp(otp);
  return otp;
}

/**
 * Non-corporate flow: the handset is whatever the Cards CSV supplied, so there
 * is nothing to lease — that row is used by exactly one order at a time anyway.
 */
export async function awaitOtpOnCsvPhone(ctx: PaymentContext, round: OtpRound): Promise<string> {
  const phone = String(ctx.card.otpPhoneNumber || "").replace(/\D/g, "");
  if (phone.length !== 10) {
    throw new Error("otp_phone_number is required (10 digits) for a non-corporate OTP card");
  }
  const cardLast4 = parentCardLast4(ctx.card.parentCardNumber);
  return requestAndAwaitOtp(ctx, round, { phoneNumber: phone, cardLast4 });
}
