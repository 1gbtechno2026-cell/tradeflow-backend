import { awaitOtpOnCsvPhone } from "./shared/otpEntry.js";
import { parentCardLast4, type PaymentContext, type PaymentResult, type PaymentStrategy } from "./types.js";

/**
 * AMEX Corporate (AMEX_CORPORATE) — OTP to the phone in the Cards CSV.
 *
 * Owns this bank's 3-D Secure page and nothing else. The OTP mechanics — stamp
 * the clock, wait, verify the code names this parent card, never log it — live
 * in shared/otpEntry.ts, so this file is only selectors.
 *
 * Carries a corporate identity and will be onboarded like ICICI Corp Virtual
 * later. Until its Corporate IDs exist the OTP goes to the CSV phone, so this
 * stays a plain OTP flow. When you onboard it: tick "Corporate card" in
 * Cards Config, add its Corporate IDs, then switch this file to
 * withLeasedPhone the way iciciCorporate.ts does.
 */
export const amexCorporate: PaymentStrategy = {
  cardTypeName: "AMEX_CORPORATE",
  supportedAuth: ["otp"],
  requiresCorporateId: false,

  async authenticate(ctx: PaymentContext): Promise<PaymentResult> {
    await awaitOtpOnCsvPhone(ctx, {
      requestOtp: () => requestOtp(ctx),
      submitOtp: (otp) => enterOtp(ctx, otp),
    });
    return {
      cardTypeName: this.cardTypeName,
      cardLast4: parentCardLast4(ctx.card.parentCardNumber),
      authType: "otp",
      phoneNumber: String(ctx.card.otpPhoneNumber || "").replace(/\D/g, ""),
      authenticatedAt: new Date(),
    };
  },
};

// ---------------------------------------------------------------------------
// TODO: AMEX Corporate page selectors. Everything else is done and tested.
// ---------------------------------------------------------------------------

/** TODO: press the control that makes AMEX Corporate send the OTP. */
async function requestOtp(_ctx: PaymentContext): Promise<void> {
  throw new Error("AMEX_CORPORATE.requestOtp not implemented — run with dryRun:true");
}

/** TODO: type the code and confirm. Never log it. */
async function enterOtp(_ctx: PaymentContext, _otp: string): Promise<void> {
  throw new Error("AMEX_CORPORATE.enterOtp not implemented");
}
