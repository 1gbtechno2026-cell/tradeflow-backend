import { awaitOtpOnCsvPhone } from "./shared/otpEntry.js";
import { parentCardLast4, type PaymentContext, type PaymentResult, type PaymentStrategy } from "./types.js";

/**
 * CT Card (CT_CARD) — OTP to the phone in the Cards CSV.
 *
 * Owns this bank's 3-D Secure page and nothing else. The OTP mechanics — stamp
 * the clock, wait, verify the code names this parent card, never log it — live
 * in shared/otpEntry.ts, so this file is only selectors.
 *
 */
export const ctCard: PaymentStrategy = {
  cardTypeName: "CT_CARD",
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
// TODO: CT Card page selectors. Everything else is done and tested.
// ---------------------------------------------------------------------------

/** TODO: press the control that makes CT Card send the OTP. */
async function requestOtp(_ctx: PaymentContext): Promise<void> {
  throw new Error("CT_CARD.requestOtp not implemented — run with dryRun:true");
}

/** TODO: type the code and confirm. Never log it. */
async function enterOtp(_ctx: PaymentContext, _otp: string): Promise<void> {
  throw new Error("CT_CARD.enterOtp not implemented");
}
