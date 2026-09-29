import { awaitOtpOnCsvPhone } from "./shared/otpEntry.js";
import { parentCardLast4, type PaymentContext, type PaymentResult, type PaymentStrategy } from "./types.js";

/**
 * HDFC Rupay (HDFC_RUPAY) — OTP to the phone in the Cards CSV.
 *
 * Owns this bank's 3-D Secure page and nothing else. The OTP mechanics — stamp
 * the clock, wait, verify the code names this parent card, never log it — live
 * in shared/otpEntry.ts, so this file is only selectors.
 *
 */
export const hdfcRupay: PaymentStrategy = {
  cardTypeName: "HDFC_RUPAY",
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
// TODO: HDFC Rupay page selectors. Everything else is done and tested.
// ---------------------------------------------------------------------------

/** TODO: press the control that makes HDFC Rupay send the OTP. */
async function requestOtp(_ctx: PaymentContext): Promise<void> {
  throw new Error("HDFC_RUPAY.requestOtp not implemented — run with dryRun:true");
}

/** TODO: type the code and confirm. Never log it. */
async function enterOtp(_ctx: PaymentContext, _otp: string): Promise<void> {
  throw new Error("HDFC_RUPAY.enterOtp not implemented");
}
