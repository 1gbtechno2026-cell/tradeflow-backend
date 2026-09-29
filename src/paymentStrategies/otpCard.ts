import { awaitOtpOnCsvPhone } from "./shared/otpEntry.js";
import { parentCardLast4, type PaymentContext, type PaymentResult, type PaymentStrategy } from "./types.js";

/**
 * Default OTP card — the code goes to the otp_phone_number in the Cards CSV.
 *
 * Covers every non-corporate OTP card type (AMEX_CORPORATE, CT_CARD, INDUSIND,
 * PINE, RBL, TIDE_EXPENSE_PREPAID, the AXIS physicals, HDFC_PARENT, HDFC_RUPAY).
 * They differ only in selectors, so they share this until one proves it needs
 * its own file — a new card type added in Cards Config works with no code
 * change at all.
 */
export const otpCard: PaymentStrategy = {
  cardTypeName: "*OTP",
  supportedAuth: ["otp"],
  requiresCorporateId: false,

  async authenticate(ctx: PaymentContext): Promise<PaymentResult> {
    await awaitOtpOnCsvPhone(ctx, {
      requestOtp: () => submitForOtp(ctx),
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

/** TODO: press the bank's "send OTP" control. */
async function submitForOtp(_ctx: PaymentContext): Promise<void> {
  throw new Error("otpCard.submitForOtp not implemented — run with dryRun:true");
}

/** TODO: type the code and confirm. Never log it. */
async function enterOtp(_ctx: PaymentContext, _otp: string): Promise<void> {
  throw new Error("otpCard.enterOtp not implemented");
}
