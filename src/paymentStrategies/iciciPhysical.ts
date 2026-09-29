import { awaitOtpOnCsvPhone } from "./shared/otpEntry.js";
import { parentCardLast4, type PaymentContext, type PaymentResult, type PaymentStrategy } from "./types.js";

/**
 * ICICI Physical (ICICI_PHYSICAL) — the only card type with two auth arms.
 *
 * Its auth_types are ["pin", "otp"], so the order form's Authentication
 * dropdown decides which runs. Both live here rather than in two files because
 * they are the same bank and the same page — only the final credential differs,
 * and a page change would otherwise have to be made twice.
 *
 * Parent and child card numbers are the same for this card; only the two
 * "virtual" types differ.
 */
export const iciciPhysical: PaymentStrategy = {
  cardTypeName: "ICICI_PHYSICAL",
  supportedAuth: ["pin", "otp"],
  requiresCorporateId: false,

  async authenticate(ctx: PaymentContext): Promise<PaymentResult> {
    const cardLast4 = parentCardLast4(ctx.card.parentCardNumber);

    if (ctx.authType === "pin") {
      const pin = String(ctx.card.pin || "");
      if (!pin) throw new Error("ICICI_PHYSICAL with pin auth requires the pin column");
      if (!ctx.dryRun) await enterPin(ctx, pin);
      ctx.log("info", `[pay] ICICI Physical PIN submitted for ****${cardLast4}`);
      return { cardTypeName: this.cardTypeName, cardLast4, authType: "pin", authenticatedAt: new Date() };
    }

    await awaitOtpOnCsvPhone(ctx, {
      requestOtp: () => requestOtp(ctx),
      submitOtp: (otp) => enterOtp(ctx, otp),
    });
    return {
      cardTypeName: this.cardTypeName,
      cardLast4,
      authType: "otp",
      phoneNumber: String(ctx.card.otpPhoneNumber || "").replace(/\D/g, ""),
      authenticatedAt: new Date(),
    };
  },
};

// ---------------------------------------------------------------------------
// TODO: ICICI Physical page selectors. Everything else is done.
// ---------------------------------------------------------------------------

/** TODO: type the PIN and confirm. Never log it. */
async function enterPin(_ctx: PaymentContext, _pin: string): Promise<void> {
  throw new Error("ICICI_PHYSICAL.enterPin not implemented — run with dryRun:true");
}

/** TODO: press the control that makes ICICI send the OTP. */
async function requestOtp(_ctx: PaymentContext): Promise<void> {
  throw new Error("ICICI_PHYSICAL.requestOtp not implemented");
}

/** TODO: type the code and confirm. Never log it. */
async function enterOtp(_ctx: PaymentContext, _otp: string): Promise<void> {
  throw new Error("ICICI_PHYSICAL.enterOtp not implemented");
}
