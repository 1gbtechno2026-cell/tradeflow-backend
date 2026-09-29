import { parentCardLast4, type PaymentContext, type PaymentResult, type PaymentStrategy } from "./types.js";

/**
 * PIN-auth card (ICICI Physical).
 *
 * ICICI_PHYSICAL declares ["pin", "otp"], so the order form's Authentication
 * dropdown decides which is used. This handles the pin arm; the otp arm routes
 * to otpCard instead.
 */
export const pinCard: PaymentStrategy = {
  cardTypeName: "*PIN",
  supportedAuth: ["pin"],
  requiresCorporateId: false,

  async authenticate(ctx: PaymentContext): Promise<PaymentResult> {
    const pin = String(ctx.card.pin || "");
    if (!pin) throw new Error("pin is required for a pin-auth card");
    if (!ctx.dryRun) await enterPin(ctx, pin);
    ctx.log("info", `[pay] PIN submitted for ****${parentCardLast4(ctx.card.parentCardNumber)}`);
    return {
      cardTypeName: this.cardTypeName,
      cardLast4: parentCardLast4(ctx.card.parentCardNumber),
      authType: "pin",
      authenticatedAt: new Date(),
    };
  },
};

/** TODO: type the PIN and confirm. Never log it. */
async function enterPin(_ctx: PaymentContext, _pin: string): Promise<void> {
  throw new Error("pinCard.enterPin not implemented — run with dryRun:true");
}
