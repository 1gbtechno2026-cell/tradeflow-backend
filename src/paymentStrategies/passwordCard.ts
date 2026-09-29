import { parentCardLast4, type PaymentContext, type PaymentResult, type PaymentStrategy } from "./types.js";

/**
 * Password-auth card (HDFC Virtual).
 *
 * No SMS at all: the credential is the `password` column of the Cards CSV, so
 * there is no phone to lease and nothing to correlate. That is exactly why the
 * Cards CSV validation refuses a blank password for this card type — the
 * failure would otherwise surface at the bank page, after the order is already
 * in the cart.
 */
export const passwordCard: PaymentStrategy = {
  cardTypeName: "*PASSWORD",
  supportedAuth: ["password"],
  requiresCorporateId: false,

  async authenticate(ctx: PaymentContext): Promise<PaymentResult> {
    const password = String(ctx.card.password || "");
    if (!password) {
      throw new Error("password is required for a password-auth card");
    }
    if (!ctx.dryRun) await enterPassword(ctx, password);
    ctx.log("info", `[pay] password submitted for ****${parentCardLast4(ctx.card.parentCardNumber)}`);
    return {
      cardTypeName: this.cardTypeName,
      cardLast4: parentCardLast4(ctx.card.parentCardNumber),
      authType: "password",
      authenticatedAt: new Date(),
    };
  },
};

/** TODO: type the password and confirm. Never log it. */
async function enterPassword(_ctx: PaymentContext, _password: string): Promise<void> {
  throw new Error("passwordCard.enterPassword not implemented — run with dryRun:true");
}
