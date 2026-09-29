import { parentCardLast4, type PaymentContext, type PaymentResult, type PaymentStrategy } from "./types.js";

/**
 * HDFC Virtual (HDFC_VIRTUAL) — the only card type that authenticates with a
 * password rather than an OTP.
 *
 * No SMS, so nothing to lease and nothing to correlate. The CSV still carries
 * expiry and CVV — the bank's form asks for those like any other card — and the
 * `password` column is what replaces the OTP step.
 *
 * Note this is one of only two types whose parent and child card numbers
 * differ (the other being ICICI Corp Virtual); the eleven physical cards repeat
 * the same number in both columns.
 */
export const hdfcVirtual: PaymentStrategy = {
  cardTypeName: "HDFC_VIRTUAL",
  supportedAuth: ["password"],
  requiresCorporateId: false,

  async authenticate(ctx: PaymentContext): Promise<PaymentResult> {
    const password = String(ctx.card.password || "");
    // Checked before any page work so a bad batch fails here, not at the bank
    // with the order already in the cart.
    if (!password) throw new Error("HDFC_VIRTUAL requires the password column");

    if (!ctx.dryRun) await enterPassword(ctx, password);
    const cardLast4 = parentCardLast4(ctx.card.parentCardNumber);
    ctx.log("info", `[pay] HDFC Virtual password submitted for ****${cardLast4}`);

    return {
      cardTypeName: this.cardTypeName,
      cardLast4,
      authType: "password",
      authenticatedAt: new Date(),
    };
  },
};

// ---------------------------------------------------------------------------
// TODO: HDFC Virtual page selectors. Everything else is done.
// ---------------------------------------------------------------------------

/** TODO: type the password and confirm. NEVER log it. */
async function enterPassword(_ctx: PaymentContext, _password: string): Promise<void> {
  throw new Error("HDFC_VIRTUAL.enterPassword not implemented — run with dryRun:true");
}
