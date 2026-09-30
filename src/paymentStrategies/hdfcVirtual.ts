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
// SELECTORS — fill this in. Everything above is done.
//
// You are on HDFC's 3-D Secure page, handed over by
// FlipkartPayment.waitForBankHandoff. Flipkart's DOM is gone; nothing of
// Flipkart's should be looked for here.
//
// RULES
//  • NEVER log the password — not masked, not its length. ctx.log is persisted to
//    Mongo and shown in the UI. "[pay] password submitted" is the most to say.
//  • Throw CheckoutFailure, not a bare Error, when you can name the cause:
//      wrong password     CheckoutFailure("CARD_AUTH_FAILED", "<page text>") — and
//                         include the word "password" in the detail, because
//                         cardPool.verdictFor retires the card on it. That is
//                         correct: the password comes from the CSV row, so every
//                         later order on this card would fail identically.
//      field missing /
//      page never loaded  CheckoutFailure("UNABLE_TO_PLACE_ORDER", ...) so the card
//                         is NOT blamed for a bank-side outage.
//    A bare Error becomes CARD_AUTH_FAILED with no detail, which is read as "keep
//    the card" — safe, but it means a genuinely bad password is retried forever.
//  • Give every wait an explicit timeout. A hang holds the batch slot and a Chrome
//    for as long as the job lock allows.
//  • Returning normally MEANS "authenticated". If success cannot be confirmed from
//    the page, throw. Returning on an unknown state is the one mistake here that
//    silently reports purchases that never happened.
// ---------------------------------------------------------------------------

/**
 * Type the password and confirm.
 *
 * NEEDS
 *   - the password input
 *   - the submit button
 *   - a POSITIVE post-submit signal: HDFC's own success state, or the start of the
 *     redirect back to Flipkart
 *   - the wording HDFC uses for a wrong password, so it is distinguishable from a
 *     page that is merely slow
 *
 * WATCH FOR
 *   - an iframe. 3-D Secure pages very often are one, and page.fill will not reach
 *     inside; you will need frameLocator.
 *   - a "remaining attempts" warning. Worth putting in the failure detail: it is
 *     the only advance notice that a card is about to be locked out.
 */
async function enterPassword(_ctx: PaymentContext, _password: string): Promise<void> {
  throw new Error("HDFC_VIRTUAL.enterPassword not implemented — run with PAYMENT_DRY_RUN=true");
}
