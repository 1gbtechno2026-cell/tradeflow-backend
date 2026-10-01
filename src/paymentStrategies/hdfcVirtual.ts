import { CheckoutFailure, classifyBankText } from "../services/checkoutErrors.js";
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

// ── THE PAGE, as captured on 2026-10-01 (run card-2026-10-01T13-59-16) ──────
//
//   https://securehdfc-acs2ui-…hdfc.bank.in/v1/acs/services/browser/creq/…
//   HDFC's own ACS, no iframe around the form. <form id="authPasswordSet1">
//     Merchant Name / Date / Card Number 5321 XXXX XXXX 1722 / Amount ₹169.00
//     "Purchase Authentication" — tab <a class="tab-1 active">Static Password</a>
//     "Please enter your Master Card 3D Secure PIN … This information will not be
//      shared with the merchant."
//     <input type="password" id="staticPassword" name="passCode" placeholder="Enter Password Here">
//     <a class="btn cancel__btn" onclick="cancelStaticform2();">CANCEL</a>
//     <a class="btn primary__btn" onclick="authSubmit();">Submit</a>
//   "This page automatically time out after 2:58 seconds"
//   Submit runs the page's own authSubmit(), which hashes the password
//   (isOtpHashed=true, getHashedPWD) before posting — so it must be a real
//   click on that control, never a form.submit().
//   An OTP tab exists on the same page ("OTP to your registered mobile number
//   XXXXXX7656"); the static-password tab is the one this card uses.

const PASSWORD_INPUT = '#staticPassword, input[name="passCode"]';
const STATIC_TAB = "a.tab-1";
const SUBMIT_CONTROL = 'a.primary__btn, a[onclick*="authSubmit"], button:has-text("Submit")';
const PASSWORD_REJECTED =
  /(?:incorrect|invalid|wrong) (?:password|pin|credentials?)|(?:password|pin) (?:is )?(?:incorrect|invalid|wrong)|does not match|attempts? (?:left|remaining)|(?:card|account) (?:is )?(?:locked|blocked)|authentication (?:failed|unsuccessful)|cannot be blank/i;

function pageText(ctx: PaymentContext): Promise<string> {
  return ctx.page
    .evaluate(() => document.body?.innerText || "")
    .then((t) => String(t).replace(/ /g, " "))
    .catch(() => "");
}

/**
 * Type the password and confirm.
 *
 * Returning MEANS authenticated: the ACS has left the page — the redirect
 * back through the gateway to Flipkart — which is the only positive signal
 * this page gives. A rejection is named with HDFC's text; "attempts
 * remaining" is included verbatim as the one advance notice of a lockout,
 * and cardPool.verdictFor retires the card on a wrong-password detail (it is
 * the CSV row, so every later order would fail the same way). Insufficient
 * funds, if HDFC says so here, is INSUFFICIENT_BALANCE — pause, not retire.
 * The password itself is never logged.
 */
async function enterPassword(ctx: PaymentContext, password: string): Promise<void> {
  const box = ctx.page.locator(PASSWORD_INPUT).first();
  try {
    await box.waitFor({ state: "visible", timeout: 20_000 });
  } catch {
    // The OTP tab may be the one showing; the static-password tab is ours.
    const tab = ctx.page.locator(STATIC_TAB).first();
    if ((await tab.count()) > 0) await tab.click({ timeout: 5000 }).catch(() => undefined);
    try {
      await box.waitFor({ state: "visible", timeout: 10_000 });
    } catch {
      throw new CheckoutFailure(
        "UNABLE_TO_PLACE_ORDER",
        `HDFC's 3-D Secure page did not show the password field (on ${ctx.page.url().split("?")[0]})`
      );
    }
  }
  await box.fill(password, { timeout: 10_000 });
  const acsHost = new URL(ctx.page.url()).hostname;
  await ctx.page.locator(SUBMIT_CONTROL).first().click({ timeout: 10_000 });
  ctx.log("info", "[pay] HDFC Virtual: password submitted");

  const deadline = Date.now() + 90_000;
  while (Date.now() < deadline) {
    let host = "";
    try {
      host = new URL(ctx.page.url()).hostname;
    } catch {
      /* mid-navigation */
    }
    if (host && host !== acsHost) {
      ctx.log("info", `[pay] HDFC Virtual: authenticated — ACS handed back to ${host}`);
      return;
    }
    const text = await pageText(ctx);
    const bank = classifyBankText(text);
    if (bank && bank.code === "INSUFFICIENT_BALANCE") throw bank;
    const rejected = text
      .split(/\n+/)
      .map((l) => l.trim())
      .find((l) => l && l.length < 240 && PASSWORD_REJECTED.test(l));
    if (rejected) {
      throw new CheckoutFailure("CARD_AUTH_FAILED", `HDFC rejected the password: ${rejected}`);
    }
    await ctx.page.waitForTimeout(500);
  }
  throw new CheckoutFailure(
    "UNABLE_TO_PLACE_ORDER",
    `HDFC's page did not move on within 90s of submitting the password (still on ${acsHost})`
  );
}
