import { requestAndAwaitOtp, withLeasedPhone } from "./shared/otpEntry.js";
import type { PaymentContext, PaymentResult, PaymentStrategy } from "./types.js";

/**
 * ICICI Corporate Virtual — 3-D Secure with a corporate identity.
 *
 * Unlike a consumer card, the OTP does not go to a number in the Cards CSV. The
 * bank page asks for a Corporate ID and an Employee ID, and sends the code to
 * the handset registered against that employee. That handset is claimed by
 * lease for the duration of this run, so the SMS webhook can attribute the
 * incoming code to the right order.
 *
 * SELECTORS ARE TODO. Everything around them — lease, correlation, card
 * verification, release — is finished and tested. The ordering inside
 * withLeasedPhone/requestAndAwaitOtp is load-bearing; fill in the four page
 * steps without moving them.
 */
export const iciciCorporate: PaymentStrategy = {
  cardTypeName: "ICICI_CORP_VIRTUAL",
  supportedAuth: ["otp"],
  requiresCorporateId: true,

  async authenticate(ctx: PaymentContext): Promise<PaymentResult> {
    return withLeasedPhone(ctx, this.cardTypeName, async (lease, cardLast4) => {
      // The employee whose handset we hold MUST be the one selected on the
      // page — selecting a different one sends the code to a phone nothing is
      // watching, and the order times out with no way to tell why.
      if (!ctx.dryRun) {
        await selectCorporateIdentity(ctx, lease.corporateId, lease.employeeId);
      }

      await requestAndAwaitOtp(
        ctx,
        {
          requestOtp: () => submitForOtp(ctx),
          submitOtp: (otp) => enterOtp(ctx, otp),
        },
        { phoneNumber: lease.phoneNumber, cardLast4 }
      );

      return {
        cardTypeName: this.cardTypeName,
        cardLast4,
        authType: "otp",
        employeeId: lease.employeeId,
        phoneNumber: lease.phoneNumber,
        authenticatedAt: new Date(),
      };
    });
  },
};

// ---------------------------------------------------------------------------
// SELECTORS — fill these three in. Everything above is done.
//
// You are on ICICI's page, handed over by FlipkartPayment.waitForBankHandoff.
//
// THE ORDER OF THE THREE IS LOAD-BEARING, and not for tidiness:
//
//   selectCorporateIdentity   must run BEFORE submitForOtp, because it is what
//                             decides WHICH handset the bank texts. The employee
//                             it selects is the one whose phone this run holds a
//                             lease on. Select a different employee and the code
//                             goes to a phone nothing is watching: the order waits
//                             out the full OTP window and fails as a timeout, with
//                             nothing in the logs pointing at the real cause.
//
//   submitForOtp              is called from inside requestAndAwaitOtp, which
//                             stamps `submittedAt` immediately BEFORE it. That
//                             timestamp is what lets an older code sitting in the
//                             mailbox be rejected. Do not request the OTP anywhere
//                             else, or that guarantee is gone.
//
//   enterOtp                  receives a code already proven to be this order's —
//                             it arrived after submittedAt AND the SMS named this
//                             parent card's last 4. Just type it.
//
// RULES
//  • NEVER log the OTP. requestAndAwaitOtp already logs its length and the card's
//    last 4, which is everything needed to debug without leaking the code.
//  • Throw CheckoutFailure, not a bare Error, when you can name the cause:
//      wrong/expired OTP        CheckoutFailure("CARD_AUTH_FAILED", "<page text>")
//      unknown corporate or
//      employee id             CheckoutFailure("CARD_AUTH_FAILED", ...) — include
//                              the id in the detail; it means the registry and the
//                              bank disagree, which is an onboarding problem.
//      OTP field never appeared CheckoutFailure("UNABLE_TO_PLACE_ORDER", ...) so
//                              the card is not blamed for a bank-side problem.
//      insufficient balance /
//      limit                   CheckoutFailure("INSUFFICIENT_BALANCE", ...) — this
//                              pauses the card until midnight IST instead of
//                              retiring it.
//  • Give every wait an explicit timeout. A hang here holds the batch slot, a
//    Chrome, AND the phone lease — the lease is the scarce one: with 67 online
//    handsets, one stuck order removes 1/67th of your OTP capacity until the 3-min
//    TTL lapses.
//  • Returning normally from enterOtp MEANS "authenticated". If success cannot be
//    confirmed from the page, throw.
// ---------------------------------------------------------------------------

/**
 * Type the Corporate ID and select the Employee ID.
 *
 * NEEDS
 *   - the Corporate ID input
 *   - the Employee ID input or dropdown. If it is a dropdown, match the option by
 *     its VALUE/text equal to `employeeId`, never by position — the lease picks an
 *     arbitrary free employee, so index 0 is almost never the right one.
 *   - confirmation that the identity was accepted (the OTP step becoming available)
 *
 * WATCH FOR
 *   - an iframe around the 3-D Secure form.
 *   - ICICI rejecting the employee id. That means employeephones and the bank
 *     disagree; put the id in the failure detail so it is fixable.
 */
async function selectCorporateIdentity(
  _ctx: PaymentContext,
  _corporateId: string,
  _employeeId: string
): Promise<void> {
  throw new Error("iciciCorporate.selectCorporateIdentity not implemented — run with PAYMENT_DRY_RUN=true");
}

/**
 * Press the button that makes ICICI dispatch the OTP.
 *
 * NEEDS
 *   - the send/generate OTP button
 *   - confirmation it was accepted (the OTP input appearing, a "code sent" notice,
 *     a resend timer starting)
 *
 * Return as soon as the request is accepted. Do NOT wait for the SMS here —
 * requestAndAwaitOtp owns that, and it is the only place that can prove a code
 * belongs to this order.
 */
async function submitForOtp(_ctx: PaymentContext): Promise<void> {
  throw new Error("iciciCorporate.submitForOtp not implemented");
}

/**
 * Type the code and confirm.
 *
 * NEEDS
 *   - the OTP input (sometimes several single-digit boxes — then fill them one
 *     character at a time rather than one fill() call)
 *   - the submit button
 *   - a POSITIVE success signal, or the redirect back to Flipkart
 *   - the wording for a rejected OTP, so it is not read as a slow page
 *
 * NEVER log the code.
 */
async function enterOtp(_ctx: PaymentContext, _otp: string): Promise<void> {
  throw new Error("iciciCorporate.enterOtp not implemented");
}
