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
// TODO: ICICI page selectors. Everything above is done.
// ---------------------------------------------------------------------------

/** TODO: type the Corporate ID and pick the Employee ID on ICICI's page. */
async function selectCorporateIdentity(
  _ctx: PaymentContext,
  _corporateId: string,
  _employeeId: string
): Promise<void> {
  throw new Error("iciciCorporate.selectCorporateIdentity not implemented — run with dryRun:true");
}

/** TODO: press the button that makes ICICI dispatch the OTP. */
async function submitForOtp(_ctx: PaymentContext): Promise<void> {
  throw new Error("iciciCorporate.submitForOtp not implemented");
}

/** TODO: type the code and confirm. Never log it. */
async function enterOtp(_ctx: PaymentContext, _otp: string): Promise<void> {
  throw new Error("iciciCorporate.enterOtp not implemented");
}
