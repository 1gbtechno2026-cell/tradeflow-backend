import { amexCorporate } from "./amexCorporate.js";
import { axisCorporate } from "./axisCorporate.js";
import { axisPhysicalMastercard } from "./axisPhysicalMastercard.js";
import { axisPhysicalVisa } from "./axisPhysicalVisa.js";
import { ctCard } from "./ctCard.js";
import { hdfcParent } from "./hdfcParent.js";
import { hdfcRupay } from "./hdfcRupay.js";
import { hdfcVirtual } from "./hdfcVirtual.js";
import { iciciCorporate } from "./iciciCorporate.js";
import { iciciPhysical } from "./iciciPhysical.js";
import { indusind } from "./indusind.js";
import { otpCard } from "./otpCard.js";
import { passwordCard } from "./passwordCard.js";
import { pinCard } from "./pinCard.js";
import { pine } from "./pine.js";
import { rbl } from "./rbl.js";
import { tideExpensePrepaid } from "./tideExpensePrepaid.js";
import type { AuthType, PaymentContext, PaymentResult, PaymentStrategy } from "./types.js";

export * from "./types.js";
export {
  amexCorporate, axisCorporate, axisPhysicalMastercard, axisPhysicalVisa, ctCard,
  hdfcParent, hdfcRupay, hdfcVirtual, iciciCorporate, iciciPhysical, indusind,
  pine, rbl, tideExpensePrepaid, otpCard, passwordCard, pinCard,
};

/**
 * Routes a card type to the file that owns its bank page.
 *
 * One file per card type, so a change to one bank's 3-D Secure page is a change
 * to one file — nothing else is touched, and nothing else needs re-testing.
 * The shared mechanics (lease, OTP correlation, card verification, release)
 * stay in shared/otpEntry.ts, so each of these files is only selectors: a bank
 * changing its page costs a few lines, not a re-implementation.
 *
 * Keyed on cardtypes.card_type_name, which the order payload already carries as
 * `card_type`, so there is no second mapping table to drift out of sync.
 */
const BY_CARD_TYPE = new Map<string, PaymentStrategy>(
  [
    amexCorporate, axisCorporate, axisPhysicalMastercard, axisPhysicalVisa, ctCard,
    hdfcParent, hdfcRupay, hdfcVirtual, iciciCorporate, iciciPhysical, indusind,
    pine, rbl, tideExpensePrepaid,
  ].map((s) => [s.cardTypeName, s])
);

/**
 * Fallbacks for a card type added in Cards Config that has no file yet. Card
 * types are DATA — you add them in the UI, not in code — so a new one must not
 * hard-fail before someone writes its selectors. It routes by auth type and
 * fails with a named error at the page step instead of at dispatch.
 */
const BY_AUTH: Record<AuthType, PaymentStrategy> = {
  otp: otpCard,
  password: passwordCard,
  pin: pinCard,
};

export class UnsupportedPaymentError extends Error {}

export function strategyFor(cardTypeName: string, authType: AuthType): PaymentStrategy {
  const name = String(cardTypeName || "").trim().toUpperCase();
  if (!name) throw new UnsupportedPaymentError("card_type is required to pick a payment strategy");

  const named = BY_CARD_TYPE.get(name);
  if (named) {
    if (!named.supportedAuth.includes(authType)) {
      throw new UnsupportedPaymentError(
        `${name} does not support "${authType}" (supports: ${named.supportedAuth.join(", ")})`
      );
    }
    return named;
  }

  const generic = BY_AUTH[authType];
  if (!generic) {
    throw new UnsupportedPaymentError(`No payment strategy for auth type "${authType}" (card ${name})`);
  }
  return generic;
}

/** Every card type that has its own file — for coverage checks against the DB. */
export function knownCardTypes(): string[] {
  return [...BY_CARD_TYPE.keys()].sort();
}

/**
 * The single call checkout makes once Flipkart has handed the page to the bank.
 * Everything before it is Flipkart's DOM; everything inside is the bank's.
 */
export async function authenticatePayment(
  ctx: PaymentContext & { cardTypeName: string }
): Promise<PaymentResult> {
  const strategy = strategyFor(ctx.cardTypeName, ctx.authType);
  if (strategy.requiresCorporateId && !ctx.corporateId) {
    throw new UnsupportedPaymentError(`${ctx.cardTypeName} requires a Corporate ID`);
  }
  ctx.log("info", `[pay] ${ctx.cardTypeName} (${ctx.authType})`);
  const result = await strategy.authenticate(ctx);
  return { ...result, cardTypeName: ctx.cardTypeName };
}
