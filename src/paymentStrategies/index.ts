import { iciciCorporate } from "./iciciCorporate.js";
import { otpCard } from "./otpCard.js";
import { passwordCard } from "./passwordCard.js";
import { pinCard } from "./pinCard.js";
import type { AuthType, PaymentContext, PaymentResult, PaymentStrategy } from "./types.js";

export * from "./types.js";
export { iciciCorporate, otpCard, passwordCard, pinCard };

/**
 * Routes a card type to the code that knows its bank page.
 *
 * Deliberately two-tier. Banks whose flow is genuinely unusual get a named
 * entry; everything else falls through to a generic strategy chosen by auth
 * type. That matters because card types are DATA — you add them in Cards
 * Config, not in code — so the common case must need no code change at all.
 * Today 13 of your 14 card types are ordinary; only ICICI_CORP_VIRTUAL needs
 * its own file, because it authenticates against a corporate identity rather
 * than a card alone.
 *
 * The key is cardtypes.card_type_name, which the order payload already carries
 * as `card_type` — so there is no second mapping table to keep in sync.
 */
const BY_CARD_TYPE = new Map<string, PaymentStrategy>([
  [iciciCorporate.cardTypeName, iciciCorporate],
  // AXIS_CORPORATE is is_corporate:true but has no onboarded Corporate IDs yet.
  // Give it a named entry when it does; until then it routes to otpCard and
  // fails loudly on the missing corporate identity rather than silently paying
  // with the wrong flow.
]);

const BY_AUTH: Record<AuthType, PaymentStrategy> = {
  otp: otpCard,
  password: passwordCard,
  pin: pinCard,
};

export class UnsupportedPaymentError extends Error {}

/**
 * @param cardTypeName cardtypes.card_type_name, e.g. "ICICI_CORP_VIRTUAL"
 * @param authType     the Authentication dropdown's value for this order
 */
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

/**
 * The single call the checkout makes once Flipkart has handed the page to the
 * bank. Everything before this is Flipkart's DOM; everything inside is the
 * bank's. Keeping that boundary is what lets a Flipkart UI change touch one
 * file and a bank UI change touch a different one.
 */
export async function authenticatePayment(
  ctx: PaymentContext & { cardTypeName: string }
): Promise<PaymentResult> {
  const strategy = strategyFor(ctx.cardTypeName, ctx.authType);
  if (strategy.requiresCorporateId && !ctx.corporateId) {
    throw new UnsupportedPaymentError(`${ctx.cardTypeName} requires a Corporate ID`);
  }
  ctx.log("info", `[pay] ${ctx.cardTypeName} via ${strategy.cardTypeName} (${ctx.authType})`);
  const result = await strategy.authenticate(ctx);
  // Report the card type the ORDER used, not the generic strategy's placeholder.
  return { ...result, cardTypeName: ctx.cardTypeName };
}
