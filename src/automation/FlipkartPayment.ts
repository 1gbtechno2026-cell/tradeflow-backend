import type { Page } from "playwright";
import { CheckoutFailure } from "../services/checkoutErrors.js";
import type { CardDetails } from "../paymentStrategies/types.js";
import type { LogLevel } from "../types.js";

/**
 * FLIPKART's payment page. Not a bank's.
 *
 * This is the third surface in a card order and the one most easily confused with
 * the others:
 *
 *   1. Flipkart order summary   FlipkartCheckout.verifyAddressOnOrderSummary
 *   2. Flipkart PAYMENT page    ** THIS FILE **  — COD, card form, Pay button
 *   3. the bank's page          src/paymentStrategies/<card>.ts
 *   4. Flipkart confirmation    ** THIS FILE **  — did the order actually land
 *
 * Kept separate from FlipkartCheckout because that class is already ~3900 lines
 * about cart and address, and separate from the strategies because those are per
 * BANK while this is per PLATFORM: Flipkart ships payment-page changes on its own
 * schedule, and one edit here fixes every card type at once.
 *
 * ── WHAT TO FILL IN ────────────────────────────────────────────────────────────
 * Every method below throws NotImplemented with the exact selector it needs. Fill
 * them in; do not change the signatures or the order they are called in, because
 * paymentPhase.ts depends on both. The class is stateless apart from `page`, so
 * you can implement and test them one at a time.
 *
 * ── HOUSE RULES, all load-bearing ─────────────────────────────────────────────
 * • NEVER log card.cardNumber, card.cvv, card.password or card.pin. Last 4 only,
 *   via last4() below. Job logs are persisted to Mongo and shown in the UI.
 * • Throw CheckoutFailure, not a bare Error, whenever you can name the cause. The
 *   code you pick decides whether the CARD is retired from the pool
 *   (cardPool.verdictFor) and whether the order is retried — a bare Error becomes
 *   CARD_AUTH_FAILED with no detail, which is treated as "keep the card".
 * • Prefer text/role/label locators over generated class names. Flipkart's class
 *   names are hashed and rotate; the visible strings are far more stable.
 * • Every wait needs an explicit timeout. A default-timeout hang holds the batch
 *   slot, the phone lease and a Chrome for as long as the job lock allows.
 */

export type PaymentLogger = (level: LogLevel, message: string, step?: string) => void;

function last4(value: string): string {
  return String(value || "").replace(/\D/g, "").slice(-4);
}

function notImplemented(method: string, needs: string): never {
  throw new Error(
    `FlipkartPayment.${method} not implemented — needs: ${needs}. ` +
      `Run with PAYMENT_DRY_RUN=true to exercise everything around it.`
  );
}

export interface OrderConfirmation {
  /** Flipkart's order id, e.g. OD123456789012345678. Empty if not found. */
  orderId: string;
  /** What Flipkart says was charged, for reconciling against the cart total. */
  amount: string;
}

export class FlipkartPayment {
  constructor(
    private readonly page: Page,
    private readonly log: PaymentLogger
  ) {}

  // ───────────────────────────── Cash on Delivery ─────────────────────────────

  /**
   * Is COD offered for this cart at this pincode?
   *
   * COD eligibility is per product AND per pincode, and Flipkart often shows the
   * option greyed out with a reason rather than hiding it — so "the element
   * exists" is not the same as "COD is available". Check for the DISABLED state
   * too, or every ineligible order will proceed and fail at Place Order.
   *
   * NEEDS: the Cash on Delivery radio/label, and however Flipkart marks it
   *        unavailable (disabled attribute, a "Not available" note, greyed text).
   * RETURN: true only when it can actually be selected.
   */
  async isCodAvailable(): Promise<boolean> {
    notImplemented("isCodAvailable", "the COD option element + its disabled/unavailable state");
  }

  /**
   * Select COD and place the order. This is the whole payment for a COD order —
   * no card, no bank page, no OTP, which is why COD scales without any of that
   * machinery.
   *
   * Must be idempotent-safe in one respect: if it has already been clicked and
   * Flipkart is mid-navigation, do not click again. A double Place Order is the
   * one mistake here that costs real money.
   *
   * NEEDS: the COD option, the Place Order / Confirm Order button, and the
   *        post-click navigation or spinner to wait on.
   * THROW: CheckoutFailure("UNABLE_TO_PLACE_ORDER", <what the page said>) when the
   *        button is missing, disabled, or the click does not navigate.
   */
  async payWithCod(): Promise<void> {
    notImplemented("payWithCod", "COD option + Place Order button + the navigation it triggers");
  }

  // ──────────────────────────────── Card ──────────────────────────────────────

  /**
   * Choose the Credit/Debit Card payment method.
   *
   * Flipkart remembers saved cards on some accounts and opens on a saved-card
   * panel with only a CVV box. That is NOT the form we want — these are fresh
   * cards per batch. If a saved-card panel is present, click through to
   * "Add a new card" / "Use another card" first, or fillCardForm will type a PAN
   * into a CVV field.
   *
   * NEEDS: the Credit/Debit Card tab or radio, and the "add new card" escape from
   *        a saved-card panel if the account has one.
   * THROW: CheckoutFailure("UNABLE_TO_PLACE_ORDER", ...) if the method cannot be
   *        selected at all — that is a platform problem, not the card's fault, so
   *        the card must NOT be retired for it.
   */
  async selectCardPayment(): Promise<void> {
    notImplemented("selectCardPayment", "Credit/Debit Card option + 'add a new card' if a saved card is shown");
  }

  /**
   * Type the card into Flipkart's form. Do NOT press Pay here — submitCardForm
   * does that, separately, so a mistyped field can be caught before anything is
   * submitted.
   *
   * Which number goes in: card.cardNumber, the CHILD card. card.parentCardNumber
   * is not typed anywhere on this page — it exists because the bank's SMS names
   * the parent's last 4, and that is what verifies an arriving OTP. Typing the
   * parent here is a silent, expensive mistake: the form accepts it and the bank
   * declines later.
   *
   * Expiry: card.expiryMonth is "04", not "4" — the leading zero is preserved
   * deliberately upstream. If Flipkart wants a 2-digit year, slice
   * card.expiryYear ("2028" -> "28"); do not assume which it wants.
   *
   * NEEDS: PAN input, expiry month + year inputs (or a single MM/YY field, or two
   *        selects), CVV input. Note whether they are inside an iframe — card
   *        fields often are, and page.fill will not reach into one.
   * THROW: CheckoutFailure("CARD_AUTH_FAILED", ...) only if the FORM rejects the
   *        value it was given (e.g. an inline "invalid card number"), because that
   *        is genuinely about the card data and should retire the card. A missing
   *        field is a platform problem — use UNABLE_TO_PLACE_ORDER.
   */
  async fillCardForm(card: CardDetails): Promise<void> {
    this.log("info", `[fk-pay] filling card form for ****${last4(card.cardNumber)}`, "payment");
    notImplemented("fillCardForm", "PAN / expiry month / expiry year / CVV inputs (check for an iframe)");
  }

  /**
   * Press Pay. After this, money can move — so everything that could have been
   * validated should already have been.
   *
   * Do not wait for the bank here; waitForBankHandoff does that. Just confirm the
   * click was accepted, so a disabled or unresponsive button is distinguishable
   * from a slow bank.
   *
   * NEEDS: the Pay / Pay Now button and the immediate feedback it gives (spinner,
   *        button disabling, navigation starting).
   * THROW: CheckoutFailure("UNABLE_TO_PLACE_ORDER", ...) if the button is absent
   *        or stays disabled.
   */
  async submitCardForm(): Promise<void> {
    notImplemented("submitCardForm", "the Pay button + the feedback that proves the click landed");
  }

  /**
   * Wait until the page is the BANK's, and return the URL it landed on.
   *
   * This is the boundary the whole strategy split rests on: a strategy assumes it
   * has been handed the bank's page. Return too early and hdfcVirtual types a
   * password into Flipkart's DOM; the selector silently matches nothing, the OTP
   * never arrives, and the failure looks like a bank timeout.
   *
   * So decide on a POSITIVE signal, not merely "the URL changed". Flipkart's own
   * page can navigate several times before handing off. Good signals: the hostname
   * is no longer flipkart.com, or a known ACS/3-D Secure path appears.
   *
   * Also handle the case where Flipkart REJECTS the card before any bank is
   * involved — see detectCardRejectedByFlipkart, which should be checked while
   * waiting rather than after the timeout.
   *
   * NEEDS: how a hand-off looks for your cards. HDFC, ICICI, RBL, Pine and
   *        IndusInd will each land somewhere different, so match on "not
   *        flipkart.com" plus a payment-ish path rather than one bank's domain.
   * RETURN: the landed URL, for the job log.
   * THROW: CheckoutFailure("CARD_AUTH_FAILED", ...) on a Flipkart-side rejection;
   *        CheckoutFailure("UNABLE_TO_PLACE_ORDER", ...) if nothing happens at all.
   */
  async waitForBankHandoff(timeoutMs = 60_000): Promise<string> {
    void timeoutMs;
    notImplemented("waitForBankHandoff", "a positive 'we are on the bank now' signal (hostname and/or 3DS path)");
  }

  /**
   * Did Flipkart itself refuse the card, before the bank saw it?
   *
   * Worth its own method because the verdict differs: "invalid card number" is the
   * card's data and should retire it from the pool, whereas "payment method
   * temporarily unavailable" is Flipkart and must not.
   *
   * NEEDS: the inline error area on the card form, and the wording Flipkart uses.
   * RETURN: null when there is no rejection — the common case. Do NOT throw here;
   *         return the failure so the caller decides.
   */
  async detectCardRejectedByFlipkart(): Promise<CheckoutFailure | null> {
    void this.page;
    return null;
  }

  // ─────────────────────────── After the bank ─────────────────────────────────

  /**
   * Back on Flipkart after authentication: did the order actually land?
   *
   * A successful OTP is not a placed order. The bank can authorise and Flipkart
   * can still fail the order (stock gone during the 3DS round trip is the common
   * one), and a batch that counts bank success as purchase will under-buy while
   * reporting done.
   *
   * NEEDS: the confirmation page's order id and total, plus the FAILURE page's
   *        wording, because both are reached by the same redirect.
   * THROW: CheckoutFailure("UNABLE_TO_PLACE_ORDER", <what the page said>) when the
   *        redirect lands on a failure page. The card authenticated fine, so this
   *        must NOT retire the card.
   */
  async waitForOrderConfirmation(timeoutMs = 120_000): Promise<OrderConfirmation> {
    void timeoutMs;
    notImplemented("waitForOrderConfirmation", "the confirmation order id + total, AND the failure page wording");
  }
}
