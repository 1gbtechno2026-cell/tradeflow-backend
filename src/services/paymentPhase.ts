import type { Page } from "playwright";
import { FlipkartPayment, type OrderConfirmation } from "../automation/FlipkartPayment.js";
import type { PaymentApiWatcher } from "../automation/PaymentApiWatcher.js";
import { config } from "../config.js";
import { authenticatePayment, UnsupportedPaymentError } from "../paymentStrategies/index.js";
import type { AuthType, CardDetails, PaymentResult } from "../paymentStrategies/types.js";
import { CheckoutFailure } from "./checkoutErrors.js";
import { NoPhoneAvailableError } from "./employeePhoneLease.js";
import { OtpCardMismatchError, OtpTimeoutError } from "./smsOtp.js";
import {
  acquireCard,
  cardId,
  NoCardAvailableError,
  recordCardSuccess,
  reportCardOutcome,
  verdictFor,
} from "./cardPool.js";
import type { CheckoutJobData, LogLevel } from "../types.js";

/**
 * The payment phase: everything after Flipkart's order summary.
 *
 * Kept out of checkoutRunner because the two answer different questions.
 * checkoutRunner owns "does this order exist and may it proceed" — batch budget,
 * session, stock, delivery SLA, cart total. This owns "did money move", which has
 * its own failure vocabulary, its own shared resource (the card pool) and its own
 * hand-off to a bank's page.
 *
 * THREE distinct page surfaces are involved, and confusing them is the main way
 * this goes wrong:
 *
 *   1. Flipkart's payments page   — pick "Credit/Debit Card", type PAN, expiry,
 *                                   CVV, press Pay.          <- fillFlipkartCardForm
 *   2. the bank's page            — password / OTP / PIN.     <- the 14 strategies
 *   3. Flipkart's confirmation    — did the order actually land.
 *
 * Only (2) was built. (1) and (3) need Flipkart's own selectors, which are a
 * different set from any bank's and are marked TODO below.
 */

export interface PaymentPhaseInput {
  page: Page;
  data: CheckoutJobData;
  log: (level: LogLevel, message: string, step?: string) => void;
  dryRun?: boolean;
  /** Payment-page API watcher, so a failure carries the gateway's status code. */
  paymentApi?: PaymentApiWatcher | null;
  /**
   * Awaited the instant before money can move (Pay pressed / COD placed). The
   * runner uses it to write paymentStartedAt durably, so a worker that dies
   * after this point is never re-run on the same order automatically.
   */
  onBeforePay?: () => Promise<void>;
}

export type PaymentPhaseOutcome =
  /** Nothing attempted — an unrecognised mode, so the caller keeps the old
   *  stop-at-payment behaviour rather than guessing. */
  | { attempted: false; reason: string }
  | {
      attempted: true;
      ok: true;
      /** Absent for COD: there is no card and no bank to authenticate with. */
      result: PaymentResult | null;
      cardId: string | null;
      ordersOnCard: number;
      confirmation: OrderConfirmation | null;
      cardUsed: CardUsed | null;
    }
  | { attempted: true; ok: false; failure: CheckoutFailure; cardId: string | null; cardUsed: CardUsed | null };

/** Which card this order went on — what the Orders tab shows per order. Keys
 *  are the JobResultSnapshot field names, so the runner can spread it straight
 *  into the result (the first version said `name`, which the schema dropped). */
export interface CardUsed {
  cardName: string;
  parentCardLast4: string;
  childCardLast4: string;
}

function cardUsedOf(card: CardDetails): CardUsed {
  const last4 = (v: string) => String(v || "").replace(/\D/g, "").slice(-4);
  return { cardName: card.name || "", parentCardLast4: last4(card.parentCardNumber), childCardLast4: last4(card.cardNumber) };
}

export type PaymentModeClass = "cod" | "card" | "unsupported";

/**
 * Which of the four payment methods is this, as a concurrency class?
 *
 * The distinction that matters is not the bank, it is whether a shared resource is
 * needed:
 *   cod   — no card, no credential, no pool. Scales to whatever you will pay for.
 *   card  — needs a card from the pool; if auth_type is "otp" it ALSO needs a
 *           handset, which caps concurrency at the online phone count.
 * Batch-level, because card type and payment mode are singular for a whole batch —
 * so the queue an order belongs on is known at enqueue time.
 */
export function paymentModeClass(data: CheckoutJobData): PaymentModeClass {
  const mode = String(data.paymentMode || "").toLowerCase();
  if (mode === "cod" || mode === "cash_on_delivery") return "cod";
  if (mode === "card" || mode === "credit_card" || mode === "debit_card") return "card";
  return "unsupported";
}

export function needsCardPayment(data: CheckoutJobData): boolean {
  return paymentModeClass(data) === "card";
}

export async function runPaymentPhase(input: PaymentPhaseInput): Promise<PaymentPhaseOutcome> {
  const { data, log } = input;
  const mode = paymentModeClass(data);
  const fk = new FlipkartPayment(input.page, log);
  fk.api = input.paymentApi ?? null;

  if (mode === "unsupported") {
    return { attempted: false, reason: `payment_mode=${data.paymentMode || "(unset)"} is not handled here` };
  }

  // ---- Cash on Delivery -------------------------------------------------
  // No card, no credential, no pool, no bank. Every constraint the card paths
  // carry exists because of a shared resource COD does not touch.
  if (mode === "cod") {
    if (input.dryRun) {
      // Still ASK the page, because isCodAvailable only reads it — no click, no
      // order. Skipping it entirely would make a dry run prove the plumbing while
      // telling you nothing about whether the COD selectors actually match, which
      // is the one thing a first dry run is for. Failures here are reported, not
      // thrown: a selector that cannot find COD must not fail a dry run.
      try {
        const available = await fk.isCodAvailable();
        log("info", `[pay] dry run — isCodAvailable() says ${available}; nothing clicked`, "payment");
      } catch (err) {
        log("warn", `[pay] dry run — isCodAvailable() threw: ${err instanceof Error ? err.message : String(err)}`, "payment");
      }
      return { attempted: true, ok: true, result: null, cardId: null, ordersOnCard: 0, confirmation: null, cardUsed: null };
    }
    try {
      if (!(await fk.isCodAvailable())) {
        // Per account, product and pincode: a real outcome for THIS platform ID,
        // not a bug and not a reason to stop the batch.
        throw new CheckoutFailure("COD_UNAVAILABLE", "Cash on Delivery is unavailable for this cart on this account/pincode");
      }
      await input.onBeforePay?.();
      await fk.payWithCod();
      const confirmation = await fk.waitForOrderConfirmation();
      log("info", `[pay] COD order placed${confirmation.orderId ? ` — ${confirmation.orderId}` : ""}`, "payment");
      return { attempted: true, ok: true, result: null, cardId: null, ordersOnCard: 0, confirmation, cardUsed: null };
    } catch (err) {
      return { attempted: true, ok: false, failure: asFailure(err), cardId: null, cardUsed: null };
    }
  }

  // ---- Card -------------------------------------------------------------
  const cards = data.cards || [];
  if (!cards.length) {
    // Loud rather than a silent stop-at-payment: a card batch submitted with no
    // usable card rows is an operator mistake worth surfacing immediately.
    throw new CheckoutFailure("CARD_AUTH_FAILED", "payment_mode is card but no usable card rows were supplied");
  }
  const cardTypeName = String(data.cardType || "").toUpperCase();
  if (!cardTypeName) {
    throw new CheckoutFailure("CARD_AUTH_FAILED", "card_type is required to choose a payment strategy");
  }
  const authType = (data.authType || "").toLowerCase() as AuthType;
  if (!authType) {
    throw new CheckoutFailure("CARD_AUTH_FAILED", `auth_type is required for ${cardTypeName}`);
  }

  let claimed: Awaited<ReturnType<typeof acquireCard>>;
  try {
    claimed = await acquireCard(data.batchId, cards, data.cardMaxUsage);
  } catch (err) {
    if (err instanceof NoCardAvailableError) {
      // Every card is dead or paused. Not this order's fault and not retryable by
      // simply trying again — the batch is out of instruments.
      throw new CheckoutFailure("CARD_AUTH_FAILED", err.message);
    }
    throw err;
  }
  const id = claimed.id;
  log("info", `[pay] using card ${id} (${claimed.used} order(s) already placed on it)`, "payment");
  let payPressedAt = Date.now();

  try {
    // ---- surface 2: Flipkart's own payment page --------------------------
    if (!input.dryRun) {
      await fk.selectCardPayment();
      await fk.fillCardForm(claimed.card);
      // Check for a Flipkart-side refusal BEFORE pressing Pay, so an invalid card
      // is caught without a submission — and so its verdict is about the card
      // rather than about a hand-off that never happened.
      const rejected = await fk.detectCardRejectedByFlipkart();
      if (rejected) throw rejected;
      await input.onBeforePay?.();
      payPressedAt = Date.now();
      await fk.submitCardForm();
      const bankUrl = await fk.waitForBankHandoff();
      log("info", `[pay] handed off to the bank: ${bankUrl}`, "payment");
    } else {
      log("info", "[pay] dry run — Flipkart card form and bank hand-off skipped", "payment");
    }

    // ---- surface 3: the bank's page (the 14 strategies) -------------------
    const result = await authenticatePayment({
      page: input.page,
      runId: `${data.jobId}`,
      jobId: data.jobId,
      cardTypeName,
      card: claimed.card,
      authType,
      corporateId: data.corporateId ?? null,
      // One budget from the moment Pay was pressed: whatever the hand-off and
      // the bank's identity step used is gone; the OTP gets the remainder.
      otpTimeoutMs: remainingBudgetMs(payPressedAt),
      log: (level, message) => log(level, message, "payment"),
      dryRun: input.dryRun,
    });

    log(
      "info",
      `[pay] authenticated ${result.cardTypeName} (${result.authType}) on ****${result.cardLast4}` +
        `${result.employeeId ? ` via ${result.employeeId}` : ""}`,
      "payment"
    );

    // ---- surface 4: back on Flipkart -------------------------------------
    // A successful OTP is NOT a placed order. The bank can authorise and Flipkart
    // can still fail the order — stock going during the 3DS round trip is the
    // common one — and counting bank success as a purchase makes a batch under-buy
    // while reporting itself complete. So the card's success counter is only
    // incremented once Flipkart confirms.
    let confirmation: OrderConfirmation | null = null;
    if (!input.dryRun) {
      confirmation = await fk.waitForOrderConfirmation();
      log("info", `[pay] order confirmed${confirmation.orderId ? ` — ${confirmation.orderId}` : ""}`, "payment");
    }

    const ordersOnCard = await recordCardSuccess(data.batchId, id);
    return { attempted: true, ok: true, result, cardId: id, ordersOnCard, confirmation, cardUsed: cardUsedOf(claimed.card) };
  } catch (err) {
    const failure = asFailure(err);

    // What the gateway itself answered, on the job log: the only place the
    // reason behind "Payment Failed" is written down, and the thing to take
    // to Flipkart or the bank.
    const g = input.paymentApi?.lastGatewayResult() ?? null;
    if (g && g.phase === "post-bank") {
      log(
        "warn",
        `[pay] gateway said: ${g.responseStatus || "?"}${g.statusCode ? ` ${g.statusCode}` : ""}` +
          `${g.message ? ` "${g.message}"` : ""}${g.txnId ? ` txn ${g.txnId}` : ""}`,
        "payment"
      );
    } else if (g) {
      // paywithdetails answers SUCCESS on every run — it is Flipkart accepting
      // the Pay press and sending the page to the bank, not a captured payment.
      // Logged as such, so "SUCCESS txn PZT…" on a failed job is never read as
      // money having moved (it was, on 2026-10-04).
      log(
        "warn",
        `[pay] gateway accepted the Pay request (${g.responseStatus || "?"}${g.txnId ? `, txn ${g.txnId}` : ""}) — ` +
          "no post-bank verdict was seen: the bank never handed back",
        "payment"
      );
    }

    // Decide what this says about the CARD, as opposed to about the order. Only a
    // verdict of dead/paused removes it from the pool; anything ambiguous keeps
    // it, because wrongly retiring a card costs the whole batch a card while
    // wrongly keeping one costs a single attempt.
    const verdict = verdictFor(failure.code, failure.details);
    await reportCardOutcome(data.batchId, id, verdict).catch(() => undefined);
    log(
      verdict.kind === "keep" ? "warn" : "error",
      `[pay] card ${id} -> ${verdict.kind}: ${verdict.reason}`,
      "payment"
    );
    return { attempted: true, ok: false, failure, cardId: id, cardUsed: cardUsedOf(claimed.card) };
  }
}

/**
 * What is left of the payment budget (config.otpTimeoutMs) since Pay was
 * pressed. Never below 10s, so a slow hand-off still gives the bank a chance
 * to deliver rather than failing the order at the exact moment it is asked.
 */
export function remainingBudgetMs(payPressedAt: number): number {
  return Math.max(10_000, config.otpTimeoutMs - (Date.now() - payPressedAt));
}

/**
 * Anything thrown, as a CheckoutFailure.
 *
 * A bare Error becomes CARD_AUTH_FAILED with the message as detail, which
 * cardPool.verdictFor reads as "keep the card" — the deliberately cheap default.
 * Naming a real code in the page objects is what makes a card actually retire.
 */
function asFailure(err: unknown): CheckoutFailure {
  if (err instanceof CheckoutFailure) return err;
  if (err instanceof UnsupportedPaymentError) return new CheckoutFailure("CARD_AUTH_FAILED", err.message);
  // No code inside the 2-minute budget is its own outcome — the handset or the
  // bank's SMS, not the card. It has had a code since the catalog was written;
  // it was reaching the job as CARD_AUTH_FAILED because this mapping missed it.
  if (err instanceof OtpTimeoutError) return new CheckoutFailure("OTP_TIMEOUT", err.message);
  if (err instanceof OtpCardMismatchError) return new CheckoutFailure("OTP_CARD_MISMATCH", err.message);
  if (err instanceof NoPhoneAvailableError) return new CheckoutFailure("OTP_NOT_FOUND", err.message);
  return new CheckoutFailure("CARD_AUTH_FAILED", err instanceof Error ? err.message : String(err));
}

/** Never log a PAN beyond its last 4, nor a CVV/password/PIN at all. */
export function cardTag(card: CardDetails, index: number): string {
  return cardId(card, index);
}
