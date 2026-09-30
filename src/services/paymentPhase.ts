import type { Page } from "playwright";
import { authenticatePayment, UnsupportedPaymentError } from "../paymentStrategies/index.js";
import type { AuthType, CardDetails, PaymentResult } from "../paymentStrategies/types.js";
import { CheckoutFailure } from "./checkoutErrors.js";
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
}

export type PaymentPhaseOutcome =
  /** Not a card order — nothing attempted, caller keeps today's behaviour. */
  | { attempted: false; reason: string }
  | { attempted: true; ok: true; result: PaymentResult; cardId: string; ordersOnCard: number }
  | { attempted: true; ok: false; failure: CheckoutFailure; cardId: string | null };

/** Card payment is the only mode with a bank hand-off. COD and wallet-only orders
 *  have no card, no credential and no shared pool, so they never come through
 *  here — which is also why COD can scale without any of this machinery. */
export function needsCardPayment(data: CheckoutJobData): boolean {
  const mode = String(data.paymentMode || "").toLowerCase();
  return mode === "card" || mode === "credit_card" || mode === "debit_card";
}

export async function runPaymentPhase(input: PaymentPhaseInput): Promise<PaymentPhaseOutcome> {
  const { data, log } = input;

  if (!needsCardPayment(data)) {
    return { attempted: false, reason: `payment_mode=${data.paymentMode || "(unset)"} needs no card` };
  }
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
    claimed = await acquireCard(data.batchId, cards);
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

  try {
    // ---- surface 1: Flipkart's own card form -----------------------------
    if (!input.dryRun) {
      await fillFlipkartCardForm(input.page, claimed.card, log);
      await waitForBankHandoff(input.page, log);
    } else {
      log("info", "[pay] dry run — Flipkart card form and bank hand-off skipped", "payment");
    }

    // ---- surface 2: the bank's page (the 14 strategies) -------------------
    const result = await authenticatePayment({
      page: input.page,
      runId: `${data.jobId}`,
      jobId: data.jobId,
      cardTypeName,
      card: claimed.card,
      authType,
      corporateId: data.corporateId ?? null,
      log: (level, message) => log(level, message, "payment"),
      dryRun: input.dryRun,
    });

    const ordersOnCard = await recordCardSuccess(data.batchId, id);
    log(
      "info",
      `[pay] authenticated ${result.cardTypeName} (${result.authType}) on ****${result.cardLast4}` +
        `${result.employeeId ? ` via ${result.employeeId}` : ""}`,
      "payment"
    );
    return { attempted: true, ok: true, result, cardId: id, ordersOnCard };
  } catch (err) {
    const failure =
      err instanceof CheckoutFailure
        ? err
        : err instanceof UnsupportedPaymentError
          ? new CheckoutFailure("CARD_AUTH_FAILED", err.message)
          : new CheckoutFailure("CARD_AUTH_FAILED", err instanceof Error ? err.message : String(err));

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
    return { attempted: true, ok: false, failure, cardId: id };
  }
}

// ---------------------------------------------------------------------------
// TODO(selector): Flipkart's payments page. A different page from any bank's.
//
// Needs: the "Credit/Debit Card" option, the PAN / expiry month / expiry year /
// CVV inputs, and the Pay button — plus whatever Flipkart shows when a card is
// rejected before it ever reaches the bank.
//
// Until these land, a real (non-dry-run) card order stops here with a clear
// message rather than half-submitting a payment.
// ---------------------------------------------------------------------------

async function fillFlipkartCardForm(
  _page: Page,
  _card: CardDetails,
  _log: PaymentPhaseInput["log"]
): Promise<void> {
  throw new Error(
    "fillFlipkartCardForm not implemented — Flipkart payments-page selectors needed " +
      "(card option, PAN, expiry, CVV, Pay). Run with dry_run to exercise everything else."
  );
}

/** TODO(selector): wait for Flipkart to hand off to the bank's domain, so a
 *  strategy is never handed Flipkart's own page by mistake. */
async function waitForBankHandoff(_page: Page, _log: PaymentPhaseInput["log"]): Promise<void> {
  throw new Error("waitForBankHandoff not implemented — need the post-Pay redirect signal");
}

/** Never log a PAN beyond its last 4, nor a CVV/password/PIN at all. */
export function cardTag(card: CardDetails, index: number): string {
  return cardId(card, index);
}
