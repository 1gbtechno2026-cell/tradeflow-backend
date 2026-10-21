import type { CardDetails } from "../paymentStrategies/types.js";

/**
 * The order form's card rows -> the shape the 14 strategies consume.
 *
 * Accepts snake_case and camelCase because the same body is posted by the Smart
 * Bulk Order form (snake_case, matching the reference payload) and by anything
 * driving the Trade Flow API directly (camelCase), exactly as
 * jobRequest.parseCreateJobBody already does for every other field.
 *
 * Digits are stripped from the numbers here, once, so no strategy has to think
 * about whether the operator's CSV had spaces or dashes in a PAN. Expiry, CVV,
 * password and PIN are passed through verbatim — a leading zero in "04" is
 * meaningful and a password may legitimately contain anything.
 */
function str(value: unknown): string {
  return value == null ? "" : String(value).trim();
}

function digits(value: unknown): string {
  return str(value).replace(/\D/g, "");
}

function pick(row: Record<string, unknown>, snake: string, camel: string): unknown {
  return row[snake] ?? row[camel];
}

export function toCardDetails(rows: Array<Record<string, unknown>> | undefined): CardDetails[] {
  if (!Array.isArray(rows)) return [];
  const out: CardDetails[] = [];
  for (const raw of rows) {
    const row = raw && typeof raw === "object" ? (raw as Record<string, unknown>) : {};
    const cardNumber = digits(pick(row, "card_number", "cardNumber"));
    // A row with no card number cannot pay for anything. The UI already reports
    // these (cardsCsv.validateCards keeps them precisely so they are reported
    // rather than silently dropped), so by the time a batch is submitted this
    // should be empty — it is a backstop, not the validation.
    if (!cardNumber) continue;
    out.push({
      name: str(pick(row, "name", "name")) || undefined,
      parentCardNumber: digits(pick(row, "parent_card_number", "parentCardNumber")),
      cardNumber,
      expiryMonth: str(pick(row, "expiry_month", "expiryMonth")),
      expiryYear: str(pick(row, "expiry_year", "expiryYear")),
      cvv: str(pick(row, "cvv", "cvv")),
      password: str(pick(row, "password", "password")) || undefined,
      pin: str(pick(row, "pin", "pin")) || undefined,
      otpPhoneNumber: digits(pick(row, "otp_phone_number", "otpPhoneNumber")) || undefined,
    });
  }
  return out;
}

/** Last 4 of a card, for logs. Never log any other part of a PAN, and never the
 *  CVV, password or PIN at all. */
export function cardLabel(card: CardDetails): string {
  const last4 = digits(card.cardNumber).slice(-4);
  return last4 ? `card ****${last4}` : "card (no number)";
}
