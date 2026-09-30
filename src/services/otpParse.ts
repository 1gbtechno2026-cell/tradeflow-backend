/**
 * Pulling the OTP and the card's last 4 out of a bank SMS.
 *
 * Its own module because the set of banks grows: every provider phrases this
 * differently, so new formats arrive as data, and each one needs a regression
 * test rather than a tweak to a pattern that four other banks depend on.
 *
 * TWO RULES, both learned from real messages:
 *
 * 1. NEVER fall back to "the first 4-8 digit run". Real messages are full of
 *    digit runs that are not the OTP:
 *
 *      RuPay Card 817546****7845 Use OTP 620673      <- 817546 is the card BIN
 *      ... of INR 70900.00 on your ICICI Bank Card    <- 70900 is the amount
 *      Reference # ECOM_5276533398459828604           <- a 19-digit reference
 *      valid for 10 mins                              <- a duration
 *
 *    Pine Labs puts the OTP LAST, after the BIN, so a first-run fallback would
 *    confidently return 817546 and the bank would reject it. Every match here is
 *    anchored to an OTP keyword.
 *
 * 2. The OTP is a STRING, never a number. ICICI sends "059171"; parsed as a
 *    number that becomes 59171 and fails.
 */

/** How a bank might name a one-time password. Extend this, not the patterns. */
const OTP_WORD = String.raw`(?:O\.?T\.?P|One[\s-]*Time[\s-]*(?:Password|Passcode|Pass|Pin|PIN))`;

/**
 * `(?<!\d)` / `(?!\d)` matter more than they look. Without them a 4-8 digit
 * pattern happily matches an 8-digit slice out of the middle of
 * ECOM_5276533398459828604 and returns a plausible-looking wrong OTP.
 */
const D = String.raw`(?<!\d)(\d{4,8})(?!\d)`;

const OTP_PATTERNS: Array<{ why: string; re: RegExp }> = [
  {
    // "059171 is the OTP for...", "742161 is your OTP", "964589 is OTP for",
    // "667918 is the One Time Password (OTP) for..."
    why: "digits-then-keyword",
    re: new RegExp(String.raw`${D}\s+is\s+(?:the\s+|your\s+|ur\s+)?${OTP_WORD}`, "i"),
  },
  {
    // "Use OTP 620673", "OTP: 620673", "OTP is 620673", "OTP - 620673".
    //
    // The gap is deliberately tiny. Allowing even ~20 characters here breaks
    // IndusInd: "(OTP) for ECOMM txn of INR 152.00" would put the AMOUNT within
    // reach of the keyword and return 152.00's digits as the code.
    why: "keyword-then-digits",
    re: new RegExp(String.raw`${OTP_WORD}\s*(?:is|:|=|-|->)?\s*${D}`, "i"),
  },
  {
    // "OTP for txn ... is 620673" — keyword and digits separated by a clause,
    // but joined by an explicit "is", which is what makes it safe.
    why: "keyword-clause-is-digits",
    re: new RegExp(String.raw`${OTP_WORD}\b[^.\n]{0,60}?\bis\s+${D}`, "i"),
  },
];

/** The OTP, as sent. Null when no OTP keyword anchors a digit run — which is the
 *  correct answer for a delivery receipt or a marketing SMS. */
export function extractOtp(message: string): string | null {
  const text = String(message || "");
  for (const { re } of OTP_PATTERNS) {
    const hit = text.match(re);
    if (hit?.[1]) return hit[1];
  }
  return null;
}

/** Which pattern matched — for diagnosing a new bank's format without logging
 *  the code itself. */
export function explainOtpMatch(message: string): string | null {
  const text = String(message || "");
  for (const { why, re } of OTP_PATTERNS) {
    if (re.test(text)) return why;
  }
  return null;
}

const CARD_PATTERNS: Array<{ why: string; re: RegExp }> = [
  {
    // A masked PAN: "817546****7845", "4111 **** **** 1111", "XXXX1234".
    // FIRST, because it must beat the generic "card <digits>" rule below — that
    // one would read 817546****7845 as "8175" and be wrong by four digits.
    why: "masked-pan",
    re: /(?:\d[\d\s-]{2,}|[Xx*]{2,})[\s-]*[X*x]{2,}[\s-]*(\d{4})(?!\d)/,
  },
  {
    // "card ending with 8342", "Card ending 4158", "card no. 1234"
    why: "card-ending",
    re: /card\s+(?:no\.?|number|ending|end(?:ing)?\s+with|ending\s+in)\s*(?:with\s+|in\s+)?[X*x]*\s*(\d{4})(?!\d)/i,
  },
  {
    // "ending with 8342" / "ending 4158" without the word "card" adjacent.
    why: "bare-ending",
    re: /end(?:ing)?\s*(?:with|in)?\s*[X*x]*\s*(\d{4})(?!\d)/i,
  },
  {
    // "Card XX8002", "Credit Card 1190" — any card-ish word then the last 4.
    // LAST, as the loosest rule.
    why: "card-then-digits",
    re: /card\s*(?:no\.?|number)?\s*[X*x#]*\s*(\d{4})(?!\d)/i,
  },
];

/**
 * Last 4 of the card the SMS is about — the value that proves an arriving code
 * belongs to THIS order's card rather than another running one.
 */
export function extractCardLast4(message: string): string | null {
  const text = String(message || "");
  for (const { re } of CARD_PATTERNS) {
    const hit = text.match(re);
    if (hit?.[1]) return hit[1];
  }
  return null;
}

export function explainCardMatch(message: string): string | null {
  const text = String(message || "");
  for (const { why, re } of CARD_PATTERNS) {
    if (re.test(text)) return why;
  }
  return null;
}

/** Amount the bank says it is charging, e.g. "INR 70900.00" -> "70900.00".
 *  Lets a strategy refuse an OTP for a transaction that is not the one it is
 *  making — a cheap guard against authorising someone else's charge. */
export function extractAmount(message: string): string | null {
  const hit = String(message || "").match(/\b(?:INR|Rs\.?|₹)\s*([\d,]+(?:\.\d{1,2})?)/i);
  return hit ? hit[1].replace(/,/g, "") : null;
}
