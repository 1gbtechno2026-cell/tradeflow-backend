/**
 * Regression tests for bank SMS parsing.
 *
 *   npx tsx src/scripts/otpParseTest.ts
 *
 * Every case below is a REAL message. When a new bank appears, paste its SMS in
 * here with the expected OTP and last 4 before touching otpParse.ts — the point
 * of the file is that adding a sixth bank cannot quietly break the first five.
 *
 * No network, no database, no Redis.
 */
import { extractOtp, extractCardLast4, extractAmount, explainOtpMatch, explainCardMatch } from "../services/otpParse.js";

interface Case {
  bank: string;
  otp: string | null;
  last4: string | null;
  amount: string | null;
  sms: string;
  /** Why this one is interesting — printed on failure. */
  trap?: string;
}

const CASES: Case[] = [
  {
    bank: "Pine Labs",
    otp: "620673",
    last4: "7845",
    amount: "152.00",
    trap: "OTP comes LAST, after a BIN (817546) that is itself a valid-looking 6-digit OTP",
    sms:
      "To authorise your transaction of INR 152.00 at FlipkartInternetPvtL using RuPay Card " +
      "817546****7845 Use OTP 620673 . Please do not share OTP with anyone for security reasons. - Pine Labs",
  },
  {
    bank: "ICICI",
    otp: "059171",
    last4: "8002",
    amount: "70900.00",
    trap: "LEADING ZERO — must stay a string; as a number it becomes 59171",
    sms:
      "059171 is the OTP for the transaction of INR 70900.00 on your ICICI Bank Card XX8002. " +
      "OTPs are SECRET. DO NOT disclose it to anyone. Bank NEVER asks for OTP.",
  },
  {
    bank: "Transcorp",
    otp: "742161",
    last4: "8342",
    amount: null,
    trap: "no amount at all; 'card ending with' phrasing; contains a URL",
    sms:
      "742161 is your OTP for your transaction on you Transcorp card ending with 8342. " +
      "Dont share the OTP with anyone. Visit transcorpint.com/cards if unauthorized.",
  },
  {
    bank: "RBL",
    otp: "964589",
    last4: "1190",
    amount: "152.00",
    trap: "bare 'Credit Card 1190' — no XX, no 'ending' keyword",
    sms:
      "964589 is OTP for txn of INR 152.00 at FLIPKART I on RBL Bank Credit Card 1190. " +
      "Valid for one time use. DO NOT SHARE IT WITH ANYONE.",
  },
  {
    bank: "IndusInd",
    otp: "667918",
    last4: "4158",
    amount: "152.00",
    trap:
      "'One Time Password (OTP)' spelled out, AND a 19-digit reference " +
      "(ECOM_5276533398459828604) that a loose digit pattern will match inside",
    sms:
      "667918 is the One Time Password (OTP) for ECOMM txn of INR 152.00 at Flipkart on Credit Card " +
      "ending 4158 Reference # ECOM_5276533398459828604.This OTP is usable only once and is valid " +
      "for 10 mins. PLS DO NOT SHARE THE OTP WITH ANYONE - IndusInd Bank",
  },

  // ---- negative cases: these must NOT yield an OTP -----------------------
  {
    bank: "(not an OTP) delivery",
    otp: null,
    last4: null,
    amount: null,
    trap: "no OTP keyword — a bare digit-run fallback would invent a code here",
    sms: "Your Flipkart order 1234567890 will be delivered today between 10 and 6. Track at fkrt.it/abc",
  },
  {
    bank: "(not an OTP) marketing",
    otp: null,
    last4: null,
    amount: "4999",
    trap: "has an amount and digits, still no OTP",
    sms: "Flat INR 4999 off on 5678 products this weekend only! Shop now.",
  },
];

let failures = 0;

function check(label: string, got: unknown, want: unknown, trap?: string) {
  const ok = got === want;
  if (!ok) {
    failures += 1;
    console.log(`  FAIL ${label}: got ${JSON.stringify(got)} want ${JSON.stringify(want)}`);
    if (trap) console.log(`       trap: ${trap}`);
  }
  return ok;
}

console.log("Bank SMS parsing\n");
for (const c of CASES) {
  const otp = extractOtp(c.sms);
  const last4 = extractCardLast4(c.sms);
  const amount = extractAmount(c.sms);
  const results = [
    check(`${c.bank} otp`, otp, c.otp, c.trap),
    check(`${c.bank} last4`, last4, c.last4, c.trap),
    check(`${c.bank} amount`, amount, c.amount, c.trap),
  ];
  const ok = results.every(Boolean);
  console.log(
    `${ok ? "  ok  " : "  XX  "} ${c.bank.padEnd(22)} otp=${String(otp).padEnd(8)} ` +
      `last4=${String(last4).padEnd(6)} amount=${String(amount).padEnd(9)} ` +
      `via ${explainOtpMatch(c.sms) || "-"} / ${explainCardMatch(c.sms) || "-"}`
  );
}

// The specific wrong answers a naive implementation gives, asserted explicitly so
// a future "simplification" back to a digit-run fallback fails loudly.
console.log("\nTraps, stated as assertions");
const pine = CASES[0].sms;
check("Pine Labs must not return the BIN as the OTP", extractOtp(pine) === "817546", false);
check("Pine Labs must not return the BIN's first 4 as last4", extractCardLast4(pine) === "8175", false);
const icici = CASES[1].sms;
check("ICICI must not return the amount as the OTP", extractOtp(icici) === "70900", false);
check("ICICI OTP keeps its leading zero", extractOtp(icici)?.startsWith("0"), true);
const indus = CASES[4].sms;
check("IndusInd must not slice the OTP out of the reference number", extractOtp(indus) === "52765333", false);
check("IndusInd must not return the duration as the OTP", extractOtp(indus) === "10", false);
console.log("  (silent = all held)");

console.log(failures === 0 ? "\nPASS — all cases\n" : `\nFAIL — ${failures} assertion(s)\n`);
process.exit(failures === 0 ? 0 : 1);
