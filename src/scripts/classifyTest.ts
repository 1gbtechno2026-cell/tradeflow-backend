/**
 * Offline tests for the page-text classifier.
 *
 *   npm run test:classify
 *
 * No browser, no network, no database — every case is a real Flipkart string.
 * Add a sample here before touching classifyPageText, so a fix for one page
 * cannot quietly change the verdict on another.
 *
 * The samples come from the reference project's own test suite, plus the two
 * pages that produced real production failures.
 */
import { classifyPageText, classifyThrownMessage, CHECKOUT_ERRORS } from "../services/checkoutErrors.js";

type Expect = string | null;
interface Case {
  name: string;
  text: string;
  pin?: string;
  expect: Expect;
  /** Why this case exists, printed when it fails. */
  why?: string;
}

const CASES: Case[] = [
  // ---- the two that caused real production failures ----------------------
  {
    name: "pincode prompt is NOT a deliverability verdict",
    text: "Enter pincode to see if the product is in stock\nCheck",
    pin: "122017",
    expect: "PINCODE_NOT_SET",
    why:
      "was ITEM_NOT_DELIVERABLE, which has filterBatch:true — so a page state a retry " +
      "fixes marked the whole batch filtered and queued no retry",
  },
  {
    name: "enter delivery pincode (cart)",
    text: "My Cart\nEnter Delivery Pincode\nYour cart is empty!",
    pin: "122017",
    expect: "PINCODE_NOT_SET",
  },
  {
    name: "buyable PDP with another variant out of stock",
    text:
      "Selected Color: Purple\nOut of stock\nOut of stock\nBuy now\nAdd to cart\n" +
      "Samsung Galaxy A06 5G\n₹16,999",
    pin: "122016",
    expect: null,
    why: "stock words belong to other colour tiles; the page still offers Buy now",
  },

  // ---- genuine stock / deliverability ------------------------------------
  { name: "PDP sold out", text: "Apple iPhone 16\nSold Out\nThis item is currently out of stock", expect: "PRODUCT_UNAVAILABLE" },
  { name: "Notify Me only", text: "Apple iPhone 17\nComing Soon\nNotify Me", expect: "PRODUCT_UNAVAILABLE" },
  {
    name: "out of stock FOR a pincode",
    text: "Currently out of stock for 122016",
    pin: "122016",
    expect: "PRODUCT_NOT_SERVICEABLE",
  },
  {
    name: "seller does not deliver",
    text: "My Cart\nSeller does not deliver to 122016\nPlace Order",
    pin: "122016",
    expect: "ITEM_NOT_DELIVERABLE",
  },
  {
    name: "checkout not deliverable",
    text: "1 item is not deliverable to 122016. Please try changing the address.",
    pin: "122016",
    expect: "ITEM_NOT_DELIVERABLE",
  },
  { name: "not deliverable at your location", text: "Delivery details\nNot deliverable at your location\nSeller: RetailNet", expect: "ITEM_NOT_DELIVERABLE" },

  // ---- other real pages ---------------------------------------------------
  { name: "max units reached", text: "You've reached the maximum units allowed for this product", expect: "MAX_UNITS_REACHED" },
  { name: "GST not applicable", text: "Note: GST and No cost EMI will not be applicable", expect: null, why: "a note, not a failure — the GST step decides, not the classifier" },
  { name: "unable to place order", text: "We are unable to place your order right now", expect: "UNABLE_TO_PLACE_ORDER" },

  // ---- pages that must NOT be flagged -------------------------------------
  {
    name: "normal checkout",
    text: "Deliver to:\nDashmobiles Pvt Ltd\nQty: 2\nGST Invoice\nChange\nContinue\nYou'll save ₹700 on this order!",
    expect: null,
  },
  {
    name: "normal PDP",
    text: "Apple iPhone 16 (Ultramarine, 256 GB)\n₹79,900\nEXPRESS Delivery by tomorrow\nBuy with EMI\nBuy now",
    expect: null,
  },
  {
    name: "real payments page",
    text:
      "Step 3 of 3 Payments 100% Secure\nCash on Delivery\nDue to handling costs, a nominal fee of ₹7 will be charged\n" +
      "Place Order\nUPI\nCredit / Debit / ATM Card",
    expect: null,
  },
];

let failures = 0;
console.log("Page-text classifier\n");
for (const c of CASES) {
  const got = classifyPageText(c.text, c.pin || "")?.code ?? null;
  const pass = got === c.expect;
  if (!pass) {
    failures += 1;
    console.log(`  FAIL ${c.name}`);
    console.log(`       expected ${c.expect} got ${got}`);
    if (c.why) console.log(`       why this matters: ${c.why}`);
  } else {
    console.log(`  ok   ${c.name.padEnd(48)} -> ${String(got)}`);
  }
}

console.log("\nBatch-filtering — which codes may stop an ENTIRE batch");
for (const code of ["ITEM_NOT_DELIVERABLE", "PRODUCT_NOT_SERVICEABLE", "PINCODE_NOT_SET"] as const) {
  const def = CHECKOUT_ERRORS[code];
  console.log(`  ${code.padEnd(26)} filterBatch=${Boolean(def.filterBatch)}`);
}
if (CHECKOUT_ERRORS.PINCODE_NOT_SET.filterBatch) {
  failures += 1;
  console.log("  FAIL PINCODE_NOT_SET must NOT filter the batch — it is a retryable page state");
}

console.log("\nThrown-message mapping");
for (const [msg, want] of [
  ["Enter pincode to see if the product is in stock (122017)", "PINCODE_NOT_SET"],
  ["Session expired: Flipkart showed the login page on viewcart", "SESSION_EXPIRED"],
  ["Product not in viewcart", "ADD_TO_CART_FAILED"],
] as const) {
  const got = classifyThrownMessage(msg).code;
  const pass = got === want;
  if (!pass) failures += 1;
  console.log(`  ${pass ? "ok  " : "FAIL"} "${msg.slice(0, 44)}..." -> ${got}`);
}

console.log(failures === 0 ? "\nPASS — all cases\n" : `\nFAIL — ${failures} case(s)\n`);
process.exit(failures === 0 ? 0 : 1);
