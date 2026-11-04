import fs from "node:fs";
import path from "node:path";
import { connectDb } from "../db.js";
import { PlatformId } from "../models/PlatformId.js";
import { lockAccount } from "../services/accountLock.js";
import { listProxies, toPlaywrightProxy } from "../services/proxyPool.js";
import { resolveLoggedInSession, workspaceUserId } from "../services/sessionStore.js";
import { runHttpCheckout } from "../services/httpCheckout.js";
import { toCardDetails, cardLabel } from "../services/paymentCards.js";
import { seedCardPool } from "../services/cardPool.js";
import { runPaymentPhase } from "../services/paymentPhase.js";
import { FlipkartPayment } from "../automation/FlipkartPayment.js";
import { FlipkartApiWatcher } from "../automation/FlipkartApiWatcher.js";
import { PaymentApiWatcher } from "../automation/PaymentApiWatcher.js";
import { blockFlipkartLogout, launchMobileBrowser, mobileContext, restoreFlipkartSession } from "../services/browser.js";
import { config } from "../config.js";
import type { CardDetails } from "../paymentStrategies/types.js";
import type { AddressDetails, CheckoutJobData } from "../types.js";

/**
 * Test the HTTP card engine (High Speed + card) outside the queue.
 *
 * It runs the Flipkart side over HTTP exactly as the worker now does
 * (services/httpCheckout.ts, paymentMode "card"), then opens a browser at the
 * pay page and drives the payment surface — proving the token carries from the
 * HTTP session into a fresh browser, which is the one assumption of the hybrid.
 *
 *   npm run test:card-http -- --n 1 --cards ./hdfc.csv
 *       SMOKE (default): open the pay page, fill the card form with the CSV
 *       card, read any Flipkart-side rejection — and STOP before Pay. No money.
 *       Writes a screenshot per account to --out.
 *
 *   npm run test:card-http -- --n 5 --cards ./hdfc.csv --real 5
 *       REAL: places up to 5 real card orders (card + bank password + confirm).
 *       Spends real money on the card. Intended for the operator to run.
 *
 *   --skip 10          start further down the logged-in account list
 *   --email a@b.in     only this account (comma-separated for several)
 *   --product URL      what to buy (default: a ~₹200 keychain)
 *   --pincode 122017   --sla 15   --gst 1   (gst 1 = GST invoice mandatory)
 *   --direct           no proxy   --out ./debug/card-http
 *
 * Card data is read from the CSV and never logged beyond the last 4.
 */
const argv = process.argv.slice(2);
const arg = (k: string, d = "") => {
  const i = argv.indexOf(`--${k}`);
  return i >= 0 ? String(argv[i + 1] ?? "") : d;
};
const has = (k: string) => argv.includes(`--${k}`);
const N = Math.max(1, Number(arg("n", "1")) || 1);
const SKIP = Math.max(0, Number(arg("skip", "0")) || 0);
const REAL = Number(arg("real", "0")) || 0;
const EMAILS = arg("email").split(",").map((e) => e.trim().toLowerCase()).filter(Boolean);
const PRODUCT = arg(
  "product",
  "https://www.flipkart.com/kalakriti-creation-premium-keychain-key-ring-boys-motivation-bike-car-chain/p/itm2337a23e11fd9?pid=KECHSGHZCDNKS5RP&lid=LSTKECHSGHZCDNKS5RPH8MRGY&marketplace=FLIPKART"
);
const PINCODE = arg("pincode", "122017");
const SLA = Number(arg("sla", "15")) || 15;
const LIMIT = Number(arg("limit", "0")) || undefined;
const GST = has("gst") ? arg("gst", "1") !== "0" : false;
const DIRECT = has("direct");
const OUT = arg("out", "debug/card-http");
const CARDS_PATH = arg("cards", "");
const CARD_TYPE = arg("card-type", "HDFC_VIRTUAL").toUpperCase();

const ADDRESS: AddressDetails = {
  name: arg("name", "DASHMOBILES PRIVATE LIMITED"),
  mobile: "",
  pincode: PINCODE,
  locality: "UDYOG VIHAR",
  addressLine1: "Cabin 2, 581 , PHASE -5",
  city: "Gurugram",
  state: "Haryana",
  addressType: "Home",
  gstNumber: "06AALCD3578N1Z0",
  companyName: "DASHMOBILES PRIVATE LIMITED",
  checkoutPincode: "",
};

function loadCards(): CardDetails[] {
  if (!CARDS_PATH) throw new Error("--cards <path to CSV> is required (name,parent_card_number,card_number,expiry_month,expiry_year,cvv,password,pin,otp_phone_number)");
  const text = fs.readFileSync(CARDS_PATH, "utf8");
  const lines = text.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length < 2) throw new Error("CSV has no card rows");
  const header = lines[0].split(",").map((h) => h.trim());
  const rows = lines.slice(1).map((l) => {
    const cells = l.split(",");
    const row: Record<string, string> = {};
    header.forEach((h, i) => (row[h] = (cells[i] ?? "").trim()));
    return row;
  });
  return toCardDetails(rows);
}

async function main() {
  await connectDb();
  const userId = workspaceUserId();
  const cards = loadCards();
  if (!cards.length) throw new Error("no usable card rows in the CSV");
  fs.mkdirSync(OUT, { recursive: true });
  console.log(
    `card-http ${REAL ? `REAL ×${REAL}` : "SMOKE (no pay)"} · ${N} account(s) from offset ${SKIP} · ${cards.length} card(s) [${cards.map(cardLabel).join(", ")}] type ${CARD_TYPE} · gst ${GST ? "MANDATORY" : "off"} · pin ${PINCODE}`
  );

  const rows = await PlatformId.find(EMAILS.length ? { userId, email: { $in: EMAILS } } : { userId, status: "logged_in" })
    .sort({ _id: 1 })
    .skip(SKIP)
    .limit(N * 3)
    .select("email")
    .lean();
  const proxies = DIRECT ? [] : (await listProxies()).map(toPlaywrightProxy);

  const picked: Array<{ email: string; cookies: unknown[]; unlock: () => Promise<void> }> = [];
  for (const r of rows) {
    if (picked.length >= N) break;
    const s = await resolveLoggedInSession(String(r.email));
    if (!s.ok) continue;
    const unlock = await lockAccount(s.email, "card-http-test", 10 * 60_000);
    if (!unlock) continue;
    picked.push({ email: s.email, cookies: s.cookies, unlock });
  }
  console.log(`accounts ready: ${picked.length}`);

  // One synthetic batch for the card pool (seeded once; the real worker seeds per batch).
  const batchId = `cardtest-${Date.now()}`;
  await seedCardPool(batchId, cards);

  let realLeft = REAL;
  const log = (level: string, message: string, step?: string) => console.log(`    [${level}]${step ? ` [${step}]` : ""} ${message}`);

  for (let i = 0; i < picked.length; i++) {
    const acct = picked[i];
    const proxy = proxies.length ? proxies[i % proxies.length] : null;
    const real = realLeft > 0;
    console.log(`\n${i + 1}/${picked.length}  ${acct.email}${real ? "  [REAL ORDER]" : "  [smoke]"}`);
    try {
      const handoff = await runHttpCheckout({
        cookies: acct.cookies,
        proxy,
        paymentMode: "card",
        productUrl: PRODUCT,
        quantity: 1,
        pincode: PINCODE,
        address: ADDRESS,
        deliverySlaDays: SLA,
        cartAmountLimit: LIMIT,
        gstMandatory: GST,
        dryRun: false,
        log: (l, m, s) => log(l, m, s),
      });
      if (handoff.kind !== "card_handoff") {
        console.log(`  RESULT: ${handoff.kind}${handoff.kind === "failed" ? ` ${handoff.failure.code}: ${handoff.failure.details}` : handoff.kind === "fallback" ? `: ${handoff.reason}` : ""}`);
        console.log(`  (For GST-mandatory, a "fallback" here is correct — the worker then runs the full Chrome flow which ticks GST.)`);
        continue;
      }
      console.log(`  handoff OK: token carried, DC ${handoff.dc}, ₹${handoff.checkoutTotal ?? "?"}, SLA ${handoff.deliveryDays ?? "?"}d`);

      // Open the account's own session at the pay page — the worker's exact path.
      const mob = await launchMobileBrowser({ headless: config.headless, proxy: proxy ?? undefined });
      const ctx = await mobileContext(mob);
      new FlipkartApiWatcher().attach(ctx);
      const paymentApi = new PaymentApiWatcher().attach(ctx);
      const page = await ctx.newPage();
      await blockFlipkartLogout(page);
      await restoreFlipkartSession(page, acct.cookies);
      try {
        await page.goto(handoff.payPageUrl, { waitUntil: "domcontentloaded", timeout: 30_000 });
        const rendered = await page
          .getByText(/Credit\s*\/\s*Debit\s*\/\s*ATM Card/i)
          .first()
          .waitFor({ state: "visible", timeout: 20_000 })
          .then(() => true)
          .catch(() => false);
        await page.screenshot({ path: path.join(OUT, `${acct.email.replace(/[^a-z0-9]/gi, "_")}-paypage.png`) }).catch(() => undefined);
        if (!rendered) {
          console.log("  PAY PAGE: did NOT render the card option from the HTTP token — the hybrid would fall back to Chrome here.");
          continue;
        }
        console.log("  PAY PAGE: rendered from the HTTP token ✓ (card option present)");

        if (!real) {
          // Prove the card surface without paying: select card, fill the form,
          // read any inline rejection, then STOP before Pay.
          const fk = new FlipkartPayment(page, (l, m, s) => log(l, m, s));
          fk.api = paymentApi;
          await fk.selectCardPayment();
          await fk.fillCardForm(cards[0]);
          const rejected = await fk.detectCardRejectedByFlipkart();
          // Read back how many chars stuck in each field (lengths only — never the values).
          const len = async (sel: string) => (await page.locator(sel).first().inputValue({ timeout: 3000 }).catch(() => "")).replace(/\s/g, "").length;
          const panLen = await len("#cc-input");
          const expLen = await len('input[autocomplete="cc-exp"]');
          const cvvLen = await len("#cvv-input");
          const payText = await page.locator('form#cards button:has-text("Pay")').first().innerText({ timeout: 3000 }).catch(() => "?");
          await page.screenshot({ path: path.join(OUT, `${acct.email.replace(/[^a-z0-9]/gi, "_")}-cardform.png`) }).catch(() => undefined);
          console.log(`  FIELD READBACK: PAN ${panLen} digits, expiry ${expLen} chars, CVV ${cvvLen} chars, Pay button "${payText.replace(/\s+/g, " ").trim()}"`);
          if (rejected) console.log(`  CARD FORM: Flipkart rejected the card inline — ${rejected.details}`);
          else console.log("  CARD FORM: filled, no inline rejection ✓ (stopped before Pay — nothing charged)");
        } else {
          // Full real order through the existing payment phase.
          realLeft -= 1;
          const data = {
            jobId: `cardtest-${acct.email}-${Date.now()}`,
            batchId,
            email: acct.email,
            productUrl: PRODUCT,
            quantity: 1,
            totalQuantity: 1,
            quantityPerOrder: 1,
            totalAttempts: 1,
            deliverySlaDays: SLA,
            gstMandatory: GST,
            executionVersion: "high_speed",
            address: ADDRESS,
            paymentMode: "card",
            cardType: CARD_TYPE,
            authType: "password",
            cards,
          } as unknown as CheckoutJobData;
          const payment = await runPaymentPhase({ page, data, log: (l, m, s) => log(l, m, s), dryRun: false, paymentApi });
          if (payment.attempted && payment.ok) {
            console.log(`  REAL ORDER PLACED ✓ — ${payment.confirmation?.orderId || "(id pending)"} ${payment.confirmation?.amount || ""} on ${payment.cardUsed?.cardName || "card"}`);
          } else if (payment.attempted) {
            console.log(`  REAL ORDER FAILED — ${payment.failure.code}: ${payment.failure.details}`);
          } else {
            console.log(`  not attempted: ${payment.reason}`);
          }
          await page.screenshot({ path: path.join(OUT, `${acct.email.replace(/[^a-z0-9]/gi, "_")}-after.png`) }).catch(() => undefined);
        }
      } finally {
        await ctx.close().catch(() => undefined);
        await mob.close().catch(() => undefined);
      }
    } catch (err) {
      console.log(`  ERROR: ${err instanceof Error ? err.message.split("\n")[0] : String(err)}`);
    } finally {
      await acct.unlock().catch(() => undefined);
    }
  }
  console.log("\ndone.");
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
