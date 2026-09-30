import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import type { BrowserContext, Page } from "playwright";
import { config } from "../config.js";
import { FlipkartCheckout, OutOfStockPincodeError } from "../automation/FlipkartCheckout.js";
import { FlipkartPayment } from "../automation/FlipkartPayment.js";
import {
  blockFlipkartLogout,
  desktopContext,
  flipkartLoginUrl,
  launchMobileBrowser,
  launchStealthBrowser,
  mobileContext,
  restoreFlipkartSession,
  sleep,
} from "./browser.js";
import { CheckoutFailure, classifyPageText, classifyThrownMessage } from "./checkoutErrors.js";
import { resolveAddress, resolveLoggedInSession } from "./sessionStore.js";
import { toCardDetails } from "./paymentCards.js";
import type { AuthType, CardDetails } from "../paymentStrategies/types.js";
import type { AddressDetails, LogLevel } from "../types.js";

/**
 * A checkout run you can watch, re-run and read the wreckage of.
 *
 * Exists because the production pipeline gives you a job log and nothing else, and
 * the 2026-09-30 COD failure was unreadable from that alone: it looked like a
 * selector problem and was actually the wrong browser. This captures a screenshot
 * and the page's visible text at every boundary, so "it broke" becomes "here is
 * the page it broke on".
 *
 * ── WHY TWO BROWSERS ──────────────────────────────────────────────────────────
 * FlipkartCheckout is m-site automation — #msite-bottomsheet, touchscreen.tap —
 * and Flipkart picks which site to serve from the User-Agent. Run it desktop and
 * every location/pincode check silently finds nothing, so the pincode is never set
 * and Flipkart drops the item server side. The reference flow this is ported from
 * splits the run for exactly that reason:
 *
 *   DESKTOP   account mobile, address book, empty cart
 *   MOBILE    product -> add to cart -> checkout -> /payments -> pay
 *
 * ── AUTH ──────────────────────────────────────────────────────────────────────
 * Sessions come from platformids via resolveLoggedInSession, exactly as the real
 * worker does, and a dead session fails fast. There is deliberately NO interactive
 * login: a harness that can log in would stop telling the truth about a pipeline
 * that cannot. Logout is blocked on every page, and cookies are never written back.
 */

export interface TestFlowConfig {
  /** Short name for the artifact directory, e.g. "happy" or "oos". */
  caseId: string;
  /** Email or ObjectId of an already-logged-in platform id. */
  platformId: string;
  productUrl: string;
  quantity: number;
  /** From the dashboard's addresses collection. */
  addressId?: number;
  gstId?: number;
  gstMandatory?: boolean;
  /** Overrides the address's own pincode, for testing deliverability quickly. */
  checkoutPincode?: string;
  deliverySlaDays?: number;
  paymentMode?: "cod" | "card" | "none";
  cardType?: string;
  authType?: AuthType;
  /** Raw CSV-shaped rows; converted with the same mapper the real route uses. */
  cards?: Array<Record<string, unknown>>;
  mobileDevice?: string;
  headless?: boolean;
  /**
   * How far to go. Defaults to `payments` — reaching the payment page and stopping,
   * which is what proves the flow without spending money.
   *   payments      stop once /payments is reached
   *   payment_probe  + read the page (isCodAvailable / card form presence), no clicks
   *   pay            + actually pay. Requires payLive, below.
   */
  stopAfter?: "payments" | "payment_probe" | "pay";
  /** Second switch for `stopAfter: "pay"`. Both must be set; one alone does nothing.
   *  Two switches because everything before this point is reversible and this is not. */
  payLive?: boolean;
  /** For the CLI summary: what this case is supposed to end as. */
  expect?: string;
}

export interface StepArtifact {
  step: string;
  at: string;
  url: string;
  screenshot: string | null;
  textFile: string | null;
  note?: string;
}

export interface TestFlowResult {
  runId: string;
  caseId: string;
  status: "success" | "failed";
  step: string;
  code?: string;
  reason?: string;
  /** Flipkart's own wording, when the page gave any. */
  flipkartMessage?: string;
  detail?: string;
  url: string;
  reachedPayments: boolean;
  artifacts: StepArtifact[];
  startedAt: string;
  finishedAt: string;
  seconds: number;
}

/** Codes that only say WHERE it broke — the page text usually says why. */
const GENERIC_CODES = new Set(["UNABLE_TO_PLACE_ORDER", "ADD_TO_CART_FAILED", "UNKNOWN"]);

export async function runTestFlow(cfg: TestFlowConfig): Promise<TestFlowResult> {
  const runId = randomUUID();
  const startedAt = new Date();
  const dir = path.resolve(config.testArtifactDir, `${cfg.caseId}-${startedAt.toISOString().replace(/[:.]/g, "-")}`);
  fs.mkdirSync(dir, { recursive: true });

  const artifacts: StepArtifact[] = [];
  let currentStep = "starting";
  let page: Page | null = null;
  let shotCount = 0;

  const log = (level: LogLevel, message: string) => console.log(`  [${level}] ${message}`);

  /** Screenshot + visible text for one boundary. Never throws: losing an artifact
   *  must not fail the run it was meant to explain. */
  const capture = async (step: string, note?: string): Promise<void> => {
    if (!page || page.isClosed()) return;
    const base = path.join(dir, `${String(++shotCount).padStart(2, "0")}-${step}`);
    let screenshot: string | null = null;
    let textFile: string | null = null;
    try {
      await page.screenshot({ path: `${base}.png`, fullPage: true, timeout: 15_000 });
      screenshot = `${base}.png`;
    } catch { /* a navigating page cannot be shot; not worth failing over */ }
    try {
      const text = String(await page.evaluate(() => document.body?.innerText || "").catch(() => ""));
      fs.writeFileSync(`${base}.txt`, `URL: ${page.url()}\nSTEP: ${step}\n\n${text}`, "utf8");
      textFile = `${base}.txt`;
    } catch { /* same */ }
    artifacts.push({ step, at: new Date().toISOString(), url: page.url(), screenshot, textFile, note });
  };

  const step = async (n: number, title: string) => {
    currentStep = `${n}. ${title}`;
    console.log(`\n========== STEP ${n}: ${title} ==========`);
  };

  const desktopBrowser = await launchStealthBrowser({ headless: cfg.headless ?? config.headless });
  let mobileBrowser: Awaited<ReturnType<typeof launchMobileBrowser>> | null = null;
  let reachedPayments = false;

  try {
    // ---- session, before any browser work is wasted -----------------------
    await step(0, "Resolve the logged-in session from platformids");
    const session = await resolveLoggedInSession(cfg.platformId);
    if (!session.ok) {
      // Fail fast and say so plainly: the harness cannot log in, by design.
      throw new CheckoutFailure(
        "SESSION_EXPIRED",
        `${session.email}: ${session.reason}. The harness never logs in — re-login this ID in the dashboard first.`
      );
    }
    console.log(`  session ok for ${session.email} (${session.cookies.length} cookies)`);

    const address: AddressDetails = await resolveAddress({
      addressId: cfg.addressId,
      gstId: cfg.gstId,
    });
    if (cfg.checkoutPincode) address.checkoutPincode = cfg.checkoutPincode;
    const pincode = (address.checkoutPincode || address.pincode || "").replace(/\D/g, "").slice(-6);
    const gstMandatory = cfg.gstMandatory ?? true;
    console.log(`  pin=${pincode} qty=${cfg.quantity} gst=${gstMandatory ? address.gstNumber || "(none!)" : "off"}`);

    // ================= DESKTOP LEG =================
    const desk: BrowserContext = await desktopContext(desktopBrowser);
    const deskPage = await desk.newPage();
    page = deskPage;
    await blockFlipkartLogout(deskPage);
    await restoreFlipkartSession(deskPage, session.cookies);

    await step(1, "Desktop: read the account's registered mobile");
    await deskPage.goto("https://www.flipkart.com/account", { waitUntil: "domcontentloaded", timeout: 30_000 });
    await sleep(1500);
    await capture("01-account");
    if (flipkartLoginUrl(deskPage.url())) {
      throw new CheckoutFailure("SESSION_EXPIRED", "Flipkart showed the login page on /account — this session is dead");
    }
    const deskFlow = new FlipkartCheckout(deskPage, cfg.productUrl, log);
    deskFlow.setCheckoutFlags(pincode, gstMandatory);
    const accountMobile = await deskFlow.fetchAccountMobile();
    if (accountMobile) {
      const normalised = accountMobile.replace(/\D/g, "").slice(-10);
      if (normalised) address.mobile = normalised;
      console.log(`  account mobile ends ${normalised.slice(-4)}`);
    } else {
      console.log("  WARNING: could not read the account mobile; using the config's");
    }

    await step(2, "Desktop: make sure the delivery address is on the account");
    await deskFlow.ensureAddressForAccount(address, accountMobile || address.mobile);
    await capture("02-addresses");

    await step(3, "Desktop: empty the cart");
    await deskFlow.emptyCart();
    await capture("03-cart-emptied");
    await desk.close().catch(() => {});

    // ================= MOBILE LEG =================
    await step(4, `Mobile: switch context (${cfg.mobileDevice || config.mobileDevice})`);
    mobileBrowser = await launchMobileBrowser({ headless: cfg.headless ?? config.headless });
    const mob = await mobileContext(mobileBrowser, cfg.mobileDevice);
    const mobPage = await mob.newPage();
    page = mobPage;
    await blockFlipkartLogout(mobPage);
    await restoreFlipkartSession(mobPage, session.cookies);
    mobPage.on("console", (msg) => {
      const t = msg.text();
      if (t.startsWith("[")) console.log(`    (page) ${t}`);
    });
    const ua = await mobPage.evaluate(() => navigator.userAgent);
    console.log(`  UA: ${ua.slice(0, 64)}`);
    if (!/Mobile|Android/i.test(ua)) {
      // Loud, because this is the failure that masqueraded as a selector bug: the
      // m-site selectors below cannot match without a mobile UA.
      throw new Error(`Mobile context is NOT mobile (ua="${ua}") — m-site selectors will not match`);
    }

    // GUARD 1: establish the m-site session before pasting a deep product link.
    // Without it Flipkart is being handed a product URL in a session it has not
    // set up, which is one way the cart silently drops the item.
    await step(5, "Mobile: fresh m-site session, then paste the product link");
    await mobPage.goto("https://www.flipkart.com/", { waitUntil: "domcontentloaded", timeout: 30_000 });
    await mobPage.reload({ waitUntil: "domcontentloaded", timeout: 30_000 }).catch(() => {});
    await mobPage.waitForLoadState("load", { timeout: 15_000 }).catch(() => {});
    await capture("04-msite-home");

    const flow = new FlipkartCheckout(mobPage, cfg.productUrl, log);
    flow.setCheckoutFlags(pincode, gstMandatory);
    await flow.navigateToProduct();
    const details = await flow.captureProductDetails();
    await capture("05-product", `${details.model} | ${details.colour} | ${details.amount}`);
    console.log(`  product: ${details.model || "?"} | ${details.colour || "-"} | ${details.amount || "?"}`);

    const blocker = await flow.detectCheckoutBlocker(pincode);
    if (blocker) throw blocker;

    await step(6, "Mobile: add to cart (the location sheet should appear here)");
    await flow.clickAddToCart(pincode);
    await capture("06-after-add");
    const oos = await flow.detectOutOfStockForPincode(pincode);
    if (oos.matched) {
      throw new OutOfStockPincodeError(oos.rawMessage || `Out of stock for ${pincode}`, pincode);
    }
    await flow.gotoViewCart();
    await capture("07-viewcart");
    await flow.ensureProductInCart(details, pincode);
    await capture("08-cart-confirmed");

    if (cfg.deliverySlaDays != null) {
      const sla = await flow.assertDeliverySla(cfg.deliverySlaDays, details, pincode);
      console.log(`  delivery: "${sla.text}" = ${sla.days}d (limit ${cfg.deliverySlaDays}d)`);
    }

    await step(7, "Mobile: Place Order → checkout");
    await flow.clickPlaceOrder();
    await capture("09-checkout");

    // GUARD 2: Flipkart answers "Something went wrong! E002" when checkout is
    // opened without a real Place Order behind it. Retrying from the cart clears it.
    for (let retry = 1; retry <= 2 && (await checkoutErrored(mobPage)); retry++) {
      console.log(`  checkout shows "Something went wrong" — back to cart and Place Order again (${retry}/2)`);
      await capture(`09-e002-retry-${retry}`);
      await flow.gotoViewCart();
      await flow.clickPlaceOrder();
    }
    if (await checkoutErrored(mobPage)) {
      throw new CheckoutFailure("UNABLE_TO_PLACE_ORDER", 'Checkout keeps showing "Something went wrong! E002" after Place Order');
    }

    // GUARD 3: refuse to pay for a product that is not the one we added. A
    // leftover checkout session from an earlier run looks completely normal.
    const checkoutText = String(await mobPage.evaluate(() => document.body?.innerText || "").catch(() => ""));
    const modelKey = details.model.slice(0, 24).toLowerCase();
    if (modelKey && !checkoutText.toLowerCase().includes(modelKey)) {
      throw new CheckoutFailure(
        "UNABLE_TO_PLACE_ORDER",
        `Checkout does not show "${details.model}" — refusing to continue with a different product`
      );
    }

    await step(8, `Mobile: qty=${cfg.quantity}, address, GST, then Continue`);
    await flow.verifyAddressOnOrderSummary(address, cfg.quantity, gstMandatory);
    await capture("10-payments");
    reachedPayments = /\/payments/i.test(mobPage.url());
    console.log(`  landed on ${mobPage.url()}${reachedPayments ? "  (payments)" : "  (NOT payments)"}`);

    // ================= PAYMENT =================
    const stopAfter = cfg.stopAfter || "payments";
    if (stopAfter !== "payments" && reachedPayments) {
      await step(9, `Payment surface (${stopAfter})`);
      const fk = new FlipkartPayment(mobPage, (lvl, msg) => log(lvl, msg));
      const mode = cfg.paymentMode || "none";

      if (mode === "cod") {
        const available = await fk.isCodAvailable();
        await capture("11-cod-row", `isCodAvailable() = ${available}`);
        console.log(`  isCodAvailable() = ${available}`);
        if (stopAfter === "pay") {
          if (!cfg.payLive) {
            console.log("  stopAfter=pay but payLive is not set — NOT placing the order");
          } else if (!available) {
            throw new CheckoutFailure("UNABLE_TO_PLACE_ORDER", "COD is not available for this cart/pincode");
          } else {
            await fk.payWithCod();
            const confirmation = await fk.waitForOrderConfirmation();
            await capture("12-confirmation", `order ${confirmation.orderId || "(no id)"}`);
            console.log(`  ORDER PLACED: ${confirmation.orderId || "(no id read)"} ${confirmation.amount}`);
          }
        }
      } else if (mode === "card") {
        const cards: CardDetails[] = toCardDetails(cfg.cards);
        if (!cards.length) throw new CheckoutFailure("CARD_AUTH_FAILED", "paymentMode=card but no usable card rows");
        await fk.selectCardPayment();
        await capture("11-card-form");
        if (stopAfter === "pay" && cfg.payLive) {
          await fk.fillCardForm(cards[0]);
          await capture("12-card-filled");
          const rejected = await fk.detectCardRejectedByFlipkart();
          if (rejected) throw rejected;
          console.log("  card form filled. Pay is NOT pressed by the harness — press it yourself to watch the bank page.");
        } else {
          console.log("  card form reached; not filled (set stopAfter=pay and payLive=true to fill it)");
        }
      }
    }

    const finishedAt = new Date();
    const result: TestFlowResult = {
      runId,
      caseId: cfg.caseId,
      status: "success",
      step: currentStep,
      url: mobPage.url(),
      reachedPayments,
      artifacts,
      startedAt: startedAt.toISOString(),
      finishedAt: finishedAt.toISOString(),
      seconds: Math.round((finishedAt.getTime() - startedAt.getTime()) / 1000),
    };
    writeResult(dir, result);
    return result;
  } catch (err) {
    await capture("99-failure");
    const result = await explainFailure(err, page, (cfg.checkoutPincode || "").slice(-6), {
      runId,
      caseId: cfg.caseId,
      step: currentStep,
      artifacts,
      startedAt,
      reachedPayments,
    });
    writeResult(dir, result);
    return result;
  } finally {
    // Leave the window up briefly when headed, so a failure can be looked at.
    if (!(cfg.headless ?? config.headless)) await sleep(Math.min(config.keepBrowserOpenMs, 20_000));
    await desktopBrowser.close().catch(() => {});
    await mobileBrowser?.close().catch(() => {});
  }
}

/** Flipkart's E002 page. "Deliver to" present means it is a real checkout, not the error. */
async function checkoutErrored(page: Page): Promise<boolean> {
  const text = String(await page.evaluate(() => document.body?.innerText || "").catch(() => ""));
  return /Something went wrong/i.test(text) && !/Deliver to/i.test(text);
}

/**
 * Turn whatever was thrown into a readable reason: Flipkart's own words when the
 * page said something, otherwise what the script hit.
 */
async function explainFailure(
  err: unknown,
  page: Page | null,
  pincode: string,
  meta: {
    runId: string;
    caseId: string;
    step: string;
    artifacts: StepArtifact[];
    startedAt: Date;
    reachedPayments: boolean;
  }
): Promise<TestFlowResult> {
  const detail = err instanceof Error ? err.message.split("\n")[0] : String(err);
  const open = page && !page.isClosed();
  const url = open ? page.url() : "";
  const text = open ? String(await page.evaluate(() => document.body?.innerText || "").catch(() => "")) : "";
  const finishedAt = new Date();

  const pack = (code: string, reason: string, flipkartMessage?: string): TestFlowResult => ({
    runId: meta.runId,
    caseId: meta.caseId,
    status: "failed",
    step: meta.step,
    code,
    reason,
    flipkartMessage,
    detail,
    url,
    reachedPayments: meta.reachedPayments,
    artifacts: meta.artifacts,
    startedAt: meta.startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
    seconds: Math.round((finishedAt.getTime() - meta.startedAt.getTime()) / 1000),
  });

  if (err instanceof OutOfStockPincodeError) {
    return pack("PRODUCT_NOT_SERVICEABLE", "Out of stock for this pincode", err.rawMessage);
  }
  // A specific CheckoutFailure already knows more than the page scan would.
  if (err instanceof CheckoutFailure && !GENERIC_CODES.has(err.code)) {
    return pack(err.code, err.display, err.details);
  }
  if (url && flipkartLoginUrl(url)) return pack("SESSION_EXPIRED", "Flipkart showed the login page");
  const fromPage = text ? classifyPageText(text, pincode) : null;
  if (fromPage) return pack(fromPage.code, fromPage.display, fromPage.details);
  if (err instanceof CheckoutFailure) return pack(err.code, err.display, err.details);
  const guessed = classifyThrownMessage(detail);
  return pack(guessed.code, guessed.display, guessed.details);
}

function writeResult(dir: string, result: TestFlowResult) {
  fs.writeFileSync(path.join(dir, "result.json"), JSON.stringify(result, null, 2), "utf8");
  console.log(`\n  artifacts: ${path.relative(process.cwd(), dir)}/`);
}
