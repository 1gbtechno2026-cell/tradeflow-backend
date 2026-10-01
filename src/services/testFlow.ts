import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import type { BrowserContext, Page } from "playwright";
import { config } from "../config.js";
import { FlipkartCheckout, OutOfStockPincodeError } from "../automation/FlipkartCheckout.js";
import { FlipkartApiWatcher } from "../automation/FlipkartApiWatcher.js";
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
import { CheckoutFailure, classifyBankText, classifyPageText, classifyThrownMessage } from "./checkoutErrors.js";
import { resolveAddress, resolveLoggedInSession } from "./sessionStore.js";
import { toCardDetails } from "./paymentCards.js";
import { verdictFor } from "./cardPool.js";
import { authenticatePayment } from "../paymentStrategies/index.js";
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
  /** Refuse the order if the CART total exceeds this. Checked before Place Order. */
  cartAmountLimit?: number;
  /**
   * Refuse to PAY if the amount the payment page is about to charge exceeds this.
   *
   * Enforced here for the first time. It has been parsed, validated and stored on
   * every job's request snapshot since the beginning and read by nothing — so a
   * batch whose price moved between the cart and the payment page had no backstop
   * at the only point where money actually moves.
   */
  finalAmountLimit?: number;
  paymentMode?: "cod" | "card" | "none";
  cardType?: string;
  authType?: AuthType;
  /** Required by corporate card types; the OTP goes to that corporate's handset. */
  corporateId?: string;
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
  /** True while the run is still going — the UI polls on this. */
  running?: boolean;
  /** Echoed back so a run can be re-run or understood later. Cards redacted. */
  config?: Record<string, unknown>;
  /** Everything the run printed, in order — the `[GST] …`, `[API] …` lines that
   *  say which branch ran. Server-side path; the API serves it by run id. */
  logFile?: string;
  /**
   * What the card pool would do with this card after this outcome — the same
   * `verdictFor` the worker applies, computed here so a test run shows it. The
   * harness has no pool, so nothing is actually paused or retired.
   */
  cardVerdict?: { kind: "keep" | "paused" | "dead"; reason: string; pausedUntil?: string };
}

/** Codes that only say WHERE it broke — the page text usually says why. */
const GENERIC_CODES = new Set(["UNABLE_TO_PLACE_ORDER", "ADD_TO_CART_FAILED", "UNKNOWN"]);

/**
 * In-flight runs, so the UI has something to show during the 40–160s a run takes.
 *
 * A run is far too slow to answer a request synchronously, so the tab starts one
 * and polls. Finished runs are served from their result.json on disk instead —
 * this map only needs to cover the window where that file is not final yet.
 */
const liveRuns = new Map<string, TestFlowResult>();

/**
 * One at a time. Two concurrent runs would share one Flipkart account and fight
 * over the same cart: run A empties it at step 3 while run B is adding to it, and
 * both fail for reasons neither log explains.
 */
let runningId: string | null = null;

export function currentRunId(): string | null {
  return runningId;
}

export function getLiveRun(runId: string): TestFlowResult | null {
  return liveRuns.get(runId) || null;
}

export class TestRunInProgressError extends Error {
  constructor(public readonly runId: string) {
    super(`A test run is already in progress (${runId}). Wait for it, or cancel it.`);
    this.name = "TestRunInProgressError";
  }
}

/**
 * Start a run and return its id immediately; the work continues in the background
 * and the caller polls getLiveRun / the artifact directory.
 */
export function startTestFlow(cfg: TestFlowConfig): string {
  if (runningId) throw new TestRunInProgressError(runningId);
  const runId = randomUUID();
  // Seeded before the promise starts, so a poll landing in the same tick as the
  // POST still finds the run rather than a 404.
  liveRuns.set(runId, {
    runId,
    caseId: cfg.caseId,
    status: "failed",
    step: "queued",
    url: "",
    reachedPayments: false,
    artifacts: [],
    startedAt: new Date().toISOString(),
    finishedAt: "",
    seconds: 0,
    running: true,
    config: redactConfig(cfg),
  });
  runningId = runId;
  void runTestFlow(cfg, runId)
    .catch(() => undefined)
    .finally(() => {
      runningId = null;
      // Keep the finished record around briefly; result.json is the durable copy.
      setTimeout(() => liveRuns.delete(runId), 10 * 60 * 1000).unref?.();
    });
  return runId;
}

/** Card numbers and CVVs must not reach the UI or result.json. */
function redactConfig(cfg: TestFlowConfig): Record<string, unknown> {
  const { cards, ...rest } = cfg;
  return { ...rest, cards: Array.isArray(cards) ? `${cards.length} card row(s) (redacted)` : undefined };
}

export async function runTestFlow(cfg: TestFlowConfig, existingRunId?: string): Promise<TestFlowResult> {
  const runId = existingRunId || randomUUID();
  const startedAt = new Date();
  const dir = path.resolve(config.testArtifactDir, `${cfg.caseId}-${startedAt.toISOString().replace(/[:.]/g, "-")}`);
  fs.mkdirSync(dir, { recursive: true });

  // Everything the run prints goes to run.log in its artifact folder. The
  // checkout classes write with console.log directly, so wrapping it is the only
  // way to see the `[GST] …` / `[API] …` lines that say which branch actually
  // ran — the screenshots show what the page looked like, not what the code
  // decided. One run at a time (startTestFlow enforces it), so the swap cannot
  // interleave two runs; restored in `finally`.
  const logFile = path.join(dir, "run.log");
  const originalLog = console.log;
  console.log = (...args: unknown[]) => {
    originalLog(...args);
    try {
      const line = args.map((a) => (typeof a === "string" ? a : String(a))).join(" ");
      // The harness runs inside the API process, whose request logger shares
      // console.log; the UI's 2s polling would otherwise outnumber the run's
      // own lines three to one.
      if (line.startsWith("[trade-flow]")) return;
      fs.appendFileSync(logFile, `${new Date().toISOString().slice(11, 23)} ${line}\n`);
    } catch { /* the log must never fail the run it describes */ }
  };

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
    try {
      // The DOM, not just the rendered text. Selector bugs are invisible in a
      // screenshot and unreproducible from innerText: the GST block failure on
      // 2026-10-01 was a question about which ANCESTOR contains which children,
      // which only the markup can answer. Saving it means a selector can be
      // re-tested offline against the real page instead of by re-running a
      // 40-second checkout each time.
      const html = String(await page.evaluate(() => document.documentElement.outerHTML).catch(() => ""));
      if (html) fs.writeFileSync(`${base}.html`, html, "utf8");
    } catch { /* same */ }
    artifacts.push({ step, at: new Date().toISOString(), url: page.url(), screenshot, textFile, note });
    publish();
  };

  /** Push the current state to the live map and result.json, so a poll mid-run
   *  sees the steps that have already happened instead of an empty page. */
  const publish = (final?: Partial<TestFlowResult>) => {
    const snapshot: TestFlowResult = {
      runId,
      caseId: cfg.caseId,
      status: "failed",
      step: currentStep,
      url: page && !page.isClosed() ? page.url() : "",
      reachedPayments,
      artifacts,
      startedAt: startedAt.toISOString(),
      finishedAt: "",
      seconds: Math.round((Date.now() - startedAt.getTime()) / 1000),
      running: true,
      config: redactConfig(cfg),
      ...final,
    };
    liveRuns.set(runId, snapshot);
    try {
      fs.writeFileSync(path.join(dir, "result.json"), JSON.stringify(snapshot, null, 2), "utf8");
    } catch { /* a run must not die because an artifact write failed */ }
  };

  const step = async (n: number, title: string) => {
    currentStep = `${n}. ${title}`;
    console.log(`\n========== STEP ${n}: ${title} ==========`);
    publish();
  };

  // One watcher for the whole run: the flow spans two browsers and the add-to-cart
  // answer arrives on the mobile leg while the address list arrives on the desktop one.
  const apiWatcher = new FlipkartApiWatcher();
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
    apiWatcher.attach(desk);
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
    deskFlow.api = apiWatcher;
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
    apiWatcher.attach(mob);
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
    flow.api = apiWatcher;
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

    // Cart total, before Place Order — the last point an order can be abandoned
    // for free.
    const payable = await flow.captureCartPayable();
    if (payable.amount) {
      console.log(`  cart payable ₹${payable.amount} (from ${payable.source})`);
      await capture("08b-cart-total", `cart ₹${payable.amount}`);
      if (cfg.cartAmountLimit) {
        const total = Number(String(payable.amount).replace(/,/g, ""));
        if (Number.isFinite(total) && total > cfg.cartAmountLimit) {
          throw new CheckoutFailure(
            "CART_AMOUNT_LIMIT",
            `Cart total ₹${total} exceeds the cart amount limit ₹${cfg.cartAmountLimit}`
          );
        }
      }
    } else if (cfg.cartAmountLimit) {
      // A limit that cannot be evaluated is not a limit — better to stop than to
      // continue as though the check had passed.
      throw new CheckoutFailure(
        "CART_AMOUNT_LIMIT",
        "cart_amount_limit was set but the cart total could not be read"
      );
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
    const mode = cfg.paymentMode || "none";
    // With COD chosen, the payments page IS the verdict — "Cash on Delivery:
    // Unavailable" is this ID's outcome — so it is read even when the run was
    // only asked to reach payments. A run that stopped at the page and reported
    // "success" over a greyed-out COD row is what this used to do.
    if (reachedPayments && (stopAfter !== "payments" || mode === "cod")) {
      await step(9, `Payment surface (${stopAfter})`);
      const fk = new FlipkartPayment(mobPage, (lvl, msg) => log(lvl, msg));

      /**
       * The final amount, checked at the only place it is final.
       *
       * finalAmountLimit has existed on the job schema from the beginning — parsed,
       * validated, persisted — and read by nothing. So a batch whose price moved
       * between the cart and the payment page had no backstop at the exact point
       * where money moves: COD handling fees, Protect Promise fees and offers that
       * fall off all land here, after the cart total was approved.
       *
       * Read from the payment page's own text rather than carried forward from the
       * cart, because carrying it forward would check the number we already knew.
       */
      if (cfg.finalAmountLimit) {
        const payText = String(await mobPage.evaluate(() => document.body?.innerText || "").catch(() => ""));
        const finalAmount = readPayableTotal(payText);
        await capture("10b-final-amount", `final ₹${finalAmount ?? "unreadable"} / limit ₹${cfg.finalAmountLimit}`);
        if (finalAmount == null) {
          // Ambiguous, so stop rather than guess. The first version of this took
          // the LARGEST amount on the page and read ₹599 — the struck-through MRP —
          // against a real payable of ₹109, which would have blocked a perfectly
          // good order. A limit that might be measuring the wrong number is worse
          // than no limit, because it fails in the direction you cannot see.
          throw new CheckoutFailure(
            "CART_AMOUNT_LIMIT",
            "final_amount_limit was set but no labelled total could be read from the payment page"
          );
        }
        console.log(`  final amount ₹${finalAmount} (limit ₹${cfg.finalAmountLimit})`);
        if (finalAmount > cfg.finalAmountLimit) {
          throw new CheckoutFailure(
            "CART_AMOUNT_LIMIT",
            `Payment page total ₹${finalAmount} exceeds the final amount limit ₹${cfg.finalAmountLimit}`
          );
        }
      }

      if (mode === "cod") {
        const available = await fk.isCodAvailable();
        await capture("11-cod-row", `isCodAvailable() = ${available}`);
        console.log(`  isCodAvailable() = ${available}`);
        // Same verdict the worker gives: with COD chosen, a page that lists Cash
        // on Delivery as Unavailable is this ID's outcome, whether or not the run
        // was going to pay.
        if (!available) {
          throw new CheckoutFailure("COD_UNAVAILABLE", "Cash on Delivery is unavailable for this cart on this account/pincode");
        }
        if (stopAfter === "pay") {
          if (!cfg.payLive) {
            console.log("  stopAfter=pay but payLive is not set — NOT placing the order");
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
        const cardTypeName = String(cfg.cardType || "").toUpperCase();
        if (!cardTypeName) throw new CheckoutFailure("CARD_AUTH_FAILED", "cardType is required for a card run");
        if (!cfg.authType) throw new CheckoutFailure("CARD_AUTH_FAILED", `authType is required for ${cardTypeName}`);
        await fk.selectCardPayment();
        await capture("11-card-form");
        if (!(stopAfter === "pay" && cfg.payLive)) {
          console.log("  card form reached; not filled (set stopAfter=pay and payLive=true to fill it)");
        } else {
          // The same four surfaces the worker drives, in the same order, through
          // the same objects — FlipkartPayment for Flipkart's form, the strategy
          // registry for the bank's page — so what passes here passes there.
          await fk.fillCardForm(cards[0]);
          await capture("12-card-filled");
          const rejected = await fk.detectCardRejectedByFlipkart();
          if (rejected) throw rejected;
          await fk.submitCardForm();
          const bankUrl = await fk.waitForBankHandoff();
          console.log(`  handed off to the bank: ${bankUrl.split("?")[0]}`);
          // The bank's page as it first renders: the artifact a strategy's
          // selectors are written from, and the one to compare against when the
          // bank changes its page.
          await capture("13-bank-page", bankUrl.split("?")[0]);
          try {
            const auth = await authenticatePayment({
              page: mobPage,
              runId,
              jobId: runId,
              cardTypeName,
              card: cards[0],
              authType: cfg.authType,
              corporateId: cfg.corporateId ?? null,
              log: (lvl, msg) => log(lvl, msg),
              dryRun: false,
            });
            console.log(`  authenticated ${auth.cardTypeName} (${auth.authType}) on ****${auth.cardLast4}`);
          } finally {
            // Captured whether the bank accepted or refused: the refusal page is
            // the one that explains a failed run.
            await capture("14-bank-result");
          }
          // Back on Flipkart. A bank "yes" is not an order: Flipkart can still
          // fail it, and that page is the final word.
          const confirmation = await fk.waitForOrderConfirmation();
          await capture("15-flipkart-after-bank", `order ${confirmation.orderId || "(no id)"}`);
          console.log(`  ORDER PLACED: ${confirmation.orderId || "(no id read)"} ${confirmation.amount}`);
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
      // Kept on the final record too. It used to live only in the mid-run
      // snapshot, so a finished run could not say what it was asked to do.
      config: redactConfig(cfg),
      logFile,
    };
    publish({ ...result, running: false });
    writeResult(dir, result);
    return result;
  } catch (err) {
    await capture("99-failure");
    const explained = await explainFailure(err, page, (cfg.checkoutPincode || "").slice(-6), {
      runId,
      caseId: cfg.caseId,
      step: currentStep,
      artifacts,
      startedAt,
      reachedPayments,
    });
    // On a card run that got as far as paying, say what the pool would do with
    // the card. For a ₹1 account the right answer is "paused until midnight IST",
    // never "dead" — that is what this line is for.
    let cardVerdict: TestFlowResult["cardVerdict"];
    if (cfg.paymentMode === "card" && explained.code && artifacts.some((a) => a.step.startsWith("12-"))) {
      const v = verdictFor(explained.code, explained.detail || "");
      cardVerdict = {
        kind: v.kind,
        reason: v.reason,
        pausedUntil: v.kind === "paused" ? new Date(v.untilMs).toISOString() : undefined,
      };
      console.log(`  card pool verdict: ${v.kind} — ${v.reason}${cardVerdict.pausedUntil ? ` (until ${cardVerdict.pausedUntil})` : ""}`);
    }
    const result: TestFlowResult = { ...explained, config: redactConfig(cfg), logFile, cardVerdict };
    publish({ ...result, running: false });
    writeResult(dir, result);
    return result;
  } finally {
    console.log = originalLog;
    // Leave the window up briefly when headed, so a failure can be looked at.
    // Headed runs stay open briefly so the last page can be inspected; a failure
    // gets longer, because that is the one you actually want to look at.
    if (!(cfg.headless ?? config.headless)) {
      await sleep(Math.min(config.keepBrowserOpenMs, reachedPayments ? 15_000 : 45_000));
    }
    await desktopBrowser.close().catch(() => {});
    await mobileBrowser?.close().catch(() => {});
  }
}

/**
 * What the payment page is actually about to charge.
 *
 * Must be the LABELLED total, never "the biggest number on the page". A real
 * Flipkart payments page carries, in order: ₹7 (COD fee), ₹599 (MRP, struck
 * through), ₹18, ₹11, -₹508 and -₹505 (discounts), -₹3, then "Total Amount ₹109".
 * Taking the maximum returns 599 — five times the real figure — and a limit built
 * on that rejects orders that were always within budget.
 *
 * Returns null when no label is found, so the caller stops instead of guessing.
 */
export function readPayableTotal(pageText: string): number | null {
  const labelled = String(pageText || "").match(
    /(?:total amount|amount payable|total payable|order total|grand total|amount to pay|to pay)\D{0,24}₹\s?([\d,]+(?:\.\d{1,2})?)/i
  );
  if (!labelled) return null;
  const n = Number(labelled[1].replace(/,/g, ""));
  return Number.isFinite(n) && n > 0 ? n : null;
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
  // Off Flipkart means on the bank's page: its vocabulary, not Flipkart's.
  if (url && !/flipkart\.com/i.test(url)) {
    const bank = classifyBankText(text);
    if (bank) return pack(bank.code, bank.display, bank.details);
  }
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
