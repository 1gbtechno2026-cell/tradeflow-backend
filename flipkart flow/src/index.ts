import fs from "node:fs";
import path from "node:path";
import type { Page } from "playwright";
import { FlipkartCheckout2, OutOfStockPincodeError, isLoginUrl } from "./flows/FlipkartCheckout2.js";
import { navigateWithRetry } from "./flows/helpers.js";
import { CheckoutFailure, REASONS, diagnosePageText, type CheckoutFailureCode } from "./services/checkoutErrors.js";
import { desktopContext, launchBrowser, mobileContext, saveSession, sleep } from "./services/browser.js";
import type { AddressDetails, OrderConfig } from "./types.js";

const LOGIN_WAIT_MS = 5 * 60 * 1000;

function loadConfig(file: string): OrderConfig {
  const cfg = JSON.parse(fs.readFileSync(file, "utf8")) as OrderConfig;
  const missing = (["productUrl", "quantity", "registeredMobile", "address", "gstNumber"] as const).filter(
    (k) => cfg[k] === undefined || cfg[k] === ""
  );
  if (missing.length) throw new Error(`${file} is missing: ${missing.join(", ")}`);
  if (!Number.isInteger(cfg.quantity) || cfg.quantity < 1) throw new Error("quantity must be a whole number ≥ 1");
  if (!/^\d{2}[A-Z]{5}\d{4}[A-Z][A-Z0-9]Z[A-Z0-9]$/i.test(cfg.gstNumber.trim())) {
    throw new Error(`gstNumber "${cfg.gstNumber}" is not a valid 15-character GSTIN`);
  }
  return cfg;
}

const last10 = (s: string) => String(s || "").replace(/\D/g, "").slice(-10);

let currentStep = "starting";

function step(n: number, title: string) {
  currentStep = `${n}. ${title}`;
  console.log(`\n==================== STEP ${n}: ${title} ====================`);
}

/** If Flipkart asks for login, let the user do the OTP in the open window, then continue. */
async function ensureLoggedIn(page: Page, headless: boolean): Promise<void> {
  await navigateWithRetry(page, "https://www.flipkart.com/account", { timeoutMs: 15000, maxRetries: 2 });
  if (!isLoginUrl(page.url())) return;
  if (headless) throw new CheckoutFailure("LOGIN_REQUIRED", "Not logged in. Run once with \"headless\": false and log in with OTP.");
  console.log(`\n>>> Not logged in. Log in with mobile + OTP in the browser window (waiting up to ${LOGIN_WAIT_MS / 60000} min)...\n`);
  const deadline = Date.now() + LOGIN_WAIT_MS;
  while (Date.now() < deadline) {
    await sleep(2000);
    if (!isLoginUrl(page.url())) {
      await navigateWithRetry(page, "https://www.flipkart.com/account", { timeoutMs: 15000, maxRetries: 2 });
      if (!isLoginUrl(page.url())) {
        console.log("Login detected.");
        return;
      }
    }
  }
  throw new CheckoutFailure("LOGIN_REQUIRED", "Timed out waiting for Flipkart login");
}

const DEBUG_DIR = path.resolve("debug");
let dumpCount = 0;

/** Save a screenshot + visible text so failures on Flipkart pages can be inspected. */
async function dump(page: Page | null, label: string): Promise<string | null> {
  if (!page || page.isClosed()) return null;
  try {
    fs.mkdirSync(DEBUG_DIR, { recursive: true });
    const base = path.join(DEBUG_DIR, `${String(++dumpCount).padStart(2, "0")}-${label}`);
    await page.screenshot({ path: `${base}.png`, fullPage: true, timeout: 10000 });
    const text = await page.evaluate("document.body ? document.body.innerText : ''").catch(() => "");
    fs.writeFileSync(`${base}.txt`, `URL: ${page.url()}

${text}`, "utf8");
    console.log(`[debug] saved ${path.relative(process.cwd(), base)}.png/.txt`);
    return `${path.relative(process.cwd(), base)}.png`;
  } catch (err) {
    console.log(`[debug] dump ${label} failed: ${err instanceof Error ? err.message : err}`);
    return null;
  }
}

interface RunResult {
  status: "success" | "failed";
  step: string;
  code?: CheckoutFailureCode;
  reason?: string;
  flipkartMessage?: string;
  detail?: string;
  url: string;
  screenshot?: string | null;
  finishedAt: string;
}

/** Codes that only say *where* it broke — the page text usually says *why*. */
const GENERIC_CODES: CheckoutFailureCode[] = ["PAYMENT_PAGE_NOT_REACHED", "ADD_TO_CART_FAILED", "FLIPKART_ERROR", "AUTOMATION_ERROR"];

/** Turn any error into a reason: Flipkart's own message when the page shows one, else what the script hit. */
async function explainFailure(err: unknown, page: Page | null, pincode: string): Promise<RunResult> {
  const detail = err instanceof Error ? err.message.split("\n")[0] : String(err);
  const open = page && !page.isClosed();
  const url = open ? page.url() : "";
  const text = open ? String(await page.evaluate("document.body ? document.body.innerText : ''").catch(() => "")) : "";
  const pack = (code: CheckoutFailureCode, flipkartMessage?: string, extra?: string): RunResult => ({
    status: "failed",
    step: currentStep,
    code,
    reason: REASONS[code],
    flipkartMessage,
    detail: extra,
    url,
    finishedAt: new Date().toISOString(),
  });

  if (err instanceof CheckoutFailure && !GENERIC_CODES.includes(err.code)) return pack(err.code, err.message);
  if (err instanceof OutOfStockPincodeError) return pack("OUT_OF_STOCK", err.rawMessage);
  if (url && isLoginUrl(url)) return pack("LOGIN_REQUIRED", undefined, detail);

  // Product page that still offers Buy now / Add to cart → ignore stock words from other variants.
  const purchasable = /^\s*(buy now|add to cart)\b/im.test(text);
  const fromPage = diagnosePageText(text, pincode, { purchasable });
  if (fromPage) return pack(fromPage.code, fromPage.message, detail);
  if (err instanceof CheckoutFailure) return pack(err.code, err.message);
  return pack("AUTOMATION_ERROR", undefined, detail);
}

function writeResult(result: RunResult) {
  fs.mkdirSync(DEBUG_DIR, { recursive: true });
  fs.writeFileSync(path.join(DEBUG_DIR, "result.json"), JSON.stringify(result, null, 2), "utf8");
}

function printFailure(r: RunResult) {
  console.error("\n✖ ORDER NOT COMPLETED");
  console.error(`  Stopped at : step ${r.step}`);
  console.error(`  Reason     : ${r.reason} [${r.code}]`);
  if (r.flipkartMessage) console.error(`  Flipkart   : "${r.flipkartMessage}"`);
  if (r.detail && r.detail !== r.flipkartMessage) console.error(`  Detail     : ${r.detail}`);
  if (r.url) console.error(`  Page       : ${r.url}`);
  if (r.screenshot) console.error(`  Screenshot : ${r.screenshot}`);
  console.error("  (saved to debug/result.json)");
}

/**
 * Read only this product's row on the cart (title → "Remove"), not recommendation
 * tiles, and report what Flipkart says about it: Out Of Stock, not deliverable, …
 */
async function cartItemBlocker(page: Page, model: string, pincode: string): Promise<CheckoutFailure | null> {
  const text = String(await page.evaluate("document.body ? document.body.innerText : ''").catch(() => ""));
  const cartOnly = text.split(/\n\s*(Items you may have missed|Recently Viewed|Similar products|You might be interested in)\b/i)[0];
  const lines = cartOnly.split(/\n+/).map((l) => l.trim());
  const key = model.toLowerCase().slice(0, 24);
  const start = key ? lines.findIndex((l) => l.toLowerCase().startsWith(key)) : -1;
  if (start < 0) return null;
  let end = lines.findIndex((l, i) => i > start && /^(remove|save for later)$/i.test(l));
  if (end < 0) end = Math.min(lines.length, start + 25);
  const row = lines.slice(start, end).join("\n");
  const issue = diagnosePageText(row, pincode);
  if (!issue) return null;
  return new CheckoutFailure(issue.code, `${model}: ${issue.message}`);
}

async function checkoutErrored(page: Page): Promise<boolean> {
  const text = String(await page.evaluate("document.body ? document.body.innerText : ''").catch(() => ""));
  return /Something went wrong/i.test(text) && !/Deliver to/i.test(text);
}

async function main() {
  const configPath = path.resolve(process.argv[2] || "order.json");
  const cfg = loadConfig(configPath);
  const headless = cfg.headless ?? false;
  const gstMandatory = cfg.gstMandatory ?? true;
  const pincode = (cfg.address.checkoutPincode || cfg.address.pincode).trim();
  console.log(`Order: ${cfg.productUrl}\nqty=${cfg.quantity} pin=${pincode} gst=${cfg.gstNumber}`);

  // SKIP_DESKTOP=1 reruns only the mobile checkout part (steps 4-8) while debugging.
  const skipDesktop = process.env.SKIP_DESKTOP === "1";
  const addressMobile = last10(cfg.registeredMobile);
  if (addressMobile.length !== 10) throw new Error(`registeredMobile "${cfg.registeredMobile}" is not a 10-digit number`);
  const address: AddressDetails = {
    ...cfg.address,
    mobile: addressMobile,
    gstNumber: cfg.gstNumber.trim().toUpperCase(),
  };

  const browser = await launchBrowser(headless);
  let current: Page | null = null;
  try {
    if (!skipDesktop) await desktopSteps();
    else console.log("SKIP_DESKTOP=1 — skipping steps 1-3");
    await mobileSteps();
  } catch (err) {
    const result = await explainFailure(err, current, pincode);
    result.screenshot = await dump(current, "failure");
    writeResult(result);
    printFailure(result);
    process.exitCode = 1;
  } finally {
    await browser.close().catch(() => {});
  }

  async function desktopSteps() {
    // ---------------- Desktop: account checks + empty cart ----------------
    const desk = await desktopContext(browser);
    const deskPage = await desk.newPage();
    current = deskPage;
    const deskFlow = new FlipkartCheckout2(deskPage, cfg.productUrl);

    step(1, "Open account page and read the account's registered mobile");
    await ensureLoggedIn(deskPage, headless);
    await saveSession(desk);
    const accountMobile = await deskFlow.fetchAccountMobile();
    if (!accountMobile) throw new Error("Could not read the registered mobile number from /account");
    console.log(`>>> Logged-in account registered mobile: ${last10(accountMobile)}`);

    // registeredMobile from order.json is the phone number on the delivery address.
    console.log(`>>> Delivery address mobile (from order): ${addressMobile}`);

    step(2, "Check address exists on account (add if missing)");
    await deskFlow.ensureAddressExists(address, addressMobile).catch((err) => {
      if (err instanceof CheckoutFailure) throw err;
      throw new CheckoutFailure("ADDRESS_FAILED", err instanceof Error ? err.message : String(err));
    });

    step(3, "Empty the cart");
    await deskFlow.emptyCart();
    await saveSession(desk);
    await desk.close();
  }

  async function mobileSteps() {
    // ---------------- Mobile mode: add to cart → checkout → payments ----------------
    step(4, `Switch to mobile mode (${cfg.mobileDevice || "Pixel 7"})`);
    const mob = await mobileContext(browser, cfg.mobileDevice);
    const page = await mob.newPage();
    current = page;
    page.on("console", (msg) => {
      const t = msg.text();
      if (t.startsWith("[")) console.log(`  (page) ${t}`);
    });
    const flow = new FlipkartCheckout2(page, cfg.productUrl);
    flow.setCheckoutFlags(pincode, gstMandatory);

    // Fresh m-site session first: open Flipkart home and refresh, then paste the product link.
    await navigateWithRetry(page, "https://www.flipkart.com/", { timeoutMs: 15000, maxRetries: 2 });
    await page.reload({ waitUntil: "domcontentloaded", timeout: 15000 }).catch(() => {});
    await page.waitForLoadState("load", { timeout: 10000 }).catch(() => {});
    console.log(`Mobile mode ready and refreshed — ${page.url()}`);

    step(5, "Paste product link");
    await flow.navigateToProduct();
    const details = await flow.captureProductDetails();
    console.log(`Product: ${details.model || "?"} | colour=${details.colour || "-"} | price=${details.amount || "?"}`);

    step(6, "Add to cart");
    await flow.clickAddToCart(pincode);
    await flow.gotoViewCart();
    await flow.ensureProductInCart(details, pincode);

    const cartIssue = await cartItemBlocker(page, details.model, pincode);
    if (cartIssue) throw cartIssue;

    step(7, "Go to checkout");
    await dump(page, "viewcart-before-place-order");
    await flow.clickPlaceOrder();
    // Flipkart shows "Something went wrong! E002" when checkout is opened without a real Place Order.
    for (let retry = 1; retry <= 2 && (await checkoutErrored(page)); retry++) {
      console.log(`[PlaceOrder] checkout shows "Something went wrong" — back to cart and Place Order again (${retry}/2)`);
      await dump(page, `checkout-error-${retry}`);
      await navigateWithRetry(page, "https://www.flipkart.com/viewcart?exploreMode=TRUE&preference=FLIPKART", { timeoutMs: 15000, maxRetries: 2 });
      await flow.gotoViewCart();
      await flow.clickPlaceOrder();
    }
    await dump(page, "checkout");
    if (await checkoutErrored(page)) {
      throw new CheckoutFailure("FLIPKART_ERROR", "Checkout keeps showing \"Something went wrong! E002\" after Place Order");
    }

    // Guard: checkout must be for this product, not a leftover session.
    const checkoutText = String(await page.evaluate("document.body ? document.body.innerText : ''").catch(() => ""));
    const modelKey = details.model.slice(0, 24).toLowerCase();
    if (modelKey && !checkoutText.toLowerCase().includes(modelKey)) {
      throw new CheckoutFailure("PAYMENT_PAGE_NOT_REACHED", `Checkout page does not show "${details.model}" — refusing to continue with a different product`);
    }

    step(8, `Quantity=${cfg.quantity}, delivery address, GST (always ticked), then Continue`);
    await flow.verifyAddressOnOrderSummary(address, cfg.quantity, gstMandatory);
    await saveSession(mob);

    const onPayments = /\/payments/i.test(page.url());
    writeResult({ status: "success", step: currentStep, url: page.url(), finishedAt: new Date().toISOString() });
    console.log(`\n✔ Flow finished on ${page.url()}${onPayments ? " (payments page)" : ""}`);
    console.log("No payment is made by this script. Complete or abandon payment in the window; close it to exit.");
    // EXIT_ON_FINISH=1 (used by npm test) closes instead of leaving the payments page open.
    if (!headless && process.env.EXIT_ON_FINISH !== "1") {
      await page.waitForEvent("close", { timeout: 0 }).catch(() => {});
    }
  }
}

main().catch((err) => {
  // Only errors before the browser starts (bad order file) land here.
  console.error(`\n✖ Flow failed: ${err instanceof Error ? err.message : err}`);
  process.exitCode = 1;
});
