/**
 * Runs the order flow against known products and checks each one ends the way it should:
 * reaching payments, or stopping with the right reason. Also checks the reason matcher
 * offline against sample Flipkart messages.
 *
 *   npm test                 all cases
 *   npm test -- oos qty      only the named cases (+ offline checks)
 *
 * Every live case runs the full flow (empties the cart first). Nothing is ever paid.
 */
import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { diagnosePageText, type CheckoutFailureCode } from "./services/checkoutErrors.js";
import type { OrderConfig } from "./types.js";

interface LiveCase {
  id: string;
  title: string;
  productUrl: string;
  quantity?: number;
  expect: "success" | CheckoutFailureCode;
}

const LIVE_CASES: LiveCase[] = [
  {
    id: "happy",
    title: "In stock + deliverable → payments page",
    productUrl: "https://www.flipkart.com/apple-iphone-16-ultramarine-256-gb/p/itmd4c0cc4933f29",
    quantity: 2,
    expect: "success",
  },
  {
    id: "oos",
    title: "Out of stock product",
    productUrl: "https://www.flipkart.com/apple-iphone-18-pro-max-burgundy-512-gb/p/itm455c3ebe49c49?pid=MOBHQT5JVH6G2VUS",
    expect: "OUT_OF_STOCK",
  },
  {
    id: "pincode",
    title: "Not deliverable to the address pincode",
    productUrl: "https://www.flipkart.com/lifelong-20000-mah-22-5-w-compact-pocket-size-power-bank/p/itm9d12a51da298b?pid=PWBGXUQYGA5GTZJ2",
    expect: "ITEM_NOT_DELIVERABLE",
  },
  {
    id: "qty",
    title: "Quantity above Flipkart's per-order limit",
    productUrl: "https://www.flipkart.com/apple-iphone-16-ultramarine-256-gb/p/itmd4c0cc4933f29",
    quantity: 50,
    expect: "QUANTITY_LIMIT",
  },
];

/** Sample Flipkart wording → expected reason. null = must NOT be flagged. */
const OFFLINE_CASES: Array<[string, string, { purchasable?: boolean }, CheckoutFailureCode | null]> = [
  ["PDP sold out", "Apple iPhone 16\nSold Out\nThis item is currently out of stock", {}, "OUT_OF_STOCK"],
  ["PDP coming soon", "Apple iPhone 17\nComing Soon\nNotify Me", {}, "COMING_SOON"],
  ["PDP unavailable", "Samsung S24\nCurrently unavailable\nWe don't know when or if this item will be back", {}, "PRODUCT_UNAVAILABLE"],
  ["PDP not deliverable", "Delivery details\nNot deliverable at your location\nSeller: RetailNet", { purchasable: true }, "ITEM_NOT_DELIVERABLE"],
  ["cart seller no deliver", "My Cart\nSeller does not deliver to 122016\nPlace Order", {}, "ITEM_NOT_DELIVERABLE"],
  ["cart OOS item", "My Cart\nApple iPhone 18 Pro max\nOut Of Stock\nRemove", {}, "OUT_OF_STOCK"],
  ["checkout not deliverable", "1 item is not deliverable to 122016. Please try changing the address.", {}, "ITEM_NOT_DELIVERABLE"],
  ["qty cap toast", "Qty: 14\nCurrently out of stock for 122016\nYou can only purchase 14 units of Apple iPhone 16 - IWIT (Ultramarine, 256 GB) in a single order. If you have a higher requirement, please create a new order.", {}, "QUANTITY_LIMIT"],
  ["qty per customer", "Order Summary\nOnly 2 units allowed per customer\nQty: 2", {}, "QUANTITY_LIMIT"],
  ["gst invalid", "Add new GST Details\nPlease enter valid GSTIN\nConfirm and Save", {}, "GST_INVALID"],
  ["price changed", "Price has changed for 1 item\nContinue", {}, "PRICE_CHANGED"],
  ["login page", "Login\nEnter your mobile number to get OTP\nRequest OTP", {}, "LOGIN_REQUIRED"],
  ["E002 page", "Something went wrong!\nPlease try again later.\nE002\nRetry", {}, "FLIPKART_ERROR"],
  ["other colour OOS, buyable", "Selected Color: Purple\nOut of stock\nOut of stock\nBuy now\nAdd to cart", { purchasable: true }, null],
  ["normal checkout", "Deliver to:\nDashmobiles Pvt Ltd\nQty: 2\nGST Invoice\nChange\nContinue\nYou'll save ₹700 on this order!", {}, null],
  ["normal PDP", "Apple iPhone 16 - IWIT (Ultramarine, 256 GB)\n₹79,900\nEXPRESS Delivery by tomorrow\nBuy with EMI\nBuy now", { purchasable: true }, null],
];

const RESULTS_DIR = path.resolve("test-results");

function runOffline(): boolean {
  console.log("\n── Offline: reason matcher on sample Flipkart messages ──");
  let ok = true;
  for (const [name, text, opts, want] of OFFLINE_CASES) {
    const got = diagnosePageText(text, "122016", opts)?.code ?? null;
    const pass = got === want;
    ok &&= pass;
    console.log(`  ${pass ? "PASS" : "FAIL"}  ${name.padEnd(28)} expected ${String(want).padEnd(22)} got ${got}`);
  }
  return ok;
}

function runFlow(cfgPath: string, logPath: string): Promise<number> {
  return new Promise((resolve) => {
    const log = fs.createWriteStream(logPath);
    const child = spawn(process.execPath, ["dist/index.js", cfgPath], {
      env: { ...process.env, EXIT_ON_FINISH: "1", SKIP_DESKTOP: "" },
    });
    child.stdout.pipe(log);
    child.stderr.pipe(log);
    child.on("close", (code) => resolve(code ?? 1));
  });
}

async function runLive(c: LiveCase, base: OrderConfig) {
  const dir = path.join(RESULTS_DIR, c.id);
  fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(dir, { recursive: true });
  const cfgPath = path.join(dir, "order.json");
  fs.writeFileSync(cfgPath, JSON.stringify({ ...base, productUrl: c.productUrl, quantity: c.quantity ?? 1 }, null, 2));

  fs.rmSync("debug", { recursive: true, force: true });
  console.log(`\n── Live: ${c.id} — ${c.title} (expect ${c.expect}) ──`);
  const started = Date.now();
  await runFlow(cfgPath, path.join(dir, "run.log"));
  const secs = Math.round((Date.now() - started) / 1000);

  // Keep this run's screenshots + result next to its log.
  if (fs.existsSync("debug")) fs.cpSync("debug", path.join(dir, "debug"), { recursive: true });
  const resultFile = path.join("debug", "result.json");
  const result = fs.existsSync(resultFile) ? JSON.parse(fs.readFileSync(resultFile, "utf8")) : null;
  const got: string = !result ? "NO_RESULT" : result.status === "success" ? "success" : result.code;
  const pass = got === c.expect;
  console.log(`  ${pass ? "PASS" : "FAIL"}  got ${got} in ${secs}s`);
  if (result?.status === "failed") {
    console.log(`        step ${result.step}`);
    console.log(`        Flipkart: ${result.flipkartMessage ?? "-"}`);
  } else if (result?.status === "success") {
    console.log(`        reached ${String(result.url).split("?")[0]}`);
  }
  return { id: c.id, title: c.title, expect: c.expect, got, pass, secs, flipkart: result?.flipkartMessage ?? "" };
}

async function main() {
  const baseFile = path.resolve(process.env.ORDER_FILE || "order.example.json");
  const base = JSON.parse(fs.readFileSync(baseFile, "utf8")) as OrderConfig;
  const only = process.argv.slice(2);
  const cases = only.length ? LIVE_CASES.filter((c) => only.includes(c.id)) : LIVE_CASES;

  const offlineOk = runOffline();
  const rows = [];
  for (const c of cases) rows.push(await runLive(c, base));

  console.log("\n══════════════ SUMMARY ══════════════");
  console.log(`  ${offlineOk ? "PASS" : "FAIL"}  offline reason matcher (${OFFLINE_CASES.length} samples)`);
  for (const r of rows) {
    console.log(`  ${r.pass ? "PASS" : "FAIL"}  ${r.id.padEnd(8)} ${r.title.padEnd(44)} expected ${r.expect.padEnd(20)} got ${r.got}`);
  }
  console.log(`\nLogs & screenshots: ${path.relative(process.cwd(), RESULTS_DIR)}/<case>/`);
  fs.writeFileSync(path.join(RESULTS_DIR, "summary.json"), JSON.stringify({ offlineOk, rows }, null, 2));
  process.exitCode = offlineOk && rows.every((r) => r.pass) ? 0 : 1;
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
