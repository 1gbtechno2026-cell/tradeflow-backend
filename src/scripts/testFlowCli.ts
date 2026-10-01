/**
 * Run the checkout flow against real Flipkart and report how each case ended.
 *
 *   npm run test:flow -- happy              one case
 *   npm run test:flow -- happy oos          several
 *   npm run test:flow                       every case
 *   npm run test:flow -- happy --headless    no visible window
 *
 * Config comes from test-flow.json (see test-flow.example.json). Nothing is ever
 * paid: stopAfter defaults to "payments", and "pay" additionally requires
 * payLive: true in the config.
 *
 * Artifacts per run — a screenshot and the page's visible text at every boundary,
 * plus result.json — land in TEST_ARTIFACT_DIR (default debug/test-runs).
 */
import fs from "node:fs";
import path from "node:path";
import mongoose from "mongoose";
import { connectDb } from "../db.js";
import { runTestFlow, type TestFlowConfig, type TestFlowResult } from "../services/testFlow.js";

interface CaseDef {
  id: string;
  title: string;
  /** Overrides applied on top of the base config. */
  patch: Partial<TestFlowConfig>;
  /** "success" or the error code this case should produce. */
  expect: string;
}

/**
 * The four scenarios worth having. Product URLs live in the config file, not here,
 * because an out-of-stock product does not stay out of stock — these are
 * `productUrls.<id>` entries you keep current.
 */
const CASES: CaseDef[] = [
  { id: "happy", title: "In stock + deliverable → payments page", patch: {}, expect: "success" },
  // Every case below runs at the quantity in test-flow.json. Set it to what you
  // actually want to order; the `qty` case only makes sense when that number is
  // above the product's per-order cap.
  { id: "oos", title: "Out of stock product", patch: {}, expect: "PRODUCT_NOT_SERVICEABLE" },
  { id: "pincode", title: "Not deliverable to this pincode", patch: {}, expect: "ITEM_NOT_DELIVERABLE" },
  {
    id: "qty",
    title: "Desired quantity is not available",
    // No quantity override. The quantity under test is whatever the operator asked
    // for — hardcoding 50 made the case read as "test the number 50", which is not
    // the scenario. The scenario is "I asked for N and Flipkart will not give me
    // N", and N is a real business input, different per product and per batch.
    // Flipkart's own cap varies by product (16 on the Samsung M06), so a fixed
    // number would also stop being over the limit the moment a cap went above it.
    patch: {},
    expect: "MAX_UNITS_REACHED",
  },
];

interface BaseConfig extends Omit<TestFlowConfig, "caseId" | "productUrl"> {
  /** Per-case product URL. `happy` is required; the rest are optional. */
  productUrls: Record<string, string>;
}

const CONFIG_FILE = path.resolve(process.env.TEST_FLOW_CONFIG || "test-flow.json");

function loadBase(): BaseConfig {
  if (!fs.existsSync(CONFIG_FILE)) {
    console.error(`\n✖ ${path.relative(process.cwd(), CONFIG_FILE)} not found.`);
    console.error("  Copy test-flow.example.json to test-flow.json and fill in your own");
    console.error("  platformId, addressId, gstId and product URLs.\n");
    process.exit(1);
  }
  const cfg = JSON.parse(fs.readFileSync(CONFIG_FILE, "utf8")) as BaseConfig;
  const missing = (["platformId", "productUrls"] as const).filter((k) => !cfg[k]);
  if (missing.length) throw new Error(`${CONFIG_FILE} is missing: ${missing.join(", ")}`);
  if (!cfg.productUrls.happy) throw new Error(`${CONFIG_FILE} needs productUrls.happy at minimum`);
  return cfg;
}

function line(r: TestFlowResult, expect: string): string {
  const got = r.status === "success" ? "success" : r.code || "?";
  const pass = got === expect;
  return (
    `  ${pass ? "PASS" : "FAIL"}  ${r.caseId.padEnd(9)} expected ${expect.padEnd(26)} got ${String(got).padEnd(26)} ${r.seconds}s`
  );
}

async function main() {
  const argv = process.argv.slice(2);
  const headless = argv.includes("--headless");
  const wanted = argv.filter((a) => !a.startsWith("--"));
  const base = loadBase();
  const cases = wanted.length ? CASES.filter((c) => wanted.includes(c.id)) : CASES;
  if (!cases.length) {
    console.error(`No case matched "${wanted.join(" ")}". Known: ${CASES.map((c) => c.id).join(", ")}`);
    process.exit(1);
  }

  await connectDb();
  const rows: Array<{ r: TestFlowResult; expect: string; title: string }> = [];

  for (const c of cases) {
    const productUrl = base.productUrls[c.id];
    if (!productUrl) {
      console.log(`\n── skipping ${c.id}: no productUrls.${c.id} in the config ──`);
      continue;
    }
    console.log(`\n══════════ ${c.id}: ${c.title} (expect ${c.expect}) ══════════`);
    const cfg: TestFlowConfig = {
      ...base,
      caseId: c.id,
      productUrl,
      ...c.patch,
      ...(headless ? { headless: true } : {}),
    };
    const r = await runTestFlow(cfg);
    if (r.status === "failed") {
      console.log(`\n  ✖ stopped at step ${r.step}`);
      console.log(`    code     : ${r.code}`);
      console.log(`    reason   : ${r.reason}`);
      if (r.flipkartMessage) console.log(`    flipkart : "${String(r.flipkartMessage).slice(0, 160)}"`);
      if (r.detail && r.detail !== r.flipkartMessage) console.log(`    detail   : ${String(r.detail).slice(0, 160)}`);
      console.log(`    page     : ${r.url}`);
    } else {
      console.log(`\n  ✔ finished on ${r.url}${r.reachedPayments ? " (payments)" : ""}`);
    }
    rows.push({ r, expect: c.expect, title: c.title });
  }

  console.log("\n══════════════════ SUMMARY ══════════════════");
  for (const { r, expect } of rows) console.log(line(r, expect));
  const allPass = rows.every(({ r, expect }) => (r.status === "success" ? "success" : r.code) === expect);
  console.log(`\n  ${rows.length} case(s), ${allPass ? "all as expected" : "SOME UNEXPECTED"}`);
  console.log(`  artifacts under ${process.env.TEST_ARTIFACT_DIR || "debug/test-runs"}/\n`);

  await mongoose.disconnect();
  process.exit(allPass ? 0 : 1);
}

main().catch(async (err) => {
  console.error(`\n✖ harness failed to run: ${err instanceof Error ? err.message : err}`);
  await mongoose.disconnect().catch(() => {});
  process.exit(1);
});
