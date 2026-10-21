/**
 * Cancel one Flipkart order unit through the same FlipkartCancel the worker uses,
 * with a screenshot and the page's text at every screen.
 *
 *   npm run test:cancel -- <email> <OD…> [unitId]            dry run: stops before REQUEST CANCELLATION
 *   npm run test:cancel -- <email> <OD…> [unitId] --submit   actually cancels
 *   add --headless for no visible window
 *
 * unitId defaults to the order's first unit (<order digits>000).
 * Artifacts land in TEST_ARTIFACT_DIR (default debug/test-runs)/cancel-<time>/.
 */
import fs from "node:fs";
import path from "node:path";
import mongoose from "mongoose";
import { connectDb } from "../db.js";
import { FlipkartCancel } from "../automation/FlipkartCancel.js";
import { resolveLoggedInSession } from "../services/sessionStore.js";
import { blockFlipkartLogout, desktopContext, launchStealthBrowser, restoreFlipkartSession } from "../services/browser.js";

async function main() {
  const argv = process.argv.slice(2);
  const submit = argv.includes("--submit");
  const headless = argv.includes("--headless");
  const [email, orderIdRaw, unitIdRaw] = argv.filter((a) => !a.startsWith("--"));
  const orderId = String(orderIdRaw || "").toUpperCase();
  if (!email || !/^OD\d{12,}$/.test(orderId)) {
    console.error("usage: npm run test:cancel -- <email> <OD…> [unitId] [--submit] [--headless]");
    process.exit(1);
  }
  const unitId = unitIdRaw || `${orderId.slice(2)}0`.padEnd(21, "0");
  const dir = path.resolve(process.env.TEST_ARTIFACT_DIR || "debug/test-runs", `cancel-${new Date().toISOString().replace(/[:.]/g, "-")}`);
  fs.mkdirSync(dir, { recursive: true });

  await connectDb();
  const session = await resolveLoggedInSession(email);
  if (!session.ok) throw new Error(`${session.email}: ${session.reason}`);

  const browser = await launchStealthBrowser({ headless });
  try {
    const ctx = await desktopContext(browser);
    const page = await ctx.newPage();
    await blockFlipkartLogout(page);
    await restoreFlipkartSession(page, session.cookies);
    let n = 0;
    const onStep = async (name: string) => {
      const base = path.join(dir, `${String(++n).padStart(2, "0")}-${name}`);
      await page.screenshot({ path: `${base}.png`, fullPage: true });
      fs.writeFileSync(`${base}.txt`, `${page.url()}\n\n${await page.evaluate(() => document.body?.innerText || "")}`);
    };
    console.log(`${submit ? "CANCELLING" : "dry run"} ${orderId} unit ${unitId} on ${session.email}`);
    const outcome = await new FlipkartCancel(page, (lvl, msg) => console.log(`  [${lvl}] ${msg}`)).cancelUnit(
      { orderId, unitId },
      { dryRun: !submit, onStep }
    );
    console.log("\nOUTCOME:", JSON.stringify(outcome));
  } finally {
    console.log(`artifacts: ${path.relative(process.cwd(), dir)}/`);
    await browser.close().catch(() => undefined);
    await mongoose.disconnect().catch(() => undefined);
  }
}

main().catch((err) => {
  console.error("✖", err instanceof Error ? err.message : err);
  process.exit(1);
});
