import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

const here = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(here, "../.env") });

function required(name: string, fallback?: string): string {
  const value = process.env[name] ?? fallback;
  if (value == null || value === "") {
    throw new Error(`${name} is required`);
  }
  return value;
}

export const config = {
  port: Number(process.env.PORT || 4100),
  apiKey: required("API_KEY", "change-me"),
  mongoUri: required("MONGO_URI", "mongodb://localhost:27017/automation"),
  userId: process.env.TRADE_FLOW_USER_ID || "",
  redisUrl: process.env.REDIS_URL || "redis://127.0.0.1:6376",
  chromePath: process.env.CHROME_PATH || "",
  /** Headed Chrome unless HEADLESS is the literal string "true". */
  headless: process.env.HEADLESS === "true",
  workerConcurrency: Math.max(1, Number(process.env.WORKER_CONCURRENCY || 1)),
  /** How many fetch Chromes one API task may open. 0 = pick from RAM. */
  fetchWindowConcurrency: Math.max(0, Number(process.env.FETCH_WINDOW_CONCURRENCY || 0)),
  /** Pause between Playwright actions so the headed window is watchable. */
  slowMoMs: Math.max(0, Number(process.env.SLOW_MO_MS || 200)),
  /** Keep the Chrome window open after the job so you can inspect the page. */
  keepBrowserOpenMs: Math.max(0, Number(process.env.KEEP_BROWSER_OPEN_MS || 60000)),
  /** Bearer token the Android SMS forwarder sends to /api/sms. Empty = webhook disabled. */
  smsApiToken: process.env.SMS_API_TOKEN || "",
  /** How long a payment waits for the bank's OTP before the order is failed.
   *  Three minutes: the operator's rule, and the bank's own validity window —
   *  a code older than that is refused by the bank anyway. */
  otpTimeoutMs: Math.max(30_000, Number(process.env.OTP_TIMEOUT_MS || 3 * 60 * 1000)),
  /** Playwright device emulated for the m-site leg of checkout. Flipkart chooses
   *  which site to serve from the User-Agent, and FlipkartCheckout is m-site
   *  automation, so this decides whether its selectors exist at all. */
  mobileDevice: process.env.MOBILE_DEVICE || "Pixel 7",
  /** Test-flow routes and the dashboard's Test tab. Off unless explicitly on, so
   *  a harness that drives real Flipkart pages cannot be reachable by accident. */
  enableTestRoutes: process.env.ENABLE_TEST_ROUTES === "1",
  /** Where test-run screenshots and page text are written. */
  testArtifactDir: process.env.TEST_ARTIFACT_DIR || "debug/test-runs",
};
