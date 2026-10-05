import os from "node:os";
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
  /** The payment budget: from the moment Pay is pressed on Flipkart's card
   *  form, everything — the hand-off, the bank's identity step, and the OTP
   *  arriving — must complete within this, or the order is failed and the
   *  handset lease lapses on the same clock. Two minutes: the operator's rule. */
  otpTimeoutMs: Math.max(30_000, Number(process.env.OTP_TIMEOUT_MS || 2 * 60 * 1000)),
  /** Playwright device emulated for the m-site leg of checkout. Flipkart chooses
   *  which site to serve from the User-Agent, and FlipkartCheckout is m-site
   *  automation, so this decides whether its selectors exist at all. */
  mobileDevice: process.env.MOBILE_DEVICE || "Pixel 7",
  /** Test-flow routes and the dashboard's Test tab. Off unless explicitly on, so
   *  a harness that drives real Flipkart pages cannot be reachable by accident. */
  enableTestRoutes: process.env.ENABLE_TEST_ROUTES === "1",
  /** Where test-run screenshots and page text are written. */
  testArtifactDir: process.env.TEST_ARTIFACT_DIR || "debug/test-runs",
  /**
   * This process's identity in the Proxy Pool: the row it binds to is keyed on
   * it, so the binding survives restarts. Set WORKER_ID=w-001 … per AWS task;
   * locally it is the machine's hostname. The API process (Test tab) uses
   * `${workerId}-api` so it never shares a worker's proxy.
   */
  workerId: (process.env.WORKER_ID || os.hostname().split(".")[0] || "worker").trim(),
  /**
   * required  — a worker with no proxy exits at boot and takes no jobs; a job
   *             whose proxy vanishes mid-batch waits (re-queued) for a free one.
   * preferred — use a proxy when one is free, else the local connection, and
   *             say so on every job (default: this laptop).
   * off       — ignore the pool entirely.
   */
  proxyMode: (["required", "preferred", "off"].includes(String(process.env.PROXY_MODE || "").toLowerCase())
    ? String(process.env.PROXY_MODE).toLowerCase()
    : "preferred") as "required" | "preferred" | "off",
  /** Decrypts proxy passwords (services/proxySecrets.ts). Same value as the dashboard's. */
  proxyCredKey: process.env.PROXY_CRED_KEY || "",
  /** How long a job waits for a proxy to free up in `required` mode. */
  proxyWaitMs: Math.max(5_000, Number(process.env.PROXY_WAIT_MS || 30_000)),
  /** Consecutive proxy failures before a row is marked dead. */
  proxyDeadAfter: Math.max(1, Number(process.env.PROXY_DEAD_AFTER || 3)),
  /**
   * How order fetch / update read Flipkart:
   *   scrape — the My Orders and order-details PAGES, parsed from the DOM (default)
   *   api    — Flipkart's own order APIs with the saved session (services/orderApi.ts);
   *            a Chrome is opened only when a unit has to fall back to the page.
   */
  orderFetchMode: (String(process.env.ORDER_FETCH_MODE || "scrape").toLowerCase() === "api" ? "api" : "scrape") as "scrape" | "api",
};
