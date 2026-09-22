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
};
