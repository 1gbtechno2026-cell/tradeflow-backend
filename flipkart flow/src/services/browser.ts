import fs from "node:fs";
import path from "node:path";
import { chromium, devices, type Browser, type BrowserContext, type BrowserContextOptions } from "playwright";

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export const STATE_PATH = path.resolve(".auth", "flipkart-state.json");

/** esbuild/tsx keepNames injects __name() into serialized page functions — make it a no-op in the page. */
const NAME_SHIM = "globalThis.__name = globalThis.__name || ((f) => f);";

export async function launchBrowser(headless: boolean): Promise<Browser> {
  return chromium.launch({
    headless,
    args: ["--disable-blink-features=AutomationControlled"],
  });
}

async function newContext(browser: Browser, options: BrowserContextOptions): Promise<BrowserContext> {
  const context = await browser.newContext({
    ...options,
    locale: "en-IN",
    timezoneId: "Asia/Kolkata",
    storageState: fs.existsSync(STATE_PATH) ? STATE_PATH : undefined,
  });
  await context.addInitScript(NAME_SHIM);
  return context;
}

export function desktopContext(browser: Browser): Promise<BrowserContext> {
  return newContext(browser, { viewport: { width: 1440, height: 900 } });
}

/** Mobile emulation (touch + mobile UA) — Flipkart serves the m-site checkout used by FlipkartCheckout2. */
export function mobileContext(browser: Browser, deviceName = "Pixel 7"): Promise<BrowserContext> {
  const device = devices[deviceName];
  if (!device) throw new Error(`Unknown Playwright device "${deviceName}"`);
  return newContext(browser, { ...device });
}

export async function saveSession(context: BrowserContext): Promise<void> {
  fs.mkdirSync(path.dirname(STATE_PATH), { recursive: true });
  await context.storageState({ path: STATE_PATH });
}
