import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { Browser, BrowserContext, Page, Route } from "playwright";
// `devices` comes from playwright itself; the launcher is playwright-extra's so it
// carries the stealth plugin. Mixing the two is intentional and safe — the device
// descriptors are plain data.
import { devices, chromium as playwrightChromium } from "playwright";
import { addExtra, chromium } from "playwright-extra";
import stealthPlugin from "puppeteer-extra-plugin-stealth";
import { config } from "../config.js";

chromium.use(stealthPlugin());

const CHROME_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

const TIMEZONE_ALIASES: Record<string, string> = {
  "Asia/Calcutta": "Asia/Kolkata",
  "US/Eastern": "America/New_York",
  "US/Central": "America/Chicago",
  "US/Mountain": "America/Denver",
  "US/Pacific": "America/Los_Angeles",
};

function resolveTimezoneId() {
  const tz = Intl.DateTimeFormat().resolvedOptions().timeZone || "";
  return TIMEZONE_ALIASES[tz] || tz || "Asia/Kolkata";
}

export function getChromePath() {
  if (config.chromePath && fs.existsSync(config.chromePath)) return config.chromePath;
  if (process.env.CHROME_PATH && fs.existsSync(process.env.CHROME_PATH)) {
    return process.env.CHROME_PATH;
  }
  const platform = os.platform();
  if (platform === "darwin") {
    const macChrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
    if (fs.existsSync(macChrome)) return macChrome;
  } else if (platform === "win32") {
    const candidates = [
      path.join(process.env.PROGRAMFILES || "", "Google", "Chrome", "Application", "chrome.exe"),
      path.join(process.env["PROGRAMFILES(X86)"] || "", "Google", "Chrome", "Application", "chrome.exe"),
      path.join(process.env.LOCALAPPDATA || "", "Google", "Chrome", "Application", "chrome.exe"),
    ];
    for (const p of candidates) {
      if (p && fs.existsSync(p)) return p;
    }
  } else {
    for (const linuxChrome of [
      "/usr/bin/google-chrome",
      "/usr/bin/google-chrome-stable",
      "/usr/bin/chromium-browser",
      "/usr/bin/chromium",
    ]) {
      if (fs.existsSync(linuxChrome)) return linuxChrome;
    }
  }
  return null;
}

function stealthLaunchArgs() {
  const args = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-features=IsolateOrigins,site-per-process",
    "--window-size=1280,720",
    "--no-first-run",
    "--start-maximized",
    "--disable-popup-blocking",
  ];
  if (process.platform === "linux") args.push("--no-zygote");
  return args;
}

/**
 * Launch Chrome with the stealth flags, without opening a context.
 *
 * Exists so one browser can host BOTH a desktop and a mobile context — which the
 * checkout flow needs, because Flipkart serves two different sites and the
 * automation needs one leg on each. launchStealthContext below is unchanged and
 * still the right call for anything that wants a single desktop context.
 */
export async function launchStealthBrowser(options?: { headless?: boolean }): Promise<Browser> {
  const executablePath = getChromePath();
  // Visible Chrome unless the caller explicitly asks for headless true.
  const headless = options?.headless ?? config.headless;
  // Local Mac/Windows: real Google Chrome. Linux/Fargate: Playwright Chromium
  // (channel:"chrome" is not installed in the container image).
  const launchTarget = executablePath
    ? { executablePath }
    : process.platform === "linux"
      ? {}
      : { channel: "chrome" as const };
  return chromium.launch({
    headless,
    slowMo: config.slowMoMs,
    ignoreDefaultArgs: ["--enable-automation"],
    handleSIGINT: false,
    handleSIGTERM: false,
    handleSIGHUP: false,
    args: stealthLaunchArgs(),
    ...launchTarget,
  });
}

/** How tall the emulated phone is. Width stays the device's own — see mobileContext. */
const MOBILE_VIEWPORT_HEIGHT = Number(process.env.MOBILE_VIEWPORT_HEIGHT || 1400);

/** Desktop Flipkart: account pages, the address book, the cart. */
export async function desktopContext(browser: Browser): Promise<BrowserContext> {
  const context = await browser.newContext({
    viewport: { width: 1280, height: 720 },
    locale: "en-US",
    timezoneId: resolveTimezoneId(),
    colorScheme: "light",
    userAgent: CHROME_UA,
    hasTouch: true,
  });
  await applyStealth(context);
  return context;
}

/**
 * Mobile Flipkart (the m-site), via real device emulation.
 *
 * This is not cosmetic. Flipkart chooses which site to serve from the User-Agent,
 * and FlipkartCheckout is m-site automation: it looks for #msite-bottomsheet and
 * drives the page with touchscreen.tap. Run it in a desktop context and the
 * bottom sheet it needs to set the delivery pincode does not exist — add-to-cart
 * appears to work (the header count goes to 1), then Flipkart drops the item
 * server side and asks for a pincode, which is exactly the
 * "Item removed from cart / Enter Delivery Pincode" failure seen on 2026-09-30.
 *
 * hasTouch alone is not enough — it makes touchscreen.tap succeed against the
 * DESKTOP dom, which is why the failure looked like a selector problem.
 *
 * The device's own userAgent/viewport/deviceScaleFactor/isMobile must win here, so
 * CHROME_UA is deliberately NOT applied.
 */
export async function mobileContext(browser: Browser, deviceName?: string): Promise<BrowserContext> {
  const name = deviceName || config.mobileDevice;
  const device = devices[name];
  if (!device) throw new Error(`Unknown Playwright device "${name}"`);
  const context = await browser.newContext({
    ...device,
    // TALLER, never wider.
    //
    // Flipkart's breakpoint is driven by WIDTH (and the UA). Widening this would
    // flip it back to the desktop DOM and undo the whole m-site fix. Height is
    // free: more of the page is on screen at once, so drawers, APPLY buttons and
    // sticky bars are in view rather than below the fold, and anything that reads
    // geometry or visibility has more to work with.
    viewport: { width: device.viewport?.width ?? 412, height: MOBILE_VIEWPORT_HEIGHT },
    locale: "en-IN",
    timezoneId: resolveTimezoneId(),
    colorScheme: "light",
  });
  await applyStealth(context);
  return context;
}

/**
 * A Chrome for the mobile leg, launched WITHOUT the stealth plugin's
 * user-agent-override evasion.
 *
 * That evasion exists to make a headless desktop look like real desktop Chrome, so
 * it rewrites navigator.userAgent — and it wins over Playwright's device
 * descriptor. Measured:
 *
 *   devices["Pixel 7"].userAgent   Mozilla/5.0 (Linux; Android 14; Pixel 7) ...
 *   plain playwright + device      Mozilla/5.0 (Linux; Android 14; Pixel 7) ...   ok
 *   playwright-extra + stealth     Mozilla/5.0 (Macintosh; Intel Mac OS X ...)    wrong
 *
 * With the desktop UA restored, Flipkart serves the DESKTOP site no matter what the
 * viewport says — so mobileContext() on the shared browser silently did nothing,
 * and #msite-bottomsheet still did not exist. Dropping just that one evasion gives
 * the Android UA back while keeping every other stealth patch, and Flipkart then
 * serves the m-site ("Location not set / Select delivery location").
 *
 * A separate launcher instance rather than mutating the shared one, because
 * playwright-extra's `chromium` is a singleton and the login and verify flows
 * depend on its current behaviour.
 */
export async function launchMobileBrowser(options?: { headless?: boolean }): Promise<Browser> {
  const plug = stealthPlugin();
  plug.enabledEvasions.delete("user-agent-override");
  const launcher = addExtra(playwrightChromium);
  launcher.use(plug);

  const executablePath = getChromePath();
  const headless = options?.headless ?? config.headless;
  const launchTarget = executablePath
    ? { executablePath }
    : process.platform === "linux"
      ? {}
      : { channel: "chrome" as const };
  return launcher.launch({
    headless,
    slowMo: config.slowMoMs,
    ignoreDefaultArgs: ["--enable-automation"],
    handleSIGINT: false,
    handleSIGTERM: false,
    handleSIGHUP: false,
    args: stealthLaunchArgs(),
    ...launchTarget,
  });
}

export async function launchStealthContext(options?: { headless?: boolean }) {
  const browser = await launchStealthBrowser(options);
  const context = await desktopContext(browser);
  return { browser, context };
}

/** The webdriver/chrome shims plus the Logout neutraliser, applied to a context. */
async function applyStealth(context: BrowserContext) {
  await context.addInitScript(`
    try {
      Object.defineProperty(navigator, "webdriver", {
        get: function () { return undefined; },
        configurable: true,
      });
    } catch (e) {}
    try { window.chrome = window.chrome || { runtime: {} }; } catch (e) {}
    (function neutralizeFlipkartLogout() {
      const kill = () => {
        try {
          const nodes = document.querySelectorAll("a,button,span,div,li");
          for (const el of nodes) {
            const text = (el.textContent || "").replace(/\\s+/g, " ").trim();
            const href = String(el.getAttribute("href") || "");
            if (!/^logout$/i.test(text) && !/\\/(?:account\\/)?logout(?:\\/|\\?|$)/i.test(href)) continue;
            el.addEventListener("click", (e) => { e.preventDefault(); e.stopPropagation(); }, true);
            el.setAttribute("href", "#");
            if (el instanceof HTMLElement) el.style.pointerEvents = "none";
          }
        } catch (e) {}
      };
      kill();
      try {
        new MutationObserver(kill).observe(document.documentElement, { childList: true, subtree: true });
      } catch (e) {}
    })();
  `);
}

export async function closeBrowser(browser: Browser, context: BrowserContext) {
  await context.close().catch(() => {});
  await browser.close().catch(() => {});
}

function isFlipkartHost(value: string) {
  return /flipkart/i.test(value);
}

function mapSameSite(value: unknown): "Strict" | "Lax" | "None" {
  const s = String(value || "Lax");
  if (/strict/i.test(s)) return "Strict";
  if (/none/i.test(s)) return "None";
  return "Lax";
}

function plainCookie(raw: unknown): Record<string, unknown> | null {
  if (!raw || typeof raw !== "object") return null;
  const c = raw as Record<string, unknown> & { toObject?: () => Record<string, unknown> };
  if (typeof c.toObject === "function") {
    try {
      return c.toObject();
    } catch {
      /* use raw */
    }
  }
  return c;
}

export function mapPlaywrightCookies(cookies: unknown[]) {
  const mapped: Array<{
    name: string;
    value: string;
    domain: string;
    path: string;
    httpOnly: boolean;
    secure: boolean;
    sameSite: "Strict" | "Lax" | "None";
    expires?: number;
  }> = [];
  for (const raw of cookies || []) {
    const c = plainCookie(raw);
    if (!c?.name || c.value == null || c.value === "" || !c.domain) continue;
    const sameSite = mapSameSite(c.sameSite);
    let expires: number | undefined;
    const exp = Number(c.expires);
    if (Number.isFinite(exp) && exp > 0) {
      expires = exp > 1e12 ? Math.floor(exp / 1000) : exp;
    }
    mapped.push({
      name: String(c.name),
      value: String(c.value),
      domain: String(c.domain).replace(/^https?:\/\//, ""),
      path: String(c.path || "/"),
      httpOnly: Boolean(c.httpOnly),
      secure: sameSite === "None" ? true : Boolean(c.secure),
      sameSite,
      ...(expires ? { expires } : {}),
    });
  }
  return mapped;
}

/**
 * Clear Flipkart cookies in THIS throwaway Playwright context only,
 * so restore can attach the Mongo copy. Does not call Flipkart Logout
 * and does not write Mongo. Local cookie delete does not invalidate
 * the server session stored in PlatformId.
 */
export async function clearLocalThrowawayFlipkartCookies(page: Page) {
  const context = page.context();
  const fk = (await context.cookies()).filter((c) => isFlipkartHost(c.domain));
  for (const c of fk) {
    await context.clearCookies({ name: c.name, domain: c.domain, path: c.path || "/" }).catch(() => {});
  }
}

export function flipkartLoginUrl(url: string) {
  return /\/(?:account\/)?login(?:\/|\?|$)/i.test(url);
}

/** Abort any Flipkart logout request so a stray click cannot kill the saved session. */
export async function blockFlipkartLogout(page: Page) {
  const guard = async (route: Route) => {
    const url = route.request().url();
    if (/flipkart\.com/i.test(url) && /\/(?:account\/)?logout(?:\/|\?|$)/i.test(url)) {
      console.warn(`[session-guard] blocked logout request: ${url}`);
      await route.abort();
      return;
    }
    await route.continue();
  };
  // Context-level so popups are covered. Regex route only — do not intercept every request.
  await page.context().route(/logout/i, guard);
}

/** Restore cookies BEFORE opening any Flipkart page. Never clicks Logout. */
export async function restoreFlipkartSession(page: Page, cookies: unknown[]) {
  await clearLocalThrowawayFlipkartCookies(page);
  const mapped = mapPlaywrightCookies(cookies);
  if (!mapped.length) {
    throw new Error("Saved Flipkart session has no usable cookies. Re-login this ID.");
  }
  await page.context().addCookies(mapped);
  await sleep(300);
  const live = await page.context().cookies("https://www.flipkart.com");
  const names = [...new Set(live.filter((c) => isFlipkartHost(c.domain)).map((c) => c.name))];
  const hasAuth = names.includes("T") || names.includes("SN") || names.includes("at");
  if (!hasAuth) {
    throw new Error(
      `Could not attach Flipkart login cookies (got: ${names.join(", ") || "none"}). Re-login this ID.`
    );
  }
  return { count: mapped.length, names };
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
