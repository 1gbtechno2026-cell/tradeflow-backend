import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { Browser, BrowserContext, Page, Route } from "playwright";
import { chromium } from "playwright-extra";
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

export async function launchStealthContext(options?: { headless?: boolean }) {
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
  const browser = await chromium.launch({
    headless,
    slowMo: config.slowMoMs,
    ignoreDefaultArgs: ["--enable-automation"],
    handleSIGINT: false,
    handleSIGTERM: false,
    handleSIGHUP: false,
    args: stealthLaunchArgs(),
    ...launchTarget,
  });

  const context = await browser.newContext({
    viewport: { width: 1280, height: 720 },
    locale: "en-US",
    timezoneId: resolveTimezoneId(),
    colorScheme: "light",
    userAgent: CHROME_UA,
    hasTouch: true,
  });

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

  return { browser, context };
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
