import type { Page } from "playwright";
import { sleep } from "../services/browser.js";
import type { LogLevel } from "../types.js";

export type JobLogger = (level: LogLevel, message: string, step?: string) => void;

/** Puppeteer-style evaluate: multiple args, function serialized as source. */
export async function evaluate<T>(
  page: Page,
  fn: (...args: any[]) => T,
  ...args: unknown[]
): Promise<T> {
  return page.evaluate(
    ({ src, a }: { src: string; a: unknown[] }) => {
      const g = globalThis as { __name?: (fn: unknown) => unknown };
      if (typeof g.__name === "undefined") g.__name = (fn) => fn;
      const runner = eval("(" + src + ")");
      return runner(...a);
    },
    { src: fn.toString(), a: args }
  );
}

export async function waitForFunction(
  page: Page,
  fn: (...args: any[]) => unknown,
  options: { timeout?: number } = {},
  ...args: unknown[]
): Promise<void> {
  await page.waitForFunction(
    ({ src, a }: { src: string; a: unknown[] }) => {
      const g = globalThis as { __name?: (fn: unknown) => unknown };
      if (typeof g.__name === "undefined") g.__name = (fn) => fn;
      const runner = eval("(" + src + ")");
      return runner(...a);
    },
    { src: fn.toString(), a: args },
    { timeout: options.timeout ?? 10000 }
  );
}

export async function waitForSelectorVisible(page: Page, selector: string, timeout = 10000) {
  await page.waitForSelector(selector, { state: "visible", timeout });
}

export async function waitForNav(page: Page, timeout = 10000) {
  await page.waitForLoadState("domcontentloaded", { timeout }).catch(() => {});
}

export async function navigateWithRetry(
  page: Page,
  url: string,
  { timeoutMs = 10000, maxRetries = 5 } = {}
): Promise<void> {
  let lastErr = "";
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`Loading page (attempt ${attempt}/${maxRetries}): ${url}`);
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: timeoutMs });
      console.log(`Page loaded: ${page.url()}`);
      return;
    } catch (err) {
      lastErr = err instanceof Error ? err.message : String(err);
      console.log(`Attempt ${attempt} failed: ${lastErr}`);
      if (attempt < maxRetries) await sleep(500);
    }
  }
  throw new Error(`Page failed to load after ${maxRetries} attempts (${url}): ${lastErr}`);
}

export async function waitWithRetry(
  page: Page,
  waitFn: () => Promise<void>,
  { label = "", timeoutMs = 10000, maxRetries = 5 } = {}
): Promise<void> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await waitFn();
      return;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.log(`${label || "Element"} not found (attempt ${attempt}/${maxRetries}): ${msg}`);
      if (attempt < maxRetries) {
        await sleep(500);
        try {
          await page.reload({ waitUntil: "domcontentloaded", timeout: 10000 });
        } catch {
          console.log(`Page refresh timed out on attempt ${attempt}`);
        }
        await sleep(300);
      }
    }
  }
  throw new Error(
    `${label || "Element"} not found after ${maxRetries} attempts (each waited ${timeoutMs / 1000}s)`
  );
}

export async function clearAndType(page: Page, selector: string, value: string, label = "") {
  console.log(`Typing into ${label || selector} ...`);
  await waitWithRetry(
    page,
    async () => {
      await waitForSelectorVisible(page, selector, 10000);
    },
    { label: label || selector, timeoutMs: 10000, maxRetries: 5 }
  );

  const jsSet = async (val: string) => {
    await evaluate(
      page,
      (sel: string, v: string) => {
        const el = document.querySelector(sel) as HTMLInputElement | HTMLTextAreaElement | null;
        if (!el) return;
        const proto =
          el instanceof HTMLTextAreaElement
            ? window.HTMLTextAreaElement.prototype
            : window.HTMLInputElement.prototype;
        const setter = Object.getOwnPropertyDescriptor(proto, "value")?.set;
        if (setter) setter.call(el, v);
        el.dispatchEvent(new Event("input", { bubbles: true }));
      },
      selector,
      val
    );
  };

  await page.focus(selector).catch(async () => {
    await evaluate(
      page,
      (sel: string) => {
        (document.querySelector(sel) as HTMLElement | null)?.click();
      },
      selector
    );
  });
  await jsSet("");
  await jsSet(value);
  await sleep(30);

  for (let attempt = 1; attempt <= 3; attempt++) {
    const actualValue = await evaluate(
      page,
      (sel: string) => {
        const el = document.querySelector(sel) as HTMLInputElement | HTMLTextAreaElement | null;
        return el?.value || "";
      },
      selector
    );
    if (actualValue === value) break;
    console.log(`Value mismatch on ${label || selector} (attempt ${attempt}/3). Reapplying...`);
    await jsSet(value);
    await sleep(50);
  }
  console.log(`Entered value into ${label || selector}`);
}
