import type { Page } from "playwright";
import { sleep } from "../services/browser.js";

export type JobLogger = (level: "info" | "warn" | "error", message: string, step?: string) => void;

/**
 * Build a page-side expression that calls `fn(...args)`. Playwright's evaluate
 * only takes one argument, and FlipkartCheckout2 passes several, so the
 * function source and JSON args are inlined. Runs via CDP, so page CSP does not apply.
 */
function callExpression(fn: (...args: any[]) => unknown, args: unknown[]): string {
  return `(() => {
    globalThis.__name = globalThis.__name || ((f) => f);
    return (${fn.toString()})(...${JSON.stringify(args)});
  })()`;
}

export async function evaluate<T>(page: Page, fn: (...args: any[]) => T, ...args: unknown[]): Promise<T> {
  return (await page.evaluate(callExpression(fn, args))) as T;
}

export async function waitForFunction(
  page: Page,
  fn: (...args: any[]) => unknown,
  options: { timeout?: number } = {},
  ...args: unknown[]
): Promise<void> {
  await page.waitForFunction(callExpression(fn, args), undefined, {
    timeout: options.timeout ?? 10000,
    polling: 100,
  });
}

export async function waitForNav(page: Page, timeoutMs: number): Promise<void> {
  await page.waitForLoadState("domcontentloaded", { timeout: timeoutMs });
}

export async function navigateWithRetry(
  page: Page,
  url: string,
  options: { timeoutMs?: number; maxRetries?: number } = {}
): Promise<void> {
  const maxRetries = options.maxRetries ?? 3;
  let lastErr: unknown;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: options.timeoutMs ?? 15000 });
      return;
    } catch (err) {
      lastErr = err;
      const msg = err instanceof Error ? err.message : String(err);
      console.log(`[nav] ${url} failed (attempt ${attempt}/${maxRetries}): ${msg.split("\n")[0]}`);
      // Slow load but the page did arrive — good enough.
      if (/Timeout/i.test(msg) && page.url().split("?")[0] === url.split("?")[0]) return;
      await sleep(1000);
    }
  }
  throw new Error(`Could not open ${url} after ${maxRetries} attempts: ${lastErr instanceof Error ? lastErr.message : lastErr}`);
}

export async function waitWithRetry(
  page: Page,
  fn: () => Promise<void>,
  options: { label: string; timeoutMs?: number; maxRetries?: number }
): Promise<void> {
  const maxRetries = options.maxRetries ?? 3;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await fn();
      return;
    } catch (err) {
      const msg = err instanceof Error ? err.message.split("\n")[0] : String(err);
      if (attempt === maxRetries) {
        throw new Error(`${options.label} not found after ${maxRetries} attempts (${page.url()}): ${msg}`);
      }
      console.log(`[wait] ${options.label} not ready (attempt ${attempt}/${maxRetries}): ${msg}`);
      await sleep(500);
    }
  }
}
