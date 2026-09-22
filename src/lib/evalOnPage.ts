import type { Page } from "playwright";

/** Run a browser function stored as a string (avoids tsx injecting `__name`). */
export async function evalOnPage<T>(page: Page, fnSource: string, ...args: unknown[]): Promise<T> {
  const runner = new Function(
    "payload",
    'var fn = eval("(" + payload.fnSource + ")"); return fn.apply(null, payload.args);'
  );
  return page.evaluate(runner as never, { fnSource, args }) as Promise<T>;
}

export function isDestroyedContext(err: unknown) {
  const msg = err instanceof Error ? err.message : String(err);
  return /execution context was destroyed|most likely because of a navigation|target closed|has been closed/i.test(
    msg
  );
}
