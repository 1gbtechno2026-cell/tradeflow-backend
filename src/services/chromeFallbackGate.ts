/**
 * How many HTTP-engine jobs may be in the Chrome page flow at once in this
 * process. The checkout-http queue runs tens of jobs concurrently because an
 * HTTP order costs no browser; when the engine hands an order back
 * ("fallback"), that order opens the usual two Chromes. Without a gate, a
 * burst of fallbacks (an outage, a captcha wave) would open as many Chromes
 * as the HTTP concurrency and take the machine down.
 *
 *   HTTP_FALLBACK_CHROME_MAX=2   default; 0 is not allowed (a fallback must be able to run)
 */
const MAX = Math.max(1, Math.floor(Number(process.env.HTTP_FALLBACK_CHROME_MAX) || 2));
let active = 0;
const waiters: Array<() => void> = [];

export function chromeFallbackGateStatus() {
  return { max: MAX, active, waiting: waiters.length };
}

export async function acquireChromeFallbackSlot(): Promise<() => void> {
  if (active >= MAX) await new Promise<void>((resolve) => waiters.push(resolve));
  active += 1;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    active -= 1;
    waiters.shift()?.();
  };
}
