/**
 * Keeping the API process alive through the things that are not its fault:
 * the Mac's internet dropping, Atlas electing a new primary, DNS failing for
 * a minute, Redis restarting. None of those are programmer errors, so none of
 * them should end the process — the next request or the next tick simply
 * tries again once the network is back.
 *
 *   isTransientError(err)   network / DB / Redis outage, as opposed to a bug
 *   safeTask(label, fn)     an async job that logs instead of rejecting — for
 *                           every setInterval / cron / fire-and-forget call
 *   throttledWarn(key, msg) one line per key per minute, so an outage does not
 *                           write ten thousand identical stack traces
 */

const TRANSIENT_PATTERNS: RegExp[] = [
  /MongoServerSelectionError/i,
  /MongoNetworkError/i,
  /MongoNetworkTimeoutError/i,
  /MongoTopologyClosedError/i,
  /MongoNotConnectedError/i,
  /MongoPoolClearedError/i,
  /MongoExpiredSessionError/i,
  /PoolClearedOnNetworkError/i,
  /topology (was|is) (destroyed|closed)/i,
  /server selection timed out/i,
  /buffering timed out/i,
  /connection .* closed/i,
  /ReplicaSetNoPrimary/i,
  /getaddrinfo (ENOTFOUND|EAI_AGAIN)/i,
  /\b(ENOTFOUND|EAI_AGAIN|ECONNRESET|ECONNREFUSED|ETIMEDOUT|EPIPE|EHOSTUNREACH|ENETUNREACH|ENETDOWN)\b/,
  /socket hang up/i,
  /Client network socket disconnected/i,
  /read ECONNRESET/i,
  /write EPIPE/i,
  /fetch failed/i,
  /Redis (is already connecting|connection .* closed)/i,
  /Connection is closed/i,
  /MaxRetriesPerRequestError/i,
  /Stream isn't writeable/i,
];

/** A network, database or cache outage — retrying later is the right response, crashing is not. */
export function isTransientError(err: unknown): boolean {
  const seen = new Set<unknown>();
  let cur: unknown = err;
  for (let depth = 0; cur && depth < 6 && !seen.has(cur); depth += 1) {
    seen.add(cur);
    const e = cur as { name?: string; message?: string; code?: unknown; cause?: unknown; reason?: unknown };
    const text = `${e.name || ""} ${e.message || ""} ${typeof e.code === "string" ? e.code : ""}`;
    if (TRANSIENT_PATTERNS.some((re) => re.test(text))) return true;
    cur = e.cause ?? (e.reason && typeof e.reason === "object" && "type" in (e.reason as object) ? null : e.reason);
  }
  return false;
}

export function describeError(err: unknown): string {
  if (err instanceof Error) {
    const first = err.message.split("\n")[0];
    const cause = (err as { cause?: unknown }).cause;
    const causeText = cause instanceof Error ? ` ← ${cause.message.split("\n")[0]}` : "";
    return `${err.name}: ${first}${causeText}`.slice(0, 400);
  }
  return String(err).slice(0, 400);
}

const lastWarn = new Map<string, number>();
const WARN_EVERY_MS = 60_000;

/** One warning per key per minute; the rest are counted and reported on the next line that gets through. */
export function throttledWarn(key: string, message: string): void {
  const now = Date.now();
  const entry = lastWarn.get(key);
  const count = suppressed.get(key) || 0;
  if (entry && now - entry < WARN_EVERY_MS) {
    suppressed.set(key, count + 1);
    return;
  }
  lastWarn.set(key, now);
  suppressed.set(key, 0);
  console.warn(`${message}${count ? ` (+${count} similar in the last minute)` : ""}`);
}
const suppressed = new Map<string, number>();

/**
 * Wrap a background job so it can never bring the process down. A transient
 * outage is a throttled warning; anything else is logged in full (it is a bug
 * worth seeing) but still does not end the process — the loop runs again.
 */
export function safeTask<T>(label: string, fn: () => Promise<T>): () => Promise<T | undefined> {
  return async () => {
    try {
      return await fn();
    } catch (err) {
      if (isTransientError(err)) throttledWarn(`task:${label}`, `[${label}] skipped this run — ${describeError(err)}`);
      else console.error(`[${label}] failed:`, err);
      return undefined;
    }
  };
}

/** Fire-and-forget with the same guarantee: `void guarded("x", promise)` instead of `void promise`. */
export function guarded(label: string, p: Promise<unknown>): void {
  p.catch((err) => {
    if (isTransientError(err)) throttledWarn(`task:${label}`, `[${label}] — ${describeError(err)}`);
    else console.error(`[${label}] failed:`, err);
  });
}
