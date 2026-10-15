import { getRedis } from "../lib/redis.js";

const ACCOUNT_LOCK_MS = Number(process.env.ACCOUNT_LOCK_MS || 15 * 60 * 1000);

/**
 * One piece of work per Flipkart account at a time — checkout, cancel and
 * order-sync all take this same lock. The cart is shared per account and the
 * first step of every checkout EMPTIES it, so two concurrent jobs on one login
 * would each throw away the other's item; a cancel or a sync landing in the
 * middle of a checkout is the same login used twice at once. Redis SET NX with
 * a TTL, released by the holder only (compare-and-delete), so a dead worker's
 * hold lapses on its own.
 */
export async function lockAccount(
  email: string,
  holder: string,
  ttlMs = ACCOUNT_LOCK_MS
): Promise<(() => Promise<void>) | null> {
  const redis = getRedis();
  const key = `account:lock:${String(email || "").trim().toLowerCase()}`;
  const ok = await redis.set(key, holder, "PX", ttlMs, "NX");
  if (ok !== "OK") return null;
  return async () => {
    await redis
      .eval(`if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0`, 1, key, holder)
      .catch(() => undefined);
  };
}
