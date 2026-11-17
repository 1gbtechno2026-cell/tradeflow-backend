import { getRedis } from "../lib/redis.js";
import { usableOrderingPhones } from "../lib/phone.js";

/** Same 3-day horizon the card pool keeps its per-batch Redis keys for. */
const KEY_TTL_SEC = 60 * 60 * 24 * 3;

function rotKey(batchId: string): string {
  return `ordering-phone:${batchId}:rot`;
}

/**
 * The next ordering phone for ONE order, round-robin across the whole batch.
 *
 * Rotation is per order, not per account: a single atomic INCR on a
 * batch-scoped Redis counter advances the position, so consecutive orders —
 * whichever worker or account places them — take consecutive pool numbers and
 * wrap at the end. The counter is the only shared state; the pool itself is
 * read from the job (job.request.orderingPhones), identical for every worker.
 *
 * Returns "" when the pool holds no usable (10-digit) number — the caller
 * fails the order. A transient Redis error does not strand an order: it falls
 * back to a random pool number (still spreads load, just not strict order).
 */
export async function nextOrderingPhone(pool: string[], batchId: string): Promise<string> {
  const usable = usableOrderingPhones(pool);
  if (usable.length === 0) return "";

  let idx: number;
  try {
    const redis = getRedis();
    const n = await redis.incr(rotKey(batchId));
    // First increment of this batch's key — give it a TTL so finished batches
    // don't leave counters behind in Redis forever.
    if (n === 1) await redis.expire(rotKey(batchId), KEY_TTL_SEC);
    idx = (n - 1) % usable.length;
  } catch {
    idx = Math.floor(Math.random() * usable.length);
  }
  return usable[idx];
}
