import { getRedis } from "../lib/redis.js";
import type { CardDetails } from "../paymentStrategies/types.js";

/**
 * Which card should this order pay with?
 *
 * Round-robin was the obvious answer and it is the wrong one. Round-robin assigns
 * at ENQUEUE time, before any card has been tried, so the plan is fixed while the
 * facts are not: hand 500 orders to 50 cards, have card 3 die at order 40, and the
 * 9 orders already earmarked for card 3 are now guaranteed failures that each burn
 * an attempt from the batch budget. (It stays right for EMAILS, because a bad
 * session is caught up front and turned into a `skipped` job before anything is
 * queued — a card cannot be pre-checked that way.)
 *
 * So: a pool, consulted per order, at the moment of payment. A card that dies is
 * removed once and every worker sees it immediately; no order is ever
 * pre-committed to a card that may already be dead.
 *
 * State is Redis rather than Mongo for the same reason batchCounters.ts is: this
 * is contended, short-lived, and read on the hot path by every worker at once.
 * All mutation goes through Lua, never GET-then-SET in JS — with 200 workers a
 * read-modify-write in application code will hand the same card to several of them
 * at the instant one of them is killing it.
 *
 * Keys, all per batch so one batch can never poison another:
 *   cards:{batchId}:order    LIST   card ids, in preference order
 *   cards:{batchId}:dead     HASH   card id -> reason, excluded permanently
 *   cards:{batchId}:paused   HASH   card id -> unix ms until which it is skipped
 *   cards:{batchId}:used     HASH   card id -> successful orders placed
 */

const KEY_TTL_SEC = 60 * 60 * 24 * 3;

function orderKey(batchId: string) {
  return `cards:${batchId}:order`;
}
function deadKey(batchId: string) {
  return `cards:${batchId}:dead`;
}
function pausedKey(batchId: string) {
  return `cards:${batchId}:paused`;
}
function usedKey(batchId: string) {
  return `cards:${batchId}:used`;
}

/** Stable identity for a card within a batch. The PAN is the natural key but must
 *  never be a Redis key — keys turn up in SLOWLOG, MONITOR and metrics. Last 4
 *  plus position is unique within one batch and discloses nothing new. */
export function cardId(card: CardDetails, index: number): string {
  const last4 = String(card.cardNumber || "").replace(/\D/g, "").slice(-4);
  return `${index}:${last4}`;
}

/**
 * Why a card stopped being usable. The distinction is the whole point: treating
 * every failure as fatal means one flaky network moment permanently burns a card,
 * and treating none as fatal means a card with a wrong CVV is retried 500 times.
 */
export type CardVerdict =
  /** Bad data or a dead instrument — never try it again in this batch. */
  | { kind: "dead"; reason: string }
  /** Real card, temporarily out of room. Skipped until `untilMs`. */
  | { kind: "paused"; reason: string; untilMs: number }
  /** Not the card's fault (page timeout, session, OTP never arrived). Keep it. */
  | { kind: "keep"; reason: string };

/** Seed the pool once per batch. Idempotent: re-seeding an existing batch leaves
 *  its dead/paused state alone, so a retry cannot resurrect a dead card. */
export async function seedCardPool(batchId: string, cards: CardDetails[]): Promise<number> {
  if (!cards.length) return 0;
  const redis = getRedis();
  const ids = cards.map((c, i) => cardId(c, i));
  const seeded = await redis.eval(
    `
    local orderKey = KEYS[1]
    local ttl = tonumber(ARGV[1])
    if redis.call('EXISTS', orderKey) == 1 then
      return redis.call('LLEN', orderKey)
    end
    for i = 2, #ARGV do
      redis.call('RPUSH', orderKey, ARGV[i])
    end
    redis.call('EXPIRE', orderKey, ttl)
    return redis.call('LLEN', orderKey)
    `,
    1,
    orderKey(batchId),
    String(KEY_TTL_SEC),
    ...ids
  );
  return Number(seeded) || 0;
}

export interface AcquiredCard {
  card: CardDetails;
  id: string;
  index: number;
  /** Successful orders this card has already placed in this batch. */
  used: number;
}

export class NoCardAvailableError extends Error {}

/**
 * Claim the first usable card for this order.
 *
 * Deliberately NOT an exclusive lease, unlike the phone pool. A handset can serve
 * one OTP at a time, so two orders on one phone is a correctness bug. A card has
 * no such limit — banks happily take concurrent authorisations on one card — so
 * locking it would throttle the batch to one order at a time for no reason. What
 * has to be atomic is the dead/paused bookkeeping, not the handing out.
 *
 * Walks the preference list and returns the first card that is neither dead nor
 * still paused, expiring lapsed pauses as it goes.
 */
export async function acquireCard(batchId: string, cards: CardDetails[]): Promise<AcquiredCard> {
  if (!cards.length) throw new NoCardAvailableError("No cards were supplied with this order");
  const redis = getRedis();
  await seedCardPool(batchId, cards);

  const picked = (await redis.eval(
    `
    local orderKey  = KEYS[1]
    local deadKey   = KEYS[2]
    local pausedKey = KEYS[3]
    local usedKey   = KEYS[4]
    local now = tonumber(ARGV[1])

    local ids = redis.call('LRANGE', orderKey, 0, -1)
    for i = 1, #ids do
      local id = ids[i]
      if redis.call('HEXISTS', deadKey, id) == 0 then
        local until_ms = redis.call('HGET', pausedKey, id)
        if until_ms == false then
          return {id, redis.call('HGET', usedKey, id) or '0'}
        elseif tonumber(until_ms) <= now then
          -- Pause has lapsed: clear it so later passes stop re-checking.
          redis.call('HDEL', pausedKey, id)
          return {id, redis.call('HGET', usedKey, id) or '0'}
        end
      end
    end
    return nil
    `,
    4,
    orderKey(batchId),
    deadKey(batchId),
    pausedKey(batchId),
    usedKey(batchId),
    String(Date.now())
  )) as [string, string] | null;

  if (!picked) {
    const [dead, paused] = await Promise.all([
      redis.hgetall(deadKey(batchId)),
      redis.hgetall(pausedKey(batchId)),
    ]);
    throw new NoCardAvailableError(
      `No usable card left in batch ${batchId} — ` +
        `${Object.keys(dead).length} dead, ${Object.keys(paused).length} paused, of ${cards.length}. ` +
        `Reasons: ${Object.entries(dead).map(([id, why]) => `${id}=${why}`).join("; ") || "none"}`
    );
  }

  const [id, used] = picked;
  const index = Number(String(id).split(":")[0]);
  const card = cards[index];
  if (!card) throw new NoCardAvailableError(`Card ${id} is no longer in this batch's card list`);
  return { card, id, index, used: Number(used) || 0 };
}

/** Record the verdict after an attempt. Returns what the pool now thinks. */
export async function reportCardOutcome(
  batchId: string,
  id: string,
  verdict: CardVerdict
): Promise<void> {
  const redis = getRedis();
  if (verdict.kind === "dead") {
    await redis
      .multi()
      .hset(deadKey(batchId), id, verdict.reason.slice(0, 200))
      .expire(deadKey(batchId), KEY_TTL_SEC)
      .exec();
    return;
  }
  if (verdict.kind === "paused") {
    await redis
      .multi()
      .hset(pausedKey(batchId), id, String(verdict.untilMs))
      .expire(pausedKey(batchId), KEY_TTL_SEC)
      .exec();
    return;
  }
  // "keep" writes nothing: the card was never removed, so there is nothing to
  // undo, and recording every transient failure would only grow the key.
}

/** One more successful order on this card. */
export async function recordCardSuccess(batchId: string, id: string): Promise<number> {
  const redis = getRedis();
  const n = await redis.hincrby(usedKey(batchId), id, 1);
  await redis.expire(usedKey(batchId), KEY_TTL_SEC);
  return n;
}

/** Midnight IST, as unix ms — when a per-day card limit resets. */
function nextMidnightIst(now = Date.now()): number {
  const IST_OFFSET_MS = 5.5 * 60 * 60 * 1000;
  const istNow = now + IST_OFFSET_MS;
  const istMidnight = Math.floor(istNow / 86400000) * 86400000 + 86400000;
  return istMidnight - IST_OFFSET_MS;
}

/**
 * Turn a failed attempt into a verdict about the CARD.
 *
 * Built on the existing CheckoutErrorCode taxonomy rather than a second parallel
 * one, so a card is only ever blamed for a failure that checkoutErrors already
 * attributes to source: "BANK". Everything a PLATFORM failure describes — the
 * product, the cart, the session, the delivery SLA — is true no matter which card
 * is used, so blaming the card would retire the whole pool over a stock problem.
 *
 * The three buckets differ in cost of being wrong, which is why they exist:
 *   dead   — wrongly dead loses a card for the batch
 *   paused — wrongly paused loses it for hours
 *   keep   — wrongly kept costs one more failed attempt
 * So anything ambiguous goes to `keep`, the cheapest mistake.
 */
export function verdictFor(code: string, detail = ""): CardVerdict {
  const text = `${code} ${detail}`.toLowerCase();

  // Bad data on the card itself. No number of retries fixes a wrong CVV, and
  // every remaining order on this card would fail identically.
  if (/incorrect cvv|invalid cvv|wrong cvv|invalid card|card (?:number )?invalid|expired card|card expired|do not honour|do not honor|restricted card|stolen|lost card/.test(text)) {
    return { kind: "dead", reason: detail || code || "card rejected as invalid" };
  }
  // A wrong password/PIN is the credential, not the card — but it is still
  // per-card data from the same CSV row, so it will fail every time too.
  if (/incorrect password|invalid password|wrong password|incorrect pin|invalid pin|wrong pin/.test(text)) {
    return { kind: "dead", reason: detail || "card credential rejected" };
  }
  // Real card, no room right now. Limits reset daily, so pause rather than retire.
  if (code === "INSUFFICIENT_BALANCE" || /insufficient (?:balance|funds)|limit exceeded|exceeds .*limit|daily limit|transaction limit/.test(text)) {
    return {
      kind: "paused",
      reason: detail || "insufficient balance / limit reached",
      untilMs: nextMidnightIst(),
    };
  }
  // Explicitly not the card: the OTP never arrived (that is the handset or the
  // bank's SMS), the session died, the page timed out. Keep it in the pool.
  if (code === "OTP_TIMEOUT" || code === "OTP_NOT_FOUND" || code === "SESSION_EXPIRED") {
    return { kind: "keep", reason: detail || code };
  }
  // CARD_AUTH_FAILED with nothing more specific is genuinely ambiguous — a wrong
  // credential and a 3-D Secure hiccup look identical from outside. Keep, because
  // a repeat will carry more detail and the cost of keeping is one attempt.
  return { kind: "keep", reason: detail || code || "unclassified" };
}

/** Operational view for the batch endpoint — never includes a card number. */
export async function cardPoolState(batchId: string) {
  const redis = getRedis();
  const [order, dead, paused, used] = await Promise.all([
    redis.lrange(orderKey(batchId), 0, -1),
    redis.hgetall(deadKey(batchId)),
    redis.hgetall(pausedKey(batchId)),
    redis.hgetall(usedKey(batchId)),
  ]);
  const now = Date.now();
  return order.map((id) => ({
    id,
    dead: dead[id] || null,
    paused_until: paused[id] && Number(paused[id]) > now ? Number(paused[id]) : null,
    orders_placed: Number(used[id] || 0),
  }));
}
