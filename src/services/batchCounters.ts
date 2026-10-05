import { Redis } from "ioredis";
import { config } from "../config.js";

/**
 * Batch-wide counters shared by every worker.
 * Keys:
 *   batch:{batchId}:purchasedQuantity
 *   batch:{batchId}:attemptsUsed
 *   batch:{batchId}:status   completed | exhausted
 *
 * All mutations go through INCRBY/DECRBY or Lua — never GET then SET in JS.
 *
 * There is no "filtered" state any more. A batch used to be stopped the moment
 * one job saw out-of-stock / not-deliverable; now every selected platform ID is
 * attempted and reports its own reason, because a product refused to one
 * account may be sold to another. The only things that end a batch early are
 * reaching the quantity target and running out of attempts.
 */

const KEY_TTL_SEC = 60 * 60 * 24 * 14;

let client: Redis | null = null;

export function getBatchRedis(): Redis {
  if (!client) {
    const u = new URL(config.redisUrl);
    client = new Redis({
      host: u.hostname,
      port: Number(u.port || 6376),
      password: u.password || undefined,
      maxRetriesPerRequest: null,
    });
  }
  return client;
}

export function purchasedKey(batchId: string) {
  return `batch:${batchId}:purchasedQuantity`;
}
export function attemptsKey(batchId: string) {
  return `batch:${batchId}:attemptsUsed`;
}
export function statusKey(batchId: string) {
  return `batch:${batchId}:status`;
}

export type BatchStatus = "running" | "completed" | "exhausted";

export interface BatchProgress {
  status: BatchStatus;
  purchasedQuantity: number;
  attemptsUsed: number;
}

export type ReserveResult =
  | { kind: "ok"; purchasedQuantity: number; attemptsUsed: number }
  | { kind: "skip_target"; purchasedQuantity: number; attemptsUsed: number }
  | { kind: "skip_exhausted"; purchasedQuantity: number; attemptsUsed: number };

const RESERVE_LUA = `
local purchasedKey = KEYS[1]
local attemptsKey = KEYS[2]
local statusKey = KEYS[3]
local perOrder = tonumber(ARGV[1])
local totalQty = tonumber(ARGV[2])
local totalAttempts = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local function nums()
  return tonumber(redis.call('GET', purchasedKey) or '0'), tonumber(redis.call('GET', attemptsKey) or '0')
end

local function touch()
  redis.call('EXPIRE', purchasedKey, ttl)
  redis.call('EXPIRE', attemptsKey, ttl)
  redis.call('EXPIRE', statusKey, ttl)
end

local status = redis.call('GET', statusKey)
if status == 'completed' then
  local p, a = nums()
  return {'skip_target', p, a}
end
if status == 'exhausted' then
  local p, a = nums()
  return {'skip_exhausted', p, a}
end

local purchased = tonumber(redis.call('GET', purchasedKey) or '0')
if purchased >= totalQty then
  redis.call('SET', statusKey, 'completed')
  touch()
  local p, a = nums()
  return {'skip_target', p, a}
end

local newQty = redis.call('INCRBY', purchasedKey, perOrder)
if newQty > totalQty then
  local restored = redis.call('DECRBY', purchasedKey, perOrder)
  if restored < 0 then
    redis.call('SET', purchasedKey, '0')
    restored = 0
  end
  if restored >= totalQty then
    redis.call('SET', statusKey, 'completed')
  end
  touch()
  local a = tonumber(redis.call('GET', attemptsKey) or '0')
  return {'skip_target', restored, a}
end

local attempts = redis.call('INCR', attemptsKey)
if attempts > totalAttempts then
  local restored = redis.call('DECRBY', purchasedKey, perOrder)
  if restored < 0 then
    redis.call('SET', purchasedKey, '0')
    restored = 0
  end
  redis.call('SET', statusKey, 'exhausted')
  touch()
  return {'skip_exhausted', restored, attempts}
end

touch()
return {'ok', newQty, attempts}
`;

const RELEASE_LUA = `
local purchasedKey = KEYS[1]
local perOrder = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local restored = redis.call('DECRBY', purchasedKey, perOrder)
if restored < 0 then
  redis.call('SET', purchasedKey, '0')
  restored = 0
end
redis.call('EXPIRE', purchasedKey, ttl)
return restored
`;

const PROGRESS_LUA = `
local purchasedKey = KEYS[1]
local attemptsKey = KEYS[2]
local statusKey = KEYS[3]
local totalQty = tonumber(ARGV[1])
local totalAttempts = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local purchased = tonumber(redis.call('GET', purchasedKey) or '0')
local attempts = tonumber(redis.call('GET', attemptsKey) or '0')
local status = redis.call('GET', statusKey)
if purchased >= totalQty then
  redis.call('SET', statusKey, 'completed')
  redis.call('EXPIRE', statusKey, ttl)
  return {'completed', purchased, attempts}
end
if attempts >= totalAttempts and purchased < totalQty then
  redis.call('SET', statusKey, 'exhausted')
  redis.call('EXPIRE', statusKey, ttl)
  return {'exhausted', purchased, attempts}
end
return {status or 'running', purchased, attempts}
`;

function asInt(v: unknown): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

export async function initBatchCounters(batchId: string): Promise<void> {
  const r = getBatchRedis();
  await r.set(purchasedKey(batchId), "0", "NX");
  await r.set(attemptsKey(batchId), "0", "NX");
  await r.expire(purchasedKey(batchId), KEY_TTL_SEC);
  await r.expire(attemptsKey(batchId), KEY_TTL_SEC);
}

export async function reserveBatchSlot(input: {
  batchId: string;
  quantityPerOrder: number;
  totalQuantity: number;
  totalAttempts: number;
}): Promise<ReserveResult> {
  const r = getBatchRedis();
  const raw = (await r.eval(
    RESERVE_LUA,
    3,
    purchasedKey(input.batchId),
    attemptsKey(input.batchId),
    statusKey(input.batchId),
    String(input.quantityPerOrder),
    String(input.totalQuantity),
    String(input.totalAttempts),
    String(KEY_TTL_SEC)
  )) as unknown[];
  const kind = String(raw?.[0] || "skip_target") as ReserveResult["kind"];
  const purchasedQuantity = asInt(raw?.[1]);
  const attemptsUsed = asInt(raw?.[2]);
  if (kind === "ok") return { kind, purchasedQuantity, attemptsUsed };
  if (kind === "skip_exhausted") return { kind, purchasedQuantity, attemptsUsed };
  return { kind: "skip_target", purchasedQuantity, attemptsUsed };
}

const REFUND_ATTEMPT_LUA = `
local attemptsKey = KEYS[1]
local statusKey = KEYS[2]
local ttl = tonumber(ARGV[1])
local attempts = tonumber(redis.call('GET', attemptsKey) or '0')
if attempts > 0 then
  attempts = redis.call('DECR', attemptsKey)
end
if redis.call('GET', statusKey) == 'exhausted' then
  redis.call('DEL', statusKey)
end
redis.call('EXPIRE', attemptsKey, ttl)
return attempts
`;

/**
 * Gives an attempt back. For a job that never reached Flipkart because OUR
 * plumbing failed (the proxy), so the batch's attempt budget is spent only on
 * Flipkart's answers. Clears an `exhausted` mark the refunded attempt caused.
 */
export async function refundBatchAttempt(batchId: string): Promise<number> {
  const r = getBatchRedis();
  const left = await r.eval(REFUND_ATTEMPT_LUA, 2, attemptsKey(batchId), statusKey(batchId), String(KEY_TTL_SEC));
  return asInt(left);
}

export async function releaseBatchReservation(batchId: string, quantityPerOrder: number): Promise<number> {
  const r = getBatchRedis();
  const restored = await r.eval(
    RELEASE_LUA,
    1,
    purchasedKey(batchId),
    String(quantityPerOrder),
    String(KEY_TTL_SEC)
  );
  return asInt(restored);
}

export async function readBatchProgress(
  batchId: string,
  totalQuantity: number,
  totalAttempts: number
): Promise<BatchProgress> {
  const r = getBatchRedis();
  const raw = (await r.eval(
    PROGRESS_LUA,
    3,
    purchasedKey(batchId),
    attemptsKey(batchId),
    statusKey(batchId),
    String(totalQuantity),
    String(totalAttempts),
    String(KEY_TTL_SEC)
  )) as unknown[];
  const rawStatus = String(raw?.[0] || "running");
  const status: BatchStatus = rawStatus === "completed" || rawStatus === "exhausted" ? rawStatus : "running";
  return {
    status,
    purchasedQuantity: asInt(raw?.[1]),
    attemptsUsed: asInt(raw?.[2]),
  };
}
