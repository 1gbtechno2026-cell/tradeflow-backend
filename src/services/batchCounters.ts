import { Redis } from "ioredis";
import { config } from "../config.js";

/**
 * Batch-wide counters shared by every worker.
 * Keys:
 *   batch:{batchId}:purchasedQuantity
 *   batch:{batchId}:attemptsUsed
 *   batch:{batchId}:status   completed | exhausted | filtered
 *   batch:{batchId}:filterReason
 *   batch:{batchId}:filteredCount
 *
 * All mutations go through INCRBY/DECRBY or Lua — never GET then SET in JS.
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

export function filterReasonKey(batchId: string) {
  return `batch:${batchId}:filterReason`;
}
export function filteredCountKey(batchId: string) {
  return `batch:${batchId}:filteredCount`;
}

export type BatchStatus = "running" | "completed" | "exhausted" | "filtered";

export interface BatchProgress {
  status: BatchStatus;
  purchasedQuantity: number;
  attemptsUsed: number;
  filteredCount: number;
  filterReason: string | null;
}

export type ReserveResult =
  | { kind: "ok"; purchasedQuantity: number; attemptsUsed: number }
  | { kind: "skip_target"; purchasedQuantity: number; attemptsUsed: number }
  | { kind: "skip_exhausted"; purchasedQuantity: number; attemptsUsed: number }
  | { kind: "skip_filtered"; purchasedQuantity: number; attemptsUsed: number; filterReason: string | null };

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
if status == 'filtered' then
  local p, a = nums()
  return {'skip_filtered', p, a}
end
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
local reasonKey = KEYS[4]
local filteredKey = KEYS[5]
local totalQty = tonumber(ARGV[1])
local totalAttempts = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local purchased = tonumber(redis.call('GET', purchasedKey) or '0')
local attempts = tonumber(redis.call('GET', attemptsKey) or '0')
local filtered = tonumber(redis.call('GET', filteredKey) or '0')
local reason = redis.call('GET', reasonKey) or ''
local status = redis.call('GET', statusKey)
if status == 'filtered' then
  return {'filtered', purchased, attempts, filtered, reason}
end
if purchased >= totalQty then
  redis.call('SET', statusKey, 'completed')
  redis.call('EXPIRE', statusKey, ttl)
  return {'completed', purchased, attempts, filtered, reason}
end
if attempts >= totalAttempts and purchased < totalQty then
  redis.call('SET', statusKey, 'exhausted')
  redis.call('EXPIRE', statusKey, ttl)
  return {'exhausted', purchased, attempts, filtered, reason}
end
return {status or 'running', purchased, attempts, filtered, reason}
`;

const MARK_FILTERED_LUA = `
local statusKey = KEYS[1]
local reasonKey = KEYS[2]
local filteredKey = KEYS[3]
local reason = ARGV[1]
local ttl = tonumber(ARGV[2])
redis.call('SET', statusKey, 'filtered')
redis.call('SET', reasonKey, reason)
local n = redis.call('INCR', filteredKey)
redis.call('EXPIRE', statusKey, ttl)
redis.call('EXPIRE', reasonKey, ttl)
redis.call('EXPIRE', filteredKey, ttl)
return n
`;

function asInt(v: unknown): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

export async function initBatchCounters(batchId: string): Promise<void> {
  const r = getBatchRedis();
  await r.set(purchasedKey(batchId), "0", "NX");
  await r.set(attemptsKey(batchId), "0", "NX");
  await r.set(filteredCountKey(batchId), "0", "NX");
  await r.expire(purchasedKey(batchId), KEY_TTL_SEC);
  await r.expire(attemptsKey(batchId), KEY_TTL_SEC);
  await r.expire(filteredCountKey(batchId), KEY_TTL_SEC);
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
  if (kind === "skip_filtered") {
    const reason = await r.get(filterReasonKey(input.batchId));
    return { kind, purchasedQuantity, attemptsUsed, filterReason: reason || "out_of_stock_pincode" };
  }
  return { kind: "skip_target", purchasedQuantity, attemptsUsed };
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
    5,
    purchasedKey(batchId),
    attemptsKey(batchId),
    statusKey(batchId),
    filterReasonKey(batchId),
    filteredCountKey(batchId),
    String(totalQuantity),
    String(totalAttempts),
    String(KEY_TTL_SEC)
  )) as unknown[];
  const rawStatus = String(raw?.[0] || "running");
  const status: BatchStatus =
    rawStatus === "completed" || rawStatus === "exhausted" || rawStatus === "filtered"
      ? rawStatus
      : "running";
  return {
    status,
    purchasedQuantity: asInt(raw?.[1]),
    attemptsUsed: asInt(raw?.[2]),
    filteredCount: asInt(raw?.[3]),
    filterReason: String(raw?.[4] || "") || null,
  };
}

export async function markBatchFiltered(batchId: string, reason: string): Promise<number> {
  const r = getBatchRedis();
  const n = await r.eval(
    MARK_FILTERED_LUA,
    3,
    statusKey(batchId),
    filterReasonKey(batchId),
    filteredCountKey(batchId),
    reason,
    String(KEY_TTL_SEC)
  );
  return asInt(n);
}
