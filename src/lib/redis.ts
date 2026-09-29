import { Redis } from "ioredis";
import { config } from "../config.js";

let client: Redis | null = null;

/** Shared general-purpose Redis client (same instance BullMQ uses via REDIS_URL). */
export function getRedis(): Redis {
  if (!client) {
    client = new Redis(config.redisUrl, { maxRetriesPerRequest: 2 });
    client.on("error", (err) => console.warn(`[redis] ${err.message}`));
  }
  return client;
}
