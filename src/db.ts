import mongoose from "mongoose";
import { config } from "./config.js";
import { describeError, throttledWarn } from "./lib/resilience.js";

async function alignOrderDetailsIndexes() {
  const col = mongoose.connection.collection("order_details");
  try {
    await col.dropIndex("userId_1_order_id_1");
    console.log("Dropped order_details unique index userId_1_order_id_1 — uniqueness is now userId + unit_id");
  } catch (err) {
    const code = (err as { code?: number }).code;
    const msg = err instanceof Error ? err.message : String(err);
    if (code !== 27 && !/index not found/i.test(msg)) {
      console.warn("Could not drop userId_1_order_id_1:", msg);
    }
  }
}

/** One line per disconnect / reconnect instead of a stack trace per query; the driver reconnects by itself. */
let eventsBound = false;
function bindConnectionEvents() {
  if (eventsBound) return;
  eventsBound = true;
  const c = mongoose.connection;
  let downSince: number | null = null;
  c.on("disconnected", () => {
    if (downSince == null) downSince = Date.now();
    throttledWarn("mongo:disconnected", "[mongo] disconnected — queries wait, then fail, until Atlas is reachable again");
  });
  c.on("reconnected", () => {
    console.log(`[mongo] reconnected${downSince ? ` after ${Math.round((Date.now() - downSince) / 1000)} s` : ""}`);
    downSince = null;
  });
  c.on("error", (err) => throttledWarn("mongo:error", `[mongo] ${describeError(err)}`));
}

/** For /api/health: 1 = connected. */
export function dbState(): { readyState: number; label: string } {
  const rs = mongoose.connection.readyState;
  return { readyState: rs, label: rs === 1 ? "connected" : rs === 2 ? "connecting" : rs === 3 ? "disconnecting" : "disconnected" };
}

export async function connectDb() {
  bindConnectionEvents();
  mongoose.set("bufferTimeoutMS", Number(process.env.MONGO_BUFFER_TIMEOUT_MS || 15_000));
  await mongoose.connect(config.mongoUri, {
    serverSelectionTimeoutMS: Number(process.env.MONGO_SERVER_SELECTION_TIMEOUT_MS || 15_000),
    heartbeatFrequencyMS: 10_000,
    maxPoolSize: Number(process.env.MONGO_MAX_POOL_SIZE || 20),
    retryWrites: true,
    retryReads: true,
  });
  await alignOrderDetailsIndexes();
  console.log("MongoDB connected");
}
