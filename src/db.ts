import mongoose from "mongoose";
import { config } from "./config.js";

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

export async function connectDb() {
  await mongoose.connect(config.mongoUri);
  await alignOrderDetailsIndexes();
  console.log("MongoDB connected");
}
