import mongoose, { Schema } from "mongoose";

interface ICounter {
  _id: string;
  seq: number;
}

const CounterSchema = new Schema<ICounter>({
  _id: { type: String, required: true },
  seq: { type: Number, default: 0 },
});

export const Counter = mongoose.model<ICounter>("Counter", CounterSchema);

export async function nextSeq(name: string): Promise<number> {
  const row = await Counter.findByIdAndUpdate(
    name,
    { $inc: { seq: 1 } },
    { new: true, upsert: true }
  );
  return row.seq;
}
