import mongoose, { Schema, Types } from "mongoose";

export interface IPlatformId {
  userId: Types.ObjectId;
  platform: "flipkart" | "amazon";
  email: string;
  status: string;
  sessionCookies: unknown[];
  sessionState: { cookies: unknown[]; origins: unknown[] };
  sessionSavedAt: Date | null;
}

const PlatformIdSchema = new Schema<IPlatformId>(
  {
    userId: { type: Schema.Types.ObjectId, required: true, index: true },
    platform: { type: String, default: "flipkart" },
    email: { type: String, required: true, lowercase: true, trim: true },
    status: { type: String, default: "pending" },
    sessionCookies: { type: [Schema.Types.Mixed], default: [] },
    sessionState: { type: Schema.Types.Mixed, default: { cookies: [], origins: [] } },
    sessionSavedAt: { type: Date, default: null },
  },
  { timestamps: true }
);

export const PlatformId = mongoose.model<IPlatformId>("PlatformId", PlatformIdSchema);
