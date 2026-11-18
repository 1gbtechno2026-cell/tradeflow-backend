import mongoose, { Schema, Types } from "mongoose";

export interface IPlatformId {
  userId: Types.ObjectId;
  platform: "flipkart" | "amazon";
  email: string;
  status: string;
  sessionCookies: unknown[];
  sessionState: { cookies: unknown[]; origins: unknown[] };
  sessionSavedAt: Date | null;
  /** Last time a worker / the dashboard's Verify checked this saved session. */
  lastChecked: Date | null;
  /** Set when a worker demoted this row from logged_in to pending (cookies kept). The dashboard's verify-undo reverses it. */
  statusBeforeDemote?: string;
  /** The last session check a worker recorded — read by Session Health. Mirrors the dashboard model's richer field (kept Mixed here). */
  sessionCheck?: unknown;
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
    lastChecked: { type: Date, default: null },
    statusBeforeDemote: { type: String, default: "" },
    sessionCheck: { type: Schema.Types.Mixed, default: null },
  },
  { timestamps: true, strict: false }
);

export const PlatformId = mongoose.model<IPlatformId>("PlatformId", PlatformIdSchema);
