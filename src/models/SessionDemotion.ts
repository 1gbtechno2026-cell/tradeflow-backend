import mongoose, { Schema, type Types } from "mongoose";

/**
 * One Trade Flow demotion (sessionEligibility.ts) — what SESSION_DEMOTE_CAP is
 * counted against: at most that many per workspace in any rolling 6 h, shared
 * by every machine on this database.
 *
 * Its own record, not the Platform ID rows: a login (the dashboard's
 * saveFlipkartSession) and Session Health → Undo both clear a row's demotedAt,
 * which would hand the slot back — a demotion that was undone still happened.
 * And a slot is taken here BEFORE the row is written (and given back when the
 * write does not happen), so reports that run at once — fetch lanes, update
 * windows, the other Mac — cannot pass the cap together.
 *
 * Nothing else reads or writes it. Kept 7 days.
 */
export interface ISessionDemotion {
  userId: Types.ObjectId;
  platformId: Types.ObjectId | null;
  email: string;
  /** "update", "fetch", "checkout", … (the row's demotedBy is "trade-flow:<flow>"). */
  flow: string;
  requestId: string;
  at: Date;
}

const SessionDemotionSchema = new Schema<ISessionDemotion>(
  {
    userId: { type: Schema.Types.ObjectId, required: true },
    platformId: { type: Schema.Types.ObjectId, default: null },
    email: { type: String, default: "" },
    flow: { type: String, default: "" },
    requestId: { type: String, default: "" },
    at: { type: Date, required: true },
  },
  { collection: "session_demotions", versionKey: false }
);

// The cap's count: one workspace, the last 6 h.
SessionDemotionSchema.index({ userId: 1, at: -1 });
// 7-day retention.
SessionDemotionSchema.index({ at: 1 }, { expireAfterSeconds: 7 * 24 * 60 * 60 });

export const SessionDemotion = mongoose.model<ISessionDemotion>("SessionDemotion", SessionDemotionSchema);
