import { Types } from "mongoose";
import { PlatformId } from "../models/PlatformId.js";
import { OrderUnitEvent } from "../models/OrderUnitEvent.js";

/**
 * A worker found an account's saved session dead — the login was refused, the
 * order pages rendered nothing, or the order API rejected the cookies. Make the
 * Platform ID *eligible for re-login* exactly the way the dashboard's Verify
 * checker does: demote `logged_in → pending`, keep the cookies, and remember
 * `statusBeforeDemote` so the dashboard's verify-undo can reverse it. Then raise
 * a session-lost event so the dashboard surfaces the account as "needs
 * re-login".
 *
 * No login happens here. The dashboard owns the one login engine
 * (platformJob.ts → resolvePriorityLogin / startPlatformJobForIds); once this
 * row is eligible, that engine logs it back in — on the Session Health tab's
 * Log in button, or through the auto-login loop when its switch is on. This is
 * the single place any Trade Flow flow (update now; cancel / checkout / sync
 * later) marks a dead session, so none of them carry their own login code.
 */
export async function markSessionEligibleForRelogin(opts: {
  userId: Types.ObjectId | string;
  platformId?: Types.ObjectId | string | null;
  email: string;
  reason: string;
  via?: string;
  requestId?: string;
}): Promise<void> {
  const email = String(opts.email || "").trim().toLowerCase();
  const reason = String(opts.reason || "").slice(0, 300);
  const via = opts.via || "worker";
  const now = new Date();

  try {
    const row = opts.platformId
      ? await PlatformId.findById(opts.platformId).select("status").lean()
      : await PlatformId.findOne({ userId: opts.userId, platform: "flipkart", email }).select("status").lean();
    if (row) {
      const set: Record<string, unknown> = {
        lastChecked: now,
        sessionCheck: { at: now, outcome: "not_logged_in", reason, via },
      };
      // Only demote a row that still claims to be logged in — leave rows that
      // are already pending / failed / blocked where they are (they are already
      // eligible, or deliberately held out of the pool).
      if (row.status === "logged_in") {
        set.status = "pending";
        set.statusBeforeDemote = "logged_in";
      }
      await PlatformId.updateOne({ _id: row._id }, { $set: set });
    }
  } catch {
    /* best-effort: a health write must never break the run */
  }

  await OrderUnitEvent.create({
    userId: opts.userId,
    orderId: "",
    unitId: "",
    email,
    productName: "",
    type: "account_unreadable",
    at: now,
    before: null,
    after: { reason, via, needsRelogin: true },
    requestId: opts.requestId || "",
    processed: false,
    processedAt: null,
  }).catch(() => undefined);
}
