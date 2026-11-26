import mongoose, { Schema, type Types } from "mongoose";

/**
 * What a refresh found different about one unit — the stream behind the
 * dashboard's alerts (address change / mismatch) and its counts (status
 * moved, OTP arrived). Written by the update worker only
 * (services/orderUnitEvents.ts), one row per detected difference, never
 * rewritten; the dashboard marks a row `processed` once its rules ran.
 *
 *   stage_changed    lifecycle stage moved (placed → shipped → out for
 *                    delivery → delivered / cancelled …)
 *   otp_new          a delivery OTP appeared
 *   address_seen     first time this unit's delivery address is recorded
 *                    (baseline; the dashboard checks it against the book)
 *   address_changed  the delivery address differs from the previous refresh
 *   account_unreadable  every unit of an account failed with an empty order
 *                    page (and the API refused the session): the Platform ID
 *                    needs a re-check, not more refreshes (orderId/unitId "")
 *   refund_completed the unit's refund completed (data-quality U10): its
 *                    refund status turned Completed, or — on a unit that
 *                    carries no refund status — a DONE Refund stage appeared.
 *                    after: { amount, date, kind }
 *
 * Quiet runs (the one-off forced refresh, request params.quiet_events) insert
 * their events already `processed` (after.backfill true): the dashboard never
 * claims them, so they raise no alert; the baselines are still written.
 *
 * Where an event came from (T10): `source` is the writer ("update" — the
 * update worker is the only writer of unit events), `requestSource` the
 * request's own source (manual, schedule, ofd, cancel, system-order,
 * system-retry) and `sourceLabel` the words an alert shows for it
 * ("Update order details", "Update order details (6-hour sweep)", …).
 * Events from before 2026-11-26 carry none of the three.
 */
export type UnitEventType = "stage_changed" | "otp_new" | "address_seen" | "address_changed" | "account_unreadable" | "refund_completed";
export const UNIT_EVENT_TYPES: UnitEventType[] = ["stage_changed", "otp_new", "address_seen", "address_changed", "account_unreadable", "refund_completed"];

export interface IOrderUnitEvent {
  userId: Types.ObjectId;
  orderId: string;
  unitId: string;
  email: string;
  productName: string;
  type: UnitEventType;
  at: Date;
  before: Record<string, unknown> | null;
  after: Record<string, unknown> | null;
  /** The update request that found it. */
  requestId: string;
  /** The writer: "update" (T10). "" on events from before 2026-11-26 and on the sessions' account_unreadable. */
  source?: string;
  /** The request's own source (OrderUpdateRequest.source). */
  requestSource?: string;
  /** What an alert raised from this event names as its source. */
  sourceLabel?: string;
  processed: boolean;
  processedAt: Date | null;
  createdAt: Date;
}

const OrderUnitEventSchema = new Schema<IOrderUnitEvent>(
  {
    userId: { type: Schema.Types.ObjectId, required: true, index: true },
    orderId: { type: String, default: "" },
    unitId: { type: String, default: "" },
    email: { type: String, default: "", lowercase: true },
    productName: { type: String, default: "" },
    type: { type: String, enum: UNIT_EVENT_TYPES, required: true },
    at: { type: Date, required: true },
    before: { type: Schema.Types.Mixed, default: null },
    after: { type: Schema.Types.Mixed, default: null },
    requestId: { type: String, default: "" },
    source: { type: String, default: "" },
    requestSource: { type: String, default: "" },
    sourceLabel: { type: String, default: "" },
    processed: { type: Boolean, default: false },
    processedAt: { type: Date, default: null },
  },
  { timestamps: { createdAt: true, updatedAt: false }, collection: "order_unit_events" }
);

OrderUnitEventSchema.index({ userId: 1, at: -1 });
OrderUnitEventSchema.index({ processed: 1, at: 1 });
OrderUnitEventSchema.index({ userId: 1, unitId: 1, at: -1 });

export const OrderUnitEvent = mongoose.model<IOrderUnitEvent>("OrderUnitEvent", OrderUnitEventSchema);
