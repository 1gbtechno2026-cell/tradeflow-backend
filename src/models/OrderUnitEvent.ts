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
 */
export type UnitEventType = "stage_changed" | "otp_new" | "address_seen" | "address_changed" | "account_unreadable";

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
    type: { type: String, enum: ["stage_changed", "otp_new", "address_seen", "address_changed", "account_unreadable"], required: true },
    at: { type: Date, required: true },
    before: { type: Schema.Types.Mixed, default: null },
    after: { type: Schema.Types.Mixed, default: null },
    requestId: { type: String, default: "" },
    processed: { type: Boolean, default: false },
    processedAt: { type: Date, default: null },
  },
  { timestamps: { createdAt: true, updatedAt: false }, collection: "order_unit_events" }
);

OrderUnitEventSchema.index({ userId: 1, at: -1 });
OrderUnitEventSchema.index({ processed: 1, at: 1 });
OrderUnitEventSchema.index({ userId: 1, unitId: 1, at: -1 });

export const OrderUnitEvent = mongoose.model<IOrderUnitEvent>("OrderUnitEvent", OrderUnitEventSchema);
