import mongoose, { Schema, Types } from "mongoose";

/**
 * Invoice settings (Settings → Invoice settings in the dashboard): one document per workspace, `userId` = the
 * workspace id. Every rule field is OPTIONAL on purpose — a field that is not in the document follows Trade Flow's
 * `.env`, then the code's default (services/invoiceSettings.ts). The seed writes the document once with
 * `captureCancelledAfterShipping: true` only (owner decision 2026-11-26); the other fields appear when somebody saves them.
 */
export interface IInvoiceSettingsChange {
  field: string;
  /** The value that was in effect before the change (from the document, the .env or the default). */
  from: boolean | number | null;
  to: boolean | number;
  by: string;
  at: Date;
}

export interface IInvoiceSettings {
  userId: Types.ObjectId;
  captureCancelledAfterShipping?: boolean;
  refetchCancelDelayH?: number;
  refetchRefundDelayH?: number;
  refetchCodDelayH?: number;
  settleHours?: number;
  recaptureGiveUpDays?: number;
  /** A username, or "seed:<host>" for the document Trade Flow wrote at its first start. */
  changedBy: string;
  changedAt: Date | null;
  /** Newest last, at most 50. */
  history: IInvoiceSettingsChange[];
  createdAt?: Date;
  updatedAt?: Date;
}

const ChangeSchema = new Schema<IInvoiceSettingsChange>(
  { field: String, from: Schema.Types.Mixed, to: Schema.Types.Mixed, by: { type: String, default: "" }, at: Date },
  { _id: false }
);

// No defaults on the rule fields: a missing field must stay missing (it follows .env / the default).
const InvoiceSettingsSchema = new Schema<IInvoiceSettings>(
  {
    userId: { type: Schema.Types.ObjectId, required: true },
    captureCancelledAfterShipping: { type: Boolean },
    refetchCancelDelayH: { type: Number },
    refetchRefundDelayH: { type: Number },
    refetchCodDelayH: { type: Number },
    settleHours: { type: Number },
    recaptureGiveUpDays: { type: Number },
    changedBy: { type: String, default: "" },
    changedAt: { type: Date, default: null },
    history: { type: [ChangeSchema], default: undefined },
    createdAt: Date,
    updatedAt: Date,
  },
  // Written through the driver (services/invoiceSettings.ts); the collection and its unique index are made by the
  // service itself before its first write (ensureInvoiceSettingsIndexes), like the schedule lease collection.
  { collection: "invoice_settings", versionKey: false, autoCreate: false, autoIndex: false }
);

InvoiceSettingsSchema.index({ userId: 1 }, { unique: true });

export const InvoiceSettings = mongoose.model<IInvoiceSettings>("InvoiceSettings", InvoiceSettingsSchema);
