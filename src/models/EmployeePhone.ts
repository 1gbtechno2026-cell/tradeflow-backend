import mongoose, { Schema, Types } from "mongoose";

export interface IEmployeePhone {
  userId: Types.ObjectId;
  id: number;
  cardTypeId: number;
  corporateId: string;
  employeeId: string;
  rawPhoneNumber: string;
  isActive: boolean;
  connectionStatus: string;
  lastPingTime: Date | null;
  lastOtp: string;
  lastOtpTime: Date | null;
  lastCardLast4?: string | null;
  /** Lease held by a running checkout. Fields and indexes are DEFINED by the UI
   *  backend; Trade Flow only claims and releases them. */
  leasedBy?: string | null;
  leasedUntil?: Date | null;
}

// Owned and written by the UI backend. Trade Flow only $sets the OTP fields — no defaults,
// no inserts, no index builds, so it can never reshape rows the UI created.
const EmployeePhoneSchema = new Schema<IEmployeePhone>(
  {
    userId: Schema.Types.ObjectId,
    id: Number,
    cardTypeId: Number,
    corporateId: String,
    employeeId: String,
    rawPhoneNumber: String,
    isActive: Boolean,
    connectionStatus: String,
    lastPingTime: Date,
    lastOtp: String,
    lastOtpTime: Date,
    lastCardLast4: String,
    leasedBy: String,
    leasedUntil: Date,
  },
  { timestamps: true, autoIndex: false, autoCreate: false }
);

export const EmployeePhone = mongoose.model<IEmployeePhone>("EmployeePhone", EmployeePhoneSchema, "employeephones");
