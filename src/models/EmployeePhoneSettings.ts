import mongoose, { Schema, Types } from "mongoose";

/**
 * Non-owning mirror of the UI backend's employeephonesettings collection.
 *
 * autoIndex/autoCreate false, same as every other mirror here: the dashboard
 * owns this shape and this process must never reshape it.
 *
 * Read for one reason — assumeAllOnline. It is the operator's answer to "may a
 * handset be used when nothing has confirmed it is reachable?", and the lease
 * has to respect it or the switch is decoration. It was decoration until now:
 * the dashboard's own (unused) lease honoured it while THIS one, the only lease
 * that actually runs, ignored it entirely.
 */
export interface IEmployeePhoneSettings {
  userId: Types.ObjectId;
  assumeAllOnline: boolean;
}

const EmployeePhoneSettingsSchema = new Schema<IEmployeePhoneSettings>(
  {
    userId: Schema.Types.ObjectId,
    assumeAllOnline: Boolean,
  },
  { timestamps: true, autoIndex: false, autoCreate: false }
);

export const EmployeePhoneSettings = mongoose.model<IEmployeePhoneSettings>(
  "EmployeePhoneSettings",
  EmployeePhoneSettingsSchema,
  "employeephonesettings"
);
