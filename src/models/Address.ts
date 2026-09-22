import mongoose, { Schema, Types } from "mongoose";

export interface IAddress {
  userId: Types.ObjectId;
  id: number;
  label: string;
  contactName: string;
  pincode: string;
  addressLine1: string;
  addressLine2: string;
  floorNumber: string;
  city: string;
  state: string;
  linkedGstIds: Types.ObjectId[];
}

const AddressSchema = new Schema<IAddress>(
  {
    userId: { type: Schema.Types.ObjectId, required: true, index: true },
    id: { type: Number, required: true },
    label: { type: String, default: "" },
    contactName: { type: String, required: true },
    pincode: { type: String, required: true },
    addressLine1: { type: String, required: true },
    addressLine2: { type: String, default: "" },
    floorNumber: { type: String, default: "" },
    city: { type: String, required: true },
    state: { type: String, required: true },
    linkedGstIds: [{ type: Schema.Types.ObjectId, ref: "Gst" }],
  },
  { timestamps: true, id: false }
);

export const Address = mongoose.model<IAddress>("Address", AddressSchema);
