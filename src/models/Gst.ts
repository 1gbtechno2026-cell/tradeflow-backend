import mongoose, { Schema, Types } from "mongoose";

export interface IGst {
  userId: Types.ObjectId;
  id: number;
  label: string;
  gstNumber: string;
  businessName: string;
}

const GstSchema = new Schema<IGst>(
  {
    userId: { type: Schema.Types.ObjectId, required: true, index: true },
    id: { type: Number, required: true },
    label: { type: String, default: "" },
    gstNumber: { type: String, required: true },
    businessName: { type: String, required: true },
  },
  { timestamps: true, id: false }
);

export const Gst = mongoose.model<IGst>("Gst", GstSchema);
