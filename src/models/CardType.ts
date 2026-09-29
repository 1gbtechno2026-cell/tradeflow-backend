import mongoose, { Schema, Types } from "mongoose";

export interface ICardType {
  userId: Types.ObjectId;
  id: number;
  cardTypeName: string;
  label: string;
  authTypes: string[];
  isCorporate: boolean;
}

// Read-only mirror of the UI-owned cardtypes collection.
const CardTypeSchema = new Schema<ICardType>(
  {
    userId: Schema.Types.ObjectId,
    id: Number,
    cardTypeName: String,
    label: String,
    authTypes: [String],
    isCorporate: Boolean,
  },
  { timestamps: true, autoIndex: false, autoCreate: false }
);

export const CardType = mongoose.model<ICardType>("CardType", CardTypeSchema, "cardtypes");
