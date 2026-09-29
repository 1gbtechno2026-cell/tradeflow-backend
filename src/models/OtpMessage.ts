import mongoose, { Schema } from "mongoose";

export interface IOtpMessage {
  sender: string;
  recipient: string;
  message: string;
  otp: string | null;
  card_last4: string | null;
  matched: boolean;
  complete_message: string;
  received_at: Date;
}

const OtpMessageSchema = new Schema<IOtpMessage>({
  sender: { type: String, required: true },
  recipient: { type: String, default: "" },
  message: { type: String, required: true },
  otp: { type: String, default: null },
  card_last4: { type: String, default: null },
  matched: { type: Boolean, default: false },
  complete_message: { type: String, default: "" },
  received_at: { type: Date, default: Date.now, index: true },
});

export const OtpMessage = mongoose.model<IOtpMessage>("OtpMessage", OtpMessageSchema, "otp_messages");
