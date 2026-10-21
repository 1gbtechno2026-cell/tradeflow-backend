/**
 * The concurrency class of a checkout job — what shared resource it will hold
 * at the payment step. Decides which queue it goes on and which cap applies:
 *
 *   otp-phone  a corporate OTP card: the order leases one of the onboarded
 *              handsets, so the cap is "phones online under that corporate"
 *   card       any other card (password / PIN / OTP to the CSV phone): no
 *              shared resource beyond the card itself; policy cap
 *   cod        no payment instrument at all; policy cap
 *
 * Mixing them on one queue let ICICI orders waiting for a handset sit in front
 * of HDFC and COD orders that needed nothing. One queue per class, each scaled
 * to its own cap, is the only arrangement where a shortage in one class cannot
 * starve another.
 */
export type JobClass = "otp-phone" | "card" | "cod";

export const JOB_CLASSES: readonly JobClass[] = ["otp-phone", "card", "cod"];

export function jobClassFor(data: {
  paymentMode?: string | null;
  authType?: string | null;
  corporateId?: string | null;
}): JobClass {
  const mode = String(data.paymentMode || "").toLowerCase();
  if (mode === "cod" || mode === "cash_on_delivery") return "cod";
  if (String(data.authType || "").toLowerCase() === "otp" && data.corporateId) return "otp-phone";
  return "card";
}

export function isJobClass(value: string): value is JobClass {
  return (JOB_CLASSES as readonly string[]).includes(value);
}
