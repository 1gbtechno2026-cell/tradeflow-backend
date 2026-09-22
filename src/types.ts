export type AddressType = "Home" | "Work";

export type JobStatus =
  | "queued"
  | "running"
  | "reached_payment"
  | "failed"
  | "cancelled"
  | "skipped"
  | "dry_run"
  | "completed_target_reached"
  | "failed_attempt_budget_exhausted"
  | "filtered";

export type LogLevel = "info" | "warn" | "error";

export interface AddressDetails {
  name: string;
  mobile: string;
  pincode: string;
  locality: string;
  addressLine1: string;
  city: string;
  state: string;
  addressType: AddressType;
  gstNumber: string;
  companyName: string;
  checkoutPincode?: string;
}

export interface JobLog {
  at: Date;
  level: LogLevel;
  step?: string;
  message: string;
}

export interface CreateJobInput {
  productUrl: string;
  quantity: number;
  cartAmountLimit?: number;
  emails: string[];
  address?: AddressDetails;
  addressId?: number;
  gstId?: number;
}

export interface MaskedCard {
  name: string;
  cardNumberMasked: string;
  parentCardNumberMasked: string;
}

export interface JobRequestSnapshot {
  platform: string;
  paymentMode: string;
  cardType: string;
  sellerName: string;
  listingId: string;
  deliverySlaDays?: number;
  finalAmountLimit?: number;
  dryRun: boolean;
  cards: MaskedCard[];
  totalQuantity?: number;
  quantityPerOrder?: number;
  totalAttempts?: number;
}

export interface JobResultSnapshot {
  productName: string;
  colour: string;
  listingAmount: string;
  cartAmount: string;
  quantityPlaced?: number;
  deliveryText: string;
  deliveryDays?: number;
  gstNumber: string;
  gstCompany: string;
  paymentUrl: string;
  flipkartOrderId: string;
  transactionAmount: string;
  cartAfterCardOffer: string;
  cartAfterOfferPrelim: string;
  giftCardApplied: string;
}

export interface CheckoutJobData {
  jobId: string;
  batchId: string;
  email: string;
  productUrl: string;
  /** Units this checkout puts on Flipkart (quantity_per_order). */
  quantity: number;
  totalQuantity: number;
  quantityPerOrder: number;
  totalAttempts: number;
  cartAmountLimit?: number;
  deliverySlaDays?: number;
  address: AddressDetails;
  isRetry?: boolean;
}
