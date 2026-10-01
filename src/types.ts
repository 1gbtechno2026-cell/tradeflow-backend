import type { AuthType, CardDetails } from "./paymentStrategies/types.js";

export type AddressType = "Home" | "Work";

export type JobStatus =
  | "queued"
  | "running"
  | "reached_payment"
  /** Bank authentication succeeded. Distinct from reached_payment, which now
   *  only means "got as far as the payment page and stopped there". */
  | "paid"
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
  label?: string;
  gstLabel?: string;
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
  /** Credential arm chosen for this batch — also its concurrency class:
   *  "otp" needs a handset from the shared pool, password/pin do not. */
  authType?: string;
  corporateId?: string;
  sellerName: string;
  listingId: string;
  deliverySlaDays?: number;
  finalAmountLimit?: number;
  dryRun: boolean;
  gstMandatory?: boolean;
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
  /** Full form, "OD" + 18 digits (…00) — what Flipkart's order pages use. */
  flipkartOrderId: string;
  /** Short form, "OD" + 16 digits — what the confirmation URL carries. */
  flipkartReferenceId?: string;
  transactionAmount: string;
  cartAfterCardOffer: string;
  cartAfterOfferPrelim: string;
  giftCardApplied: string;
  /** From Flipkart's gateway response and order-confirmation data, read
   *  passively after a placed order. Rupees as strings; see PaymentApiWatcher. */
  paymentFee?: string;
  /** Each applied charge by type ("CORP_CARD_FEE ₹1"), plus any adjustments. */
  paymentFeeDetails?: string;
  shippingAmount?: string;
  discountPct?: string;
  mrp?: string;
  /** Flipkart's unit price from the confirmation data. */
  unitPrice?: string;
  bankTransactionId?: string;
  pgTransactionId?: string;
  bankName?: string;
  cardBrand?: string;
  supercoinsApplied?: string;
  /** Flipkart's delivery promise in days from the order (sla.maxSla). */
  promiseDays?: number;
  promiseDate?: Date;
  orderStatus?: string;
  sellerName?: string;
  /** The card the pool used for THIS order (a batch's CSV holds many).
   *  Label and last-4s only. */
  cardName?: string;
  parentCardLast4?: string;
  childCardLast4?: string;
  /** The account's registered mobile, read from /account in pre-flight — the
   *  phone on the delivery address, i.e. the order's billing phone. */
  billingPhone?: string;
  /** Which card type and credential arm actually authenticated. Last 4 only —
   *  no full PAN is ever written to a persisted document. */
  cardTypeName?: string;
  cardLast4?: string;
  authType?: string;
  /** Set only when a corporate handset was leased for the OTP. */
  employeeId?: string;
  authenticatedAt?: Date;
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
  gstMandatory?: boolean;
  address: AddressDetails;
  isRetry?: boolean;
  /** COD | card | ... — decides whether a card is needed at all. */
  paymentMode?: string;
  cardType?: string;
  authType?: AuthType;
  corporateId?: string | null;
  /**
   * The cards this batch may pay with, UNMASKED — full PAN, CVV and the card
   * type's credential. Nothing else in this codebase carries these: the stored
   * request snapshot masks them (orderSnapshot.maskCards) and always will.
   *
   * They ride the queue payload because there is nowhere else yet to read them
   * from — the order form holds cards in browser state only, and no collection
   * persists them (Order.card_number is an unpopulated placeholder). That makes
   * the payload the ONLY path from the operator's CSV to the bank's form.
   *
   * Consequences that are deliberately accepted for now, and the reason
   * queue.ts drops completed AND failed jobs immediately rather than retaining
   * them for 7/14 days:
   *   - plaintext card data exists in Redis for the lifetime of the job
   *   - it crosses the network if Redis is not on the same host
   * To be replaced by an encrypted cards collection plus a reference here,
   * matching the rule sessionVerifyQueue.ts already states for cookies: the
   * payload carries a Mongo reference, never the secret. Until then: never log
   * a member of this array, and never copy it onto a persisted document.
   */
  cards?: CardDetails[];
}
