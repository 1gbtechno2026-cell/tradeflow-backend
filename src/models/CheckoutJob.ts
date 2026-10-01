import mongoose, { Schema, Types } from "mongoose";
import type {
  AddressDetails,
  JobLog,
  JobRequestSnapshot,
  JobResultSnapshot,
  JobStatus,
  MaskedCard,
} from "../types.js";

export interface ICheckoutJob {
  batchId: string;
  userId: string;
  email: string;
  platformId?: Types.ObjectId;
  productUrl: string;
  quantity: number;
  totalQuantity?: number;
  quantityPerOrder?: number;
  totalAttempts?: number;
  cartAmountLimit?: number;
  address: AddressDetails;
  status: JobStatus;
  step: string;
  failedStep: string;
  error: string;
  failureMessage?: string;
  errorCode?: string;
  errorCodeDisplay?: string;
  errorSource?: string;
  errorDetails?: string;
  errorStage?: string;
  filterReason?: string;
  failedAt?: Date | null;
  batchStatus?: string;
  product?: { model: string; colour: string; amount: string };
  paymentUrl: string;
  request?: JobRequestSnapshot;
  result?: JobResultSnapshot;
  logs: JobLog[];
  bullmqJobId?: string;
  startedAt: Date | null;
  heartbeatAt?: Date | null;
  claimedBy?: string;
  completedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

const JobLogSchema = new Schema<JobLog>(
  {
    at: { type: Date, default: Date.now },
    level: { type: String, enum: ["info", "warn", "error"], required: true },
    step: { type: String },
    message: { type: String, required: true },
  },
  { _id: false }
);

const AddressSchema = new Schema<AddressDetails>(
  {
    name: { type: String, required: true },
    mobile: { type: String, default: "" },
    pincode: { type: String, required: true },
    locality: { type: String, default: "" },
    addressLine1: { type: String, required: true },
    city: { type: String, required: true },
    state: { type: String, required: true },
    addressType: { type: String, enum: ["Home", "Work"], default: "Home" },
    gstNumber: { type: String, default: "" },
    companyName: { type: String, default: "" },
    checkoutPincode: { type: String, default: "" },
    label: { type: String, default: "" },
    gstLabel: { type: String, default: "" },
  },
  { _id: false }
);

const CheckoutJobSchema = new Schema<ICheckoutJob>(
  {
    batchId: { type: String, required: true, index: true },
    userId: { type: String, required: true, index: true },
    email: { type: String, required: true, lowercase: true, trim: true },
    platformId: { type: Schema.Types.ObjectId },
    productUrl: { type: String, required: true },
    quantity: { type: Number, required: true, min: 1 },
    totalQuantity: { type: Number },
    quantityPerOrder: { type: Number },
    totalAttempts: { type: Number },
    cartAmountLimit: { type: Number },
    address: { type: AddressSchema, required: true },
    status: {
      type: String,
      enum: [
        "queued",
        "running",
        "reached_payment",
        "paid",
        "failed",
        "cancelled",
        "skipped",
        "dry_run",
        "completed_target_reached",
        "failed_attempt_budget_exhausted",
        "filtered",
      ],
      default: "queued",
      index: true,
    },
    step: { type: String, default: "queued" },
    failedStep: { type: String, default: "" },
    error: { type: String, default: "" },
    failureMessage: { type: String, default: "" },
    errorCode: { type: String, default: "" },
    errorCodeDisplay: { type: String, default: "" },
    errorSource: { type: String, default: "" },
    errorDetails: { type: String, default: "" },
    errorStage: { type: String, default: "" },
    filterReason: { type: String, default: "" },
    failedAt: { type: Date, default: null },
    batchStatus: { type: String, default: "" },
    product: {
      model: { type: String, default: "" },
      colour: { type: String, default: "" },
      amount: { type: String, default: "" },
    },
    paymentUrl: { type: String, default: "" },
    request: {
      platform: { type: String, default: "FLIPKART" },
      paymentMode: { type: String, default: "" },
      cardType: { type: String, default: "" },
      // Declared explicitly because Mongoose is strict by default: a field
      // present on JobRequestSnapshot but absent here is dropped on save without
      // an error, so the snapshot would silently lose the two values the payment
      // phase depends on for explaining itself after the fact.
      authType: { type: String, default: "" },
      corporateId: { type: String, default: "" },
      sellerName: { type: String, default: "" },
      listingId: { type: String, default: "" },
      deliverySlaDays: { type: Number },
      finalAmountLimit: { type: Number },
      dryRun: { type: Boolean, default: false },
      gstMandatory: { type: Boolean, default: true },
      cards: {
        type: [
          new Schema<MaskedCard>(
            {
              name: { type: String, default: "" },
              cardNumberMasked: { type: String, default: "" },
              parentCardNumberMasked: { type: String, default: "" },
            },
            { _id: false }
          ),
        ],
        default: [],
      },
    },
    result: {
      productName: { type: String, default: "" },
      colour: { type: String, default: "" },
      listingAmount: { type: String, default: "" },
      cartAmount: { type: String, default: "" },
      quantityPlaced: { type: Number },
      deliveryText: { type: String, default: "" },
      deliveryDays: { type: Number },
      gstNumber: { type: String, default: "" },
      gstCompany: { type: String, default: "" },
      paymentUrl: { type: String, default: "" },
      flipkartOrderId: { type: String, default: "" },
      flipkartReferenceId: { type: String, default: "" },
      transactionAmount: { type: String, default: "" },
      cartAfterCardOffer: { type: String, default: "" },
      cartAfterOfferPrelim: { type: String, default: "" },
      giftCardApplied: { type: String, default: "" },
      cardTypeName: { type: String, default: "" },
      cardLast4: { type: String, default: "" },
      authType: { type: String, default: "" },
      employeeId: { type: String, default: "" },
      authenticatedAt: { type: Date, default: null },
      // From Flipkart's gateway / order-confirmation responses after a placed
      // order. Declared, or strict mode drops them silently on save.
      paymentFee: { type: String, default: "" },
      bankTransactionId: { type: String, default: "" },
      pgTransactionId: { type: String, default: "" },
      bankName: { type: String, default: "" },
      cardBrand: { type: String, default: "" },
      supercoinsApplied: { type: String, default: "" },
      promiseDays: { type: Number },
      promiseDate: { type: Date, default: null },
      orderStatus: { type: String, default: "" },
      sellerName: { type: String, default: "" },
      // The card the pool actually used for this order — the batch's CSV may
      // hold many. Label and last-4s only; never a full PAN.
      cardName: { type: String, default: "" },
      parentCardLast4: { type: String, default: "" },
      childCardLast4: { type: String, default: "" },
      billingPhone: { type: String, default: "" },
    },
    logs: { type: [JobLogSchema], default: [] },
    bullmqJobId: { type: String },
    startedAt: { type: Date, default: null },
    /**
     * Refreshed while a worker is actually running this job.
     *
     * Exists because "status is running" alone cannot tell a live worker from one
     * that died mid-checkout. BullMQ hides that today behind its own Redis lock
     * and stalled-job detection; SQS has neither. Once the transport moves, a job
     * whose worker was killed would sit at `running` forever with nothing able to
     * claim it. The heartbeat is what makes claimCheckoutJob's claim reclaimable
     * rather than permanent.
     */
    heartbeatAt: { type: Date, default: null },
    /** Which worker run holds the claim, so a stale reclaim is attributable. */
    claimedBy: { type: String, default: "" },
    completedAt: { type: Date, default: null },
  },
  { timestamps: true, collection: "tradeflowjobs" }
);

// Backs the stale-claim reclaim: find `running` jobs whose heartbeat has gone
// quiet without scanning the collection.
CheckoutJobSchema.index({ status: 1, heartbeatAt: 1 });

export const CheckoutJob = mongoose.model<ICheckoutJob>("CheckoutJob", CheckoutJobSchema);
