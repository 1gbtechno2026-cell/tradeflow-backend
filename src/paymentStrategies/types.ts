import type { Page } from "playwright";

/**
 * The seam between Flipkart's UI and a bank's.
 *
 * Everything up to and including Flipkart's own card form belongs to
 * flipkartCheckout. From the redirect onward the page is the BANK's, and a
 * strategy owns it. The two change on completely independent schedules —
 * Flipkart ships cart/checkout changes, ICICI ships 3-D Secure changes — so a
 * single file touching both would be edited for reasons that have nothing to
 * do with each other.
 *
 * This is the same split checkoutErrors.ts already encodes as PLATFORM vs BANK:
 * a strategy raises BANK failures only, and never a PLATFORM one.
 */

export type AuthType = "password" | "otp" | "pin";

export type JobLogger = (level: "info" | "warn" | "error", message: string) => void;

export interface CardDetails {
  name?: string;
  /** Full parent card number. Bank OTPs name the PARENT's last 4, not the child's. */
  parentCardNumber: string;
  cardNumber: string;
  expiryMonth: string;
  expiryYear: string;
  cvv: string;
  /** Present only for password-auth cards (e.g. HDFC Virtual). */
  password?: string;
  /** Present only for pin-auth cards (e.g. ICICI Physical). */
  pin?: string;
  /** Used by NON-corporate OTP cards. Corporate cards ignore this and use the
   *  onboarded employee handset instead. */
  otpPhoneNumber?: string;
}

export interface PaymentContext {
  /** Already on the bank's page — the strategy never navigates Flipkart. */
  page: Page;
  /** Correlation key: holds the phone lease and names the per-run OTP mailbox. */
  runId: string;
  jobId: string;
  card: CardDetails;
  authType: AuthType;
  /** Required when the card type is corporate; ignored otherwise. */
  corporateId?: string | null;
  /** Optional: pin the lease to one employee's handset (testing with a phone
   *  in hand). Unset, the lease picks any claimable phone under the corporate. */
  employeeId?: string | null;
  /** For verifying the bank is charging what Flipkart quoted. */
  expectedAmount?: string | null;
  otpTimeoutMs?: number;
  log: JobLogger;
  /** Skip every page interaction — exercises correlation and cleanup only. */
  dryRun?: boolean;
}

export interface PaymentResult {
  cardTypeName: string;
  /** Last 4 of the PARENT card — what the bank's SMS names. */
  cardLast4: string;
  authType: AuthType;
  /** Set only when a corporate handset was leased. */
  employeeId?: string;
  phoneNumber?: string;
  authenticatedAt: Date;
}

export interface PaymentStrategy {
  /** Matches cardtypes.card_type_name — the string already flowing through the
   *  order payload, so the registry needs no separate mapping table. */
  readonly cardTypeName: string;
  readonly supportedAuth: readonly AuthType[];
  /** True when the order form must supply a Corporate ID. */
  readonly requiresCorporateId: boolean;
  authenticate(ctx: PaymentContext): Promise<PaymentResult>;
}

/** Last 4 of the parent card — the value a bank SMS quotes ("Card XX8002"). */
export function parentCardLast4(parentCardNumber: string): string {
  return String(parentCardNumber || "").replace(/\D/g, "").slice(-4);
}
