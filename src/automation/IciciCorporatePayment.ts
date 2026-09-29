import type { Page } from "playwright";
import {
  leaseOtpPhone,
  releaseOtpPhone,
  renewOtpPhone,
  type LeasedOtpPhone,
} from "../services/employeePhoneLease.js";
import { waitForPaymentOtp } from "../services/smsOtp.js";

/**
 * ICICI Corporate Virtual — the 3-D Secure step.
 *
 * SKELETON. The lease lifecycle, OTP correlation and release are finished and
 * tested; the four `page.*` interactions are stubbed and marked TODO. Fill
 * those in and this is production code — do NOT restructure the lifecycle
 * around them, because the ordering below is what makes the OTP correlation
 * correct:
 *
 *   1. lease a handset BEFORE submitting    (so the webhook can resolve the run
 *                                            the moment the SMS lands)
 *   2. record submittedAt BEFORE submitting  (so a pre-existing code is ignored)
 *   3. wait, verified by card last-4
 *   4. release in `finally`                  (always, even on throw)
 *
 * Leasing after submit loses the race: ICICI can deliver in ~5s, and an SMS
 * arriving before the lease exists has no run to be attributed to.
 */

export type JobLogger = (level: "info" | "warn" | "error", message: string) => void;

export interface IciciCorporateCard {
  /** Full parent card number. Only its last 4 are used for verification. */
  parentCardNumber: string;
  cardNumber: string;
  expiryMonth: string;
  expiryYear: string;
  cvv: string;
}

export interface IciciCorporatePaymentInput {
  jobId: string;
  /** Correlation key for this checkout — also the lease holder. */
  runId: string;
  corporateId: string;
  card: IciciCorporateCard;
  cardTypeName?: string;
  otpTimeoutMs?: number;
  /** Skip every browser interaction; exercises lease -> OTP -> release only. */
  dryRun?: boolean;
}

export interface IciciCorporatePaymentResult {
  employeeId: string;
  phoneNumber: string;
  cardLast4: string;
  otpEnteredAt: Date;
}

/** ICICI's message names the parent card ("...Card XX8002"), not the child. */
export function parentCardLast4(parentCardNumber: string): string {
  return String(parentCardNumber || "").replace(/\D/g, "").slice(-4);
}

export class IciciCorporatePayment {
  constructor(
    private page: Page,
    private log: JobLogger = (level, message) => console.log(`[${level}] ${message}`)
  ) {}

  /**
   * Runs the payment step. Always releases the handset, so a thrown error or a
   * cancelled job never strands a phone for the full lease TTL.
   */
  async pay(input: IciciCorporatePaymentInput): Promise<IciciCorporatePaymentResult> {
    const cardLast4 = parentCardLast4(input.card.parentCardNumber);
    if (cardLast4.length !== 4) {
      throw new Error("parentCardNumber is required — its last 4 digits verify the incoming OTP");
    }

    // 1. Claim a handset before anything is submitted.
    const lease: LeasedOtpPhone = await leaseOtpPhone({
      cardTypeName: input.cardTypeName || "ICICI_CORP_VIRTUAL",
      runId: input.runId,
      corporateId: input.corporateId,
    });
    this.log(
      "info",
      `[icici] leased ${lease.employeeId} (${lease.phoneNumber.slice(0, 2)}****${lease.phoneNumber.slice(-4)}) ` +
        `under ${lease.corporateId} until ${lease.leasedUntil.toISOString()}`
    );

    let renewTimer: NodeJS.Timeout | null = null;
    try {
      // Hold the lease while a slow page runs. Without this a payment taking
      // longer than the TTL lets another worker claim the same handset — the
      // exact race the lease exists to prevent.
      renewTimer = setInterval(() => {
        void renewOtpPhone(lease.phoneId, input.runId).then((ok) => {
          if (!ok) this.log("warn", `[icici] lease lapsed and was re-taken — ${lease.employeeId}`);
        });
      }, 60_000);
      renewTimer.unref?.();

      if (!input.dryRun) {
        await this.enterCardDetails(input.card);
        await this.selectCorporateAndEmployee(lease.corporateId, lease.employeeId);
      }

      // 2. Stamp the clock BEFORE submitting, so a code that predates this
      //    request can never be mistaken for the answer to it.
      const submittedAt = new Date();

      if (!input.dryRun) {
        await this.submitForOtp();
      }
      this.log("info", `[icici] OTP requested for card ****${cardLast4} → ${lease.employeeId}`);

      // 3. Wait. runId picks the per-run mailbox; cardLast4 proves the code is
      //    for THIS card and not a leftover from another order on this handset.
      const otp = await waitForPaymentOtp({
        jobId: input.jobId,
        runId: input.runId,
        phoneNumber: lease.phoneNumber,
        cardLast4,
        notBefore: submittedAt,
        timeoutMs: input.otpTimeoutMs,
      });
      // Never log the code itself.
      this.log("info", `[icici] OTP received for ****${cardLast4} (${otp.length} digits)`);

      if (!input.dryRun) {
        await this.enterOtp(otp);
      }

      return {
        employeeId: lease.employeeId,
        phoneNumber: lease.phoneNumber,
        cardLast4,
        otpEnteredAt: new Date(),
      };
    } finally {
      if (renewTimer) clearInterval(renewTimer);
      // 4. Always release — the handset is free for the next job immediately
      //    rather than after the TTL lapses.
      const released = await releaseOtpPhone(lease.phoneId, input.runId).catch(() => false);
      this.log("info", `[icici] ${released ? "released" : "could not release (lease already lapsed)"} ${lease.employeeId}`);
    }
  }

  // ---------------------------------------------------------------------------
  // TODO: the four browser steps. Everything above is done.
  // ---------------------------------------------------------------------------

  /** TODO: fill card number, expiry month/year and CVV on the ICICI page. */
  private async enterCardDetails(_card: IciciCorporateCard): Promise<void> {
    throw new Error("IciciCorporatePayment.enterCardDetails is not implemented yet — run with dryRun:true");
  }

  /**
   * TODO: choose the corporate ID and employee ID on ICICI's corporate page.
   * Use the values passed in: they identify the handset already leased, so the
   * OTP is routed to a phone this run owns. Picking a different employee here
   * sends the code to a phone nothing is watching.
   */
  private async selectCorporateAndEmployee(_corporateId: string, _employeeId: string): Promise<void> {
    throw new Error("IciciCorporatePayment.selectCorporateAndEmployee is not implemented yet");
  }

  /** TODO: press submit so ICICI dispatches the OTP. */
  private async submitForOtp(): Promise<void> {
    throw new Error("IciciCorporatePayment.submitForOtp is not implemented yet");
  }

  /** TODO: type the code and confirm. Never log it. */
  private async enterOtp(_otp: string): Promise<void> {
    throw new Error("IciciCorporatePayment.enterOtp is not implemented yet");
  }
}
