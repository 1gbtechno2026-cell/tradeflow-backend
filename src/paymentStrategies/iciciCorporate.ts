import fs from "fs";
import path from "path";
import { CheckoutFailure } from "../services/checkoutErrors.js";
import { requestAndAwaitOtp, withLeasedPhone } from "./shared/otpEntry.js";
import type { PaymentContext, PaymentResult, PaymentStrategy } from "./types.js";

/**
 * ICICI Corporate Virtual — 3-D Secure with a corporate identity.
 *
 * Unlike a consumer card, the OTP does not go to a number in the Cards CSV. The
 * bank page asks for a Corporate ID and an Employee ID, and sends the code to
 * the handset registered against that employee. That handset is claimed by
 * lease for the duration of this run, so the SMS webhook can attribute the
 * incoming code to the right order.
 *
 * Lease, correlation, card verification and release are finished and tested
 * (test:otp-plumbing). The identity and OTP-request steps are written from the
 * page captured on 2026-10-01; the OTP-entry step is written once Run 2 has
 * captured that page. The ordering inside withLeasedPhone/requestAndAwaitOtp
 * is load-bearing — see selectCorporateIdentity.
 */
export const iciciCorporate: PaymentStrategy = {
  cardTypeName: "ICICI_CORP_VIRTUAL",
  supportedAuth: ["otp"],
  requiresCorporateId: true,

  async authenticate(ctx: PaymentContext): Promise<PaymentResult> {
    return withLeasedPhone(ctx, this.cardTypeName, async (lease, cardLast4) => {
      // The employee whose handset we hold MUST be the one selected on the
      // page — selecting a different one sends the code to a phone nothing is
      // watching, and the order times out with no way to tell why.
      if (!ctx.dryRun) {
        await selectCorporateIdentity(ctx, lease.corporateId, lease.employeeId);
      }

      await requestAndAwaitOtp(
        ctx,
        {
          requestOtp: () => submitForOtp(ctx),
          submitOtp: (otp) => enterOtp(ctx, otp),
        },
        { phoneNumber: lease.phoneNumber, cardLast4 }
      );

      return {
        cardTypeName: this.cardTypeName,
        cardLast4,
        authType: "otp",
        employeeId: lease.employeeId,
        phoneNumber: lease.phoneNumber,
        authenticatedAt: new Date(),
      };
    });
  },
};

// ---------------------------------------------------------------------------
// SELECTORS — fill these three in. Everything above is done.
//
// You are on ICICI's page, handed over by FlipkartPayment.waitForBankHandoff.
//
// THE ORDER OF THE THREE IS LOAD-BEARING, and not for tidiness:
//
//   selectCorporateIdentity   must run BEFORE submitForOtp, because it is what
//                             decides WHICH handset the bank texts. The employee
//                             it selects is the one whose phone this run holds a
//                             lease on. Select a different employee and the code
//                             goes to a phone nothing is watching: the order waits
//                             out the full OTP window and fails as a timeout, with
//                             nothing in the logs pointing at the real cause.
//
//   submitForOtp              is called from inside requestAndAwaitOtp, which
//                             stamps `submittedAt` immediately BEFORE it. That
//                             timestamp is what lets an older code sitting in the
//                             mailbox be rejected. Do not request the OTP anywhere
//                             else, or that guarantee is gone.
//
//   enterOtp                  receives a code already proven to be this order's —
//                             it arrived after submittedAt AND the SMS named this
//                             parent card's last 4. Just type it.
//
// RULES
//  • NEVER log the OTP. requestAndAwaitOtp already logs its length and the card's
//    last 4, which is everything needed to debug without leaking the code.
//  • Throw CheckoutFailure, not a bare Error, when you can name the cause:
//      wrong/expired OTP        CheckoutFailure("CARD_AUTH_FAILED", "<page text>")
//      unknown corporate or
//      employee id             CheckoutFailure("CARD_AUTH_FAILED", ...) — include
//                              the id in the detail; it means the registry and the
//                              bank disagree, which is an onboarding problem.
//      OTP field never appeared CheckoutFailure("UNABLE_TO_PLACE_ORDER", ...) so
//                              the card is not blamed for a bank-side problem.
//      insufficient balance /
//      limit                   CheckoutFailure("INSUFFICIENT_BALANCE", ...) — this
//                              pauses the card until midnight IST instead of
//                              retiring it.
//  • Give every wait an explicit timeout. A hang here holds the batch slot, a
//    Chrome, AND the phone lease — the lease is the scarce one: with 67 online
//    handsets, one stuck order removes 1/67th of your OTP capacity until the 3-min
//    TTL lapses.
//  • Returning normally from enterOtp MEANS "authenticated". If success cannot be
//    confirmed from the page, throw.
// ---------------------------------------------------------------------------

// ── THE PAGE, as captured on 2026-10-01 (run card-2026-10-01T11-09-09) ──────
//
//   https://secure-acs2ui-…wibmo.com/v1/acs/services/browser/creq/…
//   No iframe. <form id="authPasswordSet2">
//     Merchant Name / Date / Amount / Unique Reference ID   (read-only rows)
//     <input type="password" id="corporateId" name="corporateId">
//     <input type="password" id="employeeId"  name="employeeId">
//     <a class="btn primary__btn" onclick="submit()">Submit</a>
//     <a class="btn cancel__btn">Cancel</a>
//   "This screen will automatically time out after 7 minutes."
//
// Wibmo is ICICI's 3-D Secure provider; the page is theirs, not Flipkart's.

const CORPORATE_INPUT = "#corporateId";
const EMPLOYEE_INPUT = "#employeeId";
const SUBMIT_CONTROL = 'a.primary__btn, a[onclick*="submit"], button:has-text("Submit"), input[type="submit"]';
/** Anything the ACS might render to take the code with — the OTP page has not
 *  been captured yet, so this is deliberately wide and is narrowed after Run 2. */
const OTP_INPUT =
  'input[name*="otp" i], input[id*="otp" i], input[type="tel"], input[type="number"], ' +
  'input[autocomplete="one-time-code"], input[type="password"]:not(#corporateId):not(#employeeId)';
const IDENTITY_REJECTED =
  /invalid|incorrect|not (?:valid|found|registered|recogni[sz]ed)|does not (?:exist|match)|wrong (?:corporate|employee)/i;

function pageText(ctx: PaymentContext): Promise<string> {
  return ctx.page
    .evaluate(() => document.body?.innerText || "")
    .then((t) => String(t).replace(/ /g, " "))
    .catch(() => "");
}

/**
 * Type the Corporate ID and the Employee ID — and ONLY type them.
 *
 * On this page there is no separate "send OTP" button: Submit is what makes
 * the bank text the handset. So the fields are filled here, before
 * requestAndAwaitOtp stamps `submittedAt`, and submitted in submitForOtp,
 * after it. Pressing Submit here would let a fast SMS land before the stamp
 * and be refused as a code from an earlier transaction.
 */
async function selectCorporateIdentity(
  ctx: PaymentContext,
  corporateId: string,
  employeeId: string
): Promise<void> {
  const corp = ctx.page.locator(CORPORATE_INPUT).first();
  try {
    await corp.waitFor({ state: "visible", timeout: 20_000 });
  } catch {
    throw new CheckoutFailure(
      "UNABLE_TO_PLACE_ORDER",
      `ICICI's 3-D Secure page did not show the Corporate ID field within 20s (on ${ctx.page.url().split("?")[0]})`
    );
  }
  await corp.fill(corporateId, { timeout: 10_000 });
  await ctx.page.locator(EMPLOYEE_INPUT).first().fill(employeeId, { timeout: 10_000 });
  ctx.log("info", `[pay] ICICI: corporate ${corporateId} / employee ${employeeId} entered — not yet submitted`);
}

/**
 * Press Submit — the request that makes ICICI dispatch the OTP — and return as
 * soon as the page shows it was accepted: the OTP step rendered. A rejection
 * of the identity is named with the bank's text and both ids, because it means
 * employeephones and the bank disagree, which is an onboarding fix.
 *
 * Do NOT wait for the SMS here — requestAndAwaitOtp owns that.
 */
async function submitForOtp(ctx: PaymentContext): Promise<void | "authenticated"> {
  const submit = ctx.page.locator(SUBMIT_CONTROL).first();
  if ((await submit.count()) === 0) {
    throw new CheckoutFailure("UNABLE_TO_PLACE_ORDER", "ICICI's 3-D Secure page has no Submit control");
  }
  const acsHost = hostOf(ctx);
  await submit.click({ timeout: 10_000 });
  ctx.log("info", "[pay] ICICI: identity submitted");

  // Every successful run has shown the OTP step within about a second of
  // Submit (0.5s on OD438798583788928100). On 2026-10-04 one job sat on the
  // identity page for the whole 30s and the bank never dispatched a code: the
  // click had not registered. So if nothing has changed RECLICK_AFTER_MS after
  // Submit, press it once more, and keep a picture if the page still does not
  // move — a worker has no other artifact of this page.
  const started = Date.now();
  let reclicked = false;
  while (Date.now() - started < OTP_STEP_TIMEOUT_MS) {
    const host = hostOf(ctx);
    if (host && host !== acsHost) {
      // Frictionless 3-D Secure: the bank let the transaction through without
      // asking for a code. Waiting for an SMS here would time the order out.
      ctx.log("info", `[pay] ICICI: no OTP step — ACS handed straight back to ${host} (authenticated without a code)`);
      return "authenticated";
    }
    const text = await pageText(ctx);
    const rejected = text.split(/\n+/).map((l) => l.trim()).find((l) => l && l.length < 200 && IDENTITY_REJECTED.test(l));
    if (rejected) {
      throw new CheckoutFailure(
        "CARD_AUTH_FAILED",
        `ICICI rejected the corporate identity (corporate ${ctx.corporateId ?? "?"}): ${rejected}`
      );
    }
    const otpVisible = await ctx.page.locator(OTP_INPUT).first().isVisible().catch(() => false);
    if (otpVisible || /\bOTP\b|one[- ]time password/i.test(text)) {
      ctx.log("info", "[pay] ICICI: OTP step is up");
      return;
    }
    if (!reclicked && Date.now() - started > RECLICK_AFTER_MS) {
      reclicked = true;
      const again = ctx.page.locator(SUBMIT_CONTROL).first();
      if (await again.isVisible().catch(() => false)) {
        await again.click({ timeout: 5_000 }).catch(() => undefined);
        ctx.log(
          "warn",
          `[pay] ICICI: identity page unchanged ${Math.round(RECLICK_AFTER_MS / 1000)}s after Submit — pressed Submit again`
        );
      }
    }
    await ctx.page.waitForTimeout(500);
  }
  const shot = await failureShot(ctx, "icici-identity");
  const shown = (await pageText(ctx)).replace(/\s+/g, " ").trim().slice(0, 200);
  throw new CheckoutFailure(
    "UNABLE_TO_PLACE_ORDER",
    `ICICI did not show an OTP step within ${Math.round(OTP_STEP_TIMEOUT_MS / 1000)}s of submitting the corporate identity ` +
      `(still on ${acsHost}; page says: "${shown}"${shot ? `; screenshot ${shot}` : ""})`
  );
}

/** How long the identity page may take to turn into the OTP page. Bounded well
 *  inside the 2-minute payment budget so the SMS wait keeps most of it. */
const OTP_STEP_TIMEOUT_MS = 45_000;
/** With no change this long after Submit, the click is treated as lost. */
const RECLICK_AFTER_MS = 10_000;

function hostOf(ctx: PaymentContext): string {
  try {
    return new URL(ctx.page.url()).hostname;
  } catch {
    return ""; // mid-navigation
  }
}

/** Full-page screenshot under debug/failures/, or null if it could not be taken. */
async function failureShot(ctx: PaymentContext, tag: string): Promise<string | null> {
  const shot = path.join("debug", "failures", `${new Date().toISOString().replace(/[:.]/g, "-")}-${tag}.png`);
  try {
    await fs.promises.mkdir(path.dirname(shot), { recursive: true });
    await ctx.page.screenshot({ path: shot, fullPage: true, timeout: 10_000 });
    return shot;
  } catch {
    return null;
  }
}

// ── THE OTP PAGE, as captured on Run 2 (card-2026-10-01T11-19-52) ───────────
//
//   same ACS host. <form id="enterOtpForm" onsubmit="return ValidateForm()">
//     "A one time password (OTP) has been sent to your registered mobile number
//      XXXXXX1297. Please enter the received OTP and submit…"
//     <input class="input-field" type="password" name="otpValue" maxlength="6">
//     <button id="submitBtn" onclick="enterOTP()">SUBMIT</button>
//     <button id="otpResend" onclick="resendOTP()">RESEND</button>
//     <button id="otpReset"  onclick="cancel()">CANCEL</button>
//   "This screen will automatically time out after 7 minutes."

const OTP_VALUE_INPUT = 'input[name="otpValue"], #enterOtpForm input[type="password"]';
const OTP_SUBMIT = "#submitBtn, #enterOtpForm button[type=submit]";
const OTP_REJECTED = /(?:invalid|incorrect|wrong|expired) (?:otp|one[- ]time|code)|otp (?:is )?(?:invalid|incorrect|expired)|authentication (?:failed|unsuccessful)|attempts? (?:left|remaining)/i;

/**
 * Type the code and confirm.
 *
 * Returning MEANS authenticated: the ACS has left the page (the redirect back
 * through the gateway to Flipkart), which is the only positive signal this page
 * gives. A rejection is named with the bank's text; "attempts remaining" is
 * included verbatim as the one warning of a lockout. NEVER logs the code.
 */
async function enterOtp(ctx: PaymentContext, otp: string): Promise<void> {
  const box = ctx.page.locator(OTP_VALUE_INPUT).first();
  try {
    await box.waitFor({ state: "visible", timeout: 15_000 });
  } catch {
    throw new CheckoutFailure("UNABLE_TO_PLACE_ORDER", "ICICI's OTP input was not on the page when the code arrived");
  }
  await box.fill(otp, { timeout: 10_000 });
  const acsHost = new URL(ctx.page.url()).hostname;
  await ctx.page.locator(OTP_SUBMIT).first().click({ timeout: 10_000 });
  ctx.log("info", "[pay] ICICI: OTP submitted");

  const deadline = Date.now() + 90_000;
  while (Date.now() < deadline) {
    let host = "";
    try {
      host = new URL(ctx.page.url()).hostname;
    } catch {
      /* mid-navigation */
    }
    if (host && host !== acsHost) {
      ctx.log("info", `[pay] ICICI: authenticated — ACS handed back to ${host}`);
      return;
    }
    const text = await pageText(ctx);
    const rejected = text.split(/\n+/).map((l) => l.trim()).find((l) => l && l.length < 240 && OTP_REJECTED.test(l));
    if (rejected) {
      throw new CheckoutFailure("CARD_AUTH_FAILED", `ICICI rejected the OTP: ${rejected}`);
    }
    await ctx.page.waitForTimeout(500);
  }
  throw new CheckoutFailure(
    "UNABLE_TO_PLACE_ORDER",
    `ICICI's page did not move on within 90s of submitting the OTP (still on ${acsHost})`
  );
}
