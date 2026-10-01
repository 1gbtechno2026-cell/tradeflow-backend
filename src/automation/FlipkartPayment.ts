import type { Page } from "playwright";
import { CheckoutFailure } from "../services/checkoutErrors.js";
import type { CardDetails } from "../paymentStrategies/types.js";
import type { LogLevel } from "../types.js";

export type PaymentLogger = (level: LogLevel, message: string, step?: string) => void;

function last4(value: string): string {
  return String(value || "").replace(/\D/g, "").slice(-4);
}

export interface OrderConfirmation {
  orderId: string;
  amount: string;
}

export class FlipkartPayment {
  constructor(
    private readonly page: Page,
    private readonly log: PaymentLogger
  ) {}

  // ───────────────────────────── Cash on Delivery ─────────────────────────────

  async isCodAvailable(): Promise<boolean> {
    // From the DOM: <div data-disabled="false" ...><span class="QbbOLN">Cash on Delivery</span>
    //
    // getByText(exact) rather than span:has-text(): :has-text() matches any element
    // CONTAINING the string, so a wrapping <span> matches too. Measured against
    // this DOM shape it returned 2 elements and .first() was the wrapper — which
    // starts the ancestor walk below from the wrong node.
    const codText = this.page.getByText("Cash on Delivery", { exact: true }).first();

    // count() is instantaneous, so without a wait a payment page that has not
    // finished rendering reports "COD unavailable" for a perfectly eligible order.
    // That failure gets likelier exactly when it matters least — under load, with
    // many workers, when the page is slowest.
    try {
      await codText.waitFor({ state: "attached", timeout: 15_000 });
    } catch {
      this.log("info", "[fk-pay] COD option never rendered on the payment page", "payment");
      return false;
    }

    // .last(), NOT .first() — Playwright returns ancestors in DOCUMENT order, so
    // .first() is the ancestor nearest the ROOT and .last() is the closest one.
    // Verified on this DOM shape: with an outer container at data-disabled="false"
    // and the COD row itself at "true", .first() read the outer one and reported
    // COD available while it was disabled. The order then proceeded and died at
    // Place Order.
    const codRow = codText.locator("xpath=ancestor::div[@data-disabled]").last();

    if ((await codRow.count()) > 0) {
      const isDisabled = await codRow.getAttribute("data-disabled");
      const available = isDisabled === "false";
      if (!available) {
        this.log("info", `[fk-pay] COD present but data-disabled="${isDisabled}"`, "payment");
      }
      return available;
    }

    // Secondary signal for when the attribute is absent: an "Unavailable" note
    // rendered inside the same row. Regex, so "Currently unavailable" and
    // "Unavailable for this pincode" both count.
    const row = codText.locator("xpath=ancestor::div[1]");
    if ((await row.getByText(/unavailable/i).count()) > 0) {
      this.log("info", "[fk-pay] COD present but marked unavailable", "payment");
      return false;
    }

    return true;
  }

  async payWithCod(): Promise<void> {
    // Same exact-text locator as isCodAvailable: span:has-text() can resolve to a
    // wrapping span, and clicking a wrapper's centre point is not guaranteed to hit
    // the option's own clickable area.
    const codOption = this.page.getByText("Cash on Delivery", { exact: true }).first();

    if ((await codOption.count()) === 0) {
      throw new CheckoutFailure("UNABLE_TO_PLACE_ORDER", "COD option not found on page");
    }

    // Click the COD option
    await codOption.click({ timeout: 10_000 });

    // Note: After clicking COD, Flipkart usually reveals a "Place Order" button.
    // We need to find that button. Since we don't have the exact DOM for the post-click state,
    // we look for common text patterns.
    const placeOrderBtn = this.page.locator('button:has-text("Place Order"), button:has-text("Confirm Order")').first();
    
    if (await placeOrderBtn.count() === 0) {
      throw new CheckoutFailure("UNABLE_TO_PLACE_ORDER", "COD Place Order button not found after selecting COD");
    }
    
    if (!(await placeOrderBtn.isEnabled())) {
      throw new CheckoutFailure("UNABLE_TO_PLACE_ORDER", "COD Place Order button is disabled");
    }

    await placeOrderBtn.click({ timeout: 10_000 });
    
    // Wait for the navigation to start
    await this.page.waitForTimeout(2000);
  }

  // ──────────────────────────────── Card ──────────────────────────────────────

  async selectCardPayment(): Promise<void> {
    // Exact text, for the same reason as the COD option — and a regex on the
    // separators, because Flipkart writes this label inconsistently
    // ("Credit / Debit / ATM Card" vs "Credit/Debit/ATM Card") and a hard-coded
    // spacing silently matches nothing.
    const cardTab = this.page.getByText(/^Credit\s*\/\s*Debit\s*\/\s*ATM Card$/i).first();

    if ((await cardTab.count()) === 0) {
      throw new CheckoutFailure("UNABLE_TO_PLACE_ORDER", "Credit/Debit Card option not found on page");
    }

    await cardTab.click({ timeout: 10_000 });

    // Trap #2: escape the saved-card panel if it opens.
    //
    // A comma-separated list of `text=` engines does NOT work —
    // 'text="Add a new card", text="Use another card"' matches 0 elements, measured.
    // So this check silently never fired, and fillCardForm would type a PAN into
    // the saved card's CVV box. A regex matches either wording in one locator.
    const addNewCardBtn = this.page.getByText(/Add a new card|Use another card|Add new card/i).first();

    if ((await addNewCardBtn.count()) > 0) {
      this.log("info", "[fk-pay] saved-card panel shown — switching to a new card", "payment");
      await addNewCardBtn.click({ timeout: 5000 });
    }
  }

  async fillCardForm(card: CardDetails): Promise<void> {
    this.log("info", `[fk-pay] filling card form for ****${last4(card.cardNumber)}`, "payment");

    try {
      // 1. Card Number (Trap #1: Use child card.cardNumber, NOT parentCardNumber)
      // Selector: #cc-input (from DOM: id="cc-input")
      await this.page.fill('#cc-input', card.cardNumber, { timeout: 10_000 });

      // 2. Expiry Date
      // The DOM shows a single input for MM / YY.
      // card.expiryMonth is "04" (string), card.expiryYear is "2028".
      // We need to format it as "04 / 28".
      const expiryStr = `${card.expiryMonth} / ${card.expiryYear.slice(-2)}`;
      await this.page.fill('input[autocomplete="cc-exp"]', expiryStr, { timeout: 10_000 });

      // 3. CVV
      // Selector: #cvv-input (from DOM: id="cvv-input")
      await this.page.fill('#cvv-input', card.cvv, { timeout: 10_000 });

      // 4. Checkbox: "Secure my card as per RBI guidelines"
      // We locate the label by text, then find the checkbox input inside it.
      const checkbox = this.page.locator('label:has-text("Secure my card as per RBI guidelines") input[type="checkbox"]').first();
      
      if (await checkbox.count() > 0 && !(await checkbox.isChecked())) {
        await checkbox.check({ timeout: 5000 });
      }

    } catch (error) {
      if (error instanceof CheckoutFailure) throw error;
      // Everything reaching here is a FILL failure — a missing field, an iframe we
      // did not reach into, a timeout. That is a platform/selector problem, never
      // the card's fault, so it must not retire the card from the pool.
      //
      // The previous version inspected the Playwright error text for "invalid" and
      // raised CARD_AUTH_FAILED on it. That could not detect a rejected card — a
      // fill timeout says "Timeout 10000ms exceeded", never "invalid" — but it
      // WOULD fire on Playwright's own "invalid selector" error, retiring a good
      // card because of a typo in a locator.
      //
      // Flipkart's actual inline rejection is read by detectCardRejectedByFlipkart,
      // which paymentPhase calls immediately after this method.
      throw new CheckoutFailure(
        "UNABLE_TO_PLACE_ORDER",
        `Failed to fill Flipkart's card form: ${error instanceof Error ? error.message.split("\n")[0] : String(error)}`
      );
    }
  }

  async submitCardForm(): Promise<void> {
    // Selector: button inside form#cards with text starting with "Pay"
    // DOM: <form id="cards"> ... <button ...>Pay ₹166 </button>
    // This is robust because it ignores the exact amount (which changes per order).
    const payButton = () => this.page.locator('form#cards button:has-text("Pay")').first();

    if ((await payButton().count()) === 0) {
      throw new CheckoutFailure("UNABLE_TO_PLACE_ORDER", "Pay button not found in card form");
    }

    if (!(await payButton().isEnabled())) {
      throw new CheckoutFailure("UNABLE_TO_PLACE_ORDER", "Pay button is disabled");
    }

    await payButton().click({ timeout: 10_000 });
    this.log("info", "[fk-pay] Pay pressed", "payment");

    // The first Pay on a card Flipkart has not seen does NOT go to the bank.
    // Measured on the 2026-10-01 run: Flipkart answered it with an RBI
    // tokenisation step — a bottom sheet ("Secure your card as per RBI
    // guidelines": Yes, secure my card / Maybe later), an inline "Secure my
    // card as per RBI guidelines" checkbox that is NOT in the DOM before this
    // moment, a ₹54 payment handling fee, and a re-rendered Pay button at the
    // new amount. The hand-off only happens after that consent and a second
    // Pay. Without this the run waited 60s for a bank page that never came.
    //
    // The operator's instruction is to secure (tokenise) the card, so the sheet
    // gets "Yes" and the checkbox gets ticked. The second Pay is pressed at most
    // once, and only when the consent checkbox is present — the one signal that
    // the first press was intercepted rather than slowly submitted — so a slow
    // redirect can never be double-charged.
    // <button data-testid="secure-card-primary-btn">Yes, secure my card</button>
    const consentYes = this.page
      .locator('[data-testid="secure-card-primary-btn"]')
      .or(this.page.getByText(/^\s*Yes, secure my card\s*$/i))
      .first();
    const rbiLabel = this.page.getByText(/^\s*Secure my card as per RBI guidelines\s*$/i).first();
    const rbiBox = this.page.locator('label:has-text("Secure my card as per RBI guidelines") input[type="checkbox"]').first();
    let secondPay = false;
    const deadline = Date.now() + 20_000;
    while (Date.now() < deadline) {
      if (!/flipkart\.com$/i.test(new URL(this.page.url()).hostname)) return; // handed off to the bank
      // Flipkart's own failure modal ends it here, with its sentence — not
      // after a 60s wait for a bank page that is not coming.
      const failed = await this.detectPaymentFailedModal();
      if (failed) throw failed;
      if (await consentYes.isVisible().catch(() => false)) {
        await consentYes.click({ timeout: 5000 });
        this.log("info", "[fk-pay] RBI consent sheet — chose 'Yes, secure my card'", "payment");
        await this.page.waitForTimeout(1500);
        continue;
      }
      const hasBox = (await rbiBox.count()) > 0;
      if (hasBox && !(await rbiBox.isChecked().catch(() => true))) {
        // The input is a styled control; check() fails when it is not the
        // visible element, so fall back to the label text, which is.
        try {
          await rbiBox.check({ timeout: 3000 });
        } catch {
          await rbiLabel.click({ timeout: 5000 });
        }
        this.log("info", "[fk-pay] ticked 'Secure my card as per RBI guidelines'", "payment");
        await this.page.waitForTimeout(800);
        continue;
      }
      if (hasBox && !secondPay) {
        const btn = payButton();
        if ((await btn.count()) > 0 && (await btn.isEnabled().catch(() => false))) {
          const label = (await btn.innerText().catch(() => "Pay")).replace(/\s+/g, " ").trim();
          await btn.click({ timeout: 10_000 });
          secondPay = true;
          this.log("info", `[fk-pay] consent given — pressed '${label}' again (fee may have been added)`, "payment");
          await this.page.waitForTimeout(1500);
          continue;
        }
      }
      await this.page.waitForTimeout(500);
    }
    // Still on Flipkart after the consent dance: waitForBankHandoff decides,
    // with its own timeout and inline-rejection check.
  }

  async waitForBankHandoff(timeoutMs = 60_000): Promise<string> {
    try {
      // Trap #3: Wait for a POSITIVE signal that we are no longer on Flipkart.
      // We wait until the hostname does NOT include 'flipkart.com'.
      await this.page.waitForFunction(() => {
        return !window.location.hostname.includes('flipkart.com');
      }, { timeout: timeoutMs });

      const url = this.page.url();
      this.log("info", `[fk-pay] Bank handoff successful. Landed on: ${url}`, "payment");
      return url;

    } catch (error) {
      // Before throwing a timeout, check if Flipkart rejected the card inline.
      const rejection = await this.detectCardRejectedByFlipkart();
      if (rejection) {
        throw rejection;
      }
      throw new CheckoutFailure("UNABLE_TO_PLACE_ORDER", `Bank handoff timeout after ${timeoutMs}ms`);
    }
  }

  /**
   * The modal Flipkart shows over the card form when its own side fails after
   * Pay: "Your payment couldn't be processed due to a technical error. Please
   * try again." + a "Retry payment" button. Seen 2026-10-01 on the POCO C85
   * run. Not the bank and not the card — reported as PAYMENT_FAILED with
   * Flipkart's sentence, and the card stays in the pool.
   */
  async detectPaymentFailedModal(): Promise<CheckoutFailure | null> {
    const modal = this.page.getByText(/payment (?:couldn'?t|could not|cannot) be processed|due to a technical error/i).first();
    if (!(await modal.isVisible().catch(() => false))) return null;
    const text = (await modal.innerText().catch(() => "")).replace(/\s+/g, " ").trim();
    this.log("warn", `[fk-pay] Flipkart payment failure modal: "${text}"`, "payment");
    return new CheckoutFailure("PAYMENT_FAILED", text || "Your payment couldn't be processed due to a technical error");
  }

  async detectCardRejectedByFlipkart(): Promise<CheckoutFailure | null> {
    const failed = await this.detectPaymentFailedModal();
    if (failed) return failed;
    // Look for inline error text on the card form.
    // Since we don't have the exact DOM for the error state, we look for common error classes/text.
    const errorElement = this.page.locator('.error-message, .inline-error, [data-testid="error-message"], span:has-text("Invalid Card")').first();
    
    if (await errorElement.count() > 0) {
      const text = await errorElement.innerText();
      const lowerText = text.toLowerCase();
      
      if (lowerText.includes('invalid card') || lowerText.includes('incorrect') || lowerText.includes('expired')) {
        return new CheckoutFailure("CARD_AUTH_FAILED", `Flipkart rejected card: ${text}`);
      }
    }
    
    return null; // No rejection found
  }

  // ─────────────────────────── After the bank ─────────────────────────────────

  async waitForOrderConfirmation(timeoutMs = 120_000): Promise<OrderConfirmation> {
    try {
      // Wait for the success page URL pattern
      await this.page.waitForURL(/order-confirmation|order-success|checkout\/success/, { timeout: timeoutMs });

      // NOTE: Since we don't have the confirmation page DOM, these are placeholders.
      // You will need to inspect the success page and replace these selectors.
      const orderIdElement = this.page.locator('.order-id, [data-testid="order-id"]').first();
      const amountElement = this.page.locator('.order-amount, [data-testid="order-amount"]').first();

      return {
        orderId: await orderIdElement.count() > 0 ? await orderIdElement.innerText() : '',
        amount: await amountElement.count() > 0 ? await amountElement.innerText() : ''
      };

    } catch (error) {
      // A CheckoutFailure raised inside the try (or by a nested call) must pass
      // straight through — re-wrapping it here would discard its code and detail,
      // and cardPool.verdictFor reads both.
      if (error instanceof CheckoutFailure) throw error;

      // Trap #4: the bank authorised, but Flipkart failed the order — stock going
      // during the 3DS round trip is the common one.
      //
      // Same comma-separated `text=` bug as the saved-card check: the original
      // 'text="Order Failed", text="Payment Failed", ...' matched 0 elements, so a
      // genuine Flipkart-side failure was reported as a bare 120s timeout with no
      // indication that the card had already been charged.
      const failureText = this.page.getByText(/Order Failed|Payment Failed|Something went wrong|Order could not be placed/i).first();

      if ((await failureText.count()) > 0) {
        throw new CheckoutFailure(
          "UNABLE_TO_PLACE_ORDER",
          `Order failed after bank auth: ${(await failureText.innerText()).replace(/\s+/g, " ").trim()}`
        );
      }

      throw new CheckoutFailure("UNABLE_TO_PLACE_ORDER", `Order confirmation timeout after ${timeoutMs}ms`);
    }
  }
}