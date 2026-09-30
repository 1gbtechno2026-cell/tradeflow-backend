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
    const payButton = this.page.locator('form#cards button:has-text("Pay")').first();

    if (await payButton.count() === 0) {
      throw new CheckoutFailure("UNABLE_TO_PLACE_ORDER", "Pay button not found in card form");
    }

    if (!(await payButton.isEnabled())) {
      throw new CheckoutFailure("UNABLE_TO_PLACE_ORDER", "Pay button is disabled");
    }

    await payButton.click({ timeout: 10_000 });
    
    // Give the page a moment to register the click and show a spinner/disable the button
    await this.page.waitForTimeout(2000);
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

  async detectCardRejectedByFlipkart(): Promise<CheckoutFailure | null> {
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