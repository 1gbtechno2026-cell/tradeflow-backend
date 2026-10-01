import type { Locator, Page } from "playwright";
import type { AddressDetails } from "../types.js";
import {
  evaluate as evalPage,
  navigateWithRetry,
  waitForFunction as waitFn,
  waitForNav,
  waitWithRetry,
  type JobLogger,
} from "./helpers.js";
import { sleep } from "../services/browser.js";
import { CheckoutFailure, classifyPageText } from "../services/checkoutErrors.js";

/** Remembered Flipkart checkout URLs when a button or page is missing. pageUID is per-session — omit it. */
const VIEWCART_URL = "https://www.flipkart.com/viewcart?exploreMode=TRUE&preference=FLIPKART";
const VIEWCHECKOUT_URL =
  "https://www.flipkart.com/viewcheckout?view=FLIPKART&marketplace=FLIPKART&tr_tenant=FLIPKART";
const PAYMENTS_URL = "https://www.flipkart.com/payments";

export class OutOfStockPincodeError extends Error {
  readonly failedStep = "out_of_stock_pincode" as const;
  constructor(
    public rawMessage: string,
    public pincode: string
  ) {
    super(rawMessage);
    this.name = "OutOfStockPincodeError";
  }
}

function isNavDestroyed(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  return /Execution context was destroyed|most likely because of a navigation|Frame was detached|Target closed/i.test(
    msg
  );
}

const DELAYS = {
  short: 100,
  medium: 200,
  long: 300,
};

/**
 * FlipkartCheckout — Flipkart's UI only: product, cart, address, GST, and its
 * own payment form. Ends at the bank redirect; everything past it belongs to
 * a payment strategy, because Flipkart and a bank ship UI changes on
 * completely independent schedules.
 *
 * Was the winning arm of an A/B against the original implementation; the
 * losing arm is in git history rather than a second file. Login/OTP omitted
 * (session restore).
 */
export class FlipkartCheckout {
  summaryQty = 0;
  private jobPincode = "";
  private gstMandatory = true;

  constructor(
    private page: Page,
    private productUrl: string,
    private log: JobLogger = (level, message) => console.log(`[${level}] ${message}`)
  ) {}

  setCheckoutFlags(pincode: string, gstMandatory = true) {
    this.jobPincode = String(pincode || "").replace(/\D/g, "").slice(-6);
    this.gstMandatory = gstMandatory;
  }

  /** While emptying a stale cart, pincode/OOS prompts belong to old items being removed — never fail on them. */
  private suppressLiveBlocker = false;

  /** Read Flipkart text as it appears — fail immediately on pin / OOS / not deliverable. */
  private async throwIfLivePageBlocked(): Promise<void> {
    if (this.suppressLiveBlocker) return;
    const text = await this.evaluate(() => (document.body?.innerText || "").replace(/\u00a0/g, " "));
    const fail = classifyPageText(text, this.jobPincode);
    if (fail) throw fail;
  }

  setProductUrl(url: string) {
    this.productUrl = url;
  }

  private async evaluate<T>(fn: (...args: any[]) => T, ...args: unknown[]): Promise<T> {
    return evalPage(this.page, fn as (...args: any[]) => T, ...args);
  }

  private async waitForFunction(
    fn: (...args: any[]) => unknown,
    options: { timeout?: number } = {},
    ...args: unknown[]
  ): Promise<void> {
    await waitFn(this.page, fn, options, ...args);
  }

  private async ensurePageValid(): Promise<void> {
    const STALE_ERRORS = [
      "Execution context was destroyed",
      "Session closed",
      "Target closed",
      "Protocol error",
      "Frame detached",
      "Detached",
    ];
    const isStaleError = (err: unknown): boolean => {
      const msg = err instanceof Error ? err.message : String(err);
      return STALE_ERRORS.some((s) => msg.includes(s));
    };
    let staleCount = 0;
    while (staleCount < 3) {
      try {
        await this.evaluate(() => document.readyState);
        return;
      } catch (err) {
        if (!isStaleError(err)) throw err;
        staleCount++;
        console.log(`[ensurePageValid] Page context stale (attempt ${staleCount}/3)`);
        const currentUrl = this.page.url().split("?")[0];
        await sleep(500);
        try {
          await this.page.goto(currentUrl, { waitUntil: "domcontentloaded", timeout: 15000 });
          await sleep(500);
          await this.evaluate(() => document.readyState);
          return;
        } catch (navErr) {
          if (!isStaleError(navErr)) return;
          await sleep(1000);
        }
      }
    }
  }

  async captureProductDetails(): Promise<{ model: string; colour: string; amount: string }> {
    try {
      const result = await this.evaluate(() => {
        // ─── COLOUR ────────────────────────────────────────────────────────
        // Primary: <div font="default-fk-font-l">Selected Color:</div>
        //          + next sibling = the colour name ("Blue Shadow")
        let colour = "";
        const labelDivs = Array.from(
          document.querySelectorAll('div[font="default-fk-font-l"]')
        ) as HTMLElement[];
        for (const lbl of labelDivs) {
          const t = (lbl.innerText || lbl.textContent || "").trim().toLowerCase();
          if (
            t === "selected color:" ||
            t === "selected colour:" ||
            t === "color:" ||
            t === "colour:"
          ) {
            // Walk forward through following siblings until we find a non-empty one.
            let sib = lbl.nextElementSibling as HTMLElement | null;
            while (sib) {
              const v = (sib.innerText || sib.textContent || "").trim();
              if (v && v.length >= 2 && v.length <= 60) {
                colour = v;
                break;
              }
              sib = sib.nextElementSibling as HTMLElement | null;
            }
            if (colour) break;
          }
        }
        // Fallback: any short alphabetic font-l div if no labelled match.
        if (!colour) {
          for (const el of labelDivs) {
            const t = (el.innerText || el.textContent || "").trim();
            if (
              t.length >= 3 &&
              t.length <= 40 &&
              !/[₹\d:]/.test(t) &&
              /^[A-Za-z][A-Za-z\s'/-]*$/.test(t)
            ) {
              colour = t;
              break;
            }
          }
        }

        // ─── PRICE (listing selling price, not cart total) ──────────────────
        // Never Math.max — that picks MRP / EMI / other-variant numbers
        // (this run captured 2990). Skip strikethrough; among live ₹ amounts
        // in the buy box, the selling price is the lowest plausible value.
        const parseRupee = (raw: string) => {
          const v = Number(String(raw).replace(/,/g, ""));
          return Number.isFinite(v) && v >= 100 && v < 10_000_000 ? v : null;
        };
        const isStruck = (el: HTMLElement | null) => {
          let cur: HTMLElement | null = el;
          for (let i = 0; i < 5 && cur; i++) {
            const cs = window.getComputedStyle(cur);
            const deco = `${cs.textDecorationLine || ""} ${cs.textDecoration || ""}`;
            if (/line-through/i.test(deco)) return true;
            cur = cur.parentElement;
          }
          return false;
        };
        const rupeesIn = (el: HTMLElement, skipStruck: boolean) => {
          const out: number[] = [];
          const re = /₹\s*([\d,]+(?:\.\d{1,2})?)/g;
          const nodes = [el, ...Array.from(el.querySelectorAll<HTMLElement>("*"))];
          for (const n of nodes) {
            if (skipStruck && isStruck(n)) continue;
            const t = (n.childNodes.length && [...n.childNodes].every((c) => c.nodeType === 3)
              ? n.textContent
              : n.innerText) || "";
            let m: RegExpExecArray | null;
            const local = t.replace(/\s+/g, " ");
            re.lastIndex = 0;
            while ((m = re.exec(local))) {
              const v = parseRupee(m[1]);
              if (v != null) out.push(v);
            }
          }
          return [...new Set(out)];
        };

        let amount = "";
        let amountVia = "";
        const gradients = Array.from(
          document.querySelectorAll('div[style*="linear-gradient"]')
        ) as HTMLElement[];
        const isBuyNowGradient = (style: string) =>
          style.includes("rgb(255, 229, 31)") &&
          style.includes("rgb(255, 205, 3)");
        for (const g of gradients) {
          const style = g.getAttribute("style") || "";
          if (!isBuyNowGradient(style)) continue;
          let cur: HTMLElement | null = g;
          for (let depth = 0; depth < 10 && cur; depth++) {
            const live = rupeesIn(cur, true);
            if (live.length) {
              amount = String(Math.min(...live));
              amountVia = `buy-now-live-min[${live.join(",")}]`;
              break;
            }
            cur = cur.parentElement;
          }
          if (amount) break;
        }
        if (!amount) {
          const priceCandidates = Array.from(
            document.querySelectorAll('div[font="default-fk-font-m"]')
          ) as HTMLElement[];
          const live: number[] = [];
          for (const el of priceCandidates) {
            if (isStruck(el)) continue;
            const box = el.getBoundingClientRect();
            // Recently-viewed tiles sit lower on the page (1231 last run).
            if (box.y > 560 || box.x < window.innerWidth * 0.28) continue;
            const t = (el.innerText || el.textContent || "").trim();
            const m = t.match(/^₹\s*([\d,]+(?:\.\d{1,2})?)$/);
            if (m) {
              const v = parseRupee(m[1]);
              if (v != null) live.push(v);
            }
          }
          if (live.length) {
            amount = String(Math.min(...live));
            amountVia = `font-m-live-min[${live.join(",")}]`;
          }
        }
        console.log(`[Price] listing amount=${amount || "(none)"} via ${amountVia || "miss"}`);

        // ─── MODEL ─────────────────────────────────────────────────────────
        let model = "";
        const h1 = document.querySelector("h1");
        if (h1) {
          const t = (h1.innerText || h1.textContent || "").trim();
          if (t.length > 5) model = t;
        }
        if (!model) {
          const span = document.querySelector("span.B_NuCI") as HTMLElement | null;
          if (span) {
            const t = (span.innerText || span.textContent || "").trim();
            if (t.length > 5) model = t;
          }
        }

        return { model, colour, amount, amountVia };
      });
      console.log(`[Price] listing amount=${result.amount || "(none)"} via ${result.amountVia || "miss"}`);
      let colour = result.colour;
      const pdp = await this.scanFlipkartPdp();
      const selectedColor = pdp.colors.find((c) => c.selected);
      if (!colour && selectedColor?.name) colour = selectedColor.name;
      if (pdp.notDeliverable || pdp.locationSliderOpen) {
        console.log(
          `[PDP] notDeliverable=${pdp.notDeliverable} slider=${pdp.locationSliderOpen} pin=${pdp.selectedPincode || "-"} colors=${pdp.colors.map((c) => `${c.name}${c.selected ? "*" : ""}${c.outOfStock ? "(oos)" : ""}`).join(",") || "none"}`
        );
      }
      return { model: result.model, colour, amount: result.amount };
    } catch {
      return { model: "", colour: "", amount: "" };
    }
  }

  /**
   * Payable total on viewcart / checkout — the number next to Place Order
   * and under Price Details → Total Amount (e.g. 1,735). Not listing MRP.
   */
  async captureCartPayable(): Promise<{ amount: string; source: string }> {
    try {
      const result = await this.evaluate(() => {
        const parseAmt = (raw: string) => {
          const v = Number(String(raw).replace(/[₹,\s]/g, ""));
          return Number.isFinite(v) && v >= 100 && v < 10_000_000 ? v : null;
        };
        const isStruck = (el: HTMLElement | null) => {
          let cur: HTMLElement | null = el;
          for (let i = 0; i < 5 && cur; i++) {
            const cs = window.getComputedStyle(cur);
            if (/line-through/i.test(`${cs.textDecorationLine || ""} ${cs.textDecoration || ""}`)) {
              return true;
            }
            cur = cur.parentElement;
          }
          return false;
        };
        const compact = (s: string) => s.replace(/\s+/g, " ").trim();

        // 1) Price Details row labelled "Total Amount"
        const nodes = Array.from(document.querySelectorAll("div,span")) as HTMLElement[];
        for (const el of nodes) {
          const t = compact(el.innerText || el.textContent || "");
          if (!/^Total Amount$/i.test(t) && !/^Total Amount\b/i.test(t)) continue;
          if (t.length > 40) continue;
          let scope: HTMLElement | null = el.parentElement;
          for (let d = 0; d < 5 && scope; d++) {
            const blob = compact(scope.innerText || "");
            const m = blob.match(/Total Amount\s*(?:₹\s*)?([\d,]+(?:\.\d{1,2})?)/i);
            if (m) {
              const v = parseAmt(m[1]);
              if (v != null) return { amount: String(v), source: "price-details-total-amount" };
            }
            scope = scope.parentElement;
          }
        }

        // 2) Place Order cluster: skip struck MRP (1,790), keep 1,735
        for (const el of nodes) {
          const t = compact(el.textContent || "");
          if (!/^Place Order$/i.test(t)) continue;
          if ((el.textContent || "").length > 24) continue;
          let root: HTMLElement | null = el.parentElement;
          for (let d = 0; d < 8 && root; d++) {
            const live: number[] = [];
            for (const n of Array.from(root.querySelectorAll("div,span")) as HTMLElement[]) {
              if (isStruck(n)) continue;
              const raw = compact(n.textContent || "");
              if (!/^\d{1,3}(?:,\d{2,3})+(?:\.\d{1,2})?$|^\d{3,7}(?:\.\d{1,2})?$/.test(raw)) continue;
              const v = parseAmt(raw);
              if (v != null) live.push(v);
            }
            const uniq = [...new Set(live)];
            if (uniq.length === 1) return { amount: String(uniq[0]), source: "place-order-cluster" };
            if (uniq.length > 1) {
              return { amount: String(Math.min(...uniq)), source: `place-order-cluster-min[${uniq.join(",")}]` };
            }
            root = root.parentElement;
          }
        }

        // 3) Body text fallback
        const body = compact(document.body?.innerText || "");
        const m = body.match(/Total Amount\s*(?:₹\s*)?([\d,]+(?:\.\d{1,2})?)/i);
        if (m) {
          const v = parseAmt(m[1]);
          if (v != null) return { amount: String(v), source: "body-total-amount" };
        }
        return { amount: "", source: "miss" };
      });
      console.log(`[CartPayable] amount=${result.amount || "(none)"} source=${result.source} url=${this.page.url()}`);
      return result;
    } catch (err) {
      console.log(`[CartPayable] failed: ${err instanceof Error ? err.message : String(err)}`);
      return { amount: "", source: "error" };
    }
  }

  async navigateToProduct(): Promise<void> {
    console.log("Opening product page...");
    await navigateWithRetry(this.page, this.productUrl, {
      timeoutMs: 10000,
      maxRetries: 5,
    });
    await sleep(DELAYS.medium);
  }

  async detectCheckoutBlocker(pincode = ""): Promise<CheckoutFailure | null> {
    const scan = await this.evaluate(() => {
      const visible = (el: Element) => {
        const r = (el as HTMLElement).getBoundingClientRect();
        const st = getComputedStyle(el as HTMLElement);
        return r.width > 24 && r.height > 18 && st.visibility !== "hidden" && Number(st.opacity) > 0.1;
      };
      let notifyBtn = false;
      for (const el of Array.from(document.querySelectorAll("button, a, [role=button], div, span"))) {
        if (!visible(el)) continue;
        const t = (el.textContent || "").replace(/\s+/g, " ").trim();
        if (t === "Notify Me") notifyBtn = true;
      }
      return { notifyBtn, body: document.body?.innerText || "" };
    });
    if (scan.notifyBtn) {
      return new CheckoutFailure("PRODUCT_UNAVAILABLE", "Notify Me — product is not available to purchase");
    }
    const fromText = classifyPageText(scan.body, pincode);
    if (fromText) return fromText;
    const pdp = await this.scanFlipkartPdp();
    if (pdp.locationSliderOpen || pdp.notDeliverable) {
      if (pdp.locationSliderOpen) {
        await this.dismissSelectLocationSlider().catch(() => undefined);
      }
      return this.notAvailableOnPin(pincode || pdp.selectedPincode);
    }
    return null;
  }

  private notAvailableOnPin(pincode = ""): CheckoutFailure {
    const pin = String(pincode || "").replace(/\D/g, "").slice(-6);
    return new CheckoutFailure(
      "ITEM_NOT_DELIVERABLE",
      pin ? `This product is not available on this pin (${pin})` : "This product is not available on this pin"
    );
  }

  /**
   * Flipkart PDP / location sheet — text, #msite-bottomsheet, and inline border-color.
   * Never use hashed React classes (css-g5y9jx, v1zwn221, …).
   */
  private async scanFlipkartPdp(): Promise<{
    locationSliderOpen: boolean;
    selectedAddressLabel: string;
    selectedAddressLine: string;
    selectedPincode: string;
    notDeliverable: boolean;
    notDeliverableText: string;
    colors: Array<{ name: string; selected: boolean; outOfStock: boolean; href: string }>;
  }> {
    return this.evaluate(() => {
      const compact = (s: string) => (s || "").replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
      const visible = (el: Element | null) => {
        if (!el) return false;
        const r = (el as HTMLElement).getBoundingClientRect();
        const st = getComputedStyle(el as HTMLElement);
        return (
          r.width > 8 &&
          r.height > 8 &&
          st.visibility !== "hidden" &&
          st.display !== "none" &&
          Number(st.opacity) > 0.05
        );
      };

      const byId = document.getElementById("msite-bottomsheet") as HTMLElement | null;
      const sheet =
        (byId && visible(byId) ? byId : null) ||
        (Array.from(document.querySelectorAll("[id*='bottomsheet'], [id*='bottom-sheet']")) as HTMLElement[]).find(
          (el) => visible(el)
        ) ||
        null;
      const sheetText = compact(sheet?.innerText || "");
      const pageText = compact(document.body?.innerText || "");
      const looksLikeLocationSheet =
        /select location/i.test(sheetText) ||
        /select delivery address/i.test(sheetText) ||
        (/select location/i.test(pageText) &&
          /select delivery address/i.test(pageText) &&
          /enter location manually|use my current location/i.test(pageText));
      const isCartSuccess = /go to cart|added to cart/i.test(sheetText) && !/select delivery address/i.test(sheetText);
      const locationSliderOpen = Boolean(looksLikeLocationSheet && !isCartSuccess && (sheet ? visible(sheet) : true));

      let selectedAddressLabel = "";
      let selectedAddressLine = "";
      let selectedPincode = "";
      const scope = sheet && visible(sheet) ? sheet : document.body;
      for (const n of Array.from(scope.querySelectorAll("div, span")) as HTMLElement[]) {
        const t = compact(n.childNodes.length <= 3 ? n.textContent || "" : "");
        if (!t || t.length > 90) continue;
        const pinM = t.match(/\b([1-9]\d{5})\b/);
        if (!pinM) continue;
        if (!/home|work|office|deals|limited|pvt|private|,/i.test(t)) continue;
        const parent = n.parentElement;
        if (!parent) continue;
        const kids = Array.from(parent.children) as HTMLElement[];
        const sib =
          kids[kids.indexOf(n) + 1] ||
          (n.nextElementSibling as HTMLElement | null) ||
          (parent.children[1] as HTMLElement | null);
        const sibText = compact(sib && sib !== n ? sib.innerText || sib.textContent || "" : "");
        selectedAddressLabel = t;
        selectedPincode = pinM[1];
        if (sibText && sibText !== t && sibText.length > 8 && sibText.length < 220 && !/^select /i.test(sibText)) {
          selectedAddressLine = sibText;
          break;
        }
      }

      let notDeliverable = false;
      let notDeliverableText = "";
      for (const el of Array.from(document.querySelectorAll("div, span, p")) as HTMLElement[]) {
        const t = compact(el.textContent || "");
        if (!t || t.length > 80) continue;
        if (/^not deliverable in your (location|area)$/i.test(t)) {
          notDeliverable = true;
          notDeliverableText = t;
          break;
        }
      }
      if (!notDeliverable) {
        for (const el of Array.from(document.querySelectorAll("div, span")) as HTMLElement[]) {
          const t = compact(el.textContent || "");
          if (!/not deliverable/i.test(t) || t.length > 90) continue;
          const color = getComputedStyle(el).color.replace(/\s+/g, "");
          if (color === "rgb(229,72,5)" || /not deliverable in your/i.test(t)) {
            notDeliverable = true;
            notDeliverableText = t;
            break;
          }
        }
      }

      const colorFromHref = (href: string) => {
        const path = href.split("?")[0];
        const slug = (path.split("/p/")[0] || path).split("/").filter(Boolean).pop() || "";
        const parts = slug.replace(/-\d+-gb.*$/i, "").split("-").filter(Boolean);
        return parts.length ? parts[parts.length - 1] : "";
      };
      const colors: Array<{ name: string; selected: boolean; outOfStock: boolean; href: string }> = [];
      for (const a of Array.from(document.querySelectorAll("a[href*='/p/itm']")) as HTMLAnchorElement[]) {
        if (!a.querySelector("img")) continue;
        const style = `${a.getAttribute("style") || ""};${a.style?.cssText || ""}`;
        const borderColor = getComputedStyle(a).borderColor.replace(/\s+/g, "");
        const selected =
          /border-color:\s*rgb\(\s*31\s*,\s*31\s*,\s*31\s*\)/i.test(style) || borderColor === "rgb(31,31,31)";
        const unselected =
          /border-color:\s*rgb\(\s*214\s*,\s*214\s*,\s*214\s*\)/i.test(style) || borderColor === "rgb(214,214,214)";
        if (!selected && !unselected) continue;
        const href = a.getAttribute("href") || "";
        const outOfStock = Array.from(a.querySelectorAll("div, span")).some((el) =>
          /^out of stock$/i.test(compact(el.textContent || ""))
        );
        const name = colorFromHref(href);
        if (!name || colors.some((c) => c.href === href)) continue;
        colors.push({ name, selected, outOfStock, href });
      }

      return {
        locationSliderOpen,
        selectedAddressLabel,
        selectedAddressLine,
        selectedPincode,
        notDeliverable,
        notDeliverableText,
        colors,
      };
    });
  }

  private async detectSelectLocationSlider(): Promise<boolean> {
    const scan = await this.scanFlipkartPdp();
    return scan.locationSliderOpen;
  }

  private async dismissSelectLocationSlider(): Promise<void> {
    const size = this.page.viewportSize() || { width: 1440, height: 900 };
    await this.page.mouse.click(Math.round(size.width * 0.2), Math.round(size.height * 0.4));
    await this.waitUntil(async () => !(await this.detectSelectLocationSlider()), 2500, 100, "Select location slider to close");
  }

  private async failIfLocationSlider(pincode = ""): Promise<void> {
    const scan = await this.scanFlipkartPdp();
    if (!scan.locationSliderOpen && !scan.notDeliverable) return;
    if (scan.locationSliderOpen) {
      console.log(
        `[Cart] #msite-bottomsheet Select location — ${scan.selectedAddressLabel || "no label"} | ${scan.selectedAddressLine || "no line"} | pin=${scan.selectedPincode || pincode || "?"}`
      );
      await this.dismissSelectLocationSlider().catch(() => undefined);
    } else {
      console.log(`[Cart] Flipkart: ${scan.notDeliverableText || "Not deliverable in your location"}`);
    }
    throw this.notAvailableOnPin(pincode || scan.selectedPincode);
  }

  private async tapPoint(x: number, y: number, label: string) {
    console.log(`Click ${label} at (${x.toFixed(0)}, ${y.toFixed(0)})`);
    await this.page.mouse.click(x, y);
  }

  /** Poll until the UI is ready — no fixed sleeps. Logs only when it actually waits. */
  private async waitUntil(
    fn: () => Promise<boolean>,
    timeoutMs: number,
    intervalMs = 120,
    label?: string
  ): Promise<boolean> {
    const started = Date.now();
    const secs = (ms: number) => `${(ms / 1000).toFixed(1)}s`;
    try {
      await this.throwIfLivePageBlocked();
      if (await fn()) return true;
    } catch (err) {
      if (err instanceof OutOfStockPincodeError || err instanceof CheckoutFailure) throw err;
    }
    if (label) {
      console.log(`[wait] waiting up to ${secs(timeoutMs)} for ${label}`);
    }
    const deadline = started + timeoutMs;
    while (Date.now() < deadline) {
      await sleep(intervalMs);
      try {
        await this.throwIfLivePageBlocked();
        if (await fn()) {
          if (label) console.log(`[wait] ready after ${secs(Date.now() - started)} — ${label}`);
          return true;
        }
      } catch (err) {
        if (err instanceof OutOfStockPincodeError || err instanceof CheckoutFailure) throw err;
      }
    }
    if (label) console.log(`[wait] timed out after ${secs(Date.now() - started)} — ${label}`);
    return false;
  }

  /** Find a short exact label and click its 44px action bar (not a huge wrapper). */
  private async findLabelPressable(exact: RegExp): Promise<{ x: number; y: number; w: number; h: number; text: string } | null> {
    return this.evaluate((src: string) => {
      const re = new RegExp(src, "i");
      const hits: Array<{ x: number; y: number; w: number; h: number; text: string; bottom: number }> = [];
      const nodes = Array.from(document.querySelectorAll("div,span,button")) as HTMLElement[];
      for (const n of nodes) {
        const t = (n.textContent || "").replace(/\s+/g, " ").trim();
        if (!re.test(t) || t.length > 28) continue;
        let el: HTMLElement | null = n;
        let best = n;
        while (el && el !== document.body) {
          const r = el.getBoundingClientRect();
          if (r.height >= 36 && r.height <= 80 && r.width >= 70 && r.width <= 600) {
            best = el;
            break;
          }
          el = el.parentElement;
        }
        const r = best.getBoundingClientRect();
        if (r.width < 70 || r.height < 32) continue;
        const cs = window.getComputedStyle(best);
        if (cs.display === "none" || cs.visibility === "hidden" || Number(cs.opacity) === 0) continue;
        hits.push({
          x: r.x + r.width / 2,
          y: r.y + r.height / 2,
          w: r.width,
          h: r.height,
          text: t,
          bottom: r.bottom,
        });
      }
      if (!hits.length) return null;
      hits.sort((a, b) => b.bottom - a.bottom);
      const pick = hits[0];
      return { x: pick.x, y: pick.y, w: pick.w, h: pick.h, text: pick.text };
    }, exact.source);
  }

  private async headerCartCount(): Promise<number> {
    return this.evaluate(() => {
      const t = (document.body?.innerText || "").replace(/\s+/g, " ");
      const m = t.match(/(\d+)\s+Cart\b/i) || t.match(/\bCart\s+(\d+)/i);
      return m ? Number(m[1]) : 0;
    });
  }

  private async pdpShowsGoToCart(): Promise<boolean> {
    const exact = this.page.getByText("Go to cart", { exact: true }).first();
    if (await exact.isVisible().catch(() => false)) return true;
    return this.evaluate(() =>
      Array.from(document.querySelectorAll("div, span, button, a")).some((el) => {
        const t = (el.textContent || "").replace(/\s+/g, " ").trim();
        return /^go to cart$/i.test(t);
      })
    );
  }

  private async productLooksAddedToCart(): Promise<boolean> {
    if (await this.pdpShowsGoToCart()) return true;
    if ((await this.headerCartCount()) >= 1) return true;
    return this.evaluate(() => /added to cart/i.test((document.body?.innerText || "").replace(/\s+/g, " ")));
  }

  /** 44x44 cart icon in the Buy now / Buy with EMI bar (iPhone-style layout). */
  private async clickStickyCartIcon(): Promise<boolean> {
    const box = await this.evaluate(() => {
      const label = Array.from(document.querySelectorAll("div,span")).find((el) => {
        const t = (el.textContent || "").replace(/\s+/g, " ").trim();
        return t === "Buy now" || t === "Buy with EMI";
      });
      let root: HTMLElement | null = (label as HTMLElement) || document.body;
      for (let i = 0; i < 12 && root && root !== document.body; i++) {
        root = root.parentElement;
      }
      const scopes = [root, document.body].filter(Boolean) as HTMLElement[];
      for (const scope of scopes) {
        for (const svg of Array.from(scope.querySelectorAll("svg"))) {
          if (!/AddToCart|AddedToCart/i.test(svg.innerHTML)) continue;
          let el = svg.parentElement as HTMLElement | null;
          while (el && el !== document.body) {
            const r = el.getBoundingClientRect();
            if (r.width >= 40 && r.width <= 52 && r.height >= 40 && r.height <= 52 && r.top > 80) {
              const cs = window.getComputedStyle(el);
              if (cs.display !== "none" && cs.visibility !== "hidden") {
                return { x: r.x + r.width / 2, y: r.y + r.height / 2, w: r.width, h: r.height };
              }
            }
            el = el.parentElement;
          }
        }
      }
      return null;
    });
    if (!box) return false;
    console.log(`=== Sticky cart icon ${Math.round(box.w)}x${Math.round(box.h)} at (${Math.round(box.x)}, ${Math.round(box.y)}) ===`);
    await this.page.mouse.click(box.x, box.y);
    return true;
  }

  private async pressAddToCartControl(): Promise<void> {
    const text = this.page.getByText("Add to cart", { exact: true }).first();
    if (await text.isVisible().catch(() => false)) {
      await text.scrollIntoViewIfNeeded().catch(() => {});
      console.log("=== Add to cart text (visible) ===");
      await text.click({ timeout: 5000 });
      return;
    }
    if (await this.clickStickyCartIcon()) return;
    throw new Error("Add to cart not found (no cart icon next to Buy now and no Add to cart text)");
  }

  /**
   * Buying-bot FlipkartPlatform.addToCart — SVG clipPath / path / text / gradient
   * + mouse → touch → DOM events → React fiber. Then TF-style blocker checks.
   */
  async clickAddToCart(pincode = ""): Promise<void> {
    console.log("[Checkout2/Buying-bot] Waiting for Add to Cart button...");
    const block = await this.detectCheckoutBlocker(pincode);
    if (block) throw block;
    if (await this.pdpShowsGoToCart()) {
      console.log("[Cart] PDP shows Go to cart — product already in cart, skipping add click");
      return;
    }

    await waitWithRetry(
      this.page,
      async () => {
        await this.waitForFunction(
          () => {
            if (document.querySelector('clipPath[id*="AddToCart"]')) return true;
            const allPaths = document.querySelectorAll("path");
            for (const p of allPaths) {
              if ((p.getAttribute("d") || "").startsWith("M17 18.375H7.35116")) return true;
            }
            const labels = document.querySelectorAll("div.css-146c3p1, div, span, button");
            for (const label of labels) {
              const text = label.textContent?.trim().toLowerCase();
              if (text === "add to cart" || text === "add to bag") return true;
            }
            // Buying-bot: white gradient Add-to-Cart shell
            if (document.querySelectorAll('div.css-g5y9jx[style*="border-radius: 12px"]').length > 0) {
              return true;
            }
            return false;
          },
          { timeout: 10000 }
        );
      },
      { label: "Add to Cart button (Buying-bot)", timeoutMs: 10000, maxRetries: 5 }
    );

    const beforeAdd = await this.scanFlipkartPdp();
    if (beforeAdd.notDeliverable || beforeAdd.locationSliderOpen) {
      throw this.notAvailableOnPin(pincode || beforeAdd.selectedPincode);
    }
    const before = await this.headerCartCount();

    const btnCoords = await this.evaluate(() => {
      const logs: string[] = [];
      const addToCartClip = document.querySelector('clipPath[id*="AddToCart"]');
      if (addToCartClip) {
        logs.push("Found SVG with AddToCart clipPath id");
        let best: HTMLElement =
          (addToCartClip.closest("svg") as unknown as HTMLElement) ||
          (addToCartClip as unknown as HTMLElement);
        let el: HTMLElement | null = best;
        while (el && el !== document.body) {
          const style = el.getAttribute("style") || "";
          if ((style.includes("width: 44px") || style.includes("width:44px")) && style.includes("border-radius")) {
            best = el;
            break;
          }
          if (style.includes("cursor") || el.getAttribute("role") === "button") {
            best = el;
            break;
          }
          el = el.parentElement;
        }
        best.scrollIntoView({ block: "center" });
        const rect = best.getBoundingClientRect();
        return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2, variant: "svg-clip", logs };
      }
      const allPaths = document.querySelectorAll("path");
      for (const p of allPaths) {
        if ((p.getAttribute("d") || "").startsWith("M17 18.375H7.35116")) {
          logs.push("Found SVG cart icon by path d");
          let best: HTMLElement = p as unknown as HTMLElement;
          let el: HTMLElement | null = p as unknown as HTMLElement;
          while (el && el !== document.body) {
            const style = el.getAttribute("style") || "";
            if ((style.includes("width: 44px") || style.includes("width:44px")) && style.includes("border-radius")) {
              best = el;
              break;
            }
            if (style.includes("cursor") || el.getAttribute("role") === "button") {
              best = el;
              break;
            }
            el = el.parentElement;
          }
          best.scrollIntoView({ block: "center" });
          const rect = best.getBoundingClientRect();
          return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2, variant: "svg", logs };
        }
      }
      const labels = document.querySelectorAll("div, span, button");
      for (const label of labels) {
        const text = label.textContent?.trim().toLowerCase();
        if (text === "add to cart" || text === "add to bag") {
          logs.push(`Found text button: "${label.textContent?.trim()}"`);
          let best: HTMLElement = label as HTMLElement;
          let el: HTMLElement | null = label as HTMLElement;
          while (el && el !== document.body) {
            const style = el.getAttribute("style") || "";
            if (style.includes("cursor") || el.getAttribute("role") === "button") best = el;
            if (el.getBoundingClientRect().width > 300) break;
            el = el.parentElement;
          }
          best.scrollIntoView({ block: "center" });
          const rect = best.getBoundingClientRect();
          return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2, variant: "text", logs };
        }
      }
      logs.push("No Add to Cart button found");
      return { x: 0, y: 0, variant: "none", logs };
    });

    console.log(`[Checkout2/Buying-bot] === Add to Cart (variant: ${btnCoords.variant}) ===`);
    for (const line of btnCoords.logs) console.log(line);
    if (btnCoords.variant === "none") {
      throw new Error("Add to Cart button not found (Buying-bot strategies)");
    }

    await sleep(300);
    try {
      await this.page.mouse.click(btnCoords.x, btnCoords.y);
      console.log(`Mouse clicked Add to Cart at (${btnCoords.x.toFixed(0)}, ${btnCoords.y.toFixed(0)})`);
    } catch (err) {
      console.log(`Mouse click failed: ${(err as Error).message}`);
    }
    await sleep(800);

    await this.waitUntil(async () => {
      return (
        (await this.detectSelectLocationSlider()) ||
        (await this.headerCartCount()) > before ||
        (await this.productLooksAddedToCart()) ||
        (await this.pdpShowsGoToCart())
      );
    }, 4000, 100, "Buying-bot cart add result");

    await this.failIfLocationSlider(pincode);
    let added = (await this.headerCartCount()) > before || (await this.productLooksAddedToCart());
    if (!added) {
      console.log("[Checkout2/Buying-bot] mouse may not have worked — touchscreen.tap");
      try {
        await this.page.touchscreen.tap(btnCoords.x, btnCoords.y);
      } catch (err) {
        console.log(`Tap failed: ${(err as Error).message}`);
      }
      await sleep(800);
      await this.failIfLocationSlider(pincode);
      added = (await this.headerCartCount()) > before || (await this.productLooksAddedToCart());
    }
    if (!added) {
      console.log("[Checkout2/Buying-bot] trying React fiber handler...");
      await this.evaluate(() => {
        const invokeHandler = (startEl: HTMLElement): string | null => {
          let el: HTMLElement | null = startEl;
          while (el && el !== document.body) {
            const fiberKey = Object.keys(el).find(
              (k) => k.startsWith("__reactFiber$") || k.startsWith("__reactInternalInstance$")
            );
            if (fiberKey) {
              let fiber = (el as any)[fiberKey];
              let depth = 0;
              while (fiber && depth < 30) {
                const props = fiber.memoizedProps || fiber.pendingProps;
                if (props) {
                  const h = props.onPress || props.onClick || props.onPressIn;
                  if (typeof h === "function") {
                    try {
                      h({ nativeEvent: {}, preventDefault: () => {}, stopPropagation: () => {} });
                      return "handler invoked";
                    } catch {}
                  }
                }
                fiber = fiber.return;
                depth++;
              }
            }
            el = el.parentElement;
          }
          return null;
        };
        const allPaths = document.querySelectorAll("path");
        for (const p of allPaths) {
          if ((p.getAttribute("d") || "").startsWith("M17 18.375H7.35116")) {
            if (invokeHandler(p as unknown as HTMLElement)) return "svg";
          }
        }
        for (const label of Array.from(document.querySelectorAll("div, span, button"))) {
          const text = label.textContent?.trim().toLowerCase();
          if (text === "add to cart" || text === "add to bag") {
            if (invokeHandler(label as HTMLElement)) return "text";
          }
        }
        return "none";
      });
      await sleep(DELAYS.medium);
      await this.failIfLocationSlider(pincode);
      added = (await this.headerCartCount()) > before || (await this.productLooksAddedToCart());
    }

    const count = await this.headerCartCount();
    console.log(`[Checkout2/Buying-bot] after add, header cart count=${count} added=${added}`);
    if (!added && count < 1) {
      await this.failIfLocationSlider(pincode);
      const why = await this.detectCheckoutBlocker(pincode);
      if (why) throw why;
      console.log("WARNING: Add to cart click did not confirm added state — will still open viewcart");
    }
  }

  async gotoViewCart(): Promise<void> {
    // Buying-bot goToCart URL + TF remembered query params
    if (!/viewcart/i.test(this.page.url() || "")) {
      console.log(`[Checkout2/Buying-bot] Navigating to Flipkart cart → ${VIEWCART_URL}`);
      await navigateWithRetry(this.page, VIEWCART_URL, { timeoutMs: 12000, maxRetries: 2 });
    }
    await sleep(DELAYS.long);
    const ready = await this.waitUntil(async () => {
      return this.evaluate(() => {
        const t = (document.body?.innerText || "").toLowerCase();
        return (
          t.includes("shopping cart") ||
          t.includes("my cart") ||
          t.includes("place order") ||
          t.includes("price details") ||
          t.includes("your cart is empty") ||
          t.includes("missing cart items") ||
          t.includes("save for later")
        );
      });
    }, 15000, 120, "Buying-bot cart page");
    if (!ready) console.log("WARNING: viewcart UI did not look ready within 15s");
    console.log(`On viewcart ${this.page.url()}`);
  }
  

  async clickPlaceOrder(): Promise<void> {
    if (/viewcheckout/i.test(this.page.url() || "")) {
      console.log("Already on viewcheckout — skipping Place Order");
      await this.gotoViewCheckout();
      return;
    }
    if (!/viewcart/i.test(this.page.url() || "")) {
      console.log("Not on viewcart — opening remembered viewcart before Place Order");
      await this.gotoViewCart();
    }
    /**
     * Click the real Place Order control, via a locator.
     *
     * This used to find any element whose text was "place order", walk UP for a
     * styled ancestor, compute a centre point and touchscreen.tap() it. On the
     * m-site cart that picked the wrong one: the page carries the string twice,
     * and the tap landed at CSS (206, 387) — mid-page, in the product card —
     * while the real yellow bar sits at (323, 749) on a 412x839 viewport. The tap
     * hit nothing, nothing navigated, and the old fallback below then jumped to
     * the remembered viewcheckout URL, which is exactly what makes Flipkart
     * answer "Something went wrong! E002". The retry re-tapped the same empty
     * spot, so it could never recover.
     *
     * A locator removes the whole class of problem: Playwright waits for the
     * element to be visible, stable and hit-testable, scrolls it into view and
     * clicks its real centre — and throws if something is covering it instead of
     * silently clicking the overlay.
     */
    console.log("[Checkout2/Buying-bot] Waiting for Place Order button...");
    // Ordered most- to least-specific. getByRole first because the real control is
    // a button; the text forms cover the m-site's div-based buttons.
    const candidates = [
      this.page.getByRole("button", { name: /^\s*place\s*order\s*$/i }),
      this.page.locator('button:has-text("Place Order")'),
      this.page.getByText(/^\s*place\s*order\s*$/i),
    ];

    let placeOrder: Locator | null = null;
    for (let attempt = 0; attempt < 20 && !placeOrder; attempt++) {
      for (const candidate of candidates) {
        // .last() because Flipkart renders the sticky bottom bar after the
        // in-flow copy, and the sticky one is the control a person actually taps.
        const target = candidate.last();
        if ((await target.count().catch(() => 0)) > 0 && (await target.isVisible().catch(() => false))) {
          placeOrder = target;
          break;
        }
      }
      if (placeOrder) break;
      if (attempt % 5 === 4) console.log(`Still looking for Place Order button (attempt ${attempt + 1}/20)...`);
      await sleep(500);
    }

    if (!placeOrder) {
      // Loud, not silent. gotoViewCheckout() here is what produced E002 for every
      // run that got this far: opening viewcheckout without a real Place Order
      // behind it is precisely the state Flipkart rejects.
      throw new CheckoutFailure(
        "UNABLE_TO_PLACE_ORDER",
        "Place Order button not found on the cart after 10s — refusing to open viewcheckout directly, which Flipkart answers with E002"
      );
    }

    const box = await placeOrder.boundingBox().catch(() => null);
    await placeOrder.click({ timeout: 15_000 });
    console.log(
      `Clicked Place Order${box ? ` at (${(box.x + box.width / 2).toFixed(0)}, ${(box.y + box.height / 2).toFixed(0)})` : ""}`
    );

    await sleep(DELAYS.long);
    const moved = await this.waitUntil(
      async () => /viewcheckout|\/payments|checkout|rv\/pay/i.test(this.page.url() || ""),
      15000,
      120,
      "viewcheckout after Place Order"
    );
    if (moved) {
      console.log(`Place Order opened ${this.page.url()}`);
      if (/\/payments/i.test(this.page.url() || "")) return;
      await this.gotoViewCheckout();
      return;
    }
    throw new CheckoutFailure(
      "UNABLE_TO_PLACE_ORDER",
      `Place Order was clicked but the page stayed on ${this.page.url()} — not opening viewcheckout directly, which Flipkart answers with E002`
    );
  }

  private async cartIsEmpty(): Promise<boolean> {
    return this.evaluate(() => {
      const t = (document.body?.innerText || "").replace(/\s+/g, " ");
      return /Your cart is empty!?/i.test(t) || /Missing Cart items/i.test(t);
    });
  }

  /** After login: open viewcart and click Remove until "Your cart is empty!". */
  async emptyCart(): Promise<void> {
    this.suppressLiveBlocker = true;
    try {
      await this.gotoViewCart();
      if (await this.cartIsEmpty()) {
        console.log("[Cart] already empty — Your cart is empty!");
        return;
      }
      const removeBtns = () => this.page.getByText("Remove", { exact: true });
      for (let i = 1; i <= 12; i++) {
        if (await this.cartIsEmpty()) {
          console.log("[Cart] emptied — Your cart is empty!");
          return;
        }
        const count = await removeBtns().count();
        console.log(`[Cart] ${count} Remove control(s) visible before click ${i}`);
        if (!count) {
          console.log("[Cart] no visible Remove — cart still has items, continuing");
          return;
        }
        const btn = removeBtns().first();
        await btn.scrollIntoViewIfNeeded().catch(() => {});
        console.log(`[Cart] clicking Remove (${i})`);
        await btn.click({ timeout: 5000 });
        const emptied = await this.waitUntil(() => this.cartIsEmpty(), 3500, 120, "Your cart is empty after Remove");
        if (emptied) {
          console.log("[Cart] emptied — Your cart is empty!");
          return;
        }
        const after = await removeBtns().count();
        if (after > 0) {
          const confirm = this.page.getByRole("button", { name: /^Remove$/i }).last();
          if (await confirm.isVisible().catch(() => false)) {
            console.log(`[Cart] confirming Remove (${i})`);
            await confirm.click({ timeout: 4000 }).catch(() => {});
            const done = await this.waitUntil(() => this.cartIsEmpty(), 3500, 120, "Your cart is empty after confirm Remove");
            if (done) {
              console.log("[Cart] emptied — Your cart is empty!");
              return;
            }
          }
        }
      }
      if (await this.cartIsEmpty()) {
        console.log("[Cart] emptied — Your cart is empty!");
        return;
      }
      console.log("[Cart] still had items after Remove clicks — continuing");
    } finally {
      this.suppressLiveBlocker = false;
    }
  }

  async detectOutOfStockForPincode(
    pincode: string
  ): Promise<{ matched: boolean; rawMessage?: string }> {
    const pin = String(pincode || "").replace(/\D/g, "").slice(-6);
    if (!pin || pin.length !== 6) return { matched: false };
    try {
      return await this.evaluate((want: string) => {
        const compact = (s: string) => s.replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
        const body = compact(document.body?.innerText || "");
        const lines = (document.body?.innerText || "")
          .replace(/\u00a0/g, " ")
          .split(/\n+/)
          .map(compact)
          .filter(Boolean);
        const locationLine = lines.find((l) =>
          /not deliverable in your (location|area)|currently not deliverable|delivery not available/i.test(l)
        );
        if (locationLine) {
          return { matched: true, rawMessage: `${locationLine} (${want})` };
        }
        const patterns: RegExp[] = [
          /currently out of stock for\s+(\d{6})/i,
          /out of stock for\s+(\d{6})/i,
          /not available for\s+(\d{6})/i,
          /not deliverable to\s+(\d{6})/i,
          /currently unavailable for\s+(\d{6})/i,
        ];
        const hay = `${body} ${lines.join(" ")}`;
        for (const re of patterns) {
          const m = hay.match(re);
          if (m && m[1] === want) {
            const line =
              lines.find((l) => re.test(l) && l.includes(want)) || compact(m[0]);
            return { matched: true, rawMessage: line };
          }
        }
        const sameLine = lines.find((l) => /out of stock|not available|not deliverable/i.test(l) && l.includes(want));
        if (sameLine) {
          return { matched: true, rawMessage: compact(sameLine) };
        }
        return { matched: false };
      }, pin);
    } catch (err) {
      if (!isNavDestroyed(err)) throw err;
      await sleep(400);
      try {
        return await this.evaluate((want: string) => {
          const compact = (s: string) => s.replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
          const body = compact(document.body?.innerText || "");
          const m = body.match(
            /(?:currently\s+)?(?:out of stock|not available|not deliverable|currently unavailable)\s+(?:for|to)\s+(\d{6})/i
          );
          if (m && m[1] === want) return { matched: true, rawMessage: compact(m[0]) };
          if (/out of stock/i.test(body) && body.includes(want)) {
            return { matched: true, rawMessage: `Currently out of stock for ${want}` };
          }
          return { matched: false };
        }, pin);
      } catch {
        return { matched: false };
      }
    }
  }

  async gotoViewCheckout(): Promise<void> {
    if (!/viewcheckout/i.test(this.page.url() || "")) {
      console.log(`Jumping to remembered viewcheckout ${VIEWCHECKOUT_URL}`);
      await navigateWithRetry(this.page, VIEWCHECKOUT_URL, { timeoutMs: 12000, maxRetries: 2 });
    }
    const ready = await this.waitUntil(async () => {
      return this.evaluate(() => {
        const t = document.body?.innerText || "";
        return /Deliver to/i.test(t) || /GST Invoice/i.test(t) || /^Continue$/m.test(t);
      });
    }, 15000, 120, "viewcheckout Continue / Deliver to");
    if (!ready) console.log("WARNING: viewcheckout UI did not look ready — will still look for Continue");
    console.log(`On viewcheckout ${this.page.url()}`);
  }

  async ensureProductInCart(details: { model: string; colour: string }, pincode = ""): Promise<void> {
    if (!/viewcart/i.test(this.page.url() || "")) await this.gotoViewCart();
    let found = await this.cartHasProduct(details);
    if (found) {
      console.log(`[Cart] product found in viewcart: ${found}`);
      return;
    }
    const cartPreview = await this.evaluate(() =>
      (document.body?.innerText || "").replace(/\s+/g, " ").trim().slice(0, 240)
    );
    console.log(`[Cart] product not in viewcart — page text: ${cartPreview || "(empty)"}`);
    if ((await this.headerCartCount()) >= 1) {
      console.log("[Cart] header shows items — waiting for viewcart rows (reload if needed)");
      found = await this.waitUntil(async () => Boolean(await this.cartHasProduct(details)), 6000, 120, "product row on viewcart")
        ? await this.cartHasProduct(details)
        : null;
      if (!found) {
        await navigateWithRetry(this.page, VIEWCART_URL, { timeoutMs: 12000, maxRetries: 2 });
        found = await this.waitUntil(async () => Boolean(await this.cartHasProduct(details)), 8000, 120, "product row after viewcart reload")
          ? await this.cartHasProduct(details)
          : null;
      }
      if (found) {
        console.log(`[Cart] product found in viewcart after wait/reload: ${found}`);
        return;
      }
    }
    console.log("[Cart] product not in viewcart — opening product link and adding again");
    await this.navigateToProduct();
    const beforeRetry = await this.detectCheckoutBlocker(pincode);
    if (beforeRetry) throw beforeRetry;
    await this.clickAddToCart(pincode);
    await this.setDeliveryPincode(pincode);
    await this.failIfLocationSlider(pincode);
    const afterRetry = await this.detectCheckoutBlocker(pincode);
    if (afterRetry) throw afterRetry;
    await this.gotoViewCart();
    found = await this.cartHasProduct(details);
    if (found) {
      console.log(`[Cart] product found in viewcart after re-add: ${found}`);
      return;
    }
    await this.navigateToProduct();
    const why = await this.detectCheckoutBlocker(pincode);
    if (why) throw why;
    const flipkart = await this.evaluate(() =>
      (document.body?.innerText || "").replace(/\s+/g, " ").trim().slice(0, 220)
    );
    throw new CheckoutFailure(
      "ADD_TO_CART_FAILED",
      flipkart
        ? `Product not in viewcart after Add to cart. Flipkart: ${flipkart}`
        : `Product not in viewcart after Add to cart (${details.model || this.productUrl})`
    );
  }

  async setDeliveryPincode(pincode: string): Promise<"serviceable" | "not_serviceable" | "unknown"> {
  const found = await this.evaluate((pin: string) => {
    const inputs = Array.from(document.querySelectorAll("input")) as HTMLInputElement[];
    const pinInput = inputs.find((i) =>
      /pincode|delivery pincode/i.test(
        i.placeholder || i.getAttribute("aria-label") || i.name || ""
      )
    );
    if (!pinInput) return false;
    pinInput.scrollIntoView({ block: "center" });
    pinInput.focus();
    pinInput.value = pin;
    pinInput.dispatchEvent(new Event("input",  { bubbles: true }));
    pinInput.dispatchEvent(new Event("change", { bubbles: true }));
    const btn = Array.from(document.querySelectorAll("button, div[role=button]"))
      .find((b) => /^\s*(check|submit)\s*$/i.test((b.textContent || "").trim()));
    if (btn) (btn as HTMLElement).click();
    return true;
  }, pincode);

  if (!found) return "unknown"; // no widget → treat as already-serviceable page

  await this.waitUntil(async () => this.evaluate(() => {
    const t = document.body.innerText.toLowerCase();
    return t.includes("delivery by") || t.includes("arriving by") ||
           t.includes("not deliverable") || t.includes("coming soon") ||
           t.includes("currently unavailable");
  }), 8000, 200, "pincode serviceability result");

  return await this.evaluate(() => {
    const t = document.body.innerText.toLowerCase();
    if (t.includes("delivery by") || t.includes("arriving by")) return "serviceable";
    if (t.includes("not deliverable") || t.includes("coming soon") ||
        t.includes("currently unavailable"))                    return "not_serviceable";
    return "unknown";
  });
}

  private async cartHasProduct(details: { model: string; colour: string }): Promise<string | null> {
    return this.evaluate(
      (info: { url: string; model: string; colour: string }) => {
        const body = (document.body?.innerText || "").replace(/\s+/g, " ");
        const html = document.body?.innerHTML || "";
        let pid = "";
        let itm = "";
        try {
          const u = new URL(info.url);
          pid = (u.searchParams.get("pid") || "").toLowerCase();
          const m = u.pathname.match(/\/p\/(itm[a-z0-9]+)/i);
          if (m) itm = m[1].toLowerCase();
        } catch {
          /* ignore */
        }
        if (pid && html.toLowerCase().includes(pid)) return `pid:${pid}`;
        if (itm && html.toLowerCase().includes(itm)) return `itm:${itm}`;
        const title = `${info.model} ${info.colour}`.replace(/\s+/g, " ").trim().toLowerCase();
        const words = title
          .split(/[^a-z0-9]+/i)
          .map((w) => w.toLowerCase())
          .filter((w) => w.length >= 3 && !["the", "and", "for", "with", "ram", "rom"].includes(w));
        const needle = words.slice(0, 4).join(" ");
        if (needle.length >= 6 && body.toLowerCase().includes(needle)) return `title:${needle}`;
        if (info.model && body.toLowerCase().includes(info.model.trim().toLowerCase().slice(0, 28))) {
          return `model:${info.model.slice(0, 28)}`;
        }
        return null;
      },
      { url: this.productUrl, model: details.model, colour: details.colour }
    );
  }

  async assertDeliverySla(
    maxDays: number,
    details: { model: string; colour: string },
    pincode = ""
  ): Promise<{ text: string; days: number }> {
    const today = this.todayIst();
    const latest = this.addDays(today, maxDays);
    console.log(
      `[SLA] today IST ${this.formatIstDate(today)}; delivery_sla_days=${maxDays} → latest allowed ${this.formatIstDate(latest)}`
    );

    const pinPrompt = async () =>
      this.evaluate(() =>
        /enter pincode to see if the product is in stock|enter delivery pincode/i.test(
          document.body?.innerText || ""
        )
      );
    if (await pinPrompt()) {
      throw new CheckoutFailure(
        "ITEM_NOT_DELIVERABLE",
        pincode
          ? `Enter pincode to see if the product is in stock (${pincode})`
          : "Enter pincode to see if the product is in stock"
      );
    }

    const appeared = await this.waitUntil(async () => {
      if (await pinPrompt()) {
        throw new CheckoutFailure(
          "ITEM_NOT_DELIVERABLE",
          pincode
            ? `Enter pincode to see if the product is in stock (${pincode})`
            : "Enter pincode to see if the product is in stock"
        );
      }
      try {
        return this.evaluate(() =>
          Array.from(document.querySelectorAll("div,span")).some((el) => {
            const t = (el.textContent || "").replace(/\s+/g, " ").trim();
            return /^Delivery by\b/i.test(t) || /^Delivery in\b/i.test(t) || /^EXPRESS\s+Delivery\b/i.test(t);
          })
        );
      } catch (err) {
        if (!isNavDestroyed(err)) throw err;
        if (pincode) {
          const oos = await this.detectOutOfStockForPincode(pincode);
          if (oos.matched) throw new OutOfStockPincodeError(oos.rawMessage || `Currently out of stock for ${pincode}`, pincode);
        }
        return this.evaluate(() =>
          Array.from(document.querySelectorAll("div,span")).some((el) => {
            const t = (el.textContent || "").replace(/\s+/g, " ").trim();
            return /^Delivery by\b/i.test(t) || /^Delivery in\b/i.test(t) || /^EXPRESS\s+Delivery\b/i.test(t);
          })
        );
      }
    }, 20000);
    if (!appeared) {
      if (pincode) {
        const oos = await this.detectOutOfStockForPincode(pincode);
        if (oos.matched) throw new OutOfStockPincodeError(oos.rawMessage || `Currently out of stock for ${pincode}`, pincode);
      }
      const hit = await this.readDeliveryPromise(details);
      throw new Error(
        `viewcart never showed "Delivery by …" / "Delivery in …". Nearby: ${hit.seen.slice(0, 8).join(" | ") || "none"}`
      );
    }

    let hit: { text: string; seen: string[] };
    try {
      hit = await this.readDeliveryPromise(details);
    } catch (err) {
      if (!isNavDestroyed(err)) throw err;
      if (pincode) {
        const oos = await this.detectOutOfStockForPincode(pincode);
        if (oos.matched) throw new OutOfStockPincodeError(oos.rawMessage || `Currently out of stock for ${pincode}`, pincode);
      }
      hit = await this.readDeliveryPromise(details);
    }
    console.log(`[SLA] Flipkart delivery text: "${hit.text || "(none)"}"`);
    if (hit.seen.length) console.log(`[SLA] nearby lines: ${hit.seen.slice(0, 8).join(" | ")}`);
    const parsed = hit.text ? this.parseDeliverySla(hit.text) : null;
    if (!parsed) {
      throw new Error(
        `Could not convert delivery date from "${hit.text || "none"}". Expected "Delivery by Sep 23, Wed" (delivery_sla_days=${maxDays})`
      );
    }
    const flipkartDate = this.addDays(today, parsed.days);
    console.log(
      `[SLA] "${hit.text}" → ${parsed.days} day(s) via ${parsed.via} = ${this.formatIstDate(flipkartDate)}; latest allowed ${this.formatIstDate(latest)}`
    );
    if (parsed.days > maxDays) {
      throw new Error(
        `Delivery days do not match: Flipkart ${this.formatIstDate(flipkartDate)} (${parsed.days} day(s), "${hit.text}") is after latest allowed ${this.formatIstDate(latest)} (today ${this.formatIstDate(today)} + delivery_sla_days ${maxDays})`
      );
    }
    return { text: hit.text, days: parsed.days };
  }

  private async readDeliveryPromise(
    _details: { model: string; colour: string }
  ): Promise<{ text: string; seen: string[] }> {
    return this.evaluate(() => {
      const compact = (s: string) => s.replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
      const seen: string[] = [];
      const hits: string[] = [];

      for (const el of Array.from(document.querySelectorAll("div[dir='auto'], span[dir='auto'], div, span")) as HTMLElement[]) {
        const t = compact(el.textContent || "");
        if (!t || t.length > 80) continue;
        if (
          /^Delivery by\b/i.test(t) ||
          /^Delivery in\b/i.test(t) ||
          /^EXPRESS\s+Delivery\b/i.test(t) ||
          /^Get it by\b/i.test(t)
        ) {
          hits.push(t);
        }
        if (/deliver|express|pincode/i.test(t) && t.length <= 60) seen.push(t);
      }

      const bodyLines = (document.body?.innerText || "")
        .replace(/\u00a0/g, " ")
        .split(/\n+/)
        .map(compact)
        .filter(Boolean);
      for (const line of bodyLines) {
        if (/deliver|express|pincode/i.test(line) && line.length <= 80) seen.push(line);
        if (
          /^Delivery by\b/i.test(line) ||
          /^Delivery in\b/i.test(line) ||
          /Delivery by\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/i.test(line) ||
          /Delivery in\s+\d+\s+days?/i.test(line)
        ) {
          hits.push(line);
        }
      }

      const uniqHits = [...new Set(hits)];
      const uniqSeen = [...new Set(seen)].slice(0, 12);
      uniqHits.sort(
        (a, b) =>
          (/^Delivery by\b|^Delivery in\b/i.test(a) ? 0 : 1) -
            (/^Delivery by\b|^Delivery in\b/i.test(b) ? 0 : 1) || a.length - b.length
      );
      return { text: uniqHits[0] || "", seen: uniqSeen };
    });
  }

  private todayIst(): Date {
    const parts = new Intl.DateTimeFormat("en-GB", {
      timeZone: "Asia/Kolkata",
      year: "numeric",
      month: "numeric",
      day: "numeric",
    }).formatToParts(new Date());
    const num = (type: string) => Number(parts.find((p) => p.type === type)?.value);
    return new Date(num("year"), num("month") - 1, num("day"));
  }

  private addDays(d: Date, days: number): Date {
    return new Date(d.getFullYear(), d.getMonth(), d.getDate() + days);
  }

  private formatIstDate(d: Date): string {
    return d.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" });
  }

  private monthIndex(name: string): number | null {
    const key = name.toLowerCase().replace(/\./g, "");
    const map: Record<string, number> = {
      jan: 0, january: 0,
      feb: 1, february: 1,
      mar: 2, march: 2,
      apr: 3, april: 3,
      may: 4,
      jun: 5, june: 5,
      jul: 6, july: 6,
      aug: 7, august: 7,
      sep: 8, sept: 8, september: 8,
      oct: 9, october: 9,
      nov: 10, november: 10,
      dec: 11, december: 11,
    };
    if (key in map) return map[key];
    const short = key.slice(0, 3);
    return short in map ? map[short] : key === "sep" || key === "sept" ? 8 : null;
  }

  private daysUntilCalendar(day: number, monthIdx: number, year?: number): number | null {
    const today = this.todayIst();
    let y = year ?? today.getFullYear();
    let target = new Date(y, monthIdx, day);
    if (Number.isNaN(target.getTime()) || target.getDate() !== day) return null;
    const msDay = 24 * 60 * 60 * 1000;
    let delta = Math.round((target.getTime() - today.getTime()) / msDay);
    if (delta < 0 && year == null) {
      target = new Date(y + 1, monthIdx, day);
      if (target.getDate() !== day) return null;
      delta = Math.round((target.getTime() - today.getTime()) / msDay);
    }
    if (delta < 0) return null;
    return delta;
  }

  /** Convert Flipkart text like "in 4 days" or "23 Sept" into days from today (IST). */
  private parseDeliverySla(raw: string): { days: number; via: string } | null {
    if (/pincode/i.test(raw)) return null;
    const t = raw.toLowerCase().replace(/[,.]/g, " ").replace(/\s+/g, " ").trim();

    const dmy = t.match(
      /\b(\d{1,2})(?:st|nd|rd|th)?\s+(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(?:\s+(\d{4}))?\b/
    );
    if (dmy) {
      const month = this.monthIndex(dmy[2]);
      if (month == null) return null;
      const days = this.daysUntilCalendar(Number(dmy[1]), month, dmy[3] ? Number(dmy[3]) : undefined);
      if (days == null) return null;
      return { days, via: `date ${dmy[1]} ${dmy[2]}${dmy[3] ? " " + dmy[3] : ""}` };
    }

    const mdy = t.match(
      /\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(\d{1,2})(?:st|nd|rd|th)?(?:\s+(\d{4}))?\b/
    );
    if (mdy) {
      const month = this.monthIndex(mdy[1]);
      if (month == null) return null;
      const days = this.daysUntilCalendar(Number(mdy[2]), month, mdy[3] ? Number(mdy[3]) : undefined);
      if (days == null) return null;
      return { days, via: `date ${mdy[1]} ${mdy[2]}${mdy[3] ? " " + mdy[3] : ""}` };
    }

    if (/\btoday\b|\bsame day\b|\btonight\b/.test(t)) return { days: 0, via: "today" };
    if (/\btomorrow\b/.test(t)) return { days: 1, via: "tomorrow" };

    const inDays = t.match(/\bin\s+(\d+)\s+days?\b/);
    if (inDays) return { days: Number(inDays[1]), via: `in ${inDays[1]} days` };
    const nDays = t.match(/\b(\d+)\s+days?\b/);
    if (nDays) return { days: Number(nDays[1]), via: `${nDays[1]} days` };

    return null;
  }

  async clickBuyNow(): Promise<void> {
    console.log("Waiting for Buy Now button ...");
    await waitWithRetry(
      this.page,
      async () => {
        await this.waitForFunction(
          () => {
            // Check for "Buy Now" text OR yellow gradient button
            const allEls = document.querySelectorAll("div, span, button");
            for (const el of allEls) {
              const text = el.textContent?.trim().toLowerCase();
              if (text === "buy now" || text === "buy now!") return true;
            }
            // Also check for yellow gradient (Buy Now button background)
            const gradients = document.querySelectorAll('div[style*="linear-gradient"]');
            for (const g of gradients) {
              const style = g.getAttribute("style") || "";
              if (style.includes("#ffe51f") || style.includes("#ffcd03") || style.includes("rgb(255, 229, 31)")) {
                return true;
              }
            }
            return false;
          },
          { timeout: 10000 }
        );
      },
      { label: "Buy Now button", timeoutMs: 10000, maxRetries: 5 }
    );

    // Find the Buy Now button using multiple strategies
    const buyBox = await this.evaluate(() => {
      // polyfill esbuild's __name helper which doesn't exist in browser context
      if (typeof (globalThis as any).__name === "undefined") (globalThis as any).__name = (fn: any) => fn;

      const logs: string[] = [];

      // --- Strategy 1 (HIGHEST PRIORITY): Find "Buy Now" text and walk up ---
      // This avoids clicking "Buy Combo" or other yellow-gradient buttons
      const allEls = document.querySelectorAll("div, span, button");
      for (const label of allEls) {
        const text = label.textContent?.trim().toLowerCase();
        if (text !== "buy now" && text !== "buy now!") continue;
        if (label.children.length > 3) continue;

        // REJECT if parent text contains "combo" — this is Buy Combo, not Buy Now
        const parentText = (label.parentElement?.textContent || "").toLowerCase();
        if (parentText.includes("combo")) {
          logs.push(`Skipped: "Buy Now" inside a combo context: "${parentText.slice(0, 60)}"`);
          continue;
        }

        logs.push(`Found "Buy Now" text in <${label.tagName.toLowerCase()}> class="${(label as HTMLElement).className}"`);

        let best: HTMLElement = label as HTMLElement;
        let el: HTMLElement | null = label as HTMLElement;
        while (el && el !== document.body) {
          const s = el.getAttribute("style") || "";
          if (el.getAttribute("role") === "button" || s.includes("cursor: pointer") || s.includes("cursor:pointer") || el.tagName === "BUTTON") {
            best = el;
          }
          if (el.getBoundingClientRect().width > 400) break;
          el = el.parentElement;
        }

        const rect = best.getBoundingClientRect();
        logs.push(`Text-based pressable at (${rect.x.toFixed(0)}, ${rect.y.toFixed(0)}) ${rect.width.toFixed(0)}x${rect.height.toFixed(0)}`);
        // Last run clicked a 67x20 label and never left /p/ — skip crumbs.
        if (rect.width < 100 || rect.height < 32) {
          logs.push("Skipped tiny Buy Now hit");
          continue;
        }
        best.scrollIntoView({ block: "center" });
        const r2 = best.getBoundingClientRect();
        return { x: r2.x + r2.width / 2, y: r2.y + r2.height / 2, variant: "text", logs };
      }

      // --- Strategy 2: Find yellow gradient that is specifically Buy Now (not Buy Combo) ---
      // The Buy Now gradient has: linear-gradient(90deg, rgb(255, 229, 31), rgb(255, 205, 3))
      const gradients = document.querySelectorAll('div[style*="linear-gradient"]');
      for (const g of gradients) {
        const style = g.getAttribute("style") || "";
        if (!(style.includes("#ffe51f") || style.includes("#ffcd03") || style.includes("rgb(255, 229, 31)"))) continue;

        logs.push(`Found yellow gradient: style="${style.slice(0, 80)}..."`);

        // Go up: gradient div → position:absolute wrapper → pressable parent
        let target: HTMLElement = g as HTMLElement;
        if (g.parentElement) {
          const ps = g.parentElement.getAttribute("style") || "";
          if (ps.includes("position") && ps.includes("absolute")) {
            target = g.parentElement.parentElement || g.parentElement;
          } else {
            target = g.parentElement;
          }
        }

        // Walk up to find the best pressable container
        let best: HTMLElement = target;
        let el: HTMLElement | null = target;
        while (el && el !== document.body) {
          const s = el.getAttribute("style") || "";
          if (el.getAttribute("role") === "button" || s.includes("cursor: pointer") || s.includes("cursor:pointer") || el.tagName === "BUTTON") {
            best = el;
          }
          if (el.getBoundingClientRect().width > 400) break;
          el = el.parentElement;
        }

        // REJECT if the pressable contains "combo" text
        const containerText = best.textContent?.trim().toLowerCase() || "";
        if (containerText.includes("combo")) {
          logs.push(`Skipped yellow gradient: contains 'combo' text`);
          continue;
        }

        best.scrollIntoView({ block: "center" });
        const rect = best.getBoundingClientRect();
        logs.push(`Yellow gradient pressable at (${rect.x.toFixed(0)}, ${rect.y.toFixed(0)}) ${rect.width.toFixed(0)}x${rect.height.toFixed(0)}`);

        // Confirm it's the Buy Now button
        if (rect.width < 100 || rect.height < 32) {
          logs.push("Skipped tiny yellow gradient");
          continue;
        }
        if (containerText.includes("buy now")) {
          logs.push("Confirmed: contains 'buy now' text");
          return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2, variant: "yellow-gradient", logs };
        }
        if (rect.width < 300 && rect.height > 30 && rect.height < 70) {
          logs.push("Likely Buy Now based on size (no combo text)");
          return { x: rect.x + rect.width / 2, y: rect.y + rect.height / 2, variant: "yellow-gradient-size", logs };
        }
      }

      logs.push("No Buy Now button found");
      return null;
    });

    if (buyBox) {
      for (const line of buyBox.logs) console.log(line);
    }
    if (!buyBox) throw new Error("Buy Now button not found");
    console.log(`=== Buy Now (variant: ${buyBox.variant}) ===`);

    await sleep(300);

    const onCartOrCheckout = () =>
      /viewcart|checkout|payment|order-summary|rv\/pay/i.test(this.page.url() || "");

    const waitUntilCart = async (ms = 12000) => {
      const start = Date.now();
      while (Date.now() - start < ms) {
        await waitForNav(this.page, 1500).catch(() => {});
        if (onCartOrCheckout()) return true;
        await sleep(400);
      }
      return onCartOrCheckout();
    };

    try {
      await this.page.touchscreen.tap(buyBox.x, buyBox.y);
      console.log(`Tapped Buy Now at (${buyBox.x.toFixed(0)}, ${buyBox.y.toFixed(0)})`);
      if (await waitUntilCart()) {
        console.log(`Buy Now navigated to: ${this.page.url()}`);
        return;
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.log(`Buy Now tap error: ${msg}`);
      if (await waitUntilCart(5000)) {
        console.log(`After Buy Now, URL: ${this.page.url()}`);
        return;
      }
    }

    // Method 2: mouse click fallback
    console.log("Tap may not have worked, trying mouse click...");
    try {
      await this.page.mouse.click(buyBox.x, buyBox.y);
      if (await waitUntilCart()) {
        console.log(`Mouse click navigated to: ${this.page.url()}`);
        return;
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.log(`Mouse click error: ${msg}`);
      if (await waitUntilCart(5000)) {
        console.log(`Mouse click navigated to: ${this.page.url()}`);
        return;
      }
    }

    // Method 3: React fiber handler
    console.log("Mouse click also didn't navigate, trying React fiber handler...");
    try {
      const fiberResult = await this.evaluate(() => {
        if (typeof (globalThis as any).__name === "undefined") (globalThis as any).__name = (fn: any) => fn;

        // Try yellow gradient element first
        const gradients = document.querySelectorAll('div[style*="linear-gradient"]');
        for (const g of gradients) {
          const style = g.getAttribute("style") || "";
          if (style.includes("#ffe51f") || style.includes("#ffcd03") || style.includes("rgb(255, 229, 31)")) {
            let el: HTMLElement | null = g as HTMLElement;
            while (el && el !== document.body) {
              const fiberKey = Object.keys(el).find(k => k.startsWith("__reactFiber$") || k.startsWith("__reactInternalInstance$"));
              if (fiberKey) {
                let fiber = (el as any)[fiberKey];
                let depth = 0;
                while (fiber && depth < 30) {
                  const props = fiber.memoizedProps || fiber.pendingProps;
                  if (props) {
                    const h = props.onPress || props.onClick || props.onPressIn;
                    if (typeof h === "function") {
                      try { h({ nativeEvent: {}, preventDefault: () => {}, stopPropagation: () => {} }); return "gradient handler invoked"; } catch {}
                    }
                  }
                  fiber = fiber.return;
                  depth++;
                }
              }
              el = el.parentElement;
            }
          }
        }

        // Try "Buy Now" text elements
        const allEls = document.querySelectorAll("div, span, button");
        for (const label of allEls) {
          const text = label.textContent?.trim().toLowerCase();
          if (text !== "buy now" && text !== "buy now!") continue;
          if (label.children.length > 3) continue;
          let el: HTMLElement | null = label as HTMLElement;
          while (el && el !== document.body) {
            const fiberKey = Object.keys(el).find(k => k.startsWith("__reactFiber$") || k.startsWith("__reactInternalInstance$"));
            if (fiberKey) {
              let fiber = (el as any)[fiberKey];
              let depth = 0;
              while (fiber && depth < 30) {
                const props = fiber.memoizedProps || fiber.pendingProps;
                if (props) {
                  const h = props.onPress || props.onClick || props.onPressIn;
                  if (typeof h === "function") {
                    try { h({ nativeEvent: {}, preventDefault: () => {}, stopPropagation: () => {} }); return "text handler invoked"; } catch {}
                  }
                }
                fiber = fiber.return;
                depth++;
              }
            }
            el = el.parentElement;
          }
        }
        return "no handler found";
      });
      console.log(`React fiber fallback: ${fiberResult}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes("context") || msg.includes("destroyed") || msg.includes("detached")) {
        console.log("Fiber handler triggered navigation");
        await waitForNav(this.page, 10000).catch(() => {});
      }
    }

    console.log(`After Buy Now, URL: ${this.page.url()}`);
    if (!onCartOrCheckout()) {
      throw new Error(
        `Buy Now did not open cart/checkout (still on ${this.page.url()}). Will not run order-summary checks on the product page.`
      );
    }
  }

  async fetchAccountMobile(): Promise<string | null> {
    this.log("info", "[preflight] navigating to /account");
    try {
      await navigateWithRetry(this.page, "https://www.flipkart.com/account", {
        timeoutMs: 15000,
        maxRetries: 2,
      });
      await sleep(DELAYS.medium);

      // Hard-fail if Flipkart redirected us back to login. This is a much
      // clearer signal than reading an empty mobileNumber and confusedly
      // continuing.
      const url = (this.page.url() || "").toLowerCase();
      if (url.includes("/account/login")) {
        this.log("error", `[preflight] /account redirected to ${this.page.url()} — session is not actually logged in`);
        throw new Error(
          "Pre-flight: /account redirected to login page. Flipkart did not honour the OTP — " +
          "the account may need manual verification (Profiles → Setup Login)."
        );
      }
      this.log("info", `[preflight] /account landed on ${url}`);

      const value = await this.evaluate(() => {
        const el = document.querySelector(
          'input[name="mobileNumber"]'
        ) as HTMLInputElement | null;
        return el?.value || null;
      });

      if (!value) {
        console.log("[Mobile] mobileNumber input not found on /account");
        return null;
      }
      const trimmed = value.trim();
      console.log(`[Mobile] Account mobile read: ${trimmed.slice(0, 4)}***${trimmed.slice(-3)}`);
      return trimmed;
    } catch (err) {
      console.log(`[Mobile] Failed to fetch account mobile: ${(err as Error).message}`);
      // Re-throw redirect-to-login so the iteration's catch handles it; swallow
      // benign read failures (e.g. transient DOM state) and return null so the
      // pre-flight gracefully degrades.
      if ((err as Error).message?.includes("Pre-flight:")) throw err;
      return null;
    }
  }

  /** Strip non-digits and return the last 10 digits (Indian mobile). */
  private normalizeMobile(input: string): string {
    return input.replace(/\D/g, "").slice(-10);
  }

  /**
   * Keep exactly one Flipkart address on /account/addresses:
   * - none present → ADD ADDRESSES / ADD A NEW ADDRESS
   * - one or more present → Edit the first card (never add a second)
   * - extras → Delete until one remains
   */
  async ensureAddressForAccount(
    address: AddressDetails,
    accountMobile: string
  ): Promise<void> {
    const mobile10 = this.normalizeMobile(accountMobile);
    const tail4 = mobile10.slice(-4);
    console.log(
      `[AddressPreflight] one-address policy (${address.companyName || address.name}, ` +
      `${address.city}, ${address.pincode}) mobile ending ${tail4}`
    );

    await navigateWithRetry(this.page, "https://www.flipkart.com/account/addresses", {
      timeoutMs: 15000,
      maxRetries: 2,
    });
    {
      const url = (this.page.url() || "").toLowerCase();
      if (url.includes("/account/login")) {
        this.log("error", `[preflight] /account/addresses redirected to ${this.page.url()}`);
        throw new Error(
          "Pre-flight: /account/addresses redirected to login page — Flipkart did not honour the OTP."
        );
      }
      this.log("info", `[preflight] /account/addresses landed on ${url}`);
    }
    await this.waitUntilAddressesPageReady();

    let cards = await this.listAddressCards();
    console.log(`[AddressPreflight] found ${cards.length} saved address card(s)`);

    if (cards.length > 1) {
      await this.deleteExtraAddressCards();
      cards = await this.listAddressCards();
    }

    if (!cards.length) {
      console.log("[AddressPreflight] no saved address — adding a new one");
      await this.addNewAddressWithMobile(address, mobile10);
      return;
    }

    const expected = {
      name: (address.companyName || address.name || "").trim(),
      city: address.city,
      pincode: address.pincode,
      mobile: mobile10,
    };
    const alreadyOk = this.cardMatchesExpected(cards[0].text, expected);
    if (alreadyOk && cards.length === 1) {
      console.log("[AddressPreflight] single saved address already matches — leaving it");
      return;
    }

    console.log("[AddressPreflight] editing the existing saved address (will not add another)");
    await this.clickAddressCardAction(0, "Edit");
    try {
      await this.page.waitForSelector('input[name="name"]', { state: "visible", timeout: 15000 });
    } catch {
      throw new Error('Edit-address form did not appear (input[name="name"] not visible)');
    }
    await this.fillAddressForm(this.page, { ...address, mobile: mobile10 });
    await this.saveAddressForm("edited");
  }

  /** Slow connections: wait until Manage Addresses has a card or the empty-state CTA. */
  private async waitUntilAddressesPageReady(): Promise<void> {
    console.log("[AddressPreflight] waiting until Manage Addresses finishes loading...");
    const started = Date.now();
    const deadline = started + 40000;
    while (Date.now() < deadline) {
      const state = await this.evaluate(() => {
        const t = document.body?.innerText || "";
        return {
          heading: /Manage Addresses/i.test(t),
          hasHomeWork: /\b(HOME|WORK)\b/.test(t),
          addNew: /ADD A NEW ADDRESS/i.test(t),
          addEmpty: /ADD ADDRESSES/i.test(t),
          len: t.replace(/\s+/g, " ").trim().length,
        };
      });
      if (!state.heading || state.len < 40) {
        await sleep(500);
        continue;
      }
      const cards = await this.listAddressCards();
      if (cards.length > 0) {
        console.log(`[AddressPreflight] addresses page loaded (${cards.length} card(s), ${Date.now() - started}ms)`);
        await sleep(400);
        return;
      }
      // Empty account: ADD ADDRESSES, no HOME/WORK card yet.
      if (state.addEmpty && !state.hasHomeWork) {
        console.log(`[AddressPreflight] empty addresses page loaded (${Date.now() - started}ms)`);
        await sleep(400);
        return;
      }
      // ADD A NEW ADDRESS with no card yet = still hydrating — keep waiting.
      await sleep(500);
    }
    console.log("[AddressPreflight] addresses page still slow after 40s — reading whatever is present");
  }

  /**
   * Click "ADD ADDRESSES" on /account/addresses and fill the form using
   * the supplied mobile. Same selectors used by addressRunner.ts so the
   * behaviour stays consistent across entry points.
   */
  private async addNewAddressWithMobile(
    address: AddressDetails,
    mobile10: string
  ): Promise<void> {
    // Empty-state CTA is a real <button class="dSM5Ub …">ADD ADDRESSES</button>
    // (see Flipkart empty-address card). Accounts that already have addresses
    // still use the older `div.cv8zZS` "ADD A NEW ADDRESS" control. Do not
    // click a wrapper div — its innerText includes the heading and the click
    // is a no-op.
    await waitWithRetry(
      this.page,
      async () => {
        await this.waitForFunction(
          () => {
            const labelOk = (raw: string) => {
              const t = (raw || "").replace(/\s+/g, " ").trim();
              return /^(ADD ADDRESSES|ADD A NEW ADDRESS|ADD ADDRESS|ADD NEW ADDRESS)$/i.test(t);
            };
            const buttons = Array.from(document.querySelectorAll("button"));
            if (buttons.some((b) => labelOk(b.textContent || ""))) return true;
            const cv8 = Array.from(document.querySelectorAll<HTMLElement>("div.cv8zZS, .cv8zZS"));
            if (cv8.some((el) => labelOk(el.innerText || ""))) return true;
            return false;
          },
          { timeout: 15000 }
        );
      },
      { label: "ADD ADDRESSES button", timeoutMs: 15000, maxRetries: 3 }
    );

    const clicked = await this.evaluate(() => {
      const labelOk = (raw: string) => {
        const t = (raw || "").replace(/\s+/g, " ").trim();
        return /^(ADD ADDRESSES|ADD A NEW ADDRESS|ADD ADDRESS|ADD NEW ADDRESS)$/i.test(t);
      };
      const clickEl = (el: HTMLElement, via: string) => {
        el.scrollIntoView({ block: "center" });
        el.click();
        return via;
      };

      const buttons = Array.from(document.querySelectorAll("button")) as HTMLButtonElement[];
      for (const b of buttons) {
        if (labelOk(b.textContent || "")) return clickEl(b, "button-text");
      }

      const hashed = Array.from(document.querySelectorAll<HTMLElement>("button.dSM5Ub"));
      for (const el of hashed) {
        if (labelOk(el.textContent || "")) return clickEl(el, "button.dSM5Ub");
      }

      const cv8 = Array.from(document.querySelectorAll<HTMLElement>("div.cv8zZS, .cv8zZS"));
      for (const el of cv8) {
        if (labelOk(el.innerText || "") || /ADD ADDRESSES|ADD A NEW ADDRESS/i.test(el.innerText || "")) {
          return clickEl(el, "cv8zZS");
        }
      }

      return null;
    });
    if (!clicked) {
      throw new Error("Could not find ADD ADDRESSES button on /account/addresses");
    }
    console.log(`[AddressPreflight] clicked ADD ADDRESSES via ${clicked} strategy`);
    await sleep(800);

    try {
      await this.page.waitForSelector('input[name="name"]', { state: "visible", timeout: 15000 });
    } catch {
      throw new Error('Add-address form did not appear (input[name="name"] not visible) — selector may have changed');
    }

    await this.fillAddressForm(this.page, {
      ...address,
      mobile: mobile10,
    });
    await this.saveAddressForm("new");
  }

  private cardMatchesExpected(
    text: string,
    expected: { name: string; city: string; pincode: string; mobile: string }
  ): boolean {
    const lower = (text || "").replace(/\s+/g, " ").toLowerCase();
    const name = expected.name.trim().toLowerCase();
    const city = expected.city.trim().toLowerCase();
    const pin = expected.pincode.trim();
    const mobile = expected.mobile.trim();
    const mobileTail = mobile.slice(-4);
    return (
      (!name || lower.includes(name)) &&
      (!city || lower.includes(city)) &&
      (!pin || text.includes(pin)) &&
      (!mobile || text.includes(mobile) || text.includes(mobileTail))
    );
  }

  private async listAddressCards(): Promise<
    Array<{ index: number; x: number; y: number; w: number; h: number; rightX: number; text: string }>
  > {
    return this.evaluate(() => {
      const visible = (el: HTMLElement) => {
        const r = el.getBoundingClientRect();
        const cs = window.getComputedStyle(el);
        return r.width > 0 && r.height > 0 && cs.display !== "none" && cs.visibility !== "hidden";
      };

      let els = Array.from(document.querySelectorAll(".CHc0Fj")) as HTMLElement[];
      els = els.filter(visible);

      if (!els.length) {
        const found: HTMLElement[] = [];
        for (const badge of Array.from(document.querySelectorAll("div,span")) as HTMLElement[]) {
          if (!/^(HOME|WORK)$/i.test((badge.textContent || "").trim())) continue;
          let el: HTMLElement | null = badge.parentElement;
          while (el && el !== document.body) {
            const r = el.getBoundingClientRect();
            const t = (el.innerText || "").replace(/\s+/g, " ");
            if (
              r.width >= 280 &&
              r.height >= 48 &&
              r.height <= 280 &&
              /\b\d{6}\b/.test(t) &&
              !/ADD A NEW ADDRESS|ADD ADDRESSES/i.test(t)
            ) {
              if (!found.includes(el)) found.push(el);
              break;
            }
            el = el.parentElement;
          }
        }
        els = found;
      }

      return els.map((el, index) => {
        const r = el.getBoundingClientRect();
        return {
          index,
          x: r.x + r.width / 2,
          y: r.y + r.height / 2,
          w: r.width,
          h: r.height,
          rightX: r.x + r.width - 18,
          text: (el.innerText || "").replace(/\s+/g, " ").trim().slice(0, 220),
        };
      });
    });
  }

  private async findExactSmallLabel(
    exact: RegExp
  ): Promise<{ x: number; y: number; w: number; h: number; text: string } | null> {
    return this.evaluate((src: string) => {
      const re = new RegExp(src, "i");
      const hits: Array<{ x: number; y: number; w: number; h: number; text: string; bottom: number }> = [];
      for (const n of Array.from(document.querySelectorAll("div,span,button,a,li,p")) as HTMLElement[]) {
        const t = (n.textContent || "").replace(/\s+/g, " ").trim();
        if (!re.test(t) || t.length > 18) continue;
        const r = n.getBoundingClientRect();
        if (r.width < 18 || r.height < 12 || r.width > 220 || r.height > 56) continue;
        const cs = window.getComputedStyle(n);
        if (cs.display === "none" || cs.visibility === "hidden" || Number(cs.opacity) === 0) continue;
        hits.push({
          x: r.x + r.width / 2,
          y: r.y + r.height / 2,
          w: r.width,
          h: r.height,
          text: t,
          bottom: r.bottom,
        });
      }
      if (!hits.length) return null;
      hits.sort((a, b) => a.w * a.h - b.w * b.h);
      const pick = hits[0];
      return { x: pick.x, y: pick.y, w: pick.w, h: pick.h, text: pick.text };
    }, exact.source);
  }

  private async clickAddressCardAction(index: number, action: "Edit" | "Delete"): Promise<void> {
    const cards = await this.listAddressCards();
    const card = cards[index];
    if (!card) throw new Error(`Address card ${index} not found for ${action}`);

    await this.page.mouse.move(card.x, card.y);
    await sleep(500);

    const actionRe = new RegExp(`^${action}$`, "i");
    let target = await this.findExactSmallLabel(actionRe);
    if (!target) {
      await this.page.mouse.click(card.rightX, card.y);
      await sleep(450);
      target = await this.findExactSmallLabel(actionRe);
    }
    if (!target) {
      const clicked = await this.evaluate(
        (idx: number, act: string) => {
          const hashed = Array.from(document.querySelectorAll(".CHc0Fj")) as HTMLElement[];
          const el = hashed[idx];
          if (!el) return false;
          const nodes = Array.from(el.querySelectorAll("div,span,button,a,li")) as HTMLElement[];
          for (const n of nodes) {
            if (new RegExp(`^${act}$`, "i").test((n.textContent || "").trim())) {
              n.scrollIntoView({ block: "center" });
              n.click();
              return true;
            }
          }
          return false;
        },
        index,
        action
      );
      if (clicked) {
        console.log(`[AddressPreflight] clicked ${action} inside card ${index}`);
        return;
      }
      throw new Error(`Could not click ${action} on saved address card ${index}`);
    }
    console.log(`=== ${action} address ${target.w.toFixed(0)}x${target.h.toFixed(0)} ===`);
    await this.tapPoint(target.x, target.y, `${action} address`);
  }

  private async deleteExtraAddressCards(): Promise<void> {
    for (let i = 0; i < 8; i++) {
      const cards = await this.listAddressCards();
      if (cards.length <= 1) return;
      console.log(`[AddressPreflight] ${cards.length} addresses — deleting extra so only one remains`);
      await this.clickAddressCardAction(cards.length - 1, "Delete");
      await sleep(400);
      const confirm =
        (await this.findLabelPressable(/^(Delete|Yes|REMOVE)$/i)) ||
        (await this.findExactSmallLabel(/^(Delete|Yes|REMOVE)$/i));
      if (confirm) {
        await this.tapPoint(confirm.x, confirm.y, "Confirm delete address");
      }
      await sleep(1200);
    }
    const leftover = await this.listAddressCards();
    if (leftover.length > 1) {
      throw new Error(
        `Could not reduce saved addresses to one (still ${leftover.length}). Refusing to add another.`
      );
    }
  }

  private async saveAddressForm(kind: "new" | "edited"): Promise<void> {
    await sleep(200);
    await waitWithRetry(
      this.page,
      async () => {
        await this.waitForFunction(
          () =>
            Array.from(document.querySelectorAll("button")).some((b) =>
              /^(save|update)$/i.test((b.textContent || "").trim())
            ),
          { timeout: 5000 }
        );
      },
      { label: "Save button", timeoutMs: 5000, maxRetries: 3 }
    );
    await this.evaluate(() => {
      const btns = Array.from(document.querySelectorAll("button"));
      for (const b of btns) {
        if (/^(save|update)$/i.test((b.textContent || "").trim())) {
          b.scrollIntoView({ block: "center" });
          (b as HTMLElement).click();
          return;
        }
      }
    });
    await sleep(1500);

    const ok = await this.evaluate(() => {
      const t = document.body?.innerText || "";
      return /address saved|saved successfully|added successfully|updated successfully/i.test(t);
    });
    if (ok) {
      console.log(`[AddressPreflight] ${kind} address saved`);
    } else {
      console.log(`[AddressPreflight] could not confirm ${kind} address save — continuing anyway`);
    }
  }

  async verifyAddressOnOrderSummary(
    address: AddressDetails,
    expectedQty: number,
    gstMandatory = true
  ): Promise<void> {
    const MAX_RETRIES = 3;

    for (let retry = 0; retry < MAX_RETRIES; retry++) {
      console.log(`Verifying order summary page${retry > 0 ? ` (retry ${retry}/${MAX_RETRIES - 1})` : ""}...`);

      // Track which steps completed so retries resume from the failure point
      let quantityDone = false;
      let addressDone = false;
      let gstDone = false;

      try {
        if (!/viewcheckout/i.test(this.page.url() || "")) {
          await this.gotoViewCheckout();
        } else {
          await this.waitUntil(async () => {
            return this.evaluate(() => {
              const t = document.body?.innerText || "";
              return t.length > 80 && (/Deliver to/i.test(t) || /GST Invoice/i.test(t) || /Continue/i.test(t));
            });
          }, 12000);
        }

        await this.ensurePageValid();
        const pageUrl = this.page.url();
        console.log(`Order summary page URL: ${pageUrl}`);

        // Step 1: Verify quantity on order summary
        await this.verifyQuantityOnOrderSummary(expectedQty);
        quantityDone = true;

        // Step 2: Verify delivery address
        await this.verifyDeliveryAddressOnSummary(address);
        addressDone = true;

        // Step 3: Verify GST invoice (tap / read / change if wrong)
        await this.verifyGstCheckboxOnSummary(address, gstMandatory);
        gstDone = true;

        // Step 4: Continue → payment
        await this.clickContinueToCheckout();

        if ((this.page.url() || "").includes("viewcheckout")) {
          await this.ensureGstCheckboxTicked();
        }

        console.log("Order summary verification complete");
        return; // success — exit retry loop
      } catch (err) {
        const errMsg = err instanceof Error ? err.message : String(err);
        const completedSteps = [
          quantityDone ? "quantity" : null,
          addressDone ? "address" : null,
          gstDone ? "GST" : null,
        ].filter(Boolean).join(", ");
        const failedStep = !quantityDone ? "quantity" : !addressDone ? "address" : !gstDone ? "GST" : "Continue";

        this.log("error", `Order summary verification failed at step "${failedStep}" (completed: ${completedSteps || "none"}): ${errMsg}`, failedStep);

        if (retry < MAX_RETRIES - 1) {
          console.log(`Refreshing page and retrying from "${failedStep}" step...`);
          // Refresh the order summary page to get a clean state for the failed step
          try {
            await this.page.reload({ waitUntil: "domcontentloaded", timeout: 15000 });
          } catch { /* reload may timeout but page might still load */ }
          await this.waitUntil(async () => {
            return this.evaluate(() => (document.body?.innerText || "").length > 80);
          }, 8000, 120, "viewcheckout after refresh");
        } else {
          // Final retry exhausted — re-throw
          throw new Error(`Order summary verification failed after ${MAX_RETRIES} attempts at step "${failedStep}": ${errMsg}`);
        }
      }
    }
  }

  // ================================================================
  // ORDER SUMMARY PAGE METHODS
  // These run on the intermediate order summary page after Buy Now
  // ================================================================

  private async verifyQuantityOnOrderSummary(expectedQty: number): Promise<void> {
    console.log(`Verifying quantity on order summary: expected=${expectedQty}`);
    this.summaryQty = expectedQty;

    // Try to find the quantity displayed on the order summary page
    let displayedQty = 0;
    for (let attempt = 0; attempt < 3; attempt++) {
      const qtyText = await this.evaluate(() => {
        // Look for quantity near "Qty" text or a number near product info
        const allDivs = Array.from(document.querySelectorAll("div"));

        // PASS 1: the label. "Qty: 1" is unambiguous and is what the page actually
        // renders, so it is tried before any heuristic.
        for (const d of allDivs) {
          const txt = (d.innerText || "").replace(/\s+/g, " ").trim();
          const match = txt.match(/\bqty\b[:\s]*(\d{1,3})\b/i);
          if (match) return match[1];
        }

        // PASS 2: a bare number sitting inside something that mentions qty.
        //
        // This used to run FIRST and unbounded, and it read the delivery address's
        // PHONE NUMBER as the quantity — "7256809635 vs 50 — correcting...". The
        // phone is a pure-digit div, the order-summary card contains both it and
        // the Qty row, so walking up from the phone finds an ancestor containing
        // "qty" and the scan returns it because it comes first in document order.
        // The flow then tried to "correct" a ten-digit quantity, failed to find
        // APPLY, and continued at the wrong quantity with only a warning.
        //
        // Now bounded three ways: at most 3 digits (Flipkart caps far below 1000),
        // at most 3 ancestors up (a phone and a Qty row are cousins, not parent and
        // child), and a plausibility check on the value.
        for (const d of allDivs) {
          const txt = (d.innerText || "").replace(/\s+/g, " ").trim();
          if (!/^\d{1,3}$/.test(txt) || d.offsetParent === null) continue;
          let el: HTMLElement | null = d.parentElement;
          for (let up = 0; el && el !== document.body && up < 3; up++, el = el.parentElement) {
            if (/\bqty\b/i.test((el.innerText || "").replace(/\s+/g, " "))) return txt;
          }
        }
        // Fallback: look for any number input or select with quantity
        const selects = document.querySelectorAll("select");
        for (const s of selects) {
          const parentTxt = ((s.closest("div")?.innerText) || "").toLowerCase();
          if (parentTxt.includes("qty")) {
            return (s as HTMLSelectElement).value;
          }
        }
        // Fallback: look for a visible quantity input
        const inputs = document.querySelectorAll("input");
        for (const inp of inputs) {
          const placeholder = (inp.getAttribute("placeholder") || "").toLowerCase();
          const name = (inp.getAttribute("name") || "").toLowerCase();
          if ((placeholder.includes("quantity") || name.includes("quantity")) && inp.value) {
            return inp.value;
          }
        }
        return null;
      });

      if (qtyText) {
        displayedQty = parseInt(qtyText, 10);
        if (!isNaN(displayedQty)) break;
      }
      await sleep(300);
    }

    if (displayedQty > 0) {
      console.log(`Current quantity on summary: ${displayedQty}`);
    } else {
      console.log("Could not detect quantity on order summary page — assuming correct");
      return;
    }

    if (displayedQty === expectedQty) {
      console.log("Quantity matches — no change needed");
      this.summaryQty = displayedQty;
      return;
    }

    console.log(`Quantity mismatch: ${displayedQty} vs ${expectedQty} — correcting...`);

    // Click the Qty selector to open the quantity dropdown/dialog
    // Pattern from setQuantity(): find div with class css-146c3p1 near "Qty"
    let qtyClicked = false;
    for (let attempt = 0; attempt < 3 && !qtyClicked; attempt++) {
      const result = await this.evaluate(() => {
        const allDivs = Array.from(document.querySelectorAll("div"));
        for (const d of allDivs) {
          const txt = (d.innerText || "").replace(/\s+/g, " ").trim();
          if (txt.toLowerCase().startsWith("qty")) {
            // Walk up to find clickable parent
            let el: HTMLElement | null = d;
            while (el && el !== document.body) {
              const style = el.getAttribute("style") || "";
              if (style.includes("cursor: pointer") || el.getAttribute("role") === "button") {
                el.scrollIntoView({ block: "center" });
                el.click();
                return "clicked";
              }
              el = el.parentElement;
            }
            // Fallback: click the div itself
            (d as HTMLElement).click();
            return "clicked_fallback";
          }
        }
        // Alternative: look for div with css-146c3p1 class that is near Qty text
        for (const d of allDivs) {
          const cls = d.className || "";
          if (cls.includes("css-146c3p1")) {
            let el: HTMLElement | null = d;
            while (el && el !== document.body) {
              const style = el.getAttribute("style") || "";
              if (style.includes("cursor: pointer")) {
                el.scrollIntoView({ block: "center" });
                el.click();
                return "clicked_css_class";
              }
              el = el.parentElement;
            }
          }
        }
        return null;
      });

      if (result) {
        qtyClicked = true;
        console.log(`Qty selector clicked (${result})`);
        await sleep(300);
      } else {
        console.log(`Qty selector not found (attempt ${attempt + 1}/3)`);
        await sleep(300);
      }
    }

    if (!qtyClicked) {
      console.log("WARNING: Could not open quantity selector — proceeding anyway");
      return;
    }

    // Wait for the quantity dialog/dropdown to appear
    let dialogReady = false;
    for (let i = 0; i < 5 && !dialogReady; i++) {
      dialogReady = await this.evaluate(() => {
        return (
          !!document.querySelector('input[placeholder*="Quantity" i]') ||
          !!document.querySelector('input[placeholder*="Qty" i]') ||
          !!document.querySelector(".css-146c3p1") ||
          (document.body?.innerText || "").includes("APPLY")
        );
      });
      if (!dialogReady) await sleep(300);
    }

    if (!dialogReady) {
      console.log("WARNING: Quantity dialog did not appear — proceeding anyway");
      return;
    }

    // Type the desired quantity into the input
    await this.evaluate((qty: number) => {
      // Try to find the quantity input
      const selectors = [
        'input[placeholder*="Quantity" i]',
        'input[placeholder*="Qty" i]',
        'input[name*="quantity" i]',
      ];
      for (const sel of selectors) {
        const inp = document.querySelector(sel) as HTMLInputElement | null;
        if (inp) {
          inp.focus();
          // Clear and type
          inp.value = "";
          const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")?.set;
          if (setter) setter.call(inp, String(qty));
          else inp.value = String(qty);
          inp.dispatchEvent(new Event("input", { bubbles: true }));
          inp.dispatchEvent(new Event("change", { bubbles: true }));
          return "typed";
        }
      }
      return null;
    }, expectedQty);
    await sleep(500);

    // Click APPLY button
    let applied = false;
    for (let attempt = 0; attempt < 3 && !applied; attempt++) {
      const result = await this.evaluate(() => {
        const allDivs = Array.from(document.querySelectorAll("div"));
        for (const d of allDivs) {
          const txt = (d.innerText || "").replace(/\s+/g, " ").trim().toUpperCase();
          if (txt === "APPLY") {
            let el: HTMLElement | null = d;
            while (el && el !== document.body) {
              const style = el.getAttribute("style") || "";
              if (style.includes("cursor: pointer") || el.getAttribute("role") === "button") {
                el.scrollIntoView({ block: "center" });
                el.click();
                return "clicked";
              }
              el = el.parentElement;
            }
            (d as HTMLElement).click();
            return "clicked_fallback";
          }
        }
        // Also try button
        const buttons = document.querySelectorAll("button");
        for (const btn of buttons) {
          const txt = (btn.textContent || "").replace(/\s+/g, " ").trim().toUpperCase();
          if (txt === "APPLY") {
            (btn as HTMLElement).click();
            return "clicked_button";
          }
        }
        return null;
      });

      if (result) {
        applied = true;
        console.log(`Quantity APPLY clicked (${result})`);
        await sleep(500);
      } else {
        console.log(`APPLY button not found (attempt ${attempt + 1}/3)`);
        await sleep(300);
      }
    }

    if (!applied) {
      console.log("WARNING: Could not click APPLY — quantity may not be updated");
    } else {
      console.log(`Quantity updated to ${expectedQty} on order summary`);
    }
  }

  private async verifyDeliveryAddressOnSummary(address: AddressDetails): Promise<void> {
    const effectivePincode = (address.checkoutPincode || address.pincode).trim();
    console.log("Verifying delivery address on order summary...");
    console.log(`Looking for: city="${address.city}", pincode="${effectivePincode}"${address.checkoutPincode ? ` (checkout override, original: ${address.pincode})` : ""}`);

    // Wait for the order summary page to fully load the address section
    try {
      await this.waitForFunction(
        () => {
          const body = document.body?.innerText || "";
          return body.includes("Deliver to") || body.includes("Delivery Address");
        },
        { timeout: 15000 }
      );
      console.log("Address section loaded on order summary");
    } catch {
      console.log("WARNING: Address section not found on order summary page");
    }
    await sleep(300);

    // Read current address text
    const currentText = await this.evaluate(() => {
      const allDivs = Array.from(document.querySelectorAll("div"));
      for (const d of allDivs) {
        const txt = (d.innerText || "").replace(/\s+/g, " ").trim();
        if (txt.startsWith("Deliver to:")) {
          return d.innerText || "";
        }
      }
      // Fallback: look for any div containing address-relevant text
      for (const d of allDivs) {
        const txt = (d.innerText || "").replace(/\s+/g, " ").trim();
        if (txt.includes("Deliver to")) {
          return d.innerText || "";
        }
      }
      return "";
    });

    console.log(`Current address text: "${(currentText || "").slice(0, 150)}"`);

    const matchScore = this.scoreAddressMatch(currentText, address);
    const hasPincode = (currentText || "").includes(effectivePincode);
    const hasCity = (currentText || "").toLowerCase().includes(address.city.trim().toLowerCase());
    const hasName = (currentText || "").toLowerCase().includes((address.companyName || address.name || "").trim().toLowerCase());
    console.log(`Address match score: ${matchScore}/6, pincode=${hasPincode}, city=${hasCity}, name=${hasName}`);

    // Accept if any 2 of: pincode, city, name match — the address is already correct
    if (matchScore >= 2 || (hasPincode && hasCity) || (hasCity && hasName) || (hasPincode && hasName)) {
      console.log("Delivery address matches on order summary — no change needed");
      return;
    }

    // Address mismatch — click "Change" near the address section
    console.log("Delivery address mismatch on order summary — clicking Change...");
    await this.clickAddressChangeButton();
    await sleep(500);

    // Wait for address list/modal to appear
    let addressListLoaded = false;
    for (let i = 0; i < 8 && !addressListLoaded; i++) {
      await sleep(300);
      addressListLoaded = await this.evaluate(() => {
        const body = document.body?.innerText || "";
        return (
          body.includes("Deliver to") ||
          body.includes("Select Delivery") ||
          body.includes("Saved Address") ||
          body.includes("Delivery Address") ||
          body.includes("ADD ADDRESSES")
        );
      });
      if (addressListLoaded) {
        console.log(`Address list loaded (${(i + 1) * 1000}ms)`);
        break;
      }
    }

    if (!addressListLoaded) {
      console.log("WARNING: Address list did not appear after Change click");
    }

    await sleep(300);

    // Try to select the address from the existing saved list — never add a new one
    const found = await this.selectAddressFromList(address);
    if (found) {
      console.log("Selected address from existing list on order summary");
    } else {
      console.log("WARNING: Could not find matching address in saved list — continuing with current address");
    }

    // Wait for modal to close
    let modalClosed = false;
    for (let i = 0; i < 8 && !modalClosed; i++) {
      await sleep(300);
      try {
        const stillOpen = await this.evaluate(() => {
          const body = document.body?.innerText || "";
          return (
            body.includes("Select Delivery Address") ||
            body.includes("Edit Address") ||
            body.includes("ADD ADDRESSES")
          );
        });
        if (!stillOpen) {
          modalClosed = true;
          console.log(`Address modal closed after ~${(i + 1) * 1000}ms`);
        }
      } catch (err) {
        const msg = (err as Error).message;
        if (msg.includes("detached") || msg.includes("Frame")) {
          modalClosed = true;
        }
      }
    }

    // Final mobile-aware verification — if two saved addresses share city +
    // pincode but differ in mobile, the city-only matcher above can pick the
    // wrong one. This loop opens the picker and tries the next candidate
    // until the displayed mobile matches the per-account number.
    await this.ensureMobileMatchesOnCheckout(address);
  }

  private async verifyGstCheckboxOnSummary(address: AddressDetails, gstMandatory = true): Promise<void> {
    const gstNumber = (address.gstNumber || "").trim().toUpperCase();
    const companyName = (address.companyName || "").trim();
    if (!gstMandatory && !gstNumber) {
      console.log("GST not mandatory and no GST on job — skipping GST invoice");
      return;
    }
    console.log(`Verifying GST on order summary: ${gstNumber || "(none)"} / ${companyName || "(none)"} mandatory=${gstMandatory}`);

    const gstVisible = await this.waitUntil(async () => {
      return this.evaluate(() => /Use GST Invoice|GST Invoice/i.test(document.body?.innerText || ""));
    }, 8000, 120, "GST Invoice on viewcheckout");
    if (!gstVisible) {
      if (gstMandatory) {
        throw new CheckoutFailure(
          "GST_NOT_FOUND",
          "GST is not found on page — GST Invoice is required on viewcheckout to process this order"
        );
      }
      console.log("GST section not on viewcheckout and GST is not mandatory — continuing");
      return;
    }
    if (!gstNumber) {
      throw new CheckoutFailure("GST_NOT_FOUND", "GST is not found on page — job has no GST number but GST is mandatory");
    }
    const gstinReady = await this.waitUntil(async () => Boolean((await this.locateGstInvoiceBlock()).gstin), 5000, 120, "GSTIN on GST Invoice block");
    if (!gstinReady && gstMandatory) {
      throw new CheckoutFailure("GST_NOT_FOUND", "GST is not found on page — GST Invoice label is there but no GSTIN was readable");
    }

    const panel = await this.locateGstInvoiceBlock();
    console.log(
      `[GST] before: checked=${panel.checked} gstin=${panel.gstin || "(none)"} company=${panel.company || "(none)"} text="${panel.text.slice(0, 160)}"`
    );
    if (this.gstPanelMatches(panel, gstNumber, companyName) && panel.checked) {
      console.log("GST invoice already ticked with the correct GST — leaving it");
      return;
    }

    console.log(
      `[GST] page has ${panel.gstin || "other GST"} / ${panel.company || "?"} — job wants ${gstNumber} / ${companyName || "?"} — opening GST Change drawer`
    );
    await this.clickGstChangeButton();
    if (!(await this.waitUntil(() => this.gstPickerOpen(), 6000, 120, "GST drawer after Change"))) {
      console.log("[GST] drawer not open — clicking GST Change pointer again");
      await this.clickGstChangeButton();
    }
    if (!(await this.waitUntil(() => this.gstPickerOpen(), 6000, 120, "GST drawer retry"))) {
      throw new Error("GST Change drawer did not open after clicking the GST Invoice Change button");
    }
    await this.ensureCorrectGstInPicker(gstNumber, companyName);

    await this.waitUntil(async () => {
      const p = await this.locateGstInvoiceBlock();
      return this.gstPanelMatches(p, gstNumber, companyName) && p.checked;
    }, 5000);
    const finalPanel = await this.locateGstInvoiceBlock();
    console.log(
      `[GST] after: checked=${finalPanel.checked} gstin=${finalPanel.gstin || "(none)"}`
    );
    if (!this.gstPanelMatches(finalPanel, gstNumber, companyName) || !finalPanel.checked) {
      throw new Error(
        `GST not correct after refill (have ${finalPanel.gstin || "none"}, want ${gstNumber})`
      );
    }
    console.log("GST invoice ticked with the correct GST");
  }

  private gstPanelMatches(
    panel: { checked: boolean; gstin: string; text: string; company?: string },
    gstNumber: string,
    companyName: string
  ): boolean {
    const hay = `${panel.gstin} ${panel.company || ""} ${panel.text}`.toUpperCase();
    if (!hay.includes(gstNumber.toUpperCase())) return false;
    if (companyName && !hay.includes(companyName.toUpperCase())) {
      console.log(`[GST] GSTIN matches but company text did not include "${companyName}" — accepting GSTIN`);
    }
    return true;
  }

  /**
   * Full GST Invoice card: title + Change + GSTIN + company.
   * Do not lock the title row alone (that row has Change but no GSTIN).
   */
  private async locateGstInvoiceBlock(): Promise<{
    checked: boolean;
    gstin: string;
    company: string;
    text: string;
    change: { x: number; y: number; w: number; h: number } | null;
  }> {
    return this.evaluate(() => {
      const gstinRe = /\b\d{2}[A-Z]{5}\d{4}[A-Z][A-Z0-9]Z[A-Z0-9]\b/i;
      const compact = (s: string) => (s || "").replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
      const empty = { checked: false, gstin: "", company: "", text: "", change: null as null };
      let label: HTMLElement | null = null;
      for (const n of Array.from(document.querySelectorAll("div,span")) as HTMLElement[]) {
        if (/^GST Invoice$/i.test(compact(n.textContent || ""))) {
          label = n;
          break;
        }
      }
      if (!label) return empty;

      let best: HTMLElement | null = null;
      let bestLen = Infinity;
      let walk: HTMLElement | null = label;
      while (walk && walk !== document.body) {
        const t = compact(walk.innerText || "");
        const tooWide = /Deliver to:|Price Details|Place Order|Total Amount/i.test(t);
        if (
          /GST Invoice/i.test(t) &&
          /\bChange\b/i.test(t) &&
          gstinRe.test(t) &&
          !tooWide &&
          t.length < 800 &&
          t.length < bestLen
        ) {
          best = walk;
          bestLen = t.length;
        }
        walk = walk.parentElement;
      }
      const panel = best;
      if (!panel) return empty;

      const text = compact(panel.innerText || "");
      const gstin = (text.match(gstinRe)?.[0] || "").toUpperCase();
      let company = "";
      for (const n of Array.from(panel.querySelectorAll("div,span")) as HTMLElement[]) {
        const t = compact(n.textContent || "");
        if (!t || gstinRe.test(t) || /GST Invoice|^Change$/i.test(t)) continue;
        if (n.children.length > 0) continue;
        if (t.length >= 3 && t.length <= 80 && /[A-Za-z]/.test(t)) {
          company = t;
          break;
        }
      }

      const labelBox = label.getBoundingClientRect();
      let change: { x: number; y: number; w: number; h: number } | null = null;
      let rowDist = Infinity;
      for (const n of Array.from(panel.querySelectorAll("div,span,button")) as HTMLElement[]) {
        if (compact(n.textContent || "") !== "Change") continue;
        if (compact(n.innerText || "") !== "Change") continue;
        let target = n;
        let el: HTMLElement | null = n.parentElement;
        while (el && panel.contains(el)) {
          if (
            (el.getAttribute("style") || "").includes("cursor: pointer") &&
            compact(el.innerText || "") === "Change"
          ) {
            target = el;
            break;
          }
          el = el.parentElement;
        }
        const r = target.getBoundingClientRect();
        if (r.width < 8 || r.height < 8) continue;
        const dist = Math.abs(r.y + r.height / 2 - (labelBox.y + labelBox.height / 2));
        if (dist < rowDist) {
          rowDist = dist;
          change = { x: r.x + r.width / 2, y: r.y + r.height / 2, w: r.width, h: r.height };
        }
      }

      let checked = false;
      for (const img of Array.from(panel.querySelectorAll("img"))) {
        const src = `${img.getAttribute("src") || ""} ${img.getAttribute("srcset") || ""}`;
        if (/checked-|\bchecked\b/i.test(src)) checked = true;
      }
      const box = panel.querySelector('input[type="checkbox"]') as HTMLInputElement | null;
      if (box?.checked) checked = true;
      if (!checked && gstin) checked = true;
      return { checked, gstin, company, text, change };
    });
  }

  private async gstPickerOpen(): Promise<boolean> {
    return this.evaluate(() => {
      const compact = (s: string) => (s || "").replace(/\s+/g, " ").trim();
      const body = compact(document.body?.innerText || "");
      if (/Select GST Details/i.test(body)) return true;
      if (/Add new GST Details/i.test(body) && /Confirm and (Use|Save)/i.test(body)) return true;
      if (document.querySelector('input[maxlength="15"]') && /GSTIN|Business Name/i.test(body)) {
        return true;
      }
      const sheet = document.getElementById("msite-bottomsheet");
      if (sheet && /GST/i.test(sheet.innerText || "") && /Change|Confirm|GSTIN/i.test(sheet.innerText || "")) {
        return true;
      }
      return Array.from(document.querySelectorAll("div,span,h1,h2")).some((el) =>
        /select gst details/i.test(compact(el.textContent || ""))
      );
    });
  }

  private async clickGstInvoiceControl(): Promise<void> {
    const clicked = await this.evaluate(() => {
      const clickEl = (el: HTMLElement) => {
        el.scrollIntoView({ block: "center" });
        el.click();
      };
      const boxes = Array.from(document.querySelectorAll('input[type="checkbox"]')) as HTMLInputElement[];
      for (const box of boxes) {
        let el: HTMLElement | null = box;
        for (let i = 0; i < 8 && el; i++) {
          if (/GST Invoice/i.test(el.innerText || "")) {
            clickEl(box);
            return "checkbox";
          }
          el = el.parentElement;
        }
      }
      const aria = Array.from(document.querySelectorAll('[role="checkbox"]')) as HTMLElement[];
      for (const cb of aria) {
        let el: HTMLElement | null = cb;
        for (let i = 0; i < 8 && el; i++) {
          if (/GST Invoice/i.test(el.innerText || "")) {
            clickEl(cb);
            return "aria";
          }
          el = el.parentElement;
        }
      }
      const nodes = Array.from(document.querySelectorAll("div,span,label,button")) as HTMLElement[];
      for (const n of nodes) {
        const t = (n.textContent || "").replace(/\s+/g, " ").trim();
        if (!/^Use GST Invoice$/i.test(t) && t !== "Use GST Invoice") continue;
        let el: HTMLElement | null = n;
        while (el && el !== document.body) {
          const style = el.getAttribute("style") || "";
          if (style.includes("cursor: pointer") || el.getAttribute("role") === "checkbox") {
            clickEl(el);
            return "label";
          }
          el = el.parentElement;
        }
        clickEl(n);
        return "text";
      }
      return null;
    });
    if (!clicked) throw new Error("Could not tap Use GST Invoice / GST checkbox");
    console.log(`[GST] tapped via ${clicked}`);
  }

  private async clickGstChangeButton(): Promise<void> {
    const box = await this.evaluate(() => {
      const compact = (s: string) => (s || "").replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
      const gstinRe = /\b\d{2}[A-Z]{5}\d{4}[A-Z][A-Z0-9]Z[A-Z0-9]\b/i;
      let label: HTMLElement | null = null;
      for (const n of Array.from(document.querySelectorAll("div,span")) as HTMLElement[]) {
        if (/^GST Invoice$/i.test(compact(n.textContent || ""))) {
          label = n;
          break;
        }
      }
      if (!label) return null;
      label.scrollIntoView({ block: "center", inline: "nearest" });
      let root: HTMLElement | null = label;
      while (root && root !== document.body) {
        const t = compact(root.innerText || "");
        if (/GST Invoice/i.test(t) && gstinRe.test(t) && /\bChange\b/i.test(t) && t.length < 800) {
          break;
        }
        root = root.parentElement;
      }
      if (!root || root === document.body) root = label.parentElement ?? label;
      const pointers = Array.from(root.querySelectorAll("div")) as HTMLElement[];
      const changePtrs = pointers.filter((d) => {
        const style = d.getAttribute("style") || "";
        return style.includes("cursor: pointer") && compact(d.innerText || "") === "Change";
      });
      const pointer = changePtrs[0];
      if (!pointer) return null;
      pointer.scrollIntoView({ block: "center", inline: "nearest" });
      pointer.click();
      const r = pointer.getBoundingClientRect();
      return { x: r.x + r.width / 2, y: r.y + r.height / 2, w: r.width, h: r.height, via: "pointer" };
    });
    if (!box) throw new Error("GST Invoice Change button not found (second Change, next to GST Invoice)");
    console.log(`=== GST Invoice Change ${box.w.toFixed(0)}x${box.h.toFixed(0)} via ${box.via} ===`);
    await this.tapPoint(box.x, box.y, "GST Invoice Change");
  }



  
  private async readGstDrawer(): Promise<{ gstins: string[]; hasEdit: boolean; hasAddNew: boolean; hasConfirm: boolean }> {
    return this.evaluate(() => {
      const compact = (s: string) => (s || "").replace(/\s+/g, " ").trim();
      const gstinRe = /\b\d{2}[A-Z]{5}\d{4}[A-Z][A-Z0-9]Z[A-Z0-9]\b/gi;
      const sheet = document.getElementById("msite-bottomsheet");
      const text = compact((sheet as HTMLElement | null)?.innerText || document.body?.innerText || "");
      const gstins = [...new Set((text.match(gstinRe) || []).map((g) => g.toUpperCase()))];
      return {
        gstins,
        hasEdit: /\bEdit\b/.test(text),
        hasAddNew: /Add new GST Details/i.test(text),
        hasConfirm: /Confirm and (Use|Save)/i.test(text),
      };
    });
  }

  private async ensureCorrectGstInPicker(gstNumber: string, companyName: string): Promise<void> {
    const opened = await this.waitForGstPicker(10000);
    if (!opened) throw new Error("GST select drawer did not appear after GST Invoice Change");

    const drawer = await this.readGstDrawer();
    console.log(`[GST] drawer GSTINs=${drawer.gstins.join(",") || "(none)"} edit=${drawer.hasEdit} add=${drawer.hasAddNew}`);

    const listed = drawer.gstins.includes(gstNumber.toUpperCase());
    if (listed) {
      console.log(`[GST] ${gstNumber} is in the drawer — select radio, then Confirm and Use`);
      await this.selectGstRowInPicker(gstNumber);
      await this.clickConfirmAndUseGst();
      await this.waitUntil(async () => !(await this.gstPickerOpen()), 8000, 120, "GST drawer close after radio");
      return;
    }

    if (drawer.hasEdit) {
      console.log(`[GST] ${gstNumber} not in list — Edit the selected GST and refill`);
      await this.clickGstEditInPicker();
      const formReady = await this.waitForGstFormInputs(10000);
      if (!formReady) throw new Error("GST Edit form inputs never appeared");
      await this.fillGstForm(gstNumber, companyName);
      await this.clickConfirmAndUseGst();
      await this.waitUntil(async () => !(await this.gstPickerOpen()), 8000, 120, "GST drawer close after Edit");
      return;
    }

    console.log(`[GST] target not in drawer — Add new GST Details ${gstNumber}`);
    await this.clickAddNewGstDetails();
    const formReady = await this.waitForGstFormInputs(10000);
    if (!formReady) throw new Error("GST form inputs never appeared after Add new GST Details");
    await this.fillGstForm(gstNumber, companyName);
    await this.clickConfirmAndUseGst();
    await this.waitUntil(async () => !(await this.gstPickerOpen()), 8000, 120, "GST drawer close after add");
  }

  private async clickGstEditInPicker(): Promise<void> {
    const clicked = await this.evaluate(() => {
      const compact = (s: string) => s.replace(/\s+/g, " ").trim();
      const nodes = Array.from(document.querySelectorAll("div,span,button,a")) as HTMLElement[];
      for (const n of nodes) {
        if (compact(n.textContent || "") !== "Edit") continue;
        let el: HTMLElement | null = n;
        let inGst = false;
        let pointer: HTMLElement = n;
        for (let i = 0; i < 16 && el && el !== document.body; i++) {
          const t = compact(el.innerText || "");
          if (/Select GST Details/i.test(t) || (/GST/i.test(t) && t.length < 400)) inGst = true;
          if ((el.getAttribute("style") || "").includes("cursor: pointer") || el.tagName === "BUTTON") {
            pointer = el;
          }
          el = el.parentElement;
        }
        if (!inGst) continue;
        pointer.scrollIntoView({ block: "center" });
        pointer.click();
        return true;
      }
      return false;
    });
    if (!clicked) throw new Error("GST Edit button not found in Select GST Details");
    console.log("[GST] clicked Edit in picker");
  }

  private async waitForGstPicker(timeoutMs: number): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (await this.gstPickerOpen()) return true;
      await sleep(300);
    }
    return false;
  }

  private async waitForGstFormInputs(timeoutMs: number): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const ready = await this.evaluate(() => {
        return !!(document.querySelector('input[maxlength="15"]') || document.querySelector('input[maxlength="60"]'));
      });
      if (ready) return true;
      await sleep(300);
    }
    return false;
  }

  private async selectGstRowInPicker(gstNumber: string): Promise<void> {
    const clicked = await this.evaluate((gst: string) => {
      const upper = gst.toUpperCase();
      const nodes = Array.from(document.querySelectorAll("div,label,span,li")) as HTMLElement[];
      let best: HTMLElement | null = null;
      let bestLen = Infinity;
      for (const n of nodes) {
        const t = (n.innerText || "").replace(/\s+/g, " ").trim();
        if (!t.toUpperCase().includes(upper)) continue;
        if (/^Edit$/i.test(t)) continue;
        if (t.length < bestLen && t.length < 240) {
          best = n;
          bestLen = t.length;
        }
      }
      if (!best) return false;
      let el: HTMLElement | null = best;
      while (el && el !== document.body) {
        const radio = el.querySelector('input[type="radio"]') as HTMLInputElement | null;
        if (radio) {
          radio.click();
          el.click();
          return true;
        }
        const style = el.getAttribute("style") || "";
        if (style.includes("cursor: pointer") || el.getAttribute("role") === "radio") {
          el.click();
          return true;
        }
        el = el.parentElement;
      }
      best.click();
      return true;
    }, gstNumber);
    if (!clicked) throw new Error(`Could not select GST row ${gstNumber} in picker`);
    await sleep(400);
  }

  private async clickAddNewGstDetails(): Promise<void> {
    const clicked = await this.evaluate(() => {
      const nodes = Array.from(document.querySelectorAll("div,span,button,a")) as HTMLElement[];
      for (const n of nodes) {
        const t = (n.textContent || "").replace(/\s+/g, " ").trim();
        if (!/Add new GST Details/i.test(t) || t.length > 40) continue;
        let el: HTMLElement | null = n;
        while (el && el !== document.body) {
          const style = el.getAttribute("style") || "";
          if (style.includes("cursor: pointer") || el.tagName === "BUTTON" || el.tagName === "A") {
            el.scrollIntoView({ block: "center" });
            el.click();
            return true;
          }
          el = el.parentElement;
        }
        n.scrollIntoView({ block: "center" });
        n.click();
        return true;
      }
      return false;
    });
    if (!clicked) throw new Error("Could not click Add new GST Details");
    console.log("[GST] clicked Add new GST Details");
    await sleep(800);
  }

  private async clickConfirmAndUseGst(): Promise<void> {
    const box = await this.evaluate(() => {
      const compact = (s: string) => s.replace(/\s+/g, " ").trim();
      const labels = /^(Confirm and Use|Confirm and Save)$/i;
      const hits: Array<{ x: number; y: number; w: number; h: number; bottom: number }> = [];
      for (const n of Array.from(document.querySelectorAll("button,div,span")) as HTMLElement[]) {
        if (!labels.test(compact(n.textContent || ""))) continue;
        let el: HTMLElement | null = n;
        let best = n;
        while (el && el !== document.body) {
          const r = el.getBoundingClientRect();
          if (r.height >= 36 && r.height <= 88 && r.width >= 80) best = el;
          if ((el.getAttribute("style") || "").includes("cursor: pointer")) {
            best = el;
            break;
          }
          el = el.parentElement;
        }
        const r = best.getBoundingClientRect();
        if (r.width < 40 || r.height < 20) continue;
        hits.push({ x: r.x + r.width / 2, y: r.y + r.height / 2, w: r.width, h: r.height, bottom: r.bottom });
      }
      if (!hits.length) return null;
      hits.sort((a, b) => b.bottom - a.bottom || b.w - a.w);
      const pick = hits[0];
      return { x: pick.x, y: pick.y, w: pick.w, h: pick.h };
    });
    if (box) {
      console.log(`=== Confirm GST ${box.w.toFixed(0)}x${box.h.toFixed(0)} ===`);
      await this.tapPoint(box.x, box.y, "Confirm and Use / Save");
      return;
    }
    throw new Error("Could not click Confirm and Use / Confirm and Save");
  }

  private async clickContinueToCheckout(): Promise<void> {
    console.log("Clicking Continue to proceed to checkout...");

    // Wait for the Continue button to appear
    let buttonFound = false;
    for (let attempt = 0; attempt < 3 && !buttonFound; attempt++) {
      try {
        await this.waitForFunction(
          () => {
            const allDivs = Array.from(document.querySelectorAll("div"));
            for (const d of allDivs) {
              const txt = (d.innerText || "").replace(/\s+/g, " ").trim().toLowerCase();
              // Flipkart Continue button often has "continue" text with green styling
              if (txt === "continue" || txt === "continue ") {
                return true;
              }
            }
            // Also check buttons
            const buttons = Array.from(document.querySelectorAll("button"));
            for (const b of buttons) {
              const txt = (b.textContent || "").replace(/\s+/g, " ").trim().toLowerCase();
              if (txt === "continue" || txt === "continue ") return true;
            }
            return false;
          },
          { timeout: 15000 }
        );
        buttonFound = true;
      } catch (err) {
        const msg = (err as Error).message;
        if (msg.includes("detached") || msg.includes("Frame")) {
          console.log("Frame detached while waiting for Continue (attempt " + (attempt + 1) + "/3)");
          await sleep(500);
        } else {
          console.log("Continue button not found: " + msg);
          break;
        }
      }
    }

    if (!buttonFound) {
      console.log("Continue not on this page — opening remembered payments URL");
      await this.gotoPayments();
      return;
    }

    // Click the Continue button
    let result: string | null = null;
    for (let attempt = 0; attempt < 3 && !result; attempt++) {
      result = await this.evaluate(() => {
        // Try buttons first (most reliable)
        const buttons = Array.from(document.querySelectorAll("button"));
        for (const b of buttons) {
          const txt = (b.textContent || "").replace(/\s+/g, " ").trim().toLowerCase();
          if (txt === "continue" || txt === "continue ") {
            (b as HTMLElement).click();
            return "clicked_button";
          }
        }
        // Try divs
        const allDivs = Array.from(document.querySelectorAll("div"));
        for (const d of allDivs) {
          const txt = (d.innerText || "").replace(/\s+/g, " ").trim().toLowerCase();
          if (txt === "continue" || txt === "continue ") {
            // Walk up to find clickable parent
            let el: HTMLElement | null = d;
            while (el && el !== document.body) {
              const style = el.getAttribute("style") || "";
              if (style.includes("cursor: pointer") || el.getAttribute("role") === "button") {
                el.scrollIntoView({ block: "center" });
                el.click();
                return "clicked_div";
              }
              el = el.parentElement;
            }
            // Fallback: click div itself
            (d as HTMLElement).click();
            return "clicked_div_fallback";
          }
        }
        return null;
      });

      if (result) {
        console.log(`Continue clicked (${result})`);
        break;
      } else {
        console.log(`Continue not found (attempt ${attempt + 1}/3)`);
        await sleep(300);
      }
    }

    if (!result) {
      console.log("Could not click Continue — opening remembered payments URL");
      await this.gotoPayments();
      return;
    }

    console.log("Waiting for Continue → /payments ...");
    const reached = await this.waitUntil(
      async () => /\/payments/i.test(this.page.url() || "") || !/viewcheckout/i.test(this.page.url() || ""),
      8000,
      120,
      "payments after Continue"
    );
    if (/\/payments/i.test(this.page.url() || "")) {
      console.log(`Continue navigation complete — ${this.page.url()}`);
      return;
    }
    if (reached && !/viewcheckout/i.test(this.page.url() || "")) {
      console.log(`Left viewcheckout — ${this.page.url()}`);
      return;
    }
    console.log("Continue did not open payments — using remembered /payments URL");
    await this.gotoPayments();
  }

  private async gotoPayments(): Promise<void> {
    if (/\/payments/i.test(this.page.url() || "")) {
      console.log(`Already on payments ${this.page.url()}`);
      return;
    }
    console.log(`Jumping to remembered payments ${PAYMENTS_URL}`);
    await navigateWithRetry(this.page, PAYMENTS_URL, { timeoutMs: 12000, maxRetries: 2 });
    console.log(`On payments ${this.page.url()}`);
  }

  private async ensureGstCheckboxTicked(): Promise<void> {
    console.log("Checking GST invoice checkbox...");
    const panel = await this.locateGstInvoiceBlock();
    if (panel.checked) {
      console.log("GST invoice checkbox is already ticked");
      return;
    }
    console.log("GST invoice checkbox not ticked — clicking it");
    try {
      await this.clickGstInvoiceControl();
      await sleep(800);
    } catch (err) {
      console.log(`WARNING: follow-up GST click failed: ${err instanceof Error ? err.message : String(err)}`);
      return;
    }
    const after = await this.locateGstInvoiceBlock();
    if (!after.checked) {
      console.log("WARNING: GST still not ticked after follow-up click");
    }
  }

  private async clickAddressChangeButton(): Promise<void> {
    // Wait for the address "Change" button to appear — handle detached frame errors
    let buttonFound = false;
    for (let attempt = 0; attempt < 3 && !buttonFound; attempt++) {
      try {
        await this.waitForFunction(
          () => {
            const divs = Array.from(document.querySelectorAll("div"));
            for (const d of divs) {
              const txt = (d.innerText || "").replace(/\s+/g, " ").trim();
              if (txt === "Change " || txt === "Change") return true;
            }
            return false;
          },
          { timeout: 15000 }
        );
        buttonFound = true;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (msg.includes("detached") || msg.includes("Frame")) {
          console.log(`[clickAddressChangeButton] Frame detached (attempt ${attempt + 1}/3), retrying...`);
          await sleep(500);
        } else {
          console.log(`WARNING: Address 'Change' button never appeared: ${msg}`);
          return;
        }
      }
    }

    if (!buttonFound) {
      console.log("WARNING: Address 'Change' button never appeared");
      return;
    }

    // Find all "Change" buttons and pick the one near the "Deliver to:" section
    let result: string;
    try {
      result = await this.evaluate(() => {
      const allDivs = Array.from(document.querySelectorAll("div")) as HTMLElement[];

      // Find the "Deliver to:" section container first
      let deliverToSection: HTMLElement | null = null;
      for (const d of allDivs) {
        const txt = (d.innerText || "").replace(/\s+/g, " ").trim();
        if (txt.startsWith("Deliver to:")) {
          deliverToSection = d;
          break;
        }
      }

      if (!deliverToSection) {
        // Fallback: click the first "Change" div we find
        for (const d of allDivs) {
          const txt = (d.innerText || "").replace(/\s+/g, " ").trim();
          if (txt === "Change " || txt === "Change") {
            // Walk up to find clickable parent
            let el: HTMLElement | null = d;
            let bestEl: HTMLElement | null = null;
            while (el && el !== document.body) {
              const style = el.getAttribute("style") || "";
              const cls = el.className || "";
              if (cls.includes("css-g5y9jx") && style.includes("cursor")) {
                bestEl = el;
              }
              if (style.includes("cursor: pointer")) {
                bestEl = el;
              }
              el = el.parentElement as HTMLElement | null;
            }
            if (bestEl) {
              bestEl.scrollIntoView({ block: "center" });
              bestEl.click();
              return "clicked_first_change";
            }
            (d as HTMLElement).click();
            return "clicked_first_change_fallback";
          }
        }
        return "no_change_button_found";
      }

      // Find "Change" buttons inside or near the "Deliver to:" section
      // Walk through siblings of deliverToSection
      const parent = deliverToSection.parentElement;
      if (parent) {
        const siblings = Array.from(parent.querySelectorAll(":scope > *")) as HTMLElement[];
        for (const sibling of siblings) {
          const siblingText = (sibling.innerText || "").replace(/\s+/g, " ").trim();
          if (siblingText === "Change " || siblingText === "Change") {
            sibling.scrollIntoView({ block: "center" });
            sibling.click();
            return "clicked_deliver_section_sibling";
          }
        }
      }

      // Search within the deliverToSection and nearby DOM for "Change"
      const innerDivs = Array.from(
        deliverToSection.querySelectorAll("div, span, button")
      ) as HTMLElement[];
      for (const d of innerDivs) {
        const txt = (d.innerText || "").replace(/\s+/g, " ").trim();
        if (txt === "Change " || txt === "Change") {
          let el: HTMLElement | null = d;
          while (el && el !== document.body) {
            const style = el.getAttribute("style") || "";
            if (style.includes("cursor: pointer")) {
              el.scrollIntoView({ block: "center" });
              el.click();
              return "clicked_inside_deliver_section";
            }
            el = el.parentElement;
          }
          (d as HTMLElement).click();
          return "clicked_inside_deliver_fallback";
        }
      }

      // Check if "Change" is in the parent's siblings' children
      if (parent && parent.parentElement) {
        const gpChildren = Array.from(
          parent.parentElement.querySelectorAll(":scope > *")
        ) as HTMLElement[];
        for (const child of gpChildren) {
          if (child === parent) continue;
          const childText = (child.innerText || "").replace(/\s+/g, " ").trim();
          if (childText === "Change " || childText === "Change") {
            child.scrollIntoView({ block: "center" });
            child.click();
            return "clicked_gp_sibling";
          }
          // Look in grandchildren
          const gc = Array.from(
            child.querySelectorAll("div, span, button")
          ) as HTMLElement[];
          for (const gcEl of gc) {
            const gcText = (gcEl.innerText || "").replace(/\s+/g, " ").trim();
            if (gcText === "Change " || gcText === "Change") {
              let el: HTMLElement | null = gcEl;
              while (el && el !== document.body) {
                if ((el.getAttribute("style") || "").includes("cursor: pointer")) {
                  el.scrollIntoView({ block: "center" });
                  el.click();
                  return "clicked_gc";
                }
                el = el.parentElement;
              }
            }
          }
        }
      }

      // Last resort: scan ALL divs and prefer the one closest to "Deliver to:"
      let bestDist = Infinity;
      let bestEl: HTMLElement | null = null;
      const dtRect = deliverToSection.getBoundingClientRect();
      for (const d of allDivs) {
        const txt = (d.innerText || "").replace(/\s+/g, " ").trim();
        if (txt === "Change " || txt === "Change") {
          const r = d.getBoundingClientRect();
          const dist = Math.abs(r.top - dtRect.top) + Math.abs(r.left - dtRect.left);
          if (dist < bestDist) {
            bestDist = dist;
            bestEl = d;
          }
        }
      }
      if (bestEl) {
        bestEl.scrollIntoView({ block: "center" });
        bestEl.click();
        return `clicked_nearest_change_dist=${bestDist.toFixed(0)}`;
      }

      return "no_change_button_found";
    });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes("detached") || msg.includes("Frame")) {
        console.log("[clickAddressChangeButton] Frame detached during evaluation — page likely navigated, proceeding...");
      } else {
        console.log(`[clickAddressChangeButton] Evaluate error: ${msg}`);
      }
      return;
    }

    console.log(`[clickAddressChangeButton] Result: ${result}`);
  }

  /**
   * Scans the saved-addresses modal for a card matching the target address
   * by company name + city + locality. Falls back to false if no match found.
   * Saved address cards show: company name (bold) + "locality, city" text.
   */
  private async selectAddressFromList(
    address: AddressDetails,
    excludeTexts: string[] = []
  ): Promise<boolean> {
    const targetName = (address.companyName || address.name || "").trim().toLowerCase();
    const targetCity = address.city.trim().toLowerCase();
    const targetLocality = (address.locality || "").trim().toLowerCase();
    console.log(`[SelectAddress] Looking for saved address: name="${targetName}", city="${targetCity}", locality="${targetLocality}", excluding=${excludeTexts.length} prior candidate(s)`);

    // Wait for the address modal to appear
    let modalFound = false;
    for (let attempt = 0; attempt < 8 && !modalFound; attempt++) {
      try {
        modalFound = await this.evaluate(() => {
          const body = document.body?.innerText || "";
          return (
            body.includes("Deliver to") ||
            body.includes("Select Delivery Address") ||
            body.includes("Saved Address") ||
            body.includes("Delivery Address") ||
            body.includes("ADD ADDRESSES")
          );
        });
      } catch (err) {
        const msg = (err as Error).message;
        if (msg.includes("detached") || msg.includes("Frame")) {
          console.log(`[SelectAddress] Frame detached (attempt ${attempt + 1}/8), waiting...`);
          await sleep(500);
          continue;
        }
        throw err;
      }
      if (!modalFound) await sleep(300);
    }

    if (!modalFound) {
      console.log("[SelectAddress] Address modal never appeared");
      return false;
    }
    console.log("[SelectAddress] Address modal detected");
    await sleep(500);

    // Find all address card candidates — Flipkart uses cDeXU9 for clickable address cards
    // Each card contains company name div + locality/city text div
    let result: {
      x: number;
      y: number;
      cardText: string;
      score: number;
    } | null = null;

    for (let attempt = 0; attempt < 3 && !result; attempt++) {
      try {
        result = await this.evaluate(
          (addr: AddressDetails, exclude: string[]) => {
            const targetName = (addr.companyName || addr.name || "").trim().toLowerCase();
            const targetCity = addr.city.trim().toLowerCase();
            const targetLocality = (addr.locality || "").trim().toLowerCase();
            const excluded = new Set(
              exclude.map((s) => s.replace(/\s+/g, " ").trim().toLowerCase())
            );

            const cards: {
              el: HTMLElement;
              name: string;
              text: string;
              rect: DOMRect;
            }[] = [];

            // Strategy 1: Find by cDeXU9 class (the address card class from the DOM)
            const cdexu9 = document.querySelectorAll(".cDeXU9");
            for (const el of cdexu9) {
              // cDeXU9 cards have cursor:pointer and contain address text
              const text = (el as HTMLElement).innerText || "";
              const style = (el as HTMLElement).getAttribute("style") || "";
              if (
                style.includes("cursor") &&
                text.trim().length > 5 &&
                !text.toLowerCase().includes("ADD ADDRESSES")
              ) {
                const r = (el as HTMLElement).getBoundingClientRect();
                cards.push({ el: el as HTMLElement, name: "", text, rect: r });
              }
            }

            // Strategy 2: Find all divs with cursor:pointer that look like address cards
            if (cards.length === 0) {
              const allDivs = document.querySelectorAll("div[style*='cursor']");
              for (const el of allDivs) {
                const style = (el as HTMLElement).getAttribute("style") || "";
                const text = (el as HTMLElement).innerText || "";
                if (
                  style.includes("pointer") &&
                  text.trim().length > 10 &&
                  !text.toLowerCase().includes("ADD ADDRESSES") &&
                  (text.toLowerCase().includes("deliver") ||
                    text.toLowerCase().includes(targetCity) ||
                    text.toLowerCase().includes("address"))
                ) {
                  const r = (el as HTMLElement).getBoundingClientRect();
                  cards.push({ el: el as HTMLElement, name: "", text, rect: r });
                }
              }
            }

            // Strategy 3: Find any visible divs containing address-like text
            if (cards.length === 0) {
              const allEls = document.querySelectorAll("div");
              for (const el of allEls) {
                const text = (el as HTMLElement).innerText || "";
                const style = (el as HTMLElement).getAttribute("style") || "";
                if (
                  (style.includes("cursor") || style.includes("pointer")) &&
                  text.trim().length > 20 &&
                  !text.toLowerCase().includes("ADD ADDRESSES") &&
                  (text.toLowerCase().includes(targetCity) || text.toLowerCase().includes(targetName))
                ) {
                  const r = (el as HTMLElement).getBoundingClientRect();
                  cards.push({ el: el as HTMLElement, name: "", text, rect: r });
                }
              }
            }

            console.log(`[SelectAddress] Found ${cards.length} address card candidates`);

            if (cards.length === 0) {
              // Last resort: scan all visible clickable divs with city or name text
              const allDivs = Array.from(document.querySelectorAll("div")) as HTMLElement[];
              for (const d of allDivs) {
                const style = d.getAttribute("style") || "";
                const text = d.innerText || "";
                const cls = d.className || "";
                if (
                  (style.includes("cursor") || cls.includes("css-g5y9jx")) &&
                  text.trim().length > 15 &&
                  !text.toLowerCase().includes("ADD ADDRESSES") &&
                  text.toLowerCase().includes(targetCity)
                ) {
                  const r = d.getBoundingClientRect();
                  if (r.width > 50 && r.height > 30) {
                    cards.push({ el: d, name: "", text, rect: r });
                  }
                }
              }
            }

            // Score each card
            let bestScore = -1;
            let bestCard: { el: HTMLElement; name: string; text: string; rect: DOMRect } | null = null;

            for (const card of cards) {
              const cardText = card.text.toLowerCase();
              const cardName = card.name.toLowerCase();
              const normalised = card.text.replace(/\s+/g, " ").trim().toLowerCase();
              // Skip candidates the caller has already tried (and rejected
              // because their mobile didn't match).
              if (excluded.has(normalised)) {
                console.log(`[SelectAddress] Skipping excluded card: "${card.text.replace(/\s+/g, " ").trim().slice(0, 80)}"`);
                continue;
              }
              let score = 0;

              // Name match (highest weight — company names are unique per address)
              if (targetName && (cardText.includes(targetName) || cardName.includes(targetName))) {
                score += 4;
              }

              // City match
              if (targetCity && cardText.includes(targetCity)) {
                score += 2;
              }

              // Locality keyword match
              if (targetLocality) {
                const localityParts = targetLocality.split(/\s+/).filter((p) => p.length > 3);
                for (const part of localityParts) {
                  if (cardText.includes(part)) {
                    score += 1;
                    break;
                  }
                }
              }

              console.log(
                `[SelectAddress] Card score=${score} name="${targetName}" city="${targetCity}" | text="${card.text.replace(
                  /\s+/g,
                  " "
                ).trim().slice(0, 100)}"`
              );

              if (score > bestScore) {
                bestScore = score;
                bestCard = card;
              }
            }

            if (!bestCard || bestScore < 1) {
              console.log(`[SelectAddress] No matching address card found (bestScore=${bestScore})`);
              // Log all card texts for debugging
              for (const card of cards) {
                console.log(`[SelectAddress]   candidate: "${card.text.replace(/\s+/g, " ").trim().slice(0, 120)}"`);
              }
              return null;
            }

            console.log(`[SelectAddress] Best match: score=${bestScore} text="${bestCard.text.replace(/\s+/g, " ").trim().slice(0, 100)}"`);

            // Walk up to find the most appropriate clickable ancestor
            let clickableEl: HTMLElement | null = bestCard.el;
            let el: HTMLElement | null = bestCard.el;
            while (el && el !== document.body) {
              const style = el.getAttribute("style") || "";
              const cls = el.className || "";
              if ((style.includes("cursor") || cls.includes("cDeXU9") || cls.includes("css-g5y9jx")) && style.includes("cursor")) {
                clickableEl = el;
                break;
              }
              el = el.parentElement;
            }

            const rect = clickableEl!.getBoundingClientRect();
            return {
              x: rect.left + rect.width / 2,
              y: rect.top + rect.height / 2,
              cardText: bestCard.text.replace(/\s+/g, " ").trim().slice(0, 120),
              score: bestScore,
            };
          },
          address,
          excludeTexts
        );
      } catch (err) {
        const msg = (err as Error).message;
        if (msg.includes("detached") || msg.includes("Frame")) {
          console.log(`[SelectAddress] Frame detached (attempt ${attempt + 1}/3), retrying...`);
          await sleep(500);
          continue;
        }
        console.log(`[SelectAddress] Evaluate error: ${msg}`);
        return false;
      }
    }

    if (!result) {
      console.log("[SelectAddress] Could not locate any address card — returning false");
      return false;
    }

    console.log(`[SelectAddress] Best match (score=${result.score}): "${result.cardText}"`);
    console.log(`[SelectAddress] Clicking at (${result.x.toFixed(0)}, ${result.y.toFixed(0)})`);

    try {
      await this.page.mouse.move(result.x, result.y);
      await sleep(100);
      await this.page.mouse.click(result.x, result.y);
      console.log("[SelectAddress] Mouse click succeeded");
      return true;
    } catch (err) {
      console.log(`[SelectAddress] Mouse click failed: ${(err as Error).message}`);
      try {
        await this.evaluate(
          (coords: { x: number; y: number }) => {
            const el = document.elementFromPoint(coords.x, coords.y);
            if (el) el.dispatchEvent(new MouseEvent("click", { bubbles: true, clientX: coords.x, clientY: coords.y }));
          },
          { x: result.x, y: result.y }
        );
        console.log("[SelectAddress] Clicked via elementFromPoint");
        return true;
      } catch (e) {
        console.log(`[SelectAddress] All click methods failed: ${(e as Error).message}`);
        return false;
      }
    }
  }

  /**
   * Reads the currently-displayed "Deliver to: …" delivery card on the
   * checkout / order-summary page. Returns null if the card isn't present
   * yet (e.g. modal still up, page mid-render).
   */
  private async readDeliveryCard(): Promise<{ fullText: string; mobile: string | null } | null> {
    try {
      return await this.evaluate(() => {
        const divs = Array.from(document.querySelectorAll("div"));
        let bestText = "";
        for (const d of divs) {
          const txt = (d.innerText || "").replace(/\s+/g, " ").trim();
          if (txt.startsWith("Deliver to:")) {
            // Pick the SHORTEST "Deliver to:" container — the outermost
            // contains the whole page; we want the inner card.
            if (!bestText || txt.length < bestText.length) {
              bestText = txt;
            }
          }
        }
        if (!bestText) return null;
        // Mobile: a 10-digit number after a non-digit boundary (so we don't
        // pick up "388421" from a 6-digit pincode). Look for 10-digit
        // sequences and take the LAST one — Flipkart usually shows the
        // mobile after the address.
        const matches = bestText.match(/(?:^|\D)(\d{10})(?!\d)/g) || [];
        let mobile: string | null = null;
        if (matches.length > 0) {
          const last = matches[matches.length - 1];
          const m = last.match(/(\d{10})/);
          mobile = m ? m[1] : null;
        }
        return { fullText: bestText, mobile };
      });
    } catch {
      return null;
    }
  }

  /** Strip non-digits and keep the last 10. Empty string if input is falsy. */
  private normaliseMobile(raw: string | undefined | null): string {
    if (!raw) return "";
    const digits = String(raw).replace(/\D/g, "");
    return digits.slice(-10);
  }

  /**
   * Ensures the displayed delivery card on the checkout page (or order
   * summary) shows BOTH the right pincode + city AND the right mobile
   * number. If two saved addresses share city + pincode but differ in
   * mobile (the documented duplicate-address case for accounts with
   * a per-account number), this loop opens the saved-address picker,
   * skips the previously-tried card, and selects the next candidate.
   *
   * Returns silently on success. Throws with a clear message after
   * `maxAttempts` failed attempts so the iteration fails fast instead
   * of placing the order with the wrong number.
   */
  private async ensureMobileMatchesOnCheckout(
    address: AddressDetails,
    maxAttempts = 4
  ): Promise<void> {
    const targetMobile = this.normaliseMobile(address.mobile);
    if (!targetMobile) {
      // No mobile to verify — pre-flight didn't run, so leave the existing
      // city/pincode result alone.
      return;
    }

    const tried: string[] = [];

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      // Give the card a moment to render after any prior selection.
      await sleep(800);
      const card = await this.readDeliveryCard();
      if (!card) {
        console.log(`[ensureMobileMatch] Could not read displayed delivery card on attempt ${attempt} — skipping further verification`);
        return;
      }

      const displayedMobile = this.normaliseMobile(card.mobile);
      const cardSnippet = card.fullText.slice(0, 120);
      console.log(
        `[ensureMobileMatch] attempt ${attempt}/${maxAttempts}: target=${targetMobile} displayed=${displayedMobile || "<none>"} card="${cardSnippet}"`
      );

      if (displayedMobile === targetMobile) {
        console.log(`[ensureMobileMatch] Mobile matches — delivery address verified`);
        return;
      }

      // Mark this card as already-tried so the picker won't return it again.
      tried.push(card.fullText);

      if (attempt === maxAttempts) {
        throw new Error(
          `Could not find a saved address with the expected mobile number (${targetMobile}) ` +
          `on the checkout page after ${maxAttempts} attempts. ` +
          `The Flipkart account may have multiple saved addresses sharing city + pincode but with ` +
          `different mobiles, or the right one wasn't created during pre-flight.`
        );
      }

      // Open the picker and pick the next candidate that hasn't been tried.
      console.log(`[ensureMobileMatch] mismatch — clicking Change to try next candidate (excluding ${tried.length} already-tried)`);
      await this.clickAddressChangeButton();

      // Wait for the saved-address modal to render.
      let modalLoaded = false;
      for (let i = 0; i < 8 && !modalLoaded; i++) {
        await sleep(300);
        try {
          modalLoaded = await this.evaluate(() => {
            const body = document.body?.innerText || "";
            return (
              body.includes("Select Delivery") ||
              body.includes("Saved Address") ||
              body.includes("Delivery Address") ||
              body.includes("ADD ADDRESSES")
            );
          });
        } catch { /* frame detach — keep waiting */ }
      }
      if (!modalLoaded) {
        console.log(`[ensureMobileMatch] address modal didn't open on attempt ${attempt} — aborting loop`);
        return;
      }

      const picked = await this.selectAddressFromList(address, tried);
      if (!picked) {
        throw new Error(
          `No remaining saved-address candidates to try with the expected mobile (${targetMobile}). ` +
          `Tried ${tried.length} candidate(s) on the checkout picker.`
        );
      }

      // Wait for the modal to close so the next attempt's readDeliveryCard
      // sees the new card, not the picker overlay.
      let modalClosed = false;
      for (let i = 0; i < 10 && !modalClosed; i++) {
        await sleep(400);
        try {
          modalClosed = await this.evaluate(() => {
            const body = document.body?.innerText || "";
            return !(
              body.includes("Select Delivery Address") ||
              body.includes("Edit Address") ||
              body.includes("ADD ADDRESSES")
            );
          });
        } catch { modalClosed = true; }
      }
    }
  }


  private async fillAddressForm(
    page: Page,
    address: AddressDetails
  ): Promise<void> {
    const pincodeToUse = address.checkoutPincode || address.pincode;

    // Fill using name attributes — Flipkart uses standard HTML name attributes on inputs
    await this.fillByName(page, "name", address.companyName || address.name, "Name");
    await this.fillByName(page, "phone", address.mobile, "Mobile");
    await this.fillByName(page, "pincode", pincodeToUse, "Pincode");
    await this.fillByName(page, "addressLine2", address.locality, "Locality");
    await this.fillTextareaByName(page, "addressLine1", address.addressLine1, "Address");
    await this.fillByName(page, "city", address.city, "City");

    // Select state from dropdown
    let stateSet = false;
    for (let attempt = 0; attempt < 5 && !stateSet; attempt++) {
      const result = await this.evaluate((stateVal: string) => {
        const select = document.querySelector('select[name="state"]') as HTMLSelectElement | null;
        if (!select) return "not_found";
        const options = Array.from(select.querySelectorAll("option"));
        const found = options.find((opt) => opt.value === stateVal);
        if (found) {
          select.value = stateVal;
          select.dispatchEvent(new Event("change", { bubbles: true }));
          return "found";
        }
        return "not_found";
      }, address.state);
      if (result === "found") {
        console.log(`Selected state: ${address.state}`);
        stateSet = true;
        await sleep(300);
      } else {
        console.log(`State "${address.state}" not found in dropdown (attempt ${attempt + 1}/5)`);
        await sleep(300);
      }
    }

    // Click Home or Work radio — radio inputs are readonly, so click the <label> instead
    const radioId = address.addressType === "Home" ? "HOME" : "WORK";
    let typeSet = false;
    for (let attempt = 0; attempt < 5 && !typeSet; attempt++) {
      const result = await this.evaluate((id: string) => {
        // Strategy 1: Click the <label> for the radio (most reliable since input is readonly)
        const label = document.querySelector(`label[for="${id}"]`) as HTMLLabelElement | null;
        if (label) {
          label.scrollIntoView({ block: "center" });
          label.click();
          return "label";
        }
        // Strategy 2: Click the radio input directly + set checked property
        const radio = document.getElementById(id) as HTMLInputElement | null;
        if (radio) {
          radio.checked = true;
          radio.dispatchEvent(new Event("change", { bubbles: true }));
          radio.dispatchEvent(new Event("click", { bubbles: true }));
          return "radio";
        }
        // Strategy 3: Find by text content "Home" or "Work"
        const targetText = id === "HOME" ? "home" : "work";
        const allLabels = Array.from(document.querySelectorAll("label"));
        for (const l of allLabels) {
          if ((l.textContent || "").trim().toLowerCase() === targetText) {
            l.scrollIntoView({ block: "center" });
            l.click();
            return "text";
          }
        }
        return "not_found";
      }, radioId);
      if (result !== "not_found") {
        console.log(`Selected address type: ${address.addressType} (via ${result})`);
        typeSet = true;
        await sleep(300);
      } else {
        await sleep(500);
      }
    }
  }

  /** Type into an input by its name attribute */
  private async fillByName(
    page: Page,
    name: string,
    value: string,
    label: string
  ): Promise<void> {
    let found = false;
    for (let attempt = 0; attempt < 5 && !found; attempt++) {
      const result = await this.evaluate(
        (n: string, val: string) => {
          const input = document.querySelector(`input[name="${n}"]`) as HTMLInputElement | null;
          if (!input) return "not_found";
          input.focus();
          // Clear existing value
          input.value = "";
          const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")?.set;
          if (setter) setter.call(input, val);
          else input.value = val;
          input.dispatchEvent(new Event("input", { bubbles: true }));
          input.dispatchEvent(new Event("change", { bubbles: true }));
          return "found";
        },
        name,
        value
      );
      if (result === "found") {
        console.log(`Filled ${label}`);
        found = true;
        await sleep(300);
      } else {
        console.log(`${label} (name="${name}") not found (attempt ${attempt + 1}/5)`);
        await sleep(300);
      }
    }
  }

  /** Type into a textarea by its name attribute */
  private async fillTextareaByName(
    page: Page,
    name: string,
    value: string,
    label: string
  ): Promise<void> {
    let found = false;
    for (let attempt = 0; attempt < 5 && !found; attempt++) {
      const result = await this.evaluate(
        (n: string, val: string) => {
          const textarea = document.querySelector(`textarea[name="${n}"]`) as HTMLTextAreaElement | null;
          if (!textarea) return "not_found";
          textarea.focus();
          textarea.value = "";
          const setter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, "value")?.set;
          if (setter) setter.call(textarea, val);
          else textarea.value = val;
          textarea.dispatchEvent(new Event("input", { bubbles: true }));
          textarea.dispatchEvent(new Event("change", { bubbles: true }));
          return "found";
        },
        name,
        value
      );
      if (result === "found") {
        console.log(`Filled ${label}`);
        found = true;
        await sleep(300);
      } else {
        console.log(`${label} (textarea[name="${name}"]) not found (attempt ${attempt + 1}/5)`);
        await sleep(300);
      }
    }
  }

  private async fillGstForm(gstNumber: string, companyName: string): Promise<void> {
    console.log("Filling GST form...");

    let filled = false;
    for (let attempt = 0; attempt < 5 && !filled; attempt++) {
      const result = await this.evaluate((gst: string, company: string) => {
        // Find GST number input: maxlength=15
        const gstEl = document.querySelector('input[maxlength="15"]') as HTMLInputElement | null;
        // Find company name input: maxlength=60
        const companyEl = document.querySelector('input[maxlength="60"]') as HTMLInputElement | null;

        const filledGst = gstEl !== null;
        const filledCompany = companyEl !== null;

        if (gstEl) {
          gstEl.focus();
          gstEl.value = "";
          const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")?.set;
          if (setter) setter.call(gstEl, gst);
          else gstEl.value = gst;
          gstEl.dispatchEvent(new Event("input", { bubbles: true }));
          gstEl.dispatchEvent(new Event("change", { bubbles: true }));
        }
        if (companyEl) {
          companyEl.focus();
          companyEl.value = "";
          const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")?.set;
          if (setter) setter.call(companyEl, company);
          else companyEl.value = company;
          companyEl.dispatchEvent(new Event("input", { bubbles: true }));
          companyEl.dispatchEvent(new Event("change", { bubbles: true }));
        }

        return { filledGst, filledCompany };
      }, gstNumber, companyName);

      if (result.filledGst) {
        console.log(`Entered GST number: ${gstNumber.slice(0, 2)}***${gstNumber.slice(-2)}`);
      } else {
        console.log(`WARNING: GST input [maxlength="15"] not found (attempt ${attempt + 1}/5)`);
      }
      if (result.filledCompany) {
        console.log(`Entered company name: ${companyName}`);
      } else {
        console.log(`WARNING: Company input [maxlength="60"] not found (attempt ${attempt + 1}/5)`);
      }

      if (result.filledGst && result.filledCompany) {
        filled = true;
      } else {
        await sleep(300);
      }
    }

    await sleep(500);
  }

  /**
   * Determine if the delivery address on the page matches the saved address.
   * Uses pincode + city as the primary anchor (most reliable on Flipkart).
   * Falls back to the overall text content search for partial matches.
   */
  private scoreAddressMatch(text: string, address: AddressDetails): number {
    if (!text) return 0;

    const lowerText = text.toLowerCase();
    const pincode = (address.checkoutPincode || address.pincode).trim();
    const city = address.city.trim().toLowerCase();
    const locality = address.locality.trim().toLowerCase();
    const name = address.name.trim().toLowerCase();
    const addrLine = address.addressLine1.trim().toLowerCase();

    let score = 0;

    // Pincode is the most reliable — if it matches, that's strong confirmation
    if (pincode.length === 6 && lowerText.includes(pincode)) {
      score += 2;
    }

    // City name match
    if (city.length >= 3 && lowerText.includes(city)) {
      score += 1;
    }

    // Locality — use first 2 significant words
    const localityKey = locality.split(/\s+/).filter((w) => w.length > 2).slice(0, 2).join(" ");
    if (localityKey.length > 0 && lowerText.includes(localityKey)) {
      score += 1;
    }

    // Name — flipkart may show just first name or full name
    if (name.length >= 3) {
      const nameParts = name.split(/\s+/);
      const nameMatch = nameParts.some((part) => part.length > 2 && lowerText.includes(part));
      if (nameMatch) score += 1;
    }

    // Address line — first 2 significant words
    const addrKey = addrLine.split(/\s+/).filter((w) => w.length > 3).slice(0, 2).join(" ");
    if (addrKey.length > 0 && lowerText.includes(addrKey)) {
      score += 1;
    }

    return score;
  }

}
