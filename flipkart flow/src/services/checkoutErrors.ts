export type CheckoutFailureCode =
  | "LOGIN_REQUIRED"
  | "OUT_OF_STOCK"
  | "PRODUCT_UNAVAILABLE"
  | "COMING_SOON"
  | "ITEM_NOT_DELIVERABLE"
  | "QUANTITY_LIMIT"
  | "PRICE_CHANGED"
  | "ADD_TO_CART_FAILED"
  | "ADDRESS_FAILED"
  | "GST_NOT_FOUND"
  | "GST_INVALID"
  | "PAYMENT_PAGE_NOT_REACHED"
  | "FLIPKART_ERROR"
  | "MOBILE_MISMATCH"
  | "AUTOMATION_ERROR";

/** Plain-language explanation of each reason, shown in the failure report. */
export const REASONS: Record<CheckoutFailureCode, string> = {
  LOGIN_REQUIRED: "Flipkart session is logged out — log in again with OTP",
  OUT_OF_STOCK: "Product is out of stock",
  PRODUCT_UNAVAILABLE: "Product is currently unavailable / cannot be bought",
  COMING_SOON: "Product is not on sale yet (Coming Soon / Notify Me)",
  ITEM_NOT_DELIVERABLE: "Product cannot be delivered to the delivery pincode",
  QUANTITY_LIMIT: "Requested quantity is more than Flipkart allows for this product",
  PRICE_CHANGED: "Price changed during checkout",
  ADD_TO_CART_FAILED: "Product could not be added to the cart",
  ADDRESS_FAILED: "Delivery address could not be added or selected",
  GST_NOT_FOUND: "GST Invoice option is not available on checkout",
  GST_INVALID: "Flipkart rejected the GST number",
  PAYMENT_PAGE_NOT_REACHED: "Checkout did not open the payment page",
  FLIPKART_ERROR: "Flipkart showed an error page (Something went wrong)",
  MOBILE_MISMATCH: "Logged into the wrong Flipkart account",
  AUTOMATION_ERROR: "Script could not find/operate a page element (Flipkart layout may have changed)",
};

/** A Flipkart-side reason the order cannot go through — not a selector/automation bug. */
export class CheckoutFailure extends Error {
  constructor(
    public code: CheckoutFailureCode,
    message: string
  ) {
    super(message);
    this.name = "CheckoutFailure";
  }
}

/** e.g. "You can only purchase 14 units of … in a single order", "Only 2 units allowed per customer". */
export const QTY_LIMIT_RE =
  /can only (purchase|buy|order) \d+ (unit|item)s?|only \d+ (unit|item)s?\b.*(allowed|per (customer|order))|max(imum)? (quantity|qty|units?) (is|of|allowed)|quantity limit|can(not|'t) (buy|order) more than/i;

const shortLines = (text: string, max = 160) =>
  (text || "")
    .replace(/ /g, " ")
    .split(/\n+/)
    .map((l) => l.replace(/\s+/g, " ").trim())
    .filter((l) => l && l.length <= max);

/**
 * Live check, run while the flow waits on pages. Matches short standalone
 * lines only, so a recommendation tile or long paragraph never trips it.
 */
export function classifyPageText(text: string, pincode = ""): CheckoutFailure | null {
  const pin = String(pincode || "").replace(/\D/g, "").slice(-6);
  for (const line of shortLines(text, 120)) {
    const oosForPin = line.match(/currently out of stock for\s+(\d{6})/i);
    if (oosForPin && (!pin || oosForPin[1] === pin)) {
      return new CheckoutFailure("OUT_OF_STOCK", line);
    }
    if (/^(this item is )?currently out of stock\.?$/i.test(line) || /^sold out$/i.test(line)) {
      return new CheckoutFailure("OUT_OF_STOCK", line);
    }
    if (/^(this item is )?currently unavailable\.?$/i.test(line)) {
      return new CheckoutFailure("PRODUCT_UNAVAILABLE", line);
    }
    if (/^(not deliverable (in|at|to) your (location|area|pincode))\.?$/i.test(line)) {
      return new CheckoutFailure("ITEM_NOT_DELIVERABLE", pin ? `${line} (${pin})` : line);
    }
    if (/no seller(s)? (ships|deliver)/i.test(line)) {
      return new CheckoutFailure("ITEM_NOT_DELIVERABLE", pin ? `${line} (${pin})` : line);
    }
  }
  return null;
}

/**
 * Post-failure diagnosis: the flow already stopped, so scan the whole page
 * for anything Flipkart says about why. Broader than classifyPageText.
 * Order matters — the most specific reasons come first.
 */
export function diagnosePageText(
  text: string,
  pincode = "",
  opts: { purchasable?: boolean } = {}
): CheckoutFailure | null {
  // A quantity cap explains any stock/deliverability message Flipkart shows next to it.
  const qtyCap = shortLines(text).find((l) => QTY_LIMIT_RE.test(l));
  if (qtyCap) return new CheckoutFailure("QUANTITY_LIMIT", qtyCap);
  const live = classifyPageText(text, pincode);
  if (live) return live;
  // Product page still offers Buy now / Add to cart → stock words belong to other variants/tiles.
  const stockCodes: CheckoutFailureCode[] = ["OUT_OF_STOCK", "PRODUCT_UNAVAILABLE", "COMING_SOON"];
  const pin = String(pincode || "").replace(/\D/g, "").slice(-6);
  const rules: Array<[CheckoutFailureCode, RegExp]> = [
    ["LOGIN_REQUIRED", /^(login|log in)( or sign ?up)?$|enter (your )?mobile number.*otp|request otp/i],
    ["QUANTITY_LIMIT", QTY_LIMIT_RE],
    ["GST_INVALID", /(invalid|incorrect|enter (a )?valid) (gstin|gst)|gstin (is )?(invalid|not valid)/i],
    ["PRICE_CHANGED", /price (has )?(changed|increased|decreased|dropped)/i],
    ["COMING_SOON", /^coming soon$|^notify me$/i],
    ["OUT_OF_STOCK", /\bout of stock\b|^sold out$/i],
    ["ITEM_NOT_DELIVERABLE", /not deliverable|cannot be delivered|can'?t be delivered|doesn'?t deliver|does not deliver|delivery (is )?not available|not serviceable|unserviceable/i],
    ["PRODUCT_UNAVAILABLE", /currently unavailable|no longer available|item(s)? (is |are )?unavailable|remove unavailable items|missing cart items/i],
    ["FLIPKART_ERROR", /^something went wrong!?$|^E0\d\d$/i],
  ];
  const lines = shortLines(text);
  for (const [code, re] of rules) {
    if (opts.purchasable && stockCodes.includes(code)) continue;
    const hit = lines.find((l) => re.test(l));
    if (hit) {
      const withPin = code === "ITEM_NOT_DELIVERABLE" && pin && !hit.includes(pin) ? `${hit} (${pin})` : hit;
      return new CheckoutFailure(code, withPin);
    }
  }
  return null;
}
