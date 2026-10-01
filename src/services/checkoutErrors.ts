export type CheckoutErrorSource = "PLATFORM" | "BANK" | "USER";

export type CheckoutErrorCode =
  | "PRODUCT_UNAVAILABLE"
  | "PRODUCT_NOT_SERVICEABLE"
  | "ITEM_NOT_DELIVERABLE"
  | "MAX_UNITS_REACHED"
  | "ADD_TO_CART_FAILED"
  | "EMPTY_CART_FAILED"
  | "ADDRESS_SELECTION_FAILED"
  | "ADDRESS_REMOVE_FAILED"
  | "GST_NOT_APPLICABLE"
  | "GST_NOT_FOUND"
  | "GST_SELECT_FAILED"
  | "UNABLE_TO_PLACE_ORDER"
  | "SLA_EXCEEDED"
  | "CART_AMOUNT_LIMIT"
  | "SESSION_EXPIRED"
  | "PINCODE_NOT_SET"
  | "COD_UNAVAILABLE"
  | "INSUFFICIENT_BALANCE"
  | "CARD_AUTH_FAILED"
  | "OTP_TIMEOUT"
  | "OTP_NOT_FOUND"
  | "UNKNOWN";

export interface CheckoutErrorDef {
  code: CheckoutErrorCode;
  display: string;
  source: CheckoutErrorSource;
  failedStep: string;
  stageDisplay: string;
  /**
   * A verdict about THIS platform ID on THIS product. Running the same ID again
   * shows the same page, so the job is not re-queued; the next ID in the batch is
   * the retry. Nothing here ever stops the batch — a product that is out of stock
   * or undeliverable for one account may still go through on another, and the
   * operator wants every selected ID attempted, each reporting its own reason.
   */
  noRetry?: boolean;
}

export const CHECKOUT_ERRORS: Record<CheckoutErrorCode, CheckoutErrorDef> = {
  PRODUCT_UNAVAILABLE: {
    code: "PRODUCT_UNAVAILABLE",
    display: "Product unavailable",
    source: "PLATFORM",
    failedStep: "product",
    stageDisplay: "Adding to cart",
    noRetry: true,
  },
  PRODUCT_NOT_SERVICEABLE: {
    code: "PRODUCT_NOT_SERVICEABLE",
    display: "Product not serviceable",
    source: "PLATFORM",
    failedStep: "out_of_stock_pincode",
    stageDisplay: "Adding to cart",
    noRetry: true,
  },
  ITEM_NOT_DELIVERABLE: {
    code: "ITEM_NOT_DELIVERABLE",
    display: "Item not deliverable to your address",
    source: "PLATFORM",
    failedStep: "product",
    stageDisplay: "Adding to cart",
    noRetry: true,
  },
  MAX_UNITS_REACHED: {
    code: "MAX_UNITS_REACHED",
    display: "Maximum units limit reached",
    source: "PLATFORM",
    failedStep: "product",
    stageDisplay: "Adding to cart",
    // The cap is per account: this ID has had its allowance of this product.
    noRetry: true,
  },
  ADD_TO_CART_FAILED: {
    code: "ADD_TO_CART_FAILED",
    display: "Adding to cart failed",
    source: "PLATFORM",
    failedStep: "product",
    stageDisplay: "Adding to cart",
  },
  EMPTY_CART_FAILED: {
    code: "EMPTY_CART_FAILED",
    display: "Empty cart failed",
    source: "PLATFORM",
    failedStep: "cart",
    stageDisplay: "Emptying Cart",
  },
  ADDRESS_SELECTION_FAILED: {
    code: "ADDRESS_SELECTION_FAILED",
    display: "Address selection failed",
    source: "PLATFORM",
    failedStep: "address",
    stageDisplay: "Managing Addresses",
  },
  ADDRESS_REMOVE_FAILED: {
    code: "ADDRESS_REMOVE_FAILED",
    display: "Managing addresses failed",
    source: "PLATFORM",
    failedStep: "address",
    stageDisplay: "Managing Addresses",
  },
  GST_NOT_APPLICABLE: {
    code: "GST_NOT_APPLICABLE",
    display: "GST not applicable on this product",
    source: "PLATFORM",
    failedStep: "GST",
    stageDisplay: "Validating GST",
  },
  GST_NOT_FOUND: {
    code: "GST_NOT_FOUND",
    display: "GST is not found on page",
    source: "PLATFORM",
    failedStep: "GST",
    stageDisplay: "Validating GST",
  },
  GST_SELECT_FAILED: {
    code: "GST_SELECT_FAILED",
    display: "Validating GST failed",
    source: "PLATFORM",
    failedStep: "GST",
    stageDisplay: "Validating GST",
  },
  UNABLE_TO_PLACE_ORDER: {
    code: "UNABLE_TO_PLACE_ORDER",
    display: "Unable to place order",
    source: "PLATFORM",
    failedStep: "product",
    stageDisplay: "Unable to place order",
  },
  SLA_EXCEEDED: {
    code: "SLA_EXCEEDED",
    display: "Delivery SLA exceeded",
    source: "PLATFORM",
    failedStep: "product",
    stageDisplay: "Adding to cart",
  },
  CART_AMOUNT_LIMIT: {
    code: "CART_AMOUNT_LIMIT",
    display: "Cart amount over limit",
    source: "PLATFORM",
    failedStep: "product",
    stageDisplay: "Adding to cart",
  },
  SESSION_EXPIRED: {
    code: "SESSION_EXPIRED",
    display: "Session expired",
    source: "PLATFORM",
    failedStep: "session",
    stageDisplay: "Session",
  },
  /**
   * Flipkart is asking for a delivery pincode — it has not judged the product yet.
   *
   * Its own code, because it used to be ITEM_NOT_DELIVERABLE. Back when a
   * deliverability verdict stopped the whole batch, that turned "we have not told
   * Flipkart the pincode" into "this product cannot be delivered here" and ended
   * a run with purchased 0 on 2026-09-30. Batches are no longer stopped by any
   * code, but the distinction still matters: this is a page state a retry fixes,
   * so the same ID IS re-queued (noRetry absent), unlike a real verdict.
   */
  PINCODE_NOT_SET: {
    code: "PINCODE_NOT_SET",
    display: "Delivery pincode was not set on the page",
    source: "PLATFORM",
    failedStep: "pincode",
    stageDisplay: "Product",
  },
  /**
   * The payments page lists "Cash on Delivery — Unavailable". Flipkart decides
   * this per account, product and address, so with payment mode COD it is a
   * verdict on this platform ID: recorded on this job, not retried on the same
   * ID, and the batch goes on — another ID may well be offered COD for the same
   * cart. Was reported as UNABLE_TO_PLACE_ORDER, which hid the actual reason.
   */
  COD_UNAVAILABLE: {
    code: "COD_UNAVAILABLE",
    display: "Cash on Delivery unavailable for this order",
    source: "PLATFORM",
    failedStep: "payment",
    stageDisplay: "Processing Payment",
    noRetry: true,
  },
  INSUFFICIENT_BALANCE: {
    code: "INSUFFICIENT_BALANCE",
    display: "Insufficient bank account balance",
    source: "BANK",
    failedStep: "payment",
    stageDisplay: "Processing Payment",
  },
  CARD_AUTH_FAILED: {
    code: "CARD_AUTH_FAILED",
    display: "Card authentication failed",
    source: "BANK",
    failedStep: "payment",
    stageDisplay: "Order Confirmation",
  },
  OTP_TIMEOUT: {
    code: "OTP_TIMEOUT",
    display: "Timeout waiting for OTP",
    source: "BANK",
    failedStep: "payment",
    stageDisplay: "Processing Payment",
  },
  OTP_NOT_FOUND: {
    code: "OTP_NOT_FOUND",
    display: "OTP not found",
    source: "BANK",
    failedStep: "payment",
    stageDisplay: "Processing Payment",
  },
  UNKNOWN: {
    code: "UNKNOWN",
    display: "Order failed",
    source: "PLATFORM",
    failedStep: "unknown",
    stageDisplay: "Order failed",
  },
};

export class CheckoutFailure extends Error {
  readonly code: CheckoutErrorCode;
  readonly display: string;
  readonly source: CheckoutErrorSource;
  readonly details: string;
  readonly failedStep: string;
  readonly stageDisplay: string;
  readonly noRetry: boolean;

  constructor(code: CheckoutErrorCode, details: string, overrides?: Partial<CheckoutErrorDef>) {
    const def = { ...CHECKOUT_ERRORS[code], ...overrides };
    super(details || def.display);
    this.name = "CheckoutFailure";
    this.code = def.code;
    this.display = def.display;
    this.source = def.source;
    this.details = details || def.display;
    this.failedStep = def.failedStep;
    this.stageDisplay = def.stageDisplay;
    this.noRetry = Boolean(def.noRetry);
  }
}

function lineMatching(text: string, re: RegExp): string {
  const lines = text
    .replace(/\u00a0/g, " ")
    .split(/\n+/)
    .map((l) => l.replace(/\s+/g, " ").trim())
    .filter(Boolean);
  return lines.find((l) => re.test(l) && l.length < 180) || text.replace(/\s+/g, " ").trim().slice(0, 160);
}

/** Scan Flipkart page text for a known blocker. */
export function classifyPageText(
  raw: string,
  pincode = "",
  opts: { purchasable?: boolean } = {}
): CheckoutFailure | null {
  const text = raw.replace(/\u00a0/g, " ");
  const compact = text.replace(/\s+/g, " ").trim();
  const pin = pincode.replace(/\D/g, "").slice(-6);

  /**
   * Does this page still offer to sell us the thing?
   *
   * A Flipkart PDP is full of other people's stock: colour variants, "similar
   * products", recently-viewed tiles. Any of them can say "Out of stock" while the
   * product we actually want is perfectly buyable \u2014 and a plain text scan then
   * reports PRODUCT_UNAVAILABLE for a page with a live Buy Now button on it.
   *
   * So when a top-level "Buy now" / "Add to cart" is present, stock wording is
   * treated as belonging to something else on the page. Auto-detected rather than
   * passed in, because there are many call sites and one that forgets gets a
   * confident wrong answer; a caller can still force it either way.
   */
  const purchasable = opts.purchasable ?? /^\s*(buy now|add to cart)\b/im.test(text);
  const hasNotify = !purchasable && /\bNotify Me\b/i.test(compact);

  if (/you'?ve reached the maximum units allowed/i.test(compact)) {
    return new CheckoutFailure("MAX_UNITS_REACHED", lineMatching(text, /maximum units/i));
  }
  if (/GST not applicable/i.test(compact)) {
    return new CheckoutFailure("GST_NOT_APPLICABLE", lineMatching(text, /GST not applicable/i));
  }
  if (/unable to place (your )?order/i.test(compact)) {
    return new CheckoutFailure("UNABLE_TO_PLACE_ORDER", lineMatching(text, /unable to place/i));
  }
  if (
    /enter pincode to see if the product is in stock/i.test(compact) ||
    /enter delivery pincode/i.test(compact)
  ) {
    // PINCODE_NOT_SET, not ITEM_NOT_DELIVERABLE: Flipkart is ASKING for a pincode,
    // which means it has not judged deliverability at all. Classifying it as
    // "not deliverable" filtered the whole batch over a page state a retry fixes.
    return new CheckoutFailure(
      "PINCODE_NOT_SET",
      pin
        ? `Flipkart is still asking for a delivery pincode (${pin} not applied yet)`
        : "Flipkart is still asking for a delivery pincode"
    );
  }
  if (
    /select location/i.test(compact) &&
    /select delivery address/i.test(compact) &&
    /enter location manually|use my current location/i.test(compact)
  ) {
    return new CheckoutFailure(
      "ITEM_NOT_DELIVERABLE",
      pin ? `This product is not available on this pin (${pin})` : "This product is not available on this pin"
    );
  }
  if (
    /not deliverable to (your )?address/i.test(compact) ||
    /not deliverable in your (location|area)/i.test(compact) ||
    /not deliverable at (this|your) (location|pincode|address)/i.test(compact) ||
    /currently not deliverable/i.test(compact) ||
    /cannot (be )?deliver(ed)? to (this|your)/i.test(compact) ||
    /we (do not|don't) deliver/i.test(compact) ||
    /delivery not available/i.test(compact)
  ) {
    return new CheckoutFailure(
      "ITEM_NOT_DELIVERABLE",
      pin ? `This product is not available on this pin (${pin})` : lineMatching(text, /not deliverable|cannot (be )?deliver|don't deliver|do not deliver|delivery not available/i)
    );
  }
  if (/not deliverable/i.test(compact) && /change address/i.test(compact)) {
    return new CheckoutFailure(
      "ITEM_NOT_DELIVERABLE",
      pin ? `This product is not available on this pin (${pin})` : lineMatching(text, /not deliverable/i)
    );
  }
  // A seller that will not ship here is a DELIVERY problem, not a stock one. The
  // generic rules above only match "…to your address"; Flipkart also writes the
  // pincode, or names the seller.
  if (
    /seller does ?n[o']t deliver/i.test(compact) ||
    /no sellers? (deliver|ships?)/i.test(compact) ||
    (/not deliverable/i.test(compact) && /\d{6}/.test(compact))
  ) {
    return new CheckoutFailure(
      "ITEM_NOT_DELIVERABLE",
      lineMatching(text, /not deliverable|does ?n[o']t deliver|no sellers? (deliver|ships?)/i)
    );
  }
  const oos = compact.match(
    /(?:currently\s+)?(?:out of stock|not serviceable|not available|currently unavailable)\s+(?:for|to)\s+(\d{6})/i
  );
  if (oos) {
    return new CheckoutFailure("PRODUCT_NOT_SERVICEABLE", lineMatching(text, /out of stock|not serviceable|not available/i));
  }
  if (!purchasable && pin && /out of stock/i.test(compact) && compact.includes(pin)) {
    return new CheckoutFailure("PRODUCT_NOT_SERVICEABLE", `Currently out of stock for ${pin}`);
  }
  // A sold-out PDP that names no pincode. Guarded by `purchasable`, so a live
  // page whose colour variants are out of stock is not caught by it — that is
  // the whole reason the guard exists. Checked before Notify Me so the failure
  // carries Flipkart's own words ("Out of stock") rather than ours: a real
  // sold-out iPhone 16 page says exactly "Out of stock" beside the variant, with
  // a Notify Me button under it.
  if (!purchasable && /\bsold out\b|\bout of stock\b|\bcurrently unavailable\b/i.test(compact)) {
    return new CheckoutFailure(
      "PRODUCT_UNAVAILABLE",
      lineMatching(text, /sold out|out of stock|currently unavailable/i)
    );
  }
  if (hasNotify) {
    return new CheckoutFailure("PRODUCT_UNAVAILABLE", "Notify Me — product is not available to purchase");
  }
  if (/insufficient (funds|balance)|insufficient bank/i.test(compact)) {
    return new CheckoutFailure("INSUFFICIENT_BALANCE", lineMatching(text, /insufficient/i));
  }
  if (/invalid card|card authentication failed|invalid.*expiry/i.test(compact)) {
    return new CheckoutFailure("CARD_AUTH_FAILED", lineMatching(text, /card|expiry|authentication/i));
  }
  return null;
}

/** Map a thrown message (and optional page text) to a catalog entry. */
export function classifyThrownMessage(message: string, pageText = ""): CheckoutFailure {
  const fromPage = pageText ? classifyPageText(pageText) : null;
  if (fromPage) return fromPage;
  const m = message || "";
  if (/Session expired|showed the login page/i.test(m)) return new CheckoutFailure("SESSION_EXPIRED", m);
  if (/Notify Me|no cart\+ icon|Add to cart not found|no Add to cart text/i.test(m)) {
    return new CheckoutFailure("PRODUCT_UNAVAILABLE", m);
  }
  if (/enter pincode to see if the product is in stock|enter delivery pincode/i.test(m)) {
    return new CheckoutFailure("PINCODE_NOT_SET", m);
  }
  if (/out of stock|not serviceable/i.test(m)) return new CheckoutFailure("PRODUCT_NOT_SERVICEABLE", m);
  if (/not deliverable/i.test(m)) return new CheckoutFailure("ITEM_NOT_DELIVERABLE", m);
  if (/maximum units/i.test(m)) return new CheckoutFailure("MAX_UNITS_REACHED", m);
  if (/emptying cart|empty cart|original cart/i.test(m)) return new CheckoutFailure("EMPTY_CART_FAILED", m);
  if (/Product not in viewcart|Add to cart control not found/i.test(m)) {
    return new CheckoutFailure("ADD_TO_CART_FAILED", m);
  }
  if (/GST not applicable/i.test(m)) return new CheckoutFailure("GST_NOT_APPLICABLE", m);
  if (/GST is not found on page|GST not found/i.test(m)) return new CheckoutFailure("GST_NOT_FOUND", m);
  if (/GST/i.test(m)) return new CheckoutFailure("GST_SELECT_FAILED", m);
  if (/remove address|Delete.*address|request to remove address/i.test(m)) {
    return new CheckoutFailure("ADDRESS_REMOVE_FAILED", m);
  }
  if (/address|mobile/i.test(m)) return new CheckoutFailure("ADDRESS_SELECTION_FAILED", m);
  if (/delivery_sla|Delivery days do not match|Could not convert delivery/i.test(m)) {
    return new CheckoutFailure("SLA_EXCEEDED", m);
  }
  if (/cart amount|cartAmountLimit|Total Amount|payable/i.test(m)) {
    return new CheckoutFailure("CART_AMOUNT_LIMIT", m);
  }
  if (/unable to place/i.test(m)) return new CheckoutFailure("UNABLE_TO_PLACE_ORDER", m);
  if (/OTP/i.test(m) && /timeout/i.test(m)) return new CheckoutFailure("OTP_TIMEOUT", m);
  if (/OTP/i.test(m)) return new CheckoutFailure("OTP_NOT_FOUND", m);
  if (/cash on delivery.*(unavailable|not available)|\bCOD\b.*(unavailable|not available)/i.test(m)) {
    return new CheckoutFailure("COD_UNAVAILABLE", m);
  }
  if (/insufficient/i.test(m)) return new CheckoutFailure("INSUFFICIENT_BALANCE", m);
  return new CheckoutFailure("UNKNOWN", m);
}

export function failureFields(err: CheckoutFailure) {
  return {
    errorCode: err.code,
    errorCodeDisplay: err.display,
    errorSource: err.source,
    errorDetails: err.details,
    failedStep: err.failedStep,
    failureMessage: err.details,
    error: err.details,
  };
}
