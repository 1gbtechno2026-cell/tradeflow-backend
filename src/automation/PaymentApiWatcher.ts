import fs from "node:fs";
import path from "node:path";
import type { BrowserContext } from "playwright";

/**
 * The payment page's own API traffic, kept for diagnosis.
 *
 * STRICTLY PASSIVE, like FlipkartApiWatcher: a `response` listener, never a
 * request of its own. Where that watcher reads the checkout's answers, this one
 * reads the PAYMENT page's — pay.flipkart.com and the /api/3/checkout calls it
 * makes to rome — because the modal Flipkart shows after Pay says "technical
 * error" and nothing else, while the response behind it says
 * `status_code: "PAYZIPPY_TECHNICAL_ERROR"`, a `txn_id`, and sometimes why.
 * That is the difference between "it failed" and a reason an operator can act on.
 *
 * ── REDACTION ─────────────────────────────────────────────────────────────────
 * The requests carry the card. Everything recorded passes through `redact()`
 * first: any run of 12–19 digits becomes ****<last4>, and any JSON value whose
 * key looks like cvv / password / pin / otp / expiry becomes "***". Nothing is
 * ever stored unredacted, not even in memory.
 */

export interface PaymentApiCall {
  at: string;
  method: string;
  url: string;
  status: number;
  request: unknown;
  response: unknown;
}

/** What the gateway said about the payment attempt, from its own response. */
export interface PaymentGatewayResult {
  responseStatus: string | null;
  statusCode: string | null;
  message: string | null;
  responseType: string | null;
  txnId: string | null;
  url: string;
  at: string;
}

// Any flipkart.com host: the gateway call (paywithdetails) was NOT on
// pay.flipkart.com or rome — the first recording caught 14 rome calls and
// missed the one that mattered. XHR/fetch only, so no assets.
const PAYMENT_HOST = /^https:\/\/([a-z0-9-]+\.)*flipkart\.com\//i;

/** What Flipkart's own responses say about a placed order. Rupees as strings. */
export interface PlacedOrderDetails {
  orderId?: string;
  transactionAmount?: string;
  cartAfterCardOffer?: string;
  paymentFee?: string;
  bankTransactionId?: string;
  pgTransactionId?: string;
  bankName?: string;
  cardBrand?: string;
  transactionStatus?: string;
  promiseDays?: number | null;
  supercoinsApplied?: string;
  giftCardApplied?: string;
  unitPrice?: string;
  mrp?: string;
  orderStatus?: string;
  sellerName?: string;
  instrument?: string;
}

/** Depth-first search for a key anywhere in a parsed JSON value. */
function findKey(value: unknown, key: string, depth = 0): unknown {
  if (depth > 12 || value == null || typeof value !== "object") return null;
  if (Array.isArray(value)) {
    for (const v of value) {
      const hit = findKey(v, key, depth + 1);
      if (hit) return hit;
    }
    return null;
  }
  const obj = value as Record<string, unknown>;
  if (key in obj) return obj[key];
  for (const v of Object.values(obj)) {
    const hit = findKey(v, key, depth + 1);
    if (hit) return hit;
  }
  return null;
}

export function redact(text: string): string {
  return String(text || "")
    .replace(/\b(?:\d[ -]?){12,19}\b/g, (m) => `****${m.replace(/\D/g, "").slice(-4)}`)
    .replace(
      /("(?:[a-z_]*(?:cvv|cvv2|password|pin|otp|security_?code|expiry|exp_?month|exp_?year|valid_?thru)[a-z_]*)"\s*:\s*)"[^"]*"/gi,
      '$1"***"'
    )
    .replace(/((?:cvv|password|pin|otp)=)[^&\s"]+/gi, "$1***");
}

const SENSITIVE_KEY = /cvv|cvv2|password|pin\b|otp|security_?code|expiry|exp_?month|exp_?year|valid_?thru/i;

/** Redact a parsed JSON value in place of redacting its text: strings get the
 *  digit-run mask, sensitive keys get "***", numbers and structure are left
 *  alone. Redacting the TEXT turned unquoted 13-digit timestamps into `****…`
 *  and made the whole body unparseable, so the order-confirmation data was
 *  being stored as a 4000-character string fragment. */
function redactValue(value: unknown, depth = 0): unknown {
  if (depth > 24 || value == null) return value;
  if (typeof value === "string") return redact(value);
  if (Array.isArray(value)) return value.map((v) => redactValue(v, depth + 1));
  if (typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      out[k] = SENSITIVE_KEY.test(k) && (typeof v === "string" || typeof v === "number") ? "***" : redactValue(v, depth + 1);
    }
    return out;
  }
  return value;
}

function parseRedacted(raw: string | null | undefined): unknown {
  if (!raw) return null;
  try {
    return redactValue(JSON.parse(raw));
  } catch {
    return redact(raw).slice(0, 4000);
  }
}

export class PaymentApiWatcher {
  readonly calls: PaymentApiCall[] = [];

  attach(context: BrowserContext): this {
    context.on("response", (res) => {
      void (async () => {
        const req = res.request();
        const url = req.url();
        if (!PAYMENT_HOST.test(url) || !["xhr", "fetch"].includes(req.resourceType())) return;
        let bodyText: string | null = null;
        try {
          bodyText = await res.text();
        } catch {
          /* aborted / opaque — keep the call without a body */
        }
        this.calls.push({
          at: new Date().toISOString(),
          method: req.method(),
          url: redact(url),
          status: res.status(),
          request: parseRedacted(req.postData()),
          response: parseRedacted(bodyText),
        });
      })();
    });
    return this;
  }

  /**
   * The latest gateway verdict — the response to the Pay press itself
   * (pay / paywithdetails / pgResponse). Null until one has been seen.
   */
  lastGatewayResult(): PaymentGatewayResult | null {
    for (let i = this.calls.length - 1; i >= 0; i--) {
      const c = this.calls[i];
      if (!/\/(?:paywithdetails|pay|pgResponse|submitpayment)(?:[/?#]|$)/i.test(c.url)) continue;
      const r = (c.response ?? {}) as Record<string, unknown>;
      if (typeof r !== "object" || r === null) continue;
      const messages = Array.isArray(r.messages) ? (r.messages as Array<Record<string, unknown>>) : [];
      const first = messages[0] ?? {};
      if (!("response_status" in r) && !messages.length && !("status_code" in first)) continue;
      return {
        responseStatus: r.response_status != null ? String(r.response_status) : null,
        statusCode: first.status_code != null ? String(first.status_code) : null,
        message: first.message != null ? String(first.message) : null,
        responseType: r.response_type != null ? String(r.response_type) : null,
        txnId: r.txn_id != null ? String(r.txn_id) : null,
        url: c.url,
        at: c.at,
      };
    }
    return null;
  }

  /**
   * Everything Flipkart's own responses say about a PLACED order, from two
   * calls the page makes after the bank hands back (both seen on
   * OD4387711732098711 and OD4387713530896621):
   *
   *   pgresponsehandler  → action.postbackParams: transaction_amount "17000",
   *                        primary_record {"primary_amount":16900},
   *                        payment_handling_fees_details {CORP_CARD_FEE 100},
   *                        bank_transaction_id, provider, card_brand,
   *                        merchant_transaction_id "OD…-TX-00".
   *                        Amounts are in PAISE — the cart was ₹169, the charge
   *                        ₹170 — while displayInfo.amount (170) is in rupees.
   *   page/fetch (ORDER_CONFIRMATION_PAGE) → orderLevelTrackingMap.EUCLID:
   *                        orderId, ocUnitDetails[].{price, mrp, discount,
   *                        sla[].maxSla (days), cnc.coinComponent (SuperCoins),
   *                        orderStatus, sellerName}, paymentInstrumentMetaInfo.
   *
   * The confirmation PAGE shows only "Order Placed / You saved ₹…" behind a
   * scratch card, so this is where the numbers come from, not the DOM.
   */
  placedOrderDetails(): PlacedOrderDetails | null {
    const out: PlacedOrderDetails = {};
    let any = false;
    for (let i = this.calls.length - 1; i >= 0; i--) {
      const c = this.calls[i];
      const r = c.response as Record<string, unknown> | null;
      if (!r || typeof r !== "object") continue;
      if (/pgresponsehandler/i.test(c.url) && !out.transactionAmount) {
        const action = (r.action ?? {}) as Record<string, unknown>;
        const pp = (action.postbackParams ?? {}) as Record<string, string>;
        if (!pp.transaction_amount && !pp.merchant_transaction_id) continue;
        const paise = (v: unknown) => {
          const n = Number(v);
          return Number.isFinite(n) ? String(Math.round(n) / 100) : "";
        };
        let primary = "";
        let fee = "";
        try {
          primary = paise((JSON.parse(pp.primary_record || "{}") as { primary_amount?: number }).primary_amount);
        } catch { /* absent */ }
        try {
          const charges = (JSON.parse(pp.payment_handling_fees_details || "{}") as { applied_charges?: Array<{ amount?: number }> }).applied_charges ?? [];
          fee = paise(charges.reduce((n, ch) => n + Number(ch.amount || 0), 0));
        } catch { /* absent */ }
        out.orderId = out.orderId || (pp.merchant_transaction_id || "").match(/OD\d{12,}/)?.[0] || "";
        out.transactionAmount = paise(pp.transaction_amount);
        out.cartAfterCardOffer = primary;
        out.paymentFee = fee;
        out.bankTransactionId = pp.bank_transaction_id || "";
        out.pgTransactionId = pp.pg_trackid || pp.payzippy_transaction_id || "";
        out.bankName = pp.bank_name || pp.provider || "";
        out.cardBrand = pp.card_brand || "";
        out.transactionStatus = pp.transaction_status || "";
        any = true;
      }
      if (/\/api\/4\/page\/fetch/i.test(c.url) && out.promiseDays == null) {
        const text = JSON.stringify(r);
        if (!/ORDER_CONFIRMATION_PAGE/.test(text)) continue;
        const euclid = findKey(r, "EUCLID") as Record<string, unknown> | null;
        if (!euclid) continue;
        const units = (euclid.ocUnitDetails ?? []) as Array<Record<string, unknown>>;
        const u = units[0] ?? {};
        const sla = ((u.sla ?? []) as Array<{ maxSla?: number }>)[0];
        const cnc = (u.cnc ?? {}) as { coinComponent?: number; cashComponent?: number };
        const pim = (euclid.paymentInstrumentMetaInfo ?? {}) as Record<string, string>;
        out.orderId = out.orderId || String(euclid.orderId || "").match(/OD\d{12,}/)?.[0] || "";
        out.promiseDays = sla?.maxSla ?? null;
        out.supercoinsApplied = cnc.coinComponent != null ? String(cnc.coinComponent) : "";
        out.unitPrice = u.price != null ? String(u.price) : "";
        out.mrp = u.mrp != null ? String(u.mrp) : "";
        out.orderStatus = String(u.orderStatus || "");
        out.sellerName = String(u.sellerName || "");
        out.instrument = [pim.primaryInstrumentName, pim.primaryInstrumentType].filter(Boolean).join(" ");
        out.giftCardApplied = /gift/i.test(String(pim.secondaryInstrumentType || "")) ? "applied" : "";
        any = true;
      }
    }
    return any ? out : null;
  }

  /** One-line tag for a failure detail: "[PAYZIPPY_TECHNICAL_ERROR txn PZT…]". */
  gatewayTag(): string {
    const g = this.lastGatewayResult();
    if (!g) return "";
    const parts = [g.statusCode, g.txnId ? `txn ${g.txnId}` : null].filter(Boolean);
    return parts.length ? ` [gateway: ${parts.join(", ")}]` : "";
  }

  /** Every recorded call, redacted, as <dir>/network/payments.json. */
  writeTo(dir: string): string | null {
    if (!this.calls.length) return null;
    const out = path.join(dir, "network");
    fs.mkdirSync(out, { recursive: true });
    const file = path.join(out, "payments.json");
    fs.writeFileSync(file, JSON.stringify(this.calls, null, 2), "utf8");
    return file;
  }
}
