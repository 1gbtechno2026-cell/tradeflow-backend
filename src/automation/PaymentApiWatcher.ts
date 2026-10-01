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

export function redact(text: string): string {
  return String(text || "")
    .replace(/\b(?:\d[ -]?){12,19}\b/g, (m) => `****${m.replace(/\D/g, "").slice(-4)}`)
    .replace(
      /("(?:[a-z_]*(?:cvv|cvv2|password|pin|otp|security_?code|expiry|exp_?month|exp_?year|valid_?thru)[a-z_]*)"\s*:\s*)"[^"]*"/gi,
      '$1"***"'
    )
    .replace(/((?:cvv|password|pin|otp)=)[^&\s"]+/gi, "$1***");
}

function parseRedacted(raw: string | null | undefined): unknown {
  if (!raw) return null;
  const safe = redact(raw);
  try {
    return JSON.parse(safe);
  } catch {
    return safe.slice(0, 4000);
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
