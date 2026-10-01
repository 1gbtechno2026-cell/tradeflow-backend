import type { BrowserContext } from "playwright";

/**
 * Reads the API responses Flipkart's own page receives, and keeps the parts the
 * checkout flow needs to confirm each step.
 *
 * STRICTLY PASSIVE. It attaches a `response` listener and never sends a request of
 * its own — every action is still performed by tapping the real page like a person
 * would. This only listens to what comes back.
 *
 * ── WHY ───────────────────────────────────────────────────────────────────────
 * Without it, every verdict is scraped from page text, and the flow can only say
 * "the page didn't look right". Flipkart answers each action with a precise reason
 * that the DOM may never render:
 *
 *   /api/5/cart            was add-to-cart refused, and with which error code
 *   CHECKOUT_UPDATE_ITEM_QUANTITY   "You can only purchase 16 units of ..."
 *   CHECKOUT_PAYMENT_TOKEN_GENERATE was a payment page actually created, or why not
 *   /api/1/contacts        the saved addresses as DATA, not as scraped card text
 *
 * That is the difference between reporting UNKNOWN and reporting what Flipkart
 * actually said. Ported from the reference flipkart-order-flow, which uses exactly
 * these four signals.
 *
 * Nothing here is required: every consumer treats a missing watcher, or a missing
 * action, as "no extra information" and falls back to the page checks.
 */

/** A saved address, as /api/1/contacts returns it. */
export interface FlipkartContact {
  id: string;
  name: string;
  phone: string;
  pincode: string;
  city: string;
  state: string;
  addressLine1: string;
  addressLine2: string;
  label: string;
}

/** One checkout action (/api/1/action/view), e.g. CHECKOUT_UPDATE_ITEM_QUANTITY. */
export interface FlipkartAction {
  type: string;
  success: boolean;
  messages: string[];
  /** Where Flipkart sends the page next, e.g. /payments?token=… */
  landingUrl: string | null;
  request: unknown;
  at: number;
}

/** Per-listing result of add to cart (/api/5/cart). */
export interface FlipkartCartAdd {
  listingId: string;
  productId: string;
  errorCode: string | null;
  errorMessage: string | null;
  presentInCart: boolean;
  at: number;
}

const ROME = /^https:\/\/([a-z0-9-]+\.)*rome\.api\.flipkart\.com\//i;

export class FlipkartApiWatcher {
  contacts: FlipkartContact[] | null = null;
  contactsAt = 0;
  readonly actions: FlipkartAction[] = [];
  readonly cartAdds: FlipkartCartAdd[] = [];

  /** Attach to a context. Safe to call for both the desktop and mobile legs — the
   *  flow spans two browsers, and one watcher should see all of it. */
  attach(context: BrowserContext): this {
    context.on("response", (res) => {
      void (async () => {
        const req = res.request();
        const url = req.url();
        if (!ROME.test(url) || !["xhr", "fetch"].includes(req.resourceType())) return;
        let body: Record<string, unknown> | undefined;
        try {
          body = (await res.json()) as Record<string, unknown>;
        } catch {
          return; // not JSON — images, HTML, aborted responses
        }
        let reqBody: Record<string, unknown> | null = null;
        try {
          const post = req.postData();
          reqBody = post ? (JSON.parse(post) as Record<string, unknown>) : null;
        } catch {
          /* not JSON */
        }
        const R = (body as { RESPONSE?: unknown })?.RESPONSE as Record<string, unknown> | undefined;
        const pathname = new URL(url).pathname;
        try {
          if (pathname.endsWith("/api/1/contacts") && Array.isArray(R)) {
            this.contacts = (R as Array<Record<string, unknown>>).map((a) => ({
              id: String(a.id ?? ""),
              name: String(a.name ?? ""),
              phone: String(a.phone ?? ""),
              pincode: String(a.pincode ?? ""),
              city: String(a.city ?? ""),
              state: String(a.state ?? ""),
              addressLine1: String(a.addressLine1 ?? ""),
              addressLine2: String(a.addressLine2 ?? ""),
              label: String(a.displayLabel ?? a.locationTypeTag ?? ""),
            }));
            this.contactsAt = Date.now();
          } else if (pathname.endsWith("/api/5/cart") && R?.cartResponse) {
            const at = Date.now();
            const ctx = (reqBody?.cartContext ?? {}) as Record<string, { productId?: string }>;
            for (const [listingId, raw] of Object.entries(R.cartResponse as Record<string, unknown>)) {
              const it = (raw ?? {}) as Record<string, unknown>;
              this.cartAdds.push({
                listingId,
                productId: String(it.productId ?? ctx[listingId]?.productId ?? ""),
                errorCode: (it.errorCode as string) ?? null,
                errorMessage: (it.errorMessage as string) ?? null,
                presentInCart: Boolean(it.presentInCart),
                at,
              });
            }
          } else if (pathname.endsWith("/api/1/action/view")) {
            const actionCtx = (R?.actionResponseContext ?? {}) as Record<string, unknown>;
            const reqCtx = (reqBody?.actionRequestContext ?? {}) as Record<string, unknown>;
            const landing = actionCtx.landingPageAction as { originalUrl?: string } | undefined;
            this.actions.push({
              type: String(reqCtx.type ?? actionCtx.type ?? "UNKNOWN"),
              // Flipkart leaves actionSuccess undefined on success, so only an
              // explicit false counts as a refusal.
              success: (R as { actionSuccess?: boolean })?.actionSuccess !== false,
              messages: ((actionCtx.actionMessages ?? []) as Array<{ text?: string }>)
                .map((m) => String(m?.text ?? ""))
                .filter(Boolean),
              landingUrl: landing?.originalUrl ?? null,
              request: reqCtx,
              at: Date.now(),
            });
          }
        } catch {
          /* an unexpected shape must never break a run — the page checks still run */
        }
      })();
    });
    return this;
  }

  /** A checkout action of `type` that happened after `since`. Null on timeout, which
   *  callers treat as "no extra information", never as a failure. */
  async waitForAction(type: string | RegExp, since: number, timeoutMs = 10_000): Promise<FlipkartAction | null> {
    const match = (a: FlipkartAction) =>
      a.at >= since && (typeof type === "string" ? a.type === type : type.test(a.type));
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      // Newest first: a retried step produces several, and the last is the verdict.
      const hit = [...this.actions].reverse().find(match);
      if (hit) return hit;
      if (Date.now() > deadline) return null;
      await new Promise((r) => setTimeout(r, 150));
    }
  }

  /** Saved addresses loaded after `since` — the address page fetches them on open. */
  async waitForContacts(since: number, timeoutMs = 10_000): Promise<FlipkartContact[] | null> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() <= deadline) {
      if (this.contacts && this.contactsAt >= since) return this.contacts;
      await new Promise((r) => setTimeout(r, 150));
    }
    return null;
  }

  /** Latest add-to-cart result for this product, by pid or listing id, after `since`. */
  lastCartAdd(productId: string, since: number): FlipkartCartAdd | null {
    const pid = String(productId || "").toUpperCase();
    if (!pid) return null;
    return (
      [...this.cartAdds]
        .reverse()
        .find(
          (c) => c.at >= since && (c.productId.toUpperCase() === pid || c.listingId.toUpperCase().includes(pid))
        ) ?? null
    );
  }

  /** An add that Flipkart refused outright: it carries an error AND the item did not
   *  land in the cart. An error alongside presentInCart is a warning, not a refusal. */
  refusedAdd(since: number): FlipkartCartAdd | null {
    return this.cartAdds.find((c) => c.at >= since && c.errorCode && !c.presentInCart) ?? null;
  }
}
