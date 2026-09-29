import type { Page, Response } from "playwright";

/**
 * Passive observer for Flipkart's own JSON responses.
 *
 * Read-only by design: it never routes, blocks, or modifies traffic, and every
 * handler is wrapped so a failure here can never break the checkout flow.
 * Deliverability / stock / pincode facts are read from Flipkart's API JSON
 * (ground truth) instead of scraped from DOM text, so hidden DOM and stale
 * cart items cannot produce false positives.
 *
 * Endpoint URLs are intentionally NOT hardcoded — Flipkart changes them.
 * Any same-site JSON body is walked and matched on semantic keys/phrases.
 */

export type NetSignalKind =
  | "deliverability"
  | "stock"
  | "pincode_prompt"
  | "cart"
  | "generic_error";

export interface NetSignal {
  at: string;
  url: string;
  kind: NetSignalKind;
  severity: "info" | "block";
  text: string;
  pincode?: string;
}

const MAX_BODY_BYTES = 2_000_000;
const MAX_SIGNALS = 60;
const MAX_WALK_DEPTH = 10;
const MAX_WALK_NODES = 20_000;
const MAX_TEXT = 240;
const MAX_STRING_SCAN = 500;
const DEDUPE_MS = 2000;

const BLOCK_PHRASES: Array<{ re: RegExp; kind: NetSignalKind }> = [
  {
    re: /enter pincode to see if the product is in stock|enter delivery pincode|enter pincode/i,
    kind: "pincode_prompt",
  },
  {
    re: /not deliverable|cannot be delivered|delivery not available|not serviceable|undeliverable/i,
    kind: "deliverability",
  },
  {
    re: /out of stock|currently unavailable|sold out|coming soon/i,
    kind: "stock",
  },
  {
    re: /item(s)? (is |are )?not available|not available at (this|your) (location|pincode)/i,
    kind: "stock",
  },
  {
    re: /removed from cart|cart is empty|missing cart items/i,
    kind: "cart",
  },
];

// boolean false on these keys means "cannot buy/deliver"
const NEGATIVE_FLAG_KEYS = /^(serviceable|deliverable|available|inStock|in_stock|isAvailable|isServiceable|codAvailable|buyable)$/i;
// boolean true on these keys means the same
const POSITIVE_FLAG_KEYS = /^(outOfStock|out_of_stock|unavailable|notServiceable|isOos)$/i;
const MESSAGE_KEYS = /^(message|messages|errorMessage|error|msg|title|subtitle|displayMessage|serviceabilityMessage|availabilityMessage|deliveryMessage|availabilityDisplay|messageText)$/i;
const PINCODE_RE = /\b(\d{6})\b/;

function shortUrl(url: string): string {
  try {
    const u = new URL(url);
    const s = `${u.hostname}${u.pathname}`;
    return s.length > 120 ? `${s.slice(0, 117)}...` : s;
  } catch {
    return url.slice(0, 120);
  }
}

function clip(text: string): string {
  const t = text.replace(/\s+/g, " ").trim();
  return t.length > MAX_TEXT ? `${t.slice(0, MAX_TEXT - 3)}...` : t;
}

function walkJson(
  node: unknown,
  depth: number,
  budget: { nodes: number },
  visit: (key: string | null, value: unknown) => void
): void {
  if (depth > MAX_WALK_DEPTH || budget.nodes <= 0) return;
  if (Array.isArray(node)) {
    for (const item of node) {
      if (budget.nodes-- <= 0) return;
      walkJson(item, depth + 1, budget, visit);
    }
    return;
  }
  if (node && typeof node === "object") {
    for (const [k, v] of Object.entries(node as Record<string, unknown>)) {
      if (budget.nodes-- <= 0) return;
      visit(k, v);
      if (v && typeof v === "object") walkJson(v, depth + 1, budget, visit);
    }
  }
}

export class FlipkartNetworkObserver {
  private signals: NetSignal[] = [];
  private seenAt = new Map<string, number>();
  private waiters: Array<{
    pred: (s: NetSignal) => boolean;
    resolve: (s: NetSignal | null) => void;
    timer: NodeJS.Timeout;
  }> = [];

  private readonly onResponse = (res: Response) => {
    void this.handle(res);
  };

  private constructor(private page: Page) {}

  static attach(page: Page): FlipkartNetworkObserver {
    const obs = new FlipkartNetworkObserver(page);
    page.on("response", obs.onResponse);
    return obs;
  }

  dispose(): void {
    this.page.off("response", this.onResponse);
    for (const w of this.waiters) {
      clearTimeout(w.timer);
      w.resolve(null);
    }
    this.waiters = [];
  }

  private push(kind: NetSignalKind, severity: "info" | "block", text: string, url: string): void {
    const clean = clip(text);
    if (!clean) return;
    const key = `${kind}|${severity}|${clean}`;
    const now = Date.now();
    const last = this.seenAt.get(key) || 0;
    if (now - last < DEDUPE_MS) return;
    this.seenAt.set(key, now);
    if (this.seenAt.size > 500) {
      for (const [k, t] of this.seenAt) if (now - t > 60_000) this.seenAt.delete(k);
    }
    const pin = clean.match(PINCODE_RE)?.[1];
    const signal: NetSignal = {
      at: new Date(now).toISOString(),
      url,
      kind,
      severity,
      text: clean,
      ...(pin ? { pincode: pin } : {}),
    };
    this.signals.push(signal);
    if (this.signals.length > MAX_SIGNALS) this.signals.splice(0, this.signals.length - MAX_SIGNALS);
    for (const w of [...this.waiters]) {
      let hit = false;
      try {
        hit = w.pred(signal);
      } catch {
        hit = false;
      }
      if (hit) {
        clearTimeout(w.timer);
        this.waiters.splice(this.waiters.indexOf(w), 1);
        w.resolve(signal);
      }
    }
  }

  private async handle(res: Response): Promise<void> {
    try {
      const url = res.url();
      if (!/flipkart\.com/i.test(url)) return;
      const headers = res.headers();
      if (!/json/i.test(headers["content-type"] || "")) return;
      const contentLength = Number(headers["content-length"] || 0);
      if (contentLength > MAX_BODY_BYTES) return;
      const status = res.status();
      const from = shortUrl(url);
      if (status >= 400) {
        this.push("generic_error", "info", `HTTP ${status}`, from);
        return;
      }
      const buf = await res.body().catch(() => null);
      if (!buf || buf.length > MAX_BODY_BYTES) return;
      let data: unknown;
      try {
        data = JSON.parse(buf.toString("utf8"));
      } catch {
        return;
      }
      const budget = { nodes: MAX_WALK_NODES };
      walkJson(data, 0, budget, (key, value) => {
        if (typeof value === "string") {
          if (value.length < 3 || value.length > MAX_STRING_SCAN) return;
          const looksInteresting = key ? MESSAGE_KEYS.test(key) : true;
          if (!looksInteresting && key && !/text|label|desc|info|note|alert/i.test(key)) return;
          for (const { re, kind } of BLOCK_PHRASES) {
            const m = value.match(re);
            if (m) {
              this.push(kind, "block", m[0].length < value.length && value.length <= MAX_TEXT ? value : m[0], from);
              break;
            }
          }
          return;
        }
        if (typeof value === "boolean" && key) {
          if (NEGATIVE_FLAG_KEYS.test(key) && value === false) {
            const kind: NetSignalKind = /stock|availab|buyable/i.test(key) ? "stock" : "deliverability";
            this.push(kind, "block", `${key}=false`, from);
          } else if (POSITIVE_FLAG_KEYS.test(key) && value === true) {
            const kind: NetSignalKind = /stock|oos/i.test(key) ? "stock" : "deliverability";
            this.push(kind, "block", `${key}=true`, from);
          }
        }
      });
    } catch {
      // passive observer — never let an observer error reach the checkout flow
    }
  }

  getSignals(): NetSignal[] {
    return [...this.signals];
  }

  getBlocks(): NetSignal[] {
    return this.signals.filter((s) => s.severity === "block");
  }

  latestBlock(): NetSignal | null {
    const blocks = this.getBlocks();
    return blocks.length ? blocks[blocks.length - 1] : null;
  }

  hasBlockFor(pincode?: string): boolean {
    const pin = (pincode || "").replace(/\D/g, "").slice(-6);
    return this.getBlocks().some((s) => !pin || !s.pincode || s.pincode === pin);
  }

  summary(max = 4): string {
    if (!this.signals.length) return "no Flipkart API signals captured";
    const blocks = this.getBlocks();
    const picked = (blocks.length ? blocks : this.signals).slice(-max);
    const lines = picked.map((s) => `${s.kind}: ${s.text} (${s.url})`);
    return `${this.signals.length} signal(s), ${blocks.length} block(s) — ${lines.join(" | ")}`;
  }

  waitForSignal(pred: (s: NetSignal) => boolean, timeoutMs: number): Promise<NetSignal | null> {
    const existing = this.signals.find((s) => {
      try {
        return pred(s);
      } catch {
        return false;
      }
    });
    if (existing) return Promise.resolve(existing);
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        const idx = this.waiters.findIndex((w) => w.resolve === resolve);
        if (idx >= 0) this.waiters.splice(idx, 1);
        resolve(null);
      }, timeoutMs);
      this.waiters.push({ pred, resolve, timer });
    });
  }
}
