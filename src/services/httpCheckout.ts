import { devices, request as playwrightRequest, type APIRequestContext } from "playwright";
import { config } from "../config.js";
import { fullOrderId, shortOrderId } from "../automation/PaymentApiWatcher.js";
import { mapPlaywrightCookies } from "./browser.js";
import { CheckoutFailure } from "./checkoutErrors.js";
import type { AddressDetails, LogLevel } from "../types.js";

/**
 * The HTTP checkout engine — a Flipkart COD order placed with the calls the
 * Flipkart page itself makes, no Chrome.
 *
 * Every call below was taken from a recorded, successful COD order
 * (debug/test-runs/happy-2026-10-15T07-25-34-235Z/network/payments.json,
 * OD438890447913742100), in the same order and with the same bodies:
 *
 *   1  POST rome /api/4/page/fetch  /viewcart              what is in the cart now
 *   2  POST rome /api/5/cart                               add the listing (quantity per order)
 *   3  POST rome /api/1/action/view CHECKOUT_INIT          "Place Order" on the cart → viewcheckout
 *   4  POST rome /api/4/page/fetch  /viewcheckout          the summary: address, pincode serviceability, total, checkout id
 *   5  POST rome /api/1/action/view CHECKOUT_PAYMENT_TOKEN_GENERATE  → /payments?token=PN…
 *   6  GET  pay.flipkart.com/payments?token=…              the payment page (its cookies)
 *   7  POST 2.payments.flipkart.com/fkpay/api/v3/payments/pay?instrument=COD   the order
 *   8  POST rome /api/4/page/fetch  ORDER_CONFIRMATION_PAGE  details, best effort
 *
 * Verdicts it can give on its own (no Chrome needed): the listing cannot be
 * added, the pincode is not serviceable, the cart is over the limit, COD is
 * refused. Anything it cannot be sure about — a login or captcha challenge,
 * an unexpected answer, a cart with other items, an address that is not the
 * job's, GST required — is a FALLBACK: the caller runs the Chrome flow, which
 * handles all of those today. `dryRun` stops after the payment token: every
 * call before the order is exercised, nothing is bought, and the item is
 * taken out of the cart again.
 */

/**
 * Flipkart pins every account to a data centre and the host prefix names it:
 * `1.rome.api.flipkart.com` (CH), `2.rome.api.flipkart.com` (HYD). A call on
 * the wrong one answers 406 {"ERROR_MESSAGE":"DC Change","RESPONSE":{"id":"1"}}
 * — the page client then re-issues the call on the named prefix. So does this
 * engine: the first DC Change switches the session's prefix (rome and payments
 * alike) and the call is repeated once.
 */
const DEFAULT_DC = "2";
const romeHost = (dc: string) => `https://${dc}.rome.api.flipkart.com`;
const payApiHost = (dc: string) => `https://${dc}.payments.flipkart.com`;
const PAY_PAGE = "https://pay.flipkart.com";
/**
 * The m-site identity, as the Chrome flow's mobile leg presents it: the checkout
 * page URIs this engine fetches (/viewcheckout…) are the m-site's; asked for
 * with the desktop FKUA, rome answers 404 "Url not found!!".
 */
const UA = devices[config.mobileDevice || "Pixel 7"]?.userAgent || "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36";
const FKUA = `${UA} FKUA/msite/0.0.3/msite/Mobile`;
const CALL_TIMEOUT_MS = 25_000;
const ECHO_URL = process.env.PROXY_ECHO_URL || "https://api.ipify.org?format=json";

export interface HttpProxy {
  server: string;
  username?: string;
  password?: string;
}

export interface HttpCheckoutInput {
  cookies: unknown[];
  proxy?: HttpProxy | null;
  productUrl: string;
  quantity: number;
  pincode: string;
  /** The job's delivery address. When the checkout's address is another one,
   *  the engine fixes the account's address book the way the Chrome flow does
   *  (one address per account: edit the saved one into this), then selects it. */
  address?: AddressDetails;
  cartAmountLimit?: number;
  /** Max promised delivery days; the checkout's own SLA above it = SLA_EXCEEDED. */
  deliverySlaDays?: number;
  gstMandatory?: boolean;
  /** Stop after the payment token; nothing is bought; the cart is cleaned. */
  dryRun: boolean;
  /** "phrase": a saved address also counts when its text contains address.matchPhrase (default "name": identical contact name). */
  addressMatch?: "name" | "phrase";
  /** How long to wait after Pay for My Orders to list the order (default VERIFY_WAIT_MS). */
  verifyWaitMs?: number;
  log: (level: LogLevel, message: string, step?: string) => void;
  /** Called right before the order call — the line between "safe to re-run" and "an order may exist". */
  onBeforePay?: () => Promise<void>;
}

export interface HttpStepTimings {
  [step: string]: number;
}

export interface HttpPlaced {
  kind: "placed";
  orderId: string;
  referenceId: string;
  /** Flipkart's My Orders lists the order (checked after the pay call). false = the pay call said
   *  SUCCESS and gave an id, but the account's My Orders does not show it: Flipkart dropped it. */
  confirmed: boolean;
  /** The status My Orders shows ("Approved", …) when confirmed; how long the check waited. */
  listedStatus: string;
  verifyMs: number;
  amount: string;
  paymentFee: string;
  checkoutTotal: number | null;
  deliveryDays: number | null;
  deliveryText: string;
  exitIp: string;
  details: { orderStatus?: string; sellerName?: string; price?: number; mrp?: number; slaDays?: number | null };
  timings: HttpStepTimings;
}
export interface HttpDryRun {
  kind: "dry_run";
  token: string;
  checkoutTotal: number | null;
  deliveryDays: number | null;
  deliveryText: string;
  addressId: string;
  exitIp: string;
  cartCleaned: boolean;
  timings: HttpStepTimings;
}
export interface HttpFallback {
  kind: "fallback";
  reason: string;
  timings: HttpStepTimings;
}
export interface HttpFailed {
  kind: "failed";
  failure: CheckoutFailure;
  /** The order call was sent and no answer came back: an order MAY exist.
   *  Never re-run blindly — reconcile against the account's orders first. */
  payUnknown?: boolean;
  timings: HttpStepTimings;
}
export type HttpCheckoutOutcome = HttpPlaced | HttpDryRun | HttpFallback | HttpFailed;

class Fallback extends Error {}

/** `pid` and `lid` from a Flipkart product URL. Both are required for the cart call. */
export function parseProductIds(productUrl: string): { pid: string; lid: string } | null {
  try {
    const u = new URL(productUrl);
    const pid = String(u.searchParams.get("pid") || "").toUpperCase();
    const lid = String(u.searchParams.get("lid") || "").toUpperCase();
    return pid && lid ? { pid, lid } : null;
  } catch {
    return null;
  }
}

/** pid + lid from the product page's `window.__INITIAL_STATE__` (plain GET, no cookies). */
export async function resolveProductIds(productUrl: string, proxy?: HttpProxy | null): Promise<{ pid: string; lid: string } | null> {
  const ctx = await playwrightRequest.newContext({ ...(proxy ? { proxy } : {}), extraHTTPHeaders: { "user-agent": UA, accept: "text/html,*/*" }, timeout: 20_000 });
  try {
    const res = await ctx.get(productUrl);
    if (res.status() !== 200) return null;
    const html = await res.text();
    const pid = html.match(/"productId":"([A-Z0-9]{16})"/)?.[1] || html.match(/[?&]pid=([A-Z0-9]{16})/)?.[1] || "";
    const lid = html.match(/"listingId":"(LST[A-Z0-9]{16,})"/)?.[1] || html.match(/[?&]lid=(LST[A-Z0-9]{16,})/)?.[1] || "";
    return pid && lid ? { pid: pid.toUpperCase(), lid: lid.toUpperCase() } : null;
  } finally {
    await ctx.dispose().catch(() => undefined);
  }
}

async function newSession(cookies: unknown[], proxy?: HttpProxy | null): Promise<APIRequestContext> {
  const mapped = mapPlaywrightCookies(cookies);
  if (!mapped.length) throw new CheckoutFailure("SESSION_EXPIRED", "Saved Flipkart session has no usable cookies");
  return playwrightRequest.newContext({
    ...(proxy ? { proxy } : {}),
    storageState: { cookies: mapped.map((c) => ({ ...c, expires: c.expires ?? -1 })), origins: [] },
    extraHTTPHeaders: {
      accept: "*/*",
      "accept-language": "en-IN,en;q=0.9",
      "content-type": "application/json",
      origin: "https://www.flipkart.com",
      referer: "https://www.flipkart.com/",
      "user-agent": UA,
      "x-user-agent": FKUA,
    },
    timeout: CALL_TIMEOUT_MS,
  });
}

type Json = Record<string, unknown>;

class Rome {
  dc = DEFAULT_DC;
  constructor(private readonly ctx: APIRequestContext) {}

  post(path: string, body: unknown, label: string): Promise<Json> {
    return this.send("POST", path, body, label);
  }

  get(path: string, label: string): Promise<Json> {
    return this.send("GET", path, undefined, label);
  }

  put(path: string, body: unknown, label: string): Promise<Json> {
    return this.send("PUT", path, body, label);
  }

  async send(method: "GET" | "POST" | "PUT", path: string, body: unknown, label: string, retried = false): Promise<Json> {
    let res: Awaited<ReturnType<APIRequestContext["post"]>>;
    try {
      res = await this.ctx.fetch(`${romeHost(this.dc)}${path}`, { method, ...(body === undefined ? {} : { data: body }) });
    } catch (err) {
      // A dropped tunnel on a read/prepare call (nothing is bought before the
      // pay call, which does not come through here): one more try.
      const msg = err instanceof Error ? err.message : String(err);
      if (!retried && /aborted|ECONNRESET|ECONNREFUSED|socket hang up|socket disconnected|EPIPE|ETIMEDOUT|Timeout|tunnel/i.test(msg)) return this.send(method, path, body, label, true);
      throw err;
    }
    const status = res.status();
    const text = await res.text();
    const json: Json | null = text.trim().startsWith("{") ? (JSON.parse(text) as Json) : null;
    if (status === 406 && json && /DC Change/i.test(String(json.ERROR_MESSAGE || ""))) {
      const id = String(((json.RESPONSE as Json | null) || {}).id || "");
      if (/^\d+$/.test(id) && !retried) {
        // Either the other DC, or the same one answered DC Change under load:
        // one more try on the named prefix after a short pause.
        this.dc = id;
        await new Promise((r) => setTimeout(r, id === this.dc ? 400 : 0));
        return this.send(method, path, body, label, true);
      }
      throw new Fallback(`${label}: rome insists on DC ${id || "?"} after switching — session issue`);
    }
    if (status === 401 || status === 403 || status === 406 || status === 429) throw new Fallback(`${label}: rome answered ${status} — challenge or session issue`);
    if (status >= 500) throw new Fallback(`${label}: rome answered ${status}`);
    if (!json && text.trim().startsWith("[")) return { RESPONSE: JSON.parse(text) as unknown } as Json;
    if (!json) throw new Fallback(`${label}: rome answered non-JSON (${status})`);
    if (status >= 400) throw new Fallback(`${label}: rome answered ${status}: ${text.slice(0, 160)}`);
    return json;
  }
}

// ── Address book ───────────────────────────────────────────────────────────
// The same calls the account's Manage Addresses page makes (captured 2026-10-27):
//   GET /api/1/contacts                      → RESPONSE: [{ id, name, addressLine1, addressLine2, city, state, pincode, phone, locationTypeTag, isDefault, … }]
//   PUT /api/3/user/contact  { …fields, contactId }   edit → RESPONSE.id (a NEW contact id)
//   POST /api/3/user/contact { …fields }              add
// and, on the checkout, the address sheet's own select action:
//   POST /api/1/action/view  CHECKOUT_CHANGE_SHIPPING_ADDRESS { cartItemRefIds, contactId }

interface Contact {
  id: string;
  name: string;
  pincode: string;
  phone: string;
  addressLine1: string;
  city: string;
  isDefault: boolean;
}

function contactsFrom(json: Json): Contact[] {
  const list = Array.isArray(json.RESPONSE) ? (json.RESPONSE as Json[]) : [];
  return list.map((c) => ({
    id: String(c.id || ""),
    name: String(c.name || ""),
    pincode: String(c.pincode || ""),
    phone: String(c.phone || ""),
    addressLine1: String(c.addressLine1 || ""),
    city: String(c.city || ""),
    isDefault: c.isDefault === true,
  })).filter((c) => c.id);
}

const norm = (s: string) => s.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();

export function contactMatches(c: Contact, address: AddressDetails, pincode: string, mode: "name" | "phrase" = "name"): boolean {
  if (c.pincode !== pincode) return false;
  const wanted = norm(address.companyName || address.name || "");
  if (!wanted || norm(c.name) === wanted) return true;
  // Match Phrase: the saved entry is ours when its name or address text carries the phrase.
  const phrase = mode === "phrase" ? norm(address.matchPhrase || "") : "";
  return phrase.length >= 3 && norm(`${c.name} ${c.addressLine1} ${c.city}`).includes(phrase);
}

function contactBody(address: AddressDetails, pincode: string, phone: string): Json {
  return {
    name: (address.companyName || address.name || "").trim(),
    addressLine1: address.addressLine1 || "",
    addressLine2: address.locality || "",
    city: address.city || "",
    state: address.state || "",
    pincode,
    phone,
    locationTypeTag: String(address.addressType || "Home").toUpperCase() === "WORK" ? "WORK" : "HOME",
  };
}

/** The checkout line ids the address select action needs ("LST…:0:VER-…:U"). */
function cartItemRefIds(summary: Json): string[] {
  const text = JSON.stringify(summary);
  const fromQuery = text.match(/cartItemRefIds=([^"&\\]+)/)?.[1];
  if (fromQuery) return decodeURIComponent(fromQuery).split(",").filter(Boolean);
  const ids = new Set<string>();
  for (const m of text.matchAll(/"cartItemRefId":"([^"]+)"/g)) ids.add(m[1]);
  return [...ids];
}

const R = (json: Json) => (json.RESPONSE ?? {}) as Json;

function findKey(value: unknown, key: string, depth = 0): unknown {
  if (depth > 14 || value == null || typeof value !== "object") return undefined;
  if (Array.isArray(value)) {
    for (const v of value) {
      const hit = findKey(v, key, depth + 1);
      if (hit !== undefined) return hit;
    }
    return undefined;
  }
  const obj = value as Json;
  if (key in obj) return obj[key];
  for (const v of Object.values(obj)) {
    const hit = findKey(v, key, depth + 1);
    if (hit !== undefined) return hit;
  }
  return undefined;
}

/** Every object in the tree that passes `pick` (bounded depth). */
function findAll(value: unknown, pick: (o: Json) => boolean, out: Json[] = [], depth = 0): Json[] {
  if (depth > 18 || value == null || typeof value !== "object") return out;
  if (Array.isArray(value)) {
    for (const v of value) findAll(v, pick, out, depth + 1);
    return out;
  }
  const obj = value as Json;
  if (pick(obj)) out.push(obj);
  for (const v of Object.values(obj)) findAll(v, pick, out, depth + 1);
  return out;
}

/**
 * The cart page's own "Remove" buttons: one CART_REMOVE action per cart line,
 * whose params carry the cart item id ("<listing>:<account>:P:A-0001:U:SPCMS")
 * and the product id.
 */
function cartRemoveActions(json: Json): Array<{ listingId: string; itemId: string; productId: string }> {
  const seen = new Set<string>();
  const out: Array<{ listingId: string; itemId: string; productId: string }> = [];
  for (const a of findAll(json, (o) => o.type === "CART_REMOVE" && typeof (o.params as Json | undefined)?.listingId === "string")) {
    const params = a.params as Json;
    const itemId = String(params.listingId);
    if (seen.has(itemId)) continue;
    seen.add(itemId);
    out.push({ listingId: itemId.split(":")[0].toUpperCase(), itemId, productId: String(params.productId || "") });
  }
  return out;
}

/**
 * What the m-site posts when Remove is tapped (captured from the page on
 * 2026-10-27): one CART_REMOVE action with every item to drop. The page then
 * answers with the cart as it is.
 */
async function removeCartLines(rome: Rome, lines: Array<{ itemId: string; productId: string }>): Promise<void> {
  if (!lines.length) return;
  await rome.post("/api/1/action/view", {
    actionRequestContext: {
      isPartialPageLoadEnabled: false,
      pageSlotsMap: [],
      pageFetchRequest: { pageUri: "" },
      type: "CART_REMOVE",
      items: lines.map((l) => ({ productId: l.productId, listingId: l.itemId })),
      marketPlaces: [],
      pageUri: "/viewcart?exploreMode=TRUE&marketplace=FLIPKART",
      pageNumber: 1,
      userContext: {},
    },
  }, "cart remove");
}

/**
 * After the pay call: is the order really there? Flipkart's payment answer
 * says SUCCESS and hands out an order id even for orders its risk filter
 * then drops (2026-10-28: 72 of 204 "paid" orders never appeared in My
 * Orders). The account's My Orders list is the truth: poll it for up to
 * VERIFY_WAIT_MS; an order that is listed is confirmed, with its status.
 */
// 2026-10-30: of 733 paid orders none was listed within 90 s, so the default is
// a short confirmation only; the order sync keeps checking for hours after.
const VERIFY_WAIT_MS = Math.max(5_000, Number(process.env.HTTP_VERIFY_WAIT_MS) || 15_000);
const VERIFY_STEP_MS = 6_000;

async function verifyPlaced(rome: Rome, orderId: string, log: HttpCheckoutInput["log"], waitMs = VERIFY_WAIT_MS): Promise<{ confirmed: boolean; listedStatus: string; verifyMs: number }> {
  const short = shortOrderId(orderId);
  const full = orderId;
  const t0 = Date.now();
  let tries = 0;
  while (Date.now() - t0 < waitMs) {
    tries += 1;
    try {
      const page = await rome.get("/api/5/self-serve/orders/?page=1", "my orders");
      const orders = findAll(page, (o) => typeof o.orderId === "string" && (String(o.orderId) === short || String(o.orderId) === full || `OD${String(o.orderId)}` === short));
      if (orders.length) {
        const o = orders[0];
        const unitStatus = findAll(o, (x) => typeof x.status === "string" && String(x.status).length < 40)[0];
        const status = String(unitStatus?.status || o.status || o.orderStatus || "listed");
        return { confirmed: true, listedStatus: status, verifyMs: Date.now() - t0 };
      }
    } catch (err) {
      log("warn", `[http] my orders check: ${err instanceof Error ? err.message.split("\n")[0].slice(0, 120) : String(err)}`, "verify");
    }
    await new Promise((r) => setTimeout(r, VERIFY_STEP_MS));
  }
  log("warn", `[http] ${orderId} is NOT in My Orders after ${Math.round((Date.now() - t0) / 1000)} s (${tries} checks) — Flipkart dropped the order`, "verify");
  return { confirmed: false, listedStatus: "", verifyMs: Date.now() - t0 };
}

const VIEWCART_BODY = (pincode: string) => ({ pageUri: "/viewcart?exploreMode=TRUE&preference=FLIPKART", pageContext: { trackingContext: { context: { eVar61: "" } }, networkSpeed: 10000 }, locationContext: { pincode: Number(pincode), changed: false } });

/** The cart's own lines (not the recommendations under them): pageContext.fdpTrackingData.marketplaceData[].items[]. */
function cartLines(json: Json): Array<{ listingId: string; productId: string }> {
  const fdp = (findKey(json, "fdpTrackingData") || {}) as Json;
  const out: Array<{ listingId: string; productId: string }> = [];
  for (const mp of ((fdp.marketplaceData as Json[] | undefined) || [])) {
    for (const it of ((mp.items as Json[] | undefined) || [])) {
      const listingId = String(it.listingId || "").toUpperCase();
      if (listingId) out.push({ listingId, productId: String(it.productId || "").toUpperCase() });
    }
  }
  return out;
}

export async function runHttpCheckout(input: HttpCheckoutInput): Promise<HttpCheckoutOutcome> {
  const timings: HttpStepTimings = {};
  const t0 = Date.now();
  let mark = t0;
  const lap = (step: string) => {
    const now = Date.now();
    timings[step] = now - mark;
    mark = now;
  };
  const log = input.log;
  let ids = parseProductIds(input.productUrl);
  if (!ids) {
    // A short Flipkart URL (no ?pid=&lid=): the product page HTML carries both in its
    // embedded state (pageContext.productId / listingId) — one GET, same proxy.
    ids = await resolveProductIds(input.productUrl, input.proxy).catch(() => null);
    if (ids) log("info", `[http] resolved listing from the product page: pid ${ids.pid} lid ${ids.lid}`, "product");
  }
  if (!ids) return { kind: "fallback", reason: "product URL has no pid/lid and the product page did not reveal them — the page flow resolves the listing", timings };
  const pincode = String(input.pincode || "").replace(/\D/g, "").slice(-6);
  if (pincode.length !== 6) return { kind: "fallback", reason: "no 6-digit pincode on the job", timings };

  let ctx: APIRequestContext | null = null;
  let addedToCart = false;
  try {
    ctx = await newSession(input.cookies, input.proxy);
    const rome = new Rome(ctx);
    lap("session");

    // Exit address — informational, fetched alongside the first cart call.
    let exitIp = "";
    const echo = ctx.get(ECHO_URL, { timeout: 4000 }).then(async (r) => String(((await r.json().catch(() => ({}))) as { ip?: string }).ip || "")).catch(() => "");
    log("info", `[http] engine start · ${input.proxy ? `via ${input.proxy.server.replace(/^https?:\/\//, "")}` : "direct"} · pid ${ids.pid} lid ${ids.lid} · qty ${input.quantity} · pin ${pincode}`, "session");

    // 1. the cart as it is
    const cart0 = await rome.post("/api/4/page/fetch?cacheFirst=false", VIEWCART_BODY(pincode), "viewcart");
    exitIp = await echo;
    if (exitIp) log("info", `[http] exit IP ${exitIp}`, "session");
    const loginHint = JSON.stringify(R(cart0)).includes('"LOGIN"') && !JSON.stringify(R(cart0)).includes("cartItem");
    if (loginHint && findKey(R(cart0), "loginType") === "CHECKOUT_LOGIN") throw new Fallback("cart page asks for login — session challenge");
    // Other items in the cart would ride along on the order: take them out, as
    // the page flow does, so the checkout holds this listing alone.
    const foreignLines = cartLines(R(cart0)).filter((l) => l.listingId !== ids.lid);
    if (foreignLines.length) {
      const removable = cartRemoveActions(R(cart0)).filter((a) => a.listingId !== ids.lid);
      if (!removable.length) throw new Fallback(`cart holds ${foreignLines.length} other item(s) and the page offers no remove action — the page flow empties it`);
      await removeCartLines(rome, removable);
      const again = await rome.post("/api/4/page/fetch?cacheFirst=false", VIEWCART_BODY(pincode), "viewcart after remove");
      const still = cartLines(R(again)).filter((l) => l.listingId !== ids.lid);
      if (still.length) throw new Fallback(`cart still holds ${still.length} other item(s) after removing them — the page flow empties it`);
      log("info", `[http] removed ${removable.length} other item(s) from the cart (${removable.map((l) => l.listingId).join(", ")})`, "cart");
    }
    lap("cart_view");

    // 2. add to cart
    const add = await rome.post("/api/5/cart", { cartContext: { [ids.lid]: { productId: ids.pid, quantity: input.quantity, cashifyDiscountApplied: false, vulcanDiscountApplied: false, payWithEMISelected: false, selectedActions: [] } }, pageType: "ProductPage" }, "cart add");
    const cartResponse = (R(add).cartResponse ?? {}) as Record<string, Json>;
    const mine = cartResponse[ids.lid] ?? Object.values(cartResponse)[0];
    if (!mine) throw new Fallback("cart add answered without a cartResponse for the listing");
    if (mine.errorCode && !mine.presentInCart) {
      return { kind: "failed", failure: new CheckoutFailure("ADD_TO_CART_FAILED", `${String(mine.errorCode)}${mine.errorMessage ? `: ${String(mine.errorMessage)}` : ""}`), timings };
    }
    addedToCart = true;
    if (mine.errorMessage) log("warn", `[http] cart add note: ${String(mine.errorMessage)}`, "cart");
    lap("cart_add");

    // 3. Place Order → CHECKOUT_INIT
    const init = await rome.post("/api/1/action/view", { actionRequestContext: { actionContext: {}, asgardCheckoutFlow: false, consentRequired: false, isPartialPageLoadEnabled: false, pageNumber: 1, pageUri: "/viewcheckout?view=FLIPKART&marketplace=FLIPKART", type: "CHECKOUT_INIT", userContext: {} } }, "checkout init");
    const initR = R(init);
    const initLanding = String(findKey(initR, "originalUrl") || "");
    // The checkout page the init answer points at (params.pageUri), else the recorded default.
    const landingParams = (findKey(initR, "params") || {}) as Json;
    const checkoutUri = /viewcheckout/i.test(String(landingParams.pageUri || "")) ? String(landingParams.pageUri) : /^\/viewcheckout/i.test(initLanding) ? initLanding : "/viewcheckout?view=FLIPKART&marketplace=FLIPKART&tr_tenant=FLIPKART";
    if ((initR as { actionSuccess?: boolean }).actionSuccess === false) {
      const msgs = ((findKey(initR, "actionMessages") as Array<{ text?: string }> | null) || []).map((m) => m?.text).filter(Boolean).join(" ");
      throw new Fallback(`checkout init refused${msgs ? `: ${msgs}` : ""}`);
    }
    if (/\/login/i.test(initLanding) && !/viewcheckout/i.test(initLanding)) throw new Fallback("checkout init landed on login — session challenge");
    log("info", `[http] checkout opened on DC ${rome.dc} · ${checkoutUri}`, "checkout");
    lap("checkout_init");

    // 4. the checkout summary
    const readSummary = async () => {
      const summary = await rome.post("/api/4/page/fetch?cacheFirst=false", { pageUri: checkoutUri, pageContext: { trackingContext: { context: { eVar61: "" } }, networkSpeed: 10000 }, locationContext: { pincode: Number(pincode), changed: false } }, "viewcheckout");
      const track = (findKey(R(summary), "checkoutSummaryTrackingData") || {}) as Json;
      // Delivery promise: fdpTrackingData.marketPlaceData[].checkoutItems[].slaInfo[] (maxSLA in days,
      // the selected option), with the page's "Delivery by …" text for the row.
      const slaInfos = findAll(R(summary), (o) => Array.isArray(o.slaInfo)).flatMap((o) => o.slaInfo as Json[]);
      const slaPick = slaInfos.find((x) => x.isSelected === true) || slaInfos[0];
      return {
        summary,
        track,
        addressId: String(track.addressId || ""),
        summaryPin: String(track.pincode || ""),
        serviceable: track.pincodeServiceability,
        totalCost: typeof track.totalCost === "number" ? track.totalCost : Number(track.totalCost) || null,
        checkoutId: String(track.checkoutID || ""),
        deliveryDays: slaPick && typeof slaPick.maxSLA === "number" ? (slaPick.maxSLA as number) : null,
        deliveryText: String((findAll(R(summary), (o) => typeof o.text === "string" && /^Delivery by /i.test(String(o.text)))[0] || {}).text || ""),
      };
    };
    let sum = await readSummary();
    lap("checkout_summary");

    // The checkout is on another address (or none). Same policy as the Chrome
    // flow's address preflight, one account = one address: pick the saved
    // address that already matches, else edit the saved one into the job's
    // address (its phone stays), else add it; then select it on the checkout.
    if (input.address && (!sum.addressId || (sum.summaryPin && sum.summaryPin !== pincode))) {
      const contacts = contactsFrom(await rome.get("/api/1/contacts", "address book"));
      let target = contacts.find((c) => contactMatches(c, input.address!, pincode, input.addressMatch || "name")) || null;
      if (!target) {
        const base = contacts.find((c) => c.isDefault) || contacts[0] || null;
        const phone = (base?.phone || input.address.mobile || "").replace(/\D/g, "").slice(-10);
        if (!phone) throw new Fallback("account has no saved address and the job has no phone — the page flow adds the address");
        const body = contactBody(input.address, pincode, phone);
        const saved = base
          ? await rome.put("/api/3/user/contact", { ...body, contactId: base.id }, "address edit")
          : await rome.post("/api/3/user/contact", body, "address add");
        const id = String(((saved.RESPONSE as Json | null) || {}).id || "");
        if (!id) throw new Fallback(`address ${base ? "edit" : "add"} answered without an id`);
        target = { id, name: body.name as string, pincode, phone, addressLine1: body.addressLine1 as string, city: body.city as string, isDefault: true };
        log("info", `[http] address book: ${base ? `edited ${base.city || "?"} ${base.pincode} into` : "added"} ${body.city} ${pincode} (contact ${id})`, "address");
      } else {
        log("info", `[http] address book: saved address ${target.city} ${target.pincode} matches${input.addressMatch === "phrase" && input.address.matchPhrase ? ` (phrase "${input.address.matchPhrase}")` : ""} (contact ${target.id})`, "address");
      }
      const refs = cartItemRefIds(R(sum.summary));
      if (!refs.length) throw new Fallback("checkout page shows no cart item ref — cannot select the address; the page flow does it");
      await rome.post("/api/1/action/view", { actionRequestContext: { actionContext: {}, checkoutUpsertGroupRequest: { cartItemRefIds: refs, contactId: target.id, preferredMarketplace: "FLIPKART" }, expressCoFlow: false, isPartialPageLoadEnabled: false, pageNumber: 0, pageUri: "/viewcheckout?view=FLIPKART&marketplace=FLIPKART", type: "CHECKOUT_CHANGE_SHIPPING_ADDRESS" } }, "address select");
      sum = await readSummary();
      if (!sum.addressId || (sum.summaryPin && sum.summaryPin !== pincode)) throw new Fallback(`checkout still on pincode ${sum.summaryPin || "none"} after selecting ${pincode} — the page flow selects the address`);
      lap("address_fix");
    }
    const { summary, track, addressId, summaryPin, serviceable, totalCost, checkoutId, deliveryDays, deliveryText } = sum;
    log("info", `[http] checkout summary · address ${addressId || "none"} · pin ${summaryPin || "?"} serviceable=${String(serviceable)} · total ₹${totalCost ?? "?"} · sla ${deliveryDays ?? "?"} d${deliveryText ? ` (${deliveryText})` : ""} · gst ${track.gbCheckBoxSelected === true ? "ticked" : track.gstApplicable === true ? "available, not ticked" : "n/a"}${checkoutId ? ` · checkout ${checkoutId}` : ""}`, "order-summary");
    if (!addressId) throw new Fallback("no delivery address on the checkout summary — the page flow adds one");
    if (summaryPin && summaryPin !== pincode) throw new Fallback(`checkout address pincode ${summaryPin} is not the job's ${pincode} — the page flow selects the address`);
    if (serviceable === false) return { kind: "failed", failure: new CheckoutFailure("ITEM_NOT_DELIVERABLE", `Pincode ${pincode} is not serviceable for this cart`), timings };
    if (track.addressError === true) throw new Fallback(`address error on checkout: ${String(track.addressErrorReason || "")}`);
    // gstApplicable says the box exists; gbCheckBoxSelected says it is ticked.
    if (input.gstMandatory && track.gbCheckBoxSelected !== true) throw new Fallback("GST invoice required and the GST box is not ticked on this checkout — the page flow ticks it");
    // The checkout's own lines: every product card must be this listing, at this quantity.
    const cards = findAll(R(summary), (o) => o.type === "CART_PAGE_PRODUCT_CARD" && typeof o.data === "object")
      .map((o) => (((o.data as Json).productInfo as Json | undefined)?.baseInfo as Json | undefined)?.value as Json | undefined)
      .filter((v): v is Json => !!v && typeof v.listingId === "string");
    let others = cards.filter((v) => String(v.listingId).toUpperCase() !== ids.lid);
    if (others.length) {
      // Another listing rides on this checkout (left in the cart by an earlier
      // run, or added after our cart check). Take it out here instead of
      // handing the whole order to Chrome: 6 of 30 accounts hit this on 2026-10-30.
      const cartNow = await rome.post("/api/4/page/fetch?cacheFirst=false", VIEWCART_BODY(pincode), "viewcart for checkout cleanup");
      const removable = cartRemoveActions(R(cartNow)).filter((a) => a.listingId !== ids.lid);
      if (!removable.length) {
        const lines = cartLines(R(cartNow));
        log("warn", `[http] checkout holds ${others.map((v) => v.listingId).join(", ")} but the cart page lists ${lines.length ? lines.map((l) => l.listingId).join(", ") : "nothing"} and offers ${cartRemoveActions(R(cartNow)).length} remove action(s)`, "cart");
      }
      if (removable.length) {
        await removeCartLines(rome, removable);
        log("info", `[http] removed ${removable.length} other item(s) from the cart (${removable.map((a) => a.listingId).join(", ")}) and re-read the checkout`, "cart");
        sum = await readSummary();
        const again = findAll(R(sum.summary), (o) => o.type === "CART_PAGE_PRODUCT_CARD" && typeof o.data === "object")
          .map((o) => (((o.data as Json).productInfo as Json | undefined)?.baseInfo as Json | undefined)?.value as Json | undefined)
          .filter((v): v is Json => !!v && typeof v.listingId === "string");
        others = again.filter((v) => String(v.listingId).toUpperCase() !== ids.lid);
      }
      if (others.length) throw new Fallback(`checkout holds ${others.length} other item(s) (${others.map((v) => v.listingId).join(", ")}) — the page flow empties the cart`);
    }
    const qtyOnCheckout = cards.filter((v) => String(v.listingId).toUpperCase() === ids.lid).reduce((a, v) => a + (Number(v.quantity) || 0), 0);
    if (cards.length && qtyOnCheckout !== input.quantity) throw new Fallback(`checkout shows quantity ${qtyOnCheckout}, the job wants ${input.quantity} — the page flow sets it`);
    if (input.cartAmountLimit && totalCost && totalCost > input.cartAmountLimit) {
      return { kind: "failed", failure: new CheckoutFailure("CART_AMOUNT_LIMIT", `Cart total ₹${totalCost} is above the limit ₹${input.cartAmountLimit}`), timings };
    }
    if (input.deliverySlaDays != null && deliveryDays != null && deliveryDays > input.deliverySlaDays) {
      return { kind: "failed", failure: new CheckoutFailure("SLA_EXCEEDED", `Promised delivery ${deliveryDays} day(s)${deliveryText ? ` (${deliveryText})` : ""} is above the limit of ${input.deliverySlaDays}`), timings };
    }
    if (input.deliverySlaDays != null && deliveryDays == null) throw new Fallback("checkout shows no delivery SLA — the page flow reads the delivery text");
    lap("checks");

    // 5. payment token
    const tok = await rome.post("/api/1/action/view", { actionRequestContext: { actionContext: {}, isPartialPageLoadEnabled: false, pageNumber: 1, pageUri: checkoutUri, preferredPayOptions: [], type: "CHECKOUT_PAYMENT_TOKEN_GENERATE" } }, "payment token");
    const tokR = R(tok);
    const tokenUrl = String(findKey(tokR, "originalUrl") || findKey(tokR, "url") || "");
    const token = tokenUrl.match(/[?&]token=([^&#\s"]+)/)?.[1] || "";
    if ((tokR as { actionSuccess?: boolean }).actionSuccess === false || !token) {
      const msgs = ((findKey(tokR, "actionMessages") as Array<{ text?: string }> | null) || []).map((m) => m?.text).filter(Boolean).join(" ");
      throw new Fallback(`no payment token${msgs ? `: ${msgs}` : ""}`);
    }
    lap("payment_token");

    if (input.dryRun) {
      let cartCleaned = false;
      try {
        const cartNow = await rome.post("/api/4/page/fetch?cacheFirst=false", VIEWCART_BODY(pincode), "viewcart for cleanup");
        const mine = cartRemoveActions(R(cartNow)).filter((a) => a.listingId === ids.lid);
        if (mine.length) await removeCartLines(rome, mine);
        const after = await rome.post("/api/4/page/fetch?cacheFirst=false", VIEWCART_BODY(pincode), "viewcart after cleanup");
        cartCleaned = !cartLines(R(after)).some((l) => l.listingId === ids.lid);
      } catch {
        cartCleaned = false;
      }
      lap("cart_cleanup");
      log("info", `[http] DRY RUN — payment token obtained in ${Date.now() - t0} ms; nothing bought; cart ${cartCleaned ? "cleaned" : "NOT cleaned"}`, "dry_run");
      return { kind: "dry_run", token, checkoutTotal: totalCost, deliveryDays, deliveryText, addressId, exitIp, cartCleaned, timings };
    }

    // 6. the payment page, for its cookies
    const payPage = await ctx.get(`${PAY_PAGE}/payments?token=${token}`, { headers: { accept: "text/html,application/xhtml+xml,*/*;q=0.8", referer: "https://www.flipkart.com/" }, timeout: CALL_TIMEOUT_MS });
    if (payPage.status() >= 400) throw new Fallback(`payment page answered ${payPage.status()}`);
    lap("pay_page");

    // 7. the order
    await input.onBeforePay?.();
    let pay: Awaited<ReturnType<APIRequestContext["post"]>>;
    try {
      pay = await ctx.post(`${payApiHost(rome.dc)}/fkpay/api/v3/payments/pay?instrument=COD`, {
      headers: { origin: PAY_PAGE, referer: `${PAY_PAGE}/payments?token=${token}` },
      data: {
        token,
        payment_instrument: "COD",
        remove_captcha_page: true,
        device_information: { colorDepth: 30, javaEnabled: false, javaScriptEnabled: true, language: "en-IN", screenHeight: 915, screenWidth: 412, timeDifference: -330 },
        device_capabilities: { read_sms: false, phonepe_sdk: false, juspay_sdk: false, nda_enabled: false, upi_enabled: false },
        is_diff_shown_to_user: false,
      },
    });
    } catch (err) {
      // Sent, not answered. Flipkart may have the order: this is not a fallback
      // (Chrome would buy it again) and not a retry — a person or the order
      // sync checks the account first.
      const msg = err instanceof Error ? err.message.split("\n")[0] : String(err);
      log("error", `[http] the COD order call got no answer (${msg.slice(0, 160)}) — an order may exist; NOT re-run`, "payment");
      return { kind: "failed", payUnknown: true, failure: new CheckoutFailure("PAYMENT_FAILED", `COD order call sent, no answer: ${msg.slice(0, 160)}. Check the account's orders before anything else.`), timings };
    }
    const payStatus = pay.status();
    const payText = await pay.text();
    let payJson: Json = {};
    try {
      payJson = JSON.parse(payText) as Json;
    } catch {
      /* not JSON */
    }
    lap("pay_cod");
    const responseStatus = String(payJson.response_status || "");
    const params = ((payJson.primary_action as Json | undefined)?.parameters ?? {}) as Record<string, string>;
    const orderId = fullOrderId(params.merchant_reference_id || params.merchant_transaction_id?.split("-")[0] || "");
    if (payStatus === 200 && responseStatus === "SUCCESS" && orderId) {
      const paise = Number(params.transaction_amount || 0);
      let fee = "";
      try {
        const fees = JSON.parse(params.payment_handling_fees_details || "{}") as { applied_charges?: Array<{ type: string; amount: number }> };
        fee = (fees.applied_charges || []).map((c) => `${c.type} ₹${(c.amount / 100).toFixed(0)}`).join("; ");
      } catch {
        /* none */
      }
      // 8. confirmation details, best effort
      const details: HttpPlaced["details"] = {};
      try {
        const conf = await rome.post("/api/4/page/fetch", { requestContext: { type: "ORDER_CONFIRMATION_PAGE", referenceId: shortOrderId(orderId), pageView: "", src: "or", cxTenant: "cs" }, pageType: "ORDER_CONFIRMATION_PAGE", pageUri: "/cx/order_confirmation", locationContext: { pincode: "" }, pageContext: { pageHashKey: null, slotContextMap: null, paginationContextMap: null, paginatedFetch: false, pageNumber: 1, fetchAllPages: false, networkSpeed: 0, trackingContext: null, fetchSeoData: false } }, "order confirmation");
        const unit = ((findKey(R(conf), "ocUnitDetails") as Json[] | undefined) || [])[0] || {};
        details.orderStatus = unit.orderStatus ? String(unit.orderStatus) : undefined;
        details.sellerName = unit.sellerName ? String(unit.sellerName) : undefined;
        details.price = typeof unit.price === "number" ? unit.price : undefined;
        details.mrp = typeof unit.mrp === "number" ? unit.mrp : undefined;
        const sla = (unit.sla as Array<{ maxSla?: number }> | undefined)?.[0];
        details.slaDays = sla?.maxSla ?? null;
      } catch {
        /* the order exists regardless */
      }
      lap("confirmation");
      const verify = await verifyPlaced(rome, orderId, log, input.verifyWaitMs && input.verifyWaitMs >= 5_000 ? input.verifyWaitMs : VERIFY_WAIT_MS);
      lap("verify");
      if (verify.confirmed) log("info", `[http] COD order placed — ${orderId} · ₹${(paise / 100).toFixed(0)}${fee ? ` · ${fee}` : ""} · My Orders: ${verify.listedStatus} after ${verify.verifyMs} ms · ${Date.now() - t0} ms end to end`, "paid");
      return { kind: "placed", orderId, referenceId: shortOrderId(orderId), confirmed: verify.confirmed, listedStatus: verify.listedStatus, verifyMs: verify.verifyMs, amount: paise ? `₹${(paise / 100).toFixed(2)}` : "", paymentFee: fee, checkoutTotal: totalCost, deliveryDays, deliveryText, exitIp, details: { ...details, orderStatus: details.orderStatus || verify.listedStatus || undefined }, timings };
    }
    const message = String(findKey(payJson, "default_text") || findKey(payJson, "message") || payText.slice(0, 200));
    if (payStatus === 401 || payStatus === 403 || payStatus === 429) throw new Fallback(`pay answered ${payStatus} — challenge`);
    if (/cod|cash on delivery/i.test(message) && /not (available|eligible|allowed)|unavailable|disabled/i.test(message)) {
      return { kind: "failed", failure: new CheckoutFailure("COD_UNAVAILABLE", message), timings };
    }
    return { kind: "failed", failure: new CheckoutFailure("PAYMENT_FAILED", `COD order call answered ${payStatus} ${responseStatus || ""}: ${message}`.trim()), timings };
  } catch (err) {
    if (err instanceof Fallback) {
      log("warn", `[http] falling back to the page flow — ${err.message}`, "session");
      return { kind: "fallback", reason: err.message, timings };
    }
    if (err instanceof CheckoutFailure) return { kind: "failed", failure: err, timings };
    const msg = err instanceof Error ? err.message : String(err);
    log("warn", `[http] falling back to the page flow — ${msg.split("\n")[0].slice(0, 200)}`, "session");
    return { kind: "fallback", reason: msg.split("\n")[0].slice(0, 200), timings };
  } finally {
    void addedToCart;
    await ctx?.dispose().catch(() => undefined);
  }
}
