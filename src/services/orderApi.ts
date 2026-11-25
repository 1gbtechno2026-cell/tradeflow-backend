import { request as playwrightRequest, type APIRequestContext } from "playwright";
import type { ITrackingStage, ITrackingStep } from "../models/Order.js";
import { mapPlaywrightCookies } from "./browser.js";

/** "2026-09-23T21:50:50+05:30" — the same IST form orderFetch.toIstIso writes
 *  (kept local so this module never imports the scraper, which imports it).
 *  Flipkart's milliseconds are kept when they are not zero ("…21:50:50.123+05:30",
 *  data-quality U5), so the reference's Last Tracking Step can print them. */
function toIstIso(value?: Date | string | null): string | null {
  const at = value instanceof Date ? value : value ? new Date(value) : null;
  if (!at || Number.isNaN(at.getTime())) return null;
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).formatToParts(at);
  const get = (type: string) => parts.find((p) => p.type === type)?.value || "00";
  const ms = at.getTime() % 1000;
  const fraction = ms ? `.${String(ms < 0 ? ms + 1000 : ms).padStart(3, "0")}` : "";
  return `${get("year")}-${get("month")}-${get("day")}T${get("hour")}:${get("minute")}:${get("second")}${fraction}+05:30`;
}

/**
 * Flipkart's own order APIs, called with a saved session — the two calls the
 * My Orders and order-details pages make for themselves, replayed without a
 * DOM. Verified on 2026-10-05 against real orders (debug/order-api/*): the
 * responses carry every field the page scraper reads, with timestamps instead
 * of text, plus GST / EWB / fee lines the DOM only shows after clicks.
 *
 *   list     GET  2.rome.api.flipkart.com/api/5/self-serve/orders/?page=N&st=…&ot=…
 *   details  POST 2.rome.api.flipkart.com/api/4/page/fetch  (CX_ORDER_DETAIL_PAGE)
 *
 * Nothing here writes to Mongo. mapApiUnitToOrder() produces the same
 * field set scrapeAndSave() writes, so the caller (a test run today, the
 * fetch/update services once approved) decides what to do with it.
 */

/**
 * Flipkart pins every account to a data centre and names it in the host
 * prefix (1.rome… / 2.rome…). A call on the wrong prefix answers
 * 406 {"ERROR_MESSAGE":"DC Change","RESPONSE":{"id":"1"}}. The prefix is kept
 * per session and switched on that answer — until 2026-10-28 it was fixed to
 * "2", and every DC-1 account's order sync failed with "answered HTTP 406".
 */
const DEFAULT_DC = "2";
const sessionDc = new WeakMap<APIRequestContext, string>();
const romeHost = (ctx: APIRequestContext) => `https://${sessionDc.get(ctx) || DEFAULT_DC}.rome.api.flipkart.com`;

/** Run a rome call; on "DC Change" switch this session's prefix and run it once more. */
export async function romeCall<T extends { status(): number; text(): Promise<string> }>(ctx: APIRequestContext, call: (host: string) => Promise<T>): Promise<{ res: T; text: string }> {
  for (let attempt = 0; attempt < 3; attempt++) {
    const res = await call(romeHost(ctx));
    const text = await res.text();
    if (res.status() === 406 && /DC Change/i.test(text)) {
      const id = text.match(/"id"\s*:\s*"(\d+)"/)?.[1];
      if (id && id !== sessionDc.get(ctx)) {
        sessionDc.set(ctx, id);
        continue;
      }
      await new Promise((r) => setTimeout(r, 400));
      continue;
    }
    return { res, text };
  }
  const res = await call(romeHost(ctx));
  return { res, text: await res.text() };
}
const DESKTOP_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

export class OrderApiSessionError extends Error {}
export class OrderApiError extends Error {
  constructor(message: string, public readonly status: number, public readonly body?: string) {
    super(message);
  }
}

/** A request context carrying the account's cookies and the page's headers. */
export async function openOrderApiSession(
  cookies: unknown[],
  opts: { proxy?: { server: string; username?: string; password?: string } } = {}
): Promise<APIRequestContext> {
  const mapped = mapPlaywrightCookies(cookies);
  if (!mapped.length) throw new OrderApiSessionError("Saved Flipkart session has no usable cookies");
  return playwrightRequest.newContext({
    // Through the pool when the caller has a proxy, so a background sync leaves
    // from the same kind of address a checkout does, not the server's own.
    ...(opts.proxy ? { proxy: opts.proxy } : {}),
    storageState: {
      cookies: mapped.map((c) => ({ ...c, expires: c.expires ?? -1 })),
      origins: [],
    },
    extraHTTPHeaders: {
      accept: "*/*",
      "accept-language": "en-US",
      "content-type": "application/json",
      origin: "https://www.flipkart.com",
      referer: "https://www.flipkart.com/",
      "user-agent": DESKTOP_UA,
      // The one header the page adds itself; without it rome answers 4xx.
      "x-user-agent": `${DESKTOP_UA} FKUA/website/42/website/Desktop`,
    },
    timeout: 30_000,
  });
}

// ---- Response shapes (only what is read; everything else stays `unknown`) ----

export interface ApiMoney {
  amount?: number;
  itemListingPrice?: number;
  itemSellingPrice?: number;
  adjustments?: Array<{ adjustmentDescription?: string; adjustmentType?: string; amount?: number }>;
  paymentMethods?: Array<{ paymentMode?: string[]; amounts?: Array<{ value?: number }> ; message?: string }>;
  totalSavings?: number;
}

export interface ApiUnit {
  metaData?: {
    unitId?: string;
    itemId?: string;
    listingId?: string;
    fsn?: string;
    title?: string;
    quantity?: number;
    sellerId?: string;
    trackingId?: string;
    status?: { key?: string; text?: string; fkCancelled?: boolean };
    statusReason?: string | null;
    b2BInfo?: { businessName?: string | null; gstNumber?: string | null; ewbNumber?: string | null } | null;
    deliveryType?: string;
  };
  moneyDataBag?: ApiMoney;
  deliveryDataBag?: {
    promiseDataBag?: {
      /** The first promise, made at checkout. */
      promisedDate?: number | null;
      /** A later promise when Flipkart moved the date; currentSlotEndDate equals it when it exists. */
      newPromisedDate?: number | null;
      currentSlotStartDate?: number | null;
      /** The current promise (end of the delivery slot): newPromisedDate when there is one, else promisedDate. */
      currentSlotEndDate?: number | null;
      /** Flipkart's hand-over time — the reference prints it as Actual Delivery Date. */
      actualDeliveredDate?: number | null;
      deliveryMessage?: string;
      daysLeftToDeliver?: number;
    };
    otpCallout?: unknown;
    handOverInfo?: unknown;
  };
  orderUnitProgressStepsV1?: {
    unitProgressSteps?: Array<{
      nodeTitle?: string;
      stepStatus?: string;
      date?: number | null;
      groupName?: string;
      children?: Array<{
        eventName?: string;
        eventDate?: number | null;
        /** Courier + consignment on the shipped step: "Ekart Logistics - FMPP…". */
        eventLink?: { text?: string | null } | null;
        stepState?: { key?: string; text?: string };
        progressStepInfoList?: Array<{ remark?: string; updatedDate?: number | null; updatedLocation?: string | null }> | null;
      }>;
    }>;
  };
  nonReturnRefundDataBag?: { nonReturnRefunds?: unknown[] };
  /** The refund card of the order page: "Total refund - ₹N" and one sub-widget per money movement
   *  (status Completed / Processed / Accepted, the sentence naming the card). Also carries the
   *  "Replacement Request" card ("We have resent this product"), which is not a refund. */
  actionInfoDataBag?: ApiActionInfo | null;
  cancellationFeeDataBag?: { amount?: number | null; remark?: string | null };
}

export interface ApiActionInfo {
  heading?: { contents?: Array<{ text?: string | null }> | null } | null;
  subWidgets?: Array<{
    status?: string | null;
    subHeading?: string | null;
    heading?: { contents?: Array<{ text?: string | null }> | null } | null;
  }> | null;
}

export interface ApiOrder {
  orderMetaData?: { orderId?: string; orderDate?: number; numberOfItems?: number };
  orderMoneyDataBag?: ApiMoney;
  units?: Record<string, ApiUnit>;
  sellerDataBag?: { sellerDetails?: Record<string, { sellerName?: string }> };
  /** Keyed by FSN (and by listing id): title "MOTOROLA Edge 70 Fusion (Pantone SILHOUETTE, 128 GB)", subTitle "8 GB RAM". */
  productDataBag?: Record<string, { productBasicData?: { title?: string | null; subTitle?: string | null; vertical?: string | null } }>;
}

export interface ApiOrderView extends ApiOrder {
  customerInfo?: { businessName?: string | null; gstNumber?: string | null; phoneNumber?: string; emailId?: string };
  addresses?: Record<string, { name?: string; phoneNumber?: string; pinCode?: string; city?: string; state?: string; addressLine1?: string; addressLine2?: string }>;
  actionEligibilityResponses?: Record<string, { eligible?: boolean; eligibilityReason?: string; actionMessage?: string | null; actionData?: Record<string, unknown> }>;
}

export interface OrderListPage {
  page: number;
  orders: ApiOrder[];
  moreOrder: boolean;
  nextCallParams: Array<{ key: string; value: string }>;
  ms: number;
  bytes: number;
}

/** Socket-level blips (the first fetch on a fresh context hit ECONNRESET on
 *  2026-10-05) are retried; HTTP errors and non-JSON answers are not. */
const TRANSIENT_RE = /ECONNRESET|ETIMEDOUT|ECONNREFUSED|EAI_AGAIN|EPIPE|socket hang up|Timeout \d+ms exceeded|network error/i;

async function withRetry<T>(label: string, fn: () => Promise<T>, attempts = 3): Promise<T> {
  let lastErr: unknown;
  for (let i = 1; i <= attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const msg = err instanceof Error ? err.message : String(err);
      if (err instanceof OrderApiError || err instanceof OrderApiSessionError || !TRANSIENT_RE.test(msg) || i === attempts) throw err;
      await new Promise((r) => setTimeout(r, 500 * i * i));
      console.warn(`[order-api] ${label}: ${msg.split("\n")[0].slice(0, 120)} — retry ${i}/${attempts - 1}`);
    }
  }
  throw lastErr;
}

function parseJsonOrThrow(res: { ok(): boolean; status(): number; url(): string }, text: string): unknown {
  if (!res.ok()) throw new OrderApiError(`${res.url().split("?")[0]} answered HTTP ${res.status()}`, res.status(), text.slice(0, 300));
  try {
    return JSON.parse(text);
  } catch {
    throw new OrderApiSessionError(`${res.url().split("?")[0]} did not answer JSON — the session is probably not logged in`);
  }
}

async function readJson(res: { ok(): boolean; status(): number; text(): Promise<string>; url(): string }): Promise<unknown> {
  const text = await res.text();
  if (!res.ok()) {
    throw new OrderApiError(`${res.url().split("?")[0]} answered HTTP ${res.status()}`, res.status(), text.slice(0, 300));
  }
  try {
    return JSON.parse(text);
  } catch {
    // The login page or an interstitial comes back as HTML with a 200.
    throw new OrderApiSessionError(`${res.url().split("?")[0]} did not answer JSON — the session is probably not logged in`);
  }
}

/** One page of My Orders. Pass the previous page's nextCallParams for the next. */
export async function fetchOrderList(
  ctx: APIRequestContext,
  page = 1,
  nextCallParams: Array<{ key: string; value: string }> = []
): Promise<OrderListPage> {
  const qs = new URLSearchParams({ page: String(page) });
  for (const p of nextCallParams) qs.set(p.key, p.value);
  const started = Date.now();
  const { res, text } = await withRetry(`list page ${page}`, () => romeCall(ctx, (host) => ctx.get(`${host}/api/5/self-serve/orders/?${qs.toString()}`)));
  if (!res.ok()) throw new OrderApiError(`order list answered HTTP ${res.status()}`, res.status(), text.slice(0, 300));
  let json: { RESPONSE?: { multipleOrderDetailsView?: { orders?: ApiOrder[]; moreOrder?: boolean; nextCallParams?: Array<{ key: string; value: string }> } } };
  try {
    json = JSON.parse(text);
  } catch {
    throw new OrderApiSessionError("order list did not answer JSON — the session is probably not logged in");
  }
  const view = json.RESPONSE?.multipleOrderDetailsView;
  if (!view) throw new OrderApiSessionError("order list has no multipleOrderDetailsView — not logged in, or the shape changed");
  return {
    page,
    orders: view.orders || [],
    moreOrder: Boolean(view.moreOrder),
    nextCallParams: (view.nextCallParams || []).map((p) => ({ key: String(p.key), value: String(p.value) })),
    ms: Date.now() - started,
    bytes: text.length,
  };
}

export interface OrderDetailsResult {
  orderView: ApiOrderView;
  /** The whole page/fetch body, for the artifact. */
  raw: unknown;
  ms: number;
  bytes: number;
}

/**
 * The order-details page's data. The full self-serve order view sits inside
 * the DUMMY_CARD_WIDGET slot (ssResponse.orderView); the ORDER_SUMMARY widget
 * carries a subset and is the fallback.
 */
export async function fetchOrderDetails(ctx: APIRequestContext, orderId: string, unitId: string): Promise<OrderDetailsResult> {
  const started = Date.now();
  const { res, text: detailText } = await withRetry(`details ${orderId}/${unitId}`, () => romeCall(ctx, (host) => ctx.post(`${host}/api/4/page/fetch`, {
    data: {
      requestContext: { type: "CX_ORDER_DETAIL_PAGE", orderId, unitId, pageView: "", businessCategory: "" },
      pageType: "CX_ORDER_DETAIL_PAGE",
      pageUri: "/cx/order_detail_desktop",
      locationContext: { pincode: "" },
      pageContext: {
        pageHashKey: null,
        slotContextMap: null,
        paginationContextMap: null,
        paginatedFetch: false,
        pageNumber: 1,
        fetchAllPages: false,
        networkSpeed: 0,
        trackingContext: null,
        fetchSeoData: false,
      },
    },
  })));
  const raw = parseJsonOrThrow(res, detailText);
  const orderView = orderViewFromPageFetch(raw);
  if (!orderView) {
    throw new OrderApiError(`order details for ${orderId}/${unitId} carried neither DUMMY_CARD_WIDGET nor ORDER_SUMMARY_WIDGET_CX`, 200);
  }
  return { orderView, raw, ms: Date.now() - started, bytes: JSON.stringify(raw).length };
}

/** The order view inside a CX_ORDER_DETAIL_PAGE body, or null. Shared with the
 *  trace replay, which re-maps from the raw body on disk. */
export function orderViewFromPageFetch(raw: unknown): ApiOrderView | null {
  const body = raw as { RESPONSE?: { slots?: Array<{ widget?: { type?: string; data?: Record<string, unknown> } }> } };
  const slots = body?.RESPONSE?.slots || [];
  const dummy = slots.find((s) => s.widget?.type === "DUMMY_CARD_WIDGET");
  const orderView = (dummy?.widget?.data?.ssResponse as { orderView?: ApiOrderView } | undefined)?.orderView;
  if (orderView) return orderView;
  const summary = slots.find((s) => s.widget?.type === "ORDER_SUMMARY_WIDGET_CX")?.widget?.data as
    | { orderData?: ApiOrderView; primaryItemData?: ApiUnit }
    | undefined;
  if (summary?.orderData) {
    const view: ApiOrderView = { ...summary.orderData };
    if (summary.primaryItemData?.metaData?.unitId) view.units = { [summary.primaryItemData.metaData.unitId]: summary.primaryItemData };
    return view;
  }
  return null;
}

// ---- Mapping to the order_details document -----------------------------------

function iso(ms?: number | null): string | null {
  return ms ? toIstIso(new Date(ms)) : null;
}
function dateOf(ms?: number | null): Date | null {
  return ms ? new Date(ms) : null;
}
function moneyStr(n?: number | null): string {
  return n == null || !Number.isFinite(Number(n)) ? "" : Number(n).toFixed(2);
}

/**
 * The timeline, in the exact shape parseTracking() builds from the "See all
 * updates" text: one stage per node, DONE/PENDING, each child a step with one
 * progress entry. Dates are real timestamps here, not parsed strings.
 */
export function trackingFromApi(unit: ApiUnit): ITrackingStage[] {
  const nodes = unit.orderUnitProgressStepsV1?.unitProgressSteps || [];
  const stages: ITrackingStage[] = [];
  for (const node of nodes) {
    const done = node.stepStatus === "DONE" || node.stepStatus === "CURRENT";
    const steps: ITrackingStep[] = [];
    for (const child of node.children || []) {
      const key = child.stepState?.key || "";
      if (key === "INACTIVE") continue; // a step that has not happened is not history
      // The step's own time (eventDate) comes first: it is the moment the
      // step happened. The info list holds later notes on the same step — a
      // cancelled item's "has been returned" three days on, a shipment's hub
      // scans — and must not become the step's date.
      const infos = child.progressStepInfoList || [];
      const stepAt = child.eventDate ?? infos[0]?.updatedDate ?? null;
      const progress = [
        { date: iso(stepAt), remark: String(child.stepState?.text || infos[0]?.remark || ""), location: null as string | null },
        ...infos
          .filter((i) => !(i.updatedDate === stepAt && String(i.remark || "") === String(child.stepState?.text || "")))
          .map((i) => ({
            date: iso(i.updatedDate ?? stepAt),
            remark: String(i.remark || ""),
            location: i.updatedLocation ? String(i.updatedLocation) : null,
          })),
      ];
      steps.push({
        event: child.eventName || "trackingStep",
        progress,
        step_text: String(child.stepState?.text || infos[0]?.remark || ""),
      });
    }
    const status = String(node.nodeTitle || node.groupName || "").replace(/^Out For Delivery$/i, "Out for delivery");
    if (!status) continue;
    // The stage's moment is its FIRST step's time (order placed, shipped,
    // delivered, cancelled), as the page shows it and as the reference tool
    // records it — not the node's own date, which on delivered and cancelled
    // nodes can sit hours later (a later scan, a return step).
    stages.push({
      date: steps[0]?.progress[0]?.date || iso(node.date),
      stage: done ? "DONE" : "PENDING",
      status,
      detailed_steps: steps,
    });
  }
  return stages;
}

// ---- Last Tracking Step in the reference's layout (data-quality U5) ------------

/** Characters Flipkart's text carries that Excel shows as junk in a file without a BOM. */
const ASCII_FOR: Record<string, string> = {
  "\u2018": "'",
  "\u2019": "'",
  "\u201C": '"',
  "\u201D": '"',
  "\u20B9": "Rs.",
  "\u00B7": "-",
  "\u2013": "-",
  "\u2014": "-",
  "\u2026": "...",
  "\u00A0": " ",
};

/** One cell of the reference's Last Tracking Step: ASCII only, no "|" (the separator), one line. */
export function asciiCell(value: unknown): string {
  return String(value ?? "")
    .replace(/%E2%82%B9/gi, "Rs.")
    .replace(/[^\x00-\x7F]/g, (ch) => ASCII_FOR[ch] ?? ch.normalize("NFKD").replace(/[^\x00-\x7F]/g, ""))
    .replace(/\|/g, "/")
    .replace(/\s+/g, " ")
    .trim();
}

/** A timeline date as the reference prints it: "…T21:55:34+05:30", or "…T21:55:34.123000+05:30" when it has milliseconds. */
function referenceDate(date: string): string {
  return date.replace(/\.(\d{3})\+05:30$/, ".$1000+05:30");
}

/**
 * The reference's Last Tracking Step: `${date} | ${stage}` for the newest DONE stage, and when that stage's last step
 * carries Flipkart remarks beyond our own first entry, ` | ${step} | ${last remark} | ${last remark date}`.
 * ASCII only. "" when no stage is DONE; the stage alone when it has no date at all.
 * Replaces "Delivered · 2026-10-27T21:55:34+05:30" (U+00B7 on 34,721 units, junk in Excel).
 */
export function referenceLastStep(tracking: ITrackingStage[]): string {
  const done = [...tracking].reverse().find((row) => row.stage === "DONE");
  if (!done) return "";
  const last = done.detailed_steps.slice(-1)[0];
  const when = done.date || last?.progress[0]?.date || null;
  const status = asciiCell(done.status);
  if (!when) return status;
  let out = `${referenceDate(when)} | ${status}`;
  const extra = last && last.progress.length > 1 ? last.progress[last.progress.length - 1] : null;
  const extraAt = extra ? extra.date || last?.progress[0]?.date || null : null;
  if (extra && extraAt) out += ` | ${asciiCell(last!.step_text)} | ${asciiCell(extra.remark)} | ${referenceDate(extraAt)}`;
  return out;
}

// ---- Product name (data-quality U2, owner decision 7) ---------------------------

/**
 * The complete product name, like the reference: Flipkart's title plus its subTitle in brackets
 * ("MOTOROLA Edge 70 Fusion (Pantone SILHOUETTE, 128 GB)" + "8 GB RAM" → "… (Pantone SILHOUETTE, 128 GB) (8 GB RAM)"),
 * unless the title already holds the subtitle. Exported for the backfill.
 */
export function fullProductName(title: unknown, subTitle: unknown): string {
  const base = String(title ?? "").trim();
  const sub = String(subTitle ?? "").trim();
  if (!base) return "";
  return sub && !base.toLowerCase().includes(sub.toLowerCase()) ? `${base} (${sub})` : base;
}

// ---- Refund from the order page's refund card (data-quality U3, decisions 6 and 9) ----

export interface RefundCard {
  /** "HDFC", "ICICI" — every token before the network. */
  bank: string;
  /** "MASTERCARD", "VISA", "RUPAY" — the last token before Credit|Debit. */
  network: string;
  type: "Credit" | "Debit";
  last4: string;
}

export interface RefundFromFlipkart {
  /** "Completed" when every movement is, else the first other status ("Processed", "Accepted"). */
  refund_status: string | null;
  /** "1999.00" from "Total refund - ₹1,999" (else the sum of the movements' own amounts). */
  refund_amount: string | null;
  /** The sentences, joined with " | ". */
  refund_msg: string | null;
  refund_date: Date | null;
  /** "reversal" (the charge was reversed, no money came back), "supercoins", "refund". */
  refund_kind: "reversal" | "supercoins" | "refund";
  card: RefundCard | null;
}

const REPLACEMENT_RE = /we have resent this product/i;
const MONTHS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
let refundShapeLogged = false;

/** Flipkart writes the rupee sign URL-encoded in these texts ("%E2%82%B9"). */
function decodeRupee(text: string): string {
  return text.replace(/%E2%82%B9/gi, "\u20B9");
}
function amountIn(text: string): number | null {
  const m = decodeRupee(text).match(/(?:\u20B9|Rs\.?|INR)?\s*(\d[\d,]*(?:\.\d+)?)/i);
  if (!m) return null;
  const n = Number(m[1].replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}
function contentsText(h: { contents?: Array<{ text?: string | null }> | null } | null | undefined): string {
  return (h?.contents || []).map((c) => String(c?.text ?? "")).filter((t) => t && t !== "null").join(" ").trim();
}
/**
 * Mon D[,] [YYYY][,] H:MM AM|PM — read as IST. A sentence without a year takes the one nearest `ref`: the Refund
 * stage's date when there is one (a "by Nov 23" promise read on Nov 18 belongs to the stage's Nov 23, not last year),
 * else now — and only then never after now + 1 day (`latest`).
 */
function refundDateIn(sentence: string, ref: Date, latest: number | null): Date | null {
  const m = sentence.match(/\b(?:on|by)\s+(?:by\s+)?([A-Za-z]{3})[a-z]*\.?\s+(\d{1,2})(?:,?\s+(\d{4}))?,?\s+(\d{1,2}):(\d{2})\s*([AP]M)/i);
  if (!m) return null;
  const month = MONTHS.indexOf(m[1].toLowerCase());
  if (month < 0) return null;
  const day = Number(m[2]);
  let hour = Number(m[4]) % 12;
  if (/pm/i.test(m[6])) hour += 12;
  const at = (year: number) => new Date(Date.UTC(year, month, day, hour, Number(m[5])) - 330 * 60_000);
  if (m[3]) return at(Number(m[3]));
  const refYear = Number(new Intl.DateTimeFormat("en-GB", { timeZone: "Asia/Kolkata", year: "numeric" }).format(ref));
  const candidates = [refYear - 1, refYear, refYear + 1].map(at).filter((d) => latest == null || d.getTime() <= latest);
  if (!candidates.length) return null;
  return candidates.reduce((best, d) => (Math.abs(d.getTime() - ref.getTime()) < Math.abs(best.getTime() - ref.getTime()) ? d : best));
}
const CARD_RE = /your\s+(.+?)\s+(Credit|Debit)\s+Card\s+[*Xx]+\s*(\d{4})/i;
/** "your HDFC MASTERCARD Credit Card ************1234" → { bank HDFC, network MASTERCARD, type Credit, last4 }. */
export function refundCardIn(sentence: string): RefundCard | null {
  const m = String(sentence || "").match(CARD_RE);
  if (!m) return null;
  const words = m[1].trim().split(/\s+/);
  const network = words.length > 1 ? words.pop()! : "";
  return { bank: words.join(" "), network: network.toUpperCase(), type: /^debit$/i.test(m[2]) ? "Debit" : "Credit", last4: m[3] };
}

/**
 * Flipkart's refund card (units[].actionInfoDataBag) read into the unit's refund fields, or null when there is none:
 * no sub-widgets, or only "We have resent this product" (a replacement, not a refund). Pure.
 * `tracking` gives the Refund stage (the year of a sentence date, and the date when the sentence has none: "by
 * Tomorrow"); without a dated Refund stage the year is the latest not after `now` + 1 day.
 */
export function refundFromActionInfo(unit: Pick<ApiUnit, "actionInfoDataBag">, tracking: ITrackingStage[], now: Date = new Date()): RefundFromFlipkart | null {
  const block = unit.actionInfoDataBag;
  const widgets = (block?.subWidgets || []).filter(Boolean);
  if (!widgets.length) return null;
  const sentences = widgets.map((w) => decodeRupee(String(w.subHeading ?? "")).replace(/\s+/g, " ").trim());
  if (sentences.every((t) => REPLACEMENT_RE.test(t))) return null;
  const money = widgets.filter((_, i) => !REPLACEMENT_RE.test(sentences[i]));
  const moneySentences = sentences.filter((t) => !REPLACEMENT_RE.test(t));

  const heading = contentsText(block?.heading);
  const headingAmount = /total refund/i.test(heading) ? amountIn(heading.replace(/^.*?total refund\s*-?\s*/i, "")) : null;
  const ownAmounts = money.map((w) => amountIn(contentsText(w.heading))).filter((n): n is number => n != null);
  const amount = headingAmount ?? (ownAmounts.length ? ownAmounts.reduce((a, b) => a + b, 0) : null);

  const statuses = money.map((w) => String(w.status ?? "").trim()).filter(Boolean);
  const status = statuses.length ? (statuses.every((x) => /^completed$/i.test(x)) ? "Completed" : statuses.find((x) => !/^completed$/i.test(x))!) : null;

  const joined = moneySentences.join(" ");
  const kind: RefundFromFlipkart["refund_kind"] = /should have been reversed/i.test(joined) ? "reversal" : /supercoins/i.test(joined) ? "supercoins" : "refund";

  const refundStage = [...tracking].reverse().find((s) => /^refund/i.test(s.status) && s.stage === "DONE" && s.date);
  const stageAt = refundStage?.date ? new Date(refundStage.date) : null;
  const dates = moneySentences.map((t) => refundDateIn(t, stageAt || now, stageAt ? null : now.getTime() + 24 * 3600_000)).filter((d): d is Date => Boolean(d));
  const date = dates.length ? new Date(Math.max(...dates.map((d) => d.getTime()))) : stageAt;

  let card: RefundCard | null = null;
  for (const t of moneySentences) {
    card = refundCardIn(t);
    if (card) break;
  }

  if ((amount == null || !status) && !refundShapeLogged) {
    // An answer we cannot read whole: logged once, digits masked; the fields it lacks stay as stored (unitSetFor).
    refundShapeLogged = true;
    console.warn(`[orders-api] refund card of an unknown shape: ${JSON.stringify({ heading, statuses, sentences: moneySentences }).replace(/\d/g, "9").slice(0, 400)}`);
  }
  return {
    refund_status: status,
    refund_amount: amount == null ? null : amount.toFixed(2),
    refund_msg: moneySentences.join(" | ") || null,
    refund_date: date,
    refund_kind: kind,
    card,
  };
}

/** "Ekart Logistics" from the shipped step's link text "Ekart Logistics - FMPP…". */
function shipmentPartnerFrom(unit: ApiUnit): string {
  for (const node of unit.orderUnitProgressStepsV1?.unitProgressSteps || []) {
    for (const child of node.children || []) {
      const text = String(child.eventLink?.text || "");
      const m = text.match(/^(.+?)\s*-\s*[A-Z0-9]{8,}\s*$/i);
      if (m) return m[1].trim();
    }
  }
  return "";
}

/** The fields scrapeAndSave() writes, from the API instead of the DOM. */
export interface ShippingAddress {
  name: string;
  phone: string;
  pincode: string;
  city: string;
  state: string;
  line1: string;
  line2: string;
  /** Everything above in one line, for match-phrase checks and display. */
  text: string;
}

/** Name + phone + pincode + lines, normalised: the parts a person changes when they redirect a parcel. */
export function addressHash(a: ShippingAddress | null | undefined): string {
  if (!a) return "";
  const norm = (v: string) => String(v || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  const key = [norm(a.name), norm(a.phone), norm(a.pincode), norm(a.line1), norm(a.line2), norm(a.city)].join("|");
  if (!key.replace(/\|/g, "").trim()) return "";
  let h = 2166136261;
  for (let i = 0; i < key.length; i++) {
    h ^= key.charCodeAt(i);
    h = Math.imul(h, 16777619) >>> 0;
  }
  return h.toString(16).padStart(8, "0");
}

export function shippingAddressFrom(a: { name?: string; phoneNumber?: string; pinCode?: string; city?: string; state?: string; addressLine1?: string; addressLine2?: string } | null | undefined): ShippingAddress | null {
  if (!a) return null;
  const out: ShippingAddress = {
    name: String(a.name || "").trim(),
    phone: String(a.phoneNumber || "").trim(),
    pincode: String(a.pinCode || "").trim(),
    city: String(a.city || "").trim(),
    state: String(a.state || "").trim(),
    line1: String(a.addressLine1 || "").trim(),
    line2: String(a.addressLine2 || "").trim(),
    text: "",
  };
  out.text = [out.name, out.line1, out.line2, out.city, out.state, out.pincode, out.phone].filter(Boolean).join(", ");
  return out.text ? out : null;
}

export interface MappedOrderUnit {
  order_id: string;
  item_id: string;
  unit_id: string;
  order_url: string;
  order_date: Date | null;
  product_name: string;
  seller_name: string;
  quantity: number;
  total_amount: string;
  unit_amount: string;
  delivery_date: Date | null;
  actual_delivery_date: Date | null;
  ewb_number: string;
  delivery_message: string;
  status_key: string;
  status_label: string;
  status_reason: string | null;
  business_name: string;
  business_gst_no: string;
  tracking_id: string;
  delivery_otp: string | null;
  tracking: ITrackingStage[];
  last_tracking_step: string;
  billing_phone_number: string | null;
  billing_address_name: string | null;
  billing_address_pincode: string | null;
  /** The delivery address Flipkart shows for this unit, and a hash of its
   *  identifying parts — the update worker compares hashes between refreshes
   *  (address change alerts) and the dashboard checks them against the
   *  Address book (address mismatch alerts). */
  shipping_address: ShippingAddress | null;
  shipping_address_hash: string;
  supercoin_amount_applied: number;
  cash_on_delivery: boolean;
  /** From Flipkart's refund card (refundFromActionInfo): "Completed" / "Processed" / "Accepted"; null without one. */
  refund_status: string | null;
  refund_amount: string | null;
  refund_msg: string | null;
  /** When the money came back (or will), from the sentence (IST) or the Refund stage; null without a refund card. */
  refund_date?: Date | null;
  /** "reversal" | "supercoins" | "refund"; null without a refund card. */
  refund_kind?: string | null;
  /** The card the refund sentence names. Not a unit field: saveMappedUnit writes card_name / card_number from it
   *  (card_source "flipkart_refund") only when no checkout job of ours placed the order. */
  refund_card?: RefundCard | null;
  cancelled_date: Date | null;
  cancelled_by_user: boolean;
  /** Who cancelled, from Flipkart's own flag (status.fkCancelled): "flipkart" (seller policy, risk check…), "customer" (the account, us included), "" when not cancelled or unknown. */
  cancelled_by: "" | "flipkart" | "customer";
  /** Flipkart's one-line reason when cancelled ("Sorry, your order was cancelled as per seller policy restrictions applicable to this brand."). */
  cancel_reason_text: string | null;
  order_status_pre_cancellation: string | null;
  shipment_partner_name: string;
  /** Extra, API-only: price lines the DOM never itemises. */
  api_extra: {
    listing_price: string;
    selling_price: string;
    payment_mode: string;
    adjustments: Array<{ description: string; type: string; amount: string }>;
    fsn: string;
    listing_id: string;
    seller_id: string;
    invoice_eligible: boolean | null;
    customer_email: string;
    /** Flipkart's title and subTitle as sent (product_name joins them, decision 7); pairing groups by the raw title. */
    product_title?: string;
    product_subtitle?: string;
    /** The first promise (promisedDate), IST; delivery_date is the current one. */
    first_promised_date?: string | null;
    /** The Delivered stage's time, IST; actual_delivery_date is Flipkart's hand-over time when it sends one. */
    delivered_stage_at?: string | null;
    /** The "Replacement Request" sentence(s) when the refund card is only that ("We have resent this product"). */
    action_info?: string;
    /** Written by saveMappedUnit with card_source "flipkart_refund" only. */
    refund_card?: RefundCard;
  };
}

/**
 * The delivery OTP for an out-for-delivery unit. The API carries it under
 * deliveryDataBag.otpCallout / handOverInfo, but we have never captured a live
 * out-for-delivery unit that actually had one, so the exact shape is unconfirmed
 * (every recorded sample has otpCallout: null). This reads the shapes we can be
 * sure about — a bare 4–6 digit string/number, an "OTP … 123456" string, or a
 * known OTP key on an object — and returns null otherwise. The first time either
 * field is non-null it logs the raw shape, so a real out-for-delivery unit tells
 * us exactly what to lock in. Conservative on purpose: no blind digit-scan, so it
 * never mistakes a pincode / amount for an OTP.
 */
let otpShapeLogged = false;
function pickOtp(v: unknown): string | null {
  if (v == null) return null;
  if (typeof v === "number") return /^\d{4,6}$/.test(String(v)) ? String(v) : null;
  if (typeof v === "string") {
    const t = v.trim();
    if (/^\d{4,6}$/.test(t)) return t;
    const m = v.match(/otp[^0-9]{0,10}(\d{4,6})/i);
    return m ? m[1] : null;
  }
  if (typeof v === "object") {
    const o = v as Record<string, unknown>;
    for (const k of ["otp", "otpCode", "otpValue", "code", "value", "text", "calloutText", "title", "subtitle", "message"]) {
      const got = pickOtp(o[k]);
      if (got) return got;
    }
  }
  return null;
}
function extractDeliveryOtp(dd: ApiUnit["deliveryDataBag"]): string | null {
  if (!dd) return null;
  const otpCallout = dd.otpCallout;
  const handOverInfo = dd.handOverInfo;
  const hasAny = (otpCallout != null && otpCallout !== "") || (handOverInfo != null && handOverInfo !== "");
  if (hasAny && !otpShapeLogged) {
    otpShapeLogged = true;
    try {
      console.log(`[orders-api] delivery OTP field is present — raw shape: ${JSON.stringify({ otpCallout, handOverInfo }).slice(0, 600)}`);
    } catch {
      /* ignore */
    }
  }
  return pickOtp(otpCallout) ?? pickOtp(handOverInfo);
}

/**
 * The delivery OTP as the order-detail API actually carries it. Confirmed live
 * (2026-11-20) against out-for-delivery units the page scraper had an OTP for:
 * deliveryDataBag.otpCallout is ALWAYS null on the customer API, but the OTP sits
 * in a "Share OTP" button widget at
 *   RESPONSE.slots[N].widget.data.buttons.renderableComponents[i]
 *      .action.params.templateReplacements.otpValue
 * The slot index varies (seen at 4/5/6), so scan every slot's buttons. Keyed on
 * the unambiguous otpValue field only — not on button display text, which also
 * holds prices/pincodes — so it never mistakes another 4–6 digit value for an OTP.
 * Returns null when no OTP is on the page (not out for delivery, or delivered).
 */
export function extractOtpFromPageFetch(raw: unknown): string | null {
  const slots = (raw as { RESPONSE?: { slots?: unknown[] } })?.RESPONSE?.slots;
  if (!Array.isArray(slots)) return null;
  for (const slot of slots) {
    const rcs = (slot as { widget?: { data?: { buttons?: { renderableComponents?: unknown[] } } } })?.widget?.data?.buttons?.renderableComponents;
    if (!Array.isArray(rcs)) continue;
    for (const rc of rcs) {
      const v = (rc as { action?: { params?: { templateReplacements?: { otpValue?: unknown } } } })?.action?.params?.templateReplacements?.otpValue;
      const t = String(v ?? "").trim();
      if (/^\d{4,6}$/.test(t)) return t;
    }
  }
  return null;
}

/** `now` only bounds the year of a refund sentence without one ("on Oct 12 03:15 PM"); a trace replay passes the trace time. */
export function mapApiUnitToOrder(view: ApiOrderView, unitId: string, listUnit?: ApiUnit | null, now: Date = new Date()): MappedOrderUnit {
  const unit: ApiUnit = { ...(listUnit || {}), ...((view.units || {})[unitId] || {}) };
  const meta = unit.metaData || {};
  const money = unit.moneyDataBag || {};
  const promise = unit.deliveryDataBag?.promiseDataBag || {};
  const orderId = String(view.orderMetaData?.orderId || "").toUpperCase();
  const itemId = String(meta.itemId || unitId.replace(/000$/, ""));
  const tracking = trackingFromApi(unit);

  const statusKey = String(meta.status?.key || "");
  const statusText = String(meta.status?.text || "");
  const failed = /not placed/i.test(statusKey);
  const cancelledStage = tracking.find((s) => /^cancelled$/i.test(s.status) && s.stage === "DONE");
  // Delivered comes from Flipkart's own key or a DATED Delivered stage, and a Cancelled / Refund stage after the
  // Delivered one wins (T8: a dateless DONE "Delivered" node with a Refund stage after it made a cancelled unit
  // Delivered, OD339115273746442100).
  const deliveredAt = tracking.findIndex((s) => /^delivered$/i.test(s.status) && s.stage === "DONE");
  const deliveredStage = deliveredAt >= 0 && tracking[deliveredAt].date ? tracking[deliveredAt] : undefined;
  // The DONE Cancelled / Refund stage after the Delivered one that undoes it (OD339115273746442100: Delivered with no
  // date, then Refund, no Cancelled stage).
  const endingStage = deliveredAt >= 0 ? tracking.slice(deliveredAt + 1).find((s) => s.stage === "DONE" && /^(cancelled|refund)/i.test(s.status)) : undefined;
  const keyReturned = /^returned$/i.test(statusKey);
  const keyRefunded = /^refund/i.test(statusKey);
  // A return still open ("Return requested", "Return approved") keeps the delivery it follows, as before T8; a
  // Returned / Refund key after such a stage maps to Returned / Refunded, never to Cancelled.
  const keyReturnOpen = /^return/i.test(statusKey) && !keyReturned;
  const deliveryUndone = Boolean(endingStage) && !keyReturnOpen;
  const delivered = (/^delivered$/i.test(statusKey) || Boolean(deliveredStage)) && !deliveryUndone;
  const cancelled = !delivered && (/^cancelled$/i.test(statusKey) || Boolean(cancelledStage) || (deliveryUndone && !keyReturned && !keyRefunded));
  const returned = !delivered && !cancelled && keyReturned;
  const refunded = !delivered && !cancelled && !returned && keyRefunded;
  const current = [...tracking].reverse().find((s) => s.stage === "DONE");
  const confirmed = tracking.find((s) => /confirmed/i.test(s.status));

  const placedStep = tracking
    .flatMap((s) => s.detailed_steps)
    .find((st) => /paymentApprovalStep|approvalOnHoldStep/.test(st.event) || /has been placed|put on hold/i.test(st.step_text));
  // The order's own timestamp, the moment it was created — the page and the
  // reference tool show this; the "placed" step lands 15–50s later.
  const orderDate = dateOf(view.orderMetaData?.orderDate) || (placedStep?.progress[0]?.date ? new Date(placedStep.progress[0].date) : null);

  // The cancellation itself, not the return/refund steps the same node carries.
  const cancelStep = cancelledStage?.detailed_steps.find((st) => /cancel/i.test(st.event) || /cancel/i.test(st.step_text));
  // With no Cancelled stage (T8's shape) the stage that undid the delivery dates the cancellation, not the order date.
  const cancelledAt = cancelStep?.progress[0]?.date || cancelledStage?.date || (cancelled ? endingStage?.date : null) || null;
  // How far the order had got when it was cancelled: the furthest DONE stage
  // before the Cancelled one ("Shipped", "Order Confirmed").
  const furthestBeforeCancel = [...tracking]
    .filter((s) => s.stage === "DONE" && !/cancel|return|refund/i.test(s.status))
    .slice(-1)[0];
  const cancelledText = `${cancelledStage?.detailed_steps.map((s) => `${s.step_text} ${s.progress.map((p) => p.remark).join(" ")}`).join(" ") || ""} ${statusText}`;

  const qty = Number(meta.quantity) || 1;
  const total = moneyStr(money.amount);
  const unitAmount = qty > 1 && total ? (parseFloat(total) / qty).toFixed(2) : total;
  const paymentMode = String(money.paymentMethods?.[0]?.paymentMode?.[0] || "");
  const b2b = meta.b2BInfo || {};
  const billing = view.addresses?.BILLING || view.addresses?.SHIPPING || {};
  const sellerName =
    view.sellerDataBag?.sellerDetails?.[String(meta.sellerId || "")]?.sellerName ||
    Object.values(view.sellerDataBag?.sellerDetails || {})[0]?.sellerName ||
    "";
  const invoice = view.actionEligibilityResponses?.DOWNLOAD_INVOICE;
  // Decision 7: title + subTitle. productDataBag is keyed by FSN (and by listing id).
  const product = (view.productDataBag?.[String(meta.fsn || "")] || view.productDataBag?.[String(meta.listingId || "")])?.productBasicData;
  const productTitle = String(meta.title || product?.title || "").trim();
  const productSubtitle = String(product?.subTitle || "").trim();
  const refund = refundFromActionInfo(unit, tracking, now);
  const replacement = !refund && (unit.actionInfoDataBag?.subWidgets || []).some((w) => /we have resent this product/i.test(String(w?.subHeading || "")));
  const promised = promise.currentSlotEndDate ?? promise.newPromisedDate ?? promise.promisedDate;

  // A cancelled unit reads the cancel step's sentence, never a stage title ("Refund" stood on 3,173 units); with no
  // Cancelled stage (T8's shape) the sentence of the stage that undid the delivery; then Flipkart's status text when
  // it is not one of this unit's stage titles ("Delivered" on T8's shape).
  const isStageTitle = (text: string) => tracking.some((s) => s.status.trim().toLowerCase() === text.trim().toLowerCase());
  const statusLabel = failed
    ? statusText || "Order not placed"
    : delivered
      ? "Your item has been delivered"
      : cancelled
        ? cancelStep?.step_text || (!cancelledStage ? endingStage?.detailed_steps[0]?.step_text : "") || (statusText && !isStageTitle(statusText) ? statusText : "") || "Cancelled"
        : current?.detailed_steps.slice(-1)[0]?.step_text || statusText || current?.status || "";

  return {
    order_id: orderId,
    item_id: itemId,
    unit_id: unitId,
    order_url: `https://www.flipkart.com/order_details?order_id=${orderId}&item_id=${itemId}&unit_id=${unitId}`,
    order_date: orderDate,
    product_name: fullProductName(productTitle, productSubtitle),
    seller_name: String(sellerName || ""),
    quantity: 1,
    total_amount: total,
    unit_amount: unitAmount,
    // A failed order still carries the promise it would have had; the page
    // shows no delivery date for it. A cancelled order keeps its promise,
    // as the reference export does. The CURRENT promise (U4): Flipkart moves
    // it on 164 of 3,930 traced units; the first one stays in api_extra.
    delivery_date: failed ? null : dateOf(promised),
    // Flipkart's hand-over time, the reference's Actual Delivery Date (U16,
    // "reference wins"); the "Delivered" stage — 1–4 h later on most units —
    // stays in the timeline and in api_extra.delivered_stage_at.
    actual_delivery_date: dateOf(promise.actualDeliveredDate) || (deliveredStage?.date ? new Date(deliveredStage.date) : null),
    ewb_number: String(b2b.ewbNumber || ""),
    // The page's banner. The DOM scraper is not consistent here — it saved
    // "Delivered on Sep 17" on a multi-unit order and "Your item has been
    // delivered" on single-unit ones — so the promise line (what the list
    // card shows) is the canonical value from the API.
    delivery_message: String(promise.deliveryMessage || statusText || statusLabel),
    status_key: failed
      ? "Failed"
      : delivered
        ? "Delivered"
        : cancelled
          ? "Cancelled"
          : returned
            ? "Returned"
            : refunded
              ? "Refunded"
              : statusKey || current?.status || "",
    status_label: statusLabel,
    // Only what Flipkart sends as a reason; the cancel sentence is the label now, and cancel_reason_text keeps statusText.
    status_reason: failed ? String(meta.statusReason || statusText || "Payment was not successful") : meta.statusReason ? String(meta.statusReason) : null,
    business_name: String(b2b.businessName || view.customerInfo?.businessName || ""),
    business_gst_no: String(b2b.gstNumber || view.customerInfo?.gstNumber || ""),
    tracking_id: String(meta.trackingId || "").toUpperCase(),
    delivery_otp: extractDeliveryOtp(unit.deliveryDataBag), // otpCallout / handOverInfo — null until a live out-for-delivery unit carries one
    tracking,
    last_tracking_step: referenceLastStep(tracking),
    billing_phone_number: String(billing.phoneNumber || view.customerInfo?.phoneNumber || "") || null,
    billing_address_name: String(billing.name || "").trim() || null,
    billing_address_pincode: String(billing.pinCode || "") || null,
    shipping_address: shippingAddressFrom(view.addresses?.SHIPPING || view.addresses?.BILLING),
    shipping_address_hash: addressHash(shippingAddressFrom(view.addresses?.SHIPPING || view.addresses?.BILLING)),
    supercoin_amount_applied: 0,
    cash_on_delivery: /cash on delivery/i.test(paymentMode),
    // Flipkart's refund card; null without one, which leaves a stored refund as it is (unitSetFor).
    refund_status: refund?.refund_status ?? null,
    refund_amount: refund?.refund_amount ?? null,
    refund_msg: refund?.refund_msg ?? null,
    refund_date: refund?.refund_date ?? null,
    refund_kind: refund ? refund.refund_kind : null,
    refund_card: refund?.card ?? null,
    cancelled_date: cancelledAt ? new Date(cancelledAt) : cancelled ? orderDate : null,
    cancelled_by_user: /as per your request|cancelled by you|you cancelled/i.test(cancelledText),
    cancelled_by: cancelled ? (meta.status?.fkCancelled === true ? "flipkart" : /as per your request|cancelled by you|you cancelled/i.test(cancelledText) ? "customer" : "") : "",
    cancel_reason_text: cancelled ? statusText || null : null,
    order_status_pre_cancellation: cancelled ? furthestBeforeCancel?.status || confirmed?.status || "Order Confirmed" : null,
    shipment_partner_name: shipmentPartnerFrom(unit),
    api_extra: {
      listing_price: moneyStr(money.itemListingPrice),
      selling_price: moneyStr(money.itemSellingPrice),
      payment_mode: paymentMode,
      adjustments: (money.adjustments || []).map((a) => ({
        description: String(a.adjustmentDescription || ""),
        type: String(a.adjustmentType || ""),
        amount: moneyStr(a.amount),
      })),
      fsn: String(meta.fsn || ""),
      listing_id: String(meta.listingId || ""),
      seller_id: String(meta.sellerId || ""),
      invoice_eligible: invoice ? Boolean(invoice.eligible) : null,
      customer_email: String(view.customerInfo?.emailId || ""),
      product_title: productTitle,
      product_subtitle: productSubtitle,
      first_promised_date: iso(promise.promisedDate),
      delivered_stage_at: tracking[deliveredAt]?.date || null,
      ...(replacement
        ? { action_info: (unit.actionInfoDataBag?.subWidgets || []).map((w) => decodeRupee(String(w?.subHeading || "")).trim()).filter(Boolean).join(" | ") }
        : {}),
    },
  };
}

/** Flat per-unit rows from a list page — what discovery needs before details. */
export function listUnits(page: OrderListPage): Array<{ orderId: string; unitId: string; itemId: string; orderDate: Date | null; status: string; statusText: string; amount: string; paymentMode: string; title: string; trackingId: string; unit: ApiUnit }> {
  const out: ReturnType<typeof listUnits> = [];
  for (const order of page.orders) {
    const orderId = String(order.orderMetaData?.orderId || "").toUpperCase();
    for (const [unitId, unit] of Object.entries(order.units || {})) {
      const m = unit.metaData || {};
      out.push({
        orderId,
        unitId,
        itemId: String(m.itemId || unitId.replace(/000$/, "")),
        orderDate: dateOf(order.orderMetaData?.orderDate),
        status: String(m.status?.key || ""),
        statusText: String(m.status?.text || ""),
        amount: moneyStr(unit.moneyDataBag?.amount),
        paymentMode: String(unit.moneyDataBag?.paymentMethods?.[0]?.paymentMode?.[0] || ""),
        title: String(m.title || ""),
        trackingId: String(m.trackingId || ""),
        unit,
      });
    }
  }
  return out;
}
