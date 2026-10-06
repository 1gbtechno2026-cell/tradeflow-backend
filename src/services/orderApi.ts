import { request as playwrightRequest, type APIRequestContext } from "playwright";
import type { ITrackingStage, ITrackingStep } from "../models/Order.js";
import { mapPlaywrightCookies } from "./browser.js";

/** "2026-09-23T21:50:50+05:30" — the same IST form orderFetch.toIstIso writes
 *  (kept local so this module never imports the scraper, which imports it). */
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
  return `${get("year")}-${get("month")}-${get("day")}T${get("hour")}:${get("minute")}:${get("second")}+05:30`;
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

const API_HOST = "https://2.rome.api.flipkart.com";
const DESKTOP_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

export class OrderApiSessionError extends Error {}
export class OrderApiError extends Error {
  constructor(message: string, public readonly status: number, public readonly body?: string) {
    super(message);
  }
}

/** A request context carrying the account's cookies and the page's headers. */
export async function openOrderApiSession(cookies: unknown[]): Promise<APIRequestContext> {
  const mapped = mapPlaywrightCookies(cookies);
  if (!mapped.length) throw new OrderApiSessionError("Saved Flipkart session has no usable cookies");
  return playwrightRequest.newContext({
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
      promisedDate?: number | null;
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
        stepState?: { key?: string; text?: string };
        progressStepInfoList?: Array<{ remark?: string; updatedDate?: number | null; updatedLocation?: string | null }> | null;
      }>;
    }>;
  };
  nonReturnRefundDataBag?: { nonReturnRefunds?: unknown[] };
  cancellationFeeDataBag?: { amount?: number | null; remark?: string | null };
}

export interface ApiOrder {
  orderMetaData?: { orderId?: string; orderDate?: number; numberOfItems?: number };
  orderMoneyDataBag?: ApiMoney;
  units?: Record<string, ApiUnit>;
  sellerDataBag?: { sellerDetails?: Record<string, { sellerName?: string }> };
  productDataBag?: Record<string, { productBasicData?: { title?: string } }>;
}

export interface ApiOrderView extends ApiOrder {
  customerInfo?: { businessName?: string | null; gstNumber?: string | null; phoneNumber?: string; emailId?: string };
  addresses?: Record<string, { name?: string; phoneNumber?: string; pinCode?: string; city?: string; state?: string; addressLine1?: string; addressLine2?: string }>;
  actionEligibilityResponses?: Record<string, { eligible?: boolean; eligibilityReason?: string; actionData?: Record<string, unknown> }>;
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
  const { res, text } = await withRetry(`list page ${page}`, async () => {
    const r = await ctx.get(`${API_HOST}/api/5/self-serve/orders/?${qs.toString()}`);
    return { res: r, text: await r.text() };
  });
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
  const res = await withRetry(`details ${orderId}/${unitId}`, () => ctx.post(`${API_HOST}/api/4/page/fetch`, {
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
  }));
  const raw = await readJson(res);
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
      const infos = child.progressStepInfoList?.length
        ? child.progressStepInfoList
        : [{ remark: child.stepState?.text || "", updatedDate: child.eventDate ?? null, updatedLocation: null }];
      steps.push({
        event: child.eventName || "trackingStep",
        progress: infos.map((i) => ({
          date: iso(i.updatedDate ?? child.eventDate ?? null),
          remark: String(i.remark || child.stepState?.text || ""),
          location: i.updatedLocation ? String(i.updatedLocation) : null,
        })),
        step_text: String(child.stepState?.text || infos[0]?.remark || ""),
      });
    }
    const status = String(node.nodeTitle || node.groupName || "").replace(/^Out For Delivery$/i, "Out for delivery");
    if (!status) continue;
    stages.push({
      date: iso(node.date ?? steps[0]?.progress[0]?.date ? new Date(String(steps[0]?.progress[0]?.date)).getTime() : null) ?? (node.date ? iso(node.date) : null),
      stage: done ? "DONE" : "PENDING",
      status,
      detailed_steps: steps,
    });
  }
  return stages;
}

function lastTrackingStep(tracking: ITrackingStage[]): string {
  const done = [...tracking].reverse().find((row) => row.stage === "DONE");
  if (!done) return "";
  const last = done.detailed_steps.slice(-1)[0];
  const when = last?.progress[0]?.date || done.date;
  return when ? `${done.status} · ${when}` : done.status;
}

/** The fields scrapeAndSave() writes, from the API instead of the DOM. */
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
  supercoin_amount_applied: number;
  cash_on_delivery: boolean;
  refund_status: string | null;
  refund_amount: string | null;
  refund_msg: string | null;
  cancelled_date: Date | null;
  cancelled_by_user: boolean;
  order_status_pre_cancellation: string | null;
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
  };
}

export function mapApiUnitToOrder(view: ApiOrderView, unitId: string, listUnit?: ApiUnit | null): MappedOrderUnit {
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
  const deliveredStage = tracking.find((s) => /^delivered$/i.test(s.status) && s.stage === "DONE");
  const delivered = /^delivered$/i.test(statusKey) || Boolean(deliveredStage);
  const cancelled = !delivered && (/^cancelled$/i.test(statusKey) || Boolean(cancelledStage));
  const returned = !delivered && !cancelled && /^returned$/i.test(statusKey);
  const refunded = !delivered && !cancelled && !returned && /^refund/i.test(statusKey);
  const current = [...tracking].reverse().find((s) => s.stage === "DONE");
  const confirmed = tracking.find((s) => /confirmed/i.test(s.status));

  const placedStep = tracking
    .flatMap((s) => s.detailed_steps)
    .find((st) => /paymentApprovalStep|approvalOnHoldStep/.test(st.event) || /has been placed|put on hold/i.test(st.step_text));
  const orderDate = placedStep?.progress[0]?.date ? new Date(placedStep.progress[0].date) : dateOf(view.orderMetaData?.orderDate);

  const cancelledAt = cancelledStage?.detailed_steps[0]?.progress[0]?.date || cancelledStage?.date || null;
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
  const refunds = unit.nonReturnRefundDataBag?.nonReturnRefunds || [];
  const invoice = view.actionEligibilityResponses?.DOWNLOAD_INVOICE;

  const statusLabel = failed
    ? statusText || "Order not placed"
    : delivered
      ? "Your item has been delivered"
      : current?.detailed_steps.slice(-1)[0]?.step_text || statusText || current?.status || "";

  return {
    order_id: orderId,
    item_id: itemId,
    unit_id: unitId,
    order_url: `https://www.flipkart.com/order_details?order_id=${orderId}&item_id=${itemId}&unit_id=${unitId}`,
    order_date: orderDate,
    product_name: String(meta.title || view.productDataBag?.[String(meta.fsn || "")]?.productBasicData?.title || ""),
    seller_name: String(sellerName || ""),
    quantity: 1,
    total_amount: total,
    unit_amount: unitAmount,
    // A failed order still carries the promise it would have had; the page
    // shows no delivery date for it and neither does the scraper.
    delivery_date: failed || cancelled ? null : dateOf(promise.promisedDate),
    // The "Delivered" timeline step is what the page (and so the scraper)
    // calls the delivery time; promiseDataBag.actualDeliveredDate is an
    // earlier hand-over scan, hours before it on every unit compared.
    actual_delivery_date: (deliveredStage?.date ? new Date(deliveredStage.date) : null) || dateOf(promise.actualDeliveredDate),
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
    status_reason: failed
      ? String(meta.statusReason || statusText || "Payment was not successful")
      : cancelled
        ? cancelledStage?.detailed_steps[0]?.step_text || statusText || "Cancelled on Flipkart"
        : meta.statusReason
          ? String(meta.statusReason)
          : null,
    business_name: String(b2b.businessName || view.customerInfo?.businessName || ""),
    business_gst_no: String(b2b.gstNumber || view.customerInfo?.gstNumber || ""),
    tracking_id: String(meta.trackingId || "").toUpperCase(),
    delivery_otp: null, // deliveryDataBag.otpCallout — not yet seen on a live out-for-delivery unit
    tracking,
    last_tracking_step: lastTrackingStep(tracking),
    billing_phone_number: String(billing.phoneNumber || view.customerInfo?.phoneNumber || "") || null,
    billing_address_name: String(billing.name || "").trim() || null,
    billing_address_pincode: String(billing.pinCode || "") || null,
    supercoin_amount_applied: 0,
    cash_on_delivery: /cash on delivery/i.test(paymentMode),
    refund_status: refunds.length || refunded ? "refund" : null,
    refund_amount: null,
    refund_msg: null,
    cancelled_date: cancelledAt ? new Date(cancelledAt) : cancelled ? orderDate : null,
    cancelled_by_user: /as per your request|cancelled by you|you cancelled/i.test(cancelledText),
    order_status_pre_cancellation: cancelled ? confirmed?.status || "Order Confirmed" : null,
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
