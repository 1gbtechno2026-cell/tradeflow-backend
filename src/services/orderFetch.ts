import os from "node:os";
import type { Browser, BrowserContext, Page } from "playwright";
import type { Types } from "mongoose";
import { PlatformId } from "../models/PlatformId.js";
import { Order, type ITrackingStage, type ITrackingStep } from "../models/Order.js";
import { OrderIndex } from "../models/OrderIndex.js";
import { nextSeq } from "../models/Counter.js";
import { config } from "../config.js";
import { evalOnPage, isDestroyedContext } from "../lib/evalOnPage.js";
import type { IndexScrapeStatus } from "./orderLifecycle.js";
import { assertIdleForFetch, markFetchRunning } from "./orderSyncLock.js";
import {
  blockFlipkartLogout,
  clearLocalThrowawayFlipkartCookies,
  closeBrowser,
  flipkartLoginUrl,
  launchStealthContext,
  restoreFlipkartSession,
  sleep,
} from "./browser.js";

export interface FetchLog {
  time: string;
  level: "info" | "warn" | "error";
  message: string;
}

export interface FetchReport {
  startedAt: string | null;
  finishedAt: string | null;
  elapsedMs: number;
  accountsQueued: number;
  accountsDone: number;
  idsFound: number;
  idsDiscovered: number;
  idsNew: number;
  idsDuplicate: number;
  idsOpened: number;
  enrichQueued: number;
  idsScraped: number;
  idsSkippedClosed: number;
  idsSkippedOld: number;
  idsFailed: number;
  invoicesDownloaded: number;
  imeiCaptured: number;
  withProduct: number;
  withTracking: number;
  withGst: number;
}

interface FetchJobState {
  running: boolean;
  cancelled: boolean;
  phase: "discover" | "enrich" | "";
  currentEmail: string | null;
  logs: FetchLog[];
  total: number;
  done: number;
  failed: number;
  skipped: number;
  savedOrders: number;
  sinceDate: string | null;
  sinceLabel: string;
  sinceCutoff: Date;
  dateFilter: boolean;
  windows: number;
  report: FetchReport;
  liveBrowsers: Array<{ browser: Browser; context: BrowserContext }>;
}

const jobs = new Map<string, FetchJobState>();
const ORDERS_URL = "https://www.flipkart.com/account/orders?link=home_orders";

function fetchWindowCount(emailCount: number) {
  const gb = os.totalmem() / 1024 ** 3;
  const byRam = gb >= 28 ? 5 : gb >= 14 ? 3 : 2;
  const env = config.fetchWindowConcurrency || Number(process.env.FETCH_WINDOW_CONCURRENCY || 0);
  const cap = env > 0 ? env : byRam;
  return Math.max(1, Math.min(emailCount, cap, env > 0 ? env : 4));
}

function formatSinceLabel(at: Date) {
  return at.toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric", timeZone: "Asia/Kolkata" });
}

const READ_LIST_CARDS = `() => {
  const seen = new Set();
  const out = [];
  const origin = location.origin || "https://www.flipkart.com";
  const statusRe = /Delivered|Cancelled|On the way|Arriving|Refund|Returned|Failed|Confirmed|Shipped|Out for delivery/i;
  const parseHref = (href) => {
    try {
      const url = new URL(href, origin);
      return {
        orderId: (url.searchParams.get("order_id") || "").toUpperCase(),
        itemId: url.searchParams.get("item_id") || "",
        unitId: url.searchParams.get("unit_id") || "",
        href: url.href,
      };
    } catch (e) {
      return null;
    }
  };
  const cardMeta = (link) => {
    let container = link.closest("div");
    let depth = 10;
    while (container && depth > 0) {
      const text = container.innerText || "";
      if (statusRe.test(text)) break;
      container = container.parentElement;
      depth--;
    }
    const raw = (container && container.innerText) || link.innerText || "";
    const lines = raw.split("\\n").map((t) => t.trim()).filter(Boolean);
    return {
      productName: lines[0] || "",
      status: lines.find((t) => statusRe.test(t)) || "",
      amount: (raw.match(/₹\\s*([\\d,]+(?:\\.\\d+)?)/) || [])[1] || "",
      text: raw.replace(/\\s+/g, " ").trim().slice(0, 900),
    };
  };
  const fallbackUnit = (orderId, itemId, unitId) => {
    if (unitId) return unitId;
    if (itemId) return itemId + "000";
    return String(orderId || "").replace(/^OD/i, "") + "000";
  };
  const add = (parsed, meta) => {
    if (!parsed || !parsed.orderId) return;
    if (!/^OD[A-Z0-9]{10,}$/.test(parsed.orderId) && !/^[A-Z0-9]{16,}$/.test(parsed.orderId)) return;
    const unitId = fallbackUnit(parsed.orderId, parsed.itemId, parsed.unitId);
    if (seen.has(unitId)) return;
    seen.add(unitId);
    const url = new URL(parsed.href);
    if (parsed.itemId && !url.searchParams.get("item_id")) url.searchParams.set("item_id", parsed.itemId);
    if (!url.searchParams.get("unit_id")) url.searchParams.set("unit_id", unitId);
    out.push({
      orderId: parsed.orderId,
      itemId: parsed.itemId || "",
      unitId,
      orderUrl: url.href,
      amount: meta.amount || "",
      productName: meta.productName || "",
      status: meta.status || "",
      text: meta.text || "",
    });
  };
  const withUnit = [];
  const withoutUnit = [];
  for (const link of document.querySelectorAll('a[href*="order_details"], a[href*="order_id="]')) {
    const parsed = parseHref(link.getAttribute("href") || link.href || "");
    if (!parsed || !parsed.orderId) continue;
    const row = { parsed, meta: cardMeta(link) };
    if (parsed.unitId) withUnit.push(row);
    else withoutUnit.push(row);
  }
  for (const row of withUnit) add(row.parsed, row.meta);
  const covered = new Set(withUnit.map((row) => row.parsed.orderId));
  for (const row of withoutUnit) {
    if (covered.has(row.parsed.orderId)) continue;
    add(row.parsed, row.meta);
  }
  return out;
}`;

const SCROLL_ORDERS = `() => {
  const nodes = [document.scrollingElement, document.documentElement, document.body].concat(
    Array.from(document.querySelectorAll("div"))
  );
  let best = document.documentElement;
  let bestExtra = 0;
  for (const el of nodes) {
    if (!el || !el.scrollHeight) continue;
    const extra = el.scrollHeight - (el.clientHeight || 0);
    if (extra > bestExtra) {
      bestExtra = extra;
      best = el;
    }
  }
  best.scrollTop = best.scrollHeight;
  window.scrollBy(0, Math.max(900, window.innerHeight || 800));
  window.scrollTo(0, document.documentElement.scrollHeight);
  for (const el of document.querySelectorAll("div, span, button, a")) {
    const t = (el.textContent || "").replace(/\\s+/g, " ").trim();
    if (t.length < 28 && /^(load more|show more|view more|see more orders)$/i.test(t)) {
      el.click();
      break;
    }
  }
  return best.scrollHeight + window.scrollY;
}`;

const CLICK_SHOW_MORE_ORDERS = `() => {
  const named = document.querySelector("button.dSM5Ub, button.dDeuVV");
  if (named && /show more orders/i.test(named.textContent || "")) {
    named.click();
    return "class";
  }
  for (const el of document.querySelectorAll("button, span, div, a")) {
    const t = (el.textContent || "").replace(/\\s+/g, " ").trim();
    if (/^show more orders$/i.test(t)) {
      (el.closest("button") || el).click();
      return "text";
    }
  }
  return null;
}`;

const EXPAND_PRICE_DETAILS = `() => {
  const hasInvoice = Array.from(document.querySelectorAll("div, span, a, button")).some(
    (el) => (el.textContent || "").trim() === "Download Invoice"
  );
  if (hasInvoice) return "visible";
  const block = document.querySelector('[data-tat-id="OD_PRICE_BLOCK"]');
  if (block) {
    block.click();
    return "od-price-block";
  }
  for (const el of document.querySelectorAll('[role="button"]')) {
    if (/price details/i.test(el.textContent || "") && (el.textContent || "").length < 80) {
      el.click();
      return "price-details";
    }
  }
  for (const el of document.querySelectorAll("span, div")) {
    const t = (el.textContent || "").replace(/\\s+/g, " ").trim();
    if (/^Paid ₹[\\d,]+(?:\\.\\d+)? by /i.test(t) && t.length < 90) {
      (el.closest('[role="button"]') || el.parentElement || el).click();
      return "paid-line";
    }
  }
  const chevron = document.querySelector('img[src*="promos"][width="16"], img[src*="promos"][height="16"]');
  if (chevron) {
    const wrap = chevron.closest('[role="button"]') || chevron.parentElement;
    if (wrap) {
      wrap.click();
      return "chevron";
    }
  }
  return null;
}`;

const CLICK_DOWNLOAD_INVOICE = `() => {
  const els = document.querySelectorAll("div, span, a, button");
  for (const el of els) {
    const t = (el.textContent || "").trim();
    if (t === "Download Invoice" && el.childElementCount <= 1) {
      el.click();
      return true;
    }
  }
  for (const el of els) {
    if ((el.textContent || "").trim() === "Download Invoice") {
      el.click();
      return true;
    }
  }
  return false;
}`;

const SCROLL_LAST_ORDER = `() => {
  const cards = Array.from(document.querySelectorAll('a[href*="order_id"], a[href*="order_details"]'));
  const last = cards[cards.length - 1];
  if (last) last.scrollIntoView({ block: "end", behavior: "instant" });
  window.scrollBy(0, 1400);
  return cards.length;
}`;

const CLICK_SEE_ALL_UPDATES = `() => {
  const preferred = document.querySelector("div.RMdaeX");
  if (preferred && /see all updates/i.test(preferred.textContent || "")) {
    preferred.click();
    return "class";
  }
  const nodes = document.querySelectorAll("div, span, a, button");
  for (const el of nodes) {
    const text = (el.textContent || "").replace(/\\s+/g, " ").trim();
    if (/^see all updates/i.test(text)) {
      el.click();
      return "text";
    }
  }
  return null;
}`;

const CLOSE_TRACKING = `() => {
  const buttons = document.querySelectorAll("button, div, span, img, svg, [role='button']");
  for (const el of buttons) {
    const aria = (el.getAttribute("aria-label") || "").toLowerCase();
    const title = (el.getAttribute("title") || "").toLowerCase();
    if (aria.includes("close") || title.includes("close") || aria.includes("dismiss")) {
      el.click();
      return "aria";
    }
  }
  document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", keyCode: 27, bubbles: true }));
  return "escape";
}`;

const EXPAND_ORDER_SECTIONS = `() => {
  const clickToggle = (label) => {
    const nodes = Array.from(document.querySelectorAll("div,span,button,h2,h3,section"));
    for (const n of nodes) {
      const t = (n.textContent || "").replace(/\\s+/g, " ").trim();
      if (t !== label) continue;
      let el = n;
      for (let i = 0; i < 6 && el; i++) {
        el.click();
        el.dispatchEvent(new MouseEvent("click", { bubbles: true }));
        if (el.parentElement && (el.parentElement.tagName === "BUTTON" || el.parentElement.getAttribute("role") === "button")) {
          el.parentElement.click();
        }
        el = el.parentElement;
      }
      return true;
    }
    return false;
  };
  const text = document.body && document.body.innerText ? document.body.innerText : "";
  const deliveryOpen = /Delivery details[\\s\\S]{0,500}(?:Cabin|UDYOG|PHASE)[\\s\\S]{0,200}[6-9]\\d{9}/i.test(text);
  if (!deliveryOpen) clickToggle("Delivery details");
  if (!/Total amount/i.test(text)) clickToggle("Price details");
  return true;
}`;

const SCROLL_GST = `() => {
  const nodes = Array.from(document.querySelectorAll("div, h2, h3, span, p"));
  const gst = nodes.find((el) => {
    const t = (el.textContent || "").replace(/\\s+/g, " ").trim();
    return /GST\\s*&\\s*EWB/i.test(t) && t.length < 60;
  });
  if (gst) {
    gst.scrollIntoView({ block: "center", behavior: "instant" });
    return "gst";
  }
  window.scrollTo({ top: document.documentElement.scrollHeight, behavior: "instant" });
  return "bottom";
}`;

const READ_PAGE = `() => {
  const raw = document.body && document.body.innerText ? document.body.innerText : "";
  const compact = raw.replace(/[ \\t]+/g, " ");
  const url = window.location.href;
  const unitId = (url.match(/unit_id=([A-Za-z0-9]+)/i) || [])[1] || "";
  const itemId = (url.match(/item_id=([A-Za-z0-9]+)/i) || [])[1] || "";
  const gstBlock = (compact.match(/GST\\s*&?\\s*EWB[\\s\\S]{0,500}/i) || compact.match(/GST\\s*Number[\\s\\S]{0,400}/i) || [""])[0];
  const gstNumber =
    (gstBlock.match(/GST\\s*Number\\s*([0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][A-Z0-9]Z[A-Z0-9])/i) || [])[1] ||
    (compact.match(/\\b([0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][A-Z0-9]Z[A-Z0-9])\\b/i) || [])[1] ||
    "";
  const ewbNumber =
    (gstBlock.match(/EWB\\s*Number\\s*([0-9]{10,15})/i) || [])[1] ||
    (compact.match(/EWB\\s*Number\\s*([0-9]{10,15})/i) || [])[1] ||
    "";
  const gstSellerName =
    (gstBlock.match(/GST\\s*Number\\s*[0-9A-Z]+\\s*([A-Za-z][A-Za-z0-9 .,&'-]{4,80}?)\\s*EWB/i) || [])[1] || "";
  const sellerName = (compact.match(/Seller:\\s*([^\\n₹]{2,80})/i) || [])[1] || "";
  const totalRaw =
    (compact.match(/Total amount\\s*₹\\s*([\\d,]+(?:\\.\\d+)?)/i) || [])[1] ||
    (compact.match(/₹\\s*([\\d,]+(?:\\.\\d+)?)/) || [])[1] ||
    "";
  const qty = parseInt((compact.match(/Qty[:\\s]+(\\d+)/i) || [])[1] || "1", 10) || 1;
  const supercoin = parseFloat(
    (compact.match(/Super\\s*Coin[s]?[^\\d₹]{0,20}₹?\\s*([\\d,]+(?:\\.\\d+)?)/i) || [])[1] ||
      (compact.match(/([\\d,]+)\\s*Super\\s*Coin/i) || [])[1] ||
      "0"
  );
  const otp =
    (compact.match(/\\bPIN[:\\s]+([0-9]{3,8})\\b/i) || [])[1] ||
    (compact.match(/delivery\\s*OTP[:\\s]+([0-9]{3,8})/i) || [])[1] ||
    (compact.match(/\\bOTP[:\\s]+([0-9]{4,8})\\b/i) || [])[1] ||
    "";
  const deliveryBlock = (() => {
    const headings = Array.from(document.querySelectorAll("div,span,h2,h3,section,button"));
    for (const heading of headings) {
      const label = (heading.textContent || "").replace(/\\s+/g, " ").trim();
      if (label !== "Delivery details") continue;
      let cur = heading.parentElement;
      for (let i = 0; i < 8 && cur; i++) {
        const txt = (cur.innerText || "").replace(/\\s+/g, " ").trim();
        if (txt.length > 20 && txt.length < 1200 && /PRIVATE LIMITED|[6-9]\\d{9}|Cabin|UDYOG/i.test(txt)) {
          return cur.innerText || txt;
        }
        cur = cur.parentElement;
      }
    }
    const raw = document.body && document.body.innerText ? document.body.innerText : "";
    const cut = raw.match(/Delivery details\\s*([\\s\\S]*?)(?:Price details|GST\\s*&\\s*EWB)/i);
    return cut ? cut[1] : "";
  })();
  const cleanCompany = (s) => String(s || "")
    .replace(/^Order\\s+/i, "")
    .replace(/\\s+Order\\b/gi, "")
    .replace(/\\s+/g, " ")
    .trim();
  const phone =
    (deliveryBlock.match(/(?:LIMITED|LTD\\.?|LLP)\\s*([6-9][0-9]{9})/) || [])[1] ||
    (deliveryBlock.match(/(?:^|[^0-9])([6-9][0-9]{9})(?:[^0-9]|$)/) || [])[1] ||
    "";
  const pincode =
    (deliveryBlock.match(/\\b([1-9][0-9]{5})\\b/) || [])[1] ||
    "";
  const billingName = cleanCompany(
    (deliveryBlock.match(/([A-Z][A-Z0-9 .,&'-]{2,70}\\s+(?:PRIVATE LIMITED|PVT\\.?\\s*LTD\\.?|LLP))/i) || [])[1] ||
    ""
  );
  const skipProduct = /seller|order received|order confirmed|see all|download invoice|gst|ewb|flipkart|payment successful|out for delivery|delivered to|rate your/i;
  const looksProduct = (t) => {
    const s = String(t || "").replace(/\\s+/g, " ").trim();
    if (s.length < 8 || s.length > 240 || skipProduct.test(s)) return "";
    if (/\\([^)]{2,}\\)/.test(s) || /\\b(?:GB|RAM|5G|4G|ml|cm|kg)\\b/i.test(s)) return s;
    return "";
  };
  const candidates = [];
  for (const el of Array.from(document.querySelectorAll("a[href*='/p/'], a[href*='pid='], img[alt], h1, h2, [title]"))) {
    candidates.push(looksProduct(el.getAttribute("title") || ""));
    candidates.push(looksProduct(el.getAttribute("alt") || ""));
    const own = (el.childElementCount ? "" : (el.textContent || "")).replace(/\\s+/g, " ").trim();
    if (own.length <= 220) candidates.push(looksProduct(own));
  }
  const lines = compact.split("\\n").map((l) => l.trim()).filter(Boolean);
  const sellerAt = lines.findIndex((l) => /^Seller:/i.test(l));
  if (sellerAt > 0) {
    for (let i = Math.max(0, sellerAt - 6); i < sellerAt; i++) candidates.push(looksProduct(lines[i]));
  }
  let productName = "";
  for (const name of candidates.filter(Boolean)) {
    const better =
      name.length > productName.length ||
      (/\\([^)]*(?:GB|RAM|ml)[^)]*\\)/i.test(name) && !/\\([^)]*(?:GB|RAM|ml)[^)]*\\)/i.test(productName));
    if (better) productName = name;
  }
  if (productName && !/\\([^)]+\\)/.test(productName)) {
    const idx = lines.findIndex((l) => l === productName);
    const next = idx >= 0 ? lines[idx + 1] : "";
    if (next && next.length < 60 && !/^Seller:|^₹/i.test(next)) productName = productName + " (" + next + ")";
  }
  const deliveryMessage =
    (compact.match(/((?:Delivered on|Out For Delivery|Expected By|Payment Successful)[^\\n]{0,80})/i) || [])[1] || "";
  return {
    url,
    unitId,
    itemId,
    productName: productName.replace(/\\s+/g, " ").trim(),
    sellerName: sellerName.replace(/\\s+/g, " ").trim(),
    gstSellerName: gstSellerName.replace(/\\s+/g, " ").trim(),
    gstNumber: gstNumber.toUpperCase(),
    ewbNumber,
    totalAmount: totalRaw.replace(/,/g, ""),
    quantity: qty,
    supercoin: Number.isFinite(supercoin) ? supercoin : 0,
    deliveryOtp: otp,
    billingPhone: phone,
    billingName: billingName.replace(/\\s+/g, " ").trim(),
    pincode,
    deliveryMessage: deliveryMessage.replace(/\\s+/g, " ").trim(),
    cashOnDelivery: /cash on delivery|\\bCOD\\b/i.test(compact),
    pageText: compact.slice(0, 18000),
  };
}`;

const READ_TRACKING = `() => {
  const pick = (el) => (el && el.innerText ? el.innerText.replace(/[ \\t]+/g, " ").trim() : "");
  const selectors = '[role="dialog"], [class*="modal" i], [class*="Modal"], [class*="drawer" i], [class*="overlay" i], [data-omtr-id]';
  let best = "";
  for (const el of Array.from(document.querySelectorAll(selectors))) {
    const t = pick(el);
    if (/order received|order confirmed|your order has been placed/i.test(t) && t.length > best.length && t.length < 20000) {
      best = t;
    }
  }
  if (!best) {
    for (const el of Array.from(document.querySelectorAll("div"))) {
      const t = pick(el);
      if (/your order has been placed/i.test(t) && /shipped|delivered|out for delivery|order received/i.test(t) && t.length > 80 && t.length < 12000 && t.length > best.length) {
        best = t;
      }
    }
  }
  return best || pick(document.body).slice(0, 14000);
}`;

const READ_FAIL_STATE = `() => {
  const text = (document.body && document.body.innerText ? document.body.innerText : "").replace(/[ \\t]+/g, " ");
  const html = ((document.documentElement && document.documentElement.innerHTML) || "")
    .replace(/&quot;/g, '"')
    .replace(/&#34;/g, '"');
  const all = (key) => {
    const re = new RegExp('"' + key + '"\\\\s*:\\\\s*"([^"]+)"', "g");
    const out = [];
    let m;
    while ((m = re.exec(html))) out.push(String(m[1]).replace(/\\\\n/g, " ").trim());
    return out;
  };
  const statusDisplays = all("status_display");
  const stageDisplays = all("stage_display");
  const errorCodes = all("error_code");
  const errorCodeDisplays = all("error_code_display");
  const errorSources = all("error_source");
  const errorDetails = all("error_details");
  const statusDisplay = statusDisplays.find((v) => /failed/i.test(v)) || statusDisplays[0] || "";
  const stageDisplay = stageDisplays[0] || "";
  const errorCode = errorCodes.find((v) => /TECHNICAL_ERROR|PAYMENT|BANK|FAIL/i.test(v)) || errorCodes[0] || "";
  const errorCodeDisplay = errorCodeDisplays[0] || "";
  const errorSource = errorSources[0] || "";
  const errorDetail = errorDetails[0] || "";
  const notPlaced = /order not placed|could not be placed|payment was not successful|payment was not confirmed/i.test(text);
  const failedJson = /failed/i.test(statusDisplay) || /TECHNICAL_ERROR|PAYMENT.*FAIL|ERROR_AT_BANK/i.test(errorCode);
  const failedHeading = /failed:\\s*order confirmation/i.test(text);
  if (!notPlaced && !failedJson && !failedHeading) return null;
  const label =
    (failedHeading && (text.match(/Failed:\\s*Order Confirmation/i) || [])[0]) ||
    (stageDisplay ? "Failed: " + stageDisplay : "") ||
    (text.match(/Order not placed[^.\\n]{0,60}/i) || [])[0] ||
    "Order not placed";
  const reason =
    errorDetail ||
    errorCodeDisplay ||
    (text.match(/A technical error occurred on the bank[^.]{0,80}\\.?/i) || [])[0] ||
    (text.match(/Your Payment was not confirmed[^.]{0,80}\\.?/i) || [])[0] ||
    (text.match(/Due to an error with the payment[^.]{0,120}\\.?/i) || [])[0] ||
    "Payment was not successful";
  return {
    failed: true,
    status_key: "Failed",
    status_label: String(label).replace(/\\s+/g, " ").trim().slice(0, 160),
    status_reason: String(reason).replace(/\\s+/g, " ").trim().slice(0, 400),
    error_code: errorCode,
    error_source: errorSource,
    stage_display: stageDisplay,
    status_display: statusDisplay,
  };
}`;

const MONTHS: Record<string, number> = {
  jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
  jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11,
};

const EVENT_MAP: Array<{ test: RegExp; event: string }> = [
  { test: /put on hold/i, event: "approvalOnHoldStep" },
  { test: /has been placed/i, event: "paymentApprovalStep" },
  { test: /processed your order/i, event: "itemPackedStep" },
  { test: /picked up by delivery partner|dispatched from the seller/i, event: "itemDispatchedStep" },
  { test: /has been shipped/i, event: "itemShippedStep" },
  { test: /hub nearest|received in the hub/i, event: "itemInNearestHubStep" },
  { test: /out for delivery/i, event: "itemOFDStep" },
  { test: /has been delivered/i, event: "itemDeliveredStep" },
  { test: /cancel/i, event: "cancellationStep" },
  { test: /refund/i, event: "refundProcessedStep" },
  { test: /arriv(?:ed|al)|depart/i, event: "hubTransitStep" },
];

const WEEKDAY = "(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)";
const TIMED_DATE_SRC = `${WEEKDAY},?\\s+\\d{1,2}(?:st|nd|rd|th)?\\s+[A-Za-z]{3,9}\\s+'?\\d{2,4}\\s*-\\s*\\d{1,2}:\\d{2}\\s*(?:am|pm)`;
const ANY_DATE_SRC = `${WEEKDAY},?\\s+\\d{1,2}(?:st|nd|rd|th)?\\s+[A-Za-z]{3,9}\\s+'?\\d{2,4}(?:\\s*-\\s*\\d{1,2}:\\d{2}\\s*(?:am|pm))?`;
const STAGE_TITLES =
  "Order Received|Order Confirmed|Shipped|Out for delivery|Out For Delivery|Delivered|Cancelled|Refund|Returned|Delivery Expected";

function emptyReport(): FetchReport {
  return {
    startedAt: null,
    finishedAt: null,
    elapsedMs: 0,
    accountsQueued: 0,
    accountsDone: 0,
    idsFound: 0,
    idsDiscovered: 0,
    idsNew: 0,
    idsDuplicate: 0,
    idsOpened: 0,
    enrichQueued: 0,
    idsScraped: 0,
    idsSkippedClosed: 0,
    idsSkippedOld: 0,
    idsFailed: 0,
    invoicesDownloaded: 0,
    imeiCaptured: 0,
    withProduct: 0,
    withTracking: 0,
    withGst: 0,
  };
}

function formatElapsed(ms: number) {
  const total = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  if (h) return `${h}h ${String(m).padStart(2, "0")}m ${String(s).padStart(2, "0")}s`;
  return `${m}m ${String(s).padStart(2, "0")}s`;
}

function tickElapsed(job: FetchJobState) {
  if (!job.report.startedAt) {
    job.report.elapsedMs = 0;
    return;
  }
  const end = job.report.finishedAt ? new Date(job.report.finishedAt).getTime() : Date.now();
  job.report.elapsedMs = Math.max(0, end - new Date(job.report.startedAt).getTime());
}

function state(userId: string): FetchJobState {
  if (!jobs.has(userId)) {
    jobs.set(userId, {
      running: false,
      cancelled: false,
      phase: "",
      currentEmail: null,
      logs: [],
      total: 0,
      done: 0,
      failed: 0,
      skipped: 0,
      savedOrders: 0,
      sinceDate: null,
      sinceLabel: "",
      sinceCutoff: new Date(0),
      dateFilter: true,
      windows: 1,
      report: emptyReport(),
      liveBrowsers: [],
    });
  }
  const job = jobs.get(userId)!;
  if (!job.report) job.report = emptyReport();
  if (job.report.idsDiscovered == null) {
    job.report.idsDiscovered = 0;
    job.report.idsNew = 0;
    job.report.idsDuplicate = 0;
  }
  if (job.report.enrichQueued == null) job.report.enrichQueued = 0;
  if (!job.phase) job.phase = "";
  if (!job.liveBrowsers) job.liveBrowsers = [];
  return job;
}

function jobSince(userId: string) {
  return state(userId).sinceCutoff;
}

function jobSinceLabel(userId: string) {
  return state(userId).sinceLabel || formatSinceLabel(jobSince(userId));
}

function log(userId: string, level: FetchLog["level"], message: string) {
  const job = state(userId);
  job.logs.push({ time: new Date().toISOString(), level, message });
  if (job.logs.length > 250) job.logs.splice(0, job.logs.length - 250);
  console.log(`[orders-fetch] ${message}`);
}

export function getOrderFetchJob(userId: string) {
  const job = state(userId);
  tickElapsed(job);
  const { liveBrowsers, ...rest } = job;
  return { ...rest, logs: [...job.logs], report: { ...job.report } };
}

export function isOnOrAfterSince(value?: Date | string | null, since?: Date) {
  if (!value) return false;
  const at = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(at.getTime())) return false;
  const cutoff = since || new Date(0);
  return at.getTime() >= cutoff.getTime();
}

function parseListCardDate(text: string): Date | null {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw) return null;
  const parsed = parseFlipkartDateTime(raw) || bestDateTime(raw);
  if (parsed) return parsed;
  const full = raw.match(
    /\b(\d{1,2})(?:st|nd|rd|th)?\s+(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(\d{4})\b/i
  );
  if (full) {
    const month = MONTHS[full[2].slice(0, 3).toLowerCase()];
    const day = parseInt(full[1], 10);
    const year = parseInt(full[3], 10);
    if (month == null || !day) return null;
    return new Date(`${year}-${String(month + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}T00:00:00+05:30`);
  }
  const yearNow = new Date().getFullYear();
  const todayish = raw.match(/\b(Today|Yesterday)\b[^A-Za-z0-9]{0,12}([A-Za-z]{3,9})?\s*(\d{1,2})?/i);
  if (todayish && /today/i.test(todayish[1])) return new Date();
  if (todayish && /yesterday/i.test(todayish[1])) {
    const at = new Date();
    at.setDate(at.getDate() - 1);
    return at;
  }
  const monthFirst = raw.match(
    /\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(\d{1,2})(?:,\s*(\d{4}))?\b/i
  );
  if (monthFirst) {
    const month = MONTHS[monthFirst[1].slice(0, 3).toLowerCase()];
    const day = parseInt(monthFirst[2], 10);
    const year = monthFirst[3] ? parseInt(monthFirst[3], 10) : yearNow;
    if (month == null || !day) return null;
    return new Date(`${year}-${String(month + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}T00:00:00+05:30`);
  }
  return null;
}

export function parseSinceDate(raw: string): Date {
  const text = String(raw || "").trim();
  const iso = text.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (iso) return new Date(`${iso[1]}-${iso[2]}-${iso[3]}T00:00:00+05:30`);
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) throw new Error("Enter a valid date (YYYY-MM-DD)");
  parsed.setHours(0, 0, 0, 0);
  return parsed;
}

export function cutoffMs(raw?: string | null) {
  const text = String(raw || "").trim();
  if (!text) return 0;
  try {
    if (/^\d{4}-\d{2}-\d{2}/.test(text)) return parseSinceDate(text.slice(0, 10)).getTime();
  } catch {
    /* fall through */
  }
  const at = new Date(text);
  return Number.isNaN(at.getTime()) ? 0 : at.getTime();
}

export function isClosedOrderStatus(status?: string | null) {
  return /^(delivered|cancelled|failed)$/i.test(String(status || "").trim());
}

export function isValidAfterDate(orderDate: Date | null | undefined, sinceMs: number) {
  if (!orderDate) return false;
  if (!sinceMs) return true;
  return new Date(orderDate).getTime() >= sinceMs;
}

export function toIstIso(value?: Date | string | null, endOfDay = false): string | null {
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
  const get = (type: string) => parts.find((part) => part.type === type)?.value || "00";
  let hour = get("hour");
  let minute = get("minute");
  let second = get("second");
  if (endOfDay && hour === "00" && minute === "00" && second === "00") {
    hour = "23";
    minute = "59";
    second = "59";
  }
  return `${get("year")}-${get("month")}-${get("day")}T${hour}:${minute}:${second}+05:30`;
}

export function parseFlipkartDateTime(text: string): Date | null {
  const raw = String(text || "").replace(/\s+/g, " ").trim();
  if (!raw) return null;
  const timed = raw.match(
    /(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?,?\s*(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9}),?\s+'?(\d{2,4})\s*-?\s*(\d{1,2}):(\d{2})\s*(am|pm)/i
  );
  const hit = timed || raw.match(
    /(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?,?\s*(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9}),?\s+'?(\d{2,4})\b/i
  );
  const expected = raw.match(/expected by\s+(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)?\s*(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})/i);
  const use = hit || (expected ? ([raw, expected[1], expected[2], String(new Date().getFullYear())] as unknown as RegExpMatchArray) : null);
  if (!use) return null;
  const day = parseInt(use[1], 10);
  const month = MONTHS[String(use[2]).slice(0, 3).toLowerCase()];
  if (month == null || !day) return null;
  let year = parseInt(use[3], 10);
  if (year < 100) year += 2000;
  let hour = 0;
  let minute = 0;
  let second = "00";
  if (timed) {
    hour = parseInt(timed[4], 10);
    minute = parseInt(timed[5], 10);
    if (/pm/i.test(timed[6] || "") && hour < 12) hour += 12;
    if (/am/i.test(timed[6] || "") && hour === 12) hour = 0;
  } else if (expected) {
    hour = 23;
    minute = 59;
    second = "59";
  }
  const iso = `${year}-${String(month + 1).padStart(2, "0")}-${String(day).padStart(2, "0")}T${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:${second}+05:30`;
  const at = new Date(iso);
  return Number.isNaN(at.getTime()) ? null : at;
}

function bestDateTime(text: string): Date | null {
  const raw = String(text || "");
  const timed = raw.match(new RegExp(TIMED_DATE_SRC, "i"));
  if (timed) return parseFlipkartDateTime(timed[0]);
  const any = raw.match(new RegExp(ANY_DATE_SRC, "i"));
  return any ? parseFlipkartDateTime(any[0]) : parseFlipkartDateTime(raw);
}

function looksFailedOrder(text: string) {
  return /order not placed|failed:\s*order confirmation|payment was not confirmed|payment was not successful|could not be placed|technical error at bank|technical error occurred on the bank/i.test(
    String(text || "")
  );
}

function failReasonFromText(text: string) {
  const raw = String(text || "").replace(/\s+/g, " ");
  return (
    (raw.match(/A technical error occurred on the bank[^.!]{0,120}\.?/i) || [])[0] ||
    (raw.match(/Your Payment was not confirmed[^.!]{0,120}\.?/i) || [])[0] ||
    (raw.match(/Due to an error with the payment[^.!]{0,160}\.?/i) || [])[0] ||
    (raw.match(/Technical error at bank end/i) || [])[0] ||
    "Payment was not successful"
  );
}

function money(raw: string) {
  const n = parseFloat(String(raw || "").replace(/[₹,\s]/g, ""));
  if (!Number.isFinite(n)) return "";
  return n.toFixed(2);
}

function eventFor(text: string) {
  for (const row of EVENT_MAP) {
    if (row.test.test(text)) return row.event;
  }
  return "trackingStep";
}

function parseStepsFromBody(body: string, fallback = "Status update"): ITrackingStep[] {
  const text = body.replace(/\s+/g, " ").trim();
  const steps: ITrackingStep[] = [];
  const timedRe = new RegExp(`(${TIMED_DATE_SRC})`, "gi");
  let last = 0;
  let hit: RegExpExecArray | null;
  const matches: Array<{ index: number; stamp: string }> = [];
  while ((hit = timedRe.exec(text))) {
    matches.push({ index: hit.index, stamp: hit[1] });
  }
  if (matches.length) {
    for (let i = 0; i < matches.length; i++) {
      const cur = matches[i];
      const remark = text
        .slice(last, cur.index)
        .replace(new RegExp(ANY_DATE_SRC, "gi"), " ")
        .replace(/\s+/g, " ")
        .trim()
        .replace(/^[-|:]+/, "")
        .trim();
      last = cur.index + cur.stamp.length;
      const at = parseFlipkartDateTime(cur.stamp);
      const loc =
        (remark.match(/((?:Flipkart Facility|Seller Facility)[^.]{0,50})/i) || [])[1] || null;
      const label = remark || fallback;
      steps.push({
        event: eventFor(label),
        progress: [{ date: toIstIso(at), remark: label, location: loc ? loc.replace(/\s+/g, " ").trim() : null }],
        step_text: label,
      });
    }
    return steps;
  }
  const msgRe =
    /((?:Your |Seller |Item |Order ).{6,180}?)(?:\s+((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s+\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]{3,9}\s+'?\d{2,4}))?/gi;
  while ((hit = msgRe.exec(text))) {
    const remark = hit[1].trim();
    const at = parseFlipkartDateTime(hit[2] || text);
    steps.push({
      event: eventFor(remark),
      progress: [{ date: toIstIso(at), remark, location: null }],
      step_text: remark,
    });
  }
  return steps;
}

function parseTracking(raw: string): ITrackingStage[] {
  const text = String(raw || "").replace(/\r/g, "\n");
  const split = text.replace(new RegExp(`(${STAGE_TITLES})(?=\\s*${WEEKDAY}|$)`, "g"), "\n@@@$1|||");
  const chunks = split.split("@@@").slice(1);
  const stages: ITrackingStage[] = [];
  for (const chunk of chunks) {
    const [statusRaw, rest = ""] = chunk.split("|||");
    const status = statusRaw.replace(/\s+/g, " ").trim().replace(/^Out For Delivery$/i, "Out for delivery");
    if (!status) continue;
    const pending = /^(Refund|Delivery Expected)$/i.test(status) || /expected by/i.test(rest);
    const body = rest.replace(/\s+/g, " ").trim();
    const steps = parseStepsFromBody(body, status);
    const stageDate = bestDateTime(body) || (steps[0]?.progress[0]?.date ? new Date(steps[0].progress[0].date) : null);
    if (!steps.length && stageDate) {
      steps.push({
        event: eventFor(status),
        progress: [{ date: toIstIso(stageDate), remark: status, location: null }],
        step_text: status,
      });
    }
    stages.push({
      date: toIstIso(stageDate || (steps[0]?.progress[0]?.date ? new Date(steps[0].progress[0].date) : null)),
      stage: pending ? "PENDING" : "DONE",
      status,
      detailed_steps: steps,
    });
  }
  return stages;
}

function lastTrackingStep(tracking: ITrackingStage[]) {
  const done = [...tracking].reverse().find((row) => row.stage === "DONE");
  if (!done) return "";
  const last = done.detailed_steps.slice(-1)[0];
  const when = last?.progress[0]?.date || done.date;
  if (!when) return done.status;
  return `${done.status} · ${when}`;
}

export function parseOrderId(raw: string): string | null {
  const text = String(raw || "").trim();
  if (!text) return null;
  const fromUrl = text.match(/order_id=([A-Za-z0-9]+)/i);
  if (fromUrl) return fromUrl[1].toUpperCase();
  const od = text.match(/OD\d+/i);
  if (od) return od[0].toUpperCase();
  if (/^[A-Z0-9]{10,}$/i.test(text)) return text.toUpperCase();
  return null;
}

export function cookiesFor(row: { sessionCookies?: unknown[]; sessionState?: { cookies?: unknown[] } } | null) {
  const fromState = row?.sessionState?.cookies;
  if (Array.isArray(fromState) && fromState.length) return fromState;
  return Array.isArray(row?.sessionCookies) ? row.sessionCookies : [];
}

function parseQueryParam(raw: string, key: string) {
  const match = String(raw || "").match(new RegExp(`[?&]${key}=([A-Za-z0-9]+)`, "i"));
  return match ? match[1] : "";
}

export function fallbackUnitId(orderId: string, itemId = "", unitId = "") {
  if (unitId) return unitId;
  if (itemId) return `${itemId}000`;
  return `${String(orderId || "").replace(/^OD/i, "")}000`;
}

export function orderDetailsUrl(orderId: string, itemId = "", unitId = "") {
  const params = new URLSearchParams({ order_id: orderId });
  if (itemId) params.set("item_id", itemId);
  if (unitId) params.set("unit_id", unitId);
  return `https://www.flipkart.com/order_details?${params.toString()}`;
}

function savedOrderId(row: { order_id?: string; orderId?: string; order_url?: string; orderUrl?: string }) {
  return parseOrderId(row.order_id || row.orderId || row.order_url || row.orderUrl || "") || row.order_id || row.orderId || "";
}

type ExistingUnit = {
  orderId: string;
  itemId: string;
  unitId: string;
  orderUrl: string;
  orderDate?: Date | null;
};

function savedUnit(row: {
  order_id?: string;
  item_id?: string;
  unit_id?: string;
  order_url?: string;
  order_date?: Date | null;
}): ExistingUnit | null {
  const orderId = savedOrderId(row);
  if (!orderId) return null;
  const itemId = row.item_id || parseQueryParam(row.order_url || "", "item_id");
  const unitId = fallbackUnitId(orderId, itemId, row.unit_id || parseQueryParam(row.order_url || "", "unit_id"));
  return {
    orderId,
    itemId,
    unitId,
    orderUrl: row.order_url || orderDetailsUrl(orderId, itemId, unitId),
    orderDate: row.order_date || null,
  };
}

export interface OrderAccount {
  id: Types.ObjectId;
  email: string;
  cookies: unknown[];
}

interface QueueAccount extends OrderAccount {
  existing: ExistingUnit[];
}

export type OrderUnitCard = {
  orderId: string;
  itemId: string;
  unitId: string;
  orderUrl: string;
  amount: string;
  productName?: string;
  status?: string;
  text?: string;
};
type ListCard = OrderUnitCard;

export async function patchOrderIndex(
  userId: string,
  account: OrderAccount,
  card: { orderId: string; itemId?: string; unitId: string; orderUrl?: string },
  fields: {
    scrape_status?: IndexScrapeStatus;
    list_hint?: string;
    status_key?: string;
    order_date?: Date | null;
    shipped_date?: Date | null;
    last_error?: string;
    order_url?: string;
  }
) {
  const now = new Date();
  await OrderIndex.updateOne(
    { userId, platformAccountId: account.id, unit_id: card.unitId },
    {
      $set: {
        platform_email: account.email,
        platform: "FLIPKART",
        order_id: card.orderId,
        item_id: card.itemId || "",
        order_url: fields.order_url || card.orderUrl || orderDetailsUrl(card.orderId, card.itemId, card.unitId),
        last_seen_at: now,
        ...fields,
      },
      $setOnInsert: {
        userId,
        platformAccountId: account.id,
        unit_id: card.unitId,
        first_seen_at: now,
      },
    },
    { upsert: true }
  );
}

async function discoverUnit(
  userId: string,
  account: OrderAccount,
  card: ListCard
): Promise<"new" | "duplicate" | "seeded"> {
  const now = new Date();
  const existing = await OrderIndex.findOne({
    userId,
    platformAccountId: account.id,
    unit_id: card.unitId,
  }).lean();
  if (existing) {
    await OrderIndex.updateOne(
      { _id: existing._id },
      { $set: { last_seen_at: now, list_hint: card.status || existing.list_hint || "", order_url: card.orderUrl || existing.order_url } }
    );
    return "duplicate";
  }

  const saved = await Order.findOne({ userId, unit_id: card.unitId }).select("status_key last_fetch last_error order_date").lean();
  let scrape_status: IndexScrapeStatus = "pending";
  if (saved?.last_fetch && !/^skipped — older/i.test(String(saved.last_error || ""))) {
    scrape_status = "scraped";
  } else {
    const listDate = parseListCardDate(card.status || card.text || "");
    if (listDate && listDate.getTime() < jobSince(userId).getTime()) scrape_status = "skipped_old";
  }

  await OrderIndex.updateOne(
    { userId, platformAccountId: account.id, unit_id: card.unitId },
    {
      $setOnInsert: {
        userId,
        platformAccountId: account.id,
        platform_email: account.email,
        platform: "FLIPKART",
        order_id: card.orderId,
        item_id: card.itemId || "",
        unit_id: card.unitId,
        order_url: card.orderUrl || orderDetailsUrl(card.orderId, card.itemId, card.unitId),
        scrape_status,
        list_hint: card.status || "",
        status_key: saved?.status_key || "",
        order_date: saved?.order_date || listDateSafe(card) || null,
        shipped_date: null,
        first_seen_at: now,
        last_seen_at: now,
        last_error: scrape_status === "skipped_old" ? `list date before ${jobSinceLabel(userId)}` : "",
      },
    },
    { upsert: true }
  );
  return scrape_status === "scraped" ? "seeded" : "new";
}

function listDateSafe(card: ListCard) {
  return parseListCardDate(card.status || card.text || "");
}

async function closeLiveBrowsers(userId: string) {
  const job = state(userId);
  const live = job.liveBrowsers.splice(0, job.liveBrowsers.length);
  await Promise.all(live.map(({ browser, context }) => closeBrowser(browser, context)));
}

async function wipeFlipkartLocalSession(page: Page) {
  await clearLocalThrowawayFlipkartCookies(page);
  try {
    if (!page.isClosed()) {
      await page.goto("https://www.flipkart.com", { waitUntil: "domcontentloaded", timeout: 15000 });
      await evalOnPage(
        page,
        `() => {
          try { localStorage.clear(); } catch (e) {}
          try { sessionStorage.clear(); } catch (e) {}
          return true;
        }`
      ).catch(() => {});
    }
  } catch {
    /* next restore re-attaches cookies */
  }
  await sleep(400);
}

export async function stopOrderFetch(userId: string) {
  const job = state(userId);
  job.cancelled = true;
  job.running = false;
  job.phase = "";
  job.currentEmail = null;
  markFetchRunning(userId, false);
  log(userId, "warn", "Force kill: stopping order fetch and killing Chrome");
  await closeLiveBrowsers(userId);
}

export async function stopAllOrderFetch() {
  for (const id of [...jobs.keys()]) {
    await stopOrderFetch(id);
  }
}

export async function startOrderFetch(userId: string, emails: string[], sinceDate: Date) {
  assertIdleForFetch(userId);
  const job = state(userId);
  if (job.running) throw new Error("Order fetch is already running");

  const unique = [...new Set(emails.map((e) => e.toLowerCase()).filter(Boolean))];
  if (!unique.length) throw new Error("Paste Flipkart emails in the Fetch box (comma or space separated)");
  if (!sinceDate || Number.isNaN(sinceDate.getTime())) throw new Error("Pick Fetch Orders After date");

  const queue: QueueAccount[] = [];
  const skipped: string[] = [];
  for (const email of unique) {
    const row = await PlatformId.findOne({
      userId,
      email,
      status: "logged_in",
      sessionSavedAt: { $ne: null },
    }).sort({ sessionSavedAt: -1 });
    const cookies = cookiesFor(row);
    if (!row || !cookies.length) {
      skipped.push(email);
      continue;
    }
    const existingRows = await Order.find({ userId, platformAccountId: row._id }).lean();
    const existing = new Map<string, ExistingUnit>();
    for (const saved of existingRows) {
      const unit = savedUnit(saved);
      if (!unit) continue;
      existing.set(unit.unitId, unit);
    }
    const extraLinks = (row as { orderLinks?: string[] }).orderLinks || [];
    for (const link of extraLinks) {
      const orderId = parseOrderId(link);
      if (!orderId) continue;
      const itemId = parseQueryParam(link, "item_id");
      const unitId = fallbackUnitId(orderId, itemId, parseQueryParam(link, "unit_id"));
      if (existing.has(unitId)) continue;
      existing.set(unitId, {
        orderId,
        itemId,
        unitId,
        orderUrl: link.startsWith("http") ? link : orderDetailsUrl(orderId, itemId, unitId),
      });
    }
    queue.push({ id: row._id, email: row.email, cookies, existing: [...existing.values()] });
  }

  if (!queue.length) {
    throw new Error(`None of the ${unique.length} email(s) are logged in. Login them in platform id first.`);
  }

  const windows = fetchWindowCount(queue.length);
  job.running = true;
  job.phase = "discover";
  job.cancelled = false;
  markFetchRunning(userId, true);
  job.done = 0;
  job.failed = 0;
  job.skipped = skipped.length;
  job.savedOrders = 0;
  job.total = queue.length;
  job.currentEmail = null;
  job.sinceCutoff = sinceDate;
  job.sinceDate = sinceDate.toISOString();
  job.sinceLabel = formatSinceLabel(sinceDate);
  job.dateFilter = true;
  job.windows = windows;
  job.liveBrowsers = [];
  job.logs = [];
  job.report = {
    ...emptyReport(),
    startedAt: new Date().toISOString(),
    accountsQueued: queue.length,
  };
  log(
    userId,
    "info",
    `Fetch from ${job.sinceLabel}: ${queue.length} email(s) · ${windows} Chrome window(s) · discover all unit IDs, then enrich new ones on/after placed date (no invoice)` +
      (skipped.length ? ` · skipped ${skipped.length} not logged in` : "")
  );
  for (const email of skipped.slice(0, 8)) log(userId, "warn", `Skipped ${email} — not logged in`);

  void run(userId, queue).finally(() => {
    job.running = false;
    job.phase = "";
    job.currentEmail = null;
    markFetchRunning(userId, false);
    if (!job.report.finishedAt) job.report.finishedAt = new Date().toISOString();
    tickElapsed(job);
  });
}

async function clickShowMoreOrders(page: Page) {
  try {
    const btn = page.locator("button.dSM5Ub, button.dDeuVV").filter({ hasText: /show more orders/i }).first();
    if (await btn.count()) {
      await btn.scrollIntoViewIfNeeded().catch(() => undefined);
      await btn.click({ timeout: 4000 });
      return "class";
    }
  } catch {
    /* text fallback */
  }
  try {
    const byName = page.getByRole("button", { name: /show more orders/i }).first();
    if (await byName.count()) {
      await byName.click({ timeout: 4000 });
      return "role";
    }
  } catch {
    /* eval fallback */
  }
  const how = await evalOnPage<string | null>(page, CLICK_SHOW_MORE_ORDERS);
  return how || "";
}

async function collectOrderIds(page: Page, userId: string, existing: ExistingUnit[]) {
  const kept = new Map<string, ListCard>();
  let olderStreak = 0;
  let noNew = 0;
  log(userId, "info", `Scanning My Orders until dates fall before ${jobSinceLabel(userId)}`);
  try {
    await page.waitForSelector('a[href*="order_id"]', { timeout: 20000 });
  } catch {
    log(userId, "warn", "My Orders list did not show order links yet — still scrolling");
  }

  while (noNew < 10 && olderStreak < 4) {
    if (state(userId).cancelled) throw new Error("cancelled");
    const shown = await clickShowMoreOrders(page);
    if (shown) {
      log(userId, "info", "Clicked Show more orders");
      await sleep(2000);
    }
    const cards = (await evalOnPage<ListCard[]>(page, READ_LIST_CARDS)) || [];
    let added = 0;
    let olderVisible = 0;
    let datedVisible = 0;
    for (const card of cards) {
      const orderId = parseOrderId(card.orderId || card.orderUrl || "") || card.orderId;
      if (!orderId) continue;
      const itemId = card.itemId || parseQueryParam(card.orderUrl || "", "item_id");
      const unitId = fallbackUnitId(orderId, itemId, card.unitId || parseQueryParam(card.orderUrl || "", "unit_id"));
      const cardDate = parseListCardDate(card.status || card.text || "");
      if (cardDate) datedVisible += 1;
      if (cardDate && cardDate.getTime() < jobSince(userId).getTime()) {
        olderVisible += 1;
      }
      if (!kept.has(unitId)) added += 1;
      const prev = kept.get(unitId);
      kept.set(unitId, {
        orderId,
        itemId: itemId || prev?.itemId || "",
        unitId,
        orderUrl: card.orderUrl || prev?.orderUrl || orderDetailsUrl(orderId, itemId, unitId),
        amount: card.amount || prev?.amount || "",
        productName: card.productName || prev?.productName || "",
        status: card.status || prev?.status || "",
        text: card.text || prev?.text || "",
      });
    }
    if (olderVisible > 0 && added === 0 && datedVisible > 0) olderStreak += 1;
    else if (added > 0) olderStreak = 0;
    log(
      userId,
      "info",
      `Fetch: ${kept.size} unit(s) on/after ${jobSinceLabel(userId)} · ${cards.length} visible · ${olderVisible} older${added ? ` · +${added} new` : ""}`
    );
    if (added === 0 && !shown) noNew += 1;
    else noNew = 0;
    await evalOnPage(page, SCROLL_LAST_ORDER);
    await evalOnPage(page, SCROLL_ORDERS);
    await page.mouse.wheel(0, 1800).catch(() => undefined);
    await sleep(1600);
  }

  for (const row of existing) {
    if (row.orderDate && !isOnOrAfterSince(row.orderDate, jobSince(userId))) continue;
    if (kept.has(row.unitId)) continue;
    kept.set(row.unitId, {
      orderId: row.orderId,
      itemId: row.itemId,
      unitId: row.unitId,
      orderUrl: row.orderUrl || orderDetailsUrl(row.orderId, row.itemId, row.unitId),
      amount: "",
    });
  }
  log(userId, "info", `Collect done: ${kept.size} unit(s) on/after ${jobSinceLabel(userId)}`);
  return [...kept.values()];
}

async function clickSeeAllUpdates(page: Page) {
  try {
    const btn = page.locator("div.RMdaeX").filter({ hasText: /see all updates/i }).first();
    if (await btn.count()) {
      await btn.scrollIntoViewIfNeeded().catch(() => undefined);
      await btn.click({ timeout: 4000 });
      return true;
    }
  } catch {
    /* evaluate */
  }
  return Boolean(await evalOnPage<string | null>(page, CLICK_SEE_ALL_UPDATES));
}

/* Invoice download helpers omitted — scrape data only. */

export async function scrapeAndSave(
  page: Page,
  userId: string,
  account: OrderAccount,
  card: ListCard,
  opts: { applyCalendar?: boolean } = { applyCalendar: true }
) {
  const itemId = card.itemId || parseQueryParam(card.orderUrl || "", "item_id");
  const unitId = fallbackUnitId(card.orderId, itemId, card.unitId || parseQueryParam(card.orderUrl || "", "unit_id"));
  await page.goto(card.orderUrl || orderDetailsUrl(card.orderId, itemId, unitId), {
    waitUntil: "domcontentloaded",
    timeout: 25000,
  });
  await sleep(900);
  try {
    await page.waitForFunction(
      () => /order_details|order_id=/i.test(location.href) && /price details|see all updates|download invoice|order confirmed|order not placed|failed:\\s*order confirmation|delivered/i.test(document.body.innerText || ""),
      { timeout: 12000 }
    );
  } catch {
    /* still try scrape */
  }
  await sleep(500);
  if (flipkartLoginUrl(page.url())) throw new Error("Flipkart session expired");

  const deliveryHeading = page.getByText("Delivery details", { exact: true }).first();
  if (await deliveryHeading.isVisible().catch(() => false)) {
    await deliveryHeading.scrollIntoViewIfNeeded().catch(() => undefined);
    const expanded = await page.getByText(/Cabin|UDYOG VIHAR|PHASE-1/i).first().isVisible().catch(() => false);
    if (!expanded) {
      await deliveryHeading.click({ timeout: 4000 }).catch(() => undefined);
      const row = deliveryHeading.locator("xpath=ancestor::*[self::div or self::button][1]");
      await row.click({ timeout: 4000 }).catch(() => undefined);
    }
    await page.getByText(/[6-9]\d{9}/).first().waitFor({ timeout: 5000 }).catch(() => undefined);
  }
  const priceHeading = page.getByText("Price details", { exact: true }).first();
  if (await priceHeading.isVisible().catch(() => false)) {
    const totalVisible = await page.getByText("Total amount", { exact: true }).first().isVisible().catch(() => false);
    if (!totalVisible) await priceHeading.click({ timeout: 4000 }).catch(() => undefined);
  }
  await evalOnPage(page, EXPAND_ORDER_SECTIONS).catch(() => undefined);
  await sleep(700);
  const snapshot = (await evalOnPage<Record<string, string | number | boolean>>(page, READ_PAGE)) || {};
  await evalOnPage(page, SCROLL_GST).catch(() => undefined);
  await sleep(700);
  const gstSnap = (await evalOnPage<Record<string, string | number | boolean>>(page, READ_PAGE)) || snapshot;
  const pageData = { ...snapshot, ...Object.fromEntries(Object.entries(gstSnap).filter(([, v]) => v !== "" && v != null)) };

  await evalOnPage(page, `() => { window.scrollTo(0, 0); return true; }`).catch(() => undefined);
  await sleep(350);

  const pageTextEarly = String(pageData.pageText || pageData.deliveryMessage || "");
  let failState =
    (await evalOnPage<{
      failed: boolean;
      status_key: string;
      status_label: string;
      status_reason: string;
      error_code?: string;
      error_source?: string;
      stage_display?: string;
    } | null>(page, READ_FAIL_STATE).catch(() => null)) || null;
  if (!failState?.failed && looksFailedOrder(pageTextEarly)) {
    failState = {
      failed: true,
      status_key: "Failed",
      status_label: /failed:\s*order confirmation/i.test(pageTextEarly)
        ? "Failed: Order Confirmation"
        : "Order not placed",
      status_reason: failReasonFromText(pageTextEarly),
    };
  }
  const failed = Boolean(failState?.failed);

  let trackingText = "";
  if (!failed) {
    for (let attempt = 0; attempt < 2 && !trackingText; attempt++) {
      if (await clickSeeAllUpdates(page).catch(() => false)) {
        await sleep(1800);
        trackingText = (await evalOnPage<string>(page, READ_TRACKING).catch(() => "")) || "";
        await evalOnPage(page, CLOSE_TRACKING).catch(() => undefined);
        await sleep(300);
      }
    }
  }

  const tracking = parseTracking(trackingText);
  const cancelledStage = tracking.find((row) => /^cancelled$/i.test(row.status) && row.stage === "DONE");
  const deliveredStageHit = tracking.find((row) => /^delivered$/i.test(row.status) && row.stage === "DONE");
  const pageText = String(pageData.pageText || "");
  const pageDelivered = /your item has been delivered|\bdelivered\b/i.test(String(pageData.deliveryMessage || ""))
    || /Your item has been delivered/i.test(pageText);
  const returnedStage = tracking.find((row) => /^returned$/i.test(row.status) && row.stage === "DONE");
  const refundedStage = tracking.find((row) => /^refund/i.test(row.status) && row.stage === "DONE");
  const delivered = !failed && (Boolean(deliveredStageHit) || (pageDelivered && !cancelledStage));
  const cancelled = !failed && Boolean(cancelledStage) && !delivered;
  const returned = !failed && !delivered && !cancelled && Boolean(returnedStage);
  const refunded = !failed && !delivered && !cancelled && !returned && Boolean(refundedStage);
  const current =
    (delivered && (deliveredStageHit || tracking.find((row) => /^delivered$/i.test(row.status)))) ||
    (cancelled && cancelledStage) ||
    [...tracking].reverse().find((row) => row.stage === "DONE") ||
    tracking[tracking.length - 1];
  const received = tracking.find((row) => /received/i.test(row.status));
  const confirmed = tracking.find((row) => /confirmed/i.test(row.status));
  const placedIso =
    received?.detailed_steps.find((step) => /has been placed|put on hold/i.test(step.step_text))?.progress[0]?.date ||
    confirmed?.detailed_steps.find((step) => /has been placed/i.test(step.step_text))?.progress[0]?.date ||
    received?.date ||
    confirmed?.date;
  const placed = placedIso;
  const expected = tracking.find((row) => /expected/i.test(row.status));
  const deliveredStage = tracking.find((row) => /^delivered$/i.test(row.status));
  const courier = (trackingText.match(/([A-Za-z][A-Za-z0-9 .]*?(?:Logistics|Ekart|Delhivery))\s*-\s*([A-Z0-9]{8,})/i) || []) as string[];
  const trackingId = (courier[2] || String(pageData.pageText || "").match(/\b(FMPP[A-Z0-9]{6,})\b/i)?.[1] || "").toUpperCase();

  const pageQty = Number(pageData.quantity) || 1;
  const totalAmount = money(String(pageData.totalAmount || card.amount || ""));
  const unitAmount = pageQty > 1 && totalAmount ? (parseFloat(totalAmount) / pageQty).toFixed(2) : totalAmount;
  const unitKey = fallbackUnitId(
    card.orderId,
    String(pageData.itemId || itemId || ""),
    String(pageData.unitId || unitId || "")
  );
  const savedItemId = String(pageData.itemId || itemId || "");
  let productName = String(pageData.productName || "");
  if (!productName || /shared this order with you/i.test(productName)) {
    const listed = String(card.productName || "");
    if (listed && !/shared this order with you/i.test(listed)) productName = listed;
  }
  const shippedStage = tracking.find((row) => /^shipped$/i.test(row.status));
  const shippedDate = shippedStage?.date ? new Date(shippedStage.date) : null;
  const orderDate =
    placed ? new Date(placed) : failed ? parseFlipkartDateTime(String(pageData.deliveryMessage || pageText || "")) : null;
  if (opts.applyCalendar !== false && !failed && !orderDate) {
    await patchOrderIndex(userId, account, { orderId: card.orderId, itemId: savedItemId, unitId: unitKey, orderUrl: page.url() }, {
      scrape_status: "failed",
      last_error: "no placed date in tracking",
      order_url: page.url(),
    });
    log(userId, "warn", `${card.orderId} unit ${unitKey}: no placed date in See all updates — skip`);
    return {
      lastFetch: new Date(),
      productName: "",
      totalAmount: "",
      statusKey: "unknown",
      gstNumber: "",
      trackingCount: tracking.length,
      invoiceDownloaded: false,
      imei: null,
      orderDate: null,
      skippedOld: true,
    };
  }
  if (opts.applyCalendar !== false && orderDate && !isOnOrAfterSince(orderDate, jobSince(userId))) {
    log(userId, "info", `${card.orderId} unit ${unitKey}: placed ${orderDate.toLocaleDateString("en-IN")} is before ${jobSinceLabel(userId)} — index only`);
    await patchOrderIndex(
      userId,
      account,
      { orderId: card.orderId, itemId: savedItemId, unitId: unitKey, orderUrl: page.url() },
      {
        scrape_status: "skipped_old",
        order_date: orderDate,
        shipped_date: shippedDate,
        status_key: delivered ? "Delivered" : cancelled ? "Cancelled" : current?.status || "",
        last_error: `placed before ${jobSinceLabel(userId)}`,
        order_url: page.url(),
      }
    );
    return {
      lastFetch: new Date(),
      productName: "",
      totalAmount: "",
      statusKey: "older",
      gstNumber: "",
      trackingCount: 0,
      invoiceDownloaded: false,
      imei: null,
      orderDate,
      skippedOld: true,
    };
  }
  const deliveryDate = expected?.date ? new Date(expected.date) : parseFlipkartDateTime(String(pageData.deliveryMessage || ""));
  const actualDelivery = deliveredStage?.date ? new Date(deliveredStage.date) : null;
  const cancelledAt =
    cancelledStage?.detailed_steps[0]?.progress[0]?.date || cancelledStage?.date || null;
  const refundAmount =
    money(
      String(
        (String(pageData.pageText || "").match(/Total refund\s*-?\s*₹\s*([\d,]+(?:\.\d+)?)/i) || [])[1] || ""
      )
    ) || null;

  let imei: string | null = null;
  const invoicePath = "";
  const invoiceDownloaded = false;
  // Invoice download is disabled on Trade Flow fetch — scrape order data only.
  // if (delivered && !cancelled && !failed) { downloadDeliveredInvoice(...) }

  const lastFetch = new Date();
  const existing = await Order.findOne({ userId, unit_id: unitKey }).select("id").lean();
  const seq = existing?.id || (await nextSeq("order_fetch"));
  const statusKey = failed
    ? failState?.status_key || "Failed"
    : delivered
      ? "Delivered"
      : cancelled
        ? "Cancelled"
        : returned
          ? "Returned"
          : refunded
            ? "Refunded"
            : current?.status || "";
  const statusLabel = failed
    ? failState?.status_label || "Order not placed"
    : delivered
      ? "Your item has been delivered"
      : current?.detailed_steps.slice(-1)[0]?.step_text || current?.status || "";
  const statusReason = failed
    ? failState?.status_reason || "Payment was not successful"
    : cancelled
      ? cancelledStage?.detailed_steps[0]?.step_text || "Cancelled on Flipkart"
      : null;

  await Order.updateOne(
    { userId, unit_id: unitKey },
    {
      $set: {
        userId,
        platformAccountId: account.id,
        id: seq,
        platform_email: account.email,
        platform: "FLIPKART",
        order_id: card.orderId,
        item_id: savedItemId,
        order_url: page.url() || card.orderUrl || orderDetailsUrl(card.orderId, savedItemId, unitKey),
        order_date: orderDate,
        product_name: productName,
        seller_name: String(pageData.sellerName || ""),
        unit_id: unitKey,
        quantity: 1,
        total_amount: totalAmount,
        unit_amount: unitAmount,
        delivery_date: deliveryDate,
        actual_delivery_date: actualDelivery,
        ewb_number: String(pageData.ewbNumber || ""),
        delivery_message: String(pageData.deliveryMessage || statusLabel),
        status_key: statusKey,
        status_label: statusLabel,
        status_reason: statusReason,
        business_name: String(pageData.gstSellerName || pageData.billingName || ""),
        business_gst_no: String(pageData.gstNumber || ""),
        tracking_id: trackingId,
        delivery_otp: cancelled || failed ? null : String(pageData.deliveryOtp || "") || null,
        tracking,
        last_tracking_step: lastTrackingStep(tracking),
        billing_phone_number: String(pageData.billingPhone || "") || null,
        billing_address_name: String(pageData.billingName || "") || null,
        billing_address_pincode: String(pageData.pincode || "") || null,
        is_invoice_downloaded: invoiceDownloaded,
        invoice_path: invoicePath,
        supercoin_amount_applied: Number(pageData.supercoin) || 0,
        is_bae_order: false,
        cash_on_delivery: Boolean(pageData.cashOnDelivery),
        refund_status: tracking.some((row) => /refund/i.test(row.status)) || /refund/i.test(String(pageData.pageText || ""))
          ? "refund"
          : null,
        refund_amount: refundAmount,
        refund_msg: /refund/i.test(String(pageData.pageText || ""))
          ? (String(pageData.pageText || "").match(/Total refund[^.!]{0,80}/i) || [null])[0]
          : null,
        cancelled_date: cancelledAt ? new Date(cancelledAt) : cancelled ? orderDate : null,
        cancelled_from_bae: false,
        cancelled_by_user: /as per your request|cancelled by you|you cancelled/i.test(
          `${trackingText} ${pageData.pageText || ""}`
        ),
        order_status_pre_cancellation: cancelled ? (confirmed?.status || "Order Confirmed") : null,
        imei,
        is_logged_in: true,
        last_fetch: lastFetch,
        ...(opts.applyCalendar !== false ? { since_date: jobSince(userId) } : {}),
        last_error: "",
      },
    },
    { upsert: true }
  );
  await patchOrderIndex(
    userId,
    account,
    { orderId: card.orderId, itemId: savedItemId, unitId: unitKey, orderUrl: page.url() },
    {
      scrape_status: "scraped",
      status_key: statusKey,
      order_date: orderDate,
      shipped_date: shippedDate,
      last_error: "",
      order_url: page.url(),
    }
  );

  return {
    lastFetch,
    productName,
    totalAmount,
    statusKey,
    gstNumber: String(pageData.gstNumber || ""),
    trackingCount: tracking.length,
    invoiceDownloaded,
    imei,
    orderDate,
    skippedOld: false,
  };
}


async function processAccount(page: Page, userId: string, queue: QueueAccount[], row: QueueAccount, index: number) {
  const job = state(userId);
  job.currentEmail = row.email;
  job.phase = "discover";
  log(userId, "info", `Discover ${index + 1}/${queue.length}: ${row.email}`);
  await restoreFlipkartSession(page, row.cookies);
  await page.goto(ORDERS_URL, { waitUntil: "domcontentloaded", timeout: 30000 });
  await sleep(2500);
  if (flipkartLoginUrl(page.url())) {
    await restoreFlipkartSession(page, row.cookies);
    await page.goto(ORDERS_URL, { waitUntil: "domcontentloaded", timeout: 30000 });
    await sleep(2500);
  }
  if (flipkartLoginUrl(page.url())) {
    throw new Error("Flipkart session expired. Refresh this ID in platform id first.");
  }

  const cards = await collectOrderIds(page, userId, row.existing);
  job.report.idsFound += cards.length;
  tickElapsed(job);

  let discoveredNew = 0;
  let discoveredDup = 0;
  for (const card of cards) {
    if (job.cancelled) throw new Error("cancelled");
    const kind = await discoverUnit(userId, row, card);
    job.report.idsDiscovered += 1;
    if (kind === "duplicate" || kind === "seeded") {
      discoveredDup += 1;
      job.report.idsDuplicate += 1;
    } else {
      discoveredNew += 1;
      job.report.idsNew += 1;
    }
  }
  log(
    userId,
    "info",
    `${row.email}: indexed ${cards.length} unit(s) · ${discoveredNew} new · ${discoveredDup} already known`
  );

  job.phase = "enrich";
  const pending = await OrderIndex.find({
    userId,
    platformAccountId: row.id,
    scrape_status: { $in: ["pending", "failed"] },
  }).lean();
  job.report.enrichQueued += pending.length;
  log(userId, "info", `Enrich ${row.email}: ${pending.length} pending/failed unit(s) to open`);

  for (let n = 0; n < pending.length; n++) {
    const rowIdx = pending[n];
    if (job.cancelled) throw new Error("cancelled");
    const card: ListCard = {
      orderId: rowIdx.order_id,
      itemId: rowIdx.item_id,
      unitId: rowIdx.unit_id,
      orderUrl: rowIdx.order_url || orderDetailsUrl(rowIdx.order_id, rowIdx.item_id, rowIdx.unit_id),
      amount: "",
    };
    job.currentEmail = `${row.email} enrich ${n + 1}/${pending.length} ${card.orderId} ${card.unitId}`;
    job.report.idsOpened += 1;
    try {
      const scraped = await scrapeAndSave(page, userId, row, card, { applyCalendar: true });
      if (scraped.skippedOld) {
        job.report.idsSkippedOld += 1;
        tickElapsed(job);
        continue;
      }
      job.report.idsScraped += 1;
      job.savedOrders += 1;
      if (scraped.productName) job.report.withProduct += 1;
      if (scraped.trackingCount) job.report.withTracking += 1;
      if (scraped.gstNumber) job.report.withGst += 1;
      tickElapsed(job);
      log(
        userId,
        "info",
        `${card.orderId} unit ${card.unitId}: ${scraped.productName || "product"} · ₹${scraped.totalAmount || "?"} · ${scraped.statusKey || "status"} · ${scraped.trackingCount} tracking stage(s)`
      );
    } catch (err) {
      if (isDestroyedContext(err)) throw err;
      job.report.idsFailed += 1;
      tickElapsed(job);
      const message = err instanceof Error ? err.message : String(err);
      log(userId, "warn", `${card.orderId} unit ${card.unitId}: enrich failed (${message})`);
      await patchOrderIndex(userId, row, card, { scrape_status: "failed", last_error: message });
    }
  }

  job.done += 1;
  job.report.accountsDone += 1;
  tickElapsed(job);
  log(
    userId,
    "info",
    `${row.email}: discover ${cards.length} · enrich opened ${pending.length} · last fetch ${new Date().toLocaleString("en-IN")}`
  );
}

async function run(userId: string, queue: QueueAccount[]) {
  const job = state(userId);
  const windows = Math.max(1, job.windows || 1);
  let next = 0;
  log(userId, "info", `Opening ${windows} Chrome window(s) for ${queue.length} email(s)`);

  async function lane(laneId: number) {
    if (job.cancelled) return;
    const launched = await launchStealthContext();
    job.liveBrowsers.push(launched);
    const page = await launched.context.newPage();
    await blockFlipkartLogout(page);
    try {
      while (!job.cancelled) {
        const i = next++;
        if (i >= queue.length) break;
        const row = queue[i];
        try {
          await processAccount(page, userId, queue, row, i);
        } catch (err) {
          if (job.cancelled || (err instanceof Error && err.message === "cancelled")) {
            log(userId, "warn", `Stopped at ${row.email}`);
            break;
          }
          job.failed += 1;
          log(userId, "error", `${row.email} failed: ${err instanceof Error ? err.message : err}`);
        }
        try {
          await wipeFlipkartLocalSession(page);
        } catch {
          /* next restore */
        }
        await sleep(600);
      }
    } finally {
      await closeBrowser(launched.browser, launched.context).catch(() => {});
      job.liveBrowsers = job.liveBrowsers.filter((row) => row.browser !== launched.browser);
    }
    void laneId;
  }

  try {
    await Promise.all(Array.from({ length: windows }, (_, i) => lane(i)));
    if (!job.cancelled) {
      log(
        userId,
        "info",
        `Finished. IDs ok ${job.done}, failed ${job.failed}, skipped ${job.skipped}, units ${job.savedOrders}`
      );
    }
  } finally {
    tickElapsed(job);
    const r = job.report;
    log(
      userId,
      "info",
      `Report: ${formatElapsed(r.elapsedMs)} · found ${r.idsFound} units · scraped ${r.idsScraped} · tracking ${r.withTracking}`
    );
    await closeLiveBrowsers(userId);
    if (!job.cancelled) log(userId, "info", "Chrome closed. Orders are stored.");
  }
}
