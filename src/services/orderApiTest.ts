import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { config } from "../config.js";
import { Order } from "../models/Order.js";
import { resolveLoggedInSession, workspaceUserId } from "./sessionStore.js";
import {
  fetchOrderDetails,
  fetchOrderList,
  listUnits,
  mapApiUnitToOrder,
  openOrderApiSession,
  OrderApiError,
  OrderApiSessionError,
  type MappedOrderUnit,
} from "./orderApi.js";

/**
 * The Test tab's "fetch & update via API" run.
 *
 * Reads one account's orders through Flipkart's own APIs — no browser, no
 * DOM — maps each unit to the order_details field set, and diffs it against
 * what the page scraper last saved for the same unit. The diff IS the
 * acceptance test for switching fetch/update to the API. Nothing is written
 * to order_details; every raw response and mapped document is kept under
 * <testArtifactDir>/orders/<runId>/ for inspection.
 */

export interface OrderApiTestConfig {
  /** Platform ID (ObjectId) or its email. */
  platformId: string;
  /** Restrict to one order (and optionally one unit). */
  orderId?: string;
  unitId?: string;
  /** List pages to walk when no order id is given (default 1, max 5). */
  maxPages?: number;
  /** Units to fetch details for (default 3, max 20). */
  maxUnits?: number;
}

export interface FieldDiff {
  field: string;
  api: unknown;
  scraper: unknown;
  same: boolean;
}

export interface OrderApiTestUnit {
  orderId: string;
  unitId: string;
  listed: {
    status: string;
    statusText: string;
    orderDate: string | null;
    amount: string;
    paymentMode: string;
    title: string;
    trackingId: string;
  };
  details?: {
    ms: number;
    bytes: number;
    mapped: MappedOrderUnit;
    /** The scraper's saved document for this unit, trimmed to the compared fields. */
    scraper: Record<string, unknown> | null;
    diff: FieldDiff[];
    same: number;
    different: number;
    file: string;
  };
  error?: string;
}

export interface OrderApiTestResult {
  runId: string;
  status: "success" | "failed" | "running";
  startedAt: string;
  seconds: number;
  email: string;
  platformId: string;
  config: OrderApiTestConfig;
  list: {
    pages: Array<{ page: number; orders: number; units: number; ms: number; bytes: number; moreOrder: boolean }>;
    unitsSeen: number;
    file: string;
  };
  units: OrderApiTestUnit[];
  error?: string;
  dir: string;
}

export const COMPARED_FIELDS: Array<keyof MappedOrderUnit> = [
  "order_id",
  "item_id",
  "unit_id",
  "order_date",
  "product_name",
  "seller_name",
  "total_amount",
  "unit_amount",
  "delivery_date",
  "actual_delivery_date",
  "ewb_number",
  "delivery_message",
  "status_key",
  "status_label",
  "status_reason",
  "business_name",
  "business_gst_no",
  "tracking_id",
  "delivery_otp",
  "last_tracking_step",
  "billing_phone_number",
  "billing_address_name",
  "billing_address_pincode",
  "cash_on_delivery",
  "refund_status",
  "cancelled_date",
  "cancelled_by_user",
  "order_status_pre_cancellation",
];

function norm(v: unknown): string {
  if (v == null) return "";
  if (v instanceof Date) return Number.isNaN(v.getTime()) ? "" : v.toISOString().slice(0, 16);
  if (typeof v === "boolean") return v ? "true" : "false";
  if (typeof v === "number") return String(v);
  const s = String(v).trim();
  if (/^\d{4}-\d{2}-\d{2}T/.test(s)) return new Date(s).toISOString().slice(0, 16);
  // Timestamps inside text ("Delivered · 2026-09-23T21:50:50+05:30"): the DOM
  // only ever has minutes, the API has seconds — compare to the minute.
  return s.replace(/(T\d{2}:\d{2}):\d{2}/g, "$1").replace(/\s+/g, " ").toLowerCase();
}

export function diffAgainstScraper(mapped: MappedOrderUnit, saved: Record<string, unknown> | null): FieldDiff[] {
  return COMPARED_FIELDS.map((field) => {
    const api = mapped[field];
    const scraper = saved ? saved[field] : undefined;
    const a = norm(api);
    const b = norm(scraper);
    // Dates: same minute counts as same (the DOM gives minutes, the API ms).
    const same = saved ? a === b || (a && b && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(a) && a.slice(0, 16) === b.slice(0, 16)) : false;
    return { field, api, scraper, same: Boolean(same) };
  });
}

const runs = new Map<string, OrderApiTestResult>();
let live: string | null = null;

export function getOrderApiRun(runId: string): OrderApiTestResult | null {
  return runs.get(runId) || null;
}

export function listOrderApiRuns(limit = 25): OrderApiTestResult[] {
  const root = path.resolve(config.testArtifactDir, "orders");
  const out: OrderApiTestResult[] = [...runs.values()];
  if (fs.existsSync(root)) {
    for (const name of fs.readdirSync(root)) {
      if (runs.has(name)) continue;
      const file = path.join(root, name, "result.json");
      if (!fs.existsSync(file)) continue;
      try {
        out.push(JSON.parse(fs.readFileSync(file, "utf8")) as OrderApiTestResult);
      } catch {
        /* half-written */
      }
    }
  }
  return out.sort((a, b) => b.startedAt.localeCompare(a.startedAt)).slice(0, limit);
}

export async function runOrderApiTest(cfg: OrderApiTestConfig): Promise<OrderApiTestResult> {
  if (live) throw new Error(`An order-API test is already running (${live})`);
  const runId = `orders-${new Date().toISOString().replace(/[:.]/g, "-")}-${randomUUID().slice(0, 4)}`;
  const dir = path.resolve(config.testArtifactDir, "orders", runId);
  fs.mkdirSync(dir, { recursive: true });
  const started = Date.now();
  const result: OrderApiTestResult = {
    runId,
    status: "running",
    startedAt: new Date(started).toISOString(),
    seconds: 0,
    email: "",
    platformId: cfg.platformId,
    config: cfg,
    list: { pages: [], unitsSeen: 0, file: "" },
    units: [],
    dir,
  };
  runs.set(runId, result);
  live = runId;
  const save = () => {
    result.seconds = Math.round((Date.now() - started) / 10) / 100;
    fs.writeFileSync(path.join(dir, "result.json"), JSON.stringify(result, null, 1));
  };

  try {
    const session = await resolveLoggedInSession(cfg.platformId);
    if (!session.ok) throw new OrderApiSessionError(`${session.email}: ${session.reason}`);
    result.email = session.email;
    result.platformId = session.platformId;
    const userId = workspaceUserId();
    const ctx = await openOrderApiSession(session.cookies);
    try {
      // ---- discovery: list pages ------------------------------------------
      const wantOrder = (cfg.orderId || "").trim().toUpperCase();
      const maxPages = Math.min(5, Math.max(1, Number(cfg.maxPages) || 1));
      const maxUnits = Math.min(20, Math.max(1, Number(cfg.maxUnits) || 3));
      let next: Array<{ key: string; value: string }> = [];
      const seen: ReturnType<typeof listUnits> = [];
      const listFile = path.join(dir, "list.json");
      const listPages: unknown[] = [];
      for (let p = 1; p <= maxPages; p++) {
        const page = await fetchOrderList(ctx, p, next);
        listPages.push({ page: p, request: { page: p, nextCallParams: next }, response: page });
        const units = listUnits(page);
        seen.push(...units);
        result.list.pages.push({ page: p, orders: page.orders.length, units: units.length, ms: page.ms, bytes: page.bytes, moreOrder: page.moreOrder });
        if (wantOrder && seen.some((u) => u.orderId === wantOrder)) break;
        if (!page.moreOrder) break;
        next = page.nextCallParams;
      }
      fs.writeFileSync(listFile, JSON.stringify(listPages, null, 1));
      result.list.file = listFile;
      result.list.unitsSeen = seen.length;

      // ---- details per unit -------------------------------------------------
      let targets = wantOrder ? seen.filter((u) => u.orderId === wantOrder) : seen;
      if (wantOrder && !targets.length) {
        // Not on the pages walked: ask for it anyway — details needs only ids.
        const unitId = (cfg.unitId || "").trim() || `${wantOrder.replace(/^OD/, "")}000`;
        targets = [
          {
            orderId: wantOrder,
            unitId,
            itemId: unitId.replace(/000$/, ""),
            orderDate: null,
            status: "",
            statusText: "",
            amount: "",
            paymentMode: "",
            title: "",
            trackingId: "",
            unit: {},
          },
        ];
      }
      if (cfg.unitId) targets = targets.filter((u) => u.unitId === String(cfg.unitId).trim());
      targets = targets.slice(0, maxUnits);

      for (const t of targets) {
        const row: OrderApiTestUnit = {
          orderId: t.orderId,
          unitId: t.unitId,
          listed: {
            status: t.status,
            statusText: t.statusText,
            orderDate: t.orderDate ? t.orderDate.toISOString() : null,
            amount: t.amount,
            paymentMode: t.paymentMode,
            title: t.title,
            trackingId: t.trackingId,
          },
        };
        result.units.push(row);
        try {
          const details = await fetchOrderDetails(ctx, t.orderId, t.unitId);
          const mapped = mapApiUnitToOrder(details.orderView, t.unitId, t.unit);
          const saved = (await Order.findOne({ userId, unit_id: t.unitId }).lean()) as Record<string, unknown> | null;
          const scraper = saved ? Object.fromEntries(COMPARED_FIELDS.map((f) => [f, saved[f]])) : null;
          const diff = diffAgainstScraper(mapped, scraper);
          const file = path.join(dir, `${t.orderId}-${t.unitId}.json`);
          fs.writeFileSync(file, JSON.stringify({ raw: details.raw, mapped, scraper, diff }, null, 1));
          row.details = {
            ms: details.ms,
            bytes: details.bytes,
            mapped,
            scraper,
            diff,
            same: diff.filter((d) => d.same).length,
            different: diff.filter((d) => !d.same).length,
            file,
          };
        } catch (err) {
          row.error = err instanceof Error ? err.message : String(err);
        }
        save();
      }
      result.status = "success";
    } finally {
      await ctx.dispose().catch(() => undefined);
    }
  } catch (err) {
    result.status = "failed";
    result.error = err instanceof OrderApiError ? `${err.message}${err.body ? ` — ${err.body.slice(0, 120)}` : ""}` : err instanceof Error ? err.message : String(err);
  } finally {
    live = null;
    save();
  }
  return result;
}
