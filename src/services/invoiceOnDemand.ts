import type { APIRequestContext } from "playwright";
import { Order } from "../models/Order.js";
import { PlatformId } from "../models/PlatformId.js";
import { cookiesFor } from "./orderFetch.js";
import { openOrderApiSession } from "./orderApi.js";
import {
  downloadInvoiceViaApi,
  mergeInvoiceBundle,
  putInvoice,
  registerInvoice,
  rawInvoiceKey,
  invoiceExists,
  invoiceStoreEnabled,
} from "./invoiceCapture.js";

/**
 * On-demand invoice download by order ID — the "Download Order Invoices" modal.
 *
 * The operator pastes order IDs; each order already carries its account
 * (order_details.platformAccountId), so there is no account picker. We resolve
 * order → account, group by account (one session each), download zip-aware over
 * the order API, and overwrite raw/<user>/<order>.pdf — which the S3→SQS→Lambda
 * pipeline re-extracts, filling any late / missing PPF/GT parts. `force`
 * re-downloads even when a bundle already exists (the early/missing-parts path);
 * without it, an order that already has an object is left alone.
 */

export type OrderDownloadStatus = "downloaded" | "already" | "unknown_order" | "no_session" | "failed";

export interface OrderDownloadResult {
  orderId: string;
  status: OrderDownloadStatus;
  units?: number;
  parts?: string[];
  sizeKb?: number;
  error?: string;
}

interface Target {
  userId: string;
  accountId: string;
  orderId: string;
  unitIds: string[];
  rowIds: unknown[];
}

/** Split a textarea / array of order IDs into a clean, de-duplicated list. */
export function parseOrderIds(input: unknown): string[] {
  const raw = Array.isArray(input) ? input.join(" ") : String(input || "");
  return [...new Set(raw.split(/[\s,]+/).map((s) => s.trim()).filter(Boolean))];
}

export async function downloadInvoicesByOrderId(opts: {
  userId: string;
  orderIds: string[];
  force: boolean;
  concurrency?: number;
  cancelled?: () => boolean;
  log?: (level: "info" | "warn", msg: string) => void;
}): Promise<OrderDownloadResult[]> {
  const log = opts.log || (() => undefined);
  const want = [...new Set(opts.orderIds.map((s) => s.trim()).filter(Boolean))];
  const results = new Map<string, OrderDownloadResult>();
  for (const o of want) results.set(o, { orderId: o, status: "unknown_order" });

  if (!invoiceStoreEnabled()) {
    for (const o of want) results.set(o, { orderId: o, status: "failed", error: "invoice store not configured (INVOICE_S3_BUCKET)" });
    return want.map((o) => results.get(o)!);
  }
  if (!want.length) return [];

  // Resolve order → owning account, collecting every unit row of the order.
  const rows = await Order.find({ userId: opts.userId, order_id: { $in: want } })
    .select("order_id unit_id platformAccountId userId")
    .lean();
  const byOrder = new Map<string, Target>();
  for (const r of rows) {
    const orderId = String(r.order_id);
    let t = byOrder.get(orderId);
    if (!t) {
      t = { userId: String(r.userId), accountId: String(r.platformAccountId), orderId, unitIds: [], rowIds: [] };
      byOrder.set(orderId, t);
    }
    t.unitIds.push(String(r.unit_id));
    t.rowIds.push(r._id);
  }

  // Group by account so each session is opened once.
  const byAccount = new Map<string, Target[]>();
  for (const t of byOrder.values()) {
    const list = byAccount.get(t.accountId) || [];
    list.push(t);
    byAccount.set(t.accountId, list);
  }

  const processAccount = async (accountId: string, targets: Target[]) => {
    const acc = await PlatformId.findById(accountId).lean();
    const cookies = cookiesFor(acc as Parameters<typeof cookiesFor>[0]);
    if (!cookies.length) {
      for (const t of targets) results.set(t.orderId, { orderId: t.orderId, status: "no_session", units: t.unitIds.length });
      return;
    }
    let ctx: APIRequestContext;
    try {
      ctx = await openOrderApiSession(cookies);
    } catch (err) {
      for (const t of targets) results.set(t.orderId, { orderId: t.orderId, status: "no_session", units: t.unitIds.length, error: (err as Error).message });
      return;
    }
    try {
      for (const t of targets) {
        if (opts.cancelled?.()) break;
        const key = rawInvoiceKey(t.userId, t.orderId);
        try {
          if (!opts.force && (await invoiceExists(key))) {
            await Order.updateMany({ _id: { $in: t.rowIds } }, { $set: { invoice_s3_key: key, is_invoice_downloaded: true } });
            results.set(t.orderId, { orderId: t.orderId, status: "already", units: t.unitIds.length });
            continue;
          }
          const pdfs = await downloadInvoiceViaApi(ctx, t.orderId);
          const bundle = await mergeInvoiceBundle(pdfs);
          const put = await putInvoice(key, bundle.pdf);
          await registerInvoice({
            userId: t.userId,
            platformAccountId: t.accountId,
            orderId: t.orderId,
            key,
            etag: put.etag,
            size: put.size,
            parts: bundle.parts,
            unitIds: t.unitIds,
          });
          await Order.updateMany(
            { _id: { $in: t.rowIds } },
            { $set: { invoice_s3_key: key, invoice_downloaded_at: new Date(), invoice_size: put.size, is_invoice_downloaded: true, invoice_parts: bundle.parts } }
          );
          results.set(t.orderId, { orderId: t.orderId, status: "downloaded", units: t.unitIds.length, parts: bundle.parts.map((p) => p.type), sizeKb: Math.round(put.size / 1024) });
          log("info", `${t.orderId}: invoice ${opts.force ? "re-" : ""}downloaded (${bundle.parts.map((p) => p.type).join(", ")})`);
        } catch (err) {
          results.set(t.orderId, { orderId: t.orderId, status: "failed", units: t.unitIds.length, error: (err as Error).message.split("\n")[0].slice(0, 160) });
          log("warn", `${t.orderId}: invoice failed (${(err as Error).message.split("\n")[0].slice(0, 120)})`);
        }
        await new Promise((r) => setTimeout(r, 250));
      }
    } finally {
      await ctx.dispose().catch(() => undefined);
    }
  };

  // Pool of `concurrency` accounts at a time.
  const entries = [...byAccount.entries()];
  const conc = Math.max(1, Math.min(opts.concurrency || 6, entries.length || 1));
  let idx = 0;
  const worker = async () => {
    while (idx < entries.length) {
      const [accountId, targets] = entries[idx++];
      await processAccount(accountId, targets);
    }
  };
  await Promise.all(Array.from({ length: conc }, () => worker()));

  return want.map((o) => results.get(o)!);
}
