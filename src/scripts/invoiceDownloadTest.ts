/**
 * Phase 7a prototype — scoped invoice-download test.
 *
 * Picks the first N (default 100) distinct DELIVERED orders that have no
 * invoice yet, grouped by account, and — with --run — downloads each bundle
 * over Flipkart's order API (the proven path: downloadInvoiceAnswerViaApi → the one
 * store helper: originals, invoice_downloads, merge, raw PUT, register, units), then lets the
 * existing S3→SQS→Lambda pipeline extract. API-only: if the API cannot deliver
 * a bundle, it is recorded as "needs page" and skipped (no Chrome here).
 *
 *   npm run test:invoice-dl               # DRY RUN: select + verify sessions, download nothing
 *   npm run test:invoice-dl -- --run      # really download the N
 *   npm run test:invoice-dl -- --run --limit=100
 *
 * Writes to whatever INVOICE_S3_BUCKET / Atlas the .env points at (dev on this
 * Mac). Idempotent: an order whose raw key already exists is skipped.
 */
import { connectDb } from "../db.js";
import mongoose from "mongoose";
import { config } from "../config.js";
import { Order } from "../models/Order.js";
import { PlatformId } from "../models/PlatformId.js";
import { cookiesFor } from "../services/orderFetch.js";
import { openOrderApiSession } from "../services/orderApi.js";
import { downloadInvoiceAnswerViaApi, rawInvoiceKey, invoiceExists, invoiceStoreEnabled } from "../services/invoiceCapture.js";
import { storeInvoiceDownload } from "../services/invoiceOriginals.js";

const args = new Set(process.argv.slice(2));
const RUN = args.has("--run");
const LIMIT = Number((process.argv.find((a) => a.startsWith("--limit=")) || "--limit=100").split("=")[1]) || 100;
const CONC = Math.max(1, Number((process.argv.find((a) => a.startsWith("--concurrency=")) || "--concurrency=1").split("=")[1]) || 1);

interface OrderPick {
  userId: string;
  accountId: string;
  orderId: string;
  orderUrl: string;
  unitIds: string[];
  rowIds: mongoose.Types.ObjectId[];
}

async function selectOrders(limit: number): Promise<Map<string, OrderPick[]>> {
  // delivered units with no invoice key, newest first
  const rows = await Order.find({
    status_key: /deliver/i,
    $or: [{ invoice_s3_key: { $in: [null, ""] } }, { invoice_s3_key: { $exists: false } }],
  })
    .select("order_id unit_id order_url platformAccountId userId")
    .sort({ order_date: -1 })
    .lean();

  const byOrder = new Map<string, OrderPick>();
  for (const r of rows) {
    const orderId = String(r.order_id);
    if (!byOrder.has(orderId)) {
      if (byOrder.size >= limit) continue;
      byOrder.set(orderId, {
        userId: String(r.userId),
        accountId: String(r.platformAccountId),
        orderId,
        orderUrl: String((r as { order_url?: string }).order_url || ""),
        unitIds: [],
        rowIds: [],
      });
    }
    const pick = byOrder.get(orderId);
    if (pick) {
      pick.unitIds.push(String(r.unit_id));
      pick.rowIds.push(r._id as mongoose.Types.ObjectId);
    }
  }
  // group by account for one session per account
  const byAccount = new Map<string, OrderPick[]>();
  for (const pick of byOrder.values()) {
    const list = byAccount.get(pick.accountId) || [];
    list.push(pick);
    byAccount.set(pick.accountId, list);
  }
  return byAccount;
}

async function main() {
  console.log(`\n=== Invoice download test — ${RUN ? "RUN" : "DRY RUN"} · limit ${LIMIT} ===`);
  console.log(`bucket=${config.invoiceBucket || "(none)"}  region=${config.awsRegion}`);
  if (!invoiceStoreEnabled()) {
    console.error("INVOICE_S3_BUCKET not set — aborting.");
    process.exit(1);
  }
  await connectDb();

  const byAccount = await selectOrders(LIMIT);
  const totalOrders = [...byAccount.values()].reduce((a, l) => a + l.length, 0);
  const totalUnits = [...byAccount.values()].reduce((a, l) => a + l.reduce((b, p) => b + p.unitIds.length, 0), 0);
  console.log(`\nselected ${totalOrders} distinct orders across ${byAccount.size} accounts (${totalUnits} units)`);

  // session availability
  let withSession = 0;
  let withoutSession = 0;
  const accountCookies = new Map<string, unknown[]>();
  for (const accountId of byAccount.keys()) {
    const row = await PlatformId.findById(accountId).lean();
    const cookies = cookiesFor(row as Parameters<typeof cookiesFor>[0]);
    if (cookies.length) {
      withSession++;
      accountCookies.set(accountId, cookies);
    } else {
      withoutSession++;
    }
  }
  console.log(`accounts with a usable session: ${withSession} · without: ${withoutSession}`);
  const reachableOrders = [...byAccount.entries()]
    .filter(([id]) => accountCookies.has(id))
    .reduce((a, [, l]) => a + l.length, 0);
  console.log(`orders reachable over the API (session present): ${reachableOrders} / ${totalOrders}`);

  if (!RUN) {
    console.log(`\nDRY RUN — nothing downloaded. Re-run with --run to download the ${reachableOrders} reachable orders.`);
    await mongoose.connection.close();
    return;
  }

  // ---- real download (parallel account-windows) ----
  const t0 = Date.now();
  const stat = { tried: 0, stored: 0, skippedExisting: 0, needsPage: 0, failed: 0 };
  const partCount: Record<string, number> = {};

  async function processAccount(accountId: string, picks: OrderPick[]) {
    const cookies = accountCookies.get(accountId);
    if (!cookies) return;
    let ctx;
    try {
      ctx = await openOrderApiSession(cookies);
    } catch (err) {
      console.warn(`  account ${accountId}: session open failed (${(err as Error).message}) — skipping ${picks.length}`);
      stat.failed += picks.length;
      return;
    }
    for (const p of picks) {
      stat.tried++;
      const key = rawInvoiceKey(p.userId, p.orderId);
      try {
        if (await invoiceExists(key)) {
          stat.skippedExisting++;
          await Order.updateMany({ _id: { $in: p.rowIds } }, { $set: { invoice_s3_key: key, is_invoice_downloaded: true } });
          continue;
        }
        // API only (unzips multi-invoice bundles); stored through the one store helper every download path uses.
        const answer = await downloadInvoiceAnswerViaApi(ctx, p.orderId);
        const bundle = await storeInvoiceDownload({ userId: p.userId, accountId: p.accountId, orderId: p.orderId, answer, unitIds: p.unitIds, unitRowIds: p.rowIds, path: "by-order-id" });
        for (const part of bundle.parts) partCount[part.type] = (partCount[part.type] || 0) + 1;
        stat.stored++;
        if (stat.stored % 50 === 0) console.log(`  …${stat.stored} stored (${Math.round((Date.now() - t0) / 1000)}s)`);
      } catch (err) {
        const msg = (err as Error).message || String(err);
        if (/not a PDF|neither PDF|HTTP 4|HTTP 5/.test(msg)) stat.needsPage++;
        else stat.failed++;
        console.warn(`  ${p.orderId}: ${msg.split("\n")[0].slice(0, 140)}`);
      }
      await new Promise((r) => setTimeout(r, 300));
    }
    await ctx.dispose().catch(() => undefined);
  }

  // Pool of CONC accounts at a time.
  const entries = [...byAccount.entries()];
  console.log(`downloading with ${CONC} parallel account-window(s)…`);
  let idx = 0;
  async function worker() {
    while (idx < entries.length) {
      const [accountId, picks] = entries[idx++];
      await processAccount(accountId, picks);
    }
  }
  await Promise.all(Array.from({ length: Math.min(CONC, entries.length) }, () => worker()));

  const secs = Math.round((Date.now() - t0) / 1000);
  console.log(`\n=== done in ${secs}s ===`);
  console.log(`tried=${stat.tried} stored=${stat.stored} skippedExisting=${stat.skippedExisting} needsPage=${stat.needsPage} failed=${stat.failed}`);
  console.log(`rate=${stat.stored && secs ? (stat.stored / secs).toFixed(2) : "0"} stored/s`);
  console.log(`parts (worker coarse classify): ${JSON.stringify(partCount)}`);
  console.log(`\nNow watch the pipeline turn these 'pending' rows into 'processed' (S3→SQS→Lambda).`);
  await mongoose.connection.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
