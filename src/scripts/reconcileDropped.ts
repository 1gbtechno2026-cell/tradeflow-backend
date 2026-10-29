/**
 * Reconcile "dropped" HTTP-engine orders against Flipkart's My Orders.
 *
 * The HTTP engine parks a job as needs_reconciliation (ORDER_NOT_LISTED) when
 * Flipkart answered SUCCESS with an order id but My Orders did not list it
 * within HTTP_VERIFY_WAIT_MS. 2026-10-28 showed Flipkart can take far longer
 * than that: 167 of 169 such orders were in My Orders the next morning.
 *
 *   npx tsx src/scripts/reconcileDropped.ts --hours 48                 # read-only report
 *   npx tsx src/scripts/reconcileDropped.ts --hours 48 --apply         # flip the listed ones to paid
 *   npx tsx src/scripts/reconcileDropped.ts --order-ids OD…,OD… --apply
 *
 * For every needs_reconciliation job in the window (or the given order ids),
 * the account's saved session reads My Orders (read-only). A job whose order
 * id IS listed becomes `paid` (step paid, error fields cleared, the listed
 * status kept on result.listedStatus, a log line added), its batch's
 * purchasedQuantity grows by the job's quantity, and its order sync is
 * re-queued unless already done. Jobs whose order is not listed, or whose
 * session is dead, are left exactly as they are and reported.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { connectDb } from "../db.js";
import { CheckoutJob } from "../models/CheckoutJob.js";
import { CheckoutBatch } from "../models/CheckoutBatch.js";
import { resolveLoggedInSession } from "../services/sessionStore.js";
import { openOrderApiSession, fetchOrderList, listUnits } from "../services/orderApi.js";
import { enqueueOrderSync } from "../services/orderSync.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const argv = process.argv.slice(2);
const arg = (k: string) => (argv.indexOf(k) >= 0 ? String(argv[argv.indexOf(k) + 1] || "") : "");
const APPLY = argv.includes("--apply");
const hours = Number(arg("--hours")) || 48;
const onlyIds = arg("--order-ids").split(",").map((s) => s.trim().toUpperCase()).filter(Boolean);

interface Listed { orderId: string; status: string; orderDate: Date | null }

async function readMyOrders(email: string, since: number): Promise<{ ok: true; units: Listed[] } | { ok: false; reason: string }> {
  const s = await resolveLoggedInSession(email);
  if (!s.ok) return { ok: false, reason: `session: ${s.reason}` };
  const ctx = await openOrderApiSession(s.cookies);
  try {
    const units: Listed[] = [];
    let next: Array<{ key: string; value: string }> = [];
    for (let page = 1; page <= 4; page += 1) {
      const pg = await fetchOrderList(ctx, page, next);
      const rows = listUnits(pg);
      for (const u of rows) units.push({ orderId: u.orderId.toUpperCase(), status: u.status, orderDate: u.orderDate });
      const oldest = rows.map((u) => u.orderDate?.getTime() || 0).filter(Boolean).sort()[0];
      if (!pg.moreOrder || (oldest && oldest < since)) break;
      next = pg.nextCallParams;
    }
    return { ok: true, units };
  } catch (e) {
    return { ok: false, reason: `error: ${e instanceof Error ? e.message.slice(0, 140) : String(e)}` };
  } finally {
    await ctx.dispose();
  }
}

async function main() {
  await connectDb();
  const since = Date.now() - hours * 3600e3;
  const filter: Record<string, unknown> = onlyIds.length
    ? { "result.flipkartOrderId": { $in: onlyIds } }
    : { status: "needs_reconciliation", createdAt: { $gte: new Date(since) } };
  const jobs = await CheckoutJob.find(filter).select("email batchId quantity status result.flipkartOrderId orderSync createdAt").lean();
  const todo = jobs.filter((j) => j.status === "needs_reconciliation" && j.result?.flipkartOrderId);
  console.log(`${todo.length} needs_reconciliation job(s) with an order id${onlyIds.length ? " (by order id)" : ` in the last ${hours} h`}${APPLY ? " — APPLY" : " — read-only"}`);
  const byEmail = new Map<string, typeof todo>();
  for (const j of todo) byEmail.set(j.email, [...(byEmail.get(j.email) || []), j]);

  const report: Array<Record<string, unknown>> = [];
  const perBatch = new Map<string, number>();
  let flipped = 0, listed = 0, notListed = 0, unknown = 0, resynced = 0;
  let i = 0;
  for (const [email, mine] of byEmail) {
    i += 1;
    const earliest = Math.min(...mine.map((j) => new Date(j.createdAt).getTime())) - 6 * 3600e3;
    const r = await readMyOrders(email, earliest);
    if (!r.ok) {
      unknown += mine.length;
      for (const j of mine) report.push({ email, orderId: j.result?.flipkartOrderId, outcome: r.reason });
      continue;
    }
    for (const j of mine) {
      const oid = String(j.result?.flipkartOrderId).toUpperCase();
      const hit = r.units.find((u) => u.orderId === oid);
      if (!hit) {
        notListed += 1;
        report.push({ email, orderId: oid, outcome: "not in My Orders" });
        continue;
      }
      listed += 1;
      report.push({ email, orderId: oid, outcome: "listed", status: hit.status, batch: j.batchId });
      if (!APPLY) continue;
      const stamp = new Date().toISOString().slice(0, 16).replace("T", " ");
      await CheckoutJob.updateOne(
        { _id: j._id, status: "needs_reconciliation" },
        {
          $set: {
            status: "paid",
            step: "paid",
            error: "",
            errorCode: "",
            errorCodeDisplay: "",
            errorSource: "",
            errorStage: "",
            "result.listed": true,
            "result.listedStatus": hit.status,
            "result.listingCheckedAt": new Date(),
            "result.reconciledAt": new Date(),
          },
          $push: {
            logs: { at: new Date(), level: "info", message: `Reconciled ${stamp} UTC: Flipkart's My Orders lists ${oid} (${hit.status}) — counted as purchased. The post-pay check had given up before Flipkart listed it.` },
          },
        }
      );
      perBatch.set(j.batchId, (perBatch.get(j.batchId) || 0) + (j.quantity || 1));
      flipped += 1;
      if (j.orderSync?.status !== "done") {
        await CheckoutJob.updateOne({ _id: j._id }, { $set: { "orderSync.status": "pending", "orderSync.attempts": 0, "orderSync.lastError": "" } });
        await enqueueOrderSync(String(j._id));
        resynced += 1;
      }
    }
    if (i % 20 === 0) console.log(`…${i}/${byEmail.size} accounts · listed ${listed} · not listed ${notListed} · unknown ${unknown}`);
  }
  if (APPLY) {
    for (const [batchId, units] of perBatch) await CheckoutBatch.updateOne({ batchId }, { $inc: { purchasedQuantity: units } });
  }
  const out = path.resolve(here, `../../reconcile-dropped-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}.json`);
  fs.writeFileSync(out, JSON.stringify({ apply: APPLY, hours, listed, notListed, unknown, flipped, resynced, perBatch: Object.fromEntries(perBatch), rows: report }, null, 1));
  const statuses: Record<string, number> = {};
  for (const r of report) if (r.outcome === "listed") statuses[String(r.status)] = (statuses[String(r.status)] || 0) + 1;
  console.log(`\nlisted in My Orders: ${listed} ${JSON.stringify(statuses)} · not listed: ${notListed} · could not check: ${unknown}`);
  if (APPLY) console.log(`flipped to paid: ${flipped} · order sync re-queued: ${resynced} · batch units added: ${JSON.stringify(Object.fromEntries([...perBatch].map(([b, n]) => [b.slice(0, 8), n])))}`);
  for (const r of report.filter((x) => x.outcome !== "listed")) console.log("   ", r.email, r.orderId, r.outcome);
  console.log(`report → ${out}`);
  process.exit(0);
}

main().catch((e) => {
  console.error("ERR", e instanceof Error ? e.message : String(e));
  process.exit(1);
});
