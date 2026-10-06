import fs from "node:fs";
import path from "node:path";
import mongoose from "mongoose";
import { connectDb } from "../db.js";
import { Order } from "../models/Order.js";
import { workspaceUserId } from "../services/sessionStore.js";
import { mapApiUnitToOrder, orderViewFromPageFetch } from "../services/orderApi.js";
import { COMPARED_FIELDS, diffAgainstScraper } from "../services/orderApiTest.js";

/**
 * Replay a fetch/update trace (ORDER_TRACE_DIR/<run>/) with no Flipkart:
 * every API unit file is re-mapped from its raw response and compared with
 *   1. the document the run itself mapped (determinism of the mapper), and
 *   2. the order_details document in the database now.
 * Scrape-mode unit files hold page snapshots and are listed, not re-mapped.
 *
 *   npm run orders:replay -- debug/order-traces/fetch-2026-10-06T…
 */
async function main() {
  const dir = path.resolve(process.argv[2] || "");
  if (!dir || !fs.existsSync(path.join(dir, "manifest.json"))) {
    console.error("usage: npm run orders:replay -- <trace run directory containing manifest.json>");
    process.exit(2);
  }
  const manifest = JSON.parse(fs.readFileSync(path.join(dir, "manifest.json"), "utf8")) as Record<string, unknown>;
  console.log(`trace ${manifest.kind} mode=${manifest.mode} started ${manifest.startedAt} units=${manifest.units} listPages=${manifest.listPages} pass=${manifest.pass ?? "n/a"}`);
  for (const inv of (manifest.invariants as Array<{ line?: string }>) || []) console.log("  ", inv.line);

  await connectDb();
  const userId = workspaceUserId();
  const files = fs.readdirSync(dir).filter((f) => f.startsWith("unit-") && f.endsWith(".json")).sort();
  let api = 0;
  let scrape = 0;
  let mapperDrift = 0;
  let dbDiff = 0;
  let dbMissing = 0;
  for (const f of files) {
    const rec = JSON.parse(fs.readFileSync(path.join(dir, f), "utf8")) as {
      mode: string;
      orderId: string;
      unitId: string;
      raw?: unknown;
      mapped?: Record<string, unknown>;
      outcome?: string;
    };
    if (rec.mode !== "api" || !rec.raw) {
      scrape += 1;
      console.log(`  ${f}: scrape snapshot (${rec.outcome || "read"}) — not re-mapped`);
      continue;
    }
    api += 1;
    const view = orderViewFromPageFetch(rec.raw);
    if (!view) {
      console.log(`  ${f}: raw body has no order view — cannot re-map`);
      mapperDrift += 1;
      continue;
    }
    const remapped = mapApiUnitToOrder(view, rec.unitId);
    const againstRun = rec.mapped ? diffAgainstScraper(remapped, rec.mapped).filter((d) => !d.same) : [];
    const saved = (await Order.findOne({ userId, unit_id: rec.unitId }).lean()) as Record<string, unknown> | null;
    const againstDb = saved ? diffAgainstScraper(remapped, Object.fromEntries(COMPARED_FIELDS.map((k) => [k, saved[k]]))).filter((d) => !d.same) : null;
    if (againstRun.length) mapperDrift += 1;
    if (!saved) dbMissing += 1;
    else if (againstDb && againstDb.length) dbDiff += 1;
    const tag = againstRun.length ? `MAPPER DRIFT ${againstRun.map((d) => d.field).join(",")}` : "mapper ok";
    const dbTag = !saved ? "no document in order_details" : againstDb && againstDb.length ? `DB differs: ${againstDb.map((d) => d.field).join(",")}` : "db ok";
    console.log(`  ${rec.orderId} ${rec.unitId} (${rec.outcome}): ${tag} · ${dbTag}`);
    for (const d of againstDb || []) console.log(`      ${d.field}: replay=${JSON.stringify(d.api)?.slice(0, 60)} db=${JSON.stringify(d.scraper)?.slice(0, 60)}`);
  }
  console.log(`\nunits: ${files.length} (api ${api}, scrape ${scrape}) · mapper drift ${mapperDrift} · db differs ${dbDiff} · db missing ${dbMissing}`);
  await mongoose.disconnect();
  process.exit(mapperDrift || dbDiff ? 1 : 0);
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
