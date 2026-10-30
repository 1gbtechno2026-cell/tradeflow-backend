import { Types } from "mongoose";
import { connectDb } from "../db.js";
import { Order } from "../models/Order.js";
import { PlatformId } from "../models/PlatformId.js";
import { lockAccount } from "../services/accountLock.js";
import { listProxies, toPlaywrightProxy } from "../services/proxyPool.js";
import { resolveLoggedInSession, workspaceUserId } from "../services/sessionStore.js";
import { openOrderApiSession, OrderApiError, OrderApiSessionError } from "../services/orderApi.js";
import { readUnitViaApi, walkOrderList } from "../services/orderApiSync.js";
import { COMPARED_FIELDS, diffAgainstScraper } from "../services/orderApiTest.js";

/**
 * Does Flipkart's order API (no browser) give the same data the fetch stores?
 * Read-only: N logged-in Platform IDs that have orders in order_details, each
 * walked over HTTP with its saved session — list pages back to --days, then
 * details for up to --units units per account — and compared with what the
 * last fetch saved. Nothing is written to Mongo.
 *
 *   npm run orders:api-bench -- --n 300 --concurrency 8 --days 60 --units 3
 *   --direct     no proxy (default: Proxy Pool round robin)
 *   --pages 10   list pages per account at most
 *   --skip 100   start further down the account list   --email a@b.in,c@d.in   only these
 *   --verbose    every field that differs
 *
 * "listed" counts only units whose order date is inside the window: the list
 * walk stops at the first page that is entirely older, so its last page
 * carries a few older units too.
 */
const argv = process.argv.slice(2);
const arg = (k: string, d = "") => {
  const i = argv.indexOf(`--${k}`);
  return i >= 0 ? String(argv[i + 1] ?? "") : d;
};
const has = (k: string) => argv.includes(`--${k}`);
const N = Math.max(1, Number(arg("n", "100")) || 100);
const CONC = Math.max(1, Number(arg("concurrency", "8")) || 8);
const DAYS = Math.max(1, Number(arg("days", "60")) || 60);
const UNITS = Math.max(0, Number(arg("units", "3")) || 0);
const PAGES = Math.max(1, Number(arg("pages", "10")) || 10);
const SKIP = Math.max(0, Number(arg("skip", "0")) || 0);
const EMAILS = arg("email").split(",").map((e) => e.trim().toLowerCase()).filter(Boolean);
const VERBOSE = has("verbose");

function pct(xs: number[], p: number) {
  if (!xs.length) return 0;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface AccountResult {
  email: string;
  outcome: "ok" | "session_refused" | "api_error" | "locked" | "no_session";
  error?: string;
  ms: number;
  pages: number;
  listed: number;
  stored: number;
  listedNotStored: number;
  storedNotListed: number;
  details: number;
  fieldsSame: number;
  fieldsDiff: number;
  /** Of the differing fields: the stored row had nothing there (the API is richer) vs both sides had a value. */
  storedEmpty: number;
  bothDiffer: number;
  diffFields: Record<string, number>;
  dc?: string;
}

async function main() {
  await connectDb();
  const userId = new Types.ObjectId(workspaceUserId());
  const since = new Date(Date.now() - DAYS * 24 * 3600_000);
  // Accounts with orders in the window, logged in — the ones a fetch would actually read.
  const withOrders = await Order.aggregate<{ _id: string; n: number }>([
    { $match: { userId, order_date: { $gte: since } } },
    { $group: { _id: "$platform_email", n: { $sum: 1 } } },
    { $sort: { n: -1 } },
    { $limit: (SKIP + N) * 3 },
  ]);
  const emails = withOrders.map((r) => r._id);
  const loggedIn = await PlatformId.find({ userId, email: { $in: emails }, status: "logged_in" }).select("email").lean();
  const okSet = new Set(loggedIn.map((r) => String(r.email)));
  const picked = EMAILS.length ? EMAILS : emails.filter((e) => okSet.has(e)).slice(SKIP, SKIP + N);
  const proxies = has("direct") ? [] : (await listProxies()).map(toPlaywrightProxy);
  console.log(`order-api bench · ${picked.length} logged-in account(s) with orders in the last ${DAYS} d (of ${emails.length} with orders) · concurrency ${CONC} · ${proxies.length ? `${proxies.length} proxies round robin` : "direct"} · details for ${UNITS} unit(s) each`);

  const results: AccountResult[] = [];
  let next = 0;
  const wall0 = Date.now();
  const runner = async (lane: number) => {
    while (next < picked.length) {
      const i = next++;
      const email = picked[i];
      const t0 = Date.now();
      const r: AccountResult = { email, outcome: "ok", ms: 0, pages: 0, listed: 0, stored: 0, listedNotStored: 0, storedNotListed: 0, details: 0, fieldsSame: 0, fieldsDiff: 0, storedEmpty: 0, bothDiffer: 0, diffFields: {} };
      const unlock = await lockAccount(email, "bench-order-api", 5 * 60_000);
      if (!unlock) {
        r.outcome = "locked";
        r.ms = Date.now() - t0;
        results.push(r);
        continue;
      }
      let ctx: Awaited<ReturnType<typeof openOrderApiSession>> | null = null;
      try {
        const s = await resolveLoggedInSession(email);
        if (!s.ok) {
          r.outcome = "no_session";
          r.error = s.reason;
          continue;
        }
        const proxy = proxies.length ? proxies[i % proxies.length] : undefined;
        ctx = await openOrderApiSession(s.cookies, proxy ? { proxy } : {});
        const walk = await walkOrderList(ctx, since, { maxPages: PAGES });
        r.pages = walk.pages;
        const inWindow = walk.units.filter((u) => u.orderDate && u.orderDate >= since);
        r.listed = inWindow.length;
        const storedRows = await Order.find({ userId, platform_email: email, order_date: { $gte: since } }).select("unit_id").lean();
        const storedIds = new Set(storedRows.map((x) => String(x.unit_id)));
        const listedIds = new Set(inWindow.map((u) => u.unitId));
        r.stored = storedIds.size;
        r.listedNotStored = [...listedIds].filter((id) => !storedIds.has(id)).length;
        r.storedNotListed = [...storedIds].filter((id) => !listedIds.has(id)).length;
        // Details: prefer units the fetch stored, so the diff has a baseline.
        const sample = [...inWindow.filter((u) => storedIds.has(u.unitId)), ...inWindow.filter((u) => !storedIds.has(u.unitId))].slice(0, UNITS);
        for (const u of sample) {
          await sleep(250 + Math.random() * 400);
          const read = await readUnitViaApi(ctx, u.orderId, u.unitId, u.unit);
          r.details += 1;
          const saved = (await Order.findOne({ userId, unit_id: u.unitId }).lean()) as Record<string, unknown> | null;
          if (!saved) continue;
          for (const d of diffAgainstScraper(read.mapped, saved)) {
            if (d.same) r.fieldsSame += 1;
            else {
              r.fieldsDiff += 1;
              if (d.scraper == null || d.scraper === "") r.storedEmpty += 1;
              else r.bothDiffer += 1;
              r.diffFields[d.field] = (r.diffFields[d.field] || 0) + 1;
              if (VERBOSE) console.log(`    ${email} ${u.orderId}/${u.unitId} ${d.field}: api=${JSON.stringify(d.api)} stored=${JSON.stringify(d.scraper)}`);
            }
          }
        }
      } catch (err) {
        if (err instanceof OrderApiSessionError) r.outcome = "session_refused";
        else r.outcome = "api_error";
        r.error = err instanceof Error ? err.message.split("\n")[0].slice(0, 140) : String(err);
      } finally {
        r.ms = Date.now() - t0;
        results.push(r);
        await ctx?.dispose().catch(() => undefined);
        await unlock().catch(() => undefined);
        const tag = r.outcome === "ok" ? `ok · ${r.pages} page(s) · listed ${r.listed} / stored ${r.stored}${r.listedNotStored ? ` · +${r.listedNotStored} not stored` : ""}${r.storedNotListed ? ` · ${r.storedNotListed} stored not listed` : ""} · ${r.details} detail(s) · fields ${r.fieldsSame} same / ${r.fieldsDiff} differ` : `${r.outcome}${r.error ? `: ${r.error}` : ""}`;
        console.log(`${String(results.length).padStart(4)}/${picked.length} lane ${lane} ${String(r.ms).padStart(6)} ms  ${email.padEnd(32)} ${tag}`);
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(CONC, picked.length) }, (_, i) => runner(i + 1)));
  const wall = Date.now() - wall0;

  const by = (o: AccountResult["outcome"]) => results.filter((r) => r.outcome === o);
  const ok = by("ok");
  const sum = (f: (r: AccountResult) => number) => ok.reduce((a, r) => a + f(r), 0);
  console.log("\n=== matrix ===");
  console.log(`accounts ${results.length} · wall ${(wall / 1000).toFixed(1)} s · ${(results.length / (wall / 1000)).toFixed(2)} accounts/s`);
  console.log(`ok ${ok.length} · session refused ${by("session_refused").length} · api error ${by("api_error").length} · no session ${by("no_session").length} · locked (busy) ${by("locked").length}`);
  console.log(`per account: p50 ${pct(ok.map((r) => r.ms), 50)} ms · p95 ${pct(ok.map((r) => r.ms), 95)} ms · max ${pct(ok.map((r) => r.ms), 100)} ms`);
  console.log(`list: ${sum((r) => r.pages)} page(s) · ${sum((r) => r.listed)} unit(s) listed by the API · ${sum((r) => r.stored)} stored by earlier fetches · ${sum((r) => r.listedNotStored)} listed but not stored · ${sum((r) => r.storedNotListed)} stored but not listed`);
  const same = sum((r) => r.fieldsSame);
  const diff = sum((r) => r.fieldsDiff);
  console.log(`details: ${sum((r) => r.details)} unit(s) read · ${COMPARED_FIELDS.length} fields each compared with the stored row · ${same} same / ${diff} differ (${same + diff ? ((same / (same + diff)) * 100).toFixed(1) : "0"}% agreement)`);
  console.log(`  of the differing fields: ${sum((r) => r.storedEmpty)} had nothing stored (the API has the value) · ${sum((r) => r.bothDiffer)} differ on both sides (older stored snapshot or wording)`);
  const fields = new Map<string, number>();
  for (const r of ok) for (const [k, v] of Object.entries(r.diffFields)) fields.set(k, (fields.get(k) || 0) + v);
  for (const [k, v] of [...fields.entries()].sort((a, b) => b[1] - a[1])) console.log(`  differs ${String(v).padStart(4)}  ${k}`);
  const errors = new Map<string, number>();
  for (const r of results) if (r.outcome !== "ok" && r.error) errors.set(`${r.outcome}: ${r.error.replace(/OD\d+/g, "OD…").replace(/\d{3,}/g, "#")}`, (errors.get(`${r.outcome}: ${r.error.replace(/OD\d+/g, "OD…").replace(/\d{3,}/g, "#")}`) || 0) + 1);
  for (const [k, v] of [...errors.entries()].sort((a, b) => b[1] - a[1])) console.log(`  ${String(v).padStart(4)}  ${k}`);
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
