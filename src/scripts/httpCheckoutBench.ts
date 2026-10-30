import { connectDb } from "../db.js";
import { PlatformId } from "../models/PlatformId.js";
import { lockAccount } from "../services/accountLock.js";
import { listProxies, toPlaywrightProxy } from "../services/proxyPool.js";
import { resolveLoggedInSession, workspaceUserId } from "../services/sessionStore.js";
import { runHttpCheckout, type HttpCheckoutOutcome } from "../services/httpCheckout.js";

/**
 * Bench for the HTTP checkout engine, outside the queue: N logged-in platform
 * IDs run the engine at once (dry run by default — up to the payment token,
 * nothing bought, the cart is cleaned again) and the matrix is printed.
 *
 *   npm run bench:http -- --n 10                      10 accounts, all at once, dry run
 *   npm run bench:http -- --n 100 --concurrency 50    100 accounts, 50 in flight
 *   npm run bench:http -- --n 1 --real 1              ONE real COD order
 *   --skip 200     start further down the account list (fresh accounts per round)
 *   --direct       no proxy; default: the Proxy Pool's active proxies, round robin
 *   --product URL  --pincode 122016  --verbose
 *   --phrase "UDYOG VIHAR"   address match by Match Phrase (Settings → Checkout engine ON)
 *   --wait 15                seconds to wait for My Orders after a REAL Pay
 *   --name "X"               contact name on the job address (default DASHMOBILES PRIVATE LIMITED)
 *   --email a@b.in,c@d.in    only these accounts
 *
 * Accounts busy in the worker (account:lock) are skipped. Nothing is written
 * to CheckoutJob; this is the engine alone.
 */
const argv = process.argv.slice(2);
const arg = (k: string, d = "") => {
  const i = argv.indexOf(`--${k}`);
  return i >= 0 ? String(argv[i + 1] ?? "") : d;
};
const has = (k: string) => argv.includes(`--${k}`);
const N = Math.max(1, Number(arg("n", "10")) || 10);
const CONC = Math.max(1, Number(arg("concurrency", String(N))) || N);
const SKIP = Math.max(0, Number(arg("skip", "0")) || 0);
const REAL = Number(arg("real", "0")) || 0;
const PRODUCT = arg("product", "https://www.flipkart.com/techblaze-80-w-supervooc-7-3-wall-charger-mobile-detachable-cable/p/itm030b5480ff521?pid=ACCHBQAYAKFJ59W6&lid=LSTACCHBQAYAKFJ59W6A5ELR1&marketplace=FLIPKART");
const PINCODE = arg("pincode", "122016");
const VERBOSE = has("verbose");
const PHRASE = arg("phrase", "");
const NAME = arg("name", "DASHMOBILES PRIVATE LIMITED");
const ADDRESS = { name: NAME, mobile: "", pincode: PINCODE, locality: "UDYOG VIHAR", addressLine1: "Cabin 2, 581 , PHASE -5", city: "Gurugram", state: "Haryana", addressType: "Home" as const, gstNumber: "06AALCD3578N1Z0", companyName: "DASHMOBILES PRIVATE LIMITED", checkoutPincode: "", matchPhrase: PHRASE };
const DIRECT = has("direct");

function pct(xs: number[], p: number) {
  if (!xs.length) return 0;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
}

async function main() {
  await connectDb();
  const userId = workspaceUserId();
  const emails = arg("email").split(",").map((e) => e.trim().toLowerCase()).filter(Boolean);
  const rows = await PlatformId.find(emails.length ? { userId, email: { $in: emails } } : { userId, status: "logged_in" }).sort({ _id: 1 }).skip(SKIP).limit(N * 2).select("email").lean();
  const proxies = DIRECT ? [] : (await listProxies()).map(toPlaywrightProxy);
  console.log(`bench · ${N} account(s) from offset ${SKIP} · concurrency ${CONC} · address match ${PHRASE ? `phrase "${PHRASE}"` : "name"} · ${REAL ? `REAL orders: ${REAL}` : "dry run"} · ${proxies.length ? `${proxies.length} proxies round robin` : "direct"} · pin ${PINCODE}`);
  if (REAL > N) throw new Error("--real cannot exceed --n");

  const picked: Array<{ email: string; cookies: unknown[]; unlock: () => Promise<void> }> = [];
  for (const r of rows) {
    if (picked.length >= N) break;
    const s = await resolveLoggedInSession(String(r.email));
    if (!s.ok) continue;
    const unlock = await lockAccount(s.email, "bench-http", 5 * 60_000);
    if (!unlock) continue;
    picked.push({ email: s.email, cookies: s.cookies, unlock });
  }
  console.log(`accounts ready: ${picked.length}`);

  const results: Array<{ email: string; outcome: HttpCheckoutOutcome; ms: number; proxy: string }> = [];
  const wallStart = Date.now();
  let next = 0;
  let realLeft = REAL;
  const runner = async () => {
    while (next < picked.length) {
      const i = next++;
      const acct = picked[i];
      const proxy = proxies.length ? proxies[i % proxies.length] : null;
      const dryRun = realLeft <= 0;
      if (!dryRun) realLeft -= 1;
      const t = Date.now();
      const outcome = await runHttpCheckout({
        cookies: acct.cookies,
        proxy,
        productUrl: PRODUCT,
        quantity: 1,
        pincode: PINCODE,
        address: has("no-address-fix") ? undefined : ADDRESS,
        deliverySlaDays: Number(arg("sla", "9")) || 9,
        gstMandatory: false,
        dryRun,
        addressMatch: PHRASE ? "phrase" : "name",
        verifyWaitMs: arg("wait") ? Number(arg("wait")) * 1000 : undefined,
        log: (level, message) => {
          if (VERBOSE || level !== "info" || !dryRun) console.log(`  [${acct.email}] ${message}`);
        },
      });
      const ms = Date.now() - t;
      results.push({ email: acct.email, outcome, ms, proxy: proxy ? proxy.server.replace(/^https?:\/\//, "") : "direct" });
      const tag = outcome.kind === "failed" ? `failed ${outcome.failure.code}` : outcome.kind === "fallback" ? `fallback: ${outcome.reason.slice(0, 90)}` : outcome.kind === "placed" ? `PLACED ${outcome.orderId}` : `dry_run ₹${outcome.checkoutTotal ?? "?"}${outcome.cartCleaned ? "" : " (cart not cleaned)"}`;
      console.log(`${String(results.length).padStart(4)}/${picked.length} ${String(ms).padStart(6)} ms  ${acct.email.padEnd(34)} ${tag}`);
      await acct.unlock().catch(() => undefined);
    }
  };
  await Promise.all(Array.from({ length: Math.min(CONC, picked.length) }, runner));
  const wall = Date.now() - wallStart;

  const by = (k: HttpCheckoutOutcome["kind"]) => results.filter((r) => r.outcome.kind === k);
  const okMs = results.filter((r) => r.outcome.kind === "dry_run" || r.outcome.kind === "placed").map((r) => r.ms);
  console.log("\n=== matrix ===");
  console.log(`accounts ${results.length} · concurrency ${CONC} · wall ${(wall / 1000).toFixed(1)} s · ${(results.length / (wall / 1000)).toFixed(2)} checkouts/s`);
  console.log(`dry_run ${by("dry_run").length} · placed ${by("placed").length} · failed ${by("failed").length} · fallback ${by("fallback").length}`);
  console.log(`per checkout (token reached): p50 ${pct(okMs, 50)} ms · p95 ${pct(okMs, 95)} ms · max ${pct(okMs, 100)} ms`);
  const steps = ["session", "cart_view", "cart_add", "checkout_init", "checkout_summary", "address_fix", "checks", "payment_token", "cart_cleanup", "pay_page", "pay_cod", "confirmation"];
  for (const s of steps) {
    const xs = results.map((r) => r.outcome.timings[s]).filter((v): v is number => typeof v === "number");
    if (xs.length) console.log(`  ${s.padEnd(18)} n=${String(xs.length).padStart(4)}  p50 ${String(pct(xs, 50)).padStart(6)} ms  p95 ${String(pct(xs, 95)).padStart(6)} ms`);
  }
  const reasons = new Map<string, number>();
  for (const r of results) {
    const key = r.outcome.kind === "failed" ? `failed ${r.outcome.failure.code}` : r.outcome.kind === "fallback" ? `fallback ${r.outcome.reason.replace(/\d{6}/g, "<pin>").slice(0, 80)}` : "";
    if (key) reasons.set(key, (reasons.get(key) || 0) + 1);
  }
  for (const [k, v] of [...reasons.entries()].sort((a, b) => b[1] - a[1])) console.log(`  ${String(v).padStart(4)}  ${k}`);
  const byProxy = new Map<string, { n: number; ok: number }>();
  for (const r of results) {
    const e = byProxy.get(r.proxy) || { n: 0, ok: 0 };
    e.n += 1;
    if (r.outcome.kind === "dry_run" || r.outcome.kind === "placed") e.ok += 1;
    byProxy.set(r.proxy, e);
  }
  for (const [p, e] of byProxy) console.log(`  proxy ${p.padEnd(22)} ${e.ok}/${e.n} reached token`);
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
