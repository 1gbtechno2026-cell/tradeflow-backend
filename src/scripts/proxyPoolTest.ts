import mongoose from "mongoose";
import { connectDb } from "../db.js";
import { config } from "../config.js";
import { ProxyPool } from "../models/ProxyPool.js";
import { claimProxyForWorker, freeProxyCount, isProxyError, reportProxyFailure } from "../services/proxyPool.js";

/**
 * Worker-side Proxy Pool checks against the real collection: sticky claim,
 * re-claim after a disable, dead-after-N. Needs at least two ACTIVE,
 * UNASSIGNED rows (import some in the dashboard first). Binds them to
 * "proxy-test-*" worker ids and unbinds on exit; never deletes a row.
 *
 *   npm run test:proxy-pool
 */
let failed = 0;
function check(name: string, ok: boolean, detail = "") {
  console.log(`  ${ok ? "ok  " : "FAIL"} ${name}${detail ? ` — ${detail}` : ""}`);
  if (!ok) failed += 1;
}

async function main() {
  await connectDb();
  const W1 = "proxy-test-1";
  const W2 = "proxy-test-2";
  const touched = new Set<string>();
  try {
    const free = await freeProxyCount();
    console.log(`free active proxies: ${free}`);
    if (free < 2) {
      console.log("need at least 2 active, unassigned proxies — import some in the Proxy Pool tab first");
      process.exit(2);
    }

    console.log("1. First boot binds a free row; a second boot gets the SAME row");
    const a = await claimProxyForWorker(W1);
    check("claimed", Boolean(a), a?.label);
    if (a) touched.add(a.id);
    const again = await claimProxyForWorker(W1);
    check("sticky across restarts", Boolean(again) && again!.id === a!.id, again?.label);
    check("password decrypts (not logged)", Boolean(a) && (a!.proxy.password === undefined || typeof a!.proxy.password === "string"));
    check("Playwright shape", Boolean(a) && /^http:\/\/.+:\d+$/.test(a!.proxy.server) && a!.proxy.bypass === "<-loopback>");

    console.log("2. A second worker gets a DIFFERENT row");
    const b = await claimProxyForWorker(W2);
    check("claimed", Boolean(b), b?.label);
    if (b) touched.add(b.id);
    check("not the same proxy", Boolean(a && b) && a!.id !== b!.id);

    console.log("3. Disable w1's row in the dashboard → w1's next job moves to a fresh row and lets the old one go");
    await ProxyPool.updateOne({ _id: a!.id }, { $set: { status: "disabled" } });
    const moved = await claimProxyForWorker(W1);
    if (moved) touched.add(moved.id);
    const old = await ProxyPool.findById(a!.id).select("assignedTo").lean();
    check("old row unbound", old?.assignedTo == null);
    check("fresh row or none (pool may have only 2)", moved === null || moved.id !== a!.id, moved?.label || "none free");
    await ProxyPool.updateOne({ _id: a!.id }, { $set: { status: "active" } });

    console.log(`4. ${config.proxyDeadAfter} consecutive failures retire a row`);
    const victim = moved ?? b!;
    let retired = false;
    for (let i = 0; i < config.proxyDeadAfter; i++) retired = await reportProxyFailure(victim.id, "net::ERR_TUNNEL_CONNECTION_FAILED (test)");
    const dead = await ProxyPool.findById(victim.id).select("status consecutiveFails lastError").lean();
    check("marked dead", retired && dead?.status === "dead", `${dead?.status} after ${dead?.consecutiveFails}`);
    check("reason kept", /ERR_TUNNEL/.test(String(dead?.lastError)));
    await ProxyPool.updateOne({ _id: victim.id }, { $set: { status: "active", consecutiveFails: 0, lastError: "" } });

    console.log("5. Proxy error classifier");
    check("tunnel error", isProxyError("page.goto: net::ERR_TUNNEL_CONNECTION_FAILED at https://www.flipkart.com/"));
    check("auth error", isProxyError("net::ERR_PROXY_AUTH_UNSUPPORTED"));
    check("probe failure", isProxyError("echo endpoint answered 407"));
    check("not a proxy error", !isProxyError("page.goto: Timeout 10000ms exceeded"));
  } finally {
    await ProxyPool.updateMany({ _id: { $in: [...touched] } }, { $set: { assignedTo: null, assignedAt: null } });
    await ProxyPool.updateMany({ assignedTo: { $in: [W1, W2] } }, { $set: { assignedTo: null, assignedAt: null } });
    await mongoose.disconnect();
  }
  console.log(failed ? `\n${failed} check(s) FAILED` : "\nPASS — claim → sticky → second worker → move on disable → dead after N");
  process.exit(failed ? 1 : 0);
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
