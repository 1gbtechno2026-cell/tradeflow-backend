import { Types } from "mongoose";
import { connectDb } from "../db.js";
import { Order } from "../models/Order.js";
import { PlatformId } from "../models/PlatformId.js";
import { resolveLoggedInSession, workspaceUserId } from "../services/sessionStore.js";
import { fetchOrderDetails, openOrderApiSession } from "../services/orderApi.js";

/**
 * Fill cancelled_by / cancel_reason_text on cancelled units stored before
 * 2026-10-30 (the mapper only started recording Flipkart's fkCancelled flag
 * then): every cancelled unit of the last --days on a logged-in account is
 * re-read over the order API and the two fields set. Nothing else changes.
 *
 *   npm run orders:backfill-cancelled-by -- --days 30 [--concurrency 6]
 */
const argv = process.argv.slice(2);
const arg = (k: string, d = "") => { const i = argv.indexOf(`--${k}`); return i >= 0 ? String(argv[i + 1] ?? "") : d; };
const DAYS = Math.max(1, Number(arg("days", "30")) || 30);
const CONC = Math.max(1, Number(arg("concurrency", "6")) || 6);

async function main() {
  await connectDb();
  const userId = new Types.ObjectId(workspaceUserId());
  const since = new Date(Date.now() - DAYS * 24 * 3600_000);
  const rows = await Order.find({ userId, status_key: /cancel/i, order_date: { $gte: since }, $or: [{ cancelled_by: { $in: ["", null] } }, { cancelled_by: { $exists: false } }] }).select("platform_email order_id unit_id").lean();
  const byEmail = new Map<string, typeof rows>();
  for (const r of rows) byEmail.set(String(r.platform_email), [...(byEmail.get(String(r.platform_email)) || []), r]);
  const loggedIn = new Set((await PlatformId.find({ userId, email: { $in: [...byEmail.keys()] }, status: "logged_in" }).select("email").lean()).map((p) => String(p.email)));
  const emails = [...byEmail.keys()].filter((e) => loggedIn.has(e));
  console.log(`cancelled units without cancelled_by in the last ${DAYS} d: ${rows.length} on ${byEmail.size} account(s), ${emails.length} logged in`);
  let done = 0, flipkart = 0, customer = 0, unknown = 0, failed = 0;
  let next = 0;
  const lane = async () => {
    while (next < emails.length) {
      const email = emails[next++];
      const s = await resolveLoggedInSession(email);
      if (!s.ok) { failed += byEmail.get(email)!.length; continue; }
      const ctx = await openOrderApiSession(s.cookies);
      try {
        for (const r of byEmail.get(email)!) {
          try {
            const d = await fetchOrderDetails(ctx, String(r.order_id), String(r.unit_id));
            const u = d.orderView.units?.[String(r.unit_id)];
            const st = u?.metaData?.status;
            const by = st?.fkCancelled === true ? "flipkart" : /as per your request|cancelled by you|you cancelled/i.test(String(st?.text || "")) ? "customer" : "";
            await Order.updateOne({ _id: r._id }, { $set: { cancelled_by: by, cancel_reason_text: st?.text || null } });
            done += 1;
            if (by === "flipkart") flipkart += 1; else if (by === "customer") customer += 1; else unknown += 1;
          } catch (err) {
            failed += 1;
            console.log(`  ${email} ${r.order_id}/${r.unit_id}: ${err instanceof Error ? err.message.split("\n")[0].slice(0, 100) : String(err)}`);
          }
          await new Promise((res) => setTimeout(res, 250));
        }
      } finally {
        await ctx.dispose().catch(() => undefined);
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(CONC, emails.length) }, lane));
  console.log(`done ${done}: by Flipkart ${flipkart} · by customer ${customer} · unknown ${unknown} · failed ${failed}`);
  process.exit(0);
}
main().catch((err) => { console.error(err); process.exit(1); });
