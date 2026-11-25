import mongoose, { Types, type Model } from "mongoose";
import { config } from "../config.js";
import { CancelRequest } from "../models/CancelRequest.js";
import { Invoice } from "../models/Invoice.js";
import { Order } from "../models/Order.js";
import { OrderUnitEvent } from "../models/OrderUnitEvent.js";
import { OrderUpdateRequest } from "../models/OrderUpdateRequest.js";

/**
 * Puts back unit fields that fetch and update saves blanked before the save fix (data-quality plan sub-step 1,
 * operations OPS-1 and OPS-2). Mongo only: no Flipkart, no queue.
 *
 *   npm run orders:unit-backfill -- --part invoice-flag          dry run: counts and 10 sample unit ids
 *   npm run orders:unit-backfill -- --part otp
 *   npm run orders:unit-backfill -- --part invoice-flag,otp --go  writes
 *   --since 2026-11-25T14:00+05:30   count the gate's old-code requests from then (when the other Mac was upgraded or
 *                                    its worker stopped), not from the first request another machine stamped
 *
 * invoice-flag  is_invoice_downloaded false → true where the unit's invoice_s3_key is set.
 *               The key is the rule because it is the stored bundle's own address (raw/<user>/<order>.pdf), and every
 *               writer of the flag sets the key in the same $set: the capture (invoiceCapture.ts, invoiceOnDemand.ts)
 *               only after the S3 put succeeded, the pipeline's writeUnits together with invoice_id. So this restores
 *               exactly what those writers wrote and a save later reset. The two other rules are printed as checks,
 *               not used: invoice_id alone would miss a bundle stored but not yet extracted, and "the order has a
 *               processed bundle" would also flag units no writer ever linked to it (a sibling saved later).
 * otp           delivery_otp ← the newest otp_new event's code (order_unit_events after.otp, 4–6 digits) where the
 *               unit's delivery_otp is null or ''.
 *
 * Every write is one updateOne filtered on the value it replaces (the _id plus the old flag and key, or the old blank
 * OTP), so a save that lands in between wins and is not overwritten. Only rows of the workspace (TRADE_FLOW_USER_ID).
 *
 * Run --go only once the save fix (orderFetch.ts unitSetFor / UNIT_INSERT_DEFAULTS) runs in this Mac's API and worker,
 * AND the other Mac runs this build too, or its Trade Flow worker is stopped (and no fetch is started there). Pausing
 * its updates is not enough: its old update and cancel reapers have no systemId filter, so they re-run THIS Mac's quiet
 * requests from its own Redis through the old save, which resets these fields again. The dry run prints this as a gate
 * (PASS / FAIL); the proof stays the re-count after the other Mac's next routine update.
 */

const PARTS = ["invoice-flag", "otp"] as const;
type Part = (typeof PARTS)[number];
const SAMPLE = 10;
const BATCH = 500;
const OTP_RE = /^\d{4,6}$/;

interface Args {
  parts: Part[];
  go: boolean;
  since: Date | null;
}

function parseArgs(argv: string[]): Args {
  const parts: Part[] = [];
  let go = false;
  let since: Date | null = null;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--go") go = true;
    else if (a === "--since" || a.startsWith("--since=")) {
      const value = a === "--since" ? String(argv[++i] ?? "") : a.slice("--since=".length);
      since = new Date(value);
      if (!value || Number.isNaN(since.getTime())) throw new Error(`--since needs a time, e.g. 2026-11-25T14:00+05:30 (got "${value}")`);
    } else if (a === "--part" || a.startsWith("--part=")) {
      const value = a === "--part" ? String(argv[++i] ?? "") : a.slice("--part=".length);
      for (const p of value.split(",").map((s) => s.trim()).filter(Boolean)) {
        if (!(PARTS as readonly string[]).includes(p)) throw new Error(`unknown part "${p}" (parts: ${PARTS.join(", ")})`);
        if (!parts.includes(p as Part)) parts.push(p as Part);
      }
    } else throw new Error(`unknown argument "${a}"`);
  }
  if (!parts.length) throw new Error(`--part is required (${PARTS.join(", ")})`);
  return { parts, go, since };
}

const ist = (d: Date | null | undefined) => (d ? `${new Date(d).toLocaleString("en-IN", { timeZone: "Asia/Kolkata", hour12: false })} IST` : "—");
const tally = (values: string[]) =>
  Object.entries(values.reduce<Record<string, number>>((acc, v) => ((acc[v || "(blank)"] = (acc[v || "(blank)"] || 0) + 1), acc), {}))
    .sort((a, b) => b[1] - a[1])
    .map(([k, n]) => `${k} ${n}`)
    .join(", ") || "—";

/** updateOne ops in batches; each op's filter carries the old value, so matched < ops means a save got there first. */
async function writeFiltered(ops: Array<{ filter: Record<string, unknown>; set: Record<string, unknown> }>) {
  let matched = 0;
  let modified = 0;
  for (let i = 0; i < ops.length; i += BATCH) {
    const res = await Order.bulkWrite(
      ops.slice(i, i + BATCH).map((o) => ({ updateOne: { filter: o.filter, update: { $set: o.set } } })),
      { ordered: false }
    );
    matched += res.matchedCount;
    modified += res.modifiedCount;
  }
  return { matched, modified };
}

async function invoiceFlag(userId: Types.ObjectId, go: boolean) {
  const query = { userId, is_invoice_downloaded: { $ne: true }, invoice_s3_key: { $nin: [null, ""] } };
  console.log(`\ninvoice-flag — the unit's bundle is stored (invoice_s3_key set) but it reads "not downloaded"`);
  console.log(`  query: order_details { userId: <workspace>, is_invoice_downloaded: { $ne: true }, invoice_s3_key: { $nin: [null, ""] } }`);
  // The raw collection: invoice_id is the dashboard pipeline's field and not declared in this model.
  const rows = await Order.collection
    .find(query, { projection: { order_id: 1, unit_id: 1, status_key: 1, is_invoice_downloaded: 1, invoice_s3_key: 1, invoice_id: 1 } })
    .toArray();
  const orders = [...new Set(rows.map((r) => String(r.order_id)))];
  console.log(`  candidates: ${rows.length} unit(s) on ${orders.length} order(s) · by status: ${tally(rows.map((r) => String(r.status_key || "")))}`);

  const invoices = await Invoice.find({ userId, order_id: { $in: orders } }).select("order_id s3_key processing_status").lean();
  const byOrder = new Map(invoices.map((i) => [String(i.order_id), i]));
  const sameKey: string[] = [];
  let otherKey = 0;
  let noRow = 0;
  for (const r of rows) {
    const inv = byOrder.get(String(r.order_id));
    if (!inv) noRow++;
    else if (String(inv.s3_key) === String(r.invoice_s3_key)) sameKey.push(String(inv.processing_status));
    else otherKey++;
  }
  console.log(
    `  checks: invoice_id set on ${rows.filter((r) => r.invoice_id != null).length} · the order's invoices row holds the same key on ${sameKey.length} (${tally(sameKey)})` +
      ` · another key ${otherKey} · no invoices row ${noRow}`
  );
  const processedOrders = await Invoice.distinct("order_id", { userId, processing_status: "processed" });
  const orderOnly = await Order.countDocuments({ userId, is_invoice_downloaded: { $ne: true }, invoice_s3_key: { $in: [null, ""] }, order_id: { $in: processedOrders } });
  console.log(`  not taken: units without a key whose order has a processed bundle: ${orderOnly}`);
  console.log(`  sample: ${rows.slice(0, SAMPLE).map((r) => `${r.order_id}/${r.unit_id}`).join(" ") || "—"}`);

  if (!go) {
    console.log(`  dry run — nothing written. --go sets is_invoice_downloaded true on these ${rows.length} unit(s).`);
    return;
  }
  const res = await writeFiltered(
    rows.map((r) => ({
      filter: { _id: r._id, is_invoice_downloaded: r.is_invoice_downloaded ?? null, invoice_s3_key: r.invoice_s3_key },
      set: { is_invoice_downloaded: true },
    }))
  );
  const left = await Order.countDocuments(query);
  console.log(`  written: ${res.modified} of ${rows.length} (${rows.length - res.matched} changed meanwhile, left as they were) · re-count: ${left}`);
}

/** The units holding no OTP whose newest otp_new event has a usable code, with the write for each. */
async function otpCandidates(userId: Types.ObjectId) {
  const events = await OrderUnitEvent.aggregate<{ _id: string; otp: unknown; n: number; codes: unknown[] }>([
    { $match: { userId, type: "otp_new" } },
    { $sort: { at: -1, createdAt: -1, _id: -1 } },
    { $group: { _id: "$unitId", otp: { $first: "$after.otp" }, n: { $sum: 1 }, codes: { $addToSet: "$after.otp" } } },
  ]);
  const byUnit = new Map(events.map((e) => [String(e._id), e]));
  const blank = await Order.find({ userId, unit_id: { $in: [...byUnit.keys()] }, delivery_otp: { $in: [null, ""] } })
    .select("_id order_id unit_id status_key delivery_otp")
    .lean();
  const taken: typeof blank = [];
  const writes: Array<{ filter: Record<string, unknown>; set: Record<string, unknown> }> = [];
  let unusable = 0;
  let twoCodes = 0;
  for (const u of blank) {
    const e = byUnit.get(String(u.unit_id));
    const code = String(e?.otp ?? "").trim();
    if (!e || !OTP_RE.test(code)) {
      unusable++;
      continue;
    }
    if (new Set(e.codes.map((c) => String(c ?? "").trim())).size > 1) twoCodes++;
    taken.push(u);
    writes.push({ filter: { _id: u._id, delivery_otp: u.delivery_otp ?? null }, set: { delivery_otp: code } });
  }
  return { events: events.reduce((a, e) => a + e.n, 0), eventUnits: events.length, blank: blank.length, taken, writes, unusable, twoCodes };
}

async function otp(userId: Types.ObjectId, go: boolean) {
  console.log(`\notp — a delivery OTP was captured (an otp_new event) but the unit holds none now`);
  console.log(`  query: order_unit_events { userId: <workspace>, type: "otp_new" }, newest per unitId by (at, createdAt) → order_details { userId, unit_id, delivery_otp: { $in: [null, ""] } }, code /^\\d{4,6}$/`);
  const c = await otpCandidates(userId);
  console.log(`  events: ${c.events} on ${c.eventUnits} unit(s) · of those units holding no OTP now: ${c.blank}`);
  console.log(`  candidates: ${c.taken.length} · by status: ${tally(c.taken.map((u) => String(u.status_key || "")))} · events with two codes (newest taken): ${c.twoCodes} · skipped, newest code not 4–6 digits: ${c.unusable}`);
  console.log(`  sample: ${c.taken.slice(0, SAMPLE).map((u) => `${u.order_id}/${u.unit_id}`).join(" ") || "—"}`);
  if (!go) {
    console.log(`  dry run — nothing written. --go sets delivery_otp on these ${c.taken.length} unit(s).`);
    return;
  }
  const res = await writeFiltered(c.writes);
  const after = await otpCandidates(userId);
  console.log(`  written: ${res.modified} of ${c.writes.length} (${c.writes.length - res.matched} changed meanwhile, left as they were) · re-count: ${after.taken.length}`);
}

/**
 * The gate for --go, printed as PASS or FAIL: no machine on old code saves units any more. Code since step 5 stamps
 * every update and cancel request with its systemId, so one without a systemId comes from old code, whose saves reset
 * these fields (and whose reapers re-run this machine's quiet requests through them). Counted from --since, else from
 * the first request another machine stamped (it runs step-5 code from then on), else from this machine's first stamped
 * one. A request without a systemId that is still open counts too, whenever it was made: it can still run on old code.
 */
async function oldCodeGate(userId: Types.ObjectId, since: Date | null) {
  const kinds: Array<[string, Model<any>]> = [
    ["update", OrderUpdateRequest],
    ["cancel", CancelRequest],
  ];
  const firstStamped = async (systemId: unknown) => {
    const rows = await Promise.all(kinds.map(([, M]) => M.findOne({ userId, systemId }).sort({ createdAt: 1 }).select("createdAt").lean<{ createdAt: Date } | null>()));
    return rows.flatMap((r) => (r?.createdAt ? [new Date(r.createdAt)] : [])).sort((a, b) => a.getTime() - b.getTime())[0] ?? null;
  };
  const mine = await firstStamped(config.systemId);
  const other = await firstStamped({ $nin: ["", null, config.systemId] });
  const from = since ?? other ?? mine;
  console.log(`\ngate — no machine on old code saves units (needed before --go)`);
  if (!from) {
    console.log(`  UNKNOWN — no update or cancel request is stamped yet, by "${config.systemId}" or another machine: pass --since <time>.`);
    return;
  }
  const why = since ? "--since" : other ? "the first request another machine stamped" : `"${config.systemId}"'s first stamped request; no other machine has stamped one`;
  const unstamped = { userId, systemId: { $in: ["", null] } };
  const counts: string[] = [];
  let made = 0;
  let open = 0;
  let newest: Date | null = null;
  for (const [kind, M] of kinds) {
    const q = { ...unstamped, createdAt: { $gt: from } };
    const n = await M.countDocuments(q);
    const last = n ? await M.findOne(q).sort({ createdAt: -1 }).select("createdAt").lean<{ createdAt: Date } | null>() : null;
    if (last?.createdAt && (!newest || new Date(last.createdAt) > newest)) newest = new Date(last.createdAt);
    open += await M.countDocuments({ ...unstamped, status: { $in: ["queued", "running"] } });
    made += n;
    counts.push(`${kind} ${n}`);
  }
  console.log(`  requests without a systemId (old code) since ${ist(from)} (${why}): ${counts.join(", ")}${newest ? `, newest ${ist(newest)}` : ""} · open now, made any time: ${open}`);
  if (made || open) {
    console.log(
      `  FAIL — a machine still runs old code. Run --go only once it runs this build, or its Trade Flow worker is stopped and no fetch is started there (pausing its updates is not enough: its old reapers re-run this machine's quiet requests through the old save); then pass --since <the time it was upgraded or stopped>.`
    );
  } else {
    console.log(
      `  PASS — none since ${ist(from)}. A stamped request shows step-5 code, not this save rule by itself: after --go, re-run this dry run once the other machine's next routine update has run; the candidates must stay 0.`
    );
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!config.userId || !Types.ObjectId.isValid(config.userId)) throw new Error("TRADE_FLOW_USER_ID is required (og_user workspace id)");
  const userId = new Types.ObjectId(config.userId);
  // Never the app's connectDb(): no index work, no collection creation.
  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false, serverSelectionTimeoutMS: 15_000 });
  try {
    console.log(`unit backfill · ${args.go ? "WRITE (--go)" : "dry run"} · parts: ${args.parts.join(", ")} · ${ist(new Date())}`);
    await oldCodeGate(userId, args.since);
    for (const part of args.parts) {
      if (part === "invoice-flag") await invoiceFlag(userId, args.go);
      else await otp(userId, args.go);
    }
  } finally {
    await mongoose.disconnect();
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exitCode = 1;
});
