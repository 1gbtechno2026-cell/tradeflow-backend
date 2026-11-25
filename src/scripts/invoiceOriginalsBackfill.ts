/**
 * invoices:originals-backfill — data-quality sub-step 12, SO-4 (runbook OPS-14 / OPS-15).
 *
 * Downloads every stored bundle again so Flipkart's own files are kept (orig/, invoice_downloads, the raw PDF's
 * download-id — services/invoiceOriginals.ts), through the same call as Start a request → Invoices
 * (createInvoiceRequest): a task per order, 12 accounts in parallel, Stop / Retry in Batch requests, the worker's
 * liveness and reapers. A bundle counts as done once its `invoices` row has a `stored_download_id`.
 *
 *   npx tsx src/scripts/invoiceOriginalsBackfill.ts                 DRY RUN (default): counts only, writes nothing
 *   npx tsx src/scripts/invoiceOriginalsBackfill.ts --go --limit 200   OPS-14: the 1-order canary, then a 200-order pilot
 *   npx tsx src/scripts/invoiceOriginalsBackfill.ts --go               OPS-15: the canary, then everything left
 *   npx tsx src/scripts/invoiceOriginalsBackfill.ts --compare <orderId>   OPS-14's "+1 repeat" gate (read-only): the
 *                            order's two newest invoice_downloads documents hold the same sha256 set — run it after
 *                            re-downloading one pilot order (Download Order Invoices, Force) once the pilot is done
 *     --user <workspace id>    default TRADE_FLOW_USER_ID
 *     --canary-wait <seconds>  default 180
 *
 * --go:
 *   - refuses while an open invoice request with params.purpose "originals_backfill" exists — from either Mac
 *     (the requests live in the shared Mongo) — and when no invoice-batch worker is alive on THIS Mac (the jobs go to
 *     this Mac's Redis; its worker must run sub-step 12);
 *   - sends a 1-order canary first and stops unless an invoice_downloads document for it appears and the bundle's
 *     row names it (stored_download_id) within 3 minutes — and stops at once when that document says the originals
 *     could not be stored (originals_error: e.g. S3 AccessDenied on orig/ before D1's grant); warns, without stopping, when the archive the Lambda then
 *     builds is not made from the originals (zip_source "original" — sub-step 5 not deployed yet);
 *   - then requests of at most 2,000 orders (REQUEST_VALUE_MAX), cut at account boundaries so no account is in two
 *     requests (an account holding more than 2,000 orders alone gets requests of its own);
 *   - each request: force false, source "user", by "originals backfill", params.purpose "originals_backfill".
 * Resumable: every run selects only the bundles that still have no stored download (and the batch leaves orders
 * another open request holds to that request). Stop = Batch requests → Stop on its requests.
 */
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import mongoose, { Types } from "mongoose";

export const BACKFILL_PURPOSE = "originals_backfill";
export const BACKFILL_BY = "originals backfill";
export const REQUEST_MAX = 2000;
const OPEN = ["queued", "in_progress"];
/** Measured: the 100-order test took 34 s; OPS-15's plan is 35–70 min for ~4,800 orders at 12 accounts in parallel. */
const RATE_PER_MIN = { fast: 140, slow: 70 };

export interface BackfillItem {
  orderId: string;
  accountId: string;
}

/**
 * Cut the orders into requests of at most `max`, at account boundaries: every order of one account lands in the
 * same request (an account with more than `max` orders is split into requests of its own, and said so).
 */
export function chunkOrdersByAccount(items: readonly BackfillItem[], max = REQUEST_MAX): { chunks: string[][]; splitAccounts: string[] } {
  const byAccount = new Map<string, string[]>();
  for (const it of items) {
    const list = byAccount.get(it.accountId) || [];
    if (!list.includes(it.orderId)) list.push(it.orderId);
    byAccount.set(it.accountId, list);
  }
  const chunks: string[][] = [];
  const splitAccounts: string[] = [];
  let current: string[] = [];
  for (const [account, orders] of byAccount) {
    if (orders.length > max) {
      splitAccounts.push(account);
      if (current.length) chunks.push(current);
      current = [];
      for (let at = 0; at < orders.length; at += max) chunks.push(orders.slice(at, at + max));
      continue;
    }
    if (current.length + orders.length > max) {
      chunks.push(current);
      current = [];
    }
    current.push(...orders);
  }
  if (current.length) chunks.push(current);
  return { chunks, splitAccounts };
}

export interface CanaryState {
  /** The canary request's one task: queued / in_progress / success / failed / timedout / aborted. */
  taskStatus: string;
  taskError?: string;
  /** The newest invoice_downloads document of the order made since the canary was sent. */
  download: { _id: string; originals_error?: string } | null;
  /** The bundle's invoices row now. */
  row: { stored_download_id?: string; processing_status?: string; originals_download_id?: string; zip_source?: string } | null;
  elapsedMs: number;
  waitMs: number;
}

/**
 * The 1-order canary rule: "ok" once an invoice_downloads document exists for the order and the bundle's row names
 * it (the worker runs sub-step 12's store helper); "fail" when the task failed, or nothing of the kind appeared
 * within the wait (the worker does not run sub-step 12, or is not taking jobs); else "wait". `warn` when the
 * Lambda has processed that very download but its archive is not made from the originals (sub-step 5 not deployed).
 */
export function canaryVerdict(s: CanaryState): { state: "ok" | "fail" | "wait"; reason: string; warn?: string } {
  if (s.download?.originals_error) {
    return { state: "fail", reason: `the canary downloaded, but its originals were not stored (${s.download.originals_error}) — the worker's AWS principal needs the orig/* grant that D1 deploys` };
  }
  const named = Boolean(s.download && s.row && s.row.stored_download_id === s.download._id);
  if (named) {
    const processed = s.row?.processing_status === "processed" && s.row?.originals_download_id === s.download!._id;
    const warn = processed && s.row?.zip_source !== "original" ? `the archive was built with zip_source "${s.row?.zip_source || ""}", not "original" — sub-step 5 is not deployed; run the Lambda-only re-extract (OPS-16 --requeue) after D1` : undefined;
    return { state: "ok", reason: `invoice_downloads ${s.download!._id} stored and named by the bundle's row`, ...(warn ? { warn } : {}) };
  }
  if (["failed", "timedout", "aborted"].includes(s.taskStatus)) return { state: "fail", reason: `the canary's task ended ${s.taskStatus}${s.taskError ? `: ${s.taskError}` : ""}` };
  if (s.taskStatus === "success") {
    if (!s.download) return { state: "fail", reason: "the canary downloaded, but wrote no invoice_downloads document — the worker on this Mac does not run sub-step 12 (restart it: R3)" };
    return s.elapsedMs >= s.waitMs ? { state: "fail", reason: "an invoice_downloads document was written, but the bundle's row does not name it" } : { state: "wait", reason: "the row is not updated yet" };
  }
  if (s.elapsedMs >= s.waitMs) return { state: "fail", reason: `no invoice_downloads document within ${Math.round(s.waitMs / 1000)} s (task ${s.taskStatus || "not taken"}) — is this Mac's worker running sub-step 12?` };
  return { state: "wait", reason: `task ${s.taskStatus || "queued"}` };
}

const oid = (id: string) => new Types.ObjectId(id);
const fmt = (n: number) => n.toLocaleString("en-IN");

/** Open originals-backfill requests of the workspace, from either Mac. */
export async function openBackfillRequests(userId: string): Promise<Array<{ _id: string; host: string; systemId: string; createdAt: Date | null }>> {
  const { OrderInvoiceRequest } = await import("../models/OrderInvoiceRequest.js");
  const rows = await OrderInvoiceRequest.collection
    .find({ userId: oid(userId), status: { $in: OPEN }, "params.purpose": BACKFILL_PURPOSE }, { projection: { host: 1, systemId: 1, createdAt: 1 } })
    .sort({ createdAt: 1, _id: 1 })
    .toArray();
  return rows.map((r) => ({ _id: String(r._id), host: String(r.host || ""), systemId: String(r.systemId || ""), createdAt: (r.createdAt as Date) || null }));
}

/** OPS-14's repeat gate: two downloads of one order hold the same set of files (by sha256). */
export function compareDownloadShas(a: ReadonlyArray<{ sha256?: string }>, b: ReadonlyArray<{ sha256?: string }>): { same: boolean; onlyFirst: number; onlySecond: number } {
  const set = (l: ReadonlyArray<{ sha256?: string }>) => new Set(l.map((f) => String(f.sha256 || "").toLowerCase()).filter(Boolean));
  const x = set(a);
  const y = set(b);
  const onlyFirst = [...x].filter((h) => !y.has(h)).length;
  const onlySecond = [...y].filter((h) => !x.has(h)).length;
  return { same: x.size > 0 && onlyFirst === 0 && onlySecond === 0, onlyFirst, onlySecond };
}

export interface BackfillSelection {
  bundles: number;
  stored: number;
  /** Bundles with no stored download whose order has units (what a request can take), in a stable order. */
  pending: BackfillItem[];
  /** Bundles with no stored download whose order has no unit in Order Units (a request would fail them). */
  withoutUnits: number;
  /** Of `pending`, held by an open invoice request (the batch leaves them to it). */
  heldOpen: number;
  /** Account state (PlatformId.status) → orders, accounts. */
  byState: Map<string, { orders: number; accounts: number }>;
  loggedIn: Set<string>;
}

/** What is left to re-download (read-only). */
export async function backfillSelection(userId: string): Promise<BackfillSelection> {
  const { Invoice } = await import("../models/Invoice.js");
  const { Order } = await import("../models/Order.js");
  const { OrderInvoiceRequest } = await import("../models/OrderInvoiceRequest.js");
  const user = oid(userId);
  const bundles = await Invoice.collection.countDocuments({ userId: user });
  const stored = await Invoice.collection.countDocuments({ userId: user, stored_download_id: { $nin: [null, ""], $exists: true } });
  const left = (
    await Invoice.collection
      .find({ userId: user, $or: [{ stored_download_id: { $in: [null, ""] } }, { stored_download_id: { $exists: false } }] }, { projection: { order_id: 1 } })
      .sort({ createdAt: 1, _id: 1 })
      .toArray()
  ).map((r) => String(r.order_id));
  const accountOf = new Map<string, string>();
  for (let at = 0; at < left.length; at += 2000) {
    const rows = await Order.collection
      .aggregate<{ _id: string; account: unknown }>([{ $match: { userId: user, order_id: { $in: left.slice(at, at + 2000) } } }, { $group: { _id: "$order_id", account: { $first: "$platformAccountId" } } }])
      .toArray();
    for (const r of rows) accountOf.set(String(r._id), String(r.account || ""));
  }
  const pending = left.filter((o) => accountOf.has(o)).map((orderId) => ({ orderId, accountId: accountOf.get(orderId)! }));
  const held = new Set(
    (
      await OrderInvoiceRequest.aggregate<{ _id: string }>([
        { $match: { userId: user, status: { $in: OPEN } } },
        { $unwind: "$tasks" },
        { $match: { "tasks.status": { $in: OPEN } } },
        { $group: { _id: "$tasks.orderId" } },
      ])
    ).map((h) => h._id)
  );
  const accounts = [...new Set(pending.map((p) => p.accountId))].filter((a) => Types.ObjectId.isValid(a));
  const stateOf = new Map<string, string>();
  for (let at = 0; at < accounts.length; at += 2000) {
    // Only the status: never a session field.
    const rows = await mongoose.connection.db!.collection("platformids").find({ _id: { $in: accounts.slice(at, at + 2000).map(oid) } }, { projection: { status: 1 } }).toArray();
    for (const r of rows) stateOf.set(String(r._id), String(r.status || "unknown"));
  }
  const byState = new Map<string, { orders: number; accounts: number }>();
  const seen = new Set<string>();
  for (const p of pending) {
    const state = stateOf.get(p.accountId) || "no account row";
    const s = byState.get(state) || { orders: 0, accounts: 0 };
    s.orders++;
    if (!seen.has(p.accountId)) {
      seen.add(p.accountId);
      s.accounts++;
    }
    byState.set(state, s);
  }
  const loggedIn = new Set(accounts.filter((a) => stateOf.get(a) === "logged_in"));
  return { bundles, stored, pending, withoutUnits: left.length - pending.length, heldOpen: pending.filter((p) => held.has(p.orderId)).length, byState, loggedIn };
}

function printSelection(sel: BackfillSelection, limit: number | null) {
  const take = limit ? sel.pending.slice(0, limit) : sel.pending;
  const { chunks, splitAccounts } = chunkOrdersByAccount(take);
  const accounts = new Set(sel.pending.map((p) => p.accountId)).size;
  console.log(`  bundles in the workspace:                 ${fmt(sel.bundles)}`);
  console.log(`  coverage (row has a stored download):     ${fmt(sel.stored)} of ${fmt(sel.bundles)} (${sel.bundles ? ((100 * sel.stored) / sel.bundles).toFixed(1) : "0.0"} %)`);
  console.log(`  to re-download:                           ${fmt(sel.pending.length)} orders on ${fmt(accounts)} accounts`);
  if (sel.withoutUnits) console.log(`  left out — no unit in Order Units:        ${fmt(sel.withoutUnits)}`);
  if (sel.heldOpen) console.log(`  held by another open invoice request:     ${fmt(sel.heldOpen)} (left to it)`);
  console.log(`  by account state:`);
  for (const [state, s] of [...sel.byState.entries()].sort((a, b) => b[1].orders - a[1].orders)) console.log(`    ${state.padEnd(20)} ${fmt(s.orders).padStart(6)} orders on ${fmt(s.accounts)} accounts${state === "logged_in" ? "" : " (their tasks fail as not logged in — re-run after re-login)"}`);
  console.log(`  ${limit ? `pilot (--limit ${limit})` : "requests"}: ${chunks.length} of at most ${fmt(REQUEST_MAX)} orders, cut at account boundaries — ${chunks.map((c) => fmt(c.length)).join(" + ") || "none"}${splitAccounts.length ? ` (${splitAccounts.length} account(s) over ${fmt(REQUEST_MAX)} orders split)` : ""}`);
  const n = take.length;
  console.log(`  estimate: ${n ? `~${Math.max(1, Math.round(n / RATE_PER_MIN.fast))}–${Math.max(1, Math.round(n / RATE_PER_MIN.slow))} min` : "nothing to do"} for ${fmt(n)} orders (${RATE_PER_MIN.slow}–${RATE_PER_MIN.fast} a minute, 12 accounts in parallel), plus 1 canary order first`);
}

function args(argv: string[]) {
  const get = (name: string) => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : undefined;
  };
  const limit = get("--limit");
  return {
    go: argv.includes("--go"),
    limit: limit ? Math.max(1, Number(limit) || 0) : null,
    user: get("--user"),
    compare: get("--compare"),
    canaryWaitMs: Math.max(30, Number(get("--canary-wait") || 180)) * 1000,
  };
}

async function main() {
  const opt = args(process.argv.slice(2));
  const { config } = await import("../config.js");
  const userId = String(opt.user || config.userId || "").trim();
  if (!Types.ObjectId.isValid(userId)) throw new Error("no workspace: set TRADE_FLOW_USER_ID or pass --user <id>");
  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false, serverSelectionTimeoutMS: 15_000 });
  try {
    if (opt.compare) {
      // Read-only: the order's two newest downloads (OPS-14 "+1 repeat").
      const { InvoiceDownload } = await import("../models/InvoiceDownload.js");
      const docs = await InvoiceDownload.collection.find({ userId: oid(userId), order_id: opt.compare }, { projection: { at: 1, path: 1, "files.sha256": 1, originals_error: 1 } }).sort({ at: -1 }).limit(2).toArray();
      if (docs.length < 2) throw new Error(`${opt.compare}: ${docs.length} invoice_downloads document(s) — download it once more (Download Order Invoices, Force), then compare`);
      const [newer, older] = docs as unknown as Array<{ _id: string; at: Date; path: string; files: Array<{ sha256?: string }>; originals_error?: string }>;
      const c = compareDownloadShas(older.files || [], newer.files || []);
      console.log(`${opt.compare}: ${older.path} ${older.at.toISOString()} (${(older.files || []).length} files) vs ${newer.path} ${newer.at.toISOString()} (${(newer.files || []).length} files)`);
      if (older.originals_error || newer.originals_error) console.log(`  note: originals not all stored — ${newer.originals_error || older.originals_error}`);
      console.log(c.same ? "  same sha256 set — the repeat gate passes" : `  DIFFERENT: ${c.onlyFirst} file(s) only in the older, ${c.onlySecond} only in the newer (a late document, or Flipkart re-signed)`);
      if (!c.same) process.exitCode = 1;
      return;
    }
    const open = await openBackfillRequests(userId);
    const sel = await backfillSelection(userId);
    console.log(`Originals backfill — ${opt.go ? "GO" : "dry run (nothing is written; --go to send)"}`);
    printSelection(sel, opt.limit);
    console.log(`  open originals-backfill requests (either Mac): ${open.length}${open.map((o) => `\n    ${o._id} on ${o.host || o.systemId || "?"} since ${o.createdAt?.toISOString() || "?"}`).join("")}`);
    if (!opt.go) return;

    // ── --go ──
    if (open.length) throw new Error(`refused: an originals backfill is already open (${open.map((o) => `${o._id} on ${o.host || o.systemId}`).join(", ")}) — let it finish or Stop it in Batch requests first`);
    if (!sel.pending.length) {
      console.log("nothing left to re-download");
      return;
    }
    const { workerAlive } = await import("../services/requestCommon.js");
    const { INVOICE_BATCH_QUEUE } = await import("../queue.js");
    if (!(await workerAlive(INVOICE_BATCH_QUEUE))) throw new Error(`refused: no invoice-batch worker is alive on this Mac (${config.hostId}) — the requests' jobs go to this Mac's Redis; start the worker (with sub-step 12) first`);
    const { createInvoiceRequest, stopInvoiceRequest } = await import("../services/invoiceRequests.js");
    const { OrderInvoiceRequest } = await import("../models/OrderInvoiceRequest.js");
    const { Invoice } = await import("../models/Invoice.js");
    const { InvoiceDownload } = await import("../models/InvoiceDownload.js");
    const send = (orderIds: string[], params: Record<string, unknown>) =>
      createInvoiceRequest(userId, { orderIds, force: false, by: BACKFILL_BY, source: "user", params: { purpose: BACKFILL_PURPOSE, ...params } });

    // 1. The canary: one order of a logged-in account.
    const canary = sel.pending.find((p) => sel.loggedIn.has(p.accountId)) || sel.pending[0];
    const sentAt = new Date();
    const canaryId = (await send([canary.orderId], { canary: true })).request_id;
    // Two backfills started together from the two Macs: the older request wins, the younger stops itself.
    const racing = await openBackfillRequests(userId);
    if (racing.length > 1 && racing[0]._id !== canaryId) {
      if (canaryId) await stopInvoiceRequest(userId, canaryId, BACKFILL_BY).catch(() => undefined);
      throw new Error(`refused: another originals backfill started at the same time (${racing[0]._id} on ${racing[0].host}) — this one stopped its canary`);
    }
    console.log(`canary: ${canary.orderId} — request ${canaryId}; waiting up to ${Math.round(opt.canaryWaitMs / 1000)} s for its invoice_downloads document`);
    let verdict: ReturnType<typeof canaryVerdict> = { state: "wait", reason: "" };
    for (;;) {
      const req = canaryId ? await OrderInvoiceRequest.collection.findOne({ _id: oid(canaryId) }, { projection: { tasks: 1 } }) : null;
      const task = ((req?.tasks as Array<{ orderId: string; status: string; error?: string }>) || []).find((t) => t.orderId === canary.orderId);
      const download = await InvoiceDownload.collection.findOne({ userId: oid(userId), order_id: canary.orderId, at: { $gte: sentAt } }, { sort: { at: -1 }, projection: { _id: 1, originals_error: 1 } });
      const row = await Invoice.collection.findOne({ userId: oid(userId), order_id: canary.orderId }, { projection: { stored_download_id: 1, processing_status: 1, originals_download_id: 1, zip_source: 1 } });
      verdict = canaryVerdict({
        taskStatus: task?.status || "",
        taskError: task?.error,
        download: download ? { _id: String(download._id), ...(download.originals_error ? { originals_error: String(download.originals_error) } : {}) } : null,
        row: row as CanaryState["row"],
        elapsedMs: Date.now() - sentAt.getTime(),
        waitMs: opt.canaryWaitMs,
      });
      // Once stored, give the Lambda the rest of the wait to build the archive, to say whether it used the originals.
      const processed = row?.processing_status === "processed" && download && row?.originals_download_id === String(download._id);
      if (verdict.state === "fail" || (verdict.state === "ok" && (processed || Date.now() - sentAt.getTime() >= opt.canaryWaitMs))) break;
      await new Promise((r) => setTimeout(r, 5000));
    }
    if (verdict.state !== "ok") throw new Error(`canary failed: ${verdict.reason} — nothing more was sent`);
    console.log(`canary ok: ${verdict.reason}`);
    if (verdict.warn) console.log(`WARNING: ${verdict.warn}`);

    // 2. The rest (or the pilot), re-selected: resumable.
    const again = await backfillSelection(userId);
    const take = opt.limit ? again.pending.slice(0, opt.limit) : again.pending;
    const { chunks } = chunkOrdersByAccount(take);
    const ids: string[] = [];
    for (let i = 0; i < chunks.length; i++) {
      const id = (await send(chunks[i], { part: `${i + 1}/${chunks.length}`, ...(opt.limit ? { pilot: opt.limit } : {}) })).request_id;
      ids.push(id);
      console.log(`request ${i + 1}/${chunks.length}: ${fmt(chunks[i].length)} orders — ${id}`);
    }
    console.log(`sent ${fmt(take.length)} orders in ${chunks.length} request(s). Watch / Stop them in Batch requests; run this script again (dry) for the coverage.`);
  } finally {
    await mongoose.disconnect();
  }
}

const invokedAs = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href : "";
if (invokedAs === import.meta.url || fileURLToPath(import.meta.url) === path.resolve(process.argv[1] || "")) {
  main().catch((err) => {
    console.error(err instanceof Error ? err.message : err);
    process.exitCode = 1;
    void mongoose.disconnect();
  });
}
