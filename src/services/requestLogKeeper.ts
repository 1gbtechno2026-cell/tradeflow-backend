import { Types, type Model } from "mongoose";
import { countKeyOf, type RequestCounts, type RequestTaskStatus } from "../models/OrderInvoiceRequest.js";
import { SYNC_TASK_KEEP_MS, SYNC_TASK_KINDS, type SyncTaskKind } from "../models/OrderSyncTask.js";
import { emptyCounts, finishedCount, TIMED_OUT } from "./requestCommon.js";
import { forgetProgress, livenessOfRequest, noProbes, noteProgress, queueWatch, type LiveProbes, type QueueWatch } from "./syncBatches.js";
import { loadFacts, logDefaults, sameCounts, syncRequestTasks, taskLog, type LogDeps, type RequestFacts } from "./syncTaskLog.js";

/**
 * The keeper of the request log — one pass every few seconds in the API.
 *
 *   in step     every request that is open, was open at the last pass, or was
 *               created since it, has its own counters compared with its task
 *               rows; where they differ the rows are brought in line from the
 *               document (syncRequestTasks). This is what gives a request its
 *               rows when the worker running it writes none (one started
 *               before the log existed), and what catches up after an outage.
 *   history     the first pass of a process looks back 7 days — as far as the
 *               log keeps rows — so requests that ended before the log existed
 *               are listed too.
 *   rate        the finished count of every open request is noted for the ETA.
 *   stale       a request that is open with nothing behind it is closed: its
 *               unfinished tasks become "Timed out: the agent went offline or
 *               the task ran too long". Only what is this machine's: a fetch
 *               whose row names this host and a process that is gone, a worker
 *               request of this deployment whose queue has had no worker for
 *               30 minutes, pass after pass, with nothing written on the
 *               request in that time — and none of whose jobs is still on the
 *               queue (a worker that comes back runs those: the worker's own
 *               reapers resume the rest). A row that names no machine (written
 *               before 2026-11-24) is closed after a day of the same. A worker
 *               restart, or an API that starts before its worker, is seconds
 *               to minutes without one: never enough.
 *
 * Closing changes the request's own document the way its Force-clear does,
 * with the time-out sentence in place of "Cleared by …"; `afterClose` then
 * does what only the running system can (drop its queue jobs, free the Order
 * rows a cancel or reschedule parked, close its ledger row).
 */

const OPEN_STORED: Record<SyncTaskKind, string[]> = {
  fetch: ["queued", "running"],
  update: ["queued", "running"],
  invoices: ["queued", "in_progress"],
  coupons: ["queued", "in_progress"],
  cancel: ["queued", "running"],
  reschedule: ["queued", "running"],
};

type AnyModel = Model<Record<string, unknown>>;

function modelOf(kind: SyncTaskKind, deps: LogDeps): AnyModel {
  const m = kind === "fetch" ? deps.Ledger : kind === "update" ? deps.Update : kind === "invoices" ? deps.Invoice : kind === "coupons" ? deps.Coupon : kind === "cancel" ? deps.Cancel : deps.Reschedule;
  return m as unknown as AnyModel;
}

// ── closing a request nothing is behind ─────────────────────────────────────

const unitsTimedOut = (prefix: string, now: Date) => ({ [`${prefix}.status`]: "failed", [`${prefix}.message`]: TIMED_OUT, [`${prefix}.finishedAt`]: now });

/**
 * End every unfinished task of an open request as timed out and close the
 * request, in its own document. False when it was no longer open. Nothing
 * outside the request's collection is touched here.
 */
export async function closeTimedOut(f: RequestFacts, deps: LogDeps = logDefaults): Promise<boolean> {
  const _id = new Types.ObjectId(f.head.requestId);
  const now = new Date();
  const open = { $in: OPEN_STORED[f.head.kind] };
  const inFlight = { $in: ["queued", "running"] };
  if (f.head.kind === "fetch") {
    const res = await deps.Ledger.updateOne({ _id, type: "fetch", status: open }, { $set: { status: "failed", finishedAt: now, message: TIMED_OUT } });
    return res.modifiedCount === 1;
  }
  if (f.head.kind === "update") {
    const res = await deps.Update.updateOne({ _id, status: open }, { $set: { ...unitsTimedOut("accounts.$[a]", now), status: "done", finishedAt: now } }, { arrayFilters: [{ "a.status": inFlight }] });
    return res.modifiedCount === 1;
  }
  if (f.head.kind === "cancel") {
    // A request written before urgent mode has no `orders` array, and an array update on a missing path is refused.
    await deps.Cancel.updateOne({ _id, status: open, orders: { $exists: false } }, { $set: { orders: [] } });
    const res = await deps.Cancel.updateOne(
      { _id, status: open },
      { $set: { ...unitsTimedOut("units.$[u]", now), ...unitsTimedOut("orders.$[o]", now), status: "done", finishedAt: now } },
      { arrayFilters: [{ "u.status": inFlight }, { "o.status": inFlight }] }
    );
    return res.modifiedCount === 1;
  }
  if (f.head.kind === "reschedule") {
    const res = await deps.Reschedule.updateOne({ _id, status: open }, { $set: { ...unitsTimedOut("units.$[u]", now), status: "done", finishedAt: now } }, { arrayFilters: [{ "u.status": inFlight }] });
    return res.modifiedCount === 1;
  }
  const ended = { status: "timedout", errorClass: "", error: TIMED_OUT, finishedAt: now };
  if (f.head.kind === "invoices") {
    const n = (status: RequestTaskStatus) => ({ $size: { $filter: { input: "$tasks", as: "t", cond: { $eq: ["$$t.status", status] } } } });
    const literal = Object.fromEntries(Object.entries(ended).map(([k, v]) => [k, { $literal: v }]));
    const res = await deps.Invoice.collection.updateOne({ _id, status: open }, [
      { $set: { tasks: { $map: { input: "$tasks", as: "t", in: { $cond: [{ $in: ["$$t.status", ["queued", "in_progress"]] }, { $mergeObjects: ["$$t", literal] }, "$$t"] } } } } },
      { $set: { counts: { total: { $size: "$tasks" }, queued: n("queued"), in_progress: n("in_progress"), success: n("success"), failed: n("failed"), cancelled: n("cancelled"), aborted: n("aborted"), timed_out: n("timedout") } } },
      { $set: { status: "completed", message: { $literal: TIMED_OUT }, finishedAt: now, updatedAt: now } },
    ]);
    return res.modifiedCount === 1;
  }
  const res = await deps.Coupon.updateOne({ _id, status: open }, { $set: { status: "completed", message: TIMED_OUT, finishedAt: now } });
  if (res.modifiedCount !== 1) return false;
  await deps.CouponTask.updateMany({ requestId: _id, status: { $in: ["queued", "in_progress"] } }, { $set: ended });
  const rows = await deps.CouponTask.aggregate<{ _id: RequestTaskStatus; n: number }>([{ $match: { requestId: _id } }, { $group: { _id: "$status", n: { $sum: 1 } } }]);
  const counts = emptyCounts(0);
  for (const r of rows) {
    counts.total += r.n;
    counts[countKeyOf(r._id)] += r.n;
  }
  await deps.Coupon.updateOne({ _id }, { $set: { counts } });
  return true;
}

// ── one pass ────────────────────────────────────────────────────────────────

export interface KeeperOptions {
  deps?: LogDeps;
  probes?: LiveProbes;
  /** Requests created since this moment are checked, beside every open one and those in `also`. */
  since: Date;
  /** The requests that were open at the last pass: checked once more, so the pass after a request ends brings its last rows in. */
  also?: Partial<Record<SyncTaskKind, string[]>>;
  /**
   * One workspace — the deployment's own, in the API; every workspace when
   * left out. With it each read is off an index: (userId, status) or
   * (userId, createdAt).
   */
  userId?: string;
  /**
   * false = where a collection has no index on status (the fetch ledger,
   * cancel, reschedule), look for open requests among those of the last
   * OPEN_LOOKBACK_MS only. The timer passes false between its full passes, so
   * a pass every few seconds never reads a whole collection.
   */
  everyOpen?: boolean;
  /** After a request was closed as timed out: what only the running system can do for it. */
  afterClose?: (f: RequestFacts) => Promise<void>;
  /** Since when each queue has had no worker, pass after pass (syncBatches.ts QueueWatch). This process's own when left out. */
  watch?: QueueWatch;
  /** false = bring rows in line only; close nothing (the history script's dry run). */
  close?: boolean;
  log?: (line: string) => void;
}

export interface KeeperResult {
  checked: number;
  synced: number;
  rows: number;
  closed: string[];
  /** Open requests that would be closed as stale (listed also when `close` is false). */
  stale: string[];
  /** Open requests silent long enough to close whose jobs still sit on their queue: left for the worker that comes back. */
  waiting: string[];
  /** What is open now, by kind — the next pass's `also`. */
  open: Partial<Record<SyncTaskKind, string[]>>;
}

/** Between full passes, an open fetch / cancel / reschedule is looked for among the requests of these last days. */
const OPEN_LOOKBACK_MS = 3 * 24 * 60 * 60 * 1000;
/** Collections that can find their open requests off an index, however old. */
const STATUS_INDEXED: SyncTaskKind[] = ["update", "invoices", "coupons"];
/** A request whose rows took this long to bring in line is not brought in line again for ten times as long. */
const SLOW_SYNC_MS = 1500;
const notBefore = new Map<string, number>();

/** The rows of many requests by status, in one read of the log. */
async function logCountsOf(requestIds: string[], deps: LogDeps): Promise<Map<string, RequestCounts>> {
  const out = new Map<string, RequestCounts>();
  if (!requestIds.length) return out;
  const rows = await deps.Tasks.aggregate<{ _id: { r: string; s: RequestTaskStatus }; n: number }>([{ $match: { requestId: { $in: requestIds } } }, { $group: { _id: { r: "$requestId", s: "$status" }, n: { $sum: 1 } } }]);
  for (const r of rows) {
    const c = out.get(r._id.r) || emptyCounts(0);
    c.total += r.n;
    c[countKeyOf(r._id.s)] += r.n;
    out.set(r._id.r, c);
  }
  return out;
}

export async function keeperPass(opts: KeeperOptions): Promise<KeeperResult> {
  const deps = opts.deps || logDefaults;
  const probes = opts.probes || noProbes;
  const watch = opts.watch || queueWatch;
  const out: KeeperResult = { checked: 0, synced: 0, rows: 0, closed: [], stale: [], waiting: [], open: {} };
  const queueAlive = new Map<string, boolean>();
  const noWorkerSince = new Map<string, number | null>();
  // Marks still waiting are rows the comparison below would count as behind.
  await taskLog.flush();
  for (const kind of SYNC_TASK_KINDS) {
    const scope = { ...(opts.userId ? { userId: new Types.ObjectId(opts.userId) } : {}), ...(kind === "fetch" ? { type: "fetch" } : {}) };
    const recent = opts.everyOpen === false && !STATUS_INDEXED.includes(kind) ? { createdAt: { $gte: new Date(Date.now() - OPEN_LOOKBACK_MS) } } : {};
    const [openNow, created] = await Promise.all([
      modelOf(kind, deps).find({ ...scope, ...recent, status: { $in: OPEN_STORED[kind] } }).select("_id").lean(),
      modelOf(kind, deps).find({ ...scope, createdAt: { $gte: opts.since } }).select("_id").lean(),
    ]);
    const requestIds = [...new Set([...openNow, ...created].map((r) => String(r._id)).concat(opts.also?.[kind] || []))];
    if (!requestIds.length) continue;
    let facts = await loadFacts(kind, requestIds, deps);

    // Stale first: a request closed here is then brought in line like any other that changed.
    let closedHere = 0;
    for (const f of facts.values()) {
      const open = f.status === "queued" || f.status === "in_progress";
      if (!open) {
        forgetProgress(f.head.requestId);
        continue;
      }
      noteProgress(f.head.requestId, finishedCount(f.counts));
      if (f.queue && !queueAlive.has(f.queue)) {
        const alive = await probes.queueHasWorker(f.queue).catch(() => true);
        queueAlive.set(f.queue, alive);
        noWorkerSince.set(f.queue, watch.note(f.queue, alive));
      }
      const live = livenessOfRequest(f, { now: Date.now(), probes, queueAlive: queueAlive.get(f.queue) ?? true, noWorkerSince: noWorkerSince.get(f.queue) ?? null });
      if (!live.stale) continue;
      // Its jobs are still on the queue: the worker that comes back runs them. Nothing is closed under it.
      if (f.head.kind !== "fetch" && probes.jobsWaiting && (await probes.jobsWaiting(f).catch(() => true))) {
        out.waiting.push(`${kind} ${f.head.requestId}`);
        continue;
      }
      out.stale.push(`${kind} ${f.head.requestId} — ${live.stale}`);
      if (opts.close === false) continue;
      if (await closeTimedOut(f, deps)) {
        closedHere += 1;
        out.closed.push(f.head.requestId);
        opts.log?.(`${kind} ${f.head.requestId} closed as timed out — ${live.stale}`);
        await opts.afterClose?.(f).catch((err) => opts.log?.(`${kind} ${f.head.requestId}: after-close step failed — ${err instanceof Error ? err.message.split("\n")[0] : err}`));
      }
    }
    if (closedHere) facts = await loadFacts(kind, requestIds, deps);
    out.open[kind] = [...facts.values()].filter((f) => f.status === "queued" || f.status === "in_progress").map((f) => f.head.requestId);

    const have = await logCountsOf(requestIds, deps);
    const kept = Date.now() - SYNC_TASK_KEEP_MS;
    for (const f of facts.values()) {
      // A request older than the log keeps has no rows to keep in step: they would be removed as soon as written.
      if (f.head.createdAt.getTime() < kept) continue;
      out.checked += 1;
      const id = f.head.requestId;
      const rows = have.get(id) || emptyCounts(0);
      const open = f.status === "queued" || f.status === "in_progress";
      let behind: boolean;
      if (kind === "fetch") {
        // A fetch of this version writes its own rows while it runs; what is left for a pass is the run
        // that ended with rows still open, and the older run whose named accounts were never listed.
        behind = f.tasksRecorded === "all" ? !open && rows.queued + rows.in_progress > 0 : rows.total === 0 && f.counts.total > 0 ? true : !open && rows.queued + rows.in_progress > 0;
      } else behind = !sameCounts(rows, f.counts);
      if (!behind) continue;
      if ((notBefore.get(id) || 0) > Date.now()) continue;
      const t = Date.now();
      const res = await syncRequestTasks(kind, id, deps);
      const took = Date.now() - t;
      if (took > SLOW_SYNC_MS && open) notBefore.set(id, Date.now() + took * 10);
      else notBefore.delete(id);
      if (res.inserted || res.moved) {
        out.synced += 1;
        out.rows += res.inserted + res.moved;
      }
    }
  }
  return out;
}

// ── the timer ───────────────────────────────────────────────────────────────

const KEEP_EVERY_MS = Math.max(3000, Number(process.env.REQUEST_LOG_KEEP_MS) || 10_000);
/** How far behind the last pass each pass looks: a request created while the last one was reading is not missed. */
const KEEP_OVERLAP_MS = 60_000;
/** How often a pass looks for open requests of any age (and so finds one left open for days). */
const FULL_PASS_EVERY_MS = 30 * 60 * 1000;

/**
 * Start the keeper (API process) for one workspace — the deployment's own. The
 * first pass covers the 7 days the log keeps and open requests of any age;
 * after it a pass looks at what is open, what was open a pass ago and what was
 * created since.
 */
export function startRequestLogKeeper(opts: { probes: LiveProbes; afterClose?: (f: RequestFacts) => Promise<void>; deps?: LogDeps; userId?: string }): NodeJS.Timeout {
  let busy = false;
  let since = new Date(Date.now() - SYNC_TASK_KEEP_MS);
  let first = true;
  let also: KeeperOptions["also"] = {};
  let fullAt = 0;
  const pass = () => {
    if (busy) return;
    busy = true;
    const startedAt = Date.now();
    const everyOpen = startedAt - fullAt >= FULL_PASS_EVERY_MS;
    if (everyOpen) fullAt = startedAt;
    void keeperPass({ ...opts, since, also, everyOpen, log: (line) => console.log(`[request-log] ${line}`) })
      .then((res) => {
        since = new Date(startedAt - KEEP_OVERLAP_MS);
        also = res.open;
        if (first || res.synced || res.closed.length) {
          console.log(`[request-log] ${first ? "first pass (7 days)" : "pass"}: ${res.checked} request(s) checked, ${res.synced} brought in line (${res.rows} task row(s)), ${res.closed.length} closed as timed out — ${Date.now() - startedAt} ms`);
        }
        first = false;
      })
      .catch((err) => console.warn(`[request-log] pass failed: ${err instanceof Error ? err.message.split("\n")[0] : err}`))
      .finally(() => (busy = false));
  };
  pass();
  const t = setInterval(pass, KEEP_EVERY_MS);
  t.unref();
  return t;
}
