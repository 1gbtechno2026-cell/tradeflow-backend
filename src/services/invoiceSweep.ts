import type { Queue } from "bullmq";
import { Types } from "mongoose";
import { Order } from "../models/Order.js";
import { Invoice } from "../models/Invoice.js";
import { OrderInvoiceRequest } from "../models/OrderInvoiceRequest.js";
import { OrderJobRun } from "../models/OrderJobRun.js";
import { config } from "../config.js";
import { getInvoiceQueue, type InvoiceJobData } from "../queue.js";
import { invoiceStoreEnabled } from "./invoiceCapture.js";
import { downloadInvoicesByOrderId, type OrderDownloadResult } from "./invoiceOnDemand.js";
import { stopInvoiceRequest } from "./invoiceRequests.js";
import { startRun, progressRun, finishRun } from "./orderJobRuns.js";
import { recordSweepAsRequest, type SweepPhase } from "./invoiceSweepRequest.js";
import { CHAIN_INVOICE_SWEEP } from "./requestChains.js";
import { attachRun, claimManual, clockTime, markStarted, releaseLease, renewLease, slotStamp, supersededText, type LeaseHandle, type ManualClaim, type ScheduleRunner } from "./scheduleClock.js";
import { clockProcessUp, retireLegacySchedulers } from "./scheduleSettings.js";

/**
 * Automatic, time-settled invoice capture (phase 7c).
 *
 * WHEN it runs is the shared schedule clock's (services/scheduleClock.ts): the
 * "Invoice sweep" switch in Settings, every 3 h at :35, on whichever machine
 * that may run it is awake — INVOICE_SCHEDULE=0 in a machine's .env only says
 * that this machine never runs it. The clock's runner (invoiceSweepRunner)
 * queues one "sweep" job on this machine's invoice queue, which runs one job at
 * a time, so a scheduled sweep and one started by hand never overlap in one
 * process; between machines the lease does that (runSweepJob, queueManualSweep).
 *
 * Phase 1 — CAPTURE: every delivered order with no invoice yet is downloaded
 * (force off). Captured orders get `captured_at` and `settled = false`.
 *
 * Phase 2 — RE-CAPTURE + SETTLE: an invoice still `settled = false` whose
 * `captured_at` is older than the settle window (default 48 h) is re-downloaded
 * once (force on) to sweep the late Protect-Promise / GT parts, then marked
 * `settled` so it is never touched again. Bounded to one re-capture per order.
 *
 * Both phases reuse the proven downloadInvoicesByOrderId (resolve account →
 * session → zip-aware download → S3 → pipeline re-extract). Each phase is capped
 * per run (INVOICE_MAX_PER_RUN) so a sweep never blasts the whole backlog.
 *
 * Legacy invoices captured before this existed have `captured_at = null`; they
 * are excluded from phase 2 (a one-time migration settles the backfill).
 */

const SETTLE_AFTER_MS = Math.max(1, Number(process.env.INVOICE_SETTLE_HOURS || 48)) * 3_600_000;

export interface InvoiceSweepResult {
  phase1: { candidates: number; downloaded: number; failed: number };
  phase2: { candidates: number; recaptured: number; settled: number; failed: number };
}

export async function runInvoiceSweep(
  userId: string,
  opts: {
    concurrency?: number;
    cap?: number;
    cancelled?: () => boolean;
    log?: (level: "info" | "warn", msg: string) => void;
    /** For whoever records the run as a request (services/invoiceSweepRequest.ts): a phase's orders before it starts, and each order as it is taken and answered. */
    onPhase?: (phase: SweepPhase, orderIds: string[]) => void | Promise<void>;
    onStart?: (orderId: string) => void | Promise<void>;
    onResult?: (result: OrderDownloadResult) => void | Promise<void>;
  } = {}
): Promise<InvoiceSweepResult> {
  const log = opts.log || (() => undefined);
  const cap = opts.cap ?? config.invoiceMaxPerRun;
  const concurrency = opts.concurrency ?? 6;
  const now = new Date();
  const out: InvoiceSweepResult = {
    phase1: { candidates: 0, downloaded: 0, failed: 0 },
    phase2: { candidates: 0, recaptured: 0, settled: 0, failed: 0 },
  };
  if (!invoiceStoreEnabled()) {
    log("warn", "invoice sweep skipped — INVOICE_S3_BUCKET is not set");
    return out;
  }

  // Phase 1 — capture delivered orders with no invoice yet.
  const missing = (
    await Order.distinct("order_id", {
      userId,
      status_key: /deliver/i,
      $or: [{ invoice_s3_key: { $in: [null, ""] } }, { invoice_s3_key: { $exists: false } }],
    })
  ).slice(0, cap) as string[];
  out.phase1.candidates = missing.length;
  if (missing.length && !opts.cancelled?.()) {
    await opts.onPhase?.("capture", missing);
    const r = await downloadInvoicesByOrderId({ userId, orderIds: missing, force: false, concurrency, cancelled: opts.cancelled, log, onStart: opts.onStart, onResult: opts.onResult });
    const downloaded = r.filter((x) => x.status === "downloaded").map((x) => x.orderId);
    out.phase1.downloaded = downloaded.length;
    out.phase1.failed = r.filter((x) => x.status === "failed").length;
    if (downloaded.length) await Invoice.updateMany({ userId, order_id: { $in: downloaded } }, { $set: { captured_at: now, settled: false } });
  }

  // Phase 2 — re-capture once, SETTLE_AFTER_MS after capture #1, then settle.
  const due = (
    await Invoice.find({ userId, settled: false, captured_at: { $ne: null, $lte: new Date(now.getTime() - SETTLE_AFTER_MS) } })
      .select("order_id")
      .limit(cap)
      .lean()
  ).map((d) => String(d.order_id));
  out.phase2.candidates = due.length;
  if (due.length && !opts.cancelled?.()) {
    await opts.onPhase?.("recapture", due);
    const r = await downloadInvoicesByOrderId({ userId, orderIds: due, force: true, concurrency, cancelled: opts.cancelled, log, onStart: opts.onStart, onResult: opts.onResult });
    const ok = r.filter((x) => x.status === "downloaded").map((x) => x.orderId);
    out.phase2.recaptured = ok.length;
    out.phase2.failed = r.filter((x) => x.status === "failed").length;
    // Settle every due order we successfully re-downloaded; leave failures for the next sweep.
    if (ok.length) {
      const res = await Invoice.updateMany({ userId, order_id: { $in: ok } }, { $set: { settled: true, captured_at: now } });
      out.phase2.settled = res.modifiedCount || ok.length;
    }
  }

  log(
    "info",
    `invoice sweep: captured ${out.phase1.downloaded}/${out.phase1.candidates}, re-captured ${out.phase2.recaptured}/${out.phase2.candidates} (settled ${out.phase2.settled})`
  );
  return out;
}

// ── the sweep on the shared clock ───────────────────────────────────────────

/**
 * A "sweep" job of the invoice queue, as it is queued since the shared clock:
 * with the lease claim it runs under. Missing on a job of the per-machine
 * BullMQ scheduler this queue had before (`sweep: "scheduled"`, no token — it
 * runs no sweep any more) and on a manual one an API from before the clock
 * queued (the worker then takes the lease itself).
 */
export type SweepJobData = InvoiceJobData & {
  /** The claim: the clock's for a fixed time, the button's for a sweep started by hand. */
  token?: string;
  /** The fixed time, ISO — a scheduled sweep only. */
  slotAt?: string;
};

/**
 * What the sweep's schedule reaches outside Mongo and the lease. The worker
 * and the API pass nothing; scripts/scheduleSweepCheck.ts passes a throwaway
 * queue in Redis db 15 and a sweep that downloads nothing.
 */
export interface InvoiceSweepDeps {
  /** This machine's invoice queue. */
  queue: () => Queue<InvoiceJobData>;
  /** The sweep itself. */
  sweep: typeof runInvoiceSweep;
  /** Can a sweep store anything on this machine? (INVOICE_S3_BUCKET) */
  storeEnabled: () => boolean;
  /** Record the run as a request of the log (CHAIN_INVOICE_SWEEP). */
  asRequest: boolean;
  /** Stop the request a sweep is recorded as, the way Batch requests → Cancel does: what a takeover by another machine calls. */
  stopRequest: (userId: string, requestId: string, by: string) => Promise<unknown>;
  /** A leftover job of the old per-machine scheduler arrived: the worker makes its schedule clock look. Nothing by default. */
  clockLook: () => Promise<void>;
}

export const invoiceSweepDefaults: InvoiceSweepDeps = {
  queue: getInvoiceQueue,
  sweep: (userId, opts) => runInvoiceSweep(userId, opts),
  storeEnabled: invoiceStoreEnabled,
  asRequest: CHAIN_INVOICE_SWEEP,
  stopRequest: (userId, requestId, by) => stopInvoiceRequest(userId, requestId, by),
  clockLook: async () => undefined,
};

const sweepQueue = (deps: InvoiceSweepDeps) => deps.queue() as unknown as Queue<SweepJobData>;
const firstLine = (err: unknown) => (err instanceof Error ? err.message.split("\n")[0] : String(err));

/** The sweep running in this process, per workspace: what a takeover by another machine stops. */
const runningSweeps = new Map<string, { token: string; stop: (reason: string) => void; requestId: () => string }>();

/** "captured 3/3, re-captured 2" — the sentence of a sweep's last tick; "nothing to do" when neither phase had an order. */
export function sweepWords(result: InvoiceSweepResult): string {
  const failed = result.phase1.failed + result.phase2.failed;
  if (!result.phase1.candidates && !result.phase2.candidates) return "nothing to do";
  return `captured ${result.phase1.downloaded}/${result.phase1.candidates}, re-captured ${result.phase2.recaptured}${result.phase2.candidates > result.phase2.recaptured ? `/${result.phase2.candidates}` : ""}${failed ? `, failed ${failed}` : ""}`;
}

/**
 * The invoice sweep as the worker's schedule clock runs it. A fixed time this
 * machine took becomes ONE job on its invoice queue — `{ sweep: "scheduled",
 * slotAt, token }`, job id sched-invoice-<the time in ms>, so two processes
 * that both believed they had the time still queue one job. The lease is kept
 * without a run until the job starts and says which ledger run it is
 * (runTrackedInvoiceSweep); the job's end gives the lease back.
 *
 * Nothing is queued — the time's tick is "skipped — …" — when this machine
 * cannot store an invoice, or an invoice run is already in progress.
 * `supersede`: another machine took the sweep over while this one was away —
 * the sweep here stops between two orders, and its request (when it has one)
 * is stopped the way Batch requests → Cancel stops it.
 */
export function invoiceSweepRunner(overrides: Partial<InvoiceSweepDeps> = {}): ScheduleRunner {
  return {
    async run(claim) {
      const deps = { ...invoiceSweepDefaults, ...overrides };
      if (!deps.storeEnabled()) return { skipped: true, reason: "INVOICE_S3_BUCKET is not set on this machine" };
      const open = await OrderJobRun.findOne({ userId: claim.userId, type: "invoices", status: "running" }).select("_id").lean();
      if (open) return { skipped: true, reason: "an invoice run is already in progress" };
      const slot = claim.slotAt || claim.takenAt;
      await sweepQueue(deps).add("sweep", { sweep: "scheduled", slotAt: slot.toISOString(), token: claim.token }, { jobId: `sched-invoice-${slot.getTime()}`, attempts: 1 });
      return {};
    },
    async supersede(info) {
      const deps = { ...invoiceSweepDefaults, ...overrides };
      for (const [userId, sweep] of runningSweeps) {
        if (!info.token || sweep.token !== info.token) continue;
        const reason = supersededText("invoice_sweep", info.by, info.at);
        // The sweep itself first — it gives up between two orders whatever happens to its request.
        sweep.stop(reason);
        console.log(`[invoice-sweep] ${reason} — this machine's sweep stops after the order in hand`);
        const requestId = sweep.requestId();
        if (requestId) await deps.stopRequest(userId, requestId, `the schedule clock: ${info.by.host || info.by.systemId} took over the invoice sweep at ${clockTime(info.at)}`);
      }
    },
  };
}

export type ManualSweep = { ok: true; message: string; leased: boolean } | (Extract<ManualClaim, { ok: false }> & { code: "held_elsewhere" });

/**
 * POST /invoices/sweep — "Capture newly delivered now". Takes the sweep's
 * lease by hand first (it works whatever the switch says):
 *   another machine holds a live lease   → refused: 409 "The invoice sweep is
 *                                          running on <host> since <time>"
 *   taken                                → one job with the claim; the worker's
 *                                          clock keeps the lease renewed and the
 *                                          job's end gives it back
 *   this machine is sweeping already     → the job is queued behind it, as it
 *                                          always was, and takes the lease
 *                                          itself when its turn comes
 * While this machine's worker is not on the shared clock (it still runs the
 * code from before it — the hours between this API's first start and that
 * worker's restart — or is not running) nobody would renew a lease or give it
 * back: the claim is released at once, saying so, and the job goes out as it
 * always did.
 */
export async function queueManualSweep(userId: string, by: string, overrides: Partial<InvoiceSweepDeps> = {}): Promise<ManualSweep> {
  const deps = { ...invoiceSweepDefaults, ...overrides };
  const taken = await claimManual(userId, "invoice_sweep", by);
  if (!taken.ok && taken.code === "held_elsewhere") return { ...taken, code: "held_elsewhere" };
  let token = "";
  if (taken.ok) {
    if (await clockProcessUp("invoice_sweep")) token = taken.claim.token;
    else await releaseLease(taken.claim, { outcome: "ran", reason: `started by hand${by ? ` by ${by}` : ""} — queued for the worker on ${config.hostId}, which is not on the shared clock, so no lease is held for it` });
  }
  try {
    await sweepQueue(deps).add("sweep", { sweep: "manual", by, ...(token ? { token } : {}) }, { attempts: 1 });
  } catch (err) {
    if (taken.ok && token) await releaseLease(taken.claim, { outcome: "failed", reason: `could not queue the sweep: ${firstLine(err)}` }).catch(() => undefined);
    throw err;
  }
  const behind = !taken.ok && taken.code === "running_here";
  return { ok: true, leased: Boolean(token), message: behind ? "Invoice sweep queued — it starts when the sweep running on this machine has finished" : "Invoice sweep queued" };
}

/**
 * (worker) One "sweep" job of the invoice queue.
 *   scheduled, with a claim   the clock's fixed time: runs under that lease
 *   manual, with a claim      the button's: runs under that lease
 *   manual, without           queued behind a sweep, by an API from before the
 *                             clock, or for a worker that was not on the clock:
 *                             the lease is taken here; refused (and nothing
 *                             runs) while another holder has it
 *   scheduled, without        a leftover of the per-machine BullMQ scheduler:
 *                             it runs no sweep — the scheduler is removed again
 *                             and the clock looks
 */
export async function runSweepJob(userId: string, data: SweepJobData, overrides: Partial<InvoiceSweepDeps> = {}): Promise<{ skipped: boolean; reason?: string; result?: InvoiceSweepResult }> {
  const deps = { ...invoiceSweepDefaults, ...overrides };
  const manual = data.sweep === "manual";
  if (!manual && !data.token) {
    await retireLegacySchedulers(["invoice_sweep"], { update: deps.queue, invoice: deps.queue });
    await deps.clockLook();
    return { skipped: true, reason: "a leftover job of the old per-machine scheduler — no sweep is run from it, the shared clock decides" };
  }
  let lease: LeaseHandle | null = data.token ? { userId, name: "invoice_sweep", token: data.token } : null;
  if (!lease) {
    const taken = await claimManual(userId, "invoice_sweep", data.by || "");
    if (taken.ok) lease = taken.claim;
    // A workspace Trade Flow has not set a clock up for has no lease to take: the sweep runs as it always did.
    else if (taken.code !== "no_clock") return { skipped: true, reason: taken.message };
  }
  const slotAt = data.slotAt && !Number.isNaN(new Date(data.slotAt).getTime()) ? new Date(data.slotAt) : null;
  return runTrackedInvoiceSweep(userId, { trigger: manual ? "user" : "system-cron", by: data.by, lease, slotAt }, overrides);
}

/**
 * The sweep as a tracked run: records an `invoices` OrderJobRun (so the Sync
 * panel / running indicator shows it), single-flights against a run already in
 * progress, and reports counts. This is what a "sweep" job of the worker runs
 * (runSweepJob): the clock's fixed time, or the manual "Invoices" button.
 *
 * Since step 5 a run that has something to download is also a REQUEST of the
 * log — the reference's nightly "Invoice fetch", source `system-cron` (`user`
 * for the button): one order_invoice_requests document, a task per order,
 * written as the sweep goes (services/invoiceSweepRequest.ts). The sweep
 * itself is untouched: the same single-flight, the same cap per phase, the
 * same downloads, the same captured_at / settled bookkeeping. Batch requests →
 * Cancel on such a request stops the sweep between two orders.
 * CHAIN_INVOICE_SWEEP=0 leaves the request out.
 *
 * Under a lease (`input.lease`, services/scheduleClock.ts): the run does not
 * start when the lease is no longer this claim's; the lease follows the ledger
 * run from its start (the clock renews it while that row is open) and is given
 * back at the end, with the tick "finished — captured x/y, re-captured z",
 * "ran on <host> — nothing to do" (C4's wording, as the update sweeps say
 * "nothing to refresh"), or "failed — …". A fixed time of the clock also
 * says which schedule, which time and which machine: on the ledger run, and on
 * the request of the log when the run has one (Details → "Schedule").
 */
export async function runTrackedInvoiceSweep(
  userId: string,
  input: { trigger?: "user" | "system-cron"; by?: string; lease?: LeaseHandle | null; slotAt?: Date | null } = {},
  overrides: Partial<InvoiceSweepDeps> = {}
): Promise<{ skipped: boolean; reason?: string; result?: InvoiceSweepResult }> {
  const deps = { ...invoiceSweepDefaults, ...overrides };
  const lease = input.lease || null;
  const manual = input.trigger === "user";
  const quiet = (what: string) => (err: unknown) => console.warn(`[invoice-sweep] ${what}: ${firstLine(err)} — the clock's next look settles the lease`);
  // A job that waited — behind a long sweep, or while its machine slept — may have lost its lease: to another
  // machine, or to the clock's own 30-minute limit for a time nobody started.
  if (lease && !(await renewLease(lease))) return { skipped: true, reason: "its lease was taken over, or given up, before the sweep's turn came" };

  const open = await OrderJobRun.findOne({ userId, type: "invoices", status: "running" }).select("_id").lean();
  if (open) {
    if (lease) await releaseLease(lease, { outcome: "skipped", reason: "an invoice run is already in progress" }).catch(quiet("the release"));
    return { skipped: true, reason: "an invoice run is already in progress" };
  }

  // A fixed time the clock took: which schedule, which time (India time), which machine.
  const scheduled = lease && !manual ? { schedule_name: "invoice_sweep", schedule_slot: slotStamp(input.slotAt), ran_on: config.hostId } : null;
  const ledgerRunId = await startRun(userId, "invoices", {
    trigger: input.trigger || "system-cron",
    by: input.by || "schedule",
    message: "Invoice sweep running…",
    meta: {
      request: "invoice_fetch",
      platform: "FLIPKART",
      sweep: true,
      params: { force: false, cap_per_phase: config.invoiceMaxPerRun, ...(scheduled || {}) },
    },
  });
  const runRef = { kind: "invoices" as const, id: ledgerRunId };
  // From here the lease follows this ledger run. A fixed time's tick ("ran on Mac") was written when the job was
  // queued; a sweep started by hand gets its tick now.
  if (lease) await (manual ? markStarted(lease, { runRef, reason: `started by hand${input.by ? ` by ${input.by}` : ""}` }) : attachRun(lease, runRef)).catch(quiet("the run"));

  const recorder = deps.asRequest ? recordSweepAsRequest(userId, { source: manual ? "user" : "system-cron", by: manual ? input.by || "" : "", ledgerRunId, capPerPhase: config.invoiceMaxPerRun }) : null;
  // The recorder makes the request at the first phase that has an order; a sweep of the clock then writes its
  // schedule, time and machine on it, once.
  let stamped = false;
  const onPhase = async (phase: SweepPhase, orderIds: string[]) => {
    await recorder?.onPhase(phase, orderIds);
    const requestId = recorder?.requestId() || "";
    if (!scheduled || stamped || !requestId) return;
    stamped = true;
    await OrderInvoiceRequest.collection
      .updateOne({ _id: new Types.ObjectId(requestId) }, { $set: { "params.schedule_name": scheduled.schedule_name, "params.schedule_slot": scheduled.schedule_slot, "params.ran_on": scheduled.ran_on } })
      .catch((err) => console.warn(`[invoice-sweep] request ${requestId.slice(-8)}: its schedule line was not recorded — ${firstLine(err)}`));
  };
  let superseded = "";
  runningSweeps.set(userId, { token: lease?.token || "", stop: (reason) => void (superseded = reason), requestId: () => recorder?.requestId() || "" });
  let end: { outcome: "finished" | "failed" | "ran"; reason: string } = { outcome: "failed", reason: "" };
  try {
    const result = await deps.sweep(userId, {
      log: (lvl, m) => {
        if (lvl === "info") void progressRun(userId, "invoices", { message: m });
      },
      // Batch requests → Cancel on the run's request, or another machine having taken the sweep over.
      cancelled: () => Boolean(superseded) || Boolean(recorder?.stopped()),
      ...(recorder ? { onPhase, onStart: recorder.onStart, onResult: recorder.onResult } : {}),
    });
    const success = result.phase1.downloaded + result.phase2.recaptured;
    const failed = result.phase1.failed + result.phase2.failed;
    const total = result.phase1.candidates + result.phase2.candidates;
    const stopped = Boolean(recorder?.stopped()) || Boolean(superseded);
    await recorder?.finish();
    const counts = `Captured ${result.phase1.downloaded}/${result.phase1.candidates}, re-captured ${result.phase2.recaptured}/${result.phase2.candidates} (settled ${result.phase2.settled})`;
    await finishRun(userId, "invoices", {
      status: stopped ? "stopped" : "completed",
      total,
      processed: total,
      success,
      failed,
      cancelled: 0,
      message: superseded ? `${superseded} — ${counts}` : counts,
    });
    const words = sweepWords(result);
    // No order in either phase: the time "ran — nothing to do" (C4); anything captured or stopped "finished — …".
    end = stopped ? { outcome: "finished", reason: `stopped — ${words}` } : { outcome: words === "nothing to do" ? "ran" : "finished", reason: words };
    return { skipped: false, result };
  } catch (err) {
    await recorder?.finish(err);
    await finishRun(userId, "invoices", {
      status: "failed",
      total: 0,
      processed: 0,
      success: 0,
      failed: 0,
      cancelled: 0,
      message: err instanceof Error ? err.message : "invoice sweep failed",
    });
    end = { outcome: "failed", reason: firstLine(err) };
    throw err;
  } finally {
    runningSweeps.delete(userId);
    // Nothing is written when another machine took the lease over meanwhile: the release matches only this claim.
    if (lease) await releaseLease(lease, { ...end, runRef }).catch(quiet("the release"));
  }
}

/**
 * One-time: settle invoices captured before the time-settled model existed
 * (the backfilled bundles — old deliveries whose parts are already final), so
 * phase 2 does not re-download the whole backlog. Safe to run repeatedly.
 */
export async function settleLegacyInvoices(userId?: string): Promise<number> {
  const filter: Record<string, unknown> = { settled: { $ne: true }, processing_status: "processed", captured_at: null };
  if (userId) filter.userId = userId;
  const res = await Invoice.updateMany(filter, [{ $set: { settled: true, captured_at: "$updatedAt" } }]);
  return res.modifiedCount || 0;
}
