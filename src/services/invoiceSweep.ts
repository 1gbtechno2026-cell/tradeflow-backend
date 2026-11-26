import type { Queue } from "bullmq";
import { Types } from "mongoose";
import { Order } from "../models/Order.js";
import { Invoice } from "../models/Invoice.js";
import { OrderInvoiceRequest } from "../models/OrderInvoiceRequest.js";
import { OrderJobRun } from "../models/OrderJobRun.js";
import { config } from "../config.js";
import { getInvoiceQueue, type InvoiceJobData } from "../queue.js";
import { invoiceSettleMs, invoiceStoreEnabled, settledByDelivery, type SettleUnit } from "./invoiceCapture.js";
import { getInvoiceSettings, invoiceSettingsFromEnv, type EffectiveInvoiceSettings } from "./invoiceSettings.js";
import { downloadInvoicesByOrderId, type OrderDownloadResult } from "./invoiceOnDemand.js";
import { stopInvoiceRequest } from "./invoiceRequests.js";
import { startRun, progressRun, finishRun } from "./orderJobRuns.js";
import { recordSweepAsRequest, type SweepPhase } from "./invoiceSweepRequest.js";
import { emptyRefetchResult, runRefetchPhase, type RefetchPhaseResult } from "./invoiceRefetch.js";
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
 * (force off). Captured orders get `captured_at`, and `settled` by the delivery
 * rule (registerInvoice, T2 of the data-quality plan): every unit finished and
 * the last delivery older than the settle window.
 *
 * Phase 2 — RE-CAPTURE + SETTLE: an invoice still `settled = false` whose
 * `captured_at` is older than the settle window (default 48 h) is re-downloaded
 * (force on) to sweep the late Protect-Promise / GT parts — files equal to the
 * processed bundle's originals are not uploaded again (skipIfUnchanged) — then
 * settled by the same rule; one still on its way is looked at again a window
 * later (settleRecaptured), and settled anyway recaptureGiveUpDays (default 30)
 * after its row was made.
 *
 * Phase 3 — REFETCH (data-quality SO-5, services/invoiceRefetch.ts): an order
 * whose `invoices` row holds a late re-download that is due (a unit cancelled,
 * returned or refunded after it was invoiced) is claimed, downloaded again once
 * (force, skipIfUnchanged) and its due entries closed; one a later download
 * already covers closes without a call; 3 failures give up. `settled` stays.
 *
 * Phase 1's failure memory (SO-6): an order whose capture failed ON FLIPKART'S
 * SIDE (an HTTP status, an answer that is not a PDF / zip) is not tried again
 * within 24 h, and never after 3 such failures (invoice_capture_failures /
 * invoice_capture_failed_at on its units). Our own failures (S3, Mongo — the
 * result's `ours`) and "no session" are not counted; a later capture of the
 * order clears its memory. The invoice setting captureCancelledAfterShipping
 * (seeded ON, owner decision 2026-11-26; INVOICE_CAPTURE_CANCELLED=1 before it)
 * widens phase 1 to orders cancelled after shipping with no bundle, once the
 * last cancellation is 15 days old.
 *
 * The windows and the switch are invoice settings (services/invoiceSettings.ts:
 * Settings → Invoice settings, else .env, else the default), read once per sweep.
 *
 * All phases reuse the proven downloadInvoicesByOrderId (resolve account →
 * session → zip-aware download → S3 → pipeline re-extract). Each phase is capped
 * per run (INVOICE_MAX_PER_RUN) so a sweep never blasts the whole backlog.
 *
 * Legacy invoices captured before this existed have `captured_at = null`; they
 * are excluded from phase 2 (a one-time migration settles the backfill).
 */

/**
 * The invoice settings (services/invoiceSettings.ts — Settings → Invoice settings, else the .env, else the default) are
 * read ONCE at the top of each sweep (runInvoiceSweep) and passed down, so a saved change applies from the next sweep,
 * with no worker restart. The sync helpers below take them as an argument; without one they read the .env and the
 * defaults at call time (as before).
 */

/**
 * The invoice setting `recaptureGiveUpDays` (else INVOICE_RECAPTURE_GIVE_UP_DAYS, else 30): a bundle still not final
 * this long after its row was made is settled anyway, so an order stuck "on its way" is not re-downloaded every window
 * for ever.
 */
export function recaptureGiveUpMs(settings: EffectiveInvoiceSettings = invoiceSettingsFromEnv()): number {
  return settings.recaptureGiveUpDays * 86_400_000;
}

/**
 * After a successful re-capture: settle each order whose units pass the delivery rule (settledByDelivery), or whose
 * row is older than recaptureGiveUpMs(); the rest get captured_at = now, so phase 2 looks again one window later.
 * Returns how many were settled. `settings`: the sweep's; without, the workspace's are read.
 */
export async function settleRecaptured(userId: string, orderIds: string[], now = new Date(), settings?: EffectiveInvoiceSettings): Promise<number> {
  const s = settings || (await getInvoiceSettings(userId)).effective;
  const units = await Order.find({ userId, order_id: { $in: orderIds } }).select("order_id status_key actual_delivery_date cancelled_date").lean();
  const byOrder = new Map<string, SettleUnit[]>();
  for (const u of units) {
    const list = byOrder.get(String(u.order_id)) || [];
    list.push(u as SettleUnit);
    byOrder.set(String(u.order_id), list);
  }
  const rows = await Invoice.find({ userId, order_id: { $in: orderIds } }).select("order_id createdAt").lean();
  const giveUpMs = recaptureGiveUpMs(s);
  const settleMs = invoiceSettleMs(s);
  const old = new Set(rows.filter((r) => r.createdAt && now.getTime() - new Date(r.createdAt).getTime() > giveUpMs).map((r) => String(r.order_id)));
  const final = orderIds.filter((o) => old.has(o) || settledByDelivery(byOrder.get(o) || [], now, settleMs));
  const open = orderIds.filter((o) => !final.includes(o));
  let settled = 0;
  if (final.length) settled = (await Invoice.updateMany({ userId, order_id: { $in: final } }, { $set: { settled: true, captured_at: now } })).modifiedCount || final.length;
  if (open.length) await Invoice.updateMany({ userId, order_id: { $in: open } }, { $set: { captured_at: now } });
  return settled;
}

export interface InvoiceSweepResult {
  phase1: { candidates: number; downloaded: number; failed: number };
  phase2: { candidates: number; recaptured: number; settled: number; failed: number };
  /** Late re-downloads (SO-5). Absent on a result made before it existed. */
  phase3?: RefetchPhaseResult;
}

// ── phase 1's selection (SO-6) ───────────────────────────────────────────────

/**
 * SO-6 switch — the invoice setting `captureCancelledAfterShipping` ("Download invoices of orders cancelled after
 * shipping", seeded ON — owner decision 2026-11-26; else INVOICE_CAPTURE_CANCELLED=1, else off): phase 1 also captures
 * orders cancelled after shipping. Without `settings`: the .env and the default, read at call time.
 */
export function captureCancelledEnabled(settings: EffectiveInvoiceSettings = invoiceSettingsFromEnv()): boolean {
  return settings.captureCancelledAfterShipping;
}
/** Every credit note measured arrived within 14 days of the cancellation: one download at 15 days is final. */
export const CAPTURE_CANCELLED_AFTER_DAYS = 15;
/** Failure memory: an order is tried at most this often, and not again after this many failures. */
export const CAPTURE_RETRY_AFTER_MS = 24 * 3_600_000;
export const CAPTURE_MAX_FAILURES = 3;

const noKey = { $or: [{ invoice_s3_key: { $in: [null, ""] } }, { invoice_s3_key: { $exists: false } }] };
const ENDED_RE = /^(cancelled|canceled|returned)$/i;

/** Orders phase 1 must leave alone for now: a capture failed in the last 24 h, or 3 times. */
async function captureHeldBack(userId: string, now: Date): Promise<string[]> {
  return (await Order.collection.distinct("order_id", {
    userId: new Types.ObjectId(userId),
    $or: [{ invoice_capture_failures: { $gte: CAPTURE_MAX_FAILURES } }, { invoice_capture_failed_at: { $gt: new Date(now.getTime() - CAPTURE_RETRY_AFTER_MS) } }],
  })) as string[];
}

/**
 * SO-6 (only with captureCancelledAfterShipping on): orders with no bundle whose units ended after shipping — Cancelled
 * with a pre-cancellation status of shipped / out for delivery / delivery attempted…, or Flipkart's
 * invoice_eligible, or Returned — every unit finished, and the last cancellation at least 15 days old.
 */
export async function cancelledAfterShippingOrders(userId: string, now: Date, exclude: readonly string[] = []): Promise<string[]> {
  const user = new Types.ObjectId(userId);
  const cutoff = new Date(now.getTime() - CAPTURE_CANCELLED_AFTER_DAYS * 86_400_000);
  const seeds = (await Order.collection.distinct("order_id", {
    userId: user,
    status_key: ENDED_RE,
    ...noKey,
    $and: [{ $or: [{ order_status_pre_cancellation: /ship|deliver|out for/i }, { "api_extra.invoice_eligible": true }, { status_key: /^returned$/i }] }],
    cancelled_date: { $ne: null, $lte: cutoff },
    ...(exclude.length ? { order_id: { $nin: [...exclude] } } : {}),
  })) as string[];
  if (!seeds.length) return [];
  // Out: an order with a bundle on any unit or an invoices row, a unit still on its way, or a cancellation newer than the cutoff.
  const out = new Set<string>(
    (await Order.collection.distinct("order_id", {
      userId: user,
      order_id: { $in: seeds },
      $or: [{ invoice_s3_key: { $nin: [null, ""], $exists: true } }, { status_key: { $not: /^(delivered|cancelled|canceled|returned|refunded|failed)$/i } }, { cancelled_date: { $gt: cutoff } }],
    })) as string[]
  );
  for (const o of (await Invoice.collection.distinct("order_id", { userId: user, order_id: { $in: seeds } })) as string[]) out.add(o);
  return seeds.filter((o) => !out.has(o));
}

/**
 * Phase 1's orders: delivered with no bundle (as always), plus SO-6's when switched on — minus the failure memory's.
 * `captureCancelled`: the sweep's setting; without, the workspace's invoice settings are read.
 */
export async function captureCandidates(userId: string, opts: { now?: Date; cap: number; captureCancelled?: boolean }): Promise<{ orderIds: string[]; delivered: number; cancelled: number; heldBack: number }> {
  const now = opts.now || new Date();
  const held = await captureHeldBack(userId, now);
  const delivered = (await Order.distinct("order_id", { userId, status_key: /deliver/i, ...noKey, ...(held.length ? { order_id: { $nin: held } } : {}) })) as string[];
  const cancelled = (opts.captureCancelled ?? captureCancelledEnabled((await getInvoiceSettings(userId)).effective)) ? await cancelledAfterShippingOrders(userId, now, [...held, ...delivered]) : [];
  const orderIds = [...delivered, ...cancelled].slice(0, opts.cap);
  return { orderIds, delivered: delivered.length, cancelled: cancelled.length, heldBack: held.length };
}

/**
 * Failure memory: a phase-1 order whose capture failed on Flipkart's side — on every unit of the order. Not for "no
 * session" (the account's state, not the order's), nor for a failure of our own (`ours`: S3, Mongo — an outage must
 * not drop orders from capture).
 */
export async function rememberCaptureFailures(userId: string, orderIds: readonly string[], now = new Date()): Promise<void> {
  if (!orderIds.length) return;
  await Order.collection.updateMany({ userId: new Types.ObjectId(userId), order_id: { $in: [...orderIds] } }, { $inc: { invoice_capture_failures: 1 }, $set: { invoice_capture_failed_at: now } });
}

/** The phase-1 orders whose failures count against them (see rememberCaptureFailures). */
export function failuresToRemember(results: readonly OrderDownloadResult[]): string[] {
  return results.filter((x) => x.status === "failed" && !x.ours).map((x) => x.orderId);
}

/** A captured order's failure memory is cleared (only units that carry one are written). */
export async function forgetCaptureFailures(userId: string, orderIds: readonly string[]): Promise<void> {
  if (!orderIds.length) return;
  await Order.collection.updateMany(
    { userId: new Types.ObjectId(userId), order_id: { $in: [...orderIds] }, $or: [{ invoice_capture_failures: { $exists: true } }, { invoice_capture_failed_at: { $exists: true } }] },
    { $unset: { invoice_capture_failures: "", invoice_capture_failed_at: "" } }
  );
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
    /** The download every phase uses (default downloadInvoicesByOrderId) — scripts/invoiceRefetchCheck.ts injects one that calls nobody. */
    download?: typeof downloadInvoicesByOrderId;
  } = {}
): Promise<InvoiceSweepResult> {
  const log = opts.log || (() => undefined);
  const cap = opts.cap ?? config.invoiceMaxPerRun;
  const concurrency = opts.concurrency ?? 6;
  const now = new Date();
  const download = opts.download || downloadInvoicesByOrderId;
  const out: InvoiceSweepResult = {
    phase1: { candidates: 0, downloaded: 0, failed: 0 },
    phase2: { candidates: 0, recaptured: 0, settled: 0, failed: 0 },
    phase3: emptyRefetchResult(),
  };
  if (!invoiceStoreEnabled()) {
    log("warn", "invoice sweep skipped — INVOICE_S3_BUCKET is not set");
    return out;
  }

  // The invoice settings, once per sweep: a change saved in the dashboard applies from the next sweep.
  const settings = (await getInvoiceSettings(userId)).effective;
  const settleMs = invoiceSettleMs(settings);

  // Phase 1 — capture delivered orders with no invoice yet (and, switched on, orders cancelled after shipping).
  const missing = (await captureCandidates(userId, { now, cap, captureCancelled: captureCancelledEnabled(settings) })).orderIds;
  out.phase1.candidates = missing.length;
  if (missing.length && !opts.cancelled?.()) {
    await opts.onPhase?.("capture", missing);
    const r = await download({ userId, orderIds: missing, force: false, path: "sweep-capture", concurrency, cancelled: opts.cancelled, log, onStart: opts.onStart, onResult: opts.onResult });
    const downloaded = r.filter((x) => x.status === "downloaded").map((x) => x.orderId);
    out.phase1.downloaded = downloaded.length;
    out.phase1.failed = r.filter((x) => x.status === "failed").length;
    // Failure memory: not again within 24 h, never after 3 (today one failing order is otherwise retried every 3 h).
    // Only Flipkart-side failures count; a capture clears it.
    await rememberCaptureFailures(userId, failuresToRemember(r), now).catch((err) => log("warn", `capture failures not remembered: ${err instanceof Error ? err.message.split("\n")[0] : err}`));
    await forgetCaptureFailures(userId, downloaded).catch(() => undefined);
    // captured_at and settled are registerInvoice's now (T2): set on the first capture, settled by the delivery rule.
  }

  // Phase 2 — re-capture once, the settle window after capture #1, then settle.
  const due = (
    await Invoice.find({ userId, settled: false, captured_at: { $ne: null, $lte: new Date(now.getTime() - settleMs) } })
      .select("order_id")
      .limit(cap)
      .lean()
  ).map((d) => String(d.order_id));
  out.phase2.candidates = due.length;
  if (due.length && !opts.cancelled?.()) {
    await opts.onPhase?.("recapture", due);
    // skipIfUnchanged: files equal to the processed bundle's originals are not uploaded again (no Lambda run).
    const r = await download({ userId, orderIds: due, force: true, path: "sweep-recapture", skipIfUnchanged: true, concurrency, cancelled: opts.cancelled, log, onStart: opts.onStart, onResult: opts.onResult });
    const ok = r.filter((x) => x.status === "downloaded").map((x) => x.orderId);
    out.phase2.recaptured = ok.length;
    out.phase2.failed = r.filter((x) => x.status === "failed").length;
    // Settle by the delivery rule (T2): an order delivered more than the window ago is final; one still on its way
    // (fetched before delivery) is looked at again one window later, and settled anyway recaptureGiveUpMs() after
    // its row was made. Failures are left for the next sweep.
    if (ok.length) out.phase2.settled = await settleRecaptured(userId, ok, now, settings);
  }

  // Phase 3 — late re-downloads that are due (SO-5). `settled` is left as it is.
  if (!opts.cancelled?.()) {
    out.phase3 = await runRefetchPhase(userId, {
      cap,
      concurrency,
      download,
      cancelled: opts.cancelled,
      log,
      onPhase: (orderIds) => opts.onPhase?.("refetch", orderIds),
      onStart: opts.onStart,
      onResult: opts.onResult,
    });
  }

  const p3 = out.phase3 || emptyRefetchResult();
  log(
    "info",
    `invoice sweep: captured ${out.phase1.downloaded}/${out.phase1.candidates}, re-captured ${out.phase2.recaptured}/${out.phase2.candidates} (settled ${out.phase2.settled})${p3.candidates ? `, late re-downloads ${p3.refetched}/${p3.candidates - p3.alreadyNewer} (${p3.alreadyNewer} already newer${p3.gaveUp ? `, ${p3.gaveUp} gave up` : ""})` : ""}`
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
  const p3 = result.phase3 || emptyRefetchResult();
  const failed = result.phase1.failed + result.phase2.failed + p3.failed;
  if (!result.phase1.candidates && !result.phase2.candidates && !p3.candidates) return "nothing to do";
  const late = p3.candidates ? `, late re-downloads ${p3.refetched}${p3.candidates > p3.refetched ? `/${p3.candidates}` : ""}` : "";
  return `captured ${result.phase1.downloaded}/${result.phase1.candidates}, re-captured ${result.phase2.recaptured}${result.phase2.candidates > result.phase2.recaptured ? `/${result.phase2.candidates}` : ""}${late}${failed ? `, failed ${failed}` : ""}`;
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
    const p3 = result.phase3 || emptyRefetchResult();
    const success = result.phase1.downloaded + result.phase2.recaptured + p3.refetched + p3.alreadyNewer;
    const failed = result.phase1.failed + result.phase2.failed + p3.failed;
    const total = result.phase1.candidates + result.phase2.candidates + p3.candidates;
    const stopped = Boolean(recorder?.stopped()) || Boolean(superseded);
    await recorder?.finish();
    const counts = `Captured ${result.phase1.downloaded}/${result.phase1.candidates}, re-captured ${result.phase2.recaptured}/${result.phase2.candidates} (settled ${result.phase2.settled})${p3.candidates ? `, late re-downloads ${p3.refetched}/${p3.candidates} (${p3.alreadyNewer} already newer)` : ""}`;
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
