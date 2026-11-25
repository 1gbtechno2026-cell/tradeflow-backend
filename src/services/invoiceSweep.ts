import { Order } from "../models/Order.js";
import { Invoice } from "../models/Invoice.js";
import { OrderJobRun } from "../models/OrderJobRun.js";
import { config } from "../config.js";
import { invoiceStoreEnabled } from "./invoiceCapture.js";
import { downloadInvoicesByOrderId, type OrderDownloadResult } from "./invoiceOnDemand.js";
import { startRun, progressRun, finishRun } from "./orderJobRuns.js";
import { recordSweepAsRequest, type SweepPhase } from "./invoiceSweepRequest.js";
import { CHAIN_INVOICE_SWEEP } from "./requestChains.js";

/** Scheduled invoice sweep on/off (INVOICE_SCHEDULE=0 to disable). */
export const INVOICE_SCHEDULE_ENABLED = String(process.env.INVOICE_SCHEDULE ?? "1") !== "0";
/** How often the worker runs the sweep (ms, default 3 h). */
export const INVOICE_SWEEP_MS = Math.max(30 * 60 * 1000, Number(process.env.INVOICE_SWEEP_MS) || 3 * 60 * 60 * 1000);

/**
 * Automatic, time-settled invoice capture (phase 7c).
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

/**
 * The sweep as a tracked run: records an `invoices` OrderJobRun (so the Sync
 * panel / running indicator shows it), single-flights against a run already in
 * progress, and reports counts. This is what the worker schedule and the manual
 * "Invoices" button invoke.
 *
 * Since step 5 a run that has something to download is also a REQUEST of the
 * log — the reference's nightly "Invoice fetch", source `system-cron` (`user`
 * for the button): one order_invoice_requests document, a task per order,
 * written as the sweep goes (services/invoiceSweepRequest.ts). The sweep
 * itself is untouched: the same schedule, the same single-flight, the same
 * cap per phase, the same downloads, the same captured_at / settled
 * bookkeeping. Batch requests → Cancel on such a request stops the sweep
 * between two orders. CHAIN_INVOICE_SWEEP=0 leaves the request out.
 */
export async function runTrackedInvoiceSweep(
  userId: string,
  input: { trigger?: "user" | "system-cron"; by?: string } = {}
): Promise<{ skipped: boolean; reason?: string; result?: InvoiceSweepResult }> {
  const open = await OrderJobRun.findOne({ userId, type: "invoices", status: "running" }).select("_id").lean();
  if (open) return { skipped: true, reason: "an invoice run is already in progress" };

  const ledgerRunId = await startRun(userId, "invoices", {
    trigger: input.trigger || "system-cron",
    by: input.by || "schedule",
    message: "Invoice sweep running…",
    meta: { request: "invoice_fetch", platform: "FLIPKART", sweep: true, params: { force: false, cap_per_phase: config.invoiceMaxPerRun } },
  });
  const recorder = CHAIN_INVOICE_SWEEP ? recordSweepAsRequest(userId, { source: input.trigger === "user" ? "user" : "system-cron", by: input.trigger === "user" ? input.by || "" : "", ledgerRunId, capPerPhase: config.invoiceMaxPerRun }) : null;
  try {
    const result = await runInvoiceSweep(userId, {
      log: (lvl, m) => {
        if (lvl === "info") void progressRun(userId, "invoices", { message: m });
      },
      ...(recorder ? { cancelled: recorder.stopped, onPhase: recorder.onPhase, onStart: recorder.onStart, onResult: recorder.onResult } : {}),
    });
    const success = result.phase1.downloaded + result.phase2.recaptured;
    const failed = result.phase1.failed + result.phase2.failed;
    const total = result.phase1.candidates + result.phase2.candidates;
    const stopped = Boolean(recorder?.stopped());
    await recorder?.finish();
    await finishRun(userId, "invoices", {
      status: stopped ? "stopped" : "completed",
      total,
      processed: total,
      success,
      failed,
      cancelled: 0,
      message: `Captured ${result.phase1.downloaded}/${result.phase1.candidates}, re-captured ${result.phase2.recaptured}/${result.phase2.candidates} (settled ${result.phase2.settled})`,
    });
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
    throw err;
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
