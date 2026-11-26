import os from "node:os";
import { Types } from "mongoose";
import { config } from "../config.js";
import { Invoice, type IInvoiceRefetch, type RefetchReason, type RefetchResult } from "../models/Invoice.js";
import { Order } from "../models/Order.js";
import type { OrderDownloadResult, downloadInvoicesByOrderId } from "./invoiceOnDemand.js";
import { isEndedStatus, lateDocumentMoments, REFETCH_UNIT_FIELDS, refetchStateOf, type RefetchMoment, type RefetchUnitState } from "./orderUnitEvents.js";

/**
 * Late re-downloads — data-quality sub-step 12, SO-5 (owner decision 4).
 *
 * Flipkart issues more documents after an order is invoiced: product, Protect Promise and card-fee credit notes
 * dated the day of a cancellation, and a freight credit note (with a new freight invoice) 3–14 days after it,
 * median 8, 4–8 days after the refund. A bundle downloaded before those exist is never looked at again (phase 2
 * of the sweep takes `settled: false` rows only). So:
 *
 *   NOTE   when an invoiced unit of a bundled order turns Cancelled / Returned, or its refund completes, an entry
 *          {unit_id, reason, event_at, due_at = event_at + delay, attempts…} is pushed onto the order's own
 *          `invoices` row. The filter refuses a second (unit_id, reason), so the push is atomic in shared Mongo:
 *          both Macs, a repeat read, or the first store of the bundle note an event once.
 *   FETCH  the invoice sweep's third phase, "refetch" (runRefetchPhase): rows with an open entry due by now are
 *          claimed per order (20 min); entries a later download already covers close as `already_newer` without a
 *          call; the rest are downloaded once (force, path `sweep-refetch`, skipIfUnchanged — the same files as the
 *          processed bundle are not re-uploaded); a success closes every due entry of the order, a failure counts
 *          an attempt and the entry gives up at 3. `settled` is left as it is.
 *
 * The delays are owner question 4, still open — the plan's recommendation is the default, each an .env setting:
 *   INVOICE_REFETCH_CANCEL_DELAY_H  24   the cancellation (or return) look, 1 day after it — product / fee credit notes
 *   INVOICE_REFETCH_REFUND_DELAY_H  192  the refund look, 8 days after the refund — the freight credit note
 *   INVOICE_REFETCH_COD_DELAY_H     192  cash on delivery (no refund follows): the cancellation look waits 8 days
 *
 * The moments come from the ONE detection in orderUnitEvents.ts (sub-step 10, X6): the update worker's
 * recordUnitChanges hands them over (`changes.lateDocuments`, none in a quiet run) and noteLateDocuments notes them;
 * the first store of a bundle runs the same detection on its units (noteLateDocumentsForOrder).
 */

const DAY_H = 24;
const CLAIM_MS = 20 * 60_000;
export const REFETCH_MAX_ATTEMPTS = 3;

const hours = (name: string, fallback: number) => {
  const raw = String(process.env[name] ?? "").trim();
  const n = raw === "" ? NaN : Number(raw);
  return Number.isFinite(n) && n >= 0 ? n : fallback;
};

/** The three delays, read at call time (hours). */
export function refetchDelaysH(): { cancel: number; refund: number; cod: number } {
  return {
    cancel: hours("INVOICE_REFETCH_CANCEL_DELAY_H", 1 * DAY_H),
    refund: hours("INVOICE_REFETCH_REFUND_DELAY_H", 8 * DAY_H),
    cod: hours("INVOICE_REFETCH_COD_DELAY_H", 8 * DAY_H),
  };
}

/** due_at of an entry: the event plus the reason's delay (a cash-on-delivery cancellation / return waits the COD delay). */
export function refetchDueAt(reason: RefetchReason, eventAt: Date, cod: boolean): Date {
  const d = refetchDelaysH();
  const h = reason === "refund" ? d.refund : cod ? d.cod : d.cancel;
  return new Date(eventAt.getTime() + h * 3_600_000);
}

// The detection itself — the late-document moments a unit shows, and the helpers it is made of — lives in
// orderUnitEvents.ts (data-quality X6: ONE detection, which recordUnitChanges runs on every read of the update worker
// and which the first store of a bundle runs below). Re-exported here under the names this module has always had.
export {
  REFETCH_UNIT_FIELDS,
  isCashOnDelivery,
  lateDocumentMoments as refetchEventsOf,
  refetchStateOf,
  refundCompletedAt,
  unitWasInvoiced,
  type RefetchMoment,
  type RefetchUnitState,
} from "./orderUnitEvents.js";

const oid = (id: unknown) => (id instanceof Types.ObjectId ? id : new Types.ObjectId(String(id)));
const firstLine = (err: unknown) => (err instanceof Error ? err.message : String(err)).split("\n")[0].slice(0, 200);

/**
 * Push one entry onto the order's `invoices` row — unless the row already holds this (unit, reason), or the order
 * has no bundle (nothing to re-download; the first store notes it then). Atomic: the filter and the push are one
 * document update. Returns whether an entry was added.
 */
export async function noteInvoiceRefetch(input: { userId: unknown; orderId: string; unitId: string; moment: RefetchMoment; by: string; now?: Date }): Promise<boolean> {
  const now = input.now || new Date();
  const entry: IInvoiceRefetch = {
    unit_id: input.unitId,
    reason: input.moment.reason,
    event_at: input.moment.event_at,
    due_at: refetchDueAt(input.moment.reason, input.moment.event_at, input.moment.cod),
    noted_at: now,
    by: input.by,
    cod: input.moment.cod,
    attempts: 0,
    last_error: "",
    done_at: null,
    result: "",
  };
  const res = await Invoice.collection.updateOne(
    { userId: oid(input.userId), order_id: String(input.orderId).toUpperCase(), refetch: { $not: { $elemMatch: { unit_id: input.unitId, reason: input.moment.reason } } } },
    { $push: { refetch: entry } } as never
  );
  return res.modifiedCount === 1;
}

/** Note every moment of one unit state; never throws (a unit's read must not fail on this). Returns the reasons noted. */
async function noteState(userId: unknown, before: Partial<RefetchUnitState> | null, state: RefetchUnitState, by: string, now: Date): Promise<RefetchReason[]> {
  return noteLateDocuments(userId, { orderId: state.order_id, unitId: state.unit_id }, lateDocumentMoments(before, state, now), by, now);
}

/**
 * (update worker) Note the late-document moments recordUnitChanges detected on a read (`changes.lateDocuments`, the one
 * detection — a quiet run hands over none). Costs nothing when there is none. Never throws. Returns the reasons noted.
 */
export async function noteLateDocuments(
  userId: unknown,
  unit: { orderId: string; unitId: string },
  moments: RefetchMoment[],
  by: string,
  now = new Date()
): Promise<RefetchReason[]> {
  const noted: RefetchReason[] = [];
  for (const moment of moments) {
    try {
      if (await noteInvoiceRefetch({ userId, orderId: unit.orderId, unitId: unit.unitId, moment, by, now })) noted.push(moment.reason);
    } catch (err) {
      console.warn(`[invoice-refetch] ${unit.unitId}: late re-download (${moment.reason}) not noted — ${firstLine(err)}`);
    }
  }
  return noted;
}

/**
 * (store helper) A bundle was stored for the first time: note what its order's units already show — a unit
 * cancelled, returned or refunded before the bundle existed. Never throws.
 */
export async function noteLateDocumentsForOrder(userId: unknown, orderId: string, by: string, now = new Date()): Promise<RefetchReason[]> {
  try {
    const rows = (await Order.find({ userId: oid(userId), order_id: orderId, status_key: /^(cancelled|canceled|returned)$/i }).select(REFETCH_UNIT_FIELDS).lean()).filter((r) => isEndedStatus(r.status_key));
    const noted: RefetchReason[] = [];
    for (const row of rows) noted.push(...(await noteState(userId, null, refetchStateOf(row as Record<string, unknown>), by, now)));
    return noted;
  } catch (err) {
    console.warn(`[invoice-refetch] ${orderId}: late re-downloads not noted at the first store — ${firstLine(err)}`);
    return [];
  }
}

// ── the sweep's third phase ─────────────────────────────────────────────────

export interface RefetchPhaseResult {
  /** Orders with an entry due that this run claimed. */
  candidates: number;
  /** …downloaded again (new files, or the same — `unchanged` of them). */
  refetched: number;
  unchanged: number;
  /** Orders whose due entries were all covered by a later download: closed with no call. */
  alreadyNewer: number;
  failed: number;
  /** Entries closed after REFETCH_MAX_ATTEMPTS failed attempts. */
  gaveUp: number;
  /** Orders with an entry due that another sweep holds. */
  heldElsewhere: number;
}

export const emptyRefetchResult = (): RefetchPhaseResult => ({ candidates: 0, refetched: 0, unchanged: 0, alreadyNewer: 0, failed: 0, gaveUp: 0, heldElsewhere: 0 });

type Download = typeof downloadInvoicesByOrderId;

export interface RefetchPhaseOptions {
  cap: number;
  concurrency: number;
  download: Download;
  now?: Date;
  /** Who holds the claim (default this machine's host id). */
  claimBy?: string;
  cancelled?: () => boolean;
  log?: (level: "info" | "warn", msg: string) => void;
  onPhase?: (orderIds: string[]) => void | Promise<void>;
  onStart?: (orderId: string) => void | Promise<void>;
  onResult?: (result: OrderDownloadResult) => void | Promise<void>;
}

const openDue = (now: Date) => ({ done_at: null, due_at: { $lte: now } });
const unclaimed = (now: Date) => ({ $or: [{ refetch_claim_until: null }, { refetch_claim_until: { $lte: now } }] });

/** The newest download this row is known to have had: last_download_at (sub-step 12 on), else the re-capture / capture time. */
function newestDownload(row: { last_download_at?: Date | null; captured_at?: Date | null; createdAt?: Date | null }): number {
  return Math.max(...[row.last_download_at, row.captured_at, row.createdAt].map((d) => (d ? new Date(d).getTime() : 0)));
}

/**
 * Phase 3 of the invoice sweep. Claims per order, closes what a later download covers, downloads the rest once,
 * closes or counts attempts, gives the claims back. `settled` is never touched.
 */
export async function runRefetchPhase(userId: string, opts: RefetchPhaseOptions): Promise<RefetchPhaseResult> {
  const out = emptyRefetchResult();
  const now = opts.now || new Date();
  const by = opts.claimBy || config.hostId || os.hostname();
  const log = opts.log || (() => undefined);
  const user = oid(userId);
  const col = Invoice.collection;

  const rows = await col
    .find({ userId: user, refetch: { $elemMatch: openDue(now) } }, { projection: { order_id: 1, refetch: 1, last_download_at: 1, captured_at: 1, createdAt: 1, refetch_claim_until: 1 } })
    .sort({ _id: 1 })
    .limit(opts.cap)
    .toArray();
  if (!rows.length) return out;

  // Claim per order: one sweep (one machine) works an order at a time.
  const claimed: typeof rows = [];
  const until = new Date(now.getTime() + CLAIM_MS);
  for (const row of rows) {
    if (opts.cancelled?.()) break;
    const res = await col.updateOne({ _id: row._id, ...unclaimed(now) }, { $set: { refetch_claim_until: until, refetch_claim_by: by } });
    if (res.modifiedCount) claimed.push(row);
    else out.heldElsewhere++;
  }
  out.candidates = claimed.length;
  const release = (ids: unknown[]) => (ids.length ? col.updateMany({ _id: { $in: ids as never[] }, refetch_claim_by: by }, { $set: { refetch_claim_until: null, refetch_claim_by: "" } }) : Promise.resolve(null));

  try {
    // "Already newer": a due entry whose moment a later download covers closes without a call.
    const toFetch = new Map<string, unknown>();
    for (const row of claimed) {
      const newest = newestDownload(row as never);
      const due = ((row.refetch as IInvoiceRefetch[]) || []).filter((e) => !e.done_at && new Date(e.due_at).getTime() <= now.getTime());
      const covered = due.filter((e) => new Date(e.due_at).getTime() <= newest);
      if (covered.length) {
        await col.updateOne(
          { _id: row._id },
          { $set: { "refetch.$[e].done_at": now, "refetch.$[e].result": "already_newer" satisfies RefetchResult } },
          { arrayFilters: [{ "e.done_at": null, "e.due_at": { $lte: new Date(newest) } }] }
        );
      }
      if (covered.length === due.length) out.alreadyNewer++;
      else toFetch.set(String(row.order_id), row._id);
    }

    const orderIds = [...toFetch.keys()];
    if (orderIds.length && !opts.cancelled?.()) {
      await opts.onPhase?.(orderIds);
      const results = await opts.download({
        userId,
        orderIds,
        force: true,
        path: "sweep-refetch",
        skipIfUnchanged: true,
        concurrency: opts.concurrency,
        cancelled: opts.cancelled,
        log,
        onStart: opts.onStart,
        onResult: opts.onResult,
      });
      const reached = new Set<string>();
      for (const r of results) {
        const id = toFetch.get(r.orderId);
        if (!id) continue;
        if (r.status === "downloaded") {
          reached.add(r.orderId);
          out.refetched++;
          if (r.unchanged) out.unchanged++;
          const result: RefetchResult = r.unchanged ? "unchanged" : "downloaded";
          await col.updateOne({ _id: id as never }, { $set: { "refetch.$[e].done_at": new Date(), "refetch.$[e].result": result } }, { arrayFilters: [{ "e.done_at": null, "e.due_at": { $lte: now } }] });
          continue;
        }
        // An order the run never reached (it was stopped) is neither a success nor an attempt.
        if (r.status === "unknown_order" && opts.cancelled?.()) continue;
        reached.add(r.orderId);
        out.failed++;
        const why = `${r.status}${r.error ? `: ${r.error}` : ""}`.slice(0, 200);
        await col.updateOne({ _id: id as never }, { $inc: { "refetch.$[e].attempts": 1 }, $set: { "refetch.$[e].last_error": why } }, { arrayFilters: [{ "e.done_at": null, "e.due_at": { $lte: now } }] });
        const gave = await col.findOne({ _id: id as never }, { projection: { refetch: 1 } });
        const giving = ((gave?.refetch as IInvoiceRefetch[]) || []).filter((e) => !e.done_at && (e.attempts || 0) >= REFETCH_MAX_ATTEMPTS).length;
        if (giving) {
          await col.updateOne({ _id: id as never }, { $set: { "refetch.$[e].done_at": new Date(), "refetch.$[e].result": "gave_up" satisfies RefetchResult } }, { arrayFilters: [{ "e.done_at": null, "e.attempts": { $gte: REFETCH_MAX_ATTEMPTS } }] });
          out.gaveUp += giving;
          log("warn", `${r.orderId}: late re-download gave up after ${REFETCH_MAX_ATTEMPTS} attempts (${why})`);
        }
      }
    }
  } finally {
    await release(claimed.map((r) => r._id)).catch((err) => console.warn(`[invoice-refetch] claims not given back (they expire in 20 min): ${firstLine(err)}`));
  }
  return out;
}
