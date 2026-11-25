import os from "node:os";
import { Types } from "mongoose";
import { config } from "../config.js";
import { Invoice, type IInvoiceRefetch, type RefetchReason, type RefetchResult } from "../models/Invoice.js";
import { Order } from "../models/Order.js";
import type { OrderDownloadResult, downloadInvoicesByOrderId } from "./invoiceOnDemand.js";

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
 * What is detected here is what the update worker sees today (the unit's state after a read); sub-step 10 brings
 * the one shared detection (X6, a `refund_completed` event, quiet runs) and calls the same noting function.
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

/** The unit fields the detection reads (order_details). */
export interface RefetchUnitState {
  unit_id: string;
  order_id: string;
  status_key: string;
  refund_status?: string | null;
  cancelled_date?: Date | string | null;
  order_status_pre_cancellation?: string | null;
  /** checkout jobs only (orderLinks.ts): "cod" / "prepaid". */
  payment_mode?: string | null;
  /** The order API's flag (orderApi.ts: Flipkart's payment mode reads "Cash On Delivery") — what fetched units carry. */
  cash_on_delivery?: boolean | null;
  /** Flipkart's own payment-mode text, as the order API answered it (api_extra.payment_mode). */
  api_payment_mode?: string | null;
  invoice_eligible?: boolean | null;
  tracking?: Array<{ status?: string; stage?: string; date?: string | null }>;
}

export const REFETCH_UNIT_FIELDS =
  "unit_id order_id status_key refund_status cancelled_date order_status_pre_cancellation payment_mode cash_on_delivery api_extra.payment_mode api_extra.invoice_eligible tracking.status tracking.stage tracking.date";

/** A row as order_details holds it → the detection's shape. */
export function refetchStateOf(row: Record<string, unknown>): RefetchUnitState {
  const extra = (row.api_extra as Record<string, unknown> | undefined) || {};
  return {
    unit_id: String(row.unit_id || ""),
    order_id: String(row.order_id || ""),
    status_key: String(row.status_key || ""),
    refund_status: (row.refund_status as string | null) ?? null,
    cancelled_date: (row.cancelled_date as Date | null) ?? null,
    order_status_pre_cancellation: (row.order_status_pre_cancellation as string | null) ?? null,
    payment_mode: (row.payment_mode as string | null) ?? null,
    cash_on_delivery: typeof row.cash_on_delivery === "boolean" ? row.cash_on_delivery : null,
    api_payment_mode: typeof extra.payment_mode === "string" ? extra.payment_mode : null,
    invoice_eligible: typeof extra.invoice_eligible === "boolean" ? extra.invoice_eligible : null,
    tracking: Array.isArray(row.tracking) ? (row.tracking as RefetchUnitState["tracking"]) : [],
  };
}

const ENDED_RE = /^(cancelled|canceled|returned)$/i;
/** Shipped, out for delivery, delivery attempted / unsuccessful / rescheduled, delivered: Flipkart had invoiced it. */
const INVOICED_BEFORE_RE = /ship|deliver|out for/i;

/** Was the unit invoiced before it ended? A return was delivered; a cancellation counts after shipping, or when Flipkart's own answer said the invoice was eligible. The 7 bundles whose cancelled units ended before shipping have no credit note to fetch. */
export function unitWasInvoiced(u: RefetchUnitState): boolean {
  if (/^returned$/i.test(u.status_key)) return true;
  if (u.invoice_eligible === true) return true;
  return INVOICED_BEFORE_RE.test(String(u.order_status_pre_cancellation || ""));
}

const COD_TEXT_RE = /^(cod|cash)/i;
/**
 * Is the unit cash on delivery? The order API's flag `cash_on_delivery` (what every fetched unit carries — measured
 * 2026-11-26: 799 bundled units true, while top-level payment_mode reads "cod" on 6, checkout jobs only), then
 * Flipkart's own text (api_extra.payment_mode "Cash On Delivery"), then a checkout job's payment_mode.
 */
export function isCashOnDelivery(u: Pick<RefetchUnitState, "payment_mode" | "cash_on_delivery" | "api_payment_mode"> | string | null | undefined): boolean {
  if (u === null || u === undefined || typeof u === "string") return COD_TEXT_RE.test(String(u || "").trim());
  if (u.cash_on_delivery === true) return true;
  if (/cash on delivery/i.test(String(u.api_payment_mode || ""))) return true;
  return COD_TEXT_RE.test(String(u.payment_mode || "").trim());
}

const asDate = (v: unknown): Date | null => {
  if (!v) return null;
  const d = v instanceof Date ? v : new Date(String(v));
  return Number.isNaN(d.getTime()) ? null : d;
};

/**
 * When the refund completed, or null: refund_status reads "complet…" (the mapper of sub-step 9), or a DONE Refund
 * stage is on the tracking (what is stored today: 3,215 DONE Refund stages). Flipkart dates some DONE stages
 * ahead (the expected credit day) — a date after `now` is taken as now.
 */
export function refundCompletedAt(u: RefetchUnitState, now = new Date()): Date | null {
  const stage = (u.tracking || []).find((t) => /refund/i.test(String(t?.status || "")) && String(t?.stage || "").toUpperCase() === "DONE");
  const completed = /complet/i.test(String(u.refund_status || ""));
  if (!stage && !completed) return null;
  const at = asDate(stage?.date) || now;
  return at.getTime() > now.getTime() ? now : at;
}

export interface RefetchMoment {
  reason: RefetchReason;
  event_at: Date;
  cod: boolean;
}

/**
 * The late-document moments a unit's state shows (pure). `before` is the unit as it was stored before the read —
 * given, an ending only counts when the unit was not ended already (the update worker's transition); null, the
 * state alone decides (the first store of a bundle, a unit read the first time). The noting refuses a second
 * (unit, reason) either way, and an entry a later download already covers closes without a call.
 *   - "cancelled" / "returned": the unit is Cancelled / Returned now (not at "Cancellation requested");
 *   - "refund": the refund completed (refundCompletedAt);
 *   - nothing for a unit that was never invoiced (cancelled before shipping).
 */
export function refetchEventsOf(before: Pick<RefetchUnitState, "status_key"> | null, after: RefetchUnitState, now = new Date()): RefetchMoment[] {
  if (!ENDED_RE.test(after.status_key.trim())) return [];
  if (!unitWasInvoiced(after)) return [];
  const cod = isCashOnDelivery(after);
  const out: RefetchMoment[] = [];
  const endedBefore = before ? ENDED_RE.test(String(before.status_key || "").trim()) : false;
  if (!endedBefore) {
    const reason: RefetchReason = /^returned$/i.test(after.status_key.trim()) ? "returned" : "cancelled";
    const at = asDate(after.cancelled_date);
    out.push({ reason, event_at: at && at.getTime() <= now.getTime() ? at : now, cod });
  }
  const refundAt = refundCompletedAt(after, now);
  if (refundAt) out.push({ reason: "refund", event_at: refundAt, cod });
  return out;
}

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
async function noteState(userId: unknown, before: Pick<RefetchUnitState, "status_key"> | null, state: RefetchUnitState, by: string, now: Date): Promise<RefetchReason[]> {
  const noted: RefetchReason[] = [];
  for (const moment of refetchEventsOf(before, state, now)) {
    if (await noteInvoiceRefetch({ userId, orderId: state.order_id, unitId: state.unit_id, moment, by, now })) noted.push(moment.reason);
  }
  return noted;
}

/**
 * (update worker) After a unit was read: note its late-document moments. Costs nothing for a unit that is not
 * Cancelled / Returned now (one read of the unit otherwise). Never throws.
 */
export async function noteLateDocumentsAfterRead(
  userId: unknown,
  unitId: string,
  statuses: { before?: string | null; after?: string | null },
  by: string,
  now = new Date()
): Promise<RefetchReason[]> {
  if (!ENDED_RE.test(String(statuses.after || "").trim())) return [];
  try {
    const row = await Order.findOne({ userId: oid(userId), unit_id: unitId }).select(REFETCH_UNIT_FIELDS).lean();
    if (!row) return [];
    const before = statuses.before === undefined ? null : { status_key: String(statuses.before || "") };
    return await noteState(userId, before, refetchStateOf(row as Record<string, unknown>), by, now);
  } catch (err) {
    console.warn(`[invoice-refetch] ${unitId}: late re-download not noted — ${firstLine(err)}`);
    return [];
  }
}

/**
 * (store helper) A bundle was stored for the first time: note what its order's units already show — a unit
 * cancelled, returned or refunded before the bundle existed. Never throws.
 */
export async function noteLateDocumentsForOrder(userId: unknown, orderId: string, by: string, now = new Date()): Promise<RefetchReason[]> {
  try {
    const rows = await Order.find({ userId: oid(userId), order_id: orderId, status_key: ENDED_RE }).select(REFETCH_UNIT_FIELDS).lean();
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
