import type { Types } from "mongoose";
import { Order } from "../models/Order.js";
import type { RefetchReason } from "../models/Invoice.js";
import { OrderUnitEvent, type UnitEventType } from "../models/OrderUnitEvent.js";
import { stageOf } from "./orderLifecycle.js";

/**
 * Before/after of one unit around a refresh, and the events the difference
 * produces (models/OrderUnitEvent.ts). The update worker takes a snapshot,
 * lets the mapper rewrite the row, takes another, and calls recordUnitChanges.
 *
 * Address bookkeeping lives here too: the first hash seen becomes the
 * baseline (`address_first_seen_at`, an `address_seen` event); a different
 * hash later is an `address_changed` event, with the previous address pushed
 * onto `address_history` and `address_changed_at` set.
 *
 * And the ONE detection of the two late-document moments (data-quality X6) —
 * "an invoiced unit turned Cancelled / Returned" and "its refund completed" —
 * `lateDocumentMoments` below. recordUnitChanges hands them back as
 * `changes.lateDocuments`; the invoice side (invoiceRefetch.ts
 * noteLateDocuments, sub-step 12) only notes them on the bundle. The first
 * store of a bundle (invoiceRefetch.ts noteLateDocumentsForOrder) runs the
 * same function on what the units show. Quiet runs (the forced refresh)
 * note neither.
 */

// ── the late-document detection (X6) ─────────────────────────────────────────

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

/** Is this status one of the two endings the detection watches (Cancelled / Returned)? */
export function isEndedStatus(status: string | null | undefined): boolean {
  return ENDED_RE.test(String(status || "").trim());
}

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

const doneRefundStage = (u: Pick<RefetchUnitState, "tracking">) =>
  (u.tracking || []).find((t) => /refund/i.test(String(t?.status || "")) && String(t?.stage || "").toUpperCase() === "DONE");

/**
 * The refund status the rule reads: Flipkart's refund-card status, or "" when there is none. The page path writes a
 * bare marker `refund` (orderFetch.ts: "the page shows some refund row") on a unit with no stored status — that says
 * nothing about completion, so it counts as no status and the DONE Refund stage decides (as before sub-step 10).
 */
export function refundStatusOf(u: Pick<RefetchUnitState, "refund_status"> | null | undefined): string {
  const status = String(u?.refund_status || "").trim();
  return /^refund$/i.test(status) ? "" : status;
}

/**
 * Did the refund complete? The unit's refund status when it carries one (sub-step 9's mapper reads Flipkart's refund
 * card: Completed / Processed / Accepted), else a DONE Refund stage on its timeline (the page path and units without
 * a refund card). The status wins when there is one: in the saved traces 15 cancelled units show a DONE Refund stage
 * while the card still reads "Processed". The Mongo form of the same rule is orderLifecycle.ts refundNotCompletedClause.
 */
export function refundIsCompleted(u: Pick<RefetchUnitState, "refund_status" | "tracking">): boolean {
  const status = refundStatusOf(u);
  if (status) return /complet/i.test(status);
  return Boolean(doneRefundStage(u));
}

/**
 * When the refund completed, or null when it has not (refundIsCompleted): the DONE Refund stage's date, else now.
 * Flipkart dates some DONE stages ahead (the expected credit day) — a date after `now` is taken as now.
 */
export function refundCompletedAt(u: RefetchUnitState, now = new Date()): Date | null {
  if (!refundIsCompleted(u)) return null;
  const at = asDate(doneRefundStage(u)?.date) || now;
  return at.getTime() > now.getTime() ? now : at;
}

/**
 * Did the refund complete between `before` and `after`? Judged on the same evidence on both sides: when `after`
 * carries a refund status, `before`'s status (a unit stored without one — read before sub-step 9 — counts as not
 * completed, so its first refund-card read is the moment); else the DONE Refund stage. No `before`: the state alone.
 */
export function refundCompletedBetween(before: Partial<RefetchUnitState> | null, after: RefetchUnitState): boolean {
  if (!refundIsCompleted(after)) return false;
  if (!before) return true;
  if (refundStatusOf(after)) return !/complet/i.test(refundStatusOf(before));
  return !doneRefundStage(before);
}

export interface RefetchMoment {
  reason: RefetchReason;
  event_at: Date;
  cod: boolean;
}

/**
 * THE detection of the late-document moments a unit shows (pure). `before` is the unit as it was stored before the
 * read — given, an ending only counts when the unit was not ended already and a refund only when it was not
 * completed already (refundCompletedBetween); null, the state alone decides (the first store of a bundle, a unit read
 * the first time). The noting refuses a second (unit, reason) either way, and an entry a later download already
 * covers closes without a call.
 *   - "cancelled" / "returned": the unit is Cancelled / Returned now (not at "Cancellation requested");
 *   - "refund": the refund completed;
 *   - nothing for a unit that was never invoiced (cancelled before shipping).
 */
export function lateDocumentMoments(before: Partial<RefetchUnitState> | null, after: RefetchUnitState, now = new Date()): RefetchMoment[] {
  if (!isEndedStatus(after.status_key)) return [];
  if (!unitWasInvoiced(after)) return [];
  const cod = isCashOnDelivery(after);
  const out: RefetchMoment[] = [];
  const endedBefore = before ? isEndedStatus(before.status_key) : false;
  if (!endedBefore) {
    const reason: RefetchReason = /^returned$/i.test(after.status_key.trim()) ? "returned" : "cancelled";
    const at = asDate(after.cancelled_date);
    out.push({ reason, event_at: at && at.getTime() <= now.getTime() ? at : now, cod });
  }
  if (refundCompletedBetween(before, after)) {
    const at = refundCompletedAt(after, now);
    if (at) out.push({ reason: "refund", event_at: at, cod });
  }
  return out;
}

// ── snapshots ────────────────────────────────────────────────────────────────

export interface UnitSnapshot {
  orderId: string;
  email: string;
  productName: string;
  status_key: string;
  delivery_otp: string;
  shipping_address: Record<string, string> | null;
  shipping_address_hash: string;
  address_first_seen_at: Date | null;
  /** The refund card's amount / date / kind (sub-step 9), for the refund_completed event. */
  refund_amount?: string | null;
  refund_date?: Date | null;
  refund_kind?: string | null;
  /** What the late-document detection reads (REFETCH_UNIT_FIELDS). */
  late?: RefetchUnitState;
}

const ADDRESS_HISTORY_KEEP = 8;

export async function snapshotUnit(userId: Types.ObjectId | string, unitId: string): Promise<UnitSnapshot | null> {
  const row = await Order.findOne({ userId, unit_id: unitId })
    .select(`order_id platform_email product_name status_key delivery_otp shipping_address shipping_address_hash address_first_seen_at refund_amount refund_date refund_kind ${REFETCH_UNIT_FIELDS}`)
    .lean();
  if (!row) return null;
  return {
    orderId: String(row.order_id || ""),
    email: String(row.platform_email || ""),
    productName: String(row.product_name || ""),
    status_key: String(row.status_key || ""),
    delivery_otp: String(row.delivery_otp || ""),
    shipping_address: (row.shipping_address as Record<string, string> | null) || null,
    shipping_address_hash: String(row.shipping_address_hash || ""),
    address_first_seen_at: (row.address_first_seen_at as Date | null) || null,
    refund_amount: (row.refund_amount as string | null) ?? null,
    refund_date: (row.refund_date as Date | null) ?? null,
    refund_kind: (row.refund_kind as string | null) ?? null,
    late: refetchStateOf(row as unknown as Record<string, unknown>),
  };
}

/** A snapshot's detection state: the one it carries, else built from its status (a snapshot made by hand). */
function lateStateOf(unitId: string, snap: UnitSnapshot): RefetchUnitState {
  return snap.late || { unit_id: unitId, order_id: snap.orderId, status_key: snap.status_key, tracking: [] };
}

// ── events ───────────────────────────────────────────────────────────────────

/** Where the events of one recordUnitChanges call come from (T10), and whether the run is quiet (U10). */
export interface RecordOptions {
  /** The forced refresh: events inserted already processed (no alert), no late-document moment. */
  quiet?: boolean;
  /** The request's own source (OrderUpdateRequest.source). */
  requestSource?: string;
  /** What an alert names as the source; default "Update order details". */
  sourceLabel?: string;
}

/** The words an alert shows for an update request of this source (T10: the label follows the request, not the event type). */
export function updateSourceLabel(requestSource: string | null | undefined, params: Record<string, unknown> | null | undefined = {}): string {
  const p = params || {};
  if (p.purpose === "forced_refresh_2026") return "Forced refresh";
  switch (String(requestSource || "")) {
    case "schedule":
      return p.schedule_name === "update_active" || p.selection_method === "schedule_active" ? "Update order details (6-hour sweep)" : "Update order details (scheduled)";
    case "ofd":
      return "Update order details (out-for-delivery recheck)";
    case "cancel":
      return "Update order details (cancel)";
    case "system-order":
      return "Update order details (after an order)";
    case "system-retry":
      return "Update order details (retry)";
    default:
      return "Update order details";
  }
}

export interface UnitChanges {
  stageChanged: boolean;
  otpNew: boolean;
  addressSeen: boolean;
  addressChanged: boolean;
  /** The refund completed in this read (a refund_completed event; refundCompletedBetween). */
  refundCompleted: boolean;
  /** The late-document moments of this read (lateDocumentMoments) — empty in a quiet run. invoiceRefetch.ts notes them. */
  lateDocuments: RefetchMoment[];
  events: UnitEventType[];
}

export async function recordUnitChanges(
  userId: Types.ObjectId | string,
  unitId: string,
  before: UnitSnapshot | null,
  after: UnitSnapshot | null,
  requestId: string,
  opts: RecordOptions = {}
): Promise<UnitChanges> {
  const out: UnitChanges = { stageChanged: false, otpNew: false, addressSeen: false, addressChanged: false, refundCompleted: false, lateDocuments: [], events: [] };
  if (!after) return out;
  const now = new Date();
  const quiet = opts.quiet === true;
  const base = {
    userId,
    orderId: after.orderId,
    unitId,
    email: after.email,
    productName: after.productName,
    at: now,
    requestId,
    source: "update",
    requestSource: String(opts.requestSource || ""),
    sourceLabel: String(opts.sourceLabel || "Update order details"),
    // A quiet run's events are written already processed: the dashboard's drain claims `processed: { $ne: true }` only.
    processed: quiet,
    processedAt: quiet ? now : null,
  };
  const events: Array<Record<string, unknown>> = [];

  if (before && stageOf(before.status_key) !== stageOf(after.status_key)) {
    out.stageChanged = true;
    events.push({ ...base, type: "stage_changed", before: { status: before.status_key, stage: stageOf(before.status_key) }, after: { status: after.status_key, stage: stageOf(after.status_key) } });
  }
  // A code that differs from the stored one, not only the first: a save keeps the stored OTP when Flipkart shows none
  // (orderFetch.ts unitSetFor), so a second delivery attempt's code replaces a code, not a blank (U7).
  if (after.delivery_otp && after.delivery_otp !== before?.delivery_otp) {
    out.otpNew = true;
    events.push({ ...base, type: "otp_new", before: null, after: { otp: after.delivery_otp, status: after.status_key } });
  }

  // The refund (U10) and the late-document moments (X6): one detection, one condition — the event and the "refund"
  // moment both follow refundCompletedBetween, so a unit read the first time with its refund already complete (no
  // `before`) raises the event exactly when the detection returns the moment (spec U10: before?.refund_status).
  const lateAfter = lateStateOf(unitId, after);
  const lateBefore = before ? lateStateOf(unitId, before) : null;
  if (refundCompletedBetween(lateBefore, lateAfter)) {
    out.refundCompleted = true;
    const at = refundCompletedAt(lateAfter, now);
    events.push({
      ...base,
      type: "refund_completed",
      before: { refund_status: lateBefore?.refund_status ?? null },
      after: { amount: after.refund_amount ?? null, date: after.refund_date ?? at ?? null, kind: after.refund_kind ?? null },
    });
  }
  if (!quiet) out.lateDocuments = lateDocumentMoments(lateBefore, lateAfter, now);

  const prevHash = before?.shipping_address_hash || "";
  const nextHash = after.shipping_address_hash || "";
  if (nextHash) {
    if (!prevHash || !before?.address_first_seen_at) {
      // Baseline: the first address we record for this unit (a row written by
      // a Fetch already carries a hash but no first-seen stamp — same thing).
      if (!before?.address_first_seen_at) {
        await Order.updateOne({ userId, unit_id: unitId, address_first_seen_at: null }, { $set: { address_first_seen_at: now } });
        out.addressSeen = true;
        events.push({ ...base, type: "address_seen", before: null, after: { address: after.shipping_address, hash: nextHash } });
      }
      if (prevHash && prevHash !== nextHash) {
        out.addressChanged = true;
        await Order.updateOne(
          { userId, unit_id: unitId },
          { $set: { address_changed_at: now }, $push: { address_history: { $each: [{ hash: prevHash, address: before?.shipping_address || null, seenAt: before?.address_first_seen_at || now, until: now }], $slice: -ADDRESS_HISTORY_KEEP } } }
        );
        events.push({ ...base, type: "address_changed", before: { address: before?.shipping_address || null, hash: prevHash }, after: { address: after.shipping_address, hash: nextHash } });
      }
    } else if (prevHash !== nextHash) {
      out.addressChanged = true;
      await Order.updateOne(
        { userId, unit_id: unitId },
        { $set: { address_changed_at: now }, $push: { address_history: { $each: [{ hash: prevHash, address: before?.shipping_address || null, seenAt: before?.address_first_seen_at || now, until: now }], $slice: -ADDRESS_HISTORY_KEEP } } }
      );
      events.push({ ...base, type: "address_changed", before: { address: before?.shipping_address || null, hash: prevHash }, after: { address: after.shipping_address, hash: nextHash } });
    }
  }

  if (events.length) {
    // In a quiet run every event says so (after.backfill), so a later reader can tell the refresh's rows apart.
    if (quiet) for (const e of events) e.after = { ...((e.after as Record<string, unknown> | null) || {}), backfill: true };
    await OrderUnitEvent.insertMany(events, { ordered: false }).catch((err) => console.warn(`[unit-events] insert failed for ${unitId}: ${err instanceof Error ? err.message : err}`));
    out.events = events.map((e) => e.type as UnitEventType);
  }
  return out;
}
