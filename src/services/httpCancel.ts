import type { APIRequestContext } from "playwright";
import { DEFAULT_CANCEL_COMMENT, DEFAULT_CANCEL_REASON, itemIdOf, type CancelOutcome, type CancelUnitInput } from "../automation/FlipkartCancel.js";
import { fetchOrderDetails, OrderApiError, OrderApiSessionError, romeCall } from "./orderApi.js";

/**
 * Cancelling one order unit over Flipkart's own API, with the account's saved
 * session and no browser — the same two calls the cancel page makes for
 * itself (recorded by the Chrome flow, automation/FlipkartCancel.ts):
 *
 *   details  POST /api/4/page/fetch (CX_ORDER_DETAIL_PAGE)   → CANCEL eligibility
 *   data     GET  /api/1/self-serve/cancel/data/<orderId>?itemId&unitId
 *            allowOrderCancel, parentItems[].itemCancellable, cancellationReasons[],
 *            refundModes[], cancellationFeeCharges[], token
 *   cancel   POST /api/1/self-serve/cancel/<orderId>
 *            { unitId, itemId, reason, reasonIndex, comments, refundMode, token }
 *            → RESPONSE { success: true, refundTat }
 *
 * Verified 2026-10-30 (read side on fee78@bobbhai.in / OD439012907408834100:
 * allowOrderCancel true, 7 reasons, refund modes BACK_TO_SOURCE + COD, token).
 * "cancelled" is only ever reported off Flipkart's success flag. Anything the
 * API cannot answer (an order it does not carry yet, a session it refuses)
 * throws HttpCancelHandoff so the caller can give the unit to the Chrome flow.
 */

export class HttpCancelHandoff extends Error {}

interface CancelData {
  allowOrderCancel?: boolean;
  allowPartialCancellation?: boolean;
  cancellationMessage?: string | null;
  postDispatchCancelMessage?: string | null;
  cancellationGroupMessage?: string | null;
  errorMessage?: string | null;
  minCommentLength?: number;
  cancellationReasons?: Array<{ code?: string; description?: string }>;
  parentItems?: Array<{ orderItemUnitId?: string; unitId?: string; itemCancellable?: boolean; title?: string }>;
  refundModes?: Array<{ refundModeCode?: string; refundModeName?: string; tat?: string }>;
  cancellationFeeCharges?: Array<{ amountToCharge?: unknown[]; cancellationFeeRemarks?: string[]; decision?: string }>;
  token?: string;
}

export interface HttpCancelOptions {
  dryRun?: boolean;
  reason?: string;
  comments?: string;
  /** Units packed together cancel together; false refuses instead of accepting (default accept, as the Chrome flow does). */
  acceptConditions?: boolean;
  log?: (message: string) => void;
}

function feeNoteOf(data: CancelData): string {
  const charged = (data.cancellationFeeCharges ?? []).filter((c) => Array.isArray(c.amountToCharge) && c.amountToCharge.length);
  if (!charged.length) return "";
  return charged.map((c) => `${(c.cancellationFeeRemarks ?? []).join(" ")} ${JSON.stringify(c.amountToCharge).slice(0, 200)}`.trim()).join("; ");
}

export async function cancelUnitViaApi(ctx: APIRequestContext, input: CancelUnitInput, opts: HttpCancelOptions = {}): Promise<CancelOutcome> {
  const log = opts.log || (() => undefined);
  const orderId = String(input.orderId || "").trim().toUpperCase();
  const unitId = String(input.unitId || "").replace(/\D/g, "");
  const itemId = String(input.itemId || "").replace(/\D/g, "") || itemIdOf(unitId);
  if (!/^OD\d{12,}$/.test(orderId) || !unitId) throw new Error(`Bad order/unit id: ${orderId} / ${unitId}`);

  // 1. eligibility, from the order's own details (the Cancel button's source)
  let details: Awaited<ReturnType<typeof fetchOrderDetails>>;
  try {
    details = await fetchOrderDetails(ctx, orderId, unitId);
  } catch (err) {
    if (err instanceof OrderApiSessionError) throw err;
    throw new HttpCancelHandoff(`order details over the API: ${err instanceof Error ? err.message.split("\n")[0] : String(err)}`);
  }
  const unit = details.orderView.units?.[unitId] as { actionEligibilityResponses?: Record<string, { eligible?: boolean; eligibilityReason?: string | null; actionMessage?: string | null }> } | undefined;
  const cancel = unit?.actionEligibilityResponses?.CANCEL;
  if (cancel && cancel.eligible === false) {
    return { kind: "not_cancellable", message: `Flipkart: ${cancel.eligibilityReason || cancel.actionMessage || "not eligible for cancellation"}` };
  }

  // 2. the cancel page's data: reasons, refund modes, fees, token
  const { res, text } = await romeCall(ctx, (host) => ctx.get(`${host}/api/1/self-serve/cancel/data/${orderId}?itemId=${itemId}&unitId=${unitId}`));
  if (!res.ok()) throw new OrderApiError(`cancel/data for ${orderId}/${unitId} answered HTTP ${res.status()}`, res.status(), text.slice(0, 300));
  let parsed: { RESPONSE?: CancelData } & CancelData;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new OrderApiSessionError("cancel/data did not answer JSON — the session is probably not logged in");
  }
  const data: CancelData = parsed.RESPONSE ?? parsed;
  const mine = (data.parentItems ?? []).find((p) => String(p.orderItemUnitId || p.unitId || "") === unitId);
  if (data.allowOrderCancel === false || (mine && mine.itemCancellable === false)) {
    const why = data.cancellationMessage || data.postDispatchCancelMessage || data.errorMessage || "Flipkart does not allow cancelling this unit";
    return { kind: "not_cancellable", message: `Flipkart: ${String(why).slice(0, 300)}` };
  }
  const feeNote = feeNoteOf(data);
  let conditionsNote = "";
  const others = (data.parentItems ?? []).filter((p) => String(p.orderItemUnitId || p.unitId || "") !== unitId);
  if (data.allowPartialCancellation === false && others.length) {
    conditionsNote = String(data.cancellationGroupMessage || `cancelling this unit cancels the ${others.length} other item(s) packed with it`);
    if (opts.acceptConditions === false) return { kind: "not_cancellable", message: `Flipkart: ${conditionsNote}` };
    log(`[cancel-api] ${orderId}/${unitId}: conditions — ${conditionsNote}; accepting`);
  }

  // 3. reason + comment (both required), refund to the original mode
  const reasons = data.cancellationReasons ?? [];
  const wanted = opts.reason || DEFAULT_CANCEL_REASON;
  const reasonIndex = Math.max(
    0,
    reasons.findIndex((r) => r.code === wanted) >= 0 ? reasons.findIndex((r) => r.code === wanted) : reasons.findIndex((r) => /mind|not listed/i.test(`${r.code} ${r.description}`))
  );
  const reason = reasons[reasonIndex]?.code || wanted;
  const minLen = Math.max(5, Number(data.minCommentLength) || 5);
  const comments = (opts.comments || DEFAULT_CANCEL_COMMENT).padEnd(minLen, ".");
  const modes = data.refundModes ?? [];
  const refundMode = modes.find((m) => m.refundModeCode === "BACK_TO_SOURCE")?.refundModeCode || modes[0]?.refundModeCode || "BACK_TO_SOURCE";
  if (!data.token) throw new HttpCancelHandoff("cancel/data carried no token");
  log(`[cancel-api] ${orderId}/${unitId}: reason ${reason}, refund ${refundMode}${feeNote ? `, fee: ${feeNote}` : ""}`);
  if (opts.dryRun) {
    return { kind: "dry_run", message: `Stopped before the cancel call (dry run)${conditionsNote ? ` · conditions: ${conditionsNote}` : ""}`, feeNote };
  }

  // 4. the one irreversible call
  const body = { unitId, itemId, reason, reasonIndex, comments, refundMode, token: data.token };
  const post = await romeCall(ctx, (host) => ctx.post(`${host}/api/1/self-serve/cancel/${orderId}`, { data: body }));
  let answer: Record<string, unknown> | null = null;
  try {
    const j = JSON.parse(post.text) as Record<string, unknown>;
    answer = (j.RESPONSE as Record<string, unknown>) ?? j;
  } catch {
    answer = null;
  }
  if (answer?.success === true) {
    const refundTat = String(answer.refundTat || "");
    log(`[cancel-api] ${orderId}/${unitId}: cancelled — Flipkart answered success${refundTat ? ` (${refundTat})` : ""}`);
    return { kind: "cancelled", message: `Cancellation requested on Flipkart (API)${conditionsNote ? ` · ${conditionsNote}` : ""}`, refundTat, feeNote };
  }
  if (!post.res.ok()) throw new OrderApiError(`cancel ${orderId}/${unitId} answered HTTP ${post.res.status()}`, post.res.status(), post.text.slice(0, 300));
  throw new Error(`Flipkart refused the cancellation: ${String(answer?.errorMessage || answer?.ERROR_MESSAGE || post.text.slice(0, 300))}`);
}
