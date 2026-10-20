import type { Page, Response } from "playwright";
import { flipkartLoginUrl, sleep } from "../services/browser.js";
import type { LogLevel } from "../types.js";

export type CancelLogger = (level: LogLevel, message: string) => void;

/**
 * Cancelling one order unit on Flipkart (desktop site).
 *
 * The flow, recorded on OD438890447913742100 (2026-10-15):
 *
 *   order_details → Cancel → nudge "Do you still want to cancel?" → Cancel Order
 *   → /orders/cancelOrder?grocery=false&itemId=…&orderId=…&unitId=…
 *       GET  /api/1/self-serve/cancel/data/<orderId>?itemId&unitId
 *            allowOrderCancel, parentItems[].itemCancellable, cancellationReasons[],
 *            refundModes[], cancellationFeeCharges[], token
 *       <select name="reasonList"> + <textarea> (both required, comment ≥ 5 chars)
 *       CONTINUE → "REFUND MODES" (Original Payment Mode) → REQUEST CANCELLATION
 *       POST /api/1/self-serve/cancel/<orderId>
 *            {unitId, itemId, reason, reasonIndex, comments, refundMode, token}
 *            → RESPONSE {success: true, refundTat: "Refund with in 5-7 days"}
 *   → back on order_details, "Cancelled, Today".
 *
 * The cancel page is opened directly: the order and unit ids are on our Order
 * row, and the nudge in front of it is a retention prompt, not a step the
 * cancellation needs. Eligibility is read from Flipkart's own cancel/data
 * answer, never guessed from our status column; and "cancelled" is only ever
 * reported off the cancel POST's success flag — the same rule as a payment,
 * which is only "placed" off Flipkart's confirmation.
 */

export interface CancelUnitInput {
  orderId: string;
  unitId: string;
  /** Optional: unitId minus its last three digits when absent. */
  itemId?: string;
}

export interface CancelOptions {
  /** Walk every screen and stop before REQUEST CANCELLATION. */
  dryRun?: boolean;
  /** Flipkart reason code. There is no "I have changed my mind" option; its
   *  value `mind_changed` ("My reasons are not listed here") is the one. */
  reason?: string;
  comments?: string;
  /** Flipkart's "CANCELLATION CONDITIONS" step (units packed together: cancelling one cancels the
   *  others). Default true: Accept & Continue, and the note is carried into the result. */
  acceptConditions?: boolean;
  /** Called at each screen, for screenshots. */
  onStep?: (name: string) => Promise<void>;
}

export type CancelOutcome =
  | { kind: "cancelled"; message: string; refundTat: string; feeNote: string }
  | { kind: "not_cancellable"; message: string }
  | { kind: "dry_run"; message: string; feeNote: string };

export const DEFAULT_CANCEL_REASON = "mind_changed";
export const DEFAULT_CANCEL_COMMENT = "I have changed my mind";

interface CancelData {
  allowOrderCancel?: boolean;
  allowPartialCancellation?: boolean;
  cancellationMessage?: string | null;
  postDispatchCancelMessage?: string | null;
  errorMessage?: string | null;
  cancellationReasons?: Array<{ code?: string; description?: string }>;
  parentItems?: Array<{ orderItemUnitId?: string; itemCancellable?: boolean; title?: string }>;
  cancellationFeeCharges?: Array<{
    amountToCharge?: unknown[];
    cancellationFeeRemarks?: string[];
    decision?: string;
  }>;
}

/** Flipkart's unit id is its item id with a 3-digit unit suffix (…100 → …100000, …100001). */
export function itemIdOf(unitId: string): string {
  const digits = String(unitId || "").replace(/\D/g, "");
  return digits.length > 3 ? digits.slice(0, -3) : "";
}

export function cancelPageUrl(input: CancelUnitInput): string {
  const q = new URLSearchParams({
    grocery: "false",
    itemId: input.itemId || itemIdOf(input.unitId),
    orderId: input.orderId,
    unitId: input.unitId,
  });
  return `https://www.flipkart.com/orders/cancelOrder?${q.toString()}`;
}

async function jsonOf(res: Response): Promise<Record<string, unknown> | null> {
  try {
    const body = (await res.json()) as Record<string, unknown>;
    return (body?.RESPONSE as Record<string, unknown>) ?? body;
  } catch {
    return null;
  }
}

/** A fee Flipkart says it will charge, in its own words — for the record, not a stop. */
function feeNoteOf(data: CancelData): string {
  const charged = (data.cancellationFeeCharges ?? []).filter((c) => Array.isArray(c.amountToCharge) && c.amountToCharge.length);
  if (!charged.length) return "";
  return charged
    .map((c) => `${(c.cancellationFeeRemarks ?? []).join(" ")} ${JSON.stringify(c.amountToCharge).slice(0, 200)}`.trim())
    .join("; ");
}

export class FlipkartCancel {
  constructor(
    private readonly page: Page,
    private readonly log: CancelLogger
  ) {}

  async cancelUnit(input: CancelUnitInput, opts: CancelOptions = {}): Promise<CancelOutcome> {
    const step = async (name: string) => {
      await opts.onStep?.(name).catch(() => undefined);
    };
    const orderId = String(input.orderId || "").trim().toUpperCase();
    const unitId = String(input.unitId || "").replace(/\D/g, "");
    if (!/^OD\d{12,}$/.test(orderId) || !unitId) throw new Error(`Bad order/unit id: ${orderId} / ${unitId}`);

    // ---- eligibility, from the order page's own data ------------------------
    // The cancel page is no help here: for a unit that cannot be cancelled it
    // renders "Something's not right! Please try again". The order page's data
    // (CX_ORDER_DETAIL_PAGE, requested per unit) carries the CANCEL action
    // Flipkart draws its Cancel button from: eligible true/false plus a reason.
    const eligibility = await this.readEligibility(orderId, unitId, input.itemId);
    await step("order-page");
    if (eligibility && !eligibility.eligible) {
      return { kind: "not_cancellable", message: `Flipkart: ${eligibility.reason || "not eligible for cancellation"}` };
    }

    // ---- the cancel page, and Flipkart's verdict on it ---------------------
    const dataPromise = this.page
      .waitForResponse((r) => /\/self-serve\/cancel\/data\//i.test(r.url()) && r.request().method() === "GET", {
        timeout: 30_000,
      })
      .catch(() => null);
    await this.page.goto(cancelPageUrl({ orderId, unitId, itemId: input.itemId }), {
      waitUntil: "domcontentloaded",
      timeout: 45_000,
    });
    if (flipkartLoginUrl(this.page.url())) throw new Error("SESSION_EXPIRED: Flipkart showed the login page");
    const dataRes = await dataPromise;
    await sleep(1500);
    await step("cancel-page");

    const data = (dataRes ? await jsonOf(dataRes) : null) as CancelData | null;
    if (!data) {
      // No eligibility answer at all: Flipkart sent the page somewhere else
      // (order page, error page). Its text is the only explanation there is.
      const text = await this.pageText();
      if (/not eligible for cancellation|cannot be cancelled|already (?:been )?cancelled|cancelled/i.test(text)) {
        return { kind: "not_cancellable", message: this.firstLine(text, /cancel/i) || "Flipkart does not offer cancellation for this unit" };
      }
      throw new Error(`Flipkart's cancel page gave no eligibility data (at ${this.page.url().split("?")[0]})`);
    }

    const unit = (data.parentItems ?? []).find((p) => String(p.orderItemUnitId || "") === unitId);
    const why = String(data.errorMessage || data.cancellationMessage || data.postDispatchCancelMessage || "").trim();
    if (data.allowOrderCancel === false || unit?.itemCancellable === false) {
      return { kind: "not_cancellable", message: why || "Flipkart: not eligible for cancellation" };
    }
    if (data.errorMessage) return { kind: "not_cancellable", message: String(data.errorMessage) };
    const feeNote = feeNoteOf(data);
    if (feeNote) this.log("warn", `[cancel] ${orderId}/${unitId}: Flipkart will charge a cancellation fee — ${feeNote}`);
    if (data.allowPartialCancellation === false && (data.parentItems ?? []).length > 1) {
      this.log("warn", `[cancel] ${orderId}: Flipkart does not allow partial cancellation — the whole order may be cancelled`);
    }

    // ---- reason + comment -------------------------------------------------
    const reasons = data.cancellationReasons ?? [];
    const wanted = opts.reason || DEFAULT_CANCEL_REASON;
    const reason =
      reasons.find((r) => r.code === wanted)?.code ||
      reasons.find((r) => /mind|not listed/i.test(`${r.code} ${r.description}`))?.code ||
      reasons[0]?.code ||
      wanted;
    const select = this.page.locator("select[name=reasonList]");
    await select.waitFor({ state: "visible", timeout: 15_000 });
    await select.selectOption(reason);
    const comment = (opts.comments || DEFAULT_CANCEL_COMMENT).padEnd(5, ".");
    await this.page.locator("textarea").first().fill(comment);
    await sleep(500);
    await step("reason-filled");
    this.log("info", `[cancel] ${orderId}/${unitId}: reason ${reason}, comment "${comment}"`);

    const continueBtn = this.page.getByRole("button", { name: /^continue$/i }).first();
    await continueBtn.waitFor({ state: "visible", timeout: 10_000 });
    // The button enables a moment after the comment is typed; give it a few seconds.
    const enabledBy = Date.now() + 6_000;
    while (!(await continueBtn.isEnabled()) && Date.now() < enabledBy) await sleep(300);
    if (!(await continueBtn.isEnabled())) throw new Error("CONTINUE stayed disabled after reason and comment were filled");
    await continueBtn.click();

    // ---- conditions (only some orders) ------------------------------------
    // Units packed together show a "CANCELLATION CONDITIONS" step first: "N more
    // items will be cancelled — items in this order were packed together, we
    // will be unable to deliver if any item is cancelled", with Accept &
    // Continue. The whole order goes, which is what an order-level cancel asks
    // for; accept, carry on, and say so in the result.
    const requestBtn = this.page.getByRole("button", { name: /request cancellation/i }).first();
    const acceptBtn = this.page.getByText(/^\s*accept\s*(&|and)\s*continue\s*$/i).first();
    let conditionsNote = "";
    const nextBy = Date.now() + 20_000;
    let next: "request" | "accept" | null = null;
    while (!next && Date.now() < nextBy) {
      if (await requestBtn.isVisible().catch(() => false)) next = "request";
      else if (await acceptBtn.isVisible().catch(() => false)) next = "accept";
      else await sleep(400);
    }
    if (next === "accept") {
      if (opts.acceptConditions === false) {
        return { kind: "not_cancellable", message: `Flipkart: ${this.conditionsText(await this.pageText()) || "cancelling this unit cancels the other items packed with it"} — not accepted` };
      }
      conditionsNote = this.conditionsText(await this.pageText());
      this.log("warn", `[cancel] ${orderId}/${unitId}: conditions step — ${conditionsNote || "items packed together"}; accepting`);
      await acceptBtn.click();
      await sleep(800);
      await step("conditions-accepted");
    }

    // ---- refund mode, then the one irreversible press ----------------------
    await requestBtn.waitFor({ state: "visible", timeout: 20_000 });
    // "Original Payment Mode" is preselected on the screens seen so far; a click
    // on its label is harmless when it already is, and picks it when it is not.
    const original = this.page.getByText("Original Payment Mode", { exact: true }).first();
    if (await original.isVisible().catch(() => false)) await original.click().catch(() => undefined);
    await sleep(500);
    await step("refund-mode");

    if (opts.dryRun) {
      return { kind: "dry_run", message: `Stopped before REQUEST CANCELLATION (dry run)${conditionsNote ? ` · conditions: ${conditionsNote}` : ""}`, feeNote };
    }

    const submitPromise = this.page
      .waitForResponse(
        (r) => /\/self-serve\/cancel\/OD\d+/i.test(r.url()) && r.request().method() === "POST",
        { timeout: 45_000 }
      )
      .catch(() => null);
    await requestBtn.click();
    const submitRes = await submitPromise;
    await sleep(3000);
    await step("after-request");

    const answer = submitRes ? await jsonOf(submitRes) : null;
    if (answer?.success === true) {
      const refundTat = String(answer.refundTat || "");
      this.log("info", `[cancel] ${orderId}/${unitId}: cancelled — Flipkart answered success${refundTat ? ` (${refundTat})` : ""}`);
      return { kind: "cancelled", message: `Cancellation requested on Flipkart${conditionsNote ? ` · ${conditionsNote}` : ""}`, refundTat, feeNote };
    }
    if (answer) {
      throw new Error(`Flipkart refused the cancellation: ${String(answer.errorMessage || JSON.stringify(answer).slice(0, 300))}`);
    }
    // The POST was not seen. The page is the fallback: Flipkart lands back on
    // order_details with "Cancelled" on the timeline once it has taken it.
    const text = await this.pageText();
    if (/order_details/i.test(this.page.url()) && /\bCancelled\b/.test(text)) {
      return { kind: "cancelled", message: "Cancelled (read from the order page)", refundTat: "", feeNote };
    }
    throw new Error("No answer from Flipkart to REQUEST CANCELLATION");
  }

  /**
   * The unit's CANCEL action from order_details' page data. Null when the data
   * never arrived or carries no CANCEL action — then the cancel page decides.
   */
  private async readEligibility(
    orderId: string,
    unitId: string,
    itemId?: string
  ): Promise<{ eligible: boolean; reason: string } | null> {
    const odPromise = this.page
      .waitForResponse(
        (r) => /\/api\/4\/page\/fetch/i.test(r.url()) && /CX_ORDER_DETAIL_PAGE/.test(r.request().postData() || ""),
        { timeout: 30_000 }
      )
      .catch(() => null);
    const q = new URLSearchParams({ order_id: orderId, unit_id: unitId, item_id: itemId || itemIdOf(unitId) });
    await this.page.goto(`https://www.flipkart.com/order_details?${q.toString()}`, { waitUntil: "domcontentloaded", timeout: 45_000 });
    if (flipkartLoginUrl(this.page.url())) throw new Error("SESSION_EXPIRED: Flipkart showed the login page");
    const res = await odPromise;
    await sleep(1500);
    const body = res ? await jsonOf(res) : null;
    if (!body) return null;
    const actions: Array<{ eligible?: unknown; eligibilityReason?: unknown }> = [];
    const walk = (v: unknown, depth: number) => {
      if (depth > 30 || v == null || typeof v !== "object") return;
      if (Array.isArray(v)) return v.forEach((x) => walk(x, depth + 1));
      for (const [k, x] of Object.entries(v as Record<string, unknown>)) {
        if (k === "CANCEL" && x && typeof x === "object" && typeof (x as { eligible?: unknown }).eligible === "boolean") {
          actions.push(x as { eligible: boolean; eligibilityReason?: unknown });
        } else walk(x, depth + 1);
      }
    };
    walk(body, 0);
    if (!actions.length) return null;
    const ok = actions.find((a) => a.eligible === true);
    if (ok) return { eligible: true, reason: "" };
    return { eligible: false, reason: String(actions[0].eligibilityReason || "") };
  }

  /** The conditions step in one line: "2 more items will be cancelled · 3 items in this order were packed together…". */
  private conditionsText(text: string): string {
    const more = (text.match(/(\d+)\s+more items? will be cancelled/i) || [])[0] || "";
    const packed = (text.match(/\d+\s+items? in this order were packed together[^.]*\./i) || [])[0] || "";
    return [more, packed].filter(Boolean).join(" · ").trim();
  }

  private async pageText(): Promise<string> {
    return String(await this.page.evaluate(() => document.body?.innerText || "").catch(() => ""));
  }

  private firstLine(text: string, re: RegExp): string {
    return (text.split("\n").find((l) => re.test(l) && l.length < 200) || "").trim();
  }
}
