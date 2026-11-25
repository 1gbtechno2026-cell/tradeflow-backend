import { Order } from "../models/Order.js";
import { OrderJobRun } from "../models/OrderJobRun.js";
import { OrderUpdateRequest, type UpdateSource } from "../models/OrderUpdateRequest.js";
import { PlatformId } from "../models/PlatformId.js";
import { parseInvoiceMode, type InvoiceMode } from "./invoiceCapture.js";
import { parseSinceDate } from "./orderFetch.js";
import { DEFAULT_UPDATE_STATUSES } from "./orderLifecycle.js";
import { resolveFetchSinceDays, scheduledFetchCount, scheduledFetchEmails } from "./orderFetchSchedule.js";
import type { UpdateTriggerInput } from "./orderUpdate.js";
import { createUpdateRequest } from "./orderUpdateQueue.js";
import { assertFlipkart, assertValueLimit, distinct, RequestInputError, splitValues } from "./requestCommon.js";

/**
 * The bodies of "Start a request" → what the existing fetch and update take.
 *
 * The reference platform's Fetch and Update modals post these (measured,
 * docs/order-units-reference-capture.md §5); both triggers accept them beside
 * the bodies they always took:
 *
 *   fetch   { platform, email_ids?, enable_login, fetch_orders_after_date,
 *             force_fetch, run_update_after_fetch }
 *           (ours: emails, invoices, trigger, by)
 *   update  { platform } | { platform, order_status } | { platform, email_ids }
 *           | { platform, order_status, email_ids } | { platform, order_ids }
 *           | { platform, order_email_pairs: [{ email, order_id }] }
 *           (ours: emails, orderIds, csv_rows / csvRows, source, by)
 *
 * Nothing here starts anything: parse → resolve → the caller hands the result
 * to startOrderFetch / createUpdateRequest, which run as they always did.
 */

// ── fetch ───────────────────────────────────────────────────────────────────

export type FetchSelection = "email_ids" | "all_accounts";

export interface FetchBody {
  platform: "FLIPKART";
  /** Lower-cased, de-duplicated; empty = every active logged-in account. */
  emails: string[];
  /** "YYYY-MM-DD" as sent, or "" when the body carries no date. */
  afterDate: string;
  enableLogin: boolean;
  forceFetch: boolean;
  runUpdateAfterFetch: boolean;
  invoices: InvoiceMode;
  trigger: "user" | "system-cron";
  by: string;
  /** The request this fetch follows ("" = none) and why — the dashboard's Enable login sends `parent_id` + `follows: "enable_login"` on the fetch for the accounts it has just logged in. */
  parentId: string;
  follows: string;
}

export function parseFetchBody(body: unknown): FetchBody {
  const b = (body && typeof body === "object" ? body : {}) as Record<string, unknown>;
  const platform = assertFlipkart(b.platform);
  // `emails` is the key this route always took; `email_ids` is the reference's.
  const own = splitValues(b.emails);
  const emails = distinct((own.length ? own : splitValues(b.email_ids)).map((e) => e.toLowerCase()));
  assertValueLimit("Email Addresses", emails);
  return {
    platform,
    emails,
    afterDate: String(b.fetch_orders_after_date || "").trim(),
    enableLogin: b.enable_login === true,
    forceFetch: b.force_fetch === true,
    runUpdateAfterFetch: b.run_update_after_fetch === true,
    invoices: parseInvoiceMode(b.invoices),
    trigger: b.trigger === "system-cron" ? "system-cron" : "user",
    by: String(b.by || "").trim(),
    parentId: String(b.parent_id || "").trim().slice(0, 64),
    follows: String(b.follows || "").trim().slice(0, 40),
  };
}

export interface FetchPlan {
  selection: FetchSelection;
  /** The accounts the fetch is asked to read: the emails sent, or every logged-in Flipkart Platform ID. */
  emails: string[];
  /** Of those, how many are logged in with a saved session — what the fetch will open. */
  loggedIn: number;
  sinceDate: Date;
  afterDate: string;
  /** "request" = the date in the body; "lookback" = none was sent, the workspace's look-back applies. */
  afterDateSource: "request" | "lookback";
  /** What was asked, as the batch list shows it later (order_job_runs.meta.params). */
  params: Record<string, unknown>;
}

/** Logged-in Flipkart Platform IDs with a saved session — the fetch schedule's own rule ("active + has a session"). */
export async function loggedInAccountEmails(userId: string): Promise<string[]> {
  return scheduledFetchEmails(userId);
}

export async function loggedInAccountCount(userId: string): Promise<number> {
  return scheduledFetchCount(userId);
}

/** How many of these emails are logged-in Flipkart Platform IDs with a saved session. */
export async function countLoggedIn(userId: string, emails: string[]): Promise<number> {
  if (!emails.length) return 0;
  const rows = (await PlatformId.distinct("email", {
    userId,
    email: { $in: emails },
    status: "logged_in",
    sessionSavedAt: { $ne: null },
    $or: [{ "sessionState.cookies.0": { $exists: true } }, { "sessionCookies.0": { $exists: true } }],
  })) as string[];
  return rows.length;
}

function istDay(at: Date): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Kolkata", year: "numeric", month: "2-digit", day: "2-digit" }).format(at);
}

/**
 * Who a fetch body selects and from when. No emails = every logged-in account
 * of the workspace (`all_accounts`); no date = the workspace's look-back
 * (Settings → Scheduled fetch, else FETCH_SINCE_DAYS, else 60 days), the one
 * bound this fetcher has — it cannot read "all time".
 */
export async function planFetch(userId: string, body: FetchBody): Promise<FetchPlan> {
  const selection: FetchSelection = body.emails.length ? "email_ids" : "all_accounts";
  const emails = selection === "email_ids" ? body.emails : await loggedInAccountEmails(userId);
  if (!emails.length) throw new RequestInputError("No logged-in Flipkart account in this workspace. Login a Platform ID first.");
  const loggedIn = selection === "email_ids" ? await countLoggedIn(userId, emails) : emails.length;

  let afterDate = body.afterDate;
  let afterDateSource: FetchPlan["afterDateSource"] = "request";
  if (!afterDate) {
    const { days } = await resolveFetchSinceDays(userId);
    afterDate = istDay(new Date(Date.now() - days * 24 * 60 * 60 * 1000));
    afterDateSource = "lookback";
  }
  const sinceDate = parseSinceDate(afterDate);
  if (Number.isNaN(sinceDate.getTime())) throw new RequestInputError("Enter a valid date (YYYY-MM-DD)");

  return {
    selection,
    emails,
    loggedIn,
    sinceDate,
    afterDate,
    afterDateSource,
    params: {
      platform: body.platform,
      selection_method: selection,
      email_ids_count: selection === "email_ids" ? emails.length : 0,
      accounts_count: emails.length,
      logged_in_count: loggedIn,
      fetch_orders_after_date: afterDate,
      fetch_orders_after_date_source: afterDateSource,
      force_fetch: body.forceFetch,
      enable_login: body.enableLogin,
      run_update_after_fetch: body.runUpdateAfterFetch,
      invoices: body.invoices,
      // Kept on the ledger row, so the batch list can show "Follows request …" (as the update after a fetch does).
      ...(body.follows ? { follows: body.follows } : {}),
      ...(body.parentId ? { parent_id: body.parentId } : {}),
    },
  };
}

/**
 * The update that follows a fetch (`run_update_after_fetch`): ONE request for
 * the in-progress units of the accounts that were fetched, through the same
 * createUpdateRequest every update goes through, marked with the fetch's run
 * id as its parent and the selection label `after_fetch`. Skipped — never
 * queued behind — when an update request is already open, and when the fetch
 * was stopped. What happened is written on the fetch's ledger row
 * (meta.update_after), so the batch list can show "Follows request …" from
 * either side.
 */
export async function queueUpdateAfterFetch(
  userId: string,
  fetch: { runId: string; emails: string[]; selection: FetchSelection; cancelled: boolean; by: string; trigger: "user" | "system-cron" }
): Promise<{ status: "queued" | "skipped" | "failed"; request_id?: string; reason?: string; units?: number; accounts?: number }> {
  const outcome = await (async () => {
    if (fetch.cancelled) return { status: "skipped" as const, reason: "the fetch was stopped" };
    const open = await OrderUpdateRequest.findOne({ userId, status: { $in: ["queued", "running"] } }).select("_id").lean();
    if (open) return { status: "skipped" as const, reason: `update request ${String(open._id)} is already open` };
    try {
      const source: UpdateSource = fetch.trigger === "system-cron" ? "schedule" : "manual";
      // Every account = no email filter at all: the default selection is every in-progress unit.
      const emails = fetch.selection === "all_accounts" ? [] : fetch.emails;
      const request = await createUpdateRequest(
        userId,
        { emails, selectionMethod: "after_fetch", withReturned: true },
        {
          source,
          by: fetch.by,
          parentId: fetch.runId,
          // No status is named: the request records the in-progress list it stands for, as the reference does.
          params: { platform: "FLIPKART", selection_method: "after_fetch", follows: "fetch", parent_id: fetch.runId, force: false, email_ids_count: emails.length, statuses_to_update: [...DEFAULT_UPDATE_STATUSES], status_keys: [], order_ids_count: 0, order_email_pairs_count: 0 },
        }
      );
      return { status: "queued" as const, request_id: request.id, units: request.counts.units, accounts: request.counts.accounts };
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      // Nothing in progress on those accounts is an ordinary outcome, not a failure.
      return { status: /No active order units/.test(reason) ? ("skipped" as const) : ("failed" as const), reason };
    }
  })();
  console.log(`[orders-fetch] update after fetch ${fetch.runId}: ${outcome.status}${outcome.request_id ? ` → request ${outcome.request_id}` : ""}${outcome.reason ? ` (${outcome.reason})` : ""}`);
  if (fetch.runId) {
    await OrderJobRun.updateOne({ _id: fetch.runId }, { $set: { "meta.update_after": outcome, ...(outcome.request_id ? { "meta.child_ids": [outcome.request_id] } : {}) } }).catch(() => undefined);
  }
  return outcome;
}

// ── update ──────────────────────────────────────────────────────────────────

/**
 * Workspaces whose update request is being put together right now. Reading the
 * units and their accounts normally takes 1-4 s, but has been seen to take
 * minutes on a slow database; a second press (or a retry after the dashboard
 * gave up waiting) must not queue the same update twice. The update trigger
 * and Batch requests → Retry of an update share this one slot.
 */
const updatesBeingPrepared = new Set<string>();

export const UPDATE_PREPARING_MESSAGE = "An update request is already being prepared — it will appear in a moment";

/** Take the workspace's update-preparation slot. Returns the function that gives it back, or null when it is held. */
export function holdUpdatePreparation(userId: string): (() => void) | null {
  if (updatesBeingPrepared.has(userId)) return null;
  updatesBeingPrepared.add(userId);
  return () => void updatesBeingPrepared.delete(userId);
}

/** The selection method names the reference's mode banner shows. */
export type UpdateSelection = "default" | "order_status" | "email_ids" | "order_status_and_email_ids" | "order_ids" | "order_email_pairs";

/**
 * A reference status label that is stored under another spelling here: the
 * order API writes "ofd" for Out for delivery and the page scraper wrote
 * "Order Confirmed" for Approved. Only spellings of the SAME status belong
 * here. The dashboard's list filter (backend/src/services/orderUnitsApi.ts
 * STATUS_ALIASES) also lets "Cancelled" take in "Failed" — a payment the bank
 * never confirmed reads Cancelled on the reference — but an update calls
 * Flipkart once per unit it selects, so here "Cancelled" is the Cancelled key
 * alone, as this trigger always matched it, and "Failed" is its own status.
 */
export const STATUS_ALIASES: Record<string, string[]> = {
  "out for delivery": ["Out for delivery", "ofd"],
  approved: ["Approved", "Order Confirmed"],
};

// The detailed cancellations are not status keys here: a cancelled unit is
// "Cancelled" with the stage it had reached in order_status_pre_cancellation
// and cancelled_by_user saying whether the account asked for it.
const PRE_CONFIRMED = /^(approved|order confirmed|purchased|ready to ship)$/i;
const PRE_SHIPPED = /^(shipped|dispatched|out for delivery|ofd|delivery)/i;
const PRE_RECEIVED = /^order received$/i;
const cancelledAt = (byUser: boolean, stage: RegExp) => ({
  status_key: "Cancelled",
  cancelled_by_user: byUser ? true : { $ne: true },
  order_status_pre_cancellation: stage,
});

/** status values that are a condition, not a key (the reference's slugs). */
const STATUS_CONDITIONS: Record<string, Record<string, unknown>> = {
  cancelled_approved: cancelledAt(false, PRE_CONFIRMED),
  cancelled_shipped: cancelledAt(false, PRE_SHIPPED),
  cancelled_received: cancelledAt(false, PRE_RECEIVED),
  cancelled_by_user_approved: cancelledAt(true, PRE_CONFIRMED),
  cancelled_by_user_shipped: cancelledAt(true, PRE_SHIPPED),
};

/** The same five, when a caller sends the label instead of the slug. */
const CONDITION_LABELS: Record<string, string> = {
  "cancelled - order confirmed but not shipped": "cancelled_approved",
  "cancelled - order shipped": "cancelled_shipped",
  "cancelled - received but not confirmed/shipped": "cancelled_received",
  "cancelled by user - order confirmed but not shipped": "cancelled_by_user_approved",
  "cancelled by user - order shipped": "cancelled_by_user_shipped",
};

export interface StatusSelection {
  /** status_key values to match exactly. */
  keys: string[];
  /** Detailed cancellations, each a condition on a cancelled unit; ORed with `keys`. */
  conditions: Array<Record<string, unknown>>;
}

/**
 * Reference status labels → what to match in order_details. `stored` is the
 * workspace's own status_key spellings: a label is matched to them without
 * regard to case ("OFD" → "ofd"), since the update selects by exact key. A
 * label nothing is stored under stays as typed and simply matches no unit.
 * A detailed cancellation beside plain "Cancelled" adds nothing and is dropped.
 */
export function statusSelection(labels: string[], stored: string[] = []): StatusSelection {
  const spelling = new Map(stored.filter(Boolean).map((k) => [k.toLowerCase(), k]));
  const keys = new Map<string, string>();
  const slugs = new Set<string>();
  for (const raw of labels) {
    const value = String(raw || "").trim();
    if (!value) continue;
    const slug = CONDITION_LABELS[value.toLowerCase()] || value.toLowerCase();
    if (STATUS_CONDITIONS[slug]) slugs.add(slug);
    // An unknown cancelled_* value of the reference is still a cancellation.
    else if (slug.startsWith("cancelled_")) keys.set("cancelled", spelling.get("cancelled") || "Cancelled");
    else for (const k of STATUS_ALIASES[slug] || [value]) keys.set(k.toLowerCase(), spelling.get(k.toLowerCase()) || k);
  }
  const conditions = keys.has("cancelled") ? [] : [...slugs].map((s) => STATUS_CONDITIONS[s]);
  return { keys: [...keys.values()], conditions };
}

export interface UpdateBody {
  platform: "FLIPKART";
  emails: string[];
  orderIds: string[];
  /** Status labels exactly as sent. */
  statuses: string[];
  pairs: Array<{ email: string; order_id: string }>;
  /** Which of the reference's six methods these inputs make. */
  selection: UpdateSelection;
  source: UpdateSource;
  by: string;
  parentId: string;
}

function parsePairs(value: unknown): Array<{ email: string; order_id: string }> {
  if (!Array.isArray(value)) return [];
  const seen = new Set<string>();
  const out: Array<{ email: string; order_id: string }> = [];
  for (const row of value) {
    const rec = row && typeof row === "object" ? (row as Record<string, unknown>) : {};
    const email = String(rec.email || "").trim().toLowerCase();
    const order_id = String(rec.order_id || rec.orderId || "").trim();
    if (!email || !order_id || seen.has(`${email} ${order_id.toUpperCase()}`)) continue;
    seen.add(`${email} ${order_id.toUpperCase()}`);
    out.push({ email, order_id });
  }
  return out;
}

/** order_status as sent: an array (one status per element), or a typed list split on commas — statuses hold spaces. */
function parseStatuses(value: unknown): string[] {
  const list = Array.isArray(value) ? value : value == null || typeof value === "object" ? [] : String(value).split(/[,;\n]+/);
  return distinct(list.map((item) => String(item ?? "").trim()).filter(Boolean));
}

/**
 * The selection a body makes, in the order the update itself applies it
 * (services/orderUpdate.ts resolveTargets): pairs win over order ids, order
 * ids over statuses and emails.
 */
export function parseUpdateBody(body: unknown): UpdateBody {
  const b = (body && typeof body === "object" ? body : {}) as Record<string, unknown>;
  const platform = assertFlipkart(b.platform);
  const ownEmails = splitValues(b.emails);
  const emails = distinct((ownEmails.length ? ownEmails : splitValues(b.email_ids)).map((e) => e.toLowerCase()));
  const orderIds = distinct(splitValues(b.order_ids ?? b.orderIds).map((id) => id.toUpperCase()));
  const statuses = parseStatuses(b.order_status);
  const ownPairs = parsePairs(b.csv_rows ?? b.csvRows);
  const pairs = ownPairs.length ? ownPairs : parsePairs(b.order_email_pairs);
  assertValueLimit("Order IDs", orderIds);
  assertValueLimit("Email Addresses", emails);
  assertValueLimit("CSV email + order ID pairs", pairs.map((p) => `${p.email} ${p.order_id.toUpperCase()}`));
  const selection: UpdateSelection = pairs.length
    ? "order_email_pairs"
    : orderIds.length
      ? "order_ids"
      : statuses.length && emails.length
        ? "order_status_and_email_ids"
        : statuses.length
          ? "order_status"
          : emails.length
            ? "email_ids"
            : "default";
  return {
    platform,
    emails,
    orderIds,
    statuses,
    pairs,
    selection,
    source: String(b.source || "") === "cancel" ? "cancel" : "manual",
    by: String(b.by || "").trim(),
    parentId: String(b.parent_id || "").trim(),
  };
}

export interface UpdatePlan {
  input: UpdateTriggerInput;
  selection: UpdateSelection;
  /** What was asked, as the batch list shows it later (order_update_requests.params). */
  params: Record<string, unknown>;
}

/** The workspace's own status_key spellings (index userId + status_key). */
export async function storedStatusKeys(userId: string): Promise<string[]> {
  return ((await Order.distinct("status_key", { userId })) as string[]).filter(Boolean);
}

/**
 * body → the existing selection (services/orderUpdate.ts UpdateTriggerInput).
 * Only what the chosen method uses is passed on, so the recorded method is
 * the one that ran. `stored` is given by the checks; a live call reads it.
 */
export async function planUpdate(userId: string, body: UpdateBody, stored?: string[]): Promise<UpdatePlan> {
  const usesFilters = body.selection !== "order_email_pairs" && body.selection !== "order_ids";
  const status = usesFilters && body.statuses.length ? statusSelection(body.statuses, stored ?? (await storedStatusKeys(userId))) : { keys: [], conditions: [] };
  // Default and "email accounts": no status is named, so every in-progress unit is taken — the reference's eleven.
  const inProgress = usesFilters && !body.statuses.length;
  const input: UpdateTriggerInput = {
    emails: usesFilters ? body.emails : [],
    orderIds: body.selection === "order_ids" ? body.orderIds : [],
    orderStatus: status.keys,
    statusConditions: status.conditions,
    csvRows: body.selection === "order_email_pairs" ? body.pairs : [],
    selectionMethod: body.selection,
    withReturned: inProgress,
  };
  return {
    input,
    selection: body.selection,
    params: {
      platform: body.platform,
      selection_method: body.selection,
      // The reference records `force` on every update request; nothing here sends it, so it is always false.
      force: false,
      // The statuses asked for; with none named, the in-progress list the selection stands for (as the reference records it).
      statuses_to_update: inProgress ? [...DEFAULT_UPDATE_STATUSES] : usesFilters ? body.statuses : [],
      status_keys: status.keys,
      email_ids_count: usesFilters ? body.emails.length : 0,
      order_ids_count: body.selection === "order_ids" ? body.orderIds.length : 0,
      order_email_pairs_count: body.selection === "order_email_pairs" ? body.pairs.length : 0,
    },
  };
}
