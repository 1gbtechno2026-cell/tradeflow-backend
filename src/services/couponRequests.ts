import { DelayedError, type Job } from "bullmq";
import { Types } from "mongoose";
import type { APIRequestContext } from "playwright";
import { config } from "../config.js";
import { OrderCouponRequest, OrderCouponTask, type ICouponLinkResult, type IOrderCouponRequest, type IOrderCouponTask } from "../models/OrderCouponRequest.js";
import { countKeyOf, type RequestTaskStatus } from "../models/OrderInvoiceRequest.js";
import { OrderJobRun } from "../models/OrderJobRun.js";
import { PlatformId } from "../models/PlatformId.js";
import { COUPON_QUEUE, getCouponQueue, type CouponJobData } from "../queue.js";
import { lockAccount } from "./accountLock.js";
import { flipkartLoginUrl, sleep } from "./browser.js";
import { openOrderApiSession } from "./orderApi.js";
import {
  ABORTED_WHILE_RUNNING,
  assertValueLimit,
  CANCELLED_BEFORE_START,
  classifyPlatformError,
  classOfHttpStatus,
  countsOf,
  distinct,
  emptyCounts,
  ERROR_CLASSES,
  finishedCount,
  hostName,
  openRequestRun,
  RequestInputError,
  requestProgress,
  splitValues,
  withLiveness,
  WORKER_NOT_PICKED_UP,
  workerAlive,
  writeRequestRun,
  type ErrorClass,
  type LedgerModel,
} from "./requestCommon.js";
import { resolveLoggedInSession, type SessionLookup } from "./sessionStore.js";

/**
 * "Apply Company Coupons" — the reference's "Coupon apply" batch.
 *
 * NOBODY HAS OBSERVED HOW FLIPKART ACTIVATES A COMPANY COUPON. No coupon link
 * was available when this was built (2026-11-24), so this is the honest
 * version: for every account, every coupon link is opened with that account's
 * saved session over plain HTTP — the request context the order API uses,
 * desktop User-Agent, redirects followed by hand, no browser — and exactly
 * what came back is recorded: each hop, the final URL, the HTTP status,
 * whether Flipkart bounced to login, the page title and its first words.
 *
 * A task that ends `success` therefore says "Link opened with the account's
 * session (HTTP 200)" and nothing more. Whether that request alone activates
 * a coupon — or the page needs a click, or makes an API call of its own — is
 * unverified until one real coupon link is tried on one account and the
 * account's coupons are looked at afterwards; the recorded hops and page text
 * of that first run are what will tell.
 *
 *   createCouponRequest   links checked (https, a flipkart.com host), one task
 *                         per account — the emails given, or every logged-in
 *                         Flipkart Platform ID — one queue job per account
 *   runCouponJob          (worker) one account under the per-account lock:
 *                         its links one after another, a pause between them
 *   stopCouponRequest     queued accounts → cancelled; a running one finishes
 *   requeueStrandedCouponJobs / failUnclaimedCouponRequests
 *                         as for the invoice batch (services/invoiceRequests.ts)
 *
 *   COMPANY_COUPONS_ENABLED=0   switches the trigger off (default: on)
 *   COUPON_CONCURRENCY=4        accounts at once
 *   COUPON_LINK_PAUSE_MS=1500   pause between two links of one account
 */

export const COMPANY_COUPONS_ENABLED = !["0", "off", "false", "no"].includes(String(process.env.COMPANY_COUPONS_ENABLED ?? "1").trim().toLowerCase());
export const COUPON_CONCURRENCY = Math.max(1, Math.floor(Number(process.env.COUPON_CONCURRENCY) || 4));
/** Most links one request may carry (ours — the reference's own limit was not captured). */
export const COUPON_LINKS_MAX = 50;
const LINK_PAUSE_MS = Math.max(0, Number(process.env.COUPON_LINK_PAUSE_MS ?? 1500));
/** Pause after an account, before this lane takes the next one. */
const ACCOUNT_PAUSE_MS = Math.max(0, Number(process.env.COUPON_ACCOUNT_PAUSE_MS ?? 500));
const LOCK_RETRY_MS = 20_000;
const MAX_HOPS = 10;
const REQUEST_PICKUP_MS = Math.max(30_000, Number(process.env.REQUEST_PICKUP_MS) || 3 * 60 * 1000);
const STRANDED_AFTER_MS = 2 * 60 * 1000;
const REAP_EVERY_MS = 60 * 1000;
const WATCHDOG_EVERY_MS = 30 * 1000;
const COUNTS_EVERY_MS = 2000;

const OPEN = ["queued", "in_progress"] as const;

export const LINK_OPENED = (status: number) => `Link opened with the account's session (HTTP ${status})`;

// ── links ───────────────────────────────────────────────────────────────────

/** `coupon_links` as the reference sends it — one string, links separated by spaces or commas — or an array. */
export function parseCouponLinks(value: unknown): string[] {
  return distinct(splitValues(value));
}

export function isFlipkartHost(host: string): boolean {
  const h = String(host || "").toLowerCase();
  return h === "flipkart.com" || h.endsWith(".flipkart.com");
}

/** A coupon link must be https on a flipkart.com host; anything else is refused before a task exists. */
export function assertCouponLinks(links: string[]): void {
  if (!links.length) throw new RequestInputError("Coupon links are required");
  if (links.length > COUPON_LINKS_MAX) throw new RequestInputError(`Too many values in "Coupon links": ${links.length} entered, but the maximum is ${COUPON_LINKS_MAX}. Please reduce it.`, 400, "too_many_values");
  for (const link of links) {
    let url: URL | null = null;
    try {
      url = new URL(link);
    } catch {
      /* not a URL */
    }
    if (!url || url.protocol !== "https:" || !isFlipkartHost(url.hostname)) {
      throw new RequestInputError(`Not a Flipkart coupon link: ${link.slice(0, 120)} — a link must start with https:// and be on flipkart.com`, 400, "invalid_coupon_link");
    }
  }
}

function decodeEntities(text: string): string {
  return text
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#0?39;|&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");
}

/** The page's title and its first visible words — scripts, styles and markup left out. */
export function pageTitleAndExcerpt(body: string): { title: string; excerpt: string; text: string } {
  const title = decodeEntities((body.match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [])[1] || "").replace(/\s+/g, " ").trim().slice(0, 160);
  const looksHtml = /<html|<body|<div|<head/i.test(body);
  const text = looksHtml
    ? decodeEntities(
        body
          .replace(/<head[\s\S]*?<\/head>/gi, " ")
          .replace(/<(script|style|noscript|svg|template)[\s\S]*?<\/\1>/gi, " ")
          .replace(/<[^>]+>/g, " ")
      )
        .replace(/\s+/g, " ")
        .trim()
    : body.replace(/\s+/g, " ").trim();
  return { title, excerpt: text.slice(0, 240), text: text.slice(0, 4000) };
}

/** Words on the page that say the coupon itself is no good. */
const INVALID_COUPON_RE = /\b(expired|invalid|no longer (?:valid|available)|not valid)\b/i;

const PAGE_HEADERS = {
  accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "upgrade-insecure-requests": "1",
};

export interface CouponOpenOptions {
  /** Where a link is really requested — the identity in production; the checks point it at a local fake. */
  resolveUrl?: (link: string) => string;
  /** Hosts a link may be followed to. The session's cookies are never sent anywhere else. */
  allowHost?: (host: string) => boolean;
  timeoutMs?: number;
}

/**
 * Open one coupon link with the account's session and say what came back.
 * Redirects are followed by hand so every hop is on record, never off
 * Flipkart, and never INTO the login page — a redirect there is the answer
 * ("this session is not logged in"), not a page to load.
 */
export async function openCouponLink(ctx: APIRequestContext, link: string, opts: CouponOpenOptions = {}): Promise<ICouponLinkResult> {
  const allowHost = opts.allowHost || isFlipkartHost;
  const started = Date.now();
  const out: ICouponLinkResult = { link, outcome: "failed", errorClass: "", message: "", finalUrl: "", httpStatus: 0, loginBounce: false, hops: [], title: "", excerpt: "", ms: 0 };
  const fail = (errorClass: ErrorClass, detail = ""): ICouponLinkResult => ({ ...out, outcome: "failed", errorClass, message: ERROR_CLASSES[errorClass], excerpt: out.excerpt || detail.slice(0, 240), ms: Date.now() - started });
  let url = (opts.resolveUrl || ((u: string) => u))(link);
  try {
    for (let hop = 0; hop <= MAX_HOPS; hop++) {
      out.finalUrl = url;
      // The path alone: a link that only mentions /login in its query (…?ret=/account/login) is not the login page.
      if (flipkartLoginUrl(new URL(url).pathname)) {
        out.loginBounce = true;
        return fail("not_logged_in", "Flipkart sent this session to the login page");
      }
      if (!allowHost(new URL(url).hostname)) return fail("unexpected_response", `The link left Flipkart: ${new URL(url).hostname}`);
      const res = await ctx.get(url, { maxRedirects: 0, timeout: opts.timeoutMs ?? 30_000, failOnStatusCode: false, headers: PAGE_HEADERS });
      const status = res.status();
      out.httpStatus = status;
      out.hops.push({ status, url });
      const location = res.headers()["location"];
      if (status >= 300 && status < 400 && location) {
        url = new URL(location, url).toString();
        continue;
      }
      const body = await res.text().catch(() => "");
      const page = pageTitleAndExcerpt(body);
      out.title = page.title;
      out.excerpt = page.excerpt;
      if (status === 404 || status === 410) return fail("invalid_coupon");
      const byStatus = classOfHttpStatus(status);
      if (byStatus) return fail(byStatus);
      if (INVALID_COUPON_RE.test(`${page.title} ${page.text}`)) return fail("invalid_coupon");
      return { ...out, outcome: "opened", errorClass: "", message: LINK_OPENED(status), ms: Date.now() - started };
    }
    return fail("unexpected_response", `More than ${MAX_HOPS} redirects`);
  } catch (err) {
    const why = classifyPlatformError(err);
    return fail(why.errorClass, why.detail);
  }
}

/** One account's outcome from its link results. */
export function couponTaskOutcome(results: ICouponLinkResult[]): { status: "success" | "failed"; errorClass: string; error: string; note: string } {
  const opened = results.filter((r) => r.outcome === "opened");
  const failed = results.filter((r) => r.outcome === "failed");
  if (!failed.length && opened.length) {
    return { status: "success", errorClass: "", error: "", note: opened.length === 1 ? opened[0].message : `${opened.length} links opened with the account's session (HTTP ${distinct(opened.map((r) => String(r.httpStatus))).join(", ")})` };
  }
  // A login bounce is the account's problem, whatever the other links did.
  const bounce = failed.find((r) => r.errorClass === "not_logged_in");
  const classes = distinct(failed.map((r) => r.errorClass));
  const errorClass = (bounce ? "not_logged_in" : opened.length || classes.length > 1 ? "partially_failed" : classes[0] || "unknown") as ErrorClass;
  return { status: "failed", errorClass, error: ERROR_CLASSES[errorClass], note: opened.length ? `${opened.length} of ${results.length} link(s) opened` : "" };
}

// ── dependencies ────────────────────────────────────────────────────────────

export interface CouponAccount {
  email: string;
  accountId: string;
}

/** What the batch needs from outside. The worker and the API use `couponDefaults`; the checks swap parts of it. */
export interface CouponDeps extends CouponOpenOptions {
  /** Every logged-in Flipkart Platform ID with a saved session, or those among `emails`. */
  listAccounts: (userId: string, emails?: string[]) => Promise<CouponAccount[]>;
  loadSession: (email: string) => Promise<SessionLookup>;
  openContext: (cookies: unknown[]) => Promise<APIRequestContext>;
  /** The per-account lock, held for `ttlMs` at most — it lapses by itself if the worker dies holding it. */
  lock: (email: string, holder: string, ttlMs: number) => Promise<(() => Promise<void>) | null>;
  /** How long a job waits before it tries a busy account again. */
  lockRetryMs: number;
  ledger: LedgerModel;
  queueName: string;
  linkPauseMs: number;
  accountPauseMs: number;
}

export const couponDefaults: CouponDeps = {
  listAccounts: async (userId, emails) => {
    const rows = await PlatformId.find({
      userId,
      platform: "flipkart",
      status: "logged_in",
      sessionSavedAt: { $ne: null },
      $or: [{ "sessionState.cookies.0": { $exists: true } }, { "sessionCookies.0": { $exists: true } }],
      ...(emails ? { email: { $in: emails } } : {}),
    })
      .select("email")
      .sort({ createdAt: 1 })
      .lean();
    return rows.map((r) => ({ email: r.email, accountId: String(r._id) }));
  },
  loadSession: resolveLoggedInSession,
  openContext: (cookies) => openOrderApiSession(cookies),
  lock: lockAccount,
  lockRetryMs: LOCK_RETRY_MS,
  ledger: OrderJobRun,
  queueName: COUPON_QUEUE,
  linkPauseMs: LINK_PAUSE_MS,
  accountPauseMs: ACCOUNT_PAUSE_MS,
};

const jobIdOf = (requestId: string, email: string) => `${requestId}-${email}`;

// ── shape ───────────────────────────────────────────────────────────────────

type CouponRequestRow = Partial<IOrderCouponRequest> & { _id: unknown };

function publicCouponTask(r: CouponRequestRow, t: Partial<IOrderCouponTask> & { _id: unknown }) {
  return {
    id: String(t._id),
    request_id: String(r._id),
    platform: r.platform || "FLIPKART",
    task_type: "company_coupon_apply",
    source: r.source || "user",
    status: t.status,
    email: t.email || "",
    order_id: null,
    unit_id: null,
    triggered_by: r.createdBy || "",
    created_at: t.createdAt ?? r.createdAt ?? null,
    started_at: t.startedAt ?? null,
    finished_at: t.finishedAt ?? null,
    duration_seconds: t.startedAt && t.finishedAt ? Math.round((new Date(t.finishedAt).getTime() - new Date(t.startedAt).getTime()) / 10) / 100 : null,
    error: t.error || "",
    error_code: t.errorClass || "",
    note: t.note || "",
    results: (t.results || []).map((x) => ({
      link: x.link,
      outcome: x.outcome,
      error_code: x.errorClass || "",
      message: x.message || "",
      final_url: x.finalUrl || "",
      http_status: x.httpStatus || 0,
      login_bounce: Boolean(x.loginBounce),
      hops: (x.hops || []).map((h) => ({ status: h.status, url: h.url })),
      title: x.title || "",
      excerpt: x.excerpt || "",
      ms: x.ms || 0,
    })),
  };
}

/** The request in the reference's batch shape. */
export function publicCouponRequest(r: CouponRequestRow) {
  const counts = { ...emptyCounts(0), ...(r.counts || {}) };
  return {
    request_id: String(r._id),
    platform: r.platform || "FLIPKART",
    task_type: "company_coupon_apply",
    source: r.source || "user",
    status: r.status || "queued",
    triggered_by: r.createdBy || "",
    parent_id: r.parentId || null,
    retry_of: r.retryOf || null,
    params: r.params || {},
    coupon_links: r.links || [],
    message: r.message || "",
    created_at: r.createdAt ?? null,
    started_at: r.startedAt ?? null,
    finished_at: r.finishedAt ?? null,
    counts,
    ...requestProgress(counts, r.startedAt, r.finishedAt),
    can_cancel: r.status === "queued" || r.status === "in_progress",
    host: r.host || "",
    worker_host: r.workerHost || "",
    /** Said on every answer: success here means the link opened, not that a coupon is known to be active. */
    activation_verified: false,
  };
}

// ── counts and the ledger row ───────────────────────────────────────────────

async function recount(requestId: Types.ObjectId) {
  const rows = await OrderCouponTask.aggregate<{ _id: RequestTaskStatus; n: number }>([{ $match: { requestId } }, { $group: { _id: "$status", n: { $sum: 1 } } }]);
  const counts = emptyCounts(0);
  for (const r of rows) {
    counts.total += r.n;
    const key = countKeyOf(r._id);
    counts[key] = (counts[key] || 0) + r.n;
  }
  return counts;
}

const countedAt = new Map<string, number>();

/** Bring the request's counts and its ledger row in line with the tasks. Throttled while a run is going; never when something ends. */
async function syncCounts(requestId: string, deps: CouponDeps, opts: { final?: boolean; throttle?: boolean; parked?: string } = {}) {
  if (opts.throttle) {
    if (Date.now() - (countedAt.get(requestId) || 0) < COUNTS_EVERY_MS) return;
  }
  countedAt.set(requestId, Date.now());
  const id = new Types.ObjectId(requestId);
  const counts = await recount(id);
  // Several jobs recount at once and their writes can land out of order: a count is stored
  // only when it knows of at least as many finished tasks as the one already there.
  const finished = { $add: ["$counts.success", "$counts.failed", "$counts.cancelled", "$counts.aborted", "$counts.timed_out"] };
  await OrderCouponRequest.updateOne({ _id: id, $expr: { $lte: [finished, finishedCount(counts)] } }, { $set: { counts } });
  const row = await OrderCouponRequest.findById(id).select("userId status counts startedAt finishedAt message").lean();
  if (!row) return;
  const failedItems = opts.final
    ? (await OrderCouponTask.find({ requestId: id, status: { $in: ["failed", "timedout", "aborted"] } }).select("email error status").limit(500).lean()).map((t) => ({ key: t.email, reason: t.error || t.status }))
    : undefined;
  await writeRequestRun(
    { userId: row.userId, type: "coupons", refId: requestId },
    { status: row.status, counts: { ...emptyCounts(0), ...row.counts }, startedAt: row.startedAt, finishedAt: row.finishedAt, message: row.status === "queued" || row.status === "in_progress" ? "" : row.message, parked: opts.parked, failedItems },
    deps.ledger,
    { final: row.status === "completed" || row.status === "cancelled" }
  );
  if (opts.final) countedAt.delete(requestId);
}

// ── create / read / stop ────────────────────────────────────────────────────

export async function createCouponRequest(userId: string, input: { links: unknown; emails?: unknown; by?: string; source?: string }, overrides: Partial<CouponDeps> = {}) {
  const deps = { ...couponDefaults, ...overrides };
  if (!COMPANY_COUPONS_ENABLED) throw new RequestInputError("Company Coupons is switched off on this server (COMPANY_COUPONS_ENABLED=0)", 503, "coupons_disabled");
  const links = parseCouponLinks(input.links);
  assertCouponLinks(links);
  const emails = distinct(splitValues(input.emails).map((e) => e.toLowerCase()));
  assertValueLimit("Email addresses", emails);
  const selection = emails.length ? ("email_ids" as const) : ("all_accounts" as const);

  const accounts = await deps.listAccounts(userId, emails.length ? emails : undefined);
  const loggedIn = new Map(accounts.map((a) => [a.email, a]));
  // An email that was asked for and cannot be used still gets its task — failed, with the reason.
  const refused = emails.filter((e) => !loggedIn.has(e));
  if (!accounts.length && !refused.length) throw new RequestInputError("No logged-in Flipkart account in this workspace. Login a Platform ID first.");

  const now = new Date();
  const request = await OrderCouponRequest.create({
    userId: new Types.ObjectId(userId),
    platform: "FLIPKART",
    source: input.source || "user",
    createdBy: String(input.by || "").trim(),
    status: accounts.length ? "queued" : "completed",
    message: accounts.length ? "" : "None of these accounts is logged in",
    links,
    params: { coupon_links_count: links.length, email_ids_count: emails.length, selection_method: selection, accounts_count: accounts.length },
    counts: countsOf([...accounts.map(() => "queued" as const), ...refused.map(() => "failed" as const)]),
    queue: deps.queueName,
    systemId: config.systemId,
    host: hostName(),
    finishedAt: accounts.length ? null : now,
  });
  const requestId = String(request._id);
  const base = { requestId: request._id, userId: request.userId, note: "", results: [], startedAt: null };
  const tasks = [
    ...accounts.map((a) => ({ ...base, email: a.email, accountId: a.accountId, status: "queued" as const, errorClass: "", error: "", finishedAt: null })),
    ...refused.map((email) => ({ ...base, email, accountId: "", status: "failed" as const, errorClass: "not_logged_in", error: ERROR_CLASSES.not_logged_in, finishedAt: now })),
  ];
  for (let i = 0; i < tasks.length; i += 1000) await OrderCouponTask.insertMany(tasks.slice(i, i + 1000), { ordered: false });

  await openRequestRun(
    { userId, type: "coupons", refId: requestId, trigger: "user", by: String(input.by || "").trim(), total: tasks.length, message: `Coupon apply · ${accounts.length} account(s) waiting for the worker`, meta: { request: "company_coupon_apply", platform: "FLIPKART", params: request.params, queue: deps.queueName, activation_verified: false } },
    deps.ledger
  ).catch(() => undefined);

  if (!accounts.length) {
    await syncCounts(requestId, deps, { final: true });
  } else {
    try {
      const queue = getCouponQueue(deps.queueName);
      for (let i = 0; i < accounts.length; i += 500) {
        await queue.addBulk(accounts.slice(i, i + 500).map((a) => ({ name: "account", data: { requestId, email: a.email }, opts: { jobId: jobIdOf(requestId, a.email) } })));
      }
    } catch (err) {
      const reason = `Could not queue the request: ${err instanceof Error ? err.message.split("\n")[0].slice(0, 160) : String(err)}`;
      await endOpenTasks(request._id, { status: "failed", errorClass: "internal_error", error: ERROR_CLASSES.internal_error }, reason);
      await syncCounts(requestId, deps, { final: true });
      throw new RequestInputError(reason, 503, "queue_unavailable");
    }
  }
  console.log(`[coupons] request ${requestId}: ${links.length} link(s) × ${accounts.length} account(s) queued${refused.length ? `, ${refused.length} not logged in` : ""}${input.by ? ` (${input.by})` : ""}`);
  return { ...publicCouponRequest(request.toObject()), tasks_created: tasks.length, accounts: accounts.length, not_logged_in: refused.length, selection_method: selection };
}

/** End every queued (and, when asked, running) task of an open request, and close the request. False when it was no longer open. */
async function endOpenTasks(
  id: Types.ObjectId,
  patch: { status: RequestTaskStatus; errorClass: string; error: string },
  message: string,
  opts: { onlyIfQueued?: boolean; requestStatus?: "completed" | "cancelled"; alsoRunning?: { status: RequestTaskStatus; error: string }; stoppedBy?: string } = {}
) {
  const now = new Date();
  const closed = await OrderCouponRequest.updateOne(
    { _id: id, status: opts.onlyIfQueued ? "queued" : { $in: [...OPEN] } },
    { $set: { status: opts.requestStatus || "completed", message, finishedAt: now, ...(opts.stoppedBy != null ? { stoppedBy: opts.stoppedBy } : {}) } }
  );
  if (!closed.modifiedCount) return false;
  await OrderCouponTask.updateMany({ requestId: id, status: "queued" }, { $set: { ...patch, finishedAt: now } });
  if (opts.alsoRunning) await OrderCouponTask.updateMany({ requestId: id, status: "in_progress" }, { $set: { ...opts.alsoRunning, errorClass: "", finishedAt: now } });
  return true;
}

export async function getCouponRequest(userId: string, requestId: string, opts: { status?: string; limit?: number; skip?: number } = {}) {
  if (!Types.ObjectId.isValid(requestId)) return null;
  const row = await OrderCouponRequest.findOne({ _id: requestId, userId }).lean();
  if (!row) return null;
  // Twenty thousand tasks do not go out in one answer: a page of them, failures first.
  const limit = Math.min(500, Math.max(1, Math.floor(Number(opts.limit) || 200)));
  const filter = { requestId: row._id, ...(opts.status ? { status: opts.status } : {}) };
  const tasks = await OrderCouponTask.find(filter).sort({ status: 1, email: 1 }).skip(Math.max(0, Math.floor(Number(opts.skip) || 0))).limit(limit).lean();
  const [request] = await withLiveness([{ row: publicCouponRequest(row), queue: row.queue || COUPON_QUEUE }], "account");
  return { ...request, tasks: tasks.map((t) => publicCouponTask(row, t)), tasks_page: { limit, skip: Math.max(0, Math.floor(Number(opts.skip) || 0)), returned: tasks.length } };
}

/** Like the single read, each row says whether a worker is behind an open request (`worker_alive`, `stalled`, `stalled_message`). */
export async function listCouponRequests(userId: string, limit = 20) {
  const rows = await OrderCouponRequest.find({ userId }).sort({ createdAt: -1 }).limit(Math.min(100, Math.max(1, limit))).lean();
  return withLiveness(rows.map((r) => ({ row: publicCouponRequest(r), queue: r.queue || COUPON_QUEUE })), "account");
}

/** Stop: accounts still queued → cancelled "Cancelled before it started", their jobs removed; an account being worked finishes its links. */
export async function stopCouponRequest(userId: string, requestId: string, by = "", overrides: Partial<CouponDeps> = {}) {
  const deps = { ...couponDefaults, ...overrides };
  if (!Types.ObjectId.isValid(requestId)) return null;
  const row = await OrderCouponRequest.findOne({ _id: requestId, userId }).select("status queue").lean();
  if (!row) return null;
  let removed = 0;
  if (row.status === "queued" || row.status === "in_progress") {
    const id = new Types.ObjectId(requestId);
    const queueName = row.queue || deps.queueName;
    const waiting = (await OrderCouponTask.find({ requestId: id, status: { $in: [...OPEN] } }).select("email").lean()).map((t) => t.email);
    const alive = await workerAlive(queueName);
    const stopped = await endOpenTasks(id, { status: "cancelled", errorClass: "", error: CANCELLED_BEFORE_START }, `Stopped by ${by || "a user"}`, {
      requestStatus: "cancelled",
      stoppedBy: by,
      alsoRunning: alive ? undefined : { status: "aborted", error: ABORTED_WHILE_RUNNING },
    });
    if (stopped) {
      const queue = getCouponQueue(queueName);
      for (let i = 0; i < waiting.length; i += 50) {
        const gone = await Promise.all(
          waiting.slice(i, i + 50).map(async (email) => {
            // A job a worker holds cannot be removed (it throws); it ends by itself.
            const job = await queue.getJob(jobIdOf(requestId, email)).catch(() => null);
            return job ? job.remove().then(() => true, () => false) : false;
          })
        );
        removed += gone.filter(Boolean).length;
      }
      await syncCounts(requestId, deps, { final: true });
      console.log(`[coupons] request ${requestId} stopped by ${by || "a user"} — ${removed} account job(s) removed from the queue`);
    }
  }
  const updated = await OrderCouponRequest.findById(requestId).lean();
  return updated ? { ...publicCouponRequest(updated), removed } : null;
}

// ── worker ──────────────────────────────────────────────────────────────────

/** Done once no task is queued or running — the last job to finish closes the request and its ledger row. */
async function settleCouponRequest(requestId: string, deps: CouponDeps) {
  const id = new Types.ObjectId(requestId);
  if (await OrderCouponTask.exists({ requestId: id, status: { $in: [...OPEN] } })) {
    await syncCounts(requestId, deps, { throttle: true });
    return;
  }
  const res = await OrderCouponRequest.updateOne({ _id: id, status: { $in: [...OPEN] } }, { $set: { status: "completed", finishedAt: new Date() } });
  await syncCounts(requestId, deps, { final: true });
  if (res.modifiedCount) {
    const row = await OrderCouponRequest.findById(id).select("counts").lean();
    console.log(`[coupons] request ${requestId} finished: ${row?.counts.success ?? 0} account(s) opened every link, ${row?.counts.failed ?? 0} failed`);
  }
}

/** One account of one request: every coupon link, one after another, with the account's session. */
export async function runCouponJob(job: Job<CouponJobData>, token: string | undefined, overrides: Partial<CouponDeps> = {}): Promise<void> {
  const deps = { ...couponDefaults, ...overrides };
  const { requestId } = job.data;
  const email = String(job.data.email || "").toLowerCase();
  if (!Types.ObjectId.isValid(requestId)) return;
  const id = new Types.ObjectId(requestId);
  const request = await OrderCouponRequest.findById(id).select("status links").lean();
  if (!request || (request.status !== "queued" && request.status !== "in_progress")) return;

  // The same lock checkout, cancel, update and the order sync take: one piece of work per account at a time.
  // Held only as long as these links can take (a minute, plus 45 s a link), not the lock's usual 15 minutes:
  // a worker that dies holding it must not keep the account from checkout that long.
  const lockMs = Math.min(15 * 60 * 1000, 60_000 + request.links.length * 45_000);
  const unlock = await deps.lock(email, `coupon:${requestId}:${config.workerId}`, lockMs);
  if (!unlock) {
    await job.moveToDelayed(Date.now() + deps.lockRetryMs, token);
    throw new DelayedError();
  }
  let ctx: APIRequestContext | null = null;
  try {
    // "in_progress" is also taken: a task left that way belongs to a run of this job that died.
    const claimed = await OrderCouponTask.findOneAndUpdate({ requestId: id, email, status: { $in: [...OPEN] } }, { $set: { status: "in_progress", startedAt: new Date(), results: [], error: "", errorClass: "", note: "" } }, { new: true })
      .select("_id")
      .lean();
    if (!claimed) return; // cancelled, or ended by someone else
    const started = new Date();
    await OrderCouponRequest.updateOne({ _id: id, status: "queued" }, { $set: { status: "in_progress", startedAt: started, workerHost: hostName() } });
    await syncCounts(requestId, deps, { throttle: true });

    const end = async (patch: { status: "success" | "failed"; errorClass: string; error: string; note: string }, results: ICouponLinkResult[]) => {
      await OrderCouponTask.updateOne({ _id: claimed._id, status: "in_progress" }, { $set: { ...patch, results, finishedAt: new Date() } });
    };

    const session = await deps.loadSession(email);
    if (!session.ok) {
      await end({ status: "failed", errorClass: "not_logged_in", error: ERROR_CLASSES.not_logged_in, note: session.reason }, []);
      return;
    }
    try {
      ctx = await deps.openContext(session.cookies);
    } catch (err) {
      await end({ status: "failed", errorClass: "not_logged_in", error: ERROR_CLASSES.not_logged_in, note: err instanceof Error ? err.message.split("\n")[0].slice(0, 160) : String(err) }, []);
      return;
    }

    const results: ICouponLinkResult[] = [];
    let bounced = false;
    for (let n = 0; n < request.links.length; n++) {
      const link = request.links[n];
      if (bounced) {
        // The session was sent to login on an earlier link: the rest would only repeat it.
        results.push({ link, outcome: "skipped", errorClass: "", message: "Not tried — the session was sent to login on an earlier link", finalUrl: "", httpStatus: 0, loginBounce: false, hops: [], title: "", excerpt: "", ms: 0 });
        continue;
      }
      const result = await openCouponLink(ctx, link, deps);
      results.push(result);
      if (result.errorClass === "not_logged_in") bounced = true;
      if (result.errorClass === "rate_limited") await sleep(5000);
      if (n < request.links.length - 1 && deps.linkPauseMs) await sleep(deps.linkPauseMs);
    }
    await end(couponTaskOutcome(results), results);
  } finally {
    await ctx?.dispose().catch(() => undefined);
    await unlock();
    await settleCouponRequest(requestId, deps);
    if (deps.accountPauseMs) await sleep(deps.accountPauseMs);
  }
}

// ── reapers ─────────────────────────────────────────────────────────────────

/** Worker side: an open request of this deployment, quiet a while, gets a job back for every account with none on the queue. */
export async function requeueStrandedCouponJobs(opts: { staleMs?: number; queueName?: string } = {}): Promise<number> {
  const queueName = opts.queueName || COUPON_QUEUE;
  const before = new Date(Date.now() - (opts.staleMs ?? STRANDED_AFTER_MS));
  const open = await OrderCouponRequest.find({ status: { $in: [...OPEN] }, systemId: config.systemId, queue: queueName, updatedAt: { $lte: before } }).select("_id").lean();
  if (!open.length) return 0;
  const queue = getCouponQueue(queueName);
  let requeued = 0;
  for (const r of open) {
    const requestId = String(r._id);
    const emails = (await OrderCouponTask.find({ requestId: r._id, status: { $in: [...OPEN] } }).select("email").lean()).map((t) => t.email);
    for (const email of emails) {
      if (await queue.getJob(jobIdOf(requestId, email)).catch(() => null)) continue;
      await queue.add("account", { requestId, email }, { jobId: jobIdOf(requestId, email) }).catch(() => undefined);
      requeued++;
    }
    // No task left and still open: its last job died between its task and the close.
    if (!emails.length) await settleCouponRequest(requestId, { ...couponDefaults, queueName });
  }
  if (requeued) console.log(`[coupons] re-enqueued ${requeued} stranded account job(s) across ${open.length} open request(s)`);
  return requeued;
}

let reaping = false;

/** Start the periodic resume sweep (worker, coupon role). */
export function startCouponRequestReaper(queueName = COUPON_QUEUE) {
  const tick = (staleMs?: number) => {
    if (reaping) return;
    reaping = true;
    void requeueStrandedCouponJobs({ staleMs, queueName })
      .catch((e) => console.error("[coupons] reaper:", e instanceof Error ? e.message : e))
      .finally(() => (reaping = false));
  };
  tick(0);
  const t = setInterval(() => tick(), REAP_EVERY_MS);
  t.unref();
  return t;
}

/**
 * API side, as for the invoice batch: with no worker heartbeat on the queue, a
 * request nobody started within `pickupMs` is failed — "The worker has not
 * picked this up — restart the worker" — and one that was running keeps its
 * tasks for a resume but no longer reads `running` in the ledger.
 */
export async function failUnclaimedCouponRequests(opts: { pickupMs?: number; queueName?: string; ledger?: LedgerModel } = {}): Promise<{ failed: number; parked: number }> {
  const queueName = opts.queueName || COUPON_QUEUE;
  const deps = { ...couponDefaults, queueName, ...(opts.ledger ? { ledger: opts.ledger } : {}) };
  const out = { failed: 0, parked: 0 };
  const before = new Date(Date.now() - (opts.pickupMs ?? REQUEST_PICKUP_MS));
  const waiting = await OrderCouponRequest.find({ status: { $in: [...OPEN] }, systemId: config.systemId, queue: queueName, updatedAt: { $lte: before } }).select("_id userId status counts").lean();
  if (!waiting.length || (await workerAlive(queueName))) return out;
  const queue = getCouponQueue(queueName);
  for (const r of waiting) {
    const requestId = String(r._id);
    if (r.status === "queued") {
      const emails = (await OrderCouponTask.find({ requestId: r._id, status: "queued" }).select("email").lean()).map((t) => t.email);
      const ended = await endOpenTasks(r._id as Types.ObjectId, { status: "failed", errorClass: "internal_error", error: WORKER_NOT_PICKED_UP }, WORKER_NOT_PICKED_UP, { onlyIfQueued: true });
      if (!ended) continue;
      for (let i = 0; i < emails.length; i += 50) {
        await Promise.all(emails.slice(i, i + 50).map(async (email) => (await queue.getJob(jobIdOf(requestId, email)).catch(() => null))?.remove().catch(() => undefined)));
      }
      await syncCounts(requestId, deps, { final: true, parked: WORKER_NOT_PICKED_UP });
      out.failed++;
      console.warn(`[coupons] request ${requestId} failed: ${WORKER_NOT_PICKED_UP}`);
    } else {
      const left = (r.counts?.queued || 0) + (r.counts?.in_progress || 0);
      const parked = await deps.ledger.updateOne(
        { userId: r.userId, type: "coupons", refId: requestId, status: "running" },
        { $set: { status: "failed", finishedAt: new Date(), message: `The worker stopped with ${left} account(s) left — restart the worker to resume` } }
      );
      if (parked.modifiedCount) out.parked++;
    }
  }
  return out;
}

/** Start the watchdog (API process). */
export function startCouponRequestWatchdog() {
  let busy = false;
  const t = setInterval(() => {
    if (busy) return;
    busy = true;
    void failUnclaimedCouponRequests()
      .catch((e) => console.error("[coupons] watchdog:", e instanceof Error ? e.message : e))
      .finally(() => (busy = false));
  }, WATCHDOG_EVERY_MS);
  t.unref();
  return t;
}
