import { getOrderFetchJob, OrderFetchStoppedError, reserveOrderFetch, startOrderFetch, type OrderFetchStartOptions } from "./orderFetch.js";
import { createQueuedRun, failRunById, stopRunById } from "./orderJobRuns.js";
import { hostName } from "./requestCommon.js";
import { planFetch, queueUpdateAfterFetch, type FetchBody } from "./startRequests.js";

/**
 * Start a fetch from a parsed body — what POST /api/orders/fetch/trigger does,
 * and what Batch requests → Retry does for a fetch (services/syncActions.ts),
 * so both go through the one fetch slot and answer the same refusals:
 * "Order fetch is already running", "None of the N email(s) are logged in…".
 *
 * Over FETCH_INLINE_START_MAX accounts the answer is given at once
 * (`preparing: true`) with the id of a queued ledger row, and the run starts
 * when its account list has been read.
 */

/** Up to this many accounts the trigger answers once the run is going; a longer list is prepared in the background (≈0.1 s per account). */
export const FETCH_INLINE_START_MAX = 100;

export interface FetchTriggerExtra {
  /** The request log's source word when it is not the trigger ("system-retry"). */
  source?: string;
  /** Recorded on the run beside what planFetch records (a retry: retry_of, retry_statuses). */
  params?: Record<string, unknown>;
}

export async function triggerFetch(userId: string, body: FetchBody, extra: FetchTriggerExtra = {}) {
  // The workspace's one fetch slot, held from the first check until startOrderFetch has taken over:
  // working out who to fetch takes a second or two, and a second trigger in that window is refused, not answered.
  const release = reserveOrderFetch(userId);
  try {
    const plan = await planFetch(userId, body);
    if (!plan.loggedIn) throw new Error(`None of the ${plan.emails.length} email(s) are logged in. Login them in platform id first.`);
    const params = { ...plan.params, ...(extra.params || {}) };
    const opts: OrderFetchStartOptions = {
      invoices: body.invoices,
      trigger: body.trigger,
      by: body.by,
      params,
      reserved: true,
      source: extra.source,
      onFinished: body.runUpdateAfterFetch
        ? async (info) => {
            await queueUpdateAfterFetch(userId, { runId: info.runId, emails: info.emails, selection: plan.selection, cancelled: info.cancelled, by: body.by, trigger: body.trigger });
          }
        : undefined,
    };
    const preparing = plan.emails.length > FETCH_INLINE_START_MAX;
    let requestId = "";
    if (preparing) {
      // The id is handed out now; the run adopts this row when its account list is ready.
      requestId = await createQueuedRun(userId, "fetch", {
        trigger: body.trigger,
        by: body.by,
        total: plan.loggedIn,
        message: `Preparing ${plan.loggedIn} account(s) — the fetch starts when their saved units are read`,
        meta: { platform: body.platform, params, emails_count: plan.loggedIn, host: hostName(), pid: process.pid, ...(extra.source ? { source: extra.source } : {}) },
      });
      void startOrderFetch(userId, plan.emails, plan.sinceDate, { ...opts, runId: requestId }).catch((err) => {
        const reason = err instanceof Error ? err.message : String(err);
        console.warn(`[orders-fetch] fetch ${requestId} could not start: ${reason}`);
        // Force kill during the preparation is a stop, not a failure.
        return err instanceof OrderFetchStoppedError ? stopRunById(requestId, reason) : failRunById(requestId, `Could not start: ${reason}`);
      });
    } else {
      requestId = (await startOrderFetch(userId, plan.emails, plan.sinceDate, opts)).runId;
    }
    const job = getOrderFetchJob(userId);
    const queued = preparing ? plan.loggedIn : job.total;
    return {
      message: "Order fetch triggered successfully",
      platform: body.platform,
      selection_method: plan.selection,
      total_accounts_found: plan.emails.length,
      valid_accounts_queued: queued,
      tasks_created: queued,
      windows: preparing ? null : job.windows,
      fetch_orders_after_date: plan.afterDate,
      fetch_orders_after_date_source: plan.afterDateSource,
      invoices: body.invoices,
      request_id: requestId,
      params,
      preparing,
      timestamp: Math.floor(Date.now() / 1000),
    };
  } finally {
    // startOrderFetch marks the workspace as preparing before its first await, so nothing slips in between.
    release();
  }
}
