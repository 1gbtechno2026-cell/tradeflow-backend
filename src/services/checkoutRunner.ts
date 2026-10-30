import type { Page } from "playwright";
import { CheckoutJob } from "../models/CheckoutJob.js";
import { FlipkartCheckout, OutOfStockPincodeError } from "../automation/FlipkartCheckout.js";
import { FlipkartApiWatcher } from "../automation/FlipkartApiWatcher.js";
import { PaymentApiWatcher, type PlacedOrderDetails } from "../automation/PaymentApiWatcher.js";
import { closeReporter, openReporter, reporterFor } from "./jobReporter.js";
import {
  isBatchCancelled,
  readBatchProgress,
  refundBatchAttempt,
  releaseBatchReservation,
  reserveBatchSlot,
  settleBatchIfCancelling,
} from "./batchCounters.js";
import { claimEgress, isProxyError, probeExitIp, recordProxyUse, reportProxyFailure } from "./proxyPool.js";
import { currentProxyMode } from "./proxyMode.js";
import { setNavigationTimeoutScale } from "../automation/helpers.js";
import {
  blockFlipkartLogout,
  desktopContext,
  flipkartLoginUrl,
  launchMobileBrowser,
  launchStealthBrowser,
  mobileContext,
  restoreFlipkartSession,
  sleep,
} from "./browser.js";
import { config } from "../config.js";
import { enqueueCheckoutJob, requeueCheckoutJob } from "./checkoutEnqueue.js";
import { jobClassFor } from "./jobClass.js";
import { availablePhoneCapacity } from "./employeePhoneLease.js";
import { cardPoolAdmission } from "./cardPool.js";
import { getRedis } from "../lib/redis.js";
import { resolveLoggedInSession } from "./sessionStore.js";
import { lockAccount } from "./accountLock.js";
import { enqueueOrderSync } from "./orderSync.js";
import { resolveCheckoutSettings } from "./checkoutSettings.js";
import { runPaymentPhase } from "./paymentPhase.js";
import { runHttpCheckout } from "./httpCheckout.js";
import { acquireChromeFallbackSlot } from "./chromeFallbackGate.js";
import { FlipkartNetworkObserver } from "../automation/FlipkartNetworkObserver.js";
import {
  CheckoutFailure,
  classifyThrownMessage,
  failureFields,
} from "./checkoutErrors.js";
import type { CheckoutJobData, JobRequestSnapshot, JobResultSnapshot, JobStatus, LogLevel } from "../types.js";

/** Inside a run, lines go through the job's JobReporter (batched: one write per
 *  flush instead of one per line). Outside one — claim-time messages before the
 *  reporter exists — they are written directly, as before. */
async function appendLog(jobId: string, level: LogLevel, message: string, step?: string) {
  const reporter = reporterFor(jobId);
  if (reporter) {
    reporter.log(level, message, step);
    return;
  }
  console.log(`[${jobId}] [${level}]${step ? ` [${step}]` : ""} ${message}`);
    const update: Record<string, unknown> = {
      $push: { logs: { at: new Date(), level, step, message } },
    };
    if (step) update.$set = { step };
    await CheckoutJob.updateOne({ _id: jobId }, update);
}

async function patchResult(jobId: string, patch: Partial<JobResultSnapshot>) {
  const reporter = reporterFor(jobId);
  if (reporter) {
    reporter.patch(patch);
    return;
  }
  const set: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(patch)) {
    if (value === undefined) continue;
    set[`result.${key}`] = value;
  }
  if (Object.keys(set).length) await CheckoutJob.updateOne({ _id: jobId }, { $set: set });
}

/**
 * The placed order's numbers, as result fields. From Flipkart's own gateway
 * and confirmation responses (PaymentApiWatcher.placedOrderDetails) — the
 * confirmation PAGE says only "Order Placed" behind a scratch card.
 */
export function orderDetailFields(d: PlacedOrderDetails | null | undefined, placedAt = new Date()): Partial<JobResultSnapshot> {
  if (!d) return {};
  const promiseDate =
    d.promiseDays != null ? new Date(placedAt.getTime() + d.promiseDays * 24 * 60 * 60 * 1000) : undefined;
  return {
    ...(d.transactionAmount ? { transactionAmount: d.transactionAmount } : {}),
    ...(d.cartAfterCardOffer ? { cartAfterCardOffer: d.cartAfterCardOffer } : {}),
    ...(d.paymentFee ? { paymentFee: d.paymentFee } : {}),
    ...(d.paymentFeeDetails ? { paymentFeeDetails: d.paymentFeeDetails } : {}),
    ...(d.shippingAmount ? { shippingAmount: d.shippingAmount } : {}),
    ...(d.discountPct ? { discountPct: d.discountPct } : {}),
    ...(d.mrp ? { mrp: d.mrp } : {}),
    // The product page's price read is unreliable (empty on the keychain);
    // Flipkart's own unit price from the confirmation data fills the gap.
    ...(d.unitPrice ? { unitPrice: d.unitPrice } : {}),
    ...(d.bankTransactionId ? { bankTransactionId: d.bankTransactionId } : {}),
    ...(d.pgTransactionId ? { pgTransactionId: d.pgTransactionId } : {}),
    ...(d.bankName ? { bankName: d.bankName } : {}),
    ...(d.cardBrand ? { cardBrand: d.cardBrand } : {}),
    ...(d.supercoinsApplied ? { supercoinsApplied: d.supercoinsApplied } : {}),
    ...(d.giftCardApplied ? { giftCardApplied: d.giftCardApplied } : {}),
    ...(d.promiseDays != null ? { promiseDays: d.promiseDays, promiseDate } : {}),
    ...(d.orderStatus ? { orderStatus: d.orderStatus } : {}),
    ...(d.sellerName ? { sellerName: d.sellerName } : {}),
  };
}

function batchLimits(data: CheckoutJobData) {
  const quantityPerOrder = data.quantityPerOrder || data.quantity;
  const totalQuantity = data.totalQuantity || quantityPerOrder;
  const totalAttempts = data.totalAttempts || 1;
  return { quantityPerOrder, totalQuantity, totalAttempts };
}

async function finishWithoutChrome(
  jobId: string,
  status: JobStatus,
  step: string,
  message: string,
  level: LogLevel = "info",
  extra?: { failedStep?: string; failure?: CheckoutFailure }
) {
  const mapped = extra?.failure;
  await reporterFor(jobId)?.beforeTerminal();
  await CheckoutJob.updateOne(
    { _id: jobId, status: { $ne: "cancelled" } },
    {
      $set: {
        status,
        step,
        failedStep: extra?.failedStep || mapped?.failedStep || (status.startsWith("failed") ? step : ""),
        error: status === "skipped" || status.startsWith("failed") ? message : "",
        failureMessage: mapped ? message : "",
        ...(mapped ? failureFields(mapped) : {}),
        completedAt: new Date(),
      },
      $push: { logs: { at: new Date(), level, step, message } },
    }
  );
  console.log(`[${jobId}] [${level}] [${step}] ${message}`);
}

async function maybeEnqueueRetry(
  data: CheckoutJobData,
  userId: string,
  reason: string,
  request?: JobRequestSnapshot
) {
  const latest = await CheckoutJob.findById(data.jobId).select("status");
  if (latest?.status === "cancelled") return;
  const { quantityPerOrder, totalQuantity, totalAttempts } = batchLimits(data);
  const progress = await readBatchProgress(data.batchId, totalQuantity, totalAttempts);
  if (isBatchCancelled(progress.status)) {
    console.log(`[${data.jobId}] [info] [batch] batch cancelled — no retry`);
    return;
  }
  if (progress.status === "completed" || progress.purchasedQuantity >= totalQuantity) {
    console.log(
      `[${data.jobId}] [info] [batch] target reached purchased=${progress.purchasedQuantity}/${totalQuantity} — no retry`
    );
    return;
  }
  if (progress.status === "exhausted" || progress.attemptsUsed >= totalAttempts) {
    console.log(
      `[${data.jobId}] [info] [batch] attempt budget exhausted ${progress.attemptsUsed}/${totalAttempts} — no retry`
    );
    return;
  }
  const { job } = await enqueueCheckoutJob({
    userId,
    batchId: data.batchId,
    email: data.email,
    productUrl: data.productUrl,
    quantityPerOrder,
    totalQuantity,
    totalAttempts,
    quantityMode: data.quantityMode,
    cartAmountLimit: data.cartAmountLimit,
    deliverySlaDays: data.deliverySlaDays,
    gstMandatory: data.gstMandatory ?? request?.gstMandatory,
    address: data.address,
    request,
    isRetry: true,
    // Carried from the payload we were handed, not re-read from Mongo: the
    // persisted snapshot's cards are masked, so a retry that rebuilt them from
    // `request` would queue a job holding "54XXXXXXXX000759" and fail at the
    // bank with nothing in the logs to explain why.
    paymentMode: data.paymentMode,
    cardType: data.cardType,
    authType: data.authType,
    corporateId: data.corporateId,
    cardMaxUsage: data.cardMaxUsage,
    cards: data.cards,
  });
  console.log(
    `[${data.jobId}] [info] [batch] retry queued job=${job._id} after ${reason} (purchased=${progress.purchasedQuantity}/${totalQuantity} attempts=${progress.attemptsUsed}/${totalAttempts})`
  );
}

/**
 * Session safety:
 * - PlatformId cookies are read-only. This worker never updates/deletes them.
 * - Chrome is a throwaway window; we never click Flipkart Logout.
 * - Closing the window only drops local cookies in that process.
 */
/**
 * Exercise the payment phase without touching a real bank page.
 *
 * Defaults to ON deliberately. Flipkart's payments-page selectors are not written
 * yet (paymentPhase.fillFlipkartCardForm), so a live run would stop mid-flow with
 * a card already chosen; and the first real charge should be a decision someone
 * makes explicitly, not something that happens because a default flipped. Set
 * PAYMENT_DRY_RUN=false once the selectors land and you mean it.
 */
// Dry run ONLY when asked for explicitly. It used to be the default unless
// PAYMENT_DRY_RUN=false was set — and a machine that forgot the variable (the
// ops box, 2026-10-27) ran every order as a dry run while reporting "paid".
const PAYMENT_DRY_RUN = String(process.env.PAYMENT_DRY_RUN || "").trim().toLowerCase() === "true";
if (PAYMENT_DRY_RUN) console.warn("[checkout] PAYMENT_DRY_RUN=true — orders stop at the payment page and are recorded as dry_run, nothing is placed");

/** Flipkart's E002 page. "Deliver to" present means a real checkout, not the error. */
async function checkoutShowsE002(page: Page): Promise<boolean> {
  const text = String(await page.evaluate(() => document.body?.innerText || "").catch(() => ""));
  return /Something went wrong/i.test(text) && !/Deliver to/i.test(text);
}

/** How quiet a `running` job's heartbeat must go before another worker may take
 *  it. Comfortably longer than HEARTBEAT_MS so a slow page never looks dead. */
const CLAIM_STALE_MS = Number(process.env.CHECKOUT_CLAIM_STALE_MS || 10 * 60 * 1000);
const HEARTBEAT_MS = Number(process.env.CHECKOUT_HEARTBEAT_MS || 60 * 1000);
/** How long an otp-phone job waits before asking again when no handset is free. */
const PHONE_WAIT_MS = Number(process.env.PHONE_WAIT_MS || 20 * 1000);
/** Upper bound on one job's hold of a Flipkart account; released on exit. */

/**
 * Take exclusive ownership of a job, or report that someone else has it.
 *
 * One findOneAndUpdate, so two workers handed the same job cannot both win —
 * which matters because the delivery guarantee is about to change. BullMQ holds a
 * Redis lock per job today, so `findById` then `save()` was safe; SQS Standard is
 * at-least-once BY DESIGN, and the same message WILL occasionally be delivered
 * twice. Without this, the second delivery opens a second Chrome on the same
 * order and the product is bought twice.
 *
 * The filter also readmits a job whose worker died: `running` with a heartbeat
 * older than CLAIM_STALE_MS. Without that clause the claim would be permanent and
 * a killed worker would strand its order forever — BullMQ's stalledInterval does
 * this job today and SQS has no equivalent.
 */
async function claimCheckoutJob(jobId: string, runId: string) {
  const staleBefore = new Date(Date.now() - CLAIM_STALE_MS);
  return CheckoutJob.findOneAndUpdate(
    {
      _id: jobId,
      $or: [
        { status: "queued" },
        // Reclaim only a *silent* running job. heartbeatAt null means it was
        // claimed before this field existed, or claimed and never beat once.
        // NEVER one that had already pressed Pay (paymentStartedAt set): money
        // may have moved, and a second run would buy the product twice. Those
        // go to needs_reconciliation in runCheckoutJob instead.
        { status: "running", heartbeatAt: { $lt: staleBefore }, paymentStartedAt: null },
        { status: "running", heartbeatAt: null, startedAt: { $lt: staleBefore }, paymentStartedAt: null },
      ],
    },
    {
      $set: {
        status: "running",
        step: "session",
        startedAt: new Date(),
        heartbeatAt: new Date(),
        claimedBy: runId,
      },
    },
    { new: true }
  );
}

export async function runCheckoutJob(data: CheckoutJobData) {
  const runId = `${data.jobId}:${Date.now().toString(36)}`;
  const existing = await CheckoutJob.findById(data.jobId).select("status claimedBy");
  if (!existing) throw new Error(`Job ${data.jobId} not found`);
  if (existing.status === "cancelled") return;

  const job = await claimCheckoutJob(data.jobId, runId);
  if (!job) {
    // A worker that died AFTER pressing Pay leaves a silent `running` job with
    // paymentStartedAt set. It must not be re-run — the bank may have charged
    // and Flipkart may have placed the order — so it is parked for a person or
    // for reconciliation against Flipkart's orders, with whatever is known.
    const staleBefore = new Date(Date.now() - CLAIM_STALE_MS);
    const parked = await CheckoutJob.findOneAndUpdate(
      {
        _id: data.jobId,
        status: "running",
        paymentStartedAt: { $ne: null },
        $or: [{ heartbeatAt: { $lt: staleBefore } }, { heartbeatAt: null, startedAt: { $lt: staleBefore } }],
      },
      {
        $set: { status: "needs_reconciliation", step: "needs_reconciliation", completedAt: new Date() },
        $push: {
          logs: {
            at: new Date(),
            level: "error",
            step: "needs_reconciliation",
            message:
              "Worker went silent after Pay was pressed and before Flipkart confirmed. NOT re-run: " +
              "check the account's My Orders / the bank for this attempt before doing anything with this job.",
          },
        },
      },
      { new: true }
    ).select("result.flipkartOrderId");
    if (parked) {
      console.log(`[${data.jobId}] [error] [claim] silent after Pay — parked as needs_reconciliation (order id known: ${parked.result?.flipkartOrderId || "no"})`);
      // Whatever Flipkart made of that attempt goes into Order Units.
      await enqueueOrderSync(data.jobId);
      return;
    }
    // Already finished, or another worker is actively on it. Either way this
    // delivery must not open a browser — dropping it is the correct outcome, not
    // an error, so the message is acknowledged and no attempt is consumed.
    console.log(
      `[${data.jobId}] [info] [claim] not claimable (status=${existing.status}` +
        `${existing.claimedBy ? `, held by ${existing.claimedBy}` : ""}) — dropping this delivery`
    );
    return;
  }

  // Keeps the claim alive for as long as this worker is genuinely working. Also
  // what a future SQS ChangeMessageVisibility heartbeat should ride alongside.
  const heartbeat = setInterval(() => {
    void CheckoutJob.updateOne(
      { _id: data.jobId, claimedBy: runId },
      { $set: { heartbeatAt: new Date() } }
    ).catch(() => undefined);
  }, HEARTBEAT_MS);
  heartbeat.unref?.();
  // All of this job's log lines and result fields go to Mongo through one
  // batched reporter from here on (see jobReporter.ts); closed, with a final
  // flush, whatever way the run ends.
  const reporter = openReporter(data.jobId);
  try {
    return await runClaimedCheckoutJob(data, job);
  } finally {
    clearInterval(heartbeat);
    await closeReporter(data.jobId);
    console.log(`[${data.jobId}] [info] [reporter] ${reporter.writes} batched write(s) for this job`);
    // Hand the order to the sync worker (a few ms, never throws, never waits on
    // Flipkart): paid or failed, it lands in Order Units without a manual fetch.
    await enqueueOrderSync(data.jobId);
  }
}

async function runClaimedCheckoutJob(
  data: CheckoutJobData,
  job: NonNullable<Awaited<ReturnType<typeof claimCheckoutJob>>>
) {

  const { quantityPerOrder, totalQuantity, totalAttempts } = batchLimits(data);

  // ADMISSION, before an attempt is reserved: a job that cannot pay right now
  // — no handset free under its corporate, every card paused — waits, it does
  // not fail, and the batch's attempt budget is untouched. Failing here used to
  // cost an attempt per wait, which at 50 phones and 1,000 orders would have
  // spent the budget on nothing.
  const cls = jobClassFor(data);
  if (cls === "otp-phone" && data.cardType) {
    const cap = await availablePhoneCapacity({ cardTypeName: data.cardType, corporateId: data.corporateId || undefined }).catch(() => null);
    if (cap && cap.claimable <= 0) {
      await requeueCheckoutJob(
        data,
        PHONE_WAIT_MS,
        `No free handset under ${data.corporateId || data.cardType} (${cap.totalOnboarded} onboarded` +
          `${cap.onlineOnly ? ", online only" : ""})`
      );
      return;
    }
  }
  if (cls !== "cod" && data.cards?.length) {
    const pool = await cardPoolAdmission(data.batchId, data.cards.length, data.cardMaxUsage).catch(() => null);
    if (pool && pool.usable <= 0 && pool.paused > 0) {
      const wait = Math.min(Math.max((pool.resumeAtMs ?? 0) - Date.now(), 30_000), 10 * 60 * 1000);
      await requeueCheckoutJob(data, wait, `Every usable card is paused (${pool.paused} paused, ${pool.dead} dead, ${pool.capped} at their cap)`);
      return;
    }
  }
  const unlockAccount = await lockAccount(data.email, job.claimedBy || String(job._id));
  if (!unlockAccount) {
    await requeueCheckoutJob(data, 15_000, `${data.email} already has an order in progress (one job per account at a time)`);
    return;
  }

  const slot = await reserveBatchSlot({
    batchId: data.batchId,
    quantityPerOrder,
    totalQuantity,
    totalAttempts,
  });
  if (slot.kind === "skip_target") {
    await finishWithoutChrome(
      data.jobId,
      "completed_target_reached",
      "batch",
      `Batch target already reached (purchased ${slot.purchasedQuantity}/${totalQuantity}) — Chrome not opened`
    );
    await unlockAccount();
    return;
  }
  if (slot.kind === "skip_exhausted") {
    await finishWithoutChrome(
      data.jobId,
      "failed_attempt_budget_exhausted",
      "batch",
      `Attempt budget exhausted (${slot.attemptsUsed}/${totalAttempts}, purchased ${slot.purchasedQuantity}/${totalQuantity}) — Chrome not opened`,
      "error"
    );
    await unlockAccount();
    return;
  }
  if (slot.kind === "skip_cancelled") {
    await finishWithoutChrome(data.jobId, "cancelled", "cancelled", `Batch cancelled by operator (purchased ${slot.purchasedQuantity}/${totalQuantity}) — Chrome not opened`, "warn");
    await unlockAccount();
    return;
  }

  let reservationHeld = true;
  const releaseReservation = async () => {
    if (!reservationHeld) return;
    reservationHeld = false;
    const left = await releaseBatchReservation(data.batchId, quantityPerOrder);
    console.log(
      `[${data.jobId}] [info] [batch] released reservation of ${quantityPerOrder}; purchased now ${left}`
    );
  };

  // status/step/startedAt were set by claimCheckoutJob — re-saving them here
  // would overwrite the claim's own fields and, worse, a full save() of this
  // document could clobber a concurrent cancel.
  await appendLog(
    data.jobId,
    "info",
    `Reserved ${quantityPerOrder} unit(s) (purchased ${slot.purchasedQuantity}/${totalQuantity}, attempt ${slot.attemptsUsed}/${totalAttempts})`,
    "batch"
  );

  const session = await resolveLoggedInSession(data.email);
  if (!session.ok) {
    const sessionFail = new CheckoutFailure("SESSION_EXPIRED", session.reason);
    const fields = failureFields(sessionFail);
    job.status = "failed";
    job.failedStep = fields.failedStep;
    job.error = fields.error;
    job.failureMessage = fields.failureMessage;
    job.errorCode = fields.errorCode;
    job.errorCodeDisplay = fields.errorCodeDisplay;
    job.errorSource = fields.errorSource;
    job.errorDetails = fields.errorDetails;
    job.completedAt = new Date();
    job.logs.push({ at: new Date(), level: "error", step: "session", message: session.reason });
    await job.save();
    await releaseReservation();
    await maybeEnqueueRetry(data, job.userId, "session", job.request);
    return;
  }

  /**
   * TWO browsers, because Flipkart is two sites and this job needs one leg on each.
   *
   * FlipkartCheckout is m-site automation — it looks for #msite-bottomsheet and
   * drives the page with touchscreen.tap — and Flipkart chooses which site to serve
   * from the User-Agent. Running the whole job in the desktop context (what this
   * did until now) meant every location/pincode check silently matched nothing, so
   * the delivery pincode was never set, Flipkart dropped the item server side, and
   * the job died with "Your cart is empty! / Enter Delivery Pincode" — a failure
   * that reads exactly like a selector bug and is not one.
   *
   * The account pages, the address book and the cart are fine on desktop and are
   * left there; only the add-to-cart -> checkout -> payments leg moves.
   * Proven by scripts/testFlowCli.ts: `npm run test:flow -- happy` reaches
   * pay.flipkart.com with the item in the cart.
   */
  // Egress: this worker's proxy from the pool, re-read per job so a change in
  // the dashboard applies to the next order. In `required` mode a job with no
  // proxy waits rather than leaving from this machine's own address — and the
  // attempt it reserved is given back, because Flipkart never saw it.
  const proxyMode = await currentProxyMode();
  const proxyHolder = `${config.workerId}#${data.jobId}`;
  let egress: Awaited<ReturnType<typeof claimEgress>>["egress"] = null;
  let releaseEgress: () => Promise<void> = async () => undefined;
  try {
    const lease = await claimEgress(proxyHolder);
    egress = lease.egress;
    releaseEgress = lease.release;
  } catch (err) {
    console.warn(`[${data.jobId}] [warn] [proxy] pool unavailable: ${err instanceof Error ? err.message : String(err)}`);
  }
  if (!egress && proxyMode === "required") {
    await releaseReservation();
    await refundBatchAttempt(data.batchId);
    await requeueCheckoutJob(data, config.proxyWaitMs, `no free proxy for ${proxyHolder} (proxy mode required, ${config.envName})`);
    await unlockAccount();
    return;
  }

  // A tunnel adds latency to every request of every page: give page loads
  // twice the time when the job leaves through a proxy. Process-wide, which
  // is right — every job in this process uses the same proxy.
  if (egress) setNavigationTimeoutScale(Math.max(2, Number(process.env.NAV_TIMEOUT_SCALE || 1)));

  // ── HTTP engine ("High Speed", COD) ──────────────────────────────────────
  // The order is placed with Flipkart's own API calls (services/httpCheckout.ts)
  // and no Chrome. Three of its four outcomes end the job right here; the
  // fourth, "fallback", carries on into the page flow below exactly as a Chrome
  // job would — behind a gate, so a burst of fallbacks cannot open more Chromes
  // than this process can hold (HTTP_FALLBACK_CHROME_MAX).
  let releaseChromeSlot: () => void = () => undefined;
  if (data.executionVersion === "high_speed" && cls === "cod") {
    const httpLog = (level: LogLevel, message: string, step?: string) => void appendLog(data.jobId, level, message, step);
    const endHere = async () => {
      await releaseEgress().catch(() => undefined);
      const latest = await CheckoutJob.findById(data.jobId).select("status");
      if (latest?.status === "cancelled") await releaseReservation();
      await unlockAccount();
      await settleBatchIfCancelling(data.batchId).catch(() => undefined);
    };
    let httpPayPressed = false;
    const checkoutSettings = await resolveCheckoutSettings(String(job.userId || "")).catch(() => null);
    const outcome = await runHttpCheckout({
      cookies: session.cookies,
      proxy: egress?.proxy ?? null,
      verifyWaitMs: checkoutSettings ? checkoutSettings.httpVerifyWaitSec * 1000 : undefined,
      quantityMode: data.quantityMode,
      productUrl: data.productUrl,
      quantity: quantityPerOrder,
      pincode: (data.address.checkoutPincode || data.address.pincode || ""),
      address: data.address,
      cartAmountLimit: data.cartAmountLimit,
      deliverySlaDays: data.deliverySlaDays,
      gstMandatory: data.gstMandatory ?? job.request?.gstMandatory ?? true,
      dryRun: PAYMENT_DRY_RUN,
      log: httpLog,
      onBeforePay: async () => {
        httpPayPressed = true;
        await CheckoutJob.updateOne({ _id: data.jobId }, { $set: { paymentStartedAt: new Date() } });
      },
    });
    const timing = Object.entries(outcome.timings).map(([k, v]) => `${k} ${v}ms`).join(" · ");
    if (timing) httpLog("info", `[http] ${outcome.kind} · ${timing}`, "session");
    if (egress && "exitIp" in outcome && outcome.exitIp) {
      await recordProxyUse(egress.id, data.jobId, outcome.exitIp);
      await patchResult(data.jobId, { egressProxy: egress.label, exitIp: outcome.exitIp });
    } else if (outcome.kind !== "fallback") {
      await patchResult(data.jobId, { egressProxy: egress ? egress.label : "direct" });
    }

    if (outcome.kind === "placed") {
      // Flipkart answered SUCCESS with an order id. Whether My Orders already
      // lists it only decides result.listed: Flipkart can take hours to list a
      // COD order (2026-10-28: 171 of 173 "unlisted" orders appeared later), so
      // an unlisted order is still a purchase here and keeps its batch slot.
      // The order sync keeps checking (orderSync.ts, listing-pending schedule)
      // and parks the job — returning the units — only if it never appears.
      await reporterFor(data.jobId)?.beforeTerminal();
      job.status = "paid";
      job.step = "paid";
      job.completedAt = new Date();
      await job.save();
      await patchResult(data.jobId, {
        flipkartOrderId: outcome.orderId,
        flipkartReferenceId: outcome.referenceId,
        ...(outcome.amount ? { transactionAmount: outcome.amount } : {}),
        ...(outcome.paymentFee ? { paymentFee: outcome.paymentFee, paymentFeeDetails: outcome.paymentFee } : {}),
        ...(outcome.details.orderStatus ? { orderStatus: outcome.details.orderStatus } : {}),
        ...(outcome.details.sellerName ? { sellerName: outcome.details.sellerName } : {}),
        ...(outcome.details.mrp ? { mrp: `₹${outcome.details.mrp}` } : {}),
        ...(outcome.details.price ? { unitPrice: `₹${outcome.details.price}` } : {}),
        ...(outcome.details.slaDays != null ? { promiseDays: outcome.details.slaDays, promiseDate: new Date(Date.now() + outcome.details.slaDays * 86_400_000) } : {}),
        ...(outcome.checkoutTotal ? { cartAmount: `₹${outcome.checkoutTotal}` } : {}),
        ...(outcome.deliveryText ? { deliveryText: outcome.deliveryText } : {}),
        ...(outcome.deliveryDays != null ? { deliveryDays: outcome.deliveryDays } : {}),
        listed: outcome.confirmed,
        listedStatus: outcome.confirmed ? outcome.listedStatus || "listed" : "pending",
        listingCheckedAt: new Date(),
      });
      // Upto: Flipkart may have held fewer units than asked. Record what was placed, give the
      // rest back to the batch and queue a follow-up order for it on this account.
      const placedQty = outcome.quantityPlaced ?? data.quantity;
      await patchResult(data.jobId, { quantityPlaced: placedQty });
      reservationHeld = false;
      if (placedQty < quantityPerOrder) {
        const left = await releaseBatchReservation(data.batchId, quantityPerOrder - placedQty);
        httpLog("info", `Upto mode: Flipkart allowed ${placedQty} of ${quantityPerOrder} unit(s) — ${quantityPerOrder - placedQty} unit(s) given back to the batch (purchased now ${left})`, "batch");
        if (data.quantityMode === "upto") await maybeEnqueueRetry(data, job.userId, "upto: units still to place", job.request);
      }
      const progress = await readBatchProgress(data.batchId, totalQuantity, totalAttempts);
      if (!outcome.confirmed) {
        httpLog("warn", `${outcome.orderId}: not in My Orders yet after ${Math.round(outcome.verifyMs / 1000)} s — counted as placed; the order sync keeps checking for ~8 h and parks it only if Flipkart never lists it`, "paid");
      }
      httpLog("info", `COD order placed (HTTP engine)${outcome.confirmed ? "" : " · listing pending"} — batch purchased ${progress.purchasedQuantity}/${totalQuantity}, attempts ${progress.attemptsUsed}/${totalAttempts}`, "paid");
      await endHere();
      return;
    }
    if (outcome.kind === "dry_run") {
      await releaseReservation();
      await refundBatchAttempt(data.batchId);
      await reporterFor(data.jobId)?.beforeTerminal();
      job.status = "dry_run";
      job.step = "dry_run";
      job.completedAt = new Date();
      await job.save();
      httpLog("warn", "DRY RUN — HTTP engine reached the payment token and stopped. No order was placed (PAYMENT_DRY_RUN=true on this worker).", "dry_run");
      await endHere();
      return;
    }
    if (outcome.kind === "fallback" && egress && !httpPayPressed && isProxyError(outcome.reason)) {
      const retired = await reportProxyFailure(egress.id, outcome.reason.split("\n")[0]);
      httpLog("warn", `[proxy] ${egress.label} failed in the HTTP engine: ${outcome.reason.slice(0, 200)}${retired ? " — marked DEAD" : ""}; attempt refunded, job re-queued`, "session");
      await releaseReservation();
      await refundBatchAttempt(data.batchId);
      await requeueCheckoutJob(data, config.proxyWaitMs, `proxy ${egress.label} failed — PROXY_FAILED`);
      await endHere();
      return;
    }
    if (outcome.kind === "failed" && outcome.payUnknown) {
      // Same parking as a worker that dies after Pay: the reservation stays,
      // nothing is re-queued, the order sync looks the account up.
      await reporterFor(data.jobId)?.beforeTerminal();
      await CheckoutJob.updateOne(
        { _id: data.jobId, status: { $ne: "cancelled" } },
        { $set: { status: "needs_reconciliation", step: "needs_reconciliation", completedAt: new Date(), ...failureFields(outcome.failure) } }
      );
      httpLog("error", "Parked as needs_reconciliation — check My Orders for this account before doing anything with this job.", "needs_reconciliation");
      reservationHeld = false;
      await endHere();
      return;
    }
    if (outcome.kind === "failed") {
      const failure = outcome.failure;
      httpLog("error", `${failure.display}: ${failure.details}`, failure.failedStep);
      await reporterFor(data.jobId)?.beforeTerminal();
      await CheckoutJob.updateOne(
        { _id: data.jobId, status: { $ne: "cancelled" } },
        { $set: { status: "failed", completedAt: new Date(), step: failure.failedStep, ...failureFields(failure) } }
      );
      await releaseReservation();
      if (failure.noRetry) httpLog("info", `${failure.code} is a verdict on this platform ID — not re-queued; the batch continues with the next ID`, "batch");
      else await maybeEnqueueRetry(data, job.userId, failure.failedStep, job.request);
      await endHere();
      return;
    }
    // fallback → the page flow, one Chrome slot at a time
    httpLog("info", `[http] handing this order to the Chrome flow (${outcome.reason})`, "session");
    releaseChromeSlot = await acquireChromeFallbackSlot();
    // The wait at the gate can be long; a batch cancelled meanwhile must not open Chrome.
    const latestAfterWait = await CheckoutJob.findById(data.jobId).select("status");
    if (latestAfterWait?.status === "cancelled") {
      releaseChromeSlot();
      httpLog("info", "Cancelled while waiting for a Chrome slot — not opened", "cancelled");
      await releaseReservation();
      await endHere();
      return;
    }
  }

  // One watcher across both legs — see testFlow for why.
  const apiWatcher = new FlipkartApiWatcher();
  const deskBrowser = await launchStealthBrowser({ headless: config.headless, proxy: egress?.proxy });
  const deskContext = await desktopContext(deskBrowser);
    apiWatcher.attach(deskContext);
  let mobBrowser: Awaited<ReturnType<typeof launchMobileBrowser>> | null = null;
  let paymentApi: PaymentApiWatcher | null = null;
  // Reassigned when the job moves to the mobile leg; everything below reads
  // whichever page is current.
  let page: Page = await deskContext.newPage();
  await blockFlipkartLogout(page);
  let netObserver = FlipkartNetworkObserver.attach(page);
  const address = { ...data.address };

  const log = (level: LogLevel, message: string, step?: string) => {
    void appendLog(data.jobId, level, message, step);
  };

  const jobPincode = (address.checkoutPincode || address.pincode || "").replace(/\D/g, "").slice(-6);

  log(
    "info",
    `${config.headless ? "Opened headless Chromium" : "Opened headed Chrome"}. Session cookies are copied into this window only — Mongo login is not modified, Logout is blocked.`,
    "session"
  );

  let checkout: FlipkartCheckout | undefined;
  // Once Pay has been pressed, a tunnel error is NOT re-queued: money may have
  // moved, and the paymentStartedAt rule (never re-run blindly) applies.
  let payPressed = false;
  try {
    // Prove the tunnel before a single Flipkart request: the address Flipkart
    // sees goes on the order row, and a dead proxy fails HERE as PROXY_FAILED
    // (re-queued, attempt refunded) instead of minutes later as a page timeout
    // blamed on the platform ID.
    if (egress) {
      const exitIp = await probeExitIp(deskContext);
      log("info", `[proxy] egress ${egress.label} → exit IP ${exitIp} (worker ${config.workerId})`, "session");
      await recordProxyUse(egress.id, data.jobId, exitIp);
      await patchResult(data.jobId, { egressProxy: egress.label, exitIp });
    } else {
      log("warn", `[proxy] no proxy — leaving from this machine's own address (proxy mode ${proxyMode}, ${config.envName})`, "session");
      await patchResult(data.jobId, { egressProxy: "direct" });
    }

    log("info", `Restoring Flipkart session for ${session.email}`, "session");
    await restoreFlipkartSession(page, session.cookies);

    checkout = new FlipkartCheckout(page, data.productUrl, log);
    checkout.api = apiWatcher;
    checkout.setCheckoutFlags(jobPincode, data.gstMandatory ?? job.request?.gstMandatory ?? true);
    log("info", "Using FlipkartCheckout (Buying-bot add-to-cart / place-order strategies)", "session");

    log("info", "Opening viewcart to empty existing items", "cart");
    await checkout.emptyCart();
    if (flipkartLoginUrl(page.url())) {
      throw new Error("Session expired: Flipkart showed the login page on viewcart");
    }

    log("info", "Pre-flight: fetch account mobile", "preflight");
    const mobile = await checkout.fetchAccountMobile();
    if (mobile) {
      const normalised = mobile.replace(/\D/g, "").slice(-10);
      if (normalised) {
        address.mobile = normalised;
        // The dashboard's address has no phone; the one on the Flipkart
        // address IS the account's. Recorded so the order row can show it.
        await patchResult(data.jobId, { billingPhone: normalised });
      }
      await checkout.ensureAddressForAccount(address, mobile);
      log("info", `Pre-flight done: one Flipkart address, mobile ending ${normalised.slice(-4)}`, "preflight");
    } else {
      log("warn", "Could not read mobile from /account — using job config mobile", "preflight");
    }

    if (flipkartLoginUrl(page.url())) {
      throw new Error("Session expired: Flipkart showed the login page during pre-flight");
    }

    // ---- desktop leg done; everything from here is the m-site ----------------
    netObserver.dispose();
    await deskContext.close().catch(() => undefined);
    mobBrowser = await launchMobileBrowser({ headless: config.headless, proxy: egress?.proxy });
    const mobContext = await mobileContext(mobBrowser);
    apiWatcher.attach(mobContext);
    paymentApi = new PaymentApiWatcher().attach(mobContext);
    page = await mobContext.newPage();
    await blockFlipkartLogout(page);
    netObserver = FlipkartNetworkObserver.attach(page);
    await restoreFlipkartSession(page, session.cookies);

    const ua = await page.evaluate(() => navigator.userAgent).catch(() => "");
    if (!/Mobile|Android/i.test(ua)) {
      // Loud, because this is the exact failure that masqueraded as a selector
      // bug: without a mobile UA the m-site selectors below cannot match, and the
      // job would fail several minutes later for an unrelated-looking reason.
      throw new Error(`Mobile context is not mobile (ua="${ua.slice(0, 80)}") — m-site selectors cannot match`);
    }
    log("info", `Switched to the m-site (${config.mobileDevice})`, "session");

    // Establish the m-site session before pasting a deep product link. Handing
    // Flipkart a product URL in a session it has not set up is one of the ways
    // the cart silently drops the item.
    await page.goto("https://www.flipkart.com/", { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.reload({ waitUntil: "domcontentloaded", timeout: 30_000 }).catch(() => undefined);
    await page.waitForLoadState("load", { timeout: 15_000 }).catch(() => undefined);

    log("info", "Opening product page", "product");
    checkout = new FlipkartCheckout(page, data.productUrl, log);
    checkout.api = apiWatcher;
    checkout.setCheckoutFlags(jobPincode, data.gstMandatory ?? job.request?.gstMandatory ?? true);
    await checkout.navigateToProduct();
    const details = await checkout.captureProductDetails();
    job.product = details;
    await job.save();
    await patchResult(data.jobId, {
      productName: details.model,
      colour: details.colour,
      listingAmount: details.amount,
    });
    log(
      "info",
      `Product details: ${details.model || "(no model)"} | ${details.colour || "(no colour)"} | ${details.amount ? "₹" + details.amount : "(no price)"}`,
      "product"
    );

    const productBlock = await checkout.detectCheckoutBlocker(jobPincode);
    if (productBlock) throw productBlock;

    log("info", "Add to cart, then jump straight to viewcart", "product");
    try {
      await checkout.clickAddToCart(jobPincode);
    } catch (err) {
      if (err instanceof CheckoutFailure) throw err;
      const again = await checkout.detectCheckoutBlocker(jobPincode);
      if (again) throw again;
      throw classifyThrownMessage(err instanceof Error ? err.message : String(err));
    }
    const oosOnProduct = await checkout.detectOutOfStockForPincode(jobPincode);
    if (oosOnProduct.matched) {
      throw new CheckoutFailure(
        "PRODUCT_NOT_SERVICEABLE",
        oosOnProduct.rawMessage || `Currently out of stock for ${jobPincode}`
      );
    }
    await checkout.gotoViewCart();
    try {
      await checkout.ensureProductInCart(details, jobPincode);
    } catch (err) {
      if (err instanceof CheckoutFailure) throw err;
      await checkout.navigateToProduct().catch(() => undefined);
      const why = await checkout.detectCheckoutBlocker(jobPincode);
      if (why) throw why;
      throw classifyThrownMessage(err instanceof Error ? err.message : String(err));
    }
    log("info", "Product is in viewcart", "product");

    const oosOnCart = await checkout.detectOutOfStockForPincode(jobPincode);
    if (oosOnCart.matched) {
      throw new CheckoutFailure("PRODUCT_NOT_SERVICEABLE", oosOnCart.rawMessage || `Currently out of stock for ${jobPincode}`);
    }

    if (data.deliverySlaDays == null) {
      throw new Error("delivery_sla_days is required: convert Flipkart delivery text (e.g. 4 days / 23 Sept) and match it before Place Order");
    }
    log("info", `Checking delivery SLA against max ${data.deliverySlaDays} day(s)`, "product");
    const sla = await checkout.assertDeliverySla(data.deliverySlaDays, details, jobPincode);
    await patchResult(data.jobId, {
      deliveryText: sla.text,
      deliveryDays: sla.days,
    });
    log("info", `Delivery SLA within ${data.deliverySlaDays} day(s)`, "product");

    const payable = await checkout.captureCartPayable();
    if (payable.amount) {
      job.product = { ...details, amount: payable.amount };
      await job.save();
      await patchResult(data.jobId, { cartAmount: payable.amount });
      log(
        "info",
        `Cart payable ₹${payable.amount} from ${payable.source}${data.cartAmountLimit ? ` (limit ₹${data.cartAmountLimit})` : ""}`,
        "product"
      );
      if (data.cartAmountLimit) {
        const total = Number(payable.amount);
        if (Number.isFinite(total) && total > data.cartAmountLimit) {
          throw new Error(
            `Cart amount ₹${total} exceeds cartAmountLimit ₹${data.cartAmountLimit}`
          );
        }
      }
    } else if (data.cartAmountLimit) {
      throw new Error("Could not read cart Total Amount / Place Order payable (expected e.g. 1,686)");
    }

    log("info", "Place Order → viewcheckout for GST, then Continue", "product");
    await checkout.clickPlaceOrder();

    // Flipkart answers "Something went wrong! E002" when viewcheckout is opened
    // without a real Place Order behind it. Going back to the cart and placing
    // again clears it; retrying in place does not.
    for (let retry = 1; retry <= 2 && (await checkoutShowsE002(page)); retry++) {
      log("warn", `Checkout shows "Something went wrong" — Place Order again (${retry}/2)`, "order-summary");
      await checkout.gotoViewCart();
      await checkout.clickPlaceOrder();
    }
    if (await checkoutShowsE002(page)) {
      throw new CheckoutFailure(
        "UNABLE_TO_PLACE_ORDER",
        'Checkout keeps showing "Something went wrong! E002" after Place Order'
      );
    }

    // Refuse to pay for something we did not add. A checkout session left over
    // from an earlier run looks entirely normal until the order confirmation.
    const checkoutText = String(await page.evaluate(() => document.body?.innerText || "").catch(() => ""));
    const modelKey = details.model.slice(0, 24).toLowerCase();
    if (modelKey && checkoutText && !checkoutText.toLowerCase().includes(modelKey)) {
      throw new CheckoutFailure(
        "UNABLE_TO_PLACE_ORDER",
        `Checkout does not show "${details.model}" — refusing to continue with a different product`
      );
    }

    log("info", "viewcheckout: qty / address / GST then Continue", "order-summary");
    await checkout.verifyAddressOnOrderSummary(
      address,
      data.quantity,
      data.gstMandatory ?? job.request?.gstMandatory ?? true,
      data.quantityMode || "minimum"
    );
    await patchResult(data.jobId, {
      quantityPlaced: checkout.summaryQty || data.quantity,
      gstNumber: address.gstNumber,
      gstCompany: address.companyName,
    });

    const stillOpen = await CheckoutJob.findById(data.jobId).select("status");
    if (stillOpen?.status === "cancelled") {
      log("warn", "Job cancelled after reaching payment", "cancelled");
      reservationHeld = false;
      return;
    }

    job.status = "reached_payment";
    job.step = "payment";
    job.paymentUrl = page.url();
    job.completedAt = new Date();
    job.logs.push({
      at: new Date(),
      level: "info",
      step: "payment",
      message: `Reached payment page: ${page.url()}`,
    });
    await job.save();
    await patchResult(data.jobId, { paymentUrl: page.url() });

    // Reaching payment is no longer the end of the road for a card order. The
    // reservation is held until the payment phase resolves: releasing it here
    // would let a sibling job claim the slot this order is about to spend.
    const payment = await runPaymentPhase({
      page,
      data,
      log,
      dryRun: PAYMENT_DRY_RUN,
      paymentApi,
      // Durable, direct, awaited — not through the batched reporter — because
      // this is the line between "safe to re-run" and "money may have moved".
      onBeforePay: async () => {
        payPressed = true;
        await CheckoutJob.updateOne({ _id: data.jobId }, { $set: { paymentStartedAt: new Date() } });
      },
    });
    if (payment.attempted && payment.ok && PAYMENT_DRY_RUN) {
      // A dry run proves the plumbing up to the payment page. Nothing was
      // clicked, so nothing was bought: the job ends as dry_run (never paid),
      // the batch slot and the attempt go back, and the Orders tab says so.
      await releaseReservation();
      await refundBatchAttempt(data.batchId);
      await reporterFor(data.jobId)?.beforeTerminal();
      job.status = "dry_run";
      job.step = "dry_run";
      job.completedAt = new Date();
      await job.save();
      log("warn", "DRY RUN — reached the payment page and stopped. No order was placed (PAYMENT_DRY_RUN=true on this worker).", "dry_run");
    } else if (payment.attempted) {
      if (!payment.ok) {
        // Which card the failed attempt went on, before the failure is recorded.
        if (payment.cardUsed) await patchResult(data.jobId, payment.cardUsed);
        throw payment.failure;
      }
      await reporterFor(data.jobId)?.beforeTerminal();
      job.status = "paid";
      job.step = "paid";
      job.completedAt = new Date();
      await job.save();
      // result is null for COD — no card, no bank, nothing to record about either.
      await patchResult(data.jobId, {
        ...(payment.result
          ? {
              cardTypeName: payment.result.cardTypeName,
              cardLast4: payment.result.cardLast4,
              authType: payment.result.authType,
              employeeId: payment.result.employeeId,
              authenticatedAt: payment.result.authenticatedAt,
            }
          : {}),
        ...(payment.confirmation?.orderId ? { flipkartOrderId: payment.confirmation.orderId } : {}),
        ...(payment.confirmation?.referenceId ? { flipkartReferenceId: payment.confirmation.referenceId } : {}),
        ...(payment.confirmation?.amount ? { transactionAmount: payment.confirmation.amount } : {}),
        ...orderDetailFields(payment.confirmation?.details),
        ...(payment.cardUsed ?? {}),
      });
      log(
        "info",
        payment.cardId
          ? `Payment authenticated with card ${payment.cardId} (${payment.ordersOnCard} order(s) on it in this batch)`
          : "COD order placed",
        "paid"
      );
    } else {
      log("info", `Stopping at payment page — ${payment.reason}`, "payment");
    }
    reservationHeld = false;
    if (payment.attempted && payment.ok) {
      const placedQty = checkout.summaryQty || data.quantity;
      if (placedQty < quantityPerOrder) {
        const left = await releaseBatchReservation(data.batchId, quantityPerOrder - placedQty);
        log("info", `Upto mode: Flipkart allowed ${placedQty} of ${quantityPerOrder} unit(s) — ${quantityPerOrder - placedQty} unit(s) given back to the batch (purchased now ${left})`, "batch");
        if (data.quantityMode === "upto") await maybeEnqueueRetry(data, job.userId, "upto: units still to place", job.request);
      }
    }
    const progress = await readBatchProgress(data.batchId, totalQuantity, totalAttempts);
    log(
      "info",
      `${payment.attempted ? "Finished on" : "Stopped at payment page:"} ${page.url().split("?")[0]} — leaving Chrome open so you can watch (batch purchased ${progress.purchasedQuantity}/${totalQuantity}, attempts ${progress.attemptsUsed}/${totalAttempts}${progress.status === "completed" ? ", batch complete" : ""})`,
      "payment"
    );
    await sleep(config.keepBrowserOpenMs);
  } catch (err) {
    const rawMessage = err instanceof Error ? err.message : String(err);

    // The tunnel failed, not the order. The proxy takes the blame (dead after
    // PROXY_DEAD_AFTER), the attempt goes back to the batch, and the job is
    // re-queued so the next pick leaves through a different proxy. No status
    // of "failed" and no error code on the job: Flipkart never saw it.
    if (egress && !payPressed && isProxyError(rawMessage)) {
      const retired = await reportProxyFailure(egress.id, rawMessage.split("\n")[0]);
      log(
        "warn",
        `[proxy] ${egress.label} failed: ${rawMessage.split("\n")[0].slice(0, 200)}` +
          `${retired ? " — marked DEAD" : ""}; attempt refunded, job re-queued`,
        "session"
      );
      await releaseReservation();
      await refundBatchAttempt(data.batchId);
      await requeueCheckoutJob(data, config.proxyWaitMs, `proxy ${egress.label} failed — PROXY_FAILED`);
      return;
    }

    for (const s of netObserver.getBlocks().slice(-3)) {
      log("warn", `[net] ${s.kind}: ${s.text} — ${s.url}`, "net");
    }
    // Every failure is THIS job's failure, recorded on this job with its own
    // reason. Out of stock / not deliverable used to stop the whole batch here;
    // they no longer do — the next platform ID gets its own attempt, because a
    // product Flipkart refuses to one account may still be sold to another.
    let failure = err instanceof CheckoutFailure ? err : null;
    if (!failure && err instanceof OutOfStockPincodeError) {
      failure = new CheckoutFailure("PRODUCT_NOT_SERVICEABLE", err.rawMessage);
    }
    if (
      !failure &&
      checkout &&
      /Execution context was destroyed|most likely because of a navigation/i.test(rawMessage)
    ) {
      const oos = await checkout.detectOutOfStockForPincode(jobPincode).catch(() => ({ matched: false as const }));
      if (oos.matched) {
        failure = new CheckoutFailure("PRODUCT_NOT_SERVICEABLE", oos.rawMessage || `Currently out of stock for ${jobPincode}`);
      }
    }
    if (!failure && checkout) {
      failure = await checkout.detectCheckoutBlocker(jobPincode).catch(() => null);
    }
    if (!failure) failure = classifyThrownMessage(rawMessage);
    const failedStep = failure.failedStep;
    log("error", `${failure.display}: ${failure.details}`, failedStep);
    await reporterFor(data.jobId)?.beforeTerminal();
    await CheckoutJob.updateOne(
      { _id: data.jobId, status: { $ne: "cancelled" } },
      {
        $set: {
          status: "failed",
          completedAt: new Date(),
          step: failedStep,
          ...failureFields(failure),
        },
      }
    );
    await releaseReservation();
    if (failure.noRetry) {
      log("info", `${failure.code} is a verdict on this platform ID — not re-queued; the batch continues with the next ID`, "batch");
    } else {
      await maybeEnqueueRetry(data, job.userId, failedStep, job.request);
    }
    await sleep(Math.min(config.keepBrowserOpenMs, 15000));
  } finally {
    releaseChromeSlot();
    netObserver.dispose();
    await deskBrowser.close().catch(() => undefined);
    await mobBrowser?.close().catch(() => undefined);
    await releaseEgress().catch(() => undefined);
    const latest = await CheckoutJob.findById(data.jobId).select("status");
    if (latest?.status === "cancelled") await releaseReservation();
    await unlockAccount();
    // A batch being cancelled is done the moment its last open attempt ends.
    await settleBatchIfCancelling(data.batchId).catch(() => undefined);
  }
}
