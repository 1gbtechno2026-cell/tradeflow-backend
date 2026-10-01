import type { Page } from "playwright";
import { CheckoutJob } from "../models/CheckoutJob.js";
import { FlipkartCheckout, OutOfStockPincodeError } from "../automation/FlipkartCheckout.js";
import { FlipkartApiWatcher } from "../automation/FlipkartApiWatcher.js";
import {
  readBatchProgress,
  releaseBatchReservation,
  reserveBatchSlot,
} from "./batchCounters.js";
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
import { enqueueCheckoutJob } from "./checkoutEnqueue.js";
import { resolveLoggedInSession } from "./sessionStore.js";
import { runPaymentPhase } from "./paymentPhase.js";
import { FlipkartNetworkObserver } from "../automation/FlipkartNetworkObserver.js";
import {
  CheckoutFailure,
  classifyThrownMessage,
  failureFields,
} from "./checkoutErrors.js";
import type { CheckoutJobData, JobRequestSnapshot, JobResultSnapshot, JobStatus, LogLevel } from "../types.js";

async function appendLog(jobId: string, level: LogLevel, message: string, step?: string) {
  console.log(`[${jobId}] [${level}]${step ? ` [${step}]` : ""} ${message}`);
    const update: Record<string, unknown> = {
      $push: { logs: { at: new Date(), level, step, message } },
    };
    if (step) update.$set = { step };
    await CheckoutJob.updateOne({ _id: jobId }, update);
}

async function patchResult(jobId: string, patch: Partial<JobResultSnapshot>) {
  const set: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(patch)) {
    if (value === undefined) continue;
    set[`result.${key}`] = value;
  }
  if (Object.keys(set).length) await CheckoutJob.updateOne({ _id: jobId }, { $set: set });
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
const PAYMENT_DRY_RUN = process.env.PAYMENT_DRY_RUN !== "false";

/** Flipkart's E002 page. "Deliver to" present means a real checkout, not the error. */
async function checkoutShowsE002(page: Page): Promise<boolean> {
  const text = String(await page.evaluate(() => document.body?.innerText || "").catch(() => ""));
  return /Something went wrong/i.test(text) && !/Deliver to/i.test(text);
}

/** How quiet a `running` job's heartbeat must go before another worker may take
 *  it. Comfortably longer than HEARTBEAT_MS so a slow page never looks dead. */
const CLAIM_STALE_MS = Number(process.env.CHECKOUT_CLAIM_STALE_MS || 10 * 60 * 1000);
const HEARTBEAT_MS = Number(process.env.CHECKOUT_HEARTBEAT_MS || 60 * 1000);

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
        { status: "running", heartbeatAt: { $lt: staleBefore } },
        { status: "running", heartbeatAt: null, startedAt: { $lt: staleBefore } },
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
  try {
    return await runClaimedCheckoutJob(data, job);
  } finally {
    clearInterval(heartbeat);
  }
}

async function runClaimedCheckoutJob(
  data: CheckoutJobData,
  job: NonNullable<Awaited<ReturnType<typeof claimCheckoutJob>>>
) {

  const { quantityPerOrder, totalQuantity, totalAttempts } = batchLimits(data);
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
  // One watcher across both legs — see testFlow for why.
  const apiWatcher = new FlipkartApiWatcher();
  const deskBrowser = await launchStealthBrowser({ headless: config.headless });
  const deskContext = await desktopContext(deskBrowser);
    apiWatcher.attach(deskContext);
  let mobBrowser: Awaited<ReturnType<typeof launchMobileBrowser>> | null = null;
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
  try {
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
      if (normalised) address.mobile = normalised;
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
    mobBrowser = await launchMobileBrowser({ headless: config.headless });
    const mobContext = await mobileContext(mobBrowser);
    apiWatcher.attach(mobContext);
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
      data.gstMandatory ?? job.request?.gstMandatory ?? true
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
    const payment = await runPaymentPhase({ page, data, log, dryRun: PAYMENT_DRY_RUN });
    if (payment.attempted) {
      if (!payment.ok) throw payment.failure;
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
        ...(payment.confirmation?.amount ? { transactionAmount: payment.confirmation.amount } : {}),
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
    const progress = await readBatchProgress(data.batchId, totalQuantity, totalAttempts);
    log(
      "info",
      `Stopped at payment page: ${page.url()} — leaving Chrome open so you can watch (batch purchased ${progress.purchasedQuantity}/${totalQuantity}, attempts ${progress.attemptsUsed}/${totalAttempts}${progress.status === "completed" ? ", batch complete" : ""})`,
      "payment"
    );
    await sleep(config.keepBrowserOpenMs);
  } catch (err) {
    for (const s of netObserver.getBlocks().slice(-3)) {
      log("warn", `[net] ${s.kind}: ${s.text} — ${s.url}`, "net");
    }
    // Every failure is THIS job's failure, recorded on this job with its own
    // reason. Out of stock / not deliverable used to stop the whole batch here;
    // they no longer do — the next platform ID gets its own attempt, because a
    // product Flipkart refuses to one account may still be sold to another.
    const rawMessage = err instanceof Error ? err.message : String(err);
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
    netObserver.dispose();
    await deskBrowser.close().catch(() => undefined);
    await mobBrowser?.close().catch(() => undefined);
    const latest = await CheckoutJob.findById(data.jobId).select("status");
    if (latest?.status === "cancelled") await releaseReservation();
  }
}
