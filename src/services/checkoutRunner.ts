import type { Page } from "playwright";
import { CheckoutJob } from "../models/CheckoutJob.js";
import { FlipkartCheckout2, OutOfStockPincodeError } from "../automation/FlipkartCheckout2.js";
import {
  readBatchProgress,
  releaseBatchReservation,
  reserveBatchSlot,
  markBatchFiltered,
} from "./batchCounters.js";
import { CheckoutBatch } from "../models/CheckoutBatch.js";
import {
  blockFlipkartLogout,
  closeBrowser,
  flipkartLoginUrl,
  launchStealthContext,
  restoreFlipkartSession,
  sleep,
} from "./browser.js";
import { config } from "../config.js";
import { enqueueCheckoutJob } from "./checkoutEnqueue.js";
import { resolveLoggedInSession } from "./sessionStore.js";
import { FlipkartNetworkObserver } from "../automation/FlipkartNetworkObserver.js";
import {
  CheckoutFailure,
  classifyPageText,
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
  extra?: { failedStep?: string; filterReason?: string; batchStatus?: string; failure?: CheckoutFailure }
) {
  const mapped = extra?.failure;
  await CheckoutJob.updateOne(
    { _id: jobId, status: { $ne: "cancelled" } },
    {
      $set: {
        status,
        step,
        failedStep: extra?.failedStep || mapped?.failedStep || (status.startsWith("failed") ? step : ""),
        error: status === "skipped" || status.startsWith("failed") || status === "filtered" ? message : "",
        failureMessage: extra?.failedStep === "out_of_stock_pincode" || extra?.failedStep === "batch_filtered" || mapped ? message : "",
        ...(mapped ? failureFields(mapped) : {}),
        filterReason: extra?.filterReason || "",
        batchStatus: extra?.batchStatus || "",
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
  if (progress.status === "filtered") {
    console.log(
      `[${data.jobId}] [info] [batch] filtered (${progress.filterReason || "out_of_stock_pincode"}) — no retry`
    );
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
    cartAmountLimit: data.cartAmountLimit,
    deliverySlaDays: data.deliverySlaDays,
    gstMandatory: data.gstMandatory ?? request?.gstMandatory,
    address: data.address,
    request,
    isRetry: true,
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
export async function runCheckoutJob(data: CheckoutJobData) {
  const job = await CheckoutJob.findById(data.jobId);
  if (!job) throw new Error(`Job ${data.jobId} not found`);
  if (job.status === "cancelled") return;

  const { quantityPerOrder, totalQuantity, totalAttempts } = batchLimits(data);
  const already = await readBatchProgress(data.batchId, totalQuantity, totalAttempts);
  if (already.status === "filtered") {
    await finishWithoutChrome(
      data.jobId,
      "skipped",
      "batch_filtered",
      `Batch already filtered (${already.filterReason || "out_of_stock_pincode"}) — Chrome not opened`,
      "warn",
      {
        failedStep: "batch_filtered",
        filterReason: already.filterReason || "out_of_stock_pincode",
        batchStatus: "filtered",
      }
    );
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

  if (slot.kind === "skip_filtered") {
    await finishWithoutChrome(
      data.jobId,
      "skipped",
      "batch_filtered",
      `Batch already filtered (${slot.filterReason || "out_of_stock_pincode"}) — Chrome not opened`,
      "warn",
      {
        failedStep: "batch_filtered",
        filterReason: slot.filterReason || "out_of_stock_pincode",
        batchStatus: "filtered",
      }
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

  job.status = "running";
  job.startedAt = new Date();
  job.step = "session";
  await job.save();
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

  const launched = await launchStealthContext({ headless: config.headless });
  const page: Page = await launched.context.newPage();
  await blockFlipkartLogout(page);
  const netObserver = FlipkartNetworkObserver.attach(page);
  const address = { ...data.address };

  const log = (level: LogLevel, message: string, step?: string) => {
    void appendLog(data.jobId, level, message, step);
  };

  const jobPincode = (address.checkoutPincode || address.pincode || "").replace(/\D/g, "").slice(-6);

  const applyOutOfStockFilter = async (rawMessage: string, failure?: CheckoutFailure) => {
    const mapped =
      failure ||
      classifyPageText(rawMessage, jobPincode) ||
      new CheckoutFailure("PRODUCT_NOT_SERVICEABLE", rawMessage);
    console.log(
      `[${data.jobId}] [info] [batch] filtered: ${mapped.code} for pincode ${jobPincode} — batch stopped, no retry`
    );
    log("error", rawMessage, mapped.failedStep);
    await releaseReservation();
    const filteredCount = await markBatchFiltered(data.batchId, "out_of_stock_pincode");
    const progress = await readBatchProgress(data.batchId, totalQuantity, totalAttempts);
    await CheckoutBatch.findOneAndUpdate(
      { batchId: data.batchId },
      {
        $set: {
          batchStatus: "filtered",
          filterReason: "out_of_stock_pincode",
          filteredCount,
          purchasedQuantity: progress.purchasedQuantity,
          attemptsUsed: progress.attemptsUsed,
        },
      }
    );
    const now = new Date();
    await CheckoutJob.updateOne(
      { _id: data.jobId, status: { $ne: "cancelled" } },
      {
        $set: {
          status: "filtered",
          step: mapped.failedStep,
          filterReason: "out_of_stock_pincode",
          failedAt: now,
          batchStatus: "filtered",
          completedAt: now,
          ...failureFields(mapped),
        },
      }
    );
    await CheckoutJob.updateMany(
      { batchId: data.batchId, _id: { $ne: data.jobId } },
      { $set: { batchStatus: "filtered", filterReason: "out_of_stock_pincode" } }
    );
  };

  log(
    "info",
    `${config.headless ? "Opened headless Chromium" : "Opened headed Chrome"}. Session cookies are copied into this window only — Mongo login is not modified, Logout is blocked.`,
    "session"
  );

  let checkout: FlipkartCheckout2 | undefined;
  try {
    log("info", `Restoring Flipkart session for ${session.email}`, "session");
    await restoreFlipkartSession(page, session.cookies);

    checkout = new FlipkartCheckout2(page, data.productUrl, log);
    checkout.setCheckoutFlags(jobPincode, data.gstMandatory ?? job.request?.gstMandatory ?? true);
    log("info", "Using FlipkartCheckout2 (Buying-bot add-to-cart / place-order strategies)", "session");

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

    log("info", "Opening product page", "product");
    checkout.setProductUrl(data.productUrl);
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
      await applyOutOfStockFilter(
        oosOnProduct.rawMessage || `Currently out of stock for ${jobPincode}`
      );
      await sleep(Math.min(config.keepBrowserOpenMs, 15000));
      return;
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
      await applyOutOfStockFilter(oosOnCart.rawMessage || `Currently out of stock for ${jobPincode}`);
      await sleep(Math.min(config.keepBrowserOpenMs, 15000));
      return;
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
    if (err instanceof OutOfStockPincodeError) {
      await applyOutOfStockFilter(err.rawMessage);
      await sleep(Math.min(config.keepBrowserOpenMs, 15000));
      return;
    }
    if (err instanceof CheckoutFailure && err.filterBatch) {
      await applyOutOfStockFilter(err.details, err);
      await sleep(Math.min(config.keepBrowserOpenMs, 15000));
      return;
    }
    const rawMessage = err instanceof Error ? err.message : String(err);
    if (
      /Execution context was destroyed|most likely because of a navigation/i.test(rawMessage) &&
      checkout
    ) {
      const oos = await checkout.detectOutOfStockForPincode(jobPincode).catch(() => ({ matched: false as const }));
      if (oos.matched) {
        await applyOutOfStockFilter(oos.rawMessage || `Currently out of stock for ${jobPincode}`);
        await sleep(Math.min(config.keepBrowserOpenMs, 15000));
        return;
      }
    }
    let failure = err instanceof CheckoutFailure ? err : null;
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
    await maybeEnqueueRetry(data, job.userId, failedStep, job.request);
    await sleep(Math.min(config.keepBrowserOpenMs, 15000));
  } finally {
    netObserver.dispose();
    await closeBrowser(launched.browser, launched.context);
    const latest = await CheckoutJob.findById(data.jobId).select("status");
    if (latest?.status === "cancelled") await releaseReservation();
  }
}
