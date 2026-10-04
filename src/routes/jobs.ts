import { randomUUID } from "node:crypto";
import { Router } from "express";
import { z } from "zod";
import { CheckoutJob } from "../models/CheckoutJob.js";
import { CheckoutBatch } from "../models/CheckoutBatch.js";
import { getCheckoutQueue } from "../queue.js";
import { jobClassFor } from "../services/jobClass.js";
import { effectiveAttemptsFor, parseCreateJobBody, requiredOrdersFor } from "../services/jobRequest.js";
import { initBatchCounters, readBatchProgress } from "../services/batchCounters.js";
import { enqueueCheckoutJob } from "../services/checkoutEnqueue.js";
import { requestSnapshot } from "../services/orderSnapshot.js";
import { resolveAddress, resolveLoggedInSession, workspaceUserId } from "../services/sessionStore.js";
import { toCardDetails } from "../services/paymentCards.js";
import type { AuthType } from "../paymentStrategies/types.js";
import type { AddressDetails, JobRequestSnapshot, JobResultSnapshot } from "../types.js";

export const jobsRouter = Router();

function publicJob(job: {
  _id: unknown;
  batchId: string;
  email: string;
  userId?: string;
  status: string;
  step: string;
  failedStep: string;
  error: string;
  productUrl: string;
  quantity: number;
  totalQuantity?: number;
  quantityPerOrder?: number;
  totalAttempts?: number;
  cartAmountLimit?: number;
  paymentUrl: string;
  product?: { model?: string; colour?: string; amount?: string };
  address?: { name?: string; city?: string; pincode?: string; gstNumber?: string; companyName?: string };
  request?: JobRequestSnapshot;
  result?: JobResultSnapshot;
  failureMessage?: string;
  errorCode?: string;
  errorCodeDisplay?: string;
  errorSource?: string;
  errorDetails?: string;
  errorStage?: string;
  /** Set the instant before Pay was pressed; a job with this and no order id
   *  must be reconciled against Flipkart's orders, never re-run blindly. */
  paymentStartedAt?: Date | null;
  filterReason?: string;
  failedAt?: Date | null;
  batchStatus?: string;
  logs?: unknown[];
  startedAt: Date | null;
  completedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}) {
  return {
    id: String(job._id),
    tradeflowId: String(job._id),
    batchId: job.batchId,
    email: job.email,
    userId: job.userId,
    platform: job.request?.platform || "FLIPKART",
    status: job.status,
    step: job.step,
    failedStep: job.failedStep || undefined,
    error: job.error || undefined,
    productUrl: job.productUrl,
    quantity: job.quantity,
    totalQuantity: job.totalQuantity,
    quantityPerOrder: job.quantityPerOrder ?? job.quantity,
    totalAttempts: job.totalAttempts,
    cartAmountLimit: job.cartAmountLimit,
    paymentUrl: job.paymentUrl || job.result?.paymentUrl || undefined,
    product: job.product,
    address: job.address,
    request: job.request,
    result: job.result,
    failureMessage: job.failureMessage || undefined,
    errorCode: job.errorCode || undefined,
    errorCodeDisplay: job.errorCodeDisplay || undefined,
    errorSource: job.errorSource || undefined,
    errorDetails: job.errorDetails || undefined,
    errorStage: job.errorStage || undefined,
    paymentStartedAt: job.paymentStartedAt ?? null,
    filterReason: job.filterReason || undefined,
    failedAt: job.failedAt,
    batchStatus: job.batchStatus || undefined,
    logs: job.logs,
    startedAt: job.startedAt,
    completedAt: job.completedAt,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
  };
}

async function attachBatches<T extends { batchId: string; batchStatus?: string; filterReason?: string }>(
  jobs: T[]
) {
  const ids = [...new Set(jobs.map((j) => j.batchId))];
  if (!ids.length) return jobs;
  const rows = await CheckoutBatch.find({ batchId: { $in: ids } }).lean();
  const map = new Map(rows.map((b) => [String(b.batchId), b]));
  return jobs.map((j) => {
    const b = map.get(j.batchId);
    if (!b) return j;
    return {
      ...j,
      batchStatus: j.batchStatus || b.batchStatus,
      filterReason: j.filterReason || b.filterReason || undefined,
      batch: {
        batchStatus: b.batchStatus,
        filterReason: b.filterReason,
        filteredCount: b.filteredCount,
        purchasedQuantity: b.purchasedQuantity,
        attemptsUsed: b.attemptsUsed,
        totalQuantity: b.totalQuantity,
        quantityPerOrder: b.quantityPerOrder,
        totalAttempts: b.totalAttempts,
      },
    };
  });
}

jobsRouter.post("/", async (req, res) => {
  const body = (req.body || {}) as Record<string, unknown>;
  const emailsPreview = Array.isArray(body.emails) ? body.emails.length : 0;
  console.log(
    `[trade-flow] POST /api/jobs received dry_run=${String(body.dry_run)} platform=${String(body.platform)} emails=${emailsPreview} address_id=${String(body.address_id)} gst_id=${String(body.gst_id)}`
  );
  try {
    const data = parseCreateJobBody(req.body);
    const platform = (data.platform || "FLIPKART").toUpperCase();
    if (platform !== "FLIPKART") {
      res.status(400).json({ error: `Platform ${platform} is not supported yet. Use FLIPKART.` });
      return;
    }

    let hostname = "";
    try {
      hostname = new URL(data.productUrl).hostname;
    } catch {
      res.status(400).json({ error: "Invalid product URL" });
      return;
    }
    if (!hostname.includes("flipkart.com")) {
      res.status(400).json({ error: "Product URL must be a Flipkart URL" });
      return;
    }

    if (data.gstMandatory && data.gstId == null && !data.address?.gstNumber) {
      res.status(400).json({ error: "gst_id is required when gst_mandatory is true" });
      return;
    }

    const address: AddressDetails = await resolveAddress({
      address: data.address as AddressDetails | undefined,
      addressId: data.addressId,
      gstId: data.gstId,
    });

    if (data.gstMandatory && (!address.gstNumber || !address.companyName)) {
      res.status(400).json({
        error: "GST number and company name are required (gst_id or address.gstNumber / companyName)",
      });
      return;
    }

    const userId = workspaceUserId();
    const batchId = randomUUID();
    const uniqueEmails = [...new Set(data.emails.map((e) => e.trim()).filter(Boolean))];
    const perOrderQty = data.quantityPerOrder;
    const totalQuantity = data.totalQuantity;
    const requiredOrders = requiredOrdersFor(totalQuantity, perOrderQty);
    const effectiveAttempts = effectiveAttemptsFor(data.totalAttempts, requiredOrders);
    const jobs: Array<ReturnType<typeof publicJob>> = [];
    const request = requestSnapshot({ ...data, totalAttempts: effectiveAttempts });
    // Unmasked, for the queue payload only. `request.cards` just above is the
    // masked copy that gets persisted on the job document; the two must never be
    // confused for one another.
    const cards = toCardDetails(data.cards);
    const authType = data.authType as AuthType | undefined;

    // Emails are assigned round-robin across requiredOrders jobs.
    // uniqueEmails.length may be < requiredOrders — the same logged-in ID is reused
    // on later slots (each worker still opens its own Chrome with a cookie copy).
    const sessionByEmail = new Map<string, Awaited<ReturnType<typeof resolveLoggedInSession>>>();
    for (const idOrEmail of uniqueEmails) {
      const session = await resolveLoggedInSession(idOrEmail);
      sessionByEmail.set(idOrEmail, session);
      if (!session.ok) {
        console.log(`[trade-flow] SKIP ${session.email} — ${session.reason}`);
        const skipped = await CheckoutJob.create({
          batchId,
          userId,
          email: session.email,
          productUrl: data.productUrl,
          quantity: perOrderQty,
          totalQuantity,
          quantityPerOrder: perOrderQty,
          totalAttempts: effectiveAttempts,
          cartAmountLimit: data.cartAmountLimit,
          address,
          request,
          status: "skipped",
          step: "session",
          failedStep: "session",
          error: session.reason,
          completedAt: new Date(),
          logs: [{ at: new Date(), level: "error", step: "session", message: session.reason }],
        });
        jobs.push(publicJob(skipped));
      }
    }

    const goodEmails = uniqueEmails.filter((e) => sessionByEmail.get(e)?.ok);
    if (!goodEmails.length) {
      console.log(`[trade-flow] batch ${batchId} no logged-in emails — nothing queued`);
      res.status(201).json({
        batchId,
        dry_run: data.dryRun,
        queued: 0,
        skipped: jobs.filter((j) => j.status === "skipped").length,
        dry_run_ok: 0,
        quantity: totalQuantity,
        quantity_per_order: perOrderQty,
        total_attempts: effectiveAttempts,
        required_orders: requiredOrders,
        cart_amount_limit: data.cartAmountLimit,
        jobs,
      });
      return;
    }

    if (!data.dryRun) {
      await initBatchCounters(batchId);
      await CheckoutBatch.create({
        batchId,
        userId,
        batchStatus: "running",
        filterReason: null,
        filteredCount: 0,
        purchasedQuantity: 0,
        attemptsUsed: 0,
        totalQuantity,
        quantityPerOrder: perOrderQty,
        totalAttempts: effectiveAttempts,
      });
    }

    for (let i = 0; i < requiredOrders; i++) {
      const idOrEmail = goodEmails[i % goodEmails.length];
      const session = sessionByEmail.get(idOrEmail);
      if (!session?.ok) continue;
      const { job: created } = await enqueueCheckoutJob({
        userId,
        batchId,
        email: session.email,
        platformId: session.platformId,
        productUrl: data.productUrl,
        quantityPerOrder: perOrderQty,
        totalQuantity,
        totalAttempts: effectiveAttempts,
        cartAmountLimit: data.cartAmountLimit,
        deliverySlaDays: data.deliverySlaDays,
        gstMandatory: data.gstMandatory,
        address,
        request,
        dryRun: data.dryRun,
        paymentMode: data.paymentMode,
        cardType: data.cardType,
        authType,
        corporateId: data.corporateId,
        cardMaxUsage: data.cardMaxUsage,
        cards,
      });
      if (!data.dryRun) {
        console.log(`[trade-flow] QUEUED ${session.email} job=${created._id} redis=${created.bullmqJobId || created._id}`);
      } else {
        console.log(`[trade-flow] DRY_RUN ${session.email} job=${created._id} (not pushed to Redis)`);
      }
      jobs.push(publicJob(created));
    }

    const queuedCount = jobs.filter((j) => j.status === "queued").length;
    const skippedCount = jobs.filter((j) => j.status === "skipped").length;
    const dryRunCount = jobs.filter((j) => j.status === "dry_run").length;
    console.log(
      `[trade-flow] batch ${batchId} done queued=${queuedCount} skipped=${skippedCount} dry_run=${dryRunCount} required_orders=${requiredOrders} total_qty=${totalQuantity} per_order=${perOrderQty} attempts=${effectiveAttempts}`
    );

    res.status(201).json({
      batchId,
      dry_run: data.dryRun,
      queued: jobs.filter((j) => j.status === "queued").length,
      skipped: jobs.filter((j) => j.status === "skipped").length,
      dry_run_ok: jobs.filter((j) => j.status === "dry_run").length,
      quantity: totalQuantity,
      quantity_per_order: perOrderQty,
      total_attempts: effectiveAttempts,
      required_orders: requiredOrders,
      cart_amount_limit: data.cartAmountLimit,
      jobs,
    });
  } catch (err) {
    if (err instanceof z.ZodError) {
      console.error("[trade-flow] VALIDATION FAILED", err.issues);
      res.status(400).json({ error: "Validation failed", details: err.issues });
      return;
    }
    const message = err instanceof Error ? err.message : "Could not create jobs";
    console.error("[trade-flow] CREATE JOBS FAILED", message);
    res.status(400).json({ error: message });
  }
});

jobsRouter.get("/", async (req, res) => {
  const userId = workspaceUserId();
  const status = String(req.query.status || "").trim();
  const limit = Math.min(200, Math.max(1, Number(req.query.limit) || 80));
  const filter: Record<string, unknown> = { userId };
  if (status) filter.status = status;
  // Omit logs on list — unbounded log arrays OOM the API under jobs-proxy polling.
  const rows = await CheckoutJob.find(filter)
    .select("-logs")
    .sort({ createdAt: -1 })
    .limit(limit)
    .lean();
  const grouped = await CheckoutJob.aggregate([
    { $match: { userId } },
    { $group: { _id: "$status", n: { $sum: 1 } } },
  ]);
  const counts: Record<string, number> = {};
  for (const row of grouped) counts[String(row._id)] = row.n;
  res.json({ jobs: await attachBatches(rows.map(publicJob)), counts, total: rows.length });
});

jobsRouter.get("/batch/:batchId", async (req, res) => {
  const rows = await CheckoutJob.find({ batchId: req.params.batchId })
    .select("-logs")
    .sort({ createdAt: 1 })
    .lean();
  const first = rows[0];
  const totalQuantity = first?.totalQuantity || 0;
  const totalAttempts = first?.totalAttempts || 0;
  const counters =
    totalQuantity && totalAttempts
      ? await readBatchProgress(req.params.batchId, totalQuantity, totalAttempts)
      : undefined;
  res.json({
    batchId: req.params.batchId,
    jobs: await attachBatches(rows.map(publicJob)),
    counters,
    batch: await CheckoutBatch.findOne({ batchId: req.params.batchId }).lean(),
  });
});

jobsRouter.get("/:id", async (req, res) => {
  const job = await CheckoutJob.findById(req.params.id);
  if (!job) {
    res.status(404).json({ error: "Job not found" });
    return;
  }
  res.json(publicJob(job));
});

jobsRouter.post("/:id/cancel", async (req, res) => {
  const job = await CheckoutJob.findById(req.params.id);
  if (!job) {
    res.status(404).json({ error: "Job not found" });
    return;
  }
  if (
    job.status === "reached_payment" ||
    job.status === "failed" ||
    job.status === "cancelled" ||
    job.status === "skipped" ||
    job.status === "dry_run" ||
    job.status === "completed_target_reached" ||
    job.status === "failed_attempt_budget_exhausted" ||
    job.status === "filtered" ||
    // Parked after Pay was pressed: cancelling would hide a possible charge.
    job.status === "needs_reconciliation"
  ) {
    res.json(publicJob(job));
    return;
  }
  job.status = "cancelled";
  job.step = "cancelled";
  job.completedAt = new Date();
  job.logs.push({ at: new Date(), level: "warn", step: "cancelled", message: "Cancelled by API" });
  await job.save();
  try {
    // The job sits on its class's queue (and, if it was requeued while
    // waiting for capacity, under a suffixed id on the same queue).
    const req = job.request;
    const queue = getCheckoutQueue(
      jobClassFor({ paymentMode: req?.paymentMode, authType: req?.authType, corporateId: req?.corporateId })
    );
    const queued = await queue.getJob(String(job._id));
    if (queued) await queued.remove();
    for (const j of await queue.getDelayed()) {
      if (String(j.id || "").startsWith(`${String(job._id)}:w`)) await j.remove().catch(() => undefined);
    }
  } catch {
    /* ignore */
  }
  res.json(publicJob(job));
});
