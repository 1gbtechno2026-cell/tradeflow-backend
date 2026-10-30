import { randomUUID } from "node:crypto";
import { Router } from "express";
import { z } from "zod";
import { CheckoutJob } from "../models/CheckoutJob.js";
import { CheckoutBatch } from "../models/CheckoutBatch.js";
import { getCheckoutQueue } from "../queue.js";
import { JOB_CLASSES, jobClassFor } from "../services/jobClass.js";
import { effectiveAttemptsFor, parseCreateJobBody, requiredOrdersFor } from "../services/jobRequest.js";
import { readBatchProgress, settleBatchIfCancelling } from "../services/batchCounters.js";
import { config } from "../config.js";
import { enqueueCheckoutJob } from "../services/checkoutEnqueue.js";
import { requestSnapshot } from "../services/orderSnapshot.js";
import { resolveAddress, resolveLoggedInSession, workspaceUserId } from "../services/sessionStore.js";
import { toCardDetails } from "../services/paymentCards.js";
import { resyncCheckoutJob } from "../services/orderSync.js";
import { batchSummary, listBatches } from "../services/batchList.js";
import { containsRegex, listParam, paginationMeta, parseDateRange, parsePagination, rangeFilter } from "../services/listFilters.js";
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
  systemId?: string;
  logs?: unknown[];
  startedAt: Date | null;
  completedAt: Date | null;
  orderSync?: unknown;
  createdAt: Date;
  updatedAt: Date;
}) {
  return {
    id: String(job._id),
    tradeflowId: String(job._id),
    batchId: job.batchId,
    email: job.email,
    userId: job.userId,
    systemId: job.systemId || undefined,
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
    /** Whether this attempt's order reached Order Units (services/orderSync.ts). */
    orderSync: job.orderSync ?? null,
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
        systemId: b.systemId || "",
        cancelRequestedAt: b.cancelRequestedAt ?? null,
        cancelRequestedBy: b.cancelRequestedBy || "",
        cancelledAt: b.cancelledAt ?? null,
        completedAt: b.completedAt ?? null,
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
        quantity_per_order_mode: data.quantityPerOrderMode,
        total_attempts: effectiveAttempts,
        required_orders: requiredOrders,
        cart_amount_limit: data.cartAmountLimit,
        jobs,
      });
      return;
    }

    if (!data.dryRun) {
      // The batch document is the progress ledger (services/batchCounters.ts) and the unit of cancellation.
      await CheckoutBatch.create({
        batchId,
        userId,
        systemId: config.systemId,
        batchStatus: "running",
        filterReason: null,
        filteredCount: 0,
        purchasedQuantity: 0,
        attemptsUsed: 0,
        totalQuantity,
        quantityPerOrder: perOrderQty,
        quantityPerOrderMode: data.quantityPerOrderMode,
        totalAttempts: effectiveAttempts,
        productUrl: data.productUrl,
        paymentMode: data.paymentMode || "",
        createdBy: String(req.header("x-requested-by") || ""),
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
        quantityMode: data.quantityPerOrderMode,
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

/**
 * The orders listing ("All Orders"). Paged and filtered on the server:
 *   page, page_size (≤ 500)            default 100 per page; `limit` alone still works as page 1
 *   range_mode / range_key / start / end   see services/listFilters.ts
 *   status=a,b   platform=FLIPKART   batchId=x,y   email=<contains>   product=<contains>
 *   orderId=OD…,OD…   (Flipkart order id or the Trade Flow id)
 *   system=mine|all|<id>   ordering=-created_at|-updated_at|created_at
 */
jobsRouter.get("/", async (req, res) => {
  const userId = workspaceUserId();
  const q = req.query as Record<string, unknown>;
  const filter: Record<string, unknown> = { userId };
  const statuses = listParam(q, "status");
  if (statuses.length) filter.status = statuses.length === 1 ? statuses[0] : { $in: statuses };
  // ?system=mine → only this deployment's jobs (pre-systemId rows count as ours); ?system=<id> → that one; default all.
  const system = String(q.system || "").trim();
  if (system === "mine") filter.systemId = { $in: [config.systemId, "", null] };
  else if (system && system !== "all") filter.systemId = system;
  const range = parseDateRange(q);
  const created = rangeFilter(range);
  if (created) filter.createdAt = created;
  const batchIds = listParam(q, "batchId").map((s) => s.replace(/^#/, ""));
  // A single short value is a prefix typed by hand ("#1fcd5ced"); full ids match exactly.
  if (batchIds.length === 1 && batchIds[0].length < 36) filter.batchId = new RegExp(`^${batchIds[0].replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`, "i");
  else if (batchIds.length) filter.batchId = batchIds.length === 1 ? batchIds[0] : { $in: batchIds };
  const email = String(q.email || "").trim();
  if (email) filter.email = containsRegex(email);
  const platforms = listParam(q, "platform").map((p) => p.toUpperCase());
  const and: Record<string, unknown>[] = [];
  if (platforms.length) {
    and.push(
      platforms.includes("FLIPKART")
        ? { $or: [{ "request.platform": { $in: platforms } }, { "request.platform": { $exists: false } }, { "request.platform": "" }] }
        : { "request.platform": { $in: platforms } }
    );
  }
  const orderIds = listParam(q, "orderId").map((s) => s.toUpperCase());
  if (orderIds.length) {
    const objectIds = orderIds.map((s) => s.toLowerCase()).filter((s) => /^[0-9a-f]{24}$/.test(s));
    and.push({
      $or: [
        { "result.flipkartOrderId": { $in: orderIds } },
        { "result.flipkartReferenceId": { $in: orderIds } },
        ...(objectIds.length ? [{ _id: { $in: objectIds } }] : []),
      ],
    });
  }
  const product = String(q.product || "").trim();
  if (product) {
    const rx = containsRegex(product);
    and.push({ $or: [{ "result.productName": rx }, { "product.model": rx }, { productUrl: rx }] });
  }
  // Advanced filters (the reference UI's second filter row).
  const stage = String(q.stage || "").trim();
  if (stage) and.push({ $or: [{ errorStage: stage }, { failedStep: stage }] });
  const errorCodes = listParam(q, "errorCode");
  if (errorCodes.length) filter.errorCode = { $in: errorCodes };
  const cardName = String(q.cardName || "").trim();
  if (cardName) {
    const rx = containsRegex(cardName);
    and.push({ $or: [{ "result.cardName": rx }, { "request.cards.name": rx }] });
  }
  const cardLast4 = String(q.cardNumber || "").replace(/\D/g, "").slice(-4);
  if (cardLast4) {
    const rx = new RegExp(`${cardLast4}$`);
    and.push({ $or: [{ "result.childCardLast4": rx }, { "result.cardLast4": rx }, { "result.parentCardLast4": rx }, { "request.cards.cardNumberMasked": rx }, { "request.cards.parentCardNumberMasked": rx }] });
  }
  const employeeIds = listParam(q, "employeeId");
  if (employeeIds.length) filter["result.employeeId"] = { $in: employeeIds };
  const cartAmount = String(q.cartAmount || "").replace(/[₹,\s]/g, "");
  if (cartAmount && /^\d+(\.\d+)?$/.test(cartAmount)) {
    const n = Math.round(Number(cartAmount));
    const rx = new RegExp(`^₹?\\s?(${n.toLocaleString("en-IN")}|${n})(\\.\\d{1,2})?$`);
    and.push({ $or: [{ "result.cartAmount": rx }, { "result.transactionAmount": rx }, { "result.cartAfterCardOffer": rx }] });
  }
  if (and.length) filter.$and = and;

  // ?with_meta=1 → the choices the advanced filters offer (error codes and stages seen on this workspace's jobs).
  const metadata =
    String(q.with_meta || "") === "1"
      ? await (async () => {
          const [codes, stages] = await Promise.all([
            CheckoutJob.aggregate<{ _id: { code: string; display: string }; n: number }>([
              { $match: { userId, errorCode: { $nin: ["", null] } } },
              { $group: { _id: { code: "$errorCode", display: { $ifNull: ["$errorCodeDisplay", ""] } }, n: { $sum: 1 } } },
              { $sort: { n: -1 } },
              { $limit: 60 },
            ]),
            CheckoutJob.aggregate<{ _id: string; n: number }>([
              { $match: { userId, $or: [{ errorStage: { $nin: ["", null] } }, { failedStep: { $nin: ["", null] } }] } },
              { $group: { _id: { $ifNull: ["$errorStage", "$failedStep"] }, n: { $sum: 1 } } },
              { $sort: { n: -1 } },
              { $limit: 40 },
            ]),
          ]);
          return {
            error_codes: codes.map((c) => ({ value: c._id.code, label: c._id.display || c._id.code, n: c.n })),
            stages: stages.filter((s) => s._id).map((s) => ({ value: s._id, n: s.n })),
          };
        })()
      : undefined;

  const page = parsePagination(q, { pageSize: q.page === undefined && q.page_size === undefined ? 80 : 100, maxPageSize: 500 });
  const ordering = String(q.ordering || "-created_at");
  const sort: Record<string, 1 | -1> =
    ordering === "created_at" ? { createdAt: 1 } : ordering === "-updated_at" ? { updatedAt: -1 } : ordering === "-started_at" ? { startedAt: -1 } : { createdAt: -1 };

  // Omit logs on list — unbounded log arrays OOM the API under jobs-proxy polling.
  const [rows, count, grouped] = await Promise.all([
    CheckoutJob.find(filter).select("-logs").sort(sort).skip(page.skip).limit(page.pageSize).lean(),
    CheckoutJob.countDocuments(filter),
    CheckoutJob.aggregate([{ $match: { userId } }, { $group: { _id: "$status", n: { $sum: 1 } } }]),
  ]);
  const counts: Record<string, number> = {};
  for (const row of grouped) counts[String(row._id)] = row.n;
  res.json({
    jobs: await attachBatches(rows.map(publicJob)),
    counts,
    total: count,
    pagination: paginationMeta(count, page),
    range: { mode: range.mode, key: range.key, from: range.from, to: range.to },
    metadata,
    systemId: config.systemId,
  });
});

/** The "Bulk Orders" listing: one row per batch, paged and filtered on the server (services/batchList.ts). */
jobsRouter.get("/batches", async (req, res) => {
  try {
    res.json(await listBatches(workspaceUserId(), req.query as Record<string, unknown>, config.systemId));
  } catch (err) {
    res.status(500).json({ error: err instanceof Error ? err.message : "Could not list batches" });
  }
});

/** One batch's configuration (the request it was placed with) and its failure breakdown. */
jobsRouter.get("/batch/:batchId/summary", async (req, res) => {
  const summary = await batchSummary(req.params.batchId);
  if (!summary) {
    res.status(404).json({ error: "Batch not found" });
    return;
  }
  res.json(summary);
});

// ── Batch-level cancel ──────────────────────────────────────────────────────
// "Stop placing": waiting attempts are cancelled and taken off this system's
// queues, attempts still before Pay are marked cancelled and stop at the
// runner's next checkpoint (which gives their reservation back), attempts past
// Pay are left to finish — money is never left ambiguous. Nothing is deleted;
// who / when / why is recorded on the batch. Idempotent.

/** Every BullMQ entry of this batch still waiting in this system's queues. */
async function removeQueuedForBatch(batchId: string): Promise<number> {
  let removed = 0;
  for (const cls of JOB_CLASSES) {
    const q = getCheckoutQueue(cls);
    const waiting = await q.getJobs(["waiting", "delayed", "prioritized", "paused"]);
    for (const j of waiting) {
      if (j.data?.batchId !== batchId) continue;
      await j.remove().catch(() => undefined);
      removed += 1;
    }
  }
  return removed;
}

type CancelBatchResult =
  | { ok: false; status: 404 | 409; error: string; systemId?: string }
  | {
      ok: true;
      batchId: string;
      batchStatus: string;
      systemId: string;
      queuedCancelled: number;
      removedFromQueue: number;
      runningAborted: number;
      finishingAfterPay: number;
      purchasedQuantity: number;
      attemptsUsed: number;
      totalQuantity: number;
      totalAttempts: number;
    };

async function cancelBatch(batchId: string, by: string, reason: string, force: boolean): Promise<CancelBatchResult> {
  const batch = await CheckoutBatch.findOne({ batchId }).lean();
  if (!batch) return { ok: false, status: 404, error: "Batch not found" };
  const foreign = Boolean(batch.systemId) && batch.systemId !== config.systemId;
  if (foreign && !force) {
    return { ok: false, status: 409, error: `This batch belongs to system "${batch.systemId}" (this is "${config.systemId}"); cancel it from there, or pass force to mark it cancelled here (its queue entries there cannot be removed from here)`, systemId: batch.systemId };
  }
  const now = new Date();
  await CheckoutBatch.updateOne(
    { batchId, batchStatus: "running" },
    { $set: { batchStatus: "cancelling", cancelRequestedAt: now, cancelRequestedBy: by, cancelReason: reason } }
  );
  // A batch already completed/exhausted can still have retries open; record the request without changing its status.
  await CheckoutBatch.updateOne({ batchId, cancelRequestedAt: null }, { $set: { cancelRequestedAt: now, cancelRequestedBy: by, cancelReason: reason } });

  const who = by || "operator";
  const base = `Batch cancelled by ${who}${reason ? ` — ${reason}` : ""}`;
  const queued = await CheckoutJob.updateMany(
    { batchId, status: "queued" },
    { $set: { status: "cancelled", step: "cancelled", completedAt: now }, $push: { logs: { at: now, level: "warn", step: "cancelled", message: base } } }
  );
  const removed = foreign ? 0 : await removeQueuedForBatch(batchId);
  // Before Pay: the runner sees `cancelled` at its next checkpoint, closes Chrome and releases the reservation.
  const aborted = await CheckoutJob.updateMany(
    { batchId, status: "running", paymentStartedAt: null },
    { $set: { status: "cancelled", step: "cancelled", completedAt: now }, $push: { logs: { at: now, level: "warn", step: "cancelled", message: `${base} — attempt stopped before Pay` } } }
  );
  const finishing = await CheckoutJob.countDocuments({ batchId, status: "running", paymentStartedAt: { $ne: null } });
  const status = await settleBatchIfCancelling(batchId);
  const after = await CheckoutBatch.findOne({ batchId }).lean();
  console.log(`[trade-flow] batch ${batchId.slice(0, 8)} cancel by ${who}: queued ${queued.modifiedCount} · removed ${removed} · aborted ${aborted.modifiedCount} · finishing after Pay ${finishing} → ${status}`);
  return {
    ok: true,
    batchId,
    batchStatus: String(status ?? after?.batchStatus ?? "cancelling"),
    systemId: after?.systemId || "",
    queuedCancelled: queued.modifiedCount,
    removedFromQueue: removed,
    runningAborted: aborted.modifiedCount,
    finishingAfterPay: finishing,
    purchasedQuantity: after?.purchasedQuantity ?? 0,
    attemptsUsed: after?.attemptsUsed ?? 0,
    totalQuantity: after?.totalQuantity ?? 0,
    totalAttempts: after?.totalAttempts ?? 0,
  };
}

jobsRouter.post("/batch/:batchId/cancel", async (req, res) => {
  const body = (req.body || {}) as { by?: unknown; reason?: unknown; force?: unknown };
  const result = await cancelBatch(String(req.params.batchId), String(body.by || ""), String(body.reason || ""), body.force === true);
  if (!result.ok) {
    res.status(result.status).json({ error: result.error, systemId: result.systemId });
    return;
  }
  res.json(result);
});

/** Empty this system's queue: cancel every batch of this workspace that still has open attempts here, then drop any stray entries. */
jobsRouter.post("/cancel-all", async (req, res) => {
  const body = (req.body || {}) as { by?: unknown; reason?: unknown };
  const by = String(body.by || "");
  const reason = String(body.reason || "queue emptied");
  const userId = workspaceUserId();
  const openBatches = (await CheckoutJob.distinct("batchId", { userId, status: { $in: ["queued", "running"] }, systemId: { $in: [config.systemId, "", null] } })) as string[];
  const batches: CancelBatchResult[] = [];
  for (const id of openBatches) batches.push(await cancelBatch(id, by, reason, false));
  let strays = 0;
  for (const cls of JOB_CLASSES) {
    const q = getCheckoutQueue(cls);
    const n = await q.getJobCounts("wait", "delayed", "prioritized");
    strays += (n.wait || 0) + (n.delayed || 0) + (n.prioritized || 0);
    await q.drain(true);
  }
  console.log(`[trade-flow] cancel-all by ${by || "operator"}: ${openBatches.length} batch(es), ${strays} stray queue entr${strays === 1 ? "y" : "ies"} drained`);
  res.json({ systemId: config.systemId, batches, drainedStrays: strays });
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

/** Put this attempt's order into Order Units again (or for the first time) — the
 *  manual lever for a sync that gave up, e.g. after the ID was re-logged in. */
jobsRouter.post("/:id/order-sync", async (req, res) => {
  const job = await CheckoutJob.findById(req.params.id).select("_id status");
  if (!job) {
    res.status(404).json({ error: "Job not found" });
    return;
  }
  await resyncCheckoutJob(String(job._id));
  const after = await CheckoutJob.findById(job._id).select("orderSync").lean();
  res.status(202).json({ jobId: String(job._id), orderSync: after?.orderSync ?? null });
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
      if (String(j.id || "").startsWith(`${String(job._id)}-w`)) await j.remove().catch(() => undefined);
    }
  } catch {
    /* ignore */
  }
  res.json(publicJob(job));
});
