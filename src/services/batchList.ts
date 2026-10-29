/**
 * The Orders tab's "Bulk Orders" listing and the per-batch summary behind
 * its configuration / error-summary dialogs.
 *
 * A bulk order is one CheckoutBatch row; what the table shows beside it
 * (platform, card type, address, success / failure counts, time taken) is
 * aggregated from that batch's CheckoutJobs — one aggregation per page, over
 * the page's batch ids only, so a 90-day listing never scans every job.
 */
import { CheckoutBatch, type ICheckoutBatch } from "../models/CheckoutBatch.js";
import { CheckoutJob } from "../models/CheckoutJob.js";
import { containsRegex, listParam, paginationMeta, parseDateRange, parsePagination, rangeFilter } from "./listFilters.js";

const SUCCESS = ["paid", "reached_payment"];
const FAILED = ["failed", "skipped", "failed_attempt_budget_exhausted"];

export const BATCH_STATUS_CHOICES = [
  { value: "running", label: "Running" },
  { value: "completed", label: "Completed" },
  { value: "exhausted", label: "Exhausted" },
  { value: "cancelling", label: "Cancelling" },
  { value: "cancelled", label: "Cancelled" },
  { value: "filtered", label: "Filtered" },
];

export const PLATFORM_CHOICES = [
  { value: "FLIPKART", label: "Flipkart" },
  { value: "AMAZON", label: "Amazon" },
];

interface JobRollup {
  _id: string;
  jobs: number;
  ok: number;
  failed: number;
  cancelled: number;
  running: number;
  queued: number;
  filtered: number;
  needsReconciliation: number;
  mock: number;
  unitsOk: number;
  firstStart: Date | null;
  lastEnd: Date | null;
  lastUpdate: Date | null;
  platform: string | null;
  cardType: string | null;
  corporateId: string | null;
  paymentMode: string | null;
  executionVersion: string | null;
  authType: string | null;
  dryRun: boolean | null;
  address: Record<string, string> | null;
  productName: string | null;
  colour: string | null;
  productUrl: string | null;
  parentCardsMasked: Array<string | null>;
  parentCardsLast4: Array<string | null>;
  emails: number;
}

async function rollupJobs(batchIds: string[]): Promise<Map<string, JobRollup>> {
  if (!batchIds.length) return new Map();
  const rows = await CheckoutJob.aggregate<JobRollup & { emailSet?: string[] }>([
    { $match: { batchId: { $in: batchIds } } },
    { $sort: { createdAt: 1 } },
    {
      $group: {
        _id: "$batchId",
        jobs: { $sum: 1 },
        ok: { $sum: { $cond: [{ $in: ["$status", SUCCESS] }, 1, 0] } },
        failed: { $sum: { $cond: [{ $in: ["$status", FAILED] }, 1, 0] } },
        cancelled: { $sum: { $cond: [{ $eq: ["$status", "cancelled"] }, 1, 0] } },
        running: { $sum: { $cond: [{ $eq: ["$status", "running"] }, 1, 0] } },
        queued: { $sum: { $cond: [{ $eq: ["$status", "queued"] }, 1, 0] } },
        filtered: { $sum: { $cond: [{ $eq: ["$status", "filtered"] }, 1, 0] } },
        needsReconciliation: { $sum: { $cond: [{ $eq: ["$status", "needs_reconciliation"] }, 1, 0] } },
        mock: { $sum: { $cond: [{ $eq: ["$status", "dry_run"] }, 1, 0] } },
        unitsOk: {
          $sum: {
            $cond: [{ $in: ["$status", SUCCESS] }, { $ifNull: ["$result.quantityPlaced", { $ifNull: ["$quantity", 1] }] }, 0],
          },
        },
        firstStart: { $min: "$startedAt" },
        lastEnd: { $max: "$completedAt" },
        lastUpdate: { $max: "$updatedAt" },
        platform: { $first: "$request.platform" },
        cardType: { $first: "$request.cardType" },
        corporateId: { $first: "$request.corporateId" },
        paymentMode: { $first: "$request.paymentMode" },
        executionVersion: { $first: "$request.executionVersion" },
        authType: { $first: "$request.authType" },
        dryRun: { $first: "$request.dryRun" },
        address: { $first: "$address" },
        productName: { $max: "$result.productName" },
        colour: { $max: "$result.colour" },
        productUrl: { $first: "$productUrl" },
        parentCardsMasked: { $addToSet: { $arrayElemAt: ["$request.cards.parentCardNumberMasked", 0] } },
        parentCardsLast4: { $addToSet: "$result.parentCardLast4" },
        emailSet: { $addToSet: "$email" },
      },
    },
    { $addFields: { emails: { $size: "$emailSet" } } },
    { $project: { emailSet: 0 } },
  ]);
  return new Map(rows.map((r) => [String(r._id), r]));
}

/** "55XX...100233" style, the way the reference table prints a parent card. */
function shortCard(masked: string): string {
  const digits = masked.replace(/[^0-9X*]/gi, "");
  if (digits.length < 10) return masked;
  return `${digits.slice(0, 4)}...${digits.slice(-6)}`;
}

export function batchRow(b: ICheckoutBatch, r: JobRollup | undefined) {
  const parentCards = [
    ...new Set(
      [
        ...(r?.parentCardsMasked || []).filter((s): s is string => Boolean(s)).map(shortCard),
        ...(r?.parentCardsLast4 || []).filter((s): s is string => Boolean(s)).map((s) => `…${s}`),
      ].filter(Boolean)
    ),
  ];
  const startedAt = r?.firstStart ?? null;
  const endedAt = b.completedAt ?? b.cancelledAt ?? r?.lastEnd ?? null;
  const open = (r?.running || 0) + (r?.queued || 0);
  const addr = r?.address || {};
  return {
    batchId: b.batchId,
    createdAt: b.createdAt,
    updatedAt: r?.lastUpdate && r.lastUpdate > b.updatedAt ? r.lastUpdate : b.updatedAt,
    batchStatus: b.batchStatus,
    systemId: b.systemId || "",
    platform: String(r?.platform || "FLIPKART").toUpperCase(),
    mock: Boolean(r?.dryRun) || (r ? r.mock > 0 && r.mock === r.jobs : false),
    executionVersion: r?.executionVersion || "",
    productName: b.productName || r?.productName || "",
    // A scraped colour is a short word or two; anything else is page text that leaked in.
    colour: r?.colour && /^[A-Za-z][A-Za-z &-]{1,24}$/.test(r.colour) && !/search|product/i.test(r.colour) ? r.colour : "",
    productUrl: b.productUrl || r?.productUrl || "",
    totalQuantity: b.totalQuantity,
    quantityPerOrder: b.quantityPerOrder,
    totalAttempts: b.totalAttempts,
    purchasedQuantity: b.purchasedQuantity,
    attemptsUsed: b.attemptsUsed,
    unitsOk: r?.unitsOk || 0,
    counts: {
      jobs: r?.jobs || 0,
      ok: r?.ok || 0,
      failed: r?.failed || 0,
      cancelled: r?.cancelled || 0,
      running: r?.running || 0,
      queued: r?.queued || 0,
      filtered: r?.filtered || 0,
      needsReconciliation: r?.needsReconciliation || 0,
      emails: r?.emails || 0,
    },
    open,
    createdBy: b.createdBy || "",
    corporateId: r?.corporateId || "",
    cardType: r?.cardType || "",
    authType: r?.authType || "",
    parentCards,
    paymentMode: b.paymentMode || r?.paymentMode || "",
    address: {
      label: addr.label || "",
      name: addr.name || "",
      companyName: addr.companyName || "",
      city: addr.city || "",
      pincode: addr.pincode || "",
      gstLabel: addr.gstLabel || "",
      gstNumber: addr.gstNumber || "",
    },
    startedAt,
    completedAt: endedAt,
    timeTakenMs: startedAt && endedAt ? Math.max(0, new Date(endedAt).getTime() - new Date(startedAt).getTime()) : startedAt && open ? Math.max(0, Date.now() - new Date(startedAt).getTime()) : null,
    filterReason: b.filterReason,
    filteredCount: b.filteredCount,
    cancelRequestedAt: b.cancelRequestedAt,
    cancelRequestedBy: b.cancelRequestedBy,
    cancelReason: b.cancelReason,
    cancelledAt: b.cancelledAt,
  };
}

export type BatchListRow = ReturnType<typeof batchRow>;

export async function listBatches(userId: string, query: Record<string, unknown>, systemId: string) {
  const range = parseDateRange(query);
  const page = parsePagination(query, { pageSize: 50, maxPageSize: 200 });
  const filter: Record<string, unknown> = { userId };
  const created = rangeFilter(range);
  if (created) filter.createdAt = created;

  const statuses = listParam(query, "status").map((s) => s.toLowerCase());
  if (statuses.length) filter.batchStatus = { $in: statuses };
  else if (String(query.include_filtered || "") !== "1") filter.batchStatus = { $ne: "filtered" };

  const system = String(query.system || "").trim();
  if (system === "mine") filter.systemId = { $in: [systemId, "", null] };
  else if (system && system !== "all") filter.systemId = system;

  const createdBy = listParam(query, "created_by");
  if (createdBy.length) filter.createdBy = { $in: createdBy };

  const q = String(query.q || query.product || "").trim();
  if (q) filter.productName = containsRegex(q);

  const batchIdsWanted = listParam(query, "batchId");
  if (batchIdsWanted.length) filter.batchId = { $in: batchIdsWanted };

  // Platform and mock/real live on the jobs, not the batch row — narrow by batch id first.
  const platforms = listParam(query, "platform").map((p) => p.toUpperCase());
  const kind = String(query.kind || "").toLowerCase(); // "mock" | "real" | ""
  if ((platforms.length && !(platforms.length >= PLATFORM_CHOICES.length)) || kind === "mock" || kind === "real") {
    const jobMatch: Record<string, unknown> = { userId };
    if (created) jobMatch.createdAt = created;
    if (platforms.length) {
      jobMatch.$or = platforms.includes("FLIPKART")
        ? [{ "request.platform": { $in: platforms } }, { "request.platform": { $exists: false } }, { "request.platform": "" }]
        : [{ "request.platform": { $in: platforms } }];
    }
    if (kind === "mock") jobMatch["request.dryRun"] = true;
    if (kind === "real") jobMatch["request.dryRun"] = { $ne: true };
    const ids = (await CheckoutJob.distinct("batchId", jobMatch)) as string[];
    const allowed = batchIdsWanted.length ? ids.filter((id) => batchIdsWanted.includes(id)) : ids;
    filter.batchId = { $in: allowed };
  }

  const ordering = String(query.ordering || "-created_at");
  const sort: Record<string, 1 | -1> =
    ordering === "created_at" ? { createdAt: 1 } : ordering === "-updated_at" ? { updatedAt: -1 } : { createdAt: -1 };

  const [count, rows] = await Promise.all([
    CheckoutBatch.countDocuments(filter),
    CheckoutBatch.find(filter).sort(sort).skip(page.skip).limit(page.pageSize).lean<ICheckoutBatch[]>(),
  ]);
  const rollups = await rollupJobs(rows.map((b) => b.batchId));
  const results = rows.map((b) => batchRow(b, rollups.get(b.batchId)));

  const creators = (await CheckoutBatch.distinct("createdBy", { userId })) as string[];
  return {
    metadata: {
      platform_choices: PLATFORM_CHOICES,
      status_choices: BATCH_STATUS_CHOICES,
      created_by_choices: creators.filter(Boolean).sort(),
      range: { mode: range.mode, key: range.key, from: range.from, to: range.to },
    },
    pagination: paginationMeta(count, page),
    results,
  };
}

/** Everything the configuration and error-summary dialogs show for one batch. */
export async function batchSummary(batchId: string) {
  const batch = await CheckoutBatch.findOne({ batchId }).lean<ICheckoutBatch>();
  if (!batch) return null;
  const [rollups, first, errors, stages] = await Promise.all([
    rollupJobs([batchId]),
    CheckoutJob.findOne({ batchId }).sort({ createdAt: 1 }).select("request address cartAmountLimit productUrl quantity totalQuantity quantityPerOrder totalAttempts product result.productName result.colour result.sellerName").lean(),
    CheckoutJob.aggregate<{ _id: { code: string; display: string; source: string; stage: string; status: string }; n: number; sample: string }>([
      { $match: { batchId, status: { $in: [...FAILED, "cancelled", "needs_reconciliation", "filtered"] } } },
      {
        $group: {
          _id: {
            code: { $ifNull: ["$errorCode", ""] },
            display: { $ifNull: ["$errorCodeDisplay", ""] },
            source: { $ifNull: ["$errorSource", ""] },
            stage: { $ifNull: ["$errorStage", ""] },
            status: "$status",
          },
          n: { $sum: 1 },
          sample: { $first: { $ifNull: ["$errorDetails", { $ifNull: ["$failureMessage", "$error"] }] } },
        },
      },
      { $sort: { n: -1 } },
      { $limit: 40 },
    ]),
    CheckoutJob.aggregate<{ _id: string; n: number }>([
      { $match: { batchId, status: { $in: FAILED } } },
      { $group: { _id: { $ifNull: ["$errorStage", { $ifNull: ["$failedStep", ""] }] }, n: { $sum: 1 } } },
      { $sort: { n: -1 } },
    ]),
  ]);
  const row = batchRow(batch, rollups.get(batchId));
  const failedTotal = errors.reduce((n, e) => n + e.n, 0);
  return {
    batch: row,
    request: first?.request ?? null,
    address: first?.address ?? null,
    cartAmountLimit: first?.cartAmountLimit ?? null,
    product: {
      name: row.productName || first?.result?.productName || first?.product?.model || "",
      colour: row.colour || "",
      url: row.productUrl,
      seller: first?.result?.sellerName || first?.request?.sellerName || "",
      listingId: first?.request?.listingId || "",
    },
    errors: {
      failedOrders: failedTotal,
      types: errors.length,
      breakdown: errors.map((e) => ({
        status: e._id.status,
        code: e._id.code,
        display: e._id.display || e._id.code || (e._id.status === "cancelled" ? "Cancelled" : "Unknown"),
        source: e._id.source,
        stage: e._id.stage,
        count: e.n,
        pct: failedTotal ? Math.round((e.n / failedTotal) * 1000) / 10 : 0,
        sample: e.sample || "",
      })),
      byStage: stages.map((s) => ({ stage: s._id || "—", count: s.n })),
    },
  };
}
