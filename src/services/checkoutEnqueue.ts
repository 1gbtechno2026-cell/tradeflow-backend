import { CheckoutJob } from "../models/CheckoutJob.js";
import { getCheckoutQueue } from "../queue.js";
import { jobClassFor } from "./jobClass.js";
import type { AuthType, CardDetails } from "../paymentStrategies/types.js";
import type { AddressDetails, CheckoutJobData, JobRequestSnapshot } from "../types.js";

/**
 * Put a claimed-but-not-startable job back on its queue after `delayMs`,
 * WITHOUT it counting as an attempt. For capacity, not failures: no handset
 * free under the corporate, every card paused. The job's own status goes back
 * to `queued` so the next claim is a normal one; the delay is BullMQ's.
 */
export async function requeueCheckoutJob(data: CheckoutJobData, delayMs: number, reason: string): Promise<void> {
  const now = new Date();
  await CheckoutJob.updateOne(
    { _id: data.jobId, status: "running" },
    {
      $set: { status: "queued", step: "waiting", claimedBy: null, heartbeatAt: null },
      $push: { logs: { at: now, level: "info", step: "waiting", message: `${reason} — back in the queue in ${Math.round(delayMs / 1000)}s, no attempt used` } },
    }
  );
  // A fresh BullMQ job id each time: the original id is still "active" until
  // this processor returns, and BullMQ refuses a duplicate.
  await getCheckoutQueue(jobClassFor(data)).add("checkout", data, {
    jobId: `${data.jobId}:w${now.getTime().toString(36)}`,
    delay: Math.max(1000, delayMs),
  });
}

export async function enqueueCheckoutJob(input: {
  userId: string;
  batchId: string;
  email: string;
  platformId?: string;
  productUrl: string;
  quantityPerOrder: number;
  totalQuantity: number;
  totalAttempts: number;
  cartAmountLimit?: number;
  deliverySlaDays?: number;
  gstMandatory?: boolean;
  address: AddressDetails;
  request?: JobRequestSnapshot;
  dryRun?: boolean;
  isRetry?: boolean;
  paymentMode?: string;
  cardType?: string;
  authType?: AuthType;
  corporateId?: string | null;
  cardMaxUsage?: number;
  /** Unmasked. Goes on the Redis payload ONLY — see CheckoutJobData.cards. It is
   *  never written to the CheckoutJob document, whose `request.cards` stays
   *  masked by orderSnapshot.maskCards. */
  cards?: CardDetails[];
}): Promise<{ job: InstanceType<typeof CheckoutJob>; queued: boolean }> {
  const perOrderQty = input.quantityPerOrder;
  const created = await CheckoutJob.create({
    batchId: input.batchId,
    userId: input.userId,
    email: input.email,
    platformId: input.platformId,
    productUrl: input.productUrl,
    quantity: perOrderQty,
    totalQuantity: input.totalQuantity,
    quantityPerOrder: perOrderQty,
    totalAttempts: input.totalAttempts,
    cartAmountLimit: input.cartAmountLimit,
    address: input.address,
    request: input.request,
    status: input.dryRun ? "dry_run" : "queued",
    step: input.dryRun ? "dry_run" : "queued",
    completedAt: input.dryRun ? new Date() : null,
    logs: [
      {
        at: new Date(),
        level: "info",
        step: input.dryRun ? "dry_run" : input.isRetry ? "retry" : "queued",
        message: input.dryRun
          ? `Dry run — session OK for ${input.email}, Flipkart not opened`
          : input.isRetry
            ? `Retry queued for ${input.email} (same batch, qty ${perOrderQty})`
            : `Queued for ${input.email}`,
      },
    ],
  });

  if (input.dryRun) {
    return { job: created, queued: false };
  }

  const payload: CheckoutJobData = {
    jobId: String(created._id),
    batchId: input.batchId,
    email: input.email,
    productUrl: input.productUrl,
    quantity: perOrderQty,
    totalQuantity: input.totalQuantity,
    quantityPerOrder: perOrderQty,
    totalAttempts: input.totalAttempts,
    cartAmountLimit: input.cartAmountLimit,
    deliverySlaDays: input.deliverySlaDays,
    gstMandatory: input.gstMandatory,
    address: input.address,
    isRetry: input.isRetry,
    paymentMode: input.paymentMode,
    cardType: input.cardType,
    authType: input.authType,
    corporateId: input.corporateId,
    cardMaxUsage: input.cardMaxUsage,
    cards: input.cards,
  };
  // Routed by concurrency class: otp-phone / card / cod each have their own
  // queue, cap and workers (see jobClass.ts, queue.ts).
  const queued = await getCheckoutQueue(jobClassFor(payload)).add("checkout", payload, { jobId: String(created._id) });
  created.bullmqJobId = queued.id || String(created._id);
  await created.save();
  return { job: created, queued: true };
}
