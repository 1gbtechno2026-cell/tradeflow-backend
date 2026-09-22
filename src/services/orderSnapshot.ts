import type { NormalizedCreateJob } from "./jobRequest.js";
import type { JobRequestSnapshot, MaskedCard } from "../types.js";

/** 16-digit PAN → 54XXXXXXXX000759 style. Never keep CVV / PIN / password. */
export function maskPan(raw: unknown): string {
  const digits = String(raw || "").replace(/\D/g, "");
  if (digits.length < 10) return "";
  const first = digits.slice(0, 2);
  const last = digits.slice(-6);
  return `${first}${"X".repeat(Math.max(4, digits.length - first.length - last.length))}${last}`;
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

export function maskCards(cards: unknown[] | undefined): MaskedCard[] {
  if (!Array.isArray(cards)) return [];
  const out: MaskedCard[] = [];
  for (const row of cards) {
    const c = asRecord(row);
    const cardNumberMasked = maskPan(c.card_number ?? c.cardNumber);
    if (!cardNumberMasked) continue;
    out.push({
      name: String(c.name || "").trim(),
      cardNumberMasked,
      parentCardNumberMasked: maskPan(c.parent_card_number ?? c.parentCardNumber),
    });
  }
  return out;
}

export function requestSnapshot(data: NormalizedCreateJob): JobRequestSnapshot {
  return {
    platform: (data.platform || "FLIPKART").toUpperCase(),
    paymentMode: data.paymentMode || "",
    cardType: data.cardType || "",
    sellerName: data.sellerName || "",
    listingId: data.listingId || "",
    deliverySlaDays: data.deliverySlaDays,
    finalAmountLimit: data.finalAmountLimit,
    dryRun: data.dryRun,
    cards: maskCards(data.cards),
    totalQuantity: data.totalQuantity,
    quantityPerOrder: data.quantityPerOrder,
    totalAttempts: data.totalAttempts,
  };
}
