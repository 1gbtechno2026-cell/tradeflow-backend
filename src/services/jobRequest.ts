import { z } from "zod";

function asNumber(value: unknown): number | undefined {
  if (value == null || value === "") return undefined;
  const n = typeof value === "number" ? value : Number(value);
  return Number.isFinite(n) ? n : undefined;
}

function asString(value: unknown): string | undefined {
  if (value == null) return undefined;
  const s = String(value).trim();
  return s || undefined;
}

function asBool(value: unknown): boolean | undefined {
  if (value == null) return undefined;
  if (typeof value === "boolean") return value;
  if (value === "true" || value === 1 || value === "1") return true;
  if (value === "false" || value === 0 || value === "0") return false;
  return undefined;
}

const addressSchema = z.object({
  name: z.string().min(1),
  mobile: z.string().optional().default(""),
  pincode: z.string().min(6),
  locality: z.string().optional().default(""),
  addressLine1: z.string().min(1),
  city: z.string().min(1),
  state: z.string().min(1),
  addressType: z.enum(["Home", "Work"]).optional().default("Home"),
  gstNumber: z.string().optional().default(""),
  companyName: z.string().optional().default(""),
  checkoutPincode: z.string().optional().default(""),
});

const normalizedSchema = z.object({
  productUrl: z.string().url(),
  quantityPerOrder: z.number().int().positive(),
  totalQuantity: z.number().int().positive({
    message: "quantity (totalQuantity) must be a positive integer",
  }),
  totalAttempts: z.number().int().positive().optional(),
  cartAmountLimit: z.number().positive().optional(),
  finalAmountLimit: z.number().positive().optional(),
  emails: z.array(z.string().min(1)).min(1),
  address: addressSchema.optional(),
  addressId: z.number().int().positive().optional(),
  gstId: z.number().int().positive().optional(),
  gstMandatory: z.boolean().default(true),
  platform: z.string().optional(),
  sellerName: z.string().optional(),
  listingId: z.string().optional(),
  paymentMode: z.string().optional(),
  cardType: z.string().optional(),
  /** Which credential arm the operator picked. Load-bearing, not cosmetic: it
   *  decides whether this order needs a handset from the shared phone pool
   *  (otp) or carries its own credential in the CSV (password/pin), which is
   *  what determines the order's concurrency class. */
  authType: z.enum(["password", "otp", "pin"]).optional(),
  /** Required by corporate card types; names the onboarded handset group. */
  corporateId: z.string().optional(),
  deliverySlaDays: z.number().int().positive(),
  cards: z.array(z.record(z.unknown())).optional(),
  /** Orders one card may place in this batch; absent = no cap. */
  cardMaxUsage: z.number().int().positive().optional(),
  dryRun: z.boolean().default(false),
});

export type NormalizedCreateJob = z.infer<typeof normalizedSchema>;

/** Accepts both the Trade Flow camelCase body and the Smart Bulk Order snake_case payload. */
export function parseCreateJobBody(raw: unknown): NormalizedCreateJob {
  const b = (raw ?? {}) as Record<string, unknown>;
  const quantityPerOrder =
    asNumber(b.quantity_per_order) ??
    asNumber(b.quantityPerOrder) ??
    asNumber(b.quantity) ??
    1;

  const parsed = normalizedSchema.parse({
    productUrl: asString(b.productUrl) ?? asString(b.product_url),
    quantityPerOrder,
    totalQuantity: asNumber(b.quantity) ?? asNumber(b.totalQuantity),
    totalAttempts: asNumber(b.total_attempts) ?? asNumber(b.totalAttempts),
    cartAmountLimit: asNumber(b.cartAmountLimit) ?? asNumber(b.cart_amount_limit),
    finalAmountLimit: asNumber(b.finalAmountLimit) ?? asNumber(b.final_amount_limit),
    emails: b.emails,
    address: b.address,
    addressId: asNumber(b.addressId) ?? asNumber(b.address_id),
    gstId: asNumber(b.gstId) ?? asNumber(b.gst_id),
    gstMandatory: asBool(b.gstMandatory) ?? asBool(b.gst_mandatory) ?? true,
    platform: asString(b.platform),
    sellerName: asString(b.sellerName) ?? asString(b.seller_name),
    listingId: asString(b.listingId) ?? asString(b.listing_id),
    paymentMode: asString(b.paymentMode) ?? asString(b.payment_mode),
    cardType: asString(b.cardType) ?? asString(b.card_type),
    // Both of these were being dropped on the floor: the order form has sent
    // auth_type and corporate_id all along, and nothing mapped them, so a
    // corporate card arrived with no Corporate ID and the lease could not scope
    // to the onboarded group.
    authType: (asString(b.authType) ?? asString(b.auth_type))?.toLowerCase(),
    corporateId: (asString(b.corporateId) ?? asString(b.corporate_id))?.toUpperCase(),
    deliverySlaDays: asNumber(b.deliverySlaDays) ?? asNumber(b.delivery_sla_days),
    cards: Array.isArray(b.cards) ? b.cards : undefined,
    cardMaxUsage:
      asNumber(b.cardMaxUsage) ??
      asNumber(b.card_max_usage) ??
      (asBool(b.smart_cards_max_usage_enabled) ? asNumber(b.smart_cards_max_usage) : undefined),
    dryRun:
      asBool(b.dry_run) ??
      asBool(b.dryRun) ??
      (asString(b.mode) === "mock" || asString(b.mode) === "dry_run" ? true : undefined) ??
      false,
  });

  const requiredOrders = Math.ceil(parsed.totalQuantity / parsed.quantityPerOrder);
  if (parsed.totalAttempts != null && parsed.totalAttempts < requiredOrders) {
    throw new Error(
      `total_attempts must be >= ceil(quantity / quantity_per_order) (need >= ${requiredOrders}, got ${parsed.totalAttempts})`
    );
  }

  return parsed;
}

export function requiredOrdersFor(totalQuantity: number, quantityPerOrder: number): number {
  return Math.ceil(totalQuantity / quantityPerOrder);
}

export function effectiveAttemptsFor(totalAttempts: number | undefined, requiredOrders: number): number {
  return Math.max(totalAttempts ?? requiredOrders, requiredOrders);
}
