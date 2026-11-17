/** Last 10 digits, so "+919821589547", "09821589547" and "9821589547" compare equal. */
export function normalizePhone(raw: unknown): string {
  return String(raw ?? "").replace(/\D/g, "").slice(-10);
}

/**
 * The dashboard's Ordering Phone pool, cleaned for rotation: each raw entry
 * reduced to its 10-digit form, anything shorter dropped, duplicates removed
 * while preserving the pool's order (the order rotation walks through).
 *
 * Pure and order-stable so every worker derives the same sequence from the same
 * stored pool; services/orderingPhonePool.ts turns that into a per-order pick.
 */
export function usableOrderingPhones(pool: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const raw of pool || []) {
    const p = normalizePhone(raw);
    if (p.length === 10 && !seen.has(p)) {
      seen.add(p);
      out.push(p);
    }
  }
  return out;
}
