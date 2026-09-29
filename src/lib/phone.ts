/** Last 10 digits, so "+919821589547", "09821589547" and "9821589547" compare equal. */
export function normalizePhone(raw: unknown): string {
  return String(raw ?? "").replace(/\D/g, "").slice(-10);
}
