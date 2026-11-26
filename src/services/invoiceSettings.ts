/**
 * Invoice settings — the invoice rules an operator sets in the dashboard (Settings → Invoice settings) instead of in
 * Trade Flow's `.env`. One document per workspace in `invoice_settings` (models/InvoiceSettings.ts).
 *
 *   field                          .env it replaces                  default  range
 *   captureCancelledAfterShipping  INVOICE_CAPTURE_CANCELLED (=1)    false*   bool
 *   refetchCancelDelayH            INVOICE_REFETCH_CANCEL_DELAY_H    24       0–720
 *   refetchRefundDelayH            INVOICE_REFETCH_REFUND_DELAY_H    192      0–720
 *   refetchCodDelayH               INVOICE_REFETCH_COD_DELAY_H       192      0–720
 *   settleHours                    INVOICE_SETTLE_HOURS              48       1–720
 *   recaptureGiveUpDays            INVOICE_RECAPTURE_GIVE_UP_DAYS    30       1–365
 *
 *   * the code's default stays off (as before); the SEED writes the workspace's document once, with
 *     captureCancelledAfterShipping: true (owner decision 2026-11-26) and nothing else.
 *
 * PRECEDENCE, per field: the document's value wins; a field missing from the document follows the `.env` (read at
 * call time, parsed exactly as the code did before), then the default.
 *
 * READS: getInvoiceSettings(userId) — the document is cached 30 s per workspace (the `.env` is read on every call),
 * and the cache is cleared by a save in this process. The worker reads through the same cache, so a save in the
 * dashboard reaches the worker's next sweep / next unit read within 30 s, with no restart. A database error on a
 * read falls back to the last document seen, else to `.env` / defaults (a warning, once a minute) — a sweep never
 * fails on its settings. invoiceSettingsFromEnv() is the same resolution without a document, for the sync helpers'
 * default arguments (what the checks call).
 */
import { Types } from "mongoose";
import { config } from "../config.js";
import { throttledWarn } from "../lib/resilience.js";
import { InvoiceSettings, type IInvoiceSettings, type IInvoiceSettingsChange } from "../models/InvoiceSettings.js";
import { RequestInputError } from "./requestCommon.js";

export const INVOICE_SETTING_FIELDS = ["captureCancelledAfterShipping", "refetchCancelDelayH", "refetchRefundDelayH", "refetchCodDelayH", "settleHours", "recaptureGiveUpDays"] as const;
export type InvoiceSettingField = (typeof INVOICE_SETTING_FIELDS)[number];

export interface EffectiveInvoiceSettings {
  captureCancelledAfterShipping: boolean;
  refetchCancelDelayH: number;
  refetchRefundDelayH: number;
  refetchCodDelayH: number;
  settleHours: number;
  recaptureGiveUpDays: number;
}

export type InvoiceSettingSource = "db" | "env" | "default";

interface FieldSpec {
  env: string;
  default: boolean | number;
  /** The `.env` value as the code always parsed it; undefined = unset or unusable (the default applies). */
  parseEnv: (raw: string) => boolean | number | undefined;
  min?: number;
  max?: number;
  unit?: "hours" | "days";
}

const num = (raw: string) => (raw === "" ? NaN : Number(raw));

export const INVOICE_SETTING_SPECS: Record<InvoiceSettingField, FieldSpec> = {
  // Before: String(env ?? "0").trim() === "1" — any other value set is off.
  captureCancelledAfterShipping: { env: "INVOICE_CAPTURE_CANCELLED", default: false, parseEnv: (raw) => raw === "1" },
  // Before: hours(name, fallback) — a finite number ≥ 0, else the fallback.
  refetchCancelDelayH: { env: "INVOICE_REFETCH_CANCEL_DELAY_H", default: 24, min: 0, max: 720, unit: "hours", parseEnv: (raw) => (Number.isFinite(num(raw)) && num(raw) >= 0 ? num(raw) : undefined) },
  refetchRefundDelayH: { env: "INVOICE_REFETCH_REFUND_DELAY_H", default: 192, min: 0, max: 720, unit: "hours", parseEnv: (raw) => (Number.isFinite(num(raw)) && num(raw) >= 0 ? num(raw) : undefined) },
  refetchCodDelayH: { env: "INVOICE_REFETCH_COD_DELAY_H", default: 192, min: 0, max: 720, unit: "hours", parseEnv: (raw) => (Number.isFinite(num(raw)) && num(raw) >= 0 ? num(raw) : undefined) },
  // Before: Math.max(1, Number(env || 48)) — at least 1 h.
  settleHours: { env: "INVOICE_SETTLE_HOURS", default: 48, min: 1, max: 720, unit: "hours", parseEnv: (raw) => (Number.isFinite(num(raw)) ? Math.max(1, num(raw)) : undefined) },
  // Before: a finite number > 0, else 30.
  recaptureGiveUpDays: { env: "INVOICE_RECAPTURE_GIVE_UP_DAYS", default: 30, min: 1, max: 365, unit: "days", parseEnv: (raw) => (Number.isFinite(num(raw)) && num(raw) > 0 ? num(raw) : undefined) },
};

/** What the seed writes into a workspace that has no document yet. */
export const INVOICE_SETTINGS_SEED = { captureCancelledAfterShipping: true } as const;

export const CACHE_MS = 30_000;
const HISTORY_KEEP = 50;

/** The document as read (only the fields the service uses). */
type SettingsDoc = Partial<Pick<IInvoiceSettings, InvoiceSettingField>> & { changedBy?: string; changedAt?: Date | null; history?: IInvoiceSettingsChange[] };

export interface InvoiceSettingsView {
  userId: string;
  /** The rule fields the document holds — only those (a field not here follows .env / the default). */
  settings: Partial<EffectiveInvoiceSettings>;
  /** What applies now, field by field. */
  effective: EffectiveInvoiceSettings;
  sources: Record<InvoiceSettingField, InvoiceSettingSource>;
  /** Newest first, at most 50. */
  history: IInvoiceSettingsChange[];
  changedBy: string;
  changedAt: Date | null;
  /** For the form: each field's default, range and unit. */
  limits: Record<InvoiceSettingField, { type: "boolean" | "number"; default: boolean | number; min?: number; max?: number; unit?: "hours" | "days" }>;
}

// ── resolution ──────────────────────────────────────────────────────────────

function dbValue(field: InvoiceSettingField, value: unknown): boolean | number | undefined {
  const spec = INVOICE_SETTING_SPECS[field];
  if (typeof spec.default === "boolean") return typeof value === "boolean" ? value : undefined;
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  return value >= (spec.min ?? -Infinity) && value <= (spec.max ?? Infinity) ? value : undefined;
}

/** The precedence: the document's value, else the .env's, else the default — and which one it was. */
export function resolveInvoiceSettings(doc: SettingsDoc | null, env: NodeJS.ProcessEnv = process.env): { effective: EffectiveInvoiceSettings; sources: Record<InvoiceSettingField, InvoiceSettingSource>; settings: Partial<EffectiveInvoiceSettings> } {
  const effective = {} as Record<InvoiceSettingField, boolean | number>;
  const sources = {} as Record<InvoiceSettingField, InvoiceSettingSource>;
  const settings: Record<string, boolean | number> = {};
  for (const field of INVOICE_SETTING_FIELDS) {
    const spec = INVOICE_SETTING_SPECS[field];
    const fromDb = doc ? dbValue(field, doc[field]) : undefined;
    if (fromDb !== undefined) {
      effective[field] = fromDb;
      sources[field] = "db";
      settings[field] = fromDb;
      continue;
    }
    const fromEnv = spec.parseEnv(String(env[spec.env] ?? "").trim());
    if (String(env[spec.env] ?? "").trim() !== "" && fromEnv !== undefined) {
      effective[field] = fromEnv;
      sources[field] = "env";
    } else {
      effective[field] = spec.default;
      sources[field] = "default";
    }
  }
  return { effective: effective as unknown as EffectiveInvoiceSettings, sources, settings: settings as Partial<EffectiveInvoiceSettings> };
}

/** `.env`, then the defaults — no document. What the sync helpers use when they are given no settings. */
export function invoiceSettingsFromEnv(env: NodeJS.ProcessEnv = process.env): EffectiveInvoiceSettings {
  return resolveInvoiceSettings(null, env).effective;
}

// ── the document, cached ────────────────────────────────────────────────────

const cache = new Map<string, { at: number; doc: SettingsDoc | null }>();
/** The last document read per workspace, whatever its age: what a read falls back to when the database is down. */
const lastSeen = new Map<string, SettingsDoc | null>();

/** Forget the cached document of one workspace (every workspace with no argument). */
export function clearInvoiceSettingsCache(userId?: string): void {
  if (userId === undefined) cache.clear();
  else cache.delete(String(userId));
}

const validId = (userId: unknown) => Types.ObjectId.isValid(String(userId ?? "")) && String(userId).length === 24;

async function loadDoc(userId: string): Promise<SettingsDoc | null> {
  const row = await InvoiceSettings.collection.findOne({ userId: new Types.ObjectId(userId) }, { projection: { _id: 0, userId: 0 } });
  return (row as unknown as SettingsDoc) || null;
}

async function cachedDoc(userId: string): Promise<SettingsDoc | null> {
  const hit = cache.get(userId);
  if (hit && Date.now() - hit.at < CACHE_MS) return hit.doc;
  try {
    const doc = await loadDoc(userId);
    cache.set(userId, { at: Date.now(), doc });
    lastSeen.set(userId, doc);
    return doc;
  } catch (err) {
    const fallback = lastSeen.get(userId) ?? null;
    throttledWarn(`invoice-settings:${userId}`, `[invoice-settings] could not read the settings (${err instanceof Error ? err.message.split("\n")[0] : err}) — using ${fallback ? "the last ones read" : ".env / defaults"} for now`);
    return fallback;
  }
}

/**
 * The invoice settings that apply to a workspace now: `effective` (what the sweep, the refetch noting and the settle
 * rule use), `sources` and the document's own fields. Never throws.
 */
export async function getInvoiceSettings(userId: unknown): Promise<{ effective: EffectiveInvoiceSettings; sources: Record<InvoiceSettingField, InvoiceSettingSource>; settings: Partial<EffectiveInvoiceSettings> }> {
  const doc = validId(userId) ? await cachedDoc(String(userId)) : null;
  return resolveInvoiceSettings(doc);
}

const limits = Object.fromEntries(
  INVOICE_SETTING_FIELDS.map((f) => {
    const s = INVOICE_SETTING_SPECS[f];
    return [f, typeof s.default === "boolean" ? { type: "boolean", default: s.default } : { type: "number", default: s.default, min: s.min, max: s.max, unit: s.unit }];
  })
) as InvoiceSettingsView["limits"];

function viewOf(userId: string, doc: SettingsDoc | null): InvoiceSettingsView {
  const r = resolveInvoiceSettings(doc);
  return {
    userId,
    settings: r.settings,
    effective: r.effective,
    sources: r.sources,
    history: [...(doc?.history || [])].reverse(),
    changedBy: doc?.changedBy || "",
    changedAt: doc?.changedAt || null,
    limits,
  };
}

function requireWorkspace(userId: unknown): string {
  const id = String(userId ?? "").trim();
  if (!validId(id)) throw new RequestInputError("userId must be a workspace id", 400, "invalid_user");
  return id;
}

/** GET /invoice-settings: read fresh from the database (and refresh this process's cache). Throws when it cannot. */
export async function invoiceSettingsView(userId: unknown): Promise<InvoiceSettingsView> {
  const id = requireWorkspace(userId);
  const doc = await loadDoc(id);
  cache.set(id, { at: Date.now(), doc });
  lastSeen.set(id, doc);
  return viewOf(id, doc);
}

// ── writes ──────────────────────────────────────────────────────────────────

const indexed = { done: false };
const isDuplicateKey = (err: unknown) => (err as { code?: number })?.code === 11000;

/** The collection and its unique index on userId (one document per workspace): made before the first write. */
export async function ensureInvoiceSettingsIndexes(): Promise<void> {
  if (indexed.done) return;
  await InvoiceSettings.createIndexes();
  indexed.done = true;
}

/** Validate a PUT body: only the six fields (plus userId and by), each in its range. Throws RequestInputError (400). */
export function parseInvoiceSettingsBody(body: unknown): { changes: Partial<EffectiveInvoiceSettings>; by: string } {
  if (!body || typeof body !== "object" || Array.isArray(body)) throw new RequestInputError("The body must be a JSON object", 400, "invalid_body");
  const input = body as Record<string, unknown>;
  const allowed = new Set<string>(["userId", "by", ...INVOICE_SETTING_FIELDS]);
  const unknown = Object.keys(input).filter((k) => !allowed.has(k));
  if (unknown.length) throw new RequestInputError(`Unknown field${unknown.length > 1 ? "s" : ""}: ${unknown.join(", ")}`, 400, "unknown_field");
  const changes: Record<string, boolean | number> = {};
  const errors: string[] = [];
  for (const field of INVOICE_SETTING_FIELDS) {
    if (!(field in input)) continue;
    const value = input[field];
    const spec = INVOICE_SETTING_SPECS[field];
    if (typeof spec.default === "boolean") {
      if (typeof value !== "boolean") errors.push(`${field} must be true or false`);
      else changes[field] = value;
      continue;
    }
    if (typeof value !== "number" || !Number.isInteger(value) || value < spec.min! || value > spec.max!) errors.push(`${field} must be a whole number of ${spec.unit} between ${spec.min} and ${spec.max}`);
    else changes[field] = value;
  }
  if (errors.length) throw new RequestInputError(errors.join("; "), 400, "invalid_value");
  if (!Object.keys(changes).length) throw new RequestInputError(`Nothing to save — send one or more of: ${INVOICE_SETTING_FIELDS.join(", ")}`, 400, "nothing_to_save");
  const by = String(input.by ?? "").trim().slice(0, 100);
  return { changes: changes as Partial<EffectiveInvoiceSettings>, by };
}

/**
 * PUT /invoice-settings: save the fields given (validated first — nothing is written on a 400), note each real change
 * in `history` (from = what was in effect, to = the new value; last 50 kept), clear this process's cache and answer
 * what GET answers. A field saved with the value it already had in the document is not a change.
 */
export async function setInvoiceSettings(body: unknown, now = new Date()): Promise<InvoiceSettingsView> {
  const { changes, by } = parseInvoiceSettingsBody(body);
  const id = requireWorkspace((body as Record<string, unknown>).userId);
  const before = await loadDoc(id);
  const was = resolveInvoiceSettings(before);
  const entries: IInvoiceSettingsChange[] = [];
  for (const [field, to] of Object.entries(changes) as Array<[InvoiceSettingField, boolean | number]>) {
    if (was.sources[field] === "db" && was.effective[field] === to) continue;
    entries.push({ field, from: was.effective[field], to, by, at: now });
  }
  await ensureInvoiceSettingsIndexes();
  const oid = new Types.ObjectId(id);
  const update: Record<string, unknown> = { $set: { ...changes, updatedAt: now, ...(entries.length ? { changedBy: by, changedAt: now } : {}) }, $setOnInsert: { userId: oid, createdAt: now, ...(entries.length ? {} : { changedBy: by, changedAt: now }) } };
  if (entries.length) update.$push = { history: { $each: entries, $slice: -HISTORY_KEEP } };
  const write = () => InvoiceSettings.collection.updateOne({ userId: oid }, update, { upsert: true });
  // Two first saves at the same moment: the unique index refuses one insert; it then updates the row the other made.
  await write().catch((err) => (isDuplicateKey(err) ? write() : Promise.reject(err)));
  clearInvoiceSettingsCache(id);
  if (entries.length) console.log(`[invoice-settings] ${entries.map((e) => `${e.field} ${e.from} → ${e.to}`).join(", ")} by ${by || "?"} (the worker picks it up within ${CACHE_MS / 1000} s)`);
  return invoiceSettingsView(id);
}

/**
 * The seed: the workspace's document, written ONCE if it does not exist — with captureCancelledAfterShipping: true
 * and no other rule field (they keep following .env / defaults until somebody saves them). Idempotent: an existing
 * document is never touched, and two seeds at the same moment (the API and a worker starting together, or two Macs on
 * one Atlas) are safe — the unique index makes one insert fail, and that failure means "it exists". Returns whether
 * this call wrote it.
 */
export async function seedInvoiceSettings(userId: string, opts: { by?: string; now?: Date } = {}): Promise<boolean> {
  const id = requireWorkspace(userId);
  const now = opts.now || new Date();
  const by = opts.by || `seed:${config.hostId}`;
  await ensureInvoiceSettingsIndexes();
  const from = invoiceSettingsFromEnv().captureCancelledAfterShipping;
  try {
    const res = await InvoiceSettings.collection.updateOne(
      { userId: new Types.ObjectId(id) },
      {
        $setOnInsert: {
          userId: new Types.ObjectId(id),
          ...INVOICE_SETTINGS_SEED,
          changedBy: by,
          changedAt: now,
          history: [{ field: "captureCancelledAfterShipping", from, to: INVOICE_SETTINGS_SEED.captureCancelledAfterShipping, by, at: now }],
          createdAt: now,
          updatedAt: now,
        },
      },
      { upsert: true }
    );
    if (res.upsertedCount) clearInvoiceSettingsCache(id);
    return res.upsertedCount === 1;
  } catch (err) {
    if (isDuplicateKey(err)) return false;
    throw err;
  }
}

/** At a process's start (API and worker): seed this workspace's document; never throws. */
export async function seedInvoiceSettingsAtStart(userId: string): Promise<void> {
  if (!userId) return;
  try {
    const wrote = await seedInvoiceSettings(userId);
    console.log(wrote ? `[invoice-settings] first start on this workspace — written: captureCancelledAfterShipping ON; the other invoice rules follow .env / defaults until saved in Settings → Invoice settings` : "[invoice-settings] in place — nothing written");
  } catch (err) {
    console.warn(`[invoice-settings] could not write the invoice settings: ${err instanceof Error ? err.message.split("\n")[0] : err} — the invoice rules follow .env / defaults`);
  }
}
