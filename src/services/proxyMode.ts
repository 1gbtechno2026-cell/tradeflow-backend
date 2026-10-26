import { Types } from "mongoose";
import { config } from "../config.js";
import { OrderSyncSettings } from "../models/OrderSyncSettings.js";
import { workspaceUserId } from "./sessionStore.js";

/**
 * Proxy on/off at runtime, per environment.
 *
 * Every Trade Flow process runs as one environment (TRADEFLOW_ENV = local |
 * production). The dashboard's Settings → Environments card keeps one proxy
 * mode per environment on the shared settings row; each process reads the
 * value for ITS environment (cached 10 s) and falls back to its own
 * PROXY_MODE env when nothing has been saved. So "proxy off on local, required
 * on production" is one setting, and flipping it applies to the next job
 * without a restart — the same arrangement as the fetch mode and cancel speed.
 *
 *   off        never claim a proxy; every job leaves from the machine's own address
 *   preferred  claim one when the pool has one free, else go direct
 *   required   a job with no free proxy waits (re-queued), never goes direct
 */
export type ProxyMode = "off" | "preferred" | "required";
export type EnvName = "local" | "production";

export const PROXY_MODES: readonly ProxyMode[] = ["off", "preferred", "required"];

export function isProxyMode(v: unknown): v is ProxyMode {
  return typeof v === "string" && (PROXY_MODES as readonly string[]).includes(v);
}

const CACHE_MS = 10_000;
let cached: { at: number; mode: ProxyMode; source: "ui" | "env" } | null = null;

function fieldFor(env: EnvName): "proxyModeLocal" | "proxyModeProduction" {
  return env === "production" ? "proxyModeProduction" : "proxyModeLocal";
}

/** The mode this process must apply right now. */
export async function currentProxyMode(): Promise<ProxyMode> {
  if (cached && Date.now() - cached.at < CACHE_MS) return cached.mode;
  const resolved = await resolveProxyMode(config.envName).catch(() => ({ mode: config.proxyMode as ProxyMode, source: "env" as const }));
  cached = { at: Date.now(), mode: resolved.mode, source: resolved.source };
  return resolved.mode;
}

export async function resolveProxyMode(env: EnvName): Promise<{ mode: ProxyMode; source: "ui" | "env"; changedBy: string; changedAt: Date | null }> {
  const userId = workspaceUserId();
  const row = Types.ObjectId.isValid(userId) ? await OrderSyncSettings.findOne({ userId }).select("proxyModeLocal proxyModeProduction proxyChangedBy proxyChangedAt").lean() : null;
  const ui = row?.[fieldFor(env)];
  if (isProxyMode(ui)) return { mode: ui, source: "ui", changedBy: row?.proxyChangedBy || "", changedAt: row?.proxyChangedAt || null };
  return { mode: config.proxyMode as ProxyMode, source: "env", changedBy: "", changedAt: null };
}

export async function setProxyMode(userId: string, env: EnvName, mode: unknown, by: string) {
  if (!isProxyMode(mode)) throw new Error(`mode must be one of ${PROXY_MODES.join(", ")}`);
  if (env !== "local" && env !== "production") throw new Error('env must be "local" or "production"');
  await OrderSyncSettings.updateOne({ userId }, { $set: { [fieldFor(env)]: mode, proxyChangedBy: by || "", proxyChangedAt: new Date() }, $setOnInsert: { userId } }, { upsert: true });
  if (env === config.envName) cached = null;
  console.log(`[proxy] ${env} mode → ${mode}${by ? ` by ${by}` : ""}${env === config.envName ? " (applies to this process from the next job)" : ""}`);
  return proxyModeStatus(userId);
}

/** What the dashboard shows: this process's environment and the mode of both. */
export async function proxyModeStatus(userId: string) {
  const [local, production] = await Promise.all([resolveProxyMode("local"), resolveProxyMode("production")]);
  const mine = config.envName === "production" ? production : local;
  return {
    env: config.envName,
    workerId: config.workerId,
    lease: config.proxyLease,
    restMs: config.proxyRestMs,
    envDefault: config.proxyMode,
    effective: mine.mode,
    source: mine.source,
    byEnv: { local: local.mode, production: production.mode },
    sources: { local: local.source, production: production.source },
    changedBy: mine.changedBy,
    changedAt: mine.changedAt,
    userId,
  };
}
