import { Types } from "mongoose";
import type { BrowserContext } from "playwright";
import { config } from "../config.js";
import { ProxyPool, type IProxyPool } from "../models/ProxyPool.js";
import { decryptProxySecret } from "./proxySecrets.js";
import { workspaceUserId } from "./sessionStore.js";

/**
 * Egress for the checkout workers: one proxy per worker PROCESS, bound by
 * WORKER_ID and sticky across restarts (see the dashboard's ProxyPool model).
 *
 *   claimProxyForWorker  the row already bound to this worker, else a free
 *                        active row (idle longest) which it binds to itself
 *   probeExitIp          one request through the launched Chrome, so the job
 *                        log says which address Flipkart actually saw
 *   recordProxyUse       stamps lastUsed on the row for the table
 *   reportProxyFailure   bumps the fail counter; dead after PROXY_DEAD_AFTER
 *
 * Passwords are decrypted here and handed straight to Playwright's launch
 * options; they are never logged and never put on a job document.
 */

export interface ProxyEntry {
  host: string;
  port: string;
  username?: string;
  password?: string;
}

export interface PlaywrightProxy {
  server: string;
  username?: string;
  password?: string;
  bypass?: string;
}

export interface ClaimedProxy {
  id: string;
  /** "203.0.113.10:12323" — what the job result and the log carry. */
  label: string;
  proxy: PlaywrightProxy;
}

/** Same return shape as the hand-kept proxy_ip.json reader this replaces. */
export async function listProxies(): Promise<ProxyEntry[]> {
  const rows = await ProxyPool.find({ userId: new Types.ObjectId(workspaceUserId()), status: "active" })
    .select("host port username passwordEnc")
    .lean();
  return rows.map((r) => ({
    host: String(r.host),
    port: String(r.port),
    username: r.username || undefined,
    password: r.passwordEnc ? decryptProxySecret(r.passwordEnc) : undefined,
  }));
}

/** One proxy entry in Playwright's launch-option shape. */
export function toPlaywrightProxy(proxy: ProxyEntry): PlaywrightProxy {
  return {
    server: `http://${proxy.host}:${proxy.port}`,
    username: proxy.username,
    password: proxy.password,
    // Redis, Mongo and the local API are reached from Node, not Chrome — this
    // only keeps a stray localhost page load off the tunnel.
    bypass: "<-loopback>",
  };
}

function toClaimed(row: IProxyPool & { _id: Types.ObjectId }): ClaimedProxy {
  return {
    id: String(row._id),
    label: `${row.host}:${row.port}`,
    proxy: toPlaywrightProxy({
      host: row.host,
      port: String(row.port),
      username: row.username || undefined,
      password: row.passwordEnc ? decryptProxySecret(row.passwordEnc) : undefined,
    }),
  };
}

/**
 * The worker's proxy, or null when the pool has none free (or PROXY_MODE=off).
 *
 * Re-read at every job start, not only at boot: a reassignment or a disable
 * in the dashboard then applies to the next order without a restart. When
 * this worker's own row is no longer usable (disabled, dead, reassigned), it
 * lets that row go and binds a fresh one, so one worker never holds two.
 */
export async function claimProxyForWorker(workerId: string): Promise<ClaimedProxy | null> {
  if (config.proxyMode === "off") return null;
  const userId = new Types.ObjectId(workspaceUserId());

  const own = await ProxyPool.findOne({ userId, assignedTo: workerId, status: "active" }).lean();
  if (own) return toClaimed(own as IProxyPool & { _id: Types.ObjectId });

  // Let go of a row that is bound to us but no longer usable, so the table
  // does not show a dead proxy as "worker w-001's" while w-001 is on another.
  await ProxyPool.updateMany(
    { userId, assignedTo: workerId, status: { $ne: "active" } },
    { $set: { assignedTo: null, assignedAt: null } }
  );

  const fresh = await ProxyPool.findOneAndUpdate(
    { userId, status: "active", assignedTo: null },
    { $set: { assignedTo: workerId, assignedAt: new Date() } },
    { new: true, sort: { lastUsedAt: 1, createdAt: 1 } }
  ).lean();
  return fresh ? toClaimed(fresh as IProxyPool & { _id: Types.ObjectId }) : null;
}

/** Active rows nobody is bound to — how many more workers the pool can serve. */
export async function freeProxyCount(): Promise<number> {
  return ProxyPool.countDocuments({ userId: new Types.ObjectId(workspaceUserId()), status: "active", assignedTo: null });
}

const ECHO_URL = process.env.PROXY_ECHO_URL || "https://api.ipify.org?format=json";

/**
 * What address the launched Chrome leaves from. Done in the real browser, on
 * a throwaway page, so it proves the tunnel Flipkart will see — not a Node
 * request that could take a different path.
 */
export async function probeExitIp(context: BrowserContext, timeoutMs = 20_000): Promise<string> {
  const page = await context.newPage();
  try {
    const res = await page.goto(ECHO_URL, { waitUntil: "domcontentloaded", timeout: timeoutMs });
    if (!res || !res.ok()) throw new Error(`echo endpoint answered ${res ? res.status() : "nothing"}`);
    const text = await page.evaluate(() => document.body?.innerText || "");
    const ip = (JSON.parse(text) as { ip?: string }).ip || "";
    if (!ip) throw new Error("echo endpoint returned no ip");
    return ip;
  } finally {
    await page.close().catch(() => undefined);
  }
}

export async function recordProxyUse(id: string, jobId: string, exitIp: string): Promise<void> {
  const now = new Date();
  await ProxyPool.updateOne(
    { _id: id },
    { $set: { lastUsedAt: now, lastUsedJobId: jobId, lastCheckedAt: now, lastOkAt: now, exitIp, lastError: "", consecutiveFails: 0 } }
  ).catch(() => undefined);
}

/** Bumps the failure counter; marks the row dead once it reaches the cap.
 *  Returns true when the row was just retired. */
export async function reportProxyFailure(id: string, error: string): Promise<boolean> {
  const row = await ProxyPool.findOneAndUpdate(
    { _id: id },
    { $set: { lastCheckedAt: new Date(), lastError: error.slice(0, 300) }, $inc: { consecutiveFails: 1 } },
    { new: true }
  )
    .select("consecutiveFails status")
    .lean();
  if (!row) return false;
  if ((row.consecutiveFails || 0) >= config.proxyDeadAfter && row.status === "active") {
    await ProxyPool.updateOne({ _id: id }, { $set: { status: "dead" } });
    return true;
  }
  return false;
}

/** Chrome's own words for "the tunnel, not the site, failed". */
const PROXY_ERROR_RE =
  /ERR_PROXY_CONNECTION_FAILED|ERR_TUNNEL_CONNECTION_FAILED|ERR_PROXY_AUTH_(?:UNSUPPORTED|REQUESTED)|ERR_PROXY_CERTIFICATE_INVALID|ERR_NO_SUPPORTED_PROXIES|ERR_SOCKS_CONNECTION_FAILED|ERR_MANDATORY_PROXY_CONFIGURATION_FAILED|407 Proxy Authentication|echo endpoint (?:answered|returned)/i;

export function isProxyError(message: string): boolean {
  return PROXY_ERROR_RE.test(message);
}
