/**
 * What the three schedule checks (scheduleLeaseCheck, scheduleSweepCheck, fetchScheduleCheck) share so that a run
 * leaves nothing behind — also a run that is stopped, or killed outright.
 *
 * Every run stamps what it makes with its run id (its start time, base 36) and a tag per check:
 *   verify_<tag>_<run>_<name>      throwaway collections (Mongo)
 *   verify-<tag>-<name>-<run>      throwaway queues (Redis db 15)
 *   verify_<tag>_<run>_ids         the invented workspace ids whose rows the run writes to REAL collections
 *
 *   registerRunIds   write those ids down before the first row goes to a real collection
 *   onInterrupt      Ctrl-C, kill, a closed terminal: the check still removes what it made, then leaves
 *   interruptedCleanup  how: the run's own connection is closed first — a check that is still in the middle of a step
 *                    can then write nothing more — and the cleanup goes over a connection of its own
 *   cleanRun         everything of one run: the rows of its registered workspaces, its queues' keys, its collections
 *   removeStaleRuns  the same for every EARLIER run of this check that never cleaned up (it was killed outright) —
 *                    called at the start of a run. Only runs that started 30 minutes ago or more: a run takes two
 *                    minutes, so one that another terminal is still in the middle of is never touched.
 *
 * Rows are only ever deleted for a workspace id that (a) a run of these checks wrote down, (b) is not the workspace
 * of this machine's .env and (c) owns no Platform ID or order unit whose e-mail is not an @verify.invalid one. An id
 * that fails (b) or (c) is refused, said, and left exactly as it is.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { Redis } from "ioredis";
import mongoose, { Types } from "mongoose";

type Db = NonNullable<typeof mongoose.connection.db>;

/** scheduleLeaseCheck, scheduleSweepCheck, fetchScheduleCheck. */
export type CheckTag = "sl" | "ss" | "fs";

export const STALE_RUN_MS = 30 * 60_000;
const INVENTED_MAIL = /@verify\.invalid$/i;

export interface RunCleanup {
  db: Db;
  tag: CheckTag;
  /** The real collections this check writes rows of its invented workspaces to (each row carries `userId`). */
  rowCollections?: string[];
  /** Redis as the checks reach it; the queues' keys are looked for in db 15 only. Left out = this check uses no queue. */
  redis?: { host: string; port: number; password?: string };
}

export interface RunCleanupResult {
  run: string;
  rows: Record<string, number>;
  redisKeys: number;
  collections: number;
  /** Workspace ids whose rows were NOT deleted, each with why. */
  refused: string[];
}

/** How long ago a run started; NaN when the id is not one of ours. */
export function runAgeMs(run: string, now: number = Date.now()): number {
  if (!/^[0-9a-z]{6,12}$/.test(run)) return NaN;
  const startedAt = parseInt(run, 36);
  return Number.isSafeInteger(startedAt) ? now - startedAt : NaN;
}

const idsCollection = (tag: CheckTag, run: string) => `verify_${tag}_${run}_ids`;

/** The workspace of this machine's .env — read from the file: a check has replaced it in its own environment. */
export function envWorkspaceId(): string {
  try {
    const here = path.dirname(fileURLToPath(import.meta.url));
    return String(dotenv.parse(fs.readFileSync(path.resolve(here, "../../.env"))).TRADE_FLOW_USER_ID || "").trim();
  } catch {
    return "";
  }
}

/** Write down the invented workspace ids of a run — before any of their rows is written to a real collection. */
export async function registerRunIds(db: Db, tag: CheckTag, run: string, ids: Array<Types.ObjectId | string>): Promise<void> {
  if (!ids.length) return;
  await db.collection<{ _id: Types.ObjectId }>(idsCollection(tag, run)).bulkWrite(ids.map((id) => ({ updateOne: { filter: { _id: new Types.ObjectId(String(id)) }, update: { $setOnInsert: { at: new Date() } }, upsert: true } })));
}

/** May the rows of this workspace id be deleted? Null = yes; else why not. */
export async function refusalFor(db: Db, id: Types.ObjectId, realWorkspace: string = envWorkspaceId()): Promise<string | null> {
  if (realWorkspace && String(id) === realWorkspace) return "it is the workspace of this machine's .env";
  const accounts = await db.collection("platformids").countDocuments({ userId: id, email: { $not: INVENTED_MAIL } }, { limit: 1 });
  if (accounts) return "it owns a Platform ID that is not an @verify.invalid one";
  const units = await db.collection("order_details").countDocuments({ userId: id, platform_email: { $not: INVENTED_MAIL } }, { limit: 1 });
  if (units) return "it owns an order unit of an account that is not an @verify.invalid one";
  return null;
}

/** The invented workspace ids a run wrote down, and the ones its throwaway clocks and switches name. */
async function idsOfRun(db: Db, tag: CheckTag, run: string, have: Set<string>): Promise<Types.ObjectId[]> {
  const out = new Map<string, Types.ObjectId>();
  const add = (value: unknown) => {
    if (value && Types.ObjectId.isValid(String(value))) out.set(String(value), new Types.ObjectId(String(value)));
  };
  if (have.has(idsCollection(tag, run))) for (const doc of await db.collection(idsCollection(tag, run)).find({}).project({ _id: 1 }).toArray()) add(doc._id);
  for (const name of ["leases", "settings"]) {
    const coll = `verify_${tag}_${run}_${name}`;
    if (have.has(coll)) for (const id of await db.collection(coll).distinct("userId")) add(id);
  }
  return [...out.values()];
}

/**
 * Remove everything one run made. The rows first, the collections last — the collection of ids last of all, and only
 * when no row is left: a cleanup that is itself interrupted can be taken up again by the next run.
 */
export async function cleanRun(o: RunCleanup, run: string): Promise<RunCleanupResult> {
  const out: RunCleanupResult = { run, rows: {}, redisKeys: 0, collections: 0, refused: [] };
  const prefix = `verify_${o.tag}_${run}_`;
  const names = (await o.db.listCollections({ name: { $regex: `^${prefix}` } }, { nameOnly: true }).toArray()).map((c) => c.name);
  const have = new Set(names);

  let rowsLeft = 0;
  if (o.rowCollections?.length) {
    const real = envWorkspaceId();
    const ids: Types.ObjectId[] = [];
    for (const id of await idsOfRun(o.db, o.tag, run, have)) {
      const why = await refusalFor(o.db, id, real);
      if (why) out.refused.push(`${String(id)}: ${why}`);
      else ids.push(id);
    }
    if (ids.length) {
      const byUser = { userId: { $in: ids } };
      // A task row is found by its request as well: the requests are read before they are deleted.
      const requestIds: string[] = [];
      for (const coll of ["order_update_requests", "order_invoice_requests", "order_job_runs"]) {
        if (o.rowCollections.includes(coll)) for (const row of await o.db.collection(coll).find(byUser).project({ _id: 1 }).toArray()) requestIds.push(String(row._id));
      }
      for (const coll of o.rowCollections) {
        const filter = coll === "order_sync_tasks" && requestIds.length ? { $or: [byUser, { requestId: { $in: requestIds } }] } : byUser;
        const n = (await o.db.collection(coll).deleteMany(filter)).deletedCount;
        if (n) out.rows[coll] = n;
      }
      for (const coll of o.rowCollections) rowsLeft += await o.db.collection(coll).countDocuments(byUser);
    }
  }

  if (o.redis) {
    const db15 = new Redis({ host: o.redis.host, port: o.redis.port, password: o.redis.password, db: 15, maxRetriesPerRequest: 2, lazyConnect: true });
    try {
      await db15.connect();
      const keys = await db15.keys(`bull:verify-${o.tag}-*-${run}:*`);
      if (keys.length) out.redisKeys = await db15.del(...keys);
    } catch {
      // Redis away: its keys are looked for again by the next run.
    } finally {
      db15.disconnect();
    }
  }

  for (const name of names.filter((n) => n !== idsCollection(o.tag, run))) out.collections += await o.db.dropCollection(name).then(() => 1, () => 0);
  if (have.has(idsCollection(o.tag, run)) && !rowsLeft && !out.refused.length) out.collections += await o.db.dropCollection(idsCollection(o.tag, run)).then(() => 1, () => 0);
  return out;
}

export function describeCleanup(r: RunCleanupResult): string {
  const rows = Object.entries(r.rows).map(([coll, n]) => `${coll} ${n}`);
  const startedAt = new Date(parseInt(r.run, 36) + 330 * 60_000).toISOString().replace("T", " ").slice(0, 16);
  return `run ${r.run} (started ${startedAt} IST): ${rows.length ? `rows deleted — ${rows.join(" · ")}` : "no rows"} · ${r.collections} throwaway collection(s) dropped · ${r.redisKeys} Redis key(s) deleted in db 15${r.refused.length ? ` · REFUSED, left as they are — ${r.refused.join("; ")}` : ""}`;
}

/** The runs of this check that left something: their ids, from the throwaway collections and the queues' keys. */
async function runsFound(o: RunCleanup): Promise<string[]> {
  const runs = new Set<string>();
  for (const c of await o.db.listCollections({ name: { $regex: `^verify_${o.tag}_` } }, { nameOnly: true }).toArray()) {
    const m = new RegExp(`^verify_${o.tag}_([0-9a-z]+)_`).exec(c.name);
    if (m) runs.add(m[1]);
  }
  if (o.redis) {
    const db15 = new Redis({ host: o.redis.host, port: o.redis.port, password: o.redis.password, db: 15, maxRetriesPerRequest: 2, lazyConnect: true });
    try {
      await db15.connect();
      for (const key of await db15.keys(`bull:verify-${o.tag}-*`)) {
        const m = /^bull:verify-[a-z]+-[a-z]+-([0-9a-z]+):/.exec(key);
        if (m) runs.add(m[1]);
      }
    } catch {
      // Redis away: nothing is known about its keys, and nothing is claimed.
    } finally {
      db15.disconnect();
    }
  }
  return [...runs];
}

/**
 * What earlier runs of this check that never cleaned up left behind — removed, and said. Never this run, and never
 * a run that started less than 30 minutes ago.
 */
export async function removeStaleRuns(o: RunCleanup & { thisRun: string; now?: number }): Promise<RunCleanupResult[]> {
  const out: RunCleanupResult[] = [];
  for (const run of await runsFound(o)) {
    const age = runAgeMs(run, o.now);
    if (run === o.thisRun || !(age >= STALE_RUN_MS)) continue;
    out.push(await cleanRun(o, run));
  }
  return out;
}

/**
 * The cleanup of a run that is being stopped while it is still in the middle of a step. The run's own connection goes
 * first: whatever the interrupted step would still have written (a re-seeded clock, another row) can no longer be
 * written, so it cannot reappear behind the cleanup. Then everything of the run is removed over a second connection.
 * `before`: what else must stop first (a listener, the run's queues).
 */
export async function interruptedCleanup(mongoUri: string, o: Omit<RunCleanup, "db">, run: string, before?: () => Promise<unknown> | unknown): Promise<RunCleanupResult> {
  await Promise.resolve(before?.()).catch(() => undefined);
  await mongoose.disconnect().catch(() => undefined);
  const conn = await mongoose.createConnection(mongoUri, { autoIndex: false, autoCreate: false, serverSelectionTimeoutMS: 15_000 }).asPromise();
  try {
    const result = await cleanRun({ ...o, db: conn.db as Db }, run);
    console.error(`removed · ${describeCleanup(result)}`);
    return result;
  } finally {
    await conn.close().catch(() => undefined);
  }
}

let interrupted: NodeJS.Signals | null = null;

/**
 * Has this run been stopped from outside? Its own steps and its ordinary cleanup then stand still (standStill): the
 * interrupt's cleanup is the only thing that still writes, and the only thing that ends the process.
 */
export const isInterrupted = (): boolean => interrupted !== null;

/** Never comes back: where an interrupted run's own flow waits for the process to leave. */
export const standStill = (): Promise<never> => new Promise<never>(() => undefined);

/**
 * Ctrl-C, kill, a closed terminal: `cleanup` runs once, then the process leaves with the signal's usual code.
 * Answers the function that takes the handlers off again.
 */
export function onInterrupt(cleanup: (signal: NodeJS.Signals) => Promise<unknown>): () => void {
  const signals: NodeJS.Signals[] = ["SIGINT", "SIGTERM", "SIGHUP"];
  const handler = (signal: NodeJS.Signals) => {
    if (interrupted) return;
    interrupted = signal;
    console.error(`\ninterrupted (${signal}) — removing what this run made before leaving`);
    void cleanup(signal)
      .catch((err) => console.error(err))
      .finally(() => process.exit(signal === "SIGINT" ? 130 : signal === "SIGHUP" ? 129 : 143));
  };
  for (const signal of signals) process.on(signal, handler);
  return () => {
    for (const signal of signals) process.off(signal, handler);
  };
}
