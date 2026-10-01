import { CheckoutJob } from "../models/CheckoutJob.js";
import type { JobResultSnapshot, LogLevel } from "../types.js";

/**
 * One job's reporting back to Mongo, batched.
 *
 * ── WHY ───────────────────────────────────────────────────────────────────────
 * The worker used to write every log line as its own updateOne ($push) and
 * every result field group as another, plus a heartbeat on a timer — about
 * sixty round trips per job. One worker never noticed. A few hundred workers
 * each placing an order a minute turn that into thousands of tiny writes a
 * minute against one collection, every one of them a lock and an index touch
 * on the same document, for data nobody reads until the job is over.
 *
 * So: log lines and result fields are buffered here and written as ONE
 * updateOne — `$push: { logs: { $each } }` plus one `$set` — at most every
 * FLUSH_MS, immediately on an error line or when the buffer is full, and
 * always before a terminal status write and at close. Every flush also stamps
 * heartbeatAt, so a chatty job needs no separate heartbeat round trip.
 *
 * Ordering is preserved: lines keep their own timestamps, and a flush is a
 * single atomic update, so the UI never sees a result field without the log
 * line that produced it.
 *
 * What is NOT buffered: status changes. `queued → running → paid/failed` are
 * written directly by the runner, each preceded by flush(), so the one thing
 * other workers and the operator key on is never stale.
 */

const FLUSH_MS = Number(process.env.JOB_REPORT_FLUSH_MS || 1500);
const MAX_BUFFERED_LINES = 40;

interface BufferedLine {
  at: Date;
  level: LogLevel;
  step?: string;
  message: string;
}

export class JobReporter {
  private lines: BufferedLine[] = [];
  private pending: Record<string, unknown> = {};
  private timer: NodeJS.Timeout | null = null;
  private inflight: Promise<void> | null = null;
  private closed = false;
  /** Once the runner has written a terminal status, later lines must not move
   *  `step` back to whatever they were tagged with. */
  private terminal = false;
  writes = 0;

  constructor(private readonly jobId: string) {}

  log(level: LogLevel, message: string, step?: string): void {
    console.log(`[${this.jobId}] [${level}]${step ? ` [${step}]` : ""} ${message}`);
    this.lines.push({ at: new Date(), level, step, message });
    if (step && !this.terminal) this.pending.step = step;
    if (level === "error" || this.lines.length >= MAX_BUFFERED_LINES) void this.flush();
    else this.schedule();
  }

  /** Result fields, merged into the next flush. Last write of a key wins. */
  patch(patch: Partial<JobResultSnapshot>): void {
    for (const [key, value] of Object.entries(patch)) {
      if (value === undefined) continue;
      this.pending[`result.${key}`] = value;
    }
    this.schedule();
  }

  /** Call right before the runner writes a terminal status: everything
   *  buffered lands first, and no later line may change `step`. */
  async beforeTerminal(): Promise<void> {
    this.terminal = true;
    delete this.pending.step;
    await this.flush();
  }

  /** Final flush. Safe to call more than once. */
  async close(): Promise<void> {
    this.closed = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    await this.flush();
  }

  private schedule(): void {
    if (this.timer || this.closed) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.flush();
    }, FLUSH_MS);
    this.timer.unref?.();
  }

  async flush(): Promise<void> {
    // Serialise: a flush that starts while one is in flight waits for it, so
    // two updates can never race each other's $set.
    if (this.inflight) await this.inflight;
    if (!this.lines.length && !Object.keys(this.pending).length) return;
    const lines = this.lines;
    const set: Record<string, unknown> = { ...this.pending, heartbeatAt: new Date() };
    this.lines = [];
    this.pending = {};
    if (this.terminal) delete set.step;
    const update: Record<string, unknown> = { $set: set };
    if (lines.length) update.$push = { logs: { $each: lines } };
    this.inflight = CheckoutJob.updateOne({ _id: this.jobId }, update)
      .then(() => {
        this.writes += 1;
      })
      .catch((err: unknown) => {
        // Never lose the lines: put them back for the next flush. The console
        // already has them, so the operator is not blind meanwhile.
        this.lines = [...lines, ...this.lines];
        this.pending = { ...set, ...this.pending };
        console.warn(`[${this.jobId}] [reporter] flush failed, will retry: ${err instanceof Error ? err.message : String(err)}`);
      })
      .finally(() => {
        this.inflight = null;
      });
    await this.inflight;
  }
}

const reporters = new Map<string, JobReporter>();

/** The reporter for a job the runner has opened, or null outside a run. */
export function reporterFor(jobId: string): JobReporter | null {
  return reporters.get(jobId) ?? null;
}

export function openReporter(jobId: string): JobReporter {
  const r = new JobReporter(jobId);
  reporters.set(jobId, r);
  return r;
}

export async function closeReporter(jobId: string): Promise<void> {
  const r = reporters.get(jobId);
  reporters.delete(jobId);
  if (r) await r.close();
}
