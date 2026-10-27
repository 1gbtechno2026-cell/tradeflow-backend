import { CloudWatchClient, PutMetricDataCommand, type MetricDatum } from "@aws-sdk/client-cloudwatch";
import { config } from "../config.js";
import { CANCEL_QUEUE, HTTP_QUEUE, LEGACY_QUEUE, metricsQueue, queueNameFor, RESCHEDULE_QUEUE, SYNC_QUEUE, UPDATE_QUEUE } from "../queue.js";
import { JOB_CLASSES } from "./jobClass.js";

/**
 * Queue depth → CloudWatch, so ECS can scale the Chrome workers
 * (infra/lib/worker-fleet-stack.ts in the dashboard repo).
 *
 * Every 30 s, for each BullMQ queue: TradeFlow/QueueWaiting, QueueActive and
 * QueueBacklog (= waiting + delayed + prioritized + active). Two rolled-up
 * series drive the scaling policies: Queue=checkout (all checkout-* queues
 * plus the legacy "checkout") and Queue=cancel (order-cancel +
 * order-reschedule). Backlog, not waiting, so tasks are never removed while
 * jobs are still running on them. Off unless QUEUE_METRICS=1 (local dev has
 * no CloudWatch). Dimension Env = QUEUE_METRICS_ENV (default "dev").
 */

const NAMESPACE = "TradeFlow";
const INTERVAL_MS = 30_000;
let timer: NodeJS.Timeout | null = null;

export function queueMetricsEnabled(): boolean {
  return ["1", "true", "on"].includes(String(process.env.QUEUE_METRICS || "").trim().toLowerCase());
}

interface Counts {
  waiting: number;
  active: number;
  backlog: number;
}

async function counts(name: string): Promise<Counts> {
  const c = await metricsQueue(name).getJobCounts("waiting", "active", "delayed", "prioritized", "waiting-children");
  const waiting = (c.waiting || 0) + (c.delayed || 0) + (c.prioritized || 0) + (c["waiting-children"] || 0);
  const active = c.active || 0;
  return { waiting, active, backlog: waiting + active };
}

export async function collectQueueMetrics(): Promise<Record<string, Counts>> {
  const names = [LEGACY_QUEUE, ...JOB_CLASSES.map((cls) => queueNameFor(cls)), HTTP_QUEUE, CANCEL_QUEUE, RESCHEDULE_QUEUE, UPDATE_QUEUE, SYNC_QUEUE];
  const out: Record<string, Counts> = {};
  await Promise.all(
    names.map(async (n) => {
      out[n] = await counts(n).catch(() => ({ waiting: 0, active: 0, backlog: 0 }));
    })
  );
  const sum = (keys: string[]): Counts => keys.reduce((a, k) => ({ waiting: a.waiting + out[k].waiting, active: a.active + out[k].active, backlog: a.backlog + out[k].backlog }), { waiting: 0, active: 0, backlog: 0 });
  out.checkout = sum([LEGACY_QUEUE, ...JOB_CLASSES.map((cls) => queueNameFor(cls)), HTTP_QUEUE]);
  out.cancel = sum([CANCEL_QUEUE, RESCHEDULE_QUEUE]);
  return out;
}

let client: CloudWatchClient | null = null;

async function publish(): Promise<void> {
  const env = String(process.env.QUEUE_METRICS_ENV || "dev");
  const all = await collectQueueMetrics();
  const data: MetricDatum[] = [];
  const now = new Date();
  for (const [queue, c] of Object.entries(all)) {
    const dims = [
      { Name: "Queue", Value: queue },
      { Name: "Env", Value: env },
    ];
    data.push({ MetricName: "QueueWaiting", Dimensions: dims, Timestamp: now, Unit: "Count", Value: c.waiting });
    data.push({ MetricName: "QueueActive", Dimensions: dims, Timestamp: now, Unit: "Count", Value: c.active });
    data.push({ MetricName: "QueueBacklog", Dimensions: dims, Timestamp: now, Unit: "Count", Value: c.backlog });
  }
  if (!client) client = new CloudWatchClient({ region: process.env.AWS_REGION || config.awsRegion || "ap-south-1" });
  // PutMetricData takes at most 1000 datums per call; we have ~30.
  await client.send(new PutMetricDataCommand({ Namespace: NAMESPACE, MetricData: data }));
}

export function startQueueMetrics(): void {
  if (timer) return;
  if (!queueMetricsEnabled()) {
    console.log("[queue-metrics] off (QUEUE_METRICS is not 1)");
    return;
  }
  const tick = () =>
    publish().catch((err) => console.warn(`[queue-metrics] ${err instanceof Error ? err.message : String(err)}`));
  void tick();
  timer = setInterval(tick, INTERVAL_MS);
  timer.unref();
  console.log(`[queue-metrics] publishing ${NAMESPACE}/QueueBacklog every ${INTERVAL_MS / 1000} s (Env=${process.env.QUEUE_METRICS_ENV || "dev"})`);
}

export function stopQueueMetrics(): void {
  if (timer) clearInterval(timer);
  timer = null;
}
