import cors from "cors";
import express from "express";
import { config } from "./config.js";
import { connectDb, dbState } from "./db.js";
import { jobsRouter } from "./routes/jobs.js";
import { testRouter } from "./routes/test.js";
import { orderFetchRouter } from "./routes/orderFetch.js";
import { startFetchSchedule } from "./services/orderFetchSchedule.js";
import { startQueueMetrics } from "./services/queueMetrics.js";
import { smsRouter } from "./routes/sms.js";
import { describeError, isTransientError, throttledWarn } from "./lib/resilience.js";

function stamp() {
  return new Date().toISOString();
}

function requireApiKey(req: express.Request, res: express.Response, next: express.NextFunction) {
  const key = req.header("x-api-key") || "";
  if (!key || key !== config.apiKey) {
    console.warn(`[trade-flow] ${stamp()} AUTH FAIL ${req.method} ${req.originalUrl} — missing or wrong x-api-key`);
    res.status(401).json({ error: "Unauthorized" });
    return;
  }
  console.log(`[trade-flow] ${stamp()} AUTH OK ${req.method} ${req.originalUrl}`);
  next();
}

// A network / Atlas / Redis outage is not a bug: log it once a minute and keep
// running — the driver reconnects, queues resume. Real bugs still end the process.
process.on("unhandledRejection", (reason) => {
  if (isTransientError(reason)) {
    throttledWarn("guard:transient", `[guard] outage, process stays up: ${describeError(reason)}`);
    return;
  }
  console.error("[guard] UNHANDLED REJECTION — exiting:", reason);
  process.exit(1);
});
process.on("uncaughtException", (err) => {
  if (isTransientError(err)) {
    throttledWarn("guard:transient", `[guard] outage (sync), process stays up: ${describeError(err)}`);
    return;
  }
  console.error("[guard] UNCAUGHT EXCEPTION — exiting:", err);
  process.exit(1);
});

async function main() {
  await connectDb();
  const app = express();
  app.use(cors());
  app.use(express.json({ limit: "2mb" }));

  app.use((req, res, next) => {
    const start = Date.now();
    console.log(`[trade-flow] ${stamp()} --> ${req.method} ${req.originalUrl}`);
    res.on("finish", () => {
      console.log(
        `[trade-flow] ${stamp()} <-- ${req.method} ${req.originalUrl} ${res.statusCode} (${Date.now() - start}ms)`
      );
    });
    next();
  });

  app.get("/api/health", (_req, res) => {
    const db = dbState();
    res.status(db.readyState === 1 ? 200 : 503).json({ ok: db.readyState === 1, db: db.label, env: config.envName, workerId: config.workerId, proxyLease: config.proxyLease, uptimeSec: Math.round(process.uptime()) });
  });
  app.use("/api/sms", smsRouter);
  app.use(requireApiKey);

  app.use("/api/jobs", jobsRouter);
  app.use("/api/orders", orderFetchRouter);
  // Only when explicitly enabled: these drive real Flipkart pages with a real
  // logged-in account, so they must not be reachable by default.
  if (config.enableTestRoutes) {
    app.use("/api/test", testRouter);
    console.log("[trade-flow] test routes ENABLED at /api/test (ENABLE_TEST_ROUTES=1)");
  }

  // Last stop for anything a route threw or rejected: a DB/network outage is a
  // 503 the dashboard proxy passes through, a bad body a 400, the rest a logged 500.
  app.use((err: unknown, req: express.Request, res: express.Response, _next: express.NextFunction) => {
    const e = err as { status?: number; statusCode?: number; type?: string; message?: string };
    const status = Number(e.status || e.statusCode) || 0;
    if (res.headersSent) {
      res.end();
      return;
    }
    if (isTransientError(err)) {
      throttledWarn("http:transient", `[trade-flow] ${req.method} ${req.originalUrl} → 503 (${describeError(err)})`);
      res.status(503).json({ error: "Database or network is unreachable right now — try again in a moment", retryable: true });
      return;
    }
    if (status >= 400 && status < 500) {
      res.status(status).json({ error: e.type === "entity.parse.failed" ? "Request body is not valid JSON" : e.message || "Bad request" });
      return;
    }
    console.error(`[trade-flow] ${req.method} ${req.originalUrl} failed:`, err);
    res.status(500).json({ error: "Internal error" });
  });

  app.listen(config.port, "0.0.0.0", () => {
    console.log(`[trade-flow] ${stamp()} API listening on http://0.0.0.0:${config.port}`);
    console.log(`[trade-flow] Submit Order should show: --> POST /api/jobs`);
  });
  // Scheduled Fetch (FETCH_SCHEDULE); off unless set.
  startFetchSchedule();
  startQueueMetrics();
}

main().catch((err) => {
  console.error("Failed to start API:", err);
  process.exit(1);
});
