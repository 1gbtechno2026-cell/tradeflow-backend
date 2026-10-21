import cors from "cors";
import express from "express";
import { config } from "./config.js";
import { connectDb } from "./db.js";
import { jobsRouter } from "./routes/jobs.js";
import { testRouter } from "./routes/test.js";
import { orderFetchRouter } from "./routes/orderFetch.js";
import { smsRouter } from "./routes/sms.js";

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
    res.json({ ok: true });
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

  app.listen(config.port, "0.0.0.0", () => {
    console.log(`[trade-flow] ${stamp()} API listening on http://0.0.0.0:${config.port}`);
    console.log(`[trade-flow] Submit Order should show: --> POST /api/jobs`);
  });
}

main().catch((err) => {
  console.error("Failed to start API:", err);
  process.exit(1);
});
