import fs from "node:fs";
import path from "node:path";
import { Router } from "express";
import { config } from "../config.js";
import {
  currentRunId,
  getLiveRun,
  startTestFlow,
  TestRunInProgressError,
  type TestFlowConfig,
  type TestFlowResult,
} from "../services/testFlow.js";

/**
 * The Test tab's backend.
 *
 * A run drives a real browser against a real Flipkart account for 40-160s, which
 * rules out answering synchronously: POST starts one and returns an id, the UI
 * polls GET. There is no Mongo model — every run already writes result.json beside
 * its screenshots, so that directory IS the index. Tiny data, nothing to migrate,
 * and the artifacts stay the source of truth.
 *
 * Mounted only when ENABLE_TEST_ROUTES=1. Off by default because this drives real
 * pages with a real logged-in account and should never be reachable by accident.
 */
export const testRouter = Router();

const ARTIFACT_ROOT = path.resolve(config.testArtifactDir);

/** Finished runs, newest first, read from the artifact directory. */
function listRunsFromDisk(limit = 25): TestFlowResult[] {
  if (!fs.existsSync(ARTIFACT_ROOT)) return [];
  const out: TestFlowResult[] = [];
  for (const name of fs.readdirSync(ARTIFACT_ROOT)) {
    const file = path.join(ARTIFACT_ROOT, name, "result.json");
    if (!fs.existsSync(file)) continue;
    try {
      out.push(JSON.parse(fs.readFileSync(file, "utf8")) as TestFlowResult);
    } catch { /* a half-written result.json is not worth failing the list over */ }
  }
  return out.sort((a, b) => String(b.startedAt).localeCompare(String(a.startedAt))).slice(0, limit);
}

function findRun(runId: string): TestFlowResult | null {
  // The live map first: during a run its result.json is still being rewritten.
  return getLiveRun(runId) || listRunsFromDisk(200).find((r) => r.runId === runId) || null;
}

/** Trim a run for the list view — artifacts are large and the list does not need them. */
function summarise(r: TestFlowResult) {
  return {
    runId: r.runId,
    caseId: r.caseId,
    status: r.status,
    running: Boolean(r.running),
    step: r.step,
    code: r.code,
    reason: r.reason,
    url: r.url,
    reachedPayments: r.reachedPayments,
    steps: r.artifacts.length,
    startedAt: r.startedAt,
    seconds: r.seconds,
    config: r.config,
  };
}

testRouter.post("/flow", (req, res) => {
  const body = (req.body || {}) as Partial<TestFlowConfig>;
  if (!body.platformId || !body.productUrl) {
    res.status(400).json({ error: "platformId and productUrl are required" });
    return;
  }
  const cfg: TestFlowConfig = {
    caseId: String(body.caseId || "adhoc").replace(/[^a-z0-9_-]/gi, "").slice(0, 40) || "adhoc",
    platformId: String(body.platformId),
    productUrl: String(body.productUrl),
    quantity: Number(body.quantity) > 0 ? Number(body.quantity) : 1,
    addressId: body.addressId != null ? Number(body.addressId) : undefined,
    gstId: body.gstId != null ? Number(body.gstId) : undefined,
    gstMandatory: Boolean(body.gstMandatory),
    checkoutPincode: body.checkoutPincode ? String(body.checkoutPincode) : undefined,
    deliverySlaDays: body.deliverySlaDays != null ? Number(body.deliverySlaDays) : undefined,
    paymentMode: body.paymentMode,
    cardType: body.cardType,
    authType: body.authType,
    cards: Array.isArray(body.cards) ? body.cards : undefined,
    mobileDevice: body.mobileDevice,
    // Always headless from the UI: the browser runs on the server, and a headed
    // window would open on the server's screen where nobody is looking.
    headless: true,
    stopAfter: body.stopAfter || "payments",
    // Both switches required. The UI sends payLive only from an explicit tick.
    payLive: body.stopAfter === "pay" && body.payLive === true,
    expect: body.expect,
  };

  try {
    const runId = startTestFlow(cfg);
    res.status(201).json({ runId });
  } catch (err) {
    if (err instanceof TestRunInProgressError) {
      // 409, not 500: this is the one-at-a-time rule working, not a fault.
      res.status(409).json({ error: err.message, runId: err.runId });
      return;
    }
    res.status(400).json({ error: err instanceof Error ? err.message : "Could not start the run" });
  }
});

testRouter.get("/runs", (_req, res) => {
  const live = currentRunId();
  res.json({ running: live, runs: listRunsFromDisk().map(summarise) });
});

testRouter.get("/runs/:runId", (req, res) => {
  const run = findRun(String(req.params.runId));
  if (!run) {
    res.status(404).json({ error: "Run not found" });
    return;
  }
  // Artifact file paths are server-side absolute; the UI gets indexes instead.
  res.json({
    ...run,
    artifacts: run.artifacts.map((a, i) => ({
      index: i,
      step: a.step,
      at: a.at,
      url: a.url,
      note: a.note,
      hasScreenshot: Boolean(a.screenshot),
      hasText: Boolean(a.textFile),
    })),
  });
});

/** One artifact: `?kind=text` for the page text, otherwise the screenshot. */
testRouter.get("/runs/:runId/art/:index", (req, res) => {
  const run = findRun(String(req.params.runId));
  const artifact = run?.artifacts[Number(req.params.index)];
  if (!artifact) {
    res.status(404).json({ error: "Artifact not found" });
    return;
  }
  const wantText = String(req.query.kind || "") === "text";
  const file = wantText ? artifact.textFile : artifact.screenshot;
  if (!file) {
    res.status(404).json({ error: wantText ? "No page text captured" : "No screenshot captured" });
    return;
  }
  // Never serve outside the artifact root, whatever a result.json claims.
  const resolved = path.resolve(file);
  if (!resolved.startsWith(ARTIFACT_ROOT + path.sep) || !fs.existsSync(resolved)) {
    res.status(404).json({ error: "Artifact file is missing" });
    return;
  }
  res.type(wantText ? "text/plain" : "image/png").sendFile(resolved);
});
