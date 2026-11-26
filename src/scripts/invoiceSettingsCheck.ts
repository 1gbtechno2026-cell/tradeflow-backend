/**
 * Invoice settings (Settings → Invoice settings; services/invoiceSettings.ts) — checked WITHOUT Flipkart and WITHOUT S3:
 *
 *   · the seed writes a workspace's document once, with captureCancelledAfterShipping ON and nothing else — again,
 *     six at once, or after a save: nothing more
 *   · precedence per field: the document > the .env > the default (the .env read at call time, as before)
 *   · the cache: a document read is kept 30 s per workspace; a save (PUT) clears it, so the next read has it
 *   · validation: unknown fields, values out of range, wrong types, nothing to save → 400, nothing written
 *   · history: from = what was in effect, to = the new value; a value saved again is no change; the last 50 kept
 *   · the sweep reads captureCancelledAfterShipping from the document (the .env saying 0), settleHours for phase 2
 *     and recaptureGiveUpDays for the settle — and a save applies to the next sweep, no restart
 *   · refetchDueAt / noteInvoiceRefetch use a delay saved in the document
 *   · the real routes, GET and PUT /api/orders/invoice-settings, on a listener of this process (127.0.0.1)
 *   · then it re-runs scripts/invoiceRefetchCheck.ts and scripts/invoiceOriginalsCheck.ts (skip: --no-rerun)
 *
 * Mongo is real, rows are invented: made-up workspace ids (never the real one), order ids ODINVSET…; every
 * invoice_settings, invoices, order_details and order_unit_events row of those workspaces is deleted at the end and the
 * leftovers counted (must be 0). Connected with autoIndex / autoCreate off; the service itself makes the
 * invoice_settings collection and its unique index on userId (what the seed does at the API's start). Every download
 * is a function injected into the sweep that answers "no session"; the S3 client is one that refuses every call.
 *
 *   npm run test:invoice-settings
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import type { AddressInfo } from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";
import mongoose, { Types } from "mongoose";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const originalEnv = { ...process.env };

let passed = 0;
const failures: string[] = [];
async function check(name: string, fn: () => void | Promise<void>) {
  try {
    await fn();
    passed++;
    console.log(`  ok    ${name}`);
  } catch (err) {
    failures.push(name);
    console.log(`  FAIL  ${name}\n        ${err instanceof Error ? (err.stack || err.message).split("\n").slice(0, 12).join("\n        ") : err}`);
  }
}

const H = 3_600_000;
const D = 24 * H;
const ago = (ms: number) => new Date(Date.now() - ms);
const ENV_KEYS = ["INVOICE_CAPTURE_CANCELLED", "INVOICE_REFETCH_CANCEL_DELAY_H", "INVOICE_REFETCH_REFUND_DELAY_H", "INVOICE_REFETCH_COD_DELAY_H", "INVOICE_SETTLE_HOURS", "INVOICE_RECAPTURE_GIVE_UP_DAYS"];
const REAL_WORKSPACE = "6a846111a5195d3043deef7c";

async function rejects400(p: Promise<unknown>, code: string, re?: RegExp) {
  try {
    await p;
  } catch (err) {
    const e = err as { status?: number; code?: string; message?: string };
    assert.equal(e.status, 400, `expected a 400, got ${e.status} (${e.message})`);
    assert.equal(e.code, code, e.message);
    if (re) assert.match(String(e.message), re);
    return;
  }
  assert.fail(`expected a 400 ${code}, but it saved`);
}

async function main() {
  const wsSeed = new Types.ObjectId();
  const wsRace = new Types.ObjectId();
  const wsSavedFirst = new Types.ObjectId();
  const wsPrec = new Types.ObjectId();
  const wsCache = new Types.ObjectId();
  const wsHist = new Types.ObjectId();
  const wsSweep = new Types.ObjectId();
  const wsNote = new Types.ObjectId();
  const wsRoute = new Types.ObjectId();
  const workspaces = [wsSeed, wsRace, wsSavedFirst, wsPrec, wsCache, wsHist, wsSweep, wsNote, wsRoute];
  assert.ok(workspaces.every((w) => String(w) !== REAL_WORKSPACE));
  process.env.TRADE_FLOW_USER_ID = String(wsRoute);
  process.env.INVOICE_S3_BUCKET = `verify-no-s3-${Date.now().toString(36)}`;

  const { config } = await import("../config.js");
  // AFTER config.ts has run dotenv (it never runs again): whatever this Mac's .env sets, the check starts with none.
  for (const k of ENV_KEYS) delete process.env[k];
  assert.equal(config.userId, String(wsRoute));
  const { InvoiceSettings } = await import("../models/InvoiceSettings.js");
  const { Invoice } = await import("../models/Invoice.js");
  const { Order } = await import("../models/Order.js");
  const { OrderUnitEvent } = await import("../models/OrderUnitEvent.js");
  const svc = await import("../services/invoiceSettings.js");
  const capture = await import("../services/invoiceCapture.js");
  const refetch = await import("../services/invoiceRefetch.js");
  const sweep = await import("../services/invoiceSweep.js");
  type DownloadFn = NonNullable<NonNullable<Parameters<typeof sweep.runInvoiceSweep>[1]>["download"]>;
  type DownloadOpts = Parameters<DownloadFn>[0];

  capture.useInvoiceS3ForChecks({ send: async () => assert.fail("a call reached S3") } as unknown as import("../services/invoiceCapture.js").InvoiceS3);
  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false, serverSelectionTimeoutMS: 15_000 });

  const tag = Date.now().toString().slice(-8);
  const account = new Types.ObjectId();
  let seq = 0;
  const order = (n: number) => `ODINVSET${tag}${String(n).padStart(2, "0")}`;
  const insertUnits = async (ws: Types.ObjectId, orderId: string, units: Array<Record<string, unknown>>) => {
    const docs = units.map((u) => ({
      userId: ws,
      platformAccountId: account,
      platform_email: "invoice-settings@verify.invalid",
      id: 992_000_000 + Number(tag.slice(-4)) * 100 + ++seq,
      order_id: orderId,
      unit_id: `${orderId}-U${seq}`,
      item_id: `ITEM${seq}`,
      invoice_s3_key: "",
      invoice_parts: [],
      is_invoice_downloaded: false,
      tracking: [],
      ...u,
    }));
    await Order.collection.insertMany(docs as never[]);
  };
  const insertInvoice = async (ws: Types.ObjectId, orderId: string, extra: Record<string, unknown> = {}) => {
    await Invoice.collection.insertOne({
      userId: ws,
      order_id: orderId,
      s3_key: capture.rawInvoiceKey(String(ws), orderId),
      source_key: capture.rawInvoiceKey(String(ws), orderId),
      etag: "verify-etag",
      size: 1,
      parts: [],
      processing_status: "processed",
      settled: true,
      captured_at: ago(20 * D),
      unit_ids: [],
      createdAt: ago(20 * D),
      updatedAt: new Date(),
      ...extra,
    } as never);
  };
  const docOf = async (ws: Types.ObjectId) => InvoiceSettings.collection.findOne({ userId: ws });
  const put = (ws: Types.ObjectId, fields: Record<string, unknown>, by = "verify-user") => svc.setInvoiceSettings({ userId: String(ws), by, ...fields });
  let server: import("node:http").Server | null = null;

  try {
    console.log("SEED");
    await check("seed: a workspace with no document gets one — captureCancelledAfterShipping ON and no other rule field (they follow .env / defaults); by seed:<host>; one history line from the old value (off) to ON", async () => {
      assert.equal(await svc.seedInvoiceSettings(String(wsSeed)), true);
      const doc = await docOf(wsSeed);
      assert.ok(doc);
      assert.equal(doc.captureCancelledAfterShipping, true);
      for (const f of ["refetchCancelDelayH", "refetchRefundDelayH", "refetchCodDelayH", "settleHours", "recaptureGiveUpDays"]) assert.equal(f in doc, false, `${f} was written`);
      assert.equal(doc.changedBy, `seed:${config.hostId}`);
      assert.deepEqual(doc.history.map((h: Record<string, unknown>) => [h.field, h.from, h.to]), [["captureCancelledAfterShipping", false, true]]);
      const s = await svc.getInvoiceSettings(String(wsSeed));
      assert.equal(s.effective.captureCancelledAfterShipping, true);
      assert.deepEqual(s.sources, { captureCancelledAfterShipping: "db", refetchCancelDelayH: "default", refetchRefundDelayH: "default", refetchCodDelayH: "default", settleHours: "default", recaptureGiveUpDays: "default" });
      assert.deepEqual(s.settings, { captureCancelledAfterShipping: true });
    });

    await check("seed is idempotent: a second seed writes nothing; six seeds at once (API + worker + another Mac) write ONE document, exactly one of them says it wrote", async () => {
      const before = await docOf(wsSeed);
      assert.equal(await svc.seedInvoiceSettings(String(wsSeed)), false);
      assert.deepEqual(await docOf(wsSeed), before);
      const results = await Promise.all(Array.from({ length: 6 }, () => svc.seedInvoiceSettings(String(wsRace))));
      assert.equal(results.filter(Boolean).length, 1, JSON.stringify(results));
      assert.equal(await InvoiceSettings.collection.countDocuments({ userId: wsRace }), 1);
    });

    await check("seed never touches a document that exists: switched OFF by a person stays OFF; a document a save made first (another field) gets no seed value — the switch keeps following .env / default", async () => {
      await put(wsSeed, { captureCancelledAfterShipping: false });
      assert.equal(await svc.seedInvoiceSettings(String(wsSeed)), false);
      assert.equal((await docOf(wsSeed))!.captureCancelledAfterShipping, false);
      await put(wsSavedFirst, { settleHours: 72 });
      assert.equal(await svc.seedInvoiceSettings(String(wsSavedFirst)), false);
      const doc = await docOf(wsSavedFirst);
      assert.equal("captureCancelledAfterShipping" in doc!, false);
      assert.equal((await svc.getInvoiceSettings(String(wsSavedFirst))).sources.captureCancelledAfterShipping, "default");
    });

    await check("seedInvoiceSettingsAtStart never throws (an id that is not a workspace: a warning); seedInvoiceSettings refuses the same with a 400", async () => {
      await svc.seedInvoiceSettingsAtStart("not-a-workspace");
      await svc.seedInvoiceSettingsAtStart("");
      await rejects400(svc.seedInvoiceSettings("not-a-workspace"), "invalid_user");
    });

    console.log("PRECEDENCE");
    await check("pure resolution: document > .env > default, field by field; a .env value is parsed as the code always did (INVOICE_CAPTURE_CANCELLED=1 only; settle ≥ 1; delays ≥ 0; give-up > 0); an unusable one falls to the default", () => {
      const r = svc.resolveInvoiceSettings;
      assert.deepEqual(r(null, {}).effective, { captureCancelledAfterShipping: false, refetchCancelDelayH: 24, refetchRefundDelayH: 192, refetchCodDelayH: 192, settleHours: 48, recaptureGiveUpDays: 30 });
      const env = { INVOICE_CAPTURE_CANCELLED: "1", INVOICE_REFETCH_CANCEL_DELAY_H: "2", INVOICE_REFETCH_REFUND_DELAY_H: "abc", INVOICE_REFETCH_COD_DELAY_H: "-1", INVOICE_SETTLE_HOURS: "0", INVOICE_RECAPTURE_GIVE_UP_DAYS: "0" };
      const e = r(null, env);
      assert.deepEqual(e.effective, { captureCancelledAfterShipping: true, refetchCancelDelayH: 2, refetchRefundDelayH: 192, refetchCodDelayH: 192, settleHours: 1, recaptureGiveUpDays: 30 });
      assert.deepEqual(e.sources, { captureCancelledAfterShipping: "env", refetchCancelDelayH: "env", refetchRefundDelayH: "default", refetchCodDelayH: "default", settleHours: "env", recaptureGiveUpDays: "default" });
      assert.equal(r(null, { INVOICE_CAPTURE_CANCELLED: "yes" }).effective.captureCancelledAfterShipping, false, "only 1 switches it on, as before");
      const d = r({ captureCancelledAfterShipping: false, refetchCancelDelayH: 5, settleHours: 12 }, env);
      assert.deepEqual([d.effective.captureCancelledAfterShipping, d.effective.refetchCancelDelayH, d.effective.settleHours, d.effective.recaptureGiveUpDays], [false, 5, 12, 30]);
      assert.deepEqual([d.sources.captureCancelledAfterShipping, d.sources.refetchCancelDelayH, d.sources.settleHours, d.sources.refetchCodDelayH], ["db", "db", "db", "default"]);
      // A document value of the wrong type or out of range is ignored (the .env / default applies).
      const bad = r({ refetchCancelDelayH: 9999, settleHours: "12" as never, captureCancelledAfterShipping: 1 as never }, {});
      assert.deepEqual([bad.effective.refetchCancelDelayH, bad.effective.settleHours, bad.effective.captureCancelledAfterShipping], [24, 48, false]);
      assert.deepEqual([bad.sources.refetchCancelDelayH, bad.sources.settleHours, bad.sources.captureCancelledAfterShipping], ["default", "default", "default"]);
    });

    await check("through Mongo: no document → default; .env set → env; saved → db, and the saved value wins over the .env (both ways for the switch: saved OFF beats INVOICE_CAPTURE_CANCELLED=1)", async () => {
      let s = await svc.getInvoiceSettings(String(wsPrec));
      assert.equal(s.sources.refetchCancelDelayH, "default");
      process.env.INVOICE_REFETCH_CANCEL_DELAY_H = "10";
      process.env.INVOICE_CAPTURE_CANCELLED = "1";
      process.env.INVOICE_SETTLE_HOURS = "12";
      try {
        s = await svc.getInvoiceSettings(String(wsPrec));
        assert.deepEqual([s.effective.refetchCancelDelayH, s.sources.refetchCancelDelayH, s.effective.captureCancelledAfterShipping, s.sources.captureCancelledAfterShipping], [10, "env", true, "env"]);
        const view = await put(wsPrec, { refetchCancelDelayH: 3, captureCancelledAfterShipping: false });
        assert.deepEqual([view.effective.refetchCancelDelayH, view.sources.refetchCancelDelayH, view.effective.captureCancelledAfterShipping, view.sources.captureCancelledAfterShipping], [3, "db", false, "db"]);
        assert.deepEqual([view.effective.settleHours, view.sources.settleHours], [12, "env"], "a field not saved keeps following the .env");
        s = await svc.getInvoiceSettings(String(wsPrec));
        assert.deepEqual([s.effective.refetchCancelDelayH, s.effective.captureCancelledAfterShipping, s.effective.settleHours], [3, false, 12]);
        assert.deepEqual(s.settings, { captureCancelledAfterShipping: false, refetchCancelDelayH: 3 });
      } finally {
        for (const k of ENV_KEYS) delete process.env[k];
      }
      s = await svc.getInvoiceSettings(String(wsPrec));
      assert.deepEqual([s.effective.settleHours, s.sources.settleHours], [48, "default"], "the .env is read on every call, not cached");
    });

    await check("the sync helpers without settings read the .env at call time (what invoiceRefetchCheck / invoiceOriginalsCheck rely on); with settings they use them", () => {
      assert.deepEqual(refetch.refetchDelaysH(), { cancel: 24, refund: 192, cod: 192 });
      process.env.INVOICE_REFETCH_REFUND_DELAY_H = "100";
      process.env.INVOICE_SETTLE_HOURS = "6";
      process.env.INVOICE_RECAPTURE_GIVE_UP_DAYS = "4";
      process.env.INVOICE_CAPTURE_CANCELLED = "1";
      try {
        assert.equal(refetch.refetchDelaysH().refund, 100);
        assert.equal(capture.invoiceSettleMs(), 6 * H);
        assert.equal(sweep.recaptureGiveUpMs(), 4 * D);
        assert.equal(sweep.captureCancelledEnabled(), true);
      } finally {
        for (const k of ENV_KEYS) delete process.env[k];
      }
      const mine = { ...svc.invoiceSettingsFromEnv(), refetchCodDelayH: 7, settleHours: 3, recaptureGiveUpDays: 2, captureCancelledAfterShipping: true };
      assert.equal(refetch.refetchDelaysH(mine).cod, 7);
      assert.equal(capture.invoiceSettleMs(mine), 3 * H);
      assert.equal(sweep.recaptureGiveUpMs(mine), 2 * D);
      assert.equal(sweep.captureCancelledEnabled(mine), true);
      assert.equal(sweep.captureCancelledEnabled(), false);
    });

    console.log("CACHE");
    await check("the document is cached per workspace: a write that bypasses the service is not seen yet; a save through the service (PUT) clears the cache and the next read has both", async () => {
      await put(wsCache, { refetchRefundDelayH: 100 });
      assert.equal((await svc.getInvoiceSettings(String(wsCache))).effective.refetchRefundDelayH, 100);
      await InvoiceSettings.collection.updateOne({ userId: wsCache }, { $set: { refetchRefundDelayH: 77 } });
      assert.equal((await svc.getInvoiceSettings(String(wsCache))).effective.refetchRefundDelayH, 100, "read from the cache");
      await put(wsCache, { refetchCodDelayH: 50 });
      const s = await svc.getInvoiceSettings(String(wsCache));
      assert.deepEqual([s.effective.refetchRefundDelayH, s.effective.refetchCodDelayH], [77, 50]);
      assert.equal(svc.CACHE_MS, 30_000);
    });

    await check("the cache expires: a document read more than 30 s ago is read again (the worker's way to see a save made in the API)", async () => {
      await InvoiceSettings.collection.updateOne({ userId: wsCache }, { $set: { refetchRefundDelayH: 66 } });
      assert.equal((await svc.getInvoiceSettings(String(wsCache))).effective.refetchRefundDelayH, 77, "still cached");
      const realNow = Date.now;
      Date.now = () => realNow() + svc.CACHE_MS + 1_000;
      try {
        assert.equal((await svc.getInvoiceSettings(String(wsCache))).effective.refetchRefundDelayH, 66);
      } finally {
        Date.now = realNow;
      }
    });

    console.log("VALIDATION");
    await check("PUT validation: an unknown field, a value out of range, a fraction, a string, a non-boolean switch, nothing to save, an id that is not a workspace → 400 with a code — and nothing is written", async () => {
      await put(wsPrec, { settleHours: 24 });
      const before = await docOf(wsPrec);
      await rejects400(put(wsPrec, { settleHours: 30, bogus: 1 }), "unknown_field", /bogus/);
      await rejects400(put(wsPrec, { refetchCancelDelayH: 721 }), "invalid_value", /refetchCancelDelayH.*0 and 720/);
      await rejects400(put(wsPrec, { refetchRefundDelayH: -1 }), "invalid_value");
      await rejects400(put(wsPrec, { refetchCodDelayH: 1.5 }), "invalid_value");
      await rejects400(put(wsPrec, { settleHours: 0 }), "invalid_value", /settleHours.*1 and 720/);
      await rejects400(put(wsPrec, { settleHours: "12" }), "invalid_value");
      await rejects400(put(wsPrec, { recaptureGiveUpDays: 0 }), "invalid_value", /recaptureGiveUpDays.*days between 1 and 365/);
      await rejects400(put(wsPrec, { recaptureGiveUpDays: 366 }), "invalid_value");
      await rejects400(put(wsPrec, { captureCancelledAfterShipping: "true" }), "invalid_value");
      await rejects400(put(wsPrec, { captureCancelledAfterShipping: null }), "invalid_value");
      await rejects400(put(wsPrec, { settleHours: 30, refetchCodDelayH: 999 }), "invalid_value", /refetchCodDelayH/);
      await rejects400(put(wsPrec, {}), "nothing_to_save");
      await rejects400(svc.setInvoiceSettings({ userId: "abc", settleHours: 30 }), "invalid_user");
      await rejects400(svc.setInvoiceSettings([1, 2] as never), "invalid_body");
      assert.deepEqual(await docOf(wsPrec), before, "a refused save wrote something");
    });

    await check("PUT accepts the edges: 0 and 720 hours for the delays, 1 and 720 for settle, 1 and 365 days for give-up, true / false for the switch", async () => {
      const v = await put(wsPrec, { refetchCancelDelayH: 0, refetchRefundDelayH: 720, refetchCodDelayH: 0, settleHours: 1, recaptureGiveUpDays: 365, captureCancelledAfterShipping: true });
      assert.deepEqual(v.effective, { captureCancelledAfterShipping: true, refetchCancelDelayH: 0, refetchRefundDelayH: 720, refetchCodDelayH: 0, settleHours: 1, recaptureGiveUpDays: 365 });
      const w = await put(wsPrec, { settleHours: 720, recaptureGiveUpDays: 1, captureCancelledAfterShipping: false });
      assert.deepEqual([w.effective.settleHours, w.effective.recaptureGiveUpDays, w.effective.captureCancelledAfterShipping], [720, 1, false]);
    });

    console.log("HISTORY");
    await check("history: from = what was in effect (the default here), to = the new value, by and at; changedBy / changedAt follow the last real change; a value saved again is no change; newest first in the view; the last 50 kept", async () => {
      const first = await put(wsHist, { refetchCancelDelayH: 30 }, "verify-a");
      assert.deepEqual(first.history.map((h) => [h.field, h.from, h.to, h.by]), [["refetchCancelDelayH", 24, 30, "verify-a"]]);
      assert.equal(first.changedBy, "verify-a");
      const again = await put(wsHist, { refetchCancelDelayH: 30 }, "verify-b");
      assert.equal(again.history.length, 1, "the same value again is no change");
      assert.equal(again.changedBy, "verify-a");
      const two = await put(wsHist, { refetchCancelDelayH: 31, settleHours: 60 }, "verify-b");
      assert.deepEqual(two.history.slice(0, 2).map((h) => [h.field, h.from, h.to]).sort(), [["refetchCancelDelayH", 30, 31], ["settleHours", 48, 60]].sort());
      assert.equal(two.changedBy, "verify-b");
      assert.ok(two.changedAt instanceof Date || typeof two.changedAt === "string");
      for (let i = 0; i < 55; i++) await put(wsHist, { recaptureGiveUpDays: 2 + i }, "verify-c");
      const doc = await docOf(wsHist);
      assert.equal(doc!.history.length, 50);
      const view = await svc.invoiceSettingsView(String(wsHist));
      assert.deepEqual([view.history[0].to, view.history[49].to], [56, 7], "newest first; the oldest kept is change 9 of 58");
      assert.deepEqual(view.limits.settleHours, { type: "number", default: 48, min: 1, max: 720, unit: "hours" });
      assert.deepEqual(view.limits.captureCancelledAfterShipping, { type: "boolean", default: false });
    });

    console.log("THE SWEEP AND THE REFETCH NOTING");
    const [Oa, Ob, Oc, Od] = [1, 2, 3, 4].map(order);
    const calls: Array<{ path: string; orderIds: string[] }> = [];
    const noSession: DownloadFn = async (opts: DownloadOpts) => {
      calls.push({ path: String(opts.path), orderIds: [...opts.orderIds].sort() });
      return opts.orderIds.map((orderId) => ({ orderId, status: "no_session" as const }));
    };
    await check("the sweep reads captureCancelledAfterShipping from the document: saved ON (the .env saying 0) → phase 1 also takes the order cancelled after shipping; saved OFF → the next sweep does not — no restart", async () => {
      await insertUnits(wsSweep, Oa, [{ status_key: "Cancelled", order_status_pre_cancellation: "Shipped", cancelled_date: ago(16 * D) }]);
      await insertUnits(wsSweep, Ob, [{ status_key: "Delivered", actual_delivery_date: ago(3 * D) }]);
      process.env.INVOICE_CAPTURE_CANCELLED = "0";
      try {
        assert.equal(await svc.seedInvoiceSettings(String(wsSweep)), true);
        calls.length = 0;
        const on = await sweep.runInvoiceSweep(String(wsSweep), { download: noSession });
        assert.equal(on.phase1.candidates, 2);
        assert.deepEqual(calls.filter((c) => c.path === "sweep-capture").map((c) => c.orderIds), [[Oa, Ob].sort()]);
        assert.deepEqual((await sweep.captureCandidates(String(wsSweep), { cap: 200 })).orderIds.sort(), [Oa, Ob].sort(), "captureCandidates without the option reads the workspace's settings");
        await put(wsSweep, { captureCancelledAfterShipping: false });
        calls.length = 0;
        const off = await sweep.runInvoiceSweep(String(wsSweep), { download: noSession });
        assert.equal(off.phase1.candidates, 1);
        assert.deepEqual(calls.filter((c) => c.path === "sweep-capture").map((c) => c.orderIds), [[Ob]]);
      } finally {
        delete process.env.INVOICE_CAPTURE_CANCELLED;
      }
    });

    await check("the sweep's phase 2 window is settleHours from the document: a bundle captured 10 h ago is not due at 48 h (default), due once 6 h is saved; the settle rule (registerInvoice's orderSettledByDelivery) uses it too", async () => {
      await insertUnits(wsSweep, Oc, [{ status_key: "Delivered", actual_delivery_date: ago(10 * H) }]);
      await insertInvoice(wsSweep, Oc, { settled: false, captured_at: ago(10 * H), createdAt: ago(10 * H) });
      await put(wsSweep, { captureCancelledAfterShipping: false });
      assert.equal(await capture.orderSettledByDelivery(String(wsSweep), Oc), false);
      calls.length = 0;
      const before = await sweep.runInvoiceSweep(String(wsSweep), { download: noSession });
      assert.equal(before.phase2.candidates, 0);
      await put(wsSweep, { settleHours: 6 });
      assert.equal(await capture.orderSettledByDelivery(String(wsSweep), Oc), true);
      calls.length = 0;
      const after = await sweep.runInvoiceSweep(String(wsSweep), { download: noSession });
      assert.equal(after.phase2.candidates, 1);
      assert.deepEqual(calls.filter((c) => c.path === "sweep-recapture").map((c) => c.orderIds), [[Oc]]);
    });

    await check("settleRecaptured gives up after recaptureGiveUpDays from the document: a row 3 days old with a unit on its way stays open at 30 days, settles once 2 days is saved", async () => {
      await insertUnits(wsSweep, Od, [{ status_key: "Shipped" }]);
      await insertInvoice(wsSweep, Od, { settled: false, captured_at: ago(3 * D), createdAt: ago(3 * D) });
      assert.equal(await sweep.settleRecaptured(String(wsSweep), [Od]), 0);
      await put(wsSweep, { recaptureGiveUpDays: 2 });
      assert.equal(await sweep.settleRecaptured(String(wsSweep), [Od]), 1);
      assert.equal((await Invoice.collection.findOne({ userId: wsSweep, order_id: Od }))!.settled, true);
    });

    await check("refetchDueAt uses a delay saved in the document: noteInvoiceRefetch on a bundled order puts due_at = the event + the saved cancellation delay (5 h, the .env saying 2), the COD delay saved (9 h) for a cash-on-delivery one, and the default refund delay for a refund", async () => {
      const [On] = [11].map(order);
      await insertInvoice(wsNote, On);
      process.env.INVOICE_REFETCH_CANCEL_DELAY_H = "2";
      try {
        await put(wsNote, { refetchCancelDelayH: 5, refetchCodDelayH: 9 });
        const s = (await svc.getInvoiceSettings(String(wsNote))).effective;
        const at = ago(1 * H);
        assert.equal(refetch.refetchDueAt("cancelled", at, false, s).getTime(), at.getTime() + 5 * H);
        assert.equal(refetch.refetchDueAt("cancelled", at, false).getTime(), at.getTime() + 2 * H, "without settings: the .env, as before");
        assert.equal(await refetch.noteInvoiceRefetch({ userId: wsNote, orderId: On, unitId: "U1", moment: { reason: "cancelled", event_at: at, cod: false }, by: "verify" }), true);
        assert.equal(await refetch.noteInvoiceRefetch({ userId: String(wsNote), orderId: On, unitId: "U2", moment: { reason: "returned", event_at: at, cod: true }, by: "verify" }), true);
        assert.equal(await refetch.noteInvoiceRefetch({ userId: wsNote, orderId: On, unitId: "U1", moment: { reason: "refund", event_at: at, cod: false }, by: "verify" }), true);
        const row = await Invoice.collection.findOne({ userId: wsNote, order_id: On });
        const due = Object.fromEntries((row!.refetch as Array<{ unit_id: string; reason: string; due_at: Date }>).map((e) => [`${e.unit_id}:${e.reason}`, e.due_at.getTime() - at.getTime()]));
        assert.deepEqual(due, { "U1:cancelled": 5 * H, "U2:returned": 9 * H, "U1:refund": 192 * H });
      } finally {
        delete process.env.INVOICE_REFETCH_CANCEL_DELAY_H;
      }
    });

    console.log("THE ROUTES (a listener of this process, 127.0.0.1)");
    const { default: express } = await import("express");
    const { orderFetchRouter } = await import("../routes/orderFetch.js");
    const app = express();
    app.use(express.json());
    app.use("/api/orders", orderFetchRouter);
    server = await new Promise<import("node:http").Server>((resolve) => {
      const s = app.listen(0, "127.0.0.1", () => resolve(s));
    });
    const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}/api/orders`;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    type Json = Record<string, any>;
    const call = async (method: "GET" | "PUT", p: string, body?: unknown): Promise<{ status: number; body: Json }> => {
      const res = await fetch(`${base}${p}`, { method, headers: { "content-type": "application/json" }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(30_000) });
      return { status: res.status, body: (await res.json().catch(() => ({}))) as Json };
    };

    await check("GET /invoice-settings?userId= → { userId, settings, effective, sources, history, changedBy, changedAt, limits }; without userId: the workspace of TRADE_FLOW_USER_ID", async () => {
      assert.equal(await svc.seedInvoiceSettings(String(wsRoute)), true);
      const r = await call("GET", `/invoice-settings?userId=${wsRoute}`);
      assert.equal(r.status, 200);
      assert.deepEqual(Object.keys(r.body).sort(), ["changedAt", "changedBy", "effective", "history", "limits", "settings", "sources", "userId"]);
      assert.deepEqual(r.body.settings, { captureCancelledAfterShipping: true });
      assert.equal(r.body.effective.captureCancelledAfterShipping, true);
      assert.equal(r.body.sources.captureCancelledAfterShipping, "db");
      assert.equal(r.body.history.length, 1);
      assert.equal(typeof r.body.history[0].at, "string");
      const plain = await call("GET", "/invoice-settings");
      assert.equal(plain.body.userId, String(wsRoute));
      const bad = await call("GET", "/invoice-settings?userId=abc");
      assert.deepEqual([bad.status, bad.body.code], [400, "invalid_user"]);
    });

    await check("PUT /invoice-settings { userId, by, ...fields } → 200 with what GET answers; 400 { error, code } for an unknown field or a value out of range, nothing written", async () => {
      const ok = await call("PUT", "/invoice-settings", { userId: String(wsRoute), by: "verify-route", settleHours: 36, captureCancelledAfterShipping: false });
      assert.equal(ok.status, 200);
      assert.deepEqual(ok.body.settings, { captureCancelledAfterShipping: false, settleHours: 36 });
      assert.equal(ok.body.changedBy, "verify-route");
      assert.equal(ok.body.history[0].by, "verify-route");
      const unknown = await call("PUT", "/invoice-settings", { userId: String(wsRoute), by: "verify-route", settle_hours: 36 });
      assert.deepEqual([unknown.status, unknown.body.code], [400, "unknown_field"]);
      assert.match(unknown.body.error, /settle_hours/);
      const range = await call("PUT", "/invoice-settings", { userId: String(wsRoute), by: "verify-route", recaptureGiveUpDays: 400 });
      assert.deepEqual([range.status, range.body.code], [400, "invalid_value"]);
      const none = await call("PUT", "/invoice-settings", { userId: String(wsRoute), by: "verify-route" });
      assert.deepEqual([none.status, none.body.code], [400, "nothing_to_save"]);
      const noUser = await call("PUT", "/invoice-settings", { by: "verify-route", refetchCodDelayH: 100 });
      assert.equal(noUser.status, 200, "without userId: the workspace of TRADE_FLOW_USER_ID");
      const after = await call("GET", `/invoice-settings?userId=${wsRoute}`);
      assert.deepEqual(after.body.settings, { captureCancelledAfterShipping: false, settleHours: 36, refetchCodDelayH: 100 });
    });
  } finally {
    capture.useInvoiceS3ForChecks(null);
    if (server) await new Promise((resolve) => server!.close(resolve));
    let removed = 0;
    for (const ws of workspaces) {
      removed += (await InvoiceSettings.collection.deleteMany({ userId: ws })).deletedCount;
      removed += (await Invoice.collection.deleteMany({ userId: ws })).deletedCount;
      removed += (await Order.collection.deleteMany({ userId: ws })).deletedCount;
      removed += (await OrderUnitEvent.collection.deleteMany({ userId: ws })).deletedCount;
    }
    let left = 0;
    for (const ws of workspaces) {
      left += await InvoiceSettings.collection.countDocuments({ userId: ws });
      left += await Invoice.collection.countDocuments({ userId: ws });
      left += await Order.collection.countDocuments({ userId: ws });
      left += await OrderUnitEvent.collection.countDocuments({ userId: ws });
    }
    left += await Invoice.collection.countDocuments({ order_id: { $regex: `^ODINVSET${tag}` } });
    left += await Order.collection.countDocuments({ order_id: { $regex: `^ODINVSET${tag}` } });
    console.log(`\ncleanup: ${removed} invented row(s) deleted (invoice_settings, invoices, order_details, order_unit_events of ${workspaces.length} invented workspaces) — ${left} left; no S3 call was made`);
    if (left) failures.push("cleanup left rows behind");
    await mongoose.disconnect();
  }

  console.log(`\n${passed} passed, ${failures.length} failed (invoice settings)`);

  if (!process.argv.includes("--no-rerun")) {
    for (const script of ["invoiceRefetchCheck.ts", "invoiceOriginalsCheck.ts"]) {
      console.log(`\nRE-RUN src/scripts/${script}`);
      const r = spawnSync("npx", ["tsx", path.join("src/scripts", script)], { cwd: repoRoot, env: originalEnv, encoding: "utf8", timeout: 600_000 });
      const out = `${r.stdout || ""}${r.stderr || ""}`.trim().split("\n");
      const summary = out.filter((l) => /passed, \d+ failed|^cleanup:|FAIL/.test(l));
      for (const l of summary) console.log(`  ${l}`);
      if (r.status !== 0) failures.push(`${script} exited ${r.status}`);
    }
  }

  if (failures.length) {
    console.log("\nFAILED:");
    for (const f of failures) console.log(`  - ${f}`);
    process.exitCode = 1;
  } else console.log("\nall green");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
