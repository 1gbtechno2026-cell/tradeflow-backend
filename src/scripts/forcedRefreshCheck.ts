/**
 * The forced refresh's planning and its stop rules (data-quality sub-step 10 U15) — with a fake create and poll:
 * no Mongo, no queue, no worker, no Redis, no Flipkart.
 *
 *   · planBatches: freshest accounts first, the ones unread for more than 14 days last, ~200 a batch
 *   · planOrderBatches (the never-stored variant): ≤ 2,000 order ids a request, cut at account boundaries
 *   · pickCanary: read in the last day, ≤ 15 units, both statuses first
 *   · runBatches: the batch order; the stop after a batch with 6 % failed accounts, a "[demotion cap reached" or a
 *     "[circuit open" note, or a stopped request (and not at 5 %); the wait while a schedule request is open;
 *     --from-batch; Ctrl-C before the next batch
 *   · runBatches: no batch is created while the worker's heartbeat is gone; a batch that cannot be created stops the
 *     run (resume at it); a heartbeat lost while a batch runs is only reported
 *   · classifyAccounts: not logged in / looks logged out, several platformids rows per e-mail
 *   · neverStoredAccounts: only the orders of units still missing are sent
 *   · fieldChanges + canaryVerdict: the canary's counts and the OPS-11 gate
 *   · unit-backfill names / steps: backfilledName, stepsFix, cancelledLabelFrom, the old-value filter
 *   · cancelledLabelFrom against the mapper on every cancelled unit of the saved traces (read-only; skipped when no
 *     trace directory is given or found): the copy in unit-backfill must give the label mapApiUnitToOrder gives
 *   · maskEmail (the domain masked too) and the canary's alert scope (its orders and its accounts)
 *
 *   npm run test:forced-refresh [-- --traces <debug/order-traces>]   (or FORCED_REFRESH_TRACE_DIR)
 */
import assert from "node:assert/strict";
import {
  CANARY_FIELDS,
  canaryAlertScope,
  canaryVerdict,
  classifyAccounts,
  fieldChanges,
  maskEmail,
  neverStoredAccounts,
  orderAccounts,
  pickCanary,
  planBatches,
  planOrderBatches,
  runBatches,
  stopReason,
  type PolledRequest,
  type RefreshAccount,
  type RefreshBatch,
} from "./orderForcedRefresh.js";
import fs from "node:fs";
import { addName, backfilledName, cancelledLabelFrom, forEachTraceUnit, oldValue, stepsFix, type NameMap } from "./unitDataBackfill.js";

let passed = 0;
const failures: string[] = [];
async function check(name: string, fn: () => void | Promise<void>) {
  try {
    await fn();
    passed++;
    console.log(`  ok    ${name}`);
  } catch (err) {
    failures.push(name);
    console.log(`  FAIL  ${name}\n        ${err instanceof Error ? err.message.split("\n").slice(0, 12).join("\n        ") : err}`);
  }
}

const NOW = new Date("2026-11-26T12:00:00+05:30");
const DAY = 86_400_000;
const ago = (days: number) => new Date(NOW.getTime() - days * DAY);
const acct = (i: number, lastFetchDaysAgo: number | null, units = 12): RefreshAccount => ({
  email: `a${String(i).padStart(4, "0")}@verify.invalid`,
  units,
  orders: Math.ceil(units / 2),
  lastFetch: lastFetchDaysAgo === null ? null : ago(lastFetchDaysAgo),
});

/** A fake request store: create() makes a request whose accounts end as `script` says; poll() answers "running" twice, then the end. */
function fakeWorker(script: (batch: RefreshBatch) => PolledRequest) {
  const created: RefreshBatch[] = [];
  const polls = new Map<string, number>();
  const ends = new Map<string, PolledRequest>();
  return {
    created,
    create: async (b: RefreshBatch) => {
      created.push(b);
      const id = `req-${b.n}`;
      ends.set(id, script(b));
      return { id };
    },
    poll: async (id: string): Promise<PolledRequest | null> => {
      const n = (polls.get(id) || 0) + 1;
      polls.set(id, n);
      return n < 3 ? { status: "running", accounts: [] } : ends.get(id) || null;
    },
  };
}
const accountsOf = (batch: RefreshBatch, failed = 0, message = "") =>
  batch.emails.map((_, i) => ({ status: i < failed ? "failed" : "done", message: i === 0 ? message : "" }));

async function main() {
  console.log("planning");
  // 2,379 accounts like the measured scope: most read in the last days, 259 unread for more than 14 days, one never.
  const scope: RefreshAccount[] = [];
  for (let i = 0; i < 2379; i++) scope.push(acct(i, i < 2119 ? (i % 60) / 10 : i < 2378 ? 15 + (i % 40) : null));
  await check("planBatches: 2,379 accounts → 12 batches of 200 (the last 179); every account once; the 260 unread for more than 14 days (or never) are the last ones", () => {
    const batches = planBatches(scope, { perBatch: 200, now: NOW });
    assert.equal(batches.length, 12);
    assert.deepEqual(batches.map((b) => b.accounts), [...Array(11).fill(200), 179]);
    assert.deepEqual(batches.map((b) => b.n), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
    const order = batches.flatMap((b) => b.emails);
    assert.equal(new Set(order).size, 2379);
    const stale = new Set(scope.filter((a) => !a.lastFetch || NOW.getTime() - a.lastFetch.getTime() > 14 * DAY).map((a) => a.email));
    assert.equal(stale.size, 260);
    assert.ok(order.slice(-260).every((e) => stale.has(e)), "the stale accounts are the last 260");
    assert.equal(batches.reduce((s, b) => s + b.units, 0), 2379 * 12);
  });
  await check("orderAccounts: newest read first; among the stale ones newest first too; never read is last", () => {
    const list = orderAccounts([acct(1, 3), acct(2, 0.5), acct(3, 20), acct(4, null), acct(5, 16), acct(6, 1)], NOW).map((a) => a.email.slice(0, 5));
    assert.deepEqual(list, ["a0002", "a0006", "a0001", "a0005", "a0003", "a0004"]);
  });
  await check("planOrderBatches: order ids ≤ 2,000 a request, an account never split across two requests unless it alone holds more than 2,000", () => {
    const withOrders = (i: number, n: number): RefreshAccount => ({ ...acct(i, 1, n), orderIds: Array.from({ length: n }, (_, k) => `OD${i}-${k}`) });
    const batches = planOrderBatches([withOrders(1, 1500), withOrders(2, 400), withOrders(3, 300), withOrders(4, 4500), withOrders(5, 10)], { now: NOW });
    assert.ok(batches.every((b) => b.orderIds!.length <= 2000));
    const total = batches.reduce((s, b) => s + b.orderIds!.length, 0);
    assert.equal(total, 1500 + 400 + 300 + 4500 + 10);
    for (const email of ["a0001", "a0002", "a0003", "a0005"]) assert.equal(batches.filter((b) => b.emails.some((e) => e.startsWith(email))).length, 1, `${email} in one request`);
    assert.equal(batches.filter((b) => b.emails.some((e) => e.startsWith("a0004"))).length, 3, "4,500 orders of one account: three requests of its own");
  });
  await check("pickCanary: read in the last day, ≤ 15 units; accounts holding Delivered and Cancelled first", () => {
    const pool = [
      { ...acct(1, 0.2, 10), statuses: ["Delivered"] },
      { ...acct(2, 0.5, 9), statuses: ["Delivered", "Cancelled"] },
      { ...acct(3, 2, 5), statuses: ["Delivered", "Cancelled"] },
      { ...acct(4, 0.1, 40), statuses: ["Delivered", "Cancelled"] },
      { ...acct(5, 0.3, 15), statuses: ["Cancelled", "Delivered"] },
    ];
    assert.deepEqual(pickCanary(pool, 2, NOW).map((a) => a.email.slice(0, 5)), ["a0005", "a0002"]);
    assert.deepEqual(pickCanary(pool, 50, NOW).map((a) => a.email.slice(0, 5)), ["a0005", "a0002", "a0001"]);
  });

  console.log("\nrunning (fake create / poll)");
  const batches = planBatches(scope, { perBatch: 200, now: NOW });
  const quick = { sleep: async () => undefined, pollEveryMs: 0, waitEveryMs: 0 };
  await check("every batch runs in order, one request each, each polled to its end", async () => {
    const w = fakeWorker((b) => ({ status: "done", accounts: accountsOf(b, 2) }));
    const lines: string[] = [];
    const r = await runBatches(batches, { ...w, openSchedules: async () => 0, log: (l) => lines.push(l), ...quick });
    assert.equal(r.stopped, "");
    assert.deepEqual(w.created.map((b) => b.n), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
    assert.deepEqual(r.ran.map((o) => o.requestId), batches.map((b) => `req-${b.n}`));
    assert.equal(r.ran[0].failed, 2);
  });
  await check("a batch with 6 % failed accounts stops the run before the next batch (resume at the next); 5 % does not", async () => {
    const w = fakeWorker((b) => ({ status: "done", accounts: accountsOf(b, b.n === 3 ? 12 : b.n === 2 ? 10 : 0) }));
    const r = await runBatches(batches, { ...w, openSchedules: async () => 0, log: () => undefined, ...quick });
    assert.deepEqual(w.created.map((b) => b.n), [1, 2, 3], "10 of 200 (5 %) goes on, 12 of 200 (6 %) stops");
    assert.match(r.stopped, /batch 3: 12 of 200 account\(s\) failed \(6\.0 % > 5 %\)/);
    assert.equal(r.resumeAt, 4);
  });
  for (const note of ["[demotion cap reached — status left unchanged]", "[circuit open — 30 % of this update's IDs]"]) {
    await check(`an account message carrying "${note.slice(0, 22)}" stops the run before the next batch`, async () => {
      const w = fakeWorker((b) => ({ status: "done", accounts: accountsOf(b, 0, b.n === 2 ? `Session dead ${note}` : "") }));
      const r = await runBatches(batches, { ...w, openSchedules: async () => 0, log: () => undefined, ...quick });
      assert.deepEqual(w.created.map((b) => b.n), [1, 2]);
      assert.match(r.stopped, /an account message carries "\[(demotion cap reached|circuit open)"/);
      assert.equal(r.resumeAt, 3);
    });
  }
  await check("a stopped request (Stop in Batch requests) stops the run", async () => {
    const w = fakeWorker((b) => ({ status: b.n === 1 ? "cancelled" : "done", accounts: accountsOf(b) }));
    const r = await runBatches(batches, { ...w, openSchedules: async () => 0, log: () => undefined, ...quick });
    assert.deepEqual(w.created.map((b) => b.n), [1]);
    assert.match(r.stopped, /batch 1 was stopped/);
  });
  await check("while a schedule request is open the next batch waits (and says so once); it starts when the schedule ends", async () => {
    const w = fakeWorker((b) => ({ status: "done", accounts: accountsOf(b) }));
    let open = 3; // open for the first three looks before batch 1, then once more before batch 2
    let reopened = false;
    const looks: number[] = [];
    let sleeps = 0;
    const lines: string[] = [];
    const r = await runBatches(batches.slice(0, 2), {
      ...w,
      openSchedules: async () => {
        looks.push(w.created.length);
        if (w.created.length === 1 && !reopened) {
          reopened = true;
          open = 1;
        }
        return open > 0 ? (open--, 1) : 0;
      },
      log: (l) => lines.push(l),
      sleep: async () => void sleeps++,
      pollEveryMs: 0,
      waitEveryMs: 0,
    });
    assert.equal(r.stopped, "");
    assert.deepEqual(w.created.map((b) => b.n), [1, 2]);
    assert.equal(lines.filter((l) => /a schedule request is open/.test(l)).length, 2, "one line per wait, not per look");
    assert.ok(looks.filter((n) => n === 0).length === 4 && looks.filter((n) => n === 1).length === 2, `looks ${looks.join(",")}`);
    assert.ok(sleeps >= 4);
  });
  await check("--from-batch 10 starts at batch 10; Ctrl-C stops before the next batch and names it", async () => {
    const w = fakeWorker((b) => ({ status: "done", accounts: accountsOf(b) }));
    const r = await runBatches(batches, { ...w, openSchedules: async () => 0, log: () => undefined, ...quick }, { fromBatch: 10 });
    assert.deepEqual(w.created.map((b) => b.n), [10, 11, 12]);
    let stop = false;
    const w2 = fakeWorker((b) => {
      if (b.n === 2) stop = true;
      return { status: "done", accounts: accountsOf(b) };
    });
    const r2 = await runBatches(batches, { ...w2, openSchedules: async () => 0, log: () => undefined, stopRequested: () => stop, ...quick });
    assert.deepEqual(w2.created.map((b) => b.n), [1, 2]);
    assert.deepEqual([r2.stopped, r2.resumeAt], ["stopped by hand (Ctrl-C)", 3]);
    assert.equal(r.stopped, "");
  });
  await check("no update worker when a batch is due: the run stops before creating it and resumes at it", async () => {
    const w = fakeWorker((b) => ({ status: "done", accounts: accountsOf(b) }));
    let alive = true;
    const r = await runBatches(batches, {
      ...w,
      openSchedules: async () => 0,
      log: () => undefined,
      workerAlive: async () => {
        if (w.created.length === 2) alive = false;
        return alive;
      },
      ...quick,
    });
    assert.deepEqual(w.created.map((b) => b.n), [1, 2]);
    assert.match(r.stopped, /batch 3 not created: no update worker/);
    assert.equal(r.resumeAt, 3);
  });
  await check("a heartbeat lost while a batch runs is reported once and the batch is waited for (a restart resumes the request)", async () => {
    let polls = 0;
    const lines: string[] = [];
    const created: number[] = [];
    const r = await runBatches(batches.slice(0, 1), {
      create: async (b) => (created.push(b.n), { id: "req-1" }),
      poll: async () => (++polls < 6 ? { status: "running", accounts: [] } : { status: "done", accounts: accountsOf(batches[0]) }),
      openSchedules: async () => 0,
      log: (l) => lines.push(l),
      workerAlive: async () => !(created.length && polls >= 1 && polls <= 3),
      ...quick,
    });
    assert.equal(r.stopped, "");
    assert.equal(polls, 6);
    assert.equal(lines.filter((l) => /heartbeat is gone/.test(l)).length, 1);
    assert.equal(lines.filter((l) => /worker is back/.test(l)).length, 1);
  });
  await check("a batch that cannot be created (no account has a usable session) stops the run at that batch", async () => {
    const r = await runBatches(batches, {
      create: async (b) => {
        if (b.n === 2) throw new Error("No active order units to update. Fetch first, or paste a logged-in email + order id.");
        return { id: `req-${b.n}` };
      },
      poll: async () => ({ status: "done", accounts: accountsOf(batches[0]) }),
      openSchedules: async () => 0,
      log: () => undefined,
      ...quick,
    });
    assert.equal(r.ran.length, 1);
    assert.match(r.stopped, /batch 2 could not be created: No active order units/);
    assert.equal(r.resumeAt, 2);
  });
  await check("stopReason's table", () => {
    const o = (status: string, accounts: number, failed: number, notes: string[] = []) => ({ n: 4, requestId: "r", status, accounts, failed, notes });
    assert.equal(stopReason(o("done", 200, 10)), "");
    assert.match(stopReason(o("done", 200, 11)), /5\.5 %/);
    assert.match(stopReason(o("done", 50, 3)), /6\.0 %/);
    assert.match(stopReason(o("cancelled", 200, 0)), /stopped/);
    assert.match(stopReason(o("done", 200, 0, ['an account message carries "[circuit open"'])), /circuit open/);
    assert.equal(stopReason(o("done", 0, 0)), "", "a batch whose accounts all left (no accounts) is no failure");
  });

  console.log("\naccounts left out, never-stored ids");
  await check("classifyAccounts: no logged-in row with a session → not logged in; a 48 h task or a later expired check → looks logged out; the newest logged-in row decides", () => {
    const list = [1, 2, 3, 4, 5, 6, 7].map((i) => acct(i, 1));
    const e = (i: number) => list[i - 1].email;
    const rows = [
      { email: e(1), status: "logged_in", sessionSavedAt: ago(5), sessionCheck: { at: ago(1), outcome: "valid" } },
      { email: e(2), status: "not_logged_in", sessionSavedAt: ago(5) },
      { email: e(3), status: "logged_in", sessionSavedAt: null },
      { email: e(4), status: "logged_in", sessionSavedAt: ago(5), sessionCheck: { at: ago(1), outcome: "expired" } },
      { email: e(5), status: "logged_in", sessionSavedAt: ago(1), sessionCheck: { at: ago(2), outcome: "expired" } },
      // two rows: an old dead one and a newer logged-in one — logged in, and its own check is what counts
      { email: e(6), status: "not_logged_in", sessionSavedAt: ago(30), sessionCheck: { at: ago(20), outcome: "expired" } },
      { email: e(6).toUpperCase(), status: "logged_in", sessionSavedAt: ago(2), sessionCheck: { at: ago(1), outcome: "valid" } },
      { email: e(7), status: "logged_in", sessionSavedAt: ago(5) },
    ];
    const c = classifyAccounts(list, rows, new Set([e(7)]));
    assert.deepEqual(c.notLoggedIn.map((a) => a.email), [e(2), e(3)]);
    assert.deepEqual(c.looksOut.map((a) => a.email), [e(4), e(7)], "a check older than the session (5) does not count");
    assert.deepEqual(c.inScope.map((a) => a.email), [e(1), e(5), e(6)]);
    assert.deepEqual([c.why(e(4)), c.why(e(7))], ["check", "task"]);
  });
  await check("neverStoredAccounts: only the units not in order_details count, and only their orders are sent", () => {
    const rows = [
      { email: "A@verify.invalid", orderId: "OD1", unitId: "u1" },
      { email: "a@verify.invalid", orderId: "OD1", unitId: "u2" },
      { email: "a@verify.invalid", orderId: "OD2", unitId: "u3" },
      { email: "b@verify.invalid", orderId: "OD3", unitId: "u4" },
    ];
    const out = neverStoredAccounts(rows, new Set(["u3", "u4"]));
    assert.deepEqual(out.map((a) => [a.email, a.units, a.orders, a.orderIds]), [["a@verify.invalid", 2, 1, ["OD1"]]], "OD2 (all stored) and b (nothing missing) are not sent");
  });

  console.log("\nthe canary's counts");
  await check("fieldChanges counts changes per field and what went blank (imei / flag / OTP must stay 0)", () => {
    const before = new Map<string, Record<string, unknown>>([
      ["u1", { status_key: "Cancelled", product_name: "Phone (Black, 128 GB)", refund_status: null, imei: "IMEI-A", is_invoice_downloaded: true, delivery_otp: "123456" }],
      ["u2", { status_key: "Delivered", product_name: "Phone", refund_status: null, imei: null, is_invoice_downloaded: false, delivery_otp: null }],
    ]);
    const after = new Map<string, Record<string, unknown>>([
      ["u1", { status_key: "Cancelled", product_name: "Phone (Black, 128 GB) (8 GB RAM)", refund_status: "Completed", imei: "IMEI-A", is_invoice_downloaded: true, delivery_otp: "123456" }],
      ["u2", { status_key: "Delivered", product_name: "Phone (8 GB RAM)", refund_status: null, imei: null, is_invoice_downloaded: false, delivery_otp: null }],
    ]);
    const c = fieldChanges(before, after);
    assert.deepEqual(Object.keys(c), [...CANARY_FIELDS]);
    assert.deepEqual(c.product_name, { changed: 2, blanked: 0, filled: 0 });
    assert.deepEqual(c.refund_status, { changed: 1, blanked: 0, filled: 1 });
    for (const f of ["imei", "is_invoice_downloaded", "delivery_otp", "status_key"]) assert.deepEqual(c[f], { changed: 0, blanked: 0, filled: 0 }, f);
    after.set("u1", { ...after.get("u1")!, imei: null, delivery_otp: "" });
    const d = fieldChanges(before, after);
    assert.deepEqual([d.imei.blanked, d.delivery_otp.blanked], [1, 1]);
    // Sub-step 9's new refund / card fields are counted too (the canary checks that mapper on live data).
    for (const f of ["refund_kind", "card_source", "parent_card_number"]) assert.ok((CANARY_FIELDS as readonly string[]).includes(f), f);
    before.set("u3", { status_key: "Cancelled", refund_kind: null, card_source: "", parent_card_number: null });
    after.set("u3", { status_key: "Cancelled", refund_kind: "refund", card_source: "flipkart_refund", parent_card_number: null });
    const e = fieldChanges(before, after);
    assert.deepEqual([e.refund_kind, e.card_source, e.parent_card_number], [{ changed: 1, blanked: 0, filled: 1 }, { changed: 1, blanked: 0, filled: 1 }, { changed: 0, blanked: 0, filled: 0 }]);
  });
  await check("the canary's alerts are its orders' and its accounts' (an account.unreadable alert has an e-mail and no order id)", () => {
    const scope = canaryAlertScope(["OD1", ""], [" Ab@Verify.Invalid "]);
    assert.deepEqual(scope, [{ orderId: { $in: ["OD1"] } }, { email: { $in: ["ab@verify.invalid"] } }]);
    const matches = (alert: { orderId: string; email: string }) =>
      scope.some((c) => Object.entries(c).every(([k, v]) => ((v as { $in: string[] }).$in).includes((alert as Record<string, string>)[k])));
    assert.equal(matches({ orderId: "", email: "ab@verify.invalid" }), true, "account-level alert of a canary account");
    assert.equal(matches({ orderId: "OD1", email: "" }), true);
    assert.equal(matches({ orderId: "OD9", email: "zz@verify.invalid" }), false);
  });
  await check("maskEmail hides the domain as well as the name", () => {
    assert.equal(maskEmail("someone@examplebusiness.co.in"), "so***@e***.in");
    assert.equal(maskEmail("ab@verify.invalid"), "ab***@v***.invalid");
    assert.equal(maskEmail("x@host"), "x***@h***");
    assert.ok(!maskEmail("someone@examplebusiness.com").includes("examplebusiness"));
  });
  await check("canaryVerdict: fails on a blanked IMEI / flag / OTP or a new open alert; > 1 % status changes is a REVIEW, not a failure", () => {
    const zero = () => ({ changed: 0, blanked: 0, filled: 0 });
    const counts = Object.fromEntries(CANARY_FIELDS.map((f) => [f, zero()]));
    const ok = canaryVerdict({ ...counts, product_name: { changed: 300, blanked: 0, filled: 0 }, status_key: { changed: 4, blanked: 0, filled: 0 } }, { units: 450, cancelledUnits: 200, cancelledRefundFilled: 150, newOpenAlerts: 0 });
    assert.equal(ok.fail, false);
    assert.ok(ok.lines.some((l) => /^ok — status changed on 4 of 450/.test(l)));
    assert.ok(ok.lines.some((l) => /refunds filled on 150 of 200 Cancelled/.test(l)));
    const review = canaryVerdict({ ...counts, status_key: { changed: 9, blanked: 0, filled: 0 } }, { units: 450, cancelledUnits: 0, cancelledRefundFilled: 0, newOpenAlerts: 0 });
    assert.equal(review.fail, false);
    assert.ok(review.lines.some((l) => /^REVIEW — status changed on 9 of 450 unit\(s\) \(2\.0 %/.test(l)));
    assert.equal(canaryVerdict({ ...counts, imei: { changed: 1, blanked: 1, filled: 0 } }, { units: 450, cancelledUnits: 0, cancelledRefundFilled: 0, newOpenAlerts: 0 }).fail, true);
    assert.equal(canaryVerdict(counts, { units: 450, cancelledUnits: 0, cancelledRefundFilled: 0, newOpenAlerts: 1 }).fail, true);
  });

  console.log("\nunit-backfill names / steps (pure parts)");
  await check("names: the FSN's one subtitle for the stored bare title; two subtitles, another title, an already complete name or an unknown FSN → nothing", () => {
    const map: NameMap = new Map();
    addName(map, "FSN1", "Phone X (Black, 128 GB)", "8 GB RAM");
    addName(map, "FSN1", "Phone X (Black, 128 GB)", "8 GB RAM");
    addName(map, "FSN2", "Phone Y (Blue, 256 GB)", "8 GB RAM");
    addName(map, "FSN2", "Phone Y (Blue, 256 GB)", "12 GB RAM");
    addName(map, "FSN3", "Charger", "");
    assert.equal(backfilledName(map, "FSN1", "Phone X (Black, 128 GB)"), "Phone X (Black, 128 GB) (8 GB RAM)");
    assert.equal(backfilledName(map, "FSN1", "Phone X (Black, 128 GB) (8 GB RAM)"), null, "already complete");
    assert.equal(backfilledName(map, "FSN1", "Phone X"), null, "another stored name");
    assert.equal(backfilledName(map, "FSN2", "Phone Y (Blue, 256 GB)"), null, "two subtitles: left alone");
    assert.equal(backfilledName(map, "FSN3", "Charger"), null, "no subtitle");
    assert.equal(backfilledName(map, "FSN9", "Phone X (Black, 128 GB)"), null, "FSN not in the map");
  });
  const st = (status: string, date: string | null, steps: Array<[string, string]> = [], stage: "DONE" | "PENDING" = "DONE") => ({
    status,
    stage,
    date,
    detailed_steps: steps.map(([event, step_text]) => ({ event, step_text, progress: [{ date, remark: "" }] })),
  });
  await check("steps: the old 'stage · date' cell becomes the reference's form; a new-form cell is left alone", () => {
    const tracking = [st("Order Confirmed", "2026-10-20T10:00:00+05:30"), st("Delivered", "2026-10-27T21:55:34+05:30", [["deliveredStep", "Your item has been delivered"]])];
    const fix = stepsFix({ status_key: "Delivered", status_label: "Your item has been delivered", last_tracking_step: "Delivered \u00b7 2026-10-27T21:55:34+05:30", tracking });
    assert.ok(fix.last_tracking_step && !/\u00b7/.test(fix.last_tracking_step) && / \| Delivered$/.test(fix.last_tracking_step), fix.last_tracking_step);
    assert.equal(fix.status_label, undefined);
    assert.deepEqual(stepsFix({ status_key: "Delivered", last_tracking_step: fix.last_tracking_step, tracking }), {});
  });
  await check("steps: a Cancelled unit labelled with a stage title gets the cancel step's sentence; a real sentence is kept; a timeline missing its step lists does not throw", () => {
    const tracking = [
      st("Order Confirmed", "2026-10-20T10:00:00+05:30"),
      st("Cancelled", "2026-10-21T10:00:00+05:30", [["cancelledStep", "Your order was cancelled as per your request"]]),
      st("Refund", "2026-10-22T10:00:00+05:30", [["refundStep", "Refund completed"]]),
    ];
    assert.equal(stepsFix({ status_key: "Cancelled", status_label: "Refund", last_tracking_step: "", tracking }).status_label, "Your order was cancelled as per your request");
    assert.deepEqual(stepsFix({ status_key: "Cancelled", status_label: "Shipment cancelled by the seller", last_tracking_step: "", tracking }), {});
    assert.equal(cancelledLabelFrom([], ""), "Cancelled");
    const ragged = [{ status: "Cancelled", stage: "DONE", date: null }, { stage: "DONE", date: null, detailed_steps: [{ event: "x" }] }];
    assert.deepEqual(stepsFix({ status_key: "Cancelled", status_label: "Cancelled", last_tracking_step: "Cancelled \u00b7 2026", tracking: ragged }), {});
  });
  await check("the old-value filter: a missing field is matched by null (which matches a missing field), a value by itself", () => {
    assert.equal(oldValue(undefined), null);
    assert.equal(oldValue(null), null);
    assert.equal(oldValue(""), "");
    assert.equal(oldValue("Refund"), "Refund");
  });

  {
    const argAt = process.argv.indexOf("--traces");
    const dir = (argAt >= 0 ? process.argv[argAt + 1] : "") || process.env.FORCED_REFRESH_TRACE_DIR || process.env.UNIT_SAVE_TRACE_DIR || "debug/order-traces";
    if (!fs.existsSync(dir)) console.log(`\nskipped: cancelledLabelFrom against the mapper — no trace directory (${dir}); pass --traces <dir>`);
    else
      await check("cancelledLabelFrom (unit-backfill steps) gives the mapper's own label on every cancelled unit of the saved traces", () => {
        let cancelled = 0;
        const differ: string[] = [];
        const answers = forEachTraceUnit(dir, (m) => {
          if (!/^cancel/i.test(String(m.status_key || ""))) return;
          cancelled++;
          // Given the mapper's label as the stored one: its rule must hand that label back (a cancel step's sentence,
          // the undoing stage's sentence, a non-title status text, or "Cancelled") — any drift between the two copies shows.
          if (cancelledLabelFrom(m.tracking, m.status_label) !== m.status_label) differ.push(`${m.order_id}/${m.unit_id}`);
        });
        console.log(`        ${answers} answer(s), ${cancelled} cancelled unit(s) compared, ${differ.length} differ`);
        assert.ok(cancelled > 0, "the traces hold cancelled units");
        assert.deepEqual(differ.slice(0, 5), [], `${differ.length} unit(s) whose label the copy rebuilds differently`);
      });
  }

  console.log(`\n${passed} passed, ${failures.length} failed${failures.length ? ` — ${failures.join("; ")}` : ""}`);
  process.exitCode = failures.length ? 1 : 0;
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
