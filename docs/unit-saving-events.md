# Unit saving, events and watches (data-quality sub-step 10)

Plan: the dashboard repo's `docs/order-data-quality-plan.md`, section 10 (changes U6–U10, U12, U15, the integration's
X5 and X6) and finding T10 of the 100-order test. Built on sub-step 9 (15d1d2f) and sub-step 12 (540d409, which waits
for the Lambda deploy D1); goes live with them at the worker restart **R3**.

## What changed

| Id | Where | What |
|---|---|---|
| U6 | `orderLinks.ts` `cardFieldsFor`, `models/Order.ts`, `routes/jobs.ts` | **Created By.** The checkout job's `batchId` → the batch's `createdBy` (cached per batch 10 min) → `created_by` on the unit, only when the batch has one: from the dashboard's next API reload on, past orders stay blank. `routes/jobs.ts` reads `x-requested-by` through `requestedByHeader` (percent-decoded, trimmed, ≤ 120 characters). Both save paths write it (the API path through `cardColumnsFor`, the page path through its `cardFieldsFor` spread). |
| U7 | `orderUnitEvents.ts` | `otp_new` on every code change (already in sub-step 1's tree); tested by the plan's table. |
| U8 | `orderUpdateQueue.ts` `saveMissingSiblings` | After an API read, the order's units that `order_details` does not hold are read and saved — once per order per job, the request's own units of the account left out, one `Order.find` per order, a failure logged and counted, never thrown. The account's message gains `+N sibling unit(s) saved`. |
| U9 | `orderFetch.ts` `discoverUnit`, `requeueSkippedOld` | A `skipped_old` index row listed again inside the run's window and not stored goes back to `pending` (counted new); the enrich pass of the same run reads it. A row the *enrich* step skipped (`last_error` "placed before …") is judged on the placed date it keeps (`order_date`), not on the list card's date — a page list card shows a status date ("Cancelled on Oct 12"), and re-queuing on that would read and skip the unit again on every fetch. |
| U10 | `orderUnitEvents.ts`, `models/OrderUnitEvent.ts`, `orderUpdateQueue.ts` | **Quiet mode**: a request with `params.quiet_events === true` (the forced refresh) inserts its events already `processed` (`after.backfill: true`) — the dashboard's drain claims `processed: { $ne: true }` only, so no alert; baselines and address history are still written; no late-document moment. **`refund_completed`** event (`after: { amount, date, kind }`), enum extended. |
| X6 | `orderUnitEvents.ts` `lateDocumentMoments` | **The one detection** of the two late-document moments ("an invoiced unit turned Cancelled / Returned", "refund completed"). `recordUnitChanges` returns them as `changes.lateDocuments`; `invoiceRefetch.ts` (sub-step 12) only notes them (`noteLateDocuments`); its first-store path runs the same function. `noteLateDocumentsAfterRead` (a second read per unit with only the statuses) is removed. |
| U12 + X5 | `orderLifecycle.ts` `refundWatchFilter`, `refundNotCompletedClause`, `orderUpdate.ts` `withRefundWatch`, `runUpdateSweep("active")` | **One refund watch**, ORed into the 6-hour active sweep's selection (`params.refund_watch: true` on its request): *recent* — Cancelled ≤ 7 days, no completed refund, not COD; *open* — refund Processed / Accepted / Pending, cancelled ≤ 30 days; *bundled* — invoiced unit (after shipping, or `invoice_eligible`) of an order with a stored bundle, cancelled ≤ 30 days, no completed refund, not COD. "No completed refund" is the one detection's rule in Mongo form (`refundNotCompletedClause`) in both clauses. The out-for-delivery recheck does not take it. |
| T10 | `orderUnitEvents.ts` `updateSourceLabel`, event fields `source` / `requestSource` / `sourceLabel` | Every event the update worker writes says it came from an update and which request kind; the label the dashboard should show is on the event. The dashboard line below uses it. |
| U15 | `scripts/orderForcedRefresh.ts`, `scripts/unitDataBackfill.ts` (`names`, `steps`) | The forced refresh (OPS-11/12/18, dry run by default) and the two stored-data backfills of OPS-13. |

### Decisions taken while building (within the plan)

- **"Refund completed" = the refund status when the unit carries one, else a DONE Refund stage.** Sub-step 12 used
  "status /complet/ OR a DONE Refund stage"; the saved traces show 15 cancelled units with a DONE Refund stage while
  the refund card still reads "Processed" (198 Completed + DONE, 2 Completed without a stage, 1 DONE without a card).
  So the status wins when there is one, and the moment is a *transition*: a unit stored without a refund status
  counts as not completed, so its first refund-card read (after R3) is its moment once. The page path's bare marker
  `refund_status: "refund"` ("the page shows some refund row", orderFetch.ts) is **no status**: the DONE stage decides
  (`refundStatusOf`) — otherwise a unit read through the Chrome fallback could never count as refunded (47 units carry
  the marker today, none with a DONE Refund stage, so this is latent).
- **One rule everywhere.** The `refund_completed` event, the "refund" late-document moment and the refund watch's
  "no completed refund" all follow that rule: the event and the moment share one condition (`refundCompletedBetween`)
  — a unit read with no "before" and its refund already complete raises the event *and* returns the moment (spec U10:
  `before?.refund_status !== "Completed"`); the watch's *recent* and *bundled* clauses use the same rule in Mongo
  (`refundNotCompletedClause`). Applied to *recent*, it leaves out 304 units the detection already counts as refunded
  (a DONE Refund stage, no status): **498 units a sweep instead of 802**, inside the plan's ~730.
- **The `bundled` clause leaves cash-on-delivery units out — OWNER DECISION NEEDED (a narrowing of the plan).** Plan
  §10 U12 / SO-5 say "invoiced units of bundled orders cancelled ≤ 30 days ago with no refund yet", with no COD
  condition. A COD order has no refund to wait for; its one late look (the cancellation's, +8 days) is noted when it
  turns Cancelled. With COD in, those units would be re-read every 6 hours for 30 days for nothing (19 → 13 units
  today). To follow the plan literally, drop `...notCod` from the *bundled* clause in `orderLifecycle.ts` (+6 units).
- **Siblings get no events.** A sibling saved by U8 is new to `order_details` (no "before"), so it would only raise
  an address baseline; that is left to its next read, like any newly fetched unit.
- **The forced refresh fails closed.** `workerAlive()` answers "alive" when Redis cannot be reached (so a request is
  never shown stalled by mistake); the refresh's gate PINGs Redis first and counts an unreachable Redis as "no
  worker". **And the worker must run this code:** the update worker stamps `tradeflow:worker-caps:order-update`
  (`{"caps":["quiet_events"],…}`, every 20 s, 75 s TTL — `orderUpdateQueue.ts` `startUpdateCapabilityStamp`, started
  with the update reaper) and `--go` refuses a worker without `quiet_events`: an older worker would ignore the
  request's `quiet_events` and the dashboard would raise the alerts quiet mode exists to prevent (12,302 units without
  an address baseline). The dry run prints the gate; today it reads "it honours quiet_events: NO" (the live worker is
  older), which is right until R3. It also asks before each batch: no batch is created while the heartbeat is gone (the run stops, resume with
  `--from-batch`); a heartbeat lost while a batch runs is only reported, because the owner's restart resumes the
  request. A batch that cannot be created (none of its accounts has a usable session) stops the run at that batch.
- **"Logged in" is the worker's own rule**: an account counts as logged in when any of its `platformids` rows is
  `logged_in` with a saved session, and the newest such row's session check is the one read (an e-mail may have
  several rows). The session cookies are never read.
- **The never-stored variant (OPS-18) sends only the orders of units still missing** (an order whose index units are
  all stored is not read again) and orders its accounts by their newest read of any stored unit, like the main run.
- **The canary's gate (OPS-11)** waits one dashboard alert poll (90 s) after its batch, then counts new open alerts on
  the canary's own orders **and its accounts** since the run started (not the workspace's total; an account-level
  alert such as `account.unreadable` carries the e-mail and no order id — `canaryAlertScope`), prints changed / went blank / filled per
  field, refunds filled on Cancelled units, the status-change share (REVIEW above ~1 %), and exits 3 on a blanked
  IMEI / invoice flag / OTP or a new open alert.
- **unit-backfill writes filter on the old value with `null` for a missing field** (Mongo's `{f: ""}` does not match
  a missing field, so a write would have found nothing), and `steps` reads the stored timeline normalised (a stage
  without a step list cannot throw). Both are latent today: 0 of the 34,891 `steps` units and 0 of the `names` units
  miss the field, and 0 stages miss their step list.
- **The canary counts sub-step 9's new fields too**: `refund_kind`, `card_source`, `parent_card_number` beside the
  refund, card, IMEI, flag, OTP, name, status and delivery-date fields.
- **The dry run masks the e-mail's domain as well** (`so***@e***.in`): a domain is often a business name.
  `--show-emails` prints them in full, for the owner's own terminal only.
- **unit-backfill `steps` keeps its copy of the mapper's cancelled-label rule** (`cancelledLabelFrom`; the mapper is
  sub-step 9's file), tied to it by a check: `test:forced-refresh -- --traces <dir>` re-maps every saved answer and
  asserts the copy gives the mapper's own label on every cancelled unit (14,302 answers, 34,332 cancelled units, 0
  differ).
- **The "9 logged-out accounts" of the plan's gate are 4 accounts holding units of the scope, plus accounts with
  nothing to refresh.** The plan's 9 counted every account with a not_logged_in update / fetch task in 48 h (8) plus
  the not-logged-in one; only the 29 units were counted in scope — and the dry run's 29 units match. Today 7 accounts
  have such a task, 4 of them hold Delivered / Cancelled units (1 of those is also the not-logged-in one); the other 3
  hold none and have nothing to refresh. The dry run now prints them as one count line. Gate value proposed for the
  owner: **4 accounts / 29 units listed apart (+3 with nothing to refresh)**.

## The dashboard changes (applied by the owner / orchestrator — this build does not edit the dashboard)

**T10 — the alert's source follows the request** (`backend/src/services/orderAlerts.ts`, in `handleEvent`, the
`order.address_mismatch` raise, today line 227):

```ts
// old
        source: ev.type === "address_seen" ? "Fetch account details" : "Update order details",
// new
        source: String((ev as EventDoc & { sourceLabel?: string }).sourceLabel || "Update order details"),
```

Every unit event is written by the update worker (fetch writes none), so an event without the field — older than
this build — is an update's too. Optional, same idea, for the `order.address_change` raise (line 210):
`source: "Update order details",` → `source: String((ev as EventDoc & { sourceLabel?: string }).sourceLabel || "Update order details"),`.
`refund_completed` needs no dashboard change: `handleEvent` ignores types it does not know (it is claimed, no alert).

**U6 — `createJob` sends `x-requested-by`** (live at the dashboard API reload):

`backend/src/services/jobsClient.ts`

```ts
// JobsCallOptions (line 20) gains
  /** The signed-in user's name, sent as x-requested-by (Trade Flow keeps it as the batch's createdBy → Created By). */
  requestedBy?: string;

// jobsRequest, the fetch headers (line 84–87)
      headers: {
        "Content-Type": "application/json",
        "x-api-key": config.jobsApiKey,
        ...(opts.requestedBy ? { "x-requested-by": encodeURIComponent(opts.requestedBy) } : {}),
      },

// createJob (line 121)
export function createJob(payload: Record<string, unknown>, requestedBy = "") {
  return jobsRequest<Record<string, unknown>>("POST", "/api/jobs", payload, { requestedBy });
}
```

`backend/src/routes/jobs.ts`, `POST /` (line 64):

```ts
    const result = await createJob(
      { ...(req.body || {}), use_ordering_phone: useOrderingPhone, ordering_phones: orderingPhones },
      req.user?.username || ""
    );
```

The value is percent-encoded because a header carries Latin-1 only; Trade Flow decodes it (`requestedByHeader`).

The spec's dashboard check, to add with that change — `backend/test/jobsRequestedBy.test.mts` (a throwaway listener on
a random port as `JOBS_API_URL`, set before `config` is imported; dotenv does not override it; nothing reaches :4100),
run with `npx tsx --test test/jobsRequestedBy.test.mts` from `backend/`:

```ts
import { test } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import type { AddressInfo } from "node:net";

test("createJob sends the signed-in user's name as x-requested-by (percent-encoded), and none without one", async () => {
  const seen: Array<string | undefined> = [];
  const server = http.createServer((req, res) => {
    seen.push(req.headers["x-requested-by"] as string | undefined);
    req.resume();
    req.on("end", () => {
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true }));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    process.env.JOBS_API_URL = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    const { createJob } = await import("../src/services/jobsClient.js");
    await createJob({ dry_run: true }, "alice");
    await createJob({ dry_run: true }, "Ölaf R");
    await createJob({ dry_run: true });
    assert.deepEqual(seen, ["alice", "%C3%96laf%20R", undefined]);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
```

## Checks

| Command | What | Result (2026-11-26) |
|---|---|---|
| `npm run typecheck` | the project | clean |
| `UNIT_SAVE_TRACE_DIR=<main tree>/debug/order-traces npm run test:unit-save` | sub-step 1's cases plus: U6 (invented batch + job → `created_by` "tester", none without a creator, the header's reading), U7 table, U10/X6 (Processed → Completed one event + one moment, the status wins over the stage, a first read with the refund complete gives the event and the moment, the page's bare `refund` marker + DONE stage is completed, quiet = processed + backfill + no moment, T10 labels, `eventOptionsFor` per request and its use at both save calls, the worker's capability stamp, the model takes the type and fields), the quiet insert against the **running dashboard API** (a control event of the invented workspace drained within 90 s, 0 alerts for the quiet unit), U8 through a stubbed API context serving a saved 7-unit answer (scrubbed, ids invented: 6 saved, then 0, a 500 counted not thrown, skip honoured), U9 through `discoverUnit` on invented index rows (a "placed before" row is not re-queued on a status date) + the invariant (0 unaccounted), U12/X5 through `resolveTargets` (an invented logged-in row; *recent* units refunded by a DONE stage, with no status or the bare marker, left out) | 47 passed, 0 rows left (units, index rows, events, the control's alert, jobs, batches, the platformids row) |
| `npm run test:forced-refresh [-- --traces <dir>]` | planning and stops with a fake create / poll (batch order, 6 % / 5 %, cap and circuit notes, Stop, the wait while a schedule is open, `--from-batch`, Ctrl-C, no worker heartbeat when a batch is due, a heartbeat lost mid-batch, a batch that cannot be created); the left-out classification (several rows per e-mail); never-stored order ids; the canary's counts and gate; unit-backfill `names` / `steps` pure parts and the old-value filter; the canary's new fields and alert scope; the e-mail mask; with `--traces`, `cancelledLabelFrom` against the mapper on every cancelled trace unit | 26 passed with `-- --traces <main tree>/debug/order-traces` (25 without: the trace case is skipped) |
| `npm run test:invoice-refetch` | sub-step 12's check, its update-worker case now through the one detection (+ a quiet read noting nothing) | 17 passed, 0 left |

Run unchanged and passing: `test:refund-card` 9, `test:start-requests` 46, `test:request-log` 51,
`test:dead-session-flows` 16, `test:invoice-originals` 22, `test:request-actions` 38, `test:schedule-sweep` 18,
`test:fetch-schedule` 16, `test:invoice-batch` 18, `test:unit-mapper` 41.

## Measured (read-only, 2026-11-26 ~05:00 IST)

- `npm run orders:refund-watch` (06:00 IST, after the one-rule fix): recent 485 · open 0 (no unit carries a refund
  status yet — sub-step 9 is not live) · bundled 13 → **498 units a sweep on 53 accounts**, all logged in, none
  already in the active selection (768 units), inside the plan's ~730. Context: 1,405 cancelled in 7 days, 789 of them
  not COD (the plan's 720 on 11-25); 304 of those 789 are already refunded by their DONE stage. "open" grows once
  sub-step 9's refund status is live; the total falls as refunds complete.
- `npm run orders:forced-refresh` (dry run): Delivered + Cancelled 28,811 units · 12,121 orders · 2,380 accounts →
  **12 batches** (11 × 200 accounts + 176; 1,506–3,923 units, ~2,400 on average), 314 accounts unread for more than
  14 days last. Left out and listed apart: **4 accounts / 29 units** — 1 not logged in (2 units) and 3 that look
  logged out (27 units: a not_logged_in task in 48 h, one also by its session check), plus 3 accounts with such a task
  that hold no unit of the scope. The plan's 9 counted every account with a task (see the decisions above); its 29
  units match. Gates: the update worker is alive here but does NOT yet honour quiet_events (older code until R3, so
  `--go` refuses), 0 open schedule requests.
- `orders:forced-refresh -- --canary 50` (dry run): 50 accounts read in the last day with ≤ 15 units → **591 units**
  (the spec's ~450–500).
- `orders:forced-refresh -- --never-stored-since 2026-01-01` (dry run): **30,207 units · 15,727 orders · 2,196
  accounts** (the plan's 30,192 · 15,712 · 2,196) → 8 requests of ≤ 2,000 order ids (1,755–2,000), 260 accounts unread
  for more than 14 days last; left apart 1 not logged in (13 units) and 2 that look logged out (72 units).
- `orders:unit-backfill -- --part names` (dry run, traces of the main tree): FSN map of 142 FSNs from 79,212 traced
  units (0 from refreshed units yet); **4,632 units** gain the subtitle (Delivered 2,190, Cancelled 1,064, Failed
  874, Shipped 364, …); 28,474 units carry no FSN (not matched). After the refresh, the Failed units' 874 match the
  plan's number.
- `--part steps` (dry run): 34,891 units in the old "stage · date" form, 3,214 cancelled labels that are a stage title
  (the plan's 3,173 "Refund"). After OPS-12 only the units the refresh did not reach remain.

## Live

At **R3** (the worker restart after D1, with sub-steps 9 and 12): the update worker writes the new events, quiet
runs, siblings, the noting through the one detection and the refund watch on the next 6-hour tick. U9 is in the
fetch (the Trade Flow API process): it goes live at the API reload that follows the merge. The dashboard's two
changes go live at its API reload. Then OPS-10 … OPS-13 as the plan's runbook says; `orders:forced-refresh --go`
refuses without a live update worker on this Mac.
