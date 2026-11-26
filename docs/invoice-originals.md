# Invoice downloads: Flipkart's files kept, one store helper

Data-quality sub-step 12 (dashboard `docs/order-data-quality-plan.md`): SO-1, T1, T2, P9, X3 and T4's part
labels (the store), SO-5 late re-downloads, SO-6 the cancelled-after-shipping capture (built switched off) with
phase 1's failure memory, and SO-4 the originals backfill script.

## What a download does now

Every path that downloads an order's invoices goes through ONE helper,
`services/invoiceOriginals.ts` `storeInvoiceDownload`:

| path (`invoice_downloads.path`) | caller | `skipIfUnchanged` |
|---|---|---|
| `batch` | Start a request → Invoices, the Delivered check, OPS-15 (`invoiceRequests.ts`) | no — a batch means "extract again" |
| `by-order-id` | Download Order Invoices modal (`invoiceOnDemand.ts`, inline in the API) | no |
| `sweep-capture` | 3-hourly sweep, phase 1 | no (new bundles) |
| `sweep-recapture` | 3-hourly sweep, phase 2 | **yes** |
| `sweep-refetch` | the refetch phase (SO-5) | **yes** |
| `fetch` | the fetch's after-enrichment capture (`captureInvoicesForAccount`) | no |

In this order:

1. **Flipkart's answer, as it came.** `downloadInvoiceAnswerViaApi` / `captureInvoiceAnswer` return the answer
   (`kind` zip | pdf, its size and sha256) and every PDF under Flipkart's own name (`file_7.pdf`; `<order>.pdf`
   for a single-PDF answer), bytes untouched, in Flipkart's file order — **numeric** (P9: `file_7` before
   `file_11`; the text sort put it after `file_32`). Safe because the deployed pipeline no longer reads position
   (plan conflict 6); a re-downloaded bundle's merged page order and Compile's `_n` numbering follow Flipkart's
   order.
2. **Originals** — each file at `orig/<userId>/<orderId>/<sha256>.pdf`: `PutObject` with `IfNoneMatch: "*"` and
   `ChecksumSHA256`, 4 at a time. 412 = already stored. A store that refuses the condition (409/501/400) gets a
   HEAD, then a plain PUT only when absent. Content addressing makes "untouched" and two-Mac safety structural:
   a file is written once; the other Mac writing the same bytes is a no-op. **Best-effort:** a PUT S3 refuses or
   fails (403 AccessDenied while the Writer policy has no `orig/*` grant — it comes with D1 — or any S3 error)
   never fails the download. The file is recorded `stored: "failed"`, the record gets `originals_error` (the first
   error, `S3 put original …`), a `[invoice-originals] … not kept` warning is logged, the merged PDF is stored
   **without** `download-id` (the Lambda then splits the merged copy, as before sub-step 5), and the row's
   `stored_download_id` is cleared, so the backfill (SO-4) takes the order again. Before sub-step 12 the download
   paths needed `raw/*` rights only; they still do.
3. **`invoice_downloads`** — one document per download (`models/InvoiceDownload.ts`, `_id` = the download id, a
   24-hex string): userId, order_id, platformAccountId, at, via api|page, path, request_id, host, system_id,
   `answer {kind, bytes, sha256}`, `files [{name, key, sha256, bytes, pages, type, invoice_number, merge_index,
   page_from, stored new|existing|failed}]`, `merged {key, bytes, pages, etag}`, `unchanged`, `originals_error`
   (only when an original was not kept). Index
   `{userId:1, order_id:1, at:-1}` (a new collection; nothing new on `order_details`). This is Flipkart's file
   list — it never goes into `invoices.parts` again.
4. **The merged raw PDF** — unchanged merge (`mergeInvoiceBundle`: product → Protect Promise → card fee →
   freight → other, numeric within each), PUT to `raw/<userId>/<orderId>.pdf` with S3 metadata
   `download-id` = the document above. The dashboard's extract Lambda (sub-step 5) reads it from its HEAD and
   builds the archive from the originals. Skipped when `skipIfUnchanged` and the files' sha256 set equals the
   processed row's `originals` (no Lambda run; the record says `unchanged: true`).
5. **`registerInvoice`** (`invoiceCapture.ts`):
   - **T1** — `etag`, `size`, `parts` go to `$setOnInsert` only. On an existing row the Lambda's claim owns the
     etag and `describeParts` owns `parts`. Before, the new etag was `$set` here before the S3 event's claim
     ("processed and etag differs") could see it: a re-downloaded processed bundle was never re-extracted (70 of
     70 in the 100-order test) and `parts` flipped to the per-file list (73 rows).
   - `$set stored_download_id` (not on an unchanged download; `$unset` when this download's originals were not all
     kept), `last_download_at`; `$inc download_count`.
   - The S3 event's claim can create the row first (its upsert writes `parts: []`): the worker's list then fills
     an **empty** list only; the pipeline stays the owner of every list it wrote.
   - **T2** — a row with `captured_at: null` gets `captured_at = now` and `settled` by the delivery rule
     (`settledByDelivery`: every unit finished — Delivered/Cancelled/Returned/Refunded/Failed — and the last
     delivery, or a cancelled unit's cancellation, older than `INVOICE_SETTLE_HOURS`; a delivered unit without its
     time keeps the bundle open). The 1,308 rows never re-captured get this at their next download.
6. **Units** (`stampInvoiceUnits`) — key, `is_invoice_downloaded`, `invoice_downloaded_at`, and `invoice_size`
   (not on an unchanged download) on every unit row given. **X3** — `invoice_parts` only on a unit that has none:
   the pipeline (sub-step 4) is the one writer of the order's part list; an unchanged download (no Lambda run)
   must not put the worker's list back.

The sweep's phase 1 no longer forces `settled: false` (registerInvoice decides); phase 2 settles by the same
rule after its re-download (`settleRecaptured`): an order still on its way gets `captured_at = now` and is looked
at again one window later, and is settled anyway `INVOICE_RECAPTURE_GIVE_UP_DAYS` (default 30) after its row was
made. **This goes beyond the plan's wording** (phase 2 there re-downloads once, then settles): it serves T2's intent
(a bundle fetched in transit gets its look after delivery) — read-only today, 177 of 5,075 bundles are not settled
by the rule, 13 with a unit not finished. Until D1 writes `originals`, `skipIfUnchanged` never matches, so each
repeat re-uploads and the Lambda re-extracts. The owner may shorten or end it with the setting.

**Errors of our own.** Whatever `storeInvoiceDownload` throws (the merge, the record, the raw PUT, the row) is
marked as ours (`isInvoiceStoreError`): the batch files it `internal_error` — never `platform_unavailable`, even
when it carries `ECONNRESET` (classifyPlatformError alone would blame Flipkart) — and the modal / sweep result
carries `ours: true`, which the sweep's failure memory does not count.

**T4's part labels** — `classifyInvoiceText` labels the sellers' goods layout (`HSN: <goods code>`, no FSN) and
the grocery "Tax Invoice/Bill of Supply … HSN (Tax%)" as `product`, after the fee and freight rules, as the
dashboard parser does since sub-step 3.

**pdf-parse reliability** — `classifyInvoicePdf` gives pdf.js 1.10 its own copy of the bytes and tries a failed
read up to three times: measured on the offline fixtures, a read right after another document sometimes failed
("bad XRef entry") on a file read fine a moment later, which labelled a product invoice `other` and moved it in
the merged order. Since the refetch part: an empty text counts as a failed read too (pdf.js returned "" for a product
invoice it read in full the next try), and every pdf-parse read in a process goes one at a time (six overlapping
reads failed 2 of 6 with "Command token too long"; a merge reads all files of an answer at once and a batch stores
6–12 orders at once). Found when a new import shifted the module load order and the check failed 2 of 19 on every
run; with both, 19 of 19 on every run. Each read is bounded (`PDF_PARSE_TIMEOUT_MS`, 20 s): a read that never
settles is a failed read and the queue moves on, so one stuck file cannot stall every later classification in the
process. An image-only PDF (no text) is read three times before it is `other`.

## Late re-downloads (SO-5, owner decision 4)

Flipkart issues more documents after an order is invoiced: product, Protect Promise and card-fee credit notes
dated the day of a cancellation, and a freight credit note (with a new freight invoice) 3–14 days after it
(median 8), 4–8 days after the refund. `services/invoiceRefetch.ts`:

- **Noting.** An entry `{unit_id, reason cancelled|returned|refund, event_at, due_at = event_at + delay, noted_at,
  by, cod, attempts, last_error, done_at, result}` is pushed onto the order's own `invoices` row (`refetch[]`).
  The filter refuses a second `(unit_id, reason)`, so the push is atomic in the shared Mongo: both Macs, a repeat
  read or a duplicate run note a moment once. An order without a bundle gets nothing.
- **Which moments.** Only a unit that was invoiced: Returned, or Cancelled with a pre-cancellation status of
  shipped / out for delivery / delivery attempted, unsuccessful, rescheduled / delivered, or Flipkart's
  `invoice_eligible`. *cancelled* / *returned* when the unit turns Cancelled / Returned (not at "Cancellation
  requested"), at `cancelled_date`; *refund* when the refund completed — `refund_status` /complet/ (sub-step 9's
  mapper) when the unit carries a refund status, else a DONE Refund stage on the tracking (what is stored today), at
  the stage's date (a date ahead of now counts as now; Flipkart dates some DONE stages ahead). Since sub-step 10 the
  status wins over the stage (15 traced cancelled units show a DONE Refund stage while the card still reads
  "Processed"), and the refund counts as a moment only when it was not completed before the read (a unit stored
  without a refund status counts as not completed, so its first refund-card read is the moment). The page path's bare
  marker `refund_status: "refund"` counts as no status, so its DONE stage decides (`refundStatusOf`).
- **Who detects, who notes (sub-step 10, X6).** ONE detection, `lateDocumentMoments` in `orderUnitEvents.ts`
  (re-exported here as `refetchEventsOf`). The update worker's `recordUnitChanges` runs it on the before / after
  snapshots of every unit it reads and hands the moments over as `changes.lateDocuments` (none in a quiet run — the
  forced refresh); `orderUpdateQueue.ts` (phase 1 and the page phase, next to `noteDeliveredOrder`) passes them to
  `noteLateDocuments`, which only notes (`noteInvoiceRefetch`; never fails the unit; costs nothing when there is no
  moment). The store helper at a bundle's first capture (`registerInvoice` reports `captured`) runs the same
  detection on the order's ended units (`noteLateDocumentsForOrder`), for units that ended before the bundle
  existed. The refund watch (X5, `orderLifecycle.ts refundWatchFilter`) makes the 6-hourly sweep read cancelled units
  at all; `noteLateDocumentsAfterRead` (a second read of the unit with only its statuses) is gone.
- **The delays** are owner question 4, still open; the plan's recommendation is the default, each an `.env`
  setting (`.env.example`): `INVOICE_REFETCH_CANCEL_DELAY_H=24` (the cancellation / return look, 1 day),
  `INVOICE_REFETCH_REFUND_DELAY_H=192` (the refund look, 8 days), `INVOICE_REFETCH_COD_DELAY_H=192` (cash on
  delivery: no refund follows, the cancellation look waits 8 days). Whoever notes computes `due_at`: set the same
  values on both Macs. **Cash on delivery** is read from the order API's `cash_on_delivery` flag (what fetched units
  carry: 799 bundled units true on 2026-11-26), then Flipkart's own `api_extra.payment_mode` ("Cash On Delivery"),
  then a checkout job's `payment_mode` — the top-level `payment_mode` alone reads "cod" on only 6 bundled units.
- **Phase 3 "refetch"** of the 3-hourly sweep (`runRefetchPhase`, after phase 2): rows with an open entry due by
  now (index `{userId:1, "refetch.due_at":1}` on `invoices`), at most `INVOICE_MAX_PER_RUN`. Each order is claimed
  (`refetch_claim_until` = now + 20 min, `refetch_claim_by` = host) — a second sweep, on either Mac, skips it.
  Entries whose `due_at` is not after the newest download the row knows (`last_download_at`, else the
  re-capture / capture time) close `already_newer` with no call. The rest: one download per order
  (`force`, path `sweep-refetch`, `skipIfUnchanged` — the same files as the processed bundle are not re-uploaded,
  so no Lambda run). A success closes every due entry of the order (`downloaded` / `unchanged`); a failure adds one
  to `attempts` (and `last_error`), and an entry closes `gave_up` at 3 (one try per sweep, ~9 h). `settled` is
  left as it is: this is how a settled bundle is looked at again. Claims are given back at the end. The run shows
  in Batch requests as part of the sweep's request (its orders are tasks; `params.refetch_count` when it had any).
- **Measured on today's data (read-only, 2026-11-26):** 59 Cancelled units carry a bundle key; 48 of them (30
  orders) show a moment (48 cancellation, 28 refund); 56 of the 76 moments are already covered by the bundle's
  last download, 0 are due now, the rest fall due within the next 8 days. Expected volume after that: the few
  invoiced units a week that turn Cancelled / Returned (38 invoiced cancelled units in bundled orders in total).

## Cancelled after shipping (SO-6, built OFF) and phase 1's failure memory

- `INVOICE_CAPTURE_CANCELLED=0` (default; owner question 5 open). With `1`, phase 1 also takes orders with no
  bundle whose units ended after shipping (the invoiced rule above), every unit finished, no `invoices` row, and the
  last cancellation at least 15 days old (every credit note measured arrived within 14 days, so one download is
  final). Measured now (read-only): 2,542 such orders 15 days or older, 4,032 of any age (the plan's 2,462 /
  3,955 on 11-25). Switched off, phase 1 selects exactly what it did before (checked).
- **Failure memory, always on:** an order whose phase-1 download failed **on Flipkart's side** (an HTTP status, an
  answer that is not a PDF / zip) gets `invoice_capture_failures` + 1 and `invoice_capture_failed_at` on every unit
  (no index; written through the driver — the Order model and the dashboard mirror do not declare them yet). Phase
  1 skips it for 24 h, and for good after 3 failures (today one failing order is retried every 3 h). Not counted:
  "no session" (the account's state, not the order's) and failures of our own (`ours`: S3, Mongo — an outage must
  not drop orders from capture). A later capture of the order clears its memory. To clear it by hand:
  `db.order_details.updateMany({order_id: "<id>"}, {$unset: {invoice_capture_failures: "", invoice_capture_failed_at: ""}})`.
  The batch and the modal are not affected — they download what they are given.

## The originals backfill (SO-4, OPS-14 / OPS-15)

`npm run invoices:originals-backfill` (`src/scripts/invoiceOriginalsBackfill.ts`):

- **Dry run by default** (read-only): bundles, coverage (rows with `stored_download_id`), orders to re-download and
  their accounts, orders without units, orders held by another open request, orders by account state
  (`platformids.status` only), the requests it would send (≤ 2,000 orders, cut at account boundaries) and a time
  estimate (70–140 orders a minute). Today: 5,075 bundles, 0 covered, 5,075 orders on 1,820 accounts (5,073 on
  logged-in accounts), 3 requests (1,999 + 1,996 + 1,080), ~36–73 min.
- `--go`: refuses while an open invoice request with `params.purpose: "originals_backfill"` exists (either Mac —
  shared Mongo; two started together: the older request wins, the younger stops its canary), and when no
  invoice-batch worker is alive on this Mac (the jobs go to this Mac's Redis — run it on the Mac whose worker runs
  sub-step 12). Then a **1-order canary** (an order of a logged-in account): it must produce an
  `invoice_downloads` document that the bundle's row names (`stored_download_id`) within 3 minutes
  (`--canary-wait`), or nothing more is sent — and it stops at once when that document carries `originals_error`
  (e.g. AccessDenied on `orig/` before D1's grant); a warning (no stop) when the Lambda builds that download's archive
  with `zip_source` other than `original` (sub-step 5 not deployed). Then the rest — or `--limit N` for the pilot —
  as requests through `createInvoiceRequest` (force false, source `user`, by `originals backfill`,
  `params.purpose`), each ≤ 2,000 orders and no account in two requests (an account with more than 2,000 orders
  alone gets requests of its own).
- **Resumable:** every run re-selects only rows still without a stored download. **Stop:** Batch requests → Stop.
- **OPS-14's "+1 repeat" gate is a manual step:** once the pilot has finished, re-download one pilot order with
  Download Order Invoices (Force), then `npm run invoices:originals-backfill -- --compare <orderId>` (read-only)
  compares the sha256 sets of that order's two newest `invoice_downloads` documents: "same sha256 set" passes;
  a difference names how many files are only in one of them (a late document, or Flipkart re-signed).
- Built here, run here only as the dry run. OPS-14 / OPS-15 are the owner's, after R3 on this Mac, the other Mac on
  this code, and D1.

## When to merge: after D1 (the plan's gate for sub-step 12)

The plan's gate for 12 is "All pass; D1 is deployed", and this build depends on it:

- **T1 re-extracts through whichever Lambda is deployed.** With the fix, every re-download of a processed bundle
  is claimed and extracted again. The extract Lambda deployed before D1 is the pre-sub-step-4 one: it pairs
  product invoices to units by page position and fills an IMEI where a unit has none — plan finding T12: "do not
  re-queue processed bundles: fill-when-blank would add wrong IMEIs".
- **P9 changes the merged page order** of answers with 10 or more files (read-only today: 843 of 5,075 bundles
  hold 10 or more documents and 2 or more product lines), which moves that Lambda's positional pairing; plan
  conflict 6 allows numeric order only once no deployed reader uses position.
- **X3** leaves a unit's part list to the pipeline, which only from sub-step 4 (deployed by D1) rewrites it on every
  unit; before D1, a re-download that gains a freight invoice would leave the units' list stale.
- **Originals need the `orig/*` grant** of D1. Without it the store is best-effort (above), so nothing breaks, but
  no original is kept.

So: **OPS-6 (backup) → D1 → merge this → R3**, then OPS-14 / OPS-15. Not before D1.

## Between the merge and the owner's restart (R3)

The Trade Flow API (tsx watch) reloads at once; the worker runs the old code until the owner restarts it.

| runs in | what | code in between |
|---|---|---|
| API | Download Order Invoices modal (`by-order-id`); the fetch's capture (`fetch` — the fetch runs in the API process) | **new**: originals (best-effort), `invoice_downloads`, `download-id`, T1, T2, X3, P9; a first store notes late re-downloads for units already ended (SO-5) |
| worker | invoice batches (Start a request → Invoices, the Delivered check — the path OPS-15 uses), the 3-hourly sweep, the update chain's invoices, the update sweeps | **old**: no originals, no `download-id`, `$set etag/size/parts` (T1 bug), part list stamped on every unit, text sort; no noting from updates, no phase 3, no failure memory, no SO-6 |

**Indexes:** both new indexes already exist on Atlas — `invoice_downloads {userId:1, order_id:1, at:-1}` since
2026-11-25 21:32 UTC and `invoices {userId:1, "refetch.due_at":1}` since 21:55 UTC — built by this build's own
`test:invoice-batch` / `test:start-requests` runs, which then connected with Mongoose's default autoIndex and
imported the new models. Both checks now connect with `autoIndex: false, autoCreate: false`. The reload builds
nothing new (nothing ever new on `order_details`).

Between the two, SO-5 entries noted by the API's first stores simply wait (nothing runs phase 3 until R3); the old
worker's downloads do not set `last_download_at`, so phase 3 may later download such an order once more than needed
(never less). The backfill script runs as it is under tsx, but its canary refuses until the worker runs this code
(the old worker writes no `invoice_downloads` document).

**The other Mac on old code cannot break the code, but it brings T1 back on every row it touches**: its
`registerInvoice` still `$set`s the new etag (and size, parts) before the claim, so the Lambda does not
re-extract what it downloaded, its raw PDF carries no `download-id`, and it stamps its part list on the units.
So the other Mac must pull and restart (API and worker) — or keep its invoice downloads and updates paused —
before OPS-15. On old code it also notes no late re-downloads and runs no phase 3 (harmless: this Mac's
updates and first stores note them, and the entries live in the shared rows); once on this code, its sweep's
phase 3 and this Mac's are kept apart by the sweep lease and the per-order claim. Set the same
`INVOICE_REFETCH_*_DELAY_H`, `INVOICE_CAPTURE_CANCELLED` and `INVOICE_RECAPTURE_GIVE_UP_DAYS` on both Macs. OPS-8
re-reads every processed bundle from the bytes stored then, so it also repairs rows re-downloaded before the fix.

## Mirrors still owed (not editable from this build)

- Dashboard `backend/src/models/Invoice.ts`: `refetch[]`, `refetch_claim_until`, `refetch_claim_by`, the index
  `{userId:1, "refetch.due_at":1}` (it already exists on Atlas), and `stored: "failed"` / `originals_error` on the
  dashboard's `InvoiceDownload` model.
- Dashboard `backend/src/models/Order.ts` and Trade Flow `src/models/Order.ts`: `invoice_capture_failures`
  (Number), `invoice_capture_failed_at` (Date) — no index on `order_details`.
Nothing breaks without them: both apps write these through the driver / pipeline updates, which keep unknown fields.

## Checks

`npm run test:invoice-originals` (`src/scripts/invoiceOriginalsCheck.ts`) — no Flipkart, no S3: a fake order-API
context answers a zip built with fflate from the dashboard's offline fixtures (`infra/test-fixtures`, read in place;
`DASHBOARD_REPO=<path>` when elsewhere) and an in-memory S3 that enforces `IfNoneMatch` and the checksum. Rows
are invented (made-up workspace, `@verify.invalid`, `ODVERIFY…`) and deleted; leftovers are counted.
It checks: numeric order (`file_1…file_36`, product block 7, 11, 12, 17, 19, 22, 32), T4 labels, the delivery
rule, one `orig/` object per file with the fixture's sha256, a second run storing 0 new objects, `download-id`
on the raw PUT naming the record, `page_from` tiling the merged PDF in numeric order, `captured_at` / `settled`,
X3, `skipIfUnchanged` making no raw PUT, the fetch path, the claim-made row's empty parts filled, the 501
fallback, best-effort originals (403 AccessDenied and 503: the merged PDF still stored without `download-id`,
`stored: failed`, `stored_download_id` cleared), a raw-PUT `ECONNRESET` marked as ours — and the **T1 proof
against the dashboard's own claim rule**: the filter of `claim()` is read from the dashboard's
`backend/src/services/invoicePipeline.ts` (read-only) and evaluated in Mongo: after a re-download of a processed
row the S3 event's new etag is claimed, a duplicate event of the old bytes is not, and the old `$set` would
have hidden it. `CLAIM_RULE_FILES=<a.ts,…>` checks more copies of the rule (e.g. a dashboard worktree).

`npm run test:invoice-refetch` (`src/scripts/invoiceRefetchCheck.ts`) — no Flipkart, no S3 (every download is a
function injected into the sweep; the one store through the real helper uses an in-memory S3); rows invented in four
made-up workspaces and deleted, leftovers counted. It checks the moment table (Shipped → Cancelled invoiced gives
`cancelled`, Order Confirmed → Cancelled / Cancellation requested give nothing, Returned, COD delay, DONE / PENDING
Refund stage, a stage dated ahead), the `.env` delays, the atomic noting (twice and six at once leave one entry),
the update worker's call, the first-store noting, phase 3 (one download per due order with `sweep-refetch` /
force / skipIfUnchanged, `already_newer` without a call, not-yet-due and held-elsewhere left, `settled` stays true,
3 failures → `gave_up` and no further call, two concurrent sweeps download once), SO-6's selection (off = the old
selection exactly; on = cancelled after shipping ≥ 15 days, not 10 days, not at Order Confirmed, not with a unit on
its way, not with an `invoices` row; held back after 3 failures or within 24 h), the failure memory (no-session and
our own failures not counted, a capture clears it), a COD unit shaped like a real API row (`cash_on_delivery: true`,
no top-level `payment_mode`) due 8 days out, and SO-4's chunker (no request over 2,000, no account split; a
2,500-order account alone), canary rule (including `originals_error`), the `--compare` repeat gate, the either-Mac
guard and the selection. It clears the SO-5 / SO-6 settings AFTER config has loaded `.env`, so it runs on the
defaults whatever a Mac's `.env` sets.

`npm run test:invoice-batch` now also asserts that the batch stores with path `batch`, its request id and no
`skipIfUnchanged`, and that a store failure of our own carrying `ECONNRESET` ends `internal_error`.

The update worker's path is checked as the worker runs it (`snapshotUnit` → `recordUnitChanges` →
`noteLateDocuments`, a quiet read noting nothing) in `test:invoice-refetch`; the `schedule_active` selection (the
refund watch, X5) in `test:unit-save` (sub-step 10, `docs/unit-saving-events.md`).
