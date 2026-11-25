# Unit fields from Flipkart's answer (data-quality sub-step 9)

What the order mapper (`src/services/orderApi.ts` `mapApiUnitToOrder`) writes on a unit since sub-step 9, and the
two save rules in `src/services/orderFetch.ts` that go with it. Plan: the dashboard repo's
`docs/order-data-quality-plan.md`, section 9 (changes U2, U3, U4, U5, U16 and finding T8).

| Field | Rule | Was |
|---|---|---|
| `product_name` | `fullProductName(title, subTitle)`: Flipkart's title + ` (subTitle)` unless the title already holds it (decision 7) | the title alone |
| `api_extra.product_title`, `.product_subtitle` | Flipkart's title and subTitle as sent (pairing groups by the raw title) | — |
| `refund_status`, `refund_amount`, `refund_msg` | `refundFromActionInfo`: the order page's refund card (`units[].actionInfoDataBag`): status Completed when every movement is, else the first other one; amount from "Total refund - ₹N" (else the movements' own amounts summed); the sentences joined with ` \| ` | `"refund"` from `nonReturnRefunds` (empty on every traced unit) |
| `refund_date`, `refund_kind` | the sentence's date (IST; a sentence without a year takes the year nearest the Refund stage — a "by Nov 23" read on Nov 18 is the stage's Nov 23 — or, with no dated Refund stage, the latest year not after now + 1 day), else the Refund stage; `reversal` / `supercoins` / `refund` | — |
| `card_name`, `card_number`, `card_source` | our checkout job's card wins (`card_source` `checkout`; a COD job writes its blank columns with `""`); only when no job of ours placed the order, the card the refund sentence names: bank, `XXXXXXXXXXXX` + last four, `card_source` `flipkart_refund`, and `api_extra.refund_card` (carried forward from the stored row by a later save that writes no card columns) | checkout job only |
| `delivery_date` | `currentSlotEndDate ?? newPromisedDate ?? promisedDate`; the first promise in `api_extra.first_promised_date` | `promisedDate` |
| `status_label` (cancelled) | the cancel step's sentence; with no Cancelled stage (T8's shape) the sentence of the Refund / Cancelled stage that undid the delivery; else Flipkart's status text when it is not one of the unit's stage titles; else "Cancelled" | the newest stage's last step ("Refund") |
| `status_reason` | Flipkart's `statusReason` only (failed units as before) | the cancel sentence on cancelled units |
| `last_tracking_step` | `referenceLastStep`: `date \| stage [\| step \| last remark \| its date]`, ASCII, dates with a 6-digit fraction when they have milliseconds (also the page path) | `stage · date` |
| timeline dates | keep Flipkart's milliseconds when not zero (`…T21:50:50.123+05:30`) | whole seconds |
| `actual_delivery_date` | Flipkart's `actualDeliveredDate` (the reference's value), else the Delivered stage; the stage time in `api_extra.delivered_stage_at` | the Delivered stage |
| `status_key` Delivered (T8) | Flipkart's key or a **dated** Delivered stage; a DONE Cancelled / Refund stage after the Delivered one undoes it: Cancelled (cancelled_date = that stage), Returned for a Returned key, Refunded for a Refund* key; an open return ("Return requested" / "approved") stays Delivered as before | any DONE Delivered stage |

A refund answer without a refund card leaves the stored refund fields as they are (`unitSetFor` for status / amount /
message; `saveMappedUnit` writes date and kind only with a card). "We have resent this product" is a replacement: no
refund fields; the sentence is kept in `api_extra.action_info`.

The page path (`scrapeAndSave`) keeps a stored product name that starts with the page's title (the page has no
subtitle), writes the same Last Tracking Step layout, and writes its bare `"refund"` marker (and its "Total refund"
snippet) only on a unit with no refund status yet, so Flipkart's "Completed" / "Processed" and sentences stay.

**The page fallback still uses the old rules** for the fields below (64 page saves against 23,425 API saves in the
14 days to 2026-11-26). A unit that falls back to the page rewrites them; its next API read puts them back:

- `status_reason` on a cancelled unit (the page writes the cancel sentence; the API writes Flipkart's `statusReason`
  or null) and `status_label` on a cancelled unit (the page writes the newest step);
- `actual_delivery_date` (the page writes the Delivered stage time, the API `actualDeliveredDate`);
- the T8 rule (the page makes any DONE Delivered stage Delivered, dated or not);
- `card_source`: the page writes our checkout card through `cardFieldsFor` (`orderLinks.ts`) without the mark, so a
  card saved by the page keeps `card_source` `""` until its next API read (owner decision: add `card_source` to
  `cardFieldsFor`, or accept).

**The one stored unit T8 changes**: OD339115273746442100's Delivered unit (Order Confirmed > Shipped > Out for
delivery > Delivery Unsuccessful > Delivered without a date > Refund, no Cancelled stage; the other 14 units of the
order are already Cancelled). Delivered is terminal, so the update sweep skips it: name it in the OPS-10 forced
refresh, after both Macs run this code (an old-code read flips it back to Delivered and raises a stage_changed event).

Beyond the spec's letter, kept on purpose (0 traced units use the first, third and fourth): the product title and
subtitle also read `productDataBag[listingId]` when there is no FSN entry; Last Tracking Step cells turn `₹` /
`%E2%82%B9` into `Rs.` and a `|` inside a cell into `/` (the separator); the last remark falls back to the step's
first date when it has none; a mixed card drops its "We have resent this product" sentence from the refund.

## Checks

- `npm run test:unit-mapper [-- --dir <debug/order-traces>]` — no network, no Mongo: unit cases, then every saved
  trace re-mapped from Flipkart's raw answer. 24-11 nightly trace (3,930 units): 2,725 names gain the subtitle, 20
  already hold it, 1,185 have none; 314 refund cards over the 11,685 traced units (258 money, 56 replacements; 7
  replacements in the nightly), 268 card sentences, 0 Delivered units with a refund; `delivery_date` =
  `currentSlotEndDate` on every non-failed unit, moving on the 164 with a later promise and on no other; every Last
  Tracking Step ASCII in the reference's pattern; `actual_delivery_date` = `actualDeliveredDate` on 1,835 of 1,835,
  another IST day on 212; T8 changes 0 traced statuses; no `refund_date` more than 60 days from its Refund stage.
  Where the plan's sentences differ from the measure: the 314 refund cards are 215 with a "Total refund" heading +
  43 without one (all on Failed units: the movements' amounts are summed) + 56 replacements (7 in the nightly); the
  compared order's 5 Delivered cells each read their own Delivered stage time (unit …002: `2026-10-27T21:55:34+05:30
  | Delivered`); the plan's 1,841 delivered units are 1,835 in the nightly (it holds 1,838 units with
  `actualDeliveredDate`: 1,835 Delivered + 3 Cancelled; stage split 187 / 368 / 1,063 / 217), the 212 agree.
  In a worktree (no `debug/`) pass `--dir`; elsewhere it reads `ORDER_TRACE_DIR` from the environment or `.env`.
- `npm run test:refund-card` — invented workspace, rows deleted: the card rule (and `api_extra.refund_card` carried
  forward), the page path's name rule and its refund marker.

## Live

The worker (update, cancel, sync) at its restart (R3); the fetch and the Test tab when the Trade Flow API reloads.
Units change on their next read; the forced refresh (OPS-10…13) brings Delivered and Cancelled units over.
