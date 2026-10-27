# HTTP checkout engine ("High Speed")

A Flipkart COD order placed with the same API calls the Flipkart page makes,
and no Chrome. Code: `src/services/httpCheckout.ts`; wired into the worker in
`src/services/checkoutRunner.ts` (the "HTTP engine" block, before the browser
launch). Everything is additive: a job that is not `high_speed` + COD never
touches it, and an HTTP job the engine cannot settle continues into the Chrome
flow exactly as before.

## How an order is routed

| Dashboard field | Trade Flow | Queue | Engine |
|---|---|---|---|
| Order execution version = **High Speed** (`order_execution_version: sonic_boom`/`high_speed`), payment mode COD | `request.executionVersion = "high_speed"` | `checkout-http` | HTTP engine, Chrome fallback |
| High Speed, card payment | `high_speed` | `checkout-card` / `checkout-otp-phone` | Chrome (cards are not in the engine) |
| Standard / Safe | `chrome` | `checkout-<class>` | Chrome |

`executionVersion` is parsed in `services/jobRequest.ts`, stored on
`CheckoutJob.request`, carried on the queue payload, and `queue.ts
checkoutQueueFor()` picks the queue. Retries keep it.

Worker: `HTTP_CHECKOUT_CONCURRENCY` jobs of the `checkout-http` queue at once
per process (default 25; 0 = none). A fallback opens Chrome in the same
process, at most `HTTP_FALLBACK_CHROME_MAX` (default 2) at a time
(`services/chromeFallbackGate.ts`). **The worker must be restarted** to start
consuming `checkout-http`.

## The calls, in order

Session: Playwright `request` context with the platform ID's saved cookies,
the m-site identity (`devices[MOBILE_DEVICE].userAgent`,
`x-user-agent: … FKUA/msite/0.0.3/msite/Mobile`), `origin`/`referer`
flipkart.com, through the job's Proxy Pool lease (same `claimEgress` as
Chrome).

**Data centre.** Every account is pinned to a DC; the host prefix names it
(`1.rome.api.flipkart.com` = CH, `2.rome…` = HYD). A call on the wrong prefix
answers `406 {"ERROR_MESSAGE":"DC Change","RESPONSE":{"id":"1"}}`; the engine
switches the prefix once per session (rome and `N.payments.flipkart.com`
alike) and repeats the call.

| # | Call | Purpose / what is read |
|---|---|---|
| 1 | `POST /api/4/page/fetch?cacheFirst=false` `pageUri=/viewcart?exploreMode=TRUE&preference=FLIPKART` | cart lines from `pageContext.fdpTrackingData.marketplaceData[].items[]`; other items are removed with the page's own `CART_REMOVE` action (`POST /api/1/action/view`, `items:[{productId, listingId:"<lid>:<acc>:P:A-0001:U:SPCMS"}]`) and the cart re-read |
| 2 | `POST /api/5/cart` `{cartContext:{<lid>:{productId, quantity, …}}, pageType:"ProductPage"}` | sets the line to the job's quantity; `cartResponse[lid].errorCode` → `ADD_TO_CART_FAILED` |
| 3 | `POST /api/1/action/view` `type: CHECKOUT_INIT` | opens the checkout; `landingPageAction.params.pageUri` is the checkout page (login landing → fallback) |
| 4 | `POST /api/4/page/fetch?cacheFirst=false` `pageUri=/viewcheckout?…` | `checkoutSummaryTrackingData`: `addressId`, `pincode`, `pincodeServiceability`, `totalCost`, `checkoutID`, `gbCheckBoxSelected`; `CART_PAGE_PRODUCT_CARD` cards (listing + quantity); `slaInfo[].maxSLA` and the "Delivery by …" text |
| 4b | address fix, only when the checkout is on another pincode or has no address: `GET /api/1/contacts`; `PUT /api/3/user/contact {…, contactId}` (edit the saved one into the job's address, phone kept) or `POST /api/3/user/contact` (add); `POST /api/1/action/view` `type: CHECKOUT_CHANGE_SHIPPING_ADDRESS {checkoutUpsertGroupRequest:{cartItemRefIds, contactId}}`; summary re-read | same one-address policy as the Chrome preflight |
| 5 | `POST /api/1/action/view` `type: CHECKOUT_PAYMENT_TOKEN_GENERATE` | `originalUrl = /payments?token=PN…` |
| — | **dry run stops here** (`PAYMENT_DRY_RUN=true`, or the bench): the line is removed from the cart again | |
| 6 | `GET https://pay.flipkart.com/payments?token=…` | the payment page's cookies |
| 7 | `POST https://N.payments.flipkart.com/fkpay/api/v3/payments/pay?instrument=COD` `{token, payment_instrument:"COD", remove_captcha_page:true, device_information, device_capabilities}` | the order. `response_status: SUCCESS` + `primary_action.parameters.merchant_reference_id` (OD… 18 digits) = placed; `transaction_amount` in paise; `payment_handling_fees_details` COD fee |
| 8 | `POST /api/4/page/fetch` `ORDER_CONFIRMATION_PAGE` | best effort: `ocUnitDetails[0]` status, seller, price, SLA |

The runner writes `paymentStartedAt` right before call 7 (`onBeforePay`), as
the Chrome flow does before pressing Pay.

## Verdicts, fallbacks, reconciliation

The engine decides on its own only what it can be sure of:

| Outcome | When | Job |
|---|---|---|
| `placed` | pay answered SUCCESS with an order id | `paid`, result fields as a Chrome order (`flipkartOrderId`, amount, fee, SLA, cart total) |
| `failed` ADD_TO_CART_FAILED / ITEM_NOT_DELIVERABLE / CART_AMOUNT_LIMIT / SLA_EXCEEDED / COD_UNAVAILABLE / PAYMENT_FAILED | the listing cannot be added; pincode not serviceable; total over the limit; promised days over the limit; COD refused; pay answered a non-success | `failed` with the same codes, retry rules and batch bookkeeping as Chrome |
| `failed` + `payUnknown` | call 7 was sent and got no answer | `needs_reconciliation` (never re-run; the order sync looks the account up) |
| `fallback` | login/captcha challenge (401/403/406 after the DC switch/429, HTML), a cart it cannot empty, no address and no phone to add one, GST required but not ticked, quantity on the checkout not the job's, no SLA on the page, any unexpected answer | the job continues into the Chrome flow (behind the fallback gate); the log says why |
| proxy error before pay | tunnel dropped | PROXY_FAILED path: attempt refunded, job re-queued, proxy reported |

What still goes to Chrome today: **GST invoice** (the GST box; `CHECKOUT_UPDATE_GST`
is not implemented), card payments, accounts whose saved address cannot be
edited.

## Bench (engine alone, no queue)

```
npm run bench:http -- --n 10                    10 accounts at once, dry run
npm run bench:http -- --n 100 --skip 500        100 fresh accounts
npm run bench:http -- --n 1 --skip 9 --real 1   ONE real COD order on one account
   --direct   no proxy    --concurrency N    --pincode 122016    --verbose
```

Dry run = every call up to the payment token, nothing bought, the cart line
removed again. The bench locks each account (`account:lock`) so it never
collides with the worker, and writes nothing to `CheckoutJob`. Note that the
address fix is real: an account whose single saved address differs gets it
edited into the job's address, exactly as a Chrome order would do.

### Matrix — 2026-10-27, this Mac, 5 Proxy Pool proxies round robin, dry run

| accounts (all at once) | reached token | wall | checkouts/s | p50 per checkout | p95 | fallbacks |
|---|---|---|---|---|---|---|
| 10 | 10 | 6.9 s | 1.5 | 5.2 s | 6.9 s | 0 |
| 50 | 49 | 28.7 s* | 1.7 | 5.8 s | 6.7 s | 1 (DC answer under load) |
| 100 | 99 | 12.9 s | 7.8 | 8.2 s | 10.1 s | 1 (proxy TLS drop) |
| 150 | 144 | 17.7 s | 8.5 | 12.7 s | 14.9 s | 4 proxy drops, 2 DC |
| 200 | 193 | 24.2 s | 8.3 | 17.9 s | 21.5 s | 2 proxy drops, 5 DC |

\* one straggler retried for 28 s; the rest finished in 7 s.

Per-call latency at 10 in flight: cart view 1.3 s (with removal), add 0.5 s,
init 0.6 s, summary 0.4 s, token 0.3 s, address fix 1.7 s when needed. At 200
in flight every call is 3–4× slower: the five proxies are the ceiling, not
the engine (CPU of this process stays near idle). Half of the fresh accounts
needed the address fix (two extra calls); a second pass over the same
accounts needs none.

What it means for "3,000 in 1.5 minutes": that is 33 orders/s. Five proxies
gave 8–9 checkouts/s; the real flow is two calls longer (pay page + pay) but
skips the dry-run cleanup. Expect roughly linear gains with more proxies:
about 20–25 proxies at the same per-proxy load, or the same five with a
higher per-proxy limit if the provider allows it. No Chrome fleet is needed
for COD at that scale; one process with `HTTP_CHECKOUT_CONCURRENCY=200` on
the ops box is enough, Chrome only for the fallbacks.

## Not yet verified

The real COD pay call (steps 6–8) was recorded from a successful Chrome order
and is implemented from that recording; it has not yet been exercised by the
engine itself (a permission rule stopped the one-order test). Run
`npm run bench:http -- --n 1 --skip 9 --real 1` once and check the printed
order id in Order Units.
