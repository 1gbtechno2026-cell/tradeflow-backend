# Card payments — test report, 1 Oct 2026

Two card types were driven through the real Flipkart checkout and the real
bank pages, first from the Test tab (harness), then from Submit Order (the
worker). Account `actwrysum@bobbhai.in` / `actbogtee@bobbhai.in`, address 19
(TRANSPARENTS, 122017), GST 14 (`06AAUFT2204P1ZO`).

## Result in one line

| card type | bank step | outcome |
|---|---|---|
| **ICICI Corporate Virtual** (OTP via leased employee handset) | works | **3 real orders placed** — 2 from the harness, 1 from Submit Order |
| **HDFC Virtual** (password) | works (2 Oct) | **2 real orders placed** — 1 from the harness, 1 from Submit Order (worker) — after the card-side block was cleared |

## ICICI Corporate Virtual — placed orders

| order | via | charged | bank txn | gateway txn | Pay → confirmation |
|---|---|---|---|---|---|
| OD4387711732098711 | Test tab | ₹170 (₹169 + ₹1 corp-card fee) | 31021981313 | PZT2610011737JOCAY01 | 26 s |
| OD4387713530896621 | Test tab | ₹170 | 7908582524816559806041 | PZT2610011807Y56OU01 | 30 s |
| **OD4387715339643561** | **Submit Order → worker** | ₹170 | 7908600588366626806025 | PZT2610011837VSXW601 | 29 s (62 s claim → confirmation) |

Product for all three: Sheval dino keychain, 1 unit, cart ₹169. Handset `DMPL001`
(`87****1297`) under corporate `ER648J` each time; the OTP arrived 8–13 s after
the request, well inside the 2-minute budget that starts at the Pay press.

The worker job for OD4387715339643561 ended `paid` with every result field
filled: order id, transaction amount ₹170, cart after offer ₹169, fee ₹1,
bank/brand ICICI MASTERCARD, bank and gateway txn ids, SuperCoins 0, promise
**6 days → 7 Oct** (Flipkart's own checkout text said "Delivery by Oct 7, Wed"),
seller, order status Approved, card `****8002`, handset `DMPL001`. The Orders
tab shows it as Success with those columns.

### The ICICI bank pages (Wibmo ACS, no iframe)

1. `#corporateId`, `#employeeId` (both `type=password`), Submit — "times out after 7 minutes".
2. "A one time password (OTP) has been sent to your registered mobile number XXXXXX1297" —
   `input[name=otpValue]` (6 digits), `#submitBtn`, RESEND, CANCEL.

Submit on page 1 is what sends the SMS, so the identity is *filled* before the
OTP timestamp is taken and *submitted* after it — otherwise a fast SMS would be
refused as older than the request.

### What the confirmation page is worth

Nothing. The m-site shows "Order Placed / You saved ₹429" behind a scratch
card. Every number above comes from two responses the page receives after the
bank hands back — the gateway's `pgresponsehandler` (amounts in **paise**) and
the `ORDER_CONFIRMATION_PAGE` data (`sla.maxSla`, `cnc.coinComponent`,
seller, order status) — read passively by `PaymentApiWatcher`. This is the same
for every card type: only the bank page differs per method.

## HDFC Virtual — placed on 2 Oct

| order | via | charged | bank txn | gateway txn | Pay → confirmation |
|---|---|---|---|---|---|
| OD438780031777963100 | Test tab | ₹189 (no handling fee on this card) | 31046828862 | PZT2610021813122YN01 | 20 s |
| **OD438780503746531100** | **Submit Order → worker** | ₹189 | 31048609079 | PZT26100219321KHSW01 | 24 s (100 s claim → confirmation) |

Cards `****2311` (harness) and `****2212` (worker), cart ₹189 (keychain, 14 % off
MRP ₹599), promise 5 days → 7 Oct. The worker job carries every per-order field.

Between those two, five worker attempts on this product failed for reasons that
were each fixed on the spot and are worth knowing:

| job | what happened | fix |
|---|---|---|
| 13:06 | Continue not found after an E002 retry → old code jumped to a hard-coded www.flipkart.com/payments (no card form) | fallback removed; Continue tapped via locator, token URL required |
| 13:11 | hand-off caught on PayU's intermediate page, password typed during the ACS's init, Submit click swallowed | let the ACS settle; submit three ways |
| 13:18 | click on "Credit / Debit / ATM Card" hung 10 s | tap + prove the form opened, retry |
| 13:30, 13:42 | ACS accepted the password, then "Payment Failed" — same `****2311` card that had just paid ₹189 | not code: the card. Gateway's post-bank verdict now logged on the job |
| 13:49 | `GST_NOT_FOUND` with GST mandatory ON | correct: the keychain says "GST … will not be applicable"; now reported as GST_NOT_APPLICABLE with that note |
| 13:54 | payment token issued with no landing URL; page stayed on viewcheckout | open Flipkart's own token URL; screenshot + page text on failure |
Password accepted by HDFC's ACS in 3 s; the ACS handed back through
`2.uiscoop.flipkart.com` ("Please wait while we are confirming your payment")
to the same confirmation page as ICICI. Everything after the bank page is the
shared code; only `enterPassword` was new.

### The HDFC bank page (HDFC's own ACS, no iframe)

`#staticPassword` (name `passCode`), Submit link whose `authSubmit()` hashes the
password before posting, a "Static Password" tab beside an OTP tab, 3-minute
timeout: "Please enter your Master Card 3D Secure PIN … This information will
not be shared with the merchant."

## HDFC Virtual on 1 Oct — blocked before the bank (resolved on the card side)

| run | after Pay | reported |
|---|---|---|
| 10:05 | RBI consent sheet appeared, unknown to the code | hand-off timeout |
| 10:17 | consent → ticked → Pay ₹18,182 → modal *"Your payment couldn't be processed due to a technical error"* | `PAYMENT_FAILED` |
| 10:20, 10:35, 10:38 | *"There was a technical error at the bank's end"* straight after consent; retry without tokenisation → same | `PAYMENT_FAILED` |

Behind the modal, the gateway response says `response_status: FAILED`,
`status_code: PAYZIPPY_TECHNICAL_ERROR` (e.g. txn `PZT2610011555KSJI202`). The
ICICI card passes the identical page, product and account, so the cause is the
HDFC card itself — tokenisation refused by the issuer, online use disabled, or
a limit below the amount are the candidates. HDFC's password page has therefore
never been seen; `hdfcVirtual.enterPassword` is still the documented stub and
will be written from the first capture, exactly as ICICI's was.

Flipkart's card form itself worked for both cards (`#cc-input`, `cc-exp`
zero-padded, `#cvv-input`), as did the RBI consent sequence the first Pay
triggers on a card Flipkart has not seen.

## Other outcomes exercised today (harness)

| scenario | code | Flipkart's words |
|---|---|---|
| sold-out product | `PRODUCT_UNAVAILABLE` | "Out of stock" |
| COD greyed out | `COD_UNAVAILABLE` | Cash on Delivery · Unavailable |
| cart over limit | `CART_AMOUNT_LIMIT` | — (₹169 vs limit ₹159, nothing charged) |
| GST switch (address 19 / GST 14 vs page GST) | success | — |
| quantity above cap | `MAX_UNITS_REACHED` | "You can only purchase N units…" |

Batches are never stopped by any of these; each platform ID runs and reports
its own reason. Per-ID verdicts (out of stock, not deliverable, unit cap, COD
unavailable) are not retried on the same ID.

## Since this report was first written (1–2 Oct)

- Four ICICI orders in total, the later ones through Submit Order → worker;
  per-order data now includes the full `OD…00` id, the card used, billing
  phone, fee breakdown, shipping, MRP, unit price, discount.
- Worker reporting batched: ~13 writes per job instead of ~60.
- HDFC Virtual placed (above).

## Open

- Not-deliverable scenario still needs a product Flipkart refuses for 122017.
- Only one worker may run at a time on this machine (concurrency is per
  process; two processes both take jobs). Restarts are needed after every
  code change — the worker does not watch files.

## Where the evidence is

`debug/test-runs/<case>-<timestamp>/` — screenshot, visible text and DOM at
every step, `network/payments.json` (redacted), `run.log`. The worker job's
own log is on the job (`CheckoutJob.logs`), visible in the Orders tab.
