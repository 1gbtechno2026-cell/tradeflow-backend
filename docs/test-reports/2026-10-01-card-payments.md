# Card payments — test report, 1 Oct 2026

Two card types were driven through the real Flipkart checkout and the real
bank pages, first from the Test tab (harness), then from Submit Order (the
worker). Account `actwrysum@bobbhai.in` / `actbogtee@bobbhai.in`, address 19
(TRANSPARENTS, 122017), GST 14 (`06AAUFT2204P1ZO`).

## Result in one line

| card type | bank step | outcome |
|---|---|---|
| **ICICI Corporate Virtual** (OTP via leased employee handset) | works | **3 real orders placed** — 2 from the harness, 1 from Submit Order |
| **HDFC Virtual** (password) | never reached | blocked at Flipkart's gateway: `PAYZIPPY_TECHNICAL_ERROR` — specific to this card |

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

## HDFC Virtual — blocked before the bank

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

## Open

- HDFC Virtual: card-side fix (see above), then one discovery run to capture
  the password page.
- `listingAmount` is empty on the worker's result for the keychain — product
  page price capture, unrelated to payment.
- Not-deliverable scenario still needs a product Flipkart refuses for 122017.

## Where the evidence is

`debug/test-runs/<case>-<timestamp>/` — screenshot, visible text and DOM at
every step, `network/payments.json` (redacted), `run.log`. The worker job's
own log is on the job (`CheckoutJob.logs`), visible in the Orders tab.
