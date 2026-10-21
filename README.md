# Trade Flow Backend

Flipkart checkout queue: HTTP API enqueues one job per logged-in account, a BullMQ worker restores the Mongo-saved session and runs through product → Buy Now → order summary (qty / address / GST), then **stops on the payment page**.

## Run

```bash
cp .env.example .env
# set MONGO_URI, TRADE_FLOW_USER_ID (og_user id), API_KEY
docker compose up -d redis
npm install
npx playwright install chromium   # only if Chrome is not installed
npm run dev      # API, default :4100
npm run worker   # checkout consumer
```

## Stopping these processes — do not use a bare `pkill -f`

This service and the dashboard backend run the **identical** command line:

```
node .../node_modules/.bin/tsx watch src/index.ts
```

`pkill -f` matches the full command line and ignores the working directory, so
`pkill -f "tsx watch src/index.ts"` intended for one of them kills **both**. It
happened on 2026-09-30: restarting the dashboard API silently took Trade Flow's
API down with it, and because `tsx watch` only restarts on a file change — never
on a kill or a crash — it stayed down unnoticed for about an hour.

Qualify the pattern with the repo path, so it can only match one:

```bash
# this repo only
pkill -f "Trade_Flow_Backend.*tsx watch src/index.ts"
# the dashboard only
pkill -f "gmail-id-dashboard_mongodb.*tsx watch src/index.ts"
```

Safer still: `pgrep -af "<pattern>"` first and read what it matched, or stop the
process from the terminal that started it. The same trap applies to the workers,
since `npm run worker` here and the dashboard's verify worker are both `tsx`
processes.

## Integration endpoint

Post your Smart Bulk Order payload as-is:

```
POST http://localhost:4100/api/jobs
Content-Type: application/json
x-api-key: change-me
```

(`POST /api/orders` is the same handler.)

Used from your body:

| Field | How it is used |
|---|---|
| `product_url` | Product page |
| `quantity_per_order` | Qty on Flipkart order summary (fallback: `quantity`) |
| `total_attempts` | Cap on how many emails to queue |
| `emails` | One checkout job per logged-in Flipkart ID |
| `cart_amount_limit` | Fail the job if scraped price × qty exceeds this |
| `address_id` + `gst_id` | Loaded from dashboard Address / Gst collections |
| `gst_mandatory` | Requires `gst_id` when true |

Cards / payment_mode are accepted but **not charged** in v1 — the worker stops on the payment page.

Poll progress:

```
GET  /api/jobs/batch/:batchId
GET  /api/jobs/:id
POST /api/jobs/:id/cancel
```
