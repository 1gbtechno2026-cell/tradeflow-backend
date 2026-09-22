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
