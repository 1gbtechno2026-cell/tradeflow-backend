# Trade Flow AWS Deployment — Current State

**Last updated:** 8 October 2026 (infra) · 14 October 2026 (note below)
**Region:** ap-south-1 (Mumbai)
**AWS Account:** 390346501537 (old) → 515927224223 (new, see note)

> **14 Oct 2026:** the account below (390346501537) is the OLD account and its access key no longer works. Work continues in the NEW account **515927224223** (IAM user `tradeflow-admin`). Everything in sections 1–7 still exists only in the old account and has to be re-created in the new one (same steps; new account ID in the image URI, new subnets/SG, new Redis endpoint, secrets re-created by hand). First resource in the new account: S3 bucket `tradeflow-invoices-515927224223` (invoice PDFs, private, encrypted, versioned).
**ECS Cluster:** tradeflow

---

## 1. What exists (as of this writing)

### AWS infrastructure

| Resource | Identifier | Status | Notes |
|---|---|---|---|
| ECS Cluster | `tradeflow` | Active | ap-south-1 |
| Trade Flow API service | `tradeflow-api` | 1/1 running | Behind ALB, image `tradeflow-fetch:latest` |
| Trade Flow workers | `tradeflow-worker-w-01` … `w-10` | 10 x 1/1 running | Each has own task def with `WORKER_ID` baked in |
| ElastiCache Redis | `tradeflow-redis` | Available | Redis 7.1, `cache.r6g.large`, endpoint `tradeflow-redis.6abibr.ng.0001.aps1.cache.amazonaws.com:6379` |
| Redis parameter group | `tradeflow-redis-noeviction` | Attached | `maxmemory-policy = noeviction` |
| Redis security group | `sg-076c49587772e984c` | 1 rule | Allows TCP 6379 from `sg-0e4a81a96183876c3` |
| ECS task security group | `sg-0e4a81a96183876c3` | — | `tradeflow-task-sg` — used by API + workers |
| Application Load Balancer | `tradeflow-alb` | Active | DNS: `tradeflow-api-1294134007.ap-south-1.elb.amazonaws.com` |
| ECR repo | `tradeflow-fetch` | Latest pushed | From commit `9212e99` |
| Subnets (ECS) | `subnet-0e59561b55c1a5005`, `subnet-0bccdb1c055e936ea`, `subnet-05a6d7b610e57e7fe` | 3 AZs | ap-south-1a, 1b, 1c |
| VPC | `vpc-0a11250e37f5e983d` | — | — |

### Secrets Manager

| Name | Used by | Notes |
|---|---|---|
| `tradeflow/mongo-uri` | Workers, API | MongoDB Atlas connection string |
| `tradeflow/proxy-cred-key` | Workers, Dashboard | Must match dashboard's `PROXY_CRED_KEY` exactly (64 hex chars) |
| `tradeflow/sms-api-token` | API only | Bearer token for Android OTP forwarders |

**Note:** `PROXY_CRED_KEY` is currently also duplicated as a plain env var in the base task definition. It works, but should eventually be moved to Secrets-only to avoid drift.

### Task definitions

| Family | Revision | Purpose |
|---|---|---|
| `tradeflow-api` | latest | API + order fetch (existing) |
| `tradeflow-worker` | 3 | Base template (has `PROXY_MODE=required`, `PROXY_CRED_KEY`, `MONGO_URI`, `REDIS_URL`) |
| `tradeflow-worker-w-01` … `w-10` | 3 | Derived from base, adds `WORKER_ID=w-XX` |

### Proxy pool

- **10 live proxies** loaded in Mongo (all `103.144.36.x:50100`)
- All 10 currently ASSIGNED to `w-01` … `w-10`
- 0 FREE, 0 DEAD, 0 DISABLED

### MongoDB Atlas

- Shared by dashboard, API, workers
- Holds: jobs, batches, orders, proxies, phone leases, OTPs, platform accounts, cookies

---

## 2. What runs where

| Component | Location | Purpose |
|---|---|---|
| **Dashboard (`ordergine`)** | Local Mac only | Frontend `localhost:5173`, backend `localhost:4000` |
| Trade Flow API | AWS Fargate | Receives orders, creates jobs, pushes to Redis |
| Trade Flow workers | AWS Fargate x 10 | Claim proxies, run Chrome, place Flipkart orders |
| Redis | AWS ElastiCache | BullMQ queues, locks, batch counters, card pool, OTP mailboxes |
| MongoDB | Atlas | All shared state |
| SMS forwarders | Android phones | POST OTPs to API route with bearer token |

**How it works end to end today:**
1. You submit an order on your local dashboard at `localhost:5173`
2. Dashboard backend POSTs to AWS Trade Flow API via `JOBS_API_URL_AWS`
3. API creates a job in Mongo, pushes to Redis on ElastiCache
4. One of 10 Fargate workers picks it up, claims a proxy, runs headless Chrome
5. Worker places the order, writes result to Mongo
6. Your local dashboard reads Mongo and shows status

So: **you drive from your Mac; AWS does the work.** Only you can submit orders until `ordergine` is deployed to AWS.

---

## 3. Deployment history (in order)

1. **Created ElastiCache Redis 7.1** (`tradeflow-redis`), initially `cache.t3.small`, later resized to `cache.r6g.large`
2. **Configured security groups:**
   - `tradeflow-redis-sg` allows port 6379 from `tradeflow-task-sg`
   - `tradeflow-task-sg` used by Fargate tasks
3. **Created 3 Secrets Manager entries:**
   - `tradeflow/mongo-uri` (plaintext, Atlas URI)
   - `tradeflow/proxy-cred-key` (plaintext, 64-char hex)
   - `tradeflow/sms-api-token` (plaintext)
4. **Built and pushed Docker image** `tradeflow-fetch:latest`
   - Fixed Dockerfile: `npm ci --ignore-scripts` (bypasses broken `postinstall` that called `tsc` before `node_modules/.bin` was linked)
5. **Deleted old `tradeflow-worker` service** (0/0 running, unused)
6. **Created base task definition `tradeflow-worker:1`** with worker command override and env vars
7. **Created 10 derived task definitions** (`tradeflow-worker-w-01` … `w-10`) each with `WORKER_ID`
8. **Created 10 ECS services** via CLI (each points to its own task definition)
9. **First boot failed** — `PROXY_CRED_KEY is not set`. Fixed by adding `PROXY_CRED_KEY` as env var to base task def → revision 2
10. **Second boot partially worked** — 8/10 workers claimed proxies, but logs showed:
    - `PROXY_MODE=preferred` (should be `required`) — worker could fall back to AWS datacenter IP
    - `IMPORTANT! Eviction policy is volatile-lru` (BullMQ warning — would delete job state under memory pressure)
11. **Fixed Redis eviction policy:**
    - Created parameter group `tradeflow-redis-noeviction`
    - Set `maxmemory-policy = noeviction`
    - Attached to `tradeflow-redis` cluster
12. **Fixed `PROXY_MODE`:**
    - Added `PROXY_MODE=required` to base task def → revision 3
    - Re-derived all 10 worker task defs to revision 3
    - Updated all 10 services to revision 3
13. **All 10 workers now running on revision 3** — awaiting first COD test

---

## 4. Current state pending action

**Status:** 10 workers on revision 3, `1/1 running`. `w-01` and `w-03` may briefly show `2/1` during rollout drain; settles to `1/1`.

**Next step is to run a single COD test order** to prove the end-to-end loop before:
- Scaling the batch to 10 orders
- Deploying `ordergine` to AWS for team access

---

## 5. CLI commands and patterns

### Re-derive worker task definitions from a new base revision

```bash
CLUSTER="tradeflow"
BASE_TASK_DEF="tradeflow-worker:3"    # change revision as needed
SUBNETS="subnet-0e59561b55c1a5005,subnet-0bccdb1c055e936ea,subnet-05a6d7b610e57e7fe"
SG="sg-0e4a81a96183876c3"

for i in $(seq -w 1 10); do
  WORKER_ID="w-${i}"
  FAMILY="tradeflow-worker-${WORKER_ID}"
  echo "=== Building $WORKER_ID ==="
  aws ecs describe-task-definition --task-definition "$BASE_TASK_DEF" --query 'taskDefinition' --output json > /tmp/base-task.json
  jq --arg wid "$WORKER_ID" '
    .containerDefinitions[0].environment += [{"name":"WORKER_ID","value":$wid}]
    | del(.taskDefinitionArn, .revision, .status, .requiresAttributes, .compatibilities, .registeredAt, .registeredBy, .enableFaultInjection)
  ' /tmp/base-task.json > /tmp/new-task.json
  aws ecs register-task-definition --family "$FAMILY" --cli-input-json file:///tmp/new-task.json --query 'taskDefinition.taskDefinitionArn' --output text
done
```

### Update services to the new revision

```bash
for i in $(seq -w 1 10); do
  FAMILY="tradeflow-worker-w-${i}"
  aws ecs update-service --cluster tradeflow --service "$FAMILY" --task-definition "$FAMILY" --force-new-deployment --query 'service.serviceName' --output text
done
```

### Rebuild + push image

```bash
cd Trade_Flow_Backend
bash deploy/fargate.sh
```

### Delete a service (cleanup)

```bash
aws ecs delete-service --cluster tradeflow --service <service-name> --force
```

---

## 6. Key learnings / gotchas

- `npm ci --omit=dev` fails if package.json has a postinstall that calls `tsc`. Use `npm ci --ignore-scripts` since the runtime uses tsx.
- Fargate `create-service` does NOT support `--overrides`. Must bake `WORKER_ID` into the task definition family.
- `awsvpcConfiguration` subnets must be comma-separated inside brackets, not space-separated.
- `#!/bin/bash` at the start of a zsh paste triggers "event not found" because of `!`. Do not paste the shebang into zsh.
- `PROXY_MODE=required` must be set. Default is `preferred`, which falls back to the ECS task's public IP and will get flagged by Flipkart.
- Redis `maxmemory-policy` must be `noeviction` for BullMQ. `volatile-lru` silently evicts job state.
- `PROXY_CRED_KEY` must be a 64-char hex string (AES-256-GCM). Must match between dashboard `.env` and the AWS secret byte-for-byte.
- `/dev/shm` must be ≥ 2048 MB in the worker task definition or headless Chrome crashes on boot.
- Container stop timeout should be 120s so a worker that has pressed Pay can wait for bank confirmation before being killed.
- Redis security group must allow 6379 from `tradeflow-task-sg`. Otherwise workers can't reach the queue.

---

## 7. Environment variables — worker base task definition

| Key | Value | Source |
|---|---|---|
| API_KEY | shared secret | env |
| HEADLESS | true | env |
| KEEP_BROWSER_OPEN_MS | 0 | env |
| MONGO_URI | Atlas URI | plain env (works, but should be Secret) |
| PROXY_CRED_KEY | 64-char hex | plain env (works, but should be Secret) |
| PROXY_MODE | required | env |
| REDIS_URL | `redis://tradeflow-redis.6abibr.ng.0001.aps1.cache.amazonaws.com:6379/0` | env |
| SLOW_MO_MS | 0 | env |
| TRADE_FLOW_USER_ID | 6a8… | env |
| WORKER_CONCURRENCY | 1 | env |
| WORKER_CLASSES | cod (currently; will change to otp-phone,card) | env |
| PAYMENT_DRY_RUN | false | env |
| WORKER_ID | w-XX | baked per family |

---

## 8. What has NOT been done yet

- End-to-end test with a real order
- Deploy `ordergine` dashboard to AWS (currently local-only)
- HTTPS / custom domain for the dashboard
- Auth hardening (WAF, IP allowlist, or Cognito) for team access
- Scale beyond 10 workers (need more proxies first)
- Move `MONGO_URI` and `PROXY_CRED_KEY` from plain env to Secrets-only
- Dynamic `WORKER_ID` derivation from ECS task metadata (for scaling to 200)
- Add NAT gateway EIP to Atlas IP allowlist if not already there
- Set up CloudWatch alarms for worker crashes and Redis memory

---

## 9. Repos

| Repo | Local path | Purpose |
|---|---|---|
| Trade_Flow_Backend | working dir | API + workers (`tradeflow-fetch` image) |
| ordergine (`gmail-id-dashboard_mongodb`) | separate folder | Dashboard (frontend + backend), not yet deployed to AWS |


## Pending: order-sync and order-cancel (added 2026-10-15)

Finished checkouts now hand their order to an `order-sync` queue, and the
dashboard's Cancel Order Units goes to an `order-cancel` queue. Both are served
by the same image as the workers, picked with `WORKER_ROLES`:

| Where | `WORKER_ROLES` | Notes |
|---|---|---|
| `tradeflow-worker-w-01` … `w-10` | `checkout,cancel` | add the env var; `CANCEL_CONCURRENCY=1` per worker is enough |
| **new** `tradeflow-sync` service (1 task) | `sync` | `deploy/ecs-task-definition-sync.json` — 0.5 vCPU / 1 GB, no Chrome, Flipkart order API over HTTP |

To create the sync service:

1. Fill the `REPLACE_*` values in `deploy/ecs-task-definition-sync.json` from the
   `tradeflow-worker` base task definition (execution role, Redis URL, secret ARNs).
2. `aws ecs register-task-definition --cli-input-json file://deploy/ecs-task-definition-sync.json`
3. `aws ecs create-service --cluster <cluster> --service-name tradeflow-sync --task-definition tradeflow-sync --desired-count 1 --launch-type FARGATE --network-configuration "awsvpcConfiguration={subnets=[<same as workers>],securityGroups=[sg-0e4a81a96183876c3],assignPublicIp=ENABLED}"`
4. Add `WORKER_ROLES=checkout,cancel` to the `tradeflow-worker` base and re-derive w-01…w-10
   (without it they default to all three roles and would ALSO run syncs — harmless, but
   then every worker holds a sync slot and the sweep schedule).

Notes:
- With `PROXY_MODE=required` the sync task binds one proxy from the pool as `sync-01`
  (11 proxies needed for 10 workers + sync, or set `PROXY_MODE=preferred` on it).
- Scale by `desired-count` or `SYNC_CONCURRENCY`; the sweep is one Redis schedule
  however many sync tasks run. Every sync takes the per-account lock checkouts take.
- The outcome of each sync is on the job: `orderSync.status` (`done | skipped | failed`)
  and `orderSync.lastError`; `POST /api/jobs/:id/order-sync` re-runs one.
