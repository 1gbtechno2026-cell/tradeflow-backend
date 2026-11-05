#!/usr/bin/env bash
#
# standalone.sh — run a STANDALONE copy of the Trade Flow backend on a second Mac.
#
# Idempotent: the first run sets everything up; every run after pulls your
# latest pushed code, rebuilds, and restarts the processes. So your workflow is:
#
#     (primary Mac)   commit your change, then `git push`
#     (this Mac)      ./standalone.sh          # pulls + restarts, change is live
#
# The API runs with `tsx watch`, so if you also edit code ON this Mac the API
# reloads on save by itself; the worker is restarted by re-running this script.
#
# STANDALONE = its own data. Before the first run, copy your primary Mac's
# `.env` here (AirDrop/scp), then change ONLY these keys so this copy can never
# touch production:
#     MONGO_URI        -> a SEPARATE database   (e.g. end the path with /tradeflow_dev)
#     REDIS_URL        -> redis://127.0.0.1:6379 (the local Redis this script starts)
#     PAYMENT_DRY_RUN  -> true                   (dev copy: reach the pay page, buy nothing)
#     PROXY_MODE       -> off                    (leave from this Mac, no proxy pool)
#     WORKER_ID        -> mac-m4-standalone      (a name that is not your primary's)
# Keep PROXY_CRED_KEY and API_KEY the same as primary.
#
set -euo pipefail
cd "$(dirname "$0")"

echo "==> Node $(node -v 2>/dev/null || echo MISSING)  (primary uses v26 — match it with nvm if these differ)"
command -v pm2 >/dev/null 2>&1 || { echo "==> installing pm2 globally"; npm i -g pm2; }

# Local Redis for the BullMQ queue.
if ! redis-cli ping >/dev/null 2>&1; then
  echo "==> starting local Redis"
  brew services start redis 2>/dev/null || { echo "!! Redis not found. Run: brew install redis"; exit 1; }
  sleep 1
fi

if [ ! -f .env ]; then
  echo "!! No .env here yet."
  echo "   Copy your primary Mac's Trade_Flow_Backend/.env to this folder (AirDrop/scp),"
  echo "   then change the keys listed in the header of this script. Re-run when done."
  exit 1
fi

echo "==> git pull"
git pull --ff-only || echo "!! pull skipped (local changes or diverged) — resolve, then re-run"

echo "==> npm install"
npm install

echo "==> Playwright Chromium"
npx playwright install chromium

echo "==> build (tsc)"
npm run build

echo "==> (re)start processes under pm2"
pm2 describe tf-api    >/dev/null 2>&1 && pm2 restart tf-api    || pm2 start npm --name tf-api    -- run dev
pm2 describe tf-worker >/dev/null 2>&1 && pm2 restart tf-worker || pm2 start npm --name tf-worker -- run worker
pm2 save >/dev/null

echo "==> done.  API + worker are up.   Logs: pm2 logs    Status: pm2 ls    Stop: pm2 delete all"
