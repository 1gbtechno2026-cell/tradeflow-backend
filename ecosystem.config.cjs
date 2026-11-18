// pm2 process file for the Trade Flow server — one supervised command.
//
//   npm run build && pm2 start ecosystem.config.cjs     # start API + worker
//   pm2 logs            # tail both
//   pm2 restart tf-worker   # bounce just the worker (e.g. after new code)
//   pm2 stop all / pm2 delete all
//
// Why pm2 here: it auto-restarts a crashed process so jobs never sit with no
// consumer, and `instances: 1` on the worker GUARANTEES there is never a second
// worker competing for the same per-account locks (the cardinal rule).
module.exports = {
  apps: [
    {
      name: "tf-api",
      script: "dist/index.js",
      instances: 1,
      exec_mode: "fork",
      max_memory_restart: "800M",
      time: true,
    },
    {
      name: "tf-worker",
      script: "dist/worker.js",
      // Exactly one worker. Do NOT raise this — concurrency is controlled inside
      // the worker via WORKER_CONCURRENCY / CANCEL_CONCURRENCY, not by forking.
      instances: 1,
      exec_mode: "fork",
      max_memory_restart: "1500M",
      time: true,
    },
  ],
};
