# Schedules — one clock per schedule, shared by every machine

Trade Flow runs four things on a schedule. Each has **one clock, kept in the database**, and **one switch, set in the
dashboard** (Settings → Scheduled fetch). Every machine that runs Trade Flow on the same database looks at the same
clock: one machine runs a time, and another takes over when that machine is away.

| Schedule (`name`) | In Settings | Fixed times (India time) | Runs in | May start late by |
|---|---|---|---|---|
| `fetch` | Nightly fetch | 05:00 every day | the API | 3 h, and once more if its run died |
| `update_active` | Update all active units | 00:15 · 06:15 · 12:15 · 18:15 | the worker (`update` role) | 3 h |
| `update_ofd` | Out-for-delivery recheck | every 2 h at :45 (00:45, 02:45, …) | the worker (`update` role) | 1 h |
| `invoice_sweep` | Invoice sweep | every 3 h at :35 (00:35, 03:35, …) | the worker (`invoice` role) | 1 h 30 |

Before this, each machine had its own clocks: BullMQ schedulers in its own Redis for the three sweeps and a cron
timer in its own API for the fetch. They drifted (a sweep ran 92 minutes after the last one when Redis came back
empty), skipped a 05:00 the machine slept through, and two machines would both have run every time.

## Who decides what

| Question | Decided by | Where |
|---|---|---|
| Does the schedule run at all? | its **switch** | dashboard → Settings → Scheduled fetch; stored on `order_sync_settings.schedules.<name>` |
| When? | its **clock** | `order_schedule_leases` (`clock`, `nextAt`) |
| May *this machine* be the one that runs it? | this machine's **`.env`** | `UPDATE_SCHEDULE`, `INVOICE_SCHEDULE`, `FETCH_SCHEDULE` |
| Which process of the machine? | the kind of work | the API for the fetch; a worker whose `WORKER_ROLES` has `update` / `invoice` for the sweeps |
| Which machine first? | the clock's **preferred machine** | `clock.preferSystemId`; the others wait 2 minutes for it |

Only a switch that is stored as on runs a schedule. The `.env` can never switch one on.

## The `.env` lines

They say only whether this machine may run a schedule. They do not switch anything on or off for anybody else.

| Line | Meaning |
|---|---|
| `UPDATE_SCHEDULE=0` | this machine never runs the two update sweeps. Missing, or `1`: it may. |
| `INVOICE_SCHEDULE=0` | this machine never runs the invoice sweep. Missing, or `1`: it may. Set it on a machine that has no `INVOICE_S3_BUCKET` — such a machine can only skip a time it took. |
| `FETCH_SCHEDULE=0` | this machine never runs the nightly fetch. The same when the line is missing. `1`, or a time such as `0 5 * * *`: it may. |
| `TRADEFLOW_SYSTEM_ID`, `TRADEFLOW_HOST_ID` | this machine's name for the lease and for every row it writes. **Each machine needs its own.** Two machines under one system id count as one: each would carry on the other's runs and neither would take over. Trade Flow says so in its log when it sees it. |

`0`, `off`, `false` and `no` all mean "never on this machine". The Update, Fetch and Invoices buttons work whatever
these lines say.

Read **once**, by the first start on a workspace (the seed, below), and never again: the time in `FETCH_SCHEDULE`
(`M H * * *`; anything else gives 05:00), `CRON_TIMEZONE` (`Asia/Kolkata` or `UTC` — the clock knows no other),
`UPDATE_SWEEP_MS`, `UPDATE_OFD_SWEEP_MS`, `INVOICE_SWEEP_MS`. Changing them afterwards does nothing.

## The seed

When the API or the worker starts, it writes what the workspace does not have yet — and only that:

- the four switches. Each sweep starts as the starting machine's `.env` has it (`UPDATE_SCHEDULE=0` there → both
  update switches start off). **The nightly fetch always starts off**, whatever `FETCH_SCHEDULE` says: it is switched
  on in Settings.
- the four clocks, with the starting machine as the preferred one.

A switch somebody has set and a clock that exists are never touched, so a second start, the other process and the
other machine change nothing. That is also why the seed should run first on the machine whose `.env` the switches
are to start from.

`npm run schedules:seed -- --check` prints what a seed would write for this machine's workspace, and writes nothing.
`-- --go` writes it now instead of at the next start. Without a flag the script refuses.

## How a fixed time is dealt with

Every 30 seconds (± 5) each process looks at the clocks it serves. When a time is due, **one** write to the clock
decides what happens to it — whichever machine's write lands first, the others' do nothing:

- **taken** — the lease is free: this machine holds it for 5 minutes and starts the work. Tick: `ran on <host>`.
- **skipped** — switched off, or the run of an earlier time still holds a live lease.
- **missed** — no machine that may run it looked within the late limit.

A process that serves a schedule but may not run it (its .env says 0) never takes a time. It still records
`skipped — switched off` on time while the switch is off, so the next time moves on instead of reading overdue for the
late limit; with the switch on it leaves the time to a machine that may run it and records `missed` only after the
late limit.

The next time is always the next one after *now*: a machine that slept ten hours deals with one time, not five.

While its run is open the machine renews the lease on every look; when the run ends it gives the lease back with how
it went (`finished — ok 926, failed 3`). A machine that goes quiet (asleep, stopped, no network) loses the lease after
5 minutes. The next fixed time is then taken by another machine (`took over on <host>`), and when the first machine
comes back it is told: it stops what it had left of that run (`superseded`). If nobody took over, it simply carries on.
A new process of the same machine carries on the lease of a run that is still open — a worker restart in the middle
of a sweep changes nothing.

A machine that is not the preferred one waits 2 minutes before it takes a time, but only while the preferred machine
was seen in the last 10 minutes and may run that schedule.

The first time a machine that may run a schedule serves its clock, a time that is already due is **not** run late:
the tick says `skipped — the shared clock started on <host> after this time`, and the first run is the next fixed
time. Nothing sweeps because a worker was restarted.

The nightly fetch alone may be started a second time for the same night: when its run did not finish (its machine went
quiet, or the API restarted under it), within 3 hours of the time. Never a third time.

## What runs under the lease

**Update sweeps.** A time that was taken makes one update request. The request carries `schedule { name, slotAt,
token }` and its params say `schedule_name`, `schedule_slot` (India time), `ran_on` and `attempt`; Batch requests →
Details shows them as *Schedule: Update all active units · 2026-11-25 18:15 · ran on Mac*. When the last account
ends, the lease is given back. With no unit to refresh there is one tick (`ran on Mac — nothing to refresh`) and no
request. A sweep request this machine made that is still open makes the next time `skipped`.
An account job of a sweep that another machine has taken over since ends before the account is locked, its session
looked up or Flipkart called: the account reads *Superseded — Mac-2 took over the update of all active units at
18:15*, and everything the request had left ends with it.

The out-for-delivery recheck selects every spelling of that status: the order API stores `ofd`, the page wrote
`Out for delivery`. A unit read over the API that is out for delivery and has no OTP gets one page read for the OTP;
a unit whose page shows none either (cash on delivery) is left as it is — no failure, no alert.

**Invoice sweep.** A time that was taken becomes one job on this machine's invoice queue (job id
`sched-invoice-<time>`), which runs one job at a time. The job's ledger run (`order_job_runs`, type `invoices`) is the
lease's run; its end gives the lease back (`finished — captured 3/3, re-captured 2`, or `ran on Mac — nothing to do`
when neither phase had an order).
When the run has orders it is also a request of the log, which carries the same *Schedule* line.

**Nightly fetch.** Before anything else the ledger row is written (`queued`), so an API restart while 21,000
accounts are being prepared reads as a fetch that failed, not as a night nobody started. A time is skipped while a
fetch — anybody's — runs in this API, or was seen running on another machine in the last 10 minutes (a running fetch
touches its ledger row once a minute). When the fetch ends, the update of the in-progress units is queued, as before.

## The buttons

- **Run the scheduled fetch now** (`POST /api/orders/fetch/schedule/run`) and **Capture newly delivered now**
  (`POST /api/orders/invoices/sweep`) take the lease by hand. They work with the switch off and on a machine the
  schedule never uses, and use up no fixed time.
- While **another machine** holds the lease they answer **409**: *The invoice sweep is running on Mac-2 since 12:35*
  (`code: "held_elsewhere"`, `holder`).
- While this machine is sweeping, a second invoice sweep is queued behind the first, as it always was.

## Routes (all behind `x-api-key`)

| Route | Answer |
|---|---|
| `GET /api/orders/schedules` | `{ timezone, now, here { systemId, host, eligible }, machines[], schedules[4] }`. Per schedule: `on`, `source` (`ui` / `seed` / `unset`), `changedBy`, `changedAt`, `eligibleHere`, `clock`, `nextAt`, `overdue`, `lease` (who holds it, `live`), `lastRun`, `lastTick`, `ticks` (newest 20), `servedOn`, `runnableOn`, `legacy`, and `state` + `note`: one word and one sentence for where it stands. |
| `PUT /api/orders/schedules/:name` | body `{ on?: boolean, preferSystemId?: string, by }`. 400 for an unknown name, an `on` that is not true / false, or nothing to save — checked before anything is written. Answers what the GET answers. |
| `GET /api/orders/fetch/schedule` | the keys it always had; `enabled` is now the switch and `lastTick` the clock's. New: `eligibleHere`, `nextAt`, `lease`. |
| `GET /api/orders/update/settings` | the keys it always had; `schedule.enabled` is the *Update all active units* switch. New: `ofdEnabled`, `activeNextAt`, `ofdNextAt`, `eligibleHere`. |

`state` is one of `running`, `scheduled`, `off`, `no_machine` (switched on, but no machine that may run it is up),
`not_seeded`, and `own_clock` — this machine's Redis still holds the sweep's old BullMQ scheduler, so a worker that
has not been restarted yet still runs it on its own clock, whatever the switch says.

A tick's `outcome` is `ran`, `took_over`, `skipped`, `missed`, `finished`, `failed` or `superseded`; `reason` is the
sentence, `runRef { kind, id }` the run — an update request, or a ledger run for the fetch and the invoice sweep
(the invoice sweep's request of the log, when it has one, is that ledger row's `refId`). The lease's claim token is
never in an answer.

## Collections

- `order_sync_settings.schedules.<name>` — `{ on, changedBy, changedAt }`. No path = never set = it does not run.
- `order_schedule_leases` — one document per workspace and schedule: the clock, `nextAt`, the holder and its
  expiry, the run, the last 50 ticks.
- `order_schedule_hosts` — one row per process that runs the clock (`<systemId>:api`, `<systemId>:worker`), written
  once a minute: which schedules it serves and may run. Dropped 7 days after it was last written.

Both collections and their indexes are created by the seed, by nothing else.

## Going live on a machine

1. **The API starts on this code.** It seeds (above), then serves the `fetch` clock: it writes its presence, records
   `skipped — switched off` at 05:00 while the switch is off, and runs the fetch only if the switch is on and
   `FETCH_SCHEDULE` lets this machine.
2. **Until the worker is restarted** it keeps its old BullMQ schedulers and runs the sweeps on its own clock; the
   switches do not reach it. `GET /api/orders/schedules` says so (`own_clock`). The API never runs a sweep, and
   *Capture newly delivered now* still queues a job for that worker as it always did.
3. **The worker starts on this code.** It removes `order-update-active`, `order-update-ofd` and
   `order-invoice-sweep` from this machine's Redis, each with its next job (`order-sync-sweep` stays), and serves
   the three sweep clocks from then on. Its first run of each is the next fixed time.

Read-only checks afterwards:

```bash
redis-cli -p 6376 ZRANGE bull:order-update:repeat 0 -1    # empty
redis-cli -p 6376 ZRANGE bull:order-invoice:repeat 0 -1   # empty
curl -s -H "x-api-key: $API_KEY" localhost:4100/api/orders/schedules   # 4 schedules, each with a future nextAt
```

The first fixed time then writes a tick: `ran on <host>`, or `skipped — switched off`.

**Another machine.** Until it runs this code it ignores the switches and the leases: keep its `UPDATE_SCHEDULE`,
`INVOICE_SCHEDULE` and `FETCH_SCHEDULE` at `0`. When it has the code, give it its own `TRADEFLOW_SYSTEM_ID` /
`TRADEFLOW_HOST_ID` first. To let it take over when the first machine is away, remove its `UPDATE_SCHEDULE` and
`INVOICE_SCHEDULE` lines and set `FETCH_SCHEDULE=1`; to keep it out, leave the three at `0`.

**Moving to a server.** Set each clock's preferred machine to the server (`PUT …/schedules/:name` with
`preferSystemId`), and set the three lines to `0` on the machines that should no longer run anything — otherwise a
machine that is awake stands by and runs any time the server misses.

## Things to know

- A switch changes the next time. A run in progress finishes; stop it in Batch requests.
- The update that follows a nightly fetch belongs to the fetch: it obeys the fetch switch, not the update switches.
- There is no account lock across machines. In the half minute between a machine waking and its clock seeing that it
  was superseded, both machines can work the same account.
- The dashboard's own timers (price scraper, session checks, the 04:30 e-mail) are not on this clock.

## Checks

| Command | What it proves | Touches |
|---|---|---|
| `npm run test:schedule-lease` | the clock and the lease: 50 machines on one time → one runs it; off, still running, missed, takeover, standby grace, the fetch's second start; the seed; the routes | throwaway collections, Redis db 15 |
| `npm run test:schedule-sweep` | the update sweeps and the invoice sweep under the lease, the superseded account job, the retired schedulers, the `ofd` spelling and its OTP page pass, *Capture newly delivered now* | invented rows of an invented workspace, Redis db 15 |
| `npm run test:fetch-schedule` | the nightly fetch under the lease with the fetch itself injected: not twice, the restart in the middle, the takeover, *Run now* | invented ledger rows of an invented workspace |
| `npm run schedules:seed -- --check` | what the seed would write here | reads only |

None of them calls Flipkart, opens Chrome, starts a worker or puts a job on a live queue. Each removes what it made
and prints what was left (it must be nothing). A run that is stopped (Ctrl-C, kill) still cleans up; what a run that
was killed outright left is removed by the next run of the same check once it is 30 minutes old.

Run the schedule checks one after another, and never next to `test:start-requests`: `test:schedule-sweep` writes
its invented rows into the real `order_update_requests` and `order_job_runs` for a moment, and `test:start-requests`
ends by comparing whole-collection counts, so it fails while another check's rows are there (neither check is wrong).
