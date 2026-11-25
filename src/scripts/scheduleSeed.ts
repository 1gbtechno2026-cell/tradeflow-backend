/**
 * The schedule seed, by hand (services/scheduleSettings.ts seedSchedules).
 *
 *   npx tsx src/scripts/scheduleSeed.ts --check   what a seed would write for the workspace in TRADE_FLOW_USER_ID —
 *                                                 the 4 switches and the 4 clocks — and what is already there.
 *                                                 Reads only; writes nothing.
 *   npx tsx src/scripts/scheduleSeed.ts --go      write what is missing, now. The API and the worker do the same by
 *                                                 themselves at their next start; this is for doing it before that.
 *
 * Without a flag it refuses. A seed copies THIS machine's .env into switches that every machine then shares (each
 * sweep starts as this machine runs it today; the nightly fetch always starts off), so it is never a default action:
 * run it on the machine whose .env the switches should start from. What exists is never changed, by either flag.
 *
 * Same as:  npm run schedules:seed -- --check
 */
import { pathToFileURL } from "node:url";
import mongoose from "mongoose";
import { config } from "../config.js";
import { clockStamp } from "../services/scheduleClock.js";
import { ELIGIBILITY_ENV, seedPlan, seedSchedules, type SeedOptions, type SeedPlan } from "../services/scheduleSettings.js";

type Out = (line: string) => void;

const pad = (text: string, width: number) => text.padEnd(width);

export function printSeedPlan(plan: SeedPlan, out: Out, verb: "would write" | "wrote"): void {
  out(`workspace ${plan.userId} · this machine: system id "${plan.systemId}", host "${plan.host}"`);
  out(`may run here (.env): ${plan.switches.map((s) => `${s.name} ${plan.eligible[s.name] ? "yes" : `no (${ELIGIBILITY_ENV[s.name]})`}`).join(" · ")}`);
  out("");
  out("switches — order_sync_settings.schedules.<name>");
  for (const s of plan.switches) {
    const what = s.write ? `${verb} ${s.on ? "ON " : "OFF"}` : `exists: ${s.on ? "ON " : "OFF"}`;
    const who = s.current ? ` (changed by ${s.current.changedBy || "?"}${s.current.changedAt ? `, ${clockStamp(s.current.changedAt)}` : ""})` : "";
    out(`  ${pad(s.name, 14)} ${pad(what, 16)} ${s.why}${who}`);
  }
  out("");
  out("clocks — order_schedule_leases");
  for (const c of plan.clocks) {
    const what = c.write ? `${verb === "wrote" ? "created" : "would create"}` : "exists";
    out(`  ${pad(c.name, 14)} ${pad(what, 13)} ${c.text} ${c.clock.tz} · may start up to ${c.clock.lateMin} min late · runs on ${c.clock.preferSystemId || "whichever machine looks first"} · next ${clockStamp(c.nextAt, c.clock.tz)}`);
  }
  for (const note of plan.notes) out(`note: ${note}`);
}

/**
 * The command itself: answers the exit code. `io` is for scripts/scheduleLeaseCheck.ts, which runs it against
 * throwaway collections and an invented workspace.
 */
export async function runScheduleSeedCli(argv: string[], io: { userId?: string; out?: Out; seed?: SeedOptions } = {}): Promise<number> {
  const out = io.out || ((line: string) => console.log(line));
  const check = argv.includes("--check");
  const go = argv.includes("--go");
  if (check === go) {
    out("Schedule seed — nothing done.");
    out("  --check   print the 4 switches and 4 clocks a seed would write for TRADE_FLOW_USER_ID; writes nothing");
    out("  --go      write the ones that are missing (the API and the worker do this themselves at their next start)");
    out(check ? "Give one of the two, not both." : "A flag is required: a seed copies this machine's .env into switches every machine shares.");
    return 2;
  }
  const userId = io.userId ?? config.userId;
  if (!userId) {
    out("TRADE_FLOW_USER_ID is not set — there is no workspace to seed.");
    return 2;
  }
  const opts = io.seed || {};
  if (check) {
    const plan = await seedPlan(userId, opts);
    out("Schedule seed — CHECK (reads only; nothing is written)");
    printSeedPlan(plan, out, "would write");
    out("");
    out(`would write ${plan.switches.filter((s) => s.write).length} switch(es) and ${plan.clocks.filter((c) => c.write).length} clock(s); nothing was written.`);
    return 0;
  }
  const done = await seedSchedules(userId, opts);
  out("Schedule seed — GO");
  printSeedPlan(done.plan, out, "wrote");
  out("");
  out(`wrote ${done.switchesWritten.length} switch(es)${done.switchesWritten.length ? ` (${done.switchesWritten.join(", ")})` : ""} and ${done.clocksWritten.length} clock(s)${done.clocksWritten.length ? ` (${done.clocksWritten.join(", ")})` : ""}; what existed was left as it was.`);
  return 0;
}

async function main() {
  const argv = process.argv.slice(2);
  // Nothing is read from the database for a refusal.
  if (argv.includes("--check") === argv.includes("--go")) {
    process.exitCode = await runScheduleSeedCli(argv);
    return;
  }
  await mongoose.connect(config.mongoUri, { autoIndex: false, autoCreate: false, serverSelectionTimeoutMS: 15_000 });
  try {
    process.exitCode = await runScheduleSeedCli(argv);
  } finally {
    await mongoose.disconnect();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error(err instanceof Error ? err.message : err);
    process.exitCode = 1;
  });
}
