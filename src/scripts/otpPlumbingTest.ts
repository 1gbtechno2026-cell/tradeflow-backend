/**
 * Proves the corporate OTP chain without a bank: lease a handset, feed the
 * webhook a synthetic forwarder SMS for that handset, and check the waiting
 * run receives exactly that code — and refuses one sent for another card.
 *
 *   npm run test:otp-plumbing -- ER648J            any claimable phone
 *   npm run test:otp-plumbing -- ER648J EMP0042    a specific employee
 *
 * Exercises leaseOtpPhone → recordIncomingSms → waitForPaymentOtp exactly as
 * a real payment does, in-process (no HTTP, no token). Side effects on the
 * real data: the leased row's lastOtp/lastOtpTime are stamped with the
 * synthetic code and an otpmessages audit row is written — the same as any
 * incoming SMS. The code is synthetic and is never printed.
 */
import { randomUUID } from "node:crypto";
import mongoose, { Types } from "mongoose";
import { connectDb } from "../db.js";
import { leaseOtpPhone, releaseOtpPhone } from "../services/employeePhoneLease.js";
import { OtpTimeoutError, recordIncomingSms, waitForPaymentOtp } from "../services/smsOtp.js";

const CARD_TYPE = "ICICI_CORP_VIRTUAL";
const PARENT_LAST4 = "8002";
const SYNTHETIC_OTP = "059171";

function forwarderPayload(phone: string, cardLast4: string, otp: string) {
  // The SMS-forwarder app's shape: subject names the receiving device, message wraps the SMS.
  return {
    subject: `SMS received at the number ${phone}`,
    message:
      `Incoming - ICICIB (SIM1)<br/>Message: ${otp} is the OTP for the transaction of INR 18128.00 ` +
      `on your ICICI Bank Card XX${cardLast4}. OTP is valid for 3 mins. Do not share with anyone. (${new Date().toISOString()})`,
  };
}

async function main() {
  const [corporateId, employeeId] = process.argv.slice(2);
  if (!corporateId) {
    console.error("usage: npm run test:otp-plumbing -- <corporateId> [employeeId]");
    process.exit(1);
  }
  await connectDb();
  const runId = `plumb-${randomUUID()}`;
  const jobId = new Types.ObjectId().toString();
  let failures = 0;
  const check = (name: string, ok: boolean) => {
    console.log(`  ${ok ? "ok  " : "FAIL"} ${name}`);
    if (!ok) failures += 1;
  };

  console.log(`\nLeasing a ${CARD_TYPE} handset under ${corporateId}${employeeId ? ` (employee ${employeeId})` : ""}…`);
  const lease = await leaseOtpPhone({ cardTypeName: CARD_TYPE, runId, corporateId, employeeId });
  console.log(`  leased ${lease.employeeId} (${lease.phoneNumber.slice(0, 2)}****${lease.phoneNumber.slice(-4)}) until ${lease.leasedUntil.toISOString()}`);

  try {
    // 1. A code for a DIFFERENT parent card must not be accepted.
    console.log("\n1. SMS naming another card (XX1234) — must be ignored");
    const wrongWait = waitForPaymentOtp({
      jobId,
      runId,
      phoneNumber: lease.phoneNumber,
      cardLast4: PARENT_LAST4,
      notBefore: new Date(Date.now() - 1000),
      timeoutMs: 6000,
    });
    await new Promise((r) => setTimeout(r, 500));
    await recordIncomingSms(forwarderPayload(lease.phoneNumber, "1234", "111111"));
    let ignored = false;
    try {
      await wrongWait;
    } catch (err) {
      ignored = err instanceof OtpTimeoutError;
    }
    check("code for XX1234 was not handed to a run paying with XX8002", ignored);

    // 2. The right card's code reaches the run.
    console.log("\n2. SMS naming XX8002 — must be delivered to this run");
    const notBefore = new Date();
    const rightWait = waitForPaymentOtp({
      jobId,
      runId,
      phoneNumber: lease.phoneNumber,
      cardLast4: PARENT_LAST4,
      notBefore,
      timeoutMs: 15000,
    });
    await new Promise((r) => setTimeout(r, 500));
    const rec = await recordIncomingSms(forwarderPayload(lease.phoneNumber, PARENT_LAST4, SYNTHETIC_OTP));
    check("webhook matched an active employeephones row for this handset", rec.matched);
    const got = await rightWait.catch((err) => (err instanceof Error ? err : new Error(String(err))));
    check("run received the code", typeof got === "string");
    check("received code is the one sent (6 digits, leading zero kept)", got === SYNTHETIC_OTP);
  } finally {
    const released = await releaseOtpPhone(lease.phoneId, runId);
    console.log(`\n  ${released ? "released" : "lease already lapsed"} ${lease.employeeId}`);
  }

  console.log(failures ? `\nFAIL — ${failures} check(s)` : "\nPASS — lease → webhook → run mailbox → card-verified delivery");
  await mongoose.disconnect();
  process.exit(failures ? 1 : 0);
}

main().catch(async (err) => {
  console.error(`\n✖ ${err instanceof Error ? err.message : err}`);
  await mongoose.disconnect().catch(() => {});
  process.exit(1);
});
