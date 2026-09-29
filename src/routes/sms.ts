import { timingSafeEqual } from "node:crypto";
import { Router, type NextFunction, type Request, type Response } from "express";
import { config } from "../config.js";
import { getLatestOtp, recordIncomingSms, SmsPayloadError } from "../services/smsOtp.js";

export const smsRouter = Router();

const BEARER = "Bearer ";

function tokenMatches(given: string, expected: string) {
  const a = Buffer.from(given);
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

// Mounted before the x-api-key gate: the Android SMS forwarder authenticates with its own Bearer token.
function requireSmsToken(req: Request, res: Response, next: NextFunction) {
  if (!config.smsApiToken) {
    res.status(503).json({ success: false, message: "SMS OTP webhook disabled — set SMS_API_TOKEN" });
    return;
  }
  const header = req.header("authorization") || "";
  if (!header.startsWith(BEARER) || !tokenMatches(header.slice(BEARER.length), config.smsApiToken)) {
    res.status(401).json({ success: false, message: "Unauthorized" });
    return;
  }
  next();
}

smsRouter.use(requireSmsToken);

smsRouter.post("/receive", async (req, res) => {
  try {
    const result = await recordIncomingSms(req.body);
    res.json({ success: true, matched: result.matched, receivedAt: result.receivedAt.toISOString() });
  } catch (err) {
    if (err instanceof SmsPayloadError) {
      res.status(400).json({ success: false, message: err.message });
      return;
    }
    console.error("[sms-otp] receive failed:", err instanceof Error ? err.message : err);
    res.status(500).json({ success: false, message: "Could not record SMS" });
  }
});

smsRouter.get("/latest", async (_req, res) => {
  try {
    res.json(await getLatestOtp());
  } catch (err) {
    res.status(500).json({ success: false, message: err instanceof Error ? err.message : "Could not read OTP" });
  }
});
