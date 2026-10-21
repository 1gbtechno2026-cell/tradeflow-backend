import crypto from "node:crypto";
import { config } from "../config.js";

/**
 * Decrypts a proxy password the dashboard stored (AES-256-GCM, layout
 * iv(12) || authTag(16) || ciphertext, base64). Decrypt only: this process
 * never writes a credential, so it never needs to encrypt one. The key is
 * PROXY_CRED_KEY and must equal the dashboard's.
 */
const ALGO = "aes-256-gcm";
const IV_LEN = 12;
const AUTH_TAG_LEN = 16;

function loadKey(): Buffer {
  if (!config.proxyCredKey) {
    throw new Error("PROXY_CRED_KEY is not set — required to read proxy passwords (copy the dashboard's value)");
  }
  const key = Buffer.from(config.proxyCredKey, "hex");
  if (key.length !== 32) {
    throw new Error(`PROXY_CRED_KEY must decode to exactly 32 bytes (got ${key.length})`);
  }
  return key;
}

export function decryptProxySecret(ciphertext: string): string {
  const raw = Buffer.from(ciphertext, "base64");
  if (raw.length < IV_LEN + AUTH_TAG_LEN) throw new Error("Stored proxy password is malformed");
  const iv = raw.subarray(0, IV_LEN);
  const authTag = raw.subarray(IV_LEN, IV_LEN + AUTH_TAG_LEN);
  const encrypted = raw.subarray(IV_LEN + AUTH_TAG_LEN);
  const decipher = crypto.createDecipheriv(ALGO, loadKey(), iv, { authTagLength: AUTH_TAG_LEN });
  decipher.setAuthTag(authTag);
  return Buffer.concat([decipher.update(encrypted), decipher.final()]).toString("utf8");
}
