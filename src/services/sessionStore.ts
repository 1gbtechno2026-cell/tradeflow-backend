import { Types } from "mongoose";
import { config } from "../config.js";
import { Address } from "../models/Address.js";
import { Gst } from "../models/Gst.js";
import { PlatformId, type IPlatformId } from "../models/PlatformId.js";
import type { AddressDetails } from "../types.js";

const OBJECT_ID_RE = /^[a-f0-9]{24}$/i;

export type SessionLookup =
  | { ok: true; email: string; platformId: string; cookies: unknown[] }
  | { ok: false; email: string; reason: string };

function cookiesFor(row: Pick<IPlatformId, "sessionCookies" | "sessionState"> | null) {
  const fromState = row?.sessionState?.cookies;
  if (Array.isArray(fromState) && fromState.length) return fromState;
  return Array.isArray(row?.sessionCookies) ? row.sessionCookies : [];
}

export function workspaceUserId() {
  if (!config.userId) {
    throw new Error("TRADE_FLOW_USER_ID is required (og_user workspace id)");
  }
  return config.userId;
}

export async function resolveLoggedInSession(idOrEmail: string): Promise<SessionLookup> {
  // Read-only: never update PlatformId status or cookies.
  const trimmed = String(idOrEmail || "").trim();
  const userId = workspaceUserId();
  const row = OBJECT_ID_RE.test(trimmed)
    ? await PlatformId.findOne({ _id: trimmed, userId }).lean()
    : await PlatformId.findOne({ userId, email: trimmed.toLowerCase() }).lean();

  if (!row) {
    return { ok: false, email: trimmed, reason: "Platform ID not found" };
  }

  if (row.status !== "logged_in") {
    return {
      ok: false,
      email: row.email,
      reason: `Status is "${row.status}", not logged_in`,
    };
  }

  if (!row.sessionSavedAt) {
    return { ok: false, email: row.email, reason: "No sessionSavedAt — session was never stored" };
  }

  const cookies = cookiesFor(row);
  if (!cookies.length) {
    return { ok: false, email: row.email, reason: "No saved session cookies" };
  }

  return {
    ok: true,
    email: row.email,
    platformId: String(row._id),
    cookies,
  };
}

export async function resolveAddress(input: {
  address?: AddressDetails;
  addressId?: number;
  gstId?: number;
}): Promise<AddressDetails> {
  if (input.address) {
    return {
      ...input.address,
      mobile: input.address.mobile || "",
      locality: input.address.locality || "",
      addressType: input.address.addressType || "Home",
      checkoutPincode: input.address.checkoutPincode || "",
    };
  }

  if (input.addressId == null) {
    throw new Error("Provide address or addressId");
  }

  const userId = workspaceUserId();
  const row = await Address.findOne({ userId, id: input.addressId }).lean();
  if (!row) throw new Error(`Address ${input.addressId} not found`);

  let gstNumber = "";
  let companyName = "";
  if (input.gstId != null) {
    const gst = await Gst.findOne({ userId, id: input.gstId }).lean();
    if (!gst) throw new Error(`GST ${input.gstId} not found`);
    gstNumber = gst.gstNumber;
    companyName = gst.businessName;
  } else if (row.linkedGstIds?.length) {
    const gst = await Gst.findOne({
      userId,
      _id: { $in: row.linkedGstIds as Types.ObjectId[] },
    }).lean();
    if (gst) {
      gstNumber = gst.gstNumber;
      companyName = gst.businessName;
    }
  }

  const line1 = [row.addressLine1, row.floorNumber].filter(Boolean).join(", ");
  return {
    name: row.contactName,
    mobile: "",
    pincode: row.pincode,
    locality: row.addressLine2 || "",
    addressLine1: line1,
    city: row.city,
    state: row.state,
    addressType: "Home",
    gstNumber,
    companyName,
  };
}
