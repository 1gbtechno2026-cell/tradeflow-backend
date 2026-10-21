import mongoose, { Schema, Types } from "mongoose";

/**
 * The Proxy Pool — owned and written by the dashboard backend (its
 * ProxyPool model defines the schema and indexes). Trade Flow only CLAIMS a
 * row for a worker (assignedTo) and stamps usage and health on it: no
 * defaults, no inserts, no index builds, so it can never reshape rows the
 * dashboard created. Same arrangement as employeephones.
 */
export type ProxyStatus = "active" | "disabled" | "dead";

export interface IProxyPool {
  userId: Types.ObjectId;
  host: string;
  port: number;
  username: string;
  passwordEnc: string;
  status: ProxyStatus;
  assignedTo: string | null;
  assignedAt: Date | null;
  exitIp: string;
  latencyMs: number | null;
  lastCheckedAt: Date | null;
  lastOkAt: Date | null;
  lastError: string;
  consecutiveFails: number;
  lastUsedAt: Date | null;
  lastUsedJobId: string;
}

const ProxyPoolSchema = new Schema<IProxyPool>(
  {
    userId: Schema.Types.ObjectId,
    host: String,
    port: Number,
    username: String,
    passwordEnc: String,
    status: String,
    assignedTo: String,
    assignedAt: Date,
    exitIp: String,
    latencyMs: Number,
    lastCheckedAt: Date,
    lastOkAt: Date,
    lastError: String,
    consecutiveFails: Number,
    lastUsedAt: Date,
    lastUsedJobId: String,
  },
  { timestamps: true, autoIndex: false, autoCreate: false, collection: "proxy_pool" }
);

export const ProxyPool = mongoose.model<IProxyPool>("ProxyPool", ProxyPoolSchema, "proxy_pool");
