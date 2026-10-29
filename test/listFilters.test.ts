import { test } from "node:test";
import assert from "node:assert/strict";
import { containsRegex, listParam, paginationMeta, parseDateRange, parsePagination, rangeFilter } from "../src/services/listFilters.js";

test("listParam accepts repeated keys and comma lists, trims, drops empties", () => {
  assert.deepEqual(listParam({ status: "a, b,,c " }, "status"), ["a", "b", "c"]);
  assert.deepEqual(listParam({ status: ["a", "b,c"] }, "status"), ["a", "b", "c"]);
  assert.deepEqual(listParam({}, "status"), []);
});

test("parseDateRange: relative keys give a from and no to", () => {
  const before = Date.now();
  const r = parseDateRange({ range_mode: "relative", range_key: "last_24h" });
  assert.equal(r.mode, "relative");
  assert.ok(r.from && Math.abs(before - 24 * 3600e3 - r.from.getTime()) < 5000);
  assert.equal(r.to, null);
  const d = parseDateRange({ range_key: "last_14d" });
  assert.ok(d.from && Math.abs(before - 14 * 864e5 - d.from.getTime()) < 5000);
});

test("parseDateRange: absolute IST calendar days are inclusive and win over a relative key", () => {
  const r = parseDateRange({ range_mode: "relative", range_key: "last_24h", start: "2026-10-28", end: "2026-10-28" });
  assert.equal(r.mode, "absolute");
  assert.equal(r.from?.toISOString(), "2026-10-27T18:30:00.000Z");
  assert.equal(r.to?.toISOString(), "2026-10-28T18:29:59.999Z");
  const open = parseDateRange({ start: "2026-10-28" });
  assert.equal(open.to, null);
  assert.deepEqual(rangeFilter(open), { $gte: open.from });
});

test("parseDateRange: unknown or missing key is all time, with no filter fragment", () => {
  const r = parseDateRange({ range_key: "whatever" });
  assert.equal(r.mode, "all");
  assert.equal(rangeFilter(r), null);
  assert.equal(parseDateRange({}).mode, "all");
  assert.equal(parseDateRange({ range_mode: "all", range_key: "last_7d" }).mode, "all");
});

test("parsePagination clamps page and page_size and honours legacy limit", () => {
  assert.deepEqual(parsePagination({}, { pageSize: 100, maxPageSize: 500 }), { page: 1, pageSize: 100, skip: 0 });
  assert.deepEqual(parsePagination({ page: "3", page_size: "50" }, { pageSize: 100, maxPageSize: 500 }), { page: 3, pageSize: 50, skip: 100 });
  assert.equal(parsePagination({ page_size: "9999" }, { pageSize: 100, maxPageSize: 500 }).pageSize, 500);
  assert.equal(parsePagination({ page: "-4" }, { pageSize: 100, maxPageSize: 500 }).page, 1);
  assert.equal(parsePagination({ limit: "80" }, { pageSize: 100, maxPageSize: 500 }).pageSize, 80);
});

test("paginationMeta reports pages correctly, never zero", () => {
  assert.deepEqual(paginationMeta(0, { page: 1, pageSize: 50, skip: 0 }), { count: 0, num_pages: 1, current_page: 1, page_size: 50 });
  assert.equal(paginationMeta(101, { page: 1, pageSize: 50, skip: 0 }).num_pages, 3);
});

test("containsRegex escapes user input and is case-insensitive", () => {
  const rx = containsRegex("a.b(c)");
  assert.ok(rx.test("xxA.B(C)yy"));
  assert.ok(!rx.test("axb(c)"));
});
