import type { APIRequestContext } from "playwright";
import {
  fetchOrderDetails,
  fetchOrderList,
  listUnits,
  mapApiUnitToOrder,
  type ApiUnit,
  type MappedOrderUnit,
} from "./orderApi.js";

/**
 * The API-mode half of order fetch: walk My Orders until the orders are older
 * than the since-date, and read one unit's details. Pure Flipkart I/O — the
 * Mongo writes and the job bookkeeping stay in orderFetch.ts, next to the
 * scraper's, so both modes persist through one code path.
 */

export interface ApiListedUnit {
  orderId: string;
  itemId: string;
  unitId: string;
  orderUrl: string;
  orderDate: Date | null;
  /** The list card's status key ("Delivered", "Order Not Placed"…). */
  status: string;
  statusText: string;
  amount: string;
  productName: string;
  unit: ApiUnit;
}

export interface ApiListWalk {
  units: ApiListedUnit[];
  pages: number;
  calls: number;
  /** True when the walk stopped because every order on a page was older than since. */
  stoppedAtSince: boolean;
}

/**
 * Pages are newest-first. Stop when a page holds nothing on/after `since`
 * (and at least one dated order), or when Flipkart says there is no more.
 * `maxPages` is a guard against an account with years of history.
 */
export async function walkOrderList(
  ctx: APIRequestContext,
  since: Date,
  opts: { maxPages?: number; onPage?: (info: { page: number; units: number; older: number; ms: number }) => void } = {}
): Promise<ApiListWalk> {
  const maxPages = Math.max(1, opts.maxPages ?? 60);
  const units: ApiListedUnit[] = [];
  const seen = new Set<string>();
  let next: Array<{ key: string; value: string }> = [];
  let pages = 0;
  let stoppedAtSince = false;
  for (let p = 1; p <= maxPages; p++) {
    const page = await fetchOrderList(ctx, p, next);
    pages += 1;
    const rows = listUnits(page);
    let older = 0;
    let dated = 0;
    for (const r of rows) {
      if (r.orderDate) {
        dated += 1;
        if (r.orderDate.getTime() < since.getTime()) older += 1;
      }
      if (seen.has(r.unitId)) continue;
      seen.add(r.unitId);
      units.push({
        orderId: r.orderId,
        itemId: r.itemId,
        unitId: r.unitId,
        orderUrl: `https://www.flipkart.com/order_details?order_id=${r.orderId}&item_id=${r.itemId}&unit_id=${r.unitId}`,
        orderDate: r.orderDate,
        status: r.status,
        statusText: r.statusText,
        amount: r.amount,
        productName: r.title,
        unit: r.unit,
      });
    }
    opts.onPage?.({ page: p, units: rows.length, older, ms: page.ms });
    if (dated > 0 && older === dated) {
      stoppedAtSince = true;
      break;
    }
    if (!page.moreOrder) break;
    next = page.nextCallParams;
  }
  return { units, pages, calls: pages, stoppedAtSince };
}

export interface ApiUnitRead {
  mapped: MappedOrderUnit;
  ms: number;
  bytes: number;
}

/** One unit's details, mapped. The list row (if any) fills gaps the details
 *  page leaves, e.g. the status text on a failed order. */
export async function readUnitViaApi(
  ctx: APIRequestContext,
  orderId: string,
  unitId: string,
  listUnit?: ApiUnit | null
): Promise<ApiUnitRead> {
  const details = await fetchOrderDetails(ctx, orderId, unitId);
  return { mapped: mapApiUnitToOrder(details.orderView, unitId, listUnit), ms: details.ms, bytes: details.bytes };
}
