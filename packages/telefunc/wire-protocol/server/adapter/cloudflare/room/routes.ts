/// <reference types="@cloudflare/workers-types" />
// The readiness invariant (I7) and the CF handshake of readiness-ordering.md §2.3. One row per
// (inc, lane_key, subscriber); the CURRENT leaseId is a VALUE, so re-establishment after a renewal loss
// atomically REPLACES the prior lease in one UPSERT and can never coexist with an unexpired old row —
// that is what makes the renewal-loss-before-expiry path exactly-once (no duplicate delivery). Routes are
// inc-scoped, so a surviving old-inc subscription can never be a target of a recreated room (I11).

export const ROUTE_TTL_MS = 90_000
export const ROUTE_RENEW_EVERY_MS = ROUTE_TTL_MS / 3
export const ROUTE_RENEW_FAILURE_LIMIT = 2
export const ROUTE_DELIVERY_FAILURE_LIMIT = 3
// A capture pin is refreshed by every successful establishment/renewal and shares the route lease's
// authority-bounded lifetime. That comfortably contains the five-attempt initial retry schedule, while
// a crashed lifecycle with no route activity becomes mechanically reclaimable.
export const ROUTE_CAPTURE_TTL_MS = ROUTE_TTL_MS

export type RouteTarget = { subscriberDoId: string; leaseId: string; generationToken: string }
export type RouteInstallation = {
  roomId: string
  inc: string
  laneKey: string
  subscriberDoId: string
  leaseId: string
  generationToken: string
}

// Establishment: the open-head check happens in the DO (it holds the head); this UPSERT replaces any
// prior lease for the same (inc, lane, subscriber) in a single statement.
export function upsertRoute(
  sql: SqlStorage,
  roomId: string,
  inc: string,
  laneKey: string,
  subscriberDoId: string,
  leaseId: string,
  generationToken: string,
  now: number,
  ttlMs: number = ROUTE_TTL_MS,
): number {
  const expiresAt = now + ttlMs
  sql.exec(
    'INSERT OR REPLACE INTO route (room_id, inc, lane_key, subscriber_do_id, lease_id, generation_token, expires_at, failures) VALUES (?, ?, ?, ?, ?, ?, ?, 0)',
    roomId,
    inc,
    laneKey,
    subscriberDoId,
    leaseId,
    generationToken,
    expiresAt,
  )
  return expiresAt
}

export function listRouteInstallations(sql: SqlStorage, inc: string): RouteInstallation[] {
  return sql
    .exec<{
      room_id: string
      inc: string
      lane_key: string
      subscriber_do_id: string
      lease_id: string
      generation_token: string
    }>('SELECT room_id, inc, lane_key, subscriber_do_id, lease_id, generation_token FROM route WHERE inc = ?', inc)
    .toArray()
    .map((row) => ({
      roomId: row.room_id,
      inc: row.inc,
      laneKey: row.lane_key,
      subscriberDoId: row.subscriber_do_id,
      leaseId: row.lease_id,
      generationToken: row.generation_token,
    }))
}

export function listExpiredRouteInstallations(sql: SqlStorage, now: number): RouteInstallation[] {
  return sql
    .exec<{
      room_id: string
      inc: string
      lane_key: string
      subscriber_do_id: string
      lease_id: string
      generation_token: string
    }>(
      'SELECT room_id, inc, lane_key, subscriber_do_id, lease_id, generation_token FROM route WHERE expires_at <= ? OR failures >= ?',
      now,
      ROUTE_DELIVERY_FAILURE_LIMIT,
    )
    .toArray()
    .map((row) => ({
      roomId: row.room_id,
      inc: row.inc,
      laneKey: row.lane_key,
      subscriberDoId: row.subscriber_do_id,
      leaseId: row.lease_id,
      generationToken: row.generation_token,
    }))
}

// Renewal compares all four fields (inc, lane_key, subscriber, leaseId); a stale lease id matches nothing
// and the renewal is lost.
export function renewRoute(
  sql: SqlStorage,
  inc: string,
  laneKey: string,
  subscriberDoId: string,
  leaseId: string,
  now: number,
  ttlMs: number = ROUTE_TTL_MS,
): { ok: true; expiresAt: number } | { ok: false } {
  const expiresAt = now + ttlMs
  const changed = sql.exec(
    'UPDATE route SET expires_at = ? WHERE inc = ? AND lane_key = ? AND subscriber_do_id = ? AND lease_id = ? AND expires_at > ? AND failures < ?',
    expiresAt,
    inc,
    laneKey,
    subscriberDoId,
    leaseId,
    now,
    ROUTE_DELIVERY_FAILURE_LIMIT,
  ).rowsWritten
  return changed === 1 ? { ok: true, expiresAt } : { ok: false }
}

// Unsubscribe deletes only when all four match, so a stale renewal or a racing old lease can't resurrect
// a removed route.
export function deleteRoute(
  sql: SqlStorage,
  inc: string,
  laneKey: string,
  subscriberDoId: string,
  leaseId: string,
): void {
  sql.exec(
    'DELETE FROM route WHERE inc = ? AND lane_key = ? AND subscriber_do_id = ? AND lease_id = ?',
    inc,
    laneKey,
    subscriberDoId,
    leaseId,
  )
}

// The delivery target snapshot at acceptance: live (non-expired) routes for this (inc, lane) only.
export function snapshotRoutes(sql: SqlStorage, inc: string, laneKey: string, now: number): RouteTarget[] {
  return sql
    .exec<{ subscriber_do_id: string; lease_id: string; generation_token: string }>(
      'SELECT subscriber_do_id, lease_id, generation_token FROM route WHERE inc = ? AND lane_key = ? AND expires_at > ? AND failures < ?',
      inc,
      laneKey,
      now,
      ROUTE_DELIVERY_FAILURE_LIMIT,
    )
    .toArray()
    .map((row) => ({
      subscriberDoId: row.subscriber_do_id,
      leaseId: row.lease_id,
      generationToken: row.generation_token,
    }))
}

// Delivery outcomes are guarded by the snapshotted lease. A late failure from a replaced lease cannot
// increment or evict its successor. Success resets the consecutive-failure count; the third matching
// failure makes the row non-live but retains it as the durable exact-lease invalidation source.
export function recordRouteDeliverySuccess(
  sql: SqlStorage,
  inc: string,
  laneKey: string,
  subscriberDoId: string,
  leaseId: string,
): void {
  sql.exec(
    'UPDATE route SET failures = 0 WHERE inc = ? AND lane_key = ? AND subscriber_do_id = ? AND lease_id = ?',
    inc,
    laneKey,
    subscriberDoId,
    leaseId,
  )
}

export function recordRouteDeliveryFailure(
  sql: SqlStorage,
  inc: string,
  laneKey: string,
  subscriberDoId: string,
  leaseId: string,
  limit: number = ROUTE_DELIVERY_FAILURE_LIMIT,
): { matched: boolean; evicted: boolean } {
  const changed = sql.exec(
    'UPDATE route SET failures = failures + 1 WHERE inc = ? AND lane_key = ? AND subscriber_do_id = ? AND lease_id = ?',
    inc,
    laneKey,
    subscriberDoId,
    leaseId,
  ).rowsWritten
  if (changed !== 1) return { matched: false, evicted: false }
  const failures = sql
    .exec<{ failures: number }>(
      'SELECT failures FROM route WHERE inc = ? AND lane_key = ? AND subscriber_do_id = ? AND lease_id = ?',
      inc,
      laneKey,
      subscriberDoId,
      leaseId,
    )
    .toArray()[0]?.failures
  if (failures === undefined || failures < limit) return { matched: true, evicted: false }
  sql.exec(
    'UPDATE route SET expires_at = 0 WHERE inc = ? AND lane_key = ? AND subscriber_do_id = ? AND lease_id = ?',
    inc,
    laneKey,
    subscriberDoId,
    leaseId,
  )
  return { matched: true, evicted: true }
}

// A single route's current lease id, for the addressability/expiry checks the DO runs.
export function routeExists(
  sql: SqlStorage,
  inc: string,
  laneKey: string,
  subscriberDoId: string,
  now: number,
): boolean {
  return (
    sql
      .exec(
        'SELECT 1 FROM route WHERE inc = ? AND lane_key = ? AND subscriber_do_id = ? AND expires_at > ? AND failures < ? LIMIT 1',
        inc,
        laneKey,
        subscriberDoId,
        now,
        ROUTE_DELIVERY_FAILURE_LIMIT,
      )
      .toArray().length > 0
  )
}

// Alarm hygiene: drop every lapsed route row.
export function pruneExpiredRoutes(sql: SqlStorage, now: number): number {
  return sql.exec('DELETE FROM route WHERE expires_at <= ?', now).rowsWritten
}
