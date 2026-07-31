/// <reference types="@cloudflare/workers-types" />
// One exact-lease row per (incarnation, lane, subscriber). Re-establishment atomically replaces the
// prior lease, and incarnation scoping fences recreated rooms from surviving old subscriptions.

const ROUTE_TTL_MS = 90_000
export const ROUTE_RENEW_EVERY_MS = ROUTE_TTL_MS / 3
const ROUTE_DELIVERY_FAILURE_LIMIT = 3

export type RouteTarget = { subscriberDoId: string; leaseId: string; generationToken: string }
export type RouteInstallation = {
  roomId: string
  inc: string
  laneKey: string
  subscriberDoId: string
  leaseId: string
  generationToken: string
}

// The DO checks the open head; this UPSERT atomically replaces the prior exact lease.
export function upsertRoute(
  sql: SqlStorage,
  roomId: string,
  inc: string,
  laneKey: string,
  subscriberDoId: string,
  leaseId: string,
  generationToken: string,
  now: number,
): void {
  const expiresAt = now + ROUTE_TTL_MS
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
}

export function listRouteInstallations(sql: SqlStorage, inc: string): RouteInstallation[] {
  return sql
    .exec<RouteInstallation>(
      'SELECT room_id AS roomId, inc, lane_key AS laneKey, subscriber_do_id AS subscriberDoId, lease_id AS leaseId, generation_token AS generationToken FROM route WHERE inc = ?',
      inc,
    )
    .toArray()
}

export function listExpiredRouteInstallations(sql: SqlStorage, now: number): RouteInstallation[] {
  return sql
    .exec<RouteInstallation>(
      'SELECT room_id AS roomId, inc, lane_key AS laneKey, subscriber_do_id AS subscriberDoId, lease_id AS leaseId, generation_token AS generationToken FROM route WHERE expires_at <= ? OR failures >= ?',
      now,
      ROUTE_DELIVERY_FAILURE_LIMIT,
    )
    .toArray()
}

// Renewal matches the exact incarnation, lane, subscriber, and lease.
export function renewRoute(
  sql: SqlStorage,
  inc: string,
  laneKey: string,
  subscriberDoId: string,
  leaseId: string,
  now: number,
): boolean {
  const expiresAt = now + ROUTE_TTL_MS
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
  return changed === 1
}

// Exact-lease deletion prevents a racing old lease from removing its successor.
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

// Outcomes are exact-lease guarded. Success resets failures; the third failure makes the row non-live
// while retaining it as the durable invalidation source.
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
): boolean {
  const changed = sql.exec(
    'UPDATE route SET failures = failures + 1 WHERE inc = ? AND lane_key = ? AND subscriber_do_id = ? AND lease_id = ?',
    inc,
    laneKey,
    subscriberDoId,
    leaseId,
  ).rowsWritten
  if (changed !== 1) return false
  const failures = sql
    .exec<{
      failures: number
    }>(
      'SELECT failures FROM route WHERE inc = ? AND lane_key = ? AND subscriber_do_id = ? AND lease_id = ?',
      inc,
      laneKey,
      subscriberDoId,
      leaseId,
    )
    .toArray()[0]?.failures
  if (failures === undefined || failures < ROUTE_DELIVERY_FAILURE_LIMIT) {
    return false
  }
  sql.exec(
    'UPDATE route SET expires_at = 0 WHERE inc = ? AND lane_key = ? AND subscriber_do_id = ? AND lease_id = ?',
    inc,
    laneKey,
    subscriberDoId,
    leaseId,
  )
  return true
}
