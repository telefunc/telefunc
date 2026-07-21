/// <reference types="@cloudflare/workers-types" />
// The readiness invariant (I7) and the CF handshake of readiness-ordering.md §2.3. One row per
// (inc, lane_key, subscriber); the CURRENT leaseId is a VALUE, so re-establishment after a renewal loss
// atomically REPLACES the prior lease in one UPSERT and can never coexist with an unexpired old row —
// that is what makes the renewal-loss-before-expiry path exactly-once (no duplicate delivery). Routes are
// inc-scoped, so a surviving old-inc subscription can never be a target of a recreated room (I11).

export const ROUTE_TTL_MS = 90_000

// Establishment: the open-head check happens in the DO (it holds the head); this UPSERT replaces any
// prior lease for the same (inc, lane, subscriber) in a single statement.
export function upsertRoute(
  sql: SqlStorage,
  inc: string,
  laneKey: string,
  subscriber: string,
  leaseId: string,
  bucket: string | null,
  now: number,
  ttlMs: number = ROUTE_TTL_MS,
): number {
  const expiresAt = now + ttlMs
  sql.exec(
    'INSERT OR REPLACE INTO route (inc, lane_key, subscriber, lease_id, bucket, expires_at) VALUES (?, ?, ?, ?, ?, ?)',
    inc,
    laneKey,
    subscriber,
    leaseId,
    bucket,
    expiresAt,
  )
  return expiresAt
}

// Renewal compares all four fields (inc, lane_key, subscriber, leaseId); a stale lease id matches nothing
// and the renewal is lost.
export function renewRoute(
  sql: SqlStorage,
  inc: string,
  laneKey: string,
  subscriber: string,
  leaseId: string,
  now: number,
  ttlMs: number = ROUTE_TTL_MS,
): { ok: true; expiresAt: number } | { ok: false } {
  const expiresAt = now + ttlMs
  const changed = sql.exec(
    'UPDATE route SET expires_at = ? WHERE inc = ? AND lane_key = ? AND subscriber = ? AND lease_id = ?',
    expiresAt,
    inc,
    laneKey,
    subscriber,
    leaseId,
  ).rowsWritten
  return changed === 1 ? { ok: true, expiresAt } : { ok: false }
}

// Unsubscribe deletes only when all four match, so a stale renewal or a racing old lease can't resurrect
// a removed route.
export function deleteRoute(sql: SqlStorage, inc: string, laneKey: string, subscriber: string, leaseId: string): void {
  sql.exec(
    'DELETE FROM route WHERE inc = ? AND lane_key = ? AND subscriber = ? AND lease_id = ?',
    inc,
    laneKey,
    subscriber,
    leaseId,
  )
}

// The delivery target snapshot at acceptance: live (non-expired) routes for this (inc, lane) only.
export function snapshotRoutes(sql: SqlStorage, inc: string, laneKey: string, now: number): string[] {
  return sql
    .exec<{ subscriber: string }>(
      'SELECT subscriber FROM route WHERE inc = ? AND lane_key = ? AND expires_at > ?',
      inc,
      laneKey,
      now,
    )
    .toArray()
    .map((row) => row.subscriber)
}

// A single route's current lease id, for the addressability/expiry checks the DO runs.
export function routeExists(sql: SqlStorage, inc: string, laneKey: string, subscriber: string, now: number): boolean {
  return (
    sql
      .exec('SELECT 1 FROM route WHERE inc = ? AND lane_key = ? AND subscriber = ? AND expires_at > ? LIMIT 1', inc, laneKey, subscriber, now)
      .toArray().length > 0
  )
}

// Alarm hygiene: drop every lapsed route row.
export function pruneExpiredRoutes(sql: SqlStorage, now: number): number {
  return sql.exec('DELETE FROM route WHERE expires_at <= ?', now).rowsWritten
}
