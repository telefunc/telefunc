/// <reference types="@cloudflare/workers-types" />
export { ROUTE_RENEW_EVERY_MS, upsertRoute, deleteExpiredRoutes, renewRoute, deleteRoute, snapshotRoutes }
export type { RouteInstallation }

// One exact-lease row per (incarnation, lane, session). Re-establishment atomically replaces the
// prior lease, and incarnation scoping fences recreated rooms from surviving old subscriptions.

const ROUTE_TTL_MS = 90_000
const ROUTE_RENEW_EVERY_MS = ROUTE_TTL_MS / 3
const ROUTE_COLUMNS = 'room_id AS roomId, inc, lane_key AS laneKey, session_do_id AS sessionDoId, lease_id AS leaseId'
const EXACT_ROUTE = 'inc = ? AND lane_key = ? AND session_do_id = ? AND lease_id = ?'

type RouteInstallation = {
  roomId: string
  inc: string
  laneKey: string
  sessionDoId: string
  leaseId: string
}

const exact = (route: RouteInstallation) => [route.inc, route.laneKey, route.sessionDoId, route.leaseId]

// The DO checks the open head; this UPSERT atomically replaces the prior exact lease.
function upsertRoute(sql: SqlStorage, route: RouteInstallation, now: number): void {
  sql.exec(
    'INSERT OR REPLACE INTO route (room_id, inc, lane_key, session_do_id, lease_id, expires_at) VALUES (?, ?, ?, ?, ?, ?)',
    route.roomId,
    ...exact(route),
    now + ROUTE_TTL_MS,
  )
}

function deleteExpiredRoutes(sql: SqlStorage, now: number): void {
  sql.exec('DELETE FROM route WHERE expires_at <= ?', now)
}

function renewRoute(sql: SqlStorage, route: RouteInstallation, now: number): boolean {
  const changed = sql.exec(
    `UPDATE route SET expires_at = ? WHERE ${EXACT_ROUTE} AND expires_at > ?`,
    now + ROUTE_TTL_MS,
    ...exact(route),
    now,
  ).rowsWritten
  return changed === 1
}

// Exact-lease deletion prevents a racing old lease from removing its successor.
function deleteRoute(sql: SqlStorage, route: RouteInstallation): void {
  sql.exec(`DELETE FROM route WHERE ${EXACT_ROUTE}`, ...exact(route))
}

// The delivery target snapshot at acceptance: live (non-expired) routes for this (inc, lane) only.
function snapshotRoutes(sql: SqlStorage, inc: string, laneKey: string, now: number): RouteInstallation[] {
  return sql
    .exec<RouteInstallation>(
      `SELECT ${ROUTE_COLUMNS} FROM route WHERE inc = ? AND lane_key = ? AND expires_at > ?`,
      inc,
      laneKey,
      now,
    )
    .toArray()
}
