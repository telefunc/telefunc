/// <reference types="@cloudflare/workers-types" />
export { ROUTE_RENEW_EVERY_MS, upsertRoute, deleteExpiredRoutes, renewRoute, deleteRoute, snapshotRoutes, laneRoute }
export type { RouteInstallation }

import type { BroadcastRoute } from 'telefunc/__internal'
import { ROOM_CLOUDFLARE_PREFIX } from '../server/constants.js'

// One exact-lease row per (incarnation, lane, session). Re-establishment atomically replaces the
// prior lease, and incarnation scoping fences recreated rooms from surviving old subscriptions.

const ROUTE_TTL_MS = 90_000
const ROUTE_RENEW_EVERY_MS = ROUTE_TTL_MS / 3
const ROUTE_COLUMNS =
  'room_id AS roomId, inc, lane_key AS laneKey, session_do_id AS sessionDoId, lease_id AS leaseId, bucket'
const EXACT_ROUTE = 'inc = ? AND lane_key = ? AND session_do_id = ? AND lease_id = ?'

type RouteInstallation = {
  roomId: string
  inc: string
  laneKey: string
  sessionDoId: string
  leaseId: string
  /** The bucket its session DO was placed in, where Broadcast's coordinators hand it the lane's frames. */
  bucket: string
}

function exact(route: RouteInstallation): string[] {
  return [route.inc, route.laneKey, route.sessionDoId, route.leaseId]
}

/** The DO checks the open head; this UPSERT atomically replaces the prior exact lease. */
function upsertRoute(sql: SqlStorage, route: RouteInstallation, now: number): void {
  sql.exec(
    'INSERT OR REPLACE INTO route (room_id, inc, lane_key, session_do_id, lease_id, bucket, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
    route.roomId,
    ...exact(route),
    route.bucket,
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
  // The key and lease match one row at most; rowsWritten also counts the expires_at index entry.
  return changed > 0
}

/** Exact-lease deletion prevents a racing old lease from removing its successor. */
function deleteRoute(sql: SqlStorage, route: RouteInstallation): void {
  sql.exec(`DELETE FROM route WHERE ${EXACT_ROUTE}`, ...exact(route))
}

/** The Broadcast route a session's subscription to a lane takes the lane's frames on; an incarnation id is unique, so
 *  it names the lane across rooms. */
function laneRoute(inc: string, laneKey: string): BroadcastRoute<'binary'> {
  return { key: ROOM_CLOUDFLARE_PREFIX + JSON.stringify([inc, laneKey]), kind: 'binary' }
}

/** The delivery target snapshot at acceptance: live (non-expired) routes for these (inc, lane) pairs only. */
function snapshotRoutes(sql: SqlStorage, inc: string, laneKeys: readonly string[], now: number): RouteInstallation[] {
  return sql
    .exec<RouteInstallation>(
      `SELECT ${ROUTE_COLUMNS} FROM route WHERE inc = ? AND lane_key IN (${laneKeys.map(() => '?').join(', ')}) AND expires_at > ?`,
      inc,
      ...laneKeys,
      now,
    )
    .toArray()
}
