export { laneRoute, routeLease, routeScope, incarnationScopes }
export type { RouteInstallation }

import type { BroadcastRoute } from 'telefunc/__internal'
import { ROOM_CLOUDFLARE_PREFIX } from '../server/constants.js'

// A session's subscription to a lane is its lease on the lane's scope at the room's authority (core's `leases.ts`),
// held by its session DO. Scopes are an incarnation's, so a recreated room fences off surviving old subscriptions.

type RouteInstallation = {
  roomId: string
  inc: string
  laneKey: string
  sessionDoId: string
  leaseId: string
  /** The bucket its session DO was placed in, where Broadcast's coordinators hand it the lane's frames. */
  bucket: string
}

function routeScope(inc: string, laneKey: string): string {
  return JSON.stringify([inc, laneKey])
}

/** What every scope of `inc` starts with. */
function incarnationScopes(inc: string): string {
  return JSON.stringify([inc, '']).slice(0, -'""]'.length)
}

function routeLease({ inc, laneKey, sessionDoId, leaseId, bucket }: RouteInstallation) {
  return { scope: routeScope(inc, laneKey), holder: sessionDoId, lease: leaseId, bucket }
}

/** The Broadcast route a session's subscription to a lane takes the lane's frames on; an incarnation id is unique, so
 *  it names the lane across rooms. */
function laneRoute(inc: string, laneKey: string): BroadcastRoute<'binary'> {
  return { key: ROOM_CLOUDFLARE_PREFIX + JSON.stringify([inc, laneKey]), kind: 'binary' }
}
