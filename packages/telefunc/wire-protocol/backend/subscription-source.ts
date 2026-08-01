export { broadcastRouteKey, laneKey, subscriptionSourceKey }

import { broadcastRouteKey } from './broadcast/route-key.js'
import { laneKey } from './room/lane-key.js'
import type { BackendSubscriptionSource } from './spi.js'

/** Temporary key delegate for the fused adapters; plane supervisors no longer consume this union. */
const subscriptionSourceKey = (source: BackendSubscriptionSource) =>
  source.kind === 'broadcast'
    ? `broadcast:${broadcastRouteKey(source.lane)}`
    : `durable:${encodeURIComponent(source.roomId)}:${encodeURIComponent(source.inc)}:${laneKey(source.lane)}`
