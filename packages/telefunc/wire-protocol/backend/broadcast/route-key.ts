export { broadcastRouteKey }

import type { BroadcastLane } from './contract.js'

const broadcastRouteKey = (lane: BroadcastLane) => `${lane.kind}:${encodeURIComponent(lane.key)}`
