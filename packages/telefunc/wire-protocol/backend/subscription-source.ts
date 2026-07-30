export { broadcastRouteKey, laneKey, subscriptionSourceKey }

import type { BackendSubscriptionSource, BroadcastLane, LaneId } from './spi.js'

function laneKey(lane: LaneId): string {
  switch (lane.kind) {
    case 'semantic':
      return 'semantic'
    case 'control':
      return 'control'
    case 'binary':
      return `binary:${encodeURIComponent(lane.member)}:${encodeURIComponent(lane.track)}`
    case 'inbox':
      return `inbox:${encodeURIComponent(lane.member)}`
  }
}

function broadcastRouteKey(lane: BroadcastLane): string {
  return `${lane.kind}:${encodeURIComponent(lane.key)}`
}

function subscriptionSourceKey(source: BackendSubscriptionSource): string {
  if (source.kind === 'broadcast') return `broadcast:${broadcastRouteKey(source.lane)}`
  return `durable:${encodeURIComponent(source.roomId)}:${encodeURIComponent(source.inc)}:${laneKey(source.lane)}`
}
