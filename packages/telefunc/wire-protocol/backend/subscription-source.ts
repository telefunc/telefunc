export { broadcastRouteKey, laneKey, subscriptionSourceKey }

import type { BackendSubscriptionSource, BroadcastLane, LaneId } from './spi.js'

function laneKey(lane: LaneId): string {
  if (lane.kind === 'semantic' || lane.kind === 'control') return lane.kind
  if (lane.kind === 'inbox') return `inbox:${encodeURIComponent(lane.member)}`
  return `binary:${encodeURIComponent(lane.member)}:${encodeURIComponent(lane.track)}`
}

const broadcastRouteKey = (lane: BroadcastLane) => `${lane.kind}:${encodeURIComponent(lane.key)}`

const subscriptionSourceKey = (source: BackendSubscriptionSource) =>
  source.kind === 'broadcast'
    ? `broadcast:${broadcastRouteKey(source.lane)}`
    : `durable:${encodeURIComponent(source.roomId)}:${encodeURIComponent(source.inc)}:${laneKey(source.lane)}`
