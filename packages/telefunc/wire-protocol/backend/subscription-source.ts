export { broadcastRouteKey, laneKey, subscriptionSourceKey }

import { LANE_KEY_LAYOUT, type BackendSubscriptionSource, type BroadcastLane, type LaneId } from './spi.js'

function laneKey(lane: LaneId): string {
  const values = lane as LaneId & Record<'member' | 'track', string>
  const fields = LANE_KEY_LAYOUT.fields[lane.kind]
  return [lane.kind, ...fields.map((field) => encodeURIComponent(values[field]))].join(LANE_KEY_LAYOUT.separator)
}

function broadcastRouteKey(lane: BroadcastLane): string {
  return `${lane.kind}:${encodeURIComponent(lane.key)}`
}

function subscriptionSourceKey(source: BackendSubscriptionSource): string {
  if (source.kind === 'broadcast') return `broadcast:${broadcastRouteKey(source.lane)}`
  return `durable:${encodeURIComponent(source.roomId)}:${encodeURIComponent(source.inc)}:${laneKey(source.lane)}`
}
