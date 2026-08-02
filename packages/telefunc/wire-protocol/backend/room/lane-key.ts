export { encodeLaneKey, roomSubscriptionSourceKey }

import type { LaneId, RoomSubscriptionSource } from './contract.js'

function encodeLaneKey(lane: LaneId): string {
  if (lane.kind === 'semantic' || lane.kind === 'control') return lane.kind
  if (lane.kind === 'inbox') return `inbox:${encodeURIComponent(lane.member)}`
  return `binary:${encodeURIComponent(lane.member)}:${encodeURIComponent(lane.track)}`
}

const roomSubscriptionSourceKey = (source: RoomSubscriptionSource) =>
  `${encodeURIComponent(source.roomId)}:${encodeURIComponent(source.inc)}:${encodeLaneKey(source.lane)}`
