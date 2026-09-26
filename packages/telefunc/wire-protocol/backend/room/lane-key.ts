export { encodeLaneKey, decodeLaneKey, roomSubscriptionSourceKey }

import { assert } from '../../../utils/assert.js'
import type { LaneId, RoomSubscriptionSource } from './contract.js'

function encodeLaneKey(lane: LaneId): string {
  if (lane.kind === 'semantic' || lane.kind === 'control') return lane.kind
  if (lane.kind === 'inbox') return `inbox:${encodeURIComponent(lane.member)}`
  return `binary:${encodeURIComponent(lane.member)}:${encodeURIComponent(lane.track)}`
}

function decodeLaneKey(key: string): LaneId {
  if (key === 'semantic' || key === 'control') return { kind: key }
  const [kind, member = '', track = ''] = key.split(':')
  if (kind === 'inbox') return { kind: 'inbox', member: decodeURIComponent(member) }
  assert(kind === 'binary', `Not a lane key: ${key}`)
  return { kind: 'binary', member: decodeURIComponent(member), track: decodeURIComponent(track) }
}

const roomSubscriptionSourceKey = (source: RoomSubscriptionSource) =>
  `${encodeURIComponent(source.roomId)}:${encodeURIComponent(source.inc)}:${encodeLaneKey(source.lane)}`
