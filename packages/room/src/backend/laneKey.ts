export {
  encodeLaneKey,
  decodeLaneKey,
  everyTrackLaneKey,
  everyMemberLaneKey,
  binaryLaneKeyPrefix,
  roomSubscriptionSourceKey,
}

import { assert } from '../utils/assert.js'
import type { EveryMemberLane, EveryTrackLane, LaneId, RoomSubscriptionSource } from './contract.js'

function encodeLaneKey(lane: LaneId | EveryTrackLane | EveryMemberLane): string {
  if (lane.kind === 'semantic' || lane.kind === 'control' || lane.kind === 'demand') return lane.kind
  if (lane.kind === 'inbox') return `inbox:${encodeURIComponent(lane.member)}`
  if (lane.kind === 'binary-all') return everyTrackLaneKey(lane.member)
  if (lane.kind === 'binary-track') return everyMemberLaneKey(lane.track)
  return `${binaryLaneKeyPrefix(lane.member)}${encodeURIComponent(lane.track)}`
}

/** What the key of each binary lane of `member` starts with. */
function binaryLaneKeyPrefix(member: string): string {
  return `binary:${encodeURIComponent(member)}:`
}

/** The key of the lane that receives every binary commit of `member`. */
function everyTrackLaneKey(member: string): string {
  return `binary-all:${encodeURIComponent(member)}`
}

/** The key of the lane that receives every member's binary commits on `track`. */
function everyMemberLaneKey(track: string): string {
  return `binary-track:${encodeURIComponent(track)}`
}

/** A committed lane's key back to its lane; an every-track or every-member lane holds no commits of its own. */
function decodeLaneKey(key: string): LaneId {
  if (key === 'semantic' || key === 'control' || key === 'demand') return { kind: key }
  const [kind, member = '', track = ''] = key.split(':')
  if (kind === 'inbox') return { kind: 'inbox', member: decodeURIComponent(member) }
  assert(kind === 'binary', `Not a lane key: ${key}`)
  return { kind: 'binary', member: decodeURIComponent(member), track: decodeURIComponent(track) }
}

function roomSubscriptionSourceKey(source: RoomSubscriptionSource): string {
  return `${encodeURIComponent(source.roomId)}:${encodeURIComponent(source.inc)}:${encodeLaneKey(source.lane)}`
}
