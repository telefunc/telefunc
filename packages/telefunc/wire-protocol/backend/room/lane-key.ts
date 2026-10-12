export {
  encodeLaneKey,
  decodeLaneKey,
  everyTrackLaneKey,
  binaryLaneKeyPrefix,
  inProcessBinaryLaneKey,
  roomSubscriptionSourceKey,
}

import { assert } from '../../../utils/assert.js'
import type { EveryTrackLane, LaneId, RoomSubscriptionSource } from './contract.js'

function encodeLaneKey(lane: LaneId | EveryTrackLane): string {
  if (lane.kind === 'semantic' || lane.kind === 'control') return lane.kind
  if (lane.kind === 'inbox') return `inbox:${encodeURIComponent(lane.member)}`
  if (lane.kind === 'binary-all') return everyTrackLaneKey(lane.member)
  return `${binaryLaneKeyPrefix(lane.member)}${encodeURIComponent(lane.track)}`
}

/** A binary lane's key within this process, never decoded or stored: distinct per member and track, as no member id
 *  holds a NUL, and without `encodeLaneKey`'s escaping, which a replan pays per member. */
function inProcessBinaryLaneKey(member: string, track: string): string {
  return `${member}\0${track}`
}

/** What the key of each binary lane of `member` starts with. */
function binaryLaneKeyPrefix(member: string): string {
  return `binary:${encodeURIComponent(member)}:`
}

/** The key of the lane that receives every binary commit of `member`. */
function everyTrackLaneKey(member: string): string {
  return `binary-all:${encodeURIComponent(member)}`
}

/** A committed lane's key back to its lane; an every-track lane holds no commits of its own. */
function decodeLaneKey(key: string): LaneId {
  if (key === 'semantic' || key === 'control') return { kind: key }
  const [kind, member = '', track = ''] = key.split(':')
  if (kind === 'inbox') return { kind: 'inbox', member: decodeURIComponent(member) }
  assert(kind === 'binary', `Not a lane key: ${key}`)
  return { kind: 'binary', member: decodeURIComponent(member), track: decodeURIComponent(track) }
}

const roomSubscriptionSourceKey = (source: RoomSubscriptionSource) =>
  `${encodeURIComponent(source.roomId)}:${encodeURIComponent(source.inc)}:${encodeLaneKey(source.lane)}`
