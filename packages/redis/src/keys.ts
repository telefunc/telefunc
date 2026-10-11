export {
  DEFAULT_PREFIX,
  assertKeyPrefix,
  broadcastSequenceKey,
  broadcastChannel,
  headKey,
  generationKeysKey,
  revKey,
  cellIndexKey,
  cellValuesKey,
  cellVersionsKey,
  orderKey,
  retainedKeyPrefix,
  retainedKey,
  retainedOfKey,
  retainedOfTrackKey,
  channelKey,
  commitChannels,
  generationInvalidationChannel,
  directoryIndexKey,
  directoryTagsKey,
}
export type { CommitChannels }

// Redis keys. Every per-room key shares `{rid}`, so a room is one Cluster slot.

import { everyTrackLaneKey, type BroadcastRoute, type LaneId } from 'telefunc/__internal'

/** The channels a commit publishes on: its lane's, and a binary lane's member's every-track one. */
type CommitChannels = { lane: string; everyTrack: string | null }

const DEFAULT_PREFIX = 'tf:'

/** Builders take the prefix as checked here, once: a `{` would open a hash tag of its own. */
function assertKeyPrefix(prefix: string): void {
  if (prefix.includes('{')) throw new Error("installRedis(): the prefix must not contain '{'")
}
// Encoded like a room id, so any key is one hash tag; an empty tag would hash the whole name instead.
function broadcastTag(key: string): string {
  return key === '' ? '{_}:empty' : `{${encodeURIComponent(key)}}`
}
function broadcastSequenceKey(prefix: string, key: string): string {
  return `${prefix}seq:${broadcastTag(key)}`
}
function broadcastChannel(prefix: string, route: BroadcastRoute): string {
  const kind = route.kind === 'text' ? 't' : 'b'
  return `${prefix}${kind}:${broadcastTag(route.key)}`
}

function roomTag(prefix: string, roomId: string): string {
  // A Redis hash tag ends at the first `}`. Encode caller input before placing it in braces so an
  // arbitrary room id cannot escape the tag or split one logical room across slots.
  return `${prefix}room:{${encodeURIComponent(roomId)}}`
}
function headKey(prefix: string, roomId: string): string {
  return `${roomTag(prefix, roomId)}:head`
}
function genPrefix(prefix: string, roomId: string, inc: string): string {
  return `${roomTag(prefix, roomId)}:g:${inc}`
}
function generationKeysKey(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:keys`
}
function revKey(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:rev`
}
/** The generation's cell keys, all at score 0, so a prefix read is a lexicographic range. */
function cellIndexKey(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:cells`
}
/** Each cell's bytes, by its key. */
function cellValuesKey(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:values`
}
/** Each cell's version, by its key. */
function cellVersionsKey(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:versions`
}
function orderKey(prefix: string, roomId: string, inc: string, laneKey: string): string {
  return `${genPrefix(prefix, roomId, inc)}:o:${laneKey}`
}
function retainedKeyPrefix(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:rt:`
}
function retainedKey(prefix: string, roomId: string, inc: string, laneKey: string): string {
  return `${retainedKeyPrefix(prefix, roomId, inc)}${laneKey}`
}
/** The lane keys of one member's retained binary frames. */
function retainedOfKey(prefix: string, roomId: string, inc: string, member: string): string {
  return `${genPrefix(prefix, roomId, inc)}:rtm:${encodeURIComponent(member)}`
}
/** The lane keys of one track's retained binary frames. */
function retainedOfTrackKey(prefix: string, roomId: string, inc: string, track: string): string {
  return `${genPrefix(prefix, roomId, inc)}:rtt:${encodeURIComponent(track)}`
}
function channelKey(prefix: string, roomId: string, inc: string, laneKey: string): string {
  return `${roomTag(prefix, roomId)}:ch:${inc}:${laneKey}`
}
function commitChannels(prefix: string, roomId: string, inc: string, lane: LaneId, laneKey: string): CommitChannels {
  const channels = `${roomTag(prefix, roomId)}:ch:${inc}:`
  return {
    lane: channels + laneKey,
    everyTrack: lane.kind === 'binary' ? channels + everyTrackLaneKey(lane.member) : null,
  }
}
function generationInvalidationChannel(prefix: string, roomId: string, inc: string): string {
  return `${roomTag(prefix, roomId)}:invalidate:${inc}`
}
// The directory's two keys share their own tag so the tag-guarded delete stays one slot under Cluster.
function directoryIndexKey(prefix: string): string {
  return `${prefix}room-dir:{${prefix}dir}:index`
}
function directoryTagsKey(prefix: string): string {
  return `${prefix}room-dir:{${prefix}dir}:tags`
}
