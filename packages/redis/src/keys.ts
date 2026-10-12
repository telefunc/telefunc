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
  memberOrderKey,
  trackLanesKey,
  retainedKeyPrefix,
  retainedKey,
  retainedOfKey,
  retainedOfTrackKey,
  channelKey,
  commitChannels,
  generationInvalidationChannel,
  directoryIndexKey,
  directoryTagsKey,
  directoryChecksKey,
}
export type { CommitChannels }

// Redis keys. Every per-room key shares `{rid}`, so a room is one Cluster slot.

import { everyMemberLaneKey, everyTrackLaneKey, type BroadcastRoute, type LaneId } from 'telefunc/__internal'

/** The channels a commit publishes on: its lane's, and a binary lane's member's every-track one. */
/** A binary commit's lane, its member's every-track and its track's every-member channels; another commit's lane only. */
type CommitChannels = { lane: string; everyTrack: string | null; everyMember: string | null }

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
/** The order of one member's inbox and binary lanes, by lane key. */
function memberOrderKey(prefix: string, roomId: string, inc: string, member: string): string {
  return `${genPrefix(prefix, roomId, inc)}:om:${encodeURIComponent(member)}`
}
/** The keys of one track's binary lanes that have a commit. */
function trackLanesKey(prefix: string, roomId: string, inc: string, track: string): string {
  return `${genPrefix(prefix, roomId, inc)}:ot:${encodeURIComponent(track)}`
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
    everyMember: lane.kind === 'binary' ? channels + everyMemberLaneKey(lane.track) : null,
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
/** Each listed room scored by when it is due, if ever. */
function directoryChecksKey(prefix: string): string {
  return `${prefix}room-dir:{${prefix}dir}:checks`
}
