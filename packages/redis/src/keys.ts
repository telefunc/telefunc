export {
  DEFAULT_PREFIX,
  assertKeyPrefix,
  broadcastSequenceKey,
  broadcastChannel,
  headKey,
  incarnationKeysKey,
  revKey,
  cellIndexKey,
  cellValuesKey,
  cellVersionsKey,
  orderKey,
  memberOrderKey,
  trackLanesKey,
  retainedKey,
  retainedLanesKey,
  retainedOwnersKey,
  retainedOfKey,
  retainedOfTrackKey,
  channelKey,
  commitChannels,
  incarnationInvalidationChannel,
  directoryIndexKey,
  directoryTagsKey,
  expiringCellKey,
  directoryChecksKey,
}
export type { CommitChannels, FenceTokens }

// Redis keys. Every per-room key shares `{rid}`, so a room is one Cluster slot.

import type { BroadcastRoute } from 'telefunc/__internal'
import type { LaneId, RoomPlaneKit } from '@telefunc/room/backend'

/** The channels a commit is published on: its lane's, and for a binary commit also the every-track channel of its
 *  member and the every-member channel of its track. */
type CommitChannels = { lane: string; everyTrack: string | null; everyMember: string | null }
/** Each commit channel's delivery-fence token, `''` where no local subscription awaits the commit. */
type FenceTokens = { [Channel in keyof CommitChannels]: string }

const DEFAULT_PREFIX = 'tf:'

/** Builders take the prefix as checked here, once: a `{` would open a hash tag of its own. */
function assertKeyPrefix(prefix: string): void {
  if (prefix.includes('{')) throw new Error("installRedis(): the prefix must not contain '{'")
}
/** Encoded like a room id, so any key is one hash tag; an empty tag would hash the whole name instead. */
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
function incarnationPrefix(prefix: string, roomId: string, inc: string): string {
  return `${roomTag(prefix, roomId)}:g:${inc}`
}
function incarnationKeysKey(prefix: string, roomId: string, inc: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:keys`
}
function revKey(prefix: string, roomId: string, inc: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:rev`
}
/** The incarnation's cell keys, all at score 0, so a prefix read is a lexicographic range. */
function cellIndexKey(prefix: string, roomId: string, inc: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:cells`
}
/** Each cell's bytes, by its key. */
function cellValuesKey(prefix: string, roomId: string, inc: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:values`
}
/** Each cell's version, by its key. */
function cellVersionsKey(prefix: string, roomId: string, inc: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:versions`
}
/** An expiring cell's bytes, under a TTL of its own, so Redis deletes it. It isn't in the incarnation's manifest:
 *  it expires by itself. */
function expiringCellKey(prefix: string, roomId: string, inc: string, cellKey: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:x:${cellKey}`
}
function orderKey(prefix: string, roomId: string, inc: string, laneKey: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:o:${laneKey}`
}
/** The order of one member's inbox and binary lanes, by lane key. */
function memberOrderKey(prefix: string, roomId: string, inc: string, member: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:om:${encodeURIComponent(member)}`
}
/** The keys of one track's binary lanes that have a commit. */
function trackLanesKey(prefix: string, roomId: string, inc: string, track: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:lt:${encodeURIComponent(track)}`
}
function retainedKey(prefix: string, roomId: string, inc: string, laneKey: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:rt:${laneKey}`
}
/** The lane keys of the incarnation's retained frames. */
function retainedLanesKey(prefix: string, roomId: string, inc: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:rtl`
}
/** The member each retained frame of a lane other than a binary one is from, by lane key. */
function retainedOwnersKey(prefix: string, roomId: string, inc: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:rt-owners`
}
/** The lane keys of one member's retained binary frames. */
function retainedOfKey(prefix: string, roomId: string, inc: string, member: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:rtm:${encodeURIComponent(member)}`
}
/** The lane keys of one track's retained binary frames. */
function retainedOfTrackKey(prefix: string, roomId: string, inc: string, track: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:rtt:${encodeURIComponent(track)}`
}
function channelKey(prefix: string, roomId: string, inc: string, laneKey: string): string {
  return channelsPrefix(prefix, roomId, inc) + laneKey
}
function commitChannels(
  prefix: string,
  roomId: string,
  inc: string,
  lane: LaneId,
  laneKey: string,
  kit: RoomPlaneKit,
): CommitChannels {
  const channels = channelsPrefix(prefix, roomId, inc)
  return {
    lane: channels + laneKey,
    everyTrack: lane.kind === 'binary' ? channels + kit.everyTrackLaneKey(lane.member) : null,
    everyMember: lane.kind === 'binary' ? channels + kit.everyMemberLaneKey(lane.track) : null,
  }
}
/** What every lane channel of an incarnation starts with. */
function channelsPrefix(prefix: string, roomId: string, inc: string): string {
  return `${roomTag(prefix, roomId)}:ch:${inc}:`
}
function incarnationInvalidationChannel(prefix: string, roomId: string, inc: string): string {
  return `${roomTag(prefix, roomId)}:invalidate:${inc}`
}
/** The directory's two keys share their own tag so the tag-guarded delete stays one slot under Cluster. */
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
