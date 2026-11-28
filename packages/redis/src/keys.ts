export {
  DEFAULT_PREFIX,
  assertKeyPrefix,
  broadcastSequenceKey,
  broadcastChannel,
  broadcastPublishKeys,
  headKey,
  incarnationKeysKey,
  revKey,
  cellIndexKey,
  cellValuesKey,
  cellVersionsKey,
  cellLapsesKey,
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
  // The directory's hash tag holds it encoded (`hashTag`), and a string that isn't well formed has no encoding.
  if (!prefix.isWellFormed()) throw new Error('installRedis(): the prefix must be a well-formed string')
}
/** The hash tag of `text`, encoded: a Redis hash tag ends at the first `}`, so a `}` in a room id, a broadcast key or the
 *  prefix can't end it early, and one logical name can't be split across slots or share another's. */
function hashTag(text: string): string {
  return `{${encodeURIComponent(text)}}`
}
/** An empty tag would hash the whole name instead. */
function broadcastTag(key: string): string {
  return key === '' ? '{_}:empty' : hashTag(key)
}
function broadcastSequenceKey(prefix: string, key: string): string {
  return sequenceKeyOf(prefix, broadcastTag(key))
}
function broadcastChannel(prefix: string, route: BroadcastRoute): string {
  return channelOf(prefix, route, broadcastTag(route.key))
}
/** A publish's keys, its key's order record and its kind's channel, from one encoding of its key. */
function broadcastPublishKeys(prefix: string, route: BroadcastRoute): [order: string, channel: string] {
  const tag = broadcastTag(route.key)
  return [sequenceKeyOf(prefix, tag), channelOf(prefix, route, tag)]
}
function sequenceKeyOf(prefix: string, tag: string): string {
  return `${prefix}seq:${tag}`
}
function channelOf(prefix: string, { kind }: BroadcastRoute, tag: string): string {
  return `${prefix}${kind === 'text' ? 't' : 'b'}:${tag}`
}

function roomTag(prefix: string, roomId: string): string {
  return `${prefix}room:${hashTag(roomId)}`
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
/** Each cell's lapse, by its key: the authority time it lapses at, or '' for a cell written with no `lapsesInMs`. A
 *  field for every cell keeps this hash there whenever the values are: a script of a slot that migrates is asked to the
 *  importing node only while it finds all of its keys there. */
function cellLapsesKey(prefix: string, roomId: string, inc: string): string {
  return `${incarnationPrefix(prefix, roomId, inc)}:lapses`
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
/** The directory's keys share their own tag so the tag-guarded delete stays one slot under Cluster; two prefixes have
 *  two tags. */
function directoryIndexKey(prefix: string): string {
  return `${prefix}room-dir:${hashTag(`${prefix}dir`)}:index`
}
function directoryTagsKey(prefix: string): string {
  return `${prefix}room-dir:${hashTag(`${prefix}dir`)}:tags`
}
/** Each listed room scored by when it is due, if ever. */
function directoryChecksKey(prefix: string): string {
  return `${prefix}room-dir:${hashTag(`${prefix}dir`)}:checks`
}
