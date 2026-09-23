// Redis Room keys; every per-room key shares `{rid}` and one Cluster slot.
// head/headrev: tf:room:{rid}:{head|headrev}; JSON head plus monotonic revision.
// cells/revision: tf:room:{rid}:g:<inc>:{c:<key>|rev}; logical cells plus coarse generation revision.
// order/retained: tf:room:{rid}:g:<inc>:{o|rt}:<laneKey>; ordering mark or 16-byte mark + payload.
// gen keys: tf:room:{rid}:g:<inc>:keys; generation-owned physical-key set.
// channels: tf:room:{rid}:ch:<inc>:<laneKey>; incarnation-scoped PUBLISH/SUBSCRIBE.
// gens: tf:room:{rid}:gens; installed incarnations.
// directory: <prefix>room-dir:{<prefix>dir}:{index|tags}; one global, co-slotted pair.
// Commands take authority time from Redis TIME, never from the caller.

import type { BroadcastLane } from 'telefunc/__internal'

export const DEFAULT_ROOM_PREFIX = 'tf:'

/** Builders take the prefix as validated here, once: a `{` would open a hash tag of its own. */
export function redisKeyPrefix(prefix: string): string {
  if (prefix.includes('{')) throw new Error("Redis key prefix must not contain '{'")
  return prefix
}
function broadcastTag(key: string): string {
  if (key.startsWith('}')) throw new Error("Redis Broadcast key must not start with '}'")
  return key === '' ? '{_}:empty' : `{${key}}`
}
export function broadcastSequenceKey(prefix: string, key: string): string {
  return `${prefix}seq:${broadcastTag(key)}`
}
export function broadcastChannel(prefix: string, lane: BroadcastLane): string {
  const kind = lane.kind === 'text' ? 't' : 'b'
  return `${prefix}${kind}:${broadcastTag(lane.key)}`
}

// `{<rid>}` is the Cluster hash tag; every per-room key carries it so the room is one slot.
export function roomTag(prefix: string, roomId: string): string {
  // A Redis hash tag ends at the first `}`. Encode caller input before placing it in braces so an
  // arbitrary room id cannot escape the tag or split one logical room across slots.
  return `${prefix}room:{${encodeURIComponent(roomId)}}`
}
export function headKey(prefix: string, roomId: string): string {
  return `${roomTag(prefix, roomId)}:head`
}
export function headRevKey(prefix: string, roomId: string): string {
  return `${roomTag(prefix, roomId)}:headrev`
}
export function gensKey(prefix: string, roomId: string): string {
  return `${roomTag(prefix, roomId)}:gens`
}
export function genPrefix(prefix: string, roomId: string, inc: string): string {
  return `${roomTag(prefix, roomId)}:g:${inc}`
}
export function generationKeysKey(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:keys`
}
export function revKey(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:rev`
}
export function cellKeyPrefix(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:c:`
}
export function cellKey(prefix: string, roomId: string, inc: string, key: string): string {
  return `${cellKeyPrefix(prefix, roomId, inc)}${key}`
}
export function orderKey(prefix: string, roomId: string, inc: string, laneKey: string): string {
  return `${genPrefix(prefix, roomId, inc)}:o:${laneKey}`
}
export function retainedKeyPrefix(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:rt:`
}
export function retainedKey(prefix: string, roomId: string, inc: string, laneKey: string): string {
  return `${retainedKeyPrefix(prefix, roomId, inc)}${laneKey}`
}
export function channelKey(prefix: string, roomId: string, inc: string, laneKey: string): string {
  return `${roomTag(prefix, roomId)}:ch:${inc}:${laneKey}`
}
export function generationInvalidationChannel(prefix: string, roomId: string, inc: string): string {
  return `${roomTag(prefix, roomId)}:invalidate:${inc}`
}
// The directory's two keys share their own tag so the tag-guarded delete stays one slot under Cluster.
export function directoryIndexKey(prefix: string): string {
  return `${prefix}room-dir:{${prefix}dir}:index`
}
export function directoryTagsKey(prefix: string): string {
  return `${prefix}room-dir:{${prefix}dir}:tags`
}
