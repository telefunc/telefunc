export {
  DEFAULT_PREFIX,
  redisKeyPrefix,
  broadcastSequenceKey,
  broadcastChannel,
  headKey,
  headRevKey,
  gensKey,
  genPrefix,
  generationKeysKey,
  revKey,
  cellKeyPrefix,
  cellKey,
  orderKey,
  retainedKeyPrefix,
  retainedKey,
  channelKey,
  generationInvalidationChannel,
  directoryIndexKey,
  directoryTagsKey,
}

// Redis keys. Every per-room key shares `{rid}`, so a room is one Cluster slot.
// head/headrev: tf:room:{rid}:{head|headrev}; JSON head plus monotonic revision.
// cells/revision: tf:room:{rid}:g:<inc>:{c:<key>|rev}; logical cells plus coarse generation revision.
// order/retained: tf:room:{rid}:g:<inc>:{o|rt}:<laneKey>; ordering mark or 16-byte mark + payload.
// gen keys: tf:room:{rid}:g:<inc>:keys; generation-owned physical-key set.
// channels: tf:room:{rid}:ch:<inc>:<laneKey>; incarnation-scoped PUBLISH/SUBSCRIBE.
// invalidation: tf:room:{rid}:invalidate:<inc>; published once when a generation drops.
// gens: tf:room:{rid}:gens; installed incarnations.
// directory: <prefix>room-dir:{<prefix>dir}:{index|tags}; one global, co-slotted pair.
// Broadcast: <prefix>seq:{key} (sequence) and <prefix>{t|b}:{key} (text/binary channel), one slot per key.
// Commands take authority time from Redis TIME, never from the caller.

import type { BroadcastRoute } from 'telefunc/__internal'

const DEFAULT_PREFIX = 'tf:'

/** Builders take the prefix as validated here, once: a `{` would open a hash tag of its own. */
function redisKeyPrefix(prefix: string): string {
  if (prefix.includes('{')) throw new Error("Redis key prefix must not contain '{'")
  return prefix
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
function headRevKey(prefix: string, roomId: string): string {
  return `${roomTag(prefix, roomId)}:headrev`
}
function gensKey(prefix: string, roomId: string): string {
  return `${roomTag(prefix, roomId)}:gens`
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
function cellKeyPrefix(prefix: string, roomId: string, inc: string): string {
  return `${genPrefix(prefix, roomId, inc)}:c:`
}
function cellKey(prefix: string, roomId: string, inc: string, key: string): string {
  return `${cellKeyPrefix(prefix, roomId, inc)}${key}`
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
function channelKey(prefix: string, roomId: string, inc: string, laneKey: string): string {
  return `${roomTag(prefix, roomId)}:ch:${inc}:${laneKey}`
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
