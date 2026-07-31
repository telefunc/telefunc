export {
  roomCtrlKey,
  roomMemberKvKey,
  roomMemberKvPrefix,
  roomMemberCleanupKvKey,
  roomMemberCleanupKvPrefix,
  roomIdentityMemberKvKey,
  roomIdentityKvPrefix,
}

import { assertUsage } from '../../utils/assert.js'

/** Reserved pub/sub + KV namespace for rooms. Don't use it for `BroadcastChannel` keys. */
const ROOM_KEY_NAMESPACE = 'telefunc:room:'
/** App-supplied components may contain delimiters; malformed UTF-16 fails as usage. */
function roomKeyComponent(value: string): string {
  assertUsage(value.isWellFormed(), 'Room key components should be well-formed strings')
  return encodeURIComponent(value)
}
/** Stable channel identity used by a Room stub crossing a response. */
function roomCtrlKey(roomId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomKeyComponent(roomId)}`
}
/** KV key of one member record. */
function roomMemberKvKey(roomId: string, memberId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomKeyComponent(roomId)}:m:${memberId}`
}
/** KV prefix under which all of a room's member records live. Member IDs are UUIDs, which is how member records are told apart from keys of other rooms whose ID shares the prefix. */
function roomMemberKvPrefix(roomId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomKeyComponent(roomId)}:m:`
}
/** Durable work left by membership eviction. The marker is committed in the same cell transaction that removes the member, then cleared only after retained data and the semantic leave are done. */
function roomMemberCleanupKvKey(roomId: string, memberId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomKeyComponent(roomId)}:cleanup:${memberId}`
}
function roomMemberCleanupKvPrefix(roomId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomKeyComponent(roomId)}:cleanup:`
}
/** Reserved KV namespace for the identity→membership index — kept separate from `ROOM_KEY_NAMESPACE` so a room's member-record scan (`roomMemberKvPrefix`) never sweeps it. */
const IDENTITY_KEY_NAMESPACE = 'telefunc:identity:'
/** KV key marking one membership of an app identity: one key per (room, identity, member), so concurrent joins of the same identity never clobber each other and each membership stays independently
 * removable — a shared list value would put every membership of an identity behind one read-modify-write of the same record. The index is a hint — written before the member record and cleared after
 * it, so it may transiently over-include but never silently under-includes; readers confirm each member ID against its record (identity match), which makes phantoms impossible. Room and identity are
 * encoded so a `:` in either can't collide across pairs; the member ID is a delimiter-free UUID.
 */
function roomIdentityMemberKvKey(roomId: string, identity: string, memberId: string): string {
  return `${IDENTITY_KEY_NAMESPACE}${roomKeyComponent(roomId)}:${roomKeyComponent(identity)}:${memberId}`
}
/** KV prefix enumerating every membership of one identity in one room (`keys()` → member IDs). */
function roomIdentityKvPrefix(roomId: string, identity: string): string {
  return `${IDENTITY_KEY_NAMESPACE}${roomKeyComponent(roomId)}:${roomKeyComponent(identity)}:`
}
