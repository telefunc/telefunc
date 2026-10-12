export {
  MEMBER_CELL_PREFIX,
  CLEANUP_CELL_PREFIX,
  memberCellKey,
  memberIdOfCellKey,
  cleanupCellKey,
  memberIdOfCleanupKey,
  identityCellKey,
  identityCellPrefix,
  HOLDS_CELL_KEY,
  JOINS_CELL_KEY,
  VIEW_CELL_PREFIX,
  viewCellKey,
  viewIdOfCellKey,
  META_CELL_PREFIX,
  META_SEQ_CELL_KEY,
  META_GEN_CELL_KEY,
  META_DROP_CELL_PREFIX,
  metaDropCellKey,
  roomMetaGenPrefix,
  MEMBER_META_CELL_PREFIX,
  memberMetaCellPrefix,
  memberMetaGenPrefix,
  memberIdOfMetaCellKey,
  metaKeyCellKey,
  metaKeyOfCellKey,
}

// Cell keys: drivers scope cells by (room, incarnation), so a key names only what is inside the room.
const MEMBER_CELL_PREFIX = 'm:'
const CLEANUP_CELL_PREFIX = 'cleanup:'
function memberCellKey(memberId: string): string {
  return MEMBER_CELL_PREFIX + memberId
}
function memberIdOfCellKey(key: string): string {
  return key.slice(MEMBER_CELL_PREFIX.length)
}
/** Durable eviction work: committed with the member's removal, cleared once retained data and the leave are done. */
function cleanupCellKey(memberId: string): string {
  return CLEANUP_CELL_PREFIX + memberId
}
function memberIdOfCleanupKey(key: string): string {
  return key.slice(CLEANUP_CELL_PREFIX.length)
}
/** One marker per (identity, member), written and removed in the compare-exchange that writes or removes the member. */
function identityCellPrefix(identity: string): string {
  return `identity:${encodeURIComponent(identity)}:`
}
function identityCellKey(identity: string, memberId: string): string {
  return identityCellPrefix(identity) + memberId
}
/** Counters: what holds the room, counted in the compare-exchange that adds or removes a hold, and its joins. */
const HOLDS_CELL_KEY = 'holds'
const JOINS_CELL_KEY = 'joins'
/** One per page holding the room: its view, renewed like a member and reaped once it lapses. */
const VIEW_CELL_PREFIX = 'v:'
function viewCellKey(viewId: string): string {
  return VIEW_CELL_PREFIX + viewId
}
function viewIdOfCellKey(key: string): string {
  return key.slice(VIEW_CELL_PREFIX.length)
}
/** The room's meta once first written (see `meta.ts`): a counter of the writes, the generation the last replace
 *  started, a cell per top-level key in that generation, and a mark per generation a replace left to drop. */
const META_CELL_PREFIX = 'meta:'
const META_SEQ_CELL_KEY = 'meta:seq'
const META_GEN_CELL_KEY = 'meta:gen'
const META_DROP_CELL_PREFIX = 'meta:drop:'
function metaDropCellKey(gen: number): string {
  return META_DROP_CELL_PREFIX + gen
}
function roomMetaGenPrefix(gen: number): string {
  return `meta:k:${gen}:`
}

/** A member's meta, a cell per top-level key in the generation its record names (`RoomMemberRecord.metaGen`). Member
 *  ids hold no `:`. */
const MEMBER_META_CELL_PREFIX = 'pm:'
function memberMetaCellPrefix(memberId: string): string {
  return `${MEMBER_META_CELL_PREFIX}${memberId}:`
}
function memberMetaGenPrefix(memberId: string, gen: number): string {
  return `${memberMetaCellPrefix(memberId)}${gen}:`
}
function memberIdOfMetaCellKey(cellKey: string): string {
  return cellKey.slice(MEMBER_META_CELL_PREFIX.length, cellKey.indexOf(':', MEMBER_META_CELL_PREFIX.length))
}
// A meta key's cell, the room's or a member's, under its generation's prefix. JSON keeps any string, lone surrogates
// included, as a well-formed one.
function metaKeyCellKey(genPrefix: string, key: string): string {
  return genPrefix + JSON.stringify(key)
}
/** The key of a cell under `genPrefix`, or null for another generation's. */
function metaKeyOfCellKey(genPrefix: string, cellKey: string): string | null {
  return cellKey.startsWith(genPrefix) ? (JSON.parse(cellKey.slice(genPrefix.length)) as string) : null
}
