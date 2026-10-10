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
  META_KEY_CELL_PREFIX,
  META_SEQ_CELL_KEY,
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
/** The room's meta once first written (see `meta.ts`): a cell per top-level key, and a counter of the writes. */
const META_CELL_PREFIX = 'meta:'
const META_KEY_CELL_PREFIX = 'meta:k:'
const META_SEQ_CELL_KEY = 'meta:seq'
// JSON keeps any string, lone surrogates included, as a well-formed one.
function metaKeyCellKey(key: string): string {
  return META_KEY_CELL_PREFIX + JSON.stringify(key)
}
function metaKeyOfCellKey(cellKey: string): string {
  return JSON.parse(cellKey.slice(META_KEY_CELL_PREFIX.length)) as string
}
