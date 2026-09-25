export {
  MEMBER_CELL_PREFIX,
  CLEANUP_CELL_PREFIX,
  memberCellKey,
  memberIdOfCellKey,
  cleanupCellKey,
  memberIdOfCleanupKey,
  identityCellKey,
  identityCellPrefix,
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
