export {
  MEMBER_CELL_PREFIX,
  memberCellKey,
  memberIdOfCellKey,
  DEPARTING_CELL_PREFIX,
  departingCellKey,
  memberIdOfDepartingKey,
  identityCellKey,
  identityCellPrefix,
  HOLDS_CELL_KEY,
  JOINS_CELL_KEY,
  LEASE_CELL_PREFIX,
  leaseCellKey,
  leaseMemberCellPrefix,
  leaseMemberCellKey,
  memberIdOfLeaseMemberKey,
  leaseViewCellPrefix,
  viewCellKey,
  META_CELL_PREFIX,
  META_SEQ_CELL_KEY,
  META_GEN_CELL_KEY,
  META_DROP_CELL_PREFIX,
  metaDropCellKey,
  roomMetaGenPrefix,
  MEMBER_META_CELL_PREFIX,
  memberMetaCellPrefix,
  memberMetaGenPrefix,
  memberMetaGenOfCellKey,
  memberIdOfMetaCellKey,
  metaKeyCellKey,
  metaKeyOfCellKey,
  dmReplyCellPrefix,
  dmReplyCellKey,
  ackIdOfDmReplyCellKey,
}

// Cell keys: drivers scope cells by (room, incarnation), so a key names only what is inside the room.
const MEMBER_CELL_PREFIX = 'm:'
function memberCellKey(memberId: string): string {
  return MEMBER_CELL_PREFIX + memberId
}
function memberIdOfCellKey(key: string): string {
  return key.slice(MEMBER_CELL_PREFIX.length)
}
/** A member that left, with its cause, written by its leave and swept `ROOM_DEPARTING_RECORD_MS` after it: a view
 *  that missed the leave event ends the member with it (`DepartingRecord`). */
const DEPARTING_CELL_PREFIX = 'departing:'
function departingCellKey(memberId: string): string {
  return DEPARTING_CELL_PREFIX + memberId
}
function memberIdOfDepartingKey(key: string): string {
  return key.slice(DEPARTING_CELL_PREFIX.length)
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
/** One per server hosting a member or a page's view in the room: its lease (see `lease.ts`). Lease ids
 *  hold no `:`. */
const LEASE_CELL_PREFIX = 'session:'
function leaseCellKey(lease: string): string {
  return LEASE_CELL_PREFIX + lease
}
/** A member's mark under its lease, written and removed with its record, so the lease's cleaner finds it. */
function leaseMemberCellPrefix(lease: string): string {
  return `sm:${lease}:`
}
function leaseMemberCellKey(lease: string, memberId: string): string {
  return leaseMemberCellPrefix(lease) + memberId
}
function memberIdOfLeaseMemberKey(key: string): string {
  return key.slice(key.lastIndexOf(':') + 1)
}
/** One per page holding the room: its view, under the lease that wrote it. */
function leaseViewCellPrefix(lease: string): string {
  return `v:${lease}:`
}
function viewCellKey(lease: string, viewId: string): string {
  return leaseViewCellPrefix(lease) + viewId
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

/** A member's meta, a cell per top-level key in the generation its record names (`RoomMemberRecord.metaGen`), and a
 *  replace's mark, its generation's prefix, until the generation it replaced is dropped. Member ids hold no `:`. */
const MEMBER_META_CELL_PREFIX = 'pm:'
function memberMetaCellPrefix(memberId: string): string {
  return `${MEMBER_META_CELL_PREFIX}${memberId}:`
}
function memberMetaGenPrefix(memberId: string, gen: number): string {
  return `${memberMetaCellPrefix(memberId)}${gen}:`
}
function memberMetaGenOfCellKey(memberId: string, cellKey: string): number {
  const start = memberMetaCellPrefix(memberId).length
  return Number(cellKey.slice(start, cellKey.indexOf(':', start)))
}
function memberIdOfMetaCellKey(cellKey: string): string {
  return cellKey.slice(MEMBER_META_CELL_PREFIX.length, cellKey.indexOf(':', MEMBER_META_CELL_PREFIX.length))
}
/** The cell of a meta key (of the room or of a member), under its generation's prefix. JSON keeps any string, lone
 *  surrogates included, as a well-formed one. */
function metaKeyCellKey(genPrefix: string, key: string): string {
  return genPrefix + JSON.stringify(key)
}
/** The key of a cell under `genPrefix`, or null for another generation's and for the generation's mark, `genPrefix`
 *  itself. */
function metaKeyOfCellKey(genPrefix: string, cellKey: string): string | null {
  return cellKey.startsWith(genPrefix) && cellKey !== genPrefix
    ? (JSON.parse(cellKey.slice(genPrefix.length)) as string)
    : null
}

/** A recipient's reply to an acknowledged DM, under its sender's prefix until the sender settles it. */
function dmReplyCellPrefix(senderId: string): string {
  return `ack:${senderId}:`
}
function dmReplyCellKey(senderId: string, ackId: string): string {
  return dmReplyCellPrefix(senderId) + ackId
}
function ackIdOfDmReplyCellKey(cellKey: string): string {
  return cellKey.slice(cellKey.lastIndexOf(':') + 1)
}
