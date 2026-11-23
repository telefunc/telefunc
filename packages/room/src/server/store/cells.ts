export {
  MEMBER_CELL_PREFIX,
  memberCellKey,
  memberLeaseCellKey,
  MEMBER_LEASE_BESIDE,
  memberIdOfCellKey,
  departingCellKey,
  identityCellKey,
  identityCellPrefix,
  HOLDS_CELL_KEY,
  JOINS_CELL_KEY,
  LEASE_CELL_PREFIX,
  leaseCellKey,
  leaseClaimCellKey,
  leaseOfCellKey,
  hostedMembers,
  leaseMemberCellPrefix,
  leaseMemberCellKey,
  memberIdOfLeaseMemberKey,
  leaseViewCellPrefix,
  viewCellKey,
  META_CELL_PREFIX,
  META_SEQ_CELL_KEY,
  META_GEN_CELL_KEY,
  META_DROP_CELL_PREFIX,
  META_DROP_PENDING_CELL_KEY,
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

import type { CellsBeside, RequiredCells } from '../../backend/contract.js'

// Cell keys: drivers scope cells by (room, incarnation), so a key names only what is inside the room.
const MEMBER_CELL_PREFIX = 'm:'
function memberCellKey(memberId: string): string {
  return MEMBER_CELL_PREFIX + memberId
}
function memberIdOfCellKey(key: string): string {
  return key.slice(MEMBER_CELL_PREFIX.length)
}
/** The id of the lease a member joined under, as text: written and removed with its record, so a read of the record
 *  reads its lease beside it, by `MEMBER_LEASE_BESIDE`. */
const MEMBER_LEASE_CELL_PREFIX = 'ml:'
function memberLeaseCellKey(memberId: string): string {
  return MEMBER_LEASE_CELL_PREFIX + memberId
}
/** A member that left, with its cause: an expiring cell its leave writes, which the backend deletes
 *  `ROOM_DEPARTING_RECORD_MS` later. A view that missed the leave event ends the member with it. */
const DEPARTING_CELL_PREFIX = 'departing:'
function departingCellKey(memberId: string): string {
  return DEPARTING_CELL_PREFIX + memberId
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
/** One per server hosting a member or a page's view in the room: its lease (see `lease.ts`), a cell that lapses unless
 *  the server renews it. Once a reap claims it, its claim is a cell of its own, which names the reaping server and
 *  lapses unless that server renews it, and the lease's cell is gone. Lease ids hold no `:`, so no lease's key is a
 *  claim's. */
const LEASE_CELL_PREFIX = 'session:'
function leaseCellKey(lease: string): string {
  return LEASE_CELL_PREFIX + lease
}
const LEASE_CLAIM_CELL_PREFIX = `${LEASE_CELL_PREFIX}claim:`
function leaseClaimCellKey(lease: string): string {
  return LEASE_CLAIM_CELL_PREFIX + lease
}
/** The lease a lease's or a claim's cell is of, and whether it is the claim. */
function leaseOfCellKey(key: string): { lease: string; claim: boolean } {
  return key.startsWith(LEASE_CLAIM_CELL_PREFIX)
    ? { lease: key.slice(LEASE_CLAIM_CELL_PREFIX.length), claim: true }
    : { lease: key.slice(LEASE_CELL_PREFIX.length), claim: false }
}
/** Beside each member's record, the cell of the lease it joined under, which a read gives only while it is live. */
const MEMBER_LEASE_BESIDE: CellsBeside = {
  of: MEMBER_CELL_PREFIX,
  reference: MEMBER_LEASE_CELL_PREFIX,
  target: LEASE_CELL_PREFIX,
}
/** What a commit to or from `ids` requires: each member there, on a lease that can host it, unclaimed and not lapsed. */
function hostedMembers(...ids: string[]): RequiredCells {
  return { keys: ids.map(memberCellKey), beside: MEMBER_LEASE_BESIDE }
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
 *  started, a cell per top-level key in that generation, a mark per generation a replace left to drop, and beside the
 *  marks the flag that a drop is pending. */
const META_CELL_PREFIX = 'meta:'
const META_SEQ_CELL_KEY = 'meta:seq'
const META_GEN_CELL_KEY = 'meta:gen'
const META_DROP_CELL_PREFIX = 'meta:drop:'
/** Written by each replace with its mark, under the marks' prefix, so a read of the marks reads it at the version that
 *  covers them; retired by the drop that finished them. A meta write reads it to take a pending drop forward. */
const META_DROP_PENDING_CELL_KEY = 'meta:drop:pending'
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
