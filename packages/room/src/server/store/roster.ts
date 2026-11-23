export { reapAndReadRoster, readLeaveCauses, reapAndReadMembersById, reapAndCountPresence, reapAndResolveIdentity }

import { assert } from '../../utils/assert.js'
import type { MemberSnapshot } from '../../shared/protocol.js'
import type { RoomMemberRecord, RoomIncarnation } from '../protocol.js'
import type { LeaveCause, ParticipantMeta } from '../../types.js'
import { memberMetaOf, readMembersMetaCells, type MemberMetaCells } from './meta.js'
import { readOpenCells, readPrefix, readPrefixAt, type Kept } from './cellStore.js'
import { eachInFlight } from './inFlight.js'
import type { CellsRead } from '../../backend/contract.js'
import type { RoomStore } from '../../backend/RoomStore.js'
import { roomClosedError } from '../../shared/errors.js'
import { leaveCauseFromWire, type WireLeaveCause } from '../../shared/protocol.js'
import { decodeRoomRecord, type CommitFitsBeforeWriting } from '../lanes/lanes.js'
import {
  LEASE_CELL_PREFIX,
  MEMBER_CELL_PREFIX,
  MEMBER_LEASE_BESIDE,
  departingCellKey,
  identityCellPrefix,
  memberCellKey,
  memberIdOfCellKey,
} from './cells.js'
import { leaseCanHost, reapLeases } from './lease.js'

// The room's members as read from its cells, each read after reaping the lapsed leases it found. A member counts only
// while it hosts: the lease it joined under, read beside its record in the same turn, can host.

/** The members that host, and the ids of those a read left out as their lease can't: their removal is under way. */
type Roster = { members: MemberSnapshot[]; leaving: ReadonlySet<string> }

/** Whether the member whose record is `key` in `read` hosts: the lease read beside it can host. A member with no lease
 *  reference was written by a server from before members had one; it is read as it is, and once that server stopped,
 *  its lease lapses within `ROOM_LEASE_TTL_MS` and the next read's reap ends it. */
function hosts(read: Extract<CellsRead, { kind: 'read' }>, key: string): boolean {
  const lease = read.beside?.get(key)
  if (lease === undefined) return true
  return lease !== null && leaseCanHost(key, lease, read.now)
}

const HOSTED_MEMBERS: Kept = { beside: MEMBER_LEASE_BESIDE, keeps: hosts }

/** Live members, read again after a lapsed lease's were removed. */
async function reapAndReadRoster(store: RoomStore, room: RoomIncarnation): Promise<Roster> {
  const [leases, roster] = await Promise.all([
    readPrefixAt(store, room.id, room.inc, LEASE_CELL_PREFIX),
    readRoster(store, room),
  ])
  return (await reapLeases(store, room, leases)) ? await readRoster(store, room) : roster
}

async function readRoster(store: RoomStore, room: RoomIncarnation): Promise<Roster> {
  const { cells, left } = await readPrefixAt(store, room.id, room.inc, MEMBER_CELL_PREFIX, HOSTED_MEMBERS)
  // After the member read: each member's cells are as new as its record or newer, so the updates after the record's
  // seq bring a view to them.
  const metaCells = await readMembersMetaCells(room, cells)
  return { members: await liveMembers(memberRecords(cells), metaCells), leaving: new Set(left.map(memberIdOfCellKey)) }
}

/** Why each of `ids` left, for those whose departing record the backend hasn't expired yet. Read after the members, a
 *  leave committing in between has its record. */
async function readLeaveCauses(
  store: RoomStore,
  room: RoomIncarnation,
  ids: readonly string[],
): Promise<Map<string, LeaveCause>> {
  const causes = new Map<string, LeaveCause>()
  if (ids.length === 0) return causes
  const read = await store.reads.readExpiringCells(room.id, room.inc, ids.map(departingCellKey))
  if (read.kind === 'stale-incarnation') throw roomClosedError(room.id)
  for (const id of ids) {
    const raw = read.cells.get(departingCellKey(id))
    if (raw !== undefined) causes.set(id, leaveCauseFromWire(decodeRoomRecord<WireLeaveCause>(raw)))
  }
  return causes
}

function* memberRecords(cells: ReadonlyMap<string, Uint8Array>): Generator<readonly [string, Uint8Array]> {
  for (const [key, raw] of cells) yield [memberIdOfCellKey(key), raw]
}

/** Read again after a lapsed lease's members were removed. `checkBeforeWriting` runs on each read's members before
 *  what follows it: on the first read's before the reap, the lookup's first write, so a call its own commit would
 *  refuse writes nothing; on the reread's before the call's own commits to them. */
async function reapAndReadMembersById(
  store: RoomStore,
  room: RoomIncarnation,
  ids: string[],
  checkBeforeWriting?: CommitFitsBeforeWriting,
): Promise<MemberSnapshot[]> {
  const [leases, members] = await Promise.all([
    readPrefixAt(store, room.id, room.inc, LEASE_CELL_PREFIX),
    readMembersById(store, room, ids),
  ])
  checkBeforeWriting?.check(members)
  if (!(await reapLeases(store, room, leases))) return members
  const reread = await readMembersById(store, room, ids)
  checkBeforeWriting?.check(reread)
  return reread
}

async function readMembersById(store: RoomStore, room: RoomIncarnation, ids: string[]): Promise<MemberSnapshot[]> {
  const read = await readOpenCells(store, room.id, room.inc, ids.map(memberCellKey), MEMBER_LEASE_BESIDE)
  const { cells } = read
  const metaCells = new Map<string, MemberMetaCells>()
  const entries = ids.flatMap((id) => {
    const key = memberCellKey(id)
    const raw = cells.get(key)
    return raw === undefined || !hosts(read, key) ? [] : [[id, raw] as const]
  })
  await eachInFlight(entries, async ([id]) => {
    metaCells.set(id, (await readMembersMetaCells(room, cells, id)).get(id) ?? [])
  })
  return await liveMembers(entries, metaCells)
}

/** In the order of `entries`. */
async function liveMembers(
  entries: Iterable<readonly [string, Uint8Array]>,
  metaCells: ReadonlyMap<string, MemberMetaCells>,
): Promise<MemberSnapshot[]> {
  const members: MemberSnapshot[] = []
  await eachInFlight(numbered(entries), async ([i, [id, raw]]) => {
    const record = decodeRoomRecord<RoomMemberRecord>(raw)
    const cells = metaCells.get(id)
    const meta = cells !== undefined ? await memberMetaOf(id, record.metaGen, cells) : {}
    members[i] = memberSnapshot(id, record, meta)
  })
  return members
}

function* numbered<T>(items: Iterable<T>): Generator<readonly [number, T]> {
  let i = 0
  for (const item of items) yield [i++, item]
}

function memberSnapshot(id: string, record: RoomMemberRecord, meta: ParticipantMeta): MemberSnapshot {
  const snapshot: MemberSnapshot = {
    id,
    meta,
    joinedAt: record.joinedAt,
    metaSeq: record.metaSeq,
    identity: record.identity ?? null,
  }
  if (record.hidden) snapshot.hidden = true
  if (record.disconnected) snapshot.disconnected = true
  return snapshot
}

/** Read again after a lapsed lease's members were removed. */
async function reapAndCountPresence(store: RoomStore, room: RoomIncarnation): Promise<number> {
  const [leases, count] = await Promise.all([
    readPrefixAt(store, room.id, room.inc, LEASE_CELL_PREFIX),
    countPresence(store, room),
  ])
  return (await reapLeases(store, room, leases)) ? await countPresence(store, room) : count
}

/** The members that host and aren't hidden, from their records alone: a count reads no meta. */
async function countPresence(store: RoomStore, room: RoomIncarnation): Promise<number> {
  const { cells } = await readPrefixAt(store, room.id, room.inc, MEMBER_CELL_PREFIX, HOSTED_MEMBERS)
  let count = 0
  for (const raw of cells.values()) {
    if (!decodeRoomRecord<RoomMemberRecord>(raw).hidden) count++
  }
  return count
}

/** Its tabs, as `reapAndReadMembersById` reads them, with `checkBeforeWriting` run as it runs it. */
async function reapAndResolveIdentity(
  store: RoomStore,
  room: RoomIncarnation,
  identity: string,
  checkBeforeWriting?: CommitFitsBeforeWriting,
): Promise<MemberSnapshot[]> {
  const prefix = identityCellPrefix(identity)
  const markers = await readPrefix(store, room.id, room.inc, prefix)
  const ids = Array.from(markers.keys(), (key) => key.slice(prefix.length))
  const members = await reapAndReadMembersById(store, room, ids, checkBeforeWriting)
  // A member and its identity marker are written and removed in one compare-exchange.
  assert(members.every((member) => member.identity === identity))
  return members
}
