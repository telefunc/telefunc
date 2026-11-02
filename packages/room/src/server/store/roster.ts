export { reapAndReadRoster, reapAndReadMembersById, reapAndCountPresence, reapAndResolveIdentity }

import { assert } from '../../utils/assert.js'
import type { MemberSnapshot } from '../../shared/protocol.js'
import type { RoomMemberRecord, RoomIncarnation } from '../protocol.js'
import type { ParticipantMeta } from '../../types.js'
import { memberMetaOf, readMembersMetaCells, type MemberMetaCells } from './meta.js'
import { eachInFlight, readOpenCells, readPrefix, readPrefixAt } from './cellStore.js'
import { decodeRoomRecord } from '../lanes/lanes.js'
import {
  DEPARTING_CELL_PREFIX,
  LEASE_CELL_PREFIX,
  MEMBER_CELL_PREFIX,
  identityCellPrefix,
  memberCellKey,
  memberIdOfCellKey,
  memberIdOfDepartingKey,
} from './cells.js'
import { reapLeases } from './lease.js'

// The room's members as read from its cells, each read after reaping the lapsed leases it found.

type Roster = { members: MemberSnapshot[]; departing: Set<string> }

/** Live members, and the departing ones, whose leave is not handed off yet; read again after a lapsed lease's were
 *  removed. */
async function reapAndReadRoster(room: RoomIncarnation): Promise<Roster> {
  const [leases, roster] = await Promise.all([readPrefixAt(room.id, room.inc, LEASE_CELL_PREFIX), readRoster(room)])
  return (await reapLeases(room, leases)) ? await readRoster(room) : roster
}

async function readRoster(room: RoomIncarnation): Promise<Roster> {
  const cells = await readPrefix(room.id, room.inc, MEMBER_CELL_PREFIX)
  // Both after the member read: a leave committing in between shows up as departing, and each member's cells are as new
  // as its record or newer, so the updates after the record's seq bring a view to them.
  const [departingCells, metaCells] = await Promise.all([
    readPrefix(room.id, room.inc, DEPARTING_CELL_PREFIX),
    readMembersMetaCells(room, cells),
  ])
  const departing = new Set(Array.from(departingCells.keys(), memberIdOfDepartingKey))
  const members = await liveMembers(memberRecords(cells), metaCells)
  return { members, departing }
}

function* memberRecords(cells: ReadonlyMap<string, Uint8Array>): Generator<readonly [string, Uint8Array]> {
  for (const [key, raw] of cells) yield [memberIdOfCellKey(key), raw]
}

/** Read again after a lapsed lease's members were removed. */
async function reapAndReadMembersById(room: RoomIncarnation, ids: string[]): Promise<MemberSnapshot[]> {
  const [leases, members] = await Promise.all([
    readPrefixAt(room.id, room.inc, LEASE_CELL_PREFIX),
    readMembersById(room, ids),
  ])
  return (await reapLeases(room, leases)) ? await readMembersById(room, ids) : members
}

async function readMembersById(room: RoomIncarnation, ids: string[]): Promise<MemberSnapshot[]> {
  const { cells } = await readOpenCells(room.id, room.inc, ids.map(memberCellKey))
  const metaCells = new Map<string, MemberMetaCells>()
  const entries = ids.flatMap((id) => {
    const raw = cells.get(memberCellKey(id))
    return raw === undefined ? [] : [[id, raw] as const]
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
async function reapAndCountPresence(room: RoomIncarnation): Promise<number> {
  const [leases, count] = await Promise.all([readPrefixAt(room.id, room.inc, LEASE_CELL_PREFIX), countPresence(room)])
  return (await reapLeases(room, leases)) ? await countPresence(room) : count
}

/** The members not hidden, from their records alone: a count reads no meta. */
async function countPresence(room: RoomIncarnation): Promise<number> {
  const cells = await readPrefix(room.id, room.inc, MEMBER_CELL_PREFIX)
  let count = 0
  for (const raw of cells.values()) {
    if (!decodeRoomRecord<RoomMemberRecord>(raw).hidden) count++
  }
  return count
}

async function reapAndResolveIdentity(room: RoomIncarnation, identity: string): Promise<MemberSnapshot[]> {
  const prefix = identityCellPrefix(identity)
  const markers = await readPrefix(room.id, room.inc, prefix)
  const ids = Array.from(markers.keys(), (key) => key.slice(prefix.length))
  const members = await reapAndReadMembersById(room, ids)
  // A member and its identity marker are written and removed in one compare-exchange.
  assert(members.every((member) => member.identity === identity))
  return members
}
