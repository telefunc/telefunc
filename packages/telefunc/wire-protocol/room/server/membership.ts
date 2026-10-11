export {
  holdViews,
  removeView,
  createMember,
  updateMemberRecord,
  renewMemberLeases,
  evictMember,
  reapAndReadRoster,
  reapAndReadMembersById,
  reapAndCountPresence,
  reapAndResolveIdentity,
}

import { assert } from '../../../utils/assert.js'
import { getRoomBackend } from '../../backend/install.js'
import { nextTurnOf } from '../../flow-control/macrotask-yield.js'
import type { CellCount, CellCounter, CellMutation, CellSelector } from '../../backend/room/contract.js'
import { ROOM_MEMBER_TTL_MS, ROOM_MEMBER_WRITES_IN_FLIGHT_MAX, ROOM_PLAN_SLICE_MS } from '../constants.js'
import { participantGoneError, roomClosedError } from '../errors.js'
import {
  leaveCauseToWire,
  type HoldConfig,
  type MemberSnapshot,
  type RoomDataEnvelope,
  type RoomMemberRecord,
  type WireLeaveCause,
} from '../protocol.js'
import type { LeaveCause } from '../types.js'
import { SEMANTIC_LANE, decodeRoomRecord, encodeRoomRecord, publishCtrl } from './lanes.js'
import { CX_CONFLICT, retryCompareExchange } from './cx.js'
import { afterHoldChange, holdChange, holdCounts, type RoomIncarnation } from './lifecycle.js'
import {
  CLEANUP_CELL_PREFIX,
  MEMBER_CELL_PREFIX,
  VIEW_CELL_PREFIX,
  cleanupCellKey,
  identityCellKey,
  identityCellPrefix,
  memberCellKey,
  memberIdOfCellKey,
  memberIdOfCleanupKey,
  viewCellKey,
  viewIdOfCellKey,
} from './cells.js'

type CellPlan<T> = { value: T; mutations: CellMutation[]; counts?: CellCount[] }
type CellCommit<T> = { value: T; counters: ReadonlyMap<string, CellCounter> }
type PendingMemberCleanup = { cause: WireLeaveCause; hidden?: true }
/** A page's view of the room, renewed like a member record. */
type ViewRecord = { seenAt: number }

function isLapsed(record: { seenAt: number }): boolean {
  return Date.now() - record.seenAt > ROOM_MEMBER_TTL_MS
}

async function readCells(roomId: string, inc: string, selector: CellSelector): Promise<Map<string, Uint8Array>> {
  const result = await getRoomBackend().readCells(roomId, inc, selector)
  if ('staleInc' in result) throw roomClosedError(roomId)
  return result.cells
}

/** Reads `keys`, plans, and commits only while each of them is still as read. */
async function mutateCells<T>(
  roomId: string,
  inc: string,
  keys: string[],
  plan: (cells: ReadonlyMap<string, Uint8Array>) => CellPlan<T>,
): Promise<CellCommit<T>> {
  const backend = getRoomBackend()
  return await retryCompareExchange(async () => {
    const read = await backend.readCells(roomId, inc, { keys })
    if ('staleInc' in read) throw roomClosedError(roomId)
    const { value, mutations, counts = [] } = plan(read.cells)
    if (mutations.length === 0 && counts.length === 0) return { value, counters: new Map() }
    const expect = keys.map((key) => ({ key, version: read.versions.get(key) ?? null }))
    const result = await backend.compareExchangeCells(roomId, inc, { expect, mutations, counts })
    if (result === 'stale-inc') throw roomClosedError(roomId)
    return result === 'conflict' ? CX_CONFLICT : { value, counters: result.committed }
  })
}

/** Persist a join's member record and identity marker; a non-hidden member holds the room. */
async function createMember(room: RoomIncarnation, id: string, record: RoomMemberRecord): Promise<void> {
  const mutations: CellMutation[] = [{ key: memberCellKey(id), bytes: encodeRoomRecord(record) }]
  if (record.identity !== undefined)
    mutations.push({ key: identityCellKey(record.identity, id), bytes: new Uint8Array() })
  const config = record.hidden ? null : room.hold
  const keys = mutations.map(({ key }) => key)
  const { counters } = await mutateCells(room.id, room.inc, keys, () => ({
    value: undefined,
    mutations,
    counts: holdCounts(config, 1, true),
  }))
  afterHoldChange(room, holdChange(config, counters, 1, true))
}

/** Writes or renews pages' views in one write; each holds the room, and a room without hold timeouts keeps none. A
 *  renewal also restores a view whose write failed or that was reaped while its page held on. */
async function holdViews(room: RoomIncarnation, ids: string[]): Promise<void> {
  const { hold } = room
  if (hold === null) return
  const keys = ids.map(viewCellKey)
  const { value: added, counters } = await mutateCells(room.id, room.inc, keys, (cells) => {
    const added = keys.filter((key) => !cells.has(key)).length
    const view = encodeRoomRecord({ seenAt: Date.now() } satisfies ViewRecord)
    const views = keys.map((key) => ({ key, bytes: view }))
    return { value: added, mutations: views, counts: holdCounts(hold, added) }
  })
  if (added > 0) afterHoldChange(room, holdChange(hold, counters, added))
}

/** With `onlyIfLapsed`, a view renewed meanwhile stays. */
async function removeView(room: RoomIncarnation, id: string, opts?: { onlyIfLapsed: true }): Promise<void> {
  const key = viewCellKey(id)
  const { value: config, counters } = await mutateCells(room.id, room.inc, [key], (cells) => {
    const raw = cells.get(key)
    if (raw === undefined || (opts?.onlyIfLapsed && !isLapsed(decodeRoomRecord<ViewRecord>(raw))))
      return { value: null, mutations: [] }
    return { value: room.hold, mutations: [{ key, bytes: null }], counts: holdCounts(room.hold, -1) }
  })
  afterHoldChange(room, holdChange(config, counters, -1))
}

/** Read-modify-write one member record; a returned `next` is stored with a renewed lease. */
async function mutateMember<T>(
  roomId: string,
  inc: string,
  id: string,
  update: (record: RoomMemberRecord | null) => { value: T; next?: RoomMemberRecord },
): Promise<T> {
  const key = memberCellKey(id)
  const { value } = await mutateCells(roomId, inc, [key], (cells) => {
    const raw = cells.get(key)
    const { value, next } = update(raw === undefined ? null : decodeRoomRecord<RoomMemberRecord>(raw))
    if (next === undefined) return { value, mutations: [] }
    return { value, mutations: [{ key, bytes: encodeRoomRecord({ ...next, seenAt: Date.now() }) }] }
  })
  return value
}

async function updateMemberRecord<T>(
  roomId: string,
  inc: string,
  id: string,
  update: (record: RoomMemberRecord) => { value: T; next?: RoomMemberRecord },
): Promise<T> {
  return await mutateMember(roomId, inc, id, (record) => {
    if (record === null) throw participantGoneError(id)
    return update(record)
  })
}

/** Each renewal is its own compare-exchange; a failed one fails the call once all are tried. */
async function renewMemberLeases(roomId: string, inc: string, ids: readonly string[]): Promise<void> {
  await eachInFlight(ids, (id) =>
    mutateMember(roomId, inc, id, (record) =>
      record === null ? { value: undefined } : { value: undefined, next: record },
    ),
  )
}

/** Runs `task` on each item, `ROOM_MEMBER_WRITES_IN_FLIGHT_MAX` at a time and a `ROOM_PLAN_SLICE_MS` slice per turn;
 *  throws the first failure once every task ran. */
async function eachInFlight<T>(items: readonly T[], task: (item: T) => Promise<unknown>): Promise<void> {
  const pending = items.values()
  let deadline = performance.now() + ROOM_PLAN_SLICE_MS
  let turn: Promise<void> | null = null
  let failure = null as { error: unknown } | null
  const worker = async () => {
    for (let next = pending.next(); next.done !== true; next = pending.next()) {
      if (performance.now() >= deadline)
        await (turn ??= nextTurnOf().then(() => {
          turn = null
          deadline = performance.now() + ROOM_PLAN_SLICE_MS
        }))
      try {
        await task(next.value)
      } catch (error) {
        failure ??= { error }
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(items.length, ROOM_MEMBER_WRITES_IN_FLIGHT_MAX) }, worker))
  if (failure) throw failure.error
}

/** Remove a member and record the cleanup in one write, then finish the cleanup. With `onlyIfLapsed`, a member whose
 *  lease was renewed meanwhile stays, and its record is returned. */
async function evictMember(
  room: RoomIncarnation,
  id: string,
  identity: string | null,
  cause: LeaveCause,
  opts?: { onlyIfLapsed: true },
): Promise<RoomMemberRecord | null> {
  try {
    return await removeMemberCells(room, id, identity, cause, opts)
  } catch (error) {
    // A room that closed under the eviction ended the member with everyone else.
    const head = await getRoomBackend().readHead(room.id)
    if (head?.state !== 'open' || head.currentInc !== room.inc) return null
    throw error
  }
}

async function removeMemberCells(
  room: RoomIncarnation,
  id: string,
  identity: string | null,
  cause: LeaveCause,
  opts?: { onlyIfLapsed: true },
): Promise<RoomMemberRecord | null> {
  const memberKey = memberCellKey(id)
  const cleanupKey = cleanupCellKey(id)
  const removedKeys = identity === null ? [memberKey] : [memberKey, identityCellKey(identity, id)]
  const { value: outcome, counters } = await mutateCells<
    { live: RoomMemberRecord } | { cleanup: boolean; config: HoldConfig | null }
  >(room.id, room.inc, [...removedKeys, cleanupKey], (cells) => {
    const pending = cells.has(cleanupKey)
    const raw = cells.get(memberKey)
    const record = raw === undefined ? null : decodeRoomRecord<RoomMemberRecord>(raw)
    if (record !== null && opts?.onlyIfLapsed && !isLapsed(record)) return { value: { live: record }, mutations: [] }
    if (record === null) return { value: { cleanup: pending, config: null }, mutations: [] }
    const cleanup: PendingMemberCleanup = {
      cause: leaveCauseToWire(cause),
      ...(record.hidden ? { hidden: true } : {}),
    }
    const config = record.hidden ? null : room.hold
    return {
      value: { cleanup: true, config },
      mutations: [
        ...removedKeys.map((key) => ({ key, bytes: null })),
        ...(pending ? [] : [{ key: cleanupKey, bytes: encodeRoomRecord(cleanup) }]),
      ],
      counts: holdCounts(config, -1),
    }
  })
  if ('live' in outcome) return outcome.live
  afterHoldChange(room, holdChange(outcome.config, counters, -1))
  if (outcome.cleanup) await finishPendingMemberCleanup(room.id, room.inc, id)
  return null
}

/** Live members, and departing ones whose eviction the read completes; lapsed members and views are reaped on the way. */
async function reapAndReadRoster(
  room: RoomIncarnation,
): Promise<{ members: MemberSnapshot[]; departing: Set<string> }> {
  const cells = await readCells(room.id, room.inc, { prefix: MEMBER_CELL_PREFIX })
  // After the member read, so an eviction committing in between shows up as departing.
  const cleanups = await readCells(room.id, room.inc, { prefix: CLEANUP_CELL_PREFIX })
  const departing = new Set([...cleanups.keys()].map(memberIdOfCleanupKey))
  await eachInFlight([...cleanups], ([key, raw]) => completeCleanup(room.id, room.inc, memberIdOfCleanupKey(key), raw))
  const views = await readCells(room.id, room.inc, { prefix: VIEW_CELL_PREFIX })
  await eachInFlight([...views], async ([key, raw]) => {
    if (isLapsed(decodeRoomRecord<ViewRecord>(raw)))
      await removeView(room, viewIdOfCellKey(key), { onlyIfLapsed: true })
  })
  const members = await liveMembers(
    room,
    [...cells].map(([key, raw]) => [memberIdOfCellKey(key), raw] as const).filter(([id]) => !departing.has(id)),
  )
  return { members, departing }
}

async function reapAndReadMembersById(room: RoomIncarnation, ids: string[]): Promise<MemberSnapshot[]> {
  const cells = await readCells(room.id, room.inc, { keys: ids.map(memberCellKey) })
  return await liveMembers(
    room,
    ids.flatMap((id) => {
      const raw = cells.get(memberCellKey(id))
      return raw === undefined ? [] : [[id, raw] as const]
    }),
  )
}

async function liveMembers(
  room: RoomIncarnation,
  entries: ReadonlyArray<readonly [string, Uint8Array]>,
): Promise<MemberSnapshot[]> {
  const members: (MemberSnapshot | null)[] = []
  await eachInFlight([...entries.keys()], async (i) => {
    const [id, raw] = entries[i]!
    const candidate = decodeRoomRecord<RoomMemberRecord>(raw)
    const record = isLapsed(candidate)
      ? await evictMember(room, id, candidate.identity ?? null, { type: 'disconnected' }, { onlyIfLapsed: true })
      : candidate
    members[i] = record && memberSnapshot(id, record)
  })
  return members.filter((member) => member !== null)
}

function memberSnapshot(id: string, record: RoomMemberRecord): MemberSnapshot {
  return {
    id,
    meta: record.meta,
    joinedAt: record.joinedAt,
    metaSeq: record.metaSeq,
    identity: record.identity ?? null,
    ...(record.hidden ? { hidden: true } : {}),
  }
}

async function reapAndCountPresence(room: RoomIncarnation): Promise<number> {
  return (await reapAndReadRoster(room)).members.filter((member) => !member.hidden).length
}

async function reapAndResolveIdentity(room: RoomIncarnation, identity: string): Promise<MemberSnapshot[]> {
  const prefix = identityCellPrefix(identity)
  const markers = await readCells(room.id, room.inc, { prefix })
  const ids = [...markers.keys()].map((key) => key.slice(prefix.length))
  const members = await reapAndReadMembersById(room, ids)
  // A member and its identity marker are written and removed in one compare-exchange.
  assert(members.every((member) => member.identity === identity))
  return members
}

async function dropRetainedOwnedBy(roomId: string, inc: string, memberId: string): Promise<void> {
  const backend = getRoomBackend()
  for (const lane of await backend.listRetained(roomId, inc, { member: memberId }))
    await backend.deleteRetained(roomId, inc, lane)
  const text = await backend.readRetained(roomId, inc, SEMANTIC_LANE)
  if (text !== null && decodeRoomRecord<RoomDataEnvelope>(text.payload).from === memberId)
    await backend.deleteRetained(roomId, inc, SEMANTIC_LANE, { ifSeq: text.seq })
}

async function finishPendingMemberCleanup(roomId: string, inc: string, memberId: string): Promise<void> {
  const key = cleanupCellKey(memberId)
  const raw = (await readCells(roomId, inc, { keys: [key] })).get(key)
  if (raw !== undefined) await completeCleanup(roomId, inc, memberId, raw)
}

async function completeCleanup(roomId: string, inc: string, memberId: string, raw: Uint8Array): Promise<void> {
  const key = cleanupCellKey(memberId)
  const cleanup = decodeRoomRecord<PendingMemberCleanup>(raw)
  await dropRetainedOwnedBy(roomId, inc, memberId)
  await publishCtrl(roomId, inc, {
    __r: 'leave',
    id: memberId,
    ...cleanup.cause,
    ...(cleanup.hidden ? { hidden: true } : {}),
  })
  await mutateCells(roomId, inc, [key], (cells) => ({
    value: undefined,
    mutations: cells.has(key) ? [{ key, bytes: null }] : [],
  }))
}
