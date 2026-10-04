export {
  createHold,
  createView,
  renewView,
  removeView,
  createMember,
  updateMemberRecord,
  renewMemberLease,
  evictMember,
  reapAndReadRoster,
  reapAndReadMembersById,
  reapAndCountPresence,
  reapAndResolveIdentity,
}

import { assert } from '../../../utils/assert.js'
import { getRoomBackend } from '../../backend/install.js'
import type { CellMutation, CellSelector } from '../../backend/room/contract.js'
import { ROOM_MEMBER_TTL_MS } from '../constants.js'
import { participantGoneError, roomClosedError } from '../errors.js'
import { leaveCauseToWire } from '../model.js'
import type { MemberSnapshot, RoomDataEnvelope, RoomMemberRecord, WireLeaveCause } from '../protocol.js'
import type { LeaveCause } from '../types.js'
import { SEMANTIC_LANE, decodeRoomRecord, encodeRoomRecord, publishCtrl } from './lanes.js'
import { CX_CONFLICT, retryCompareExchange } from './cx.js'
import { afterHoldChange, countHold, scheduleClose, type HoldChange, type HoldRecord } from './lifecycle.js'
import {
  CLEANUP_CELL_PREFIX,
  HOLD_CELL_KEY,
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

type CellPlan<T> = { value: T; mutations: CellMutation[] }
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

async function mutateCells<T>(
  roomId: string,
  inc: string,
  selector: CellSelector,
  plan: (cells: ReadonlyMap<string, Uint8Array>) => CellPlan<T>,
): Promise<T> {
  const backend = getRoomBackend()
  return await retryCompareExchange(roomId, async () => {
    const read = await backend.readCells(roomId, inc, selector)
    if ('staleInc' in read) throw roomClosedError(roomId)
    const next = plan(read.cells)
    if (next.mutations.length === 0) return next.value
    const result = await backend.compareExchangeCells(roomId, inc, read.revision, next.mutations)
    if (result === 'stale-inc') throw roomClosedError(roomId)
    return result === 'committed' ? next.value : CX_CONFLICT
  })
}

/** A new room's hold record, before anything can hold it. */
async function createHold(roomId: string, inc: string, record: HoldRecord): Promise<void> {
  const mutations = [{ key: HOLD_CELL_KEY, bytes: encodeRoomRecord(record) }]
  await mutateCells(roomId, inc, { keys: [HOLD_CELL_KEY] }, () => ({ value: undefined, mutations }))
  if (record.closesAt !== undefined) scheduleClose(roomId, inc, record.closesAt)
}

/** Persist a join's member record and identity marker; a non-hidden member holds the room. */
async function createMember(roomId: string, inc: string, id: string, record: RoomMemberRecord): Promise<void> {
  const mutations: CellMutation[] = [{ key: memberCellKey(id), bytes: encodeRoomRecord(record) }]
  if (record.identity !== undefined)
    mutations.push({ key: identityCellKey(record.identity, id), bytes: new Uint8Array() })
  const keys = [...mutations.map(({ key }) => key), HOLD_CELL_KEY]
  const hold = await mutateCells(roomId, inc, { keys }, (cells) => {
    const hold = record.hidden ? null : countHold(cells, 1, true)
    return { value: hold, mutations: hold === null ? mutations : [...mutations, hold.mutation] }
  })
  afterHoldChange(roomId, inc, hold)
}

/** A page's view holds the room; a room without a hold record keeps none. */
async function createView(roomId: string, inc: string, id: string): Promise<void> {
  const key = viewCellKey(id)
  const hold = await mutateCells(roomId, inc, { keys: [key, HOLD_CELL_KEY] }, (cells) => {
    const hold = cells.has(key) ? null : countHold(cells, 1)
    if (hold === null) return { value: null, mutations: [] }
    const view: ViewRecord = { seenAt: Date.now() }
    return { value: hold, mutations: [{ key, bytes: encodeRoomRecord(view) }, hold.mutation] }
  })
  afterHoldChange(roomId, inc, hold)
}

async function renewView(roomId: string, inc: string, id: string): Promise<void> {
  const key = viewCellKey(id)
  const view: ViewRecord = { seenAt: Date.now() }
  await mutateCells(roomId, inc, { keys: [key] }, (cells) => ({
    value: undefined,
    mutations: cells.has(key) ? [{ key, bytes: encodeRoomRecord(view) }] : [],
  }))
}

/** With `onlyIfLapsed`, a view renewed meanwhile stays. */
async function removeView(roomId: string, inc: string, id: string, opts?: { onlyIfLapsed: true }): Promise<void> {
  const key = viewCellKey(id)
  const hold = await mutateCells(roomId, inc, { keys: [key, HOLD_CELL_KEY] }, (cells) => {
    const raw = cells.get(key)
    if (raw === undefined || (opts?.onlyIfLapsed && !isLapsed(decodeRoomRecord<ViewRecord>(raw))))
      return { value: null, mutations: [] }
    const hold = countHold(cells, -1)
    return { value: hold, mutations: [{ key, bytes: null }, ...(hold === null ? [] : [hold.mutation])] }
  })
  afterHoldChange(roomId, inc, hold)
}

/** Read-modify-write one member record; a returned `next` is stored with a renewed lease. */
async function mutateMember<T>(
  roomId: string,
  inc: string,
  id: string,
  update: (record: RoomMemberRecord | null) => { value: T; next?: RoomMemberRecord },
): Promise<T> {
  const key = memberCellKey(id)
  return await mutateCells(roomId, inc, { keys: [key] }, (cells) => {
    const raw = cells.get(key)
    const { value, next } = update(raw === undefined ? null : decodeRoomRecord<RoomMemberRecord>(raw))
    if (next === undefined) return { value, mutations: [] }
    return { value, mutations: [{ key, bytes: encodeRoomRecord({ ...next, seenAt: Date.now() }) }] }
  })
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

async function renewMemberLease(roomId: string, inc: string, id: string): Promise<void> {
  await mutateMember(roomId, inc, id, (record) =>
    record === null ? { value: undefined } : { value: undefined, next: record },
  )
}

/** Remove a member and record the cleanup in one write, then finish the cleanup. With `onlyIfLapsed`, a member whose
 *  lease was renewed meanwhile stays, and its record is returned. */
async function evictMember(
  roomId: string,
  inc: string,
  id: string,
  identity: string | null,
  cause: LeaveCause,
  opts?: { onlyIfLapsed: true },
): Promise<RoomMemberRecord | null> {
  try {
    return await removeMemberCells(roomId, inc, id, identity, cause, opts)
  } catch (error) {
    // A room that closed under the eviction ended the member with everyone else.
    const head = await getRoomBackend().readHead(roomId)
    if (head?.state !== 'open' || head.currentInc !== inc) return null
    throw error
  }
}

async function removeMemberCells(
  roomId: string,
  inc: string,
  id: string,
  identity: string | null,
  cause: LeaveCause,
  opts?: { onlyIfLapsed: true },
): Promise<RoomMemberRecord | null> {
  const memberKey = memberCellKey(id)
  const cleanupKey = cleanupCellKey(id)
  const removedKeys = identity === null ? [memberKey] : [memberKey, identityCellKey(identity, id)]
  const outcome = await mutateCells<{ live: RoomMemberRecord } | { cleanup: boolean; hold: HoldChange | null }>(
    roomId,
    inc,
    { keys: [...removedKeys, cleanupKey, HOLD_CELL_KEY] },
    (cells) => {
      const pending = cells.has(cleanupKey)
      const raw = cells.get(memberKey)
      const record = raw === undefined ? null : decodeRoomRecord<RoomMemberRecord>(raw)
      if (record !== null && opts?.onlyIfLapsed && !isLapsed(record)) return { value: { live: record }, mutations: [] }
      if (record === null) return { value: { cleanup: pending, hold: null }, mutations: [] }
      const cleanup: PendingMemberCleanup = {
        cause: leaveCauseToWire(cause),
        ...(record.hidden ? { hidden: true } : {}),
      }
      const hold = record.hidden ? null : countHold(cells, -1)
      return {
        value: { cleanup: true, hold },
        mutations: [
          ...removedKeys.map((key) => ({ key, bytes: null })),
          ...(pending ? [] : [{ key: cleanupKey, bytes: encodeRoomRecord(cleanup) }]),
          ...(hold === null ? [] : [hold.mutation]),
        ],
      }
    },
  )
  if ('live' in outcome) return outcome.live
  afterHoldChange(roomId, inc, outcome.hold)
  if (outcome.cleanup) await finishPendingMemberCleanup(roomId, inc, id)
  return null
}

/** Live members, and departing ones whose eviction the read completes; lapsed members and views are reaped on the way. */
async function reapAndReadRoster(
  roomId: string,
  inc: string,
): Promise<{ members: MemberSnapshot[]; departing: Set<string> }> {
  const cells = await readCells(roomId, inc, { prefix: MEMBER_CELL_PREFIX })
  // After the member read, so an eviction committing in between shows up as departing.
  const cleanups = await readCells(roomId, inc, { prefix: CLEANUP_CELL_PREFIX })
  const departing = new Set<string>()
  for (const [key, raw] of cleanups) {
    const id = memberIdOfCleanupKey(key)
    departing.add(id)
    await completeCleanup(roomId, inc, id, raw)
  }
  for (const [key, raw] of await readCells(roomId, inc, { prefix: VIEW_CELL_PREFIX })) {
    if (isLapsed(decodeRoomRecord<ViewRecord>(raw)))
      await removeView(roomId, inc, viewIdOfCellKey(key), { onlyIfLapsed: true })
  }
  const members = await liveMembers(
    roomId,
    inc,
    [...cells].map(([key, raw]) => [memberIdOfCellKey(key), raw] as const).filter(([id]) => !departing.has(id)),
  )
  return { members, departing }
}

async function reapAndReadMembersById(roomId: string, inc: string, ids: string[]): Promise<MemberSnapshot[]> {
  const cells = await readCells(roomId, inc, { keys: ids.map(memberCellKey) })
  return await liveMembers(
    roomId,
    inc,
    ids.flatMap((id) => {
      const raw = cells.get(memberCellKey(id))
      return raw === undefined ? [] : [[id, raw] as const]
    }),
  )
}

async function liveMembers(
  roomId: string,
  inc: string,
  entries: ReadonlyArray<readonly [string, Uint8Array]>,
): Promise<MemberSnapshot[]> {
  const members: MemberSnapshot[] = []
  for (const [id, raw] of entries) {
    const candidate = decodeRoomRecord<RoomMemberRecord>(raw)
    const record = isLapsed(candidate)
      ? await evictMember(roomId, inc, id, candidate.identity ?? null, { type: 'disconnected' }, { onlyIfLapsed: true })
      : candidate
    if (record !== null) members.push(memberSnapshot(id, record))
  }
  return members
}

function memberSnapshot(id: string, record: RoomMemberRecord): MemberSnapshot {
  return {
    id,
    meta: record.meta,
    joinedAt: record.joinedAt,
    metaSeq: record.metaSeq,
    identity: record.identity ?? null,
    ...(record.tracks === undefined ? {} : { tracks: record.tracks }),
    ...(record.hidden ? { hidden: true } : {}),
  }
}

async function reapAndCountPresence(roomId: string, inc: string): Promise<number> {
  return (await reapAndReadRoster(roomId, inc)).members.filter((member) => !member.hidden).length
}

async function reapAndResolveIdentity(roomId: string, inc: string, identity: string): Promise<MemberSnapshot[]> {
  const prefix = identityCellPrefix(identity)
  const markers = await readCells(roomId, inc, { prefix })
  const ids = [...markers.keys()].map((key) => key.slice(prefix.length))
  const members = await reapAndReadMembersById(roomId, inc, ids)
  // A member and its identity marker are written and removed in one compare-exchange.
  assert(members.every((member) => member.identity === identity))
  return members
}

async function dropRetainedOwnedBy(roomId: string, inc: string, memberId: string): Promise<void> {
  const backend = getRoomBackend()
  for (const lane of await backend.listRetained(roomId, inc)) {
    if (lane.kind === 'binary' && lane.member === memberId) await backend.deleteRetained(roomId, inc, lane)
  }
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
  await mutateCells(roomId, inc, { keys: [key] }, (cells) => ({
    value: undefined,
    mutations: cells.has(key) ? [{ key, bytes: null }] : [],
  }))
}
