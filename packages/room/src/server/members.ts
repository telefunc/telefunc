export { createMember, evictMember, completeCleanup }
export type { PendingMemberCleanup }

import { getRoomBackend } from '../backend/plane.js'
import type { CellMutation } from '../backend/contract.js'
import { isOpenIncarnation } from '../backend/semantics.js'
import { leaveCauseToWire, type RoomCtrlEnvelope, type WireLeaveCause } from '../shared/protocol.js'
import type { RoomDataEnvelope, RoomMemberRecord } from './protocol.js'
import type { LeaveCause, ParticipantMeta } from '../types.js'
import { dropMemberMeta, memberMetaMutations } from './meta.js'
import { dropCells, mutateCells, readOpenCells } from './cellStore.js'
import { SEMANTIC_LANE, acceptedOrThrow, commitLane, controlLane, decodeRoomRecord, encodeRoomRecord } from './lanes.js'
import { afterHoldChange, holdDelta, type HoldDelta, type RoomIncarnation } from './lifecycle.js'
import { cleanupCellKey, dmReplyCellPrefix, identityCellKey, leaseMemberCellKey, memberCellKey } from './cells.js'
import type { RoomLease } from './lease.js'
import { mutateHosted } from './leases.js'

// A member's cells: written by its join under a lease, removed by its eviction with a cleanup that finishes its leave.

/** `session`, the lease, hosted the member: its mark goes with the cleanup. */
type PendingMemberCleanup = { cause: WireLeaveCause; session: string; hidden?: true }
const identityTextEncoder = new TextEncoder()

/** Persist a join's member record under the lease, its mark, its meta's keys and identity marker; a non-hidden
 *  member holds the room. */
async function createMember(
  room: RoomIncarnation,
  lease: RoomLease,
  id: string,
  record: Omit<RoomMemberRecord, 'session'>,
  meta: ParticipantMeta,
): Promise<void> {
  const hold = holdDelta(record.hidden ? null : room.hold, 1, { joining: true })
  const { counters } = await mutateHosted(room, lease, (hostId) => {
    const mutations: CellMutation[] = [
      { key: memberCellKey(id), bytes: encodeRoomRecord({ ...record, session: hostId } satisfies RoomMemberRecord) },
      { key: leaseMemberCellKey(hostId, id), bytes: identityTextEncoder.encode(record.identity ?? '') },
      ...memberMetaMutations(id, meta),
    ]
    if (record.identity !== undefined) {
      mutations.push({ key: identityCellKey(record.identity, id), bytes: new Uint8Array() })
    }
    const plan = () => ({ value: undefined, mutations, counts: hold.counts })
    return { keys: mutations.map(({ key }) => key), plan }
  })
  afterHoldChange(room, hold.after(counters))
}

/** Remove a member and record the cleanup in one write, then finish the cleanup; false once neither the member nor
 *  its cleanup was there. */
async function evictMember(room: RoomIncarnation, id: string, cause: LeaveCause): Promise<boolean> {
  try {
    return await removeMemberCells(room, id, cause)
  } catch (error) {
    // A room that closed under the eviction ended the member with everyone else.
    if (!isOpenIncarnation(await getRoomBackend().readHead(room.id), room.inc)) return true
    throw error
  }
}

async function removeMemberCells(room: RoomIncarnation, id: string, cause: LeaveCause): Promise<boolean> {
  const memberKey = memberCellKey(id)
  const cleanupKey = cleanupCellKey(id)
  const { value: outcome, counters } = await mutateCells<{ cleanup: boolean; hold: HoldDelta | null }>(
    room.id,
    room.inc,
    [memberKey, cleanupKey],
    (cells) => {
      const pending = cells.has(cleanupKey)
      const raw = cells.get(memberKey)
      const record = raw === undefined ? null : decodeRoomRecord<RoomMemberRecord>(raw)
      if (record === null) return { value: { cleanup: pending, hold: null }, mutations: [] }
      const cleanup: PendingMemberCleanup = { cause: leaveCauseToWire(cause), session: record.session }
      if (record.hidden) cleanup.hidden = true
      const hold = holdDelta(record.hidden ? null : room.hold, -1)
      // The marker is written with the record, so the record's version covers it.
      const mutations: CellMutation[] = [{ key: memberKey, bytes: null }]
      if (record.identity !== undefined) mutations.push({ key: identityCellKey(record.identity, id), bytes: null })
      if (!pending) mutations.push({ key: cleanupKey, bytes: encodeRoomRecord(cleanup) })
      return { value: { cleanup: true, hold }, mutations, counts: hold.counts }
    },
  )
  if (outcome.hold !== null) afterHoldChange(room, outcome.hold.after(counters))
  if (outcome.cleanup) await finishPendingMemberCleanup(room.id, room.inc, id)
  return outcome.cleanup
}

async function dropRetainedOwnedBy(roomId: string, inc: string, memberId: string): Promise<void> {
  const backend = getRoomBackend()
  for (const lane of await backend.listRetained(roomId, inc, { member: memberId })) {
    await backend.deleteRetained(roomId, inc, lane)
  }
  const text = await backend.readRetained(roomId, inc, SEMANTIC_LANE)
  if (text !== null && decodeRoomRecord<RoomDataEnvelope>(text.payload).from === memberId) {
    await backend.deleteRetained(roomId, inc, SEMANTIC_LANE, { ifSeq: text.seq })
  }
}

async function finishPendingMemberCleanup(roomId: string, inc: string, memberId: string): Promise<void> {
  const key = cleanupCellKey(memberId)
  const raw = (await readOpenCells(roomId, inc, [key])).cells.get(key)
  if (raw !== undefined) await completeCleanup(roomId, inc, memberId, raw)
}

async function completeCleanup(roomId: string, inc: string, memberId: string, raw: Uint8Array): Promise<void> {
  const key = cleanupCellKey(memberId)
  const cleanup = decodeRoomRecord<PendingMemberCleanup>(raw)
  await dropRetainedOwnedBy(roomId, inc, memberId)
  // Every commit to the member's lanes requires its cell, which the eviction removed.
  await getRoomBackend().dropLaneOrders(roomId, inc, memberId)
  await dropMemberMeta(roomId, inc, memberId)
  await dropCells(roomId, inc, dmReplyCellPrefix(memberId))
  const leave: Extract<RoomCtrlEnvelope, { __r: 'leave' }> = { __r: 'leave', id: memberId, ...cleanup.cause }
  if (cleanup.hidden) leave.hidden = true
  acceptedOrThrow(roomId, await commitLane(controlLane(roomId, inc), encodeRoomRecord(leave)))
  // The mark goes last, so a lease's cleaner finds the member until its leave is out.
  const mark = leaseMemberCellKey(cleanup.session, memberId)
  await mutateCells(roomId, inc, [key, mark], (cells) => ({
    value: undefined,
    mutations: [key, mark].filter((at) => cells.has(at)).map((at) => ({ key: at, bytes: null })),
  }))
}
