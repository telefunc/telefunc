export { createMember, evictMember, finishCleanup }
export type { PendingMemberCleanup }

import { getRoomPlane } from '../../backend/plane.js'
import type { CellMutation } from '../../backend/contract.js'
import { isOpenIncarnation } from '../../backend/semantics.js'
import { leaveCauseToWire, type RoomCtrlEnvelope, type WireLeaveCause } from '../../shared/protocol.js'
import type { RoomDataEnvelope, RoomMemberRecord, RoomIncarnation } from '../protocol.js'
import type { LeaveCause, ParticipantMeta } from '../../types.js'
import { dropMemberMeta, memberMetaMutations } from './meta.js'
import { deleteCells, dropCells, mutateCells } from './cellStore.js'
import { roomClosedError } from '../../shared/errors.js'
import {
  SEMANTIC_LANE,
  acceptedOrThrow,
  commitLane,
  controlLane,
  decodeRoomRecord,
  encodeRoomRecord,
} from '../lanes/lanes.js'
import { afterHoldChange, holdDelta, type HoldDelta } from '../lifecycle/hold.js'
import { cleanupCellKey, dmReplyCellPrefix, identityCellKey, leaseMemberCellKey, memberCellKey } from './cells.js'
import { mutateHosted, type RoomLease } from './lease.js'
import { ROOM_LEASE_TTL_MS } from '../constants.js'

// A member's cells: written by its join under a lease, removed by its eviction with a cleanup that finishes its leave.

/** `session`, the lease, hosted the member: its mark goes with the cleanup. One finisher at a time claims it, until
 *  `finishingUntil` (authority time): only the claimant commits the leave, and the next one takes over a claim that
 *  lapsed, as its claimant stopped. */
type PendingMemberCleanup = { cause: WireLeaveCause; session: string; hidden?: true; finishingUntil?: number }
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
    if (!isOpenIncarnation(await getRoomPlane().driver.readHead(room.id), room.inc)) return true
    throw error
  }
}

async function removeMemberCells(room: RoomIncarnation, id: string, cause: LeaveCause): Promise<boolean> {
  const memberKey = memberCellKey(id)
  const cleanupKey = cleanupCellKey(id)
  // The cleanup is the one pending, else the one this exchange writes, claimed by this eviction unless another
  // finisher's claim holds.
  type Outcome = { found: boolean; claimed: Uint8Array | null; hold: HoldDelta | null }
  const { value: outcome, counters } = await mutateCells<Outcome>(
    room.id,
    room.inc,
    [memberKey, cleanupKey],
    (cells, now) => {
      const pending = cells.get(cleanupKey)
      const raw = cells.get(memberKey)
      const record = raw === undefined ? null : decodeRoomRecord<RoomMemberRecord>(raw)
      if (record === null) {
        const claimed = pending === undefined ? null : claimCleanup(decodeRoomRecord(pending), now)
        const mutations = claimed === null ? [] : [{ key: cleanupKey, bytes: claimed }]
        return { value: { found: pending !== undefined, claimed, hold: null }, mutations }
      }
      const cleanup: PendingMemberCleanup = { cause: leaveCauseToWire(cause), session: record.session }
      if (record.hidden) cleanup.hidden = true
      const hold = holdDelta(record.hidden ? null : room.hold, -1)
      // The marker is written with the record, so the record's version covers it.
      const mutations: CellMutation[] = [{ key: memberKey, bytes: null }]
      if (record.identity !== undefined) mutations.push({ key: identityCellKey(record.identity, id), bytes: null })
      const claimed = claimCleanup(pending === undefined ? cleanup : decodeRoomRecord(pending), now)
      if (claimed !== null) mutations.push({ key: cleanupKey, bytes: claimed })
      return { value: { found: true, claimed, hold }, mutations, counts: hold.counts }
    },
  )
  if (outcome.hold !== null) afterHoldChange(room, outcome.hold.after(counters))
  if (outcome.claimed !== null) await completeCleanup(room.id, room.inc, id, outcome.claimed)
  return outcome.found
}

/** Finishes a cleanup its evicting server left, unless another finisher's claim on it holds. */
async function finishCleanup(room: RoomIncarnation, id: string): Promise<void> {
  const key = cleanupCellKey(id)
  const { value: claimed } = await mutateCells(room.id, room.inc, [key], (cells, now) => {
    const pending = cells.get(key)
    const claimed = pending === undefined ? null : claimCleanup(decodeRoomRecord(pending), now)
    return { value: claimed, mutations: claimed === null ? [] : [{ key, bytes: claimed }] }
  })
  if (claimed !== null) await completeCleanup(room.id, room.inc, id, claimed)
}

/** `cleanup` claimed for a new finisher, or null while another's claim holds. */
function claimCleanup(cleanup: PendingMemberCleanup, now: number): Uint8Array | null {
  if (cleanup.finishingUntil !== undefined && cleanup.finishingUntil >= now) return null
  return encodeRoomRecord({ ...cleanup, finishingUntil: now + ROOM_LEASE_TTL_MS })
}

async function dropRetainedOwnedBy(roomId: string, inc: string, memberId: string): Promise<void> {
  const plane = getRoomPlane()
  for (const lane of await plane.driver.listRetained(roomId, inc, { member: memberId })) {
    await plane.driver.deleteRetained(roomId, inc, lane)
  }
  const text = await plane.readRetained(roomId, inc, SEMANTIC_LANE)
  if (text !== null && decodeRoomRecord<RoomDataEnvelope>(text.payload).from === memberId) {
    await plane.driver.deleteRetained(roomId, inc, SEMANTIC_LANE, { ifSeq: text.seq })
  }
}

async function completeCleanup(roomId: string, inc: string, memberId: string, raw: Uint8Array): Promise<void> {
  const key = cleanupCellKey(memberId)
  const cleanup = decodeRoomRecord<PendingMemberCleanup>(raw)
  // The member's state goes before its leave is out, its four parts at once: none reads what another drops. Every
  // commit to the member's lanes requires its cell, which the eviction removed.
  await Promise.all([
    dropRetainedOwnedBy(roomId, inc, memberId),
    getRoomPlane().driver.dropLaneOrders(roomId, inc, memberId),
    dropMemberMeta(roomId, inc, memberId),
    dropCells(roomId, inc, dmReplyCellPrefix(memberId)),
  ])
  const leave: Extract<RoomCtrlEnvelope, { __r: 'leave' }> = { __r: 'leave', id: memberId, ...cleanup.cause }
  if (cleanup.hidden) leave.hidden = true
  acceptedOrThrow(roomId, await commitLane(controlLane(roomId, inc), encodeRoomRecord(leave)))
  // The mark goes last, so a lease's cleaner finds the member until its leave is out.
  const mark = leaseMemberCellKey(cleanup.session, memberId)
  if (!(await deleteCells(roomId, inc, [key, mark]))) throw roomClosedError(roomId)
}
