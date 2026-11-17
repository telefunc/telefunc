export { JOIN, LEAVE, joinMember, evictMember }

import { getRoomPlane } from '../../backend/plane.js'
import type { RoomOperation } from '../../backend/contract.js'
import { isOpenIncarnation } from '../../backend/semantics.js'
import { leaveCauseToWire, type RoomCtrlEnvelope, type WireLeaveCause } from '../../shared/protocol.js'
import type { HoldConfig, RoomMemberRecord, RoomIncarnation } from '../protocol.js'
import type { LeaveCause, ParticipantMeta } from '../../types.js'
import { memberMetaMutations } from './meta.js'
import { runOperation } from './operations.js'
import { decodeRoomRecord, encodeRoomRecord } from '../lanes/lanes.js'
import { afterHoldChange, holdDelta } from '../lifecycle/hold.js'
import { ROOM_DEPARTING_RECORD_MS } from '../constants.js'
import {
  departingCellKey,
  dmReplyCellPrefix,
  identityCellKey,
  leaseCellKey,
  leaseMemberCellKey,
  memberCellKey,
  memberMetaCellPrefix,
} from './cells.js'
import { leaseHosting, runHosted, type LeaseState, type RoomLease } from './lease.js'

// A member's cells: written with its join under a lease, removed with its leave, each one operation.

type JoinArgs = {
  lease: LeaseState
  hold: HoldConfig | null
  id: string
  record: Omit<RoomMemberRecord, 'session'>
  meta: ParticipantMeta
  join: Extract<RoomCtrlEnvelope, { __r: 'join' }>
}

/** A join: the member's record under the lease, its mark, its meta's keys and identity marker, and its join event; a
 *  non-hidden member holds the room. `'lease-lost'` once the lease's cell was claimed or removed. */
const JOIN: RoomOperation<JoinArgs, 'joined' | 'lease-lost'> = {
  name: 'join',
  plan(tx, { lease, hold, id, record, meta, join }) {
    const leaseWrites = leaseHosting(lease, tx.cell(leaseCellKey(lease.id)), tx.now)
    if (leaseWrites === null) return 'lease-lost'
    for (const mutation of leaseWrites) tx.write(mutation)
    tx.write({
      key: memberCellKey(id),
      bytes: encodeRoomRecord({ ...record, session: lease.id } satisfies RoomMemberRecord),
    })
    tx.write({ key: leaseMemberCellKey(lease.id, id), bytes: identityTextEncoder.encode(record.identity ?? '') })
    for (const mutation of memberMetaMutations(id, meta)) tx.write(mutation)
    if (record.identity !== undefined) tx.write({ key: identityCellKey(record.identity, id), bytes: new Uint8Array() })
    for (const count of holdDelta(record.hidden ? null : hold, 1, { joining: true }).counts) tx.count(count)
    tx.commit(encodeRoomRecord(join))
    return 'joined'
  },
}

type LeaveArgs = { id: string; cause: WireLeaveCause }

/** A leave: the member's record, identity marker, lease mark, meta, DM replies, lanes and the frames it retained go,
 *  with its leave event, and its departing record is written. Null once the member was gone; else whether it was
 *  hidden, as a hidden one held nothing. */
const LEAVE: RoomOperation<LeaveArgs & { hold: HoldConfig | null }, { hidden: boolean } | null> = {
  name: 'leave',
  plan(tx, { id, cause, hold }) {
    const memberKey = memberCellKey(id)
    const raw = tx.cell(memberKey)
    if (raw === undefined) return null
    const record = decodeRoomRecord<RoomMemberRecord>(raw)
    tx.write({ key: memberKey, bytes: null })
    tx.writeExpiring({ key: departingCellKey(id), bytes: encodeRoomRecord(cause), ttlMs: ROOM_DEPARTING_RECORD_MS })
    tx.write({ key: leaseMemberCellKey(record.session, id), bytes: null })
    if (record.identity !== undefined) tx.write({ key: identityCellKey(record.identity, id), bytes: null })
    for (const count of holdDelta(record.hidden ? null : hold, -1).counts) tx.count(count)
    tx.dropPrefix(memberMetaCellPrefix(id))
    tx.dropPrefix(dmReplyCellPrefix(id))
    tx.dropMemberLanes(id)
    const leave: Extract<RoomCtrlEnvelope, { __r: 'leave' }> = { __r: 'leave', id, ...cause }
    if (record.hidden) leave.hidden = true
    tx.commit(encodeRoomRecord(leave))
    return { hidden: record.hidden === true }
  },
}

const identityTextEncoder = new TextEncoder()

/** Joins a member under the lease. */
async function joinMember(
  room: RoomIncarnation,
  lease: RoomLease,
  member: Omit<JoinArgs, 'lease' | 'hold'>,
): Promise<void> {
  const joined = await runHosted(room, lease, JOIN, { ...member, hold: room.hold })
  afterHoldChange(room, holdDelta(member.record.hidden ? null : room.hold, 1, { joining: true }).after(joined.counters))
}

/** Removes a member with its leave; false once it was gone. */
async function evictMember(room: RoomIncarnation, id: string, cause: LeaveCause): Promise<boolean> {
  try {
    const left = await runOperation(room, LEAVE, { id, cause: leaveCauseToWire(cause), hold: room.hold })
    if (left.value === null) return false
    afterHoldChange(room, holdDelta(left.value.hidden ? null : room.hold, -1).after(left.counters))
    return true
  } catch (error) {
    // A room that closed under the eviction ended the member with everyone else.
    if (!isOpenIncarnation(await getRoomPlane().driver.readHead(room.id), room.inc)) return true
    throw error
  }
}
