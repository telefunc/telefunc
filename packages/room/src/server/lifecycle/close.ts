export { closeIncarnation, takeOverClose, cleanupFinalizedIncarnation }

import { assert } from '../../utils/assert.js'
import { getRoomPlane } from '../../backend/plane.js'
import type { RoomHead } from '../../backend/contract.js'
import type { RoomPlane } from '../../backend/RoomPlane.js'
import { ROOM_CLOSE_LEASE_MS, ROOM_TOMBSTONE_TTL_MS } from '../constants.js'
import { commitLane, configFromHead, controlLane, encodeRoomRecord } from '../lanes/lanes.js'

/** Closes the room, or only its incarnation `inc` when given; returns the incarnation that ended closed, if any. */
async function closeIncarnation(id: string, inc?: string): Promise<string | null> {
  const plane = getRoomPlane()
  for (;;) {
    const current = await plane.driver.readHead(id)
    if (current === null) return null
    if (current.state === 'closed') {
      await cleanupFinalizedIncarnation(plane, id, current)
      return configFromHead(current).inc
    }
    if (inc !== undefined && current.currentInc !== inc) return null
    if (await takeOverClose(plane, id, current)) return current.currentInc
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

/** Takes the close lease and finishes the close; false when the head changed meanwhile. */
async function takeOverClose(plane: RoomPlane, roomId: string, current: RoomHead): Promise<boolean> {
  const closing = await acquireClosingLease(plane, roomId, current)
  return closing !== null && (await finishClose(plane, roomId, closing))
}

async function acquireClosingLease(plane: RoomPlane, roomId: string, current: RoomHead): Promise<RoomHead | null> {
  assert(current.currentInc !== null) // only open and closing heads reach here; both name an incarnation
  const closeLease = { id: crypto.randomUUID(), durationMs: ROOM_CLOSE_LEASE_MS }
  const result = await plane.compareExchangeHead(
    roomId,
    { form: current.state === 'open' ? 'rev' : 'takeover', rev: current.rev },
    {
      head: {
        currentInc: current.currentInc,
        state: 'closing',
        config: current.config,
        closeLease,
      },
    },
  )
  if (result.kind === 'conflict') return null
  return result.head
}

async function finishClose(plane: RoomPlane, roomId: string, closing: RoomHead): Promise<boolean> {
  const inc = closing.currentInc
  const lease = closing.closeLease
  assert(inc !== null && lease !== undefined) // the closing head acquireClosingLease just wrote
  const closed = encodeRoomRecord({ __r: 'closed' })
  const closedEvent = await commitLane(controlLane(roomId, inc), closed, { closingLease: lease.id })
  if (closedEvent.kind !== 'accepted') return false
  const finalized = await plane.compareExchangeHead(
    roomId,
    { form: 'finalize', rev: closing.rev, lease: lease.id },
    {
      head: { currentInc: null, state: 'closed', config: closing.config },
      ttlMs: ROOM_TOMBSTONE_TTL_MS,
    },
  )
  if (finalized.kind === 'conflict') return false
  await cleanupFinalizedIncarnation(plane, roomId, finalized.head)
  return true
}

async function cleanupFinalizedIncarnation(plane: RoomPlane, roomId: string, closed: RoomHead): Promise<void> {
  // A closed tombstone's incarnation, which a random `inc` never makes current again.
  assert(closed.state === 'closed' && closed.currentInc === null, 'Dropping the current incarnation')
  const inc = configFromHead(closed).inc
  await plane.driver.dropIncarnation(roomId, inc)
  await plane.driver.directoryDelete(roomId, inc)
}
