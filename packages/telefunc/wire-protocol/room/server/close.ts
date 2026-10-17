export { closeIncarnation, takeOverClose, cleanupFinalizedIncarnation }

import { assert } from '../../../utils/assert.js'
import { getRoomBackend } from '../../backend/install.js'
import type { RoomBackend, RoomHead } from '../../backend/room/contract.js'
import { ROOM_CLOSE_LEASE_MS, ROOM_TOMBSTONE_TTL_MS } from './constants.js'
import { commitControl, configFromHead } from './lanes.js'

/** Closes the room, or only its incarnation `inc` when given. */
async function closeIncarnation(id: string, inc?: string): Promise<void> {
  const backend = getRoomBackend()
  for (;;) {
    const current = await backend.readHead(id)
    if (current === null) return
    if (current.state === 'closed') {
      await cleanupFinalizedIncarnation(backend, id, current)
      return
    }
    if (inc !== undefined && current.currentInc !== inc) return
    if (await takeOverClose(backend, id, current)) return
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

/** Takes the close lease and finishes the close; false when the head changed meanwhile. */
async function takeOverClose(backend: RoomBackend, roomId: string, current: RoomHead): Promise<boolean> {
  const closing = await acquireClosingLease(backend, roomId, current)
  return closing !== null && (await finishClose(backend, roomId, closing))
}

async function acquireClosingLease(backend: RoomBackend, roomId: string, current: RoomHead): Promise<RoomHead | null> {
  assert(current.currentInc !== null) // only open and closing heads reach here; both name an incarnation
  const closeLease = { id: crypto.randomUUID(), durationMs: ROOM_CLOSE_LEASE_MS }
  const result = await backend.compareExchangeHead(
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
  if ('conflict' in result) return null
  assert('head' in result)
  return result.head
}

async function finishClose(backend: RoomBackend, roomId: string, closing: RoomHead): Promise<boolean> {
  const inc = closing.currentInc
  const lease = closing.closeLease
  assert(inc !== null && lease !== undefined) // the closing head acquireClosingLease just wrote
  const closedEvent = await commitControl(roomId, inc, { __r: 'closed' }, { closingLease: lease.id })
  if ('stale' in closedEvent) return false
  const finalized = await backend.compareExchangeHead(
    roomId,
    { form: 'finalize', rev: closing.rev, lease: lease.id },
    {
      head: { currentInc: null, state: 'closed', config: closing.config },
      ttlMs: ROOM_TOMBSTONE_TTL_MS,
    },
  )
  if ('conflict' in finalized) return false
  assert('head' in finalized)
  await cleanupFinalizedIncarnation(backend, roomId, finalized.head)
  return true
}

async function cleanupFinalizedIncarnation(backend: RoomBackend, roomId: string, closed: RoomHead): Promise<void> {
  // A closed tombstone's incarnation, which a random `inc` never makes current again.
  assert(closed.state === 'closed' && closed.currentInc === null, 'Dropping the current incarnation')
  const inc = configFromHead(closed).inc
  await backend.dropGeneration(roomId, inc)
  await backend.directoryDelete(roomId, inc)
}
