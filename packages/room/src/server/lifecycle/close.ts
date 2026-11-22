export { closeIncarnation, takeOverClose, cleanupFinalizedIncarnation }

import { assert } from '../../utils/assert.js'
import type { RoomHead } from '../../backend/contract.js'
import type { RoomStore } from '../../backend/RoomStore.js'
import { ROOM_CLOSE_LEASE_MS, ROOM_TOMBSTONE_TTL_MS } from '../constants.js'
import { commitLane, configFromHead, controlLane, encodeRoomRecord } from '../lanes/lanes.js'

/** Closes the incarnation `inc`, or when not given the one its first read finds; returns the incarnation that ended
 *  closed, if any. Once that incarnation is gone, closed, lapsed or replaced, it writes nothing to the room under `id`. */
async function closeIncarnation(store: RoomStore, id: string, inc?: string): Promise<string | null> {
  let closing = inc
  for (;;) {
    const current = await store.reads.readHead(id)
    if (current === null) return null
    if (current.state === 'closed') {
      const closed = configFromHead(current).inc
      if (closing !== undefined && closed !== closing) return null
      await cleanupFinalizedIncarnation(store, id, current)
      return closed
    }
    assert(current.currentInc !== null) // an open or a closing head names its incarnation
    closing ??= current.currentInc
    if (current.currentInc !== closing) return null
    if (await takeOverClose(store, id, current)) return closing
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

/** Takes the close lease and finishes the close; false when the head changed meanwhile. */
async function takeOverClose(store: RoomStore, roomId: string, current: RoomHead): Promise<boolean> {
  const closing = await acquireClosingLease(store, roomId, current)
  return closing !== null && (await finishClose(store, roomId, closing))
}

async function acquireClosingLease(store: RoomStore, roomId: string, current: RoomHead): Promise<RoomHead | null> {
  assert(current.currentInc !== null) // only open and closing heads reach here; both name an incarnation
  const closeLease = { id: crypto.randomUUID(), durationMs: ROOM_CLOSE_LEASE_MS }
  const result = await store.compareExchangeHead(
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

async function finishClose(store: RoomStore, roomId: string, closing: RoomHead): Promise<boolean> {
  const inc = closing.currentInc
  const lease = closing.closeLease
  assert(inc !== null && lease !== undefined) // the closing head acquireClosingLease just wrote
  const closed = encodeRoomRecord({ __r: 'closed' })
  const closedEvent = await commitLane(store, controlLane(roomId, inc), closed, { closingLease: lease.id })
  if (closedEvent.kind !== 'accepted') return false
  const finalized = await store.compareExchangeHead(
    roomId,
    { form: 'finalize', rev: closing.rev, lease: lease.id },
    {
      head: { currentInc: null, state: 'closed', config: closing.config },
      ttlMs: ROOM_TOMBSTONE_TTL_MS,
    },
  )
  if (finalized.kind === 'conflict') return false
  await cleanupFinalizedIncarnation(store, roomId, finalized.head)
  return true
}

async function cleanupFinalizedIncarnation(store: RoomStore, roomId: string, closed: RoomHead): Promise<void> {
  // A closed tombstone's incarnation, which a random `inc` never makes current again.
  assert(closed.state === 'closed' && closed.currentInc === null, 'Dropping the current incarnation')
  const inc = configFromHead(closed).inc
  await store.dropIncarnation(roomId, inc)
  // The listing goes last: while it stays, `Room.list()`, a later create of the room and the directory sweep (for a
  // room with hold timeouts, the only kind it visits) drop the incarnation again if this drop failed.
  await store.directoryDelete(roomId, inc)
}
