export { listRoom, repairRoomIndex, sweepLapsedRoomsSoon }

// Each listed room is due once its head may have lapsed: a sweep then drops what a lapsed incarnation left, as
// `Room.list()` does, so no lapsed room's data waits for a list call.

import { getRoomPlane } from '../../backend/plane.js'
import { assert } from '../../utils/assert.js'
import type { DirectoryEntry, RoomHead } from '../../backend/contract.js'
import type { RoomPlane } from '../../backend/RoomPlane.js'
import type { RoomStore } from '../../backend/RoomStore.js'
import { Slice } from '../../shared/Slice.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_LEASE_TTL_MS } from '../constants.js'
import type { RoomIncarnation } from '../protocol.js'
import { configFromHead, openConfig } from '../lanes/lanes.js'
import { takeOverClose } from '../lifecycle/close.js'
import { reportRoomError } from '../reportRoomError.js'
import { eachInFlight } from './inFlight.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'

/** Lists the room, to be looked at again once its head lapses, and a heartbeat from now at the earliest. A room
 *  without hold timeouts never lapses; one with them but no lapse yet gets one once held, so it is looked at again a
 *  lease from now. */
async function listRoom(
  store: RoomStore,
  { id, inc, hold }: RoomIncarnation,
  expiresAt: number | undefined,
): Promise<void> {
  if (hold === null) return await store.directoryPut(id, inc, Infinity, 0)
  if (expiresAt === undefined) return await store.directoryPut(id, inc, 0, ROOM_LEASE_TTL_MS)
  await store.directoryPut(id, inc, expiresAt, ROOM_HEARTBEAT_INTERVAL_MS)
}

/** A listed incarnation no head names as current has no owner left (its close finished, or was interrupted and its
 *  tombstone lapsed), so it is dropped with its listing, at the version read with it: a create whose head exchange
 *  came after `head` was read lists its room again, which that delete leaves. Its writes are steps of the call
 *  `store` is of, which a listing that needs no repair doesn't mark as having written. */
async function repairRoomIndex(store: RoomStore, listed: DirectoryEntry, head: RoomHead | null): Promise<void> {
  const { roomId: id, incTag, version } = listed
  if (head?.currentInc === incTag) return
  await store.dropIncarnation(id, incTag)
  const live = openConfig(head)
  if (live && head) await listRoom(store, { id, inc: live.inc, hold: live.hold }, head.expiresAt)
  else await store.directoryDelete(id, version)
}

const globalObject = getGlobalObject('server/store/directory.ts', {
  /** When this process may sweep again: once a heartbeat interval at most, as a room is created or a heartbeat
   *  ticks. */
  nextSweepAt: 0,
})

function sweepLapsedRoomsSoon(): void {
  const now = performance.now()
  if (now < globalObject.nextSweepAt) return
  globalObject.nextSweepAt = now + ROOM_HEARTBEAT_INTERVAL_MS
  void sweepLapsedRooms(getRoomPlane()).catch(reportRoomError)
}

/** Each due room leaves the due ones: due again under the head that still names it, or repaired. A page and a
 *  `ROOM_PLAN_SLICE_MS` slice at a time. */
async function sweepLapsedRooms(plane: RoomPlane): Promise<void> {
  const slice = new Slice()
  const { reads } = plane.direct
  for (let due = await reads.directoryDue(); due.length > 0; due = await reads.directoryDue()) {
    await eachInFlight(due, (entry) => sweepDueRoom(plane, entry))
    if (slice.over) await slice.next()
  }
}

/** A due room whose head no longer names it is repaired. A closing one is due at its close lease's end: once the lease
 *  ran out, its closer stopped, and the sweep takes the close over; a replica drops the second `closed` frame a
 *  takeover commits after a closer that committed its own. Any other is due again. */
async function sweepDueRoom(plane: RoomPlane, due: DirectoryEntry): Promise<void> {
  const { roomId, incTag } = due
  const head = await plane.direct.reads.readHead(roomId)
  if (head?.currentInc !== incTag) return await plane.call((store) => repairRoomIndex(store, due, head))
  if (head.state !== 'closing') {
    return await listRoom(plane.direct, { id: roomId, inc: incTag, hold: configFromHead(head).hold }, head.expiresAt)
  }
  const lease = head.closeLease
  assert(lease !== undefined) // a closing head carries its close lease
  if (lease.until < Date.now() && (await plane.call((store) => takeOverClose(store, roomId, head)))) return
  // Its lease holds, by this process's clock or the store's, or a closer took it over since: due at the lease's end.
  await plane.direct.directoryPut(roomId, incTag, lease.until, 0)
}
