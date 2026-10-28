export { listRoom, repairRoomIndex, sweepLapsedRoomsSoon }

// Each listed room is due once its head may have lapsed: a sweep then drops what a lapsed incarnation left, as
// `Room.list()` does, so no lapsed room's data waits for a list call.

import { getRoomBackend } from '../../backend/room/plane.js'
import type { RoomBackend, RoomHead } from '../../backend/room/contract.js'
import { Slice } from '../Slice.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_SESSION_TTL_MS } from './constants.js'
import type { RoomIncarnation } from './lifecycle.js'
import { configFromHead, openConfig } from './lanes.js'
import { reportRoomError } from './errors.js'

/** Lists the room, to be looked at again once its head lapses, and a heartbeat from now at the earliest. A room
 *  without hold timeouts never lapses; one with them but no lapse yet gets one once held, so it is looked at again a
 *  session's lease from now. */
async function listRoom(
  backend: RoomBackend,
  { id, inc, hold }: RoomIncarnation,
  expiresAt: number | undefined,
): Promise<void> {
  if (hold === null) return await backend.directoryPut(id, inc, Infinity, 0)
  if (expiresAt === undefined) return await backend.directoryPut(id, inc, 0, ROOM_SESSION_TTL_MS)
  await backend.directoryPut(id, inc, expiresAt, ROOM_HEARTBEAT_INTERVAL_MS)
}

/** A listed incarnation no head names as current has no owner left (its close finished, or was interrupted and its
 *  tombstone lapsed), so it is dropped with its listing. */
async function repairRoomIndex(
  backend: RoomBackend,
  id: string,
  listedInc: string,
  head: RoomHead | null,
): Promise<void> {
  if (head?.currentInc === listedInc) return
  await backend.dropGeneration(id, listedInc)
  const live = openConfig(head)
  if (live) await listRoom(backend, { id, inc: live.inc, hold: live.hold }, head!.expiresAt)
  else await backend.directoryDelete(id, listedInc)
}

/** When this process may sweep again: once a heartbeat interval at most, as a room is created or a heartbeat ticks. */
let nextSweepAt = 0

function sweepLapsedRoomsSoon(): void {
  const now = performance.now()
  if (now < nextSweepAt) return
  nextSweepAt = now + ROOM_HEARTBEAT_INTERVAL_MS
  void sweepLapsedRooms(getRoomBackend()).catch(reportRoomError)
}

/** Each due room leaves the due ones: due again under the head that still names it, or repaired. A page and a
 *  `ROOM_PLAN_SLICE_MS` slice at a time. */
async function sweepLapsedRooms(backend: RoomBackend): Promise<void> {
  const slice = new Slice()
  for (let due = await backend.directoryDue(); due.length > 0; due = await backend.directoryDue()) {
    for (const { roomId, incTag } of due) {
      const head = await backend.readHead(roomId)
      if (head?.currentInc !== incTag) await repairRoomIndex(backend, roomId, incTag, head)
      else await listRoom(backend, { id: roomId, inc: incTag, hold: configFromHead(head).hold }, head.expiresAt)
    }
    if (slice.over) await slice.next()
  }
}
