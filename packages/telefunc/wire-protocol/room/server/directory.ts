export { directoryCheckAt, repairRoomIndex, sweepLapsedRoomsSoon }

// Each listed room is due once its head may have lapsed: a sweep then drops what a lapsed incarnation left, as
// `Room.list()` does, so no lapsed room's data waits for a list call.

import { getRoomBackend } from '../../backend/install.js'
import type { RoomBackend, RoomHead } from '../../backend/room/contract.js'
import { nextTurnOf } from '../../flow-control/macrotask-yield.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_MEMBER_TTL_MS, ROOM_PLAN_SLICE_MS } from '../constants.js'
import type { HoldConfig } from '../protocol.js'
import { configFromHead, openConfig } from './lanes.js'
import { reportRoomError } from './errors.js'

/** When to look at a listed room again: once its head lapses, and a heartbeat from now at the earliest. A room
 *  without hold timeouts never lapses; one with them but no lapse yet gets one once held, so it is looked at again a
 *  member's lease from now. */
function directoryCheckAt(hold: HoldConfig | null, expiresAt: number | undefined): number {
  if (hold === null) return Infinity
  if (expiresAt === undefined) return Date.now() + ROOM_MEMBER_TTL_MS
  return Math.max(expiresAt, Date.now() + ROOM_HEARTBEAT_INTERVAL_MS)
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
  if (live) await backend.directoryPut(id, live.inc, directoryCheckAt(live.hold, head!.expiresAt))
  else await backend.directoryDelete(id, listedInc)
}

/** When this process may sweep again: once a heartbeat interval at most, as a room is created or a heartbeat ticks. */
let nextSweepAt = 0

function sweepLapsedRoomsSoon(): void {
  const now = Date.now()
  if (now < nextSweepAt) return
  nextSweepAt = now + ROOM_HEARTBEAT_INTERVAL_MS
  void sweepLapsedRooms(getRoomBackend()).catch(reportRoomError)
}

/** Each due room leaves the due ones: due again under the head that still names it, or repaired. A page and a
 *  `ROOM_PLAN_SLICE_MS` slice at a time. */
async function sweepLapsedRooms(backend: RoomBackend): Promise<void> {
  let deadline = performance.now() + ROOM_PLAN_SLICE_MS
  for (let due = await backend.directoryDue(Date.now()); due.length > 0; due = await backend.directoryDue(Date.now())) {
    for (const { roomId, incTag } of due) {
      const head = await backend.readHead(roomId)
      if (head?.currentInc !== incTag) await repairRoomIndex(backend, roomId, incTag, head)
      else await backend.directoryPut(roomId, incTag, directoryCheckAt(configFromHead(head).hold, head.expiresAt))
    }
    if (performance.now() < deadline) continue
    await nextTurnOf()
    deadline = performance.now() + ROOM_PLAN_SLICE_MS
  }
}
