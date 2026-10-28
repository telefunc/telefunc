export { getRoomBackend, ROOM_PLANE }

import { backendPlane } from 'telefunc/__internal'
import type { RoomBackend, RoomDriver, RoomPlaneKit } from './contract.js'
import { CELL_PAGE_SIZE, DIRECTORY_PAGE_SIZE } from './contract.js'
import { decodeLaneKey, encodeLaneKey, everyMemberLaneKey, everyTrackLaneKey } from './laneKey.js'
import { MemoryRoomDriver } from './memory.js'
import { superviseRoomDriver } from './supervise.js'

const ROOM_PLANE = Symbol.for('telefunc.room.plane')

/** What a backend's Room driver, built outside this package, keys and pages Room's data by. */
const kit: RoomPlaneKit = {
  encodeLaneKey,
  decodeLaneKey,
  everyMemberLaneKey,
  everyTrackLaneKey,
  CELL_PAGE_SIZE,
  DIRECTORY_PAGE_SIZE,
}

/** The Room plane of the installed backend, the in-memory one when the process installed none. */
function getRoomBackend(): RoomBackend {
  return backendPlane<RoomDriver, RoomBackend>(ROOM_PLANE, {
    fallback: () => new MemoryRoomDriver(),
    supervise: superviseRoomDriver,
    kit,
  })
}
