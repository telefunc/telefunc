export { getRoomPlane, ROOM_PLANE }

import { backendPlane, type PlaneDefinition } from 'telefunc/__internal'
import type { RoomDriver, RoomPlaneKit } from './contract.js'
import { CELL_PAGE_SIZE, DIRECTORY_PAGE_SIZE } from './contract.js'
import { decodeLaneKey, encodeLaneKey, everyMemberLaneKey, everyTrackLaneKey } from './laneKey.js'
import { MemoryRoomDriver } from './MemoryRoomDriver.js'
import { OPERATION_COMMIT_LANE, recordOperation } from './operation.js'
import { RoomPlane } from './RoomPlane.js'

const ROOM_PLANE = Symbol.for('telefunc.room.plane')

/** What a backend's Room driver, built outside this package, keys and pages Room's data and records its operations
 *  by. */
const kit: RoomPlaneKit = {
  encodeLaneKey,
  decodeLaneKey,
  everyMemberLaneKey,
  everyTrackLaneKey,
  CELL_PAGE_SIZE,
  DIRECTORY_PAGE_SIZE,
  recordOperation,
  OPERATION_COMMIT_LANE,
}

const roomPlane: PlaneDefinition<RoomDriver, RoomPlane> = {
  key: ROOM_PLANE,
  kit,
  memoryDriver: () => new MemoryRoomDriver(),
  create: (driver) => new RoomPlane({ driver }),
}

/** The Room plane of the installed backend, the in-memory one when the process installed none. */
function getRoomPlane(): RoomPlane {
  return backendPlane(roomPlane)
}
