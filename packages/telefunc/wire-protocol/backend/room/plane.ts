export { getRoomBackend, ROOM_PLANE }

import { backendPlane } from '../../../node/server/__internal.js'
import type { RoomBackend, RoomDriver } from './contract.js'
import { MemoryRoomDriver } from './memory.js'
import { superviseRoomDriver } from './supervise.js'

const ROOM_PLANE = Symbol.for('telefunc.room.plane')

/** The Room plane of the installed backend, the in-memory one when the process installed none. */
function getRoomBackend(): RoomBackend {
  return backendPlane<RoomDriver, RoomBackend>(ROOM_PLANE, {
    fallback: () => new MemoryRoomDriver(),
    supervise: superviseRoomDriver,
  })
}
