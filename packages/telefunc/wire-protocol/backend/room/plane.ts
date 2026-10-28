export { getRoomBackend, ROOM_PLANE }

import { backendPlane } from '../../../node/server/__internal.js'
import type { RoomBackend, RoomDriver } from './contract.js'
import { MemoryRoomDriver } from './memory.js'
import { superviseRoomDriver } from './supervise.js'

const ROOM_PLANE = Symbol.for('telefunc.room.plane')

const supervised = new WeakMap<RoomDriver, RoomBackend>()

/** The Room plane of the installed backend, the in-memory one when the process installed none. */
function getRoomBackend(): RoomBackend {
  const driver = backendPlane<RoomDriver>(ROOM_PLANE, () => new MemoryRoomDriver())
  let backend = supervised.get(driver)
  if (backend === undefined) supervised.set(driver, (backend = superviseRoomDriver(driver)))
  return backend
}
