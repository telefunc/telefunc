// For backend packages that store rooms elsewhere (`@telefunc/redis`), never for apps: Room's storage driver contract,
// which their Room driver implements, and the plane it is installed on.
export type * from './contract.js'
export { getRoomPlane, ROOM_PLANE } from './plane.js'
export type { RoomPlane } from './RoomPlane.js'
export { MemoryRoomDriver } from './MemoryRoomDriver.js'
export type { OperationReads, OperationWrites } from './operation.js'
