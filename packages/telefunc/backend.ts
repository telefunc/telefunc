// Public Room backend boundary. The SPI itself lives in one source file so published declarations
// cannot drift from the implementations and conformance harnesses that use it.
export * from './wire-protocol/backend/spi.js'
export {
  disposeRoomBackend,
  getRoomBackend,
  installRoomBackend,
  type RoomBackendFactory,
} from './wire-protocol/backend/install.js'
