import {
  MAX_CLOSE_LEASE_MS,
  MIN_CLOSE_LEASE_MS,
  ROOM_SPI_VERSION,
  type LaneId,
  type RoomBackendFactory,
  type RoomBackendSpi,
} from 'telefunc/backend'

const _version: typeof ROOM_SPI_VERSION = ROOM_SPI_VERSION
const _bounds: readonly [typeof MIN_CLOSE_LEASE_MS, typeof MAX_CLOSE_LEASE_MS] = [
  MIN_CLOSE_LEASE_MS,
  MAX_CLOSE_LEASE_MS,
]
const _lane: LaneId = { kind: 'semantic' }
declare const _factory: RoomBackendFactory
declare const _backend: RoomBackendSpi

void [_version, _bounds, _lane, _factory, _backend]
