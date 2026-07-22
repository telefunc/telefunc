import {
  MAX_CLOSE_LEASE_MS,
  MIN_CLOSE_LEASE_MS,
  ROOM_SPI_VERSION,
  type LaneId,
  type RoomBackendFactory,
  type RoomBackendSpi,
} from 'telefunc/backend'
import { RedisRoomBackend, type RedisRoomBackendOptions } from '@telefunc/redis'
// @ts-expect-error fixtures are test-only and no Redis deep modules are package exports
import type { RedisBackendFixture } from '@telefunc/redis/room/fixture'

const _version: typeof ROOM_SPI_VERSION = ROOM_SPI_VERSION
const _bounds: readonly [typeof MIN_CLOSE_LEASE_MS, typeof MAX_CLOSE_LEASE_MS] = [
  MIN_CLOSE_LEASE_MS,
  MAX_CLOSE_LEASE_MS,
]
const _lane: LaneId = { kind: 'semantic' }
declare const _factory: RoomBackendFactory
declare const _backend: RoomBackendSpi
declare const _redisOptions: RedisRoomBackendOptions
const _redisBackend: RoomBackendSpi = new RedisRoomBackend(_redisOptions)
// @ts-expect-error the released constructor has no test-hook/runtime dependency argument
const _noRuntimeHooks = new RedisRoomBackend(_redisOptions, { authorityNow: () => 0 })

void [_version, _bounds, _lane, _factory, _backend, _redisBackend, _noRuntimeHooks]
void (null as unknown as RedisBackendFixture)
