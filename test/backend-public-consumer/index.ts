import {
  BACKEND_SPI_VERSION,
  MAX_CLOSE_LEASE_MS,
  MIN_CLOSE_LEASE_MS,
  type BackendFactory,
  type BackendSpi,
  type LaneId,
} from 'telefunc/backend'
import {
  installRedis,
  type InstallRedisOptions,
  type RedisBroadcastOptions,
  RedisRoomBackend,
  type RedisRoomBackendOptions,
  RedisTransport,
} from '@telefunc/redis'
// @ts-expect-error fixtures are test-only and no Redis deep modules are package exports
import type { RedisBackendFixture } from '@telefunc/redis/room/fixture'

const _version: typeof BACKEND_SPI_VERSION = BACKEND_SPI_VERSION
const _bounds: readonly [typeof MIN_CLOSE_LEASE_MS, typeof MAX_CLOSE_LEASE_MS] = [
  MIN_CLOSE_LEASE_MS,
  MAX_CLOSE_LEASE_MS,
]
const _lane: LaneId = { kind: 'semantic' }
declare const _factory: BackendFactory
declare const _backend: BackendSpi
declare const _redisOptions: RedisRoomBackendOptions
const _redisBackend: BackendSpi = new RedisRoomBackend(_redisOptions)
declare const _redisBroadcastOptions: RedisBroadcastOptions
const _redisTransport = new RedisTransport(_redisBroadcastOptions)
const _installRedisOptions: InstallRedisOptions = { prefix: 'consumer:' }
const _installRedis: (redis: RedisBroadcastOptions['redis'], options?: InstallRedisOptions) => void = installRedis
// @ts-expect-error the released constructor has no test-hook/runtime dependency argument
const _noRuntimeHooks = new RedisRoomBackend(_redisOptions, { authorityNow: () => 0 })

void [
  _version,
  _bounds,
  _lane,
  _factory,
  _backend,
  _redisBackend,
  _redisTransport,
  _installRedisOptions,
  _installRedis,
  _noRuntimeHooks,
]
void (null as unknown as RedisBackendFixture)
