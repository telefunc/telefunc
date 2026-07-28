import {
  BACKEND_SPI_VERSION,
  MAX_CLOSE_LEASE_MS,
  MIN_CLOSE_LEASE_MS,
  type BackendDriver,
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
const _version: typeof BACKEND_SPI_VERSION = BACKEND_SPI_VERSION
const _bounds: readonly [typeof MIN_CLOSE_LEASE_MS, typeof MAX_CLOSE_LEASE_MS] = [
  MIN_CLOSE_LEASE_MS,
  MAX_CLOSE_LEASE_MS,
]
const _lane: LaneId = { kind: 'semantic' }
declare const _factory: BackendFactory
declare const _backend: BackendSpi
declare const _redisOptions: RedisRoomBackendOptions
const _redisBackend: BackendDriver = new RedisRoomBackend(_redisOptions)
declare const _redisBroadcastOptions: RedisBroadcastOptions
const _redisTransport: BackendDriver = new RedisTransport(_redisBroadcastOptions)
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
