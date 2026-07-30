import {
  BACKEND_SPI_VERSION,
  laneKey,
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
const _lane: LaneId = { kind: 'semantic' }
const _laneKey = laneKey(_lane)
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
  _lane,
  _laneKey,
  _factory,
  _backend,
  _redisBackend,
  _redisTransport,
  _installRedisOptions,
  _installRedis,
  _noRuntimeHooks,
]
