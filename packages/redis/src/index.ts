export { installRedis, RedisRoomBackend }
export type { InstallRedisOptions }
export type { RedisRoomBackendOptions } from './room/backend.js'

import type { Cluster, Redis } from 'ioredis'
import { setDefaultBackend } from 'telefunc/__internal'
import { RedisRoomBackend } from './room/backend.js'

/** Installs Redis as Telefunc's complete backend: generic Broadcast plus durable Room state. */
function installRedis(redis: Redis | Cluster, options: InstallRedisOptions = {}) {
  return setDefaultBackend(
    () => new RedisRoomBackend({ redis, prefix: options.prefix }),
    redisBackendDefaultIdentity(redis, options.prefix),
  )
}

const REDIS_BACKEND_DEFAULT_IDENTITIES = Symbol.for('telefunc.redis.backendDefaultIdentities')
type RedisBackendDefaultIdentities = WeakMap<object, Map<string, object>>
const redisBackendDefaultIdentities = (() => {
  const global = globalThis as typeof globalThis & {
    [REDIS_BACKEND_DEFAULT_IDENTITIES]?: RedisBackendDefaultIdentities
  }
  return (global[REDIS_BACKEND_DEFAULT_IDENTITIES] ??= new WeakMap())
})()

function redisBackendDefaultIdentity(redis: Redis | Cluster, prefix: string | undefined): object {
  let byPrefix = redisBackendDefaultIdentities.get(redis)
  if (byPrefix === undefined) {
    byPrefix = new Map()
    redisBackendDefaultIdentities.set(redis, byPrefix)
  }
  const normalizedPrefix = prefix ?? 'tf:'
  let identity = byPrefix.get(normalizedPrefix)
  if (identity === undefined) {
    identity = {}
    byPrefix.set(normalizedPrefix, identity)
  }
  return identity
}

type InstallRedisOptions = {
  /** Default: `tf:`. */
  prefix?: string
}
