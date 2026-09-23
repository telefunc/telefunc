export { installRedis }
export type { InstallRedisOptions }

import type { RedisClient } from './ioredis.js'
import { getGlobalObject, installBackend, type BackendDriverPair } from 'telefunc/__internal'
import { RedisBackend, type RedisBackendOptions } from './room/backend.js'

function installRedis(redis: RedisClient, options: InstallRedisOptions = {}): void {
  installBackend(
    () => createRedisBackendPair({ redis, prefix: options.prefix }),
    internRedisBackendIdentity(redis, options.prefix),
  )
}

function createRedisBackendPair(options: RedisBackendOptions): BackendDriverPair {
  const driver = new RedisBackend(options)
  return {
    driver,
    dispose: () => driver.dispose(),
  }
}

type RedisBackendDefaultIdentities = WeakMap<object, Map<string, object>>

function getRedisBackendDefaultIdentities(): RedisBackendDefaultIdentities {
  return getGlobalObject('redis/index.ts', { backendDefaultIdentities: new WeakMap() }).backendDefaultIdentities
}

function internRedisBackendIdentity(redis: RedisClient, prefix: string | undefined): object {
  const redisBackendDefaultIdentities = getRedisBackendDefaultIdentities()
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
