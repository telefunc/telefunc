export { installRedis, RedisTransport }
export type { InstallRedisOptions, RedisBroadcastOptions }

import type { Cluster, Redis } from 'ioredis'
import {
  BACKEND_SPI_VERSION,
  setDefaultBackend,
  superviseBroadcastDriver,
  type BackendDriverPair,
} from 'telefunc/__internal'
import { RedisBackend, type RedisBackendOptions } from './room/backend.js'

function installRedis(redis: Redis | Cluster, options: InstallRedisOptions = {}): void {
  setDefaultBackend(
    () => createRedisBackendPair({ redis, prefix: options.prefix }),
    internRedisBackendIdentity(redis, options.prefix),
  )
}

function createRedisBackendPair(options: RedisBackendOptions): BackendDriverPair {
  const driver = new RedisBackend(options)
  return {
    spiVersion: BACKEND_SPI_VERSION,
    driver,
    dispose: () => driver.dispose(),
  }
}

const REDIS_BACKEND_DEFAULT_IDENTITIES = Symbol.for('telefunc.redis.backendDefaultIdentities')
type RedisBackendDefaultIdentities = WeakMap<object, Map<string, object>>
const redisBackendDefaultIdentities = (() => {
  const global = globalThis as typeof globalThis & {
    [REDIS_BACKEND_DEFAULT_IDENTITIES]?: RedisBackendDefaultIdentities
  }
  return (global[REDIS_BACKEND_DEFAULT_IDENTITIES] ??= new WeakMap())
})()

function internRedisBackendIdentity(redis: Redis | Cluster, prefix: string | undefined): object {
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

type RedisBroadcastOptions = RedisBackendOptions

/** Released legacy transport wrapper; new applications should use installRedis(). */
class RedisTransport {
  private readonly _backend

  constructor(options: RedisBroadcastOptions) {
    const driver = new RedisBackend(options)
    this._backend = superviseBroadcastDriver(driver, () => driver.dispose())
  }

  async send(key: string, payload: string): Promise<{ seq: number; timestamp: number }> {
    const { seq, timestamp } = await this._backend.publish({ key, kind: 'text' }, textEncoder.encode(payload))
    return { seq, timestamp }
  }

  async sendBinary(key: string, payload: Uint8Array): Promise<{ seq: number; timestamp: number }> {
    const { seq, timestamp } = await this._backend.publish({ key, kind: 'binary' }, payload)
    return { seq, timestamp }
  }

  listen(key: string, onMessage: (payload: string, info: { seq: number; timestamp: number }) => void): () => void {
    return this._listen({ key, kind: 'text' }, (payload, info) => onMessage(textDecoder.decode(payload), info))
  }

  listenBinary(
    key: string,
    onMessage: (payload: Uint8Array, info: { seq: number; timestamp: number }) => void,
  ): () => void {
    return this._listen({ key, kind: 'binary' }, onMessage)
  }

  private _listen(
    lane: { key: string; kind: 'text' | 'binary' },
    onMessage: (payload: Uint8Array, info: { seq: number; timestamp: number }) => void,
  ): () => void {
    const subscription = this._backend.subscribe(lane, onMessage)
    void subscription.ready.catch(() => {})
    return () => void subscription.unsubscribe()
  }
}

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()
