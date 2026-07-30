export { installRedis, RedisRoomBackend, RedisTransport }
export type { InstallRedisOptions, RedisBroadcastOptions }
export type { RedisRoomBackendOptions } from './room/backend.js'

import type { Cluster, Redis } from 'ioredis'
import { setDefaultBackend, superviseBackend } from 'telefunc/__internal'
import { RedisRoomBackend, type RedisRoomBackendOptions } from './room/backend.js'

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

type RedisBroadcastOptions = RedisRoomBackendOptions

/**
 * Compatibility wrapper for the released pre-Room Redis transport. New applications should use
 * installRedis(), while existing consumers retain the original text/binary send and listen surface.
 */
class RedisTransport {
  readonly #backend

  constructor(options: RedisBroadcastOptions) {
    this.#backend = superviseBackend(new RedisRoomBackend(options))
  }

  async send(key: string, payload: string): Promise<{ seq: number; timestamp: number }> {
    const { seq, timestamp } = await this.#backend.publish({ key, kind: 'text' }, textEncoder.encode(payload))
    return { seq, timestamp }
  }

  async sendBinary(key: string, payload: Uint8Array): Promise<{ seq: number; timestamp: number }> {
    const { seq, timestamp } = await this.#backend.publish({ key, kind: 'binary' }, payload)
    return { seq, timestamp }
  }

  listen(key: string, onMessage: (payload: string, info: { seq: number; timestamp: number }) => void): () => void {
    return this.#listen({ key, kind: 'text' }, (payload, info) => onMessage(textDecoder.decode(payload), info))
  }

  listenBinary(
    key: string,
    onMessage: (payload: Uint8Array, info: { seq: number; timestamp: number }) => void,
  ): () => void {
    return this.#listen({ key, kind: 'binary' }, onMessage)
  }

  #listen(
    lane: { key: string; kind: 'text' | 'binary' },
    onMessage: (payload: Uint8Array, info: { seq: number; timestamp: number }) => void,
  ): () => void {
    const subscription = this.#backend.subscribe(lane, onMessage)
    void subscription.ready.catch(() => {})
    return () => void subscription.unsubscribe()
  }
}

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()
