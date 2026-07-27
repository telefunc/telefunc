export { installRedis, RedisRoomBackend, RedisTransport }
export type { InstallRedisOptions, RedisBroadcastOptions }
export type { RedisRoomBackendOptions } from './room/backend.js'

import type { Cluster, Redis } from 'ioredis'
import { config, type BroadcastTransport, type BroadcastUnsubscribe } from 'telefunc'
import { setDefaultRoomBackend } from 'telefunc/__internal'
import { assert } from './assert.js'
import { callDefinedCommand } from './callDefinedCommand.js'
import { RedisRoomBackend } from './room/backend.js'

/** Wires Redis-backed fan-out into the telefunc broadcast transport so
 *  `Broadcast.publish()`/`subscribe()` and `Room` state cross instances. Pair with
 *  sticky-session routing at the load balancer so each client's channel traffic
 *  stays on one instance. */
function installRedis(redis: Redis | Cluster, options: InstallRedisOptions = {}): void {
  config.broadcast.transport = new RedisTransport({ redis, prefix: options.prefix })
  setDefaultRoomBackend(
    () => new RedisRoomBackend({ redis, prefix: options.prefix }),
    redisRoomBackendDefaultIdentity(redis, options.prefix),
  )
}

const REDIS_ROOM_BACKEND_DEFAULT_IDENTITIES = Symbol.for('telefunc.redis.roomBackendDefaultIdentities')
type RedisRoomBackendDefaultIdentities = WeakMap<object, Map<string, object>>
const redisRoomBackendDefaultIdentities = (() => {
  const global = globalThis as typeof globalThis & {
    [REDIS_ROOM_BACKEND_DEFAULT_IDENTITIES]?: RedisRoomBackendDefaultIdentities
  }
  return (global[REDIS_ROOM_BACKEND_DEFAULT_IDENTITIES] ??= new WeakMap())
})()

function redisRoomBackendDefaultIdentity(redis: Redis | Cluster, prefix: string | undefined): object {
  let byPrefix = redisRoomBackendDefaultIdentities.get(redis)
  if (byPrefix === undefined) {
    byPrefix = new Map()
    redisRoomBackendDefaultIdentities.set(redis, byPrefix)
  }
  const normalizedPrefix = prefix ?? DEFAULT_PREFIX
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

type RedisBroadcastOptions = {
  /** ioredis client (Redis or Cluster). `duplicate()`-d for the subscriber connection. */
  redis: Redis | Cluster
  /** Default: `tf:`. */
  prefix?: string
}

// Wire frame: [u32 BE seq][u32 BE ts_hi][u32 BE ts_lo][payload bytes]. `ts` split into two
// u32s to keep ms-Unix accurate beyond ~50 days. `INCR` + `PUBLISH` happen in one Lua call;
// `TIME` from the single Redis clock orders concurrent publishers across instances.

const DEFAULT_PREFIX = 'tf:'
const HEADER_BYTES = 12
const U32_RANGE = 0x1_0000_0000

/** KEYS[1]=seq counter, KEYS[2]=broadcast channel, ARGV[1]=payload bytes; returns
 *  [seq, ts, receivers] — receivers is PUBLISH's return: how many connections got the message. */
const PUBLISH_LUA = `
local seq = redis.call('INCR', KEYS[1])
local t = redis.call('TIME')
local ts = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ts_hi = math.floor(ts / 4294967296)
local ts_lo = ts - ts_hi * 4294967296
local header = struct.pack('>I4I4I4', seq, ts_hi, ts_lo)
local receivers = redis.call('PUBLISH', KEYS[2], header .. ARGV[1])
return {seq, ts, receivers}
`.trim()

const PUBLISH_CMD = 'tfPublish'

class RedisTransport implements BroadcastTransport {
  private readonly publisher: Redis | Cluster
  private readonly subscriber: Redis | Cluster
  private readonly prefix: string
  private readonly textCallbacks = new Map<string, TextOnMessage>()
  private readonly binaryCallbacks = new Map<string, BinaryOnMessage>()

  constructor(options: RedisBroadcastOptions) {
    this.publisher = options.redis
    this.subscriber = options.redis.duplicate()
    this.prefix = options.prefix ?? DEFAULT_PREFIX
    this.publisher.defineCommand(PUBLISH_CMD, { numberOfKeys: 2, lua: PUBLISH_LUA })
    this.subscriber.on('messageBuffer', this._onMessage)
  }

  async send(key: string, payload: string): Promise<{ seq: number; timestamp: number; receivers: number }> {
    return this._publish(this.channelKey(key, 't'), this.seqKey(key), textEncoder.encode(payload))
  }

  async sendBinary(key: string, payload: Uint8Array): Promise<{ seq: number; timestamp: number; receivers: number }> {
    return this._publish(this.channelKey(key, 'b'), this.seqKey(key), payload)
  }

  listen(key: string, onMessage: TextOnMessage): BroadcastUnsubscribe {
    const channel = this.channelKey(key, 't')
    assert(!this.textCallbacks.has(channel), `Duplicate text listener for key "${key}"`)
    this.textCallbacks.set(channel, onMessage)
    const unsub: BroadcastUnsubscribe = () => {
      this.textCallbacks.delete(channel)
      void this.subscriber.unsubscribe(channel)
    }
    unsub.ready = this._subscribeReady(channel)
    return unsub
  }

  listenBinary(key: string, onMessage: BinaryOnMessage): BroadcastUnsubscribe {
    const channel = this.channelKey(key, 'b')
    assert(!this.binaryCallbacks.has(channel), `Duplicate binary listener for key "${key}"`)
    this.binaryCallbacks.set(channel, onMessage)
    const unsub: BroadcastUnsubscribe = () => {
      this.binaryCallbacks.delete(channel)
      void this.subscriber.unsubscribe(channel)
    }
    unsub.ready = this._subscribeReady(channel)
    return unsub
  }

  /** Real SUBSCRIBE-ack readiness (see `BroadcastUnsubscribe.ready`): a publish only reaches a Redis
   *  subscriber once its SUBSCRIBE has taken effect, so the ack promise is the subscription's readiness.
   *  Resolves rather than rejects on failure — a failed subscribe means the connection is down, which
   *  ioredis re-establishes on reconnect; blocking a retained replay forever on a transient error would
   *  be worse than proceeding degraded. */
  private _subscribeReady(channel: string): Promise<void> {
    return this.subscriber.subscribe(channel).then(
      () => undefined,
      () => undefined,
    )
  }

  private readonly _onMessage = (channelBytes: Uint8Array, frame: Uint8Array): void => {
    assert(frame.byteLength >= HEADER_BYTES, 'Malformed publish frame: header too short')
    const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength)
    const seq = view.getUint32(0, false)
    const timestamp = view.getUint32(4, false) * U32_RANGE + view.getUint32(8, false)
    const payload = frame.subarray(HEADER_BYTES)
    const channel = utf8.decode(channelBytes)
    const text = this.textCallbacks.get(channel)
    if (text) {
      text(utf8.decode(payload), { seq, timestamp })
      return
    }
    const binary = this.binaryCallbacks.get(channel)
    if (binary) binary(payload, { seq, timestamp })
  }

  // ── Publish (private) ─────────────────────────────────────────────────

  private async _publish(
    channelKey: string,
    seqKey: string,
    payload: Uint8Array,
  ): Promise<{ seq: number; timestamp: number; receivers: number }> {
    // ioredis 5.x checks `arg instanceof Buffer` to pick its binary path; a raw
    // `Uint8Array` falls into `String(arg)` and gets serialised as a comma-joined
    // string of byte values, corrupting the bytes. `Buffer.from(buf, off, len)`
    // constructs a zero-copy Buffer view over the same ArrayBuffer.
    const buf = Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength)
    const reply = await callDefinedCommand(this.publisher, PUBLISH_CMD, [seqKey, channelKey, buf])
    assert(Array.isArray(reply) && reply.length === 3, 'Publish script returned an unexpected shape')
    const [seq, timestamp, receivers] = reply
    assert(
      typeof seq === 'number' && typeof timestamp === 'number' && typeof receivers === 'number',
      'Publish script returned non-numeric seq/ts/receivers',
    )
    return { seq, timestamp, receivers }
  }

  // ── Key naming (private) ──────────────────────────────────────────────
  //
  // `{<key>}` braces force seq counter and broadcast channel onto the same Redis
  // Cluster hash slot, so the publish Lua script can touch both keys atomically.

  private seqKey(key: string): string {
    return `${this.prefix}seq:{${key}}`
  }

  private channelKey(key: string, kind: 't' | 'b'): string {
    return `${this.prefix}${kind}:{${key}}`
  }
}

type TextOnMessage = (payload: string, info: { seq: number; timestamp: number }) => void
type BinaryOnMessage = (payload: Uint8Array, info: { seq: number; timestamp: number }) => void

/** Module-level codec — allocating one per call would burn measurable CPU on hot paths. */
const utf8 = new TextDecoder('utf-8')
const textEncoder = new TextEncoder()
