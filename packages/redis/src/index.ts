export { installRedis, RedisTransport }
export type { InstallRedisOptions, RedisBroadcastOptions }

import type { Cluster, Redis } from 'ioredis'
import { config, type BroadcastTransport } from 'telefunc'
import { assert } from './assert.js'
import { callDefinedCommand } from './callDefinedCommand.js'

/** Wires Redis-backed fan-out into the telefunc broadcast transport so
 *  `Broadcast.publish()`/`subscribe()` and `Room` state cross instances. Pair with
 *  sticky-session routing at the load balancer so each client's channel traffic
 *  stays on one instance. */
function installRedis(redis: Redis | Cluster, options: InstallRedisOptions = {}): void {
  config.broadcast.transport = new RedisTransport({ redis, prefix: options.prefix })
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

/** KEYS[1]=seq counter, KEYS[2]=broadcast channel, ARGV[1]=payload bytes; returns [seq,ts]. */
const PUBLISH_LUA = `
local seq = redis.call('INCR', KEYS[1])
local t = redis.call('TIME')
local ts = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ts_hi = math.floor(ts / 4294967296)
local ts_lo = ts - ts_hi * 4294967296
local header = struct.pack('>I4I4I4', seq, ts_hi, ts_lo)
redis.call('PUBLISH', KEYS[2], header .. ARGV[1])
return {seq, ts}
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

  async send(key: string, payload: string): Promise<{ seq: number; timestamp: number }> {
    return this._publish(this.channelKey(key, 't'), this.seqKey(key), textEncoder.encode(payload))
  }

  async sendBinary(key: string, payload: Uint8Array): Promise<{ seq: number; timestamp: number }> {
    return this._publish(this.channelKey(key, 'b'), this.seqKey(key), payload)
  }

  listen(key: string, onMessage: TextOnMessage): () => void {
    const channel = this.channelKey(key, 't')
    assert(!this.textCallbacks.has(channel), `Duplicate text listener for key "${key}"`)
    this.textCallbacks.set(channel, onMessage)
    void this.subscriber.subscribe(channel)
    return () => {
      this.textCallbacks.delete(channel)
      void this.subscriber.unsubscribe(channel)
    }
  }

  listenBinary(key: string, onMessage: BinaryOnMessage): () => void {
    const channel = this.channelKey(key, 'b')
    assert(!this.binaryCallbacks.has(channel), `Duplicate binary listener for key "${key}"`)
    this.binaryCallbacks.set(channel, onMessage)
    void this.subscriber.subscribe(channel)
    return () => {
      this.binaryCallbacks.delete(channel)
      void this.subscriber.unsubscribe(channel)
    }
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
  ): Promise<{ seq: number; timestamp: number }> {
    // ioredis 5.x checks `arg instanceof Buffer` to pick its binary path; a raw
    // `Uint8Array` falls into `String(arg)` and gets serialised as a comma-joined
    // string of byte values, corrupting the bytes. `Buffer.from(buf, off, len)`
    // constructs a zero-copy Buffer view over the same ArrayBuffer.
    const buf = Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength)
    const reply = await callDefinedCommand(this.publisher, PUBLISH_CMD, [seqKey, channelKey, buf])
    assert(Array.isArray(reply) && reply.length === 2, 'Publish script returned an unexpected shape')
    const [seq, timestamp] = reply
    assert(typeof seq === 'number' && typeof timestamp === 'number', 'Publish script returned non-numeric seq/ts')
    return { seq, timestamp }
  }

  // ── KV (backs `Room` state) ───────────────────────────────────────────

  async get(key: string): Promise<string | null> {
    return await this.publisher.get(this.kvKey(key))
  }

  async set(key: string, value: string, options?: { ttlMs?: number }): Promise<void> {
    if (options?.ttlMs === undefined) await this.publisher.set(this.kvKey(key), value)
    else await this.publisher.set(this.kvKey(key), value, 'PX', options.ttlMs)
  }

  async delete(key: string): Promise<void> {
    await this.publisher.del(this.kvKey(key))
  }

  async keys(prefix: string): Promise<string[]> {
    const namespace = this.kvKey('')
    const pattern = `${escapeGlob(this.kvKey(prefix))}*`
    const keys: string[] = []
    // On a Cluster, SCAN only walks the node it's sent to — walk every master.
    // Master keyspaces are disjoint, so no deduplication is needed.
    for (const node of this.scanTargets()) {
      let cursor = '0'
      do {
        const [next, page] = await node.scan(cursor, 'MATCH', pattern, 'COUNT', 250)
        cursor = next
        for (const key of page) keys.push(key.slice(namespace.length))
      } while (cursor !== '0')
    }
    return keys
  }

  private scanTargets(): Array<Redis | Cluster> {
    const cluster = this.publisher as Cluster
    return typeof cluster.nodes === 'function' ? cluster.nodes('master') : [this.publisher]
  }

  // ── Key naming (private) ──────────────────────────────────────────────
  //
  // `{<key>}` braces force seq counter and broadcast channel onto the same Redis
  // Cluster hash slot, so the publish Lua script can touch both keys atomically.
  // KV keys are unbraced: every KV operation is single-key, so no cross-key
  // atomicity — and thus no hash tag — is needed.

  private seqKey(key: string): string {
    return `${this.prefix}seq:{${key}}`
  }

  private channelKey(key: string, kind: 't' | 'b'): string {
    return `${this.prefix}${kind}:{${key}}`
  }

  private kvKey(key: string): string {
    return `${this.prefix}kv:${key}`
  }
}

/** SCAN's MATCH is a glob — keys built from user-supplied room IDs must match literally. */
function escapeGlob(pattern: string): string {
  return pattern.replace(/[*?[\]\\]/g, '\\$&')
}

type TextOnMessage = (payload: string, info: { seq: number; timestamp: number }) => void
type BinaryOnMessage = (payload: Uint8Array, info: { seq: number; timestamp: number }) => void

/** Module-level codec — allocating one per call would burn measurable CPU on hot paths. */
const utf8 = new TextDecoder('utf-8')
const textEncoder = new TextEncoder()
