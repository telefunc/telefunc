export { installRedis, RedisTransport }
export type { InstallRedisOptions, RedisBroadcastOptions }
export { RedisRoomBackend } from './room/backend.js'
export type { RedisRoomBackendOptions } from './room/backend.js'

import type { Cluster, Redis } from 'ioredis'
import {
  config,
  KV_KEEP,
  type BroadcastTransport,
  type BroadcastUnsubscribe,
  type KvMutate,
  type RoomFrameCommit,
  type RoomFrameCommitResult,
} from 'telefunc'
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

// Compare-and-set for `update()`: apply the caller's next value only if the key still holds the
// value `mutate` saw (`ARGV[1]`), so a concurrent writer's change is never lost — the transport
// loops and re-runs `mutate` on the fresh value. Single-key, so Cluster-safe with no hash tag.
// `ARGV[1]` is `\0NIL` when the key was absent (room JSON never starts with a NUL byte).
const CAS_LUA = `
local cur = redis.call('GET', KEYS[1])
local matched = (cur == false and ARGV[1] == '\\0NIL') or (cur ~= false and cur == ARGV[1])
if not matched then return 0 end
if ARGV[2] == 'del' then
  redis.call('DEL', KEYS[1])
elseif ARGV[4] == '' then
  redis.call('SET', KEYS[1], ARGV[3])
else
  redis.call('SET', KEYS[1], ARGV[3], 'PX', tonumber(ARGV[4]))
end
return 1
`.trim()

const CAS_CMD = 'tfCas'
const NIL_SENTINEL = '\0NIL'

// The fused assign-order + retain + publish behind a room's semantic timeline (`Room` text and
// `announce`). One Lua call so a room's order stays gap-free and no later frame overtakes: compare
// every fence (a stale incarnation loses), advance the clamped clock at the order key from Redis
// `TIME` (the one cross-instance clock), optionally store the framed retained text, then PUBLISH the
// same wire frame `_publish` emits — the assigned order rides the header, so a subscriber reads it
// there. KEYS[1]=order key, KEYS[2]=channel; ARGV[1]=payload, ARGV[2]=retain key ('' = none),
// ARGV[3]=order TTL ms ('' = none), ARGV[4]=fence count, then (key, expected) pairs. Returns {0} on a
// stale fence (nothing written or published), else {1, seq, ts, receivers}.
//
// NOTE (Cluster): the two declared keys carry no shared intentional hash tag — the order key
// (KEYS[1]) is unbraced, so the whole key hashes, while the channel key (KEYS[2]) is tagged on the
// broadcast key alone — so nothing co-slots them and any agreement is accidental. The retain and
// fence keys are worse: the script reaches them through ARGV instead of declaring them as KEYS at
// all. Either fact rules Cluster out, so this is correct on a single Redis only. Giving a room's
// keys one shared tag and declaring them all in KEYS is the versioned key migration tracked for the
// Redis hardening pass — it needs a real Cluster to certify.
const COMMIT_FRAME_LUA = `
local nf = tonumber(ARGV[4])
for i = 1, nf do
  local cur = redis.call('GET', ARGV[3 + i * 2])
  if not cur or cur ~= ARGV[4 + i * 2] then return {0} end
end
local prev = redis.call('GET', KEYS[1])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local seq, ts
if not prev then
  seq = 1
  ts = now
else
  local pseq, pts = string.match(prev, '(%d+):(%d+)')
  pseq = tonumber(pseq)
  pts = tonumber(pts)
  if now > pts then seq = 1; ts = now else seq = pseq + 1; ts = pts end
end
if ARGV[3] == '' then
  redis.call('SET', KEYS[1], seq .. ':' .. ts)
else
  redis.call('SET', KEYS[1], seq .. ':' .. ts, 'PX', tonumber(ARGV[3]))
end
if ARGV[2] ~= '' then
  redis.call('SET', ARGV[2], seq .. ',' .. ts .. '\\n' .. ARGV[1])
end
local ts_hi = math.floor(ts / 4294967296)
local ts_lo = ts - ts_hi * 4294967296
local header = struct.pack('>I4I4I4', seq, ts_hi, ts_lo)
local receivers = redis.call('PUBLISH', KEYS[2], header .. ARGV[1])
return {1, seq, ts, receivers}
`.trim()

const COMMIT_CMD = 'tfCommitFrame'

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
    this.publisher.defineCommand(CAS_CMD, { numberOfKeys: 1, lua: CAS_LUA })
    this.publisher.defineCommand(COMMIT_CMD, { numberOfKeys: 2, lua: COMMIT_FRAME_LUA })
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

  // ── commitFrame (backs `Room`'s semantic timeline) ────────────────────

  async commitFrame(input: RoomFrameCommit): Promise<RoomFrameCommitResult> {
    // Payload bytes ride the frame (publish); the retain copy stores the same bytes framed as text.
    const buf = Buffer.from(input.payload, 'utf8')
    const argv: (string | Buffer)[] = [
      this.kvKey(input.orderKey), // KEYS[1] — the room's semantic-clock watermark
      this.channelKey(input.channelKey, 't'), // KEYS[2] — the same channel `listen` subscribes to
      buf, // ARGV[1]
      input.retainKey === undefined ? '' : this.kvKey(input.retainKey), // ARGV[2]
      input.orderTtlMs === undefined ? '' : String(input.orderTtlMs), // ARGV[3]
      String(input.fences.length), // ARGV[4]
    ]
    for (const fence of input.fences) argv.push(this.kvKey(fence.key), fence.expected)
    const reply = await callDefinedCommand(this.publisher, COMMIT_CMD, argv)
    assert(Array.isArray(reply) && reply.length >= 1, 'commitFrame script returned an unexpected shape')
    if (reply[0] !== 1) return { ok: false, reason: 'stale-fence' }
    const [, seq, timestamp, receivers] = reply
    assert(
      typeof seq === 'number' && typeof timestamp === 'number' && typeof receivers === 'number',
      'commitFrame script returned non-numeric seq/ts/receivers',
    )
    return { ok: true, seq, timestamp, receivers }
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
    // Not deduplicated: Redis allows SCAN to return the same element more than once (e.g. across a
    // rehash of the walked node), so a caller can see a repeated key.
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

  async setIfAbsent(key: string, value: string, options?: { ttlMs?: number }): Promise<boolean> {
    const k = this.kvKey(key)
    // `SET key value NX` (with `PX` when a TTL is given) writes iff the key is absent — Redis's
    // native create-if-absent — and replies `OK` only when it wrote.
    const res =
      options?.ttlMs === undefined
        ? await this.publisher.set(k, value, 'NX')
        : await this.publisher.set(k, value, 'PX', options.ttlMs, 'NX')
    return res === 'OK'
  }

  async update(key: string, mutate: KvMutate, options?: { ttlMs?: number }): Promise<string | null> {
    const k = this.kvKey(key)
    const ttl = options?.ttlMs === undefined ? '' : String(options.ttlMs)
    // Optimistic CAS loop: GET, run `mutate` in JS, then apply via the Lua script only if the key
    // is unchanged. A lost race re-runs `mutate` on the fresh value, so it converges without holding
    // a WATCH/MULTI transaction open on the shared connection.
    for (;;) {
      const current = await this.publisher.get(k)
      const next = mutate(current)
      if (next === KV_KEEP) return current
      const applied = await callDefinedCommand(this.publisher, CAS_CMD, [
        k,
        current === null ? NIL_SENTINEL : current,
        next === null ? 'del' : 'set',
        next ?? '',
        ttl,
      ])
      if (applied === 1) return next
    }
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
