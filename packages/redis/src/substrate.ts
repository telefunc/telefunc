export { RedisChannelSubstrate }
export type { RedisChannelSubstrateOptions }

import type { Cluster, Redis } from 'ioredis'
import {
  decodeProxyEnvelope,
  dispatchEnvelope,
  encodeProxyEnvelope,
  type ChannelSubstrate,
  type ChannelSubstrateHandlers,
  type ProxyEnvelope,
} from 'telefunc/__internal'
import { assert } from './assert.js'
import { callDefinedCommand } from './callDefinedCommand.js'

// Three Redis primitives:
//   - Pin (KV)        `tf:home:<channelId>` → `<homeInstanceId>`, TTL'd.
//   - Inbox (Stream)  `tf:proxy:inbox:<instanceId>` — point-to-point envelope delivery.
//   - Fanout (Stream) `tf:reg` — global registration log; entries `{c: channelId, h: home}`.
//
// Streams (not Pub/Sub) for envelope routing because Pub/Sub drops messages on subscriber
// reconnect, and a dropped envelope silently breaks the channel state machine. `MAXLEN` +
// `lastId` resume bound the buffer and recover any blip shorter than the window.

type RedisChannelSubstrateOptions = {
  /** ioredis Redis or Cluster client. `duplicate()`-d twice for blocking consumer loops. */
  redis: Redis | Cluster
  /** Stable per-process id. Default: random UUID. */
  instanceId?: string
  /** Default: `tf:`. */
  prefix?: string
  /** Default: 30s. Refreshed by heartbeat. */
  pinTtlSeconds?: number
  /** `XADD MAXLEN ~ N` bound on the inbox stream. Default: 10_000. */
  inboxMaxLen?: number
  /** XREAD batch size. Default: 100. */
  readCount?: number
}

const DEFAULT_PREFIX = 'tf:'
const DEFAULT_PIN_TTL_SECONDS = 30
const DEFAULT_INBOX_MAX_LEN = 10_000
const DEFAULT_READ_COUNT = 100
/** Heartbeat at one-third of the pin TTL — margin for latency and clock skew. */
const HEARTBEAT_FRACTION = 3

const FIELD_ENVELOPE = 'e'
const FIELD_CHANNEL = 'c'
const FIELD_HOME = 'h'
const REGISTER_STREAM_MAX_LEN = 10_000
const REGISTER_CMD = 'tfRegisterChannel'

/** Atomic: SET pin + XADD fanout entry in one round trip. */
const REGISTER_LUA = `
local pinKey = KEYS[1]
local fanoutKey = KEYS[2]
local channelId = ARGV[1]
local home = ARGV[2]
local ttl = tonumber(ARGV[3])
local maxLen = tonumber(ARGV[4])
redis.call('SET', pinKey, home, 'EX', ttl)
redis.call('XADD', fanoutKey, 'MAXLEN', '~', maxLen, '*', '${FIELD_CHANNEL}', channelId, '${FIELD_HOME}', home)
return 1
`.trim()

class RedisChannelSubstrate implements ChannelSubstrate {
  readonly selfInstanceId: string
  readonly heartbeatIntervalMs: number
  private readonly client: Redis | Cluster
  private readonly inboxBlockingClient: Redis | Cluster
  private readonly fanoutBlockingClient: Redis | Cluster
  private readonly prefix: string
  private readonly pinTtlSeconds: number
  private readonly inboxMaxLen: number
  private readonly readCount: number
  private readonly registrationWaiters = new Map<string, Set<(home: string) => void>>()
  private readonly listeners = new Set<ChannelSubstrateHandlers>()
  private disposed = false

  constructor(options: RedisChannelSubstrateOptions) {
    this.client = options.redis
    this.inboxBlockingClient = options.redis.duplicate()
    this.fanoutBlockingClient = options.redis.duplicate()
    this.selfInstanceId = options.instanceId ?? crypto.randomUUID()
    this.prefix = options.prefix ?? DEFAULT_PREFIX
    this.pinTtlSeconds = options.pinTtlSeconds ?? DEFAULT_PIN_TTL_SECONDS
    this.inboxMaxLen = options.inboxMaxLen ?? DEFAULT_INBOX_MAX_LEN
    this.readCount = options.readCount ?? DEFAULT_READ_COUNT
    this.heartbeatIntervalMs = Math.max(1_000, (this.pinTtlSeconds * 1000) / HEARTBEAT_FRACTION)
    this.client.defineCommand(REGISTER_CMD, { numberOfKeys: 2, lua: REGISTER_LUA })
    void this.runInboxLoop()
    void this.runFanoutLoop()
  }

  async pinChannel(channelId: string): Promise<void> {
    await callDefinedCommand(this.client, REGISTER_CMD, [
      this.pinKey(channelId),
      this.fanoutKey(),
      channelId,
      this.selfInstanceId,
      String(this.pinTtlSeconds),
      String(REGISTER_STREAM_MAX_LEN),
    ])
  }

  async unpinChannel(channelId: string): Promise<void> {
    await this.client.del(this.pinKey(channelId))
  }

  async refreshPins(channelIds: readonly string[]): Promise<void> {
    if (channelIds.length === 0) return
    const pipeline = this.client.pipeline()
    for (const id of channelIds) pipeline.expire(this.pinKey(id), this.pinTtlSeconds)
    await pipeline.exec()
  }

  /** Subscribe BEFORE GET so any register hitting Redis from now on lands in a live
   *  waiter (no race window). Self pins are filtered. The deadline timer awaits the
   *  in-flight GET before declaring null, so `timeoutMs=0` still gives the synchronous
   *  lookup a chance to complete. */
  async locateRemoteHome(channelId: string, timeoutMs: number): Promise<string | null> {
    return new Promise<string | null>((resolve) => {
      let settled = false
      const finish = (home: string | null): void => {
        if (settled) return
        settled = true
        const set = this.registrationWaiters.get(channelId)
        if (set) {
          set.delete(finish)
          if (set.size === 0) this.registrationWaiters.delete(channelId)
        }
        clearTimeout(timer)
        resolve(home)
      }

      let set = this.registrationWaiters.get(channelId)
      if (!set) {
        set = new Set()
        this.registrationWaiters.set(channelId, set)
      }
      set.add(finish)

      const getPromise: Promise<string | null> = this.client
        .get(this.pinKey(channelId))
        .then((pin) => (pin !== null && pin !== this.selfInstanceId ? pin : null))
        .catch(() => null)
      void getPromise.then((home) => {
        if (home !== null) finish(home)
      })

      const timer = setTimeout(async () => {
        if (settled) return
        finish(await getPromise)
      }, timeoutMs)
    })
  }

  async forward(targetInstance: string, envelope: ProxyEnvelope): Promise<void> {
    // ioredis 5.x checks `arg instanceof Buffer` for the binary path; a raw Uint8Array
    // falls into `String(arg)` and corrupts the bytes. `Buffer.from(buf, off, len)` is a
    // zero-copy view over the same ArrayBuffer.
    const encoded = encodeProxyEnvelope(envelope)
    const value = Buffer.from(encoded.buffer, encoded.byteOffset, encoded.byteLength)
    await this.client.xadd(this.inboxKey(targetInstance), 'MAXLEN', '~', this.inboxMaxLen, '*', FIELD_ENVELOPE, value)
  }

  listen(handlers: ChannelSubstrateHandlers): () => void {
    this.listeners.add(handlers)
    return () => {
      this.listeners.delete(handlers)
    }
  }

  async dispose(): Promise<void> {
    this.disposed = true
    this.listeners.clear()
    this.registrationWaiters.clear()
    // Disconnecting unblocks each parked XREAD with an error; the loops catch it,
    // see `disposed`, and exit cleanly. The user's main client is untouched.
    this.inboxBlockingClient.disconnect()
    this.fanoutBlockingClient.disconnect()
  }

  // ── Always-on consumer loops ──────────────────────────────────────────
  // Each loop holds one duplicated connection on `XREAD BLOCK 0`, resumes from
  // `lastId` across reconnects, and on transient error backs off briefly and retries.

  private async runInboxLoop(): Promise<void> {
    const inboxKey = this.inboxKey(this.selfInstanceId)
    let lastId = '$'
    while (!this.disposed) {
      try {
        const result = await this.inboxBlockingClient.xreadBuffer(
          'COUNT',
          this.readCount,
          'BLOCK',
          0,
          'STREAMS',
          inboxKey,
          lastId,
        )
        assert(result !== null, 'XREAD BLOCK 0 returned null — expected to block indefinitely')
        for (const [, entries] of result) {
          for (const [id, fields] of entries) {
            lastId = utf8.decode(id)
            // Wire format (set by `forward`'s XADD): [field='e'][envelope bytes].
            const envelopeBytes = fields[1]
            assert(envelopeBytes !== undefined, 'Malformed inbox entry')
            const envelope = decodeProxyEnvelope(envelopeBytes)
            for (const handlers of this.listeners) {
              try {
                dispatchEnvelope(handlers, envelope)
              } catch (err) {
                console.error('[telefunc:redis-substrate] listener error', err)
              }
            }
          }
        }
      } catch {
        if (this.disposed) return
        await sleep(1000)
      }
    }
  }

  private async runFanoutLoop(): Promise<void> {
    const key = this.fanoutKey()
    let lastId = '$'
    while (!this.disposed) {
      try {
        const result = await this.fanoutBlockingClient.xread(
          'COUNT',
          this.readCount,
          'BLOCK',
          0,
          'STREAMS',
          key,
          lastId,
        )
        assert(result !== null, 'XREAD BLOCK 0 returned null — expected to block indefinitely')
        for (const [, entries] of result) {
          for (const [id, fields] of entries) {
            lastId = id
            // Wire format (set by REGISTER_LUA's XADD): [c, channelId, h, home].
            const channelId = fields[1]
            const home = fields[3]
            assert(channelId !== undefined && home !== undefined, 'Malformed fanout entry')
            // Same-instance entries already settled in-process by `pinChannel`'s waiter fire.
            if (home === this.selfInstanceId) continue
            const set = this.registrationWaiters.get(channelId)
            if (!set) continue
            // Snapshot — callbacks delete themselves from the set via `finish`.
            for (const cb of [...set]) cb(home)
          }
        }
      } catch {
        if (this.disposed) return
        await sleep(1000)
      }
    }
  }

  // ── Key naming ────────────────────────────────────────────────────────

  private pinKey(channelId: string): string {
    return `${this.prefix}home:${channelId}`
  }

  private fanoutKey(): string {
    return `${this.prefix}reg`
  }

  private inboxKey(instanceId: string): string {
    return `${this.prefix}proxy:inbox:${instanceId}`
  }
}

const utf8 = new TextDecoder('utf-8')

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
