import { afterEach, describe, expect, it, vi } from 'vitest'
import type { Redis } from 'ioredis'
import { config, DefaultBroadcastAdapter } from 'telefunc'
import { disposeRoomBackend, getRoomBackend, installRoomBackend } from 'telefunc/backend'
import { installRedis, RedisRoomBackend, RedisTransport } from './index.js'

// Fake `ioredis` — `defineCommand` + `duplicate()` + broadcast subscribe/dispatch. Lua
// execution emulated in TS so we exercise the adapter's call graph without a real Redis.

class FakeIoredis {
  readonly status = 'ready'
  readonly definedCommands: string[] = []
  /** `seqKey → counter` for the in-script `INCR`. */
  private readonly counters = new Map<string, number>()
  private readonly listeners: Array<(channel: Uint8Array, message: Uint8Array) => void> = []
  lastPublishKeys: readonly [seqKey: string, channelKey: string] | undefined
  /** Mocked clock so tests can assert deterministic ts. */
  private clockMs = 1_700_000_000_000

  setClock(ms: number): void {
    this.clockMs = ms
  }

  // `duplicate()` would normally allocate a new TCP connection; in the fake we
  // share state — there's only one in-memory Redis to emulate.
  duplicate(): this {
    return this
  }

  defineCommand(name: string, _def: { numberOfKeys: number; lua: string }): void {
    this.definedCommands.push(name)
    ;(this as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>)[name] = (
      ...args: unknown[]
    ): Promise<unknown> => Promise.resolve(this.runPublishScript(args))
  }

  /** Channels the (shared) subscriber connection is subscribed to — backs PUBLISH's receiver count. */
  private readonly subscribedChannels = new Set<string>()

  async subscribe(...channels: string[]): Promise<number> {
    for (const channel of channels) this.subscribedChannels.add(channel)
    return this.subscribedChannels.size
  }

  async unsubscribe(...channels: string[]): Promise<number> {
    for (const channel of channels) this.subscribedChannels.delete(channel)
    return this.subscribedChannels.size
  }

  on(_event: 'messageBuffer', listener: (channel: Uint8Array, message: Uint8Array) => void): this {
    this.listeners.push(listener)
    return this
  }

  off(): this {
    return this
  }

  async ping(): Promise<string> {
    return 'PONG'
  }

  async quit(): Promise<'OK'> {
    return 'OK'
  }

  disconnect(): void {}

  // ── Private: emulate the Lua publish script's effect ─────────────────

  private runPublishScript(args: unknown[]): [number, number, number] {
    const [seqKey, channelKey, payload] = args as [string, string, Buffer]
    this.lastPublishKeys = [seqKey, channelKey]
    const seq = (this.counters.get(seqKey) ?? 0) + 1
    this.counters.set(seqKey, seq)
    const ts = this.clockMs
    const frame = encodeFrame(seq, ts, payload)
    const channelBytes = new TextEncoder().encode(channelKey)
    for (const cb of this.listeners) cb(channelBytes, frame)
    // Like real PUBLISH: how many subscriber connections got it — the fake has one.
    return [seq, ts, this.subscribedChannels.has(channelKey) ? 1 : 0]
  }
}

function encodeFrame(seq: number, ts: number, payload: Uint8Array): Uint8Array {
  const HEADER = 12
  const out = new Uint8Array(HEADER + payload.byteLength)
  const view = new DataView(out.buffer)
  view.setUint32(0, seq, false)
  const tsHi = Math.floor(ts / 0x1_0000_0000)
  view.setUint32(4, tsHi, false)
  view.setUint32(8, ts - tsHi * 0x1_0000_0000, false)
  out.set(payload, HEADER)
  return out
}

// ───────────────────────────────────────────────────────────────────────────
// Spec
// ───────────────────────────────────────────────────────────────────────────

function newAdapter() {
  const fake = new FakeIoredis()
  const transport = new RedisTransport({ redis: fake as unknown as Redis })
  const adapter = new DefaultBroadcastAdapter(transport)
  return { fake, transport, adapter }
}

describe('released installRedis surface', () => {
  afterEach(async () => {
    await disposeRoomBackend()
  })

  it('installs the Redis Room backend from the same client', () => {
    const fake = new FakeIoredis()
    installRedis(fake as unknown as Redis)
    expect(getRoomBackend()).toBeInstanceOf(RedisRoomBackend)
    expect(fake.definedCommands).toContain('tfRoomCommit')
  })

  it('keeps repeated setup with the same client and prefix Room-connection idempotent', () => {
    const fake = new FakeIoredis()
    installRedis(fake as unknown as Redis, { prefix: 'custom:' })
    const backend = getRoomBackend()
    const roomCommandCount = fake.definedCommands.filter((name) => name === 'tfRoomCommit').length

    installRedis(fake as unknown as Redis, { prefix: 'custom:' })
    expect(getRoomBackend()).toBe(backend)
    expect(fake.definedCommands.filter((name) => name === 'tfRoomCommit')).toHaveLength(roomCommandCount)
  })

  it('lets an explicit Room backend installed after installRedis win', () => {
    const automaticRedis = new FakeIoredis()
    installRedis(automaticRedis as unknown as Redis)
    const automatic = getRoomBackend()
    const dispose = vi.spyOn(automatic, 'dispose')
    const explicit = new RedisRoomBackend({ redis: new FakeIoredis() as unknown as Redis })

    expect(installRoomBackend(() => explicit)).toBe(explicit)
    expect(getRoomBackend()).toBe(explicit)
    expect(dispose).toHaveBeenCalledTimes(1)
  })

  it('never lets installRedis overwrite an earlier explicit Room backend', () => {
    const explicit = new RedisRoomBackend({ redis: new FakeIoredis() as unknown as Redis })
    installRoomBackend(() => explicit)

    installRedis(new FakeIoredis() as unknown as Redis)
    expect(getRoomBackend()).toBe(explicit)
  })

  it('installs the existing Redis transport with its prefix option unchanged', async () => {
    const fake = new FakeIoredis()
    installRedis(fake as unknown as Redis, { prefix: 'custom:' })
    const transport = config.broadcast.transport as RedisTransport
    await transport.send('key', 'value')
    expect(fake.lastPublishKeys).toEqual(['custom:seq:{key}', 'custom:t:{key}'])
  })
})

describe('Redis adapter — atomic publish via Lua', () => {
  it('returns the monotonic per-key seq and Redis-clock timestamp assigned by the script', async () => {
    const { fake, adapter } = newAdapter()
    fake.setClock(1_700_000_001_000)

    const first = await adapter.publish('room:a', 'hello')
    const second = await adapter.publish('room:a', 'world')

    expect(first).toMatchObject({ seq: 1, timestamp: 1_700_000_001_000 })
    expect(second).toMatchObject({ seq: 2, timestamp: 1_700_000_001_000 })
  })

  it('keeps separate seq counters per key', async () => {
    const { adapter } = newAdapter()

    const a1 = await adapter.publish('room:a', 'one')
    const b1 = await adapter.publish('room:b', 'one')
    const a2 = await adapter.publish('room:a', 'two')

    expect(a1.seq).toBe(1)
    expect(b1.seq).toBe(1)
    expect(a2.seq).toBe(2)
  })
})

describe('Redis adapter — live delivery', () => {
  it('decodes the binary header and UTF-8 payload for text subscribers with the same seq/timestamp the publisher saw', async () => {
    const { fake, adapter } = newAdapter()
    fake.setClock(1_700_000_002_000)

    const received: Array<{ payload: string; seq: number; timestamp: number }> = []
    adapter.subscribe('room:live', (payload, info) => received.push({ payload, ...info }))

    await adapter.publish('room:live', 'msg-1')
    await adapter.publish('room:live', 'msg-2')

    expect(received).toEqual([
      { payload: 'msg-1', seq: 1, timestamp: 1_700_000_002_000 },
      { payload: 'msg-2', seq: 2, timestamp: 1_700_000_002_000 },
    ])
  })

  it('decodes binary frames including the 12-byte BE header (seq + u64 ts split into two halves)', async () => {
    const { fake, adapter } = newAdapter()
    fake.setClock(1_700_000_003_000)

    const received: Array<{ payload: Uint8Array; seq: number; timestamp: number }> = []
    adapter.subscribeBinary('room:bin', (payload, info) => received.push({ payload, ...info }))

    await adapter.publishBinary('room:bin', new Uint8Array([0xde, 0xad, 0xbe, 0xef]))

    expect(received).toHaveLength(1)
    expect(Array.from(received[0]!.payload)).toEqual([0xde, 0xad, 0xbe, 0xef])
    expect(received[0]!.seq).toBe(1)
    expect(received[0]!.timestamp).toBe(1_700_000_003_000)
  })
})
