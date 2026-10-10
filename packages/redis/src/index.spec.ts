import { describe, expect, it } from 'vitest'
import type { Redis } from 'ioredis'
import { DefaultBroadcastAdapter } from 'telefunc'
import { RedisTransport } from './index.js'

// Fake `ioredis` — `defineCommand` + `duplicate()` + broadcast subscribe/dispatch. Lua
// execution emulated in TS so we exercise the adapter's call graph without a real Redis.

class FakeIoredis {
  /** `seqKey → counter` for the in-script `INCR`. */
  private readonly counters = new Map<string, number>()
  private readonly listeners: Array<(channel: Uint8Array, message: Uint8Array) => void> = []
  /** Mocked clock so tests can assert deterministic ts. */
  private clockMs = 1_700_000_000_000

  setClock(ms: number): void {
    this.clockMs = ms
  }

  /** What the next `INCR` on `seqKey` counts on from. */
  setCounter(seqKey: string, seq: number): void {
    this.counters.set(seqKey, seq)
  }

  // `duplicate()` would normally allocate a new TCP connection; in the fake we
  // share state — there's only one in-memory Redis to emulate.
  duplicate(): this {
    return this
  }

  defineCommand(name: string, _def: { numberOfKeys: number; lua: string }): void {
    ;(this as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>)[name] = (
      ...args: unknown[]
    ): Promise<unknown> => Promise.resolve(this.runPublishScript(args))
  }

  async subscribe(..._channels: string[]): Promise<number> {
    return _channels.length
  }

  async unsubscribe(..._channels: string[]): Promise<number> {
    return 0
  }

  on(_event: 'messageBuffer', listener: (channel: Uint8Array, message: Uint8Array) => void): this {
    this.listeners.push(listener)
    return this
  }

  off(): this {
    return this
  }

  // ── Private: emulate the Lua publish script's effect ─────────────────

  private runPublishScript(args: unknown[]): [number, number] {
    const [seqKey, channelKey, payload] = args as [string, string, Buffer]
    const seq = (this.counters.get(seqKey) ?? 0) + 1
    this.counters.set(seqKey, seq)
    const ts = this.clockMs
    const frame = encodeFrame(seq, ts, payload)
    const channelBytes = new TextEncoder().encode(channelKey)
    for (const cb of this.listeners) cb(channelBytes, frame)
    return [seq, ts]
  }
}

function encodeFrame(seq: number, ts: number, payload: Uint8Array): Uint8Array {
  const HEADER = 16
  const out = new Uint8Array(HEADER + payload.byteLength)
  const view = new DataView(out.buffer)
  const seqHi = Math.floor(seq / 0x1_0000_0000)
  view.setUint32(0, seqHi, false)
  view.setUint32(4, seq - seqHi * 0x1_0000_0000, false)
  const tsHi = Math.floor(ts / 0x1_0000_0000)
  view.setUint32(8, tsHi, false)
  view.setUint32(12, ts - tsHi * 0x1_0000_0000, false)
  out.set(payload, HEADER)
  return out
}

// ───────────────────────────────────────────────────────────────────────────
// Spec
// ───────────────────────────────────────────────────────────────────────────

function newAdapter() {
  const fake = new FakeIoredis()
  const adapter = new DefaultBroadcastAdapter(new RedisTransport({ redis: fake as unknown as Redis }))
  return { fake, adapter }
}

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

  it('decodes binary frames including the 16-byte BE header (seq and ts, each split into two halves)', async () => {
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

  it('gives subscribers the seq the publisher saw past 2^32 publishes on a key, text and binary alike', async () => {
    const { fake, adapter } = newAdapter()
    fake.setCounter('tf:seq:{room:far}', 2 ** 32 - 2)
    const received: number[] = []
    adapter.subscribe('room:far', (_, info) => void received.push(info.seq))
    adapter.subscribeBinary('room:far', (_, info) => void received.push(info.seq))

    const published = [
      await adapter.publish('room:far', 'a'),
      await adapter.publishBinary('room:far', new Uint8Array([1])),
      await adapter.publish('room:far', 'b'),
    ]

    expect(published.map(({ seq }) => seq)).toEqual([2 ** 32 - 1, 2 ** 32, 2 ** 32 + 1])
    expect(received).toEqual([2 ** 32 - 1, 2 ** 32, 2 ** 32 + 1])
  })
})
