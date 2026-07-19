import { describe, expect, it } from 'vitest'
import type { Redis } from 'ioredis'
import { config, DefaultBroadcastAdapter, KV_KEEP, Room } from 'telefunc'
import { RedisTransport } from './index.js'

// Fake `ioredis` — `defineCommand` + `duplicate()` + broadcast subscribe/dispatch. Lua
// execution emulated in TS so we exercise the adapter's call graph without a real Redis.

class FakeIoredis {
  /** `seqKey → counter` for the in-script `INCR`. */
  private readonly counters = new Map<string, number>()
  private readonly listeners: Array<(channel: Uint8Array, message: Uint8Array) => void> = []
  /** Backs GET/SET/DEL/SCAN. Public so a test can simulate a concurrent writer. */
  readonly store = new Map<string, string>()
  /** Mocked clock so tests can assert deterministic ts. */
  private clockMs = 1_700_000_000_000

  setClock(ms: number): void {
    this.clockMs = ms
  }

  async get(key: string): Promise<string | null> {
    return this.store.get(key) ?? null
  }

  readonly ttls = new Map<string, number>()

  // Emulates the option tokens the transport emits: `PX <ms>` and/or `NX`. `NX` (create-if-absent)
  // replies `null` when the key already exists.
  async set(key: string, value: string, ...opts: unknown[]): Promise<'OK' | null> {
    if (opts.includes('NX') && this.store.has(key)) return null
    const pxIndex = opts.indexOf('PX')
    this.store.set(key, value)
    if (pxIndex >= 0 && typeof opts[pxIndex + 1] === 'number') this.ttls.set(key, opts[pxIndex + 1] as number)
    return 'OK'
  }

  async del(key: string): Promise<number> {
    return this.store.delete(key) ? 1 : 0
  }

  /** Single-page SCAN honoring a literal MATCH pattern with a trailing `*` (the only shape the transport emits). */
  async scan(
    _cursor: string,
    _match: 'MATCH',
    pattern: string,
    _count: 'COUNT',
    _n: number,
  ): Promise<[string, string[]]> {
    const literalPrefix = pattern.slice(0, -1).replace(/\\([*?[\]\\])/g, '$1')
    const keys = [...this.store.keys()].filter((key) => key.startsWith(literalPrefix))
    return ['0', keys]
  }

  // `duplicate()` would normally allocate a new TCP connection; in the fake we
  // share state — there's only one in-memory Redis to emulate.
  duplicate(): this {
    return this
  }

  defineCommand(name: string, _def: { numberOfKeys: number; lua: string }): void {
    const run =
      name === 'tfCas'
        ? (args: unknown[]) => this.runCasScript(args)
        : name === 'tfCommitFrame'
          ? (args: unknown[]) => this.runCommitFrameScript(args)
          : (args: unknown[]) => this.runPublishScript(args)
    ;(this as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>)[name] = (
      ...args: unknown[]
    ): Promise<unknown> => Promise.resolve(run(args))
  }

  /** Emulate the compare-and-set Lua: apply only if the current value matches `expected`
   *  (`\0NIL` = absent). Returns 1 on apply, 0 on mismatch. */
  private runCasScript(args: unknown[]): number {
    const [key, expected, op, next, ttl] = args as [string, string, string, string, string]
    const cur = this.store.has(key) ? this.store.get(key)! : null
    const matched = (cur === null && expected === '\0NIL') || cur === expected
    if (!matched) return 0
    if (op === 'del') {
      this.store.delete(key)
      this.ttls.delete(key)
    } else {
      this.store.set(key, next)
      if (ttl === '') this.ttls.delete(key)
      else this.ttls.set(key, Number(ttl))
    }
    return 1
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

  // ── Private: emulate the Lua publish script's effect ─────────────────

  private runPublishScript(args: unknown[]): [number, number, number] {
    const [seqKey, channelKey, payload] = args as [string, string, Buffer]
    const seq = (this.counters.get(seqKey) ?? 0) + 1
    this.counters.set(seqKey, seq)
    const ts = this.clockMs
    const frame = encodeFrame(seq, ts, payload)
    const channelBytes = new TextEncoder().encode(channelKey)
    for (const cb of this.listeners) cb(channelBytes, frame)
    // Like real PUBLISH: how many subscriber connections got it — the fake has one.
    return [seq, ts, this.subscribedChannels.has(channelKey) ? 1 : 0]
  }

  /** Emulate the commitFrame Lua: fence-check, advance the clamped clock at the order key, store the
   *  framed retained text, then publish the frame. Returns [0] on a stale fence, else [1, seq, ts, receivers]. */
  private runCommitFrameScript(args: unknown[]): [number] | [number, number, number, number] {
    const [orderKey, channelKey, payload, retainKey, orderTtl] = args as [string, string, Buffer, string, string]
    const nf = Number(args[5])
    for (let i = 0; i < nf; i++) {
      const cur = this.store.get(args[6 + i * 2] as string)
      if (cur === undefined || cur !== (args[7 + i * 2] as string)) return [0]
    }
    const prev = this.store.get(orderKey)
    const now = this.clockMs
    let seq: number
    let ts: number
    if (prev === undefined) {
      seq = 1
      ts = now
    } else {
      const parts = prev.split(':')
      const pseq = Number(parts[0])
      const pts = Number(parts[1])
      if (now > pts) {
        seq = 1
        ts = now
      } else {
        seq = pseq + 1
        ts = pts
      }
    }
    this.store.set(orderKey, `${seq}:${ts}`)
    if (orderTtl === '') this.ttls.delete(orderKey)
    else this.ttls.set(orderKey, Number(orderTtl))
    if (retainKey !== '') this.store.set(retainKey, `${seq},${ts}\n${payload.toString('utf8')}`)
    const frame = encodeFrame(seq, ts, payload)
    const channelBytes = new TextEncoder().encode(channelKey)
    for (const cb of this.listeners) cb(channelBytes, frame)
    return [1, seq, ts, this.subscribedChannels.has(channelKey) ? 1 : 0]
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

describe('Redis adapter — KV (backs `Room` state)', () => {
  it('round-trips values under the transport prefix and strips it from keys()', async () => {
    const { fake, adapter } = newAdapter()

    expect(await adapter.get('telefunc:room:lobby:config')).toBe(null)
    await adapter.set('telefunc:room:lobby:config', '{"a":1}')
    expect(await adapter.get('telefunc:room:lobby:config')).toBe('{"a":1}')
    expect(await fake.get('tf:kv:telefunc:room:lobby:config')).toBe('{"a":1}') // namespaced in Redis

    await adapter.set('telefunc:room:lobby:m:x', '{}')
    await adapter.set('unrelated', '{}')
    expect((await adapter.keys('telefunc:room:')).sort()).toEqual([
      'telefunc:room:lobby:config',
      'telefunc:room:lobby:m:x',
    ])

    await adapter.delete('telefunc:room:lobby:config')
    expect(await adapter.get('telefunc:room:lobby:config')).toBe(null)
  })

  it('passes the TTL through as Redis PX — native expiry backs the crash reaper', async () => {
    const { fake, adapter } = newAdapter()
    await adapter.set!('telefunc:room:r:m:x', '{"seenAt":1}', { ttlMs: 180_000 })
    expect(fake.ttls.get('tf:kv:telefunc:room:r:m:x')).toBe(180_000)
    await adapter.set!('telefunc:room:r:config', '{}') // no TTL — config records persist
    expect(fake.ttls.has('tf:kv:telefunc:room:r:config')).toBe(false)
  })

  it('matches glob metacharacters in room IDs literally, not as patterns', async () => {
    const { adapter } = newAdapter()

    await adapter.set('telefunc:room:a*b:config', '{}')
    await adapter.set('telefunc:room:axb:config', '{}')

    expect(await adapter.keys('telefunc:room:a*b')).toEqual(['telefunc:room:a*b:config'])
  })
})

describe('Redis adapter — atomic KV primitives (back race-free room state)', () => {
  it('setIfAbsent writes once via SET NX — the first caller wins, the rest see it present', async () => {
    const { fake, adapter } = newAdapter()

    expect(await adapter.setIfAbsent!('telefunc:room:x:config', 'first')).toBe(true)
    expect(await adapter.setIfAbsent!('telefunc:room:x:config', 'second')).toBe(false)
    expect(await adapter.get('telefunc:room:x:config')).toBe('first')
    expect(await fake.get('tf:kv:telefunc:room:x:config')).toBe('first') // untouched by the loser
  })

  it('setIfAbsent passes the TTL through as PX', async () => {
    const { fake, adapter } = newAdapter()
    await adapter.setIfAbsent!('telefunc:room:x:m:1', '{}', { ttlMs: 180_000 })
    expect(fake.ttls.get('tf:kv:telefunc:room:x:m:1')).toBe(180_000)
  })

  it('update seeds an absent key, read-modify-writes a present one, keeps on KEEP, deletes on null', async () => {
    const { adapter } = newAdapter()
    const key = 'telefunc:room:x:m:1'

    expect(await adapter.update!(key, (cur) => (cur === null ? '{"n":1}' : cur))).toBe('{"n":1}')
    expect(await adapter.update!(key, (cur) => JSON.stringify({ n: JSON.parse(cur!).n + 1 }))).toBe('{"n":2}')
    expect(await adapter.update!(key, () => KV_KEEP)).toBe('{"n":2}')
    expect(await adapter.get(key)).toBe('{"n":2}')
    expect(await adapter.update!(key, () => null)).toBe(null)
    expect(await adapter.get(key)).toBe(null)
  })

  it('update retries when the key changed between its read and its write, re-running the mutator', async () => {
    const { fake, adapter } = newAdapter()
    const key = 'telefunc:room:x:c'
    await adapter.set(key, '0')

    let calls = 0
    const result = await adapter.update!(key, (cur) => {
      calls++
      // On the first pass, a concurrent writer lands between this mutator's read and its CAS,
      // so the CAS mismatches and the mutator re-runs on the fresh value.
      if (calls === 1) fake.store.set('tf:kv:telefunc:room:x:c', '9')
      return String(Number(cur) + 1)
    })

    expect(calls).toBe(2) // first CAS saw '0' but the store is '9' → retry
    expect(result).toBe('10') // mutator re-ran on the winner's '9'
    expect(await adapter.get(key)).toBe('10')
  })
})

describe('Room over the Redis transport', () => {
  // End-to-end: install the transport globally (per-isolate — safe, vitest isolates spec files)
  // and run the full room lifecycle so KV and pub/sub delegation are exercised together.
  it('runs the full room lifecycle — create, join, publish, kick', async () => {
    const fake = new FakeIoredis()
    config.broadcast.transport = new RedisTransport({ redis: fake as unknown as Redis })

    const lobby = await Room.create('lobby', { meta: { topic: 'redis' } })
    const observer = await Room.get('lobby')
    const log: string[] = []
    observer.onJoin((m) => log.push(`join:${m.meta.name}`))
    observer.onLeave((m) => log.push(`leave:${m.id}`))
    observer.subscribe((data, _info, from) => log.push(`msg:${from.meta.name}:${data}`))

    const me = await lobby.join({ meta: { name: 'Alice' } })
    await me.publish('hello')
    await Room.removeParticipant('lobby', { id: me.id })

    expect(log).toEqual([`join:Alice`, `msg:Alice:hello`, `leave:${me.id}`])
    expect(observer.count).toBe(0)
    expect(await Room.list()).toMatchObject([{ id: 'lobby', meta: { topic: 'redis' }, count: 0 }])
  })
})
