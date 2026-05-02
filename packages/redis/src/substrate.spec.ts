import { afterEach, describe, expect, it, vi } from 'vitest'
import { ENVELOPE_KIND, PROXY_DIRECTION, type ProxyEnvelope } from 'telefunc/__internal'
import { RedisChannelSubstrate } from './substrate.js'

// Fake Redis emulating the surface the substrate uses (SET/GET/DEL/EXPIRE, XADD/XREAD,
// `defineCommand`). `duplicate()` returns the same instance so the cross-instance flow
// (forward → listen on another instance) is exercised end to end.

type StreamEntry = { id: string; fields: Uint8Array[] }

const utf8Encode = (s: string): Uint8Array => new TextEncoder().encode(s)

class FakeRedis {
  private readonly kv = new Map<string, string>()
  private readonly ttls = new Map<string, number>()
  private readonly streams = new Map<string, StreamEntry[]>()
  private readonly waiters = new Map<string, Array<(entry: StreamEntry) => void>>()
  private idSeq = 0

  /** ioredis exposes `duplicate()` for separate connections; the fake shares state. */
  duplicate(): this {
    return this
  }

  /** Called by the substrate's dispose path to interrupt the blocking XREAD. */
  disconnect(): void {
    for (const [, list] of this.waiters) {
      while (list.length > 0) list.shift()
    }
  }

  /** Emulate ioredis's `defineCommand`. The atomic registration Lua is the only script the
   *  substrate uses — emulate its effect directly: SET pin + XADD fanout entry. */
  defineCommand(name: string, _def: { numberOfKeys: number; lua: string }): void {
    if (name !== 'tfRegisterChannel') return
    // Args mirror the Lua: KEYS = (pinKey, fanoutKey); ARGV = (channelId, home, ttl, maxLen).
    const fn = async (
      pinKey: string,
      fanoutKey: string,
      channelId: string,
      home: string,
      ttlStr: string,
      maxLenStr: string,
    ): Promise<unknown> => {
      const ttl = parseInt(ttlStr, 10)
      const maxLen = parseInt(maxLenStr, 10)
      await this.set(pinKey, home, 'EX', ttl)
      // Two-field XADD: c=channelId, h=home.
      await this.xaddPair(fanoutKey, maxLen, 'c', utf8Encode(channelId), 'h', utf8Encode(home))
      return 1
    }
    ;(this as unknown as Record<string, unknown>)[name] = fn
  }

  /** Two-field XADD helper used by the registration Lua. The single-field `xadd` mock above
   *  is kept for the existing inbox path. */
  private xaddPair(
    key: string,
    maxLen: number,
    f1: string,
    v1: Uint8Array,
    f2: string,
    v2: Uint8Array,
  ): Promise<unknown> {
    const id = `${Date.now()}-${++this.idSeq}`
    const entry: StreamEntry = { id, fields: [utf8Encode(f1), v1, utf8Encode(f2), v2] }
    const stream = this.streams.get(key) ?? []
    stream.push(entry)
    while (stream.length > maxLen) stream.shift()
    this.streams.set(key, stream)
    const w = this.waiters.get(key)
    if (w && w.length > 0) {
      const waiter = w.shift()!
      waiter(entry)
    }
    return Promise.resolve(id)
  }

  set = vi.fn(async (key: string, value: string, _ex: 'EX', seconds: number): Promise<unknown> => {
    this.kv.set(key, value)
    this.ttls.set(key, Date.now() + seconds * 1000)
    return 'OK'
  })

  get = vi.fn(async (key: string): Promise<string | null> => {
    const expiresAt = this.ttls.get(key)
    if (expiresAt !== undefined && expiresAt < Date.now()) {
      this.kv.delete(key)
      this.ttls.delete(key)
      return null
    }
    return this.kv.get(key) ?? null
  })

  del = vi.fn(async (...keys: string[]): Promise<unknown> => {
    for (const key of keys) {
      this.kv.delete(key)
      this.ttls.delete(key)
      this.streams.delete(key)
    }
    return keys.length
  })

  expire = vi.fn(async (key: string, seconds: number): Promise<unknown> => {
    if (!this.kv.has(key) && !this.streams.has(key)) return 0
    this.ttls.set(key, Date.now() + seconds * 1000)
    return 1
  })

  /** ioredis pipeline mock — queues commands and dispatches them through the same
   *  spied methods on `exec()`. Sufficient for our usage (`expire` only). */
  pipeline = vi.fn((): { expire: (key: string, seconds: number) => unknown; exec: () => Promise<unknown[]> } => {
    const queued: Array<() => Promise<unknown>> = []
    const p = {
      expire: (key: string, seconds: number) => {
        queued.push(() => this.expire(key, seconds))
        return p
      },
      exec: async () => Promise.all(queued.map((fn) => fn())),
    }
    return p
  })

  xadd = vi.fn(
    async (
      key: string,
      _maxlen: 'MAXLEN',
      _approx: '~',
      maxLen: number,
      _id: '*',
      field: string,
      value: Uint8Array,
    ): Promise<unknown> => {
      const id = `${Date.now()}-${++this.idSeq}`
      const entry: StreamEntry = { id, fields: [utf8Encode(field), value] }
      const stream = this.streams.get(key) ?? []
      stream.push(entry)
      while (stream.length > maxLen) stream.shift()
      this.streams.set(key, stream)
      const w = this.waiters.get(key)
      if (w && w.length > 0) {
        const waiter = w.shift()!
        waiter(entry)
      }
      return id
    },
  )

  xreadBuffer = vi.fn(
    async (
      _count: 'COUNT',
      _countN: number,
      _block: 'BLOCK',
      _blockMs: number,
      _streams: 'STREAMS',
      key: string,
      id: string,
    ): Promise<Array<[Uint8Array, Array<[Uint8Array, Uint8Array[]]>]> | null> => {
      const stream = this.streams.get(key) ?? []
      const lastId = id === '$' ? this.peekLatestId(key) : id
      const next = stream.find((e) => e.id > lastId)
      if (next) {
        return [[utf8Encode(key), [[utf8Encode(next.id), next.fields]]]]
      }
      return new Promise((resolve) => {
        const waiters = this.waiters.get(key) ?? []
        waiters.push((entry) => resolve([[utf8Encode(key), [[utf8Encode(entry.id), entry.fields]]]]))
        this.waiters.set(key, waiters)
      })
    },
  )

  /** XREAD in string mode — used by the substrate's fanout consumer. Mirrors `xreadBuffer`
   *  but returns string ids/fields. */
  xread = vi.fn(
    async (
      _count: 'COUNT',
      _countN: number,
      _block: 'BLOCK',
      _blockMs: number,
      _streams: 'STREAMS',
      key: string,
      id: string,
    ): Promise<Array<[string, Array<[string, string[]]>]> | null> => {
      const stream = this.streams.get(key) ?? []
      const lastId = id === '$' ? this.peekLatestId(key) : id
      const next = stream.find((e) => e.id > lastId)
      if (next) {
        return [[key, [[next.id, decodeFields(next.fields)]]]]
      }
      return new Promise((resolve) => {
        const waiters = this.waiters.get(key) ?? []
        waiters.push((entry) => resolve([[key, [[entry.id, decodeFields(entry.fields)]]]]))
        this.waiters.set(key, waiters)
      })
    },
  )

  private peekLatestId(key: string): string {
    const stream = this.streams.get(key) ?? []
    return stream.length === 0 ? '0-0' : stream[stream.length - 1]!.id
  }
}

const utf8Decode = new TextDecoder('utf-8')
function decodeFields(fields: Uint8Array[]): string[] {
  return fields.map((f) => utf8Decode.decode(f))
}

// ───────────────────────────────────────────────────────────────────────────
// Spec
// ───────────────────────────────────────────────────────────────────────────

const liveSubstrates: RedisChannelSubstrate[] = []
afterEach(async () => {
  for (const s of liveSubstrates.splice(0)) await s.dispose()
})

function createSubstrate(redis: FakeRedis, instanceId: string): RedisChannelSubstrate {
  const substrate = new RedisChannelSubstrate({
    redis: redis as unknown as import('ioredis').Redis,
    instanceId,
    pinTtlSeconds: 60,
    inboxMaxLen: 100,
  })
  liveSubstrates.push(substrate)
  return substrate
}

describe('RedisChannelSubstrate — pin / locate', () => {
  it("locateRemoteHome filters self pins — same-instance is the runtime's job", async () => {
    const redis = new FakeRedis()
    const substrate = createSubstrate(redis, 'instance-A')

    await substrate.pinChannel('room:foo')

    expect(await substrate.locateRemoteHome('room:foo', 50)).toBe(null)
  })

  it('a different instance locates a channel pinned elsewhere', async () => {
    const redis = new FakeRedis()
    const home = createSubstrate(redis, 'instance-home')
    const other = createSubstrate(redis, 'instance-other')

    await home.pinChannel('room:cross')

    expect(await other.locateRemoteHome('room:cross', 5_000)).toBe('instance-home')
  })

  it('locateRemoteHome times out when only self is pinned', async () => {
    const redis = new FakeRedis()
    const substrate = createSubstrate(redis, 'instance-A')
    const waiter = substrate.locateRemoteHome('room:deferred', 50)
    await substrate.pinChannel('room:deferred')
    expect(await waiter).toBe(null)
  })

  it('refreshPins extends every pin TTL in a single batched round-trip', async () => {
    const redis = new FakeRedis()
    const substrate = createSubstrate(redis, 'instance-A')
    await substrate.pinChannel('room:1')
    await substrate.pinChannel('room:2')
    await substrate.pinChannel('room:3')
    redis.expire.mockClear()
    redis.pipeline.mockClear()

    await substrate.refreshChannels(['room:1', 'room:2', 'room:3'])

    // One pipeline → one round-trip. Three EXPIRE commands queued onto it.
    expect(redis.pipeline).toHaveBeenCalledTimes(1)
    expect(redis.expire).toHaveBeenCalledTimes(3)
    expect(redis.expire).toHaveBeenNthCalledWith(1, expect.stringContaining('room:1'), 60)
    expect(redis.expire).toHaveBeenNthCalledWith(2, expect.stringContaining('room:2'), 60)
    expect(redis.expire).toHaveBeenNthCalledWith(3, expect.stringContaining('room:3'), 60)
  })
})

describe('RedisChannelSubstrate — envelope routing', () => {
  it('forwards envelopes from one instance to the inbox of another, decoded by the destination listener', async () => {
    const redis = new FakeRedis()
    const proxy = createSubstrate(redis, 'instance-proxy')
    const home = createSubstrate(redis, 'instance-home')

    const received: ProxyEnvelope[] = []
    home.listen({ onAttach: (env) => received.push(env) })
    // Yield once so `home` has issued its first XREAD and is blocking.
    await new Promise((r) => setTimeout(r, 0))

    const sent: ProxyEnvelope = {
      channelId: 'room:cross',
      fromInstance: 'instance-proxy',
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.ATTACH, reconnectTimeout: 5000, ix: 1, lastSeq: 0 },
    }
    await proxy.forward('instance-home', sent)

    // Allow the consumer loop to deliver and the dispatch microtask to run.
    await new Promise((r) => setTimeout(r, 0))
    await new Promise((r) => setTimeout(r, 0))

    expect(received).toEqual([sent])
  })

  it('preserves binary frame bytes — including embedded newlines — through the substrate', async () => {
    const redis = new FakeRedis()
    const proxy = createSubstrate(redis, 'instance-proxy')
    const home = createSubstrate(redis, 'instance-home')

    const received: ProxyEnvelope[] = []
    home.listen({ onPeerFrame: (env) => received.push(env) })
    await new Promise((r) => setTimeout(r, 0))

    const frame = new Uint8Array([0x01, 0x0a, 0x02, 0x0a, 0x0a, 0xff])
    await proxy.forward('instance-home', {
      channelId: 'room:bin',
      fromInstance: 'instance-proxy',
      direction: PROXY_DIRECTION.TO_PEER,
      payload: { kind: ENVELOPE_KIND.FRAME, frame },
    })

    await new Promise((r) => setTimeout(r, 0))
    await new Promise((r) => setTimeout(r, 0))

    expect(received).toHaveLength(1)
    expect(received[0]!.payload.kind).toBe(ENVELOPE_KIND.FRAME)
    if (received[0]!.payload.kind !== ENVELOPE_KIND.FRAME) throw new Error('Expected frame payload')
    expect(Array.from(received[0]!.payload.frame)).toEqual(Array.from(frame))
  })
})
