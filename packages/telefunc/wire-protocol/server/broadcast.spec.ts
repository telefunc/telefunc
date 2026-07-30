import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Broadcast, ServerBroadcast } from './server-broadcast.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { ACK_STATUS, TAG, decode } from '../shared-ws.js'
import { IndexedPeer } from './IndexedPeer.js'
import { disposeBackend, getBackend, installBackend } from '../backend/install.js'
import { MemoryBackend, MemoryBackendState } from '../backend/memory/backend.js'
import type { BackendSubscription, SubscriptionState } from '../backend/spi.js'

let memoryState: MemoryBackendState
beforeEach(async () => {
  await disposeBackend()
  memoryState = new MemoryBackendState()
  installBackend(() => new MemoryBackend({ state: memoryState }))
})
afterEach(() => disposeBackend())

function pendingSubscription(): {
  subscription: BackendSubscription
  ready(): void
} {
  let resolveReady!: () => void
  const ready = new Promise<void>((resolve) => {
    resolveReady = resolve
  })
  let state: SubscriptionState = 'establishing'
  return {
    subscription: {
      ready,
      state: () => state,
      onStateChange: () => () => {},
      unsubscribe: async () => {
        state = 'closed'
        resolveReady()
      },
    },
    ready() {
      state = 'ready'
      resolveReady()
    },
  }
}

// ───────────────────────────────────────────────────────────────────────────
// In-process delivery — bug classes targeted: cross-key bleed, dropped
// subscribers, delivery reordering, self-echo loss, late-register and
// late-attach buffer correctness, error isolation between subscribers.
// ───────────────────────────────────────────────────────────────────────────

describe('keyed in-process broadcast', () => {
  it('delivers a published message to a sibling broadcast on the same key', () => {
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:basic' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:basic' })
    sender._registerChannel()
    receiver._registerChannel()

    const received: Array<{ text: string }> = []
    receiver.subscribe((msg) => received.push(msg))
    sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  // The publisher's own subscribe must fire too — same instance is both pub and sub.
  // Catches a "skip-self" bug that excludes the source from delivery.
  it('delivers a published message to the source broadcast (self-echo)', () => {
    const broadcast = new ServerBroadcast<{ text: string }>({ key: 'room:self' })
    broadcast._registerChannel()

    const received: Array<{ text: string }> = []
    broadcast.subscribe((msg) => received.push(msg))
    broadcast.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  // Catches a key-mixup where the adapter routes by reference instead of by key,
  // or strips the key prefix and ends up with a single global topic.
  it('isolates messages by key — publishing on key A does not reach key B subscribers', () => {
    const a = new ServerBroadcast<{ from: string }>({ key: 'room:A' })
    const b = new ServerBroadcast<{ from: string }>({ key: 'room:B' })
    a._registerChannel()
    b._registerChannel()

    const receivedA: Array<{ from: string }> = []
    const receivedB: Array<{ from: string }> = []
    a.subscribe((m) => receivedA.push(m))
    b.subscribe((m) => receivedB.push(m))

    a.publish({ from: 'A' })
    b.publish({ from: 'B' })

    expect(receivedA).toEqual([{ from: 'A' }])
    expect(receivedB).toEqual([{ from: 'B' }])
  })

  // 3-subscriber fan-out catches a subscriber-set bug that delivers to only the first
  // (or last) registered listener.
  it('fans out to every subscriber on the key', () => {
    const k = 'room:fanout'
    const pub = new ServerBroadcast<{ n: number }>({ key: k })
    const subA = new ServerBroadcast<{ n: number }>({ key: k })
    const subB = new ServerBroadcast<{ n: number }>({ key: k })
    const subC = new ServerBroadcast<{ n: number }>({ key: k })
    pub._registerChannel()
    subA._registerChannel()
    subB._registerChannel()
    subC._registerChannel()

    const log: Array<[string, number]> = []
    subA.subscribe((m) => log.push(['A', m.n]))
    subB.subscribe((m) => log.push(['B', m.n]))
    subC.subscribe((m) => log.push(['C', m.n]))

    pub.publish({ n: 1 })

    expect(log.sort()).toEqual([
      ['A', 1],
      ['B', 1],
      ['C', 1],
    ])
  })

  // Catches a reordering bug introduced by an async adapter that races publishes
  // (e.g. swapping `await publish(a)` with `await publish(b)` in flight).
  it('preserves publish order across multiple in-flight messages', () => {
    const sender = new ServerBroadcast<{ n: number }>({ key: 'room:order' })
    const receiver = new ServerBroadcast<{ n: number }>({ key: 'room:order' })
    sender._registerChannel()
    receiver._registerChannel()

    const seen: number[] = []
    receiver.subscribe((m) => seen.push(m.n))
    for (let i = 0; i < 5; i++) sender.publish({ n: i })

    expect(seen).toEqual([0, 1, 2, 3, 4])
  })

  // Common defensive bug: a single throwing subscriber takes down the whole fan-out.
  // ServerBroadcast must isolate per-subscriber errors so one bad listener doesn't
  // starve the rest.
  it('isolates subscriber errors — a throwing subscriber does not break others', () => {
    const broadcast = new ServerBroadcast<{ text: string }>({ key: 'room:err' })
    broadcast._registerChannel()

    const goodReceived: string[] = []
    broadcast.subscribe(() => {
      throw new Error('bad subscriber')
    })
    broadcast.subscribe((m) => goodReceived.push(m.text))
    broadcast.subscribe(() => {
      throw new Error('also bad')
    })

    broadcast.publish({ text: 'hi' })

    expect(goodReceived).toEqual(['hi'])
  })

  // The unsubscribe handle returned by `subscribe()` is the only way for users to
  // detach a callback. A regression here causes silent listener leaks that look
  // like duplicate deliveries.
  it('subscribe() returns an unsubscribe that actually stops further delivery', () => {
    const broadcast = new ServerBroadcast<{ n: number }>({ key: 'room:unsub' })
    broadcast._registerChannel()

    const seen: number[] = []
    const unsubscribe = broadcast.subscribe((m) => seen.push(m.n))

    broadcast.publish({ n: 1 })
    unsubscribe()
    broadcast.publish({ n: 2 })

    expect(seen).toEqual([1])
  })

  // Edge case: a Broadcast can be created and have `publish` called on it BEFORE
  // any peer attaches. The behavioral contract: when the peer eventually attaches,
  // the previously-published message is delivered to it (not silently dropped).
  // Asserts at the count level — exact frame encoding is impl detail.
  it('buffers wire frames until a peer attaches, then flushes them on attach', () => {
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:late-attach' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:late-attach' })
    sender._registerChannel()
    receiver._registerChannel()
    receiver._onPeerBroadcastSubscribe(false) // simulate client subscribe over wire

    sender.publish({ text: 'hello' })

    const frames: Uint8Array[] = []
    receiver._attachPeer(
      new IndexedPeer(
        {
          send: (frame) => {
            frames.push(frame)
          },
        },
        7,
        new ReplayBuffer(1024 * 1024, 60_000, 2 * 1024 * 1024),
      ),
    )

    // One publish made before attach → exactly one frame replayed on attach.
    expect(frames.length).toBe(1)
  })

  it('buffers keyed publishes that arrive before a sibling has registered yet', () => {
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:late-register' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:late-register' })

    const received: Array<{ text: string }> = []
    receiver.subscribe((m) => received.push(m))
    // Note: no `_registerChannel()` calls here — exercises the "publish before register" path.

    sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  it('waits for a sibling subscription to be ready before publishing', async () => {
    const backend = getBackend()
    const controlled = pendingSubscription()
    const subscribe = vi.spyOn(backend, 'subscribe').mockReturnValue(controlled.subscription)
    const publish = vi.spyOn(backend, 'publish').mockReturnValue({ seq: 1, timestamp: 1 })
    const sender = new ServerBroadcast({ key: 'broadcast:sibling-ready' })
    const receiver = new ServerBroadcast({ key: 'broadcast:sibling-ready' })
    try {
      receiver.subscribe(() => {})
      const publishing = sender.publish('after-ready')
      await Promise.resolve()
      expect(publish).not.toHaveBeenCalled()

      controlled.ready()
      await publishing
      expect(publish).toHaveBeenCalledOnce()
    } finally {
      sender.abort()
      receiver.abort()
      subscribe.mockRestore()
      publish.mockRestore()
    }
  })

  it('publish receipts are key-scoped and seq increments monotonically per key', async () => {
    const k1 = 'room:receipts:A'
    const k2 = 'room:receipts:B'
    const a = new ServerBroadcast<{ text: string }>({ key: k1 })
    const b = new ServerBroadcast<{ text: string }>({ key: k2 })
    a._registerChannel()
    b._registerChannel()

    const a1 = await a.publish({ text: 'a-one' })
    const a2 = await a.publish({ text: 'a-two' })
    const b1 = await b.publish({ text: 'b-one' })

    // Each receipt is keyed to its own topic, and seq counts independently per key.
    expect(a1.key).toBe(k1)
    expect(a2.key).toBe(k1)
    expect(b1.key).toBe(k2)
    expect(a2.seq).toBe(a1.seq + 1)
    expect(b1.seq).toBe(a1.seq) // separate key → seq counter is independent
    expect(typeof a1.timestamp).toBe('number')
    expect(a1.meta).toEqual({ delivered: 0, transport: 'in-memory' })
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Binary path — publishBinary/subscribeBinary roundtrip with high-bit bytes.
// Catches accidental string-coercion or UTF-8 transcoding of binary frames.
// ───────────────────────────────────────────────────────────────────────────

describe('binary in-process broadcast', () => {
  it('waits for an establishing binary subscription before publishing on that lane', async () => {
    const backend = getBackend()
    const pending = pendingSubscription()
    const subscribe = vi.spyOn(backend, 'subscribe').mockReturnValue(pending.subscription)
    const publish = vi.spyOn(backend, 'publish').mockReturnValue({
      seq: 1,
      timestamp: 1,
      receivers: 1,
    })
    try {
      const broadcast = new ServerBroadcast({ key: 'room:bin-ready' })
      broadcast._registerChannel()
      broadcast._onPeerBroadcastSubscribe(true)

      const publishing = broadcast.publishBinary(new Uint8Array([1, 2, 3]))
      await Promise.resolve()
      expect(publish).not.toHaveBeenCalled()

      pending.ready()
      await expect(publishing).resolves.toMatchObject({ seq: 1, timestamp: 1, receivers: 1 })
      expect(publish).toHaveBeenCalledOnce()
    } finally {
      subscribe.mockRestore()
      publish.mockRestore()
    }
  })

  it('round-trips binary publishes preserving high-bit bytes', () => {
    const sender = new ServerBroadcast({ key: 'room:bin' })
    const receiver = new ServerBroadcast({ key: 'room:bin' })
    sender._registerChannel()
    receiver._registerChannel()

    const received: Uint8Array[] = []
    receiver.subscribeBinary((data) => received.push(data))
    sender.publishBinary(new Uint8Array([0x00, 0x7f, 0x80, 0xff]))

    expect(received).toHaveLength(1)
    expect(Array.from(received[0]!)).toEqual([0x00, 0x7f, 0x80, 0xff])
  })

  it('binary subscribers do NOT receive text publishes (and vice versa)', () => {
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:mixed' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:mixed' })
    sender._registerChannel()
    receiver._registerChannel()

    const text: Array<{ text: string }> = []
    const bin: Uint8Array[] = []
    receiver.subscribe((m) => text.push(m))
    receiver.subscribeBinary((d) => bin.push(d))

    sender.publish({ text: 'just text' })
    sender.publishBinary(new Uint8Array([1, 2, 3]))

    expect(text).toEqual([{ text: 'just text' }])
    expect(bin).toHaveLength(1)
    expect(Array.from(bin[0]!)).toEqual([1, 2, 3])
  })

  it('preserves a sequence wider than 32 bits through the generic public wire frame', async () => {
    const key = 'broadcast:wide-seq'
    memoryState.broadcastOrder.set(key, { seq: 0xffff_ffff, timestamp: 10 })
    const sender = new ServerBroadcast({ key })
    const receiver = new ServerBroadcast({ key })
    sender._registerChannel()
    receiver._registerChannel()
    receiver._onPeerBroadcastSubscribe(true)
    const frames: Uint8Array[] = []
    receiver._attachPeer(
      new IndexedPeer(
        { send: (frame) => frames.push(frame) },
        7,
        new ReplayBuffer(1024 * 1024, 60_000, 2 * 1024 * 1024),
      ),
    )

    const receipt = await sender.publishBinary(new Uint8Array([7]))
    const publish = frames
      .map((frame) => decode(frame as Uint8Array<ArrayBuffer>))
      .find((frame) => frame.tag === TAG.PUBLISH_BINARY)

    expect(receipt.seq).toBe(0x1_0000_0000)
    expect(publish?.tag).toBe(TAG.PUBLISH_BINARY)
    if (publish?.tag !== TAG.PUBLISH_BINARY) throw new Error('Expected binary publish frame')
    expect(publish.info.seq).toBe(0x1_0000_0000)
    expect(Array.from(publish.data)).toEqual([7])
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Channel-method gating — Broadcast extends ServerChannel but the inherited
// send/listen/sendBinary/listenBinary must throw at runtime since the type
// hides them. Catches a regression where a subclass forgets to override one.
// ───────────────────────────────────────────────────────────────────────────

describe('Broadcast disallows channel methods', () => {
  it.each([
    ['listen', (b: ServerBroadcast) => b.listen()],
    ['listenBinary', (b: ServerBroadcast) => b.listenBinary()],
    ['send', (b: ServerBroadcast) => b.send()],
    ['sendBinary', (b: ServerBroadcast) => b.sendBinary()],
  ])('calling %s() throws — not available on a Broadcast', (_name, call) => {
    const broadcast = new ServerBroadcast({ key: 'room:disallowed' })
    expect(() => call(broadcast)).toThrow()
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Shield — the runtime gate that protects the server from untyped client
// publishes. The shield is wired via `[TELEFUNC_SHIELDS]` on the type;
// the runtime check lives in _dispatchPublishAckReq.
// ───────────────────────────────────────────────────────────────────────────

describe('Broadcast shield validation', () => {
  it('rejects client publishes that fail the data shield with a SHIELD_ERROR ack', () => {
    const broadcast = new ServerBroadcast<{ text: string }>({ key: 'room:shield' })
    broadcast._validators.set('data', (value) => {
      const v = value as { text?: unknown }
      return typeof v?.text === 'string' ? true : 'expected { text: string }'
    })
    broadcast._registerChannel()

    const frames: Uint8Array[] = []
    broadcast._attachPeer(
      new IndexedPeer(
        {
          send: (frame) => {
            frames.push(frame)
          },
        },
        7,
        new ReplayBuffer(1024 * 1024, 60_000, 2 * 1024 * 1024),
      ),
    )

    void broadcast._onPeerPublishAckReqMessage(JSON.stringify({ text: 42 }), 1)

    const ack = frames.map((f) => decode(f as Uint8Array<ArrayBuffer>)).find((d) => d.tag === TAG.ACK_RES)
    expect(ack).toBeDefined()
    if (ack?.tag !== TAG.ACK_RES) throw new Error('Expected ACK_RES')
    expect(ack.status).toBe(ACK_STATUS.SHIELD_ERROR)
    expect(ack.text).toBe('expected { text: string }')
  })

  // Shield rejection MUST short-circuit the publish — bad payloads should never reach
  // any subscriber, including the publisher's own self-echo.
  it('a shield-rejected publish is not delivered to subscribers', () => {
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:shield-drop' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:shield-drop' })
    sender._registerChannel()
    receiver._registerChannel()

    sender._validators.set('data', () => 'always reject')

    const seen: Array<{ text: string }> = []
    receiver.subscribe((m) => seen.push(m))

    sender._attachPeer(new IndexedPeer({ send: () => {} }, 7, new ReplayBuffer(1024 * 1024, 60_000, 2 * 1024 * 1024)))
    void sender._onPeerPublishAckReqMessage(JSON.stringify({ text: 'malicious' }), 1)

    expect(seen).toEqual([])
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Static bus (`Broadcast.*`) — server-only fire-and-forget broadcast. Bypasses
// the instance-lifecycle (no register, no peer) and goes straight to the adapter.
// Bug class: regression where the static bus starts touching instance state.
// ───────────────────────────────────────────────────────────────────────────

describe('Broadcast static bus (publish/subscribe)', () => {
  it('waits for a static subscription to be ready before publishing', async () => {
    const backend = getBackend()
    const pending = pendingSubscription()
    const subscribe = vi.spyOn(backend, 'subscribe').mockReturnValue(pending.subscription)
    const publish = vi.spyOn(backend, 'publish').mockReturnValue({ seq: 1, timestamp: 1 })
    const unsubscribe = Broadcast.subscribe('broadcast:static-ready', () => {})
    try {
      const publishing = Broadcast.publish('broadcast:static-ready', 'after-ready')
      await Promise.resolve()
      expect(publish).not.toHaveBeenCalled()

      pending.ready()
      await publishing
      expect(publish).toHaveBeenCalledOnce()
    } finally {
      unsubscribe()
      subscribe.mockRestore()
      publish.mockRestore()
    }
  })

  it('static publish + static subscribe deliver without any instance', async () => {
    const received: Array<{ text: string }> = []
    const unsubscribe = Broadcast.subscribe<{ text: string }>('room:static', (msg) => received.push(msg))

    await Broadcast.publish('room:static', { text: 'fire-and-forget' })

    expect(received).toEqual([{ text: 'fire-and-forget' }])
    unsubscribe()
  })

  it('static unsubscribe stops further deliveries', async () => {
    const received: Array<{ text: string }> = []
    const unsubscribe = Broadcast.subscribe<{ text: string }>('room:static-unsub', (m) => received.push(m))

    await Broadcast.publish('room:static-unsub', { text: 'first' })
    unsubscribe()
    await Broadcast.publish('room:static-unsub', { text: 'second' })

    expect(received).toEqual([{ text: 'first' }])
  })

  it('shares one monotonic per-key sequence across text and binary routes', async () => {
    const text = await Broadcast.publish('broadcast:shared-order', { text: 'one' })
    const binary = await Broadcast.publishBinary('broadcast:shared-order', new Uint8Array([2]))

    expect(text.seq).toBe(1)
    expect(binary.seq).toBe(2)
  })
})
