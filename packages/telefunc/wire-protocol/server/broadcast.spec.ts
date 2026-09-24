import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Broadcast, ServerBroadcast } from './server-broadcast.js'
import { ServerChannel } from './channel.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { ACK_STATUS, ProtocolViolationError, TAG, decode, encode, type DecodedFrame } from '../shared-ws.js'
import { ChannelMux, type ServerTransport } from './mux.js'
import { IndexedPeer } from './IndexedPeer.js'
import { disposeBackend, installBackend } from '../backend/install.js'
import { MemoryBackend, MemoryBackendState } from '../backend/memory/backend.js'
import type { SubscriptionAttempt, SubscriptionState } from '../backend/subscription.js'
import { ChannelClosedError, ChannelOverflowError } from '../channel-errors.js'
import { BROADCAST_ESTABLISH_HOLD_MS, CHANNEL_BUFFER_LIMIT_BINARY_BYTES } from '../constants.js'
import { Abort } from '../../shared/Abort.js'
import { config } from '../../node/server/serverConfig.js'

let memoryState: MemoryBackendState
beforeEach(async () => {
  await disposeBackend()
  memoryState = new MemoryBackendState()
  installBackend(() => new MemoryBackend({ state: memoryState }))
})
afterEach(async () => {
  await disposeBackend()
  vi.restoreAllMocks()
})

function pendingSubscription() {
  let state: SubscriptionState = 'establishing'
  const listeners = new Set<(state: SubscriptionState) => void>()
  const transition = (next: SubscriptionState) => {
    state = next
    for (const listener of listeners) listener(next)
  }
  return {
    subscription: {
      state: () => state,
      onStateChange: (listener) => {
        listeners.add(listener)
        return () => listeners.delete(listener)
      },
      unsubscribe: async () => transition('closed'),
    } satisfies SubscriptionAttempt,
    ready: () => transition('ready'),
    lost: () => transition('lost'),
    close: () => transition('closed'),
  }
}

async function installPendingSubscriptionBackend(result: { seq: number; timestamp: number; receivers?: number }) {
  await disposeBackend()
  const controlled = pendingSubscription()
  const driver = new MemoryBackend({ state: memoryState })
  const bind = driver.subscriptions.bind.bind(driver.subscriptions)
  driver.subscriptions.bind = (source) => ({ ...bind(source), open: () => controlled.subscription })
  const publish = vi.spyOn(driver, 'publish').mockReturnValue(result)
  installBackend(() => driver)
  return { controlled, publish }
}

function registeredBroadcast<T = unknown>(key: string): ServerBroadcast<T> {
  const broadcast = new ServerBroadcast<T>({ key })
  broadcast._registerChannel()
  return broadcast
}

function peer(send: (frame: Uint8Array) => void): IndexedPeer {
  return new IndexedPeer({ send }, 7, new ReplayBuffer(1024 * 1024, 60_000, 2 * 1024 * 1024))
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
  it('isolates and reports thrown or rejected subscriber errors without breaking others', async () => {
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const broadcast = registeredBroadcast<{ text: string }>('room:err')
    const goodReceived: string[] = []
    broadcast.subscribe(() => Promise.reject(new Error('bad subscriber')))
    broadcast.subscribe((m) => goodReceived.push(m.text))
    broadcast.subscribe(() => {
      throw new Error('also bad')
    })

    broadcast.subscribeBinary(() => Promise.reject(new Error('bad binary subscriber')))
    broadcast.publish({ text: 'hi' })

    broadcast.publishBinary(new Uint8Array())
    expect(goodReceived).toEqual(['hi'])
    await vi.waitFor(() => expect(report).toHaveBeenCalledTimes(3))
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

  it.each([
    ['as it opens', true],
    ['after it opened', false],
  ])(
    'reports a subscription that ends on its own %s, and the next subscribe opens a fresh one',
    async (_when, atOpen) => {
      await disposeBackend()
      const ending = pendingSubscription()
      const driver = new MemoryBackend({ state: memoryState })
      const bind = driver.subscriptions.bind.bind(driver.subscriptions)
      let opens = 0
      driver.subscriptions.bind = (source) => {
        const binding = bind(source)
        return {
          ...binding,
          open: (...args) => {
            if (opens++ > 0) return binding.open(...args)
            if (atOpen) throw new Error('listen refused')
            return ending.subscription
          },
        }
      }
      installBackend(() => driver)
      const report = vi.spyOn(console, 'error').mockImplementation(() => {})
      const receiver = new ServerBroadcast<string>({ key: 'broadcast:ended' })
      const received: string[] = []
      receiver.subscribe(() => {})
      if (!atOpen) ending.close()
      await vi.waitFor(() =>
        expect(report).toHaveBeenCalledWith(expect.stringContaining(atOpen ? 'listen refused' : 'subscription closed')),
      )
      receiver.subscribe((text) => received.push(text))
      await Broadcast.publish('broadcast:ended', 'after')
      expect(received).toEqual(['after'])
      receiver.abort()
    },
  )

  it('waits for a sibling subscription to be ready before publishing', async () => {
    const { controlled, publish } = await installPendingSubscriptionBackend({ seq: 1, timestamp: 1 })
    const sender = new ServerBroadcast({ key: 'broadcast:sibling-ready' })
    const receiver = new ServerBroadcast({ key: 'broadcast:sibling-ready' })
    try {
      receiver.subscribe(() => {})
      const publishing = sender.publish('after-ready')
      await Promise.resolve()
      expect(publish).not.toHaveBeenCalled()
      controlled.ready()
      controlled.lost()
      await publishing
      expect(publish).toHaveBeenCalledOnce()
      // Only the establishment gates: a later loss holds nothing.
      await sender.publish('while-lost')
      expect(publish).toHaveBeenCalledTimes(2)
    } finally {
      sender.abort()
      receiver.abort()
    }
  })

  it('holds a publish for a subscription that never establishes only until the hold ends', async () => {
    vi.useFakeTimers()
    try {
      const { publish } = await installPendingSubscriptionBackend({ seq: 1, timestamp: 1 })
      new ServerBroadcast({ key: 'broadcast:never-ready' }).subscribe(() => {})
      const publishing = new ServerBroadcast({ key: 'broadcast:never-ready' }).publish('held')
      await vi.advanceTimersByTimeAsync(BROADCAST_ESTABLISH_HOLD_MS - 1)
      expect(publish).not.toHaveBeenCalled()
      await vi.advanceTimersByTimeAsync(1)
      await expect(publishing).resolves.toMatchObject({ seq: 1 })
    } finally {
      vi.useRealTimers()
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
    expect(a1.receivers).toBe(0)
    expect(a1.meta).toEqual({ transport: 'in-memory' })
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Binary path — publishBinary/subscribeBinary roundtrip with high-bit bytes.
// Catches accidental string-coercion or UTF-8 transcoding of binary frames.
// ───────────────────────────────────────────────────────────────────────────

describe('binary in-process broadcast', () => {
  it('waits for an establishing binary subscription before publishing on that lane', async () => {
    const { controlled: pending, publish } = await installPendingSubscriptionBackend({
      seq: 1,
      timestamp: 1,
      receivers: 1,
    })
    const broadcast = registeredBroadcast('room:bin-ready')
    broadcast._onPeerBroadcastSubscribe(true)
    const publishing = broadcast.publishBinary(new Uint8Array([1, 2, 3]))
    await Promise.resolve()
    expect(publish).not.toHaveBeenCalled()
    pending.ready()
    await expect(publishing).resolves.toMatchObject({ seq: 1, timestamp: 1, receivers: 1 })
    expect(publish).toHaveBeenCalledOnce()
  })

  it('caps binary payload bytes held while a subscription is establishing at the binary buffer limit', async () => {
    const { controlled: pending, publish } = await installPendingSubscriptionBackend({ seq: 1, timestamp: 1 })
    const receiver = new ServerBroadcast({ key: 'broadcast:bounded-ready' })
    const sender = new ServerBroadcast({ key: 'broadcast:bounded-ready' })
    receiver.subscribeBinary(() => {})
    const payload = new Uint8Array(CHANNEL_BUFFER_LIMIT_BINARY_BYTES)
    payload[0] = 7
    const first = sender.publishBinary(payload)
    payload[0] = 9
    const overflow = sender.publishBinary(new Uint8Array(1))
    try {
      pending.ready()
      await expect(overflow).rejects.toBeInstanceOf(ChannelOverflowError)
    } finally {
      await Promise.allSettled([first, overflow])
      expect(publish.mock.calls[0]?.[1][0]).toBe(7)
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
    const sender = registeredBroadcast(key)
    const receiver = registeredBroadcast(key)
    receiver._onPeerBroadcastSubscribe(true)
    const frames: Uint8Array[] = []
    receiver._attachPeer(peer((frame) => frames.push(frame)))
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

describe('publish frames outside a broadcast', () => {
  it('treats a publish on a plain channel as a protocol violation instead of never answering it', () => {
    const channel = new ServerChannel()
    for (const frame of [
      { tag: TAG.PUBLISH_ACK_REQ, index: 1, seq: 1, text: '"hi"' },
      { tag: TAG.PUBLISH_BINARY_ACK_REQ, index: 1, seq: 2, data: new Uint8Array([1]) },
    ] as const)
      expect(() => channel._dispatchFrame(frame)).toThrow(ProtocolViolationError)
  })
})

describe('Broadcast lifecycle and route ownership', () => {
  it('keeps a local route after peer unsubscribe and releases it with the final local listener', async () => {
    const key = 'broadcast:mixed-owners'
    const broadcast = new ServerBroadcast<string>({ key })
    const received: string[] = []
    const unsubscribe = broadcast.subscribe((message) => received.push(message))
    broadcast._onPeerBroadcastSubscribe(false)
    broadcast._onPeerBroadcastUnsubscribe(false)
    expect((await Broadcast.publish(key, 'kept')).receivers).toBe(1)
    expect(received).toEqual(['kept'])
    unsubscribe()
    expect((await Broadcast.publish(key, 'released')).receivers).toBe(0)
  })

  it('absorbs a client subscribe that crosses our close on the wire', async () => {
    const key = 'broadcast:sub-crosses-close'
    const broadcast = registeredBroadcast(key)
    broadcast._attachPeer(peer(() => {}))
    void broadcast.close()
    expect(() => broadcast._dispatchFrame({ tag: TAG.BROADCAST_SUB, index: 7, binary: false })).not.toThrow()
    expect((await Broadcast.publish(key, 'after-close')).receivers).toBe(0)
  })

  it("delivers onOpen's publish to the client whose open declared its subscription", async () => {
    const mux = new ChannelMux()
    const connection = {}
    const sent: DecodedFrame[] = []
    let sessionId: string | undefined
    const transport: ServerTransport<object> = {
      getSessionId: () => sessionId,
      setSessionId: (_connection, id) => (sessionId = id),
      getConnId: () => null,
      sendNow: (_connection, frame) => sent.push(decode(frame)),
      terminateConnection: () => {},
    }
    mux.onConnectionOpen(connection, transport)
    const chat = new ServerBroadcast<string>({ key: 'broadcast:joined-on-open' })
    chat.onOpen(() => void chat.publish('joined'))
    mux.registerChannel(chat)
    const entry = { id: chat.id, ix: 0, lastSeq: 0, initial: true, broadcast: { text: true, binary: false } } as const
    await mux.onConnectionRawMessage(connection, encode.reconcile({ open: [entry] }))
    await vi.waitFor(() =>
      expect(sent.some((frame) => frame.tag === TAG.PUBLISH && frame.text === '"joined"')).toBe(true),
    )
  })

  it('restores a subscription whose BROADCAST_SUB died with the previous transport from the reconnect entry', async () => {
    const mux = new ChannelMux()
    const sent: DecodedFrame[] = []
    const sessions = new Map<object, string>()
    const transport: ServerTransport<object> = {
      getSessionId: (connection) => sessions.get(connection),
      setSessionId: (connection, id) => void sessions.set(connection, id),
      getConnId: () => null,
      sendNow: (_connection, frame) => sent.push(decode(frame)),
      terminateConnection: () => {},
    }
    const key = 'broadcast:reconnect-entry'
    const chat = new ServerBroadcast<string>({ key })
    mux.registerChannel(chat)
    const first = {}
    mux.onConnectionOpen(first, transport)
    const entry = { id: chat.id, ix: 0, lastSeq: 0, broadcast: { text: false, binary: false } }
    await mux.onConnectionRawMessage(first, encode.reconcile({ open: [{ ...entry, initial: true }] }))
    mux.onConnectionClosed(first, { permanent: false })

    const second = {}
    mux.onConnectionOpen(second, transport)
    const reopened = { ...entry, broadcast: { text: true, binary: false } }
    await mux.onConnectionRawMessage(second, encode.reconcile({ sessionId: sessions.get(first), open: [reopened] }))
    await Broadcast.publish(key, 'after-reconnect')
    await vi.waitFor(() =>
      expect(sent.some((frame) => frame.tag === TAG.PUBLISH && frame.text === '"after-reconnect"')).toBe(true),
    )
  })

  it('forgets a broadcast route once its last subscriber leaves', async () => {
    const unsubscribe = Broadcast.subscribe('broadcast:route-released', () => {})
    expect(memoryState.broadcastSubs.size).toBe(1)
    unsubscribe()
    await vi.waitFor(() => expect(memoryState.broadcastSubs.size).toBe(0))
  })

  it('publishes from onClose to the key, as the documented chat pattern does', async () => {
    const key = 'broadcast:publish-on-close'
    const received: string[] = []
    const unsubscribe = Broadcast.subscribe<string>(key, (message) => received.push(message))
    const chat = registeredBroadcast<string>(key)
    chat.onClose(() => void chat.publish('left'))
    await chat.close({ timeout: 0 })
    await vi.waitFor(() => expect(received).toEqual(['left']))
    unsubscribe()
  })

  it.each([
    ['subscribe', (broadcast: ServerBroadcast) => broadcast.subscribe(() => {})],
    ['subscribeBinary', (broadcast: ServerBroadcast) => broadcast.subscribeBinary(() => {})],
  ])('%s() throws after abort', (_name, operation) => {
    const broadcast = new ServerBroadcast({ key: 'broadcast:closed' })
    broadcast.abort()
    expect(() => operation(broadcast)).toThrow(ChannelClosedError)
  })
})

describe('Broadcast client publish acks', () => {
  it.each([
    ['accepted acks OK', 'ok', ACK_STATUS.OK],
    ['refused by a full buffer acks OVERFLOW, reporting no bug', 'overflow', ACK_STATUS.OVERFLOW],
    ['failing with a bug acks ERROR and reports it', 'bug', ACK_STATUS.ERROR],
  ] as const)('a client publish %s', async (_name, outcome, status) => {
    const { controlled, publish } = await installPendingSubscriptionBackend({ seq: 1, timestamp: 1 })
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const broadcast = registeredBroadcast('broadcast:client-publish')
    const frames: Uint8Array[] = []
    broadcast._attachPeer(peer((frame) => frames.push(frame)))
    if (outcome === 'overflow') broadcast.subscribe(() => {})
    if (outcome === 'bug')
      publish.mockImplementation(() => {
        throw new Error('driver bug')
      })
    await broadcast._onPeerPublishAckReqMessage(
      JSON.stringify(outcome === 'overflow' ? 'x'.repeat(600 * 1024) : 'x'),
      1,
    )
    const ack = frames.map((f) => decode(f as Uint8Array<ArrayBuffer>)).find((d) => d.tag === TAG.ACK_RES)
    if (ack?.tag !== TAG.ACK_RES) throw new Error('Expected ACK_RES')
    expect(ack.status).toBe(status)
    expect(report).toHaveBeenCalledTimes(outcome === 'bug' ? 1 : 0)
    controlled.ready()
  })
})

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
// Static bus (`Broadcast.*`): server-only fire-and-forget broadcast. Bypasses
// the instance-lifecycle (no register, no peer) and goes straight to the adapter.
// Bug class: regression where the static bus starts touching instance state.
// ───────────────────────────────────────────────────────────────────────────

describe('Broadcast static bus (publish/subscribe)', () => {
  it('releases a queued publish when its establishing subscriber terminates, and reports the end', async () => {
    const { controlled, publish } = await installPendingSubscriptionBackend({ seq: 1, timestamp: 1 })
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const unsubscribe = Broadcast.subscribe('broadcast:terminal-ready', () => {})
    try {
      const publishing = Broadcast.publish('broadcast:terminal-ready', 'after-terminal')
      controlled.close()
      await expect(publishing).resolves.toMatchObject({ seq: 1 })
      expect(publish).toHaveBeenCalledOnce()
      expect(report).toHaveBeenCalledWith(expect.stringContaining('Backend subscription closed'))
    } finally {
      unsubscribe()
    }
  })

  it('waits for a static subscription to be ready before publishing', async () => {
    const { controlled: pending, publish } = await installPendingSubscriptionBackend({ seq: 1, timestamp: 1 })
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const unsubscribe = Broadcast.subscribe('broadcast:static-ready', () => {})
    try {
      const publishing = Broadcast.publish('broadcast:static-ready', 'after-ready')
      await Promise.resolve()
      expect(publish).not.toHaveBeenCalled()
      pending.ready()
      await publishing
      expect(publish).toHaveBeenCalledOnce()
      pending.close()
      expect(await Broadcast.publish('broadcast:static-ready', 'after-terminal')).toBeDefined()
      expect(publish).toHaveBeenCalledTimes(2)
      expect(report).toHaveBeenCalledWith(expect.stringContaining('Backend subscription closed'))
    } finally {
      unsubscribe()
    }
  })

  it('holds up to config.channel.bufferLimit bytes while a subscription is establishing', async () => {
    const { controlled, publish } = await installPendingSubscriptionBackend({ seq: 1, timestamp: 1 })
    config.channel = { bufferLimit: 4 * 1024 * 1024 }
    try {
      new ServerBroadcast({ key: 'broadcast:configured-limit' }).subscribe(() => {})
      const publishing = Broadcast.publish('broadcast:configured-limit', 'x'.repeat(600 * 1024))
      controlled.ready()
      await expect(publishing).resolves.toMatchObject({ seq: 1 })
      expect(publish).toHaveBeenCalledOnce()
    } finally {
      config.channel = {}
    }
  })

  it('static publish + static subscribe deliver without any instance', async () => {
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const received: Array<{ text: string }> = []
    const unsubscribe = Broadcast.subscribe<{ text: string }>('room:static', (msg) => received.push(msg))

    const unsubscribeBug = Broadcast.subscribe('room:static', () => Promise.reject(new Error('static text bug')))
    const unsubscribeAbort = Broadcast.subscribe('room:static', () => Promise.reject(Abort('expected')))
    const unsubscribeClosed = Broadcast.subscribe('room:static', () => {
      throw new ChannelClosedError()
    })
    await Broadcast.publish('room:static', { text: 'fire-and-forget' })

    // Every error but the Abort, as for a channel listener.
    await vi.waitFor(() => expect(report).toHaveBeenCalledTimes(2))
    expect(received).toEqual([{ text: 'fire-and-forget' }])
    unsubscribe()
    unsubscribeBug()
    unsubscribeAbort()
    unsubscribeClosed()
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
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const unsubscribeBug = Broadcast.subscribeBinary('broadcast:shared-order', () =>
      Promise.reject(new Error('static binary bug')),
    )
    const unsubscribeAbort = Broadcast.subscribeBinary('broadcast:shared-order', () =>
      Promise.reject(Abort('expected')),
    )
    const text = await Broadcast.publish('broadcast:shared-order', { text: 'one' })
    const binary = await Broadcast.publishBinary('broadcast:shared-order', new Uint8Array([2]))
    await vi.waitFor(() => expect(report).toHaveBeenCalledOnce())
    expect(text.seq).toBe(1)
    expect(binary.seq).toBe(2)
    unsubscribeBug()
    unsubscribeAbort()
  })
})
