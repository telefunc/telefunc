import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Broadcast, ServerBroadcast } from './server-broadcast.js'
import { ServerChannel } from './channel.js'
import { ReplayBuffer } from '../replay-buffer.js'
import {
  ACK_STATUS,
  TAG,
  decode,
  encode,
  encodePublishText,
  type BroadcastKind,
  type DecodedFrame,
} from '../shared-ws.js'
import { ChannelMux, type ServerTransport } from './mux.js'
import { IndexedPeer } from './IndexedPeer.js'
import { disposeBackend, installBackend } from '../backend/install.js'
import { MemoryBackend, MemoryBackendState } from '../backend/memory/backend.js'
import type { SubscriptionAttempt, SubscriptionState } from '../backend/subscription.js'
import { ChannelClosedError, ChannelOverflowError } from '../channel-errors.js'
import { ESTABLISH_HOLD_MS, CHANNEL_BUFFER_LIMIT_BINARY_BYTES } from '../constants.js'
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

/** A memory backend whose subscription attempts come from `open`, which may defer to the driver's own. */
async function installOpeningBackend(
  open: (
    source: Parameters<MemoryBackend['subscriptions']['bind']>[0],
    driverOpen: () => SubscriptionAttempt,
  ) => SubscriptionAttempt,
): Promise<MemoryBackend> {
  await disposeBackend()
  const driver = new MemoryBackend({ state: memoryState })
  const bind = driver.subscriptions.bind.bind(driver.subscriptions)
  driver.subscriptions.bind = (source) => {
    const binding = bind(source)
    return { ...binding, open: (...args) => open(source, () => binding.open(...args)) }
  }
  installBackend(() => driver)
  return driver
}

async function installPendingSubscriptionBackend(result: { seq: number; timestamp: number; receivers?: number }) {
  const controlled = pendingSubscription()
  const driver = await installOpeningBackend(() => controlled.subscription)
  const publish = vi.spyOn(driver, 'publish').mockReturnValue(result)
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

  // Catches a key-mixup where the backend routes by reference instead of by key,
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

  // Catches a reordering bug introduced by an async backend that races publishes
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

  it("applies a reattach's declarations to the new peer, not to one that never detached", () => {
    class Announcing extends ServerChannel {
      override _onPeerSubscription(_kind: BroadcastKind, on: boolean): void {
        if (on) this._sendPublish(encodePublishText('"declared"', { seq: 1, timestamp: 1 }))
      }
    }
    const channel = new Announcing()
    channel._registerChannel()
    const publishes = (frames: Uint8Array[]) =>
      frames.map((frame) => decode(frame as Uint8Array<ArrayBuffer>)).filter((frame) => frame.tag === TAG.PUBLISH)
    const previous: Uint8Array[] = []
    const next: Uint8Array[] = []
    channel._attachPeer(peer((frame) => previous.push(frame)))
    channel._attachPeer(
      peer((frame) => next.push(frame)),
      { broadcast: { text: true, binary: false } },
    )
    expect([publishes(previous).length, publishes(next).length]).toEqual([0, 1])
  })

  it('rejects a key that is not a well-formed string as a usage error', () => {
    const lone = 'room:\ud800'
    const usage = 'The broadcast key should be a well-formed string'
    expect(() => new ServerBroadcast({ key: lone })).toThrow(usage)
    expect(() => Broadcast.publish(lone, 'x')).toThrow(usage)
    expect(() => Broadcast.publishBinary(lone, new Uint8Array([1]))).toThrow(usage)
    expect(() => Broadcast.subscribeBinary(lone, () => {})).toThrow(usage)
  })

  it('a subscribe that throws leaves no listener behind', async () => {
    await disposeBackend()
    const driver = new MemoryBackend({ state: memoryState })
    installBackend(() => driver)
    const bind = driver.subscriptions.bind.bind(driver.subscriptions)
    vi.spyOn(driver.subscriptions, 'bind').mockImplementationOnce(() => {
      throw new Error('no session to deliver to')
    })
    const broadcast = registeredBroadcast<{ n: number }>('room:subscribe-throws')
    const seen: string[] = []
    expect(() => broadcast.subscribe((m) => seen.push(`first:${m.n}`))).toThrow('no session to deliver to')
    vi.mocked(driver.subscriptions.bind).mockImplementation(bind)
    broadcast.subscribe((m) => seen.push(`second:${m.n}`))
    await broadcast.publish({ n: 1 })
    expect(seen).toEqual(['second:1'])
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
    receiver._onPeerSubscription('text', true) // simulate client subscribe over wire

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
    ['a BroadcastChannel', 'as it opens', true],
    ['a BroadcastChannel', 'after it opened', false],
    ['Broadcast.subscribe()', 'as it opens', true],
    ['Broadcast.subscribe()', 'after it opened', false],
  ] as const)(
    '%s: a subscription that ends on its own %s is reported and replaced',
    async (subscriber, _when, atOpen) => {
      const ending = pendingSubscription()
      let opens = 0
      await installOpeningBackend((_source, driverOpen) => {
        if (opens++ > 0) return driverOpen()
        if (atOpen) throw new Error('listen refused')
        return ending.subscription
      })
      const report = vi.spyOn(console, 'error').mockImplementation(() => {})
      const received: string[] = []
      const onMessage = (text: string) => void received.push(text)
      const channel = new ServerBroadcast<string>({ key: 'broadcast:ended' })
      const unsubscribe =
        subscriber === 'a BroadcastChannel'
          ? channel.subscribe(onMessage)
          : Broadcast.subscribe<string>('broadcast:ended', onMessage)
      if (!atOpen) ending.close()
      await vi.waitFor(() => expect(opens).toBe(2))
      expect(report).toHaveBeenCalledWith(expect.stringContaining(atOpen ? 'listen refused' : 'subscription closed'))
      await Broadcast.publish('broadcast:ended', 'after')
      expect(received).toEqual(['after'])
      unsubscribe()
    },
  )

  it('replaces a subscription once per end: a replacement that ends before it was ready is dropped', async () => {
    const attempts = [pendingSubscription(), pendingSubscription(), pendingSubscription()]
    let opens = 0
    await installOpeningBackend((_source, driverOpen) => attempts[opens++]?.subscription ?? driverOpen())
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const unsubscribe = Broadcast.subscribe('broadcast:replaced-once', () => {})
    attempts[0]!.close()
    await vi.waitFor(() => expect(opens).toBe(2))
    attempts[1]!.ready()
    attempts[1]!.close()
    await vi.waitFor(() => expect(opens).toBe(3))
    attempts[2]!.close()
    await vi.waitFor(() => expect(report).toHaveBeenCalledTimes(3))
    expect(opens).toBe(3)
    unsubscribe()
  })

  it('opens no replacement for a subscription that ends after its route was released', async () => {
    const ending = pendingSubscription()
    let opens = 0
    await installOpeningBackend(() => {
      opens++
      return ending.subscription
    })
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const unsubscribe = Broadcast.subscribe('broadcast:released', () => {})
    ending.close()
    unsubscribe()
    await vi.waitFor(() => expect(report).toHaveBeenCalledOnce())
    expect(opens).toBe(1)
  })

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
      await vi.advanceTimersByTimeAsync(ESTABLISH_HOLD_MS - 1)
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
// Binary path — publishBinary/subscribeBinary gating and roundtrip with high-bit bytes.
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
    broadcast._onPeerSubscription('binary', true)
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
    receiver._onPeerSubscription('binary', true)
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

describe('Broadcast lifecycle and route ownership', () => {
  it('keeps a local route after peer unsubscribe and releases it with the final local listener', async () => {
    const key = 'broadcast:mixed-owners'
    const broadcast = new ServerBroadcast<string>({ key })
    const received: string[] = []
    const unsubscribe = broadcast.subscribe((message) => received.push(message))
    broadcast._onPeerSubscription('text', true)
    broadcast._onPeerSubscription('text', false)
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
    ['subscribe', (broadcast: ServerBroadcast) => broadcast.subscribe(() => {}), 'text'],
    ['subscribeBinary', (broadcast: ServerBroadcast) => broadcast.subscribeBinary(() => {}), 'binary'],
  ] as const)(
    '%s() after abort opens no route, and throws nothing, as on the client',
    async (_name, operation, kind) => {
      const broadcast = new ServerBroadcast({ key: 'broadcast:closed' })
      broadcast.abort()
      operation(broadcast)
      const receipt = await (kind === 'text'
        ? Broadcast.publish('broadcast:closed', 'after-abort')
        : Broadcast.publishBinary('broadcast:closed', new Uint8Array([1])))
      expect(receipt.receivers).toBe(0)
    },
  )
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
// Static bus (`Broadcast.*`): server-only fire-and-forget broadcast. Bypasses
// the instance-lifecycle (no register, no peer) and goes straight to the backend.
// Bug class: regression where the static bus starts touching instance state.
// ───────────────────────────────────────────────────────────────────────────

describe('Broadcast static bus (publish/subscribe)', () => {
  it('reports the end of a subscription its consumers share once', async () => {
    const ending = pendingSubscription()
    let opens = 0
    await installOpeningBackend((_source, driverOpen) => (opens++ === 0 ? ending.subscription : driverOpen()))
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const stops = [
      new ServerBroadcast({ key: 'broadcast:shared-end' }).subscribe(() => {}),
      new ServerBroadcast({ key: 'broadcast:shared-end' }).subscribe(() => {}),
      Broadcast.subscribe('broadcast:shared-end', () => {}),
    ]
    try {
      ending.close()
      await vi.waitFor(() => expect(opens).toBe(2))
      await new Promise((resolve) => setTimeout(resolve, 10))
      const ends = report.mock.calls.filter(([logged]) => String(logged).includes('Backend subscription closed'))
      expect(ends).toHaveLength(1)
    } finally {
      for (const stop of stops) stop()
    }
  })

  it('releases a queued publish once its key has no establishing subscription, and reports each end', async () => {
    const attempts: Array<ReturnType<typeof pendingSubscription>> = []
    const driver = await installOpeningBackend(() => {
      const attempt = pendingSubscription()
      attempts.push(attempt)
      return attempt.subscription
    })
    const publish = vi.spyOn(driver, 'publish').mockReturnValue({ seq: 1, timestamp: 1 })
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const unsubscribe = Broadcast.subscribe('broadcast:terminal-ready', () => {})
    try {
      const publishing = Broadcast.publish('broadcast:terminal-ready', 'after-terminal')
      attempts[0]!.close()
      // The ended subscription is replaced once, and the publish waits for the replacement too.
      await vi.waitFor(() => expect(attempts).toHaveLength(2))
      expect(publish).not.toHaveBeenCalled()
      attempts[1]!.close()
      await expect(publishing).resolves.toMatchObject({ seq: 1 })
      expect(publish).toHaveBeenCalledOnce()
      const ends = report.mock.calls.filter(([logged]) => String(logged).includes('Backend subscription closed'))
      expect(ends).toHaveLength(2)
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

  it("keeps a key's text and binary publishes in call order while one kind's subscription establishes", async () => {
    const controlled = pendingSubscription()
    const driver = await installOpeningBackend((source, driverOpen) =>
      'kind' in source && source.kind === 'text' ? controlled.subscription : driverOpen(),
    )
    const publish = vi.spyOn(driver, 'publish').mockReturnValue({ seq: 1, timestamp: 1 })
    const unsubscribe = Broadcast.subscribe('broadcast:cross-kind', () => {})
    try {
      const text = Broadcast.publish('broadcast:cross-kind', 'a')
      const binary = Broadcast.publishBinary('broadcast:cross-kind', new Uint8Array([1]))
      controlled.ready()
      await Promise.all([text, binary])
      expect(publish.mock.calls.map(([route]) => route.kind)).toEqual(['text', 'binary'])
    } finally {
      unsubscribe()
    }
  })

  it('keeps holding for a subscription of the other kind that starts establishing during the hold', async () => {
    const attempts = { text: pendingSubscription(), binary: pendingSubscription() }
    const driver = await installOpeningBackend((source, driverOpen) =>
      'kind' in source ? attempts[source.kind].subscription : driverOpen(),
    )
    const publish = vi.spyOn(driver, 'publish').mockReturnValue({ seq: 1, timestamp: 1 })
    const stops = [Broadcast.subscribe('broadcast:late-kind', () => {})]
    try {
      const held = [Broadcast.publish('broadcast:late-kind', 'a')]
      stops.push(Broadcast.subscribeBinary('broadcast:late-kind', () => {}))
      held.push(Broadcast.publishBinary('broadcast:late-kind', new Uint8Array([1])))
      attempts.text.ready()
      await new Promise((resolve) => setTimeout(resolve, 0))
      expect(publish).not.toHaveBeenCalled()
      attempts.binary.ready()
      await Promise.all(held)
      expect(publish.mock.calls.map(([route]) => route.kind)).toEqual(['text', 'binary'])
    } finally {
      for (const stop of stops) stop()
    }
  })

  it("hands the driver a publish's bytes as they were at the call, of a Node Buffer too, sent now or held", async () => {
    const attempt = pendingSubscription()
    const driver = await installOpeningBackend(() => attempt.subscription)
    // A driver may read its payload later, as ioredis does for a queued command.
    const sent: Uint8Array[] = []
    vi.spyOn(driver, 'publish').mockImplementation((_route, payload) => {
      sent.push(payload)
      return { seq: sent.length, timestamp: 1 }
    })
    const scratch = Buffer.from([1])
    await Broadcast.publishBinary('broadcast:reused-buffer', scratch)
    scratch[0] = 2
    const unsubscribe = Broadcast.subscribeBinary('broadcast:reused-buffer', () => {})
    try {
      const held = [Broadcast.publishBinary('broadcast:reused-buffer', scratch)]
      scratch[0] = 3
      held.push(Broadcast.publishBinary('broadcast:reused-buffer', scratch))
      scratch[0] = 4
      attempt.ready()
      await Promise.all(held)
      expect(sent.map((payload) => Array.from(payload))).toEqual([[1], [2], [3]])
    } finally {
      unsubscribe()
    }
  })

  it('opens channels under a zero config.channel.bufferLimit and holds no publish', async () => {
    const { controlled } = await installPendingSubscriptionBackend({ seq: 1, timestamp: 1 })
    config.channel = { bufferLimit: 0, bufferLimitBinary: 0 }
    try {
      new ServerBroadcast({ key: 'broadcast:zero-limit' }).subscribe(() => {})
      await expect(Broadcast.publish('broadcast:zero-limit', 'x')).rejects.toBeInstanceOf(ChannelOverflowError)
      controlled.ready()
    } finally {
      config.channel = {}
    }
  })

  it('static publish + static subscribe deliver without any instance', async () => {
    const received: Array<{ text: string }> = []
    const unsubscribe = Broadcast.subscribe<{ text: string }>('room:static', (msg) => received.push(msg))

    await Broadcast.publish('room:static', { text: 'fire-and-forget' })

    expect(received).toEqual([{ text: 'fire-and-forget' }])
    unsubscribe()
  })

  it.each([
    ['subscribe', Broadcast.subscribe, () => Broadcast.publish('broadcast:static-errors', 'x')],
    [
      'subscribeBinary',
      Broadcast.subscribeBinary,
      () => Broadcast.publishBinary('broadcast:static-errors', new Uint8Array()),
    ],
  ] as const)(
    'a static %s() listener error is reported unless it is an Abort, as for a channel listener',
    async (_name, subscribe, publish) => {
      const report = vi.spyOn(console, 'error').mockImplementation(() => {})
      const stops = [
        subscribe('broadcast:static-errors', () => Promise.reject(new Error('static bug'))),
        subscribe('broadcast:static-errors', () => Promise.reject(Abort('expected'))),
        subscribe('broadcast:static-errors', () => {
          throw new ChannelClosedError()
        }),
      ]
      await publish()
      await vi.waitFor(() => expect(report).toHaveBeenCalledTimes(2))
      for (const stop of stops) stop()
    },
  )

  it('leaves no unhandled rejection behind a fire-and-forget publish that fails', async () => {
    await disposeBackend()
    const driver = new MemoryBackend({ state: memoryState })
    vi.spyOn(driver, 'publish').mockRejectedValue(new Error('connection lost'))
    installBackend(() => driver)
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown) => void unhandled.push(reason)
    process.on('unhandledRejection', onUnhandled)
    try {
      Broadcast.publish('broadcast:fire-and-forget', 'text')
      Broadcast.publishBinary('broadcast:fire-and-forget', new Uint8Array([1]))
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(unhandled).toEqual([])
      await expect(Broadcast.publish('broadcast:fire-and-forget', 'awaited')).rejects.toThrow('connection lost')
    } finally {
      process.off('unhandledRejection', onUnhandled)
    }
  })

  it('counts in receivers the subscribers a publish reached, when a subscriber leaves or joins during delivery', async () => {
    const received: string[] = []
    const unsubscribe = Broadcast.subscribe<string>('broadcast:receivers', (message) => {
      received.push(message)
      unsubscribe()
      Broadcast.subscribe('broadcast:receivers', () => {})
    })
    const receipt = await Broadcast.publish('broadcast:receivers', 'hello')
    expect(received).toEqual(['hello'])
    expect(receipt.receivers).toBe(1)
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
