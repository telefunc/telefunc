import { afterEach, describe, expect, it } from 'vitest'
import { defined } from '../../utils/defined.fixture.js'
import { getEventListeners } from 'node:events'
import { Broadcast, ServerBroadcast } from '../channel/server/ServerBroadcast.js'
import { ServerReplay } from '../channel/server/ServerReplay.js'
import { ACK_STATUS, decode, encode, type ChannelFrame } from '../wire/frame.js'
import { TAG } from '../wire/tags.js'
import { IndexedPeer } from '../channel/server/IndexedPeer.js'
import { getBroadcastAdapter, _resetBroadcastAdapterForTesting, DefaultBroadcastAdapter } from './broadcastAdapter.js'
import type { BroadcastTransport, Listening } from './broadcastAdapter.js'

/** The adapter's subscription as the function that stops it. */
const stopOf = (subscription: Listening) => subscription.unsubscribe
import { ChannelClosedError, ChannelOverflowError } from '../channel/channelErrors.js'
import type { Unsubscribe } from '../channel/channel.js'
import { ATTACHABLE_NOW } from '../channel/server/ServerChannel.js'

const previousBroadcastAdapter = getBroadcastAdapter()
afterEach(() => _resetBroadcastAdapterForTesting(previousBroadcastAdapter))

// ───────────────────────────────────────────────────────────────────────────
// In-process delivery — bug classes targeted: cross-key bleed, dropped
// subscribers, delivery reordering, self-echo loss, late-register and
// late-attach buffer correctness, error isolation between subscribers.
// ───────────────────────────────────────────────────────────────────────────

describe('keyed in-process broadcast', () => {
  it('delivers a published message to a sibling broadcast on the same key', () => {
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:basic' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:basic' })
    sender._registerChannel(ATTACHABLE_NOW)
    receiver._registerChannel(ATTACHABLE_NOW)

    const received: Array<{ text: string }> = []
    receiver.subscribe((msg) => received.push(msg))
    sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  // The publisher's own subscribe must fire too — same instance is both pub and sub.
  // Catches a "skip-self" bug that excludes the source from delivery.
  it('delivers a published message to the source broadcast (self-echo)', () => {
    const broadcast = new ServerBroadcast<{ text: string }>({ key: 'room:self' })
    broadcast._registerChannel(ATTACHABLE_NOW)

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
    a._registerChannel(ATTACHABLE_NOW)
    b._registerChannel(ATTACHABLE_NOW)

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
    pub._registerChannel(ATTACHABLE_NOW)
    subA._registerChannel(ATTACHABLE_NOW)
    subB._registerChannel(ATTACHABLE_NOW)
    subC._registerChannel(ATTACHABLE_NOW)

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
    sender._registerChannel(ATTACHABLE_NOW)
    receiver._registerChannel(ATTACHABLE_NOW)

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
    broadcast._registerChannel(ATTACHABLE_NOW)

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
    broadcast._registerChannel(ATTACHABLE_NOW)

    const seen: number[] = []
    const unsubscribe = broadcast.subscribe((m) => seen.push(m.n))

    broadcast.publish({ n: 1 })
    unsubscribe()
    broadcast.publish({ n: 2 })

    expect(seen).toEqual([1])
  })

  it('buffers wire frames until a peer attaches, then flushes them on attach', () => {
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:late-attach' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:late-attach' })
    sender._registerChannel(ATTACHABLE_NOW)
    receiver._registerChannel(ATTACHABLE_NOW)
    receiver._dispatchFrame(decode(encode.want(7, { version: 1, text: true, binary: false })) as ChannelFrame)

    sender.publish({ text: 'hello' })

    const frames: Uint8Array[] = []
    receiver._attachPeer(
      new IndexedPeer({
        sender: {
          sendPublish(_index, _seq, frame) {
            frames.push(frame)
          },
          send() {},
          carry() {},
          bufferedAmount: () => 0,
          behindAfter: () => 700,
          deliveredBefore: () => 0,
          sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
          waitingPublishes: () => 0,
          dropUngiven: () => 0,
          dropWaitingPublishes() {},
          dropWaitingFrom() {},
        },
        index: 7,
        replay: new ServerReplay({ heldBytes: 0 }),
      }),
    )

    expect(frames).toHaveLength(1)
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

  it('publish receipts are key-scoped and seq increments monotonically per key', async () => {
    const k1 = 'room:receipts:A'
    const k2 = 'room:receipts:B'
    const a = new ServerBroadcast<{ text: string }>({ key: k1 })
    const b = new ServerBroadcast<{ text: string }>({ key: k2 })
    a._registerChannel(ATTACHABLE_NOW)
    b._registerChannel(ATTACHABLE_NOW)

    const a1 = await a.publish({ text: 'a-one' })
    const a2 = await a.publish({ text: 'a-two' })
    const b1 = await b.publish({ text: 'b-one' })

    // Each receipt is keyed to its own topic.
    expect(a1.key).toBe(k1)
    expect(a2.key).toBe(k1)
    expect(b1.key).toBe(k2)
    expect(a2.seq).toBe(a1.seq + 1)
    expect(b1.seq).toBeGreaterThan(0)
    expect(typeof a1.timestamp).toBe('number')
  })
})

describe('in-memory publishes in answer', () => {
  it('delivers a chain of 5,000 answers', async () => {
    const adapter = new DefaultBroadcastAdapter()
    let received = 0
    const unsubscribe = stopOf(
      adapter.subscribe('room:finite', (serialized) => {
        received++
        const n = Number(serialized)
        if (n < 5000) void adapter.publish('room:finite', String(n + 1))
      }),
    )
    await adapter.publish('room:finite', '0')
    unsubscribe()
    expect(received).toBe(5001)
  })

  it.each(['text', 'binary'] as const)(
    "a listener a broadcast adds while it delivers gets that message and what's published after, not an answer published before it was added (%s)",
    async (kind) => {
      const key = `room:joins-mid-delivery:${kind}`
      const room = new ServerBroadcast<number>({ key })
      const send = (n: number) =>
        kind === 'text' ? Broadcast.publish(key, n) : Broadcast.publishBinary(key, Uint8Array.of(n))
      const listen = (callback: (n: number) => void) =>
        kind === 'text' ? room.subscribe(callback) : room.subscribeBinary((data) => callback(data[0] ?? -1))
      const late: number[] = []
      const stops = [
        listen((n) => {
          if (n !== 0) return
          void send(1)
          stops.push(listen((m) => late.push(m)))
        }),
      ]
      await send(0)
      await send(2)
      for (const stop of stops) stop()
      expect(late).toEqual([0, 2])
    },
  )

  it('rejects a listener that answers its own messages without end, and timers run again', async () => {
    const adapter = new DefaultBroadcastAdapter()
    const errors: unknown[] = []
    const unsubscribe = stopOf(
      adapter.subscribe('room:endless', () => {
        const result = adapter.publish('room:endless', 'again')
        if (result instanceof Promise) result.catch((err: unknown) => errors.push(err))
      }),
    )
    const timerRan = new Promise((resolve) => setTimeout(resolve, 0))
    adapter.publish('room:endless', 'start')
    await timerRan
    unsubscribe()
    expect(errors).toHaveLength(1)
    expect(errors[0]).toBeInstanceOf(ChannelOverflowError)
    expect(errors[0]).toHaveProperty('message', expect.stringContaining("past Telefunc's limit on nested publishes"))
  }, 60_000)
})

// ───────────────────────────────────────────────────────────────────────────
// Binary path — publishBinary/subscribeBinary roundtrip with high-bit bytes.
// Catches accidental string-coercion or UTF-8 transcoding of binary frames.
// ───────────────────────────────────────────────────────────────────────────

describe('binary in-process broadcast', () => {
  it('round-trips binary publishes preserving high-bit bytes', () => {
    const sender = new ServerBroadcast({ key: 'room:bin' })
    const receiver = new ServerBroadcast({ key: 'room:bin' })
    sender._registerChannel(ATTACHABLE_NOW)
    receiver._registerChannel(ATTACHABLE_NOW)

    const received: Uint8Array[] = []
    receiver.subscribeBinary((data) => received.push(data))
    sender.publishBinary(new Uint8Array([0x00, 0x7f, 0x80, 0xff]))

    expect(received).toHaveLength(1)
    expect(Array.from(received[0]!)).toEqual([0x00, 0x7f, 0x80, 0xff])
  })

  it('binary subscribers do NOT receive text publishes (and vice versa)', () => {
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:mixed' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:mixed' })
    sender._registerChannel(ATTACHABLE_NOW)
    receiver._registerChannel(ATTACHABLE_NOW)

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
})

// ───────────────────────────────────────────────────────────────────────────
// Channel-method gating — Broadcast extends ServerChannel but the inherited
// send/listen/sendBinary/listenBinary must throw at runtime since the type
// hides them. Catches a regression where a subclass forgets to override one.
// ───────────────────────────────────────────────────────────────────────────

// The Cloudflare adapter refuses a server-side subscription on a Worker without AsyncLocalStorage; the app may catch it.
class AdapterRefusingFirstSubscription extends DefaultBroadcastAdapter {
  private refuse = true
  override subscribe(...args: Parameters<DefaultBroadcastAdapter['subscribe']>) {
    this.refuseOnce()
    return super.subscribe(...args)
  }
  override subscribeBinary(...args: Parameters<DefaultBroadcastAdapter['subscribeBinary']>) {
    this.refuseOnce()
    return super.subscribeBinary(...args)
  }
  private refuseOnce(): void {
    if (!this.refuse) return
    this.refuse = false
    throw new Error('subscription refused')
  }
}

describe('a subscription the adapter refused', () => {
  it('subscribe(): its callback gets no later publish', () => {
    _resetBroadcastAdapterForTesting(new AdapterRefusingFirstSubscription())
    const broadcast = new ServerBroadcast<string>({ key: 'room:refused' })
    broadcast._registerChannel(ATTACHABLE_NOW)
    const refused: string[] = []
    const later: string[] = []
    expect(() => broadcast.subscribe((msg) => refused.push(msg))).toThrow('subscription refused')
    broadcast.subscribe((msg) => later.push(msg))
    broadcast.publish('hello')
    expect(later).toEqual(['hello'])
    expect(refused).toEqual([])
  })

  it('subscribeBinary(): its callback gets no later publish', () => {
    _resetBroadcastAdapterForTesting(new AdapterRefusingFirstSubscription())
    const broadcast = new ServerBroadcast<string>({ key: 'room:refused-binary' })
    broadcast._registerChannel(ATTACHABLE_NOW)
    const refused: Uint8Array[] = []
    const later: Uint8Array[] = []
    expect(() => broadcast.subscribeBinary((data) => refused.push(data))).toThrow('subscription refused')
    broadcast.subscribeBinary((data) => later.push(data))
    broadcast.publishBinary(new Uint8Array([1, 2, 3]))
    expect(later).toEqual([new Uint8Array([1, 2, 3])])
    expect(refused).toEqual([])
  })
})

// The backend confirms no subscription until each test is over.
describe("a server subscriber's `opened` while its backend hasn't confirmed the subscription", () => {
  const pending = new Promise<void>(() => {})
  const sent = () => ({ seq: 1, timestamp: 1 })
  const listen = () => ({ unsubscribe: () => {}, opened: pending })
  const unconfirmed = () =>
    _resetBroadcastAdapterForTesting(
      new DefaultBroadcastAdapter({
        send: sent,
        sendBinary: sent,
        listen,
        listenBinary: listen,
      }),
    )
  const settled = async (opened: Promise<void>) => {
    let state = 'pending'
    opened.then(
      () => (state = 'resolved'),
      () => (state = 'rejected'),
    )
    await new Promise((resolve) => setTimeout(resolve, 0))
    return state
  }
  type Subscribe = (signal: AbortSignal) => { remove: () => void; opened: Promise<void>; end?: () => void }
  const instance =
    (binary: boolean): Subscribe =>
    (signal) => {
      const broadcast = new ServerBroadcast<string>({ key: crypto.randomUUID() })
      const remove = binary
        ? broadcast.subscribeBinary(() => {}, { signal })
        : broadcast.subscribe(() => {}, { signal })
      return { remove, opened: remove.opened, end: () => broadcast.abort() }
    }
  const statically =
    (binary: boolean): Subscribe =>
    (signal) => {
      const key = crypto.randomUUID()
      const remove = binary
        ? Broadcast.subscribeBinary(key, () => {}, { signal })
        : Broadcast.subscribe(key, () => {}, { signal })
      return { remove, opened: remove.opened }
    }
  const SUBSCRIBERS: Record<string, Subscribe> = {
    'subscribe()': instance(false),
    'subscribeBinary()': instance(true),
    'Broadcast.subscribe()': statically(false),
    'Broadcast.subscribeBinary()': statically(true),
  }
  for (const [name, subscribe] of Object.entries(SUBSCRIBERS)) {
    it(`${name}: resolves once removed by its remover`, async () => {
      unconfirmed()
      const subscriber = subscribe(new AbortController().signal)
      subscriber.remove()
      expect(await settled(subscriber.opened)).toBe('resolved')
      subscriber.end?.()
    })
    it(`${name}: resolves once removed by its signal`, async () => {
      unconfirmed()
      const controller = new AbortController()
      const subscriber = subscribe(controller.signal)
      controller.abort()
      expect(await settled(subscriber.opened)).toBe('resolved')
      subscriber.end?.()
    })
  }
})

// A backend that refused a key's subscription keeps that outcome: removing a subscriber afterwards doesn't turn its
// `opened` into a success, whether the app read `opened` before the removal or not.
describe("a static subscriber's `opened` once its backend refused the key's subscription", () => {
  const sent = () => ({ seq: 1, timestamp: 1 })
  const refusing = () => {
    const opened = Promise.reject(new Error('NOPERM'))
    const listen = () => ({ unsubscribe: () => {}, opened })
    _resetBroadcastAdapterForTesting(
      new DefaultBroadcastAdapter({ send: sent, sendBinary: sent, listen, listenBinary: listen }),
    )
  }
  const SUBSCRIBE: Record<string, () => Unsubscribe> = {
    'Broadcast.subscribe()': () => Broadcast.subscribe('room:refused', () => {}),
    'Broadcast.subscribeBinary()': () => Broadcast.subscribeBinary('room:refused', () => {}),
  }
  for (const [name, subscribe] of Object.entries(SUBSCRIBE)) {
    for (const removed of ['after', 'before']) {
      it(`${name}: rejects with the refusal when removed ${removed} its first read`, async () => {
        refusing()
        const remove = subscribe()
        await new Promise((resolve) => setTimeout(resolve, 0))
        let opened: Promise<void> | undefined
        if (removed === 'after') opened = remove.opened
        remove()
        opened ??= remove.opened
        await expect(opened).rejects.toThrow('NOPERM')
      })
    }
    it(`${name}: a refusal nobody reads makes no unhandled rejection`, async () => {
      const unhandled: unknown[] = []
      const onUnhandled = (reason: unknown) => void unhandled.push(reason)
      process.on('unhandledRejection', onUnhandled)
      try {
        refusing()
        const remove = subscribe()
        await new Promise((resolve) => setTimeout(resolve, 10))
        remove()
        await new Promise((resolve) => setTimeout(resolve, 10))
        expect(unhandled).toEqual([])
      } finally {
        process.off('unhandledRejection', onUnhandled)
      }
    })
  }
})

// A broker's subscribe can throw, as NATS's does while its connection is closed or draining; the app retries.
describe("a transport's listen() that throws", () => {
  type Info = { seq: number; timestamp: number }
  /** A transport whose first `listen()` and first `listenBinary()` throw, counting the listeners it holds. */
  const failingFirst = () => {
    const state = {
      attempts: 0,
      active: 0,
      text: (_payload: string, _info: Info) => {},
      binary: (_payload: Uint8Array, _info: Info) => {},
    }
    const hold = () => {
      if (++state.attempts === 1) throw new Error('broker connection closed')
      state.active++
      return () => {
        state.active--
      }
    }
    const sent = () => ({ seq: 1, timestamp: 1 })
    _resetBroadcastAdapterForTesting(
      new DefaultBroadcastAdapter({
        send: sent,
        sendBinary: sent,
        listen: (_key, onMessage) => {
          const stop = hold()
          state.text = onMessage
          return stop
        },
        listenBinary: (_key, onMessage) => {
          const stop = hold()
          state.binary = onMessage
          return stop
        },
      }),
    )
    return state
  }

  it('Broadcast.subscribe(): keeps no callback of the call that threw; the retry hears its own messages only', async () => {
    const transport = failingFirst()
    const heard: string[] = []
    expect(() => Broadcast.subscribe<string>('room:throws', (text) => heard.push(`failed ${text}`))).toThrow(
      'broker connection closed',
    )
    const stop = Broadcast.subscribe<string>('room:throws', (text) => heard.push(`retry ${text}`))
    await stop.opened
    transport.text('"hello"', { seq: 1, timestamp: 1 })
    stop()
    expect({ heard, active: transport.active, attempts: transport.attempts }).toEqual({
      heard: ['retry hello'],
      active: 0,
      attempts: 2,
    })
  })

  it('BroadcastChannel.subscribe(): keeps no listener of the call that threw; the retry hears its own messages only', async () => {
    const transport = failingFirst()
    const room = new ServerBroadcast<string>({ key: 'room:throws' })
    const heard: string[] = []
    expect(() => room.subscribe((text) => heard.push(`failed ${text}`))).toThrow('broker connection closed')
    const stop = room.subscribe((text) => heard.push(`retry ${text}`))
    await stop.opened
    transport.text('"hello"', { seq: 1, timestamp: 1 })
    room.abort()
    expect({ heard, active: transport.active, attempts: transport.attempts }).toEqual({
      heard: ['retry hello'],
      active: 0,
      attempts: 2,
    })
  })

  it('BroadcastChannel.subscribeBinary(): keeps no listener of the call that threw; the retry hears its own messages only', async () => {
    const transport = failingFirst()
    const room = new ServerBroadcast({ key: 'room:throws' })
    const heard: string[] = []
    expect(() => room.subscribeBinary((data) => heard.push(`failed ${data}`))).toThrow('broker connection closed')
    const stop = room.subscribeBinary((data) => heard.push(`retry ${data}`))
    await stop.opened
    transport.binary(new Uint8Array([1, 2, 3]), { seq: 1, timestamp: 1 })
    room.abort()
    expect({ heard, active: transport.active, attempts: transport.attempts }).toEqual({
      heard: ['retry 1,2,3'],
      active: 0,
      attempts: 2,
    })
  })

  it('Broadcast.subscribeBinary(): keeps no callback of the call that threw; the retry hears its own messages only', async () => {
    const transport = failingFirst()
    const heard: string[] = []
    expect(() => Broadcast.subscribeBinary('room:throws', (data) => heard.push(`failed ${data}`))).toThrow(
      'broker connection closed',
    )
    const stop = Broadcast.subscribeBinary('room:throws', (data) => heard.push(`retry ${data}`))
    await stop.opened
    transport.binary(new Uint8Array([1, 2, 3]), { seq: 1, timestamp: 1 })
    stop()
    expect({ heard, active: transport.active, attempts: transport.attempts }).toEqual({
      heard: ['retry 1,2,3'],
      active: 0,
      attempts: 2,
    })
  })
})

// A broker that replays on connect can deliver a message, then fail to connect: a listener added on that message came
// for the start that failed, and goes with it.
describe("a transport's listen() that delivers a message, then throws", () => {
  type Info = { seq: number; timestamp: number }
  const KEY = 'room:delivers-then-throws'
  /** A transport whose first `listen()` and first `listenBinary()` deliver `1`, then throw; it counts the listeners it
   *  holds. */
  const deliveringThenThrowing = () => {
    let attempts = 0
    let seq = 0
    const text = new Set<(payload: string, info: Info) => void>()
    const binary = new Set<(payload: Uint8Array, info: Info) => void>()
    const info = () => ({ seq: ++seq, timestamp: 1 })
    _resetBroadcastAdapterForTesting(
      new DefaultBroadcastAdapter({
        send: (_key, payload) => {
          const sent = info()
          for (const onMessage of text) onMessage(payload, sent)
          return sent
        },
        sendBinary: (_key, payload) => {
          const sent = info()
          for (const onMessage of binary) onMessage(payload, sent)
          return sent
        },
        listen: (_key, onMessage) => {
          if (++attempts === 1) {
            onMessage('1', info())
            throw new Error('broker connection closed')
          }
          text.add(onMessage)
          return () => text.delete(onMessage)
        },
        listenBinary: (_key, onMessage) => {
          if (++attempts === 1) {
            onMessage(Uint8Array.of(1), info())
            throw new Error('broker connection closed')
          }
          binary.add(onMessage)
          return () => binary.delete(onMessage)
        },
      }),
    )
    return { listening: () => text.size + binary.size }
  }
  /** Each surface's subscribe of `onMessage`, which hears numbers, and its publish of `2`. */
  const SURFACES: Record<
    string,
    () => { subscribe: (onMessage: (n: number) => void) => Unsubscribe; publish: () => unknown; end?: () => void }
  > = {
    'Broadcast.subscribe()': () => ({
      subscribe: (onMessage) => Broadcast.subscribe<number>(KEY, onMessage),
      publish: () => Broadcast.publish(KEY, 2),
    }),
    'Broadcast.subscribeBinary()': () => ({
      subscribe: (onMessage) => Broadcast.subscribeBinary(KEY, (data) => data.forEach((n) => onMessage(n))),
      publish: () => Broadcast.publishBinary(KEY, Uint8Array.of(2)),
    }),
    'BroadcastChannel.subscribe()': () => {
      const broadcast = new ServerBroadcast<number>({ key: KEY })
      return {
        subscribe: (onMessage) => broadcast.subscribe(onMessage),
        publish: () => Broadcast.publish(KEY, 2),
        end: () => broadcast.abort(),
      }
    },
    'BroadcastChannel.subscribeBinary()': () => {
      const broadcast = new ServerBroadcast({ key: KEY })
      return {
        subscribe: (onMessage) => broadcast.subscribeBinary((data) => data.forEach((n) => onMessage(n))),
        publish: () => Broadcast.publishBinary(KEY, Uint8Array.of(2)),
        end: () => broadcast.abort(),
      }
    },
  }

  for (const [surface, of] of Object.entries(SURFACES)) {
    it(`${surface}: a listener added on that message gets the error and goes, so the retry alone listens`, async () => {
      const transport = deliveringThenThrowing()
      const { subscribe, publish, end } = of()
      const joinedHeard: number[] = []
      let joined: Unsubscribe | undefined
      expect(() =>
        subscribe(() => {
          joined ??= subscribe((n) => joinedHeard.push(n))
        }),
      ).toThrow('broker connection closed')
      await expect(joined?.opened).rejects.toThrow('broker connection closed')
      const retry = subscribe(() => {})
      await retry.opened
      await publish()
      retry()
      const listeningAfterRetry = transport.listening()
      end?.()
      expect({ joinedHeard: joinedHeard.filter((n) => n === 2), listeningAfterRetry }).toEqual({
        joinedHeard: [],
        listeningAfterRetry: 0,
      })
    })
  }

  // An app that subscribes with one long-lived signal: each attempt that fails must leave nothing on it.
  const WITH_SIGNAL: Record<string, (signal: AbortSignal) => (onMessage: () => void) => Unsubscribe> = {
    'Broadcast.subscribe()': (signal) => (onMessage) => Broadcast.subscribe(KEY, onMessage, { signal }),
    'Broadcast.subscribeBinary()': (signal) => (onMessage) => Broadcast.subscribeBinary(KEY, onMessage, { signal }),
  }
  for (const [surface, of] of Object.entries(WITH_SIGNAL)) {
    it(`${surface}: a listener added on that message with a signal leaves no abort listener on it once it goes`, async () => {
      deliveringThenThrowing()
      const signal = new AbortController().signal
      const subscribe = of(signal)
      let joined: Unsubscribe | undefined
      expect(() =>
        subscribe(() => {
          joined ??= subscribe(() => {})
        }),
      ).toThrow('broker connection closed')
      await expect(joined?.opened).rejects.toThrow('broker connection closed')
      expect(getEventListeners(signal, 'abort')).toHaveLength(0)
    })
  }

  // An app that subscribes one function of its own again, through the adapter, after a start that failed.
  for (const binary of [false, true]) {
    it(`the adapter's ${binary ? 'subscribeBinary()' : 'subscribe()'}: the remover of a callback the failed start removed leaves that callback's later subscription be`, async () => {
      const transport = deliveringThenThrowing()
      const adapter = getBroadcastAdapter()
      const heard: number[] = []
      const onText = (payload: string) => heard.push(Number(payload))
      const onBinary = (payload: Uint8Array) => heard.push(...payload)
      const subscribe = (starting?: () => void) =>
        binary
          ? adapter.subscribeBinary(KEY, starting ? () => starting() : onBinary)
          : adapter.subscribe(KEY, starting ? () => starting() : onText)
      let failed: Listening | undefined
      expect(() =>
        subscribe(() => {
          failed ??= subscribe()
        }),
      ).toThrow('broker connection closed')
      const later = subscribe()
      failed?.unsubscribe()
      await (binary ? adapter.publishBinary(KEY, Uint8Array.of(2)) : adapter.publish(KEY, '2'))
      const listening = transport.listening()
      later.unsubscribe()
      expect({ heard, listening }).toEqual({ heard: [1, 2], listening: 1 })
    })
  }
})

// An app that subscribes and unsubscribes with one long-lived signal while its key stays subscribed.
describe('static subscribers that come and go with one signal on a key a healthy start listens to', () => {
  for (const binary of [false, true]) {
    it(`${binary ? 'Broadcast.subscribeBinary()' : 'Broadcast.subscribe()'}: leave nothing registered on that signal or the start's`, () => {
      const sent = () => ({ seq: 1, timestamp: 1 })
      const listen = () => () => {}
      _resetBroadcastAdapterForTesting(
        new DefaultBroadcastAdapter({ send: sent, sendBinary: sent, listen, listenBinary: listen }),
      )
      const key = 'room:signal-churn'
      const subscribe = (signal?: AbortSignal) =>
        binary ? Broadcast.subscribeBinary(key, () => {}, { signal }) : Broadcast.subscribe(key, () => {}, { signal })
      const holder = subscribe()
      const signal = new AbortController().signal
      for (let i = 0; i < 100; i++) subscribe(signal)()
      const adapter = getBroadcastAdapter()
      const { removed, unsubscribe } = binary
        ? adapter.subscribeBinary(key, () => {})
        : adapter.subscribe(key, () => {})
      const registered = {
        signal: getEventListeners(signal, 'abort').length,
        start: removed ? getEventListeners(removed, 'abort').length : 0,
      }
      unsubscribe()
      holder()
      expect(registered).toEqual({ signal: 0, start: 0 })
    })
  }
})

// A broker may deliver a message before its subscribe returns: an in-process one whose producer publishes as soon as the
// key has a listener.
describe('a message the transport delivers inside listen()', () => {
  type Info = { seq: number; timestamp: number }
  const KEY = 'room:inside-listen'
  /** A transport whose `listen()` and `listenBinary()` deliver `1` to their key before they return. */
  const deliveringInsideListen = () => {
    let seq = 0
    let starts = 0
    let active = 0
    let text: ((payload: string, info: Info) => void) | undefined
    let binary: ((payload: Uint8Array, info: Info) => void) | undefined
    const send = (_key: string, payload: string) => {
      const sent = { seq: ++seq, timestamp: 1 }
      text?.(payload, sent)
      return sent
    }
    const sendBinary = (_key: string, payload: Uint8Array) => {
      const sent = { seq: ++seq, timestamp: 1 }
      binary?.(payload, sent)
      return sent
    }
    _resetBroadcastAdapterForTesting(
      new DefaultBroadcastAdapter({
        send,
        sendBinary,
        listen: (key, onMessage) => {
          starts++
          active++
          text = onMessage
          send(key, '1')
          return () => {
            active--
            text = undefined
          }
        },
        listenBinary: (key, onMessage) => {
          starts++
          active++
          binary = onMessage
          sendBinary(key, Uint8Array.of(1))
          return () => {
            active--
            binary = undefined
          }
        },
      }),
    )
    return { listening: () => active > 0, starts: () => starts }
  }
  const room = () => new ServerBroadcast<number>({ key: KEY })
  const SURFACES: Record<string, (heard: number[]) => { stop: Unsubscribe; publish: () => unknown; end?: () => void }> =
    {
      'Broadcast.subscribe()': (heard) => ({
        stop: Broadcast.subscribe<number>(KEY, (n) => heard.push(n)),
        publish: () => Broadcast.publish(KEY, 2),
      }),
      'Broadcast.subscribeBinary()': (heard) => ({
        stop: Broadcast.subscribeBinary(KEY, (data) => heard.push(...data)),
        publish: () => Broadcast.publishBinary(KEY, Uint8Array.of(2)),
      }),
      'BroadcastChannel.subscribe()': (heard) => {
        const broadcast = room()
        return {
          stop: broadcast.subscribe((n) => heard.push(n)),
          publish: () => Broadcast.publish(KEY, 2),
          end: () => broadcast.abort(),
        }
      },
      'BroadcastChannel.subscribeBinary()': (heard) => {
        const broadcast = room()
        return {
          stop: broadcast.subscribeBinary((data) => heard.push(...data)),
          publish: () => Broadcast.publishBinary(KEY, Uint8Array.of(2)),
          end: () => broadcast.abort(),
        }
      },
    }

  for (const [surface, subscribe] of Object.entries(SURFACES)) {
    it(`${surface}: the subscriber hears it, then the next publish`, async () => {
      const transport = deliveringInsideListen()
      const heard: number[] = []
      const { stop, publish, end } = subscribe(heard)
      await stop.opened
      await publish()
      stop()
      end?.()
      expect({ heard, listening: transport.listening() }).toEqual({ heard: [1, 2], listening: false })
    })
  }

  // A static subscriber's signal is listened to once its subscribe returns.
  const LEAVING: Record<string, (onMessage: () => void, signal: AbortSignal) => void> = {
    'BroadcastChannel.subscribe()': (onMessage, signal) => room().subscribe(onMessage, { signal }),
    'BroadcastChannel.subscribeBinary()': (onMessage, signal) => room().subscribeBinary(onMessage, { signal }),
  }

  for (const [surface, subscribe] of Object.entries(LEAVING)) {
    it(`${surface}: a listener that leaves as that message comes leaves nothing listening`, () => {
      const transport = deliveringInsideListen()
      const leaving = new AbortController()
      subscribe(() => leaving.abort(), leaving.signal)
      expect({ starts: transport.starts(), listening: transport.listening() }).toEqual({ starts: 1, listening: false })
    })
  }

  /** The first listener of each surface adds a second of its kind from its callback, as that message comes. */
  const NESTED: Record<
    string,
    (heard: { first: number[]; second: number[] }) => { stops: Unsubscribe[]; publish: () => unknown; end?: () => void }
  > = {
    'Broadcast.subscribe()': (heard) => {
      const stops: Unsubscribe[] = []
      const second = (n: number) => heard.second.push(n)
      const first = (n: number) => {
        heard.first.push(n)
        if (heard.first.length === 1) stops.push(Broadcast.subscribe<number>(KEY, second))
      }
      stops.push(Broadcast.subscribe<number>(KEY, first))
      return { stops, publish: () => Broadcast.publish(KEY, 2) }
    },
    'Broadcast.subscribeBinary()': (heard) => {
      const stops: Unsubscribe[] = []
      const second = (data: Uint8Array) => heard.second.push(...data)
      const first = (data: Uint8Array) => {
        heard.first.push(...data)
        if (heard.first.length === 1) stops.push(Broadcast.subscribeBinary(KEY, second))
      }
      stops.push(Broadcast.subscribeBinary(KEY, first))
      return { stops, publish: () => Broadcast.publishBinary(KEY, Uint8Array.of(2)) }
    },
    'BroadcastChannel.subscribe()': (heard) => {
      const broadcast = room()
      const stops: Unsubscribe[] = []
      const first = (n: number) => {
        heard.first.push(n)
        if (heard.first.length === 1) stops.push(broadcast.subscribe((n) => heard.second.push(n)))
      }
      stops.push(broadcast.subscribe(first))
      return { stops, publish: () => Broadcast.publish(KEY, 2), end: () => broadcast.abort() }
    },
    'BroadcastChannel.subscribeBinary()': (heard) => {
      const broadcast = room()
      const stops: Unsubscribe[] = []
      const first = (data: Uint8Array) => {
        heard.first.push(...data)
        if (heard.first.length === 1) stops.push(broadcast.subscribeBinary((data) => heard.second.push(...data)))
      }
      stops.push(broadcast.subscribeBinary(first))
      return { stops, publish: () => Broadcast.publishBinary(KEY, Uint8Array.of(2)), end: () => broadcast.abort() }
    },
  }

  for (const [surface, subscribe] of Object.entries(NESTED)) {
    it(`${surface}: a listener its callback adds as that message comes shares the one start, and nothing listens after`, async () => {
      const transport = deliveringInsideListen()
      const heard = { first: [], second: [] }
      const { stops, publish, end } = subscribe(heard)
      await Promise.all(stops.map((stop) => stop.opened))
      await publish()
      for (const stop of stops) stop()
      end?.()
      // Whether the second hears the message it was added on is delivery's to decide, not this case's.
      expect({
        first: heard.first,
        second: heard.second.filter((n) => n === 2),
        starts: transport.starts(),
        listening: transport.listening(),
      }).toEqual({ first: [1, 2], second: [2], starts: 1, listening: false })
    })
  }
})

// A subscriber may end its broadcast on a message that ends what it waited for, before its backend confirmed the
// subscription: its `opened` rejects with that end, whether the message came inside listen() or after it.
describe('a broadcast its subscriber ends on the first message, its backend not confirmed yet', () => {
  for (const binary of [false, true]) {
    for (const inside of [true, false]) {
      it(`${binary ? 'subscribeBinary()' : 'subscribe()'}, the message ${inside ? 'inside' : 'after'} listen(): \`opened\` rejects with the end`, async () => {
        let listening = 0
        let deliver = () => {}
        const start = (deliverFirst: () => void) => {
          listening++
          deliver = deliverFirst
          if (inside) deliver()
          return { unsubscribe: () => listening--, opened: new Promise<void>(() => {}) }
        }
        const sent = () => ({ seq: 1, timestamp: 1 })
        _resetBroadcastAdapterForTesting(
          new DefaultBroadcastAdapter({
            send: sent,
            sendBinary: sent,
            listen: (_key, onMessage) => start(() => onMessage('1', sent())),
            listenBinary: (_key, onMessage) => start(() => onMessage(Uint8Array.of(1), sent())),
          }),
        )
        const room = new ServerBroadcast<string>({ key: 'room:ends-on-first' })
        const end = () => room.abort('terminal event')
        const stop = binary ? room.subscribeBinary(end) : room.subscribe(end)
        if (!inside) deliver()
        const opened = await stop.opened.then(
          () => 'resolved',
          (err: unknown) => err,
        )
        expect({ opened, listening }).toEqual({ opened: await room.closed, listening: 0 })
      })
    }
  }
})

// An app may clean its listeners up in `onClose()`: a listener whose `opened` was pending when the broadcast ended still
// gets that end, whatever a close callback does after.
describe('a broadcast that ends while a subscriber waits, its `onClose()` removing that subscriber', () => {
  for (const binary of [false, true]) {
    it(`${binary ? 'subscribeBinary()' : 'subscribe()'}: \`opened\` rejects with the end`, async () => {
      const sent = () => ({ seq: 1, timestamp: 1 })
      const listen = () => ({ unsubscribe() {}, opened: new Promise<void>(() => {}) })
      _resetBroadcastAdapterForTesting(
        new DefaultBroadcastAdapter({ send: sent, sendBinary: sent, listen, listenBinary: listen }),
      )
      const room = new ServerBroadcast<string>({ key: 'room:cleans-up-on-close' })
      const stop = binary ? room.subscribeBinary(() => {}) : room.subscribe(() => {})
      room.onClose(() => stop())
      room.abort('terminal event')
      const opened = await stop.opened.then(
        () => 'resolved',
        (err: unknown) => err,
      )
      expect(opened).toBe(await room.closed)
    })
  }
})

// Its close's outcome is decided after its callbacks start: here the cleanup one of them runs outlasts the close.
describe("a broadcast whose graceful close times out on an `onClose()` callback's cleanup while a subscriber waits", () => {
  for (const binary of [false, true])
    for (const removes of [false, true]) {
      it(`${binary ? 'subscribeBinary()' : 'subscribe()'}: ${removes ? 'removed by the callback, its `opened` rejects at the removal with the close, not the end it times out with' : 'still subscribed, its `opened` rejects with the end the close times out with'}`, async () => {
        const sent = () => ({ seq: 1, timestamp: 1 })
        const listen = () => ({ unsubscribe() {}, opened: new Promise<void>(() => {}) })
        _resetBroadcastAdapterForTesting(
          new DefaultBroadcastAdapter({ send: sent, sendBinary: sent, listen, listenBinary: listen }),
        )
        const room = new ServerBroadcast<string>({ key: 'room:close-times-out' })
        const stop = binary ? room.subscribeBinary(() => {}) : room.subscribe(() => {})
        room.onClose(() => {
          if (removes) stop()
          return new Promise<void>(() => {})
        })
        await room.close({ timeout: 10 })
        const ended = await room.closed
        expect(ended).toMatchObject({ message: 'Channel close timed out' })
        const opened = await stop.opened.then(
          () => 'resolved',
          (err: unknown) => err,
        )
        if (removes) {
          expect(opened).toBeInstanceOf(ChannelClosedError)
          expect(opened).not.toBe(ended)
        } else expect(opened).toBe(ended)
      })
    }
})

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
// the runtime check lives in dispatchPublishAckReq.
// ───────────────────────────────────────────────────────────────────────────

describe('Broadcast shield validation', () => {
  it('rejects client publishes that fail the data shield with a SHIELD_ERROR ack', () => {
    const broadcast = new ServerBroadcast<{ text: string }>({ key: 'room:shield' })
    broadcast._validators.set('data', (value) => {
      const v = value as { text?: unknown }
      return typeof v?.text === 'string' ? true : 'expected { text: string }'
    })
    broadcast._registerChannel(ATTACHABLE_NOW)

    const frames: Uint8Array[] = []
    broadcast._attachPeer(
      new IndexedPeer({
        sender: {
          sendPublish(index, seq, body) {
            this.send(encode.publishTo(index, seq))
            this.send(body)
          },
          send: (frame) => {
            frames.push(frame)
          },
          carry() {},
          bufferedAmount: () => 0,
          behindAfter: () => 700,
          deliveredBefore: () => 0,
          sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
          waitingPublishes: () => 0,
          dropUngiven: () => 0,
          dropWaitingPublishes() {},
          dropWaitingFrom() {},
        },
        index: 7,
        replay: new ServerReplay({ heldBytes: 0 }),
      }),
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
    sender._registerChannel(ATTACHABLE_NOW)
    receiver._registerChannel(ATTACHABLE_NOW)

    sender._validators.set('data', () => 'always reject')

    const seen: Array<{ text: string }> = []
    receiver.subscribe((m) => seen.push(m))

    sender._attachPeer(
      new IndexedPeer({
        sender: {
          sendPublish() {},
          send: () => {},
          carry() {},
          bufferedAmount: () => 0,
          behindAfter: () => 700,
          deliveredBefore: () => 0,
          sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
          waitingPublishes: () => 0,
          dropUngiven: () => 0,
          dropWaitingPublishes() {},
          dropWaitingFrom() {},
        },
        index: 7,
        replay: new ServerReplay({ heldBytes: 0 }),
      }),
    )
    void sender._onPeerPublishAckReqMessage(JSON.stringify({ text: 'malicious' }), 1)

    expect(seen).toEqual([])
  })
})

describe('lossy broadcast under memory pressure', () => {
  // Another owner's publish sheds it: the channel ends once its own owner runs, but what it holds goes at once.
  it('lets go of the newest it holds for a page that is behind', async () => {
    const broadcast = new ServerBroadcast<string>({ key: 'room:lossy-shed', lossy: true })
    broadcast._registerChannel(ATTACHABLE_NOW)
    broadcast._attachPeer(
      new IndexedPeer({
        sender: {
          sendPublish() {},
          send() {},
          carry() {},
          bufferedAmount: () => 0,
          behindAfter: () => 0,
          deliveredBefore: () => 0,
          sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
          waitingPublishes: () => 0,
          dropUngiven: () => 0,
          dropWaitingPublishes() {},
          dropWaitingFrom() {},
        },
        index: 7,
        replay: defined(broadcast._replayBuffer),
      }),
      { want: { version: 1, text: true, binary: false } },
    )
    await broadcast.publish('first')
    await new Promise((resolve) => setTimeout(resolve, 5))
    await broadcast.publish('x'.repeat(256 * 1024))
    expect(broadcast.newestUnhanded).toBeDefined()
    expect(broadcast.heldBytes).greaterThan(256 * 1024)

    broadcast.shed(0, 'another owner')

    expect(broadcast.newestUnhanded).toBe(undefined)
    expect(broadcast.heldBytes).lessThan(1024)
  })
})

// ───────────────────────────────────────────────────────────────────────────
// DefaultBroadcastAdapter with a custom transport — multi-node behavior. Each
// node has its own DefaultBroadcastAdapter; the transport simulates the bus.
// Bug classes targeted: same-node echo causing double-delivery, cross-node
// delivery silently dropped, transport subscription leaks (no refcount).
// ───────────────────────────────────────────────────────────────────────────

describe('DefaultBroadcastAdapter — multi-node transport', () => {
  type TextListener = (payload: string, info: { seq: number; timestamp: number }) => void

  /** Synchronous-delivery bus for fast, deterministic tests. */
  function createMockTransport(): BroadcastTransport & { _listeners: Map<string, Set<TextListener>> } {
    const listeners = new Map<string, Set<TextListener>>()
    const seqs = new Map<string, number>()
    return {
      _listeners: listeners,
      send(key, payload) {
        const seq = (seqs.get(key) ?? 0) + 1
        seqs.set(key, seq)
        const timestamp = Date.now()
        const set = listeners.get(key)
        if (set) for (const cb of set) cb(payload, { seq, timestamp })
        return { seq, timestamp }
      },
      listen(key, onMessage) {
        let set = listeners.get(key)
        if (!set) {
          set = new Set()
          listeners.set(key, set)
        }
        set.add(onMessage)
        return () => listeners.delete(key)
      },
      sendBinary(_key, _payload) {
        return { seq: 0, timestamp: Date.now() }
      },
      listenBinary() {
        return () => {}
      },
    }
  }

  it('publishes go through the transport and locally-subscribed receivers see the message', async () => {
    const transport = createMockTransport()
    _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter(transport))
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:tx:basic' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:tx:basic' })
    sender._registerChannel(ATTACHABLE_NOW)
    receiver._registerChannel(ATTACHABLE_NOW)

    const received: Array<{ text: string }> = []
    receiver.subscribe((m) => received.push(m))

    const receipt = await sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
    expect(receipt.seq).toBe(1)
    expect(typeof receipt.timestamp).toBe('number')
  })

  it('does not double-deliver when the transport echoes back to the publisher node', () => {
    const transport = createMockTransport()
    _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter(transport))
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:tx:echo' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:tx:echo' })
    sender._registerChannel(ATTACHABLE_NOW)
    receiver._registerChannel(ATTACHABLE_NOW)

    const received: Array<{ text: string }> = []
    receiver.subscribe((m) => received.push(m))

    sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  it('cross-node: a publish from node1 reaches node2 subscribers via the shared transport', () => {
    const transport = createMockTransport()
    const node1 = new DefaultBroadcastAdapter(transport)
    const node2 = new DefaultBroadcastAdapter(transport)

    const received: string[] = []
    node2.subscribe('room:cross-node', (serialized) => received.push(serialized))
    node1.publish('room:cross-node', '{"text":"from-node1"}')

    expect(received).toEqual(['{"text":"from-node1"}'])
  })

  // Refcount: multiple local subscribers must share ONE transport subscription; the
  // upstream subscription must persist until the LAST local subscriber leaves. Otherwise
  // either the transport is over-subscribed (leak) or unsubscribed prematurely (loss).
  it('refcounts the transport subscription: holds while any local subscriber is attached', () => {
    const transport = createMockTransport()
    const adapter = new DefaultBroadcastAdapter(transport)

    const u1 = stopOf(adapter.subscribe('room:rc', () => {}))
    const u2 = stopOf(adapter.subscribe('room:rc', () => {}))
    expect(transport._listeners.get('room:rc')?.size).toBe(1) // exactly one upstream sub for both

    u1()
    expect(transport._listeners.has('room:rc')).toBe(true) // still subscribed for u2

    u2()
    expect(transport._listeners.has('room:rc')).toBe(false) // last out → upstream released
  })

  it('refcount survives sub→unsub→sub: the second subscribe re-establishes the upstream sub', () => {
    const transport = createMockTransport()
    const adapter = new DefaultBroadcastAdapter(transport)

    const u1 = stopOf(adapter.subscribe('room:rc-cycle', () => {}))
    u1()
    expect(transport._listeners.has('room:rc-cycle')).toBe(false)

    const received: string[] = []
    adapter.subscribe('room:rc-cycle', (s) => received.push(s))
    expect(transport._listeners.get('room:rc-cycle')?.size).toBe(1)

    adapter.publish('room:rc-cycle', 'after-resub')
    expect(received).toEqual(['after-resub'])
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Static bus (`Broadcast.*`) — server-only fire-and-forget broadcast. Bypasses
// the instance-lifecycle (no register, no peer) and goes straight to the adapter.
// Bug class: regression where the static bus starts touching instance state.
// ───────────────────────────────────────────────────────────────────────────

describe('Broadcast static bus (publish/subscribe)', () => {
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
})

describe('a subscriber a close callback removes while its backend starts', () => {
  for (const method of ['subscribe', 'subscribeBinary'] as const) {
    it(`${method}(): a callback that awaits its \`opened\` lets the close end gracefully`, async () => {
      const listen = () => ({ unsubscribe() {}, opened: new Promise<void>(() => {}) })
      _resetBroadcastAdapterForTesting(
        new DefaultBroadcastAdapter({
          send: () => ({ seq: 1, timestamp: 1 }),
          sendBinary: () => ({ seq: 1, timestamp: 1 }),
          listen,
          listenBinary: listen,
        }),
      )
      const room = new ServerBroadcast({ key: method })
      const remove = room[method](() => {})
      room.onClose(async () => {
        remove()
        await remove.opened.catch(() => {})
      })
      const closed = room.closed
      await room.close({ timeout: 50 })
      await expect(closed).resolves.toBeNull()
    })
  }
  for (const method of ['subscribe', 'subscribeBinary'] as const) {
    it(`${method}(): a confirmation during the cleanup leaves its \`opened\` rejected with the close`, async () => {
      const backend = Promise.withResolvers<void>()
      const cleanup = Promise.withResolvers<void>()
      const entered = Promise.withResolvers<void>()
      const listen = () => ({ unsubscribe() {}, opened: backend.promise })
      _resetBroadcastAdapterForTesting(
        new DefaultBroadcastAdapter({
          send: () => ({ seq: 1, timestamp: 1 }),
          sendBinary: () => ({ seq: 1, timestamp: 1 }),
          listen,
          listenBinary: listen,
        }),
      )
      const room = new ServerBroadcast({ key: method })
      const remove = room[method](() => {})
      const sibling = room[method](() => {})
      const outcome = remove.opened.then(
        () => 'resolved',
        (err: Error) => err.message,
      )
      room.onClose(() => {
        remove()
        entered.resolve()
        return cleanup.promise
      })
      const close = room.close({ timeout: 500 })
      try {
        await entered.promise
        backend.resolve()
        await sibling.opened.catch(() => {})
      } finally {
        cleanup.resolve()
        await close
      }
      expect(await outcome).toBe('Channel is closed')
    })
  }
})

// When a waiting `opened` settles around its broadcast's close: at the moment its subscription can no longer take effect,
// with what is known of the end then.
describe("a server subscriber's `opened` around its broadcast's graceful close", () => {
  /** A backend whose first start waits for `confirm()`, and whose later ones wait for it too, or are confirmed or refused
   *  at once, as `next` says. */
  const backend = (next: 'pending' | 'confirms' | 'refuses' = 'pending') => {
    const confirmation = Promise.withResolvers<void>()
    let starts = 0
    const listen = () => {
      if (starts++ === 0 || next === 'pending') return { unsubscribe() {}, opened: confirmation.promise }
      if (next === 'confirms') return () => {}
      return { unsubscribe() {}, opened: Promise.reject(new Error('NOPERM')) }
    }
    const sent = () => ({ seq: 1, timestamp: 1 })
    _resetBroadcastAdapterForTesting(
      new DefaultBroadcastAdapter({ send: sent, sendBinary: sent, listen, listenBinary: listen }),
    )
    return { confirm: () => confirmation.resolve() }
  }
  const outcome = (opened: Promise<void> | undefined) =>
    Promise.resolve(opened).then(
      () => 'resolved',
      (err: Error) => err.message,
    )

  for (const kind of ['subscribe', 'subscribeBinary'] as const) {
    it(`${kind}(): one its callback removes, then subscribes again with a start its backend confirms at once, stays rejected; the new one resolves`, async () => {
      backend('confirms')
      const room = new ServerBroadcast({ key: `room:replaced:${kind}` })
      const removed = room[kind](() => {})
      let added: Unsubscribe | undefined
      room.onClose(() => {
        removed()
        added = room[kind](() => {})
      })
      const removedOutcome = outcome(removed.opened)
      await room.close({ timeout: 50 })
      expect({ removed: await removedOutcome, added: await outcome(added?.opened) }).toEqual({
        removed: 'Channel is closed',
        added: 'resolved',
      })
    })

    it(`${kind}(): one its callback removes, then subscribes again with a start its backend refuses, stays rejected with the close`, async () => {
      backend('refuses')
      const room = new ServerBroadcast({ key: `room:replaced-refused:${kind}` })
      const removed = room[kind](() => {})
      let added: Unsubscribe | undefined
      room.onClose(() => {
        removed()
        added = room[kind](() => {})
      })
      const removedOutcome = outcome(removed.opened)
      await room.close({ timeout: 50 }).catch(() => {})
      expect(await removedOutcome).toBe('Channel is closed')
      expect(await outcome(added?.opened)).not.toBe('resolved')
    })

    it(`${kind}(): one its callback removes and replaces, while the close then times out: the removed one has the close, the new one the timeout`, async () => {
      backend()
      const room = new ServerBroadcast({ key: `room:replaced-pending:${kind}` })
      const removed = room[kind](() => {})
      let added: Unsubscribe | undefined
      room.onClose(() => {
        removed()
        added = room[kind](() => {})
        return new Promise<void>(() => {})
      })
      const removedOutcome = outcome(removed.opened)
      await room.close({ timeout: 10 })
      expect({ removed: await removedOutcome, added: await outcome(added?.opened) }).toEqual({
        removed: 'Channel is closed',
        added: 'Channel close timed out',
      })
    })

    it(`${kind}(): one still subscribed while the callbacks' cleanup runs resolves once its backend confirms meanwhile`, async () => {
      const { confirm } = backend()
      const room = new ServerBroadcast({ key: `room:confirmed-in-cleanup:${kind}` })
      const subscriber = room[kind](() => {})
      const cleanup = Promise.withResolvers<void>()
      const entered = Promise.withResolvers<void>()
      room.onClose(() => {
        entered.resolve()
        return cleanup.promise
      })
      const close = room.close({ timeout: 500 })
      await entered.promise
      confirm()
      const subscribed = await outcome(subscriber.opened)
      cleanup.resolve()
      await close
      expect(subscribed).toBe('resolved')
    })

    it(`${kind}(): one that resolved stays resolved when its callback removes it`, async () => {
      backend('confirms')
      const room = new ServerBroadcast({ key: `room:resolved:${kind}` })
      // Its first start waits: one that comes and goes ends it, so the next one starts again and is confirmed.
      room[kind](() => {})()
      const subscriber = room[kind](() => {})
      await subscriber.opened
      room.onClose(() => subscriber())
      await room.close({ timeout: 50 })
      expect(await outcome(subscriber.opened)).toBe('resolved')
    })

    it(`${kind}(): one nobody reads, removed by its callback, makes no unhandled rejection`, async () => {
      backend()
      const room = new ServerBroadcast({ key: `room:unread:${kind}` })
      const subscriber = room[kind](() => {})
      room.onClose(() => subscriber())
      await room.close({ timeout: 50 })
    })
  }
})
