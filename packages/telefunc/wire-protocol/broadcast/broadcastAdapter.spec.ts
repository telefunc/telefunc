import { afterEach, describe, expect, it } from 'vitest'
import { createHook } from 'node:async_hooks'
import { Broadcast, ServerBroadcast } from '../channel/server/ServerBroadcast.js'
import { ServerReplay } from '../channel/server/ServerReplay.js'
import { ACK_STATUS, decode, encode, type ChannelFrame } from '../wire/frame.js'
import { TAG } from '../wire/tags.js'
import { IndexedPeer } from '../channel/server/IndexedPeer.js'
import { Channel } from '../channel/server/ServerChannel.js'
import { getBroadcastAdapter, _resetBroadcastAdapterForTesting, DefaultBroadcastAdapter } from './broadcastAdapter.js'
import type { BroadcastTransport, Listening } from './broadcastAdapter.js'

/** The adapter's subscription as the function that stops it. */
const stopOf = (subscription: Listening) => subscription.unsubscribe
import { ChannelOverflowError } from '../channel/channelErrors.js'
import type { Unsubscribe } from '../channel/channel.js'

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

  it('does not call a subscriber that an earlier one unsubscribed during the same delivery', () => {
    const broadcast = new ServerBroadcast<string>({ key: 'room:unsub-in-delivery' })
    broadcast._registerChannel()

    const seen: string[] = []
    broadcast.subscribe(() => unsubscribe())
    const unsubscribe = broadcast.subscribe((m) => seen.push(m))
    broadcast.publish('first')

    expect(seen).toEqual([])
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
    a._registerChannel()
    b._registerChannel()

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

  it('delivers the bytes an answer published, though its publisher reuses its buffer once publishBinary() returns', async () => {
    const adapter = new DefaultBroadcastAdapter()
    const received: number[][] = []
    const unsubscribeBinary = stopOf(adapter.subscribeBinary('room:reused', (data) => received.push(Array.from(data))))
    const unsubscribe = stopOf(
      adapter.subscribe('room:reused', () => {
        const bytes = new Uint8Array([1, 2, 3])
        void adapter.publishBinary('room:reused', bytes)
        bytes.fill(0)
      }),
    )
    await adapter.publish('room:reused', 'question')
    unsubscribe()
    unsubscribeBinary()
    expect(received).toEqual([[1, 2, 3]])
  })

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
    expect((errors[0] as Error).message).toContain('a listener that answers its own messages')
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
    broadcast._registerChannel()
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
    broadcast._registerChannel()
    const refused: Uint8Array[] = []
    const later: Uint8Array[] = []
    expect(() => broadcast.subscribeBinary((data) => refused.push(data))).toThrow('subscription refused')
    broadcast.subscribeBinary((data) => later.push(data))
    broadcast.publishBinary(new Uint8Array([1, 2, 3]))
    expect(later).toEqual([new Uint8Array([1, 2, 3])])
    expect(refused).toEqual([])
  })
})

// The same rule as a page's `ready`: it resolves once the listener is in effect or removed, and rejects where the
// broadcast ends first. The backend here confirms no subscription until each test is over.
describe("a server subscriber's `ready` while its backend hasn't confirmed the subscription", () => {
  const pending = new Promise<void>(() => {})
  const sent = () => ({ seq: 1, timestamp: 1 })
  const listen = () => ({ unsubscribe: () => {}, ready: pending })
  const unconfirmed = () =>
    _resetBroadcastAdapterForTesting(
      new DefaultBroadcastAdapter({
        send: sent,
        sendBinary: sent,
        listen,
        listenBinary: listen,
      }),
    )
  const settled = async (ready: Promise<void>) => {
    let state = 'pending'
    ready.then(
      () => (state = 'resolved'),
      () => (state = 'rejected'),
    )
    await new Promise((resolve) => setTimeout(resolve, 0))
    return state
  }
  type Subscribe = (signal: AbortSignal) => { remove: () => void; ready: Promise<void>; end?: () => void }
  const instance =
    (binary: boolean): Subscribe =>
    (signal) => {
      const broadcast = new ServerBroadcast<string>({ key: crypto.randomUUID() })
      const remove = binary
        ? broadcast.subscribeBinary(() => {}, { signal })
        : broadcast.subscribe(() => {}, { signal })
      return { remove, ready: remove.ready, end: () => broadcast.abort() }
    }
  const statically =
    (binary: boolean): Subscribe =>
    (signal) => {
      const key = crypto.randomUUID()
      const remove = binary
        ? Broadcast.subscribeBinary(key, () => {}, { signal })
        : Broadcast.subscribe(key, () => {}, { signal })
      return { remove, ready: remove.ready }
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
      expect(await settled(subscriber.ready)).toBe('resolved')
      subscriber.end?.()
    })
    it(`${name}: resolves once removed by its signal`, async () => {
      unconfirmed()
      const controller = new AbortController()
      const subscriber = subscribe(controller.signal)
      controller.abort()
      expect(await settled(subscriber.ready)).toBe('resolved')
      subscriber.end?.()
    })
  }
  const ENDING: Record<string, Subscribe> = { 'subscribe()': instance(false), 'subscribeBinary()': instance(true) }
  for (const [name, subscribe] of Object.entries(ENDING)) {
    it(`${name}: rejects once its broadcast ends first`, async () => {
      unconfirmed()
      const subscriber = subscribe(new AbortController().signal)
      subscriber.end?.()
      expect(await settled(subscriber.ready)).toBe('rejected')
    })
  }
})

// A backend that refused a key's subscription keeps that outcome: removing a subscriber afterwards doesn't turn its
// `ready` into a success, whether the app read `ready` before the removal or not.
describe("a static subscriber's `ready` once its backend refused the key's subscription", () => {
  const sent = () => ({ seq: 1, timestamp: 1 })
  const refusing = () => {
    const ready = Promise.reject(new Error('NOPERM'))
    const listen = () => ({ unsubscribe: () => {}, ready })
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
        let ready: Promise<void> | undefined
        if (removed === 'after') ready = remove.ready
        remove()
        ready ??= remove.ready
        await expect(ready).rejects.toThrow('NOPERM')
      })
    }
  }
})

// A `ready` nobody reads makes no promise, so subscribers that come and go while the backend waits leave nothing behind.
describe("subscribers that come and go while their backend hasn't confirmed the key's subscription", () => {
  const pending = new Promise<void>(() => {})
  const sent = () => ({ seq: 1, timestamp: 1 })
  const listen = () => ({ unsubscribe: () => {}, ready: pending })
  /** How many promises `run` made. */
  const promisesMadeBy = (run: () => void) => {
    let made = 0
    const hook = createHook({
      init(_id, type) {
        if (type === 'PROMISE') made++
      },
    }).enable()
    try {
      run()
    } finally {
      hook.disable()
    }
    return made
  }
  const broadcasts = new Map<string, ServerBroadcast<string>>()
  const broadcastOf = (key: string) => {
    let broadcast = broadcasts.get(key)
    if (broadcast === undefined) broadcasts.set(key, (broadcast = new ServerBroadcast<string>({ key })))
    return broadcast
  }
  const channels = new Map<string, Channel<never, string>>()
  const channelOf = (key: string) => {
    let channel = channels.get(key)
    if (channel === undefined) channels.set(key, (channel = new Channel<never, string>()))
    return channel
  }
  const CHURN: Record<string, (key: string) => Unsubscribe> = {
    'subscribe()': (key) => broadcastOf(key).subscribe(() => {}),
    'subscribeBinary()': (key) => broadcastOf(key).subscribeBinary(() => {}),
    // In effect at once: its `ready`, read, is one promise every such listener shares.
    'Broadcast.subscribe()': (key) => Broadcast.subscribe(key, () => {}),
    'Broadcast.subscribeBinary()': (key) => Broadcast.subscribeBinary(key, () => {}),
    "a channel's listen(), its `ready` read": (key) => {
      const remove = channelOf(key).listen(() => {})
      void remove.ready
      return remove
    },
  }
  for (const [name, subscribe] of Object.entries(CHURN)) {
    it(`${name}: 1,000 subscribers added and removed make no promise`, () => {
      _resetBroadcastAdapterForTesting(
        new DefaultBroadcastAdapter({ send: sent, sendBinary: sent, listen, listenBinary: listen }),
      )
      const key = crypto.randomUUID()
      // Holds the key's subscription, which waits for its backend.
      const holder = subscribe(key)
      const made = promisesMadeBy(() => {
        for (let i = 0; i < 1_000; i++) subscribe(key)()
      })
      expect(made).toBe(0)
      holder()
      broadcasts.get(key)?.abort()
      channels.get(key)?.abort()
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
    await stop.ready
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
    await stop.ready
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
    await stop.ready
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
    await stop.ready
    transport.binary(new Uint8Array([1, 2, 3]), { seq: 1, timestamp: 1 })
    stop()
    expect({ heard, active: transport.active, attempts: transport.attempts }).toEqual({
      heard: ['retry 1,2,3'],
      active: 0,
      attempts: 2,
    })
  })
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
      await stop.ready
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
      await Promise.all(stops.map((stop) => stop.ready))
      await publish()
      for (const stop of stops) stop()
      end?.()
      // Whether the second hears the message it was added on is delivery's to decide, not this case's.
      const laterPublish = { first: heard.first.filter((n) => n === 2), second: heard.second.filter((n) => n === 2) }
      expect({ laterPublish, starts: transport.starts(), listening: transport.listening() }).toEqual({
        laterPublish: { first: [2], second: [2] },
        starts: 1,
        listening: false,
      })
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
    broadcast._registerChannel()

    const frames: Uint8Array[] = []
    broadcast._attachPeer(
      new IndexedPeer({
        sender: {
          send: (frame) => {
            frames.push(frame)
          },
          carry() {},
          bufferedAmount: () => 0,
          behindAfter: () => 700,
          deliveredBefore: () => 0,
          sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
          queue: {
            waitingPublishes: () => 0,
            dropWaitingPublishes() {},
            dropWaitingFrom() {},
          },
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
    sender._registerChannel()
    receiver._registerChannel()

    sender._validators.set('data', () => 'always reject')

    const seen: Array<{ text: string }> = []
    receiver.subscribe((m) => seen.push(m))

    sender._attachPeer(
      new IndexedPeer({
        sender: {
          send: () => {},
          carry() {},
          bufferedAmount: () => 0,
          behindAfter: () => 700,
          deliveredBefore: () => 0,
          sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
          queue: {
            waitingPublishes: () => 0,
            dropWaitingPublishes() {},
            dropWaitingFrom() {},
          },
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
    broadcast._registerChannel()
    broadcast._attachPeer(
      new IndexedPeer({
        sender: {
          send() {},
          carry() {},
          bufferedAmount: () => 0,
          behindAfter: () => 0,
          deliveredBefore: () => 0,
          sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
          queue: {
            waitingPublishes: () => 0,
            dropWaitingPublishes() {},
            dropWaitingFrom() {},
          },
        },
        index: 7,
        replay: broadcast._replayBuffer!,
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
    sender._registerChannel()
    receiver._registerChannel()

    const received: Array<{ text: string }> = []
    receiver.subscribe((m) => received.push(m))

    const receipt = await sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
    expect(receipt.seq).toBe(1)
    expect(typeof receipt.timestamp).toBe('number')
  })

  // The classic distributed-bus footgun: the transport delivers to all subscribers
  // INCLUDING the publisher's own node, AND the adapter delivers locally. Without
  // dedup, every same-node publish fans out twice.
  it('does not double-deliver when the transport echoes back to the publisher node', () => {
    const transport = createMockTransport()
    _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter(transport))
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:tx:echo' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:tx:echo' })
    sender._registerChannel()
    receiver._registerChannel()

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
