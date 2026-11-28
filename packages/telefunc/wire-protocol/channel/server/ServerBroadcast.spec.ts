import { ATTACHABLE_NOW } from './ServerChannel.js'
import { afterEach, assert, describe, expect, it, test, vi } from 'vitest'
import { createHook } from 'node:async_hooks'
import { getEventListeners } from 'node:events'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { ServerBroadcastEnd } from './ServerBroadcastEnd.js'
import { ServerReplay } from './ServerReplay.js'
import { decode, encode, type DecodedFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import {
  configureBroadcastTransport,
  disposeBackend,
  getBroadcastPlane,
  installBackend,
} from '../../backend/install.js'
import type { BroadcastTransport, BroadcastUnsubscribe } from '../../backend/broadcast/TransportBroadcastDriver.js'
import { MemoryBackend } from '../../backend/memory/MemoryBackend.js'
import { DriverAttempt, type BackendReceiver, type BroadcastRoute } from '../../backend/driver.js'
import { onBug } from '../../../node/server/runTelefunc/onBug.js'
import { collectGarbage } from '../../test/collectGarbage.js'
import { binaryData } from '../broadcastPayload.js'
import { ChannelClosedError } from '../channelErrors.js'
import { BackendError } from '../../backend/BackendError.js'
import type { Unsubscribe } from '../channel.js'

import { ClientBroadcast } from '../client/ClientBroadcast.js'
import { getChannelMux } from './ChannelMux.js'
import { parseSseRequestMetadata } from '../../wire/envelope.js'
import { BodyReader } from '../../wire/BodyReader.js'
import { queueMemory } from './channelMemory.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { CHANNEL_TRANSPORT, SSE_METADATA_MAX_BYTES, WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'
import { createDeferred, type Deferred } from '../../../utils/createDeferred.js'

afterEach(() => disposeBackend())

describe("a server subscriber's `opened` while its backend hasn't confirmed the subscription", () => {
  const pending = new Promise<void>(() => {})
  const sent = () => ({ seq: 1, timestamp: 1 })
  const listen = () => ({ unsubscribe: () => {}, opened: pending })
  const unconfirmed = () =>
    configureBroadcastTransport({
      send: sent,
      sendBinary: sent,
      listen,
      listenBinary: listen,
    })
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
      return { remove, ready: remove.opened, end: () => broadcast.abort() }
    }
  const statically =
    (binary: boolean): Subscribe =>
    (signal) => {
      const key = crypto.randomUUID()
      const remove = binary
        ? Broadcast.subscribeBinary(key, () => {}, { signal })
        : Broadcast.subscribe(key, () => {}, { signal })
      return { remove, ready: remove.opened }
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
// `opened` into a success, whether the app read `opened` before the removal or not.
describe("a static subscriber's `opened` once its backend refused the key's subscription", () => {
  const sent = () => ({ seq: 1, timestamp: 1 })
  const refusing = () => {
    const ready = Promise.reject(new Error('NOPERM'))
    const listen = () => ({ unsubscribe: () => {}, opened: ready })
    configureBroadcastTransport({ send: sent, sendBinary: sent, listen, listenBinary: listen })
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
        if (removed === 'after') ready = remove.opened
        remove()
        ready ??= remove.opened
        await expect(ready).rejects.toMatchObject({ cause: { message: 'NOPERM' } })
      })
    }
    it(`${name}: rejects with the store's BackendError itself`, async () => {
      const refusal = new BackendError({ code: 'denied', outcome: 'may-have-run', message: 'refused' })
      const ready = Promise.reject(refusal)
      const listen = () => ({ unsubscribe: () => {}, opened: ready })
      configureBroadcastTransport({ send: sent, sendBinary: sent, listen, listenBinary: listen })
      await expect(subscribe().opened).rejects.toBe(refusal)
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
    configureBroadcastTransport({
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
    })
    return state
  }

  // The throw is the start's failure, as a refusal is: a static subscriber is removed, its `opened` keeping the throw, so
  // the one an app subscribes on the rejection is its only subscription; a `BroadcastChannel` ends, its subscription not
  // in effect.
  for (const binary of [false, true]) {
    it(`Broadcast.${binary ? 'subscribeBinary' : 'subscribe'}(): \`opened\` rejects with the throw and stays rejected, and the one an app subscribes on the rejection hears each publish once`, async () => {
      const transport = failingFirst()
      const heard: string[] = []
      const subscribe = () =>
        binary
          ? Broadcast.subscribeBinary('room:throws', (data) => heard.push(`${data}`))
          : Broadcast.subscribe<string>('room:throws', (text) => heard.push(text))
      const failed = subscribe()
      await expect(failed.opened).rejects.toMatchObject({ cause: { message: 'broker connection closed' } })
      const again = subscribe()
      await again.opened
      if (binary) transport.binary(new Uint8Array([1, 2, 3]), { seq: 1, timestamp: 1 })
      else transport.text('"hello"', { seq: 1, timestamp: 1 })
      failed()
      await expect(failed.opened).rejects.toMatchObject({ cause: { message: 'broker connection closed' } })
      again()
      expect({ heard, active: transport.active, attempts: transport.attempts }).toEqual({
        heard: [binary ? '1,2,3' : 'hello'],
        active: 0,
        attempts: 2,
      })
    })
  }

  it('Broadcast.subscribe(): a subscriber whose start failed stays removed when a transport replaces the plane', async () => {
    failingFirst()
    const failed = Broadcast.subscribe('room:throws', () => {})
    await expect(failed.opened).rejects.toThrow()
    let listens = 0
    const listen = () => {
      listens++
      return () => {}
    }
    const sent = () => ({ seq: 1, timestamp: 1 })
    configureBroadcastTransport({ send: sent, sendBinary: sent, listen, listenBinary: listen })
    failed()
    expect(listens).toBe(0)
  })

  for (const binary of [false, true]) {
    it(`BroadcastChannel.${binary ? 'subscribeBinary' : 'subscribe'}(): the broadcast ends, its subscription not in effect, and the transport holds no listener`, async () => {
      const transport = failingFirst()
      const room = new ServerBroadcast<string>({ key: 'room:throws' })
      const stop = binary ? room.subscribeBinary(() => {}) : room.subscribe(() => {})
      await expect(stop.opened).rejects.toThrow()
      expect(await room.closed).toBeInstanceOf(Error)
      expect({ active: transport.active, attempts: transport.attempts }).toEqual({ active: 0, attempts: 1 })
    })
  }
})

// A transport that keeps one listener per key, as most do, would refuse the key's next `listen()` while it still holds
// the refused one.
describe("a transport's listener whose broker refuses it", () => {
  /** A transport whose broker refuses each listener it holds once `refuse()` runs, counting its listeners and stops. */
  const refusingBroker = () => {
    const state = { listens: 0, stops: 0, refusals: new Set<(err: Error) => void>() }
    const listen = () => {
      state.listens++
      const ready = Promise.withResolvers<void>()
      state.refusals.add(ready.reject)
      return { unsubscribe: () => void state.stops++, opened: ready.promise }
    }
    const sent = () => ({ seq: 1, timestamp: 1 })
    configureBroadcastTransport({ send: sent, sendBinary: sent, listen, listenBinary: listen })
    const refuse = () => {
      for (const reject of state.refusals) reject(new Error('NOPERM'))
      state.refusals.clear()
    }
    return { state, refuse }
  }
  const SURFACES: Record<string, () => { ready: Promise<void>; end: () => void }> = {
    'Broadcast.subscribe()': () => {
      const remove = Broadcast.subscribe('room:refused-listener', () => {})
      return { ready: remove.opened, end: remove }
    },
    'Broadcast.subscribeBinary()': () => {
      const remove = Broadcast.subscribeBinary('room:refused-listener', () => {})
      return { ready: remove.opened, end: remove }
    },
    'BroadcastChannel.subscribe()': () => {
      const broadcast = new ServerBroadcast({ key: 'room:refused-listener' })
      return { ready: broadcast.subscribe(() => {}).opened, end: () => broadcast.abort() }
    },
  }
  for (const [surface, subscribe] of Object.entries(SURFACES)) {
    it(`${surface}: the transport is told to stop each listener its broker refused, once`, async () => {
      const { state, refuse } = refusingBroker()
      const subscriber = subscribe()
      refuse()
      await expect(subscriber.ready).rejects.toThrow()
      // Any listener started after the refusal is refused too.
      await vi.waitFor(() => {
        refuse()
        expect(state.stops).toBe(state.listens)
      })
      subscriber.end()
      expect(state.stops).toBe(state.listens)
    })
    it(`${surface}: a listener the broker refuses after it was removed is stopped once`, async () => {
      const { state, refuse } = refusingBroker()
      subscribe().end()
      refuse()
      await new Promise((resolve) => setTimeout(resolve, 0))
      expect({ listens: state.listens, stops: state.stops }).toEqual({ listens: 1, stops: 1 })
    })
  }

  /** A broker that holds a key from `listen()` until its `unsubscribe()`, and refuses another listener of a key it holds;
   *  until `accepts` is set, it refuses each new one in its `opened`, or in `listen()` itself, which then holds nothing. */
  const reservingBroker = (refusesInListen: boolean) => {
    const state = { accepts: false, unsubscribes: 0, held: new Set<string>() }
    const listen = (key: string) => {
      if (state.held.has(key))
        return { unsubscribe: () => {}, opened: Promise.reject(new Error('attempt still reserved')) }
      if (!state.accepts && refusesInListen) throw new Error('NOPERM')
      state.held.add(key)
      const unsubscribe = () => {
        state.unsubscribes++
        state.held.delete(key)
      }
      return { unsubscribe, opened: state.accepts ? Promise.resolve() : Promise.reject(new Error('NOPERM')) }
    }
    const sent = () => ({ seq: 1, timestamp: 1 })
    configureBroadcastTransport({ send: sent, sendBinary: sent, listen, listenBinary: listen })
    return state
  }
  // A broker may keep the promise it gave pending for good, as one whose connection dropped does: what telefunc attached
  // to it holds no stopped listener.
  // Made here: an error made in the stop would keep the stop reachable from its stack, and the log keeps the error.
  const teardownFailed = new Error('teardown failed')
  // The broker keeps the stop it never answers.
  const unanswered = new Promise<void>(() => {})
  const STOPS = {
    'never settles': () => unanswered,
    rejects: () => Promise.reject(teardownFailed),
    throws: () => {
      throw teardownFailed
    },
  }
  for (const binary of [false, true]) {
    const surface = binary ? 'subscribeBinary' : 'subscribe'
    for (const [stop, stopped] of Object.entries(STOPS)) {
      it(`Broadcast.${surface}(): a transport's \`unsubscribe()\` that ${stop} is called once a round, its failure logged, and keeps no listener reachable`, async () => {
        const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
        try {
          const state = await subscribeRounds(binary, (onMessage, counted) => () => {
            // As a broker client's stop refers to the listener it stops.
            void onMessage
            counted.stops++
            return stopped()
          })
          await collectGarbage()
          expect({ stops: state.stops, logged: logged.mock.calls.length, reachable: state.reachable() }).toEqual({
            stops: 8,
            logged: stop === 'never settles' ? 0 : 8,
            reachable: 0,
          })
        } finally {
          logged.mockRestore()
        }
      })
    }
    it(`Broadcast.${surface}(): a transport's listener stopped while its broker keeps \`opened\` pending keeps none of it reachable`, async () => {
      const pending = new Promise<void>(() => {})
      const state = await subscribeRounds(binary, (onMessage) => ({
        unsubscribe: () => void onMessage,
        opened: pending,
      }))
      await collectGarbage()
      expect(state.reachable()).toBe(0)
    })
    it(`Broadcast.${surface}(): a transport's listener its broker confirmed, then stopped, keeps none of it reachable`, async () => {
      const state = await subscribeRounds(
        binary,
        (onMessage) => ({ unsubscribe: () => void onMessage, opened: Promise.resolve() }),
        'once ready',
      )
      await collectGarbage()
      expect(state.reachable()).toBe(0)
    })
  }
  /** Eight rounds of subscribing and unsubscribing on a transport whose listener is `listening(onMessage)`: each
   *  subscriber removed at once, or once its `opened` resolved. */
  const subscribeRounds = async (
    binary: boolean,
    listening: (onMessage: object, counted: { stops: number }) => BroadcastUnsubscribe,
    removed: 'at once' | 'once ready' = 'at once',
  ) => {
    const listeners: WeakRef<object>[] = []
    const state = { stops: 0, reachable: () => listeners.filter((listener) => listener.deref() !== undefined).length }
    const listen = (_key: string, onMessage: object) => {
      listeners.push(new WeakRef(onMessage))
      return listening(onMessage, state)
    }
    const sent = () => ({ seq: 1, timestamp: 1 })
    configureBroadcastTransport({ send: sent, sendBinary: sent, listen, listenBinary: listen })
    for (let round = 0; round < 8; round++) {
      const remove = binary
        ? Broadcast.subscribeBinary('room:held', () => {})
        : Broadcast.subscribe('room:held', () => {})
      if (removed === 'once ready') await remove.opened
      remove()
    }
    return state
  }
  // A transport may deliver what its broker already holds while `listen()` runs, and the listener may replace the
  // transport on that message.
  for (const binary of [false, true]) {
    it(`Broadcast.${binary ? 'subscribeBinary' : 'subscribe'}(): a subscriber whose listener replaces the transport on a message delivered during \`listen()\` is subscribed on the new one`, async () => {
      const KEY = 'room:replaced-in-listen'
      const encode = (text: string) => new TextEncoder().encode(text)
      const publish = (text: string) =>
        binary ? Broadcast.publishBinary(KEY, encode(text)) : Broadcast.publish(KEY, text)
      const info = { seq: 1, timestamp: 1 }
      const held: { text?: string; binary?: Uint8Array } = {}
      const holding: BroadcastTransport = {
        send: (_key, payload) => {
          held.text = payload
          return info
        },
        sendBinary: (_key, payload) => {
          held.binary = payload.slice()
          return info
        },
        listen: (_key, onMessage) => {
          if (held.text !== undefined) onMessage(held.text, info)
          return () => {}
        },
        listenBinary: (_key, onMessage) => {
          if (held.binary !== undefined) onMessage(held.binary, info)
          return () => {}
        },
      }
      configureBroadcastTransport(holding)
      await publish('held')
      const relayed: { text: ((payload: string) => void)[]; binary: ((payload: Uint8Array) => void)[] } = {
        text: [],
        binary: [],
      }
      const relaying: BroadcastTransport = {
        send: (_key, payload) => {
          for (const onMessage of relayed.text) onMessage(payload)
          return { seq: 2, timestamp: 1 }
        },
        sendBinary: (_key, payload) => {
          for (const onMessage of relayed.binary) onMessage(payload)
          return { seq: 2, timestamp: 1 }
        },
        listen: (_key, onMessage) => {
          relayed.text.push((payload) => onMessage(payload, { seq: 2, timestamp: 1 }))
          return () => {}
        },
        listenBinary: (_key, onMessage) => {
          relayed.binary.push((payload) => onMessage(payload, { seq: 2, timestamp: 1 }))
          return () => {}
        },
      }
      const heard: string[] = []
      const hear = (text: string) => {
        heard.push(text)
        if (text === 'held') configureBroadcastTransport(relaying)
      }
      const subscriber = binary
        ? Broadcast.subscribeBinary(KEY, (data) => hear(new TextDecoder().decode(data)))
        : Broadcast.subscribe<string>(KEY, hear)
      const ready = await Promise.race([
        subscriber.opened.then(() => 'resolved'),
        new Promise((resolve) => setTimeout(() => resolve('pending'), 100)),
      ])
      await publish('after')
      subscriber()
      expect({ ready, listens: relayed[binary ? 'binary' : 'text'].length, heard }).toEqual({
        ready: 'resolved',
        listens: 1,
        heard: ['held', 'after'],
      })
    })
  }

  // A broker client's subscription object, whose `unsubscribe()` uses its own state.
  class BrokerSubscription {
    stops = 0
    readonly opened: Promise<void>
    constructor(opened: Promise<void>) {
      this.opened = opened
    }
    unsubscribe(): void {
      this.stops++
    }
  }
  for (const binary of [false, true]) {
    for (const path of ['removed', 'refused'] as const) {
      it(`Broadcast.${binary ? 'subscribeBinary' : 'subscribe'}(): a listener ${path} whose transport returned an object is stopped through that object's \`unsubscribe()\`, once`, async () => {
        const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
        try {
          const subscriptions: BrokerSubscription[] = []
          const listen = () => {
            const subscription = new BrokerSubscription(
              path === 'refused' ? Promise.reject(new Error('NOPERM')) : Promise.resolve(),
            )
            subscriptions.push(subscription)
            return subscription
          }
          const sent = () => ({ seq: 1, timestamp: 1 })
          configureBroadcastTransport({ send: sent, sendBinary: sent, listen, listenBinary: listen })
          const subscriber = binary
            ? Broadcast.subscribeBinary('room:object-stop', () => {})
            : Broadcast.subscribe('room:object-stop', () => {})
          await subscriber.opened.catch(() => {})
          subscriber()
          await new Promise((resolve) => setTimeout(resolve, 0))
          const typeErrors = logged.mock.calls.filter(([error]) => error instanceof TypeError)
          expect({ stops: subscriptions.map(({ stops }) => stops), typeErrors }).toEqual({ stops: [1], typeErrors: [] })
        } finally {
          logged.mockRestore()
        }
      })
    }
  }
  // A transport may stop its listener asynchronously, as one waiting for its broker's acknowledgment does.
  for (const binary of [false, true]) {
    for (const path of ['refused', 'removed'] as const) {
      it(`Broadcast.${binary ? 'subscribeBinary' : 'subscribe'}(): what an async \`unsubscribe()\` rejects with once its listener was ${path} is logged, not left unhandled`, async () => {
        const teardown = new Error('broker refused async teardown')
        const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
        const unhandled: unknown[] = []
        const onUnhandled = (reason: unknown) => unhandled.push(reason)
        process.on('unhandledRejection', onUnhandled)
        try {
          const listen = () => ({
            opened: path === 'refused' ? Promise.reject(new Error('NOPERM')) : Promise.resolve(),
            unsubscribe: async () => {
              throw teardown
            },
          })
          const sent = () => ({ seq: 1, timestamp: 1 })
          configureBroadcastTransport({ send: sent, sendBinary: sent, listen, listenBinary: listen })
          const subscriber = binary
            ? Broadcast.subscribeBinary('room:async-stop', () => {})
            : Broadcast.subscribe('room:async-stop', () => {})
          await subscriber.opened.catch(() => {})
          if (path === 'removed') subscriber()
          await new Promise((resolve) => setTimeout(resolve, 10))
          expect({ unhandled, logged: logged.mock.calls.some(([error]) => error === teardown) }).toEqual({
            unhandled: [],
            logged: true,
          })
        } finally {
          process.off('unhandledRejection', onUnhandled)
          logged.mockRestore()
        }
      })
    }
  }

  // A broker may refuse with a status of its own, which may not even convert to a string.
  const STATUSES = {
    'a plain object': () => ({ code: 'NOPERM', detail: 'subscription ACL changed' }),
    'an object without a prototype': () => ({ __proto__: null, code: 'NOPERM', detail: 'subscription ACL changed' }),
  }
  for (const [given, status] of Object.entries(STATUSES)) {
    it(`a broker that rejects a listener's \`ready\` with ${given} gives the subscriber that value as the cause, and its listener is stopped`, async () => {
      const refused = status()
      const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
      const unhandled: unknown[] = []
      const onUnhandled = (reason: unknown) => unhandled.push(reason)
      process.on('unhandledRejection', onUnhandled)
      try {
        let stops = 0
        const listen = () => ({ opened: Promise.reject(refused), unsubscribe: () => void stops++ })
        const sent = () => ({ seq: 1, timestamp: 1 })
        configureBroadcastTransport({ send: sent, sendBinary: sent, listen, listenBinary: listen })
        const subscriber = Broadcast.subscribe('room:broker-status', () => {})
        const cause = await subscriber.opened.then(
          () => 'resolved',
          (err: unknown) => err instanceof Error && err.cause,
        )
        await new Promise((resolve) => setTimeout(resolve, 10))
        expect({ causeIsStatus: cause === refused, stops, unhandled }).toEqual({
          causeIsStatus: true,
          stops: 1,
          unhandled: [],
        })
      } finally {
        process.off('unhandledRejection', onUnhandled)
        logged.mockRestore()
      }
    })
  }

  for (const refusesInListen of [false, true]) {
    it(`the backend's subscribe(): a listener the broker refused ${refusesInListen ? 'in `listen()` gets no `unsubscribe()` call, as it returned none' : 'in its `opened` gets one `unsubscribe()` call'}, and the next one is accepted`, async () => {
      const broker = reservingBroker(refusesInListen)
      const route = { key: 'room:reserved', kind: 'text' } as const
      const refused = getBroadcastPlane().subscribe(route, () => {})
      await expect(refused.ready).rejects.toMatchObject({ cause: { message: 'NOPERM' } })
      broker.accepts = true
      const accepted = getBroadcastPlane().subscribe(route, () => {})
      await accepted.ready
      const unsubscribes = broker.unsubscribes
      void accepted.unsubscribe()
      expect({ unsubscribes, held: [...broker.held] }).toEqual({ unsubscribes: refusesInListen ? 0 : 1, held: [] })
    })
  }
})

/** The failures the server reported through `onBug()`, by the message of their cause. */
const reported: string[] = []
onBug((err) => reported.push(err instanceof Error && err.cause instanceof Error ? err.cause.message : String(err)))

/** Ready at once, as a memory subscription is, until the test loses it or ends it, as a backend does that loses its
 *  connection or whose renewal is refused. */
class SteeredAttempt extends DriverAttempt {
  private readonly inner: DriverAttempt

  constructor(inner: DriverAttempt) {
    super()
    this.inner = inner
    this.transition('ready')
  }

  steer(state: 'ready' | 'lost' | 'closed', reason?: Error): void {
    this.transition(state, reason)
    if (state === 'closed') void this.inner.unsubscribe()
  }

  unsubscribe(): Promise<void> {
    this.transition('closed')
    return this.inner.unsubscribe()
  }
}

class BackendOfSteeredAttempts extends MemoryBackend {
  readonly attempts: SteeredAttempt[] = []

  subscribe(route: BroadcastRoute, receiver: BackendReceiver): DriverAttempt {
    const attempt = new SteeredAttempt(super.subscribe(route, receiver))
    this.attempts.push(attempt)
    return attempt
  }
}

// A backend renews a subscription that lost its connection, and closes one with a reason only once it can't: the
// subscription is over, and a fresh `subscribe()` starts another.
describe('a server-side subscription in effect whose backend ends it', () => {
  const KEY = 'room:in-effect'
  const STATIC: Record<string, (heard: string[], signal?: AbortSignal) => Unsubscribe> = {
    'Broadcast.subscribe()': (heard, signal) =>
      Broadcast.subscribe<string>(KEY, (text) => heard.push(text), { signal }),
    'Broadcast.subscribeBinary()': (heard, signal) =>
      Broadcast.subscribeBinary(KEY, (data) => heard.push(new TextDecoder().decode(data)), { signal }),
  }
  const publish = (binary: boolean, text: string) =>
    binary ? Broadcast.publishBinary(KEY, new TextEncoder().encode(text)) : Broadcast.publish(KEY, text)
  for (const [surface, subscribe] of Object.entries(STATIC)) {
    const binary = surface.includes('Binary')
    it(`${surface}: is removed, its \`ready\` resolved and its signal let go, the failure reported once, and a fresh one hears each publish once`, async () => {
      reported.length = 0
      const backend = installBackend(() => new BackendOfSteeredAttempts(), [crypto.randomUUID()])
      const heard: string[] = []
      const signal = new AbortController().signal
      const ended = subscribe(heard, signal)
      await ended.opened
      backend.attempts[0]?.steer('closed', new Error('NOPERM renewal refused'))
      await vi.waitFor(() => expect(getEventListeners(signal, 'abort')).toHaveLength(0))
      await publish(binary, 'unheard')
      const fresh = subscribe(heard)
      await fresh.opened
      await publish(binary, 'heard')
      ended()
      fresh()
      await expect(ended.opened).resolves.toBeUndefined()
      expect({ heard, attempts: backend.attempts.length, reported }).toEqual({
        heard: ['heard'],
        attempts: 2,
        reported: ['NOPERM renewal refused'],
      })
    })

    it(`${surface}: stays subscribed through a loss, and hears what is published once it is back`, async () => {
      reported.length = 0
      const backend = installBackend(() => new BackendOfSteeredAttempts(), [crypto.randomUUID()])
      const heard: string[] = []
      const stop = subscribe(heard)
      await stop.opened
      backend.attempts[0]?.steer('lost')
      backend.attempts[0]?.steer('ready')
      await publish(binary, 'after')
      stop()
      expect({ heard, attempts: backend.attempts.length, reported }).toEqual({
        heard: ['after'],
        attempts: 1,
        reported: [],
      })
    })
  }

  for (const [surface, subscribe] of Object.entries(STATIC)) {
    it(`${surface}: keeps its resolved \`ready\` and lets its signal go where a transport that replaced its own refuses it`, async () => {
      const sent = () => ({ seq: 1, timestamp: 1 })
      const accepted = () => () => {}
      configureBroadcastTransport({ send: sent, sendBinary: sent, listen: accepted, listenBinary: accepted })
      const signal = new AbortController().signal
      const subscriber = subscribe([], signal)
      await subscriber.opened
      const refused = () => ({ unsubscribe() {}, opened: Promise.reject(new Error('NOPERM: replacement refused')) })
      configureBroadcastTransport({ send: sent, sendBinary: sent, listen: refused, listenBinary: refused })
      await vi.waitFor(() => expect(getEventListeners(signal, 'abort')).toHaveLength(0))
      await expect(subscriber.opened).resolves.toBeUndefined()
    })
  }

  it("BroadcastChannel: closes with the backend's error", async () => {
    const backend = installBackend(() => new BackendOfSteeredAttempts(), [crypto.randomUUID()])
    const room = new ServerBroadcast<string>({ key: KEY })
    await room.subscribe(() => {}).opened
    backend.attempts[0]?.steer('closed', new Error('NOPERM renewal refused'))
    expect(await room.closed).toMatchObject({ cause: { message: 'NOPERM renewal refused' } })
    expect(backend.attempts).toHaveLength(1)
  })
})

// One failed attempt is one report, however many of the server's subscriptions shared it and whether any read `opened`.
describe('a refused attempt that server-side subscriptions of one key share', () => {
  const KEY = 'room:shared-refusal'
  /** A transport whose broker refuses each listener it holds once `refuse()` runs, with `reason`. */
  const refusingBroker = () => {
    const state = { listens: 0, stops: 0 }
    const refusals = new Set<(reason?: Error) => void>()
    const listen = () => {
      state.listens++
      const ready = Promise.withResolvers<void>()
      refusals.add(ready.reject)
      return { unsubscribe: () => void state.stops++, opened: ready.promise }
    }
    const sent = () => ({ seq: 1, timestamp: 1 })
    configureBroadcastTransport({ send: sent, sendBinary: sent, listen, listenBinary: listen })
    const refuse = (reason?: Error) => {
      for (const reject of refusals) reject(reason)
      refusals.clear()
    }
    return { state, refuse }
  }

  it('ends each of them, stops the listener once, and is reported once', async () => {
    reported.length = 0
    const { state, refuse } = refusingBroker()
    const controller = new AbortController()
    const first = Broadcast.subscribe(KEY, () => {})
    const second = Broadcast.subscribe(KEY, () => {}, { signal: controller.signal })
    const room = new ServerBroadcast({ key: KEY })
    room.subscribe(() => {})
    // A subscriber that removes another on its own rejection.
    void first.opened.catch(() => controller.abort())
    refuse(new Error('NOPERM'))
    await expect(first.opened).rejects.toThrow()
    expect(await room.closed).toBeInstanceOf(Error)
    await new Promise((resolve) => setTimeout(resolve, 0))
    second()
    expect({ ...state, reported }).toEqual({ listens: 1, stops: 1, reported: ['NOPERM'] })
  })

  for (const binary of [false, true]) {
    const subscribe = () => (binary ? Broadcast.subscribeBinary(KEY, () => {}) : Broadcast.subscribe(KEY, () => {}))
    it(`${binary ? 'subscribeBinary' : 'subscribe'}(): is reported where the broker refuses with no reason in its \`opened\``, async () => {
      reported.length = 0
      const { refuse } = refusingBroker()
      const subscriber = subscribe()
      refuse()
      await expect(subscriber.opened).rejects.toThrow()
      await new Promise((resolve) => setTimeout(resolve, 0))
      expect(reported).toHaveLength(1)
    })

    it(`${binary ? 'subscribeBinary' : 'subscribe'}(): is reported where \`listen()\` throws with no reason`, async () => {
      reported.length = 0
      const throwing = () => {
        throw undefined
      }
      const sent = () => ({ seq: 1, timestamp: 1 })
      configureBroadcastTransport({ send: sent, sendBinary: sent, listen: throwing, listenBinary: throwing })
      const subscriber = subscribe()
      await expect(subscriber.opened).rejects.toThrow()
      await new Promise((resolve) => setTimeout(resolve, 0))
      expect(reported).toHaveLength(1)
    })
  }

  it('is not reported once every subscription left before the refusal', async () => {
    reported.length = 0
    const { refuse } = refusingBroker()
    Broadcast.subscribe(KEY, () => {})()
    refuse(new Error('NOPERM'))
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(reported).toEqual([])
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
    configureBroadcastTransport({
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
    })
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

  // A message comes to a consumer only once its start is ready, so one of a start that failed reaches no listener.
  for (const [surface, of] of Object.entries(SURFACES)) {
    it(`${surface}: that message reaches no listener, and only what the next start delivers does`, async () => {
      const transport = deliveringThenThrowing()
      const { subscribe, publish, end } = of()
      const heard: number[] = []
      const stop = subscribe((n) => heard.push(n))
      await expect(stop.opened).rejects.toThrow()
      if (end === undefined) {
        const again = subscribe((n) => heard.push(n))
        await again.opened
        await publish()
        again()
      } else end()
      expect({ heard, listening: transport.listening() }).toEqual({ heard: end ? [] : [2], listening: 0 })
    })
  }

  // An app that subscribes with one long-lived signal: each attempt that fails must leave nothing on it.
  const WITH_SIGNAL: Record<string, (signal: AbortSignal) => (onMessage: () => void) => Unsubscribe> = {
    'Broadcast.subscribe()': (signal) => (onMessage) => Broadcast.subscribe(KEY, onMessage, { signal }),
    'Broadcast.subscribeBinary()': (signal) => (onMessage) => Broadcast.subscribeBinary(KEY, onMessage, { signal }),
    'BroadcastChannel.subscribe()': (signal) => {
      const broadcast = new ServerBroadcast({ key: KEY })
      return (onMessage) => broadcast.subscribe(onMessage, { signal })
    },
    'BroadcastChannel.subscribeBinary()': (signal) => {
      const broadcast = new ServerBroadcast({ key: KEY })
      return (onMessage) => broadcast.subscribeBinary(onMessage, { signal })
    },
  }
  // A failed start ends a `BroadcastChannel`, which removes its listeners, and removes a static subscriber.
  for (const [surface, of] of Object.entries(WITH_SIGNAL)) {
    it(`${surface}: a ${surface.startsWith('Broadcast.') ? 'subscriber' : 'listener'} with a signal leaves no abort listener on it once its start failed`, async () => {
      deliveringThenThrowing()
      const signal = new AbortController().signal
      const subscribed = of(signal)(() => {})
      await expect(subscribed.opened).rejects.toThrow()
      expect(getEventListeners(signal, 'abort')).toHaveLength(0)
    })
  }

  // One receiver subscribed again on a key's route after a start that failed: each subscription is its own consumer of
  // the slot it joined, and a failed start's slot is left, so the first one's remover leaves the later one be.
  for (const kind of ['text', 'binary'] as const) {
    it(`the backend's ${kind} subscribe(): the remover of a receiver's subscription the failed start closed leaves that receiver's later subscription be`, async () => {
      const transport = deliveringThenThrowing()
      const heard: number[] = []
      const receiver = (payload: string | Uint8Array) =>
        typeof payload === 'string' ? heard.push(Number(payload)) : heard.push(...binaryData(payload))
      const route = { key: KEY, kind }
      const failed = getBroadcastPlane().subscribe(route, receiver)
      await expect(failed.ready).rejects.toThrow()
      const later = getBroadcastPlane().subscribe(route, receiver)
      await later.ready
      void failed.unsubscribe()
      await (kind === 'binary' ? Broadcast.publishBinary(KEY, Uint8Array.of(2)) : Broadcast.publish(KEY, 2))
      const listening = transport.listening()
      void later.unsubscribe()
      expect({ heard, listening }).toEqual({ heard: [2], listening: 1 })
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
    configureBroadcastTransport({
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
    })
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

// A message its backend delivers before confirming the subscription waits for that confirmation, whether it came inside
// listen() or after it: until then no listener hears it and `opened` stays pending.
describe('a message its backend delivers before it confirmed the subscription', () => {
  for (const binary of [false, true]) {
    for (const inside of [true, false]) {
      it(`${binary ? 'subscribeBinary()' : 'subscribe()'}, the message ${inside ? 'inside' : 'after'} listen(): the listener hears it once the backend confirms`, async () => {
        let deliver = () => {}
        const confirmed = Promise.withResolvers<void>()
        const start = (deliverFirst: () => void) => {
          deliver = deliverFirst
          if (inside) deliver()
          return { unsubscribe: () => {}, opened: confirmed.promise }
        }
        const sent = () => ({ seq: 1, timestamp: 1 })
        configureBroadcastTransport({
          send: sent,
          sendBinary: sent,
          listen: (_key, onMessage) => start(() => onMessage('1', sent())),
          listenBinary: (_key, onMessage) => start(() => onMessage(Uint8Array.of(1), sent())),
        })
        const room = new ServerBroadcast<string>({ key: 'room:before-confirmed' })
        const order: string[] = []
        const stop = binary
          ? room.subscribeBinary(() => order.push('heard'))
          : room.subscribe(() => order.push('heard'))
        void stop.opened.then(() => order.push('ready'))
        if (!inside) deliver()
        await new Promise((resolve) => setTimeout(resolve, 0))
        expect(order).toEqual([])
        confirmed.resolve()
        await vi.waitFor(() => expect(order).toEqual(['heard', 'ready']))
        room.abort()
      })
    }
  }
})

// An app may clean its listeners up in `onClose()`: a listener whose `opened` was pending when the broadcast ended still
// gets that end, whatever a close callback does after.
describe('a broadcast that ends while a subscriber waits, its `onClose()` removing that subscriber', () => {
  for (const binary of [false, true]) {
    it(`${binary ? 'subscribeBinary()' : 'subscribe()'}: \`ready\` rejects with the end`, async () => {
      const sent = () => ({ seq: 1, timestamp: 1 })
      const listen = () => ({ unsubscribe() {}, opened: new Promise<void>(() => {}) })
      configureBroadcastTransport({ send: sent, sendBinary: sent, listen, listenBinary: listen })
      const room = new ServerBroadcast<string>({ key: 'room:cleans-up-on-close' })
      const stop = binary ? room.subscribeBinary(() => {}) : room.subscribe(() => {})
      room.onClose(() => stop())
      room.abort('terminal event')
      const ready = await stop.opened.then(
        () => 'resolved',
        (err: unknown) => err,
      )
      expect(ready).toBe(await room.closed.catch((err: unknown) => err))
    })
  }
})

// Its close's outcome is decided after its callbacks start: here the cleanup one of them runs outlasts the close.
describe("a broadcast whose graceful close times out on an `onClose()` callback's cleanup while a subscriber waits", () => {
  for (const binary of [false, true])
    for (const removes of [false, true]) {
      it(`${binary ? 'subscribeBinary()' : 'subscribe()'}${removes ? ', the callback removing that subscriber' : ''}: \`ready\` rejects with the end the close times out with`, async () => {
        const sent = () => ({ seq: 1, timestamp: 1 })
        const listen = () => ({ unsubscribe() {}, opened: new Promise<void>(() => {}) })
        configureBroadcastTransport({ send: sent, sendBinary: sent, listen, listenBinary: listen })
        const room = new ServerBroadcast<string>({ key: 'room:close-times-out' })
        const stop = binary ? room.subscribeBinary(() => {}) : room.subscribe(() => {})
        room.onClose(() => {
          if (removes) stop()
          return new Promise<void>(() => {})
        })
        await room.close({ timeout: 10 })
        const ended = await room.closed.catch((err: unknown) => err)
        expect(ended).toMatchObject({ message: 'Channel close timed out' })
        expect(
          await stop.opened.then(
            () => 'resolved',
            (err: unknown) => err,
          ),
        ).toBe(ended)
      })
    }
})

/** The replay `end` keeps for its page. */
function replayOf(end: ServerBroadcastEnd): ServerReplay {
  const replay = end._replayBuffer
  assert(replay, 'The end keeps no replay')
  return replay
}

describe('a page subscription whose backend is replaced', () => {
  afterEach(() => vi.unstubAllGlobals())

  // A page's subscription that took effect stays one that did when `config.broadcast.transport` is replaced under it.
  for (const kind of ['text', 'binary'] as const) {
    const ENDED = 'its subscription was in effect on the server until its backend ended it'
    const NOT_IN_EFFECT = 'its subscription did not take effect on the server'
    for (const [before, outcome] of [
      ['took effect and heard a publish', ENDED],
      ['never took effect', NOT_IN_EFFECT],
    ] as const) {
      test(`${kind}: a page whose subscription ${before} gets "${outcome}" where a transport that replaced its own refuses it`, async () => {
        const heard: string[] = []
        const page = await subscribedPage(kind, heard)
        relay(before === 'never took effect' ? 'held' : 'at once').configure()
        const wanted = await broadcastWantedBy(page, kind)
        if (before !== 'never took effect') {
          await wanted.inEffect
          await (kind === 'text'
            ? wanted.broadcast.publish('live')
            : wanted.broadcast.publishBinary(new TextEncoder().encode('live')))
          await vi.waitFor(() => expect(heard).toEqual(['live']))
        }
        const refused = () => ({ unsubscribe() {}, opened: Promise.reject(new Error('NOPERM: replacement refused')) })
        const sent = () => ({ seq: 1, timestamp: 1 })
        configureBroadcastTransport({ send: sent, sendBinary: sent, listen: refused, listenBinary: refused })
        const given = await page.given()
        expect(given.onClose).toBeInstanceOf(ChannelClosedError)
        expect(given.onClose instanceof Error && given.onClose.message).toContain(outcome)
      })
    }
  }

  // Whether a page's subscription took effect is said of the subscription it wants now, on the plane in effect now.
  for (const kind of ['text', 'binary'] as const) {
    test(`${kind}: a page's subscription still being established when a transport replaces its own is confirmed, and its call goes on, once the new transport's listener is`, async () => {
      const page = await subscribedPage(kind)
      relay('held').configure()
      const wanted = await broadcastWantedBy(page, kind)
      relay('at once').configure()
      const settled = await Promise.race([
        Promise.all([page.opened, wanted.inEffect]).then(() => 'confirmed'),
        new Promise((resolve) => setTimeout(() => resolve('waiting'), 500)),
      ])
      expect(settled).toBe('confirmed')
    })

    test(`${kind}: a page's subscription still being established across two transports that replace their own waits for the last one's listener, and its call goes on once it is`, async () => {
      const page = await subscribedPage(kind)
      const first = relay('held')
      first.configure()
      const wanted = await broadcastWantedBy(page, kind)
      const second = relay('held')
      second.configure()
      const last = relay('held')
      last.configure()
      const settled = () =>
        Promise.race([
          Promise.all([page.opened, wanted.inEffect]).then(() => 'confirmed'),
          new Promise((resolve) => setTimeout(() => resolve('waiting'), 100)),
        ])
      first.confirm()
      second.confirm()
      const beforeTheLast = await settled()
      last.confirm()
      expect({ beforeTheLast, once: await settled() }).toEqual({ beforeTheLast: 'waiting', once: 'confirmed' })
    })

    for (const first of ['at once', 'held'] as const) {
      test(`${kind}: a page's new subscription that a replacing transport refuses gets "did not take effect", though a server-side listener on its broadcast kept the earlier one, which its transport confirmed ${first === 'held' ? 'later' : 'at once'}, in effect`, async () => {
        const page = await subscribedPage(kind)
        const transport = relay(first)
        transport.configure()
        const wanted = await broadcastWantedBy(page, kind)
        transport.confirm()
        await wanted.inEffect
        const listener =
          kind === 'text' ? wanted.broadcast.subscribe(() => {}) : wanted.broadcast.subscribeBinary(() => {})
        await listener.opened
        const replacement = relay('held')
        replacement.configure()
        await wanted.want(2, false)
        void wanted.want(3, true)
        replacement.refuse()
        const given = await page.given()
        expect(given.onClose instanceof Error && given.onClose.message).toContain(
          'its subscription did not take effect on the server',
        )
      })
    }
  }

  // A page's subscription its page withdrew before its transport confirmed it holds nothing on the server, while the page
  // stays connected and its transport never answers.
  for (const kind of ['text', 'binary'] as const) {
    test(`${kind}: the subscriptions a page withdrew while its transport's first confirmation was pending hold nothing on the server`, async () => {
      const page = await subscribedPage(kind)
      relay('held').configure()
      const wanted = await broadcastWantedBy(page, kind)
      // What waits for each of the page's wants to take effect.
      const waits: WeakRef<object>[] = []
      const hook = createHook({
        init(_id, type, _trigger, resource) {
          if (type === 'PROMISE' && new Error().stack?.includes('whenInEffect')) waits.push(new WeakRef(resource))
        },
      }).enable()
      let version = 1
      for (let round = 0; round < 16; round++) {
        void wanted.want(++version, true)
        void wanted.want(++version, false)
      }
      hook.disable()
      await collectGarbage()
      expect({
        waited: waits.length > 0,
        reachable: waits.filter((wait) => wait.deref() !== undefined).length,
      }).toEqual({
        waited: true,
        reachable: 0,
      })
    })
  }

  /** A transport that delivers each publish to every listener on this server, and confirms each listener at once, or where
   *  `held` once `confirm()` confirms or `refuse()` refuses those it hasn't settled. */
  function relay(confirms: 'at once' | 'held') {
    const listeners = new Set<(payload: never, info: { seq: number; timestamp: number }) => void>()
    const unconfirmed: Deferred<void>[] = []
    let seq = 0
    const listen = (_key: string, onMessage: (payload: never, info: { seq: number; timestamp: number }) => void) => {
      listeners.add(onMessage)
      const unsubscribe = () => void listeners.delete(onMessage)
      if (confirms === 'at once') return unsubscribe
      const ready = createDeferred<void>()
      unconfirmed.push(ready)
      return { unsubscribe, opened: ready.promise }
    }
    const send = (_key: string, payload: never) => {
      const info = { seq: ++seq, timestamp: 1 }
      for (const onMessage of listeners) onMessage(payload, info)
      return info
    }
    return {
      configure: () => configureBroadcastTransport({ send, sendBinary: send, listen, listenBinary: listen }),
      confirm: () => {
        for (const ready of unconfirmed.splice(0)) ready.resolve()
      },
      refuse: () => {
        for (const ready of unconfirmed.splice(0)) ready.reject(new Error('NOPERM: replacement refused'))
      },
    }
  }

  /** A broadcast whose first call carries `page`'s subscription of `kind`, on the transport configured now; what the server
   *  sends the page reaches it as sent, and `want()` is a later call's want of the same kind. */
  async function broadcastWantedBy(page: Awaited<ReturnType<typeof subscribedPage>>, kind: 'text' | 'binary') {
    const broadcast = new ServerBroadcast<string>({ key: crypto.randomUUID() })
    const end = broadcast._end
    const call = new Request('https://example.test/_telefunc?session=page', { method: 'POST' })
    getChannelMux().registerChannel(end, ATTACHABLE_NOW, 'page')
    const sender: PeerSender = {
      send: (frame) => page.receive(new Uint8Array(frame)),
      carry() {},
      bufferedAmount: () => 0,
      behindAfter: () => Number.POSITIVE_INFINITY,
      deliveredBefore: () => 0,
      sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
      sendPublish(index, seq, body) {
        this.send(encode.publishTo(index, seq))
        this.send(body)
      },
      waitingPublishes: () => 0,
      dropUngiven: () => 0,
      dropWaitingPublishes() {},
      dropWaitingFrom() {},
      terminate() {},
    }
    const replay = end._replayBuffer
    if (!replay) throw new Error('A registered broadcast has its replay')
    end._attachPeer(new IndexedPeer({ sender, index: page.index, replay }), {})
    const want = (version: number, on: boolean) =>
      getChannelMux().applyCallWants(call, [
        { channel: broadcast.id, version, text: on && kind === 'text', binary: on && kind === 'binary' },
      ])
    return { broadcast, inEffect: want(1, true), want }
  }

  /** A page's broadcast, subscribed, on an SSE wire this test serves once it reconciled: `receive()` hands it a frame the
   *  server sent, and `given()` is what its `opened` rejects with and its `onClose()` callback gets. */
  async function subscribedPage(kind: 'text' | 'binary' = 'text', heard: string[] = []) {
    const reconciled = createDeferred<{ index: number; push: (frame: Uint8Array<ArrayBuffer>) => void }>()
    vi.stubGlobal('fetch', async (_url: string, init: RequestInit) => {
      const { streamResponse, frames } = await batchOf(init.body)
      if (!streamResponse) return new Response('', { status: 200 })
      let index = 0
      for (const frame of frames) if (frame.tag === TAG.RECONCILE) index = frame.payload.open[0]?.ix ?? index
      const encoder = new TextEncoder()
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          const push = (frame: Uint8Array<ArrayBuffer>) =>
            controller.enqueue(encoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`))
          controller.enqueue(encoder.encode(': open\n\n'))
          push(encode.streamRequestOpenAck())
          push(
            encode.reconciled({ ...SERVER_OPTIONS, sessionId: crypto.randomUUID(), open: [{ ix: index, lastSeq: 0 }] }),
          )
          reconciled.resolve({ index, push })
        },
      })
      return new Response(stream, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
    })
    const page = new ClientBroadcast({
      channelId: crypto.randomUUID(),
      key: 'k',
      connection: {
        transports: [CHANNEL_TRANSPORT.SSE],
        telefuncUrl: 'http://test.local/_telefunc',
        connectionKey: crypto.randomUUID(),
      },
    })
    const onClose = new Promise<unknown>((resolve) => page.onClose(resolve))
    const subscription =
      kind === 'text'
        ? page.subscribe((message) => heard.push(String(message)))
        : page.subscribeBinary((data) => heard.push(new TextDecoder().decode(data)))
    const { index, push } = await reconciled.promise
    return {
      index,
      receive: push,
      opened: subscription.opened,
      given: async () => ({ opened: await subscription.opened.catch((err: unknown) => err), onClose: await onClose }),
    }
  }

  const SERVER_OPTIONS = {
    reconnectTimeout: 60_000,
    idleTimeout: 60_000,
    pingInterval: 100_000,
    maxFrameBytes: 64 * 1024 * 1024,
    creditWindow: 1_000_000,
    sseFlushThrottle: 300,
    ssePostIdleFlushDelay: 50,
    transports: [CHANNEL_TRANSPORT.SSE],
  }

  /** A page's POST, read as the server's SSE handler reads it: its metadata, then its frames. */
  async function batchOf(body: unknown): Promise<{ streamResponse: boolean; frames: DecodedFrame[] }> {
    if (!(body instanceof Blob)) return { streamResponse: false, frames: [] }
    // A `Blob`'s stream doesn't fail.
    const reader = new BodyReader(body.stream(), () => false, queueMemory)
    const metadata = await reader.readFrame(SSE_METADATA_MAX_BYTES)
    if (metadata === null) return { streamResponse: false, frames: [] }
    const { kind } = parseSseRequestMetadata(JSON.parse(new TextDecoder().decode(metadata)))
    const frames: DecodedFrame[] = []
    for (let frame; (frame = await reader.readFrame(WIRE_MAX_RAW_FRAME_BYTES)); ) frames.push(decode(frame))
    return { streamResponse: kind === 'streamResponse', frames }
  }
})
