import { ATTACHABLE_NOW } from './ServerChannel.js'
import { afterEach, assert, describe, expect, it, test, vi } from 'vitest'
import { parse } from '@brillout/json-serializer/parse'
import { createHook } from 'node:async_hooks'
import { getEventListeners } from 'node:events'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { ServerBroadcastEnd } from './ServerBroadcastEnd.js'
import { publishBody } from './PublishBody.js'
import { ServerReplay } from './ServerReplay.js'
import { decode, encode, type DecodedFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import { Channel } from './ServerChannel.js'
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
import { MONITORING_FAILURES } from '../../test/monitoringFailures.js'
import { binaryData } from '../broadcastPayload.js'
import { ChannelClosedError, ChannelOverflowError } from '../channelErrors.js'
import { BackendError } from '../../backend/BackendError.js'
import type { BroadcastPublishOptions, Unsubscribe } from '../channel.js'

import { ClientBroadcast } from '../client/ClientBroadcast.js'
import { getChannelMux } from './ChannelMux.js'
import { parseSseRequestMetadata } from '../../wire/envelope.js'
import { BodyReader } from '../../wire/BodyReader.js'
import { queueMemory } from './channelMemory.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { CHANNEL_TRANSPORT, SSE_METADATA_MAX_BYTES, WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'
import { createDeferred, type Deferred } from '../../../utils/createDeferred.js'

afterEach(() => disposeBackend())

// ───────────────────────────────────────────────────────────────────────────
// In-process delivery — bug classes targeted: cross-key bleed, dropped
// subscribers, delivery reordering, self-echo loss, late-register and
// late-attach buffer correctness, error isolation between subscribers.
// ───────────────────────────────────────────────────────────────────────────

describe('keyed in-process broadcast', () => {
  // Catches a key-mixup where the adapter routes by reference instead of by key,
  // or strips the key prefix and ends up with a single global topic.
  it('isolates messages by key — publishing on key A does not reach key B subscribers', () => {
    const a = new ServerBroadcast<{ from: string }>({ key: 'room:A' })
    const b = new ServerBroadcast<{ from: string }>({ key: 'room:B' })
    a._end._registerChannel(ATTACHABLE_NOW)
    b._end._registerChannel(ATTACHABLE_NOW)

    const receivedA: Array<{ from: string }> = []
    const receivedB: Array<{ from: string }> = []
    a.subscribe((m) => receivedA.push(m))
    b.subscribe((m) => receivedB.push(m))

    a.publish({ from: 'A' })
    b.publish({ from: 'B' })

    expect(receivedA).toEqual([{ from: 'A' }])
    expect(receivedB).toEqual([{ from: 'B' }])
  })

  it('does not call a subscriber that an earlier one unsubscribed during the same delivery', () => {
    const broadcast = new ServerBroadcast<string>({ key: 'room:unsub-in-delivery' })
    broadcast._end._registerChannel(ATTACHABLE_NOW)

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
    // Note: no `_registerChannel(ATTACHABLE_NOW)` calls here — exercises the "publish before register" path.

    sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })
  // Options read from a form or a configuration file: `'false'` would otherwise publish it lossy.
  it("refuses a `lossy` that isn't a boolean, at the call", () => {
    const key = 'room:lossy-option'
    const broadcast = new ServerBroadcast({ key })
    const options: BroadcastPublishOptions = JSON.parse('{ "lossy": "false" }')

    expect(() => broadcast.publish(1, options)).toThrow("`publish()`'s `lossy` should be a boolean")
    expect(() => Broadcast.publish(key, 1, options)).toThrow("`publish()`'s `lossy` should be a boolean")
    expect(() => broadcast.publishBinary(new Uint8Array([1]), options)).toThrow(
      "`publishBinary()`'s `lossy` should be a boolean",
    )
    expect(() => Broadcast.publishBinary(key, new Uint8Array([1]), options)).toThrow(
      "`publishBinary()`'s `lossy` should be a boolean",
    )
  })
})

describe('in-memory publishes in answer', () => {
  it('delivers a chain of 5,000 answers', async () => {
    const backend = new MemoryBackend()
    const route = { key: 'room:finite', kind: 'text' } as const
    let received = 0
    const attempt = backend.subscribe(route, {
      deliver(payload) {
        received++
        const n = Number(payload)
        if (n < 5000) void backend.publish(route, String(n + 1))
      },
    })
    await backend.publish(route, '0')
    await attempt.unsubscribe()
    expect(received).toBe(5001)
  })

  it('delivers the bytes an answer published, though its publisher reuses its buffer once publishBinary() returns', async () => {
    const backend = new MemoryBackend()
    const received: number[][] = []
    const binary = backend.subscribe(
      { key: 'room:reused', kind: 'binary' },
      { deliver: (data) => received.push(typeof data === 'string' ? [] : Array.from(data)) },
    )
    const text = backend.subscribe(
      { key: 'room:reused', kind: 'text' },
      {
        deliver() {
          const bytes = new Uint8Array([1, 2, 3])
          void backend.publish({ key: 'room:reused', kind: 'binary' }, bytes)
          bytes.fill(0)
        },
      },
    )
    await backend.publish({ key: 'room:reused', kind: 'text' }, 'question')
    const unsubscribe = () => void text.unsubscribe()
    const unsubscribeBinary = () => void binary.unsubscribe()
    unsubscribe()
    unsubscribeBinary()
    expect(received).toEqual([[1, 2, 3]])
  })

  it('rejects a listener that answers its own messages without end, and timers run again', async () => {
    const backend = new MemoryBackend()
    const route = { key: 'room:endless', kind: 'text' } as const
    const errors: unknown[] = []
    const attempt = backend.subscribe(route, {
      deliver() {
        const result = backend.publish(route, 'again')
        if (result instanceof Promise) result.catch((err: unknown) => errors.push(err))
      },
    })
    const timerRan = new Promise((resolve) => setTimeout(resolve, 0))
    void backend.publish(route, 'start')
    await timerRan
    await attempt.unsubscribe()
    expect(errors).toHaveLength(1)
    const [error] = errors
    assert(error instanceof ChannelOverflowError)
    expect(error.message).toContain("past Telefunc's limit on nested publishes")
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
    sender._end._registerChannel(ATTACHABLE_NOW)
    receiver._end._registerChannel(ATTACHABLE_NOW)

    const received: Uint8Array[] = []
    receiver.subscribeBinary((data) => received.push(data))
    sender.publishBinary(new Uint8Array([0x00, 0x7f, 0x80, 0xff]))

    expect(received.map((data) => Array.from(data))).toEqual([[0x00, 0x7f, 0x80, 0xff]])
  })
})

describe('ServerBroadcastEnd', () => {
  // Room's stubs deliver from the moment a response returns them, ahead of the page's attach.
  it('sends what was delivered before its page attached, once it attaches', () => {
    const end = new ServerBroadcastEnd()
    end.setOwner({ _onPagePublish() {}, _onPagePublishBinary() {} })
    end._registerChannel(ATTACHABLE_NOW)

    end.deliver(publishBody('"hello"', { seq: 1, timestamp: 1 }, 'room:early', true))

    const frames: Uint8Array[] = []
    end._attachPeer(
      new IndexedPeer({
        sender: {
          send: (frame) => {
            frames.push(frame)
          },
          carry() {},
          bufferedAmount: () => 0,
          behindAfter: () => 700,
          deliveredBefore: () => 0,
          terminate() {},
          sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
          sendPublish(index, seq, body) {
            this.send(encode.publishTo(index, seq))
            this.send(body)
          },
          waitingPublishes: () => 0,
          dropWaitingPublishes() {},
          dropUngiven: () => 0,
          dropWaitingFrom() {},
        },
        index: 7,
        replay: new ServerReplay({ heldBytes: 0 }),
      }),
    )

    expect(frames.map((frame) => decode(new Uint8Array(frame)).tag)).toEqual([TAG.PUBLISH_TO, TAG.PUBLISH])
  })

  it("sends a stream's held lossy publish before a plain one of that stream", () => {
    const { end, sent } = behindEnd()
    end.deliver(publishBody('"first"', { seq: 1, timestamp: 1 }, null, false))
    end.deliver(publishBody('"lossy"', { seq: 2, timestamp: 2 }, 'cursor', true))
    end.deliver(publishBody('"plain"', { seq: 3, timestamp: 3 }, 'cursor', false))

    expect(sent).toEqual(['"first"', '"lossy"', '"plain"'])
  })

  it('lets a plain publish of another stream go ahead of a held one, which a newer one still replaces', () => {
    const { end, sent } = behindEnd()
    end.deliver(publishBody('"first"', { seq: 1, timestamp: 1 }, null, false))
    end.deliver(publishBody('"older"', { seq: 2, timestamp: 2 }, 'cursor', true))
    end.deliver(publishBody('"plain"', { seq: 3, timestamp: 3 }, 'drawing', false))
    end.deliver(publishBody('"newer"', { seq: 4, timestamp: 4 }, 'cursor', true))
    void end.close()

    expect(sent).toEqual(['"first"', '"plain"', '"newer"'])
  })

  // As a Channel's send and a page's publish: a lossy publish replaces only lossy ones after the last plain one of its stream.
  describe.each(['behind', 'away'] as const)('to a page that is %s', (wire) => {
    it.each([
      [
        ['l1', 'plain', 'l2'],
        ['l1', 'plain', 'l2'],
      ],
      [
        ['l1', 'l2', 'plain', 'l3', 'l4'],
        ['l2', 'plain', 'l4'],
      ],
    ])('keeps a lossy publish before a plain one of its stream: %j arrives as %j', (published, arrives) => {
      const { end, peer } = behindEnd(Number.POSITIVE_INFINITY)
      if (wire === 'away') end._onPeerDisconnect(peer, 60_000)
      end.deliver(publishBody('"first"', { seq: 1, timestamp: 1 }, null, false))
      published.forEach((text, i) => {
        const info = { seq: i + 2, timestamp: i + 2 }
        end.deliver(publishBody(JSON.stringify(text), info, 'cursor', text !== 'plain'))
      })
      void end.close()

      expect(replayed(end)).toEqual(['first', ...arrives].map((text) => JSON.stringify(text)))
    })
  })
})

/** The publishes a reconnect of `end`'s page would get, in order. */
function replayed(end: ServerBroadcastEnd): string[] {
  return replayOf(end)
    .getAfter(0)
    .map((frame) => decode(frame))
    .flatMap((decoded) => (decoded.tag === TAG.PUBLISH ? [decoded.text] : []))
}

/** The `info.seq` of the publishes, of both kinds, a reconnect of `end`'s page would get, in order. */
function replayedSeqs(end: ServerBroadcastEnd): number[] {
  return (end._replayBuffer?.getAfter(0) ?? [])
    .map((frame) => decode(frame))
    .flatMap((decoded) => (decoded.tag === TAG.PUBLISH || decoded.tag === TAG.PUBLISH_BINARY ? [decoded.info.seq] : []))
}

/** An attached page's end whose page is behind once a publish waits for its confirmation, with `waiting` of its newest
 *  publishes not yet taken by its wire; the publishes it sends. */
function behindEnd(waiting = 0) {
  const end = new ServerBroadcastEnd()
  end.setOwner({ _onPagePublish() {}, _onPagePublishBinary() {} })
  return behindPage(end, waiting)
}

/** `end`'s page, attached and reading both kinds, as `behindEnd`'s; the text publishes it gets, the binary ones, and the
 *  `info.seq` of both in the order they arrive. */
function behindPage(end: ServerBroadcastEnd, waiting = 0) {
  end._registerChannel(ATTACHABLE_NOW)
  const sent: string[] = []
  const seqs: number[] = []
  const binary: number[][] = []
  const peer = new IndexedPeer({
    sender: {
      send: (frame) => {
        const decoded = decode(new Uint8Array(frame))
        if (decoded.tag === TAG.PUBLISH_BINARY) binary.push([...decoded.data])
        else if (decoded.tag === TAG.PUBLISH) sent.push(decoded.text)
        else return
        seqs.push(decoded.info.seq)
      },
      carry() {},
      bufferedAmount: () => 0,
      behindAfter: () => -1,
      deliveredBefore: () => 0,
      terminate() {},
      sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
      sendPublish(index, seq, body) {
        this.send(encode.publishTo(index, seq))
        this.send(body)
      },
      waitingPublishes: () => waiting,
      dropWaitingPublishes() {},
      dropUngiven: () => 0,
      dropWaitingFrom() {},
    },
    index: 7,
    // Its own replay: what it hasn't confirmed there is what makes its page behind.
    replay: replayOf(end),
  })
  end._attachPeer(peer, { want: { version: 1, text: true, binary: true } })
  return { end, peer, sent, seqs, binary }
}

// ───────────────────────────────────────────────────────────────────────────
// Channel-method gating — the type hides send/listen/sendBinary/listenBinary,
// and calling one at runtime still throws a usage error.
// ───────────────────────────────────────────────────────────────────────────

// The Cloudflare backend refuses a server-side subscription on a Worker without AsyncLocalStorage, where its host has no
// session; the app may catch it.
class BackendRefusingFirstSubscription extends MemoryBackend {
  private refuse = true
  hostKey(): string {
    if (this.refuse) {
      this.refuse = false
      throw new Error('subscription refused')
    }
    return ''
  }
}

describe('a subscription the adapter refused', () => {
  it('subscribe(): its callback gets no later publish', () => {
    installBackend(() => new BackendRefusingFirstSubscription(), [crypto.randomUUID()])
    const broadcast = new ServerBroadcast<string>({ key: 'room:refused' })
    broadcast._end._registerChannel(ATTACHABLE_NOW)
    const refused: string[] = []
    const later: string[] = []
    expect(() => broadcast.subscribe((msg) => refused.push(msg))).toThrow('subscription refused')
    broadcast.subscribe((msg) => later.push(msg))
    broadcast.publish('hello')
    expect(later).toEqual(['hello'])
    expect(refused).toEqual([])
  })

  it('subscribeBinary(): its callback gets no later publish', () => {
    installBackend(() => new BackendRefusingFirstSubscription(), [crypto.randomUUID()])
    const broadcast = new ServerBroadcast<string>({ key: 'room:refused-binary' })
    broadcast._end._registerChannel(ATTACHABLE_NOW)
    const refused: Uint8Array[] = []
    const later: Uint8Array[] = []
    expect(() => broadcast.subscribeBinary((data) => refused.push(data))).toThrow('subscription refused')
    broadcast.subscribeBinary((data) => later.push(data))
    broadcast.publishBinary(new Uint8Array([1, 2, 3]))
    expect(later).toEqual([new Uint8Array([1, 2, 3])])
    expect(refused).toEqual([])
  })
})

// The same rule as a page's `opened`: it resolves once the listener is in effect or removed, and rejects where the
// broadcast ends first. The backend here confirms no subscription until each test is over.
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

// A `opened` nobody reads makes no promise, so subscribers that come and go while the backend waits leave nothing behind.
describe("subscribers that come and go while their backend hasn't confirmed the key's subscription", () => {
  const pending = new Promise<void>(() => {})
  const sent = () => ({ seq: 1, timestamp: 1 })
  const listen = () => ({ unsubscribe: () => {}, opened: pending })
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
    // In effect at once: its `opened`, read, is one promise every such listener shares.
    'Broadcast.subscribe()': (key) => Broadcast.subscribe(key, () => {}),
    'Broadcast.subscribeBinary()': (key) => Broadcast.subscribeBinary(key, () => {}),
    "a channel's listen(), its `opened` read": (key) => {
      const remove = channelOf(key).listen(() => {})
      void remove.opened
      return remove
    },
  }
  for (const [name, subscribe] of Object.entries(CHURN)) {
    it(`${name}: 1,000 subscribers added and removed make no promise`, () => {
      configureBroadcastTransport({ send: sent, sendBinary: sent, listen, listenBinary: listen })
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

  // A monitoring client an app reports bugs to may fail on its own, also with the error of a stream it closed early.
  for (const [given, failure] of Object.entries(MONITORING_FAILURES)) {
    for (const fails of ['throws', 'rejects'] as const) {
      it(`a refusal reported to an \`onBug()\` listener that ${fails} with ${given} still rejects the subscriber's \`ready\` and reaches the next listener, the listener's failure logged once`, async () => {
        const monitoringFailed = failure()
        failingBugListener =
          fails === 'throws'
            ? () => {
                throw monitoringFailed
              }
            : () => Promise.reject(monitoringFailed)
        const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
        const unhandled: unknown[] = []
        const onUnhandled = (reason: unknown) => unhandled.push(reason)
        process.on('unhandledRejection', onUnhandled)
        try {
          const before = reported.length
          const { refuse } = refusingBroker()
          const subscriber = Broadcast.subscribe('room:failing-monitoring', () => {})
          refuse()
          const cause = await subscriber.opened.then(
            () => 'resolved',
            (err: unknown) => err instanceof Error && err.cause instanceof Error && err.cause.message,
          )
          await new Promise((resolve) => setTimeout(resolve, 10))
          expect({
            cause,
            unhandled,
            nextListener: reported.slice(before),
            logged: logged.mock.calls.filter(([error]) => String(error).includes(monitoringFailed.message)).length,
          }).toEqual({ cause: 'NOPERM', unhandled: [], nextListener: ['NOPERM'], logged: 1 })
        } finally {
          failingBugListener = undefined
          process.off('unhandledRejection', onUnhandled)
          logged.mockRestore()
        }
      })
    }
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

/** What the first `onBug()` listener does with each bug, where a test sets it: throw, or return a promise that rejects. */
let failingBugListener: (() => unknown) | undefined
onBug(() => failingBugListener?.())
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
// the runtime check is in _onPagePublish.
// ───────────────────────────────────────────────────────────────────────────

describe('lossy broadcast under memory pressure', () => {
  // Another object's publish sheds it: the channel ends once its own object runs, but what it holds goes at once.
  it('lets go of the newest it holds for a page that is behind', async () => {
    const broadcast = new ServerBroadcast<string>({ key: 'room:lossy-shed' })
    const end = broadcast._end
    end._registerChannel(ATTACHABLE_NOW)
    end._attachPeer(
      new IndexedPeer({
        sender: {
          send() {},
          carry() {},
          bufferedAmount: () => 0,
          behindAfter: () => 0,
          deliveredBefore: () => 0,
          terminate() {},
          sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
          sendPublish(index, seq, body) {
            this.send(encode.publishTo(index, seq))
            this.send(body)
          },
          waitingPublishes: () => 0,
          dropWaitingPublishes() {},
          dropUngiven: () => 0,
          dropWaitingFrom() {},
        },
        index: 7,
        replay: replayOf(end),
      }),
      { want: { version: 1, text: true, binary: false } },
    )
    await broadcast.publish('first', { lossy: true })
    await new Promise((resolve) => setTimeout(resolve, 5))
    await broadcast.publish('x'.repeat(256 * 1024), { lossy: true })
    expect(end.newestUnhanded).toBeDefined()
    expect(end.heldBytes).greaterThan(256 * 1024)

    end.shed(0)

    expect(end.newestUnhanded).toBe(undefined)
    expect(end.heldBytes).lessThan(1024)
  })
})

// ───────────────────────────────────────────────────────────────────────────
// TransportBroadcastDriver, Broadcast on a custom `config.broadcast.transport` —
// multi-node behavior; the transport simulates the bus.
// Bug classes targeted: same-node echo causing double-delivery, cross-node
// delivery silently dropped, transport subscription leaks (no refcount).
// ───────────────────────────────────────────────────────────────────────────

describe('TransportBroadcastDriver — multi-node transport', () => {
  type Info = { seq: number; timestamp: number }
  type TextListener = (payload: string, info: Info) => void
  type BinaryListener = (payload: Uint8Array, info: Info) => void

  /** Synchronous-delivery bus for fast, deterministic tests; `_carried` is every payload it was sent, as sent. */
  function createMockTransport(): BroadcastTransport & {
    _listeners: Map<string, Set<TextListener>>
    _carried: (string | number[])[]
  } {
    const listeners = new Map<string, Set<TextListener>>()
    const binaryListeners = new Map<string, Set<BinaryListener>>()
    const seqs = new Map<string, number>()
    const carried: (string | number[])[] = []
    const nextInfo = (key: string): Info => {
      const seq = (seqs.get(key) ?? 0) + 1
      seqs.set(key, seq)
      return { seq, timestamp: Date.now() }
    }
    return {
      _listeners: listeners,
      _carried: carried,
      send(key, payload) {
        carried.push(payload)
        const info = nextInfo(key)
        const set = listeners.get(key)
        if (set) for (const cb of set) cb(payload, info)
        return info
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
      sendBinary(key, payload) {
        carried.push([...payload])
        const info = nextInfo(key)
        for (const cb of binaryListeners.get(key) ?? []) cb(payload, info)
        return info
      },
      listenBinary(key, onMessage) {
        const set = binaryListeners.get(key) ?? new Set()
        binaryListeners.set(key, set.add(onMessage))
        return () => binaryListeners.delete(key)
      },
    }
  }

  it('publishes go through the transport and locally-subscribed receivers see the message', async () => {
    const transport = createMockTransport()
    configureBroadcastTransport(transport)
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:tx:basic' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:tx:basic' })
    sender._end._registerChannel(ATTACHABLE_NOW)
    receiver._end._registerChannel(ATTACHABLE_NOW)

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
    configureBroadcastTransport(transport)
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:tx:echo' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:tx:echo' })
    sender._end._registerChannel(ATTACHABLE_NOW)
    receiver._end._registerChannel(ATTACHABLE_NOW)

    const received: Array<{ text: string }> = []
    receiver.subscribe((m) => received.push(m))

    sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  // A key carries both kinds: a page that is behind skips to the newest lossy publish, and gets the plain one after it.
  it('delivers the newest lossy publish of a key, then the plain one after it, to a page that is behind', async () => {
    configureBroadcastTransport(createMockTransport())
    const key = 'room:tx:lossy-then-plain'
    const { sent, seqs } = behindPage(new ServerBroadcast({ key })._end)

    await Broadcast.publish(key, 'first')
    for (let n = 1; n <= 10; n++) void Broadcast.publish(key, n, { lossy: true })
    await Broadcast.publish(key, 'plain')

    expect(sent.map((text) => parse(text))).toEqual(['first', 10, 'plain'])
    expect(seqs).toEqual([1, 11, 12])
  })

  // One order spans a key's two kinds: a plain publish of either goes after the lossy ones of both, oldest first.
  describe.each(['behind', 'away'] as const)('to a page that is %s', (wire) => {
    it.each([
      [
        ['binary lossy', 'text', 'binary lossy'],
        [1, 2, 3, 4],
      ],
      [
        ['text lossy', 'binary', 'text lossy'],
        [1, 2, 3, 4],
      ],
      [
        ['text lossy', 'binary lossy', 'text lossy', 'binary'],
        [1, 3, 4, 5],
      ],
    ])('delivers %j after a first plain publish with info.seq %j', async (published, arrives) => {
      configureBroadcastTransport(createMockTransport())
      const key = `room:tx:across-kinds:${wire}:${published.join()}`
      const broadcast = new ServerBroadcast({ key })
      const { end, peer, seqs } = behindPage(broadcast._end)
      await Broadcast.publish(key, 'first')
      if (wire === 'away') end._onPeerDisconnect(peer, 60_000)
      for (const publish of published) {
        const options = { lossy: publish.endsWith(' lossy') }
        if (publish.startsWith('text')) void Broadcast.publish(key, publish, options)
        else void Broadcast.publishBinary(key, new Uint8Array([9]), options)
      }
      void broadcast.close()

      expect(wire === 'behind' ? seqs : replayedSeqs(end)).toEqual(arrives)
    })
  })

  // What a transport carries is the caller's JSON, a lossy one behind a line feed, and the caller's bytes, a lossy one
  // behind 0xfe; every reader gets what the caller published.
  it("hands every reader the caller's data, and the transport the payload that says whether it is lossy", async () => {
    const transport = createMockTransport()
    configureBroadcastTransport(transport)
    const key = 'room:tx:payloads'
    const broadcast = new ServerBroadcast({ key })
    const { sent, binary } = behindPage(broadcast._end)
    const fromObject: unknown[] = []
    const fromStatic: unknown[] = []
    const fromObjectBinary: number[][] = []
    const fromStaticBinary: number[][] = []
    broadcast.subscribe((data) => fromObject.push(data))
    broadcast.subscribeBinary((data) => fromObjectBinary.push([...data]))
    Broadcast.subscribe(key, (data) => fromStatic.push(data))
    Broadcast.subscribeBinary(key, (data) => fromStaticBinary.push([...data]))

    await broadcast.publish({ n: 1 })
    await Broadcast.publish(key, { n: 2 }, { lossy: true })
    await broadcast.publishBinary(new Uint8Array([3, 1]))
    await Broadcast.publishBinary(key, new Uint8Array([4]), { lossy: true })
    await broadcast.publishBinary(new Uint8Array([5]), { lossy: true })
    await Broadcast.publish(key, 'plain')
    void broadcast.close()

    expect(transport._carried).toEqual(['{"n":1}', '\n{"n":2}', [3, 1], [0xfe, 4], [0xfe, 5], '"plain"'])
    expect(fromObject).toEqual([{ n: 1 }, { n: 2 }, 'plain'])
    expect(fromStatic).toEqual(fromObject)
    expect(fromObjectBinary).toEqual([[3, 1], [4], [5]])
    expect(fromStaticBinary).toEqual(fromObjectBinary)
    // Its page is behind from the first publish on: the newest lossy one of each kind waits, for a plain one or the close.
    expect(sent).toEqual(['{"n":1}', '\n{"n":2}', '"plain"'])
    expect(binary).toEqual([
      [3, 1],
      [0xfe, 5],
    ])
  })

  it('cross-node: a publish from node1 reaches node2 subscribers via the shared transport', () => {
    const transport = createMockTransport()
    configureBroadcastTransport(transport)

    const received: unknown[] = []
    Broadcast.subscribe('room:cross-node', (message) => received.push(message))
    // node1's publish, as it reaches the shared transport
    transport.send('room:cross-node', '{"text":"from-node1"}')

    expect(received).toEqual([{ text: 'from-node1' }])
  })

  // Refcount: multiple local subscribers must share ONE transport subscription; the
  // upstream subscription must persist until the LAST local subscriber leaves. Otherwise
  // either the transport is over-subscribed (leak) or unsubscribed prematurely (loss).
  it('refcounts the transport subscription: holds while any local subscriber is attached', () => {
    const transport = createMockTransport()
    configureBroadcastTransport(transport)

    const u1 = Broadcast.subscribe('room:rc', () => {})
    const u2 = Broadcast.subscribe('room:rc', () => {})
    expect(transport._listeners.get('room:rc')?.size).toBe(1) // exactly one upstream sub for both

    u1()
    expect(transport._listeners.has('room:rc')).toBe(true) // still subscribed for u2

    u2()
    expect(transport._listeners.has('room:rc')).toBe(false) // last out → upstream released
  })

  it('refcount survives sub→unsub→sub: the second subscribe re-establishes the upstream sub', () => {
    const transport = createMockTransport()
    configureBroadcastTransport(transport)

    const u1 = Broadcast.subscribe('room:rc-cycle', () => {})
    u1()
    expect(transport._listeners.has('room:rc-cycle')).toBe(false)

    const received: unknown[] = []
    Broadcast.subscribe('room:rc-cycle', (s) => received.push(s))
    expect(transport._listeners.get('room:rc-cycle')?.size).toBe(1)

    Broadcast.publish('room:rc-cycle', 'after-resub')
    expect(received).toEqual(['after-resub'])
  })
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
