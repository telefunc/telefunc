import { afterEach, describe, expect, it } from 'vitest'
import { parse } from '@brillout/json-serializer/parse'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { ServerBroadcastEnd } from './ServerBroadcastEnd.js'
import { publishBody } from './PublishBody.js'
import { ServerReplay } from './ServerReplay.js'
import { ACK_STATUS, decode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { IndexedPeer } from './IndexedPeer.js'
import { configureBroadcastTransport, disposeBackend } from '../../backend/install.js'
import type { BroadcastTransport } from '../../backend/broadcast/TransportBroadcastDriver.js'
import { MemoryBackend } from '../../backend/memory/MemoryBackend.js'
import { ChannelOverflowError } from '../channelErrors.js'
import type { BroadcastPublishOptions } from '../channel.js'

afterEach(() => disposeBackend())

// ───────────────────────────────────────────────────────────────────────────
// In-process delivery — bug classes targeted: cross-key bleed, dropped
// subscribers, delivery reordering, self-echo loss, late-register and
// late-attach buffer correctness, error isolation between subscribers.
// ───────────────────────────────────────────────────────────────────────────

describe('keyed in-process broadcast', () => {
  it('delivers a published message to a sibling broadcast on the same key', () => {
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:basic' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:basic' })
    sender._end._registerChannel()
    receiver._end._registerChannel()

    const received: Array<{ text: string }> = []
    receiver.subscribe((msg) => received.push(msg))
    sender.publish({ text: 'hello' })

    expect(received).toEqual([{ text: 'hello' }])
  })

  // The publisher's own subscribe must fire too — same instance is both pub and sub.
  // Catches a "skip-self" bug that excludes the source from delivery.
  it('delivers a published message to the source broadcast (self-echo)', () => {
    const broadcast = new ServerBroadcast<{ text: string }>({ key: 'room:self' })
    broadcast._end._registerChannel()

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
    a._end._registerChannel()
    b._end._registerChannel()

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
    pub._end._registerChannel()
    subA._end._registerChannel()
    subB._end._registerChannel()
    subC._end._registerChannel()

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
    sender._end._registerChannel()
    receiver._end._registerChannel()

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
    broadcast._end._registerChannel()

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
    broadcast._end._registerChannel()

    const seen: number[] = []
    const unsubscribe = broadcast.subscribe((m) => seen.push(m.n))

    broadcast.publish({ n: 1 })
    unsubscribe()
    broadcast.publish({ n: 2 })

    expect(seen).toEqual([1])
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
    a._end._registerChannel()
    b._end._registerChannel()

    const a1 = await a.publish({ text: 'a-one' })
    const a2 = await a.publish({ text: 'a-two' })
    const b1 = await b.publish({ text: 'b-one' })

    // Each receipt is keyed to its own topic, and seq increases per key.
    expect(a1.key).toBe(k1)
    expect(a2.key).toBe(k1)
    expect(b1.key).toBe(k2)
    expect(a2.seq).toBe(a1.seq + 1)
    expect(b1.seq).toBeGreaterThan(0)
    expect(typeof a1.timestamp).toBe('number')
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
    sender._end._registerChannel()
    receiver._end._registerChannel()

    const received: Uint8Array[] = []
    receiver.subscribeBinary((data) => received.push(data))
    sender.publishBinary(new Uint8Array([0x00, 0x7f, 0x80, 0xff]))

    expect(received).toHaveLength(1)
    expect(Array.from(received[0]!)).toEqual([0x00, 0x7f, 0x80, 0xff])
  })

  it('binary subscribers do NOT receive text publishes (and vice versa)', () => {
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:mixed' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:mixed' })
    sender._end._registerChannel()
    receiver._end._registerChannel()

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

describe('ServerBroadcastEnd', () => {
  // Room's stubs deliver from the moment a response returns them, ahead of the page's attach.
  it('sends what was delivered before its page attached, once it attaches', () => {
    const end = new ServerBroadcastEnd()
    end.setOwner({ _onPagePublish() {}, _onPagePublishBinary() {} })
    end._registerChannel()

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

    expect(frames.map((frame) => decode(frame as Uint8Array<ArrayBuffer>).tag)).toEqual([TAG.PUBLISH_TO, TAG.PUBLISH])
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
  return end
    ._replayBuffer!.getAfter(0)
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
  end._registerChannel()
  const sent: string[] = []
  const seqs: number[] = []
  const binary: number[][] = []
  const peer = new IndexedPeer({
    sender: {
      send: (frame) => {
        const decoded = decode(frame as Uint8Array<ArrayBuffer>)
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
      queue: {
        waitingPublishes: () => waiting,
        dropWaitingPublishes() {},
        dropWaitingFrom() {},
      },
    },
    index: 7,
    // Its own replay: what it hasn't confirmed there is what makes its page behind.
    replay: end._replayBuffer!,
  })
  end._attachPeer(peer, { broadcast: { text: true, binary: true } })
  return { end, peer, sent, seqs, binary }
}

// ───────────────────────────────────────────────────────────────────────────
// Channel-method gating — the type hides send/listen/sendBinary/listenBinary,
// and calling one at runtime still throws a usage error.
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
// the runtime check is in _onPagePublish.
// ───────────────────────────────────────────────────────────────────────────

describe('Broadcast shield validation', () => {
  it('rejects client publishes that fail the data shield with a SHIELD_ERROR ack', async () => {
    const broadcast = new ServerBroadcast<{ text: string }>({ key: 'room:shield' })
    broadcast._publishShield = (value) => {
      const v = value as { text?: unknown }
      return typeof v?.text === 'string' ? true : 'expected { text: string }'
    }
    broadcast._end._registerChannel()

    const frames: Uint8Array[] = []
    broadcast._end._attachPeer(
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

    await broadcast._end._onPeerPublishAckReqMessage(JSON.stringify({ text: 42 }), 1)

    const ack = frames.map((f) => decode(f as Uint8Array<ArrayBuffer>)).find((d) => d.tag === TAG.ACK_RES)
    expect(ack).toBeDefined()
    if (ack?.tag !== TAG.ACK_RES) throw new Error('Expected ACK_RES')
    expect(ack.status).toBe(ACK_STATUS.SHIELD_ERROR)
    expect(ack.text).toBe('expected { text: string }')
  })

  // Shield rejection MUST short-circuit the publish — bad payloads should never reach
  // any subscriber, including the publisher's own self-echo.
  it('a shield-rejected publish is not delivered to subscribers', async () => {
    const sender = new ServerBroadcast<{ text: string }>({ key: 'room:shield-drop' })
    const receiver = new ServerBroadcast<{ text: string }>({ key: 'room:shield-drop' })
    sender._end._registerChannel()
    receiver._end._registerChannel()

    sender._publishShield = () => 'always reject'

    const seen: Array<{ text: string }> = []
    receiver.subscribe((m) => seen.push(m))

    sender._end._attachPeer(
      new IndexedPeer({
        sender: {
          send: () => {},
          carry() {},
          bufferedAmount: () => 0,
          behindAfter: () => 700,
          deliveredBefore: () => 0,
          terminate() {},
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
    await sender._end._onPeerPublishAckReqMessage(JSON.stringify({ text: 'malicious' }), 1)

    expect(seen).toEqual([])
  })
})

describe('lossy broadcast under memory pressure', () => {
  // Another object's publish sheds it: the channel ends once its own object runs, but what it holds goes at once.
  it('lets go of the newest it holds for a page that is behind', async () => {
    const broadcast = new ServerBroadcast<string>({ key: 'room:lossy-shed' })
    const end = broadcast._end
    end._registerChannel()
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
          queue: {
            waitingPublishes: () => 0,
            dropWaitingPublishes() {},
            dropWaitingFrom() {},
          },
        },
        index: 7,
        replay: end._replayBuffer!,
      }),
      { broadcast: { text: true, binary: false } },
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
    sender._end._registerChannel()
    receiver._end._registerChannel()

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
    sender._end._registerChannel()
    receiver._end._registerChannel()

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

  // What a transport carries is the caller's JSON, a lossy one behind a line feed, and the caller's bytes behind one
  // byte, 1 where lossy; every reader gets what the caller published.
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

    expect(transport._carried).toEqual(['{"n":1}', '\n{"n":2}', [0, 3, 1], [1, 4], [1, 5], '"plain"'])
    expect(fromObject).toEqual([{ n: 1 }, { n: 2 }, 'plain'])
    expect(fromStatic).toEqual(fromObject)
    expect(fromObjectBinary).toEqual([[3, 1], [4], [5]])
    expect(fromStaticBinary).toEqual(fromObjectBinary)
    // Its page is behind from the first publish on: the newest lossy one of each kind waits, for a plain one or the close.
    expect(sent).toEqual(['{"n":1}', '\n{"n":2}', '"plain"'])
    expect(binary).toEqual([
      [0, 3, 1],
      [1, 5],
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

  it('does not call a subscriber that an earlier one unsubscribed during the same delivery', async () => {
    const received: string[] = []
    Broadcast.subscribe<string>('room:static-unsub-in-delivery', () => unsubscribe())
    const unsubscribe = Broadcast.subscribe<string>('room:static-unsub-in-delivery', (m) => received.push(m))

    await Broadcast.publish('room:static-unsub-in-delivery', 'first')

    expect(received).toEqual([])
  })
})
