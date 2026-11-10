import { afterEach, describe, expect, it } from 'vitest'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { ServerBroadcastEnd } from './ServerBroadcastEnd.js'
import { getChannelMux, type ChannelMux } from './ChannelMux.js'
import { publishBody, type PublishBody } from './PublishBody.js'
import { ServerReplay } from './ServerReplay.js'
import { ACK_STATUS, decode, encode, type ChannelFrame } from '../../wire/frame.js'
import type { DomainAnnouncement } from '../../wire/payloads.js'
import { MAX_DOMAINS_PER_CHANNEL } from '../../constants.js'
import { TAG } from '../../wire/tags.js'
import { IndexedPeer, type LossyLane, type PeerSender, type RoomWaiter } from './IndexedPeer.js'
import { configureBroadcastTransport, disposeBackend } from '../../backend/install.js'
import type { BroadcastTransport } from '../../backend/broadcast/TransportBroadcastDriver.js'
import { PathEstimate } from '../../flowControl/PathEstimate.js'
import { PathLane } from './PathLane.js'
import type { WireQueue } from '../../flowControl/server/waitingPublishes.js'

/** `sender` with the lossy lane of its own queue. */
const withLossyLane = (sender: Omit<PeerSender, 'lossy' | 'queue'> & { queue: WireQueue }): PeerSender => ({
  ...sender,
  lossy: new PathLane(sender.queue),
})

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

    end.deliver(publishBody('"hello"', { seq: 1, timestamp: 1 }, 'room:early'))

    const frames: Uint8Array[] = []
    end._attachPeer(
      new IndexedPeer({
        sender: withLossyLane({
          send: (frame) => {
            frames.push(frame)
          },
          carry() {},
          bufferedAmount: () => 0,
          deliveredBefore: () => 0,
          terminate() {},
          sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
          queue: {
            waitingPublishes: () => 0,
            dropWaitingPublishes() {},
            dropWaitingFromPublish() {},
            drainRate: () => 0,
            acknowledged: () => true,
            unhandedBytes: 0,
            path: new PathEstimate({ reportGap: 0 }),
          },
        }),
        index: 7,
        replay: new ServerReplay({ heldBytes: 0 }),
      }),
    )

    expect(frames.map((frame) => decode(frame as Uint8Array<ArrayBuffer>).tag)).toEqual([TAG.PUBLISH_TO, TAG.PUBLISH])
  })
  it('opens a domain per stream, announced in order with what its channel carries', () => {
    const { end, frames } = attachedEnd()
    end.deliver(publishBody(new Uint8Array([1]), { seq: 1, timestamp: 1 }, 'cam', 'cam'))
    end.deliver(publishBody('"event"', { seq: 2, timestamp: 1 }, null))
    end.deliver(publishBody(new Uint8Array([2]), { seq: 3, timestamp: 1 }, null, 'mic'))
    end.deliver(publishBody(new Uint8Array([3]), { seq: 4, timestamp: 1 }, 'cam', 'cam'))

    const decoded = frames.map((frame) => decode(frame as Uint8Array<ArrayBuffer>))
    expect(decoded.map((frame) => frame.tag)).toEqual([TAG.DOMAIN, TAG.PUBLISH_TO, TAG.PUBLISH, TAG.DOMAIN])
    const [cam, mic] = announcements(frames)
    expect(cam).toMatchObject({ stream: 'cam', lossy: true })
    expect(mic).toMatchObject({ stream: 'mic', lossy: false })
    expect(typeof cam!.channelId).toBe('string')
    expect(cam!.channelId).not.toBe(mic!.channelId)
  })

  it("answers its page's request for a domain, and past the cap with none", () => {
    const { end, frames } = attachedEnd()
    const ask = (stream: string, seq: number) =>
      end._dispatchFrame(decode(encode.domain(7, JSON.stringify({ stream }), seq)) as ChannelFrame)

    ask('mine', 1)
    for (let i = 1; i < MAX_DOMAINS_PER_CHANNEL; i++) {
      end.deliver(publishBody(new Uint8Array([i]), { seq: i, timestamp: 1 }, null, `track-${i}`))
    }
    // Asked again, an open domain isn't announced twice: the page has its first announcement.
    ask('mine', 2)
    ask('one too many', 3)

    const asked = announcements(frames).filter(({ stream }) => stream === 'mine' || stream === 'one too many')
    expect(asked.map(({ stream, channelId }) => ({ stream, named: channelId !== null }))).toEqual([
      { stream: 'mine', named: true },
      { stream: 'one too many', named: false },
    ])
  })

  it('registers its domains on its mux, one opened before it registered as it registers', () => {
    const registered: string[] = []
    const mux = { registerChannel: (channel: ServerBroadcastEnd) => registered.push(channel.id) }
    const end = new ServerBroadcastEnd()
    end.setOwner({ _onPagePublish() {}, _onPagePublishBinary() {} })

    end.deliver(publishBody(new Uint8Array([1]), { seq: 1, timestamp: 1 }, null, 'cam'))
    expect(registered).toHaveLength(0)
    end._registerChannel(mux as unknown as ChannelMux)
    end.deliver(publishBody(new Uint8Array([2]), { seq: 2, timestamp: 1 }, null, 'mic'))

    expect(new Set(registered).size).toBe(2)
  })
})

describe('lossy groups (`delta`)', () => {
  const publish = (end: ServerBroadcastEnd, seq: number, delta: boolean) =>
    end.deliver(publishBody(new Uint8Array([seq]), { seq, timestamp: 1 }, 'cam', null, delta))

  it('a page behind its path skips whole groups, and gets the newest from its first publish on', () => {
    const lane = gateLane()
    const { end, frames } = attachedEnd(lane)
    publish(end, 1, false)
    publish(end, 2, true)
    publish(end, 3, true)
    publish(end, 4, false)
    publish(end, 5, true)
    lane.setOpen(true)

    expect(publishedSeqs(frames)).toEqual([4, 5])
  })

  it("once a publish of a group is skipped, the page gets none of that group's later ones", () => {
    const lane = gateLane()
    // On the mux it runs on, as memory pressure skips waiting publishes only there.
    const { end, frames } = attachedEnd(lane, getChannelMux())
    lane.setOpen(true)
    publish(end, 1, false)
    lane.setOpen(false)
    publish(end, 2, true)
    end.replaceWaiting('cam')
    publish(end, 3, true)
    lane.setOpen(true)
    publish(end, 4, false)

    expect(publishedSeqs(frames)).toEqual([1, 4])
  })

  it('a page that starts within a group gets nothing of it', () => {
    const lane = gateLane()
    const { end, frames } = attachedEnd(lane)
    lane.setOpen(true)
    publish(end, 1, true)
    publish(end, 2, false)
    publish(end, 3, true)

    expect(publishedSeqs(frames)).toEqual([2, 3])
  })
})

/** A lossy lane whose path is the limit, which hands only while open. */
function gateLane() {
  let open = false
  const waiters = new Set<RoomWaiter>()
  return {
    pathBound: true,
    offer() {},
    mayHand(_now: number, waiter: RoomWaiter) {
      if (!open) waiters.add(waiter)
      return open
    },
    hand: (peer: IndexedPeer, body: PublishBody) => peer.sendPublish(body),
    setOpen(next: boolean) {
      open = next
      for (const waiter of waiters) waiter.roomMayHaveOpened()
      waiters.clear()
    },
  }
}

function publishedSeqs(frames: Uint8Array[]): number[] {
  return frames.flatMap((frame) => {
    const decoded = decode(frame as Uint8Array<ArrayBuffer>)
    return decoded.tag === TAG.PUBLISH_BINARY ? [decoded.info.seq] : []
  })
}

/** An end attached on index 7, and the frames it sent. */
function attachedEnd(lossy?: LossyLane, mux?: ChannelMux) {
  const end = new ServerBroadcastEnd()
  end.setOwner({ _onPagePublish() {}, _onPagePublishBinary() {} })
  end._registerChannel(mux)
  const frames: Uint8Array[] = []
  const sender = withLossyLane({
    send: (frame) => {
      frames.push(frame)
    },
    carry() {},
    bufferedAmount: () => 0,
    deliveredBefore: () => 0,
    terminate() {},
    sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
    queue: {
      waitingPublishes: () => 0,
      dropWaitingPublishes() {},
      dropWaitingFromPublish() {},
      drainRate: () => 0,
      acknowledged: () => true,
      unhandedBytes: 0,
      path: new PathEstimate({ reportGap: 0 }),
    },
  })
  end._attachPeer(
    new IndexedPeer({
      sender: lossy ? { ...sender, lossy } : sender,
      index: 7,
      replay: new ServerReplay({ heldBytes: 0 }),
    }),
  )
  return { end, frames }
}

function announcements(frames: Uint8Array[]): DomainAnnouncement[] {
  return frames.flatMap((frame) => {
    const decoded = decode(frame as Uint8Array<ArrayBuffer>)
    return decoded.tag === TAG.DOMAIN ? [JSON.parse(decoded.text) as DomainAnnouncement] : []
  })
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
        sender: withLossyLane({
          send: (frame) => {
            frames.push(frame)
          },
          carry() {},
          bufferedAmount: () => 0,
          deliveredBefore: () => 0,
          terminate() {},
          sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
          queue: {
            waitingPublishes: () => 0,
            dropWaitingPublishes() {},
            dropWaitingFromPublish() {},
            drainRate: () => 0,
            acknowledged: () => true,
            unhandedBytes: 0,
            path: new PathEstimate({ reportGap: 0 }),
          },
        }),
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
        sender: withLossyLane({
          send: () => {},
          carry() {},
          bufferedAmount: () => 0,
          deliveredBefore: () => 0,
          terminate() {},
          sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
          queue: {
            waitingPublishes: () => 0,
            dropWaitingPublishes() {},
            dropWaitingFromPublish() {},
            drainRate: () => 0,
            acknowledged: () => true,
            unhandedBytes: 0,
            path: new PathEstimate({ reportGap: 0 }),
          },
        }),
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
    const broadcast = new ServerBroadcast<string>({ key: 'room:lossy-shed', lossy: true })
    const end = broadcast._end
    end._registerChannel()
    end._attachPeer(
      new IndexedPeer({
        sender: withLossyLane({
          send() {},
          carry() {},
          bufferedAmount: () => 0,
          deliveredBefore: () => 0,
          terminate() {},
          sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
          queue: {
            waitingPublishes: () => 0,
            dropWaitingPublishes() {},
            dropWaitingFromPublish() {},
            drainRate: () => 0,
            acknowledged: () => true,
            // More than its path has room for: the page is behind.
            unhandedBytes: Infinity,
            path: new PathEstimate({ reportGap: 0 }),
          },
        }),
        index: 7,
        replay: end._replayBuffer!,
      }),
      { broadcast: { text: true, binary: false } },
    )
    await broadcast.publish('first')
    await new Promise((resolve) => setTimeout(resolve, 5))
    await broadcast.publish('x'.repeat(256 * 1024))
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
})
