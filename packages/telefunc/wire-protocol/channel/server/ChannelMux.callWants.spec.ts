// A page's want reaches its broadcast both ways, on its wire and with its telefunction calls: the broadcast applies the
// newest whichever comes first, and a call changes only the wants of the broadcasts its own session's calls returned.
// The real ChannelMux, ServerBroadcast and in-memory Broadcast run; the page's wire is a sender that records frames.

import { expect, test } from 'vitest'

import { getChannelMux } from './ChannelMux.js'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import { decode } from '../../wire/frame.js'
import {
  DefaultBroadcastAdapter,
  _resetBroadcastAdapterForTesting,
  getBroadcastAdapter,
} from '../../broadcast/broadcastAdapter.js'
import { TAG } from '../../wire/tags.js'
import type { Want } from '../../wire/payloads.js'

/** A broadcast that the call of `session` returned, its page attached at index 0 and wanting nothing yet. */
function attachedBroadcast(session: string) {
  const page = returnedBroadcast(session)
  page.attach()
  return page
}

/** A broadcast that the call of `session` returned, its page not attached yet. */
function returnedBroadcast(session: string) {
  const key = crypto.randomUUID()
  const broadcast = new ServerBroadcast<string>({ key })
  // As `carryChannel` stamps it.
  broadcast._session = session
  getChannelMux().registerChannel(broadcast)
  const frames: Uint8Array[] = []
  const sender: PeerSender = {
    send: (frame) => frames.push(frame),
    carry() {},
    bufferedAmount: () => 0,
    behindAfter: () => Number.POSITIVE_INFINITY,
    deliveredBefore: () => 0,
    sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
    queue: { waitingPublishes: () => 0, dropWaitingPublishes() {}, dropWaitingFrom() {} },
  }
  const count = (tag: number) => frames.filter((frame) => frame[0] === tag).length
  return {
    key,
    broadcast,
    /** The page attaches at index 0, its RECONCILE restating `want`. */
    attach(want?: Want) {
      const replay = broadcast._replayBuffer
      if (!replay) throw new Error('A registered broadcast has its replay')
      broadcast._attachPeer(new IndexedPeer({ sender, index: 0, replay }), { want })
    },
    publishes: () => ({ text: count(TAG.PUBLISH), binary: count(TAG.PUBLISH_BINARY) }),
    tags: () => frames.map((frame) => frame[0]),
    /** The page's WANT frame arrives. */
    wire: (want: Want) => broadcast._dispatchFrame({ tag: TAG.WANT, index: 0, want }),
    /** A call of `from` carries the want. */
    call: (want: Want, from = session) => getChannelMux().applyCallWants(from, [{ channel: broadcast.id, ...want }]),
    /** Whether a publish to the key now reaches the page. */
    async hears(): Promise<boolean> {
      const before = frames.filter((frame) => frame[0] === TAG.PUBLISH).length
      await Broadcast.publish(key, 'probe')
      return frames.filter((frame) => frame[0] === TAG.PUBLISH).length > before
    },
    wanted: () => frames.filter((frame) => frame[0] === TAG.WANTED).map((frame) => decode(frame)),
  }
}

const on = (version: number): Want => ({ version, text: true, binary: false })
const off = (version: number): Want => ({ version, text: false, binary: false })

test("a call that names another session's broadcast changes nothing", async () => {
  const page = attachedBroadcast('session-a')
  await page.call(on(1), 'session-b')
  expect(await page.hears()).toBe(false)
  expect(page.wanted()).toEqual([])
  await page.call(on(1))
  expect(await page.hears()).toBe(true)
  expect(page.wanted()).toEqual([{ tag: TAG.WANTED, index: 0, version: 1 }])
  page.broadcast.abort()
})

test('a call of a session that names a broadcast no session holds changes nothing', async () => {
  const page = attachedBroadcast('session-a')
  expect(getChannelMux().applyCallWants('session-a', [{ channel: crypto.randomUUID(), ...on(1) }])).toBe(undefined)
  expect(await page.hears()).toBe(false)
  page.broadcast.abort()
})

// Subscribe (1), unsubscribe (2), each reaching the server on the wire and with a call, in every order.
const ORDERS: Record<string, { steps: ((page: ReturnType<typeof attachedBroadcast>) => unknown)[]; hears: boolean }> = {
  'the wire, then the call, of a subscribe': { steps: [(p) => p.wire(on(1)), (p) => p.call(on(1))], hears: true },
  'the call, then the wire, of a subscribe': { steps: [(p) => p.call(on(1)), (p) => p.wire(on(1))], hears: true },
  'an unsubscribe by call ahead of the subscribe by wire': {
    steps: [(p) => p.call(off(2)), (p) => p.wire(on(1))],
    hears: false,
  },
  'an unsubscribe by wire ahead of the subscribe by call': {
    steps: [(p) => p.wire(off(2)), (p) => p.call(on(1))],
    hears: false,
  },
  'a subscribe, an unsubscribe and a subscribe again, the last by call ahead of the rest': {
    steps: [(p) => p.call(on(3)), (p) => p.wire(on(1)), (p) => p.wire(off(2))],
    hears: true,
  },
  'a subscribe and an unsubscribe by wire, then the call of the subscribe': {
    steps: [(p) => p.wire(on(1)), (p) => p.wire(off(2)), (p) => p.call(on(1))],
    hears: false,
  },
}

for (const [order, { steps, hears }] of Object.entries(ORDERS)) {
  test(`the newest want holds: ${order}`, async () => {
    const page = attachedBroadcast('session-a')
    for (const step of steps) await step(page)
    expect(await page.hears()).toBe(hears)
    page.broadcast.abort()
  })
}

// A call can bring the page's subscription before the page's first attach: the page subscribed, then called at once.
test("before the page's first attach, what it subscribed to by a call waits for the attach, and nothing else does", async () => {
  const page = returnedBroadcast('session-a')
  await page.call(on(1))
  await Broadcast.publish(page.key, 'after-subscribe')
  await Broadcast.publishBinary(page.key, new Uint8Array([1]))
  page.attach(on(1))
  expect(page.publishes()).toEqual({ text: 1, binary: 0 })
  page.broadcast.abort()
})

test('before the first attach of a page that has not subscribed, nothing published is held for it', async () => {
  const page = returnedBroadcast('session-a')
  await Broadcast.publish(page.key, 'before-subscribe')
  page.attach(on(1))
  await Broadcast.publish(page.key, 'after-subscribe')
  expect(page.publishes()).toEqual({ text: 1, binary: 0 })
  page.broadcast.abort()
})

// As a plain channel sends what it held across a close that ran out before its page attached.
test("a close that runs out before the page's first attach keeps what the page subscribed to by a call, ahead of the CLOSE", async () => {
  const page = returnedBroadcast('session-a')
  await page.call(on(1))
  await Broadcast.publish(page.key, 'after-subscribe')
  await page.broadcast.close({ timeout: 1 })
  page.attach(on(1))
  expect(page.tags()).toEqual([TAG.PUBLISH_TO, TAG.PUBLISH, TAG.CLOSE])
  page.broadcast._release()
})

test('an ended broadcast takes no newer want its late attach brings, and listens to its backend no more', async () => {
  let listening = 0
  const listen = () => {
    listening++
    return () => {
      listening--
    }
  }
  const sent = () => ({ seq: 1, timestamp: 1 })
  const previous = getBroadcastAdapter()
  _resetBroadcastAdapterForTesting(
    new DefaultBroadcastAdapter({ send: sent, sendBinary: sent, listen, listenBinary: listen }),
  )
  try {
    const page = returnedBroadcast('session-a')
    await page.call(on(1))
    expect(listening).toBe(1)
    page.broadcast.abort()
    const counts = [listening]
    // Meanwhile the page subscribed to its binary publishes too.
    page.attach({ version: 2, text: true, binary: true })
    counts.push(listening)
    page.broadcast._release()
    counts.push(listening)
    expect(counts).toEqual([0, 0, 0])
  } finally {
    _resetBroadcastAdapterForTesting(previous)
  }
})

// A transport that refuses a page's subscription at once fails it as a backend that gives it up later does: the
// broadcast ends with that error, which the page reads, and the call that carried the subscription goes on.
const KINDS: ('text' | 'binary')[] = ['text', 'binary']
const ARRIVALS: ('call' | 'wire')[] = ['call', 'wire']
for (const kind of KINDS) {
  for (const arrival of ARRIVALS) {
    test(`a ${kind} subscription the transport refuses at once, arriving by ${arrival}, ends the broadcast with the transport's error`, async () => {
      const refuse = () => {
        throw new Error('broker connection closed')
      }
      const sent = () => ({ seq: 1, timestamp: 1 })
      const previous = getBroadcastAdapter()
      _resetBroadcastAdapterForTesting(
        new DefaultBroadcastAdapter({ send: sent, sendBinary: sent, listen: refuse, listenBinary: refuse }),
      )
      try {
        const page = attachedBroadcast('session-a')
        const want = { version: 1, text: kind === 'text', binary: kind === 'binary' }
        if (arrival === 'call') await page.call(want)
        else page.wire(want)
        await expect(page.broadcast.closed).rejects.toThrow('broker connection closed')
        expect(page.tags()).toContain(TAG.ERROR)
      } finally {
        _resetBroadcastAdapterForTesting(previous)
      }
    })
  }
}
