// A page's want reaches its broadcast both ways, on its wire and with its telefunction calls: the broadcast applies the
// newest whichever comes first, and a call changes only the wants of the broadcasts its own session's calls returned.
// The real ChannelMux, ServerBroadcast and in-memory Broadcast run; the page's wire is a sender that records frames.

import { expect, test, vi } from 'vitest'

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
import { createDeferred, type Deferred } from '../../../utils/createDeferred.js'
import { config } from '../../../node/server/serverConfig.js'

config.channel = { maxMemory: '2m' }

/** A broadcast that the call of `session` returned, its page attached at index 0 and wanting nothing yet. */
function attachedBroadcast(session: string) {
  const page = returnedBroadcast(session)
  page.attach()
  return page
}

/** A broadcast that the call of `session` returned, its page not attached yet. */
function returnedBroadcast(session: string, lossy = false) {
  const key = crypto.randomUUID()
  const broadcast = new ServerBroadcast<string>({ key, lossy })
  // As `carryChannel` stamps it.
  broadcast._callRequest = callOf(session)
  getChannelMux().registerChannel(broadcast)
  const frames: Uint8Array[] = []
  const sender: PeerSender = {
    send: (frame) => frames.push(frame),
    carry() {},
    bufferedAmount: () => 0,
    behindAfter: () => Number.POSITIVE_INFINITY,
    deliveredBefore: () => 0,
    sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
    queue: { waitingPublishes: () => 0, dropUngiven: () => 0, dropWaitingPublishes() {}, dropWaitingFrom() {} },
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
    /** A call of `from` carries the want; `callAborted` aborts where its request goes. */
    call: (want: Want, from = session, callAborted = new AbortController().signal) =>
      getChannelMux().applyCallWants(callOf(from, callAborted), [{ channel: broadcast.id, ...want }]),
    /** Whether a publish to the key now reaches the page. */
    async hears(): Promise<boolean> {
      const before = frames.filter((frame) => frame[0] === TAG.PUBLISH).length
      await Broadcast.publish(key, 'probe')
      return frames.filter((frame) => frame[0] === TAG.PUBLISH).length > before
    },
    wanted: () => frames.filter((frame) => frame[0] === TAG.WANTED).map((frame) => decode(frame)),
  }
}

/** A telefunction call of `session`'s page, as `getSessionUrl` names it. */
function callOf(session: string, signal?: AbortSignal) {
  return new Request(`https://example.test/_telefunc?session=${session}`, { method: 'POST', signal })
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
  const call = [{ channel: crypto.randomUUID(), ...on(1) }]
  expect(getChannelMux().applyCallWants(callOf('session-a'), call)).toBe(undefined)
  expect(await page.hears()).toBe(false)
  page.broadcast.abort()
})

// Subscribe (1), unsubscribe (2), each reaching the server on the wire and with a call, in every order.
const ORDERS: Record<string, { steps: ((page: ReturnType<typeof attachedBroadcast>) => unknown)[]; hears: boolean }> = {
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

// What a page subscribed to by a call and waits for its first attach counts as what any page holds that no wire has.
for (const lossy of [false, true]) {
  test(`memory pressure ends the ${lossy ? 'lossy ' : ''}page whose first attach is late with what it holds, not a page that keeps up`, async () => {
    const binary = { version: 1, text: false, binary: true }
    const late = returnedBroadcast('session-a', lossy)
    await late.call(binary)
    // A lossy page holds its newest only.
    for (const size of lossy ? [1_000_000] : [570_000, 570_000, 570_000])
      await Broadcast.publishBinary(late.key, new Uint8Array(size))
    const live = attachedBroadcast('session-a')
    live.wire(binary)
    await Broadcast.publishBinary(live.key, new Uint8Array(lossy ? 1_200_000 : 520_000))
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect({ late: late.broadcast.isClosed, live: live.broadcast.isClosed }).toEqual({ late: true, live: false })
    live.broadcast.abort()
    late.broadcast._release()
  })
}

test('before the first attach of a page that has not subscribed, nothing published is held for it', async () => {
  const page = returnedBroadcast('session-a')
  await Broadcast.publish(page.key, 'before-subscribe')
  page.attach(on(1))
  await Broadcast.publish(page.key, 'after-subscribe')
  expect(page.publishes()).toEqual({ text: 1, binary: 0 })
  page.broadcast.abort()
})

// As a plain channel sends what it held across a close that ran out before its page attached.
test("a close that runs out before the page's first attach keeps what the page subscribed to by a call, ahead of the CLOSE and its timeout", async () => {
  const page = returnedBroadcast('session-a')
  await page.call(on(1))
  await Broadcast.publish(page.key, 'after-subscribe')
  await page.broadcast.close({ timeout: 1 })
  page.attach(on(1))
  expect(page.tags()).toEqual([TAG.PUBLISH_TO, TAG.PUBLISH, TAG.CLOSE, TAG.ERROR])
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
        await expect(page.broadcast.closed).resolves.toHaveProperty(
          'message',
          expect.stringContaining('broker connection closed'),
        )
        expect(page.tags()).toContain(TAG.ERROR)
      } finally {
        _resetBroadcastAdapterForTesting(previous)
      }
    })
  }
}

/** A broker that confirms or refuses each key's listener when the test says so. */
function confirmingLater(run: (listening: (key: string) => Deferred<void>) => Promise<void>) {
  return async () => {
    const waits = new Map<string, Deferred<void>>()
    const listen = (key: string) => {
      const opened = createDeferred()
      waits.set(key, opened)
      return { unsubscribe() {}, opened: opened.promise }
    }
    const sent = () => ({ seq: 1, timestamp: 1 })
    const previous = getBroadcastAdapter()
    _resetBroadcastAdapterForTesting(
      new DefaultBroadcastAdapter({ send: sent, sendBinary: sent, listen, listenBinary: listen }),
    )
    try {
      await run((key) => {
        const opened = waits.get(key)
        if (opened === undefined) throw new Error(`Nothing listens to ${key}`)
        return opened
      })
    } finally {
      _resetBroadcastAdapterForTesting(previous)
    }
  }
}

/** Whether `pending` settled by the time the timers due now ran. */
async function settled(pending: Promise<unknown> | undefined): Promise<boolean> {
  let done = false
  void pending?.finally(() => (done = true))
  await new Promise((resolve) => setTimeout(resolve, 0))
  return done
}

test(
  'a call waits for each subscription it carries by itself: one its backend refuses leaves it waiting for the other',
  confirmingLater(async (listening) => {
    const refused = attachedBroadcast('session-a')
    const healthy = attachedBroadcast('session-a')
    const wants = [refused, healthy].map((page) => ({ channel: page.broadcast.id, ...on(1) }))
    const call = getChannelMux().applyCallWants(callOf('session-a'), wants)
    listening(refused.key).reject(new Error('NOPERM'))
    await expect(refused.broadcast.closed).resolves.toHaveProperty('message', expect.stringContaining('NOPERM'))
    expect(await settled(call)).toBe(false)
    listening(healthy.key).resolve()
    expect(await settled(call)).toBe(true)
    expect(healthy.wanted()).toEqual([{ tag: TAG.WANTED, index: 0, version: 1 }])
    healthy.broadcast.abort()
  }),
)

test(
  "a call whose request goes while a subscription it carries is on its way ends that broadcast, before the call's telefunction would run",
  confirmingLater(async () => {
    const page = attachedBroadcast('session-a')
    const request = new AbortController()
    const call = page.call(on(1), 'session-a', request.signal)
    expect(await settled(call)).toBe(false)
    request.abort()
    await call
    expect(page.broadcast.isClosed).toBe(true)
    await expect(page.broadcast.closed).resolves.toHaveProperty(
      'message',
      expect.stringContaining('before the subscription took effect'),
    )
    expect(page.tags()).toContain(TAG.ERROR)
    expect(page.wanted()).toEqual([])
  }),
)

test(
  'a call waits `connectTtl` at most for a subscription its backend neither confirms nor refuses: that broadcast then ends with the error, the call goes on, and a sibling in effect stays',
  confirmingLater(async (listening) => {
    vi.useFakeTimers()
    try {
      const silent = attachedBroadcast('session-a')
      const healthy = attachedBroadcast('session-a')
      const wants = [silent, healthy].map((page) => ({ channel: page.broadcast.id, ...on(1) }))
      let done = false
      void getChannelMux()
        .applyCallWants(callOf('session-a'), wants)
        ?.finally(() => (done = true))
      listening(healthy.key).resolve()
      await vi.advanceTimersByTimeAsync(getChannelMux().connectTtl - 1)
      expect(done).toBe(false)
      await vi.advanceTimersByTimeAsync(1)
      expect(done).toBe(true)
      await expect(silent.broadcast.closed).resolves.toHaveProperty(
        'message',
        expect.stringContaining('did not take effect within'),
      )
      expect(silent.tags()).toContain(TAG.ERROR)
      expect({ closed: healthy.broadcast.isClosed, wanted: healthy.wanted() }).toEqual({
        closed: false,
        wanted: [{ tag: TAG.WANTED, index: 0, version: 1 }],
      })
      healthy.broadcast.abort()
    } finally {
      vi.useRealTimers()
    }
  }),
)

// Each way a call's wait ends leaves no timer of its own behind, so none ends the broadcast later.
const WAIT_ENDS: Record<
  string,
  (end: { opened: Deferred<void>; request: AbortController; page: ReturnType<typeof attachedBroadcast> }) => void
> = {
  'the subscription takes effect': ({ opened }) => opened.resolve(),
  'its backend refuses it': ({ opened }) => opened.reject(new Error('NOPERM')),
  "the call's request goes": ({ request }) => request.abort(),
  'the broadcast is closed': ({ page }) => page.broadcast.abort(),
}
for (const [end, endWait] of Object.entries(WAIT_ENDS)) {
  test(
    `a call's wait for a subscription ends with no timer left once ${end}`,
    confirmingLater(async (listening) => {
      vi.useFakeTimers()
      try {
        const page = attachedBroadcast('session-a')
        const before = vi.getTimerCount()
        const request = new AbortController()
        const call = page.call(on(1), 'session-a', request.signal)
        endWait({ opened: listening(page.key), request, page })
        await call
        await vi.advanceTimersByTimeAsync(0)
        const left = vi.getTimerCount() - before
        await vi.advanceTimersByTimeAsync(getChannelMux().connectTtl)
        expect({ left, closed: page.broadcast.isClosed }).toEqual({
          left: 0,
          closed: end !== 'the subscription takes effect',
        })
        page.broadcast.abort()
      } finally {
        vi.useRealTimers()
      }
    }),
  )
}

test("a call whose new subscription reaches a broadcast the server is closing waits for its end, and the page isn't told it's in effect", async () => {
  const page = attachedBroadcast('session-a')
  // The page hasn't answered the server's CLOSE: the broadcast is closing.
  const closing = page.broadcast.close({ timeout: 50 })
  let ran = false
  const call = page.call(on(1))
  void Promise.resolve(call).then(() => {
    ran = true
  })
  await new Promise((resolve) => setTimeout(resolve, 10))
  expect({ ran, wanted: page.wanted(), hears: await page.hears() }).toEqual({ ran: false, wanted: [], hears: false })
  await closing
  await call
  expect(ran).toBe(true)
  page.broadcast._release()
})
