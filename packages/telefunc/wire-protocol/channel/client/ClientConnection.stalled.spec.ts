// A channel whose data gets no further while the page reconnects: judged at each loss once a reconnect was answered
// after its date, as on 0.3.1 (decisions/516-sse-carrier.md, 13:08Z step 3). A REAL ClientChannel and its connection
// against a scripted server (scriptedServer.fixture.ts), over SSE batch POSTs unless a test says otherwise.

import { expect, test } from 'vitest'
import { SSE_RECONCILE_DEADLINE_MS } from '../../constants.js'
import {
  FakeWebSocket,
  RECONNECT_TIMEOUT_MS,
  advance,
  holdPage,
  openChannel,
  serveSse,
  useScriptedServer,
  windowOf,
  world,
} from './scriptedServer.fixture.js'
import type { PostAnswer } from './scriptedServer.fixture.js'

useScriptedServer()

/** `value`, which the test must have by now: `what` says what it is. */
function defined<T>(value: T | null | undefined, what: string): T {
  if (value === null || value === undefined) throw new Error(`Expected ${what}`)
  return value
}

/** Opens `count` channels over SSE, the first at index 0, and waits till their wire carries them. */
async function openOverSse(count: number) {
  serveSse()
  const opened = Array.from({ length: count }, () => openChannel(['sse']))
  await advance(SSE_RECONCILE_DEADLINE_MS + 500)
  return (i: number) => defined(opened[i], `channel ${i}`)
}

/** What the server got of channel `ix`. */
const gotOf = (ix: number) => world.server.got.filter((got) => got.ix === ix)

/** The most a reconnect waits, with the fixture's jitter. */
const MOST_BACKOFF_MS = 4_500

test('a channel whose data never gets through ends at the first loss after reconnectTimeout, naming what the page saw', async () => {
  const opened = await openOverSse(2)
  const stuck = opened(0)
  const other = opened(1)
  world.server.cuts.add(0)
  const sentAt = performance.now()
  void stuck.channel.send('never through').catch(() => {})
  await advance(1_000)
  // Queued behind the stuck data in the one outbox: it goes once the stuck channel has ended.
  void other.channel.send('behind it').catch(() => {})
  await advance(30_000)
  expect(stuck.ended).toHaveLength(1)
  const { at, err } = defined(stuck.ended[0], 'the stuck channel ending')
  expect(err?.message).toMatch(
    /the server hasn't confirmed what the page sent on it for over \d+ s of reconnects\. The last try: Connection dropped/,
  )
  expect(at - sentAt).toBeGreaterThan(RECONNECT_TIMEOUT_MS)
  expect(at - sentAt).toBeLessThan(RECONNECT_TIMEOUT_MS + MOST_BACKOFF_MS + 1_000)
  expect(other.ended).toEqual([])
  // The page puts the undated channel's message back ahead of the stuck data: it arrives at the next reconnect.
  expect(gotOf(1)).toHaveLength(1)
  expect(defined(gotOf(1)[0], "the other channel's message").at - sentAt).toBeLessThan(1_000 + MOST_BACKOFF_MS + 1_000)
})

test('a send behind the stuck message on its channel ends with it, with the same cause: it never goes ahead of it', async () => {
  const page = (await openOverSse(1))(0)
  world.server.cuts.add(0)
  void page.channel.send('never through').catch(() => {})
  await advance(1_000)
  // The server applies a channel's frames in seq order, so the hello can't arrive before the message ahead of it.
  const told: Error[] = []
  void page.channel.send('hello behind it', { ack: true }).catch((err: unknown) => {
    if (err instanceof Error) told.push(err)
  })
  await advance(30_000)
  const { err } = defined(page.ended[0], 'the channel ending')
  expect(err?.message).toMatch(
    /the server hasn't confirmed what the page sent on it for over \d+ s of reconnects\. The last try: Connection dropped/,
  )
  expect(told.map(({ message }) => message)).toEqual([err?.message])
  expect(gotOf(0)).toEqual([])
})

test('over WebSocket, a channel whose data never gets through ends at the first loss after reconnectTimeout, and the other goes on', async () => {
  const opened = Array.from({ length: 2 }, () => openChannel(['ws']))
  await advance(SSE_RECONCILE_DEADLINE_MS + 500)
  const stuck = defined(opened[0], 'channel 0')
  const other = defined(opened[1], 'channel 1')
  // Each frame of the stuck channel cuts the socket it goes on.
  world.server.cuts.add(0)
  const sentAt = performance.now()
  void stuck.channel.send('never through').catch(() => {})
  await advance(1_000)
  void other.channel.send('behind it').catch(() => {})
  await advance(30_000)
  // The page tried again and again: every socket after the first was cut by the stuck data.
  expect(FakeWebSocket.made.length).toBeGreaterThan(3)
  const { at, err } = defined(stuck.ended[0], 'the stuck channel ending')
  expect(err?.message).toMatch(
    /the server hasn't confirmed what the page sent on it for over \d+ s of reconnects\. The last try: /,
  )
  expect(at - sentAt).toBeGreaterThan(RECONNECT_TIMEOUT_MS)
  expect(at - sentAt).toBeLessThan(RECONNECT_TIMEOUT_MS + MOST_BACKOFF_MS + 1_000)
  expect(other.ended).toEqual([])
  expect(gotOf(1)).toHaveLength(1)
  expect(defined(gotOf(1)[0], "the other channel's message").at - sentAt).toBeLessThan(1_000 + MOST_BACKOFF_MS + 1_000)
})

test('a channel whose POSTs are refused only after their wire dropped ends at the first loss after reconnectTimeout', async () => {
  const opened = await openOverSse(2)
  const stuck = opened(0)
  const other = opened(1)
  world.server.lateRefusals.add(0)
  const sentAt = performance.now()
  void stuck.channel.send('never through').catch(() => {})
  await advance(1_000)
  void other.channel.send('behind it').catch(() => {})
  await advance(30_000)
  const { at, err } = defined(stuck.ended[0], 'the stuck channel ending')
  expect(err?.message).toMatch(
    /the server hasn't confirmed what the page sent on it for over \d+ s of reconnects\. The last try: /,
  )
  expect(at - sentAt).toBeGreaterThan(RECONNECT_TIMEOUT_MS)
  expect(at - sentAt).toBeLessThan(RECONNECT_TIMEOUT_MS + MOST_BACKOFF_MS + 1_000)
  expect(other.ended).toEqual([])
  expect(gotOf(1)).toHaveLength(1)
})

test('while a channel holds a date, a reconnect answer does not reset the attempts: the delays double to the cap', async () => {
  const page = (await openOverSse(1))(0)
  world.server.cuts.add(0)
  void page.channel.send('never through').catch(() => {})
  // Before the channel ends: with the fixture's jitter the delays are 0.45, 0.9, 1.8, 3.6 s, then 4.5 s each.
  await advance(RECONNECT_TIMEOUT_MS)
  expect(page.ended).toEqual([])
  expect(world.streams.length).toBeLessThanOrEqual(7)
})

test('a channel closed behind its stuck data is judged as an open one: it ends with the cause, and its close settles', async () => {
  const page = (await openOverSse(1))(0)
  world.server.cuts.add(0)
  const sentAt = performance.now()
  void page.channel.send('never through').catch(() => {})
  await advance(1_000)
  let closed: number | 'pending' = 'pending'
  // Its own timeout past the verdict: the verdict settles it.
  void page.channel.close({ timeout: 4 * RECONNECT_TIMEOUT_MS }).then((result) => (closed = result))
  await advance(30_000)
  const { at, err } = defined(page.ended[0], 'the channel ending')
  expect(err?.message).toMatch(
    /the server hasn't confirmed what the page sent on it for over \d+ s of reconnects\. The last try: Connection dropped/,
  )
  expect(at - sentAt).toBeGreaterThan(RECONNECT_TIMEOUT_MS)
  expect(at - sentAt).toBeLessThan(RECONNECT_TIMEOUT_MS + MOST_BACKOFF_MS + 1_000)
  expect(closed).not.toBe('pending')
  expect(gotOf(0)).toEqual([])
})

test('a channel closed behind its stuck data, its close timed out, is released at its verdict: its data goes no more', async () => {
  const page = (await openOverSse(1))(0)
  world.server.cuts.add(0)
  void page.channel.send('never through').catch(() => {})
  await advance(1_000)
  void page.channel.close()
  await advance(30_000)
  expect(page.ended).toHaveLength(1)
  const tries = world.posts.filter(({ data }) => data).length
  await advance(30_000)
  expect(world.posts.filter(({ data }) => data)).toHaveLength(tries)
})

test("with no reconnect answered after its date, the connection's own recovery decides, not the channel's date", async () => {
  const page = (await openOverSse(1))(0)
  world.server.drops.add(0)
  void page.channel.send('taken, never confirmed').catch(() => {})
  await advance(1_000)
  // Every open from here goes unanswered: the outage outlasts reconnectTimeout.
  world.holdOpens = true
  const lostAt = performance.now()
  defined(world.streams.at(-1), 'an event stream').end()
  await advance(windowOf('sse') + 5_000)
  const { at, err } = defined(page.ended[0], 'the channel ending')
  expect(err?.message).not.toMatch(/hasn't confirmed/)
  expect(at - lostAt).toBeGreaterThan(windowOf('sse') - 1_000)
})

test("a refusal's cause is dropped once the data it named is confirmed: a later stall names its own loss", async () => {
  const page = (await openOverSse(1))(0)
  world.postAnswers = [{ tag: 'refused', status: 503 }]
  void page.channel.send('through after one 503').catch(() => {})
  await advance(8_000)
  expect(gotOf(0)).toHaveLength(1)
  world.server.cuts.add(0)
  void page.channel.send('never through').catch(() => {})
  await advance(30_000)
  const { err } = defined(page.ended[0], 'the channel ending')
  expect(err?.message).toMatch(/The last try: Connection dropped/)
  expect(err?.message).not.toMatch(/503/)
})

test('a reconnectTimeout that a reconnect answer changes inside the window is the one the verdict reads', async () => {
  const page = (await openOverSse(1))(0)
  world.server.cuts.add(0)
  const sentAt = performance.now()
  void page.channel.send('never through').catch(() => {})
  await advance(3_000)
  world.server.settings = { reconnectTimeout: 2 * RECONNECT_TIMEOUT_MS }
  await advance(40_000)
  const { at, err } = defined(page.ended[0], 'the channel ending')
  expect(err?.message).toMatch(new RegExp(`for over ${(2 * RECONNECT_TIMEOUT_MS) / 1_000} s of reconnects`))
  expect(at - sentAt).toBeGreaterThan(2 * RECONNECT_TIMEOUT_MS)
  expect(at - sentAt).toBeLessThan(2 * RECONNECT_TIMEOUT_MS + MOST_BACKOFF_MS + 1_000)
})

test('answers to be ridden out by time, as 503s for some seconds, end nothing: the data goes once they stop', async () => {
  const page = (await openOverSse(1))(0)
  world.postAnswers = [503, 503, 503].map((status): PostAnswer => ({ tag: 'refused', status }))
  void page.channel.send('through in the end').catch(() => {})
  await advance(20_000)
  expect(page.ended).toEqual([])
  expect(gotOf(0)).toHaveLength(1)
})

test('answers to be ridden out by time end the channel only after reconnectTimeout, naming the answer', async () => {
  const page = (await openOverSse(1))(0)
  world.postAnswers = Array.from({ length: 100 }, (): PostAnswer => ({ tag: 'refused', status: 503 }))
  const sentAt = performance.now()
  void page.channel.send('never through').catch(() => {})
  await advance(30_000)
  const { at, err } = defined(page.ended[0], 'the channel ending')
  expect(err?.message).toMatch(/The last try: The server refused what the page sent with 503 text\/html/)
  expect(at - sentAt).toBeGreaterThan(RECONNECT_TIMEOUT_MS)
})

test('a channel that got through and then stalls is dated from the loss after its progress', async () => {
  const page = (await openOverSse(1))(0)
  world.server.cuts.add(0)
  void page.channel.send('through after a while').catch(() => {})
  await advance(6_000)
  world.server.cuts.delete(0)
  await advance(6_000)
  expect(gotOf(0)).toHaveLength(1)
  world.server.cuts.add(0)
  const stalledAt = performance.now()
  void page.channel.send('never through').catch(() => {})
  await advance(30_000)
  const { at } = defined(page.ended[0], 'the channel ending')
  expect(at - stalledAt).toBeGreaterThan(RECONNECT_TIMEOUT_MS)
})

test('a channel whose data gets through between losses never ends', async () => {
  const page = (await openOverSse(1))(0)
  for (let i = 0; i < 8; i++) {
    world.server.cuts.add(0)
    void page.channel.send(`message ${i}`).catch(() => {})
    await advance(3_000)
    world.server.cuts.delete(0)
    await advance(3_000)
  }
  expect(page.ended).toEqual([])
  expect(new Set(gotOf(0).map(({ seq }) => seq)).size).toBe(8)
})

/** A channel whose data the server takes and never confirms, its wire lost at 3, 6 and 9 s after the send (dated at
 *  the first), the next open held till the date is older than reconnectTimeout: then the test answers it. */
async function overdueWithHeldOpen() {
  const page = (await openOverSse(1))(0)
  world.server.drops.add(0)
  void page.channel.send('taken, unconfirmed').catch(() => {})
  for (let i = 0; i < 3; i++) {
    await advance(3_000)
    if (i === 2) world.holdOpens = true
    defined(world.streams.at(-1), 'an event stream').end()
  }
  await advance(5_000)
  expect(world.heldOpens).toHaveLength(1)
  // The server now has the data and says so in the RECONCILED it holds.
  world.server.drops.delete(0)
  world.server.confirmed.set(0, defined(gotOf(0)[0], 'the frame the server took').seq)
  return page
}

test('a RECONCILED that confirms an overdue channel, then a loss at the same time: the channel goes on', async () => {
  const page = await overdueWithHeldOpen()
  defined(world.heldOpens.shift(), 'the held open')()
  defined(world.streams.at(-1), 'the held open stream').end()
  world.holdOpens = false
  await advance(5_000)
  expect(page.ended).toEqual([])
})

test('a loss, then the RECONCILED that would confirm an overdue channel, at the same time: the channel ends at the loss', async () => {
  const page = await overdueWithHeldOpen()
  defined(world.streams.at(-1), 'the held open stream').end()
  defined(world.heldOpens.shift(), 'the held open')()
  world.holdOpens = false
  await advance(5_000)
  expect(page.ended).toHaveLength(1)
  expect(defined(page.ended[0], 'the channel ending').err?.message).toMatch(/The last try: Connection dropped/)
})

// A held page: reconnectTimeout at its default, 60 s, so that 30 s is inside it and 90 s past it.
const DEFAULT_RECONNECT_TIMEOUT_MS = 60_000

test('a page held 30 s while its channel had data outstanding goes on, and the data arrives', async () => {
  world.server.settings = { reconnectTimeout: DEFAULT_RECONNECT_TIMEOUT_MS }
  const page = (await openOverSse(1))(0)
  world.server.cuts.add(0)
  void page.channel.send('through after the hold').catch(() => {})
  await advance(5_000)
  await holdPage(30_000)
  world.server.cuts.delete(0)
  await advance(15_000)
  expect(page.ended).toEqual([])
  expect(gotOf(0).length).toBeGreaterThan(0)
})

test('a page held 90 s whose channel had no loss before the hold makes one more attempt, which gets through', async () => {
  world.server.settings = { reconnectTimeout: DEFAULT_RECONNECT_TIMEOUT_MS }
  const page = (await openOverSse(1))(0)
  world.server.drops.add(0)
  void page.channel.send('taken before the hold, confirmed after').catch(() => {})
  await advance(1_000)
  await holdPage(90_000)
  world.server.drops.delete(0)
  world.server.confirmed.set(0, defined(gotOf(0)[0], 'the frame the server took').seq)
  await advance(30_000)
  expect(page.ended).toEqual([])
})

test('a page held 90 s whose channel was dated before the hold ends at the first loss after it, naming it', async () => {
  world.server.settings = { reconnectTimeout: DEFAULT_RECONNECT_TIMEOUT_MS }
  const page = (await openOverSse(1))(0)
  // The server takes the data and never confirms it; one loss dates the channel, and the page reconnects before the
  // hold, so that what ends it after the hold is the channel's own verdict and not the connection's recovery.
  world.server.drops.add(0)
  void page.channel.send('taken, never confirmed').catch(() => {})
  await advance(1_000)
  defined(world.streams.at(-1), 'an event stream').end()
  await advance(MOST_BACKOFF_MS + 1_000)
  expect(world.streams).toHaveLength(2)
  await holdPage(90_000)
  // The first loss after the hold: the wire's PONG is overdue, as the server's PING deadline dropped it meanwhile.
  const wokeAt = performance.now()
  await advance(5_000)
  const { at, err } = defined(page.ended[0], 'the channel ending')
  expect(at - wokeAt).toBeLessThan(5_000)
  expect(world.streams).toHaveLength(2)
  expect(err?.message).toMatch(
    /the server hasn't confirmed what the page sent on it for over \d+ s of reconnects\. The last try: Connection dropped/,
  )
})
