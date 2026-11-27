// A channel whose data gets no further while the page reconnects, at instants a browser can't stage: a loss and a
// RECONCILED at the same time, and a page whose timers were held. A REAL ClientChannel and its connection against a
// scripted server (scriptedServer.fixture.ts), over SSE batch POSTs.

import { expect, test } from 'vitest'
import { defined } from '../../../utils/defined.fixture.js'
import { SSE_RECONCILE_DEADLINE_MS } from '../../constants.js'
import { advance, holdPage, openChannel, serveSse, useScriptedServer, world } from './scriptedServer.fixture.js'

useScriptedServer()

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
const MOST_BACKOFF_MS = 7_250

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

// A held page: reconnectTimeout at its default, 60 s, so that 90 s is past it.
const DEFAULT_RECONNECT_TIMEOUT_MS = 60_000

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
