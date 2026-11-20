// How soon a frame goes over SSE batch POSTs after it waited: held while sends were held back, waiting for headroom,
// or replayed. A REAL ClientChannel and its connection against a scripted server (scriptedServer.fixture.ts), with
// Telefunc's own throttle and idle delay.

import { expect, test } from 'vitest'
import { SSE_FLUSH_THROTTLE_MS, SSE_POST_IDLE_FLUSH_DELAY_MS, SSE_RECONCILE_DEADLINE_MS } from '../../constants.js'
import { TAG } from '../../wire/tags.js'
import { advance, openChannel, serveSse, useScriptedServer, world } from './scriptedServer.fixture.js'

useScriptedServer()

/** `value`, which the test must have by now: `what` says what it is. */
function defined<T>(value: T | null | undefined, what: string): T {
  if (value === null || value === undefined) throw new Error(`Expected ${what}`)
  return value
}

/** A channel open over SSE, with Telefunc's own throttle and idle delay, idle since. */
async function openIdle() {
  world.server.settings = {
    sseFlushThrottle: SSE_FLUSH_THROTTLE_MS,
    ssePostIdleFlushDelay: SSE_POST_IDLE_FLUSH_DELAY_MS,
  }
  serveSse()
  const page = openChannel(['sse'])
  await advance(SSE_RECONCILE_DEADLINE_MS + 2_000)
  return page
}

/** When the server got the channel's `n`th frame that asks for its answer (1, the first); its seq too. */
function askedAt(n = 1) {
  return defined(
    world.server.got.filter(({ tag }) => tag === TAG.TEXT_ACK_REQ)[n - 1],
    `the server's ${n}th asking frame`,
  )
}

/** The wire goes and the page opens another, the sends held meanwhile. */
function dropWire(): void {
  defined(world.streams.at(-1), 'an event stream').end()
}

test('a send that asks for the answer goes at once on an idle channel', async () => {
  const page = await openIdle()
  const sentAt = performance.now()
  void page.channel.send('asks', { ack: true }).catch(() => {})
  await advance(1_000)
  expect(askedAt().at - sentAt).toBe(0)
})

test('a send that asks for the answer goes at once, though a POST started 100 ms before', async () => {
  const page = await openIdle()
  void page.channel.send('data').catch(() => {})
  await advance(200)
  const sentAt = performance.now()
  void page.channel.send('asks', { ack: true }).catch(() => {})
  await advance(1_000)
  expect(askedAt().at - sentAt).toBe(0)
})

test('a send that asks for the answer, held while the page reconnects, goes as the RECONCILED lets it', async () => {
  const page = await openIdle()
  dropWire()
  // The page has seen the loss: the send is held until the next wire's RECONCILED.
  await advance(1)
  void page.channel.send('asks', { ack: true }).catch(() => {})
  await advance(5_000)
  const reopened = defined(world.streams[1], 'the second event stream')
  expect(askedAt().wire).toBe(2)
  expect(askedAt().at - reopened.openedAt).toBe(0)
})

test('a send that asks for the answer, waiting behind a full POST, goes as that POST is answered', async () => {
  const page = await openIdle()
  void page.channel.send('x'.repeat(100_000)).catch(() => {})
  void page.channel.send('asks', { ack: true }).catch(() => {})
  await advance(1_000)
  const full = defined(
    world.server.got.find(({ tag }) => tag !== TAG.TEXT_ACK_REQ),
    'the full POST',
  )
  expect(askedAt().at - full.at).toBe(0)
})

test('a send that asks for the answer, replayed after a reconnect, goes as the RECONCILED asks for it', async () => {
  const page = await openIdle()
  // The server takes the frame and confirms nothing, so the next RECONCILED asks for it again.
  world.server.drops.add(0)
  void page.channel.send('asks', { ack: true }).catch(() => {})
  await advance(100)
  dropWire()
  await advance(5_000)
  const reopened = defined(world.streams[1], 'the second event stream')
  const replayed = askedAt(2)
  expect(replayed.seq).toBe(askedAt(1).seq)
  expect(replayed.at - reopened.openedAt).toBe(0)
})

test('a send just after a full POST started goes in the idle delay, not the throttle', async () => {
  const page = await openIdle()
  void page.channel.send('x'.repeat(100_000)).catch(() => {})
  await advance(5)
  const sentAt = performance.now()
  void page.channel.send('small').catch(() => {})
  await advance(1_000)
  const [, small] = world.server.got.filter(({ ix }) => ix === 0)
  expect(defined(small, 'the small send').at - sentAt).toBe(SSE_POST_IDLE_FLUSH_DELAY_MS)
})

test('frames held while the page reconnects go in the order they were sent, whatever each asks for', async () => {
  const page = await openIdle()
  dropWire()
  for (let i = 0; i < 5; i++) {
    void page.channel.send(`data ${i}`).catch(() => {})
    void page.channel.send(`asks ${i}`, { ack: true }).catch(() => {})
  }
  await advance(5_000)
  const seqs = world.server.got.filter(({ ix }) => ix === 0).map(({ seq }) => seq)
  expect(seqs).toEqual([...seqs].sort((a, b) => a - b))
  expect(seqs).toHaveLength(10)
})

test('data held while the page reconnects goes together, not a POST for each', async () => {
  const page = await openIdle()
  dropWire()
  for (let i = 0; i < 50; i++) void page.channel.send(`data ${i}`).catch(() => {})
  const before = world.posts.length
  await advance(5_000)
  expect(world.server.got.filter(({ ix }) => ix === 0)).toHaveLength(50)
  expect(world.posts.slice(before).filter(({ data }) => data).length).toBeLessThanOrEqual(2)
})

test('sends that ask for the answer, made in a loop without awaiting, go in two POSTs: the first at once, the rest together after it', async () => {
  const page = await openIdle()
  const before = world.posts.length
  for (let i = 0; i < 50; i++) void page.channel.send(`asks ${i}`, { ack: true }).catch(() => {})
  await advance(5_000)
  expect(world.server.got.filter(({ tag }) => tag === TAG.TEXT_ACK_REQ)).toHaveLength(50)
  expect(world.posts.slice(before).filter(({ data }) => data).length).toBeLessThanOrEqual(2)
})
