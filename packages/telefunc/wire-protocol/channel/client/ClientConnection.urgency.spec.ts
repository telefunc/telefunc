// How soon a frame goes over SSE batch POSTs after it waited: held while sends were held back, waiting for headroom,
// or replayed. A REAL ClientChannel and its connection against a scripted server (scriptedServer.fixture.ts), with
// Telefunc's own throttle and idle delay.

import { expect, test, vi } from 'vitest'
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

test('a send that asks for the answer on an idle channel goes at the idle delay', async () => {
  const page = await openIdle()
  const sentAt = performance.now()
  void page.channel.send('asks', { ack: true }).catch(() => {})
  await advance(1_000)
  expect(askedAt().at - sentAt).toBe(SSE_POST_IDLE_FLUSH_DELAY_MS)
})

test('a send that asks for the answer, 150 ms after a POST started, goes a throttle after that POST', async () => {
  const page = await openIdle()
  void page.channel.send('data').catch(() => {})
  await advance(200)
  void page.channel.send('asks', { ack: true }).catch(() => {})
  await advance(1_000)
  const data = defined(
    world.server.got.find(({ tag }) => tag !== TAG.TEXT_ACK_REQ),
    'the data send',
  )
  expect(askedAt().at - data.at).toBe(SSE_FLUSH_THROTTLE_MS)
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
  expect(askedAt().at - reopened.openedAt).toBe(SSE_RECONCILE_DEADLINE_MS)
})

test('a send that asks for the answer, waiting behind a full POST, goes soon after that POST is answered', async () => {
  const page = await openIdle()
  void page.channel.send('x'.repeat(100_000)).catch(() => {})
  void page.channel.send('asks', { ack: true }).catch(() => {})
  await advance(1_000)
  const full = defined(
    world.server.got.find(({ tag }) => tag !== TAG.TEXT_ACK_REQ),
    'the full POST',
  )
  expect(askedAt().at - full.at).toBe(SSE_RECONCILE_DEADLINE_MS)
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
  expect(replayed.at - reopened.openedAt).toBe(SSE_RECONCILE_DEADLINE_MS)
})

test('a send just after a full POST started waits for the throttle from that POST', async () => {
  const page = await openIdle()
  void page.channel.send('x'.repeat(100_000)).catch(() => {})
  await advance(5)
  void page.channel.send('small').catch(() => {})
  await advance(1_000)
  const [full, small] = world.server.got.filter(({ ix }) => ix === 0)
  expect(defined(small, 'the small send').at - defined(full, 'the full POST').at).toBe(SSE_FLUSH_THROTTLE_MS)
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

test('sends that ask for the answer, made in a loop without awaiting, go together in one POST at the idle delay', async () => {
  const page = await openIdle()
  const sentAt = performance.now()
  const before = world.posts.length
  for (let i = 0; i < 50; i++) void page.channel.send(`asks ${i}`, { ack: true }).catch(() => {})
  await advance(5_000)
  const asked = world.server.got.filter(({ tag }) => tag === TAG.TEXT_ACK_REQ)
  expect(asked).toHaveLength(50)
  expect(asked.map(({ at }) => at - sentAt)).toEqual(asked.map(() => SSE_POST_IDLE_FLUSH_DELAY_MS))
  expect(world.posts.slice(before).filter(({ data }) => data)).toHaveLength(1)
})

test('a page that sends what asks for the answer once the server has the one before sends a POST per throttle, the first at the idle delay', async () => {
  const page = await openIdle()
  const startedAt = performance.now()
  const before = world.posts.length
  const asked = () => world.server.got.filter(({ tag }) => tag === TAG.TEXT_ACK_REQ)
  // As an app that awaits each answer, which its server gives as it takes the POST.
  for (let i = 0; i < 5; i++) {
    void page.channel.send(`asks ${i}`, { ack: true }).catch(() => {})
    while (asked().length === i) await advance(1)
  }
  const at = asked().map((frame) => frame.at)
  expect(defined(at[0], 'the first asking frame') - startedAt).toBe(SSE_POST_IDLE_FLUSH_DELAY_MS)
  const gaps = at.slice(1).map((each, i) => each - defined(at[i], 'the asking frame before'))
  expect(gaps).toEqual([1, 2, 3, 4].map(() => SSE_FLUSH_THROTTLE_MS))
  expect(world.posts.slice(before).filter(({ data }) => data)).toHaveLength(5)
})

test("a send that asks for the answer, made while another's POST is out, goes in the next POST, a throttle after that one", async () => {
  const page = await openIdle()
  // The first POST is answered 100 ms after it went, when the server records its frame.
  world.postAnswers.push({ tag: 'slow', ms: 100 })
  const sentAt = performance.now()
  const before = world.posts.length
  void page.channel.send('first', { ack: true }).catch(() => {})
  await advance(60)
  void page.channel.send('second', { ack: true }).catch(() => {})
  await advance(1_000)
  expect(askedAt(2).at - sentAt).toBe(SSE_POST_IDLE_FLUSH_DELAY_MS + SSE_FLUSH_THROTTLE_MS)
  expect(world.posts.slice(before).filter(({ data }) => data)).toHaveLength(2)
})

test("sends 100 ms apart go a POST per throttle, though the timer that starts each POST fires a moment before its deadline, as Node's do", async () => {
  const page = await openIdle()
  const fakeSetTimeout = globalThis.setTimeout
  // Node starts a timer from its loop's clock, which lags: the timer fires before its deadline by `performance.now()`.
  const early = vi
    .spyOn(globalThis, 'setTimeout')
    .mockImplementation((run, ms = 0) => fakeSetTimeout(run, Math.max(0, ms - 1)))
  try {
    for (let i = 0; i < 10; i++) {
      void page.channel.send(`data ${i}`).catch(() => {})
      await advance(100)
    }
    await advance(1_000)
  } finally {
    early.mockRestore()
  }
  const posts = new Set(world.server.got.filter(({ ix }) => ix === 0).map(({ at }) => at))
  // At 50 ms, then at 350, 650 and 950 ms, each a throttle after the one before.
  expect(posts.size).toBe(4)
})
