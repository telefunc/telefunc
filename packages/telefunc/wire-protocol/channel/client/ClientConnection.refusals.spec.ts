// What the page sends after something in front of the server refused a batch POST, and what it learns from the
// answers to its POSTs: a REAL ClientChannel and its connection against a scripted server (scriptedServer.fixture.ts).

import { describe, expect, test } from 'vitest'
import { SSE_POST_MIN_BYTES, SSE_RECONCILE_DEADLINE_MS } from '../../constants.js'
import { FakeWebSocket, advance, openChannel, serveSse, useScriptedServer, world } from './scriptedServer.fixture.js'
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

/** `count` texts of `bytes` characters each, sent at once. */
function sendTexts(channel: ReturnType<typeof openChannel>['channel'], count: number, bytes: number): void {
  for (let i = 0; i < count; i++) void channel.send('x'.repeat(bytes)).catch(() => {})
}

describe('after a batch POST is refused for its size', () => {
  test('what a POST carries stays within the size the page learned, in bytes as they go on the wire', async () => {
    const page = (await openOverSse(1))(0)
    world.postLimit = 40_000
    sendTexts(page.channel, 400, 90)
    await advance(5_000)
    const refused = defined(
      world.posts.find(({ status }) => status === 413),
      'a POST refused for its size',
    )
    const learned = Math.floor(refused.bytes / 2)
    const after = world.posts.slice(world.posts.indexOf(refused) + 1).filter(({ data }) => data)
    expect(after.length).toBeGreaterThan(1)
    expect(after.filter(({ bytes }) => bytes > learned)).toEqual([])
    expect(world.server.got.filter(({ ix }) => ix === 0)).toHaveLength(400)
  })

  test('a message refused alone never raises the size the page learned from a larger batch', async () => {
    const opened = await openOverSse(2)
    const alone = opened(0)
    const batch = opened(1)
    world.postLimit = 40_000
    sendTexts(batch.channel, 20, 4_000)
    await advance(5_000)
    const learnedFrom = world.posts.find(({ status }) => status === 413)
    expect(learnedFrom).toBeDefined()
    // Twice as large as the server takes: refused alone, on this wire and on the next.
    sendTexts(alone.channel, 1, 100_000)
    await advance(3_000)
    const loneAt = world.posts.findIndex(({ status, bytes }) => status === 413 && bytes > 100_000)
    expect(loneAt).toBeGreaterThan(0)
    sendTexts(batch.channel, 20, 4_000)
    await advance(5_000)
    // The batch goes as before, in POSTs the server takes: none past the size the page learned.
    const refusedBatches = world.posts.slice(loneAt).filter(({ status, bytes }) => status === 413 && bytes < 100_000)
    expect(refusedBatches).toEqual([])
    expect(world.server.got.filter(({ ix }) => ix === 1)).toHaveLength(40)
  })

  test('a channel ended for it sends nothing more of what it had queued, while the wire stays up', async () => {
    const opened = await openOverSse(2)
    const ended = opened(0)
    const other = opened(1)
    world.postLimit = 40_000
    // Refused alone on this wire and on the next; the three after it wait behind it.
    sendTexts(ended.channel, 1, 100_000)
    sendTexts(ended.channel, 3, 100)
    await advance(5_000)
    expect(ended.ended).toHaveLength(1)
    expect(ended.ended[0]?.err?.message).toMatch(/refused with 413 text\/html, again after a reconnect/)
    void other.channel.send('the wire goes on')
    await advance(2_000)
    expect(world.server.got.filter(({ ix }) => ix === 1)).toHaveLength(1)
    expect(world.server.got.filter(({ ix }) => ix === 0)).toEqual([])
  })
})

test("a batch of the connection's own frames refused again after a reconnect ends the connection, naming the answer", async () => {
  const page = (await openOverSse(1))(0)
  // Every POST that carries no data, the page's PINGs: refused as a gateway that doesn't know the request refuses it.
  world.controlAnswers = Array.from({ length: 100 }, (): PostAnswer => ({ tag: 'refused', status: 400 }))
  await advance(30_000)
  expect(page.ended).toHaveLength(1)
  expect(page.ended[0]?.err?.message).toMatch(/refused with 400 text\/html/)
  expect(world.streams.length).toBeLessThanOrEqual(3)
})

test("a PONG the wire left behind carried after its own frames' refusal doesn't keep the connection from ending", async () => {
  const page = (await openOverSse(1))(0)
  // As the row above, and each refused wire carries a PONG already on its way 70 ms after the refusal, while the page
  // waits to reconnect (msg16 R1.1).
  world.controlAnswers = Array.from(
    { length: 100 },
    (): PostAnswer => ({ tag: 'refused', status: 400, pongAfterMs: 70 }),
  )
  await advance(30_000)
  expect(page.ended).toHaveLength(1)
  expect(defined(page.ended[0], 'the connection ending').err?.message).toMatch(/refused with 400 text\/html/)
  expect(world.streams.length).toBeLessThanOrEqual(3)
})

test('a PING that goes past a data POST the gateway holds, refused again after a reconnect, ends the connection', async () => {
  const page = (await openOverSse(1))(0)
  // The gateway holds the one data POST for 12 s and refuses every POST of the connection's own frames (msg16 R1.7).
  world.postAnswers = [{ tag: 'slow', ms: 12_000 }]
  world.controlAnswers = Array.from({ length: 30 }, (): PostAnswer => ({ tag: 'refused', status: 403 }))
  void page.channel.send('data held by the gateway').catch(() => {})
  await advance(8_500)
  expect(world.posts.filter(({ status, data }) => status === 403 && !data).length).toBeGreaterThan(1)
  expect(page.ended).toHaveLength(1)
  expect(defined(page.ended[0], 'the connection ending').err?.message).toMatch(/refused with 403 text\/html/)
  expect(world.streams.length).toBeLessThanOrEqual(3)
})

test("a late answer to a POST of a wire the page left doesn't size the POSTs of the wire after it", async () => {
  const page = (await openOverSse(1))(0)
  // Fast and full POSTs: the page sizes its next ones larger.
  sendTexts(page.channel, 2_000, 100)
  await advance(2_000)
  const grown = Math.max(...world.posts.filter(({ data }) => data).map(({ bytes }) => bytes))
  expect(grown).toBeGreaterThan(2 * SSE_POST_MIN_BYTES)
  // A POST answered 3 s later, after its wire went and the page opened another.
  world.postAnswers = [{ tag: 'slow', ms: 3_000 }]
  void page.channel.send('answered late').catch(() => {})
  await advance(100)
  world.streams[0]?.end()
  await advance(4_000)
  expect(world.streams.length).toBe(2)
  const before = world.posts.length
  sendTexts(page.channel, 2_000, 100)
  await advance(2_000)
  const next = world.posts.slice(before).find(({ data }) => data)
  expect(next?.bytes).toBeGreaterThan(2 * SSE_POST_MIN_BYTES)
})

test('the size the page learned outlives a move to WebSocket and the fallback to SSE after it', async () => {
  serveSse()
  // Something in front of the server takes no body larger than this, on every SSE request of the page.
  world.postLimit = 18_120
  world.holdReady = true
  const page = openChannel(['sse', 'ws'])
  await advance(SSE_RECONCILE_DEADLINE_MS + 500)
  sendTexts(page.channel, 12, 3_000)
  await advance(2_000)
  const refused = defined(
    world.posts.find(({ status }) => status === 413),
    'the 413 that taught the page its size',
  )
  const learned = Math.floor(refused.bytes / 2)
  // The move to WebSocket fails at its barrier, which a gateway refuses: the page falls back to a new SSE wire, behind
  // the same proxy, and stays on it.
  world.controlAnswers = [{ tag: 'refused', status: 503 }]
  defined(world.heldReady.shift(), "the WebSocket's READY")()
  await advance(5_000)
  const from = world.posts.length
  sendTexts(page.channel, 10, 3_000)
  await advance(2_000)
  const after = world.posts.slice(from).filter(({ data }) => data)
  expect(after.length).toBeGreaterThan(0)
  expect(after.filter(({ status }) => status === 413)).toEqual([])
  expect(Math.max(...after.map(({ bytes }) => bytes))).toBeLessThanOrEqual(learned)
  expect(page.ended).toEqual([])
})
