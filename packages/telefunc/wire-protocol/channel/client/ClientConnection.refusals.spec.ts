// What the page sends after something in front of the server refused a batch POST, and what it learns from the
// answers to its POSTs: a REAL ClientChannel and its connection against a scripted server (scriptedServer.fixture.ts).

import { describe, expect, test } from 'vitest'
import { SSE_RECONCILE_DEADLINE_MS } from '../../constants.js'
import { advance, openChannel, serveSse, useScriptedServer, world } from './scriptedServer.fixture.js'

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
