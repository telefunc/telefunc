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
})
