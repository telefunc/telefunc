// A refusal's `Retry-After` holds back the next try of what it refused: the reopen of a wire, the next POST of a stalled
// channel's data, and the smaller POSTs a batch refused for its size is split into. A REAL ClientChannel and its
// connection against a scripted server (scriptedServer.fixture.ts).

import { expect, test } from 'vitest'
import { advance, openChannel, serveSse, useScriptedServer, world } from './scriptedServer.fixture.js'

useScriptedServer()

/** The statuses of the batch POSTs that carried data, in order. */
const dataStatuses = () => world.posts.filter(({ data }) => data).map(({ status }) => status)

test("a reopen refused with a 503 waits out the answer's Retry-After", async () => {
  serveSse()
  const page = openChannel(['sse'])
  await advance(500)
  await page.channel.opened
  world.openAnswers = [{ status: 503, contentType: ['text/html'], retryAfter: 4 }]
  world.streams[0]?.end()
  // The first reopen goes within the first step's 500 ms, and its 503 asks for 4 s.
  await advance(1_000)
  expect(world.opens).toBe(2)
  await advance(2_500)
  expect(world.opens).toBe(2)
  await advance(2_500)
  expect(world.opens).toBe(3)
  expect(page.ended).toEqual([])
})

test("a stalled channel's data refused with a 503 goes again only once the answer's Retry-After is out", async () => {
  serveSse()
  const page = openChannel(['sse'])
  await advance(500)
  await page.channel.opened
  world.server.cuts.add(0)
  void page.channel.send('keep trying').catch(() => {})
  await advance(300)
  world.server.cuts.delete(0)
  world.postAnswers = [{ tag: 'refused', status: 503, retryAfter: 6 }]
  await advance(1_000)
  expect(world.postAnswers).toEqual([])
  expect(world.streams).toHaveLength(2)
  await advance(3_000)
  expect(dataStatuses()).toEqual([0, 503])
  await advance(4_000)
  expect(dataStatuses()).toEqual([0, 503, 200])
})

test("a batch refused with a 413 goes again, split, only once the answer's Retry-After is out", async () => {
  serveSse()
  const page = openChannel(['sse'])
  await advance(500)
  await page.channel.opened
  world.postAnswers = [{ tag: 'refused', status: 413, retryAfter: 3 }]
  void page.channel.send('first').catch(() => {})
  void page.channel.send('second').catch(() => {})
  await advance(500)
  expect(dataStatuses()).toEqual([413])
  await advance(2_000)
  expect(dataStatuses()).toEqual([413])
  await advance(1_000)
  expect(dataStatuses()).toEqual([413, 200, 200])
  expect(page.ended).toEqual([])
})
