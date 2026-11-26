// The real-server twins of ClientConnection.stalled.spec.ts's row on the server's UNCONFIRMED end, and of a close the
// server timed out: each ERROR reason the server ends a channel with reads on the page as that reason's error. The
// event stream keeps from the page what the row needs it not to have, and drops as a lost connection does.

import { expect, test } from 'vitest'
import { TAG } from '../../wire/tags.js'
import { delay, open, opens, textOf, until, useRealServer, withholding, world } from './realServer.fixture.js'

useRealServer(['sse'], { reconnectTimeout: 1_000 })

test("a channel the server ended because the page never confirmed what it sent reads the server's cause", async () => {
  // Each event stream keeps the data from the page, which so never confirms it.
  const streams = Array.from({ length: 3 }, () => withholding((frame) => textOf(frame, 'never read') !== null))
  world.hooks = streams.map(({ stream }) => ({ kind: 'open', stream }))
  const [pair] = await open(1)
  if (!pair) throw new Error('no pair')
  pair.channel.send('never read').catch(() => {})
  await delay(100)
  // Lost, back, and lost again past `reconnectTimeout` since the first loss: the server ends the channel at the second.
  streams[0]?.cut()
  await until(() => opens() === 2)
  await delay(1_200)
  streams[1]?.cut()
  await until(() => pair.ended.length > 0)
  expect(pair.ended.map(({ message }) => message)).toEqual([
    "Channel closed: the page hasn't confirmed what the server sent on it for over 1 s of reconnects",
  ])
})

test('a channel whose close the server timed out reads that its close timed out', async () => {
  // The page never has the server's CLOSE, so never answers it.
  world.hooks = [{ kind: 'open', stream: withholding((frame) => frame[0] === TAG.CLOSE).stream }]
  const [pair] = await open(1)
  if (!pair) throw new Error('no pair')
  void pair.channel.close({ timeout: 300 })
  await until(() => pair.ended.length > 0)
  expect(pair.ended.map(({ message }) => message)).toEqual(['Channel close timed out'])
})
