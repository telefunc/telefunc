// A channel's end the page read before its session's end keeps its own cause: the session's end reaches the page's
// other channels, on SSE and on a WebSocket, and leaves the ended channel's error as it was.

import { expect, test } from 'vitest'
import { ERROR_REASON, encode } from '../../wire/frame.js'
import { FakeWebSocket, advance, openChannel, serveSse, useScriptedServer, world } from './scriptedServer.fixture.js'

useScriptedServer()

const sessionEnd = 'text/vnd.telefunc.refusal; kind=frames; cause=8'
const told = {
  refused: "Channel closed: the server refused what the page sent on it: The server can't take a frame of the page's.",
  unreadable:
    "Channel closed: the page's connection ended: The server can't read what the page sent on its connection.",
}

/** How each of `pages`' channels ended, by message. */
const endsOf = (...pages: ReturnType<typeof openChannel>[]) =>
  pages.map(({ ended }) => ended.map(({ err }) => err?.message))

test("on SSE, a channel's ERROR read before the session's END: it keeps its cause, and the other channel reads the session's", async () => {
  serveSse()
  const refused = openChannel(['sse'])
  const other = openChannel(['sse'])
  await advance(500)
  const stream = world.streams.at(-1)
  stream?.frame(encode.error(0, ERROR_REASON.FRAME_REFUSED, 1))
  await advance(100)
  stream?.frame(encode.end(sessionEnd))
  stream?.end()
  await advance(1_000)
  expect([endsOf(refused, other), world.opens]).toEqual([[[told.refused], [told.unreadable]], 1])
})

test("on a WebSocket, a channel's ERROR read before the session's 4002 close: it keeps its cause, and the other channel reads the session's", async () => {
  const refused = openChannel(['ws'])
  const other = openChannel(['ws'])
  await advance(500)
  const socket = FakeWebSocket.open
  socket?.deliver(encode.error(0, ERROR_REASON.FRAME_REFUSED, 1))
  await advance(100)
  socket?.refuse(sessionEnd)
  await advance(1_000)
  expect([endsOf(refused, other), FakeWebSocket.made.length]).toEqual([[[told.refused], [told.unreadable]], 1])
})
