// Whose close POST ends a session: a page that leaves just after it lost its wire ends its session at once, and a close
// POST for an older wire, read after a reopen took the session, ends nothing. Through the real SSE handler and mux.

import { expect, test } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { decode, encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { openStream } from './serverPage.fixture.js'

const KEEP = 1_000
config.channel = { reconnectTimeout: KEEP, connectTtl: 15_000 }

const isReconciled = (frame: Uint8Array) => frame[0] === TAG.RECONCILED

/** A channel the server holds, and how it ended. */
function channel() {
  const c = new ServerChannel<string, string>()
  getChannelMux().registerChannel(c)
  const ends: string[] = []
  c.onClose((err) => void ends.push(err ? err.constructor.name : 'clean'))
  return { c, ends }
}

/** A page's SSE wire for `sessionId`, numbered `attempt`, naming channel `id`, once its RECONCILED was read. */
async function reconciled(sessionId: string, attempt: number, id: string) {
  const stream = await openStream(
    'web',
    encode.reconcile({ sessionId, attempt, open: [{ id, ix: 0, lastSeq: 0, initial: true }] }),
  )
  const frames = await stream.readUntil(isReconciled)
  const answer = decode(frames.find(isReconciled) ?? new Uint8Array())
  return { stream, attached: answer.tag === TAG.RECONCILED ? answer.payload.open.map(({ ix }) => ix) : [] }
}

test("a close POST for a wire the page lost before a reopen, read after the reopen's wire was lost too, ends nothing: the channel attaches on the page's next wire", async () => {
  const x = channel()
  const sessionId = crypto.randomUUID()
  const first = await reconciled(sessionId, 1, x.c.id)
  await first.stream.drop()
  const second = await reconciled(sessionId, 2, x.c.id)
  await second.stream.drop()
  await first.stream.leave()
  const third = await reconciled(sessionId, 3, x.c.id)
  try {
    expect({ ends: [...x.ends], attached: third.attached }).toEqual({ ends: [], attached: [0] })
  } finally {
    x.c.abort()
    await third.stream.drop()
  }
})

test('a close POST for the wire that held the session last, lost just before, ends the session at once as its page left', async () => {
  const x = channel()
  const sessionId = crypto.randomUUID()
  const first = await reconciled(sessionId, 1, x.c.id)
  await first.stream.drop()
  const second = await reconciled(sessionId, 2, x.c.id)
  await second.stream.drop()
  await second.stream.leave()
  expect(x.ends).toEqual(['ChannelPageLeftError'])
})
