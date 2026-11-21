// A session's end the server keeps for `reconnectTimeout`, on the real Node server and raw WebSockets: each reopen it
// answers with that end keeps it again, so a page that misses the reopen's last word too reads the cause at its next.
import { createServer } from 'node:http'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { Telefunc } from '../../../serve/node.js'
import { config } from '../../../node/server/serverConfig.js'
import { WIRE_VERSION } from '../../constants.js'
import { decode, encode, isOneFrame, messageFrames } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { END_CAUSE } from '../endCause.js'

/** How long the server keeps a session's end: short, so a row outlasts it. */
const KEEP_MS = 1_000
config.channel = { reconnectTimeout: KEEP_MS }

let port = 0
const server = createServer()
beforeAll(async () => {
  const telefunc = new Telefunc()
  server.on('request', (req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('Expected a TCP address')
  port = address.port
})
afterAll(() => {
  server.close()
})

const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

/** A page's wire whose RECONCILE names `sessionId`, or a new one: the session the RECONCILED gives it, or the close that ends
 *  it first; `socket` to go on with it. */
/** The page's number for each wire it opens. */
let attempts = 0

async function reopen(sessionId?: string) {
  const socket = new WebSocket(`ws://127.0.0.1:${port}/_telefunc?wire=${WIRE_VERSION}`)
  socket.binaryType = 'arraybuffer'
  const answer = new Promise<{ reconciled: string } | { code: number; reason: string }>((resolve) => {
    socket.addEventListener('message', ({ data }: MessageEvent<ArrayBuffer>) => {
      const message = new Uint8Array(data)
      for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) {
        const decoded = decode(frame)
        if (decoded.tag === TAG.RECONCILED) resolve({ reconciled: decoded.payload.sessionId })
      }
    })
    socket.addEventListener('close', ({ code, reason }) => resolve({ code, reason }))
  })
  await new Promise((resolve) => socket.addEventListener('open', resolve))
  socket.send(encode.reconcile({ sessionId: sessionId ?? crypto.randomUUID(), open: [], attempt: ++attempts }))
  return { socket, answer: await answer }
}

/** A session the server ended for a frame it can't read: its id. */
async function endedSession(): Promise<string> {
  const { socket, answer } = await reopen()
  if (!('reconciled' in answer)) throw new Error('no session')
  const closed = new Promise((resolve) => socket.addEventListener('close', resolve))
  // A wire-level frame of a tag no version has.
  socket.send(new Uint8Array([0x0f, 0, 0]))
  await closed
  return answer.reconciled
}

const sessionEnd = { code: 4002, reason: `text/vnd.telefunc.refusal; kind=frames; cause=${END_CAUSE.unreadable.code}` }

test("a reopen the server answers with a session's end keeps that end again: a second reopen, later than one keep after the end and within one keep of the first, reads the cause too", async () => {
  const sessionId = await endedSession()
  await delay(KEEP_MS * 0.6)
  const first = await reopen(sessionId)
  await delay(KEEP_MS * 0.6)
  const second = await reopen(sessionId)
  expect([first.answer, second.answer]).toEqual([sessionEnd, sessionEnd])
})

test("a reopen after the session's end was kept for its time, with no reopen between, finds no end: a session under the id it named, where the page reads its channels lost", async () => {
  const sessionId = await endedSession()
  await delay(KEEP_MS * 1.2)
  const { socket, answer } = await reopen(sessionId)
  socket.close()
  expect(answer).toEqual({ reconciled: sessionId })
})
