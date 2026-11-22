// A request the server holds when its wire ends is read off and answered at its body's end; a body still open one ping
// deadline after the end, as behind a proxy that keeps it open after the page let it go, or a sender that writes on,
// is answered then, and let go of: the server reads it no further and cancels nothing, and the host ends the
// connection in its own time. Through the SSE handler with a Web body, and on a real Node server, where the page's
// upload is a raw socket that writes on and ignores the answer.

import { createServer } from 'node:http'
import { connect } from 'node:net'
import { setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { Telefunc } from '../../../serve/node.js'
import { config } from '../../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest, encodeSseRequestMetadata } from '../../wire/envelope.js'
import { pingDeadline } from '../../wire/pingDeadline.js'
import { encode } from '../../wire/frame.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel, ATTACHABLE_NOW } from './ServerChannel.js'
import { handleSseChannelRequest } from './sse.js'

const PING_INTERVAL = 1_000
config.channel = { pingInterval: PING_INTERVAL }
const DEADLINE = pingDeadline(PING_INTERVAL, CHANNEL_TRANSPORT.SSE)
const RETRY = /^text\/vnd\.telefunc\.refusal; kind=retry; reason="[^"]+"$/

setFlagsFromString('--expose-gc')
const collect: unknown = runInNewContext('gc')

/** Whether `ref`'s target is gone once garbage is collected, across turns, as a `WeakRef` keeps its target to its
 *  job's end. */
async function collected(ref: WeakRef<object>): Promise<boolean> {
  if (typeof collect !== 'function') throw new Error('no gc')
  for (let i = 0; i < 3; i++) {
    collect()
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
  return ref.deref() === undefined
}

const handlerUrl = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
function post(body: BodyInit) {
  // A stream body is sent half duplex, which a `RequestInit` literal doesn't type.
  const init = { method: 'POST', body, duplex: 'half' }
  return handleSseChannelRequest(new Request(handlerUrl, init), undefined, () => false)
}

/** A real Node server serving Telefunc. */
let port = 0
const server = createServer((req, res) => void new Telefunc().serve({ req, res }))
beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  port = address.port
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})
const path = `/_telefunc?wire=${WIRE_VERSION}`
function fetchPost(body: BodyInit) {
  return fetch(`http://127.0.0.1:${port}${path}`, { method: 'POST', body, headers: { 'x-telefunc-request': 'sse' } })
}

/** A frame of the page's text, length-prefixed as the upload carries it. */
function textFrame(value: string, seq: number): Uint8Array<ArrayBuffer> {
  const text = encode.text(0, JSON.stringify(value), seq)
  const frame = new Uint8Array(4 + text.byteLength)
  new DataView(frame.buffer).setUint32(0, text.byteLength)
  frame.set(text, 4)
  return frame
}

/** A session of one channel, opened through `through` and its event stream read to its end; `wire`: its wire, held
 *  weakly; `end`: sends a frame only a server sends, which ends the session, and gives the time it was answered. */
async function session(through: 'web' | 'node') {
  const mux = getChannelMux()
  const channel = new ServerChannel()
  mux.registerChannel(channel, ATTACHABLE_NOW)
  const got: unknown[] = []
  channel.listen((message) => void got.push(message))
  const connId = crypto.randomUUID()
  const opening = encodeSseRequest({ connId, streamResponse: true }, [
    encode.reconcile({
      sessionId: crypto.randomUUID(),
      attempt: 1,
      open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }],
    }),
  ])
  const stream = through === 'web' ? (await post(opening)).body : (await fetchPost(opening)).body
  if (stream instanceof ReadableStream) {
    const events = stream.getReader()
    void (async () => {
      while (!(await events.read()).done);
    })().catch(() => {})
  }
  await channel.opened
  const opened = mux.getWireByConnId<object>(connId)
  if (opened === undefined) throw new Error('no wire')
  const wire = new WeakRef(opened)
  const end = async () => {
    const ending = encodeSseRequest({ connId }, [encode.attachResult(999, null)])
    if (through === 'web') await post(ending)
    else await (await fetchPost(ending)).text()
    return performance.now()
  }
  return { connId, got, wire, end }
}

// Each row's names come first, for its title, then what they set: whether the body is the streaming upload, and
// whether the page writes on.
test.each([
  ['the streaming upload', 'idle', true, false],
  ['a batch POST whose body streams', 'idle', false, false],
  ['the streaming upload', 'writing on', true, true],
] as const)(
  "%s that the page's path keeps open, %s, after the server ended its session, through the SSE handler with a Web body, is answered `retry` one ping deadline after the end, and let go of: none of what it writes past the end runs, the server reads none of it past the answer, and nothing of the closed wire is held through the request",
  async (_request, _page, streamRequest, writing) => {
    const { connId, got, wire, end } = await session('web')
    let ended = false
    let seq = 0
    let pulls = 0
    let done = false
    const body = new ReadableStream<Uint8Array>({
      start: (controller) =>
        controller.enqueue(encodeSseRequestMetadata(streamRequest ? { connId, streamRequest: true } : { connId })),
      // A writing page sends a text every 100 ms, before the end and on past it, until the row is done.
      pull: async (controller) => {
        if (!writing) return new Promise(() => {})
        await new Promise((resolve) => setTimeout(resolve, 100))
        if (done) return controller.close()
        pulls++
        controller.enqueue(textFrame(ended ? 'after the end' : 'before the end', ++seq))
      },
    })
    // The request and its body stay held to the row's end, as their host holds them.
    const init = { method: 'POST', body, duplex: 'half' }
    const request = new Request(handlerUrl, init)
    const answer = handleSseChannelRequest(request, undefined, () => false)
    await new Promise((resolve) => setTimeout(resolve, 300))
    const endedAt = await end()
    ended = true
    const answered = await Promise.race([
      (async () => {
        const { statusCode, contentType } = await answer
        const after = performance.now() - endedAt
        return { statusCode, contentType, onTime: after >= DEADLINE - 100 && after < DEADLINE * 1.5 }
      })(),
      new Promise<null>((resolve) => setTimeout(resolve, DEADLINE * 2, null)),
    ])
    const pullsAnswered = pulls
    await new Promise((resolve) => setTimeout(resolve, 1_000))
    const released = await collected(wire)
    expect({
      answered,
      notRun: got.filter((message) => message === 'after the end'),
      // A stream left unread fills its queue once: one chunk in flight.
      readPastAnswer: pulls - pullsAnswered <= 1,
      released,
      held: request.body !== null,
    }).toEqual({
      answered: { statusCode: 400, contentType: expect.stringMatching(RETRY), onTime: true },
      notRun: [],
      readPastAnswer: true,
      released: true,
      held: true,
    })
    done = true
  },
  DEADLINE * 4 + 5_000,
)

test.each([
  ['idle', false],
  ['writing on', true],
] as const)(
  "the streaming upload on a real Node server, %s past its session's end and ignoring the answer, is answered `retry` one ping deadline after the end, and let go of: none of what it writes runs, the server reads no more than was in flight, nothing of the closed wire is held through the request, and the host closes the connection in its own time",
  async (_page, writing) => {
    const { connId, got, wire, end } = await session('node')
    const socket = connect(port, '127.0.0.1')
    await new Promise((resolve) => socket.once('connect', resolve))
    let response = ''
    let answeredAt: number | null = null
    let closedAt: number | null = null
    socket.on('data', (data) => {
      response += data.toString('latin1')
      if (answeredAt === null && response.includes('\r\n\r\n')) answeredAt = performance.now()
    })
    socket.on('close', () => void (closedAt = performance.now()))
    socket.on('error', () => {})
    const chunk = (bytes: Uint8Array) => {
      socket.write(`${bytes.byteLength.toString(16)}\r\n`)
      socket.write(Buffer.concat([bytes, Buffer.from('\r\n')]))
    }
    socket.write(
      `POST ${path} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-telefunc-request: sse\r\nTransfer-Encoding: chunked\r\n\r\n`,
    )
    chunk(encodeSseRequestMetadata({ connId, streamRequest: true }))
    let seq = 0
    chunk(textFrame('before the end', ++seq))
    await new Promise((resolve) => setTimeout(resolve, 300))
    const endedAt = await end()
    // A writing page sends 64 KiB of text frames whenever its socket takes them, until the connection closes.
    const big = textFrame(`after the end ${'x'.repeat(64 * 1024)}`, ++seq)
    let writtenAtAnswer: number | null = null
    let released: boolean | null = null
    while (closedAt === null && performance.now() - endedAt < DEADLINE + 15_000) {
      if (writing && !socket.writableNeedDrain) chunk(big)
      await new Promise((resolve) => setTimeout(resolve, writing ? 5 : 50))
      if (answeredAt === null) continue
      writtenAtAnswer ??= socket.bytesWritten
      // A second after the answer, the connection still open.
      if (released === null && performance.now() - answeredAt > 1_000 && closedAt === null)
        released = await collected(wire)
    }
    const head = response.split('\r\n\r\n')[0]?.split('\r\n') ?? []
    const contentType = head.find((line) => /^content-type:/i.test(line))?.replace(/^content-type: */i, '') ?? null
    const after = answeredAt === null ? null : answeredAt - endedAt
    const takenPastAnswer = writtenAtAnswer === null ? null : socket.bytesWritten - writtenAtAnswer
    const closedPastAnswer = closedAt === null || answeredAt === null ? null : closedAt - answeredAt
    socket.destroy()
    expect({
      status: head[0],
      contentType,
      onTime: after !== null && after >= DEADLINE - 100 && after < DEADLINE * 1.5,
      notRun: got.filter((message) => typeof message === 'string' && message.startsWith('after the end')),
      // What the sockets' buffers take unread, a few MiB on loopback; a server that reads on takes 64 KiB each 5 ms.
      inFlightOnly: takenPastAnswer !== null && takenPastAnswer < 16 * 1024 * 1024,
      released,
      closedByHost: closedPastAnswer !== null && closedPastAnswer < 10_000,
    }).toEqual({
      status: 'HTTP/1.1 400 Bad Request',
      contentType: expect.stringMatching(RETRY),
      onTime: true,
      notRun: [],
      inFlightOnly: true,
      released: true,
      closedByHost: true,
    })
  },
  DEADLINE * 4 + 25_000,
)
