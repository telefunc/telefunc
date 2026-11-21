// A request the server holds when its wire ends is read off and answered at its body's end; a body still open one ping
// deadline after the end, as behind a proxy that keeps it open after the page let it go, or a page that writes on, is
// answered then, and read on and dropped: the server doesn't close a connection with the page's bytes unread, which
// resets it and may lose the answer. Through the SSE handler with a Web body, and on a real Node server.

import { createServer } from 'node:http'
import { afterAll, beforeAll, expect, test } from 'vitest'
import { Telefunc } from '../../../serve/node.js'
import { config } from '../../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest, encodeSseRequestMetadata } from '../../wire/envelope.js'
import { pingDeadline } from '../../wire/pingDeadline.js'
import { encode } from '../../wire/frame.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { handleSseChannelRequest } from './sse.js'

const PING_INTERVAL = 1_000
config.channel = { pingInterval: PING_INTERVAL }
const DEADLINE = pingDeadline(PING_INTERVAL, CHANNEL_TRANSPORT.SSE)

/** A real Node server serving Telefunc, which a page's own fetch reaches. */
let served = ''
const server = createServer((req, res) => void new Telefunc().serve({ req, res }))
beforeAll(async () => {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  served = `http://127.0.0.1:${address.port}/_telefunc?wire=${WIRE_VERSION}`
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

type Answer = { statusCode: number; contentType: string | null } | { error: string }

/** A request's answer, through the SSE handler with a Web body or as a page's fetch gets it from the real Node server;
 *  a network error is its `error`. `stream`: the event stream an open is answered with. */
async function send(
  through: 'web' | 'node',
  body: BodyInit,
): Promise<{ answer: Answer; stream: ReadableStream | null }> {
  // A stream body is sent half duplex, which a `RequestInit` literal doesn't type.
  const init = { method: 'POST', body, duplex: 'half', headers: { 'x-telefunc-request': 'sse' } }
  try {
    if (through === 'web') {
      const {
        statusCode,
        contentType,
        body: answered,
      } = await handleSseChannelRequest(
        new Request(`http://test.local/_telefunc?wire=${WIRE_VERSION}`, init),
        undefined,
        () => false,
      )
      return { answer: { statusCode, contentType }, stream: answered instanceof ReadableStream ? answered : null }
    }
    const response = await fetch(served, init)
    const contentType = response.headers.get('content-type')
    if (contentType === 'text/event-stream')
      return { answer: { statusCode: response.status, contentType }, stream: response.body }
    await response.text()
    return { answer: { statusCode: response.status, contentType }, stream: null }
  } catch (err) {
    return { answer: { error: err instanceof Error ? `${err.name}: ${err.message}` : String(err) }, stream: null }
  }
}

// Each row's names come first, for its title, then what they set: whether the body is the streaming upload, whether
// the page writes on, and where the request goes.
test.each([
  ['the streaming upload', 'idle', 'the SSE handler with a Web body', true, false, 'web'],
  ['a batch POST whose body streams', 'idle', 'the SSE handler with a Web body', false, false, 'web'],
  ['the streaming upload', 'writing on', 'the SSE handler with a Web body', true, true, 'web'],
  ['the streaming upload', 'idle', 'a real Node server', true, false, 'node'],
  ['the streaming upload', 'writing on', 'a real Node server', true, true, 'node'],
] as const)(
  "%s that the page's path keeps open, %s, after the server ended its session, on %s, is answered `retry` one ping deadline after the end, and what it writes after the end is read on and not run",
  async (_request, _page, _server, streamRequest, writing, through) => {
    const channel = new ServerChannel()
    getChannelMux().registerChannel(channel)
    const got: unknown[] = []
    channel.listen((message) => void got.push(message))
    const connId = crypto.randomUUID()
    const { stream } = await send(
      through,
      encodeSseRequest({ connId, streamResponse: true }, [
        encode.reconcile({
          sessionId: crypto.randomUUID(),
          attempt: 1,
          open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }],
        }),
      ]),
    )
    await channel.opened
    let ended = false
    let done = false
    let seq = 0
    let pulls = 0
    const body = new ReadableStream<Uint8Array>({
      start: (controller) =>
        controller.enqueue(encodeSseRequestMetadata(streamRequest ? { connId, streamRequest: true } : { connId })),
      // A writing page sends a text every 100 ms, before the end and on past it, until the row is done.
      pull: async (controller) => {
        if (!writing) return new Promise(() => {})
        await new Promise((resolve) => setTimeout(resolve, 100))
        if (done) return controller.close()
        pulls++
        const text = encode.text(0, JSON.stringify(ended ? 'after the end' : 'before the end'), ++seq)
        const frame = new Uint8Array(4 + text.byteLength)
        new DataView(frame.buffer).setUint32(0, text.byteLength)
        frame.set(text, 4)
        controller.enqueue(frame)
      },
    })
    const answer = send(through, body)
    await new Promise((resolve) => setTimeout(resolve, 300))
    // A frame only a server sends ends the session.
    await send(through, encodeSseRequest({ connId }, [encode.attachResult(999, null)]))
    ended = true
    const endedAt = performance.now()
    const answered = await Promise.race([
      answer.then(({ answer }) => {
        const after = performance.now() - endedAt
        return { ...answer, onTime: after >= DEADLINE - 100 && after < DEADLINE * 1.5 }
      }),
      new Promise<null>((resolve) => setTimeout(resolve, DEADLINE * 2, null)),
    ])
    const pullsAnswered = pulls
    await new Promise((resolve) => setTimeout(resolve, 500))
    done = true
    expect({
      answered,
      notRun: got.filter((message) => message === 'after the end'),
      // Through the handler, nothing in front ends a writing page's body on the answer: the server reads on.
      readOn: through === 'web' && writing ? pulls > pullsAnswered : null,
    }).toEqual({
      answered: {
        statusCode: 400,
        contentType: expect.stringMatching(/^text\/vnd\.telefunc\.refusal; kind=retry; reason="[^"]+"$/),
        onTime: true,
      },
      notRun: [],
      readOn: through === 'web' && writing ? true : null,
    })
    await stream?.cancel()
  },
  DEADLINE * 4,
)
