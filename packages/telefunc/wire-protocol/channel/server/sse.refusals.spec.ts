// Telefunc's own refusals of the page's SSE requests say so in their media type, with what the page does about each:
// `retry` (the server no longer has the wire: a loss), `frames` (it can't take a frame: the channel ends, named where
// the server knows it), `route` (it doesn't serve SSE this way: the page leaves SSE) and `reload` (another wire
// version), each with a reason for the page's error (decisions/516-sse-carrier.md, step 6 at 13:08Z and 13:14Z).

import { afterEach, expect, test, vi } from 'vitest'

import { createDeferred } from '../../../utils/createDeferred.js'

import {
  CREDIT_MSG_WINDOW_MAX,
  WIRE_GATHER_BYTES,
  WIRE_MAX_RAW_FRAME_BYTES,
  WIRE_RECV_BACKLOG_BASE_FRAMES,
  WIRE_VERSION,
} from '../../constants.js'
import { base64urlToUint8Array } from '../../wire/base64url.js'
import { encodeSseRequest, encodeSseRequestMetadata, encodeU32 } from '../../wire/envelope.js'
import { ERROR_REASON, decode, encode, type DecodedFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { ProtocolViolationError } from '../../wire/protocolViolation.js'
import { config } from '../../../node/server/serverConfig.js'
import { getChannelMux } from './ChannelMux.js'
import { END_CAUSE } from '../endCause.js'
import { ServerChannel } from './ServerChannel.js'
import { handleSseChannelRequest, type SseChannelHttpResponse } from './sse.js'

// The receive backlog's budget, small enough that a row passes it in a few hundred frames.
vi.mock('../../constants.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../constants.js')>()),
  CREDIT_MSG_WINDOW_MAX: 64,
  WIRE_RECV_BACKLOG_BASE_FRAMES: 64,
}))

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`

function post(body: BodyInit, { at = url, method = 'POST' } = {}) {
  // A stream body is sent half duplex, which a `RequestInit` literal doesn't type.
  const init = { method, body, duplex: 'half' }
  return handleSseChannelRequest(new Request(at, init), undefined, () => false)
}

/** A stream whose chunks the server reads as they come, ending after the last. */
function bodyOf(...chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start: (controller) => {
      for (const chunk of chunks) controller.enqueue(chunk)
      controller.close()
    },
  })
}

/** A page's event stream, opened with a RECONCILE that attaches one channel at index 0. */
async function openWire() {
  const channel = new ServerChannel()
  getChannelMux().registerChannel(channel)
  const connId = crypto.randomUUID()
  const opened = await post(
    encodeSseRequest({ connId, streamResponse: true }, [
      encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }),
    ]),
  )
  expect(opened.statusCode).toBe(200)
  return { connId, opened, channel }
}

async function cancel(answer: SseChannelHttpResponse): Promise<void> {
  if (answer.body instanceof ReadableStream) await answer.body.cancel()
}

/** The frames an event stream carried, read to its end. */
async function framesOfStream(answer: SseChannelHttpResponse): Promise<DecodedFrame[]> {
  if (!(answer.body instanceof ReadableStream)) throw new Error('no event stream')
  const text = await new Response(answer.body).text()
  return text
    .split('\n')
    .filter((line) => line.startsWith('data: '))
    .map((line) => decode(base64urlToUint8Array(line.slice(6))))
}

/** The refusal's `Content-Type`, read as the page reads it: kind, then the channel where named, then the code of the
 *  channel's end where it ends one, then the reason. */
const refusal = (kind: string, channel = '', cause?: number) =>
  new RegExp(
    `^text/vnd\\.telefunc\\.refusal; kind=${kind}${channel && `; channel=${channel}`}${cause === undefined ? '' : `; cause=${cause}`}; reason="[^"]+"$`,
  )

afterEach(() => {
  config.channel = {}
})

test.each([
  ['a GET', () => handleSseChannelRequest(new Request(url), undefined, () => false)],
  ['a POST with no body', () => handleSseChannelRequest(new Request(url, { method: 'POST' }), undefined, () => false)],
  ['metadata it cannot read', () => post(bodyOf(encodeU32(2), new TextEncoder().encode('{]')))],
  [
    'a server that serves no SSE',
    () => {
      config.channel = { transports: ['ws'] }
      return post(encodeSseRequest({ connId: crypto.randomUUID() }, [encode.ping([])]))
    },
  ],
])('%s is refused `route`, with its reason', async (_request, send) => {
  const answer = await send()
  expect(answer.statusCode).toBe(400)
  expect(answer.contentType).toMatch(refusal('route'))
})

test('a request of another wire version is refused `reload`', async () => {
  const answer = await post(encodeSseRequest({ connId: crypto.randomUUID() }, [encode.ping([])]), {
    at: 'http://test.local/_telefunc',
  })
  expect(answer.statusCode).toBe(409)
  expect(answer.contentType).toMatch(refusal('reload'))
})

test('a batch POST for a wire the server closed is refused `retry`', async () => {
  const { connId, opened } = await openWire()
  await cancel(opened)
  const answer = await post(encodeSseRequest({ connId }, [encode.ping([])]))
  expect(answer.statusCode).toBe(400)
  expect(answer.contentType).toMatch(refusal('retry'))
})

test('a batch POST whose body ends in the middle of a frame is refused `retry`', async () => {
  const { connId, opened } = await openWire()
  const answer = await post(bodyOf(encodeSseRequestMetadata({ connId }), encodeU32(100), new Uint8Array(10)))
  expect(answer.statusCode).toBe(400)
  expect(answer.contentType).toMatch(refusal('retry'))
  await cancel(opened)
})

/** The code of a session's end for frames the server can't read: no channel's own. */
const UNREADABLE = 0x08

test.each([
  ['a frame of a channel the session holds', encode.close(0, 0, 1), '0', END_CAUSE.frameOverLimit.code],
  ['a frame of an index the session holds in no form', encode.close(3, 0, 1), '', UNREADABLE],
  ["a frame of the connection's own", encode.ping([]), '', UNREADABLE],
])(
  '%s over the frame limit is refused `frames`, naming its channel where the session holds it: the server reads its header',
  async (_frame, frame, channel, cause) => {
    const { connId, opened } = await openWire()
    // The length says more than the limit; only the header follows it.
    const answer = await post(
      bodyOf(encodeSseRequestMetadata({ connId }), encodeU32(WIRE_MAX_RAW_FRAME_BYTES + 1), frame),
    )
    expect(answer.statusCode).toBe(400)
    expect(answer.contentType).toMatch(refusal('frames', channel, cause))
    await cancel(opened)
  },
)

test('a body the server cannot frame is refused `frames`, naming no channel', async () => {
  const { connId, opened } = await openWire()
  // A frame of no bytes: no tag, no header.
  const answer = await post(bodyOf(encodeSseRequestMetadata({ connId }), encodeU32(0)))
  expect(answer.statusCode).toBe(400)
  expect(answer.contentType).toMatch(refusal('frames', '', UNREADABLE))
  await cancel(opened)
})

test('a channel the server ends for a frame over the limit is told at the end of an event stream its page reads none of: what waited for the page goes first', async () => {
  const { connId, opened, channel } = await openWire()
  await channel.opened
  // Past what goes at once, and the page reads none of its event stream: the text waits for the stream to ask, and the
  // channel's ERROR waits behind it.
  const waits = 'x'.repeat(WIRE_GATHER_BYTES)
  void channel.send(waits).catch(() => {})
  const answer = await post(
    bodyOf(encodeSseRequestMetadata({ connId }), encodeU32(WIRE_MAX_RAW_FRAME_BYTES + 1), encode.close(0, 0, 1)),
  )
  expect(answer.contentType).toMatch(refusal('frames', '0', END_CAUSE.frameOverLimit.code))
  const frames = await framesOfStream(opened)
  expect(frames.slice(-2)).toEqual([
    expect.objectContaining({ tag: TAG.TEXT, index: 0, text: JSON.stringify(waits) }),
    expect.objectContaining({ tag: TAG.ERROR, index: 0, reason: END_CAUSE.frameOverLimit.code }),
  ])
})

test("a receive backlog past what the server holds for the page's connection ends its session: each channel ends with `ChannelOverflowError`, told on the event stream, and the POST is refused `frames` naming no channel", async () => {
  const attached = new ServerChannel()
  getChannelMux().registerChannel(attached)
  const ends: [string, string][] = []
  attached.onClose((err) => void ends.push(err === null ? ['clean', ''] : [err.name, err.message]))
  // The second channel isn't registered: the server holds what the page sends it until it is.
  const connId = crypto.randomUUID()
  const opened = await post(
    encodeSseRequest({ connId, streamResponse: true }, [
      encode.reconcile({
        open: [
          { id: attached.id, ix: 0, lastSeq: 0, initial: true },
          { id: new ServerChannel().id, ix: 1, lastSeq: 0, initial: true },
        ],
      }),
    ]),
  )
  // One frame past the base and a window for the one channel the session holds.
  const held = Array.from({ length: WIRE_RECV_BACKLOG_BASE_FRAMES + CREDIT_MSG_WINDOW_MAX + 1 }, (_, i) =>
    encode.text(1, '"x"', i + 1),
  )
  const answer = await post(encodeSseRequest({ connId }, held))
  expect(ends).toEqual([
    [
      'ChannelOverflowError',
      "Channel closed: the page's connection ended: The page sent more on its connection than the server holds while it works on what came before.",
    ],
  ])
  expect(answer.contentType).toMatch(refusal('frames', '', 0x09))
  const told = (await framesOfStream(opened)).filter(({ tag }) => tag === TAG.ERROR)
  expect(told).toEqual([expect.objectContaining({ index: 0, reason: END_CAUSE.receiveBudget.code })])
})

test("a channel's frame the server refuses is refused `frames`, naming the channel", async () => {
  const { connId, opened } = await openWire()
  // A page sends no ERROR but a lost one.
  const answer = await post(encodeSseRequest({ connId }, [encode.error(0, ERROR_REASON.UNCONFIRMED, 1)]))
  expect(answer.statusCode).toBe(400)
  expect(answer.contentType).toMatch(refusal('frames', '0', END_CAUSE.frameRefused.code))
  await cancel(opened)
})

test('a streaming upload whose wire closes under it is answered `retry` at the frame it reads after the close, which is not dispatched, without reading the rest of its body', async () => {
  const { connId, opened, channel } = await openWire()
  const got: unknown[] = []
  channel.listen((message) => void got.push(message))
  // The page's events: the RECONCILED, which also lets the upload's frames through.
  const events = opened.body instanceof ReadableStream ? opened.body.getReader() : null
  if (events === null) throw new Error('no Web event stream')
  expect((await events.read()).done).toBe(false)
  const reading = createDeferred<ReadableStreamDefaultController<Uint8Array>>()
  let reads = 0
  const body = new ReadableStream<Uint8Array>(
    {
      pull: (controller) => {
        if (++reads === 1) controller.enqueue(encodeSseRequestMetadata({ connId, streamRequest: true }))
        else reading.resolve(controller)
      },
    },
    { highWaterMark: 0 },
  )
  const answer = post(body)
  const controller = await reading.promise
  // The event stream ends, closing the wire, while the upload's next frame is on its way.
  await events.cancel()
  const text = encode.text(0, JSON.stringify('after the close'), 1)
  controller.enqueue(encodeU32(text.byteLength))
  controller.enqueue(text)
  // The body stays open: the answer comes without its end.
  const answered = await Promise.race([answer, new Promise<null>((resolve) => setTimeout(() => resolve(null), 1_000))])
  controller.close()
  if (answered === null) throw new Error('no answer while the body was open')
  expect(answered.statusCode).toBe(400)
  expect(answered.contentType).toMatch(refusal('retry'))
  expect(got).toEqual([])
})

test("a body whose source fails with Telefunc's own protocol error is the server's bug: the request throws, and the session's channels live on", async () => {
  const { connId, opened, channel } = await openWire()
  let ended = false
  channel.onClose(() => void (ended = true))
  let pulls = 0
  const failing = new ReadableStream<Uint8Array>({
    pull: (controller) => {
      if (++pulls === 1) controller.enqueue(encodeSseRequestMetadata({ connId }))
      else controller.error(new ProtocolViolationError('a source that fails so'))
    },
  })
  await expect(post(failing)).rejects.toThrow('a source that fails so')
  await new Promise<void>((resolve) => setImmediate(resolve))
  expect(ended).toBe(false)
  await cancel(opened)
})
