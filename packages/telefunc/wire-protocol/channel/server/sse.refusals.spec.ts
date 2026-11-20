// Telefunc's own refusals of the page's SSE requests say so in their media type, with what the page does about each:
// `retry` (the server no longer has the wire: a loss), `frames` (it can't take a frame: the channel ends, named where
// the server knows it), `route` (it doesn't serve SSE this way: the page leaves SSE) and `reload` (another wire
// version), each with a reason for the page's error (decisions/516-sse-carrier.md, step 6 at 13:08Z and 13:14Z).

import { afterEach, expect, test } from 'vitest'

import { WIRE_MAX_RAW_FRAME_BYTES, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest, encodeSseRequestMetadata, encodeU32 } from '../../wire/envelope.js'
import { ERROR_REASON, encode } from '../../wire/frame.js'
import { config } from '../../../node/server/serverConfig.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { handleSseChannelRequest, type SseChannelHttpResponse } from './sse.js'

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`

function post(body: BodyInit, { at = url, method = 'POST' } = {}) {
  // A stream body is sent half duplex, which a `RequestInit` literal doesn't type.
  const init = { method, body, duplex: 'half' }
  return handleSseChannelRequest(new Request(at, init))
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
  return { connId, opened }
}

async function cancel(answer: SseChannelHttpResponse): Promise<void> {
  if (answer.body instanceof ReadableStream) await answer.body.cancel()
}

/** The refusal's `Content-Type`, read as the page reads it: kind, then the channel where named, then the reason. */
const refusal = (kind: string, channel = '') =>
  new RegExp(`^text/vnd\\.telefunc\\.refusal; kind=${kind}${channel && `; channel=${channel}`}; reason="[^"]+"$`)

afterEach(() => {
  config.channel = {}
})

test.each([
  ['a GET', () => handleSseChannelRequest(new Request(url))],
  ['a POST with no body', () => handleSseChannelRequest(new Request(url, { method: 'POST' }))],
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

test('a frame over the frame limit is refused `frames`, naming no channel: the server reads no header of it', async () => {
  const { connId, opened } = await openWire()
  const answer = await post(bodyOf(encodeSseRequestMetadata({ connId }), encodeU32(WIRE_MAX_RAW_FRAME_BYTES + 1)))
  expect(answer.statusCode).toBe(400)
  expect(answer.contentType).toMatch(refusal('frames'))
  await cancel(opened)
})

test("a channel's frame the server refuses is refused `frames`, naming the channel", async () => {
  const { connId, opened } = await openWire()
  // A page sends no ERROR but a lost one.
  const answer = await post(encodeSseRequest({ connId }, [encode.error(0, ERROR_REASON.UNCONFIRMED, 1)]))
  expect(answer.statusCode).toBe(400)
  expect(answer.contentType).toMatch(refusal('frames', '0'))
  await cancel(opened)
})
