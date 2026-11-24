// A request that found its wire just before the wire ended, and whose body the server starts reading only after the
// end: it is answered `retry` by one ping deadline after the end, as every body the server reads for a wire that ended:
// at once where it finds the wire already closed, else when the server lets go of its body. Through the SSE handler
// with a Web body, the event stream cancelled at each microtask turn of the request's start.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest, encodeSseRequestMetadata } from '../../wire/envelope.js'
import { encode } from '../../wire/frame.js'
import { pingDeadline } from '../../wire/pingDeadline.js'
import { handleSseChannelRequest } from './sse.js'

const PING_INTERVAL = 1_000
config.channel = { pingInterval: PING_INTERVAL }
const DEADLINE = pingDeadline(PING_INTERVAL, CHANNEL_TRANSPORT.SSE)
const RETRY = /^text\/vnd\.telefunc\.refusal; kind=retry; reason="[^"]+"$/

beforeAll(() => void vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] }))
afterAll(() => void vi.useRealTimers())

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
function post(body: BodyInit) {
  // A stream body is sent half duplex, which a `RequestInit` literal doesn't type.
  const init = { method: 'POST', body, duplex: 'half' }
  return handleSseChannelRequest(new Request(url, init), undefined, () => false)
}

test.each([
  ['the streaming upload', true],
  ['a batch POST whose body streams', false],
] as const)(
  "%s whose wire ends at any microtask turn of the request's start is answered `retry` by one ping deadline after the end",
  async (_request, streamRequest) => {
    const unanswered: number[] = []
    for (let turns = 0; turns < 24; turns++) {
      const connId = crypto.randomUUID()
      const opening = await post(
        encodeSseRequest({ connId, streamResponse: true }, [
          encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open: [] }),
        ]),
      )
      const events = opening.body
      if (!(events instanceof ReadableStream)) throw new Error('No event stream')
      // The opening POST's body read to its end: the wire is ready.
      await new Promise((resolve) => setImmediate(resolve))
      const body = new TransformStream<Uint8Array<ArrayBuffer>, Uint8Array<ArrayBuffer>>()
      const writer = body.writable.getWriter()
      void writer.write(encodeSseRequestMetadata(streamRequest ? { connId, streamRequest: true } : { connId }))
      let contentType: string | null = null
      const answering = (async () => {
        contentType = (await post(body.readable)).contentType
      })()
      for (let turn = 0; turn < turns; turn++) await Promise.resolve()
      await events.cancel()
      await vi.advanceTimersByTimeAsync(DEADLINE + 1)
      if (contentType === null || !RETRY.test(contentType)) unanswered.push(turns)
      await writer.close()
      await answering
    }
    expect(unanswered).toEqual([])
  },
)
