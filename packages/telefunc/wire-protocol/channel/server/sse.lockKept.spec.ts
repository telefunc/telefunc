// A body the server lets go of one ping deadline after its wire's end is answered then, also on a runtime whose reader
// keeps its lock while a read is under way, as workerd's does (its `releaseLock()` throws, as the streams standard did
// before 2022, and the read under way ends only at the body's next chunk or its end). Through the SSE handler, with the
// page's streaming upload idle past the end, as a page that didn't see the end sends nothing until its next PING.

import { expect, test } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest, encodeSseRequestMetadata } from '../../wire/envelope.js'
import { encode } from '../../wire/frame.js'
import { pingDeadline } from '../../wire/pingDeadline.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel, ATTACHABLE_NOW } from './ServerChannel.js'
import { handleSseChannelRequest } from './sse.js'

const PING_INTERVAL = 1_000
config.channel = { pingInterval: PING_INTERVAL }
const DEADLINE = pingDeadline(PING_INTERVAL, CHANNEL_TRANSPORT.SSE)
const RETRY = /^text\/vnd\.telefunc\.refusal; kind=retry; reason="[^"]+"$/

/** A request body as workerd gives one: a reader's `releaseLock()` throws while a read of it is under way, and keeps
 *  the lock. */
class LockKeepingBody extends ReadableStream<Uint8Array> {
  override getReader(options: { mode: 'byob' }): ReadableStreamBYOBReader
  override getReader(): ReadableStreamDefaultReader<Uint8Array>
  override getReader(options?: ReadableStreamGetReaderOptions): ReadableStreamReader<Uint8Array>
  override getReader(options?: ReadableStreamGetReaderOptions): ReadableStreamReader<Uint8Array> {
    if (options?.mode === 'byob') return super.getReader(options)
    const reader = super.getReader()
    const read = reader.read.bind(reader)
    const releaseLock = reader.releaseLock.bind(reader)
    let reading = 0
    reader.read = async () => {
      reading++
      try {
        return await read()
      } finally {
        reading--
      }
    }
    reader.releaseLock = () => {
      if (reading > 0) throw new TypeError('Cannot call releaseLock() on a reader with outstanding read promises.')
      releaseLock()
    }
    return reader
  }
}

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
function post(body: BodyInit) {
  // A stream body is sent half duplex, which a `RequestInit` literal doesn't type.
  const init = { method: 'POST', body, duplex: 'half' }
  return handleSseChannelRequest(new Request(url, init), undefined, () => false)
}

test.each([
  ['a body whose reader lets go of its lock with a read under way', false],
  ["a body whose reader keeps its lock with a read under way, as workerd's", true],
] as const)(
  "the page's streaming upload, idle after the server ended its session, in %s, is answered `retry` one ping deadline after the end",
  async (_body, keepsLock) => {
    const mux = getChannelMux()
    const channel = new ServerChannel()
    mux.registerChannel(channel, ATTACHABLE_NOW)
    const connId = crypto.randomUUID()
    const opening = encodeSseRequest({ connId, streamResponse: true }, [
      encode.reconcile({
        sessionId: crypto.randomUUID(),
        attempt: 1,
        open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }],
      }),
    ])
    const stream = (await post(opening)).body
    if (stream instanceof ReadableStream) {
      const events = stream.getReader()
      void (async () => {
        while (!(await events.read()).done);
      })().catch(() => {})
    }
    await channel.opened
    const source = {
      start: (controller: ReadableStreamDefaultController<Uint8Array>) =>
        controller.enqueue(encodeSseRequestMetadata({ connId, streamRequest: true })),
    }
    const body = keepsLock ? new LockKeepingBody(source) : new ReadableStream<Uint8Array>(source)
    const answer = post(body)
    await new Promise((resolve) => setTimeout(resolve, 300))
    // A frame only a server sends: the server ends the session.
    await post(encodeSseRequest({ connId }, [encode.attachResult(999, null)]))
    const endedAt = performance.now()
    const answered = await Promise.race([
      (async () => {
        const { statusCode, contentType } = await answer
        const after = performance.now() - endedAt
        return { statusCode, contentType, onTime: after >= DEADLINE - 100 && after < DEADLINE * 1.5 }
      })(),
      new Promise<'unanswered'>((resolve) => setTimeout(resolve, DEADLINE * 2, 'unanswered')),
    ])
    expect(answered).toEqual({ statusCode: 400, contentType: expect.stringMatching(RETRY), onTime: true })
  },
  DEADLINE * 4,
)
