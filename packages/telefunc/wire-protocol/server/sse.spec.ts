import { expect, test, vi } from 'vitest'
import { getTelefuncSseChannelHooks } from './sse.js'
import { getChannelMux } from './mux.js'
import { ServerChannel } from './channel.js'
import { encodeSseRequestMetadata, type SseRequestMetadata } from '../sse-request.js'
import { encodeLengthPrefixedFrames } from '../frame.js'
import { base64urlToUint8Array } from '../base64url.js'
import { decode, encode, TAG, type DecodedFrame } from '../shared-ws.js'
import { getServerConfig } from '../../node/server/serverConfig.js'

function openPost(metadata: SseRequestMetadata) {
  let controller!: ReadableStreamDefaultController<Uint8Array>
  const body = new ReadableStream<Uint8Array>({ start: (c) => void (controller = c) })
  controller.enqueue(encodeSseRequestMetadata(metadata))
  const request = new Request('http://localhost/_telefunc', { method: 'POST', body, duplex: 'half' } as RequestInit)
  return {
    request,
    push: (frame: Uint8Array<ArrayBuffer>) => controller.enqueue(encodeLengthPrefixedFrames([frame])),
    end: () => controller.close(),
  }
}

function collectFrames(body: ReadableStream<Uint8Array>): DecodedFrame[] {
  const frames: DecodedFrame[] = []
  const reader = body.getReader()
  const decoder = new TextDecoder()
  let text = ''
  void (async () => {
    while (true) {
      const { value, done } = await reader.read()
      if (done) return
      text += decoder.decode(value, { stream: true })
      let end: number
      while ((end = text.indexOf('\n\n')) !== -1) {
        const event = text.slice(0, end)
        text = text.slice(end + 2)
        if (event.startsWith('data: ')) frames.push(decode(base64urlToUint8Array(event.slice('data: '.length))))
      }
    }
  })()
  return frames
}

test("an acknowledged upload POST waits out a reconcile held for a channel the server hasn't registered", async () => {
  vi.useFakeTimers()
  try {
    const sse = getTelefuncSseChannelHooks()
    const connId = crypto.randomUUID()
    const downstream = openPost({ connId, streamResponse: true })
    const response = await sse.handleRequest(downstream.request)
    const received = collectFrames(response!.body as ReadableStream<Uint8Array>)
    const upload = openPost({ connId, streamRequest: true })
    void sse.handleRequest(upload.request)
    await vi.advanceTimersByTimeAsync(100)
    // The page trusts the upload from its open-ack on.
    expect(received.some((frame) => frame.tag === TAG.STREAM_REQUEST_OPEN_ACK)).toBe(true)
    // Its first RECONCILE names a callback whose call was aborted, so the server holds it up to connectTtl.
    downstream.push(encode.reconcile({ open: [{ id: 'aborted-callback', ix: 0, lastSeq: 0, initial: true }] }))
    downstream.end()
    await vi.advanceTimersByTimeAsync(getServerConfig().channel.connectTtl)
    const reconciled = received.flatMap((frame) => (frame.tag === TAG.RECONCILED ? [frame.payload] : []))[0]
    expect(reconciled).toBeDefined()
    // A channel the page opened meanwhile is reconciled on that upload.
    const opened = new ServerChannel({ id: 'opened-meanwhile' })
    let didOpen = false
    opened.onOpen(() => void (didOpen = true))
    getChannelMux().registerChannel(opened)
    upload.push(
      encode.reconcile({
        sessionId: reconciled!.sessionId,
        open: [{ id: 'opened-meanwhile', ix: 1, lastSeq: 0, initial: true }],
      }),
    )
    await vi.advanceTimersByTimeAsync(10)
    expect(didOpen).toBe(true)
  } finally {
    vi.useRealTimers()
  }
})
