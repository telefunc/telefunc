import { expect, test, vi } from 'vitest'
import { getTelefuncSseChannelHooks } from './sse.js'
import { getChannelMux } from './mux.js'
import { ServerChannel } from './channel.js'
import { encodeSseRequestMetadata, SSE_FLUSH_READ, SSE_FLUSH_TAKEN, type SseRequestMetadata } from '../sse-request.js'
import { encodeLengthPrefixedFrames } from '../frame.js'
import { base64urlToUint8Array } from '../base64url.js'
import { decode, encode, TAG, type DecodedFrame, type SeqReader } from '../shared-ws.js'
import { Readable } from 'node:stream'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { CREDIT_MSG_WINDOW_MAX, CREDIT_WINDOW_INITIAL_BYTES, CREDIT_WINDOW_MAX_BYTES } from '../constants.js'
import { ChannelOverflowError } from '../channel-errors.js'
import type { PushReadable } from '../push-readable.js'
import type { PushReadableStream } from '../push-readable-stream.js'
import { loadStreamNodeModuleOnce } from '../../utils/loadStreamNodeModule.js'

/** A receiver with nothing of any channel: each seq reads as its low 32 bits. */
const wireSeqs: SeqReader = { received: () => 0, sent: () => 0 }

function openPost(metadata: SseRequestMetadata) {
  let controller!: ReadableStreamDefaultController<Uint8Array>
  const body = new ReadableStream<Uint8Array>({ start: (c) => void (controller = c) })
  controller.enqueue(encodeSseRequestMetadata(metadata))
  const request = new Request('http://localhost/_telefunc', { method: 'POST', body, duplex: 'half' } as RequestInit)
  return {
    request,
    push: (frame: Uint8Array<ArrayBuffer>) => controller.enqueue(encodeLengthPrefixedFrames([frame])),
    /** All of `frames` in one chunk, as a body read hands them over. */
    pushAll: (frames: Uint8Array<ArrayBuffer>[]) => controller.enqueue(encodeLengthPrefixedFrames(frames)),
    pushBytes: (bytes: Uint8Array) => controller.enqueue(bytes),
    /** Chunks pushed that the server hasn't read. */
    unread: () => 1 - controller.desiredSize!,
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
        if (event.startsWith('data: '))
          frames.push(decode(base64urlToUint8Array(event.slice('data: '.length)), wireSeqs))
      }
    }
  })()
  return frames
}

test("an acknowledged upload POST waits out the connection's first reconcile, however long that reconcile's POST takes", async () => {
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
    // Its first RECONCILE's POST body ends past connectTtl, as on a slow link, and the RECONCILED waits for that.
    downstream.push(encode.reconcile({ open: [] }))
    await vi.advanceTimersByTimeAsync(getServerConfig().channel.connectTtl)
    downstream.end()
    await vi.advanceTimersByTimeAsync(10)
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

/** An SSE wire whose page has one channel attached, which counts and keeps what reaches its listener. */
async function reconciledSseWire() {
  const sse = getTelefuncSseChannelHooks()
  const connId = crypto.randomUUID()
  const channel = new ServerChannel<unknown, never>()
  const received = { count: 0, values: [] as unknown[] }
  channel.listen((value) => {
    received.count++
    received.values.push(value)
  })
  getChannelMux().registerChannel(channel)
  const downstream = openPost({ connId, streamResponse: true })
  const response = await sse.handleRequest(downstream.request)
  const frames = collectFrames(response!.body as ReadableStream<Uint8Array>)
  downstream.push(encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }))
  downstream.end()
  await vi.waitFor(() => expect(frames.some((frame) => frame.tag === TAG.RECONCILED)).toBe(true))
  return { sse, connId, received, isOpen: () => getChannelMux().getConnectionByConnId(connId) !== undefined }
}

/** A channel's full message window, with the refresh and probe a page sends among it. */
function fullWindow() {
  const frames = Array.from({ length: CREDIT_MSG_WINDOW_MAX }, (_, i) => encode.text(0, '1', i + 1))
  frames.push(encode.msgWindow(0, 2 * CREDIT_MSG_WINDOW_MAX), encode.bdpPing(0, 1))
  return frames
}

test("a batch POST carrying a channel's full message window is processed", async () => {
  const wire = await reconciledSseWire()
  const batch = openPost({ connId: wire.connId })
  batch.pushAll(fullWindow())
  batch.end()
  expect((await wire.sse.handleRequest(batch.request))!.statusCode).toBe(200)
  expect(wire.isOpen()).toBe(true)
  expect(wire.received.count).toBe(CREDIT_MSG_WINDOW_MAX)
})

test('the server answers a flush as it begins to read it, and its answer says it read all of it as it ends', async () => {
  const wire = await reconciledSseWire()
  const flush = openPost({ connId: wire.connId, flush: true })
  const response = (await wire.sse.handleRequest(flush.request))!
  expect(response.statusCode).toBe(200)
  const answer = new Response(response.body as ReadableStream<Uint8Array>).text()
  flush.push(encode.text(0, '1', 1))
  flush.end()
  expect(await answer).toBe(SSE_FLUSH_TAKEN + SSE_FLUSH_READ)
  expect(wire.received.values).toEqual([1])
})

test('the server takes a flush once it has read the one before, whatever of it came first', async () => {
  const wire = await reconciledSseWire()
  const first = openPost({ connId: wire.connId, flush: true })
  const firstAnswer = (await wire.sse.handleRequest(first.request))!
  first.push(encode.text(0, '1', 1))
  const second = openPost({ connId: wire.connId, flush: true })
  second.pushAll([encode.text(0, '3', 3), encode.text(0, '4', 4)])
  second.end()
  let secondAnswered = false
  const secondAnswer = wire.sse.handleRequest(second.request).then((response) => {
    secondAnswered = true
    return response!
  })
  await vi.waitFor(() => expect(wire.received.values).toEqual([1]))
  await new Promise((resolve) => setTimeout(resolve, 20))
  expect(secondAnswered).toBe(false)
  first.push(encode.text(0, '2', 2))
  first.end()
  expect(await new Response(firstAnswer.body as ReadableStream<Uint8Array>).text()).toBe(
    SSE_FLUSH_TAKEN + SSE_FLUSH_READ,
  )
  expect(await new Response((await secondAnswer).body as ReadableStream<Uint8Array>).text()).toBe(
    SSE_FLUSH_TAKEN + SSE_FLUSH_READ,
  )
  expect(wire.received.values).toEqual([1, 2, 3, 4])
})

test("a flush the server couldn't read all of, cut mid-frame, ends its answer without saying it read it", async () => {
  const wire = await reconciledSseWire()
  const flush = openPost({ connId: wire.connId, flush: true })
  const response = (await wire.sse.handleRequest(flush.request))!
  const answer = new Response(response.body as ReadableStream<Uint8Array>).text()
  flush.pushBytes(encodeLengthPrefixedFrames([encode.text(0, '1', 1)]).subarray(0, 6))
  flush.end()
  expect(await answer).toBe(SSE_FLUSH_TAKEN)
})

test('the server reads the body of a flush that waits its turn as it arrives, and dispatches it in its turn', async () => {
  const wire = await reconciledSseWire()
  const first = openPost({ connId: wire.connId, flush: true })
  const firstAnswer = (await wire.sse.handleRequest(first.request))!
  first.push(encode.text(0, '1', 1))
  const second = openPost({ connId: wire.connId, flush: true })
  void wire.sse.handleRequest(second.request)
  second.push(encode.text(0, '2', 2))
  await vi.waitFor(() => expect(second.unread()).toBe(0))
  expect(wire.received.values).toEqual([1])
  first.end()
  second.end()
  await new Response(firstAnswer.body as ReadableStream<Uint8Array>).text()
  await vi.waitFor(() => expect(wire.received.values).toEqual([1, 2]))
})

// Its rest would never come: the page resends from the server's lastSeq, which what came after would have moved past it.
test("a flush after one the server couldn't read all of is refused unread", async () => {
  const wire = await reconciledSseWire()
  const cut = openPost({ connId: wire.connId, flush: true })
  const cutAnswer = (await wire.sse.handleRequest(cut.request))!
  cut.push(encode.text(0, '1', 1))
  cut.pushBytes(encodeLengthPrefixedFrames([encode.text(0, '2', 2)]).subarray(0, 6))
  cut.end()
  expect(await new Response(cutAnswer.body as ReadableStream<Uint8Array>).text()).toBe(SSE_FLUSH_TAKEN)
  const next = openPost({ connId: wire.connId, flush: true })
  next.push(encode.text(0, '3', 3))
  next.end()
  expect((await wire.sse.handleRequest(next.request))!.statusCode).toBe(400)
  expect(wire.received.values).toEqual([1])
})

test("an upload stream handed a channel's full message window at once is processed", async () => {
  const wire = await reconciledSseWire()
  const upload = openPost({ connId: wire.connId, streamRequest: true })
  void wire.sse.handleRequest(upload.request)
  upload.pushAll(fullWindow())
  await vi.waitFor(() => expect(wire.received.count === CREDIT_MSG_WINDOW_MAX || !wire.isOpen()).toBe(true), {
    timeout: 10_000,
  })
  expect(wire.isOpen()).toBe(true)
  expect(wire.received.count).toBe(CREDIT_MSG_WINDOW_MAX)
  upload.end()
})

test.each([
  ['a Node', true],
  ['a web', false],
])(
  "a page that stops reading %s SSE stream holds what a channel sends nobody awaits to the page's window and the largest window a page grants: the next send rejects with ChannelOverflowError",
  async (_, node) => {
    await loadStreamNodeModuleOnce() // as runTelefunc does before an SSE request reaches the transport
    const sse = getTelefuncSseChannelHooks()
    const channel = new ServerChannel<unknown, string>()
    let opened = false
    channel.onOpen(() => void (opened = true))
    getChannelMux().registerChannel(channel)
    const downstream = openPost({ connId: crypto.randomUUID(), streamResponse: true })
    const response = node
      ? await sse.handleRequest(downstream.request, Readable.fromWeb(downstream.request.body! as never))
      : await sse.handleRequest(downstream.request)
    // Nothing reads the event stream.
    const body = response!.body as PushReadable | PushReadableStream
    downstream.push(encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }))
    downstream.end()
    await vi.waitFor(() => expect(opened).toBe(true))

    let error: unknown
    for (let n = 0; error === undefined && n < 6_000; n++) {
      channel.send(String(n).padEnd(16 * 1024)).catch((err: unknown) => (error = err))
      await Promise.resolve()
    }
    expect(error).toBeInstanceOf(ChannelOverflowError)
    // An event carries its frame in base64.
    // One message past them, and each one's header and event framing.
    const bound = ((CREDIT_WINDOW_INITIAL_BYTES + CREDIT_WINDOW_MAX_BYTES + 128 * 1024) * 4) / 3
    expect(body.bufferedAmount).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES)
    expect(body.bufferedAmount).toBeLessThanOrEqual(bound)
  },
)
