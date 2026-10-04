import { expect, test, vi } from 'vitest'
import { getTelefuncSseChannelHooks } from './sse.js'
import { getChannelMux } from './mux.js'
import { ServerChannel } from './channel.js'
import { encodeSseBatch, encodeSseRequestMetadata, type SseRequestMetadata } from '../sse-request.js'
import { encodeLengthPrefixedFrames } from '../frame.js'
import { base64urlToUint8Array } from '../base64url.js'
import { decode, encode, TAG, type DecodedFrame, type SeqReader } from '../shared-ws.js'
import { Readable } from 'node:stream'
import { config, getServerConfig } from '../../node/server/serverConfig.js'
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
  // Within the ping deadline, which this page, sending no PING, would otherwise pass.
  config.channel = { connectTtl: 5_000 }
  ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
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
    config.channel = {}
    ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
  }
})

test.each([
  ['an upload stream', true],
  ['a batch POST', false],
])(
  'a message that %s carries for longer than the ping deadline keeps its wire, and the page hears meanwhile that it arrives',
  async (_, streamRequest) => {
    vi.useFakeTimers()
    try {
      const sse = getTelefuncSseChannelHooks()
      const connId = crypto.randomUUID()
      const channel = new ServerChannel<unknown, never>()
      let received = 0
      channel.listen(() => void received++)
      getChannelMux().registerChannel(channel)
      const downstream = openPost({ connId, streamResponse: true })
      const response = await sse.handleRequest(downstream.request)
      const frames = collectFrames(response!.body as ReadableStream<Uint8Array>)
      downstream.push(encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }))
      downstream.end()
      await vi.advanceTimersByTimeAsync(10)
      const post = openPost(streamRequest ? { connId, streamRequest } : { connId })
      void sse.handleRequest(post.request)
      // Its bytes cross a slow link half a ping interval at a time, for twice the ping deadline.
      const { pingInterval } = getServerConfig().channel
      const bytes = encodeLengthPrefixedFrames([encode.text(0, JSON.stringify('x'.repeat(8 * 1024)), 1)])
      const chunk = Math.ceil(bytes.length / 8)
      for (let at = 0; at < bytes.length; at += chunk) {
        post.pushBytes(bytes.subarray(at, at + chunk))
        await vi.advanceTimersByTimeAsync(pingInterval / 2)
      }
      expect(getChannelMux().getConnectionByConnId(connId)).toBeDefined()
      expect(received).toBe(1)
      expect(frames.filter((frame) => frame.tag === TAG.PONG).length).toBeGreaterThanOrEqual(3)
      post.end()
    } finally {
      vi.useRealTimers()
    }
  },
)

/** An SSE wire whose page has one channel attached, which counts what reaches its listener. */
async function reconciledSseWire() {
  const sse = getTelefuncSseChannelHooks()
  const connId = crypto.randomUUID()
  const channel = new ServerChannel<unknown, never>()
  const received = { count: 0 }
  channel.listen(() => void received.count++)
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

test('a batch POST whose body is built from its frames as they are reads as the frames it carries', async () => {
  const wire = await reconciledSseWire()
  const frames = [encode.text(0, '1', 1), encode.text(0, '2', 2)]
  const request = new Request('http://localhost/_telefunc', {
    method: 'POST',
    body: encodeSseBatch({ connId: wire.connId }, frames),
  })
  expect((await wire.sse.handleRequest(request))!.statusCode).toBe(200)
  expect(wire.received.count).toBe(2)
})

test("a batch POST carrying a channel's full message window is processed", async () => {
  const wire = await reconciledSseWire()
  const batch = openPost({ connId: wire.connId })
  batch.pushAll(fullWindow())
  batch.end()
  expect((await wire.sse.handleRequest(batch.request))!.statusCode).toBe(200)
  expect(wire.isOpen()).toBe(true)
  expect(wire.received.count).toBe(CREDIT_MSG_WINDOW_MAX)
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
