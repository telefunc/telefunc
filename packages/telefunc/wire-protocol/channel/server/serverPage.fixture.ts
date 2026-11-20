// A page the server's tests play against the real SSE handler and ChannelMux: its event stream, read as the page reads
// it, and a WebSocket the mux sends on.

export { openStream, order, openWebSocket }

import { Readable } from 'node:stream'

import { CHANNEL_TRANSPORT, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { decode, isOneFrame, isSequencedFrame, messageFrames } from '../../wire/frame.js'
import { SseEvents } from '../../wire/SseEvents.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux, type ServerTransport } from './ChannelMux.js'
import { handleSseChannelRequest } from './sse.js'

type ServePath = 'web' | 'node'

/** A page's event stream: what its frames carried so far, read as the page reads them. */
async function openStream(path: ServePath, reconcile: Uint8Array<ArrayBuffer>) {
  const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
  const body = encodeSseRequest({ connId: crypto.randomUUID(), streamResponse: true }, [reconcile])
  const opened =
    path === 'web'
      ? await handleSseChannelRequest(new Request(url, { method: 'POST', body }))
      : await handleSseChannelRequest(
          new Request(url, { method: 'POST' }),
          Readable.from([new Uint8Array(await body.arrayBuffer())]),
        )
  const stream = opened.body
  if (typeof stream === 'string') throw new Error(`no event stream: ${opened.statusCode} ${stream}`)
  const reader = (stream instanceof ReadableStream ? stream : webStreamOf(stream)).getReader()
  const events = new SseEvents()
  const frames: Uint8Array[] = []
  return {
    /** Reads until a frame `until` accepts has come, and returns every frame so far. */
    async readUntil(until: (frame: Uint8Array) => boolean): Promise<Uint8Array[]> {
      while (!frames.some(until)) {
        const read = await reader.read()
        if (read.done) break
        events.push(read.value, (frame) => frames.push(frame))
      }
      return frames
    },
    /** The page's connection drops: the server's wire closes transiently. */
    drop: () => (stream instanceof ReadableStream ? reader.cancel() : stream.destroy()),
  }
}

/** The Node adapter's body as a stream the page reads. */
function webStreamOf(readable: Readable): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      readable.on('data', (chunk: Uint8Array) => controller.enqueue(chunk))
      readable.on('end', () => controller.close())
    },
  })
}

/** The RECONCILED and each channel frame, as `RECONCILED` or `<seq>:<message>`; a lossy send's head names the seq its
 *  message has. */
function order(frames: Uint8Array[]): string[] {
  return frames.flatMap((frame) => {
    const decoded = decode(frame)
    if (decoded.tag === TAG.RECONCILED) return ['RECONCILED']
    if (!isSequencedFrame(decoded) || decoded.tag === TAG.PUBLISH_TO) return []
    return [`${decoded.seq}:${decoded.tag === TAG.TEXT ? JSON.parse(decoded.text) : `0x${decoded.tag.toString(16)}`}`]
  })
}

type FakeWire = { sessionId: string | undefined }

/** A WebSocket the real mux sends on: the frames it was handed, in order, and whether the mux ended it. Its socket keeps
 *  all it is handed, unread, unless `reads`; `knowsBacklog: false` is a runtime that can't tell the socket's backlog
 *  (workerd). */
function openWebSocket({ reads = false, knowsBacklog = true } = {}) {
  const mux = getChannelMux()
  const wire: FakeWire = { sessionId: undefined }
  const frames: Uint8Array[] = []
  const state = { buffered: 0, terminated: false }
  const transport: ServerTransport<FakeWire> = {
    getSessionId: (w) => w.sessionId,
    setSessionId: (w, sessionId) => {
      w.sessionId = sessionId
    },
    getConnId: () => null,
    sendNow: (_, message) => {
      if (!reads) state.buffered += message.byteLength
      for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) frames.push(frame)
    },
    type: CHANNEL_TRANSPORT.WS,
    wholeMessages: true,
    bufferedAmount: () => (knowsBacklog ? state.buffered : undefined),
    terminateWire: () => {
      state.terminated = true
    },
  }
  mux.onWireOpen(wire, transport)
  return {
    mux,
    wire,
    frames,
    state,
    /** The page's connection drops: the server's wire closes, transiently unless `permanent`. */
    close: (permanent = true) => mux.onWireClosed(wire, { permanent }),
  }
}
