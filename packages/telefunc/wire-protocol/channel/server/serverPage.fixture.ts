// A page the server's tests play against the real SSE handler and ChannelMux: its event stream, read as the page reads
// it, a WebSocket the mux sends on, and its reopens of a session the server ended.

export { endedSession, openStream, order, openWebSocket, reopen }

import { Readable } from 'node:stream'

import { CHANNEL_TRANSPORT, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { decode, encode, isOneFrame, isSequencedFrame, messageFrames } from '../../wire/frame.js'
import { SseEvents } from '../../wire/SseEvents.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux, type ServerEnd, type ServerTransport, type WireClose } from './ChannelMux.js'
import { handleSseChannelRequest } from './sse.js'
import { noUpgrades, upgradesTakenHere } from '../../../node/server/runTelefunc/takesUpgrades.js'

type ServePath = 'web' | 'node'

/** A page's event stream: what its frames carried so far, read as the page reads them. */
async function openStream(path: ServePath, reconcile: Uint8Array<ArrayBuffer>) {
  const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
  const connId = crypto.randomUUID()
  const body = encodeSseRequest({ connId, streamResponse: true }, [reconcile])
  const opened =
    path === 'web'
      ? await handleSseChannelRequest(new Request(url, { method: 'POST', body }), undefined, () => false, noUpgrades)
      : await handleSseChannelRequest(
          new Request(url, { method: 'POST' }),
          Readable.from([new Uint8Array(await body.arrayBuffer())]),
          () => false,
          noUpgrades,
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
    /** The page leaves for good, as a closed tab does: its close POST names this stream's wire. */
    leave: () =>
      handleSseChannelRequest(
        new Request(url, { method: 'POST', body: encodeSseRequest({ connId, close: true }) }),
        undefined,
        () => false,
        noUpgrades,
      ),
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

/** A WebSocket the real mux sends on: the frames it was handed, in order, and whether the mux ended it. Its socket keeps
 *  all it is handed, unread, unless `reads`; `knowsBacklog: false` is a runtime that can't tell the socket's backlog
 *  (workerd). */
function openWebSocket({ reads = false, knowsBacklog = true } = {}) {
  const mux = getChannelMux()
  // The wire keeps the id the last RECONCILED gave it.
  const wire: { sessionId?: string } = {}
  const frames: Uint8Array[] = []
  // `closedAs`: how the server ended the wire, where it did.
  const state: { buffered: number; terminated: boolean; closedAs: ServerEnd['how'] | null } = {
    buffered: 0,
    terminated: false,
    closedAs: null,
  }
  const transport: ServerTransport<typeof wire> = {
    getConnId: () => null,
    sendNow: (_, message) => {
      if (!reads) state.buffered += message.byteLength
      for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) {
        frames.push(frame)
        const decoded = frame[0] === TAG.RECONCILED ? decode(frame) : null
        if (decoded?.tag === TAG.RECONCILED) wire.sessionId = decoded.payload.sessionId
      }
    },
    type: CHANNEL_TRANSPORT.WS,
    wholeMessages: true,
    bufferedAmount: () => (knowsBacklog ? state.buffered : undefined),
    close: (_wire, { how }) => {
      state.terminated = true
      state.closedAs = how
    },
  }
  mux.onWireOpen(wire, transport, upgradesTakenHere)
  return {
    mux,
    wire,
    frames,
    state,
    /** The page's connection ends: the server's wire closes as `how` says, as the page leaving unless told. */
    close: (how: WireClose = 'pageLeft') => mux.onWireClosed(wire, how),
  }
}

/** What the wire's first RECONCILE came to: the session (`took`), a loss (`transient`), or the session's end. */
const came = (ws: ReturnType<typeof openWebSocket>) =>
  ws.frames.some((frame) => frame[0] === TAG.RECONCILED) ? 'took' : (ws.state.closedAs ?? 'nothing')

/** The page's wire numbered `attempt`, whose first RECONCILE names `sessionId`, and what it came to. */
async function reopen(sessionId: string, attempt: number, open: { id: string; ix: number }[] = []) {
  const ws = openWebSocket()
  await ws.mux.onWireMessage(
    ws.wire,
    encode.reconcile({ sessionId, attempt, open: open.map(({ id, ix }) => ({ id, ix, lastSeq: 0, initial: true })) }),
  )
  ws.close()
  return came(ws)
}

/** A session attempt 1 took, which the server ended for what it couldn't read; the clock then reads the decision. */
async function endedSession() {
  const ws = openWebSocket()
  await ws.mux.onWireMessage(ws.wire, encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open: [] }))
  const sessionId = ws.wire.sessionId ?? ''
  const decidedAt = performance.now()
  ws.mux.endFault(ws.wire, { scope: 'session', cause: 'unreadable' })
  return { sessionId, decidedAt }
}
