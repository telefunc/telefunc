export { handleSseChannelRequest }
export type { SseChannelHttpResponse, SseWire }

import type { Readable } from 'node:stream'
import { Fifo } from '../../../utils/Fifo.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'
import { handleTelefunctionBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import {
  CHANNEL_CONNECT_TTL_MS,
  CHANNEL_TRANSPORT,
  SSE_EVENT_CHUNK_BYTES,
  SSE_METADATA_MAX_BYTES,
  STATUS_CODE_WIRE_VERSION,
  WIRE_GATHER_BYTES,
  WIRE_MAX_RAW_FRAME_BYTES,
} from '../../constants.js'
import { namesWireVersion } from '../../wire/requestKind.js'
import { createPushReadableStream, type PushReadableStream } from '../../../utils/pushReadableStream.js'
import { createPushReadable, type PushReadable } from '../../../utils/pushReadable.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { parseSseRequestMetadata, textEncoder, type SseRequestMetadata } from '../../wire/envelope.js'
import { OversizeFrameError, BodyReader, StreamTruncatedError } from '../../wire/BodyReader.js'
import { getChannelMux } from './ChannelMux.js'
import type { ReconcileOutcome, ServerTransport } from './ChannelMux.js'
import { encode } from '../../wire/frame.js'
import { TAG, passesData } from '../../wire/tags.js'
import { DrainRate } from '../../flowControl/DrainRate.js'
import {
  countWaitingPublishes,
  droppingPublishes,
  overtakes,
  type WireQueue,
} from '../../flowControl/server/waitingPublishes.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import { isProtocolViolationError, ProtocolViolationError } from '../../wire/protocolViolation.js'

type SseChannelHttpResponse = {
  statusCode: 200 | 400 | 409
  contentType: 'text/plain' | 'text/event-stream'
  headers: [string, string][]
  body: string | Readable | ReadableStream<Uint8Array<ArrayBuffer>>
}

type SseWire = {
  connId: string
  events: EventStream
  closed: boolean
  sessionId: string | null
  /** Resolved by `runStreamResponse` once the stream-response POST's body is consumed. Data
   *  POSTs gate on this before dispatching so they can't race ahead of the reconcile. */
  ready: Promise<void>
  resolveReady: () => void
  /** Dispatches in flight for this wire. A reconcile waits on these before reporting a seq,
   *  so a batch POST's `_lastClientSeq` mutations can't land after the RECONCILED that reports them. */
  pendingDispatches: Set<Promise<unknown>>
}

type EventStreamOptions = {
  useNodeStream: boolean
  onCancel: () => void
}

/** The event stream to the page: data waits for it to ask for more, and the frames that go at once pass that data. An
 *  event goes in chunks, as the stream asks, and nothing passes it once it started. */
class EventStream implements WireQueue {
  readonly stream: PushReadable | PushReadableStream<Uint8Array<ArrayBuffer>>
  private waiting = new Fifo<Uint8Array<ArrayBuffer>>()
  /** The frames that go at once, waiting for the event going to the stream to end. */
  private passing = new Fifo<Uint8Array<ArrayBuffer>>()
  /** The encoded bytes of `waiting` and `passing`. */
  private waitingBytes = 0
  /** The event going to the stream, and how far it went. */
  private current: { event: Uint8Array<ArrayBuffer>; offset: number } | null = null
  /** Whether the stream asks for more. */
  private takes: boolean
  /** The rate the page takes what waits, and when it last took some; null while nothing waits. */
  private readonly drain = new DrainRate()
  private tookAt: number | null = null

  constructor({ useNodeStream, onCancel }: EventStreamOptions) {
    // A Web stream asks from inside `enqueue`: an ask waits, so that no push runs inside another.
    const onTake = () => queueMicrotask(() => this.handWaiting())
    this.stream = useNodeStream
      ? createPushReadable(onCancel, onTake)
      : createPushReadableStream<Uint8Array<ArrayBuffer>>(onCancel, onTake)
    this.takes = this.stream.push(textEncoder.encode(': open\n\n'))
  }

  /** The encoded bytes the stream holds, and those waiting for it to ask. */
  get heldBytes(): number {
    return this.streamBytes + this.waitingBytes
  }

  /** The encoded bytes a frame that goes at once is behind. */
  get streamBytes(): number {
    const { current } = this
    return this.stream.bufferedAmount + (current === null ? 0 : current.event.byteLength - current.offset)
  }

  /** Data waits for the stream to ask for it, as the frame the replay also holds: it's encoded when it's handed. */
  send(frame: Uint8Array<ArrayBuffer>): void {
    const atOnce = passesData(frame[0]!) || (frame.byteLength <= WIRE_GATHER_BYTES && overtakes(frame, this.waiting))
    if (atOnce && this.current !== null) {
      this.passing.push(frame)
      this.waitingBytes += eventBytes(frame)
      return
    }
    if (atOnce || (this.takes && this.current === null && this.waiting.length === 0)) {
      this.startEvent(frame)
      return
    }
    this.tookAt ??= performance.now()
    this.waiting.push(frame)
    this.waitingBytes += eventBytes(frame)
  }

  waitingPublishes(ix: number): number {
    return countWaitingPublishes(this.waiting, ix)
  }

  dropWaitingPublishes(ix: number, count: number): void {
    const drops = droppingPublishes(ix, count)
    this.waiting.remove((frame) => {
      if (!drops(frame)) return false
      this.waitingBytes -= eventBytes(frame)
      return true
    })
  }

  drainRate(): number {
    return this.drain.rate
  }

  /** Past a wire only lost, what waits is dropped: the page's reconnect gets it from its channels' replay, and it could
   *  wait here for as long as the socket stays open taking nothing. */
  close({ permanent }: { permanent: boolean }): void {
    while (permanent && (this.current !== null || this.waiting.length > 0)) {
      if (this.current !== null) this.pushChunks()
      else this.startEvent(this.waiting.shift()!)
    }
    this.current = null
    this.passing = new Fifo()
    this.waiting = new Fifo()
    this.waitingBytes = 0
    this.stream.close()
  }

  /** Hands the stream the data that waits, as far as it asks. */
  private handWaiting(): void {
    this.takes = true
    let bytes = this.current === null ? 0 : this.pushChunks()
    while (this.takes && this.waiting.length > 0) {
      const frame = this.waiting.shift()!
      this.waitingBytes -= eventBytes(frame)
      bytes += this.startEvent(frame)
    }
    if (this.tookAt === null) return
    const now = performance.now()
    this.drain.spanned((bytes * 3) / 4, now - this.tookAt)
    this.tookAt = this.current !== null || this.waiting.length > 0 ? now : null
  }

  /** Starts `frame`'s event and returns the bytes that went. */
  private startEvent(frame: Uint8Array<ArrayBuffer>): number {
    this.current = { event: eventOf(frame), offset: 0 }
    return this.pushChunks()
  }

  /** Pushes the current event's next chunks, at least one, as far as the stream asks, and returns their bytes. What passes
   *  goes as the event ends. */
  private pushChunks(): number {
    const current = this.current!
    const { event } = current
    const start = current.offset
    do {
      const end = Math.min(event.byteLength, current.offset + SSE_EVENT_CHUNK_BYTES)
      this.takes = this.stream.push(event.subarray(current.offset, end))
      current.offset = end
    } while (this.takes && current.offset < event.byteLength)
    if (current.offset < event.byteLength) return current.offset - start
    this.current = null
    while (this.passing.length > 0) {
      const frame = this.passing.shift()!
      this.waitingBytes -= eventBytes(frame)
      this.takes = this.stream.push(eventOf(frame))
    }
    return event.byteLength - start
  }
}

/** The event of the publish body being delivered in this task: its subscribers hand their streams the one event. */
const sharedEvent: { frame: Uint8Array | null; event: Uint8Array<ArrayBuffer> | null } = { frame: null, event: null }

function eventOf(frame: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
  if (sharedEvent.frame === frame) return sharedEvent.event!
  const event = textEncoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`)
  if (frame[0] !== TAG.PUBLISH && frame[0] !== TAG.PUBLISH_BINARY) return event
  if (sharedEvent.frame === null) queueMicrotask(forgetSharedEvent)
  sharedEvent.frame = frame
  sharedEvent.event = event
  return event
}

function forgetSharedEvent(): void {
  sharedEvent.frame = null
  sharedEvent.event = null
}

/** What `eventOf` takes for `frame`: its base64url, unpadded, and the 8 bytes around it. */
function eventBytes(frame: Uint8Array<ArrayBuffer>): number {
  return Math.ceil((frame.byteLength * 4) / 3) + 8
}

const globalObject = getGlobalObject('wire-protocol/channel/server/sse.ts', {
  sseTransport: null as SseServerTransport | null,
})

class SseServerTransport {
  /** Resolvers for data POSTs that arrived before the stream-response POST registered the
   *  wire — covers the same-instance race where the long-lived stream-request POST
   *  lands before the stream-response POST. */
  private readonly pendingWires = new Map<string, Set<(wire: SseWire | null) => void>>()
  /** When each wire closed and the session it had, for `CHANNEL_CONNECT_TTL_MS`: a page never reuses a connId, so a POST for one is
   *  refused. */
  private readonly closedWires = new Map<string, { at: number; sessionId: string | null }>()
  private readonly mux = getChannelMux()
  private readonly transport: ServerTransport<SseWire> = {
    getSessionId: (wire) => wire.sessionId ?? undefined,
    setSessionId: (wire, sessionId) => {
      wire.sessionId = sessionId
    },
    getConnId: (wire) => wire.connId,
    sendNow: (wire, frame) => this.sendNow(wire, frame),
    wholeMessages: false,
    // Events carry frames in base64 plus framing, so three quarters of what waits is at least the frames it carries.
    bufferedAmount: (wire) => Math.floor((wire.events.heldBytes * 3) / 4),
    // A frame that goes at once passes the data waiting for the stream to ask: it waits behind the stream's alone.
    queuedAhead: (wire) => Math.floor((wire.events.streamBytes * 3) / 4),
    queueOf: (wire) => wire.events,
    terminateWire: (wire) => this.terminateWire(wire),
  }

  async handleRequest(request: Request, readable?: Readable): Promise<SseChannelHttpResponse> {
    if (!getServerConfig().channel.transports.includes(CHANNEL_TRANSPORT.SSE)) return badRequest()
    if (request.method !== 'POST') return badRequest()
    if (!namesWireVersion(request)) return wireVersionRefused()
    const source = readable ?? request.body
    // A Web `Request` sent without a body has `body === null`; a Node one, an empty stream (below).
    if (!source) return noBody()
    // Adapter mode: a Node `IncomingMessage` means we're on the Node-native serve path
    // and should answer with a Node `Readable` so it pipes straight to the socket. Web
    // adapters get `request.body` (no `readable`) and want a `ReadableStream` back.
    const useNodeStream = readable !== undefined
    let connId: string | null = null
    try {
      const reader = new BodyReader(source)
      const rawMetadata = await reader.readLengthPrefixedBytesOrNull(SSE_METADATA_MAX_BYTES)
      if (rawMetadata === null) return noBody()
      const metadata = parseMetadata(new TextDecoder().decode(rawMetadata))
      connId = metadata.connId
      switch (metadata.kind) {
        case 'streamResponse':
          return await this.handleStreamResponsePost(connId, reader, useNodeStream, request)
        case 'streamRequest':
          return await this.handleStreamRequestPost(connId, reader)
        case 'close':
          return this.handleClosePost(connId)
        case 'batch':
          return await this.handleBatchPost(connId, reader)
      }
    } catch (err) {
      // An oversize frame leaves no next frame boundary to resume from, so it ends the wire, not
      // just this POST.
      if (err instanceof OversizeFrameError && connId !== null) this.closeWireOf(connId)
      // A typed protocol-input fault is the client's: answer 400 and stay quiet. Anything else is our
      // bug — rethrow so the request pipeline (`runTelefunc`) logs it and masks it as a 500.
      if (isProtocolViolationError(err) || err instanceof OversizeFrameError || err instanceof StreamTruncatedError) {
        return badRequest()
      }
      throw err
    }
  }

  /** Stream-response POST: opens the SSE downstream and consumes its body asynchronously
   *  (`runStreamResponse`). Returns immediately so the response headers can flush. */
  private async handleStreamResponsePost(
    connId: string,
    reader: BodyReader,
    useNodeStream: boolean,
    request: Request,
  ): Promise<SseChannelHttpResponse> {
    const existing = this.mux.getWireByConnId<SseWire>(connId)
    if (existing) this.closeWire(existing, { permanent: false })

    const onCancel = () => {
      const wire = this.mux.getWireByConnId<SseWire>(connId)
      if (wire) this.closeWire(wire, { permanent: false })
    }
    const events = new EventStream({ useNodeStream, onCancel })

    const { promise: ready, resolve: resolveReady } = createDeferred()
    const wire: SseWire = {
      connId,
      events,
      closed: false,
      sessionId: null,
      ready,
      resolveReady,
      pendingDispatches: new Set(),
    }
    this.mux.onWireOpen(wire, this.transport, request)
    this.resolvePendingWires(connId, wire)
    void this.runStreamResponse(wire, reader)

    return {
      statusCode: 200,
      contentType: 'text/event-stream',
      headers: [
        ['Cache-Control', 'no-cache, no-transform'],
        ['X-Accel-Buffering', 'no'],
      ],
      body: events.stream.readable,
    }
  }

  /** Long-lived client→server upload POST. The body streams for the wire's lifetime, so every
   *  frame is dispatched fire-and-forget — awaiting one would stall the read loop behind it. */
  private async handleStreamRequestPost(connId: string, reader: BodyReader): Promise<SseChannelHttpResponse> {
    const wire = await this.resolveWire(connId)
    if (!wire) return badRequest()
    reader.onChunk = () => this.mux.onWireBytes(wire)
    // The open-ack is the client's duplex probe (ACK ⇒ its upload bytes reached the server) and must
    // not wait on reconcile settlement, or a slow attach would falsely demote a healthy duplex wire to
    // sticky batch. Dispatch safety is owned by `runStreamResponse` releasing `ready` only after
    // RECONCILED — the read loop below still waits on that gate, so early bytes sit unread until then.
    this.sendNow(wire, encode.streamRequestOpenAck())
    // No deadline: the client trusts this POST from the ack on; runStreamResponse's end or the close opens the gate.
    await wire.ready
    if (wire.closed) return badRequest()
    try {
      while (true) {
        const raw = await reader.readLengthPrefixedBytesOrNull(WIRE_MAX_RAW_FRAME_BYTES)
        if (!raw || wire.closed) break
        this.dispatchAndReport(wire, raw)
      }
    } finally {
      await this.settlePendingDispatches(wire)
    }
    return okResponse()
  }

  /** The page left for good: its wire closes as a WebSocket's does when its page leaves. One its page lost first has
   *  left its session's channels waiting for a reconnect, which end now. */
  private handleClosePost(connId: string): SseChannelHttpResponse {
    const wire = this.mux.getWireByConnId<SseWire>(connId)
    if (wire) {
      this.closeWire(wire, { permanent: true })
      return okResponse()
    }
    const sessionId = this.closedWires.get(connId)?.sessionId
    if (sessionId) this.mux.endSession(sessionId)
    return okResponse()
  }

  /** Short-lived outbox batch POST. A RECONCILE's RECONCILED goes as it is answered, ahead of what the frames behind it
   *  send: those are what the page carries with it, which it doesn't replay. Tracked in `pendingDispatches` so
   *  `runStreamResponse` won't send its own reconciled mid-batch. */
  private async handleBatchPost(connId: string, reader: BodyReader): Promise<SseChannelHttpResponse> {
    const wire = await this.resolveWire(connId)
    if (!wire) return badRequest()
    reader.onChunk = () => this.mux.onWireBytes(wire)
    if (!(await this.waitReady(wire))) return badRequest()
    const drain = this.drain(wire, reader, (outcome) => {
      if (shouldSendReconciled(outcome, wire)) this.mux.sendReconciled(outcome)
    })
    wire.pendingDispatches.add(drain)
    try {
      await drain
    } finally {
      wire.pendingDispatches.delete(drain)
    }
    return okResponse()
  }

  /** Stream-response POST lifecycle: consume the initial reconcile batch; once its RECONCILE is answered, drain the
   *  batch POSTs in flight so their `_lastClientSeq` mutations land first and emit `reconciled`, ahead of what the
   *  frames carried behind it send; then release the `ready` gate the other POSTs are parked on. */
  private async runStreamResponse(wire: SseWire, reader: BodyReader): Promise<void> {
    try {
      await this.drain(wire, reader, async (outcome) => {
        if (!shouldSendReconciled(outcome, wire)) return
        await this.settlePendingDispatches(wire)
        this.mux.sendReconciled(outcome)
      })
    } catch (err) {
      // The body ended mid-frame; this promise is fire-and-forget, so a rethrow would be unhandled.
      // A truncation is the client hanging up (channels keep their grace); an oversize frame leaves
      // no next frame boundary, so that wire is finished.
      reportDispatchBug(err)
      this.closeWire(wire, { permanent: err instanceof OversizeFrameError })
    } finally {
      // Every path releases the gate here — the parked POSTs then see whatever state we left.
      wire.resolveReady()
    }
  }

  /** Registered so a reconcile waits for it, and reported here because nothing else will — awaited
   *  dispatches (the batch POST's) report through their caller. */
  private dispatchAndReport(wire: SseWire, raw: Uint8Array<ArrayBuffer>): void {
    const dispatch = this.mux.onWireMessage(wire, raw)
    wire.pendingDispatches.add(dispatch)
    const evict = () => wire.pendingDispatches.delete(dispatch)
    dispatch.then(evict, (err) => {
      evict()
      reportDispatchBug(err)
    })
  }

  /** Waits without consuming failures — every dispatch is reported by whoever started it. */
  private settlePendingDispatches(wire: SseWire): Promise<unknown> {
    return Promise.allSettled([...wire.pendingDispatches])
  }

  /** Reads `reader`'s frames and dispatches each with `dispatchWireMessage`; a RECONCILE's outcome goes to
   *  `onReconciled` before the next frame is read. */
  private async drain(
    wire: SseWire,
    reader: BodyReader,
    onReconciled: (outcome: ReconcileOutcome) => void | Promise<void>,
  ): Promise<void> {
    while (true) {
      const raw = await reader.readLengthPrefixedBytesOrNull(WIRE_MAX_RAW_FRAME_BYTES)
      if (!raw || wire.closed) break
      const outcome = await this.mux.dispatchWireMessage(wire, raw)
      if (outcome !== null) await onReconciled(outcome)
    }
  }

  private async resolveWire(connId: string): Promise<SseWire | null> {
    const wire = this.mux.getWireByConnId<SseWire>(connId)
    if (wire) return wire
    if (this.closedWires.has(connId)) return null
    return await this.waitForWire(connId)
  }

  private rememberClosed({ connId, sessionId }: SseWire): void {
    const now = performance.now()
    for (const [closed, { at }] of this.closedWires) {
      if (now - at < CHANNEL_CONNECT_TTL_MS) break
      this.closedWires.delete(closed)
    }
    this.closedWires.set(connId, { at: now, sessionId })
  }

  /** As `RegistrationWaits.stop`: the timeout path must remove the
   *  waiter and (when last) the map entry, or abandoned data POSTs leak an entry forever. */
  private waitForWire(connId: string): Promise<SseWire | null> {
    return new Promise<SseWire | null>((resolve) => {
      let settled = false
      let timer: ReturnType<typeof setTimeout>
      const pending = this.pendingWires.get(connId) ?? new Set()
      this.pendingWires.set(connId, pending)

      const settle = (wire: SseWire | null): void => {
        if (settled) return
        settled = true
        pending.delete(waiter)
        // Identity-equality guards against deleting a replacement set registered after
        // `resolvePendingWires` already consumed ours.
        if (pending.size === 0 && this.pendingWires.get(connId) === pending) {
          this.pendingWires.delete(connId)
        }
        clearTimeout(timer)
        resolve(wire)
      }
      const waiter = (wire: SseWire | null): void => settle(wire)
      pending.add(waiter)
      timer = setTimeout(() => settle(null), CHANNEL_CONNECT_TTL_MS)
    })
  }

  private resolvePendingWires(connId: string, wire: SseWire): void {
    const pending = this.pendingWires.get(connId)
    if (!pending) return
    this.pendingWires.delete(connId)
    for (const resolve of pending) resolve(wire)
  }

  private closeWireOf(connId: string): void {
    const wire = this.mux.getWireByConnId<SseWire>(connId)
    if (wire) this.closeWire(wire, { permanent: true })
  }

  private sendNow(wire: SseWire, frame: Uint8Array<ArrayBuffer>): void {
    if (!wire.closed) wire.events.send(frame)
  }

  /** Resolves false on timeout — caller drops the POST. */
  private waitReady(wire: SseWire): Promise<boolean> {
    return new Promise<boolean>((resolve) => {
      const timer = unrefTimer(setTimeout(() => resolve(false), CHANNEL_CONNECT_TTL_MS))
      wire.ready.then(() => {
        clearTimeout(timer)
        resolve(true)
      })
    })
  }

  private closeWire(wire: SseWire, { permanent }: { permanent: boolean }): void {
    if (wire.closed) return
    wire.closed = true
    this.rememberClosed(wire)
    // Unblock any data POST awaiting `ready` — its dispatch sees the closed wire and bails.
    wire.resolveReady()
    this.mux.onWireClosed(wire, { permanent })
    wire.events.close({ permanent })
  }

  private terminateWire(wire: SseWire): void {
    this.closeWire(wire, { permanent: this.mux.readPermanentTermination(wire) })
  }
}

/** Malformed metadata is untrusted client ingress, not a truncation — same class as the decode seam. */
function parseMetadata(raw: string): SseRequestMetadata {
  let metadata: unknown
  try {
    metadata = JSON.parse(raw)
  } catch {
    throw new ProtocolViolationError('malformed SSE request metadata')
  }
  return parseSseRequestMetadata(metadata)
}

function reportDispatchBug(err: unknown): void {
  if (isProtocolViolationError(err) || err instanceof OversizeFrameError || err instanceof StreamTruncatedError) return
  handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
}

/** A closed SSE wire has nothing to say — except on a barrier commit, where the RECONCILED is
 *  bound for the WS and this wire's own retirement is exactly what the upgrade just did. */
function shouldSendReconciled(outcome: ReconcileOutcome, wire: SseWire): boolean {
  return outcome.deliverTo !== wire || !wire.closed
}

function badRequest(message = ''): SseChannelHttpResponse {
  return { statusCode: 400, contentType: 'text/plain', headers: [], body: message }
}

function noBody(): SseChannelHttpResponse {
  return badRequest('The request has no body: an SSE channel request starts with its metadata.')
}

/** A page on another version reloads: none of its frames could be read. */
function wireVersionRefused(): SseChannelHttpResponse {
  return { statusCode: STATUS_CODE_WIRE_VERSION, contentType: 'text/plain', headers: [], body: 'reload the page' }
}

function okResponse(): SseChannelHttpResponse {
  return { statusCode: 200, contentType: 'text/plain', headers: [], body: '' }
}

async function handleSseChannelRequest(request: Request, readable?: Readable): Promise<SseChannelHttpResponse> {
  globalObject.sseTransport ??= new SseServerTransport()
  return globalObject.sseTransport.handleRequest(request, readable)
}
