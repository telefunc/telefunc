export { handleSseChannelRequest }
export type { SseChannelHttpResponse }

import type { Readable } from 'node:stream'
import { Fifo } from '../../../utils/Fifo.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'
import { handleTelefunctionBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import {
  CHANNEL_TRANSPORT,
  SSE_EVENT_SLICE_BYTES,
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
import type { Fault, ReconcileOutcome, ServerTransport, WireClose } from './ChannelMux.js'
import { channelOf, encode } from '../../wire/frame.js'
import { HEADER } from '../../wire/header.js'
import { TAG, passesData } from '../../wire/tags.js'
import {
  countWaitingPublishes,
  droppingFrom,
  droppingLossy,
  droppingPublishes,
  overtakes,
  type WireQueue,
} from '../../flowControl/server/waitingPublishes.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import { isProtocolViolationError, ProtocolViolationError } from '../../wire/protocolViolation.js'
import {
  WIRE_VERSION_REFUSAL,
  refusalContentType,
  wireEnd,
  type Refusal,
  type RefusalContentType,
} from '../../refusal.js'

type SseChannelHttpResponse = {
  statusCode: 200 | 400 | 409
  contentType: 'text/plain' | 'text/event-stream' | RefusalContentType
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
  /** Resolved as the wire closes: a POST whose body the page keeps open is answered then, not at its next frame. */
  ended: Promise<void>
  resolveEnded: () => void
  /** Dispatches in flight for this wire. A reconcile waits on these before reporting a seq,
   *  so a batch POST's `_lastClientSeq` mutations can't land after the RECONCILED that reports them. */
  pendingDispatches: Set<Promise<unknown>>
  /** What the POSTs that meet the wire closed are answered, where the server decided its end. */
  refused: Refusal | null
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
  /** The frame whose event is going to the stream, and how far it went. */
  private current: { frame: Uint8Array<ArrayBuffer>; offset: number } | null = null
  /** The newest event started in order where it is a publish's head, whose body comes next in order: a body at the
   *  front of `waiting` names no channel, only its head does. */
  private headStarted: Uint8Array<ArrayBuffer> | null = null
  /** Whether the stream asks for more. */
  private takes: boolean

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
    return this.stream.bufferedAmount + (current === null ? 0 : encodedBytes(current.frame.byteLength - current.offset))
  }

  /** Data waits for the stream to ask for it, as the frame the replay also holds: it's encoded a slice at a time. */
  send(frame: Uint8Array<ArrayBuffer>): void {
    const atOnce =
      passesData(frame[0]!) || (frame.byteLength <= WIRE_GATHER_BYTES && overtakes(frame, this.framesAhead()))
    if (atOnce && this.current !== null) {
      this.passing.push(frame)
      this.waitingBytes += eventBytes(frame)
      return
    }
    if (atOnce) {
      this.startEvent(frame)
      return
    }
    if (this.takes && this.current === null && this.waiting.length === 0) {
      this.startInOrder(frame)
      return
    }
    this.waiting.push(frame)
    this.waitingBytes += eventBytes(frame)
  }

  private *framesAhead(): Generator<Uint8Array> {
    if (this.headStarted !== null) yield this.headStarted
    yield* this.waiting
  }

  waitingPublishes(ix: number): number {
    return countWaitingPublishes(this.waiting, ix)
  }

  dropWaitingPublishes(ix: number, keep: number): void {
    this.dropWaiting(droppingPublishes(ix, keep))
  }

  dropWaitingFrom(ix: number, from: 'publish' | 'start'): void {
    this.dropWaiting(droppingFrom(ix, from))
  }

  /** The stream is given an event as it starts: what passes and what waits, it has no byte of. */
  dropUngiven(ix: number): number {
    const cut = droppingLossy(ix)
    this.dropWaiting(cut.drops)
    return cut.count()
  }

  /** `drops` is asked in the order the stream takes the frames: what passes goes first. */
  private dropWaiting(drops: (frame: Uint8Array) => boolean): void {
    const goes = (frame: Uint8Array<ArrayBuffer>) => {
      if (!drops(frame)) return false
      this.waitingBytes -= eventBytes(frame)
      return true
    }
    this.passing.remove(goes)
    this.waiting.remove(goes)
  }

  /** What waits goes no more: past a wire only lost, the page's reconnect gets it from its channels' replay, and it could
   *  wait here for as long as the socket stays open taking nothing. An end the server decided goes as the `last` event,
   *  after the event under way, which nothing passes once it started: what waits ended with the wire. */
  close(last: Uint8Array<ArrayBuffer> | null): void {
    if (last !== null) {
      while (this.current !== null) this.pushChunks()
      this.stream.push(encodeChunk(last, 0, last.byteLength))
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
    if (this.current !== null) this.pushChunks()
    while (this.takes && this.waiting.length > 0) {
      const frame = this.waiting.shift()!
      this.waitingBytes -= eventBytes(frame)
      this.startInOrder(frame)
    }
  }

  /** Starts the event of `frame`, next in the order data keeps. A head names its channel only until its body's event
   *  starts: nothing passes an event once it started. */
  private startInOrder(frame: Uint8Array<ArrayBuffer>): void {
    this.headStarted = frame[0] === TAG.PUBLISH_TO ? frame : null
    this.startEvent(frame)
  }

  private startEvent(frame: Uint8Array<ArrayBuffer>): void {
    this.current = { frame, offset: 0 }
    this.pushChunks()
  }

  /** Pushes the current event's next chunks, at least one, as far as the stream asks. What passes goes as the event
   *  ends. */
  private pushChunks(): void {
    const current = this.current!
    const { frame } = current
    do {
      const start = current.offset
      const end = Math.min(frame.byteLength, start + SSE_EVENT_SLICE_BYTES)
      this.takes = this.stream.push(encodeChunk(frame, start, end))
      current.offset = end
    } while (this.takes && current.offset < frame.byteLength)
    if (current.offset < frame.byteLength) return
    this.current = null
    while (this.passing.length > 0) {
      const passing = this.passing.shift()!
      this.waitingBytes -= eventBytes(passing)
      this.takes = this.stream.push(encodeChunk(passing, 0, passing.byteLength))
    }
  }
}

/** The part of `frame`'s event that holds its bytes from `start` to `end`, the first and last parts with the event's
 *  ends. */
function encodeChunk(frame: Uint8Array<ArrayBuffer>, start: number, end: number): Uint8Array<ArrayBuffer> {
  const base64 = uint8ArrayToBase64url(frame.subarray(start, end))
  return textEncoder.encode(`${start === 0 ? 'data: ' : ''}${base64}${end === frame.byteLength ? '\n\n' : ''}`)
}

/** What `bytes` of a frame take as base64url, unpadded. */
function encodedBytes(bytes: number): number {
  return Math.ceil((bytes * 4) / 3)
}

/** What `frame`'s event takes: its base64url and the 8 bytes around it. */
function eventBytes(frame: Uint8Array<ArrayBuffer>): number {
  return encodedBytes(frame.byteLength) + 8
}

const globalObject = getGlobalObject('wire-protocol/channel/server/sse.ts', {
  sseTransport: null as SseServerTransport | null,
})

class SseServerTransport {
  /** Resolvers for data POSTs that arrived before the stream-response POST registered the
   *  wire — covers the same-instance race where the long-lived stream-request POST
   *  lands before the stream-response POST. */
  private readonly pendingWires = new Map<string, Set<(wire: SseWire | null) => void>>()
  /** When each wire closed, the session it had, and what the server decided its end to be answered with, for
   *  `connectTtl`: a page never reuses a connId, so a POST for one is refused, as a POST that met its close is. */
  private readonly closedWires = new Map<string, { at: number; sessionId: string | null; refused: Refusal | null }>()
  private readonly mux = getChannelMux()
  private readonly transport: ServerTransport<SseWire> = {
    getSessionId: (wire) => wire.sessionId ?? undefined,
    setSessionId: (wire, sessionId) => {
      wire.sessionId = sessionId
    },
    getConnId: (wire) => wire.connId,
    sendNow: (wire, frame) => this.sendNow(wire, frame),
    type: CHANNEL_TRANSPORT.SSE,
    wholeMessages: false,
    // Events carry frames in base64 plus framing, so three quarters of what waits is at least the frames it carries.
    bufferedAmount: (wire) => Math.floor((wire.events.heldBytes * 3) / 4),
    // A frame that goes at once passes the data waiting for the stream to ask: it waits behind the stream's alone.
    queuedAhead: (wire) => Math.floor((wire.events.streamBytes * 3) / 4),
    queueOf: (wire) => wire.events,
    close: (wire, { how, refusal }) => this.closeWire(wire, how, refusal),
  }

  /** `clientLeft` says whether a failed read of the upload is its client leaving, as the server that received it does,
   *  or null where it can't say. */
  async handleRequest(
    request: Request,
    readable: Readable | undefined,
    clientLeft: (err: unknown) => boolean | null,
  ): Promise<SseChannelHttpResponse> {
    if (!getServerConfig().channel.transports.includes(CHANNEL_TRANSPORT.SSE)) return refuse(REFUSED.notServed)
    if (request.method !== 'POST') return refuse(REFUSED.notPost)
    if (!namesWireVersion(request)) return refuse(WIRE_VERSION_REFUSAL, STATUS_CODE_WIRE_VERSION)
    const source = readable ?? request.body
    // A Web `Request` sent without a body has `body === null`; a Node one, an empty stream (below).
    if (!source) return refuse(REFUSED.noBody)
    // Adapter mode: a Node `IncomingMessage` means we're on the Node-native serve path
    // and should answer with a Node `Readable` so it pipes straight to the socket. Web
    // adapters get `request.body` (no `readable`) and want a `ReadableStream` back.
    const useNodeStream = readable !== undefined
    const reader = new BodyReader(source, clientLeft)
    let metadata: SseRequestMetadata
    try {
      const rawMetadata = await reader.readLengthPrefixedBytesOrNull(SSE_METADATA_MAX_BYTES)
      if (rawMetadata === null) return refuse(REFUSED.noBody)
      metadata = parseMetadata(new TextDecoder().decode(rawMetadata))
    } catch (err) {
      if (err instanceof StreamTruncatedError) return refuse(REFUSED.truncated)
      if (isProtocolViolationError(err) || err instanceof OversizeFrameError) return refuse(REFUSED.metadata)
      throw err
    }
    const { connId } = metadata
    try {
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
      // A body that ends mid-frame or a frame over the limit is the client's: refused, quietly. Anything else is our bug
      // — rethrow so the request pipeline (`runTelefunc`) logs it and masks it as a 500.
      if (err instanceof StreamTruncatedError) return refuse(REFUSED.truncated)
      const fault = faultOf(err)
      if (fault === null) throw err
      const wire = this.mux.getWireByConnId<SseWire>(connId)
      if (!wire) return refuse(this.answerFor(connId))
      // The mux decides what the fault ends; the request is answered as any request that meets that end is.
      this.mux.endFault(wire, fault)
      return closedUnder(wire)
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
    if (existing) this.closeWire(existing, 'transient')

    const onCancel = () => {
      const wire = this.mux.getWireByConnId<SseWire>(connId)
      if (wire) this.closeWire(wire, 'transient')
    }
    const events = new EventStream({ useNodeStream, onCancel })

    const { promise: ready, resolve: resolveReady } = createDeferred()
    const { promise: ended, resolve: resolveEnded } = createDeferred()
    const wire: SseWire = {
      connId,
      events,
      closed: false,
      sessionId: null,
      ready,
      resolveReady,
      ended,
      resolveEnded,
      pendingDispatches: new Set(),
      refused: null,
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
    if (!wire) return refuse(this.answerFor(connId))
    reader.onChunk = () => this.mux.onWireBytes(wire)
    // The open-ack is the client's duplex probe (ACK ⇒ its upload bytes reached the server) and must
    // not wait on reconcile settlement, or a slow attach would falsely demote a healthy duplex wire to
    // sticky batch. Dispatch safety is owned by `runStreamResponse` releasing `ready` only after
    // RECONCILED — the read loop below still waits on that gate, so early bytes sit unread until then.
    this.sendNow(wire, encode.streamRequestOpenAck())
    // No deadline: the client trusts this POST from the ack on; runStreamResponse's end or the close opens the gate.
    await wire.ready
    if (wire.closed) return closedUnder(wire)
    await untilReadOrEnded(wire, this.readUpload(wire, reader))
    // Its wire closed under it, with a frame it read left unrun: refused, as a batch POST that met the close is.
    return wire.closed ? closedUnder(wire) : okResponse()
  }

  /** Dispatches the upload's frames as they come, to the body's end or the first frame after the wire's. */
  private async readUpload(wire: SseWire, reader: BodyReader): Promise<void> {
    try {
      while (true) {
        const raw = await reader.readLengthPrefixedBytesOrNull(WIRE_MAX_RAW_FRAME_BYTES, HEADER)
        if (!raw || wire.closed) break
        this.dispatchAndReport(wire, raw)
      }
    } finally {
      await this.settlePendingDispatches(wire)
    }
  }

  /** The page left for good: its wire closes as a WebSocket's does when its page leaves. One its page lost first has
   *  left its session's channels waiting for a reconnect, which end now. */
  private handleClosePost(connId: string): SseChannelHttpResponse {
    const wire = this.mux.getWireByConnId<SseWire>(connId)
    if (wire) {
      this.closeWire(wire, 'pageLeft')
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
    if (!wire) return refuse(this.answerFor(connId))
    reader.onChunk = () => this.mux.onWireBytes(wire)
    if (!(await this.waitReady(wire))) return refuse(REFUSED.notReady)
    const drain = this.drain(wire, reader, (outcome) => {
      if (shouldSendReconciled(outcome, wire)) this.mux.sendReconciled(outcome)
    })
    wire.pendingDispatches.add(drain)
    try {
      await untilReadOrEnded(wire, drain)
    } finally {
      wire.pendingDispatches.delete(drain)
    }
    // Frames after a close don't run: refused, as a POST for a wire closed before it is, so the page reconnects.
    return wire.closed ? closedUnder(wire) : okResponse()
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
      // A truncation is the client hanging up (channels keep their grace); an over-limit frame is refused as on any
      // other request, its channel told on this stream or at its next attach.
      reportDispatchBug(err)
      const fault = faultOf(err)
      if (fault !== null) this.mux.endFault(wire, fault)
      else this.closeWire(wire, 'transient')
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
      const raw = await reader.readLengthPrefixedBytesOrNull(WIRE_MAX_RAW_FRAME_BYTES, HEADER)
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

  private rememberClosed({ connId, sessionId, refused }: SseWire): void {
    const now = performance.now()
    for (const [closed, { at }] of this.closedWires) {
      if (now - at < this.mux.connectTtl) break
      this.closedWires.delete(closed)
    }
    this.closedWires.set(connId, { at: now, sessionId, refused })
  }

  /** What a request for `connId`, whose wire the server no longer has, is answered: the end the server decided for it,
   *  else a loss. */
  private answerFor(connId: string): Refusal {
    return this.closedWires.get(connId)?.refused ?? REFUSED.noWire
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
      timer = setTimeout(() => settle(null), this.mux.connectTtl)
    })
  }

  private resolvePendingWires(connId: string, wire: SseWire): void {
    const pending = this.pendingWires.get(connId)
    if (!pending) return
    this.pendingWires.delete(connId)
    for (const resolve of pending) resolve(wire)
  }

  private sendNow(wire: SseWire, frame: Uint8Array<ArrayBuffer>): void {
    if (!wire.closed) wire.events.send(frame)
  }

  /** Resolves false on timeout — caller drops the POST. */
  private waitReady(wire: SseWire): Promise<boolean> {
    return new Promise<boolean>((resolve) => {
      const timer = unrefTimer(setTimeout(() => resolve(false), this.mux.connectTtl))
      wire.ready.then(() => {
        clearTimeout(timer)
        resolve(true)
      })
    })
  }

  /** `refused`: what the POSTs that meet the close are answered, where the server decided it and told the channels it
   *  ends: then the stream sends what it holds before it ends. */
  private closeWire(wire: SseWire, how: WireClose, refused: Refusal | null = null): void {
    if (wire.closed) return
    wire.closed = true
    wire.refused = refused
    this.rememberClosed(wire)
    // Unblock any data POST awaiting `ready` — its dispatch sees the closed wire and bails.
    wire.resolveReady()
    wire.resolveEnded()
    this.mux.onWireClosed(wire, how)
    wire.events.close(refused === null ? null : encode.end(wireEnd(refused)))
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

/** The fault a failed read of a request's frames is: an over-limit frame is read no further than its header, so the
 *  request's frames after it go unread, and its channel, named by that header, ends. Null for any other failure: a
 *  frame the server can't take is the mux's to decide, so what else fails is the server's bug. */
function faultOf(err: unknown): Fault | null {
  return err instanceof OversizeFrameError
    ? { scope: 'channel', ix: channelOf(err.head), cause: 'frameOverLimit', wire: 'loss' }
    : null
}

/** Waits for a POST's `read` of its body, or for its wire's end where that comes first: a body the page keeps open and
 *  idle would park the answer until the page writes again, and the read then runs nothing more. A read that fails after
 *  the answer is reported as any dispatch is. */
async function untilReadOrEnded(wire: SseWire, read: Promise<void>): Promise<void> {
  await Promise.race([read, wire.ended])
  if (wire.closed) void read.catch(reportDispatchBug)
}

function reportDispatchBug(err: unknown): void {
  if (err instanceof OversizeFrameError || err instanceof StreamTruncatedError) return
  handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
}

/** A closed SSE wire has nothing to say — except on a barrier commit, where the RECONCILED is
 *  bound for the WS and this wire's own retirement is exactly what the upgrade just did. */
function shouldSendReconciled(outcome: ReconcileOutcome, wire: SseWire): boolean {
  return outcome.deliverTo !== wire || !wire.closed
}

/** Each refusal of the page's requests, by what the page does about it (`RefusalKind`). */
const REFUSED = {
  notServed: { kind: 'route', reason: "The server doesn't serve SSE channel requests." },
  notPost: { kind: 'route', reason: 'An SSE channel request is a POST.' },
  noBody: { kind: 'route', reason: 'The request has no body: an SSE channel request starts with its metadata.' },
  metadata: { kind: 'route', reason: "The server can't read the request's metadata." },
  noWire: { kind: 'retry', reason: 'The server no longer has the connection this request is for.' },
  notReady: { kind: 'retry', reason: "The connection this request is for didn't open in time." },
  closed: { kind: 'retry', reason: 'The connection this request is for closed under it.' },
  truncated: { kind: 'retry', reason: 'The request ended in the middle of a frame.' },
} as const satisfies Record<string, Refusal>

/** Telefunc's own refusal, which says so in its media type (`refusal.ts`); the body repeats its reason for a person. */
function refuse(refusal: Refusal, statusCode: 400 | 409 = 400): SseChannelHttpResponse {
  return { statusCode, contentType: refusalContentType(refusal), headers: [], body: refusal.reason }
}

/** A POST that met its wire closed: for a frame of the page's, that frame's refusal; else a loss. */
function closedUnder(wire: SseWire): SseChannelHttpResponse {
  return refuse(wire.refused ?? REFUSED.closed)
}

function okResponse(): SseChannelHttpResponse {
  return { statusCode: 200, contentType: 'text/plain', headers: [], body: '' }
}

async function handleSseChannelRequest(
  request: Request,
  readable: Readable | undefined,
  clientLeft: (err: unknown) => boolean | null,
): Promise<SseChannelHttpResponse> {
  globalObject.sseTransport ??= new SseServerTransport()
  return globalObject.sseTransport.handleRequest(request, readable, clientLeft)
}
