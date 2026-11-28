export { handleSseChannelRequest }
export type { SseChannelHttpResponse }

import type { Readable } from 'node:stream'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'
import { handleTelefunctionBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import {
  CHANNEL_TRANSPORT,
  SSE_METADATA_MAX_BYTES,
  STATUS_CODE_WIRE_VERSION,
  WIRE_MAX_RAW_FRAME_BYTES,
} from '../../constants.js'
import { namesWireVersion } from '../../wire/requestKind.js'
import { parseSseRequestMetadata, textDecoder, type SseRequestMetadata } from '../../wire/envelope.js'
import { OversizeFrameError, BodyReader, StreamTruncatedError } from '../../wire/BodyReader.js'
import { getChannelMux } from './ChannelMux.js'
import { EventStream } from './EventStream.js'
import type { Fault, ReconcileOutcome, ServerTransport, SessionHold, WireClose } from './ChannelMux.js'
import { channelOf, encode } from '../../wire/frame.js'
import { HEADER } from '../../wire/header.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import { ProtocolViolationError } from '../../wire/protocolViolation.js'
import {
  WIRE_VERSION_REFUSAL,
  refusalContentType,
  wireEnd,
  type Refusal,
  type RefusalContentType,
} from '../../refusal.js'
import { pingDeadline } from '../../wire/pingDeadline.js'

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
  /** Resolved by `runStreamResponse` once the stream-response POST's body is consumed. Data
   *  POSTs gate on this before dispatching so they can't race ahead of the reconcile. */
  ready: Promise<void>
  resolveReady: () => void
  /** Dispatches in flight for this wire. A reconcile waits on these before reporting a seq,
   *  so a batch POST's `_lastClientSeq` mutations can't land after the RECONCILED that reports them. */
  pendingDispatches: Set<Promise<unknown>>
  /** The bodies of the wire's requests the server reads: once the wire ended, each is read off for one ping deadline at
   *  most, then let go of (`endStream`). */
  bodies: Set<BodyReader>
}

const globalObject = getGlobalObject('wire-protocol/channel/server/sse.ts', {
  sseTransport: null as SseServerTransport | null,
})

class SseServerTransport {
  /** Resolvers for data POSTs that arrived before the stream-response POST registered the
   *  wire — covers the same-instance race where the long-lived stream-request POST
   *  lands before the stream-response POST. */
  private readonly pendingWires = new Map<string, Set<(wire: SseWire | null) => void>>()
  /** Each closed wire's session, for `connectTtl` from its close, for the page's close POST: a page never reuses a
   *  connId, so meanwhile a POST for one is refused `retry` at once. Each record is its own, so that the end of an older
   *  wire's time removes no newer wire's of the same connId. */
  private readonly closedWires = new Map<string, { hold: SessionHold | null }>()
  private readonly mux = getChannelMux()
  private readonly transport: ServerTransport<SseWire> = {
    getConnId: (wire) => wire.connId,
    sendNow: (wire, frame) => this.sendNow(wire, frame),
    type: CHANNEL_TRANSPORT.SSE,
    wholeMessages: false,
    // Events carry frames in base64 plus framing, so three quarters of what waits is at least the frames it carries.
    bufferedAmount: (wire) => Math.floor((wire.events.heldBytes * 3) / 4),
    // A frame that goes at once passes the data waiting for the stream to ask: it waits behind the stream's alone.
    queuedAhead: (wire) => Math.floor((wire.events.streamBytes * 3) / 4),
    queueOf: (wire) => wire.events,
    close: (wire, { refusal }, held) => this.endStream(wire, refusal, held),
  }

  async handleRequest(request: Request, readable: Readable | undefined): Promise<SseChannelHttpResponse> {
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
    const reader = new BodyReader(source)
    let metadata: SseRequestMetadata
    try {
      const rawMetadata = await reader.readFrame(SSE_METADATA_MAX_BYTES)
      if (rawMetadata === null) return refuse(REFUSED.noBody)
      metadata = parseMetadata(textDecoder.decode(rawMetadata))
    } catch (err) {
      if (err instanceof StreamTruncatedError) return refuse(REFUSED.truncated)
      if (err instanceof ProtocolViolationError || err instanceof OversizeFrameError) return refuse(REFUSED.metadata)
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
      // A body that ends mid-frame is the client's: refused, quietly. Anything else is our bug — rethrow so the request
      // pipeline (`runTelefunc`) logs it and masks it as a 500.
      if (err instanceof StreamTruncatedError) return refuse(REFUSED.truncated)
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
    if (existing) this.mux.endFault(existing, { scope: 'loss' })

    // The stream is this wire's alone: its cancel closes this wire, not the one its connId names by then.
    const events = new EventStream({ useNodeStream, onCancel: () => this.onWireGone(wire, 'transient') })

    const { promise: ready, resolve: resolveReady } = createDeferred()
    const wire: SseWire = {
      connId,
      events,
      closed: false,
      ready,
      resolveReady,
      pendingDispatches: new Set(),
      bodies: new Set([reader]),
    }
    this.mux.onWireOpen(wire, this.transport, request)
    reader.onBytes = () => this.mux.onWireBytes(wire)
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
    if (!wire) return refuse(REFUSED.noWire)
    reader.onBytes = () => this.mux.onWireBytes(wire)
    // The open-ack is the client's duplex probe (ACK ⇒ its upload bytes reached the server) and must
    // not wait on reconcile settlement, or a slow attach would falsely demote a healthy duplex wire to
    // sticky batch. Dispatch safety is owned by `runStreamResponse` releasing `ready` only after
    // RECONCILED — the read loop below still waits on that gate, so early bytes sit unread until then.
    this.sendNow(wire, encode.streamRequestOpenAck())
    // No deadline: the client trusts this POST from the ack on; runStreamResponse's end or the close opens the gate.
    await wire.ready
    return this.answerAfterBody(wire, reader, this.readUpload(wire, reader))
  }

  /** Dispatches the upload's frames as they come, to the body's end; after its wire's end, what the page writes is read
   *  off and not run, until the server lets go of the body (`endStream`). */
  private async readUpload(wire: SseWire, reader: BodyReader): Promise<void> {
    try {
      while (true) {
        if (wire.closed) return await reader.discardRest('server')
        const raw = await this.nextFrame(wire, reader)
        if (!raw) return
        if (!wire.closed) this.dispatchAndReport(wire, raw)
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
      this.onWireGone(wire, 'pageLeft')
      return okResponse()
    }
    const hold = this.closedWires.get(connId)?.hold
    if (hold) this.mux.endSession(hold)
    return okResponse()
  }

  /** Short-lived outbox batch POST. A RECONCILE's RECONCILED goes as it is answered, ahead of what the frames behind it
   *  send: those are what the page carries with it, which it doesn't replay. Tracked in `pendingDispatches` so
   *  `runStreamResponse` won't send its own reconciled mid-batch. */
  private async handleBatchPost(connId: string, reader: BodyReader): Promise<SseChannelHttpResponse> {
    const wire = await this.resolveWire(connId)
    if (!wire) return refuse(REFUSED.noWire)
    reader.onBytes = () => this.mux.onWireBytes(wire)
    if (!(await this.waitReady(wire))) return refuse(REFUSED.notReady)
    const drain = this.drain(wire, reader, (outcome) => {
      if (shouldSendReconciled(outcome, wire)) this.mux.sendReconciled(outcome)
    })
    wire.pendingDispatches.add(drain)
    try {
      return await this.answerAfterBody(wire, reader, drain)
    } finally {
      wire.pendingDispatches.delete(drain)
    }
  }

  /** What a request whose frames `read` reads from `reader` on `wire` is answered once the page ended its body, or the
   *  server let go of it one ping deadline after the wire's end (`endStream`): `retry` where its wire ended meanwhile,
   *  so the page reconnects, and its reopen reads the end the server kept; also where the body was cut in the middle of
   *  a frame the server was reading as the wire ended. A cut on a wire still open is `handleRequest`'s, the same
   *  `retry`. A frame over the limit is `wire`'s fault, whatever wire its connId names by then: the mux decides what it
   *  ends, and the rest of the body is read off, as after any end. */
  private async answerAfterBody(
    wire: SseWire,
    reader: BodyReader,
    read: Promise<void>,
  ): Promise<SseChannelHttpResponse> {
    wire.bodies.add(reader)
    try {
      await read
    } catch (err) {
      const fault = faultOf(err)
      if (fault !== null) {
        this.mux.endFault(wire, fault)
        await reader.discardRest('server')
      } else if (!(err instanceof StreamTruncatedError) || !wire.closed) throw err
    } finally {
      wire.bodies.delete(reader)
    }
    return wire.closed ? refuse(REFUSED.noWire) : okResponse()
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
      // A truncation is the client hanging up (channels keep their grace); an over-limit frame is the wire's fault, as
      // on any other request.
      reportDispatchBug(err)
      const fault = faultOf(err)
      if (fault !== null) this.mux.endFault(wire, fault)
      else this.onWireGone(wire, 'transient')
    } finally {
      wire.bodies.delete(reader)
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
   *  `onReconciled` before the next frame is read. After the wire's end, what the page writes is read off and not run. */
  private async drain(
    wire: SseWire,
    reader: BodyReader,
    onReconciled: (outcome: ReconcileOutcome) => void | Promise<void>,
  ): Promise<void> {
    while (true) {
      if (wire.closed) return reader.discardRest('server')
      const raw = await this.nextFrame(wire, reader)
      if (!raw) return
      if (wire.closed) continue
      const outcome = await this.mux.dispatchWireMessage(wire, raw)
      if (outcome !== null) await onReconciled(outcome)
    }
  }

  /** The next frame of `reader`, or null at the body's end. A frame the wire's end cut (`endStream`) is read off with the
   *  rest of the body. */
  private async nextFrame(wire: SseWire, reader: BodyReader): Promise<Uint8Array<ArrayBuffer> | null> {
    try {
      return await reader.readFrame(WIRE_MAX_RAW_FRAME_BYTES, HEADER)
    } catch (err) {
      if (!(err instanceof StreamTruncatedError) || !wire.closed) throw err
      await reader.discardRest('server')
      return null
    }
  }

  private async resolveWire(connId: string): Promise<SseWire | null> {
    const wire = this.mux.getWireByConnId<SseWire>(connId)
    if (wire) return wire
    if (this.closedWires.has(connId)) return null
    return await this.waitForWire(connId)
  }

  private rememberClosed({ connId }: SseWire, hold: SessionHold | null): void {
    const record = { hold }
    this.closedWires.set(connId, record)
    unrefTimer(
      setTimeout(() => {
        if (this.closedWires.get(connId) === record) this.closedWires.delete(connId)
      }, this.mux.connectTtl),
    )
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

  /** The page's side of the wire went (its stream cancelled, its close POST, its body cut): the mux forgets the wire,
   *  then its stream ends, also where the mux throws, as from the close of a WebSocket the page was moving to. */
  private onWireGone(wire: SseWire, how: WireClose): void {
    if (wire.closed) return
    let held: SessionHold | null = null
    try {
      held = this.mux.onWireClosed(wire, how) ?? null
    } finally {
      this.endStream(wire, null, held)
    }
  }

  /** The wire's stream ends, and the server keeps the session it held for the page's close POST (`held`). `refused`:
   *  the end the server decided and told the channels, which the stream sends as its last event after what it holds.
   *  The bodies the server reads for the wire are read off for one ping deadline at most, the frame each was reading
   *  let go of at once: a page that lives ends each by then, as it reads the end or takes the wire for lost. Past it, the
   *  server lets go of each and reads it no further, which answers each data POST `retry` (the event stream's own POST
   *  was answered at its open), and of what its event stream holds that the page didn't take (`EventStream.close`): the
   *  host ends the connection in its own time. It cancels none, since cancelling a body can close the connection under
   *  the answer, and reads none on, since that never ends for a sender that writes on. */
  private endStream(wire: SseWire, refused: Refusal | null, held: SessionHold | null): void {
    if (wire.closed) return
    wire.closed = true
    this.rememberClosed(wire, held)
    // Unblock any data POST awaiting `ready` — its dispatch sees the closed wire and bails.
    wire.resolveReady()
    const readOff = pingDeadline(this.mux.pingInterval, CHANNEL_TRANSPORT.SSE)
    wire.events.close(refused === null ? null : encode.end(wireEnd(refused)), readOff)
    const { bodies } = wire
    // Each request's own read awaits the same read-off, and takes its failure.
    for (const body of bodies) body.discardRest('server').catch(() => {})
    // Also with no body under read: a request that found the wire just before its end may yet start reading one. The
    // timer holds the bodies alone, not the closed wire.
    unrefTimer(
      setTimeout(() => {
        for (const body of bodies) body.letGo()
      }, readOff),
    )
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
  truncated: { kind: 'retry', reason: 'The request ended in the middle of a frame.' },
} as const satisfies Record<string, Refusal>

/** Telefunc's own refusal, which says so in its media type (`refusal.ts`); the body repeats its reason for a person. */
function refuse(refusal: Refusal, statusCode: 400 | 409 = 400): SseChannelHttpResponse {
  return { statusCode, contentType: refusalContentType(refusal), headers: [], body: refusal.reason }
}

function okResponse(): SseChannelHttpResponse {
  return { statusCode: 200, contentType: 'text/plain', headers: [], body: '' }
}

async function handleSseChannelRequest(
  request: Request,
  readable: Readable | undefined,
): Promise<SseChannelHttpResponse> {
  globalObject.sseTransport ??= new SseServerTransport()
  return globalObject.sseTransport.handleRequest(request, readable)
}
