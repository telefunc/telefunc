export { getTelefuncSseChannelHooks, handleSseChannelRequest }
export type { SseChannelHttpResponse, SseConnection }

import type { Readable } from 'node:stream'
import { assert } from '../../../utils/assert.js'
import { Fifo } from '../../../utils/Fifo.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'
import { handleTelefunctionBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import { CHANNEL_TRANSPORT, SSE_METADATA_MAX_BYTES, WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'
import { createPushReadableStream, type PushReadableStream } from '../../../utils/pushReadableStream.js'
import { createPushReadable, type PushReadable } from '../../../utils/pushReadable.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { textEncoder } from '../../wire/frame.js'
import { parseSseRequestMetadata, type SseRequestMetadata } from '../sseRequest.js'
import { OversizeFrameError, StreamReader, StreamTruncatedError } from '../../wire/StreamReader.js'
import { getChannelMux } from './mux.js'
import type { ChannelMux, ReconcileOutcome, ServerTransport } from './mux.js'
import { encode, isProtocolViolationError, passesData, ProtocolViolationError } from '../../wire/connectionFrame.js'

type SseChannelHttpResponse = {
  statusCode: 200 | 400
  contentType: 'text/plain' | 'text/event-stream'
  headers: [string, string][]
  body: string | Readable | ReadableStream<Uint8Array<ArrayBuffer>>
}

type SseConnection = {
  connId: string
  events: EventStream
  closed: boolean
  sessionId: string | null
  /** Resolved by `runStreamResponse` once the stream-response POST's body is consumed. Data
   *  POSTs gate on this before dispatching so they can't race ahead of the reconcile. */
  ready: Promise<void>
  resolveReady: () => void
  /** Dispatches in flight for this connection. A reconcile waits on these before reporting a seq,
   *  so a batch POST's `_lastClientSeq` mutations can't land after the RECONCILED that reports them. */
  pendingDispatches: Set<Promise<unknown>>
}

type EventStreamOptions = {
  useNodeStream: boolean
  onCancel: () => void
}

/** The event stream to the page: data waits for it to ask for more, and the frames that go at once pass that data. */
class EventStream {
  readonly stream: PushReadable | PushReadableStream<Uint8Array<ArrayBuffer>>
  private readonly waiting = new Fifo<Uint8Array<ArrayBuffer>>()
  private waitingBytes = 0
  /** Whether the stream asks for more. */
  private takes: boolean

  constructor({ useNodeStream, onCancel }: EventStreamOptions) {
    const onTake = () => this.handWaiting()
    this.stream = useNodeStream
      ? createPushReadable(onCancel, onTake)
      : createPushReadableStream<Uint8Array<ArrayBuffer>>(onCancel, onTake)
    this.takes = this.stream.push(textEncoder.encode(': open\n\n'))
  }

  /** The encoded bytes the stream holds, and those waiting for it to ask. */
  get heldBytes(): number {
    return this.stream.bufferedAmount + this.waitingBytes
  }

  get streamBytes(): number {
    return this.stream.bufferedAmount
  }

  /** Data waits for the stream to ask for it, so a frame that goes at once is behind no more than the stream holds. */
  send(frame: Uint8Array<ArrayBuffer>): void {
    const event = textEncoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`)
    if (passesData(frame[0]!) || (this.takes && this.waiting.length === 0)) {
      this.takes = this.stream.push(event)
      return
    }
    this.waiting.push(event)
    this.waitingBytes += event.byteLength
  }

  close(): void {
    while (this.waiting.length > 0) this.stream.push(this.waiting.shift()!)
    this.waitingBytes = 0
    this.stream.close()
  }

  /** Hands the stream the data that waits, as far as it asks. */
  private handWaiting(): void {
    this.takes = true
    while (this.takes && this.waiting.length > 0) {
      const event = this.waiting.shift()!
      this.waitingBytes -= event.byteLength
      this.takes = this.stream.push(event)
    }
  }
}

const globalObject = getGlobalObject('wire-protocol/channel/server/sse.ts', {
  defaultHooks: null as ReturnType<typeof getTelefuncSseChannelHooks> | null,
})

class SseConnectionTransport {
  /** Resolvers for data POSTs that arrived before the stream-response POST registered the
   *  connection — covers the same-instance race where the long-lived stream-request POST
   *  lands before the stream-response POST. */
  private readonly pendingConnections = new Map<string, Set<(connection: SseConnection | null) => void>>()
  /** When each wire closed and the session it had, for `connectTtl`: a page never reuses a connId, so a POST for one is
   *  refused. */
  private readonly closedWires = new Map<string, { at: number; sessionId: string | null }>()
  /** Per use: a Cloudflare session DO hosts its own channels, and this transport serves every one in the isolate. */
  private get mux(): ChannelMux {
    return getChannelMux()
  }
  private readonly transport: ServerTransport<SseConnection> = {
    getSessionId: (connection) => connection.sessionId ?? undefined,
    setSessionId: (connection, sessionId) => {
      connection.sessionId = sessionId
    },
    getConnId: (connection) => connection.connId,
    sendNow: (connection, frame) => this.sendNow(connection, frame),
    wholeMessages: false,
    // Events carry frames in base64 plus framing, so three quarters of what waits is at least the frames it carries.
    bufferedAmount: (connection) => Math.floor((connection.events.heldBytes * 3) / 4),
    // A frame that goes at once passes the data waiting for the stream to ask: it waits behind the stream's alone.
    queuedAhead: (connection) => Math.floor((connection.events.streamBytes * 3) / 4),
    terminateConnection: (connection) => this.terminateConnection(connection),
  }

  async handleRequest(request: Request, readable?: Readable): Promise<SseChannelHttpResponse | null> {
    if (!getServerConfig().channel.transports.includes(CHANNEL_TRANSPORT.SSE)) return badRequest()
    if (request.method !== 'POST') return badRequest()
    const source = readable ?? request.body
    assert(source)
    // Adapter mode: a Node `IncomingMessage` means we're on the Node-native serve path
    // and should answer with a Node `Readable` so it pipes straight to the socket. Web
    // adapters get `request.body` (no `readable`) and want a `ReadableStream` back.
    const useNodeStream = readable !== undefined
    let connId: string | null = null
    try {
      const reader = new StreamReader(source)
      const metadata = parseMetadata(await reader.readMetadata(SSE_METADATA_MAX_BYTES))
      connId = metadata.connId
      if (metadata.streamResponse) return await this.handleStreamResponsePost(connId, reader, useNodeStream)
      if (metadata.streamRequest) return await this.handleStreamRequestPost(connId, reader)
      if (metadata.close) return this.handleClosePost(connId)
      return await this.handleBatchPost(connId, reader)
    } catch (err) {
      // An oversize frame leaves no next frame boundary to resume from, so it ends the wire, not
      // just this POST.
      if (err instanceof OversizeFrameError && connId !== null) this.closeWire(connId)
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
    reader: StreamReader,
    useNodeStream: boolean,
  ): Promise<SseChannelHttpResponse> {
    const existing = this.mux.getConnectionByConnId<SseConnection>(connId)
    if (existing) this.closeConnection(existing, { permanent: false })

    const onCancel = () => {
      const conn = this.mux.getConnectionByConnId<SseConnection>(connId)
      if (conn) this.closeConnection(conn, { permanent: false })
    }
    const events = new EventStream({ useNodeStream, onCancel })

    let resolveReady!: () => void
    const ready = new Promise<void>((resolve) => {
      resolveReady = resolve
    })
    const connection: SseConnection = {
      connId,
      events,
      closed: false,
      sessionId: null,
      ready,
      resolveReady,
      pendingDispatches: new Set(),
    }
    this.mux.onConnectionOpen(connection, this.transport)
    this.resolvePendingConnections(connId, connection)
    void this.runStreamResponse(connection, reader)

    return {
      statusCode: 200,
      contentType: 'text/event-stream',
      headers: [
        ['Cache-Control', 'no-cache, no-transform'],
        ['X-Accel-Buffering', 'no'],
      ],
      body: events.stream,
    }
  }

  /** Long-lived client→server upload POST. The body streams for the connection's lifetime, so every
   *  frame is dispatched fire-and-forget — awaiting one would stall the read loop behind it. */
  private async handleStreamRequestPost(connId: string, reader: StreamReader): Promise<SseChannelHttpResponse> {
    const connection = await this.resolveConnection(connId)
    if (!connection) return badRequest()
    reader.onChunk = () => this.mux.onConnectionBytes(connection)
    // The open-ack is the client's duplex probe (ACK ⇒ its upload bytes reached the server) and must
    // not wait on reconcile settlement, or a slow attach would falsely demote a healthy duplex wire to
    // sticky batch. Dispatch safety is owned by `runStreamResponse` releasing `ready` only after
    // RECONCILED — the read loop below still waits on that gate, so early bytes sit unread until then.
    this.sendNow(connection, encode.streamRequestOpenAck())
    // No deadline: the client trusts this POST from the ack on; runStreamResponse's end or the close opens the gate.
    await connection.ready
    if (connection.closed) return badRequest()
    try {
      while (true) {
        const raw = await reader.readLengthPrefixedBytesOrNull(WIRE_MAX_RAW_FRAME_BYTES)
        if (!raw || connection.closed) break
        this.dispatchAndReport(connection, raw)
      }
    } finally {
      await this.settlePendingDispatches(connection)
    }
    return okResponse()
  }

  /** The page left for good: its wire closes as a WebSocket's does when its page leaves. One its page lost first has
   *  left its session's channels waiting for a reconnect, which end now. */
  private handleClosePost(connId: string): SseChannelHttpResponse {
    const connection = this.mux.getConnectionByConnId<SseConnection>(connId)
    if (connection) {
      this.closeConnection(connection, { permanent: true })
      return okResponse()
    }
    const sessionId = this.closedWires.get(connId)?.sessionId
    if (sessionId) this.mux.endSession(sessionId)
    return okResponse()
  }

  /** Short-lived outbox batch POST. A RECONCILE's RECONCILED goes as it is answered, ahead of what the frames behind it
   *  send: those are what the page carries with it, which it doesn't replay. Tracked in `pendingDispatches` so
   *  `runStreamResponse` won't send its own reconciled mid-batch. */
  private async handleBatchPost(connId: string, reader: StreamReader): Promise<SseChannelHttpResponse> {
    const connection = await this.resolveConnection(connId)
    if (!connection) return badRequest()
    reader.onChunk = () => this.mux.onConnectionBytes(connection)
    if (!(await this.waitReady(connection))) return badRequest()
    const drain = this.drain(connection, reader, (outcome) => {
      if (shouldSendReconciled(outcome, connection)) this.mux.sendReconciled(outcome)
    })
    connection.pendingDispatches.add(drain)
    try {
      await drain
    } finally {
      connection.pendingDispatches.delete(drain)
    }
    return okResponse()
  }

  /** Stream-response POST lifecycle: consume the initial reconcile batch; once its RECONCILE is answered, drain the
   *  batch POSTs in flight so their `_lastClientSeq` mutations land first and emit `reconciled`, ahead of what the
   *  frames carried behind it send; then release the `ready` gate the other POSTs are parked on. */
  private async runStreamResponse(connection: SseConnection, reader: StreamReader): Promise<void> {
    try {
      await this.drain(connection, reader, async (outcome) => {
        if (!shouldSendReconciled(outcome, connection)) return
        await this.settlePendingDispatches(connection)
        this.mux.sendReconciled(outcome)
      })
    } catch (err) {
      // The body ended mid-frame; this promise is fire-and-forget, so a rethrow would be unhandled.
      // A truncation is the client hanging up (channels keep their grace); an oversize frame leaves
      // no next frame boundary, so that wire is finished.
      reportDispatchBug(err)
      this.closeConnection(connection, { permanent: err instanceof OversizeFrameError })
    } finally {
      // Every path releases the gate here — the parked POSTs then see whatever state we left.
      connection.resolveReady()
    }
  }

  /** Registered so a reconcile waits for it, and reported here because nothing else will — awaited
   *  dispatches (the batch POST's) report through their caller. */
  private dispatchAndReport(connection: SseConnection, raw: Uint8Array<ArrayBuffer>): void {
    const dispatch = this.mux.onConnectionRawMessage(connection, raw)
    connection.pendingDispatches.add(dispatch)
    const evict = () => connection.pendingDispatches.delete(dispatch)
    dispatch.then(evict, (err) => {
      evict()
      reportDispatchBug(err)
    })
  }

  /** Waits without consuming failures — every dispatch is reported by whoever started it. */
  private settlePendingDispatches(connection: SseConnection): Promise<unknown> {
    return Promise.allSettled([...connection.pendingDispatches])
  }

  /** Reads `reader`'s frames and dispatches each through the deferred-reconcile path; a RECONCILE's outcome goes to
   *  `onReconciled` before the next frame is read. */
  private async drain(
    connection: SseConnection,
    reader: StreamReader,
    onReconciled: (outcome: ReconcileOutcome) => void | Promise<void>,
  ): Promise<void> {
    while (true) {
      const raw = await reader.readLengthPrefixedBytesOrNull(WIRE_MAX_RAW_FRAME_BYTES)
      if (!raw || connection.closed) break
      const outcome = await this.mux.onConnectionRawMessageDeferredReconciled(connection, raw)
      if (outcome !== null) await onReconciled(outcome)
    }
  }

  private async resolveConnection(connId: string): Promise<SseConnection | null> {
    const connection = this.mux.getConnectionByConnId<SseConnection>(connId)
    if (connection) return connection
    if (this.closedWires.has(connId)) return null
    return await this.waitForConnection(connId)
  }

  private rememberClosed({ connId, sessionId }: SseConnection): void {
    const now = performance.now()
    for (const [closed, { at }] of this.closedWires) {
      if (now - at < this.mux.connectTtl) break
      this.closedWires.delete(closed)
    }
    this.closedWires.set(connId, { at: now, sessionId })
  }

  /** Mirrors `ChannelMux.waitForChannelRegistration`: the timeout path must remove the
   *  waiter and (when last) the map entry, or abandoned data POSTs leak an entry forever. */
  private waitForConnection(connId: string): Promise<SseConnection | null> {
    return new Promise<SseConnection | null>((resolve) => {
      let settled = false
      let timer: ReturnType<typeof setTimeout>
      const pending = this.pendingConnections.get(connId) ?? new Set()
      this.pendingConnections.set(connId, pending)

      const settle = (connection: SseConnection | null): void => {
        if (settled) return
        settled = true
        pending.delete(waiter)
        // Identity-equality guards against deleting a replacement set registered after
        // `resolvePendingConnections` already consumed ours.
        if (pending.size === 0 && this.pendingConnections.get(connId) === pending) {
          this.pendingConnections.delete(connId)
        }
        clearTimeout(timer)
        resolve(connection)
      }
      const waiter = (connection: SseConnection | null): void => settle(connection)
      pending.add(waiter)
      timer = setTimeout(() => settle(null), this.mux.connectTtl)
    })
  }

  private resolvePendingConnections(connId: string, connection: SseConnection): void {
    const pending = this.pendingConnections.get(connId)
    if (!pending) return
    this.pendingConnections.delete(connId)
    for (const resolve of pending) resolve(connection)
  }

  private closeWire(connId: string): void {
    const connection = this.mux.getConnectionByConnId<SseConnection>(connId)
    if (connection) this.closeConnection(connection, { permanent: true })
  }

  private sendNow(connection: SseConnection, frame: Uint8Array<ArrayBuffer>): void {
    if (!connection.closed) connection.events.send(frame)
  }

  /** Resolves false on timeout — caller drops the POST. */
  private waitReady(connection: SseConnection): Promise<boolean> {
    return new Promise<boolean>((resolve) => {
      const timer = unrefTimer(setTimeout(() => resolve(false), this.mux.connectTtl))
      connection.ready.then(() => {
        clearTimeout(timer)
        resolve(true)
      })
    })
  }

  private closeConnection(connection: SseConnection, { permanent }: { permanent: boolean }): void {
    if (connection.closed) return
    connection.closed = true
    this.rememberClosed(connection)
    // Unblock any data POST awaiting `ready` — its dispatch sees the closed connection and bails.
    connection.resolveReady()
    this.mux.onConnectionClosed(connection, { permanent })
    connection.events.close()
  }

  private terminateConnection(connection: SseConnection): void {
    this.closeConnection(connection, { permanent: this.mux.readPermanentTermination(connection) })
  }
}

/** Malformed metadata is untrusted client ingress, not a truncation — same class as the decode seam. */
function parseMetadata(raw: string): SseRequestMetadata {
  try {
    return parseSseRequestMetadata(raw)
  } catch {
    throw new ProtocolViolationError('malformed SSE request metadata')
  }
}

function reportDispatchBug(err: unknown): void {
  if (isProtocolViolationError(err) || err instanceof OversizeFrameError || err instanceof StreamTruncatedError) return
  handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
}

/** A closed SSE wire has nothing to say — except on a barrier commit, where the RECONCILED is
 *  bound for the WS and this wire's own retirement is exactly what the upgrade just did. */
function shouldSendReconciled(outcome: ReconcileOutcome, connection: SseConnection): boolean {
  return outcome.deliverTo !== connection || !connection.closed
}

function badRequest(): SseChannelHttpResponse {
  return { statusCode: 400, contentType: 'text/plain', headers: [], body: '' }
}

function okResponse(): SseChannelHttpResponse {
  return { statusCode: 200, contentType: 'text/plain', headers: [], body: '' }
}

async function handleSseChannelRequest(request: Request, readable?: Readable): Promise<SseChannelHttpResponse | null> {
  globalObject.defaultHooks ??= getTelefuncSseChannelHooks()
  return globalObject.defaultHooks.handleRequest(request, readable)
}

function getTelefuncSseChannelHooks() {
  const server = new SseConnectionTransport()
  return {
    handleRequest(request: Request, readable?: Readable) {
      return server.handleRequest(request, readable)
    },
  }
}
