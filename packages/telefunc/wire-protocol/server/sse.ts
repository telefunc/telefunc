export { getTelefuncSseChannelHooks, handleSseChannelRequest }
export type { SseChannelHttpResponse }

import type { Readable } from 'node:stream'
import { assert } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { handleTelefunctionBug } from '../../node/server/runTelefunc/validateTelefunctionError.js'
import { CHANNEL_TRANSPORT, SSE_METADATA_MAX_BYTES, WIRE_MAX_RAW_FRAME_BYTES } from '../constants.js'
import { createPushReadableStream, type PushReadableStream } from '../push-readable-stream.js'
import { createPushReadable, type PushReadable } from '../push-readable.js'
import { uint8ArrayToBase64url } from '../base64url.js'
import { textEncoder } from '../frame.js'
import { parseSseRequestMetadata, type SseRequestMetadata } from '../sse-request.js'
import { OversizeFrameError, StreamReader, StreamTruncatedError } from './request/StreamReader.js'
import { getChannelMux } from './mux.js'
import type { ReconcileOutcome, ServerTransport } from './mux.js'
import { encode, ProtocolViolationError } from '../shared-ws.js'

type SseChannelHttpResponse = {
  statusCode: 200 | 400
  contentType: 'text/plain' | 'text/event-stream'
  headers: [string, string][]
  body: string | Readable | ReadableStream<Uint8Array<ArrayBuffer>>
}

type SseConnection = {
  connId: string
  /** Node-native `PushReadable` when the adapter passes a Node `IncomingMessage` (piped via
   *  `pipeline`); `PushReadableStream` (Web `ReadableStream`-backed) everywhere else. Both
   *  expose `push`, `close`, `isClosed`. */
  stream: PushReadable | PushReadableStream<Uint8Array<ArrayBuffer>>
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

const sseOpenComment = textEncoder.encode(': open\n\n')

const globalObject = getGlobalObject('wire-protocol/server/sse.ts', {
  defaultHooks: null as ReturnType<typeof getTelefuncSseChannelHooks> | null,
})

class SseConnectionTransport {
  /** Resolvers for data POSTs that arrived before the stream-response POST registered the
   *  connection — covers the same-instance race where the long-lived stream-request POST
   *  lands before the stream-response POST. */
  private readonly pendingConnections = new Map<string, Set<(connection: SseConnection | null) => void>>()
  private readonly mux = getChannelMux()
  private readonly transport: ServerTransport<SseConnection> = {
    getSessionId: (connection) => connection.sessionId ?? undefined,
    setSessionId: (connection, sessionId) => {
      connection.sessionId = sessionId
    },
    getConnId: (connection) => connection.connId,
    sendNow: (connection, frame) => this.sendNow(connection, frame),
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
    try {
      const reader = new StreamReader(source)
      const rawMetadata = await reader.readMetadata(SSE_METADATA_MAX_BYTES)
      let metadata: SseRequestMetadata
      try {
        metadata = parseSseRequestMetadata(rawMetadata)
      } catch {
        // Malformed metadata is untrusted client ingress, not a truncation — same class as the decode seam.
        throw new ProtocolViolationError('malformed SSE request metadata')
      }
      if (metadata.streamResponse) return await this.handleStreamResponsePost(metadata.connId, reader, useNodeStream)
      if (metadata.streamRequest) return await this.handleStreamRequestPost(metadata.connId, reader)
      return await this.handleBatchPost(metadata.connId, reader)
    } catch (err) {
      // A typed protocol-input fault is the client's: answer 400 and stay quiet. Anything else is our
      // bug — rethrow so the request pipeline (`runTelefunc`) logs it and masks it as a 500.
      if (
        err instanceof ProtocolViolationError ||
        err instanceof OversizeFrameError ||
        err instanceof StreamTruncatedError
      ) {
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
    const stream = useNodeStream
      ? createPushReadable(onCancel)
      : createPushReadableStream<Uint8Array<ArrayBuffer>>(onCancel)

    let resolveReady!: () => void
    const ready = new Promise<void>((resolve) => {
      resolveReady = resolve
    })
    const connection: SseConnection = {
      connId,
      stream,
      closed: false,
      sessionId: null,
      ready,
      resolveReady,
      pendingDispatches: new Set(),
    }
    this.mux.onConnectionOpen(connection, this.transport)
    this.resolvePendingConnections(connId, connection)
    stream.push(sseOpenComment)
    void this.runStreamResponse(connection, reader)

    return {
      statusCode: 200,
      contentType: 'text/event-stream',
      headers: [
        ['Cache-Control', 'no-cache, no-transform'],
        ['X-Accel-Buffering', 'no'],
      ],
      body: stream,
    }
  }

  /** Long-lived client→server upload POST. The body streams for the connection's lifetime, so every
   *  frame is dispatched fire-and-forget — awaiting one would stall the read loop behind it. */
  private async handleStreamRequestPost(connId: string, reader: StreamReader): Promise<SseChannelHttpResponse> {
    const connection = await this.resolveConnection(connId)
    if (!connection) return badRequest()
    // The open-ack is the client's duplex probe (ACK ⇒ its upload bytes reached the server) and must
    // not wait on reconcile settlement, or a slow attach would falsely demote a healthy duplex wire to
    // sticky batch. Dispatch safety is owned by `runStreamResponse` releasing `ready` only after
    // RECONCILED — the read loop below still waits on that gate, so early bytes sit unread until then.
    this.sendNow(connection, encode.streamRequestOpenAck())
    if (!(await this.waitReady(connection))) return badRequest()
    try {
      while (true) {
        const raw = await this.readFrameOrCloseWire(connection, reader)
        if (!raw || connection.closed) break
        this.dispatchAndReport(connection, raw)
      }
    } finally {
      await this.settlePendingDispatches(connection)
    }
    return okResponse()
  }

  /** Short-lived outbox batch POST. Body ends quickly, so we collect the reconcile that
   *  may fire during the body and emit `reconciled` at body end — that way all dispatched
   *  frames have lifted `_lastClientSeq` before the seq is reported. Tracked in
   *  `pendingDispatches` so `runStreamResponse` won't send its own reconciled mid-batch. */
  private async handleBatchPost(connId: string, reader: StreamReader): Promise<SseChannelHttpResponse> {
    const connection = await this.resolveConnection(connId)
    if (!connection) return badRequest()
    if (!(await this.waitReady(connection))) return badRequest()
    const drain = this.drainDeferred(connection, reader)
    connection.pendingDispatches.add(drain)
    try {
      const outcome = await drain
      if (shouldSendReconciled(outcome, connection)) this.mux.sendReconciled(outcome)
    } finally {
      connection.pendingDispatches.delete(drain)
    }
    return okResponse()
  }

  /** Stream-response POST lifecycle: consume the initial reconcile batch, drain the batch POSTs
   *  in flight so their `_lastClientSeq` mutations land first, emit `reconciled`, then release the
   *  `ready` gate the other POSTs are parked on. */
  private async runStreamResponse(connection: SseConnection, reader: StreamReader): Promise<void> {
    try {
      const outcome = await this.drainDeferred(connection, reader)
      if (!shouldSendReconciled(outcome, connection)) return
      await this.settlePendingDispatches(connection)
      this.mux.sendReconciled(outcome)
    } catch (err) {
      // Body truncated mid-frame (`StreamReader` throws). This promise is fire-and-forget, so a
      // rethrow would be an unhandled rejection. Transient: the channels keep their reconnect
      // grace and the client's retry re-attaches them.
      reportDispatchBug(err)
      this.closeConnection(connection, { permanent: false })
    } finally {
      // Every path releases the gate here — the parked POSTs then see whatever state we left.
      connection.resolveReady()
    }
  }

  /** Fire-and-forget dispatch: registered so a reconcile waits for it, and reported here because
   *  nothing else will. Awaited dispatches (the batch POST's) report through their caller instead. */
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
  private async settlePendingDispatches(connection: SseConnection): Promise<void> {
    await Promise.allSettled([...connection.pendingDispatches])
  }

  /** Read length-prefixed frames from `reader`, dispatch each through the deferred-reconcile
   *  path. Returns the last `ReconcileOutcome` produced in this body, or null if none did. */
  private async drainDeferred(connection: SseConnection, reader: StreamReader): Promise<ReconcileOutcome | null> {
    let outcome: ReconcileOutcome | null = null
    while (true) {
      const raw = await this.readFrameOrCloseWire(connection, reader)
      if (!raw || connection.closed) break
      const next = await this.mux.onConnectionRawMessageDeferredReconciled(connection, raw)
      if (next !== null) outcome = next
    }
    return outcome
  }

  /** Next frame, or null at a clean end of body. An oversize frame desynchronises the body — there
   *  is no next frame boundary to find — so the wire is closed for good and the error rethrown. */
  private async readFrameOrCloseWire(connection: SseConnection, reader: StreamReader) {
    try {
      return await reader.readLengthPrefixedBytesOrNull(WIRE_MAX_RAW_FRAME_BYTES)
    } catch (err) {
      if (err instanceof OversizeFrameError) this.closeConnection(connection, { permanent: true })
      throw err
    }
  }

  private async resolveConnection(connId: string): Promise<SseConnection | null> {
    return this.mux.getConnectionByConnId<SseConnection>(connId) ?? (await this.waitForConnection(connId))
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

  private sendNow(connection: SseConnection, frame: Uint8Array<ArrayBuffer>): void {
    if (connection.closed) return
    connection.stream.push(textEncoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`))
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
    // Unblock any data POST awaiting `ready` — its dispatch sees the closed connection and bails.
    connection.resolveReady()
    this.mux.onConnectionClosed(connection, permanent)
    connection.stream.close()
  }

  private terminateConnection(connection: SseConnection): void {
    const terminatePermanently = this.mux.readPermanentTermination(connection)
    this.closeConnection(connection, { permanent: terminatePermanently === true })
  }
}

function reportDispatchBug(err: unknown): void {
  if (err instanceof ProtocolViolationError || err instanceof OversizeFrameError || err instanceof StreamTruncatedError)
    return
  handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
}

/** A closed SSE wire has nothing to say — except on a barrier commit, where the RECONCILED is
 *  bound for the WS and this wire's own retirement is exactly what the upgrade just did. */
function shouldSendReconciled(
  outcome: ReconcileOutcome | null,
  connection: SseConnection,
): outcome is ReconcileOutcome {
  if (outcome === null) return false
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
