export { getTelefuncSseChannelHooks, handleSseChannelRequest }
export type { SseChannelHttpResponse }

import type { Readable } from 'node:stream'
import { assert } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT } from '../constants.js'
import { createPushReadableStream, type PushReadableStream } from '../push-readable-stream.js'
import { createPushReadable, type PushReadable } from '../push-readable.js'
import { uint8ArrayToBase64url } from '../base64url.js'
import { textEncoder } from '../frame.js'
import { parseSseRequestMetadata } from '../sse-request.js'
import { OversizeFrameError, StreamReader } from './request/StreamReader.js'
import { getChannelMux } from './mux.js'
import type { ReconcileOutcome, ServerTransport } from './mux.js'
import { encode } from '../shared-ws.js'

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
  /** Every in-flight dispatch on this connection, from any POST shape. ONE registry: it is both the
   *  streamRequest body's drain set and what `runStreamResponse` awaits before `sendReconciled`, so
   *  a frame that arrived on the upload wire cannot be reported un-applied by the reconcile. */
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
      const metadata = parseSseRequestMetadata(await reader.readMetadata(this.mux.maxMetadataBytes))
      if (metadata.streamResponse) return await this.handleStreamResponsePost(metadata.connId, reader, useNodeStream)
      if (metadata.streamRequest) return await this.handleStreamRequestPost(metadata.connId, reader)
      return await this.handleBatchPost(metadata.connId, reader)
    } catch (err) {
      // A 400 is the right answer to a malformed or truncated request, and those are the errors this
      // sink is FOR. Anything else reaching it is ours, and answering 400 would file our bug under
      // the client's mistakes — so it is logged rather than silently converted.
      if (!isExpectedRequestError(err)) console.error('[telefunc][channel] internal error handling an SSE POST', err)
      return badRequest()
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
    if (existing) this.closeConnection(existing, false)

    const onCancel = () => {
      const conn = this.mux.getConnectionByConnId<SseConnection>(connId)
      if (conn) this.closeConnection(conn, false)
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

  /** Long-lived client→server upload POST. Body streams over the connection's lifetime; each frame
   *  is dispatched WITHOUT being awaited inside the loop, so a slow dispatch never stalls the body,
   *  and the mux emits `reconciled` inline whenever one fires.
   *
   *  Dispatches are registered rather than discarded, so body-end is a real drain point: when this
   *  POST completes, every frame it carried has finished its recv-chain turn. The `finally` is what
   *  makes that hold on BOTH exits — a truncated body is the case a client is most likely to follow
   *  with a reconnect, and the one where a half-applied turn matters most. */
  private async handleStreamRequestPost(connId: string, reader: StreamReader): Promise<SseChannelHttpResponse> {
    const connection = await this.resolveConnection(connId)
    if (!connection) return badRequest()
    if (!(await this.waitReady(connection))) return badRequest()
    // ⚠️ AFTER the readiness gate, never on connection resolution. The client treats this ack as
    // "this wire will carry my uploads" and may enqueue immediately, so sending it while the initial
    // reconcile was still in flight opened a window whose frames raced RECONCILED. `ready` now
    // releases only once RECONCILED has been sent, which is what makes the early upload unreachable
    // rather than merely unlikely.
    this.sendNow(connection, encode.streamRequestOpenAck())
    try {
      while (true) {
        const raw = await this.readFrameOrNull(connection, reader)
        if (!raw || connection.closed) break
        const dispatch = this.mux.onConnectionRawMessage(connection, raw)
        // Settled dispatches are evicted as they complete, so a long-lived upload does not
        // accumulate one retained promise per frame for the connection's lifetime.
        connection.pendingDispatches.add(dispatch)
        const evict = () => connection.pendingDispatches.delete(dispatch)
        // `then(evict, evict)` rather than `.finally()`: it cannot itself produce an unhandled
        // rejection if a dispatch ever rejects.
        dispatch.then(evict, evict)
      }
    } finally {
      await Promise.allSettled(connection.pendingDispatches)
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
      if (shouldSendReconciled(outcome, connection)) this.mux.sendReconciled(connection, outcome)
    } finally {
      connection.pendingDispatches.delete(drain)
    }
    return okResponse()
  }

  /** Stream-response POST lifecycle: consume the initial reconcile batch, drain any dispatch already
   *  in flight (so its `_lastClientSeq` mutation lands first), emit `reconciled` — and only THEN
   *  release the `ready` gate.
   *
   *  ⚠️ The gate is released LAST, and that ordering is the whole point. Released before
   *  `sendReconciled`, it let every POST parked in `waitReady` resume and register a dispatch — but
   *  the drain below snapshots `pendingDispatches` SYNCHRONOUSLY, one turn before any of them can
   *  resume, so their work was never in the set it awaits. RECONCILED then reported a `lastSeq`
   *  taken before frames that were already being applied. Draining a set that work cannot yet have
   *  entered is not a barrier; making the gate the last thing that opens is.
   *
   *  The drain is still needed: `closeConnection` resolves `ready` early on a mid-drain failure, and
   *  a batch POST released that way registers unconditionally. */
  private async runStreamResponse(connection: SseConnection, reader: StreamReader): Promise<void> {
    let outcome: ReconcileOutcome | null = null
    try {
      outcome = await this.drainDeferred(connection, reader)
    } catch {
      // Body truncated mid-frame (`StreamReader` throws). The caller fire-and-forgets this
      // promise, so a rethrow would be an unhandled rejection. Transient close: the channels
      // keep their reconnect grace and the client's retry can re-attach them.
      this.closeConnection(connection, false) // resolves `ready` — the parked POSTs see it closed
      return
    }
    try {
      if (!shouldSendReconciled(outcome, connection)) return
      await Promise.allSettled(connection.pendingDispatches)
      this.mux.sendReconciled(connection, outcome)
    } finally {
      connection.resolveReady()
    }
  }

  /** Read length-prefixed frames from `reader`, dispatch each through the deferred-reconcile
   *  path. Returns the last `ReconcileOutcome` produced in this body, or null if none did. */
  private async drainDeferred(connection: SseConnection, reader: StreamReader): Promise<ReconcileOutcome | null> {
    let outcome: ReconcileOutcome | null = null
    while (true) {
      const raw = await this.readFrameOrNull(connection, reader)
      if (!raw || connection.closed) break
      const next = await this.mux.onConnectionRawMessageDeferredReconciled(connection, raw)
      if (next !== null) outcome = next
    }
    return outcome
  }

  /** The single read site for every C2S body, so the raw-frame ceiling has exactly one enforcement
   *  point and exactly one termination point across all three POST shapes. Only an OVERSIZE frame
   *  kills the wire: a truncated body is an ordinary client death, and the existing per-caller
   *  handling of that stays exactly as it was. Permanent, because the declared length is a
   *  protocol-level lie rather than a transient fault — a reconnect grace would just invite the
   *  next one. */
  private async readFrameOrNull(connection: SseConnection, reader: StreamReader) {
    try {
      return await reader.readLengthPrefixedBytesOrNull(this.mux.maxRawFrameBytes)
    } catch (err) {
      if (err instanceof OversizeFrameError) this.closeConnection(connection, true)
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

  private closeConnection(connection: SseConnection, permanent: boolean): void {
    if (connection.closed) return
    connection.closed = true
    // Unblock any data POST awaiting `ready` — its dispatch sees the closed connection and bails.
    connection.resolveReady()
    this.mux.onConnectionClosed(connection, permanent)
    connection.stream.close()
  }

  private terminateConnection(connection: SseConnection): void {
    const terminatePermanently = this.mux.readPermanentTermination(connection)
    this.closeConnection(connection, terminatePermanently === true)
  }
}

/** A closed connection normally suppresses its reconciled — but not when a barrier commit set
 *  `deliverTo`: that reconciled belongs to the staged WS, and this SSE connection is precisely the
 *  one the client is retiring. `mux.send` already no-ops on a connection with no live entry. */
function shouldSendReconciled(
  outcome: ReconcileOutcome | null,
  connection: SseConnection,
): outcome is ReconcileOutcome {
  if (outcome === null) return false
  return outcome.deliverTo !== undefined || !connection.closed
}

/** The two error classes a malformed or dying REQUEST legitimately produces: a body that stopped
 *  mid-frame, and a declared length over a ceiling. Everything else is ours. `parseSseRequestMetadata`
 *  and `JSON.parse` throw plain errors on junk, so the disconnect message is matched by text — the
 *  same shape `StreamReader` already uses to signal it. */
function isExpectedRequestError(err: unknown): boolean {
  if (err instanceof OversizeFrameError) return true
  if (!(err instanceof Error)) return true
  return err.message.includes('disconnected') || err.message.includes('JSON') || err.name === 'SyntaxError'
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
