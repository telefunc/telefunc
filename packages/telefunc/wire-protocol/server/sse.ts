export { getTelefuncSseChannelHooks, handleSseChannelRequest }
export type { SseChannelHttpResponse }

import type { Readable } from 'node:stream'
import { assert } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT } from '../constants.js'
import { createPushReadableStream, type PushReadableStream } from '../push-readable-stream.js'
import { createPushReadable, type PushReadable } from '../push-readable.js'
import { uint8ArrayToBase64url } from '../base64url.js'
import { parseSseRequestMetadata } from '../sse-request.js'
import { StreamReader } from './request/StreamReader.js'
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
  /** Batch data POSTs whose dispatch is in flight. Drained by `runStreamResponse` before
   *  `sendReconciled` so their `_lastClientSeq` mutations land first. */
  pendingDispatches: Set<Promise<unknown>>
}

const textEncoder = new TextEncoder()
const sseOpenComment = textEncoder.encode(': open\n\n')

const globalObject = getGlobalObject('wire-protocol/server/sse.ts', {
  defaultHooks: null as ReturnType<typeof getTelefuncSseChannelHooks> | null,
})

class SseConnectionTransport {
  /** Resolvers for data POSTs that arrived before the stream-response POST registered the
   *  connection — covers the same-instance race where the long-lived stream-request POST
   *  lands before the stream-response POST. */
  private readonly pendingConnections = new Map<string, Array<(connection: SseConnection | null) => void>>()
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
      const metadata = parseSseRequestMetadata(await reader.readMetadata())
      if ('streamResponse' in metadata)
        return await this.handleStreamResponsePost(metadata.connId, reader, useNodeStream)
      if (metadata.streamRequest) return await this.handleStreamRequestPost(metadata.connId, reader)
      return await this.handleBatchPost(metadata.connId, reader)
    } catch {
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

  /** Long-lived client→server upload POST. Body streams over the connection's lifetime;
   *  each frame is dispatched fire-and-forget so the loop never blocks the body, and the
   *  mux emits `reconciled` inline whenever one fires. */
  private async handleStreamRequestPost(connId: string, reader: StreamReader): Promise<SseChannelHttpResponse> {
    const connection = await this.resolveConnection(connId)
    if (!connection) return badRequest()
    // Send the open-ack as soon as we have a connection — the client races this ack against
    // its handshake timeout to commit to using this wire as its upload channel.
    this.sendNow(connection, encode.streamRequestOpenAck())
    if (!(await this.waitReady(connection))) return badRequest()
    while (true) {
      const raw = await reader.readLengthPrefixedBytesOrNull()
      if (!raw || connection.closed) break
      void this.mux.onConnectionRawMessage(connection, raw)
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
      if (outcome !== null && !connection.closed) this.mux.sendReconciled(connection, outcome)
    } finally {
      connection.pendingDispatches.delete(drain)
    }
    return okResponse()
  }

  /** Stream-response POST lifecycle: consume the initial reconcile batch, release the
   *  `ready` gate, drain in-flight batch POSTs (so their `_lastClientSeq` mutations land
   *  first), then emit `reconciled`. */
  private async runStreamResponse(connection: SseConnection, reader: StreamReader): Promise<void> {
    let outcome: ReconcileOutcome | null = null
    try {
      outcome = await this.drainDeferred(connection, reader)
    } finally {
      connection.resolveReady()
    }
    if (outcome === null || connection.closed) return
    if (connection.pendingDispatches.size > 0) await Promise.allSettled(connection.pendingDispatches)
    this.mux.sendReconciled(connection, outcome)
  }

  /** Read length-prefixed frames from `reader`, dispatch each through the deferred-reconcile
   *  path. Returns the last `ReconcileOutcome` produced in this body, or null if none did. */
  private async drainDeferred(connection: SseConnection, reader: StreamReader): Promise<ReconcileOutcome | null> {
    let outcome: ReconcileOutcome | null = null
    while (true) {
      const raw = await reader.readLengthPrefixedBytesOrNull()
      if (!raw || connection.closed) break
      const next = await this.mux.onConnectionRawMessageDeferredReconciled(connection, raw)
      if (next !== null) outcome = next
    }
    return outcome
  }

  private async resolveConnection(connId: string): Promise<SseConnection | null> {
    return this.mux.getConnectionByConnId<SseConnection>(connId) ?? (await this.waitForConnection(connId))
  }

  private waitForConnection(connId: string): Promise<SseConnection | null> {
    return new Promise<SseConnection | null>((resolve) => {
      let pending = this.pendingConnections.get(connId)
      if (!pending) this.pendingConnections.set(connId, (pending = []))
      pending.push(resolve)
      setTimeout(() => resolve(null), this.mux.connectTtl)
    })
  }

  private resolvePendingConnections(connId: string, connection: SseConnection | null): void {
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
      const timer = setTimeout(() => resolve(false), this.mux.connectTtl)
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
    const terminatePermanently = this.mux.consumePermanentTermination(connection)
    this.closeConnection(connection, terminatePermanently === true)
  }
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
