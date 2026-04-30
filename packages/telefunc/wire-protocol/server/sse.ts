export { getTelefuncSseChannelHooks, handleSseChannelRequest }
export type { SseChannelHttpResponse }

import { assert } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT } from '../constants.js'
import { PushToPullStream } from '../push-to-pull-stream.js'
import { uint8ArrayToBase64url } from '../base64url.js'
import { parseSseRequestMetadata } from '../sse-request.js'
import { ServerConnection } from './connection.js'
import { StreamReader } from './request/StreamReader.js'

type SseChannelHttpResponse = {
  statusCode: 200 | 400
  contentType: 'text/plain' | 'text/event-stream'
  headers: [string, string][]
  body: string | ReadableStream<Uint8Array>
}

type SseConnection = {
  connId: string
  stream: PushToPullStream<Uint8Array>
  closed: boolean
  sessionId: string | null
  /** Resolved by `runStreamPost` once the stream POST's body is consumed. Data POSTs gate on
   *  this before dispatching frames, so they can't race ahead of the reconcile and hit a
   *  `getSessionStateOrThrow(undefined)`. */
  ready: Promise<void>
  resolveReady: () => void
  /** Data POSTs whose dispatch is in flight. Drained by `runStreamPost` before `sendReconciled`,
   *  so any `entry.lastClientSeq` mutations from those dispatches are reflected in the
   *  `reconciled` frame's reported `lastSeq`. */
  pendingDispatches: Set<Promise<unknown>>
}

const textEncoder = new TextEncoder()
const sseOpenComment = textEncoder.encode(': open\n\n')

const globalObject = getGlobalObject('wire-protocol/server/sse.ts', {
  defaultHooks: null as ReturnType<typeof getTelefuncSseChannelHooks> | null,
})

class SseConnectionTransport {
  private readonly connections = new Map<string, SseConnection>()
  /** Resolvers for data POSTs that arrived before the stream POST registered the connection. */
  private readonly pendingConnections = new Map<string, Array<(connection: SseConnection | null) => void>>()
  private readonly connection = new ServerConnection<SseConnection>({
    getSessionId: (connection) => connection.sessionId ?? undefined,
    setSessionId: (connection, sessionId) => {
      connection.sessionId = sessionId
    },
    sendNow: (connection, frame) => this.sendNow(connection, frame),
    terminateConnection: (connection) => this.terminateConnection(connection),
  })

  async handleRequest(request: Request): Promise<SseChannelHttpResponse | null> {
    if (!getServerConfig().channel.transports.includes(CHANNEL_TRANSPORT.SSE)) return badRequest()
    if (request.method !== 'POST') return badRequest()
    const { body } = request
    assert(body)
    try {
      const reader = new StreamReader(body)
      const { connId, stream } = parseSseRequestMetadata(await reader.readMetadata())
      return stream ? this.handleStreamPost(connId, reader) : await this.handleDataPost(connId, reader)
    } catch {
      return badRequest()
    }
  }

  /** Stream POST — opens the SSE downstream, registers the connection, and asynchronously drives
   *  its lifecycle (`runStreamPost`). Returns immediately so the response headers can flush. */
  private handleStreamPost(connId: string, reader: StreamReader): SseChannelHttpResponse {
    const existing = this.connections.get(connId)
    if (existing) this.closeConnection(existing, false)

    const stream = new PushToPullStream<Uint8Array>(() => {
      const conn = this.connections.get(connId)
      if (conn) this.closeConnection(conn, false)
    })
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
    this.connections.set(connId, connection)
    this.resolvePendingConnections(connId, connection)
    stream.push(sseOpenComment)
    this.connection.onConnectionOpen(connection)
    void this.runStreamPost(connection, reader)

    return {
      statusCode: 200,
      contentType: 'text/event-stream',
      headers: [
        ['Cache-Control', 'no-cache, no-transform'],
        ['X-Accel-Buffering', 'no'],
      ],
      body: stream.readable,
    }
  }

  /** Data POST — waits for the connection's reconcile to complete, then dispatches its frames.
   *  Tracked in `pendingDispatches` so `runStreamPost` can drain it before `sendReconciled`.
   *
   *  If the data POST's body contains a reconcile (e.g. the client added a new channel
   *  after the initial reconcile, so the late reconcile rides over the upload stream or
   *  an outbox flush POST), we must emit `reconciled` here — `runStreamPost` only fires
   *  for the initial stream POST. */
  private async handleDataPost(connId: string, reader: StreamReader): Promise<SseChannelHttpResponse> {
    const connection = this.connections.get(connId) ?? (await this.waitForConnection(connId))
    if (!connection) return badRequest()

    let reconcileSessionId: string | null = null
    const dispatch = (async (): Promise<'ok' | 'timeout'> => {
      if (!(await this.waitReady(connection))) return 'timeout'
      reconcileSessionId = await this.processFrameReader(connection, reader)
      return 'ok'
    })()
    connection.pendingDispatches.add(dispatch)
    try {
      if ((await dispatch) === 'timeout') return badRequest()
    } finally {
      connection.pendingDispatches.delete(dispatch)
    }
    if (reconcileSessionId !== null && !connection.closed) {
      const pending = this.connection.sendReconciled(connection, reconcileSessionId)
      if (pending) await pending
    }
    return { statusCode: 200, contentType: 'text/plain', headers: [], body: '' }
  }

  /** Stream POST lifecycle: consume the initial batch, release the `ready` gate, drain in-flight
   *  data POSTs (so their `lastClientSeq` mutations land first), then emit `reconciled`. */
  private async runStreamPost(connection: SseConnection, reader: StreamReader): Promise<void> {
    let reconcileSessionId: string | null
    try {
      reconcileSessionId = await this.processFrameReader(connection, reader)
    } finally {
      connection.resolveReady()
    }
    if (reconcileSessionId === null || connection.closed) return
    if (connection.pendingDispatches.size > 0) await Promise.allSettled(connection.pendingDispatches)
    const pending = this.connection.sendReconciled(connection, reconcileSessionId)
    if (pending) await pending
  }

  /** Pure body→dispatch loop. Returns the new `sessionId` if a reconcile completed in this body. */
  private async processFrameReader(connection: SseConnection, reader: StreamReader): Promise<string | null> {
    let reconcileSessionId: string | null = null
    while (true) {
      const raw = await reader.readLengthPrefixedBytesOrNull()
      if (!raw) break
      if (connection.closed) return reconcileSessionId
      const sessionId = await this.connection.onConnectionRawMessageDeferredReconciled(connection, raw)
      if (sessionId !== null) reconcileSessionId = sessionId
    }
    return reconcileSessionId
  }

  private sendNow(connection: SseConnection, frame: Uint8Array<ArrayBuffer>): void | Promise<void> {
    if (connection.closed) return
    return connection.stream.push(textEncoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`))
  }

  /** Wait for a stream POST to register a connection for `connId`. Returns null on timeout. */
  private waitForConnection(connId: string): Promise<SseConnection | null> {
    return new Promise<SseConnection | null>((resolve) => {
      let pending = this.pendingConnections.get(connId)
      if (!pending) this.pendingConnections.set(connId, (pending = []))
      pending.push(resolve)
      setTimeout(() => resolve(null), this.connection.connectTtl)
    })
  }

  private resolvePendingConnections(connId: string, connection: SseConnection | null): void {
    const pending = this.pendingConnections.get(connId)
    if (!pending) return
    this.pendingConnections.delete(connId)
    for (const resolve of pending) resolve(connection)
  }

  /** Wait for the stream POST's reconcile to complete on this connection. False on timeout. */
  private waitReady(connection: SseConnection): Promise<boolean> {
    return new Promise<boolean>((resolve) => {
      const timer = setTimeout(() => resolve(false), this.connection.connectTtl)
      connection.ready.then(() => {
        clearTimeout(timer)
        resolve(true)
      })
    })
  }

  private closeConnection(connection: SseConnection, permanent: boolean): void {
    if (connection.closed) return
    connection.closed = true
    // Unblock any data POST awaiting `ready`. Their dispatch sees the closed connection and bails.
    connection.resolveReady()
    this.connections.delete(connection.connId)
    this.connection.onConnectionClosed(connection, permanent)
    connection.stream.close()
  }

  private terminateConnection(connection: SseConnection): void {
    const terminatePermanently = this.connection.consumePermanentTermination(connection)
    this.closeConnection(connection, terminatePermanently === true)
  }
}

function badRequest(): SseChannelHttpResponse {
  return { statusCode: 400, contentType: 'text/plain', headers: [], body: '' }
}

async function handleSseChannelRequest(request: Request): Promise<SseChannelHttpResponse | null> {
  globalObject.defaultHooks ??= getTelefuncSseChannelHooks()
  return globalObject.defaultHooks.handleRequest(request)
}

function getTelefuncSseChannelHooks() {
  const server = new SseConnectionTransport()
  return {
    handleRequest(request: Request) {
      return server.handleRequest(request)
    },
  }
}
