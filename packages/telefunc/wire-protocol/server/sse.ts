export { getTelefuncSseChannelHooks, handleSseChannelRequest }
export type { SseChannelHttpResponse }

import { assert } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT } from '../constants.js'
import { PushToPullStream } from '../push-to-pull-stream.js'
import { uint8ArrayToBase64url } from '../base64url.js'
import {
  METADATA_REFRESH_ALIAS,
  parseSseChannels,
  parseSseRequestMetadata,
  type SseDataPostMetadata,
} from '../sse-request.js'
import { StreamReader } from './request/StreamReader.js'
import { getChannelMux } from './substrate.js'
import type { ServerTransport } from './substrate-runtime.js'

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
  /** Resolved by `runStreamResponse` once the stream-response POST's body is consumed.
   *  Data POSTs gate on this before dispatching frames, so they can't race ahead of the
   *  reconcile and hit a `getSessionStateOrThrow(undefined)`. */
  ready: Promise<void>
  resolveReady: () => void
  /** Data POSTs whose dispatch is in flight. Drained by `runStreamResponse` before
   *  `sendReconciled`, so any `entry.lastClientSeq` mutations from those dispatches are
   *  reflected in the `reconciled` frame's reported `lastSeq`. */
  pendingDispatches: Set<Promise<unknown>>
}

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()
const sseOpenComment = textEncoder.encode(': open\n\n')

const globalObject = getGlobalObject('wire-protocol/server/sse.ts', {
  defaultHooks: null as ReturnType<typeof getTelefuncSseChannelHooks> | null,
})

class SseConnectionTransport {
  /** Resolvers for data POSTs that arrived before the stream-response POST registered the
   *  connection. Same-instance race only — the SSE stream-response POST and the persistent
   *  stream-request POST open in parallel on the client, so the stream-request can land
   *  before the stream-response registers. */
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

  async handleRequest(request: Request): Promise<SseChannelHttpResponse | null> {
    if (!getServerConfig().channel.transports.includes(CHANNEL_TRANSPORT.SSE)) return badRequest()
    if (request.method !== 'POST') return badRequest()
    const { body } = request
    assert(body)
    try {
      const reader = new StreamReader(body)
      const metadata = parseSseRequestMetadata(await reader.readMetadata())
      if ('streamResponse' in metadata) return this.handleStreamResponse(metadata.connId, reader)
      return await this.handleDataPost(metadata, reader)
    } catch {
      return badRequest()
    }
  }

  /** Stream-response POST — opens the SSE downstream, registers the connection, and
   *  asynchronously drives its lifecycle (`runStreamResponse`). Returns immediately so
   *  the response headers can flush. */
  private handleStreamResponse(connId: string, reader: StreamReader): SseChannelHttpResponse {
    const existing = this.mux.getConnectionByConnId<SseConnection>(connId)
    if (existing) this.closeConnection(existing, false)

    const stream = new PushToPullStream<Uint8Array>(() => {
      const conn = this.mux.getConnectionByConnId<SseConnection>(connId)
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
      body: stream.readable,
    }
  }

  /** Data POST — orchestration: resolve the local connection (owner-side only), run the
   *  alias-routing body loop in `processDataPostBody`, then emit a deferred `reconciled`
   *  if one was captured. Tracked in `pendingDispatches` so `runStreamResponse` can drain
   *  in-flight POSTs before its own `sendReconciled`. */
  private async handleDataPost(metadata: SseDataPostMetadata, reader: StreamReader): Promise<SseChannelHttpResponse> {
    const isOwner = metadata.ownerInstance === this.mux.selfInstanceId
    const localConnection = isOwner
      ? (this.mux.getConnectionByConnId<SseConnection>(metadata.connId) ??
        (await this.waitForConnection(metadata.connId)))
      : null
    if (isOwner && !localConnection) return badRequest()

    let deferredReconcileSessionId: string | null = null
    const dispatch = (async (): Promise<'ok' | 'timeout'> => {
      if (localConnection && !(await this.waitReady(localConnection))) return 'timeout'
      deferredReconcileSessionId = await this.processDataPostBody(metadata, localConnection, reader)
      return 'ok'
    })()
    if (localConnection) localConnection.pendingDispatches.add(dispatch)
    try {
      if ((await dispatch) === 'timeout') return badRequest()
    } finally {
      if (localConnection) localConnection.pendingDispatches.delete(dispatch)
    }
    if (deferredReconcileSessionId !== null && localConnection && !localConnection.closed) {
      const pending = this.mux.sendReconciled(localConnection, deferredReconcileSessionId)
      if (pending) await pending
    }
    return { statusCode: 200, contentType: 'text/plain', headers: [], body: '' }
  }

  /** Body loop for data POSTs. Each entry is `[u8 alias][bytes]`:
   *    alias `0`     — connection-level frame (ping, in-body reconcile)
   *    alias `1..FE` — channel-data frame, routes to `channels[alias − 1]`
   *    alias `0xFF`  — JSON `channels[]` refresh, swaps the routing table in-band
   *
   *  Returns the `sessionId` of a deferred reconcile if one fired in this body so the
   *  caller can emit `reconciled` at body end with all dispatched frames' `lastSeq`
   *  reflected. Long-lived stream-request POSTs use the inline path instead — their
   *  body never ends, so deferring would never emit. */
  private async processDataPostBody(
    metadata: SseDataPostMetadata,
    localConnection: SseConnection | null,
    reader: StreamReader,
  ): Promise<string | null> {
    let channels = metadata.channels
    let deferredReconcileSessionId: string | null = null
    while (true) {
      const raw = await reader.readLengthPrefixedBytesOrNull()
      if (!raw || raw.byteLength === 0) break
      if (localConnection?.closed) break
      const alias = raw[0] as number
      const payload = raw.subarray(1) as Uint8Array<ArrayBuffer>

      if (alias === METADATA_REFRESH_ALIAS) {
        channels = parseSseChannels(JSON.parse(textDecoder.decode(payload)))
        continue
      }
      if (alias === 0) {
        const sessionId = await this.dispatchConnectionFrame(metadata, localConnection, payload)
        if (sessionId !== null) deferredReconcileSessionId = sessionId
        continue
      }
      const channel = channels[alias - 1]
      if (channel) await this.mux.routeClientFrame(channel.id, channel.home, payload)
    }
    return deferredReconcileSessionId
  }

  /** Alias-0 dispatch: forward to the owner if the connection lives elsewhere, dispatch
   *  inline on a long-lived stream-request POST (so `reconciled` emits without waiting
   *  for body-end), or defer on a short-lived outbox batch (so subsequent frames in the
   *  same body lift `lastSeq` before `reconciled` is sent). Returns the deferred reconcile
   *  `sessionId` only on the deferred path. */
  private async dispatchConnectionFrame(
    metadata: SseDataPostMetadata,
    localConnection: SseConnection | null,
    payload: Uint8Array<ArrayBuffer>,
  ): Promise<string | null> {
    if (!localConnection) {
      await this.mux.forwardConnectionFrame(metadata.ownerInstance, metadata.connId, payload)
      return null
    }
    if (metadata.streamRequest) {
      await this.mux.onConnectionRawMessage(localConnection, payload)
      return null
    }
    return this.mux.onConnectionRawMessageDeferredReconciled(localConnection, payload)
  }

  /** Wait for the stream-response POST to register a connection for `connId` on this
   *  instance. Used only for the same-instance race where the stream-request POST and the
   *  stream-response POST both hit this server but the stream-request arrives first.
   *  Cross-instance POSTs never reach here — `handleDataPost` reads `ownerInstance` from
   *  metadata and forwards directly. */
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

  /** Stream-response POST lifecycle: consume the initial batch (no alias prefixes —
   *  frames flow through `onConnectionRawMessage` so the mux's session registry routes
   *  them), release the `ready` gate, drain in-flight data POSTs, then emit `reconciled`. */
  private async runStreamResponse(connection: SseConnection, reader: StreamReader): Promise<void> {
    let reconcileSessionId: string | null = null
    try {
      while (true) {
        const raw = await reader.readLengthPrefixedBytesOrNull()
        if (!raw) break
        if (connection.closed) break
        const sessionId = await this.mux.onConnectionRawMessageDeferredReconciled(connection, raw)
        if (sessionId !== null) reconcileSessionId = sessionId
      }
    } finally {
      connection.resolveReady()
    }
    if (reconcileSessionId === null || connection.closed) return
    if (connection.pendingDispatches.size > 0) await Promise.allSettled(connection.pendingDispatches)
    const pending = this.mux.sendReconciled(connection, reconcileSessionId)
    if (pending) await pending
  }

  private sendNow(connection: SseConnection, frame: Uint8Array<ArrayBuffer>): void | Promise<void> {
    if (connection.closed) return
    return connection.stream.push(textEncoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`))
  }

  /** Wait for the stream POST's reconcile to complete on this connection. False on timeout. */
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
    // Unblock any data POST awaiting `ready`. Their dispatch sees the closed connection and bails.
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
