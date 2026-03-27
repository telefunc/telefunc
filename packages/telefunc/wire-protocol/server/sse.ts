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
}

const textEncoder = new TextEncoder()
const sseOpenComment = textEncoder.encode(': open\n\n')

const globalObject = getGlobalObject('wire-protocol/server/sse.ts', {
  defaultHooks: null as ReturnType<typeof getTelefuncSseChannelHooks> | null,
})

class SseConnectionTransport {
  private readonly connections = new Map<string, SseConnection>()
  /** Resolvers for non-stream POSTs that arrived before the stream POST created the connection. */
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
    if (!getServerConfig().channel.transports.includes(CHANNEL_TRANSPORT.SSE)) {
      return { statusCode: 400, contentType: 'text/plain', headers: [], body: '' }
    }
    if (request.method !== 'POST') return { statusCode: 400, contentType: 'text/plain', headers: [], body: '' }
    const { body } = request
    assert(body)

    try {
      const reader = new StreamReader(body)
      const metadata = parseSseRequestMetadata(await reader.readMetadata())
      if (metadata.stream === true) {
        return this.openStream(metadata.connId, reader)
      }

      const connection = this.connections.get(metadata.connId) ?? (await this.waitForConnection(metadata.connId))
      if (!connection) {
        return { statusCode: 400, contentType: 'text/plain', headers: [], body: '' }
      }

      await this.processFrameReader(connection, reader)
      return { statusCode: 200, contentType: 'text/plain', headers: [], body: '' }
    } catch {
      return { statusCode: 400, contentType: 'text/plain', headers: [], body: '' }
    }
  }

  private sendNow(connection: SseConnection, frame: Uint8Array<ArrayBuffer>): void | Promise<void> {
    if (connection.closed) return
    const payload = textEncoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`)
    return connection.stream.push(payload)
  }

  /** Wait briefly for a connection to be created by a concurrent stream POST. */
  private waitForConnection(connId: string): Promise<SseConnection | null> {
    return new Promise<SseConnection | null>((resolve) => {
      let pending = this.pendingConnections.get(connId)
      if (!pending) {
        pending = []
        this.pendingConnections.set(connId, pending)
      }
      pending.push(resolve)
      // Don't wait forever — if the stream POST never arrives, give up.
      setTimeout(() => resolve(null), this.connection.connectTtl)
    })
  }

  private resolvePendingConnections(connId: string, connection: SseConnection | null): void {
    const pending = this.pendingConnections.get(connId)
    if (!pending) return
    this.pendingConnections.delete(connId)
    for (const resolve of pending) resolve(connection)
  }

  private openStream(connId: string, initialFrameReader: StreamReader): SseChannelHttpResponse {
    const existing = this.connections.get(connId)
    if (existing) this.closeConnection(existing, false)

    const stream = new PushToPullStream<Uint8Array>(() => {
      const conn = this.connections.get(connId)
      if (conn) this.closeConnection(conn, false)
    })
    const connection: SseConnection = {
      connId,
      stream,
      closed: false,
      sessionId: null,
    }
    this.connections.set(connId, connection)
    this.resolvePendingConnections(connId, connection)
    stream.push(sseOpenComment)
    this.connection.onConnectionOpen(connection)
    void this.processFrameReader(connection, initialFrameReader)

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

  private closeConnection(connection: SseConnection, permanent: boolean): void {
    if (connection.closed) return
    connection.closed = true
    this.connections.delete(connection.connId)
    this.connection.onConnectionClosed(connection, permanent)
    connection.stream.close()
  }

  private terminateConnection(connection: SseConnection): void {
    const terminatePermanently = this.connection.consumePermanentTermination(connection)
    this.closeConnection(connection, terminatePermanently === true)
  }

  private async processFrameReader(connection: SseConnection, reader: StreamReader): Promise<void> {
    let reconcileSessionId: string | null = null

    while (true) {
      const raw = await reader.readLengthPrefixedBytesOrNull()
      if (!raw) break
      if (connection.closed) return
      const sessionId = await this.connection.onConnectionRawMessageDeferredReconciled(connection, raw)
      if (sessionId !== null) reconcileSessionId = sessionId
    }

    if (reconcileSessionId !== null && !connection.closed) {
      const pending = this.connection.sendReconciled(connection, reconcileSessionId)
      if (pending) await pending
    }
  }
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
