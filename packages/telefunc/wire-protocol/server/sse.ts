export { getTelefuncSseChannelHooks, handleSseChannelRequest }
export type { SseChannelHttpResponse }

import { assert } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
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
  controller: ReadableStreamDefaultController<Uint8Array>
  pullWaiters: Array<() => void>
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
  private readonly connection = new ServerConnection<SseConnection>({
    getSessionId: (connection) => connection.sessionId ?? undefined,
    setSessionId: (connection, sessionId) => {
      connection.sessionId = sessionId
    },
    sendNow: (connection, frame) => this.sendNow(connection, frame),
    terminateConnection: (connection) => this.terminateConnection(connection),
  })

  async handleRequest(request: Request): Promise<SseChannelHttpResponse | null> {
    if (request.method !== 'POST') return { statusCode: 400, contentType: 'text/plain', headers: [], body: '' }
    const { body } = request
    assert(body)

    try {
      const reader = new StreamReader(body)
      const metadata = parseSseRequestMetadata(await reader.readMetadata())
      if (metadata.stream === true) {
        return this.openStream(metadata.connId, reader)
      }

      const connection = this.connections.get(metadata.connId)
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
    const payload = textEncoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`)
    if (!connection.closed && connection.controller.desiredSize !== null && connection.controller.desiredSize > 0) {
      connection.controller.enqueue(payload)
      return
    }
    return this._sendWhenWritable(connection, payload)
  }

  private async _sendWhenWritable(connection: SseConnection, payload: Uint8Array): Promise<void> {
    while (!connection.closed && connection.controller.desiredSize !== null && connection.controller.desiredSize <= 0) {
      await new Promise<void>((resolve) => {
        connection.pullWaiters.push(resolve)
      })
    }
    if (connection.closed) return
    connection.controller.enqueue(payload)
  }

  private openStream(connId: string, initialFrameReader: StreamReader): SseChannelHttpResponse {
    const existing = this.connections.get(connId)
    if (existing) this.closeConnection(existing, false)

    const body = new ReadableStream<Uint8Array>({
      start: (controller) => {
        const connection = {
          connId,
          controller,
          pullWaiters: [],
          closed: false,
          sessionId: null,
        }
        this.connections.set(connId, connection)
        controller.enqueue(sseOpenComment)
        this.connection.onConnectionOpen(connection)
        void this.processFrameReader(connection, initialFrameReader)
      },
      pull: () => {
        const connection = this.connections.get(connId)
        if (!connection) return
        const waiters = connection.pullWaiters.splice(0)
        for (const waiter of waiters) waiter()
      },
      cancel: () => {
        const connection = this.connections.get(connId)
        if (!connection) return
        this.closeConnection(connection, false)
      },
    })

    return {
      statusCode: 200,
      contentType: 'text/event-stream',
      headers: [
        ['Cache-Control', 'no-cache, no-transform'],
        ['X-Accel-Buffering', 'no'],
      ],
      body,
    }
  }

  private closeConnection(connection: SseConnection, permanent: boolean): void {
    if (connection.closed) return
    connection.closed = true
    connection.pullWaiters.splice(0).forEach((resolve) => resolve())
    this.connections.delete(connection.connId)
    this.connection.onConnectionClosed(connection, permanent)
    try {
      connection.controller.close()
    } catch {}
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
