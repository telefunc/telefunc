// A browser that can't stream a request body (Firefox) sends the SSE upload POST's body as the string
// "[object ReadableStream]", and the server answers 400 without reading a frame or sending the open-ack. Frames the
// page wrote into that body after its first RECONCILED must still reach the server.

import { expect, test } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'

import { ClientConnection } from './connection.js'
import { ServerChannel } from '../server/channel.js'
import { decode, encode, TAG } from '../shared-ws.js'
import { decodeU32 } from '../frame.js'
import { uint8ArrayToBase64url } from '../base64url.js'

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

async function parseBlobBody(blob: Blob): Promise<{ metadata: { streamResponse?: boolean }; frames: Uint8Array[] }> {
  const bytes = new Uint8Array(await blob.arrayBuffer())
  let offset = 0
  const next = (): Uint8Array => {
    const length = decodeU32(bytes.subarray(offset, offset + 4) as never)
    offset += 4
    const chunk = bytes.subarray(offset, offset + length)
    offset += length
    return chunk
  }
  const metadata = JSON.parse(new TextDecoder().decode(next()))
  const frames: Uint8Array[] = []
  while (offset < bytes.length) frames.push(next())
  return { metadata, frames }
}

test('a frame written into an upload POST the server refused before its open-ack is sent again', async () => {
  const received: number[] = []
  const server = new ServerChannel<number, never>()
  server.listen((n) => void received.push(n))
  let refuseUpload!: () => void
  let sendReconciled!: () => void

  const fetchImpl = (async (_url: string, init: RequestInit) => {
    const body = init.body as unknown
    if (body instanceof Blob) {
      const { metadata, frames } = await parseBlobBody(body)
      if (metadata.streamResponse) {
        let ix = 0
        for (const raw of frames) {
          const frame = decode(raw as never)
          if (frame.tag === TAG.RECONCILE) ix = frame.payload.open[0]!.ix
        }
        const encoder = new TextEncoder()
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(encoder.encode(': open\n\n'))
            sendReconciled = () => {
              const reconciled = encode.reconciled({
                sessionId: crypto.randomUUID(),
                open: [{ ix, lastSeq: 0 }],
                reconnectTimeout: 60_000,
                idleTimeout: 60_000,
                pingInterval: 100_000,
                clientReplayBuffer: 1_000_000,
                clientReplayBufferBinary: 2_000_000,
                sseFlushThrottle: 0,
                ssePostIdleFlushDelay: 0,
                transports: ['sse'],
              })
              controller.enqueue(encoder.encode(`data: ${uint8ArrayToBase64url(reconciled as never)}\n\n`))
            }
          },
        })
        return new Response(stream as never, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
      }
      for (const raw of frames) {
        const frame = decode(raw as never)
        if (frame.tag === TAG.TEXT) server._dispatchFrame(frame)
      }
      return new Response('', { status: 200 })
    }
    // The upload POST: refused, unread, once the test says so.
    return await new Promise<Response>((resolve) => {
      refuseUpload = () => resolve(new Response('bad request', { status: 400 }))
    })
  }) as unknown as typeof fetch

  const channel = { id: server.id, isClosed: false, _onTransportOpen() {}, _dispatchFrame() {}, _onTransportClose() {} }
  const connection = ClientConnection.getOrCreate('http://upload.test/_telefunc', channel as never, {
    transports: ['sse'],
    fetchImpl,
    connectionKey: crypto.randomUUID(),
  }) as any
  await delay(20)
  connection.send(channel, stringify(7)) // before RECONCILED: released into the upload body once it arrives
  sendReconciled()
  await delay(20)
  refuseUpload()
  await delay(100)
  expect(received).toEqual([7])
  connection.dispose()
})

test("a reconnect's wire gets its own connection id, so a POST still in flight for the dead wire isn't adopted by it", async () => {
  const wires: { connId: string; close: () => void }[] = []
  const fetchImpl = (async (_url: string, init: RequestInit) => {
    const body = init.body as unknown
    if (!(body instanceof Blob)) return await new Promise<Response>(() => {}) // the upload POST stays open
    const { metadata, frames } = await parseBlobBody(body)
    if (!metadata.streamResponse) return new Response('', { status: 200 })
    let ix = 0
    for (const raw of frames) {
      const frame = decode(raw as never)
      if (frame.tag === TAG.RECONCILE) ix = frame.payload.open[0]?.ix ?? 0
    }
    let controller!: ReadableStreamDefaultController<Uint8Array>
    const stream = new ReadableStream<Uint8Array>({
      start(c) {
        controller = c
        const encoder = new TextEncoder()
        c.enqueue(encoder.encode(': open\n\n'))
        const openAck = encode.streamRequestOpenAck()
        c.enqueue(encoder.encode(`data: ${uint8ArrayToBase64url(openAck as never)}\n\n`))
        const reconciled = encode.reconciled({
          sessionId: crypto.randomUUID(),
          open: [{ ix, lastSeq: 0 }],
          reconnectTimeout: 60_000,
          idleTimeout: 60_000,
          pingInterval: 100_000,
          clientReplayBuffer: 1_000_000,
          clientReplayBufferBinary: 2_000_000,
          sseFlushThrottle: 0,
          ssePostIdleFlushDelay: 0,
          transports: ['sse'],
        })
        c.enqueue(encoder.encode(`data: ${uint8ArrayToBase64url(reconciled as never)}\n\n`))
      },
    })
    wires.push({ connId: (metadata as { connId: string }).connId, close: () => controller.close() })
    return new Response(stream as never, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
  }) as unknown as typeof fetch
  const channel = {
    id: crypto.randomUUID(),
    isClosed: false,
    _onTransportOpen() {},
    _dispatchFrame() {},
    _onTransportClose() {},
  }
  const connection = ClientConnection.getOrCreate('http://conn-id.test/_telefunc', channel as never, {
    transports: ['sse'],
    fetchImpl,
    connectionKey: crypto.randomUUID(),
  }) as any
  await delay(20)
  wires[0]!.close() // the wire dies; the client reconnects
  await delay(1_500)
  expect(wires.length).toBeGreaterThan(1)
  expect(wires[1]!.connId).not.toBe(wires[0]!.connId)
  connection.dispose()
})
