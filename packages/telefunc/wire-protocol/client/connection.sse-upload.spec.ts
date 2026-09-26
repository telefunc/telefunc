// A browser that can't stream a request body (Firefox) sends the SSE upload POST's body as the string
// "[object ReadableStream]", and the server answers 400 without reading a frame or sending the open-ack. Frames the
// page wrote into that body after its first RECONCILED must still reach the server.

import { expect, test } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'

import { ClientConnection } from './connection.js'
import { ClientChannel } from './channel.js'
import { config as clientConfig } from '../../client/clientConfig.js'
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

test("a server close that reaches the page before its upload request settles gets the page's acknowledgement", async () => {
  const acked: number[] = []
  let ix = 0
  let serverSend!: (frame: Uint8Array) => void
  let refuseUpload!: () => void
  clientConfig.fetch = (async (_url: string, init: RequestInit) => {
    const body = init.body as unknown
    if (!(body instanceof Blob)) {
      return await new Promise<Response>((resolve) => {
        refuseUpload = () => resolve(new Response('bad request', { status: 400 }))
      })
    }
    const { metadata, frames } = await parseBlobBody(body)
    if (metadata.streamResponse) {
      for (const raw of frames) {
        const frame = decode(raw as never)
        if (frame.tag === TAG.RECONCILE) ix = frame.payload.open[0]!.ix
      }
      const encoder = new TextEncoder()
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(encoder.encode(': open\n\n'))
          serverSend = (frame) =>
            controller.enqueue(encoder.encode(`data: ${uint8ArrayToBase64url(frame as never)}\n\n`))
        },
      })
      return new Response(stream as never, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
    }
    for (const raw of frames) {
      const frame = decode(raw as never)
      if (frame.tag === TAG.CLOSE_ACK) acked.push(frame.index)
    }
    return new Response('', { status: 200 })
  }) as unknown as typeof fetch
  const channel = new ClientChannel({
    channelId: crypto.randomUUID(),
    transports: ['sse'],
    telefuncUrl: 'http://close-ack.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
  const closed = new Promise((resolve) => channel.onClose(resolve))
  await delay(20)
  serverSend(
    encode.reconciled({
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
    }),
  )
  await delay(20)
  serverSend(encode.close(ix, 5_000)) // the server's close()
  expect(await closed).toBeUndefined()
  refuseUpload() // a page that can't stream a request body (Firefox)
  await delay(100)
  expect(acked).toEqual([ix])
})

test("a server close that reaches the page before its upload request settles, while the page opens another channel, gets the page's acknowledgement first", async () => {
  // In order: 'ack:<ix>' for a CLOSE_ACK, 'reconcile:<ixes>' for a RECONCILE.
  const received: string[] = []
  let ix = 0
  let serverSend!: (frame: Uint8Array) => void
  let refuseUpload!: () => void
  clientConfig.fetch = (async (_url: string, init: RequestInit) => {
    const body = init.body as unknown
    if (!(body instanceof Blob)) {
      return await new Promise<Response>((resolve) => {
        refuseUpload = () => resolve(new Response('bad request', { status: 400 }))
      })
    }
    const { metadata, frames } = await parseBlobBody(body)
    if (metadata.streamResponse) {
      for (const raw of frames) {
        const frame = decode(raw as never)
        if (frame.tag === TAG.RECONCILE) ix = frame.payload.open[0]!.ix
      }
      const encoder = new TextEncoder()
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(encoder.encode(': open\n\n'))
          serverSend = (frame) =>
            controller.enqueue(encoder.encode(`data: ${uint8ArrayToBase64url(frame as never)}\n\n`))
        },
      })
      return new Response(stream as never, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
    }
    for (const raw of frames) {
      const frame = decode(raw as never)
      if (frame.tag === TAG.CLOSE_ACK) received.push(`ack:${frame.index}`)
      if (frame.tag === TAG.RECONCILE)
        received.push(`reconcile:${frame.payload.open.map((entry) => entry.ix).join(',')}`)
    }
    return new Response('', { status: 200 })
  }) as unknown as typeof fetch
  const telefuncUrl = 'http://close-ack-register.test/_telefunc'
  const connectionKey = crypto.randomUUID()
  const channel = new ClientChannel({ channelId: crypto.randomUUID(), transports: ['sse'], telefuncUrl, connectionKey })
  const closed = new Promise((resolve) => channel.onClose(resolve))
  await delay(20)
  serverSend(
    encode.reconciled({
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
    }),
  )
  await delay(20)
  serverSend(encode.close(ix, 5_000)) // the server's close()
  expect(await closed).toBeUndefined()
  // Another channel, such as the stream page's onUpload(file, onProgress) callback
  new ClientChannel({ channelId: crypto.randomUUID(), transports: ['sse'], telefuncUrl, connectionKey })
  await delay(20)
  refuseUpload() // a page that can't stream a request body (Firefox)
  await delay(100)
  // A RECONCILE that leaves the channel out before its CLOSE_ACK tells the server the page lost it.
  const ackAt = received.indexOf(`ack:${ix}`)
  expect(ackAt).not.toBe(-1)
  const omittingBefore = received
    .slice(0, ackAt)
    .filter(
      (entry) => entry.startsWith('reconcile:') && !entry.slice('reconcile:'.length).split(',').includes(String(ix)),
    )
  expect(omittingBefore).toEqual([])
})

test('a channel the page aborted while the frame saying so waits gets no more messages', async () => {
  let ix = 0
  let serverSend!: (frame: Uint8Array) => void
  clientConfig.fetch = (async (_url: string, init: RequestInit) => {
    const body = init.body as unknown
    if (!(body instanceof Blob)) return await new Promise<Response>(() => {}) // the upload request stays unsettled
    const { metadata, frames } = await parseBlobBody(body)
    if (!metadata.streamResponse) return new Response('', { status: 200 })
    for (const raw of frames) {
      const frame = decode(raw as never)
      if (frame.tag === TAG.RECONCILE) ix = frame.payload.open[0]!.ix
    }
    const encoder = new TextEncoder()
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode(': open\n\n'))
        serverSend = (frame) => controller.enqueue(encoder.encode(`data: ${uint8ArrayToBase64url(frame as never)}\n\n`))
      },
    })
    return new Response(stream as never, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
  }) as unknown as typeof fetch
  const channel = new ClientChannel<never, number>({
    channelId: crypto.randomUUID(),
    transports: ['sse'],
    telefuncUrl: 'http://abort-draining.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
  const received: number[] = []
  channel.listen((n) => void received.push(n))
  await delay(20)
  serverSend(
    encode.reconciled({
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
    }),
  )
  await delay(20)
  channel.abort() // its abort waits for the upload request to settle
  serverSend(encode.text(ix, stringify(1), 1))
  await delay(20)
  expect(received).toEqual([])
})
