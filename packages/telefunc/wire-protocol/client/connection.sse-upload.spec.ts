import { afterEach, expect, test, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'

import { ClientConnection } from './connection.js'
import { ClientBroadcast, ClientChannel } from './channel.js'
import { config } from '../../client/clientConfig.js'
import { ServerChannel } from '../server/channel.js'
import { ServerBroadcast } from '../server/server-broadcast.js'
import { getChannelMux } from '../server/mux.js'
import { getTelefuncSseChannelHooks } from '../server/sse.js'
import { decode, encode, TAG, type SeqReader } from '../shared-ws.js'
import { decodeU32 } from '../frame.js'
import { uint8ArrayToBase64url } from '../base64url.js'

/** A receiver with nothing of any channel: each seq reads as its low 32 bits. */
const wireSeqs: SeqReader = { received: () => 0, sent: () => 0 }

afterEach(() => {
  delete config.fetch
})

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

/** A server the test drives: it streams what the test sends down the latest SSE wire, hands each frame of a batch
 *  POST to `onBatchFrame`, and leaves the upload POST unsettled until the test settles it. */
function fakeServer(onBatchFrame: (frame: ReturnType<typeof decode>) => void = () => {}) {
  const encoder = new TextEncoder()
  const server = {
    /** The index the latest wire's RECONCILE opened. */
    ix: 0,
    wires: 0,
    send(_frame: Uint8Array) {},
    reconcile: () =>
      server.send(
        encode.reconciled({
          sessionId: crypto.randomUUID(),
          open: [{ ix: server.ix, lastSeq: 0 }],
          reconnectTimeout: 60_000,
          idleTimeout: 60_000,
          pingInterval: 100_000,
          serverReplayBuffer: 1_000_000,
          serverReplayBufferBinary: 2_000_000,
          clientReplayBuffer: 1_000_000,
          clientReplayBufferBinary: 2_000_000,
          sseFlushThrottle: 0,
          ssePostIdleFlushDelay: 0,
          transports: ['sse'],
        }),
      ),
    // A browser that can't stream a request body (Firefox) sends it as "[object ReadableStream]", and the server
    // answers 400 without reading a frame or sending the open-ack.
    refuseUpload: () => server.settleUpload(new Response('bad request', { status: 400 })),
    settleUpload(_response: Response) {},
    fetch: (async (_url: string, init: RequestInit) => {
      const body = init.body as unknown
      if (!(body instanceof Blob)) return await new Promise<Response>((resolve) => (server.settleUpload = resolve))
      const { metadata, frames } = await parseBlobBody(body)
      if (!metadata.streamResponse) {
        for (const raw of frames) onBatchFrame(decode(raw as never, wireSeqs))
        return new Response('', { status: 200 })
      }
      server.wires++
      for (const raw of frames) {
        const frame = decode(raw as never, wireSeqs)
        if (frame.tag === TAG.RECONCILE) server.ix = frame.payload.open[0]?.ix ?? 0
      }
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(encoder.encode(': open\n\n'))
          server.send = (frame) =>
            controller.enqueue(encoder.encode(`data: ${uint8ArrayToBase64url(frame as never)}\n\n`))
        },
      })
      return new Response(stream as never, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
    }) as unknown as typeof fetch,
  }
  return server
}

/** Minimal `MuxChannel`. */
const createChannel = (id: string = crypto.randomUUID()) => ({
  id,
  isClosed: false,
  _onTransportOpen() {},
  _dispatchFrame() {},
  _onTransportClose() {},
})

test('a frame written into an upload POST the server refused before its open-ack is sent again', async () => {
  const received: number[] = []
  const serverChannel = new ServerChannel<number, never>()
  serverChannel.listen((n) => void received.push(n))
  const server = fakeServer((frame) => {
    if (frame.tag === TAG.TEXT) serverChannel._dispatchFrame(frame)
  })
  const channel = createChannel(serverChannel.id)
  const connection = ClientConnection.getOrCreate('http://upload.test/_telefunc', channel as never, {
    transports: ['sse'],
    fetchImpl: server.fetch,
    connectionKey: crypto.randomUUID(),
  }) as any
  await delay(20)
  connection.send(channel, stringify(7)) // before RECONCILED: released into the upload body once it arrives
  server.reconcile()
  await delay(20)
  server.refuseUpload()
  await delay(100)
  expect(received).toEqual([7])
  connection.dispose()
})

test("a batch POST still in flight for a dead wire can't unsubscribe the listener the page swapped in across the reconnect", async () => {
  const sse = getTelefuncSseChannelHooks()
  const toServer = async (body: Blob): Promise<Response> => {
    const response = (await sse.handleRequest(new Request('http://localhost/_telefunc', { method: 'POST', body })))!
    return new Response(response.body as never, {
      status: response.statusCode,
      headers: { 'Content-Type': response.contentType },
    })
  }
  let wires = 0
  let cutWire = () => {}
  let holdBatches = false
  const held: Blob[] = []
  config.fetch = (async (_url: string, init: RequestInit) => {
    const body = init.body as unknown
    // A browser that can't stream a request body (Firefox, Safari) is answered 400: the page sends batch POSTs.
    if (!(body instanceof Blob)) return new Response('bad request', { status: 400 })
    if ((await parseBlobBody(body)).metadata.streamResponse) {
      wires++
      const reader = (await toServer(body)).body!.getReader()
      let cut = false
      const wire = new ReadableStream<Uint8Array>({
        start(controller) {
          cutWire = () => {
            cut = true
            controller.close()
          }
          void (async () => {
            for (let read = await reader.read(); !read.done && !cut; read = await reader.read())
              controller.enqueue(read.value)
          })()
        },
      })
      return new Response(wire, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
    }
    if (!holdBatches) return await toServer(body)
    // Sent into a wire that died silently: the server gets it late, whatever the page does meanwhile.
    held.push(body)
    return await new Promise<Response>((_resolve, reject) =>
      init.signal?.addEventListener('abort', () => reject(init.signal!.reason)),
    )
  }) as unknown as typeof fetch
  const key = 'chat:swapped-listener'
  const server = new ServerBroadcast<string>({ key })
  getChannelMux().registerChannel(server)
  const page = new ClientBroadcast<string>({
    channelId: server.id,
    key,
    transports: ['sse'],
    telefuncUrl: 'http://swap.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
  const first: string[] = []
  const offFirst = page.subscribe((message) => void first.push(message))
  await vi.waitFor(async () => {
    await server.publish('before')
    expect(first).toContain('before')
  })
  holdBatches = true
  offFirst()
  const second: string[] = []
  page.subscribe((message) => void second.push(message))
  await vi.waitFor(() => expect(held).toHaveLength(1))
  cutWire()
  await vi.waitFor(() => expect(wires).toBe(2), { timeout: 5_000 })
  await vi.waitFor(() => expect((page as any)._connection.state.tag).toBe('open'))
  // The dead wire's POST, the unsubscribe in it, arrives now. A server that doesn't take it holds it for connectTtl.
  await Promise.race([Promise.all(held.map(toServer)), delay(1_000)])
  await server.publish('after')
  await vi.waitFor(() => expect(second).toEqual(['after']))
  page.abort()
})

test("a reconnect's wire gets its own connection id, so a POST still in flight for the dead wire isn't adopted by it", async () => {
  const connIds: string[] = []
  const fetchImpl = (async (_url: string, init: RequestInit) => {
    if (init.body instanceof Blob)
      connIds.push(((await parseBlobBody(init.body)).metadata as { connId: string }).connId)
    return await new Promise<Response>(() => {})
  }) as unknown as typeof fetch
  const connection = ClientConnection.getOrCreate('http://conn-id.test/_telefunc', createChannel() as never, {
    transports: ['sse'],
    fetchImpl,
    connectionKey: crypto.randomUUID(),
  }) as any
  await vi.waitFor(() => expect(connIds).toHaveLength(1))
  void connection.transport.openStream()
  await vi.waitFor(() => expect(connIds).toHaveLength(2))
  expect(connIds[1]).not.toBe(connIds[0])
  connection.dispose()
})

test("a server close that reaches the page before its upload request settles gets the page's acknowledgement", async () => {
  const acked: number[] = []
  const server = fakeServer((frame) => {
    if (frame.tag === TAG.CLOSE_ACK) acked.push(frame.index)
  })
  config.fetch = server.fetch
  const channel = new ClientChannel({
    channelId: crypto.randomUUID(),
    transports: ['sse'],
    telefuncUrl: 'http://close-ack.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
  const closed = new Promise((resolve) => channel.onClose(resolve))
  await delay(20)
  server.reconcile()
  await delay(20)
  server.send(encode.close(server.ix, 5_000, 1)) // the server's close()
  expect(await closed).toBeUndefined()
  server.refuseUpload()
  await delay(100)
  expect(acked).toEqual([server.ix])
  expect((channel as any)._connection.ttl).not.toBe(null) // nothing holds the connection once its acknowledgement went out
})

test("a page's last channel gets its close acknowledgement out on SSE batch POSTs, though its idle timeout is 0", async () => {
  const acked: number[] = []
  const server = fakeServer((frame) => {
    if (frame.tag === TAG.CLOSE_ACK) acked.push(frame.index)
  })
  // A request aborted before the server read it (here, within 5 ms of the call) never arrives.
  config.fetch = (async (url: string, init: RequestInit) => {
    await delay(5)
    if (init.signal?.aborted) throw new DOMException('The operation was aborted.', 'AbortError')
    return server.fetch(url, init)
  }) as typeof fetch
  const channel = new ClientChannel({
    channelId: crypto.randomUUID(),
    transports: ['sse'],
    telefuncUrl: 'http://idle-zero.test/_telefunc',
    connectionKey: crypto.randomUUID(),
    idleTimeout: 0,
  })
  const closed = new Promise((resolve) => channel.onClose(resolve))
  await delay(20)
  server.reconcile()
  await delay(20)
  server.refuseUpload()
  await delay(20)
  server.send(encode.close(server.ix, 5_000, 1)) // the server's close()
  expect(await closed).toBeUndefined()
  await delay(100)
  expect(acked).toEqual([server.ix])
})

test("a server close that reaches the page before its upload request settles, while the page opens another channel, gets the page's acknowledgement first", async () => {
  // In order: 'ack:<ix>' for a CLOSE_ACK, 'reconcile:<ixes>' for a RECONCILE.
  const received: string[] = []
  const server = fakeServer((frame) => {
    if (frame.tag === TAG.CLOSE_ACK) received.push(`ack:${frame.index}`)
    if (frame.tag === TAG.RECONCILE) received.push(`reconcile:${frame.payload.open.map((entry) => entry.ix).join(',')}`)
  })
  config.fetch = server.fetch
  const telefuncUrl = 'http://close-ack-register.test/_telefunc'
  const connectionKey = crypto.randomUUID()
  const channel = new ClientChannel({ channelId: crypto.randomUUID(), transports: ['sse'], telefuncUrl, connectionKey })
  const closed = new Promise((resolve) => channel.onClose(resolve))
  await delay(20)
  server.reconcile()
  await delay(20)
  const { ix } = server
  server.send(encode.close(ix, 5_000, 1)) // the server's close()
  expect(await closed).toBeUndefined()
  // Another channel, such as the stream page's onUpload(file, onProgress) callback
  new ClientChannel({ channelId: crypto.randomUUID(), transports: ['sse'], telefuncUrl, connectionKey })
  await delay(20)
  server.refuseUpload()
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
  const server = fakeServer()
  config.fetch = server.fetch
  const channel = new ClientChannel<never, number>({
    channelId: crypto.randomUUID(),
    transports: ['sse'],
    telefuncUrl: 'http://abort-draining.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
  const received: number[] = []
  channel.listen((n) => void received.push(n))
  await delay(20)
  server.reconcile()
  await delay(20)
  channel.abort() // its abort waits for the upload request to settle
  server.send(encode.text(server.ix, stringify(1), 1))
  await delay(20)
  expect(received).toEqual([])
})

test('a channel the page aborted before the server confirmed it gets none of what the server sent it first', async () => {
  const server = fakeServer()
  config.fetch = server.fetch
  const channel = new ClientChannel<never, number>({
    channelId: crypto.randomUUID(),
    transports: ['sse'],
    telefuncUrl: 'http://abort-releasing.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
  const received: number[] = []
  channel.listen((n) => void received.push(n))
  await delay(20)
  channel.abort() // as a StrictMode effect's cleanup does, before the RECONCILED
  server.send(encode.text(server.ix, stringify(1), 1)) // what the server queued before the page connected
  await delay(20)
  expect(received).toEqual([])
})

test("an upload request the server ends after its open-ack, as Node's requestTimeout does, replaces the wire", async () => {
  const server = fakeServer()
  const connection = ClientConnection.getOrCreate('http://upload-cut.test/_telefunc', createChannel() as never, {
    transports: ['sse'],
    fetchImpl: server.fetch,
    connectionKey: crypto.randomUUID(),
  }) as any
  await delay(20)
  server.send(encode.streamRequestOpenAck())
  server.reconcile()
  await delay(20)
  expect(server.wires).toBe(1)
  // The SSE stream stays up; what the page writes into the upload no longer reaches the server.
  server.settleUpload(new Response('', { status: 408 }))
  await delay(1_500)
  expect(server.wires).toBe(2)
  connection.dispose()
})
