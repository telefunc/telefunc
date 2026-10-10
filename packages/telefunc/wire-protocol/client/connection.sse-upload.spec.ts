import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'

import { ClientConnection } from './connection.js'
import { SSE_POST_TARGET_MS, STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS, WIRE_MAX_RAW_FRAME_BYTES } from '../constants.js'
import { ClientBroadcast, ClientChannel } from './channel.js'
import { config } from '../../client/clientConfig.js'
import { ServerChannel } from '../server/channel.js'
import { ServerBroadcast } from '../server/server-broadcast.js'
import { getChannelMux } from '../server/mux.js'
import { getTelefuncSseChannelHooks } from '../server/sse.js'
import { decode, encode, TAG } from '../shared-ws.js'
import { decodeU32 } from '../frame.js'
import { uint8ArrayToBase64url } from '../base64url.js'

afterEach(() => {
  delete config.fetch
  vi.useRealTimers()
})

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** Advances the fake clock until `done()`, for at most `ms`. */
async function advanceUntil(done: () => boolean, ms: number) {
  for (let waited = 0; waited < ms && !done(); waited += 10) await vi.advanceTimersByTimeAsync(10)
}

async function parseBlobBody(
  blob: Blob,
): Promise<{ metadata: { connId: string; streamResponse?: boolean }; frames: Uint8Array[] }> {
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

/** The server's RECONCILED, attaching `ixes`, which have of the page's frames up to `lastSeqs`'s. */
const reconciled = (ixes: number[], lastSeqs = new Map<number, number>()) =>
  encode.reconciled({
    sessionId: crypto.randomUUID(),
    open: ixes.map((ix) => ({ ix, lastSeq: lastSeqs.get(ix) ?? 0 })),
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
  })

/** A server the test drives: it streams what the test sends down the latest SSE wire, hands each frame of a batch
 *  POST to `onBatchFrame`, and leaves the upload POST unsettled until the test settles it. What the test sends before
 *  the page's first SSE request reaches it goes out once it does, and an upload the test settles before the page's
 *  request reaches it is settled as it does, as a server can only answer requests it has. `uplinkBytesPerS`: a frame of a
 *  batch POST reaches it as its last byte crosses, counted from the POST's start, and the POST is answered once they all
 *  did. */
function fakeServer(onBatchFrame: (frame: ReturnType<typeof decode>) => void = () => {}, uplinkBytesPerS = Infinity) {
  const encoder = new TextEncoder()
  let write: ((frame: Uint8Array) => void) | null = null
  /** Each wire, in the order the page opened them. */
  const opened: { connId: string; write: (frame: Uint8Array) => void }[] = []
  const cut = new Set<string>()
  const unwritten: (() => Uint8Array)[] = []
  const emit = (frame: () => Uint8Array) => (write ? write(frame()) : void unwritten.push(frame))
  let pendingUpload: ((response: Response) => void) | null = null
  let uploadSettlement: Response | null = null
  const server = {
    /** The index the latest wire's RECONCILE opened. */
    ix: 0,
    wires: 0,
    send: (frame: Uint8Array) => emit(() => frame),
    /** Sends on the `wire`-th wire the page opened, from 0, which it may have given up. */
    sendOnWire: (wire: number, frame: Uint8Array) => opened[wire]!.write(frame),
    /** The latest wire's POSTs are answered 400 from now on, as the server does once it cut the wire. */
    cutWire: () => void cut.add(opened.at(-1)!.connId),
    reconcile: () => emit(() => reconciled([server.ix])),
    // A browser that can't stream a request body (Firefox) sends it as "[object ReadableStream]", and the server
    // answers 400 without reading a frame or sending the open-ack.
    refuseUpload: () => server.settleUpload(new Response('bad request', { status: 400 })),
    settleUpload(response: Response) {
      if (pendingUpload === null) uploadSettlement = response
      else pendingUpload(response)
      pendingUpload = null
    },
    fetch: (async (_url: string, init: RequestInit) => {
      const sentAt = Date.now()
      const body = init.body as unknown
      if (!(body instanceof Blob)) {
        const settlement = uploadSettlement
        uploadSettlement = null
        return settlement ?? (await new Promise<Response>((resolve) => (pendingUpload = resolve)))
      }
      const { metadata, frames } = await parseBlobBody(body)
      if (!metadata.streamResponse) {
        let crossed = 0
        for (const raw of frames) {
          crossed += raw.byteLength
          if (uplinkBytesPerS !== Infinity) await delay(sentAt + (crossed * 1000) / uplinkBytesPerS - Date.now())
          onBatchFrame(decode(raw as never))
        }
        return new Response('', { status: cut.has(metadata.connId) ? 400 : 200 })
      }
      server.wires++
      for (const raw of frames) {
        const frame = decode(raw as never)
        if (frame.tag === TAG.RECONCILE) server.ix = frame.payload.open[0]?.ix ?? 0
      }
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(encoder.encode(': open\n\n'))
          write = (frame) => controller.enqueue(encoder.encode(`data: ${uint8ArrayToBase64url(frame as never)}\n\n`))
          opened.push({ connId: metadata.connId, write })
          for (const frame of unwritten.splice(0)) write(frame())
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
  _maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
  _onTransportOpen() {},
  _dispatchFrame() {},
  _onTransportClose() {},
  _onTransportBatched() {},
  _reattachState: () => ({}),
  _fitReplays() {},
  _acknowledge() {},
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

describe('an upload POST whose open-ack never comes', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  /** A page whose upload POST gets no open-ack: it writes 7 into the upload's body, gives the upload up, then sends 8. */
  async function pageGivingUpItsUpload(name: string) {
    const received: number[] = []
    const serverChannel = new ServerChannel<number, never>()
    serverChannel.listen((n) => void received.push(n))
    const server = fakeServer((frame) => {
      if (frame.tag === TAG.TEXT) serverChannel._dispatchFrame(frame)
    })
    const uploaded: Uint8Array[] = []
    const fetchImpl = ((url: string, init: RequestInit) => {
      if (!(init.body instanceof Blob))
        void (async () => {
          for await (const chunk of init.body as ReadableStream<Uint8Array>) uploaded.push(chunk)
        })()
      return server.fetch(url, init)
    }) as typeof fetch
    const channel = createChannel(serverChannel.id)
    const connection = ClientConnection.getOrCreate(`http://${name}.test/_telefunc`, channel as never, {
      transports: ['sse'],
      fetchImpl,
      connectionKey: crypto.randomUUID(),
    }) as any
    await vi.advanceTimersByTimeAsync(20)
    connection.send(channel, stringify(7)) // before RECONCILED: released into the upload body once it arrives
    server.reconcile()
    await vi.advanceTimersByTimeAsync(STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS)
    connection.send(channel, stringify(8))
    await vi.advanceTimersByTimeAsync(100)
    /** The server runs what reached it of the upload only now, as one whose open-ack the page got too late. */
    const readUploadLate = async () => {
      const { frames } = await parseBlobBody(new Blob(uploaded as BlobPart[]))
      const texts = frames.map((raw) => decode(raw as never)).filter((frame) => frame.tag === TAG.TEXT)
      for (const frame of texts) serverChannel._dispatchFrame(frame)
      return texts.length
    }
    return { received, connection, readUploadLate }
  }

  test("a frame written into it reaches the server, before the page's next one", async () => {
    const { received, connection } = await pageGivingUpItsUpload('upload-no-ack')
    expect(received).toEqual([7, 8])
    connection.dispose()
  })

  test('a frame written into it that the server reads late reaches the server once, in order', async () => {
    const { received, connection, readUploadLate } = await pageGivingUpItsUpload('upload-ack-late')
    expect(await readUploadLate()).toBe(1)
    expect(received).toEqual([7, 8])
    connection.dispose()
  })
})

describe('an upload POST a proxy holds until its body ends', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  /** A page whose upload POST gets no open-ack: the proxy forwards it only once its body ended, when `forwardUpload` is
   *  called, and never once the page aborted it. Its first SSE request reaches the server once `connect` is called, so
   *  what the page queues before is released into the upload by the RECONCILED; everything else as the page sends it. */
  async function pageBehindABufferingProxy(name: string) {
    const sse = getTelefuncSseChannelHooks()
    const toServer = async (body: BodyInit): Promise<Response> => {
      const response = (await sse.handleRequest(new Request('http://localhost/_telefunc', { method: 'POST', body })))!
      return new Response(response.body as never, {
        status: response.statusCode,
        headers: { 'Content-Type': response.contentType },
      })
    }
    let upload: { ended: Promise<ArrayBuffer>; signal: AbortSignal } | null = null
    let connect!: () => void
    const connected = new Promise<void>((resolve) => (connect = resolve))
    config.fetch = (async (_url: string, init: RequestInit) => {
      const body = init.body as unknown
      if (body instanceof Blob) {
        if ((await parseBlobBody(body)).metadata.streamResponse) await connected
        return await toServer(body)
      }
      upload = { ended: new Response(body as ReadableStream).arrayBuffer(), signal: init.signal! }
      return await new Promise<Response>((_resolve, reject) =>
        init.signal!.addEventListener('abort', () => reject(init.signal!.reason)),
      )
    }) as unknown as typeof fetch
    const key = `chat:${name}`
    const server = new ServerBroadcast<string>({ key })
    getChannelMux().registerChannel(server)
    const page = new ClientBroadcast<string>({
      channelId: server.id,
      key,
      transports: ['sse'],
      telefuncUrl: `http://${name}.test/_telefunc`,
      connectionKey: crypto.randomUUID(),
    })
    const transport = () => (page as any)._connection.transport
    await advanceUntil(() => upload !== null, 1_000)
    expect(upload).not.toBeNull()
    const forwardUpload = async () => {
      const { ended, signal } = upload!
      if (signal.aborted) return
      await toServer(new Blob([await ended]))
    }
    const uploadGivenUp = async () => {
      await advanceUntil(() => transport().streamRequest.tag === 'failed', STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS + 2_000)
      expect(transport().streamRequest.tag).toBe('failed')
    }
    /** The page has nothing queued or in flight, so the server has taken all it sent. */
    const allSent = async () => {
      const sending = () => ({ queued: transport().outbox.length, flushing: transport().flushing })
      await advanceUntil(() => sending().queued === 0 && !sending().flushing, 1_000)
      expect(sending()).toEqual({ queued: 0, flushing: false })
    }
    return { server, page, connect, forwardUpload, uploadGivenUp, allSent }
  }

  test("a subscription the page wrote into it and then dropped doesn't come back when the server reads it late", async () => {
    const { server, page, connect, forwardUpload, uploadGivenUp, allSent } =
      await pageBehindABufferingProxy('upload-late-sub')
    const unsubscribe = page.subscribe(() => {})
    connect()
    await uploadGivenUp()
    unsubscribe()
    await allSent()
    expect((server as any)._peerSubscribedText).toBe(false)
    await forwardUpload()
    expect((server as any)._peerSubscribedText).toBe(false)
    page.abort()
  })

  test('a listener the page subscribes after dropping the one it wrote into it gets what is published, though the server reads it late', async () => {
    const { server, page, connect, forwardUpload, uploadGivenUp, allSent } =
      await pageBehindABufferingProxy('upload-late-unsub')
    page.subscribe(() => {})()
    connect()
    await uploadGivenUp()
    const received: string[] = []
    page.subscribe((message) => void received.push(message))
    await allSent()
    expect((server as any)._peerSubscribedText).toBe(true)
    await forwardUpload()
    await server.publish('after')
    await advanceUntil(() => received.length > 0, 1_000)
    expect(received).toEqual(['after'])
    page.abort()
  })
})

test("a batch POST still in flight for a dead wire can't unsubscribe the listener the page swapped in across the reconnect", async () => {
  vi.useFakeTimers()
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
  await advanceUntil(() => (server as any)._peerSubscribedText, 1_000)
  await server.publish('before')
  await advanceUntil(() => first.length > 0, 1_000)
  expect(first).toEqual(['before'])
  holdBatches = true
  offFirst()
  await advanceUntil(() => held.length === 1, 1_000)
  expect(held).toHaveLength(1)
  const second: string[] = []
  page.subscribe((message) => void second.push(message))
  cutWire()
  await advanceUntil(() => wires === 2 && (page as any)._connection.state.tag === 'open', 5_000)
  expect(wires).toBe(2)
  expect((page as any)._connection.state.tag).toBe('open')
  // The dead wire's POST, the unsubscribe in it, arrives now. A server that doesn't take it holds it for connectTtl.
  let taken = false
  void Promise.all(held.map(toServer)).then(() => (taken = true))
  await advanceUntil(() => taken, 1_000)
  await server.publish('after')
  await advanceUntil(() => second.length > 0, 1_000)
  expect(second).toEqual(['after'])
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
  vi.useFakeTimers()
  const server = fakeServer()
  const connection = ClientConnection.getOrCreate('http://upload-cut.test/_telefunc', createChannel() as never, {
    transports: ['sse'],
    fetchImpl: server.fetch,
    connectionKey: crypto.randomUUID(),
  }) as any
  await vi.advanceTimersByTimeAsync(20)
  server.send(encode.streamRequestOpenAck())
  server.reconcile()
  await vi.advanceTimersByTimeAsync(20)
  expect(server.wires).toBe(1)
  // The SSE stream stays up; what the page writes into the upload no longer reaches the server.
  server.settleUpload(new Response('', { status: 408 }))
  await advanceUntil(() => server.wires === 2, 1_500)
  expect(server.wires).toBe(2)
  connection.dispose()
})

test("a RECONCILED that comes on an SSE wire the page gave up doesn't settle the next wire's RECONCILE", async () => {
  vi.useFakeTimers()
  const server = fakeServer()
  const connectionKey = crypto.randomUUID()
  const register = () =>
    ClientConnection.getOrCreate('http://abandoned-reconciled.test/_telefunc', createChannel() as never, {
      transports: ['sse'],
      fetchImpl: server.fetch,
      connectionKey,
    }) as any
  const connection = register()
  await vi.advanceTimersByTimeAsync(20)
  server.refuseUpload()
  server.reconcile()
  await vi.advanceTimersByTimeAsync(20)
  register() // its RECONCILE goes in a batch POST
  await vi.advanceTimersByTimeAsync(20)
  register() // waits for that RECONCILE's RECONCILED
  await vi.advanceTimersByTimeAsync(20)
  // The server cut the wire: it refuses the page's next POST, a PING, while the wire's event stream still delivers.
  server.cutWire()
  connection.transport.sendPing(encode.ping())
  void connection.transport.flushOutbox() // its heartbeat delay passed
  await advanceUntil(() => server.wires === 2, 2_000)
  expect(server.wires).toBe(2)
  server.sendOnWire(0, reconciled([0, 1]))
  server.sendOnWire(1, reconciled([0, 1, 2]))
  await vi.advanceTimersByTimeAsync(20)
  expect([...connection.channels.values()].map((entry) => entry.state.tag)).toEqual(['open', 'open', 'open'])
  connection.dispose()
})

describe('a slow uplink', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  /** A page that uploads `frames` of 64 KiB on a connection to a server whose uplink takes `uplinkBytesPerS`. */
  async function uploadingPage(
    name: string,
    frames: number,
    uplinkBytesPerS: number,
    onBatchFrame: (frame: ReturnType<typeof decode>) => void,
  ) {
    const server = fakeServer(onBatchFrame, uplinkBytesPerS)
    const channel = createChannel()
    const connectionKey = crypto.randomUUID()
    const register = (channel: ReturnType<typeof createChannel>) =>
      ClientConnection.getOrCreate(`http://${name}.test/_telefunc`, channel as never, {
        transports: ['sse'],
        fetchImpl: server.fetch,
        connectionKey,
      }) as any
    const connection = register(channel)
    await vi.advanceTimersByTimeAsync(20)
    server.refuseUpload() // batch POSTs, as Firefox and Safari send them
    server.reconcile()
    await vi.advanceTimersByTimeAsync(20)
    const chunk = new Uint8Array(64 * 1024)
    for (let i = 0; i < frames; i++) connection.sendBinary(channel, chunk)
    return { server, connection, channel, register }
  }

  test("a window refresh the page queues while an upload fills it reaches the server within about a batch POST's target, not after the upload", async () => {
    const arrivals: number[] = []
    const { connection, channel } = await uploadingPage('window-hol', 24, 300 * 1024, (frame) => {
      if (frame.tag === TAG.WINDOW) arrivals.push(Date.now())
    })
    await vi.advanceTimersByTimeAsync(500)
    const queuedAt = Date.now()
    connection.sendByteWindowUpdate(channel, 1 << 20, true)
    await advanceUntil(() => arrivals.length === 1, 10_000)
    expect(arrivals).toHaveLength(1)
    expect(arrivals[0]! - queuedAt).toBeLessThan(SSE_POST_TARGET_MS + 1_000)
    connection.dispose()
  })

  test("a RECONCILE for a channel the page opens while an upload fills it reaches the server within about a batch POST's target, and the upload's frames arrive once, in order", async () => {
    const seqs: number[] = []
    const lastSeqs = new Map<number, number>()
    const reconcileArrivals: number[] = []
    let server!: ReturnType<typeof fakeServer>
    const page = await uploadingPage('reconcile-hol', 24, 300 * 1024, (frame) => {
      if (frame.tag === TAG.BINARY) {
        seqs.push(frame.seq)
        lastSeqs.set(frame.index, frame.seq)
      }
      if (frame.tag === TAG.RECONCILE) {
        reconcileArrivals.push(Date.now())
        const ixes = frame.payload.open.map((entry) => entry.ix)
        setTimeout(() => server.send(reconciled(ixes, lastSeqs)), 20)
      }
    })
    server = page.server
    await vi.advanceTimersByTimeAsync(500)
    const openedAt = Date.now()
    page.register(createChannel())
    await advanceUntil(() => reconcileArrivals.length === 1, 10_000)
    expect(reconcileArrivals).toHaveLength(1)
    expect(reconcileArrivals[0]! - openedAt).toBeLessThan(SSE_POST_TARGET_MS + 1_000)
    await advanceUntil(() => seqs.length >= 24, 20_000)
    expect(seqs).toEqual(Array.from({ length: 24 }, (_, i) => i + 1))
    page.connection.dispose()
  })
})
