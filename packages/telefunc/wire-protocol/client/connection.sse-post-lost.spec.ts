// A page that sends in batch POSTs (Firefox, Safari) sends one at a time, so a POST that is never answered holds
// everything the page sends after it. Real server, a fetch that holds the POST the test picks.

import { afterEach, beforeEach, expect, test, vi } from 'vitest'

import { ClientChannel } from './channel.js'
import { ClientConnection } from './connection.js'
import { config } from '../../client/clientConfig.js'
import {
  CHANNEL_TRANSPORT,
  SSE_POST_FLOOR_MS,
  SSE_POST_MIN_BYTES_PER_S,
  WIRE_MAX_RAW_FRAME_BYTES,
} from '../constants.js'
import { encode, TAG } from '../shared-ws.js'
import { ServerChannel } from '../server/channel.js'
import { getChannelMux } from '../server/mux.js'
import { getTelefuncSseChannelHooks } from '../server/sse.js'

beforeEach(() => {
  vi.useFakeTimers({
    shouldAdvanceTime: true,
    toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'Date', 'performance'],
  })
})

afterEach(() => {
  vi.useRealTimers()
  delete config.fetch
})

const MIB = 1024 * 1024

/** What the test does with the next batch POST: nothing (it never settles) or `slow`, which hands it to the server after
 *  the bytes' time at `bitsPerSecond`. */
type Hold = { tag: 'never' } | { tag: 'slow'; bitsPerSecond: number }

function setup() {
  const sse = getTelefuncSseChannelHooks()
  const toServer = async (body: Blob): Promise<Response> => {
    const response = (await sse.handleRequest(new Request('http://localhost/_telefunc', { method: 'POST', body })))!
    return new Response(response.body as never, {
      status: response.statusCode,
      headers: { 'Content-Type': response.contentType },
    })
  }
  const metadataOf = async (body: Blob) => {
    const bytes = new Uint8Array(await body.arrayBuffer())
    const length = new DataView(bytes.buffer).getUint32(0)
    return JSON.parse(new TextDecoder().decode(bytes.subarray(4, 4 + length))) as { streamResponse?: true }
  }
  const carriesMessage = async (body: Blob) => {
    const bytes = new Uint8Array(await body.arrayBuffer())
    const view = new DataView(bytes.buffer)
    for (let at = 4 + view.getUint32(0); at + 4 <= bytes.length; at += 4 + view.getUint32(at))
      if (bytes[at + 4] === TAG.TEXT) return true
    return false
  }
  const state = { wires: 0, held: [] as number[], hold: null as Hold | null, answering: 0 }
  config.fetch = (async (_url: string, init: RequestInit) => {
    const body = init.body as unknown
    state.answering++
    try {
      return await answer(body, init)
    } finally {
      state.answering--
    }
  }) as unknown as typeof fetch
  const answer = async (body: unknown, init: RequestInit): Promise<Response> => {
    // A browser that can't stream a request body (Firefox, Safari) is answered 400: the page sends batch POSTs.
    if (!(body instanceof Blob)) return new Response('bad request', { status: 400 })
    if ((await metadataOf(body)).streamResponse) {
      state.wires++
      const reader = (await toServer(body)).body!.getReader()
      const wire = new ReadableStream<Uint8Array>({
        start(controller) {
          init.signal?.addEventListener('abort', () => {
            void reader.cancel()
            controller.error(init.signal!.reason)
          })
          void (async () => {
            try {
              for (let read = await reader.read(); !read.done; read = await reader.read())
                controller.enqueue(read.value)
            } catch {}
          })()
        },
      })
      return new Response(wire, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
    }
    const hold = state.hold
    // The POST the test holds is the one carrying its message, not a PING's that may go first.
    if (hold === null || !(await carriesMessage(body))) return await toServer(body)
    state.hold = null
    state.held.push(body.size)
    state.answering--
    await new Promise<void>((resolve, reject) => {
      init.signal?.addEventListener('abort', () => reject(init.signal!.reason))
      if (hold.tag === 'slow') setTimeout(resolve, (body.size * 8 * 1000) / hold.bitsPerSecond)
    }).finally(() => state.answering++)
    return await toServer(body)
  }

  const received: (number | string)[] = []
  const server = new ServerChannel<number | string, never>()
  server.listen((message) => void received.push(typeof message === 'string' ? message.length : message))
  getChannelMux().registerChannel(server)
  const page = new ClientChannel<number | string, never>({
    channelId: server.id,
    transports: ['sse'],
    telefuncUrl: 'http://post-lost.test/_telefunc',
    connectionKey: crypto.randomUUID(),
  })
  /** Moves the clock on `ms`, never past a request the server is still answering, which takes real event-loop turns. */
  const advance = async (ms: number) => {
    for (let at = 0; at < ms; at += 100) {
      while (state.answering > 0) await new Promise((resolve) => setImmediate(resolve))
      await vi.advanceTimersByTimeAsync(Math.min(100, ms - at))
    }
  }
  return { state, received, page, advance }
}

test('a batch POST that is never answered ends the wire, and what it and the POSTs after it carried arrives once on the next', async () => {
  const { state, received, page, advance } = setup()
  void page.send(1)
  await vi.waitFor(() => expect(received).toEqual([1]))

  state.hold = { tag: 'never' }
  void page.send(2)
  await vi.waitFor(() => expect(state.held).toHaveLength(1))
  void page.send(3)
  void page.send(4)
  await advance(SSE_POST_FLOOR_MS / 2)
  expect(received, 'nothing goes behind the POST that is out').toEqual([1])
  expect(state.wires, 'the page keeps the wire until the POST has been out the floor').toBe(1)

  await advance(SSE_POST_FLOOR_MS)
  await vi.waitFor(() => expect(received).toEqual([1, 2, 3, 4]), { timeout: 5_000 })
  expect(state.wires).toBe(2)
  page.abort()
})

test('a POST of a few MiB, still uploading at 1 Mbit/s, is not cut', async () => {
  const { state, received, page, advance } = setup()
  void page.send(1)
  await vi.waitFor(() => expect(received).toEqual([1]))

  state.hold = { tag: 'slow', bitsPerSecond: 1_000_000 }
  void page.send('x'.repeat(2 * MIB))
  await vi.waitFor(() => expect(state.held).toHaveLength(1))
  const uploadMs = (state.held[0]! * 8 * 1000) / 1_000_000
  expect(uploadMs, 'longer than the floor').toBeGreaterThan(SSE_POST_FLOOR_MS / 2)
  await advance(uploadMs + 5_000)
  await vi.waitFor(() => expect(received).toEqual([1, 2 * MIB]), { timeout: 5_000 })
  expect(state.wires, 'the upload went on the first wire').toBe(1)
  page.abort()
})

test("a POST's allowance grows with its bytes: a lost one of 2 MiB is cut past the floor, not at it", async () => {
  const { state, received, page, advance } = setup()
  void page.send(1)
  await vi.waitFor(() => expect(received).toEqual([1]))

  state.hold = { tag: 'never' }
  void page.send('x'.repeat(2 * MIB))
  await vi.waitFor(() => expect(state.held).toHaveLength(1))
  const allowanceMs = SSE_POST_FLOOR_MS + (state.held[0]! * 1000) / SSE_POST_MIN_BYTES_PER_S
  await advance(allowanceMs - 5_000)
  expect(state.wires, 'still within the time its bytes take at the slowest rate').toBe(1)

  await advance(10_000)
  await vi.waitFor(() => expect(received).toEqual([1, 2 * MIB]), { timeout: 5_000 })
  expect(state.wires).toBe(2)
  page.abort()
})

test("a POST that comes due before its wire's first RECONCILED is left out, and ends the wire once that came", async () => {
  const channel = {
    id: crypto.randomUUID(),
    isClosed: false,
    _maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
    _onTransportOpen() {},
    _dispatchFrame() {},
    _onTransportClose() {},
    _onTransportBatched() {},
    _reattachState: () => ({}),
    _fitReplays() {},
    _acknowledge() {},
  }
  const stalled = (async () => new Response(new ReadableStream<Uint8Array>({ start() {} }), { status: 200 })) as never
  const connection = ClientConnection.getOrCreate('http://reconciling.test', channel as never, {
    transports: [CHANNEL_TRANSPORT.SSE],
    fetchImpl: stalled,
    connectionKey: crypto.randomUUID(),
  }) as any
  try {
    const transport = connection.transport
    transport.post = () => new Promise<Response>(() => {})
    transport.transportAbort = new AbortController()
    transport.outbox = [{ frame: encode.window(0, 65_536, 0), deadline: 0 }]
    connection.reconciling = true
    void transport.flushOutbox()
    await vi.advanceTimersByTimeAsync(2 * SSE_POST_FLOOR_MS)
    expect(transport.hasWire(), 'the wire is awaiting its first RECONCILED').toBe(true)

    connection.reconciling = false
    await vi.advanceTimersByTimeAsync(SSE_POST_FLOOR_MS)
    expect(transport.hasWire()).toBe(false)
  } finally {
    connection.dispose()
  }
})
