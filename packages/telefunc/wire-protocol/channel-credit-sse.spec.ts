// Channel flow control end to end over SSE: a real ClientChannel and ClientConnection talk to the real server's SSE
// handler over a link with a fixed latency and a bandwidth each way, on fake timers, so each run is deterministic. What
// the link carries has left the page and the server, as what a browser's network stack and the kernel hold has: a
// streaming upload's body and the server's event stream read empty, and a batch POST under way is no longer in the
// page's outbox. The page gets a request's response only once its body is in the socket, all but what the socket holds
// of it carried: Chromium and Firefox hand it over only once they have written the body to the socket.

import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { ClientChannel } from './client/channel.js'
import { config as clientConfig } from '../client/clientConfig.js'
import { ServerChannel } from './server/channel.js'
import { getChannelMux } from './server/mux.js'
import { getTelefuncSseChannelHooks } from './server/sse.js'
import { CREDIT_WINDOW_INITIAL_BYTES, CREDIT_WINDOW_INITIAL_BYTES_BATCH } from './constants.js'
import { config as serverConfig } from '../node/server/serverConfig.js'

const LATENCY_MS = 25
/** What one read of a request body hands the server, as a socket read does. */
const CHUNK_BYTES = 16 * 1024
const KIB = 1024
/** What the page's socket holds of a request body the link hasn't carried yet. */
const SOCKET_BYTES = 256 * KIB

/** One direction of the link: delivers what it carries in order, `LATENCY_MS` after it was sent, and no faster than
 *  `bytesPerMs`. */
class Pipe {
  bytesPerMs = Infinity
  private lastAt = 0
  private queue: { at: number; deliver: () => void }[] = []
  private timer: ReturnType<typeof setTimeout> | null = null
  push(bytes: number, deliver: () => void): void {
    const at = Math.max(Date.now() + LATENCY_MS, this.lastAt + bytes / this.bytesPerMs)
    this.lastAt = at
    this.queue.push({ at, deliver })
    this.schedule()
  }
  private schedule(): void {
    if (this.timer || this.queue.length === 0) return
    this.timer = setTimeout(
      () => {
        this.timer = null
        while (this.queue.length > 0 && this.queue[0]!.at <= Date.now()) this.queue.shift()!.deliver()
        this.schedule()
      },
      Math.max(0, this.queue[0]!.at - Date.now()),
    )
  }
}

/** The page's end of the link, and the server's SSE handler at the other. `batched`: the page's streaming upload is
 *  refused, as a browser that can't stream a request body gets, `refusedAfter` ms after it went out, and the page sends
 *  batch POSTs. */
function link({ batched, refusedAfter = 2 * LATENCY_MS }: { batched: boolean; refusedAfter?: number }) {
  const up = new Pipe()
  const down = new Pipe()
  const sse = getTelefuncSseChannelHooks()
  const fetch = (async (url: string, init: RequestInit) => {
    const signal = init.signal!
    const aborted = new Promise<never>((_resolve, reject) =>
      signal.addEventListener('abort', () => reject(new DOMException('The operation was aborted.', 'AbortError'))),
    )
    aborted.catch(() => {})
    const body = init.body as Blob | ReadableStream<Uint8Array>
    if (!(body instanceof Blob) && batched) {
      await new Promise((resolve) => setTimeout(resolve, refusedAfter))
      return new Response('', { status: 400 })
    }
    let toServer!: ReadableStreamDefaultController<Uint8Array>
    const serverBody = new ReadableStream<Uint8Array>({ start: (controller) => void (toServer = controller) })
    const send = (chunk: Uint8Array) => up.push(chunk.byteLength, () => signal.aborted || toServer.enqueue(chunk))
    const end = () => up.push(0, () => signal.aborted || toServer.close())
    signal.addEventListener('abort', () => toServer.error(new TypeError('network error')))
    let written: Promise<unknown> = Promise.resolve()
    if (body instanceof Blob) {
      // Bytes rather than the Blob: a Request reads a Blob on a later event-loop turn, which fake timers don't give.
      const bytes = new Uint8Array(await body.arrayBuffer())
      const inSocketFrom = bytes.length - SOCKET_BYTES
      for (let offset = 0; offset < bytes.length; offset += CHUNK_BYTES) {
        send(bytes.slice(offset, offset + CHUNK_BYTES))
        if (offset <= inSocketFrom && inSocketFrom < offset + CHUNK_BYTES)
          written = new Promise((resolve) => up.push(0, () => resolve(undefined)))
      }
      end()
    } else {
      // Taken as it is written, so the body reads empty while the link carries it.
      void (async () => {
        const reader = body.getReader()
        for (;;) {
          const { done, value } = await reader.read().catch(() => ({ done: true, value: undefined }))
          if (done || signal.aborted) return end()
          send(value!)
        }
      })()
    }
    const request = new Request(url, { method: 'POST', body: serverBody, duplex: 'half' } as RequestInit)
    const response = (await Promise.race([sse.handleRequest(request), aborted]))!
    let pageBody: ReadableStream<Uint8Array> | string = response.body as string
    if (response.body instanceof ReadableStream) {
      const events = response.body.getReader()
      pageBody = new ReadableStream<Uint8Array>({
        start(toPage) {
          signal.addEventListener('abort', () => {
            toPage.error(new DOMException('The operation was aborted.', 'AbortError'))
            void events.cancel()
          })
          // Taken as the server writes it, so its stream reads empty while the link carries it.
          void (async () => {
            for (;;) {
              const { done, value } = await events.read().catch(() => ({ done: true, value: undefined }))
              if (done) return down.push(0, () => signal.aborted || toPage.close())
              down.push(value!.byteLength, () => signal.aborted || toPage.enqueue(value!))
            }
          })()
        },
      })
    }
    await Promise.race([
      Promise.all([new Promise((resolve) => down.push(0, () => resolve(undefined))), written]),
      aborted,
    ])
    return new Response(pageBody, { status: response.statusCode, headers: { 'Content-Type': response.contentType } })
  }) as typeof globalThis.fetch
  clientConfig.fetch = fetch
  const telefuncUrl = `http://${crypto.randomUUID()}.test/_telefunc`
  const connectionKey = crypto.randomUUID()
  const pages: ClientChannel[] = []
  return {
    up,
    down,
    /** A channel the server has registered, and its page end, which all share one connection. */
    open<ClientToServer, ServerToClient>() {
      const server = new ServerChannel<ClientToServer, ServerToClient>()
      getChannelMux().registerChannel(server)
      const page = new ClientChannel<ClientToServer, ServerToClient>({
        channelId: server.id,
        transports: ['sse'],
        telefuncUrl,
        connectionKey,
      })
      pages.push(page as ClientChannel)
      return { server, page }
    },
    /** Whether the page sends batch POSTs. */
    get batched(): boolean {
      return (pages[0] as unknown as { _connection: { transport: { batched: boolean } } })._connection.transport.batched
    },
    dispose(): void {
      for (const page of pages) page.abort()
    },
  }
}

let current: ReturnType<typeof link> | null = null

const run = (ms: number) => vi.advanceTimersByTimeAsync(ms)

/** `for (;;) await channel.send(next)`, the channel page's backpressure loop. */
function produce(channel: { send(data: string): Promise<void>; isClosed: boolean }, message: string) {
  void (async () => {
    while (!channel.isClosed) await channel.send(message)
  })().catch(() => {})
}

/** What arrives on `channel`, in bytes. */
function received(channel: { listen(cb: (data: string) => void): unknown }) {
  const got = { bytes: 0 }
  channel.listen((data) => void (got.bytes += data.length))
  return got
}

const flowOf = (channel: unknown) => (channel as { _flow: { byteWindow: number } })._flow

beforeEach(() => {
  vi.useFakeTimers()
  ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
})

afterEach(() => {
  current?.dispose()
  current = null
  vi.useRealTimers()
  delete clientConfig.fetch
  serverConfig.channel = {}
  ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
})

describe.each([
  { mode: 'a streaming upload', batched: false, pageWindow: CREDIT_WINDOW_INITIAL_BYTES },
  { mode: 'batch POSTs', batched: true, pageWindow: CREDIT_WINDOW_INITIAL_BYTES_BATCH },
])('over SSE with $mode', ({ batched, pageWindow }) => {
  // 1.25 MB/s: a 2 MiB window takes 1.7 s to go through, 34 round trips.
  test("on a slow uplink, the server's window for an upload stays at its initial size, however little the page's body or outbox holds", async () => {
    const sse = (current = link({ batched }))
    sse.up.bytesPerMs = 1_250
    const upload = sse.open<string, never>()
    const got = received(upload.server)
    produce(upload.page, 'x'.repeat(64 * KIB))
    await run(30_000)
    expect(sse.batched).toBe(batched)
    expect(flowOf(upload.server).byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
    // The link stays full.
    expect(got.bytes / 30_000).toBeGreaterThan(0.9 * 1_250)
  })

  test("on a slow downlink, the page's window for a download stays at its initial size, however little the server's stream holds", async () => {
    const sse = (current = link({ batched }))
    sse.down.bytesPerMs = 1_250
    const download = sse.open<never, string>()
    const got = received(download.page)
    produce(download.server, 'x'.repeat(64 * KIB))
    await run(30_000)
    expect(sse.batched).toBe(batched)
    expect(flowOf(download.page).byteWindow).toBe(pageWindow)
    // An event carries its frame in base64, 4 bytes for every 3: the link stays full.
    expect(got.bytes / 30_000).toBeGreaterThan(0.9 * 1_250 * (3 / 4))
  })

  // Its RECONCILE, and the probe it carries, wait behind the first channel's upload, 2 MiB at 1.25 MB/s.
  test("on a slow uplink, the server's window for an upload a channel begins while another uploads stays at its initial size, however long its attach waited", async () => {
    const sse = (current = link({ batched }))
    sse.up.bytesPerMs = 1_250
    const first = sse.open<string, never>()
    received(first.server)
    produce(first.page, 'x'.repeat(64 * KIB))
    await run(10_000)
    const late = sse.open<string, never>()
    const got = received(late.server)
    produce(late.page, 'y'.repeat(64 * KIB))
    await run(3_000)
    first.page.abort()
    await run(30_000)
    expect(sse.batched).toBe(batched)
    expect(flowOf(late.server).byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
    expect(got.bytes / 30_000).toBeGreaterThan(0.9 * 1_250)
  })

  // Unshaped, the loop a window has to cover is the round trip, and on batch POSTs the flush throttle a frame waits
  // for its POST: the window limits the stream, and grows.
  test("on a fast link, the server's window for an upload grows", async () => {
    const sse = (current = link({ batched }))
    const upload = sse.open<string, never>()
    received(upload.server)
    produce(upload.page, 'x'.repeat(64 * KIB))
    await run(1_500)
    expect(sse.batched).toBe(batched)
    expect(flowOf(upload.server).byteWindow).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES)
  })

  test("on a fast link, the page's window for a download grows", async () => {
    const sse = (current = link({ batched }))
    const download = sse.open<never, string>()
    received(download.page)
    produce(download.server, 'x'.repeat(64 * KIB))
    await run(1_500)
    expect(sse.batched).toBe(batched)
    expect(flowOf(download.page).byteWindow).toBeGreaterThan(pageWindow)
  })
})

// The server answers a flush as it begins to read it, and the browser hands the page that answer once the body is in the
// socket: the page's next flush goes into the link while the one before still crosses it.
test("over batch POSTs, on a slow uplink, a page's upload keeps the link full from one POST to the next", async () => {
  const sse = (current = link({ batched: true }))
  sse.up.bytesPerMs = 1_250
  const upload = sse.open<string, never>()
  const got = received(upload.server)
  produce(upload.page, 'x'.repeat(64 * KIB))
  await run(5_000)
  const from = got.bytes
  await run(30_000)
  expect(sse.batched).toBe(true)
  expect((got.bytes - from) / 30_000).toBeGreaterThan(0.99 * 1_250)
})

test('over batch POSTs, what a page sends arrives in the order it sent it, as one POST crosses the link while the server reads the one before', async () => {
  const sse = (current = link({ batched: true }))
  sse.up.bytesPerMs = 1_250
  const upload = sse.open<string, never>()
  const got: number[] = []
  upload.server.listen((data) => void got.push(Number(data.slice(0, data.indexOf(':')))))
  void (async () => {
    for (let i = 0; !upload.page.isClosed; i++) await upload.page.send(`${i}:${'x'.repeat(64 * KIB)}`)
  })().catch(() => {})
  await run(20_000)
  expect(got.length).toBeGreaterThan(300)
  expect(got).toEqual(got.map((_, i) => i))
})

// Firefox answers the streaming upload it can't send with the server's 400, a round trip after the page's first RECONCILE
// went out: that RECONCILE's RECONCILED, which opens the page's channels, may come first.
test("a page's window for a download starts at the batched initial window, though its upload falls back to batch POSTs only after the download opened", async () => {
  const sse = (current = link({ batched: true, refusedAfter: 500 }))
  const download = sse.open<never, string>()
  received(download.page)
  await run(1_000)
  expect(sse.batched).toBe(true)
  expect(flowOf(download.page).byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES_BATCH)
})
