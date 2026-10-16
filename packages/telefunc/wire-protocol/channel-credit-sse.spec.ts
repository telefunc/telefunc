// Channel flow control end to end over SSE: a real ClientChannel and ClientConnection talk to the real server's SSE
// handler over a link with a fixed latency and a bandwidth each way, on fake timers, so each run is deterministic. What
// the link carries has left the page and the server, as what a browser's network stack and the kernel hold has: a
// streaming upload's body and the server's event stream read empty, and a batch POST under way is no longer in the
// page's outbox. The page gets a request's response only once the link has carried its body: Chromium and Firefox hand
// it over only once they have written the body to the socket.

import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { ClientChannel } from './client/channel.js'
import { config as clientConfig } from '../client/clientConfig.js'
import { ServerChannel } from './server/channel.js'
import { getChannelMux } from './server/mux.js'
import { getTelefuncSseChannelHooks } from './server/sse.js'
import { CREDIT_WINDOW_INITIAL_BYTES, CREDIT_WINDOW_INITIAL_BYTES_BATCH } from './constants.js'
import { config as serverConfig } from '../node/server/serverConfig.js'
import { Pipe } from './test/loopback.js'
import { flowOf, produce, run, runUntil } from './test/channel-specs.js'

const LATENCY_MS = 25
/** What one read of a request body hands the server, or of the event stream the page, as a socket read does. */
const CHUNK_BYTES = 16 * 1024
const KIB = 1024

/** The page's end of the link, and the server's SSE handler at the other. `batched`: the page's streaming upload is
 *  refused, as a browser that can't stream a request body gets, `refusedAfter` ms after it went out, and the page sends
 *  batch POSTs. `downHolds`: the most the downlink takes of the server's event stream before what it carries arrives, as
 *  a socket's send buffer does; else it takes the stream as the server writes it. */
function link({
  batched,
  refusedAfter = 2 * LATENCY_MS,
  downHolds = Number.POSITIVE_INFINITY,
}: { batched: boolean; refusedAfter?: number; downHolds?: number }) {
  const up = new Pipe(LATENCY_MS)
  const down = new Pipe(LATENCY_MS)
  const sse = getTelefuncSseChannelHooks()
  let streams = 0
  const fetch = (async (url: string, init: RequestInit) => {
    const signal = init.signal!
    if (new Headers(init.headers).get('Accept') === 'text/event-stream') streams++
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
    let sent: Promise<unknown> = Promise.resolve()
    if (body instanceof Blob) {
      // Bytes rather than the Blob: a Request reads a Blob on a later event-loop turn, which fake timers don't give.
      const bytes = new Uint8Array(await body.arrayBuffer())
      for (let offset = 0; offset < bytes.length; offset += CHUNK_BYTES) send(bytes.slice(offset, offset + CHUNK_BYTES))
      end()
      sent = new Promise((resolve) => up.push(0, () => resolve(undefined)))
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
          // Taken as written till the link holds `downHolds`, so the stream reads empty while the link carries it.
          void (async () => {
            for (;;) {
              while (down.bytes >= downHolds) await new Promise((resolve) => setTimeout(resolve, 1))
              const { done, value } = await events.read().catch(() => ({ done: true, value: undefined }))
              if (done) return down.push(0, () => signal.aborted || toPage.close())
              for (let offset = 0; offset < value!.byteLength; offset += CHUNK_BYTES) {
                const chunk = value!.slice(offset, offset + CHUNK_BYTES)
                down.push(chunk.byteLength, () => signal.aborted || toPage.enqueue(chunk))
              }
            }
          })()
        },
      })
    }
    await Promise.race([Promise.all([new Promise((resolve) => down.push(0, () => resolve(undefined))), sent]), aborted])
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
    /** The event streams the page opened. */
    get streams(): number {
      return streams
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

/** What arrives on `channel`, in bytes. */
function received(channel: { listen(cb: (data: string) => void): unknown }) {
  const got = { bytes: 0 }
  channel.listen((data) => void (got.bytes += data.length))
  return got
}

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

// A frame waits for the POST under way to be answered, and each POST costs a round trip: a larger window makes fewer,
// fuller POSTs, however full the link.
describe('over SSE with batch POSTs', () => {
  test("on a slow uplink, the server's window for an upload grows as the page's credit runs out with nothing in its outbox", async () => {
    const sse = (current = link({ batched: true }))
    sse.up.bytesPerMs = 1_250
    const upload = sse.open<string, never>()
    const got = received(upload.server)
    produce(upload.page, { message: () => 'x'.repeat(64 * KIB) })
    await run(30_000)
    expect(sse.batched).toBe(true)
    expect(flowOf(upload.server).byteWindow).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES)
    expect(got.bytes / 30_000).toBeGreaterThan(0.9 * 1_250)
  })

  test("on a slow downlink, the page's window for a download grows as the server's credit runs out with nothing in its stream", async () => {
    const sse = (current = link({ batched: true }))
    sse.down.bytesPerMs = 1_250
    const download = sse.open<never, string>()
    const got = received(download.page)
    produce(download.server, { message: () => 'x'.repeat(64 * KIB) })
    await run(30_000)
    expect(sse.batched).toBe(true)
    expect(flowOf(download.page).byteWindow).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES_BATCH)
    expect(got.bytes / 30_000).toBeGreaterThan(0.9 * 1_250 * (3 / 4))
  })
})

describe.each([
  { mode: 'a streaming upload', batched: false, pageWindow: CREDIT_WINDOW_INITIAL_BYTES },
  { mode: 'batch POSTs', batched: true, pageWindow: CREDIT_WINDOW_INITIAL_BYTES_BATCH },
])('over SSE with $mode', ({ batched, pageWindow }) => {
  test("on a fast link, the server's window for an upload grows", async () => {
    const sse = (current = link({ batched }))
    const upload = sse.open<string, never>()
    received(upload.server)
    produce(upload.page, { message: () => 'x'.repeat(64 * KIB) })
    await runUntil(() => flowOf(upload.server).byteWindow > CREDIT_WINDOW_INITIAL_BYTES, 1_500, 50)
    expect(sse.batched).toBe(batched)
    expect(flowOf(upload.server).byteWindow).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES)
  })

  test("on a fast link, the page's window for a download grows", async () => {
    const sse = (current = link({ batched }))
    const download = sse.open<never, string>()
    received(download.page)
    produce(download.server, { message: () => 'x'.repeat(64 * KIB) })
    await runUntil(() => flowOf(download.page).byteWindow > pageWindow, 1_500, 50)
    expect(sse.batched).toBe(batched)
    expect(flowOf(download.page).byteWindow).toBeGreaterThan(pageWindow)
  })
})

// The server's event stream carries the download and the WINDOW frames that let the page upload, so a WINDOW that
// waits behind the download the stream holds holds the upload back.
test("on a link that takes a socket's send buffer of the server's event stream, a page's batched upload beside a download keeps the uplink's pace", async () => {
  const sse = (current = link({ batched: true, downHolds: 256 * KIB }))
  sse.up.bytesPerMs = sse.down.bytesPerMs = 1_250
  const upload = sse.open<string, never>()
  const download = sse.open<never, string>()
  const got = received(upload.server)
  received(download.page)
  produce(upload.page, { message: () => 'x'.repeat(64 * KIB) })
  produce(download.server, { message: () => 'x'.repeat(64 * KIB) })
  await run(10_000)
  const mark = got.bytes
  await run(20_000)
  expect(sse.batched).toBe(true)
  expect((got.bytes - mark) / 20_000).toBeGreaterThan(0.75 * 1_250)
})

// The server's ping for the upload's window passes the download waiting for the event stream to ask: a wait that counted
// that download would take more than the whole round trip off it.
test("while the server's event stream holds a download, its estimate of a batched upload's path round trip is no shorter than the link's", async () => {
  const sse = (current = link({ batched: true, downHolds: 256 * KIB }))
  sse.up.bytesPerMs = sse.down.bytesPerMs = 1_250
  const upload = sse.open<string, never>()
  const download = sse.open<never, string>()
  received(upload.server)
  received(download.page)
  produce(upload.page, { message: () => 'x'.repeat(64 * KIB) })
  produce(download.server, { message: () => 'x'.repeat(64 * KIB) })
  await run(10_000)
  const { _boundRtt } = (upload.server as unknown as { _flow: { _bdp: { _boundRtt: number } } })._flow._bdp
  expect(sse.batched).toBe(true)
  expect(_boundRtt).toBeGreaterThanOrEqual(2 * LATENCY_MS)
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

// 100 KB/s, with a ping every second: each 512 KiB message takes 7 s to arrive, its event being base64, and the page's
// PONG waits behind it, past the page's 2 s pong deadline.
test('on a downlink slower than a message per pong deadline, the page keeps its wire while the message arrives', async () => {
  serverConfig.channel.pingInterval = 1_000
  const sse = (current = link({ batched: false }))
  sse.down.bytesPerMs = 100
  const download = sse.open<never, string>()
  const got = received(download.page)
  await run(100)
  produce(download.server, { message: () => 'x'.repeat(512 * KIB) })
  await run(30_000)
  expect(sse.streams).toBe(1)
  expect(got.bytes).toBeGreaterThanOrEqual(3 * 512 * KIB)
})
