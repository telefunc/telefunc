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

const LATENCY_MS = 25
/** What one read of a request body hands the server, as a socket read does. */
const CHUNK_BYTES = 16 * 1024
const KIB = 1024

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

/** Runs until `done`, `ms` at most: an unshaped link carries as much as its windows let, and each simulated ms of it
 *  costs real time. */
async function runUntil(done: () => boolean, ms: number): Promise<void> {
  for (let at = 0; at < ms && !done(); at += 50) await run(50)
}

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

// A frame waits for the POST under way to be answered, and each POST costs a round trip: a larger window makes fewer,
// fuller POSTs, however full the link.
describe('over SSE with batch POSTs', () => {
  test("on a slow uplink, the server's window for an upload grows as the page's credit runs out with nothing in its outbox", async () => {
    const sse = (current = link({ batched: true }))
    sse.up.bytesPerMs = 1_250
    const upload = sse.open<string, never>()
    const got = received(upload.server)
    produce(upload.page, 'x'.repeat(64 * KIB))
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
    produce(download.server, 'x'.repeat(64 * KIB))
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
    produce(upload.page, 'x'.repeat(64 * KIB))
    await runUntil(() => flowOf(upload.server).byteWindow > CREDIT_WINDOW_INITIAL_BYTES, 1_500)
    expect(sse.batched).toBe(batched)
    expect(flowOf(upload.server).byteWindow).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES)
  })

  test("on a fast link, the page's window for a download grows", async () => {
    const sse = (current = link({ batched }))
    const download = sse.open<never, string>()
    received(download.page)
    produce(download.server, 'x'.repeat(64 * KIB))
    await runUntil(() => flowOf(download.page).byteWindow > pageWindow, 1_500)
    expect(sse.batched).toBe(batched)
    expect(flowOf(download.page).byteWindow).toBeGreaterThan(pageWindow)
  })
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
