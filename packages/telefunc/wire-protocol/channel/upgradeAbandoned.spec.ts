// A move to a WebSocket that the server abandons before its barrier: the page stays on SSE, and its channels live on.
// A real server and a real page, in one process. The page's WebSocket never hands the page the server's READY, so the
// page never sends its barrier and the server holds the stage until something abandons it: the SSE wire closing under
// it, or the stage's deadline.

import { afterAll, beforeAll, beforeEach, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { getChannelMux } from './server/ChannelMux.js'
import { TAG } from '../wire/tags.js'
import { isOneFrame, messageFrames } from '../wire/frame.js'

/** The server's stage waits 1 s for its barrier, well before the page gives up on its attempt (10 s). */
const STAGE_TTL_MS = 1_000
vi.mock('../constants.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../constants.js')>()),
  UPGRADE_STAGE_TTL_MS: 1_000,
}))

serverConfig.channel = { transports: ['sse', 'ws'] }

/** Each WebSocket the page opened: whether the server's READY reached it (it never reaches the page), and its close. */
let sockets: { readyWithheld: boolean; closeCode: number | null }[] = []
/** Ends each event stream the page opened, in order, as a dropped connection does. */
let eventStreams: (() => void)[] = []

const carries = (data: ArrayBuffer, tag: number) => {
  const message = new Uint8Array(data)
  return (isOneFrame(message) ? [message] : Array.from(messageFrames(message))).some((frame) => frame[0] === tag)
}

const RealWebSocket = globalThis.WebSocket
/** The page's WebSocket, on a downlink that loses every message carrying the server's READY. */
class NoReadyWebSocket extends RealWebSocket {
  constructor(url: string | URL, protocols?: string | string[]) {
    super(url, protocols)
    const record: (typeof sockets)[number] = { readyWithheld: false, closeCode: null }
    sockets.push(record)
    let onmessage: ((event: MessageEvent<ArrayBuffer>) => void) | null = null
    Object.defineProperty(this, 'onmessage', {
      get: () => onmessage,
      set: (next: typeof onmessage) => (onmessage = next),
    })
    this.addEventListener('message', (event: MessageEvent<ArrayBuffer>) => {
      if (carries(event.data, TAG.READY)) record.readyWithheld = true
      else onmessage?.(event)
    })
    this.addEventListener('close', ({ code }: CloseEvent) => (record.closeCode = code))
  }
}

const realFetch = globalThis.fetch
/** The page's requests, each event stream it opens relayed so that the test can end it. */
async function fetchWithEndableStreams(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const response = await realFetch(input, init)
  const { body } = response
  if (body === null || !response.headers.get('content-type')?.startsWith('text/event-stream')) return response
  const reader = body.getReader()
  let end = () => {}
  const relay = new ReadableStream<Uint8Array>({
    start: (controller) => {
      end = () => {
        void reader.cancel()
        controller.error(new TypeError('network error'))
      }
    },
    pull: async (controller) => {
      const { done, value } = await reader.read()
      if (done) controller.close()
      else controller.enqueue(value)
    },
  })
  eventStreams.push(() => end())
  return new Response(relay, { status: response.status, headers: response.headers })
}

let server: Server
let telefuncUrl: string

beforeAll(async () => {
  vi.stubGlobal('WebSocket', NoReadyWebSocket)
  vi.stubGlobal('fetch', fetchWithEndableStreams)
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
})
beforeEach(() => {
  sockets = []
  eventStreams = []
})
afterAll(() => {
  vi.unstubAllGlobals()
  server.closeAllConnections()
  server.close()
})

async function until(done: () => boolean, ms = 5_000): Promise<void> {
  const deadline = performance.now() + ms
  while (!done()) {
    if (performance.now() > deadline) throw new Error('timed out waiting')
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** A server channel and the page's end of it on its own connection, opened over SSE; the page moves on its own. */
async function pair() {
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel)
  const got: string[] = []
  channel.listen((message) => void got.push(message))
  const serverEnds: string[] = []
  channel.onClose((err) => void serverEnds.push(err?.message ?? 'closed'))
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl, transports: ['sse', 'ws'], idleTimeout: 0, connectionKey: crypto.randomUUID() },
  })
  const pageEnds: string[] = []
  page.onClose((err) => void pageEnds.push(err?.message ?? 'closed'))
  await Promise.all([channel.opened, page.opened])
  return { page, got, serverEnds, pageEnds }
}

test.each([
  [
    'its SSE wire closes under the stage',
    async () => {
      eventStreams[0]?.()
    },
  ],
  ['the stage waits past its deadline', () => delay(STAGE_TTL_MS)],
])(
  'a move to a WebSocket the server abandons before its barrier, as %s, ends no channel: the page stays on SSE',
  async (_how, abandon) => {
    const { page, got, serverEnds, pageEnds } = await pair()
    await until(() => sockets.some(({ readyWithheld }) => readyWithheld))
    await abandon()
    await until(() => sockets.some(({ closeCode }) => closeCode !== null))
    await delay(500)
    expect([pageEnds, serverEnds]).toEqual([[], []])
    void page.send('after').catch(() => {})
    await until(() => got.includes('after'))
  },
)
