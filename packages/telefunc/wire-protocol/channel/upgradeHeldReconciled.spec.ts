// A move to a WebSocket whose RECONCILED the link holds back past the ping deadline: from the flip until it arrives,
// the page's own heartbeat on that WebSocket tells the server the page is there. A real server and a real page, in one
// process; the page's WebSocket delivers the RECONCILED, and what follows it, late.

import { afterAll, beforeAll, beforeEach, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ServerChannel, ATTACHABLE_NOW } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { WsTransport } from './client/WsTransport.js'
import { getChannelMux } from './server/ChannelMux.js'
import { TAG } from '../wire/tags.js'
import { messageFrames } from '../wire/frame.js'

// A link whose queue holds the RECONCILED back holds the old wire's FIN, which goes out after it, as long: the page's
// join then waits for both, as here.
vi.mock('../constants.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../constants.js')>()),
  UPGRADE_HANDOFF_JOIN_TIMEOUT_MS: 30_000,
}))

// A ping deadline of 2 s.
serverConfig.channel = { transports: ['sse', 'ws'], pingInterval: 1000 }

/** How long the page's WebSocket holds back the server's RECONCILED, and what came after it: past the deadline. */
const HOLD_MS = 4_000

type SocketRecord = {
  closedByPage: boolean
  closedByServer: boolean
  /** The messages it delivered to the page. */
  received: number
  socket: WebSocket
  held: Promise<void>
  released: Promise<void>
}
let sockets: SocketRecord[] = []
/** Ends each event stream the page opened, in order, as a dropped connection does. */
let eventStreams: (() => void)[] = []

const RealWebSocket = globalThis.WebSocket
/** The page's WebSocket, on a downlink that delivers the first RECONCILED, and what follows it, `HOLD_MS` late. */
class HeldBackWebSocket extends RealWebSocket {
  private readonly record: SocketRecord

  constructor(url: string | URL, protocols?: string | string[]) {
    super(url, protocols)
    let onHeld = () => {}
    let onReleased = () => {}
    const record: SocketRecord = {
      closedByPage: false,
      closedByServer: false,
      received: 0,
      socket: this,
      held: new Promise((resolve) => (onHeld = resolve)),
      released: new Promise((resolve) => (onReleased = resolve)),
    }
    this.record = record
    sockets.push(record)
    let onmessage: ((event: MessageEvent<ArrayBuffer>) => void) | null = null
    let onclose: ((event: CloseEvent) => void) | null = null
    Object.defineProperty(this, 'onmessage', {
      get: () => onmessage,
      set: (next: typeof onmessage) => (onmessage = next),
    })
    Object.defineProperty(this, 'onclose', {
      get: () => onclose,
      set: (next: typeof onclose) => (onclose = next),
    })
    let queue: (() => void)[] | 'none yet' | 'released' = 'none yet'
    const arrive = (deliver: () => void) => {
      if (Array.isArray(queue)) queue.push(deliver)
      else deliver()
    }
    this.addEventListener('message', (event: MessageEvent<ArrayBuffer>) => {
      // The acknowledgements the server carries ride ahead of the RECONCILED in its message.
      const carriesReconciled = () =>
        Array.from(messageFrames(new Uint8Array(event.data)), (frame) => frame[0]).includes(TAG.RECONCILED)
      if (queue === 'none yet' && carriesReconciled()) {
        const holding: (() => void)[] = []
        queue = holding
        onHeld()
        setTimeout(() => {
          queue = 'released'
          for (const deliver of holding) deliver()
          onReleased()
        }, HOLD_MS)
      }
      arrive(() => {
        record.received++
        onmessage?.(event)
      })
    })
    this.addEventListener('close', (event: CloseEvent) => {
      record.closedByServer = !record.closedByPage
      arrive(() => onclose?.(event))
    })
  }

  override close(code?: number, reason?: string): void {
    this.record.closedByPage = true
    super.close(code, reason)
  }
}

const realFetch = globalThis.fetch
/** The page's requests, each event stream it opens relayed so that the test can end it. */
async function fetchWithEndableStreams(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const response = await realFetch(input, init)
  const { body } = response
  if (body === null || !response.headers.get('content-type')?.startsWith('text/event-stream')) return response
  const reader = body.getReader()
  let ended = false
  let end = () => {}
  const relay = new ReadableStream<Uint8Array>({
    start(controller) {
      end = () => {
        if (ended) return
        ended = true
        void reader.cancel()
        controller.close()
      }
    },
    async pull(controller) {
      const { done, value } = await reader.read()
      if (ended) return
      if (done) {
        ended = true
        controller.close()
      } else controller.enqueue(value)
    },
  })
  eventStreams.push(() => end())
  return new Response(relay, { status: response.status, headers: response.headers })
}

let server: Server
let telefuncUrl: string

beforeAll(async () => {
  vi.stubGlobal('WebSocket', HeldBackWebSocket)
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

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** A server channel that echoes, and the page's end of it on its own connection; the page upgrades on its own. */
async function pair() {
  const channel = new ServerChannel<string, string>({ ack: true })
  getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
  channel.listen((message) => `echo ${message}`)
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    ack: true,
    connection: { telefuncUrl, transports: ['sse', 'ws'], idleTimeout: 0, connectionKey: crypto.randomUUID() },
  })
  await Promise.all([channel.opened, page.opened])
  return page
}

/** The WebSocket the page moves to, once its RECONCILED is held. */
async function heldSocket(): Promise<SocketRecord> {
  while (sockets.length === 0) await delay(10)
  const [socket] = sockets
  if (socket === undefined) throw new Error('no WebSocket')
  await socket.held
  return socket
}

test('a page whose move to a WebSocket gets its RECONCILED past the ping deadline keeps that WebSocket', async () => {
  const page = await pair()
  // From the barrier on, the server judges the WebSocket by the ping deadline.
  const socket = await heldSocket()
  await socket.released
  expect(sockets.length).toBe(1)
  expect(socket.closedByServer).toBe(false)
  expect(socket.closedByPage).toBe(false)

  // The page moved: the answer to its next message comes on the WebSocket.
  const receivedBefore = socket.received
  expect(await page.send('after the move', { ack: true })).toBe('echo after the move')
  expect(socket.received).toBeGreaterThan(receivedBefore)
  expect(socket.socket.readyState).toBe(RealWebSocket.OPEN)
  await page.close()
}, 20_000)

test('a page whose old SSE wire ends while its RECONCILED is held falls back to SSE, and the WebSocket it left beats no more', async () => {
  const page = await pair()
  const pings = vi.spyOn(WsTransport.prototype, 'sendPing')
  try {
    const socket = await heldSocket()
    eventStreams[0]?.()
    while (!socket.closedByPage) await delay(10)
    const [left] = pings.mock.contexts
    const pingsBefore = pings.mock.contexts.filter((transport) => transport === left).length

    // Two ping intervals on: the page's channel is back on SSE, and nothing beats on the WebSocket it left.
    await delay(2_000)
    expect(await page.send('after the fallback', { ack: true })).toBe('echo after the fallback')
    expect(left).toBeDefined()
    expect(pings.mock.contexts.filter((transport) => transport === left).length).toBe(pingsBefore)
    await page.close()
  } finally {
    pings.mockRestore()
  }
}, 20_000)
