import { afterAll, afterEach, beforeAll, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import type { Socket } from 'node:net'
import { Telefunc } from './node.js'
import { getChannelMux } from '../wire-protocol/channel/server/ChannelMux.js'
import { ServerChannel } from '../wire-protocol/channel/server/ServerChannel.js'
import { ClientChannel } from '../wire-protocol/channel/client/ClientChannel.js'
import { decode, isOneFrame, messageFrames } from '../wire-protocol/wire/frame.js'
import { TAG } from '../wire-protocol/wire/tags.js'
import { base64urlToUint8Array } from '../wire-protocol/wire/base64url.js'

// Which page is offered WebSocket, as an app on `serve({ req, res })` sees it: each page's real connection to a real Node
// server, the offer read on the page's side off the RECONCILED its SSE stream carries, the WebSocket from the page's own.

const NativeWebSocket = globalThis.WebSocket
/** The page's WebSockets, by the host each went to. */
const webSockets = new Map<string, WebSocket[]>()
class RecordedWebSocket extends NativeWebSocket {
  constructor(url: string | URL, protocols?: string | string[]) {
    super(url, protocols)
    const host = new URL(url).host
    webSockets.set(host, [...(webSockets.get(host) ?? []), this])
  }
}
/** The transports each RECONCILED offered, by the host of the SSE stream that carried it. */
const offers = new Map<string, string[][]>()
const nativeFetch = globalThis.fetch
beforeAll(() => {
  globalThis.WebSocket = RecordedWebSocket
  globalThis.fetch = async (input, init) => {
    const response = await nativeFetch(input, init)
    if (response.headers.get('content-type') !== 'text/event-stream' || !response.body) return response
    const host = new URL(input instanceof Request ? input.url : input).host
    const [toPage, toTest] = response.body.tee()
    const record = offerRecorder(host)
    void toTest.pipeTo(new WritableStream({ write: record })).catch(() => {})
    return new Response(toPage, response)
  }
})
afterAll(() => {
  globalThis.WebSocket = NativeWebSocket
  globalThis.fetch = nativeFetch
})

const closers: (() => Promise<void>)[] = []
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close()
  webSockets.clear()
  offers.clear()
})

function offerRecorder(host: string): (chunk: Uint8Array) => void {
  const decoder = new TextDecoder()
  let events = ''
  return (chunk) => {
    events += decoder.decode(chunk, { stream: true })
    const complete = events.split('\n\n')
    events = complete.pop() ?? ''
    for (const event of complete) offers.set(host, [...(offers.get(host) ?? []), ...offersIn(event)])
  }
}

function offersIn(event: string): string[][] {
  if (!event.startsWith('data: ')) return []
  const raw = base64urlToUint8Array(event.slice('data: '.length))
  return (isOneFrame(raw) ? [raw] : [...messageFrames(raw)])
    .map((bytes) => decode(bytes))
    .flatMap((frame) => (frame.tag === TAG.RECONCILED ? [frame.payload.transports] : []))
}

async function listen(server: Server): Promise<string> {
  const sockets = new Set<Socket>()
  server.on('connection', (socket: Socket) => {
    sockets.add(socket)
    socket.once('close', () => sockets.delete(socket))
  })
  closers.push(async () => {
    const closed = new Promise<void>((resolve) => server.close(() => resolve()))
    for (const socket of sockets) socket.destroy()
    await closed
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  return hostOf(server)
}

function hostOf(server: Server): string {
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  return `127.0.0.1:${address.port}`
}

/** A page that opens a channel to `host`. */
function pageOf(host: string): ClientChannel<string, string> {
  const serverChannel = new ServerChannel<string, string>()
  serverChannel.listen((data) => `ack:${data}`)
  getChannelMux().registerChannel(serverChannel)
  const page = new ClientChannel<string, string>({
    channelId: serverChannel.id,
    connection: {
      telefuncUrl: `http://${host}/_telefunc`,
      transports: ['sse', 'ws'],
      idleTimeout: 0,
      connectionKey: crypto.randomUUID(),
    },
  })
  closers.push(async () => {
    await page.close()
  })
  return page
}

/** The transports the page's wire is offered, and whether its WebSocket opened. */
async function openPage(host: string): Promise<{ offer: string[] | undefined; webSocketOpened: boolean }> {
  const offersBefore = offers.get(host)?.length ?? 0
  expect(await pageOf(host).send('page', { ack: true })).toBe('ack:page')
  const offer = offers.get(host)?.[offersBefore]
  let webSocketOpened = false
  if (offer?.includes('ws')) {
    await expect
      .poll(() => (webSockets.get(host) ?? []).some((ws) => ws.readyState === NativeWebSocket.OPEN), {
        timeout: 10_000,
      })
      .toBe(true)
    webSocketOpened = true
  }
  return { offer, webSocketOpened }
}

/** An app that serves Telefunc with `serve({ req, res })`. */
function nodeApp(telefunc: InstanceType<typeof Telefunc>, server: Server): void {
  server.on('request', async (req, res) => {
    if (!(await telefunc.serve({ req, res }))) res.writeHead(404).end()
  })
}

test('an app on `serve({ req, res })` that never calls `installWebSocket()` offers WebSocket to its first page, and the page moves to it', async () => {
  const server = createServer()
  nodeApp(new Telefunc(), server)
  expect(await openPage(await listen(server))).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
})

test('a page whose request an app on `serve({ req, res })` serves after it began to close its server is offered SSE alone', async () => {
  const server = createServer((req) => {
    // The page's stream request, the first one it sends that the page's offer comes back on.
    if (req.headers.accept === 'text/event-stream' && server.listening) server.close()
  })
  nodeApp(new Telefunc(), server)
  const host = await listen(server)
  void pageOf(host)
    .send('page', { ack: true })
    .catch(() => {}) // The server takes no new connection for the page's other requests.
  await expect.poll(() => offers.get(host)?.length ?? 0, { timeout: 10_000 }).toBeGreaterThan(0)
  expect(server.listening).toBe(false)
  expect(offers.get(host)?.[0]).toEqual(['sse'])
  expect(webSockets.get(host)).toBeUndefined()
}, 20_000)

test("a page of an app on `serve({ req, res })` opened after the app removed Telefunc's upgrade listener is offered SSE alone, and the server gets no upgrade listener back", async () => {
  const server = createServer()
  nodeApp(new Telefunc(), server)
  const host = await listen(server)
  expect(await openPage(host)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
  server.removeAllListeners('upgrade')
  const opened = webSockets.get(host)?.length
  expect(await openPage(host)).toEqual({ offer: ['sse'], webSocketOpened: false })
  expect(webSockets.get(host)?.length).toBe(opened)
  expect(server.listenerCount('upgrade')).toBe(0)
})

test("`installWebSocket()` on an app on `serve({ req, res })` after it removed Telefunc's upgrade listener adds the listener again, and the next page moves to WebSocket", async () => {
  const telefunc = new Telefunc()
  const server = createServer()
  nodeApp(telefunc, server)
  const host = await listen(server)
  expect(await openPage(host)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
  server.removeAllListeners('upgrade')
  telefunc.installWebSocket(server)
  expect(server.listenerCount('upgrade')).toBe(1)
  webSockets.clear()
  expect(await openPage(host)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
})
