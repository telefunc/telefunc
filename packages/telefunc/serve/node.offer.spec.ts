import { afterAll, afterEach, beforeAll, expect, test } from 'vitest'
import { once } from 'node:events'
import { createServer, Server } from 'node:http'
import { connect, createServer as createTcpServer, type Socket } from 'node:net'
import { serve as honoServe } from '@hono/node-server'
import { Hono } from 'hono'
import { Telefunc } from './node.js'
import { nodeReadableToWebRequest } from '../utils/nodeReadableToWebRequest.js'
import { getChannelMux } from '../wire-protocol/channel/server/ChannelMux.js'
import { ServerChannel } from '../wire-protocol/channel/server/ServerChannel.js'
import { ClientChannel } from '../wire-protocol/channel/client/ClientChannel.js'
import { decode, isOneFrame, messageFrames } from '../wire-protocol/wire/frame.js'
import { TAG } from '../wire-protocol/wire/tags.js'
import { base64urlToUint8Array } from '../wire-protocol/wire/base64url.js'

// Which page is offered WebSocket, as an app on `serve({ request })` (Hono, srvx…) sees it: each page's real connection
// to real Node servers, the offer read from the RECONCILED its SSE wire carries, the WebSocket from the page's own.

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
beforeAll(() => {
  globalThis.WebSocket = RecordedWebSocket
})
afterAll(() => {
  globalThis.WebSocket = NativeWebSocket
})

const closers: (() => Promise<void>)[] = []
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close()
  webSockets.clear()
})

type App = { telefunc: InstanceType<typeof Telefunc>; server: Server; host: string; offers: string[][] }

/** An app that serves Telefunc with `serve({ request })` and no upgrade listener of its own. */
async function fetchApp(port = 0): Promise<App> {
  const telefunc = new Telefunc()
  const offers: string[][] = []
  const server = createServer(async (req, res) => {
    const { request } = await nodeReadableToWebRequest(
      req,
      `http://${req.headers.host}${req.url}`,
      req.method ?? 'GET',
      req.headers,
    )
    const response = await telefunc.serve({ request })
    if (!response) return void res.writeHead(404).end()
    res.writeHead(response.status, Object.fromEntries(response.headers))
    res.flushHeaders()
    if (!response.body) return void res.end()
    const record = response.headers.get('content-type') === 'text/event-stream' ? offerRecorder(offers) : () => {}
    const reader = response.body.getReader()
    try {
      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        record(value)
        res.write(value)
      }
      res.end()
    } catch {
      res.destroy()
    }
  })
  closers.push(closerOf(server))
  await new Promise<void>((resolve) => server.listen(port, '127.0.0.1', resolve))
  return { telefunc, server, host: hostOf(server), offers }
}

/** The setup the docs give for Hono (docs/pages/Telefunc/+Page.mdx): `serve()` of `@hono/node-server`, then
 *  `installWebSocket()` on the server it returns. */
async function honoApp(): Promise<App> {
  const telefunc = new Telefunc()
  const offers: string[][] = []
  const app = new Hono()
  app.all('/_telefunc', async (c) => (await telefunc.serve({ request: c.req.raw })) ?? c.notFound())
  const server = honoServe({
    fetch: async (request) => {
      const response = await app.fetch(request)
      if (response.headers.get('content-type') !== 'text/event-stream' || !response.body) return response
      const [toPage, toTest] = response.body.tee()
      const record = offerRecorder(offers)
      void toTest.pipeTo(new WritableStream({ write: record })).catch(() => {}) // Fails once the page aborts.
      return new Response(toPage, response)
    },
    port: 0,
    hostname: '127.0.0.1',
  })
  // `serve()` is typed as returning any of Node's HTTP servers, and `installWebSocket()` doesn't take its HTTP/2 one.
  if (!(server instanceof Server)) throw new Error('@hono/node-server made no `http.Server`')
  telefunc.installWebSocket(server)
  closers.push(closerOf(server))
  await once(server, 'listening')
  return { telefunc, server, host: hostOf(server), offers }
}

/** Records the transports each RECONCILED of an SSE stream offers, fed the stream's chunks in order. */
function offerRecorder(offers: string[][]): (chunk: Uint8Array) => void {
  const decoder = new TextDecoder()
  let events = ''
  return (chunk) => {
    events += decoder.decode(chunk, { stream: true })
    const complete = events.split('\n\n')
    events = complete.pop() ?? ''
    for (const event of complete) offers.push(...offersIn(event))
  }
}

/** Closes the server and destroys the connections still open to it. */
function closerOf(server: Server | ReturnType<typeof createTcpServer>): () => Promise<void> {
  const sockets = new Set<Socket>()
  server.on('connection', (socket: Socket) => {
    sockets.add(socket)
    socket.once('close', () => sockets.delete(socket))
  })
  return () => closeServer(server, sockets)
}

/** The transports each RECONCILED in an SSE event offers. */
function offersIn(event: string): string[][] {
  if (!event.startsWith('data: ')) return []
  const raw = base64urlToUint8Array(event.slice('data: '.length))
  return (isOneFrame(raw) ? [raw] : [...messageFrames(raw)])
    .map((bytes) => decode(bytes))
    .flatMap((frame) => (frame.tag === TAG.RECONCILED ? [frame.payload.transports] : []))
}

function hostOf(server: Server | ReturnType<typeof createTcpServer>): string {
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  return `127.0.0.1:${address.port}`
}

async function closeServer(server: Server | ReturnType<typeof createTcpServer>, sockets: Set<Socket>): Promise<void> {
  const closed = new Promise<void>((resolve) => server.close(() => resolve()))
  for (const socket of sockets) socket.destroy()
  await closed
}

/** A page opens a channel through `host` (the app's, or a proxy's in front of it): the transports its wire is offered, and
 *  whether its WebSocket opened. */
async function openPage(app: App, host = app.host): Promise<{ offer: string[] | undefined; webSocketOpened: boolean }> {
  const offersBefore = app.offers.length
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
  // The page leaves before its server, as a closed tab's page does: one whose wire drops before the server has its
  // close reconnects to deliver it, to whatever listens on that address by then.
  closers.push(async () => {
    await page.close()
  })
  expect(await page.send('page', { ack: true })).toBe('ack:page')
  const offer = app.offers[offersBefore]
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

test('a Fetch-only app offers its pages SSE alone', async () => {
  const app = await fetchApp()
  expect(await openPage(app)).toEqual({ offer: ['sse'], webSocketOpened: false })
  expect(webSockets.get(app.host)).toBeUndefined()
})

test('a page opened after the app installed its upgrade listener is offered WebSocket, and moves to it', async () => {
  const app = await fetchApp()
  expect(await openPage(app)).toEqual({ offer: ['sse'], webSocketOpened: false })
  app.telefunc.installWebSocket(app.server)
  expect(await openPage(app)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
})

test('of two apps in one process, only the one with an upgrade listener offers WebSocket', async () => {
  const a = await fetchApp()
  a.telefunc.installWebSocket(a.server)
  const b = await fetchApp()
  expect(await openPage(a)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
  expect(await openPage(b)).toEqual({ offer: ['sse'], webSocketOpened: false })
  expect(webSockets.get(b.host)).toBeUndefined()
})

test('a Fetch-only app started on the address of one with an upgrade listener offers SSE alone', async () => {
  const a = await fetchApp()
  a.telefunc.installWebSocket(a.server)
  expect(await openPage(a)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
  const port = Number(a.host.split(':')[1])
  for (const close of closers.splice(0).reverse()) await close()
  webSockets.clear()
  const b = await fetchApp(port)
  expect(await openPage(b)).toEqual({ offer: ['sse'], webSocketOpened: false })
  expect(webSockets.get(b.host)).toBeUndefined()
})

test('an app behind a proxy that forwards upgrades on its public port offers WebSocket, and its pages move to it', async () => {
  const app = await fetchApp()
  app.telefunc.installWebSocket(app.server)
  const [appHost, appPort] = app.host.split(':')
  const sockets = new Set<Socket>()
  const proxy = createTcpServer((client) => {
    const upstream = connect(Number(appPort), appHost)
    for (const socket of [client, upstream]) {
      sockets.add(socket)
      socket.once('close', () => sockets.delete(socket))
      socket.on('error', () => {
        client.destroy()
        upstream.destroy()
      })
    }
    client.pipe(upstream).pipe(client)
  })
  await new Promise<void>((resolve) => proxy.listen(0, '127.0.0.1', resolve))
  closers.push(() => closeServer(proxy, sockets))
  const publicHost = hostOf(proxy)
  expect(await openPage(app, publicHost)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
})

test("Telefunc wraps a server's events once, however often `installWebSocket()` runs on it, and every other event reaches its listeners with its arguments and `emit()`'s answer", () => {
  const server = createServer()
  new Telefunc().installWebSocket(server)
  const wrapped = server.emit
  new Telefunc().installWebSocket(server)
  server.removeAllListeners('upgrade')
  new Telefunc().installWebSocket(server)
  expect(server.emit).toBe(wrapped)
  const got: unknown[][] = []
  server.on('app-event', (...args: unknown[]) => got.push(args))
  expect(server.emit('app-event', 1, 'two')).toBe(true)
  expect(server.emit('no-listener')).toBe(false)
  expect(got).toEqual([[1, 'two']])
})

test("the docs' Hono app offers WebSocket, and its pages move to it", async () => {
  const app = await honoApp()
  expect(await openPage(app)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
})
