import { afterEach, expect, test } from 'vitest'
import { once } from 'node:events'
import { createServer, type IncomingMessage, request as httpRequest, Server } from 'node:http'
import { connect, createServer as createTcpServer, type Socket } from 'node:net'
import { serve as honoServe } from '@hono/node-server'
import { Hono } from 'hono'
import type { Duplex } from 'node:stream'
import { Telefunc } from './node.js'
import { hasProp } from '../utils/hasProp.js'
import { nodeReadableToWebRequest } from '../utils/nodeReadableToWebRequest.js'
import { decode, isOneFrame, messageFrames } from '../wire-protocol/wire/frame.js'
import { TAG } from '../wire-protocol/wire/tags.js'
import { base64urlToUint8Array } from '../wire-protocol/wire/base64url.js'
import { rowOfPages, type Page } from './page.fixture.js'

// Which page is offered WebSocket, as an app on `serve({ request })` (Hono, srvx…) sees it: each page's real connection
// to real Node servers, the offer read from the RECONCILED its SSE wire carries, the WebSocket from the page's own. Each
// page runs in a realm of its own, which the row's end ends; a server closes only after the pages opened since did.

/** What the next page sends as its own `x-telefunc-server` header, on each of its requests. */
let clientMarker: string | undefined

const row = rowOfPages()
afterEach(async () => {
  await row.end()
  clientMarker = undefined
  expect(row.left.splice(0)).toEqual([])
})

/** `markers`: the `x-telefunc-server` header of each request the app's handler got with one. */
type App = {
  telefunc: InstanceType<typeof Telefunc>
  server: Server
  host: string
  offers: string[][]
  markers: string[]
}

/** An app that serves Telefunc with `serve({ request })` and no upgrade listener of its own. */
async function fetchApp(port = 0): Promise<App> {
  const telefunc = new Telefunc()
  const offers: string[][] = []
  const markers: string[] = []
  const server = createServer(async (req, res) => {
    const marker = req.headers['x-telefunc-server']
    if (typeof marker === 'string') markers.push(marker)
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
  row.closers.push(row.closerOf(server))
  await new Promise<void>((resolve) => server.listen(port, '127.0.0.1', resolve))
  return { telefunc, server, host: hostOf(server), offers, markers }
}

/** The setup the docs give for Hono (docs/pages/Telefunc/+Page.mdx): `serve()` of `@hono/node-server`, then
 *  `installWebSocket()` on the server it returns. */
async function honoApp(): Promise<App> {
  const telefunc = new Telefunc()
  const offers: string[][] = []
  const markers: string[] = []
  const app = new Hono()
  app.all('/_telefunc', async (c) => {
    const marker = c.req.header('x-telefunc-server')
    if (marker !== undefined) markers.push(marker)
    return (await telefunc.serve({ request: c.req.raw })) ?? c.notFound()
  })
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
  row.closers.push(row.closerOf(server))
  await once(server, 'listening')
  return { telefunc, server, host: hostOf(server), offers, markers }
}

/** The app's own WebSocket server, for `/app-ws`: it answers 418, so a row can tell its answer from anyone else's. */
function appUpgrade(req: IncomingMessage, socket: Duplex): void {
  if (req.url === '/app-ws') socket.end("HTTP/1.1 418 I'm a teapot\r\nContent-Length: 0\r\n\r\n")
}

/** The status a WebSocket handshake for `path` gets, or null where none comes within 2 s. */
async function upgradeStatus(host: string, path: string): Promise<number | null> {
  const [hostname, port] = host.split(':')
  const socket = connect(Number(port), hostname)
  socket.on('error', () => {})
  const { promise: answered, resolve } = Promise.withResolvers<number | null>()
  socket.once('data', (chunk: Buffer) => resolve(Number(chunk.toString('latin1').split(' ')[1])))
  socket.once('close', () => resolve(null))
  const timer = setTimeout(() => resolve(null), 2_000)
  socket.write(
    `GET ${path} HTTP/1.1\r\nHost: ${host}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n`,
  )
  const status = await answered
  clearTimeout(timer)
  socket.destroy()
  return status
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

/** A page opens a channel through `host`, and its first send is answered: the transports its wire is offered. */
async function connectPage(app: App, host = app.host): Promise<{ page: Page; offer: string[] | undefined }> {
  const offersBefore = app.offers.length
  const page = await row.page(host, { marker: clientMarker })
  expect(await page.send('page')).toBe('ack:page')
  return { page, offer: app.offers[offersBefore] }
}

/** A page opens a channel through `host`: the transports its wire is offered, and whether it moved to a WebSocket of its
 *  own. */
async function openPage(app: App, host = app.host): Promise<{ offer: string[] | undefined; webSocketOpened: boolean }> {
  const { page, offer } = await connectPage(app, host)
  return { offer, webSocketOpened: await page.moved(offer?.includes('ws') ? 10_000 : 0) }
}

test('a Fetch-only app offers its pages SSE alone', async () => {
  const app = await fetchApp()
  expect(await openPage(app)).toEqual({ offer: ['sse'], webSocketOpened: false })
  expect(await row.socketsTo(app.host)).toEqual([])
})

test('a page opened after the app installed its upgrade listener is offered WebSocket, and moves to it', async () => {
  const app = await fetchApp()
  expect(await openPage(app)).toEqual({ offer: ['sse'], webSocketOpened: false })
  app.telefunc.installWebSocket(app.server)
  expect(await openPage(app)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
})

test("a page opened after the app removed Telefunc's upgrade listener is offered SSE alone, and opens no WebSocket", async () => {
  const app = await fetchApp()
  app.telefunc.installWebSocket(app.server)
  expect(await openPage(app)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
  app.server.removeAllListeners('upgrade')
  const opened = (await row.socketsTo(app.host)).length
  expect(await openPage(app)).toEqual({ offer: ['sse'], webSocketOpened: false })
  expect(await row.socketsTo(app.host)).toHaveLength(opened)
})

test("a page opened after the app removed Telefunc's upgrade listener, with no `removeListener` listener left to see it, is offered SSE alone", async () => {
  const app = await fetchApp()
  app.telefunc.installWebSocket(app.server)
  expect(await openPage(app)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
  app.server.removeAllListeners('removeListener')
  app.server.removeAllListeners('upgrade')
  expect(await openPage(app)).toEqual({ offer: ['sse'], webSocketOpened: false })
})

test("`installWebSocket()` after the app removed every `removeListener` listener and then Telefunc's upgrade listener takes the server again: the next page moves to WebSocket, and the app's own upgrade route answers as before", async () => {
  const app = await fetchApp()
  app.telefunc.installWebSocket(app.server)
  app.server.on('upgrade', appUpgrade)
  expect(await upgradeStatus(app.host, '/app-ws')).toBe(418)
  app.server.removeAllListeners('removeListener')
  app.server.removeAllListeners('upgrade')
  app.server.on('upgrade', appUpgrade)
  app.telefunc.installWebSocket(app.server)
  expect(await openPage(app)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
  expect(app.server.listenerCount('upgrade')).toBe(2)
  expect(await upgradeStatus(app.host, '/app-ws')).toBe(418)
})

// Node has it from 22.21 and 24.9; its typings here don't.
const nodeHasTheCallback = hasProp(createServer(), 'shouldUpgradeCallback', 'function')
/** The app's own `shouldUpgradeCallback`, which upgrades only its own WebSocket server's path. */
const appsCallback = (req: IncomingMessage) => req.url === '/app-ws'

test.runIf(nodeHasTheCallback)(
  "a page opened while the app's own `shouldUpgradeCallback` is in place of Telefunc's is offered SSE alone",
  async () => {
    const app = await fetchApp()
    app.telefunc.installWebSocket(app.server)
    // What an app's `server.shouldUpgradeCallback = …` does, which the typings here don't know.
    Reflect.set(app.server, 'shouldUpgradeCallback', appsCallback)
    expect((await connectPage(app)).offer).toEqual(['sse'])
  },
)

test.runIf(nodeHasTheCallback)(
  "`installWebSocket()` after the app put its own `shouldUpgradeCallback` in place of Telefunc's moves the next page to WebSocket, and removing Telefunc's listener gives the app's callback back",
  async () => {
    const app = await fetchApp()
    app.telefunc.installWebSocket(app.server)
    Reflect.set(app.server, 'shouldUpgradeCallback', appsCallback)
    app.telefunc.installWebSocket(app.server)
    expect(await openPage(app)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
    app.server.removeAllListeners('upgrade')
    expect(hasProp(app.server, 'shouldUpgradeCallback') && app.server.shouldUpgradeCallback).toBe(appsCallback)
  },
  20_000,
)

test('of two apps in one process, only the one with an upgrade listener offers WebSocket', async () => {
  const a = await fetchApp()
  a.telefunc.installWebSocket(a.server)
  const b = await fetchApp()
  expect(await openPage(a)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
  expect(await openPage(b)).toEqual({ offer: ['sse'], webSocketOpened: false })
  expect(await row.socketsTo(b.host)).toEqual([])
})

test('a Fetch-only app started on the address of one with an upgrade listener offers SSE alone', async () => {
  const a = await fetchApp()
  a.telefunc.installWebSocket(a.server)
  expect(await openPage(a)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
  const port = Number(a.host.split(':')[1])
  await row.end()
  const b = await fetchApp(port)
  expect(await openPage(b)).toEqual({ offer: ['sse'], webSocketOpened: false })
  expect(await row.socketsTo(b.host)).toEqual([])
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
  row.closers.push(() => row.closeOnceIdle(proxy, sockets))
  const publicHost = hostOf(proxy)
  expect(await openPage(app, publicHost)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
})

test("Telefunc wraps a server's events once, however often `installWebSocket()` runs on it, and every other event reaches its listeners with its arguments and `emit()`'s answer", () => {
  const server = createServer()
  const own = server.emit
  new Telefunc().installWebSocket(server)
  const wrapped = server.emit
  expect(wrapped).not.toBe(own)
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

test("a page that sends its own `x-telefunc-server` header is offered what its server takes: the docs' Hono app WebSocket, a Fetch-only app beside it SSE alone", async () => {
  const hono = await honoApp()
  const other = await fetchApp()
  clientMarker = 'made-up'
  expect(await openPage(hono)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
  expect(hono.markers).not.toEqual([])
  expect(hono.markers.every((marker) => marker.startsWith('made-up, '))).toBe(true)
  expect(await openPage(other)).toEqual({ offer: ['sse'], webSocketOpened: false })
  expect(await row.socketsTo(other.host)).toEqual([])
})

test("a page that sends the token of the docs' Hono app to a Fetch-only app beside it is offered WebSocket there: its upgrade is refused, its channel answers over SSE, and the Hono app's server sees no upgrade", async () => {
  const hono = await honoApp()
  let honoUpgrades = 0
  hono.server.on('upgrade', () => honoUpgrades++)
  expect(await openPage(hono)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
  expect(honoUpgrades).toBe(1)
  const [token] = hono.markers
  expect(token).toMatch(/^[0-9a-f-]{36}$/)
  const other = await fetchApp()
  clientMarker = token
  const { page, offer } = await connectPage(other)
  expect(offer).toEqual(['sse', 'ws'])
  await expect.poll(async () => (await page.sockets()).some((ws) => ws.endsWith(', failed'))).toBe(true)
  expect(await page.send('again')).toBe('ack:again')
  expect((await page.sockets()).some((ws) => ws.endsWith(', open'))).toBe(false)
  expect(honoUpgrades).toBe(1)
})

test("a page whose stream request carries its server's token after the server began to close is offered SSE alone, and the closing server can't be told the page left", async () => {
  const app = await fetchApp()
  app.telefunc.installWebSocket(app.server)
  // Before the app's handler: the page's stream request, the one its offer comes back on.
  app.server.prependListener('request', (req) => {
    if (req.headers.accept === 'text/event-stream' && app.server.listening) app.server.close()
  })
  // After the page left: the closing server refused its goodbye's connection.
  let goodbyes: string[] = []
  row.closers.push(async () => {
    if (goodbyes.join() !== 'refused: ECONNREFUSED') row.left.push(`goodbye ${goodbyes.join(', ') || 'not sent'}`)
  })
  const offersBefore = app.offers.length
  // The server takes no new connection for the page's other requests, nor for its goodbye. A tab's unload ends the
  // page's requests sent without `keepalive`, its stream too: of a page whose goodbye can't arrive, its stream's end is
  // all its server sees.
  const page = await row.page(app.host, { marker: clientMarker, serverTold: false, unload: true })
  goodbyes = page.goodbyes
  void page.send('page').catch(() => {})
  await expect.poll(() => app.offers.length, { timeout: 10_000 }).toBeGreaterThan(offersBefore)
  expect(app.server.listening).toBe(false)
  expect(app.markers).not.toEqual([])
  expect(app.offers[offersBefore]).toEqual(['sse'])
  expect(await row.socketsTo(app.host)).toEqual([])
}, 20_000)

test('an app whose server closed and listens again offers WebSocket, and the token it marked requests with before names no server', async () => {
  const app = await fetchApp()
  app.telefunc.installWebSocket(app.server)
  expect(await openPage(app)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
  const [token] = app.markers
  expect(token).toMatch(/^[0-9a-f-]{36}$/)
  await row.end()
  const other = await fetchApp()
  clientMarker = token
  expect(await openPage(other)).toEqual({ offer: ['sse'], webSocketOpened: false })
  clientMarker = undefined
  row.closers.push(row.closerOf(app.server))
  await new Promise<void>((resolve) => app.server.listen(0, '127.0.0.1', resolve))
  expect(await openPage({ ...app, host: hostOf(app.server) })).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
})

test("an app that installs Telefunc's WebSocket again from its own `removeListener` listener when Telefunc's upgrade listener is removed keeps one upgrade listener, and its pages move to WebSocket", async () => {
  const app = await fetchApp()
  app.server.on('removeListener', (event: string | symbol) => {
    if (event === 'upgrade' && app.server.listenerCount('upgrade') === 0) app.telefunc.installWebSocket(app.server)
  })
  app.telefunc.installWebSocket(app.server)
  app.server.removeAllListeners('upgrade')
  expect(app.server.listenerCount('upgrade')).toBe(1)
  expect(await openPage(app)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
})

/** An HTTP proxy in front of `host` that forwards each request, upgrades too, with its path rewritten by `rewrite`, as
 *  it rewrote it: no normalizing on the way. */
async function rewritingProxy(host: string, rewrite: (path: string) => string): Promise<string> {
  const [hostname, port] = host.split(':')
  const sockets = new Set<Socket>()
  const proxy = createServer((req, res) => {
    // Each request on a connection of its own, which ends with its client's, as a proxy's to its app does.
    const upstream = httpRequest(
      {
        hostname,
        port: Number(port),
        method: req.method,
        path: rewrite(req.url ?? '/'),
        headers: req.headers,
        agent: false,
      },
      (response) => {
        res.writeHead(response.statusCode ?? 502, response.headers)
        response.pipe(res)
      },
    )
    upstream.on('error', () => res.destroy())
    res.once('close', () => upstream.destroy())
    req.pipe(upstream)
  })
  proxy.on('upgrade', (req, client: Socket, head: Buffer) => {
    const upstream = connect(Number(port), hostname, () => {
      const lines = [`${req.method} ${rewrite(req.url ?? '/')} HTTP/1.1`]
      for (let i = 0; i < req.rawHeaders.length; i += 2) lines.push(`${req.rawHeaders[i]}: ${req.rawHeaders[i + 1]}`)
      upstream.write(`${lines.join('\r\n')}\r\n\r\n`)
      upstream.write(head)
      client.pipe(upstream).pipe(client)
    })
    for (const socket of [client, upstream]) {
      sockets.add(socket)
      socket.once('close', () => sockets.delete(socket))
      socket.on('error', () => {
        client.destroy()
        upstream.destroy()
      })
    }
  })
  proxy.on('connection', (socket: Socket) => {
    sockets.add(socket)
    socket.once('close', () => sockets.delete(socket))
  })
  await new Promise<void>((resolve) => proxy.listen(0, '127.0.0.1', resolve))
  row.closers.push(() => row.closeOnceIdle(proxy, sockets))
  return hostOf(proxy)
}

test('a page behind a proxy that forwards its requests with a dot segment in the path is offered WebSocket, and moves to it', async () => {
  const app = await fetchApp()
  app.telefunc.installWebSocket(app.server)
  const publicHost = await rewritingProxy(app.host, (path) => path.replace(/^\/_telefunc/, '/x/../_telefunc'))
  expect(await openPage(app, publicHost)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
  expect(app.markers).not.toEqual([])
})
