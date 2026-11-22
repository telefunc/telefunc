import { afterEach, expect, test } from 'vitest'
import { createServer, type IncomingMessage, type Server } from 'node:http'
import { connect } from 'node:net'
import type { Duplex } from 'node:stream'
import { hasProp } from '../utils/hasProp.js'
import { Telefunc } from './node.js'
import { rowOfPages } from './page.fixture.js'

// Which page is offered WebSocket, as an app on `serve({ req, res })` sees it: each page's real connection to a real Node
// server, the offer read on the page's side off the RECONCILED its SSE stream carries, the WebSocket from the page's own.
// Each page runs in a realm of its own, which the row's end ends; a server closes only after the pages opened since did.

const row = rowOfPages()
afterEach(async () => {
  await row.end()
  expect(row.left.splice(0)).toEqual([])
})

async function listen(server: Server): Promise<string> {
  row.closers.push(row.closerOf(server))
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  return hostOf(server)
}

function hostOf(server: Server): string {
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  return `127.0.0.1:${address.port}`
}

/** The transports the page's wire is offered, and whether it moved to a WebSocket of its own. */
async function openPage(host: string): Promise<{ offer: string[] | undefined; webSocketOpened: boolean }> {
  const page = await row.page(host)
  expect(await page.send('page')).toBe('ack:page')
  const [offer] = page.offers
  return { offer, webSocketOpened: await page.moved(offer?.includes('ws') ? 10_000 : 0) }
}

/** An app that serves Telefunc with `serve({ req, res })`. */
function nodeApp(telefunc: InstanceType<typeof Telefunc>, server: Server): void {
  server.on('request', async (req, res) => {
    if (!(await telefunc.serve({ req, res }))) res.writeHead(404).end()
  })
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

// Node has it from 22.21 and 24.9; its typings here don't.
const nodeHasTheCallback = hasProp(createServer(), 'shouldUpgradeCallback', 'function')

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
  // The server takes no new connection for the page's other requests, nor for its goodbye.
  const page = await row.page(host, { serverTold: false })
  void page.send('page').catch(() => {})
  await expect.poll(() => page.offers.length, { timeout: 10_000 }).toBeGreaterThan(0)
  expect(server.listening).toBe(false)
  expect(page.offers[0]).toEqual(['sse'])
  expect(await row.socketsTo(host)).toEqual([])
}, 20_000)

test("a page of an app on `serve({ req, res })` opened after the app removed Telefunc's upgrade listener is offered SSE alone, and the server gets no upgrade listener back", async () => {
  const server = createServer()
  nodeApp(new Telefunc(), server)
  const host = await listen(server)
  expect(await openPage(host)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
  server.removeAllListeners('upgrade')
  const opened = (await row.socketsTo(host)).length
  expect(await openPage(host)).toEqual({ offer: ['sse'], webSocketOpened: false })
  expect(await row.socketsTo(host)).toHaveLength(opened)
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
  expect(await openPage(host)).toEqual({ offer: ['sse', 'ws'], webSocketOpened: true })
})

test("an app on `serve({ req, res })` that called `installWebSocket()` and removed Telefunc's upgrade listener before its first page offers that page SSE alone, and its own upgrade route answers as before", async () => {
  const telefunc = new Telefunc()
  const server = createServer()
  nodeApp(telefunc, server)
  telefunc.installWebSocket(server)
  server.removeAllListeners('upgrade')
  server.on('upgrade', appUpgrade)
  const host = await listen(server)
  expect(await openPage(host)).toEqual({ offer: ['sse'], webSocketOpened: false })
  expect(await row.socketsTo(host)).toEqual([])
  expect(server.listeners('upgrade')).toEqual([appUpgrade])
  expect(await upgradeStatus(host, '/app-ws')).toBe(418)
})

test.runIf(nodeHasTheCallback)(
  'an app on `serve({ req, res })` that set its own `shouldUpgradeCallback` after `installWebSocket()` keeps it at its first page, which is offered SSE alone',
  async () => {
    const telefunc = new Telefunc()
    const server = createServer()
    nodeApp(telefunc, server)
    server.on('upgrade', appUpgrade)
    telefunc.installWebSocket(server)
    const appsCallback = (req: IncomingMessage) => req.url === '/app-ws'
    // What an app's `server.shouldUpgradeCallback = …` does, which the typings here don't know.
    Reflect.set(server, 'shouldUpgradeCallback', appsCallback)
    const host = await listen(server)
    expect(await openPage(host)).toEqual({ offer: ['sse'], webSocketOpened: false })
    expect(hasProp(server, 'shouldUpgradeCallback') && server.shouldUpgradeCallback).toBe(appsCallback)
    expect(await upgradeStatus(host, '/app-ws')).toBe(418)
  },
)
