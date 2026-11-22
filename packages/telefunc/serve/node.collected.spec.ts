import { expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import type { Socket } from 'node:net'
import { setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'
import { Telefunc } from './node.js'
import { getChannelMux } from '../wire-protocol/channel/server/ChannelMux.js'
import { ServerChannel } from '../wire-protocol/channel/server/ServerChannel.js'
import { startPage } from './page.fixture.js'

// What Telefunc keeps of a Node server the app gave `installWebSocket()`, once the app closed it and let it go. The first
// row has to be the file's first install: Telefunc makes its WebSocket adapter once, at the first install.

setFlagsFromString('--expose-gc')
const gc: () => void = runInNewContext('gc')

/** The app's Telefunc, made once and kept for the process's life, as an app makes it. */
const telefunc = new Telefunc()

/** An app's server: its `request` listener serves Telefunc and closes over the app's state. */
async function startApp(state: {
  cache: ArrayBuffer
}): Promise<{ server: Server; host: string; close: () => Promise<void> }> {
  const server = createServer(async (req, res) => {
    if (!(await telefunc.serve({ req, res }))) res.writeHead(404).end(String(state.cache.byteLength))
  })
  const sockets = new Set<Socket>()
  server.on('connection', (socket: Socket) => {
    sockets.add(socket)
    socket.once('close', () => sockets.delete(socket))
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  const close = async () => {
    const closed = new Promise<void>((resolve) => server.close(() => resolve()))
    for (const socket of sockets) socket.destroy()
    await closed
  }
  return { server, host: `127.0.0.1:${address.port}`, close }
}

/** A page, in a realm of its own, opens a channel to `host`: whether it moved to a WebSocket of its own within 10 s,
 *  whether its sockets closed when it left, and what of it didn't end with its realm. It leaves before its server closes,
 *  so that its server reads its sockets' close as the page leaving and keeps nothing of it for a reconnect. */
async function pageMovesToWebSocket(
  host: string,
): Promise<{ moved: boolean; socketsClosed: boolean; leftBehind: string[] }> {
  const serverChannel = new ServerChannel<string, string>()
  serverChannel.listen((data) => `ack:${data}`)
  getChannelMux().registerChannel(serverChannel)
  const page = await startPage({ telefuncUrl: `http://${host}/_telefunc`, channelId: serverChannel.id })
  let moved = false
  let socketsClosed = false
  let leftBehind: string[] = []
  try {
    expect(await page.send('page')).toBe('ack:page')
    moved = await page.moved(10_000)
    await page.leave()
    socketsClosed = await page.left(10_000)
  } finally {
    leftBehind = await page.end()
  }
  return { moved, socketsClosed, leftBehind }
}

test('a server the app gave `installWebSocket()`, closed and let go, is collected, with what its `request` listener holds', async () => {
  async function serveOnePage(): Promise<{
    server: WeakRef<Server>
    state: WeakRef<object>
    page: { moved: boolean; socketsClosed: boolean; leftBehind: string[] }
  }> {
    const state = { cache: new ArrayBuffer(1024 * 1024) }
    const app = await startApp(state)
    telefunc.installWebSocket(app.server)
    const page = await pageMovesToWebSocket(app.host)
    await app.close()
    return { server: new WeakRef(app.server), state: new WeakRef(state), page }
  }
  const { server, state, page } = await serveOnePage()
  for (let i = 0; i < 10; i++) {
    gc()
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
  expect({
    ...page,
    serverCollected: server.deref() === undefined,
    stateCollected: state.deref() === undefined,
  }).toEqual({
    moved: true,
    socketsClosed: true,
    leftBehind: [],
    serverCollected: true,
    stateCollected: true,
  })
}, 20_000)

test('a second server the app gives `installWebSocket()` after the first closed moves its pages to WebSocket', async () => {
  const app = await startApp({ cache: new ArrayBuffer(0) })
  telefunc.installWebSocket(app.server)
  try {
    expect(await pageMovesToWebSocket(app.host)).toEqual({ moved: true, socketsClosed: true, leftBehind: [] })
  } finally {
    await app.close()
  }
}, 20_000)
