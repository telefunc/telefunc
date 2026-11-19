// How each end's `closed` reads when the page leaves, or the page's connection is lost for good during a close. Over a
// real server and page, on a WebSocket the spec can cut.

import { afterAll, beforeAll, expect, test } from 'vitest'
import { createServer, type IncomingMessage, type Server } from 'node:http'
import type { Duplex } from 'node:stream'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { NetworkError } from '../../shared/NetworkError.js'
import { ChannelCloseUnknownError, ChannelPageLeftError } from './channelErrors.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { getChannelMux } from './server/ChannelMux.js'

// The page's transport listens for `pagehide` on the global object, as in a browser.
const pageEvents = new EventTarget()
Object.assign(globalThis, {
  addEventListener: pageEvents.addEventListener.bind(pageEvents),
  removeEventListener: pageEvents.removeEventListener.bind(pageEvents),
})
/** The page unloads: it is closed, reloaded or navigated away from. */
const leavePage = () => pageEvents.dispatchEvent(Object.assign(new Event('pagehide'), { persisted: false }))

// The page gives up past two ping intervals and `reconnectTimeout`: 3 s.
serverConfig.channel = { transports: ['ws'], pingInterval: 1_000, reconnectTimeout: 1_000 }
serverConfig.disableNamingConvention = true

/** What the server does with the page's next WebSocket upgrades. */
const upgrades = { refuse: false, otherWireVersion: false }
const sockets = new Set<Duplex>()
let server: Server
let telefuncUrl: string

beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  const handlers = server.listeners('upgrade') as ((req: IncomingMessage, socket: Duplex, head: Buffer) => void)[]
  server.removeAllListeners('upgrade')
  server.on('upgrade', (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    if (upgrades.refuse) return void socket.destroy()
    if (upgrades.otherWireVersion) req.url = req.url?.replace(/([?&]wire=)\d+/, '$10')
    sockets.add(socket)
    for (const handler of handlers) handler(req, socket, head)
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

/** A server channel and the page's end of it, both open, on a connection of their own. */
async function pair() {
  Object.assign(upgrades, { refuse: false, otherWireVersion: false })
  const channel = new ServerChannel<unknown, unknown>()
  getChannelMux().registerChannel(channel)
  const page = new ClientChannel<unknown, unknown>({
    channelId: channel.id,
    connection: { telefuncUrl, transports: ['ws'], idleTimeout: 0, connectionKey: crypto.randomUUID() },
  })
  await Promise.all([channel.opened, page.opened])
  return [channel, page] as const
}

/** The page's WebSocket drops, as a failing network drops it. */
function dropWire(): void {
  for (const socket of sockets) socket.destroy()
  sockets.clear()
}

/** The page began a close the server's cleanup still holds: the server hasn't sent its CLOSE_DONE. */
async function pageInClose() {
  const [channel, page] = await pair()
  channel.onClose(() => new Promise((resolve) => setTimeout(resolve, 5_000)))
  const closing = page.close({ timeout: 8_000 })
  await new Promise((resolve) => setTimeout(resolve, 100))
  return { channel, page, closing }
}

test("a page that leaves ends the server's channel with ChannelPageLeftError, a channel NetworkError", async () => {
  const [channel] = await pair()
  const onClose: unknown[] = []
  channel.onClose((err) => void onClose.push(err))
  leavePage()
  const end = await channel.closed
  expect(end).toBeInstanceOf(ChannelPageLeftError)
  expect(end).toBeInstanceOf(NetworkError)
  expect(end && (end as NetworkError).isChannel).toBe(true)
  expect(onClose).toEqual([end])
})

test('a page in a close whose connection is lost for good ends with ChannelCloseUnknownError, the loss its cause', async () => {
  const { page, closing } = await pageInClose()
  upgrades.refuse = true
  dropWire()
  const end = await page.closed
  expect(end).toBeInstanceOf(ChannelCloseUnknownError)
  expect(end?.cause).toBeInstanceOf(NetworkError)
  expect((end?.cause as NetworkError).isChannel).toBe(true)
  await expect(closing).resolves.toBe(end)
}, 10_000)

test('a page in a close whose reconnect finds the channel gone on the server ends with ChannelCloseUnknownError', async () => {
  const { page, closing } = await pageInClose()
  // The server lets go of the channel, and the page, still running here, reconnects.
  leavePage()
  const end = await page.closed
  expect(end).toBeInstanceOf(ChannelCloseUnknownError)
  expect(end?.cause).toEqual(new NetworkError('Channel not acknowledged by server after reconnect', true))
  await expect(closing).resolves.toBe(end)
}, 10_000)

test('a page in a close whose reconnect reaches an updated server ends with ChannelCloseUnknownError', async () => {
  const { page, closing } = await pageInClose()
  upgrades.otherWireVersion = true
  dropWire()
  const end = await page.closed
  expect(end).toBeInstanceOf(ChannelCloseUnknownError)
  expect(end?.cause).toEqual(new NetworkError('The server was updated since this page loaded: reload the page.', true))
  await expect(closing).resolves.toBe(end)
}, 10_000)
