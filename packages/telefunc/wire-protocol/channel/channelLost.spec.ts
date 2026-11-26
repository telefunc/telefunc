// How each end's `closed` reads when the page leaves, or the page's connection is lost for good during a close; a
// close the page makes while its connection is down; and what an ended broadcast keeps of its page's wire. Over a
// real server and page, on a WebSocket the spec can cut.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { createServer, type IncomingMessage, type Server } from 'node:http'
import { setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'
import type { Duplex } from 'node:stream'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { NetworkError } from '../../shared/NetworkError.js'
import { ChannelClosedError, ChannelCloseUnknownError, ChannelPageLeftError } from './channelErrors.js'
import { ServerChannel, ATTACHABLE_NOW } from './server/ServerChannel.js'
import { ServerBroadcast } from './server/ServerBroadcast.js'
import type { IndexedPeer } from './server/IndexedPeer.js'
import { ClientChannel } from './client/ClientChannel.js'
import { ClientBroadcast } from './client/ClientBroadcast.js'
import { Abort } from '../../shared/Abort.js'
import { getChannelMux } from './server/ChannelMux.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'

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
/** When each of the page's upgrades reached the server, the refused ones too. */
const upgradeAttempts: number[] = []
let server: Server
let telefuncUrl: string

beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  // Telefunc takes the upgrades this server hands on, as they reach it.
  const telefuncUpgrades = createServer()
  telefunc.installWebSocket(telefuncUpgrades)
  server.on('upgrade', (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    upgradeAttempts.push(performance.now())
    if (upgrades.refuse) return void socket.destroy()
    if (upgrades.otherWireVersion) req.url = req.url?.replace(/([?&]wire=)\d+/, '$10')
    sockets.add(socket)
    telefuncUpgrades.emit('upgrade', req, socket, head)
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
  getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
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
  expect(end instanceof NetworkError && end.isChannel).toBe(true)
  expect(onClose).toEqual([end])
})

test('a page in a close whose connection is lost for good ends with ChannelCloseUnknownError, the loss its cause', async () => {
  const { page, closing } = await pageInClose()
  upgrades.refuse = true
  dropWire()
  const end = await page.closed
  expect(end).toBeInstanceOf(ChannelCloseUnknownError)
  const cause = end?.cause
  expect(cause instanceof NetworkError && cause.isChannel).toBe(true)
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

/** The page's connections, by the key `ClientConnection.getOrCreate()` files them under: a disposed one is gone. */
const { connections } = getGlobalObject('channel/client/ClientConnection.ts', {
  connections: new Map<string, object>(),
  wanting: new Map<string, Set<object>>(),
})
const connectionGone = (page: ClientChannel<unknown, unknown>) => ![...connections.values()].includes(page._connection)

/** The page's wire drops and its reconnects are refused; returns once the page tried one, so it knows its wire is down. */
async function connectionDown(): Promise<number> {
  upgrades.refuse = true
  const attemptsBefore = upgradeAttempts.length
  dropWire()
  const lostAt = performance.now()
  await vi.waitFor(() => expect(upgradeAttempts.length).toBeGreaterThan(attemptsBefore), {
    timeout: 2_000,
    interval: 5,
  })
  return lostAt
}

test("a page that closes its channel while its connection is down, back within the server's grace, has its close read on the server as its own, and its connection goes", async () => {
  // Each reconnect waits a tenth of its backoff: the page is back well within the server's 1 s grace.
  const random = vi.spyOn(Math, 'random').mockReturnValue(0.1)
  try {
    const [channel, page] = await pair()
    await connectionDown()
    const closing = page.close({ timeout: 8_000 })
    upgrades.refuse = false
    expect(await closing).toBeNull()
    expect(await channel.closed).toBeNull()
    await vi.waitFor(() => expect(connectionGone(page)).toBe(true), { timeout: 2_000, interval: 20 })
  } finally {
    random.mockRestore()
  }
}, 10_000)

test('a page that closes its channel while its connection is down, never back, tries until its recovery deadline, its connection goes there, and it opens nothing after', async () => {
  const [, page] = await pair()
  const lostAt = await connectionDown()
  const closing = page.close({ timeout: 8_000 })
  await vi.waitFor(() => expect(connectionGone(page)).toBe(true), { timeout: 6_000, interval: 20 })
  const goneAfter = performance.now() - lostAt
  const attemptsBeforeGone = upgradeAttempts.filter((at) => at > lostAt).length
  const attempts = upgradeAttempts.length
  await new Promise((resolve) => setTimeout(resolve, 2_000))
  expect({
    // Its window: two ping intervals and `reconnectTimeout`, 3 s from the loss.
    goneAtItsDeadline: goneAfter >= 2_900 && goneAfter < 4_000,
    triedMoreThanOnce: attemptsBeforeGone > 1,
    attemptsAfterGone: upgradeAttempts.length - attempts,
    closing: await closing,
  }).toEqual({
    goneAtItsDeadline: true,
    triedMoreThanOnce: true,
    attemptsAfterGone: 0,
    closing: expect.any(ChannelCloseUnknownError),
  })
}, 15_000)

/** The page decides a close and has the server's CLOSE_DONE when its connection is lost for good, its own cleanup
 *  running `cleanupMs`: how each end's `closed` reads. */
async function pageDecidesHoldingTheResult({ cleanupMs, timeoutMs }: { cleanupMs: number; timeoutMs: number }) {
  const [channel, page] = await pair()
  let cleanedUp = false
  page.onClose(async () => {
    await new Promise((resolve) => setTimeout(resolve, cleanupMs))
    cleanedUp = true
  })
  const closing = page.close({ timeout: timeoutMs })
  await new Promise((resolve) => setTimeout(resolve, 200))
  upgrades.otherWireVersion = true
  dropWire()
  const end = await page.closed
  return { end, cleanedUp, closing, server: await channel.closed }
}

test("a page that decides and has the server's result when its connection is lost for good closes once its cleanup is done", async () => {
  const { end, cleanedUp, closing, server } = await pageDecidesHoldingTheResult({ cleanupMs: 1_500, timeoutMs: 5_000 })
  expect(end).toBeNull()
  expect(cleanedUp).toBe(true)
  await expect(closing).resolves.toBeNull()
  // The verdict can't reach the server.
  expect(server).toBeInstanceOf(ChannelCloseUnknownError)
}, 10_000)

test("a page that decides and has the server's result when its connection is lost for good times out with a cleanup past the close's timeout", async () => {
  const { end, cleanedUp } = await pageDecidesHoldingTheResult({ cleanupMs: 3_000, timeoutMs: 2_000 })
  expect(end).toEqual(new ChannelClosedError('Channel close timed out'))
  expect(cleanedUp).toBe(false)
}, 10_000)

// A collection the test can force, as `node --expose-gc` gives.
setFlagsFromString('--expose-gc')
const gc: unknown = runInNewContext('gc')

test("an ended broadcast the app keeps lets go of its page's wire once the page confirmed the abort", async () => {
  Object.assign(upgrades, { refuse: false, otherWireVersion: false })
  // The app keeps the broadcast, as a publisher to its key.
  const channel = new ServerBroadcast<string>({ key: crypto.randomUUID() })
  const peers: WeakRef<IndexedPeer>[] = []
  const attach = channel._end._attachPeer.bind(channel._end)
  channel._end._attachPeer = (peer, state) => {
    peers.push(new WeakRef(peer))
    attach(peer, state)
  }
  getChannelMux().registerChannel(channel._end, ATTACHABLE_NOW)
  const connection = { telefuncUrl, transports: ['ws' as const], idleTimeout: 0, connectionKey: crypto.randomUUID() }
  const page = new ClientBroadcast<string>({ channelId: channel.id, connection })
  // Another channel keeps the page's wire open.
  const other = new ServerChannel<unknown, unknown>()
  getChannelMux().registerChannel(other, ATTACHABLE_NOW)
  const otherPage = new ClientChannel<unknown, unknown>({ channelId: other.id, connection })
  await Promise.all([channel.opened, page.opened, other.opened, otherPage.opened])
  channel.abort('stop')
  expect(await page.closed).toBeInstanceOf(Abort)
  // The page's next heartbeat confirms the abort, and the server releases the channel.
  await vi.waitFor(
    async () => {
      if (typeof gc !== 'function') throw new Error('no gc')
      for (let i = 0; i < 4; i++) {
        await new Promise((resolve) => setTimeout(resolve, 0))
        gc()
      }
      expect(peers.length).toBeGreaterThan(0)
      expect(peers.filter((peer) => peer.deref() !== undefined)).toEqual([])
    },
    { timeout: 6_000, interval: 250 },
  )
  expect(channel.key).toBeDefined()
  other.abort()
}, 10_000)
