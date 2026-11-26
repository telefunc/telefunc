// A page whose transports list SSE, behind a proxy that resets every WebSocket upgrade: its channels open and go on
// over the event stream, as a page that starts on SSE does; one whose WebSocket opened takes a single reset upgrade for
// a blip, and stays on WebSocket. Over a real server and page, through that proxy.

import { afterAll, afterEach, beforeAll, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import net, { type Socket } from 'node:net'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ATTACHABLE_NOW, ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { getChannelMux } from './server/ChannelMux.js'

// A page on a WebSocket gives up past two ping intervals and `reconnectTimeout`: 3 s.
serverConfig.channel = { transports: ['sse', 'ws'], pingInterval: 1_000, reconnectTimeout: 1_000 }

/** The proxy: it forwards every connection, and resets each that asks for a WebSocket upgrade while `resetting`, and the
 *  next `resets` of them; `seen` lists each connection, in order. */
const proxy = {
  resetting: false,
  resets: 0,
  eventStreams: 0,
  requests: 0,
  seen: Array<'reset' | 'upgrade' | 'request'>(),
}
const upgraded = new Set<Socket>()
let server: Server
let forwarder: net.Server
let telefuncUrl: string

beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  forwarder = net.createServer((page) => {
    page.on('error', () => {})
    page.once('data', (head: Buffer) => {
      const text = head.toString('latin1')
      const isUpgrade = /\r\nupgrade: *websocket/i.test(text)
      if (isUpgrade && (proxy.resetting || proxy.resets > 0)) {
        proxy.resets = Math.max(0, proxy.resets - 1)
        proxy.seen.push('reset')
        return void page.resetAndDestroy()
      }
      proxy.seen.push(isUpgrade ? 'upgrade' : 'request')
      if (isUpgrade) {
        upgraded.add(page)
        page.on('close', () => upgraded.delete(page))
      } else {
        proxy.requests++
        if (/\r\naccept: *text\/event-stream/i.test(text)) proxy.eventStreams++
      }
      const upstream = net.connect(address.port, '127.0.0.1')
      upstream.on('error', () => {})
      upstream.write(head)
      page.pipe(upstream)
      upstream.pipe(page)
      page.on('close', () => upstream.destroy())
      upstream.on('close', () => page.destroy())
    })
  })
  await new Promise<void>((resolve) => forwarder.listen(0, '127.0.0.1', resolve))
  const at = forwarder.address()
  if (at === null || typeof at === 'string') throw new Error('no port')
  telefuncUrl = `http://127.0.0.1:${at.port}/_telefunc`
})
// An aborted page's WebSocket stays till the server has its close: the next row starts once it is gone.
afterEach(async () => {
  await vi.waitFor(() => expect(upgraded.size).toBe(0), { timeout: 5_000 })
})
afterAll(() => {
  forwarder.close()
  server.closeAllConnections()
  server.close()
})

/** What `promise` settles to within `ms`: 'pending' past it. */
function within(ms: number, promise: Promise<unknown>): Promise<string> {
  return Promise.race([
    promise.then(
      () => 'resolved',
      (err: unknown) => `rejected: ${err instanceof Error ? err.message : String(err)}`,
    ),
    new Promise<string>((resolve) => setTimeout(() => resolve('pending'), ms)),
  ])
}

test("a page whose transports are ['ws', 'sse'] and whose WebSocket opens stays on it, after a drop too, with no request of SSE's", async () => {
  Object.assign(proxy, { resetting: false, resets: 0, eventStreams: 0, requests: 0, seen: [] })
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
  const received: string[] = []
  channel.listen((message) => void received.push(message))
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl, transports: ['ws', 'sse'], connectionKey: crypto.randomUUID() },
  })
  try {
    await Promise.all([channel.opened, page.opened])
    expect(upgraded.size).toBe(1)
    const [first] = upgraded
    if (first === undefined) throw new Error('no WebSocket')
    first.destroy()
    expect(await within(3_500, page.send('after the drop', { ack: true }))).toBe('resolved')
    expect([received.at(-1), upgraded.size, upgraded.has(first), proxy.requests]).toEqual([
      'after the drop',
      1,
      false,
      0,
    ])
  } finally {
    page.abort()
  }
})

test("a page whose WebSocket opened, whose next upgrade after a drop is reset, goes on over a WebSocket, with no request of SSE's", async () => {
  Object.assign(proxy, { resetting: false, resets: 0, eventStreams: 0, requests: 0, seen: [] })
  // Each reconnect waits a tenth of its backoff: both attempts are well within the server's 1 s grace.
  const random = vi.spyOn(Math, 'random').mockReturnValue(0.1)
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
  const received: string[] = []
  channel.listen((message) => void received.push(message))
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl, transports: ['ws', 'sse'], connectionKey: crypto.randomUUID() },
  })
  try {
    await Promise.all([channel.opened, page.opened])
    expect(upgraded.size).toBe(1)
    const [first] = upgraded
    if (first === undefined) throw new Error('no WebSocket')
    proxy.resets = 1
    first.destroy()
    // Sent once the page is back: a send held over the drop would make its second attempt wait a whole step, 1 s.
    await vi.waitFor(() => expect([proxy.resets, upgraded.size, upgraded.has(first)]).toEqual([0, 1, false]), {
      timeout: 3_000,
    })
    expect(await within(3_500, page.send('after the drop', { ack: true }))).toBe('resolved')
    expect([received.at(-1), upgraded.size, upgraded.has(first), proxy.requests]).toEqual([
      'after the drop',
      1,
      false,
      0,
    ])
  } finally {
    random.mockRestore()
    page.abort()
  }
})

test("a page whose transports are ['ws', 'sse'] opens its channel over the event stream", async () => {
  Object.assign(proxy, { resetting: true, resets: 0, eventStreams: 0, requests: 0, seen: [] })
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl, transports: ['ws', 'sse'], connectionKey: crypto.randomUUID() },
  })
  try {
    expect(await within(4_000, page.opened)).toBe('resolved')
    expect(proxy.eventStreams).toBeGreaterThan(0)
    // Its first WebSocket, reset, is its only try before SSE.
    expect(proxy.seen.slice(0, proxy.seen.indexOf('request'))).toEqual(['reset'])
  } finally {
    page.abort()
  }
})

test('an upgraded page whose WebSocket drops goes on over the event stream', async () => {
  Object.assign(proxy, { resetting: false, resets: 0, eventStreams: 0, requests: 0, seen: [] })
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
  const received: string[] = []
  channel.listen((message) => void received.push(message))
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl, transports: ['sse', 'ws'], connectionKey: crypto.randomUUID() },
  })
  try {
    await Promise.all([channel.opened, page.opened])
    await vi.waitFor(() => expect(upgraded.size).toBe(1))
    proxy.resetting = true
    for (const socket of upgraded) socket.resetAndDestroy()
    // Past the 3 s a page on a WebSocket gives a lost wire.
    expect(await within(3_500, page.send('after the drop', { ack: true }))).toBe('resolved')
    expect([page.isClosed, received.at(-1)]).toEqual([false, 'after the drop'])
  } finally {
    page.abort()
  }
})
