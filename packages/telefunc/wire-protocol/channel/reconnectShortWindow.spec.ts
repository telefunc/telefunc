// A page whose recovery window is short, 1 s pings and a 1 s `reconnectTimeout`: its wire's page side is cut, the
// server left to notice by its own ping deadline, and the page's next WebSocket is held. Over a real server and page,
// through a TCP forwarder of the row's own.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import net, { type Socket } from 'node:net'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { getChannelMux } from './server/ChannelMux.js'

serverConfig.channel = { transports: ['ws'], pingInterval: 1_000, reconnectTimeout: 1_000 }

let server: Server
let port: number
beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  port = address.port
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

test("a page whose short window's first reconnect is held gets another attempt inside it, and what it sent arrives", async () => {
  // The backoff's draws: half of each delay.
  vi.spyOn(Math, 'random').mockReturnValue(0.5)
  const pages = new Set<Socket>()
  const upstreams = new Set<Socket>()
  let holdNext = false
  const forwarder = net.createServer((page) => {
    page.on('error', () => {})
    pages.add(page)
    page.on('close', () => pages.delete(page))
    if (holdNext) {
      holdNext = false
      return
    }
    const upstream = net.connect(port, '127.0.0.1')
    upstream.on('error', () => {})
    upstreams.add(upstream)
    page.pipe(upstream)
    upstream.pipe(page)
    // The page's side alone goes: the server hears nothing of it.
    page.on('close', () => upstream.unpipe(page))
  })
  await new Promise<void>((resolve) => forwarder.listen(0, '127.0.0.1', resolve))
  const address = forwarder.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel)
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: {
      telefuncUrl: `http://127.0.0.1:${address.port}/_telefunc`,
      transports: ['ws'],
      connectionKey: crypto.randomUUID(),
    },
  })
  try {
    await Promise.all([channel.opened, page.opened])
    const received = Promise.withResolvers<string>()
    channel.listen((message) => void received.resolve(message))
    holdNext = true
    for (const socket of pages) socket.destroy()
    void page.send('after')
    const ended = page.closed.then((err) => `ended: ${err instanceof Error ? err.message : String(err)}`)
    expect(await Promise.race([received.promise, ended])).toBe('after')
  } finally {
    page.abort()
    for (const socket of [...pages, ...upstreams]) socket.destroy()
    forwarder.close()
    vi.restoreAllMocks()
  }
}, 15_000)
