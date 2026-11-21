// A page whose connection drops, and whose next connection can't finish: what it sends then still reaches the server.
// Over a real server and page, through a TCP forwarder of each row's own that the spec can cut, or make take a
// connection to nowhere.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import net, { type Socket } from 'node:net'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { ChannelMux, getChannelMux } from './server/ChannelMux.js'

serverConfig.channel = { transports: ['sse', 'ws'], pingInterval: 60_000 }

/** Past the 1 s a reconnect's first socket gets before the page races another. */
const LATE_MS = 1_500

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

/** A forwarder between one row's page and the server. Its connections forward until `drop`, which cuts them and does
 *  with the next one what `next` says: destroy it as it accepts it, hold it connected to nothing, or forward it with
 *  the server's answer `LATE_MS` late. Closed with the row, so no other row's page reconnects through it. */
async function link() {
  const sockets = new Set<Socket>()
  let next: 'forward' | 'destroy' | 'hold' | 'late' = 'forward'
  const forwarder = net.createServer((page) => {
    page.on('error', () => {})
    sockets.add(page)
    page.on('close', () => sockets.delete(page))
    const take = next
    next = 'forward'
    if (take === 'destroy') return void page.destroy()
    if (take === 'hold') return
    const upstream = net.connect(port, '127.0.0.1')
    upstream.on('error', () => {})
    page.pipe(upstream)
    if (take === 'late') upstream.on('data', (chunk: Buffer) => void setTimeout(() => page.write(chunk), LATE_MS))
    else upstream.pipe(page)
    page.on('close', () => upstream.destroy())
    upstream.on('close', () => page.destroy())
  })
  await new Promise<void>((resolve) => forwarder.listen(0, '127.0.0.1', resolve))
  const address = forwarder.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  return {
    telefuncUrl: `http://127.0.0.1:${address.port}/_telefunc`,
    drop(then: typeof next) {
      next = then
      for (const socket of sockets) socket.destroy()
    },
    close() {
      for (const socket of sockets) socket.destroy()
      forwarder.close()
    },
  }
}

/** A server channel, and its page over `transport` through a link of its own. */
async function pair(transport: 'sse' | 'ws') {
  const network = await link()
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel)
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl: network.telefuncUrl, transports: [transport], connectionKey: crypto.randomUUID() },
  })
  await Promise.all([channel.opened, page.opened])
  const received = Promise.withResolvers<string>()
  channel.listen((message) => void received.resolve(message))
  return { network, page, received: received.promise }
}

for (const transport of ['sse', 'ws'] as const)
  for (const attempt of ['destroy', 'hold'] as const) {
    // An SSE open that is held is dropped by its RECONCILE's deadline, 10 s on: not this spec's.
    if (transport === 'sse' && attempt === 'hold') continue
    test(`over ${transport === 'ws' ? 'a WebSocket' : 'SSE'}, a page whose next connection after a drop the network ${attempt === 'destroy' ? 'resets as it accepts it' : 'takes and never answers'} sends on another`, async () => {
      const { network, page, received } = await pair(transport)
      try {
        network.drop(attempt)
        void page.send('after')
        expect(await received).toBe('after')
        page.abort()
      } finally {
        network.close()
      }
    })
  }

test('over a WebSocket, a page whose next socket after a drop the server answers late sends on the one it raced, and the server keeps and logs nothing of the other', async () => {
  const { network, page, received } = await pair('ws')
  const opened = vi.spyOn(ChannelMux.prototype, 'onWireOpen')
  const closed = vi.spyOn(ChannelMux.prototype, 'onWireClosed')
  const logs = [vi.spyOn(console, 'error'), vi.spyOn(console, 'warn'), vi.spyOn(console, 'log')]
  try {
    network.drop('late')
    void page.send('after')
    expect(await received).toBe('after')
    // The server took the late socket's upgrade at once; the page closed it once the socket it raced opened.
    const wires = () => opened.mock.calls.map(([wire]) => wire)
    await vi.waitFor(() => expect(wires()).toHaveLength(2))
    const [late] = wires()
    await vi.waitFor(() => expect(closed.mock.calls.map(([wire]) => wire)).toContain(late))
    expect(logs.flatMap((log) => log.mock.calls)).toEqual([])
    page.abort()
  } finally {
    for (const spy of [opened, closed, ...logs]) spy.mockRestore()
    network.close()
  }
})
