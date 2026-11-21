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
import { WireLifecycle } from './client/WireLifecycle.js'
import { ChannelMux, getChannelMux } from './server/ChannelMux.js'
import { encode } from '../wire/frame.js'

serverConfig.channel = { transports: ['sse', 'ws'], pingInterval: 60_000 }

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
 *  with the next one what `next` says: destroy it as it accepts it, hold it connected to nothing, or forward it and hold
 *  the server's answer until `release`. Closed with the row, so no other row's page reconnects through it. */
async function link() {
  const sockets = new Set<Socket>()
  let next: 'forward' | 'destroy' | 'hold' | 'late' = 'forward'
  const late: Array<{ page: Socket; upstream: Socket; answer: Buffer[] }> = []
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
    if (take === 'late') {
      const answer: Buffer[] = []
      upstream.on('data', (chunk: Buffer) => void answer.push(chunk))
      late.push({ page, upstream, answer })
    } else upstream.pipe(page)
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
    /** The late connections get what the server answered, and what it answers from now on. */
    release() {
      for (const { page, upstream, answer } of late.splice(0)) {
        upstream.removeAllListeners('data')
        for (const chunk of answer) page.write(chunk)
        upstream.pipe(page)
      }
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
    // An SSE open that is held is dropped by its RECONCILE's deadline, 10 s on: not this spec's. A WebSocket reset at
    // accept: the row after this loop.
    if (transport === 'sse' ? attempt === 'hold' : attempt === 'destroy') continue
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

// Node 22's WebSocket fires only `error` for a connection reset at accept, Node 26's `error` then `close`.
test('over a WebSocket, a page whose next socket after a drop the network resets as it accepts it takes that as a loss, and sends on another', async () => {
  const opened = vi.spyOn(WireLifecycle.prototype, 'onTransportOpen')
  const lost = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
  const { network, page, received } = await pair('ws')
  // The page's own connection: an earlier row's, closed, may still report its sockets' ends.
  const connection = opened.mock.contexts.at(-1)
  try {
    network.drop('destroy')
    void page.send('after')
    expect(await received).toBe('after')
    // The wire's drop, then the socket the network reset: each once.
    expect(lost.mock.contexts.filter((context) => context === connection)).toHaveLength(2)
    page.abort()
  } finally {
    for (const spy of [opened, lost]) spy.mockRestore()
    network.close()
  }
})

test('over a WebSocket, a page whose next socket after a drop the server answers late sends on the one it raced; the late one sends nothing, and the server lets it go and logs nothing', async () => {
  const { network, page, received } = await pair('ws')
  const opened = vi.spyOn(ChannelMux.prototype, 'onWireOpen')
  const closed = vi.spyOn(ChannelMux.prototype, 'onWireClosed')
  const messages = vi.spyOn(ChannelMux.prototype, 'onWireMessage')
  const logs = [vi.spyOn(console, 'error'), vi.spyOn(console, 'warn'), vi.spyOn(console, 'log')]
  try {
    network.drop('late')
    void page.send('after')
    // The server took both upgrades at once; the page reads the late one's answer only once 'after' went on the other.
    expect(await received).toBe('after')
    expect(opened).toHaveBeenCalledTimes(2)
    const [late, raced] = opened.mock.calls.map(([wire]) => wire)
    network.release()
    await vi.waitFor(() => expect(closed.mock.calls.map(([wire]) => wire)).toContain(late))
    expect(messages.mock.calls.filter(([wire]) => wire === late)).toEqual([])
    expect(messages.mock.calls.some(([wire]) => wire === raced)).toBe(true)
    // The server let the late wire go: a frame it would refuse there, a CLOSE before any RECONCILE, as its adapter would
    // hand one, ends no wire and is answered on no socket.
    const peer = Object.getPrototypeOf(late)
    const acts = [vi.spyOn(peer, 'send'), vi.spyOn(peer, 'close'), vi.spyOn(peer, 'terminate')]
    await getChannelMux().onWireMessage(late, encode.close(0, 0, 0))
    expect(acts.flatMap((act) => act.mock.contexts).filter((context) => context === late)).toEqual([])
    for (const act of acts) act.mockRestore()
    expect(logs.flatMap((log) => log.mock.calls)).toEqual([])
    page.abort()
  } finally {
    for (const spy of [opened, closed, messages, ...logs]) spy.mockRestore()
    network.close()
  }
})
