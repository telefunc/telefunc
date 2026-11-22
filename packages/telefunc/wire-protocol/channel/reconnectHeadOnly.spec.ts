// A page whose next WebSocket after a drop gets the server's head and nothing more: no RECONCILED, no receipt, no
// PONG. Over a real server and page, through a forwarder of the row's own, at 1 s pings.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import net, { type Socket } from 'node:net'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { WireLifecycle } from './client/WireLifecycle.js'
import { getChannelMux } from './server/ChannelMux.js'

serverConfig.channel = { transports: ['ws'], pingInterval: 1_000 }

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

/** A forwarder that passes each connection both ways until `dropToHead()`, which cuts them; the next connection then
 *  gets the head of the server's answer alone. `connections`: when each came, whether it got the head alone, and when
 *  the page closed it. */
async function link() {
  const sockets = new Set<Socket>()
  let headOnly = false
  const connections: Array<{ at: number; headOnly: boolean; closedAt: number | null }> = []
  const forwarder = net.createServer((page) => {
    page.on('error', () => {})
    sockets.add(page)
    const connection = { at: performance.now(), headOnly, closedAt: null as number | null }
    headOnly = false
    connections.push(connection)
    page.on('close', () => void (connection.closedAt = performance.now()))
    const upstream = net.connect(port, '127.0.0.1')
    upstream.on('error', () => {})
    sockets.add(upstream)
    page.pipe(upstream)
    if (connection.headOnly) {
      let answer = Buffer.alloc(0)
      const untilHead = (chunk: Buffer) => {
        answer = Buffer.concat([answer, chunk])
        const end = answer.indexOf('\r\n\r\n')
        if (end === -1) return
        upstream.off('data', untilHead)
        page.write(answer.subarray(0, end + 4))
      }
      upstream.on('data', untilHead)
    } else upstream.pipe(page)
    page.on('close', () => upstream.destroy())
    upstream.on('close', () => page.destroy())
  })
  await new Promise<void>((resolve) => forwarder.listen(0, '127.0.0.1', resolve))
  const address = forwarder.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  return {
    telefuncUrl: `http://127.0.0.1:${address.port}/_telefunc`,
    connections,
    dropToHead() {
      headOnly = true
      for (const socket of sockets) socket.destroy()
    },
    close() {
      for (const socket of sockets) socket.destroy()
      forwarder.close()
    },
  }
}

test("over a WebSocket, a page whose next connection after a drop gets the server's head and nothing more drops it as its heartbeat finds it dead, then sends on the next", async () => {
  const network = await link()
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel)
  const register = vi.spyOn(WireLifecycle.prototype, 'register')
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl: network.telefuncUrl, transports: ['ws'], connectionKey: crypto.randomUUID() },
  })
  const lifecycle = register.mock.contexts.at(-1)
  register.mockRestore()
  const late = vi.spyOn(WireLifecycle.prototype, 'onWireLate')
  const reconcileLate = vi.spyOn(WireLifecycle.prototype, 'onReconcileLate')
  try {
    await Promise.all([channel.opened, page.opened])
    const received = Promise.withResolvers<string>()
    channel.listen((message) => void received.resolve(message))
    network.dropToHead()
    void page.send('after')
    expect(await received.promise).toBe('after')
    const headOnly = network.connections.find((connection) => connection.headOnly)
    const closedAt = headOnly?.closedAt ?? Number.POSITIVE_INFINITY
    // The page closed it, and only then made the connection that carried the message.
    expect(network.connections.some(({ at }) => at > closedAt)).toBe(true)
    expect(late.mock.contexts.filter((context) => context === lifecycle)).toHaveLength(1)
    expect(reconcileLate).not.toHaveBeenCalled()
  } finally {
    late.mockRestore()
    reconcileLate.mockRestore()
    page.abort()
    network.close()
  }
}, 20_000)
