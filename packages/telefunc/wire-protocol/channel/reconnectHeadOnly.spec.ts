// A page whose next WebSocket after a drop gets the server's head and nothing more: no RECONCILED, no receipt, no
// PONG. Over a real server and page, through a forwarder of the row's own, at 1 s pings.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import net, { type Socket } from 'node:net'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ATTACHABLE_NOW, ServerChannel } from './server/ServerChannel.js'
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

/** Passes connections both ways; after `dropToHead()`, a cut session's next connection gets the server's head alone. */
async function link() {
  const sockets = new Set<Socket>()
  /** The session each open connection's request named. */
  const sessionOf = new Map<Socket, string | null>()
  /** The sessions `dropToHead()` cut, till their next connection. */
  const headOnly = new Set<string>()
  const connections: Array<{ at: number; headOnly: boolean }> = []
  const forwarder = net.createServer((page) => {
    page.on('error', () => {})
    sockets.add(page)
    const at = performance.now()
    const upstream = net.connect(port, '127.0.0.1')
    upstream.on('error', () => {})
    sockets.add(upstream)
    page.on('close', () => {
      sessionOf.delete(page)
      upstream.destroy()
    })
    upstream.on('close', () => page.destroy())
    let request = Buffer.alloc(0)
    const untilRequestLine = (chunk: Buffer) => {
      request = Buffer.concat([request, chunk])
      const end = request.indexOf('\r\n')
      if (end === -1) return
      page.off('data', untilRequestLine)
      page.pause()
      const session = /[?&]session=([^&\s]+)/.exec(request.subarray(0, end).toString('latin1'))?.[1] ?? null
      sessionOf.set(page, session)
      const connection = { at, headOnly: session !== null && headOnly.delete(session) }
      connections.push(connection)
      upstream.write(request)
      page.pipe(upstream)
      if (!connection.headOnly) return void upstream.pipe(page)
      let answer = Buffer.alloc(0)
      const untilHead = (chunk: Buffer) => {
        answer = Buffer.concat([answer, chunk])
        const end = answer.indexOf('\r\n\r\n')
        if (end === -1) return
        upstream.off('data', untilHead)
        page.write(answer.subarray(0, end + 4))
      }
      upstream.on('data', untilHead)
    }
    page.on('data', untilRequestLine)
  })
  await new Promise<void>((resolve) => forwarder.listen(0, '127.0.0.1', resolve))
  const address = forwarder.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  return {
    telefuncUrl: `http://127.0.0.1:${address.port}/_telefunc`,
    port: address.port,
    connections,
    dropToHead() {
      for (const session of sessionOf.values()) if (session !== null) headOnly.add(session)
      for (const socket of sockets) socket.destroy()
    },
    close() {
      for (const socket of sockets) socket.destroy()
      forwarder.close()
    },
  }
}

/** `stray`: another client connects just after the drop, as a page on a reused port may; `atOnce`: no backoff. */
async function dropsHeadOnlyWire({ stray = false, atOnce = false } = {}) {
  const network = await link()
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
  const register = vi.spyOn(WireLifecycle.prototype, 'register')
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl: network.telefuncUrl, transports: ['ws'], connectionKey: crypto.randomUUID() },
  })
  const lifecycle = register.mock.contexts.at(-1)
  register.mockRestore()
  const onWireLate = WireLifecycle.prototype.onWireLate
  let droppedAt = Number.POSITIVE_INFINITY
  const late = vi.spyOn(WireLifecycle.prototype, 'onWireLate').mockImplementation(function (this: WireLifecycle, wire) {
    if (this === lifecycle) droppedAt = Math.min(droppedAt, performance.now())
    return onWireLate.call(this, wire)
  })
  const reconcileLate = vi.spyOn(WireLifecycle.prototype, 'onReconcileLate')
  const random = atOnce ? vi.spyOn(Math, 'random').mockReturnValue(0) : null
  try {
    await Promise.all([channel.opened, page.opened])
    const received = Promise.withResolvers<string>()
    channel.listen((message) => void received.resolve(message))
    network.dropToHead()
    if (stray) {
      const other = net.connect(network.port, '127.0.0.1')
      other.on('error', () => {})
      other.write('GET /_telefunc HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n')
      other.once('data', () => other.destroy())
    }
    void page.send('after')
    expect(await received.promise).toBe('after')
    expect(network.connections.filter((connection) => connection.headOnly)).toHaveLength(1)
    // The page's heartbeat dropped it, and only then did the page make the connection that carried the message.
    expect(late.mock.contexts.filter((context) => context === lifecycle)).toHaveLength(1)
    expect(network.connections.some(({ at }) => at > droppedAt)).toBe(true)
    expect(reconcileLate).not.toHaveBeenCalled()
  } finally {
    random?.mockRestore()
    late.mockRestore()
    reconcileLate.mockRestore()
    page.abort()
    network.close()
  }
}

test("over a WebSocket, a page whose next connection after a drop gets the server's head and nothing more drops it as its heartbeat finds it dead, then sends on the next", async () => {
  await dropsHeadOnlyWire()
}, 20_000)

test("over a WebSocket, a page whose next connection after a drop gets the server's head and nothing more drops it as its heartbeat finds it dead, then sends on the next, while another client's connection reaches its link first", async () => {
  await dropsHeadOnlyWire({ stray: true })
}, 20_000)

test("over a WebSocket, a page whose next connection after a drop gets the server's head and nothing more drops it as its heartbeat finds it dead, then sends on the next, when each reconnect goes at once", async () => {
  await dropsHeadOnlyWire({ atOnce: true })
}, 20_000)
