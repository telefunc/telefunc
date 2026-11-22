// What the server keeps of a page's wire that dropped, while it waits for the page to reconnect. Over a real server and
// page, on each transport, the page's connections cut by the spec and its reconnects refused.

import { afterAll, afterEach, beforeAll, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import type { Socket } from 'node:net'
import { setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { getChannelMux } from './server/ChannelMux.js'

serverConfig.channel = { transports: ['sse', 'ws'] }
serverConfig.disableNamingConvention = true

/** Whether the server refuses the page's next connections. */
let refuse = false
const sockets = new Set<Socket>()
let server: Server
let telefuncUrl: string

beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  server.on('connection', (socket: Socket) => {
    if (refuse) return void socket.destroy()
    sockets.add(socket)
    socket.once('close', () => sockets.delete(socket))
  })
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
})
afterEach(() => {
  refuse = false
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

setFlagsFromString('--expose-gc')
const gc: unknown = runInNewContext('gc')

for (const transport of ['ws', 'sse'] as const) {
  test(`over ${transport === 'ws' ? 'a WebSocket' : 'SSE'}, the server lets go of a page's wire that dropped while it waits for the page to reconnect`, async () => {
    /** A page whose wire drops, with its reconnects refused: the server's channel and the dropped sockets. */
    async function pageWhoseWireDropped() {
      const channel = new ServerChannel<string, string>()
      channel.listen((data) => `ack:${data}`)
      getChannelMux().registerChannel(channel)
      const page = new ClientChannel<string, string>({
        channelId: channel.id,
        connection: { telefuncUrl, transports: [transport], idleTimeout: 0, connectionKey: crypto.randomUUID() },
      })
      expect(await page.send('page', { ack: true })).toBe('ack:page')
      refuse = true
      const dropped = [...sockets].map((socket) => new WeakRef(socket))
      for (const socket of sockets) socket.destroy()
      return { channel, dropped }
    }
    const { channel, dropped } = await pageWhoseWireDropped()
    let ended = false
    void channel.closed.then(() => {
      ended = true
    })
    for (let i = 0; i < 10; i++) {
      await new Promise((resolve) => setTimeout(resolve, 20))
      if (typeof gc === 'function') gc()
    }
    expect(dropped.length).toBeGreaterThan(0)
    // The server holds the page's channel for `reconnectTimeout`, 60 s by default.
    expect({ waiting: !ended, collected: dropped.filter((ref) => ref.deref() === undefined).length }).toEqual({
      waiting: true,
      collected: dropped.length,
    })
  }, 10_000)
}
