// A close the server decides whose verdict the network loses with the page's connection: the server keeps the verdict
// for the page's reconnect, unless memory runs short first. Over a real server and page, on SSE and on a WebSocket, with
// a network the spec can lose bytes on and cut.

import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import type { Socket } from 'node:net'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ChannelCloseUnknownError } from './channelErrors.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { getChannelMux } from './server/ChannelMux.js'
import { queueMemory } from './server/channelMemory.js'

serverConfig.channel = { transports: ['sse', 'ws'], pingInterval: 60_000, maxMemory: '2m' }

/** The network between the page and the server: what it loses, and whether it takes new connections. */
const network = { losing: false, refusing: false }
const sockets = new Set<Socket>()
let server: Server
let telefuncUrl: string

beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  server.on('connection', (socket: Socket) => {
    if (network.refusing) return void socket.destroy()
    sockets.add(socket)
    // What the server writes while the network loses bytes never reaches the page.
    socket.write = new Proxy(socket.write, {
      apply: (write, self, args) => (network.losing ? true : Reflect.apply(write, self, args)),
    })
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

/** Every connection the page has drops, and the page's attempts to reconnect fail until `reconnect` runs. */
function dropConnections() {
  network.refusing = true
  for (const socket of sockets) socket.destroy()
  sockets.clear()
  return {
    reconnect: () => {
      Object.assign(network, { losing: false, refusing: false })
    },
  }
}

const MAX_MEMORY = 2 * 1024 * 1024

for (const transport of ['sse', 'ws'] as const) {
  describe(`over ${transport === 'sse' ? 'SSE' : 'a WebSocket'}, a close the server decided whose verdict the network lost with the connection`, () => {
    for (const short of [true, false]) {
      const title = short
        ? "with memory short before the page is back, the page's closed resolves with ChannelCloseUnknownError"
        : 'with memory not short, the page back reads the verdict, and its closed resolves with null'
      test(title, async () => {
        const channel = new ServerChannel<string, string>()
        getChannelMux().registerChannel(channel)
        const page = new ClientChannel<string, string>({
          channelId: channel.id,
          connection: { telefuncUrl, transports: [transport], idleTimeout: 0, connectionKey: crypto.randomUUID() },
        })
        await Promise.all([channel.opened, page.opened])
        // The server's cleanup holds its verdict until the network loses what the server writes.
        let cleanedUp = () => {}
        channel.onClose(() => new Promise<void>((resolve) => (cleanedUp = resolve)))
        void channel.close({ timeout: 10_000 })
        // The page's CLOSE_ACK and CLOSE_DONE reach the server.
        await new Promise((resolve) => setTimeout(resolve, 200))
        network.losing = true
        cleanedUp()
        expect(await channel.closed).toBe(null)
        const connections = dropConnections()

        // A `messages()` loop's queue needs all of `maxMemory` before the page is back, or nothing does.
        const queue = queueMemory()
        const held = short && queue.tryHold(MAX_MEMORY)
        connections.reconnect()
        expect(held).toBe(short)

        const closed = await page.closed
        queue.release()
        if (short) expect(closed).toBeInstanceOf(ChannelCloseUnknownError)
        else expect(closed).toBe(null)
      })
    }
  })
}
