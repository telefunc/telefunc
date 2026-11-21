// A close the server decides whose frames the network loses with the page's connection: the server keeps them for the
// page's reconnect. Memory running short first lets go of a verdict, never of a message. Over a real server and page, on
// SSE and on a WebSocket, with a network the spec can lose bytes on and cut.

import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import type { Socket } from 'node:net'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ChannelCloseUnknownError } from './channelErrors.js'
import { isAbort } from '../../shared/Abort.js'
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

/** A server channel and the page's end of it, both open, on a connection of their own. */
async function pair(transport: 'sse' | 'ws') {
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel)
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl, transports: [transport], idleTimeout: 0, connectionKey: crypto.randomUUID() },
  })
  await Promise.all([channel.opened, page.opened])
  return [channel, page] as const
}

for (const transport of ['sse', 'ws'] as const) {
  describe(`over ${transport === 'sse' ? 'SSE' : 'a WebSocket'}, a close the server decided whose verdict the network lost with the connection`, () => {
    for (const short of [true, false]) {
      const title = short
        ? "with memory short before the page is back, the page's closed resolves with ChannelCloseUnknownError"
        : 'with memory not short, the page back reads the verdict, and its closed resolves with null'
      test(title, async () => {
        const [channel, page] = await pair(transport)
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

// A page that never read the server's CLOSE isn't in the close: what the network lost of its end is the only way it
// learns how the channel ended, so it stays kept however short memory runs.
for (const transport of ['sse', 'ws'] as const) {
  describe(`over ${transport === 'sse' ? 'SSE' : 'a WebSocket'}, the network lost the server's last frames with the connection, and memory runs short before the page is back`, () => {
    for (const [ending, title] of [
      [
        'close',
        'a close that timed out with nothing before its CLOSE stays kept, and the page back reads the timed-out verdict',
      ],
      ['abort', 'an abort stays kept, and the page back reads it with its value'],
    ] as const) {
      test(title, async () => {
        const [channel, page] = await pair(transport)
        network.losing = true
        if (ending === 'close') await channel.close({ timeout: 100 })
        else channel.abort('bye')
        const connections = dropConnections()

        // Holding all of `maxMemory` lets go of whatever may go; the queue then holds nothing for the rows after.
        const queue = queueMemory()
        const held = queue.tryHold(MAX_MEMORY)
        queue.release()
        connections.reconnect()
        expect(held).toBe(false)

        const closed = await page.closed
        if (ending === 'close')
          expect(closed).toHaveProperty('message', expect.stringContaining('Channel close timed out'))
        else expect(isAbort(closed) && closed.abortValue).toBe('bye')
      })
    }
  })
}

// Last: what these rows keep, their messages, stays held while their pages are away.
for (const transport of ['sse', 'ws'] as const) {
  describe(`over ${transport === 'sse' ? 'SSE' : 'a WebSocket'}, a close that timed out after the network lost its CLOSE and the messages before it`, () => {
    test('with memory short before the page is back, the messages stay kept, and the page back reads each once', async () => {
      const [channel, page] = await pair(transport)
      const received: string[] = []
      page.listen((message) => void received.push(message))
      network.losing = true
      for (const message of ['m1', 'm2', 'm3']) void channel.send(message)
      await channel.close({ timeout: 100 })
      const connections = dropConnections()

      const queue = queueMemory()
      const held = queue.tryHold(MAX_MEMORY)
      connections.reconnect()
      expect(held).toBe(false)

      const closed = await page.closed
      expect(received).toEqual(['m1', 'm2', 'm3'])
      expect(closed).toHaveProperty('message', expect.stringContaining('Channel close timed out'))
    })
  })
}
