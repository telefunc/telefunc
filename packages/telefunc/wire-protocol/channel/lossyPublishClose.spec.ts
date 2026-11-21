// A lossy subscriber's newest publish, waiting while its page is behind, when its channel closes from either end: what the
// page reads of it, against when its close began. Over a real server and page, on SSE and on a WebSocket.

import { afterAll, beforeAll, describe, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import type { Socket } from 'node:net'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { Broadcast, ServerBroadcast } from './server/ServerBroadcast.js'
import { ClientBroadcast } from './client/ClientBroadcast.js'
import { getChannelMux } from './server/ChannelMux.js'

serverConfig.channel = { transports: ['sse', 'ws'], pingInterval: 60_000 }

let server: Server
let telefuncUrl: string
/** While set, what the server writes waits, as on a page whose downlink is slow; `stallDownlink(false)` writes it, in order. */
let downlinkStalled = false
const stalled: (() => void)[] = []
function stallDownlink(stall: boolean) {
  downlinkStalled = stall
  if (!stall) for (const write of stalled.splice(0)) write()
}

beforeAll(async () => {
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  server.on('connection', (socket: Socket) => {
    socket.write = new Proxy(socket.write, {
      apply: (write, self, args) => {
        if (!downlinkStalled && stalled.length === 0) return Reflect.apply(write, self, args)
        stalled.push(() => Reflect.apply(write, self, args))
        return true
      },
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

for (const transport of ['sse', 'ws'] as const) {
  describe(`over ${transport === 'sse' ? 'SSE' : 'a WebSocket'}, a lossy subscriber whose newest publish waits while its page is behind`, () => {
    for (const closer of ['server', 'page'] as const)
      test(`a close the ${closer} begins hands it ahead of the close: the page reads it before its close begins`, async () => {
        const key = `room:${crypto.randomUUID()}`
        const channel = new ServerBroadcast({ key, lossy: true })
        getChannelMux().registerChannel(channel)
        const page = new ClientBroadcast<string>({
          channelId: channel.id,
          lossy: true,
          connection: { telefuncUrl, transports: [transport], idleTimeout: 60_000, connectionKey: crypto.randomUUID() },
        })
        const read: string[] = []
        page.subscribe((value) => void read.push(value.slice(0, 2)))
        page.onClose(() => void read.push('close began'))
        await Promise.all([channel.opened, page.opened])
        // Once the page reads what the server publishes, a quarter of its credit window and more: it grants credit for that,
        // which confirms all it read, so the server measures how late its confirmations are from here on.
        await vi.waitFor(async () => {
          await Broadcast.publish(key, 'v0')
          expect(read).toContain('v0')
        })
        await Broadcast.publish(key, `v1${'x'.repeat(600_000)}`)
        await vi.waitFor(() => expect(channel.heldBytes).toBe(0))
        // v2 reaches the page, and its confirmation the server, past how long one may take: the page is behind, and v3
        // waits as the newest.
        stallDownlink(true)
        await Broadcast.publish(key, 'v2')
        await new Promise((resolve) => setTimeout(resolve, 1_500))
        await Broadcast.publish(key, 'v3')
        expect(channel.newestUnhanded).toBeDefined()

        const closing = closer === 'server' ? channel.close() : page.close()
        // The server's end of the close is written behind v2.
        await new Promise((resolve) => setTimeout(resolve, 200))
        stallDownlink(false)
        expect(await closing).toBe(null)
        expect(await (closer === 'server' ? page.closed : channel.closed)).toBe(null)
        expect(read.slice(read.indexOf('v1'))).toEqual(['v1', 'v2', 'v3', 'close began'])
      })
  })
}

for (const transport of ['sse', 'ws'] as const) {
  describe(`over ${transport === 'sse' ? 'SSE' : 'a WebSocket'}, a lossy subscriber whose page keeps up, with publishes waiting for its wire`, () => {
    for (const closer of ['server', 'page'] as const)
      test(`a close the ${closer} begins skips none of them: the page reads each before its close begins`, async () => {
        const key = `room:${crypto.randomUUID()}`
        const channel = new ServerBroadcast({ key, lossy: true })
        getChannelMux().registerChannel(channel)
        const page = new ClientBroadcast<string>({
          channelId: channel.id,
          lossy: true,
          connection: { telefuncUrl, transports: [transport], idleTimeout: 60_000, connectionKey: crypto.randomUUID() },
        })
        const read: string[] = []
        page.subscribe((value) => void read.push(value.slice(0, 2)))
        page.onClose(() => void read.push('close began'))
        await Promise.all([channel.opened, page.opened])
        // Each larger than what its wire sends before its page says it took some: the later ones wait for the wire.
        await Promise.all(['v1', 'v2', 'v3'].map((name) => Broadcast.publish(key, name + 'x'.repeat(600_000))))
        const closing = closer === 'server' ? channel.close() : page.close()
        expect(await closing).toBe(null)
        expect(await (closer === 'server' ? page.closed : channel.closed)).toBe(null)
        expect(read).toEqual(['v1', 'v2', 'v3', 'close began'])
      })

    test("in a close the page begins, the page's own publish is read before its publish() resolves", async () => {
      const key = `room:${crypto.randomUUID()}`
      const channel = new ServerBroadcast({ key, lossy: true })
      getChannelMux().registerChannel(channel)
      const page = new ClientBroadcast<string>({
        channelId: channel.id,
        lossy: true,
        connection: { telefuncUrl, transports: [transport], idleTimeout: 60_000, connectionKey: crypto.randomUUID() },
      })
      const read: string[] = []
      page.subscribe((value) => void read.push(value.slice(0, 2)))
      page.onClose(() => void read.push('close began'))
      await Promise.all([channel.opened, page.opened])
      await Promise.all(['v1', 'v2', 'v3'].map((name) => Broadcast.publish(key, name + 'x'.repeat(600_000))))
      const readBeforeAnswer = page.publish('p4').then(() => read.includes('p4'))
      expect(await page.close()).toBe(null)
      expect(await readBeforeAnswer).toBe(true)
      expect(read).toEqual(['v1', 'v2', 'v3', 'p4', 'close began'])
    })
  })
}
