// A close whose frames the network loses with the page's connection: what an end still owes the other is kept for the
// page's reconnect, and nothing else is. Memory running short first lets go of a verdict, never of a message. Over a real
// server and page, on SSE and on a WebSocket, with a network the spec can lose bytes on and cut.

import { afterAll, beforeAll, describe, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import type { Socket } from 'node:net'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ChannelCloseUnknownError } from './channelErrors.js'
import { isAbort } from '../../shared/Abort.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { WireLifecycle } from './client/WireLifecycle.js'
import { ClientConnection } from './client/ClientConnection.js'
import { SseTransport } from './client/SseTransport.js'
import { WsTransport } from './client/WsTransport.js'
import { getChannelMux } from './server/ChannelMux.js'
import { queueMemory } from './server/channelMemory.js'
import { TAG } from '../wire/tags.js'
import type { ChannelFrame } from '../wire/frame.js'

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

/** The server reads nothing more the page sends, on its connections or on new ones. */
function deafenServer() {
  network.refusing = true
  for (const socket of sockets) socket.pause()
}

const MAX_MEMORY = 2 * 1024 * 1024
/** The most one holder may hold: `maxMemory` less the eighth it keeps for sends waiting to be held. */
const HOLDS_AT_MOST = MAX_MEMORY - MAX_MEMORY / 8

/** A server channel and the page's end of it, both open, on a connection of their own that ends `idleTimeout` ms after
 *  its last channel does. */
async function pair(transport: 'sse' | 'ws', idleTimeout = 0) {
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel)
  const { page, lifecycle } = registering(
    () =>
      new ClientChannel<string, string>({
        channelId: channel.id,
        connection: { telefuncUrl, transports: [transport], idleTimeout, connectionKey: crypto.randomUUID() },
      }),
  )
  await Promise.all([channel.opened, page.opened])
  return [channel, page, lifecycle] as const
}

/** The page `open` makes, and the connection it registers on. */
function registering<Page>(open: () => Page): { page: Page; lifecycle: WireLifecycle } {
  const register = vi.spyOn(WireLifecycle.prototype, 'register')
  try {
    const page = open()
    const lifecycle = register.mock.contexts[register.mock.calls.findIndex(([registered]) => registered === page)]
    if (!(lifecycle instanceof WireLifecycle)) throw new Error('the page registered on no connection')
    return { page, lifecycle }
  } finally {
    register.mockRestore()
  }
}

// A page whose connection would stay up for the next channel, once a close the server decided ended its last one.
for (const transport of ['sse', 'ws'] as const) {
  describe(`over ${transport === 'sse' ? 'SSE' : 'a WebSocket'}, a page whose connection drops after a close the server decided`, () => {
    for (const late of [false, true])
      test(`${late ? 'having asked for a shorter deadline after its CLOSE_DONE, ' : ''}opens no connection only to report it`, async () => {
        const [channel, page, lifecycle] = await pair(transport, 60_000)
        if (late) page.onClose(() => void setImmediate(() => void page.close({ timeout: 100 })))
        expect(await channel.close({ timeout: 10_000 })).toBe(null)
        expect(await page.closed).toBe(null)
        const wire = lifecycle.transport
        const disposed = vi.spyOn(ClientConnection.prototype, 'onDisposed')
        const starts = [vi.spyOn(SseTransport.prototype, 'start'), vi.spyOn(WsTransport.prototype, 'start')]
        const connections = dropConnections()
        try {
          // Its wire's loss leaves it nothing to recover: it ends there, and starts no wire again.
          await vi.waitFor(() => expect(disposed.mock.contexts).toContain(page._connection))
          expect(starts.flatMap((start) => start.mock.contexts)).not.toContain(wire)
        } finally {
          disposed.mockRestore()
          for (const start of starts) start.mockRestore()
          connections.reconnect()
        }
      })
  })
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

        // A `messages()` loop's queue needs all one holder may hold before the page is back, or nothing does.
        const queue = queueMemory()
        const held = short && queue.tryHold(HOLDS_AT_MOST)
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

        // Holding all one holder may hold lets go of whatever may go; the queue then holds nothing for the rows after.
        const queue = queueMemory()
        const held = queue.tryHold(HOLDS_AT_MOST)
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

// Once a following end has the other's verdict, it owes nothing more: not even a CLOSE it sent after its own CLOSE_DONE,
// which the deciding end, ended by then, would read and act on nothing of. An end that decides still owes its verdict,
// and an end that aborts its ABORT.
for (const transport of ['sse', 'ws'] as const) {
  describe(`over ${transport === 'sse' ? 'SSE' : 'a WebSocket'}, the end of a close for what each end still owes`, () => {
    test('a server that follows and asks for a shorter deadline after its CLOSE_DONE holds nothing once the verdict arrives', async () => {
      const [channel, page] = await pair(transport)
      // Its CLOSE_DONE has gone, and the page's verdict can't have come back yet.
      let late: Promise<unknown> = Promise.resolve()
      channel.onClose(() => void setImmediate(() => (late = channel.close({ timeout: 100 }))))
      expect(await page.close({ timeout: 10_000 })).toBe(null)
      expect(await channel.closed).toBe(null)
      expect(await late).toBe(null)
      expect(channel.heldBytes).toBe(0)
    })

    for (const away of [false, true])
      test(`a page that follows and asks for a shorter deadline after its CLOSE_DONE${away ? ', while it is away,' : ''} sends nothing more once its verdict comes on its reconnect`, async () => {
        let pageHasVerdict = false
        let closeAfterVerdict = false
        const pageDone = Promise.withResolvers<void>()
        class ServerReading extends ServerChannel<string, string> {
          override _dispatchFrame(frame: ChannelFrame): void {
            if (frame.tag === TAG.CLOSE && pageHasVerdict) closeAfterVerdict = true
            super._dispatchFrame(frame)
            if (frame.tag === TAG.CLOSE_DONE) pageDone.resolve()
          }
        }
        const channel = new ServerReading()
        getChannelMux().registerChannel(channel)
        const connection = {
          telefuncUrl,
          transports: [transport],
          idleTimeout: 60_000,
          connectionKey: crypto.randomUUID(),
        }
        const { page, lifecycle } = registering(
          () => new ClientChannel<string, string>({ channelId: channel.id, connection }),
        )
        void page.closed.then(() => (pageHasVerdict = true))
        await Promise.all([channel.opened, page.opened])
        // The server's cleanup holds its verdict until the server reads nothing more of the page, and the network loses
        // the verdict.
        const cleanup = Promise.withResolvers<void>()
        channel.onClose(() => cleanup.promise)
        void channel.close({ timeout: 10_000 })
        await pageDone.promise
        deafenServer()
        network.losing = true
        if (!away) void page.close({ timeout: 5_000 })
        cleanup.resolve()
        expect(await channel.closed).toBe(null)
        const connections = dropConnections()
        if (away) {
          await vi.waitFor(() => expect(lifecycle.reconnecting).toBe(true))
          void page.close({ timeout: 5_000 })
        }
        connections.reconnect()
        expect(await page.closed).toBe(null)

        // What the page sends after its reconnect's replay, on the same connection, arrives after it.
        const next = new ServerChannel<string, string>()
        getChannelMux().registerChannel(next)
        const nextPage = new ClientChannel<string, string>({ channelId: next.id, connection })
        const received = Promise.withResolvers<string>()
        next.listen((message) => void received.resolve(message))
        await nextPage.opened
        void nextPage.send('after')
        expect(await received.promise).toBe('after')
        expect(closeAfterVerdict).toBe(false)
        nextPage.abort()
      })

    test("a page that follows and asks for a shorter deadline after its CLOSE_DONE shortens the deciding server's", async () => {
      const [channel, page] = await pair(transport)
      // The server's cleanup outlasts the shorter deadline, not its own.
      channel.onClose(() => new Promise<void>(() => {}))
      page.onClose(() => void setImmediate(() => void page.close({ timeout: 100 })))
      void channel.close({ timeout: 60_000 })
      expect(await channel.closed).toHaveProperty('message', expect.stringContaining('Channel close timed out'))
      expect(await page.closed).toHaveProperty('message', expect.stringContaining('Channel close timed out'))
    })

    test('a page whose verdict the network lost reconnects, and the server reads it', async () => {
      const [channel, page] = await pair(transport, 60_000)
      // The server's CLOSE_DONE has gone; what the page sends after it, its verdict, the server never reads.
      channel.onClose(() => void setImmediate(deafenServer))
      expect(await page.close({ timeout: 10_000 })).toBe(null)
      const connections = dropConnections()
      connections.reconnect()
      expect(await channel.closed).toBe(null)
    })

    test('a page whose abort the network lost reconnects, and the server reads it', async () => {
      const [channel, page] = await pair(transport, 60_000)
      deafenServer()
      page.abort('bye')
      const connections = dropConnections()
      connections.reconnect()
      const closed = await channel.closed
      expect(isAbort(closed) && closed.abortValue).toBe('bye')
    })
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
      const held = queue.tryHold(HOLDS_AT_MOST)
      connections.reconnect()
      expect(held).toBe(false)

      const closed = await page.closed
      expect(received).toEqual(['m1', 'm2', 'm3'])
      expect(closed).toHaveProperty('message', expect.stringContaining('Channel close timed out'))
    })
  })
}
