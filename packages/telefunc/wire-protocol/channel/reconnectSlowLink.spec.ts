// A page whose wire drops on the slowest link it supports: its next attempt opens, however long that takes there, and
// is never cut. Over a real server and page, through a forwarder of each row's own that makes the link slow after the
// page connected.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import net, { type Socket } from 'node:net'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ATTACHABLE_NOW, ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { WireLifecycle } from './client/WireLifecycle.js'
import { getChannelMux } from './server/ChannelMux.js'

/** 400 kbit/s each way, 200 ms each way. */
const BYTES_PER_S = 50_000
const ONE_WAY_MS = 200
/** A new connection's TCP and TLS 1.3 handshakes, a round trip each, before its first byte goes. */
const HANDSHAKE_MS = 4 * ONE_WAY_MS

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

/** A forwarder that passes bytes as they come until `slow()`, then at the link's rate and delay each way, a new
 *  connection's first bytes held for its handshakes; `drop()` cuts every connection. */
async function link() {
  let slowSince: number | null = null
  const sockets = new Set<Socket>()
  /** `from`'s bytes to `to`, in order, each way's link busy while one is under way. */
  const carry = (from: Socket, to: Socket) => {
    let free = 0
    // One timer writes what is due, in order: Node dates a timer from the loop's cached time, so one each may reorder.
    const queue: { chunk: Buffer; due: number }[] = []
    let timer: ReturnType<typeof setTimeout> | null = null
    const writeDue = () => {
      timer = null
      for (let next = queue[0]; next !== undefined && next.due <= performance.now(); next = queue[0]) {
        queue.shift()
        if (!to.destroyed) to.write(next.chunk)
      }
      const [next] = queue
      if (next !== undefined) timer = setTimeout(writeDue, Math.max(0, next.due - performance.now()))
    }
    from.on('data', (chunk: Buffer) => {
      if (slowSince === null) return void to.write(chunk)
      const now = performance.now()
      free = Math.max(free, now) + (chunk.byteLength * 1000) / BYTES_PER_S
      queue.push({ chunk, due: free + ONE_WAY_MS })
      timer ??= setTimeout(writeDue, free + ONE_WAY_MS - now)
    })
  }
  const forwarder = net.createServer((page) => {
    page.on('error', () => {})
    sockets.add(page)
    page.pause()
    setTimeout(
      () => {
        const upstream = net.connect(port, '127.0.0.1')
        upstream.on('error', () => {})
        sockets.add(upstream)
        carry(page, upstream)
        carry(upstream, page)
        page.on('close', () => upstream.destroy())
        upstream.on('close', () => page.destroy())
        page.resume()
      },
      slowSince === null ? 0 : HANDSHAKE_MS,
    )
  })
  await new Promise<void>((resolve) => forwarder.listen(0, '127.0.0.1', resolve))
  const address = forwarder.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  return {
    telefuncUrl: `http://127.0.0.1:${address.port}/_telefunc`,
    slow() {
      slowSince = performance.now()
    },
    drop() {
      for (const socket of sockets) socket.destroy()
    },
    close() {
      for (const socket of sockets) socket.destroy()
      forwarder.close()
    },
  }
}

for (const channels of [1, 4_096])
  for (const transport of ['sse', 'ws'] as const)
    test(`over ${transport === 'ws' ? 'a WebSocket' : 'SSE'}, a page with ${channels === 1 ? 'a channel' : '4,096 channels'} whose wire drops on a 400 kbit/s link with a 400 ms round trip reconnects on its first attempt, and what it sent arrives`, async () => {
      const network = await link()
      const register = vi.spyOn(WireLifecycle.prototype, 'register')
      const pages: ClientChannel<string, string>[] = []
      try {
        const connection = {
          telefuncUrl: network.telefuncUrl,
          transports: [transport],
          connectionKey: crypto.randomUUID(),
        }
        const ends = Array.from({ length: channels }, () => {
          const channel = new ServerChannel<string, string>()
          getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
          return channel
        })
        for (const channel of ends) pages.push(new ClientChannel<string, string>({ channelId: channel.id, connection }))
        // The page's own connection, which its channels registered on as they were made.
        const lifecycle = register.mock.contexts.at(-1)
        register.mockRestore()
        await Promise.all([...ends.map((channel) => channel.opened), ...pages.map((page) => page.opened)])
        const [lead] = ends
        const [first] = pages
        if (!lead || !first) throw new Error('no channel')
        const received = Promise.withResolvers<string>()
        lead.listen((message) => void received.resolve(message))
        network.slow()
        const lost = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
        try {
          network.drop()
          void first.send('after')
          expect(await received.promise).toBe('after')
          // The drop alone: no attempt was cut.
          expect(lost.mock.contexts.filter((context) => context === lifecycle)).toHaveLength(1)
        } finally {
          lost.mockRestore()
        }
      } finally {
        register.mockRestore()
        for (const page of pages) page.abort()
        network.close()
      }
    }, 60_000)

test('over a WebSocket, a page that handed 600 kB on a fast link that then drops to 400 kbit/s joins 100 channels behind it, which open on that wire', async () => {
  const network = await link()
  const register = vi.spyOn(WireLifecycle.prototype, 'register')
  const pages: ClientChannel<string, string>[] = []
  try {
    const connection = {
      telefuncUrl: network.telefuncUrl,
      transports: ['ws' as const],
      connectionKey: crypto.randomUUID(),
    }
    const lead = new ServerChannel<string, string>()
    getChannelMux().registerChannel(lead, ATTACHABLE_NOW)
    const leadPage = new ClientChannel<string, string>({ channelId: lead.id, connection })
    pages.push(leadPage)
    const lifecycle = register.mock.contexts.at(-1)
    register.mockRestore()
    await Promise.all([lead.opened, leadPage.opened])
    let took = 0
    const all = Promise.withResolvers<void>()
    lead.listenBinary((data) => {
      took += data.byteLength
      if (took >= 600_000) all.resolve()
    })
    const lost = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
    try {
      // 12 s at the floor: a server channel waits `CHANNEL_CONNECT_TTL_MS`, 15 s, for its page.
      void leadPage.sendBinary(new Uint8Array(600_000), { ack: false })
      network.slow()
      const joined = Array.from({ length: 100 }, () => {
        const channel = new ServerChannel<string, string>()
        getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
        pages.push(new ClientChannel<string, string>({ channelId: channel.id, connection }))
        return channel
      })
      await Promise.all([...joined.map((channel) => channel.opened), ...pages.map((page) => page.opened), all.promise])
      expect(lost.mock.contexts.filter((context) => context === lifecycle)).toEqual([])
    } finally {
      lost.mockRestore()
    }
  } finally {
    register.mockRestore()
    for (const page of pages) page.abort()
    network.close()
  }
}, 60_000)

test("over a WebSocket on a link that drops to 400 kbit/s, a page's join behind 500 kB of its upload, while the server's 1 MB frame to it waits for room, gets the RECONCILED past that frame and opens", async () => {
  const network = await link()
  const register = vi.spyOn(WireLifecycle.prototype, 'register')
  const pages: ClientChannel<string, string>[] = []
  try {
    const connection = {
      telefuncUrl: network.telefuncUrl,
      transports: ['ws' as const],
      connectionKey: crypto.randomUUID(),
    }
    const [lead, neighbour] = [new ServerChannel<string, string>(), new ServerChannel<string, string>()]
    getChannelMux().registerChannel(lead, ATTACHABLE_NOW)
    const leadPage = new ClientChannel<string, string>({ channelId: lead.id, connection })
    getChannelMux().registerChannel(neighbour, ATTACHABLE_NOW)
    pages.push(leadPage, new ClientChannel<string, string>({ channelId: neighbour.id, connection }))
    const lifecycle = register.mock.contexts.at(-1)
    register.mockRestore()
    await Promise.all([lead.opened, neighbour.opened, ...pages.map((page) => page.opened)])
    const lost = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
    try {
      network.slow()
      // 10 s up at the floor, ahead of the RECONCILE and of the page's confirmations of what the server sends.
      void leadPage.sendBinary(new Uint8Array(500_000), { ack: false })
      void neighbour.sendBinary(new Uint8Array(1_000_000), { ack: false })
      const joined = Array.from({ length: 10 }, () => {
        const channel = new ServerChannel<string, string>()
        getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
        pages.push(new ClientChannel<string, string>({ channelId: channel.id, connection }))
        return channel
      })
      await Promise.all([...joined.map((channel) => channel.opened), ...pages.map((page) => page.opened)])
      expect(lost.mock.contexts.filter((context) => context === lifecycle)).toEqual([])
    } finally {
      lost.mockRestore()
    }
  } finally {
    register.mockRestore()
    for (const page of pages) page.abort()
    network.close()
  }
}, 60_000)

test('over a WebSocket at 400 kbit/s, a channel that exchanges numbered messages both ways while 1,000 channels of 256-byte ids join, and whose wire drops before their RECONCILED, gets each message once and in order at both ends', async () => {
  const network = await link()
  const register = vi.spyOn(WireLifecycle.prototype, 'register')
  const pages: ClientChannel<string, string>[] = []
  try {
    const connection = {
      telefuncUrl: network.telefuncUrl,
      transports: ['ws' as const],
      connectionKey: crypto.randomUUID(),
    }
    const lead = new ServerChannel<string, string>()
    getChannelMux().registerChannel(lead, ATTACHABLE_NOW)
    const leadPage = new ClientChannel<string, string>({ channelId: lead.id, connection })
    pages.push(leadPage)
    const lifecycle = register.mock.contexts.at(-1)
    register.mockRestore()
    await Promise.all([lead.opened, leadPage.opened])
    const byServer: string[] = []
    const byPage: string[] = []
    const pageGotOne = Promise.withResolvers<void>()
    lead.listen((message) => void byServer.push(message))
    leadPage.listen((message) => {
      byPage.push(message)
      pageGotOne.resolve()
    })
    network.slow()
    const joined = Array.from({ length: 1_000 }, (_, ix) => {
      const channel = new ServerChannel<string, string>({ id: `${crypto.randomUUID()}-${ix}`.padEnd(256, 'x') })
      getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
      pages.push(new ClientChannel<string, string>({ channelId: channel.id, connection }))
      return channel
    })
    let joinedOpen = 0
    for (const channel of joined) void channel.opened.then(() => joinedOpen++)
    const numbers = Array.from({ length: 20 }, (_, n) => String(n + 1))
    for (const n of numbers) {
      void leadPage.send(n)
      void lead.send(n)
    }
    const lost = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
    try {
      // The join's RECONCILE, some 300 kB, is still going up: the wire drops while what the server sent comes down.
      await pageGotOne.promise
      expect(joinedOpen).toBe(0)
      network.drop()
      await Promise.all([...joined.map((channel) => channel.opened), ...pages.map((page) => page.opened)])
      // A stuck row ends at the harness's timeout.
      while (byServer.length < numbers.length || byPage.length < numbers.length)
        await new Promise((r) => setTimeout(r, 50))
      expect(lost.mock.contexts.filter((context) => context === lifecycle)).toHaveLength(1)
    } finally {
      lost.mockRestore()
    }
    expect(byServer).toEqual(numbers)
    expect(byPage).toEqual(numbers)
  } finally {
    register.mockRestore()
    for (const page of pages) page.abort()
    network.close()
  }
}, 90_000)

test("over a WebSocket at 400 kbit/s, a page downloading 1 MB joins 1,000 channels on the same wire, within half again the link's time for the download", async () => {
  const network = await link()
  const register = vi.spyOn(WireLifecycle.prototype, 'register')
  const pages: ClientChannel<string, string>[] = []
  try {
    const connection = {
      telefuncUrl: network.telefuncUrl,
      transports: ['ws' as const],
      connectionKey: crypto.randomUUID(),
    }
    const lead = new ServerChannel<string, string>()
    getChannelMux().registerChannel(lead, ATTACHABLE_NOW)
    const leadPage = new ClientChannel<string, string>({ channelId: lead.id, connection })
    pages.push(leadPage)
    const lifecycle = register.mock.contexts.at(-1)
    register.mockRestore()
    await Promise.all([lead.opened, leadPage.opened])
    const LARGE = 1_000_000
    let unread = LARGE
    const downloaded = Promise.withResolvers<void>()
    leadPage.listenBinary((data) => {
      unread -= data.byteLength
      if (unread === 0) downloaded.resolve()
    })
    const lost = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
    try {
      network.slow()
      void lead.sendBinary(new Uint8Array(LARGE), { ack: false })
      const joined = Array.from({ length: 1_000 }, () => {
        const channel = new ServerChannel<string, string>()
        getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
        pages.push(new ClientChannel<string, string>({ channelId: channel.id, connection }))
        return channel
      })
      let opened = 0
      for (const channel of [...joined, ...pages]) void channel.opened.then(() => opened++)
      const linkMs = (LARGE * 1_000) / BYTES_PER_S
      await Promise.race([
        Promise.all([
          ...joined.map((channel) => channel.opened),
          ...pages.map((page) => page.opened),
          downloaded.promise,
        ]),
        new Promise((resolve) => setTimeout(resolve, 1.5 * linkMs)),
      ])
      // At the deadline: what the page has yet to read, the channels not open at both ends, and the wires it lost.
      expect({
        unread,
        notOpen: joined.length + pages.length - opened,
        losses: lost.mock.contexts.filter((context) => context === lifecycle).length,
      }).toEqual({ unread: 0, notOpen: 0, losses: 0 })
    } finally {
      lost.mockRestore()
    }
  } finally {
    register.mockRestore()
    for (const page of pages) page.abort()
    network.close()
  }
}, 90_000)

for (const burst of [false, true])
  test(`over a WebSocket at 400 kbit/s, a page whose wire drops while the server sends 1 MB on one of its 1,000 channels${burst ? ' and 1 kB on each other' : ''} gets all of it on the next wire, which it keeps`, async () => {
    const network = await link()
    const register = vi.spyOn(WireLifecycle.prototype, 'register')
    const pages: ClientChannel<string, string>[] = []
    try {
      const connection = {
        telefuncUrl: network.telefuncUrl,
        transports: ['ws' as const],
        connectionKey: crypto.randomUUID(),
      }
      const ends = Array.from({ length: 1_000 }, () => {
        const channel = new ServerChannel<string, string>()
        getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
        pages.push(new ClientChannel<string, string>({ channelId: channel.id, connection }))
        return channel
      })
      const lifecycle = register.mock.contexts.at(-1)
      register.mockRestore()
      await Promise.all([...ends.map((channel) => channel.opened), ...pages.map((page) => page.opened)])
      const [lead, ...others] = ends
      const [first, ...rest] = pages
      if (!lead || !first) throw new Error('no channel')
      const LARGE = 1_000_000
      const SMALL = 1_024
      let unread = LARGE + (burst ? others.length * SMALL : 0)
      const allRead = Promise.withResolvers<void>()
      const read = (bytes: number) => {
        unread -= bytes
        if (unread === 0) allRead.resolve()
      }
      first.listenBinary((data) => read(data.byteLength))
      for (const page of rest) page.listenBinary((data) => read(data.byteLength))
      // The row ends when the page read it all or lost its next wire too, however fast this machine runs.
      const secondLoss = Promise.withResolvers<void>()
      const onTransportClosed = WireLifecycle.prototype.onTransportClosed
      let losses = 0
      const lost = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed').mockImplementation(function (
        this: WireLifecycle,
        ...args
      ) {
        if (this === lifecycle && ++losses === 2) secondLoss.resolve()
        return onTransportClosed.apply(this, args)
      })
      try {
        network.slow()
        void lead.sendBinary(new Uint8Array(LARGE), { ack: false })
        if (burst) for (const end of others) void end.sendBinary(new Uint8Array(SMALL), { ack: false })
        network.drop()
        await Promise.race([allRead.promise, secondLoss.promise])
        // What the page has yet to read, and the wires it lost: the drop alone.
        expect({ unread, losses }).toEqual({ unread: 0, losses: 1 })
      } finally {
        lost.mockRestore()
      }
    } finally {
      register.mockRestore()
      for (const page of pages) page.abort()
      network.close()
    }
  }, 90_000)
