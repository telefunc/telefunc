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
 *  with the next what `next` says: destroy the next connection as it accepts it; or, by the next request that opens a
 *  connection, an event stream's or a WebSocket's, hold it connected to nothing (an SSE upload beside it too), forward
 *  it and hold the server's answer until `release`, or forward it and give the page the head of the server's answer
 *  alone. Where `holdsAll`, it holds every request; after `answerLate(ms)`, it holds each answer `ms` from its request;
 *  after `holdUntil(at)`, it holds every request that comes before `at`. `requests`: when each connection's request
 *  came, what it did with it, and when the page closed it; Node 22's fetch may open a connection well before the
 *  request it carries. Closed with the row, so no other row's page reconnects through it. */
type Take = 'forward' | 'destroy' | 'hold' | 'late' | 'head'

async function link(holdsAll = false) {
  let lateMs = 0
  let holdsBefore = 0
  const sockets = new Set<Socket>()
  let next: Take = 'forward'
  /** Whether the last connection's open was held: an upload beside it is too. */
  let openHeld = false
  const late: Array<{ page: Socket; upstream: Socket; answer: Buffer[] }> = []
  const requests: Array<{ at: number; take: Take; closedAt: number | null }> = []
  const forwarder = net.createServer((page) => {
    page.on('error', () => {})
    sockets.add(page)
    page.on('close', () => sockets.delete(page))
    if (next === 'destroy') {
      next = 'forward'
      return void page.destroy()
    }
    page.once('data', (request: Buffer) => {
      const at = performance.now()
      const opens = /text\/event-stream|upgrade: websocket/i.test(request.toString('latin1'))
      const held = holdsAll || at < holdsBefore || next === 'hold' || (!opens && openHeld)
      const take = held ? 'hold' : opens ? next : 'forward'
      if (opens) [next, openHeld] = ['forward', held]
      const connection: { at: number; take: Take; closedAt: number | null } = { at, take, closedAt: null }
      requests.push(connection)
      page.on('close', () => void (connection.closedAt = performance.now()))
      // Read and dropped: a socket left unread would never see the page's end.
      if (take === 'hold') return
      page.pause()
      const upstream = net.connect(port, '127.0.0.1')
      upstream.on('error', () => {})
      upstream.write(request)
      page.pipe(upstream)
      if (take === 'late') {
        const answer: Buffer[] = []
        upstream.on('data', (chunk: Buffer) => void answer.push(chunk))
        late.push({ page, upstream, answer })
      } else if (take === 'head') {
        let answer = Buffer.alloc(0)
        const untilHead = (chunk: Buffer) => {
          answer = Buffer.concat([answer, chunk])
          const end = answer.indexOf('\r\n\r\n')
          if (end === -1) return
          upstream.off('data', untilHead)
          page.write(answer.subarray(0, end + 4))
        }
        upstream.on('data', untilHead)
      } else if (lateMs > 0) {
        const answer: Buffer[] = []
        const hold = (chunk: Buffer) => void answer.push(chunk)
        upstream.on('data', hold)
        setTimeout(() => {
          upstream.off('data', hold)
          for (const chunk of answer) page.write(chunk)
          upstream.pipe(page)
        }, lateMs)
      } else upstream.pipe(page)
      page.on('close', () => upstream.destroy())
      upstream.on('close', () => page.destroy())
    })
  })
  await new Promise<void>((resolve) => forwarder.listen(0, '127.0.0.1', resolve))
  const address = forwarder.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  return {
    telefuncUrl: `http://127.0.0.1:${address.port}/_telefunc`,
    requests,
    answerLate(ms: number) {
      lateMs = ms
    },
    holdUntil(at: number) {
      holdsBefore = at
    },
    drop(then: Take) {
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

/** A server channel, and its page over `transport` through a link of its own; `connection`: the page's, which it
 *  registered on as it was made. */
async function pair(transport: 'sse' | 'ws') {
  const network = await link()
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel)
  const register = vi.spyOn(WireLifecycle.prototype, 'register')
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl: network.telefuncUrl, transports: [transport], connectionKey: crypto.randomUUID() },
  })
  const connection = register.mock.contexts.at(-1)
  register.mockRestore()
  await Promise.all([channel.opened, page.opened])
  const received = Promise.withResolvers<string>()
  channel.listen((message) => void received.resolve(message))
  return { network, page, received: received.promise, connection }
}

test("over a WebSocket, a page whose every connection the network takes and never answers ends its channel at the defaults' window from its first connect: the connection was never established", async () => {
  const network = await link(true)
  const startedAt = performance.now()
  const page = new ClientChannel<string, string>({
    channelId: crypto.randomUUID(),
    connection: { telefuncUrl: network.telefuncUrl, transports: ['ws'], connectionKey: crypto.randomUUID() },
  })
  try {
    // The defaults, before any RECONCILED said the server's: two pings of 5 s, then 60 s.
    const [opened, closed] = await Promise.allSettled([page.opened, page.closed])
    const elapsed = performance.now() - startedAt
    expect(opened).toMatchObject({ status: 'rejected' })
    expect(closed).toMatchObject({
      status: 'fulfilled',
      value: { name: 'NetworkError', message: 'Connection not established' },
    })
    expect(elapsed).toBeGreaterThan(69_000)
    expect(elapsed).toBeLessThan(75_000)
  } finally {
    network.close()
  }
}, 80_000)

for (const transport of ['sse', 'ws'] as const)
  for (const attempt of ['destroy', 'hold'] as const) {
    // A WebSocket reset at accept: the row after this loop.
    if (transport === 'ws' && attempt === 'destroy') continue
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
  const lost = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
  // The page's own connection: an earlier row's, closed, may still report its sockets' ends.
  const { network, page, received, connection } = await pair('ws')
  try {
    network.drop('destroy')
    void page.send('after')
    expect(await received).toBe('after')
    // The wire's drop, then the socket the network reset: each once.
    expect(lost.mock.contexts.filter((context) => context === connection)).toHaveLength(2)
    page.abort()
  } finally {
    lost.mockRestore()
    network.close()
  }
})

for (const transport of ['sse', 'ws'] as const)
  test(`over ${transport === 'ws' ? 'a WebSocket' : 'SSE'}, a page whose next connection after a drop the network takes and never answers cuts it at its 3 s allowance, before the next and before its RECONCILE's 10 s, as a failed attempt`, async () => {
    const lost = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
    // The page's own connection: an earlier row's, closed, may still report its sockets' ends.
    const { network, page, received, connection } = await pair(transport)
    try {
      const droppedAt = performance.now()
      network.drop('hold')
      void page.send('after')
      expect(await received).toBe('after')
      const since = network.requests.filter(({ at }) => at >= droppedAt)
      const [held] = since
      const cutAt = held?.closedAt ?? Infinity
      const heldFor = cutAt - (held?.at ?? 0)
      expect(heldFor).toBeGreaterThan(2_900)
      expect(heldFor).toBeLessThan(4_000)
      // The attempt's connections, an SSE upload's too, closed with it; the next attempt's came after.
      const attempt = since.filter(({ at }) => at < cutAt)
      expect(attempt.every(({ closedAt }) => closedAt !== null && closedAt < cutAt + 100)).toBe(true)
      expect(since.some(({ at }) => at > cutAt)).toBe(true)
      // The wire's drop, then the cut.
      expect(lost.mock.contexts.filter((context) => context === connection)).toHaveLength(2)
      page.abort()
    } finally {
      lost.mockRestore()
      network.close()
    }
  }, 15_000)

for (const transport of ['sse', 'ws'] as const)
  test(`over ${transport === 'ws' ? 'a WebSocket' : 'SSE'}, a page whose server answers every connection after a drop 4 s late cuts the first at 3 s and sends on the next, given 6 s`, async () => {
    const lost = vi.spyOn(WireLifecycle.prototype, 'onTransportClosed')
    const { network, page, received, connection } = await pair(transport)
    try {
      network.answerLate(4_000)
      const droppedAt = performance.now()
      network.drop('forward')
      void page.send('after')
      expect(await received).toBe('after')
      // 3 s, at most 1 s of backoff, then 4 s.
      expect(performance.now() - droppedAt).toBeLessThan(9_000)
      expect(lost.mock.contexts.filter((context) => context === connection)).toHaveLength(2)
      page.abort()
    } finally {
      lost.mockRestore()
      network.close()
    }
  }, 15_000)

test("over SSE, a page whose opens after a drop the network holds cuts the third at its 12 s allowance, not at its RECONCILE's 10 s, before the next, and sends on that", async () => {
  vi.spyOn(Math, 'random').mockReturnValue(0.9)
  const { network, page, received } = await pair('sse')
  try {
    const droppedAt = performance.now()
    // The first three attempts: 0.9 of 500, then 0.9 of 1,000 and of 2,000 after cuts at 3 s and 6 s.
    network.holdUntil(droppedAt + 20_000)
    network.drop('forward')
    void page.send('after')
    expect(await received).toBe('after')
    const since = network.requests.filter(({ at }) => at >= droppedAt)
    // The third attempt's connections, its event stream's and its upload's.
    const third = since.filter(({ at }) => at > droppedAt + 11_000 && at < droppedAt + 13_000)
    expect(third.length).toBeGreaterThan(0)
    for (const { at, closedAt } of third) {
      expect((closedAt ?? Infinity) - at).toBeGreaterThan(11_500)
      expect((closedAt ?? Infinity) - at).toBeLessThan(13_000)
    }
    const cutAt = Math.max(...third.map(({ closedAt }) => closedAt ?? Infinity))
    expect(since.some(({ at }) => at > cutAt)).toBe(true)
    page.abort()
  } finally {
    vi.restoreAllMocks()
    network.close()
  }
}, 40_000)

// Over a WebSocket, the wire's heartbeat finds such a connection dead (reconnectHeadOnly.spec.ts).
test("over SSE, a page whose next connection after a drop gets the server's head and never its RECONCILED closes that connection at its RECONCILE's 10 s, before the next, and sends on that", async () => {
  const { network, page, received } = await pair('sse')
  try {
    const droppedAt = performance.now()
    network.drop('head')
    void page.send('after')
    expect(await received).toBe('after')
    const since = network.requests.filter(({ at }) => at >= droppedAt)
    const headOnly = since.find(({ take }) => take === 'head')
    const closedAt = headOnly?.closedAt ?? Infinity
    expect(closedAt - (headOnly?.at ?? 0)).toBeGreaterThan(9_500)
    expect(closedAt - (headOnly?.at ?? 0)).toBeLessThan(11_000)
    // The attempt's other connections, an SSE upload's, closed with it at the latest; the next attempt's came after.
    const attempt = since.filter(({ at }) => at < closedAt)
    expect(attempt.every(({ closedAt: end }) => end !== null && end < closedAt + 100)).toBe(true)
    expect(since.some(({ at }) => at > closedAt)).toBe(true)
    page.abort()
  } finally {
    network.close()
  }
}, 20_000)

test('over a WebSocket, a page whose next socket after a drop the server answers late cuts it at its allowance and sends on the next; the late one sends nothing, and the server lets it go and logs nothing', async () => {
  const { network, page, received } = await pair('ws')
  const opened = vi.spyOn(ChannelMux.prototype, 'onWireOpen')
  const closed = vi.spyOn(ChannelMux.prototype, 'onWireClosed')
  const messages = vi.spyOn(ChannelMux.prototype, 'onWireMessage')
  const logs = [vi.spyOn(console, 'error'), vi.spyOn(console, 'warn'), vi.spyOn(console, 'log')]
  try {
    network.drop('late')
    void page.send('after')
    // The server took both upgrades; the page cut the late one before it read its answer.
    expect(await received).toBe('after')
    expect(opened).toHaveBeenCalledTimes(2)
    const [late, next] = opened.mock.calls.map(([wire]) => wire)
    network.release()
    await vi.waitFor(() => expect(closed.mock.calls.map(([wire]) => wire)).toContain(late))
    expect(messages.mock.calls.filter(([wire]) => wire === late)).toEqual([])
    expect(messages.mock.calls.some(([wire]) => wire === next)).toBe(true)
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
