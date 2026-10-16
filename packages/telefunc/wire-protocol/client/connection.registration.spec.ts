// A page registers a new channel by naming it in a RECONCILE: the server may not have registered it yet (a call's callback
// whose request is still on its way, or never arrives), and the page may be moving to a WebSocket. These drive the real
// ClientChannel against the real server over each wire.

import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { ClientChannel } from './channel.js'
import { config } from '../../client/clientConfig.js'
import { ServerChannel } from '../server/channel.js'
import { getChannelMux } from '../server/mux.js'
import { getTelefuncChannelHooks } from '../server/ws.js'
import { TAG } from '../shared-ws.js'
import { config as serverConfig, getServerConfig } from '../../node/server/serverConfig.js'
import { NetworkError } from '../../shared/NetworkError.js'
import { isAbort } from '../../shared/Abort.js'
import { Net, sseFetch, webSocketTo, type Traffic } from '../test/page-server.js'

type Wire = 'sse' | 'sse-batch' | 'ws'
const WIRES: Wire[] = ['sse', 'sse-batch', 'ws']

// Before the first reconcile, so every RECONCILED offers a page the WebSocket.
getTelefuncChannelHooks()
const { connectTtl } = getServerConfig().channel

beforeEach(() => {
  vi.useFakeTimers()
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
  delete config.fetch
})

/** A page on `wire`, and the server it talks to. `upgrade`: an SSE page that may move to a WebSocket. `delays`: how
 *  long a frame of a tag takes to reach the page, the ones after it on its wire waiting behind it. `refusedAfter`: how
 *  long an SSE-batch page's upload request takes to be refused. `postMs`: how long each batch POST's body takes to end
 *  after its frames, which the server reads at once. */
function page(
  wire: Wire,
  { upgrade = false, delays = {} as Partial<Record<number, number>>, refusedAfter = 0, postMs = 0 } = {},
) {
  const net = new Net()
  net.delays = delays
  net.refusedAfter = refusedAfter
  net.postMs = postMs
  if (wire === 'ws' || upgrade) vi.stubGlobal('WebSocket', webSocketTo(net))
  if (wire !== 'ws') config.fetch = sseFetch(net, wire === 'sse-batch')
  const telefuncUrl = `http://${crypto.randomUUID()}.test/_telefunc`
  const connectionKey = crypto.randomUUID()
  return {
    traffic: net.traffic,
    /** Ends the latest wire as a network drop does. */
    cut: () => net.links.at(-1)!.drop(),
    /** How many wires the page opened. */
    wires: () => net.links.length,
    failNextReconcilePost: (after: 'read' | 'unread') => void (net.failNextReconcilePost = after),
    holdNextReconcilePost: (ms: number) => void (net.holdNextReconcilePost = ms),
    endUploadAfterNextReconcile: () => void (net.endUploadAfterNextReconcile = true),
    /** A channel the page opens, such as a call's callback. */
    channel<ClientToServer = unknown, ServerToClient = unknown>(channelId: string = crypto.randomUUID()) {
      return new ClientChannel<ClientToServer, ServerToClient>({
        channelId,
        ack: true,
        transports: wire === 'ws' ? ['ws'] : upgrade ? ['sse', 'ws'] : ['sse'],
        telefuncUrl,
        connectionKey,
      })
    },
  }
}

/** What the server registers when a call's request arrives. */
function register<ClientToServer = unknown, ServerToClient = unknown>(id: string = crypto.randomUUID()) {
  const channel = new ServerChannel<ClientToServer, ServerToClient>({ id, ack: true })
  getChannelMux().registerChannel(channel)
  return channel
}

/** A keystroke every `everyMs`, each aborting the previous call, whose request had left, and making a callback the
 *  server registers. `openedAfter`: how long each callback that opened took. */
async function searchBox(channel: ReturnType<typeof page>['channel'], keystrokes: number, everyMs: number) {
  const openedAfter: number[] = []
  const networkErrors: unknown[] = []
  const onClose = (err: unknown) => void (err instanceof NetworkError && networkErrors.push(err))
  let previous: ClientChannel<unknown, unknown> | null = null
  for (let keystroke = 0; keystroke < keystrokes; keystroke++) {
    previous?.abort()
    const server = register()
    server.onClose(onClose)
    const callback = channel(server.id)
    const createdAt = Date.now()
    callback.onOpen(() => void openedAfter.push(Date.now() - createdAt))
    callback.onClose(onClose)
    previous = callback
    await vi.advanceTimersByTimeAsync(everyMs)
  }
  return { openedAfter, networkErrors }
}

describe.each(WIRES)('over %s', (wire) => {
  test("while an aborted call's callback never registers, the next call's callback opens at once and stays open", async () => {
    const { channel } = page(wire)
    // A search box: the first keystroke's call is aborted before its request leaves, so the server never registers
    // its callback.
    channel().abort()
    await vi.advanceTimersByTimeAsync(100)
    // The next keystroke's call reaches the server.
    const server = register()
    let closedWith: unknown = 'open'
    server.onClose((err) => void (closedWith = err))
    const callback = channel(server.id)
    const createdAt = Date.now()
    let openedAfter = -1
    callback.onOpen(() => void (openedAfter = Date.now() - createdAt))
    await vi.advanceTimersByTimeAsync(3 * connectTtl)
    expect(openedAfter).toBeGreaterThanOrEqual(0)
    expect(openedAfter).toBeLessThan(connectTtl / 10)
    expect(closedWith).toBe('open')
  })

  test("a callback whose call reaches the server after the RECONCILE naming it attaches, and doesn't hold the channel named with it", async () => {
    const { channel } = page(wire)
    const returned = register() // one a call returned: the server registered it before the page saw it
    let returnedOpen = false
    channel(returned.id).onOpen(() => void (returnedOpen = true))
    const callbackId = crypto.randomUUID()
    const callback = channel<string, string>(callbackId)
    callback.listen((question) => `answer to ${question}`)
    void callback.send('sent before its call arrived')
    await vi.advanceTimersByTimeAsync(200)
    expect(returnedOpen).toBe(true)
    // Its call arrives, and the telefunction calls the callback.
    const server = register<string, string>(callbackId)
    const received: string[] = []
    server.listen((message) => void received.push(message))
    const answer = server.send('question', { ack: true })
    await vi.advanceTimersByTimeAsync(200)
    expect(await answer).toBe('answer to question')
    expect(received).toEqual(['sent before its call arrived'])
  })

  test('a callback whose call never reaches the server is released with the same error as before, connectTtl after the RECONCILE naming it', async () => {
    const { channel } = page(wire)
    // A call lost in a drop, then the next one, whose request is lost too.
    channel()
    await vi.advanceTimersByTimeAsync(100)
    const callback = channel()
    const createdAt = Date.now()
    let closed: { err: unknown; after: number } | null = null
    callback.onClose((err) => void (closed = { err, after: Date.now() - createdAt }))
    await vi.advanceTimersByTimeAsync(3 * connectTtl)
    expect(closed!.err).toMatchObject({
      name: 'NetworkError',
      message: 'Channel not acknowledged by server after reconnect',
      isChannel: true,
    })
    expect(closed!.after).toBeGreaterThanOrEqual(connectTtl)
    expect(closed!.after).toBeLessThan(connectTtl + connectTtl / 10)
  })

  test("an aborted call's callback whose request reaches the server after the RECONCILE naming it gets the abort", async () => {
    const { channel } = page(wire)
    channel(register().id)
    await vi.advanceTimersByTimeAsync(200)
    const callbackId = crypto.randomUUID()
    // Its call is aborted once its request has left.
    channel(callbackId).abort()
    await vi.advanceTimersByTimeAsync(200)
    const server = register(callbackId)
    let closedWith: unknown = 'open'
    server.onClose((err) => void (closedWith = err))
    await vi.advanceTimersByTimeAsync(200)
    expect(isAbort(closedWith)).toBe(true)
  })

  test('a callback the page aborts while the server awaits it gets the abort once its call arrives', async () => {
    const { channel } = page(wire)
    channel(register().id)
    await vi.advanceTimersByTimeAsync(200)
    const callbackId = crypto.randomUUID()
    const callback = channel(callbackId)
    await vi.advanceTimersByTimeAsync(200)
    callback.abort()
    channel(register().id) // the page's next RECONCILE
    await vi.advanceTimersByTimeAsync(200)
    const server = register(callbackId)
    let closedWith: unknown = 'open'
    server.onClose((err) => void (closedWith = err))
    await vi.advanceTimersByTimeAsync(200)
    expect(isAbort(closedWith)).toBe(true)
  })

  test('what a callback sends before and after a reconnect reaches it in order once its call arrives', async () => {
    const { channel, cut } = page(wire)
    channel(register().id)
    await vi.advanceTimersByTimeAsync(200)
    const callbackId = crypto.randomUUID()
    const callback = channel<string, string>(callbackId)
    void callback.send('before the drop', { ack: false })
    await vi.advanceTimersByTimeAsync(200)
    cut()
    await vi.advanceTimersByTimeAsync(2_000)
    void callback.send('after the drop', { ack: false })
    channel(register().id) // its RECONCILE carries what the page queued
    await vi.advanceTimersByTimeAsync(200)
    const server = register<string, string>(callbackId)
    const received: string[] = []
    server.listen((message) => void received.push(message))
    await vi.advanceTimersByTimeAsync(200)
    expect(received).toEqual(['before the drop', 'after the drop'])
  })

  test('a wire stays up while the server awaits a channel its first RECONCILE named, past the ping deadline', async () => {
    serverConfig.channel = { connectTtl: 9_000, pingInterval: 2_000 }
    const mux = getChannelMux() as unknown as { resolvedOptions: unknown }
    mux.resolvedOptions = null
    try {
      const { channel, wires } = page(wire)
      const callbackId = crypto.randomUUID()
      let opened = false
      channel(callbackId).onOpen(() => void (opened = true))
      await vi.advanceTimersByTimeAsync(8_000) // twice the ping interval, the server's deadline, and more
      register(callbackId)
      await vi.advanceTimersByTimeAsync(200)
      expect(opened).toBe(true)
      expect(wires()).toBe(1)
    } finally {
      serverConfig.channel = {}
      mux.resolvedOptions = null
    }
  })

  test("what a channel sends once its reconnect's wire dropped, while the server awaits a callback that reconnect named, reaches the page", async () => {
    const { channel, cut } = page(wire)
    const clock = register<string, string>()
    const received: string[] = []
    channel<string, string>(clock.id).listen((message) => void received.push(message))
    await vi.advanceTimersByTimeAsync(200)
    channel() // a callback whose call is lost in the cut
    cut()
    await vi.advanceTimersByTimeAsync(1_000) // the reconnect names the clock and the callback
    cut()
    await vi.advanceTimersByTimeAsync(10) // the server notices the drop
    void clock.send('after the wire dropped', { ack: false })
    await vi.advanceTimersByTimeAsync(3_000)
    expect(received).toEqual(['after the wire dropped'])
  })

  test('a message the page queues in the tick it registers a channel goes to the server once', async () => {
    const { channel, traffic } = page(wire)
    const clock = register<string, string>()
    const received: string[] = []
    clock.listen((message) => void received.push(message))
    const pageClock = channel<string, string>(clock.id)
    await vi.advanceTimersByTimeAsync(500)
    const texts = () => traffic.toServer.filter((tag) => tag === TAG.TEXT).length
    const before = texts()
    channel(register().id)
    void pageClock.send('queued', { ack: false }) // goes behind the registration's RECONCILE
    await vi.advanceTimersByTimeAsync(1_000)
    expect(received).toEqual(['queued'])
    expect(texts() - before).toBe(1)
  })
})

describe.each(['sse', 'sse-batch'] as const)('over %s', (wire) => {
  test('a page moves to a WebSocket while the server awaits a callback, and the abort it holds reaches the callback there', async () => {
    const { channel } = page(wire, { upgrade: true })
    const server = register<string, string>()
    const open = channel<string, string>(server.id)
    const received: string[] = []
    open.listen((message) => void received.push(message))
    const callbackId = crypto.randomUUID()
    channel(callbackId).abort()
    await vi.advanceTimersByTimeAsync(1_000)
    const connection = (open as any)._connection
    expect(connection.transport.type).toBe('ws')
    const callback = register(callbackId)
    let closedWith: unknown = 'open'
    callback.onClose((err) => void (closedWith = err))
    await vi.advanceTimersByTimeAsync(200)
    expect(isAbort(closedWith)).toBe(true)
    void server.send('over the WebSocket', { ack: false })
    await vi.advanceTimersByTimeAsync(200)
    expect(received).toEqual(['over the WebSocket'])
  })

  test("a search box whose aborted calls' callbacks never register doesn't keep the page off the WebSocket", async () => {
    const { channel } = page(wire, { upgrade: true })
    const connection = (channel(register().id) as any)._connection
    // A keystroke a second, each aborting its call before its request leaves.
    for (let keystroke = 0; keystroke < 8; keystroke++) {
      channel().abort()
      await vi.advanceTimersByTimeAsync(1_000)
    }
    expect(connection.transport.type).toBe('ws')
  })

  test("a callback the page aborts while its upgrade waits for the old wire's FIN gets the abort after the handoff, not a NetworkError", async () => {
    // The WebSocket commits the upgrade, and the old wire's last frame takes 300 ms.
    const { channel } = page(wire, { upgrade: true, delays: { [TAG.READY]: 1_000, [TAG.FIN]: 300 } })
    const connection = (channel(register().id) as any)._connection
    const committed = () => connection.state.upgrade?.tag === 'committing' && connection.state.upgrade.committed
    for (let waited = 0; waited < 3_000 && !committed(); waited += 5) await vi.advanceTimersByTimeAsync(5)
    expect(committed()).toBe(true)
    const callback = register()
    let closedWith: unknown = 'open'
    callback.onClose((err) => void (closedWith = err))
    channel(callback.id).abort() // its call's request had left
    await vi.advanceTimersByTimeAsync(2_000)
    expect(connection.transport.type).toBe('ws')
    expect(closedWith).not.toBe('open')
    expect(closedWith).not.toBeInstanceOf(NetworkError)
  })
})

describe.each(['sse', 'sse-batch'] as const)('over %s, while an upgrade attempt waits for its READY', (wire) => {
  test('a channel the server returns opens at once, what the page sends goes out, and the page still moves to a WebSocket', async () => {
    const { channel } = page(wire, { upgrade: true, delays: { [TAG.READY]: 6_000 } })
    const clock = register<string, string>()
    const received: string[] = []
    clock.listen((message) => void received.push(message))
    const pageClock = channel<string, string>(clock.id)
    await vi.advanceTimersByTimeAsync(500)
    const connection = (pageClock as any)._connection
    expect(connection.state.upgrade.tag).toBe('staging')
    const returned = register<string, string>()
    const closedWith: unknown[] = []
    returned.onClose((err) => void closedWith.push(err))
    const pageReturned = channel<string, string>(returned.id)
    pageReturned.onClose((err) => void closedWith.push(err))
    const messages: string[] = []
    pageReturned.listen((message) => void messages.push(message))
    const createdAt = Date.now()
    let openedAfter = -1
    pageReturned.onOpen(() => void (openedAfter = Date.now() - createdAt))
    void pageClock.send('tick', { ack: false })
    await vi.advanceTimersByTimeAsync(500)
    expect(openedAfter).toBeGreaterThanOrEqual(0)
    expect(openedAfter).toBeLessThan(500)
    expect(received).toEqual(['tick'])
    await vi.advanceTimersByTimeAsync(9_000)
    expect(closedWith).toEqual([])
    expect(connection.transport.type).toBe('ws')
    void returned.send('over the WebSocket', { ack: false })
    await vi.advanceTimersByTimeAsync(200)
    expect(messages).toEqual(['over the WebSocket'])
  })

  test('a search box registering a callback on every keystroke while the attempt stages opens each before the next keystroke, and the page still moves to a WebSocket', async () => {
    const { channel } = page(wire, { upgrade: true, delays: { [TAG.READY]: 3_000 } })
    const connection = (channel(register().id) as any)._connection
    await vi.advanceTimersByTimeAsync(500)
    expect(connection.state.upgrade.tag).toBe('staging')
    const { openedAfter, networkErrors } = await searchBox(channel, 27, 90)
    expect(connection.state.upgrade.tag).toBe('staging')
    expect(openedAfter).toHaveLength(27)
    expect(Math.max(...openedAfter)).toBeLessThan(90)
    await vi.advanceTimersByTimeAsync(2_000)
    expect(networkErrors).toEqual([])
    expect(connection.transport.type).toBe('ws')
  })

  test('a search box typing faster than a RECONCILE goes round, through READY and the handoff, gets no NetworkError, and the page still moves to a WebSocket', async () => {
    const { channel } = page(wire, { upgrade: true, delays: { [TAG.READY]: 3_000, [TAG.RECONCILED]: 100 } })
    const connection = (channel(register().id) as any)._connection
    await vi.advanceTimersByTimeAsync(500)
    const { openedAfter, networkErrors } = await searchBox(channel, 200, 60)
    await vi.advanceTimersByTimeAsync(1_000)
    expect(networkErrors).toEqual([])
    expect(openedAfter.length).toBeGreaterThan(0)
    expect(Math.max(...openedAfter)).toBeLessThan(500)
    expect(connection.transport.type).toBe('ws')
  })
})

test('on SSE batch POSTs, what the page holds for room as an upgrade attempt ends without its barrier, its WebSocket lost while the outbox drained, goes out at once, not with the next send or attempt', async () => {
  // Each POST takes 100 ms, so the barrier waits for the outbox to drain.
  const { channel, cut } = page('sse-batch', { upgrade: true, delays: { [TAG.READY]: 1_000 }, postMs: 100 })
  const server = register<string, never>()
  const received: number[] = []
  server.listen((message) => void received.push(Number(message.slice(0, 8))))
  const open = channel<string, never>(server.id)
  const connection = (open as any)._connection
  await vi.advanceTimersByTimeAsync(900)
  // More than a POST takes, none awaited.
  const count = 64
  for (let n = 0; n < count; n++) void open.send(String(n).padEnd(64 * 1024), { ack: false })
  for (let waited = 0; waited < 2_000 && connection.state.upgrade.tag !== 'committing'; waited++) {
    await vi.advanceTimersByTimeAsync(1)
  }
  expect(connection.state.upgrade.tag).toBe('committing')
  expect(connection.heldForRoom.length).toBeGreaterThan(0)
  const { attempt } = connection.state.upgrade
  cut() // the probe WebSocket
  for (let waited = 0; waited < 3_000 && connection.state.upgrade.attempt === attempt; waited++) {
    await vi.advanceTimersByTimeAsync(1)
  }
  expect(connection.state.upgrade.attempt).not.toBe(attempt)
  // The next attempt's READY takes a second.
  await vi.advanceTimersByTimeAsync(500)
  expect(received).toEqual(Array.from({ length: count }, (_, n) => n))
  expect(connection.transport.type).toBe('sse')
})

test.each([
  ['before the server read it', 'unread'],
  ['after the server read it', 'read'],
] as const)(
  'what a registration carries behind its RECONCILE on SSE batch POSTs arrives when that POST fails %s',
  async (_when, after) => {
    const { channel, failNextReconcilePost, wires } = page('sse-batch')
    const clock = register<string, string>()
    const received: string[] = []
    clock.listen((message) => void received.push(message))
    const pageClock = channel<string, string>(clock.id)
    await vi.advanceTimersByTimeAsync(500)
    failNextReconcilePost(after)
    channel(register().id)
    void pageClock.send('carried', { ack: false }) // queued behind the registration's RECONCILE
    await vi.advanceTimersByTimeAsync(3_000)
    expect(wires()).toBe(2) // the failed POST ended its wire
    expect(received).toEqual(['carried'])
  },
)

test('on SSE batch POSTs, a callback whose call registers it while the POST carrying its RECONCILE is still being read opens, its attach result ahead of that RECONCILED', async () => {
  const { channel, traffic, holdNextReconcilePost } = page('sse-batch')
  channel(register().id)
  await vi.advanceTimersByTimeAsync(500)
  holdNextReconcilePost(200)
  const callbackId = crypto.randomUUID()
  const callback = channel(callbackId)
  let opened = false
  callback.onOpen(() => (opened = true))
  await vi.advanceTimersByTimeAsync(100) // the server has read the RECONCILE, and awaits the callback
  register(callbackId)
  await vi.advanceTimersByTimeAsync(1_000)
  expect(opened).toBe(true)
  const results = traffic.toPage.filter((tag) => tag === TAG.ATTACH_RESULT || tag === TAG.RECONCILED)
  expect(results.slice(-2)).toEqual([TAG.ATTACH_RESULT, TAG.RECONCILED])
})

test('a message queued with a registration, lost with the upload request that carried that RECONCILE, reaches the server on the next wire', async () => {
  const { channel, wires, endUploadAfterNextReconcile } = page('sse')
  const clock = register<string, string>()
  const received: string[] = []
  clock.listen((message) => void received.push(message))
  const pageClock = channel<string, string>(clock.id)
  await vi.advanceTimersByTimeAsync(500)
  endUploadAfterNextReconcile()
  channel(register().id)
  void pageClock.send('queued', { ack: false })
  await vi.advanceTimersByTimeAsync(3_000)
  expect(wires()).toBe(2)
  expect(received).toEqual(['queued'])
})

test('a message a follow-up RECONCILE carried into an upload request the server refuses late reaches the server once', async () => {
  const { channel, traffic } = page('sse-batch', { refusedAfter: 300, delays: { [TAG.RECONCILED]: 100 } })
  channel(register().id)
  await vi.advanceTimersByTimeAsync(50) // the first RECONCILE is out, its RECONCILED on its way
  const late = register<string, string>()
  const received: string[] = []
  late.listen((message) => void received.push(message))
  void channel<string, string>(late.id).send('carried', { ack: false })
  await vi.advanceTimersByTimeAsync(2_000)
  expect(received).toEqual(['carried'])
  expect(traffic.toServer.filter((tag) => tag === TAG.TEXT)).toHaveLength(1)
})

test('a message queued with a registration while an upgrade attempt stages goes to the server once, and the page still moves to a WebSocket', async () => {
  const { channel, traffic } = page('sse', { upgrade: true, delays: { [TAG.READY]: 2_000 } })
  const clock = register<string, string>()
  const received: string[] = []
  clock.listen((message) => void received.push(message))
  const pageClock = channel<string, string>(clock.id)
  await vi.advanceTimersByTimeAsync(500)
  const connection = (pageClock as any)._connection
  expect(connection.state.upgrade.tag).toBe('staging')
  channel(register().id)
  void pageClock.send('queued', { ack: false })
  await vi.advanceTimersByTimeAsync(3_000)
  expect(connection.transport.type).toBe('ws')
  expect(received).toEqual(['queued'])
  expect(traffic.toServer.filter((tag) => tag === TAG.TEXT)).toHaveLength(1)
})

describe('with every channel registered, a page sends each message once, and no frame it did not before', () => {
  const tags = (list: number[]) => list.filter((tag) => tag !== TAG.PING && tag !== TAG.PONG)
  // A returned channel the page writes to at once, the server's reply, then a second returned channel.
  async function run(wire: Wire) {
    const { channel, traffic } = page(wire)
    const first = register<string, string>()
    const received: string[] = []
    first.listen((message) => void received.push(message))
    const pageFirst = channel<string, string>(first.id)
    void pageFirst.send('hi', { ack: false })
    await vi.advanceTimersByTimeAsync(200)
    void first.send('hello', { ack: false })
    const second = register()
    channel(second.id)
    await vi.advanceTimersByTimeAsync(1_000)
    expect(received).toEqual(['hi'])
    return { requests: traffic.requests, toServer: tags(traffic.toServer), toPage: tags(traffic.toPage) }
  }
  test.each(WIRES)('over %s', async (wire) => {
    expect(await run(wire)).toEqual(EXPECTED_TRAFFIC[wire])
  })
})

const { RECONCILE, RECONCILED, TEXT, WINDOW, BDP_PING, BDP_PING_ACK, STREAM_REQUEST_OPEN_ACK } = TAG
/** Every frame each wire carries for two returned channels and one message each way, PING and PONG aside: no frame
 *  or request more than needed. */
const EXPECTED_TRAFFIC: Record<Wire, Traffic> = {
  sse: {
    requests: 2,
    toServer: [RECONCILE, TEXT, BDP_PING_ACK, RECONCILE, BDP_PING],
    toPage: [STREAM_REQUEST_OPEN_ACK, BDP_PING, RECONCILED, WINDOW, TEXT, RECONCILED, BDP_PING_ACK],
  },
  'sse-batch': {
    requests: 5,
    toServer: [RECONCILE, TEXT, BDP_PING_ACK, WINDOW, RECONCILE, BDP_PING, WINDOW],
    toPage: [BDP_PING, RECONCILED, WINDOW, TEXT, BDP_PING_ACK, RECONCILED],
  },
  ws: {
    requests: 1,
    toServer: [RECONCILE, TEXT, BDP_PING_ACK, RECONCILE, BDP_PING],
    toPage: [RECONCILED, BDP_PING, TEXT, RECONCILED, BDP_PING_ACK],
  },
}
