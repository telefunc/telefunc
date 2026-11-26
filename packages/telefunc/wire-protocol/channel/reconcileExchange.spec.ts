// A page's RECONCILE on a WebSocket, over a real server: what the server answers whichever frame completes it, what
// it does with a message holding two, with a reconcile that throws, and how a large join's upload goes.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { defined } from '../../utils/defined.fixture.js'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { WIRE_VERSION, WS_CLOSE_REFUSED } from '../constants.js'
import { decode, encode, isOneFrame, messageFrames } from '../wire/frame.js'
import type { ReconciledPayload } from '../wire/payloads.js'
import { TAG } from '../wire/tags.js'
import { ATTACHABLE_NOW, ServerChannel } from './server/ServerChannel.js'
import { ChannelMux, getChannelMux } from './server/ChannelMux.js'
import { SessionRegistry } from './server/SessionRegistry.js'
import { ClientChannel } from './client/ClientChannel.js'
import { WireLifecycle } from './client/WireLifecycle.js'
import { WsTransport } from './client/WsTransport.js'

serverConfig.channel = { transports: ['ws'] }

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

/** A page's WebSocket that sends what a row hands it: the tags of the frames it got, whole ones put together. */
async function rawPage() {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/_telefunc?wire=${WIRE_VERSION}`)
  ws.binaryType = 'arraybuffer'
  const tags: number[] = []
  const reconciled: ReconciledPayload[] = []
  const closed = Promise.withResolvers<number>()
  let took = 0
  ws.addEventListener('close', ({ code }) => closed.resolve(code))
  ws.addEventListener('message', ({ data }) => {
    if (!(data instanceof ArrayBuffer)) throw new Error('Expected a binary message')
    const message = new Uint8Array(data)
    took += message.byteLength
    for (const raw of isOneFrame(message) ? [message] : messageFrames(message)) {
      const frame = decode(raw)
      tags.push(frame.tag)
      if (frame.tag === TAG.RECONCILED) reconciled.push(frame.payload)
    }
  })
  await new Promise((resolve) => ws.addEventListener('open', resolve))
  return { ws, tags, reconciled, closed: closed.promise, took: () => took }
}

/** A server channel of its own id. */
function serverChannel() {
  const channel = new ServerChannel<string, string>({ id: crypto.randomUUID() })
  getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
  return channel
}

/** Resolves once `check` holds; a stuck row ends at the harness's timeout. */
async function until(check: () => boolean) {
  while (!check()) await new Promise((resolve) => setTimeout(resolve, 5))
}

test("a join's RECONCILE whose last piece rides behind a PIECES_ACK is taken and answered", async () => {
  const a = serverChannel()
  const b = serverChannel()
  const page = await rawPage()
  page.ws.send(
    encode.reconcile({
      sessionId: crypto.randomUUID(),
      attempt: 1,
      open: [{ id: a.id, ix: 0, lastSeq: 0, initial: true }],
    }),
  )
  await until(() => page.reconciled.length === 1)
  const join = encode.reconcile({
    sessionId: defined(page.reconciled[0], 'the first RECONCILED').sessionId,
    open: [{ id: b.id, ix: 1, lastSeq: 0, initial: true }],
    released: [],
  })
  const half = Math.ceil(join.byteLength / 2)
  page.ws.send(encode.piece(join.byteLength, join.subarray(0, half)))
  const ack = encode.piecesAck(page.took(), 0, 0)
  const last = encode.piece(join.byteLength, join.subarray(half))
  page.ws.send(encode.message([ack, last], ack.byteLength + last.byteLength))
  await until(() => page.reconciled.length === 2)
  expect(defined(page.reconciled[1], "the join's RECONCILED").open).toEqual([{ ix: 1, lastSeq: 0 }])
  // Each RECONCILE's receipt came ahead of its RECONCILED.
  expect(page.tags.filter((tag) => tag === TAG.RECONCILE_TAKEN || tag === TAG.RECONCILED)).toEqual([
    TAG.RECONCILE_TAKEN,
    TAG.RECONCILED,
    TAG.RECONCILE_TAKEN,
    TAG.RECONCILED,
  ])
  page.ws.close()
})

test('a message that completes two RECONCILEs ends its wire as refused, and neither attaches a channel', async () => {
  const a = serverChannel()
  const b = serverChannel()
  const c = serverChannel()
  const attached = vi.spyOn(ServerChannel.prototype, '_attachPeer')
  try {
    const page = await rawPage()
    page.ws.send(
      encode.reconcile({
        sessionId: crypto.randomUUID(),
        attempt: 1,
        open: [{ id: a.id, ix: 0, lastSeq: 0, initial: true }],
      }),
    )
    await until(() => page.reconciled.length === 1)
    attached.mockClear()
    const join = encode.reconcile({
      sessionId: defined(page.reconciled[0], 'the first RECONCILED').sessionId,
      open: [{ id: b.id, ix: 1, lastSeq: 0, initial: true }],
      released: [],
    })
    const half = Math.ceil(join.byteLength / 2)
    page.ws.send(encode.piece(join.byteLength, join.subarray(0, half)))
    const last = encode.piece(join.byteLength, join.subarray(half))
    const other = encode.reconcile({
      sessionId: crypto.randomUUID(),
      attempt: 1,
      open: [{ id: c.id, ix: 0, lastSeq: 0, initial: true }],
    })
    // The first frame of the two needs a LENGTH ahead of it.
    page.ws.send(encode.message([last, other], 7 + last.byteLength + other.byteLength))
    expect(await page.closed).toBe(WS_CLOSE_REFUSED)
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(attached).not.toHaveBeenCalled()
    expect(page.reconciled).toHaveLength(1)
  } finally {
    attached.mockRestore()
  }
})

test("a reconcile that throws ends its wire as lost, before the page's deadline, and the page resumes on the next with what it sent", async () => {
  const first = serverChannel()
  const second = serverChannel()
  const handOver = vi.spyOn(SessionRegistry.prototype, 'handOver')
  const bug = vi.spyOn(console, 'error').mockImplementation(() => {})
  const opened = vi.spyOn(ChannelMux.prototype, 'onWireOpen')
  const closed = vi.spyOn(ChannelMux.prototype, 'onWireClosed')
  const late = vi.spyOn(WireLifecycle.prototype, 'onReconcileLate')
  const connection = {
    telefuncUrl: `http://127.0.0.1:${port}/_telefunc`,
    transports: ['ws' as const],
    connectionKey: crypto.randomUUID(),
  }
  const pages: ClientChannel<string, string>[] = []
  try {
    const firstPage = new ClientChannel<string, string>({ channelId: first.id, connection })
    pages.push(firstPage)
    await Promise.all([first.opened, firstPage.opened])
    const received = Promise.withResolvers<string>()
    first.listen((message) => void received.resolve(message))
    // The join's reconcile throws once.
    handOver.mockImplementationOnce(() => {
      throw new Error('a bug in the reconcile')
    })
    const secondPage = new ClientChannel<string, string>({ channelId: second.id, connection })
    pages.push(secondPage)
    void firstPage.send('during')
    await Promise.all([second.opened, secondPage.opened])
    expect(await received.promise).toBe('during')
    // This page's wires: an earlier row's may close meanwhile.
    const wires = new Set(opened.mock.calls.map(([wire]) => wire))
    // One wire ended, as lost; its transport and its socket's close each report it.
    const ends = closed.mock.calls.filter(([wire]) => wires.has(wire))
    expect(new Set(ends.map(([wire]) => wire)).size).toBe(1)
    expect(ends.every(([, reason]) => reason === 'transient')).toBe(true)
    expect(late).not.toHaveBeenCalled()
  } finally {
    for (const page of pages) page.abort()
    // Its wire closes before the next test counts what the server sends.
    await Promise.all([...pages.map((page) => page.closed), first.closed, second.closed])
    handOver.mockRestore()
    bug.mockRestore()
    opened.mockRestore()
    closed.mockRestore()
    late.mockRestore()
  }
})

test("a join of 1,000 channels with 256-byte ids, about 300 kB, goes on the server's PIECES_ACKs alone, with no PING or PONG, to its receipt and its RECONCILED", async () => {
  const lead = serverChannel()
  const rest = Array.from({ length: 1_000 }, (_, ix) => {
    const channel = new ServerChannel({ id: `${crypto.randomUUID()}-${ix}`.padEnd(256, 'x') })
    getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
    return channel
  })
  const connection = {
    telefuncUrl: `http://127.0.0.1:${port}/_telefunc`,
    transports: ['ws' as const],
    connectionKey: crypto.randomUUID(),
  }
  const pings = vi.spyOn(WsTransport.prototype, 'sendPing').mockImplementation(() => {})
  const pongs = vi.spyOn(encode, 'pong')
  const pages: ClientChannel<string, string>[] = []
  try {
    const leadPage = new ClientChannel<string, string>({ channelId: lead.id, connection })
    pages.push(leadPage)
    await Promise.all([lead.opened, leadPage.opened])
    // Only the PONGs of this join count.
    pongs.mockClear()
    for (const channel of rest) pages.push(new ClientChannel<string, string>({ channelId: channel.id, connection }))
    await Promise.all([...rest.map((channel) => channel.opened), ...pages.map((page) => page.opened)])
    expect(pongs).not.toHaveBeenCalled()
  } finally {
    for (const page of pages) page.abort()
    await Promise.all(pages.map((page) => page.closed))
    pings.mockRestore()
    pongs.mockRestore()
  }
}, 30_000)

test('a 10 MB download with a join in it: the server takes each report of what the page took, the download arrives whole, and the join opens', async () => {
  const lead = serverChannel()
  const joined = serverChannel()
  const opened = vi.spyOn(ChannelMux.prototype, 'onWireOpen')
  const closed = vi.spyOn(ChannelMux.prototype, 'onWireClosed')
  const connection = {
    telefuncUrl: `http://127.0.0.1:${port}/_telefunc`,
    transports: ['ws' as const],
    connectionKey: crypto.randomUUID(),
  }
  const pages: ClientChannel<string, string>[] = []
  try {
    const leadPage = new ClientChannel<string, string>({ channelId: lead.id, connection })
    pages.push(leadPage)
    await Promise.all([lead.opened, leadPage.opened])
    let took = 0
    const all = Promise.withResolvers<void>()
    leadPage.listenBinary((data) => {
      took += data.byteLength
      if (took >= 10_000_000) all.resolve()
    })
    const sent = (async () => {
      for (let i = 0; i < 10; i++) await lead.sendBinary(new Uint8Array(1_000_000), { ack: false })
    })()
    const joinedPage = new ClientChannel<string, string>({ channelId: joined.id, connection })
    pages.push(joinedPage)
    await Promise.all([joined.opened, joinedPage.opened, all.promise, sent])
    expect(took).toBe(10_000_000)
    // Its wire never ended: a report past what the server sent would have ended it.
    const wires = new Set(opened.mock.calls.map(([wire]) => wire))
    expect(wires.size).toBe(1)
    expect(closed.mock.calls.filter(([wire]) => wires.has(wire))).toEqual([])
  } finally {
    for (const page of pages) page.abort()
    opened.mockRestore()
    closed.mockRestore()
  }
}, 30_000)
