// What a page's connection makes the server hold that none of its channels holds (the answers to its PINGs, the copies
// its event stream makes of what the server sends) is held within `config.channel.maxMemory`, with what the channels
// hold, over any number of connections. A real Node server over SSE; each page reads its event stream's first chunk and
// nothing more, so what the server sends it stays in the server's userland once the kernel's buffers are full.

import { afterAll, beforeAll, expect, test } from 'vitest'
import { createServer, request, type Server, type ServerResponse } from 'node:http'
import { once } from 'node:events'
import { Telefunc } from './node.js'
import { config, getChannelConfig } from '../node/server/serverConfig.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import { CHANNEL_TRANSPORT, MAX_CHANNELS_PER_CONNECTION, WIRE_VERSION } from '../wire-protocol/constants.js'
import { encodeSseRequest } from '../wire-protocol/wire/envelope.js'
import { encode, withTook } from '../wire-protocol/wire/frame.js'
import { pingDeadline } from '../wire-protocol/wire/pingDeadline.js'
import { getChannelMux } from '../wire-protocol/channel/server/ChannelMux.js'
import { ATTACHABLE_NOW, ServerChannel } from '../wire-protocol/channel/server/ServerChannel.js'

const PING_INTERVAL = 1_000
config.channel = { transports: ['sse'], maxMemory: '16m', pingInterval: PING_INTERVAL }
const telefunc = new Telefunc()
/** What the ledger holds, for every holder together. */
const ledger = getGlobalObject<{ total: number }>('channelMemory.ts', { total: 0 })

/** Each page's event stream, as the server answers it, by the row id its request carries. */
const streams = new Map<string, (res: ServerResponse) => void>()
let server: Server
let base: string
beforeAll(async () => {
  server = createServer((req, res) => {
    const row = req.headers['x-row']
    if (typeof row === 'string') streams.get(row)?.(res)
    telefunc.serve({ req, res }).catch(() => {})
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  base = `http://127.0.0.1:${address.port}/_telefunc?wire=${WIRE_VERSION}`
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
})

/** The event loop's queued work, run to its end: `turns` macrotask turns, each after its microtasks and I/O. */
async function settle(turns = 100) {
  for (let i = 0; i < turns; i++) await new Promise((resolve) => setImmediate(resolve))
}

/** POSTs `body` as a page's SSE request; resolves with its answer once its first chunk arrived, which it then reads no
 *  further. */
async function post(body: Blob, headers: Record<string, string> = {}) {
  const bytes = Buffer.from(await body.arrayBuffer())
  return new Promise<{ status: number }>((resolve, reject) => {
    const req = request(base, { method: 'POST', headers: { 'x-telefunc-request': 'sse', ...headers } }, (res) => {
      res.once('data', () => {
        res.pause()
        resolve({ status: res.statusCode ?? 0 })
      })
      res.once('end', () => resolve({ status: res.statusCode ?? 0 }))
    })
    req.on('error', reject)
    req.end(bytes)
  })
}

/** A page whose session holds one channel, at index 0, and whose event stream it stops reading at its first chunk. */
async function openPage() {
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
  const ends: string[] = []
  channel.onClose((err) => void ends.push(err ? err.constructor.name : 'clean'))
  const connId = crypto.randomUUID()
  const row = crypto.randomUUID()
  const stream = new Promise<ServerResponse>((resolve) => streams.set(row, resolve))
  const reconcile = encode.reconcile({
    sessionId: crypto.randomUUID(),
    attempt: 1,
    open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }],
  })
  await post(encodeSseRequest({ connId, streamResponse: true }, [reconcile]), { 'x-row': row })
  const res = await stream
  return {
    channel,
    ends,
    /** What the server handed the kernel of this page's event stream, or every byte it pushed once it let go of it. */
    written: (pushed: number) => (res.destroyed ? pushed : (res.socket?.bytesWritten ?? pushed)),
    destroyed: () => res.destroyed,
    /** Ten PINGs that list as many ended channels as a page may have: each PONG answers each. */
    pings: () => post(encodeSseRequest({ connId }, Array.from({ length: 10 }, widePing))),
  }
}

const ended = Array.from({ length: MAX_CHANNELS_PER_CONNECTION }, (_, i) => ({ ix: i + 1, lastSeq: 0 }))
const widePing = () => encode.ping(ended)

/** What an event carrying a frame of `bytes` takes: `data: `, the frame in unpadded base64url, and a blank line. */
const eventBytes = (bytes: number) => 6 + Math.ceil((bytes * 4) / 3) + 2
const PONG_EVENT_BYTES = eventBytes(withTook(encode.pong(ended.map(({ ix }) => ({ ix, lastSeq: null }))), 0).byteLength)

test.each([1, 4])(
  '%i pages that flood PINGs and read nothing hold no more than `maxMemory` together, and each that passes its share ends its channels with `ChannelOverflowError`',
  async (count) => {
    const maxMemory = getChannelConfig().maxMemory
    const opened = await Promise.all(Array.from({ length: count }, openPage))
    /** Each page, with the PONGs the server was asked for and whether its POSTs are still answered 200. */
    const pages = opened.map((page) => ({ ...page, pongs: 0, answered: true }))
    let peak = 0
    for (let round = 0; round < 300 && pages.some(({ answered }) => answered); round++) {
      for (const page of pages) {
        if (!page.answered) continue
        const { status } = await page.pings()
        page.pongs += 10
        page.answered = status === 200
      }
      await settle(10)
      let held = 0
      for (const page of pages) {
        const pushed = page.pongs * PONG_EVENT_BYTES
        held += pushed - page.written(pushed)
      }
      peak = Math.max(peak, held)
    }
    await settle()
    expect({ answered: pages.filter(({ answered }) => answered).length, ends: pages.map(({ ends }) => ends) }).toEqual({
      answered: 0,
      ends: pages.map(() => ['ChannelOverflowError']),
    })
    expect(peak).toBeLessThanOrEqual(maxMemory)
    // What their event streams held stays in the ledger till each stream lets go of it, a ping deadline after its end.
    await new Promise((resolve) => setTimeout(resolve, pingDeadline(PING_INTERVAL, CHANNEL_TRANSPORT.SSE) + 500))
    expect({ ledger: ledger.total, destroyed: pages.map((page) => page.destroyed()) }).toEqual({
      ledger: 0,
      destroyed: pages.map(() => true),
    })
  },
  60_000,
)

test("a page that reads nothing: the copies its event stream holds of the server's sends are held in `maxMemory`", async () => {
  const page = await openPage()
  const text = 'x'.repeat(15 * 1024)
  // Within the page's credit, so each goes to its event stream at once.
  const count = 340
  for (let i = 0; i < count; i++) page.channel.send(text).catch(() => {})
  await settle()
  const pushed = count * eventBytes(text.length)
  const copies = pushed - page.written(pushed)
  expect(copies).toBeGreaterThan(2 ** 20)
  expect(ledger.total - page.channel.heldBytes).toBeGreaterThanOrEqual(copies)
}, 30_000)
