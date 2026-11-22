// A frame the server can't take ends only its own channel: on the real Node server and the real page, over SSE, a
// request that carries data of two channels and one bad frame of the first. The first ends on both sides with the
// server's reason; the second keeps its session, and each of its messages arrives once (decisions/516-sse-carrier.md,
// the ruling on step 6's check, R1.1). The bad frame stands for a page that breaks the protocol: a fetch wrapper puts
// it into the page's request, after the first channel's text.

import { afterAll, beforeAll, expect, test } from 'vitest'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { WIRE_MAX_RAW_FRAME_BYTES } from '../constants.js'
import { decodeU32, encodeU32 } from '../wire/envelope.js'
import { ERROR_REASON, decode, encode } from '../wire/frame.js'
import { SEQ_OFFSET, readU16, readU32 } from '../wire/header.js'
import { TAG } from '../wire/tags.js'
import { ClientChannel } from './client/ClientChannel.js'
import { getChannelMux } from './server/ChannelMux.js'
import { ServerChannel, ATTACHABLE_NOW } from './server/ServerChannel.js'

serverConfig.channel = { transports: ['sse'] }

type Bad = 'error' | 'over the limit'
/** What the wrapper puts into the next request that carries `target`'s text, after that text's frame. */
const inject: { bad: Bad | null; target: string; upload: boolean } = { bad: null, target: '', upload: false }

/** The bad frame of the channel at `ix`, whose text went at `seq`, as length-prefixed bytes. */
function badFrame(bad: Bad, ix: number, seq: number): Uint8Array<ArrayBuffer>[] {
  // A page sends no ERROR but a lost one.
  if (bad === 'error') {
    const frame = encode.error(ix, ERROR_REASON.UNCONFIRMED, seq + 1)
    return [encodeU32(frame.byteLength), frame]
  }
  // The length says more than the limit; only the header follows.
  return [encodeU32(WIRE_MAX_RAW_FRAME_BYTES + 1), encode.close(ix, 0, seq + 1).subarray(0, 7)]
}

/** `frame`'s channel and seq, where it carries `text`. */
function textOf(frame: Uint8Array<ArrayBuffer>, text: string): { ix: number; seq: number } | null {
  if (frame[0] !== TAG.TEXT) return null
  const decoded = decode(frame)
  if (decoded.tag !== TAG.TEXT || decoded.text !== JSON.stringify(text)) return null
  return { ix: readU16(frame, 1), seq: readU32(frame, SEQ_OFFSET) }
}

/** A batch POST's body, the bad frame put in after the target's text. */
function withBadFrame(body: Uint8Array<ArrayBuffer>, bad: Bad): Uint8Array<ArrayBuffer>[] | null {
  let at = 4 + decodeU32(body.subarray(0, 4))
  while (at + 4 <= body.length) {
    const end = at + 4 + decodeU32(body.subarray(at, at + 4))
    const target = textOf(body.subarray(at + 4, end), inject.target)
    if (target !== null) return [body.subarray(0, end), ...badFrame(bad, target.ix, target.seq), body.subarray(end)]
    at = end
  }
  return null
}

/** An upload's chunks, the bad frame put in after the target's text: a frame is a chunk, after its length's. */
function uploadWithBadFrame(body: ReadableStream<Uint8Array>): ReadableStream<Uint8Array> {
  return body.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      transform: (chunk, controller) => {
        controller.enqueue(chunk)
        const target = inject.bad === null ? null : textOf(new Uint8Array(chunk), inject.target)
        if (target === null || inject.bad === null) return
        for (const part of badFrame(inject.bad, target.ix, target.seq)) controller.enqueue(part)
        inject.bad = null
      },
    }),
  )
}

const realFetch = globalThis.fetch
async function injectingFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const body = init?.body
  // The upload is taken only where the row asks for it.
  if (body instanceof ReadableStream && !inject.upload) throw new TypeError('Failed to fetch')
  if (body instanceof ReadableStream) return realFetch(input, { ...init, body: uploadWithBadFrame(body) })
  if (inject.bad !== null && body instanceof Blob) {
    const parts = withBadFrame(new Uint8Array(await body.arrayBuffer()), inject.bad)
    if (parts !== null) {
      inject.bad = null
      return realFetch(input, { ...init, body: new Blob(parts) })
    }
  }
  return realFetch(input, init)
}

let server: Server
let telefuncUrl: string

beforeAll(async () => {
  globalThis.fetch = injectingFetch
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
})
afterAll(() => {
  globalThis.fetch = realFetch
  server.closeAllConnections()
  server.close()
})

/** Resolves once `done()` holds, checked every 10 ms; rejects after `ms`. */
async function until(done: () => boolean, ms = 3_500): Promise<void> {
  const deadline = performance.now() + ms
  while (!done()) {
    if (performance.now() > deadline) throw new Error('timed out waiting')
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}

/** A server channel and the page's end of it, on the page's connection `connectionKey`. */
function pair(connectionKey: string) {
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel, ATTACHABLE_NOW)
  const got: string[] = []
  channel.listen((message) => void got.push(message))
  const serverEnd: Error[] = []
  channel.onClose((err) => void serverEnd.push(err ?? new Error('closed with no error')))
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl, transports: ['sse'], idleTimeout: 0, connectionKey },
  })
  const pageEnd: Error[] = []
  page.onClose((err) => void pageEnd.push(err ?? new Error('closed with no error')))
  return { channel, page, got, serverEnd, pageEnd }
}

const told = {
  error: "Channel closed: the server refused what the page sent on it: The server can't take a frame of the page's.",
  'over the limit':
    "Channel closed: the server refused what the page sent on it: A frame is over the server's frame limit.",
}

test.each([
  ['a batch POST', 'error', false],
  ['a batch POST', 'over the limit', false],
  ['the streaming upload', 'error', true],
  ['the streaming upload', 'over the limit', true],
] as const)(
  '%s with a frame of one channel the server refuses (%s): that channel ends on both sides with the reason, the other keeps its session and each of its messages arrives once',
  async (_path, bad, upload) => {
    const connectionKey = crypto.randomUUID()
    const refused = pair(connectionKey)
    const other = pair(connectionKey)
    await Promise.all([refused.page.opened, other.page.opened])
    Object.assign(inject, { bad, target: 'a1', upload })
    void other.page.send('b1').catch(() => {})
    void refused.page.send('a1').catch(() => {})
    await until(() => refused.pageEnd.length > 0 && other.got.includes('b1'))
    void other.page.send('b2').catch(() => {})
    await until(() => other.got.includes('b2'))
    for (const end of [refused.pageEnd, refused.serverEnd]) {
      expect(end.map((err) => [err.name, err.message])).toEqual([['ChannelClosedError', told[bad]]])
    }
    expect([other.pageEnd, other.serverEnd]).toEqual([[], []])
    expect(other.got).toEqual(['b1', 'b2'])
  },
)
