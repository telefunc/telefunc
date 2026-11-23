// Deno.serve, run without `--unstable-no-legacy-abort`, aborts a request's signal at its response's end, whether or not
// its client left: that abort is not the client leaving. A call that returns a Channel answers while its body is still
// being sent, and Telefunc reads the rest of that body on past the answer. These rows hand `telefunc/deno` such a request
// (Node's server here; its signal aborts once the response is written) and read that a File the call kept reads whole,
// and that the channel's connect window counts from the upload's end. deno.clientLeft.spec.ts runs real Deno. `config.channel` can't change once a channel
// used it, so these rows have a file of their own.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { connect } from 'node:net'
import { once } from 'node:events'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Telefunc } from './deno.js'
import { nodeRequestBody } from './frameworkRequest.fixture.js'
import { config } from '../node/server/serverConfig.js'
import { WIRE_VERSION } from '../wire-protocol/constants.js'
import { Channel } from '../wire-protocol/channel/server/ServerChannel.js'
import { ClientChannel } from '../wire-protocol/channel/client/ClientChannel.js'

const file = '/requestBody.telefunc.js'
config.root = dirname(fileURLToPath(import.meta.url))
config.telefuncFiles = [`${config.root}${file}`]
config.disableNamingConvention = true
const CONNECT_TTL_MS = 1000
config.channel = { transports: ['sse'], connectTtl: CONNECT_TTL_MS }

const encoder = new TextEncoder()
const telefuncPath = `/_telefunc?wire=${WIRE_VERSION}`
const PART = 64 * 1024
let server: Server
let port = 0

/** Deno's error for a read whose client closed its side. */
class BadResource extends Error {}

beforeAll(async () => {
  // The adapter's WebSocket half (crossws) loads only where Deno is, and its body reads fail with Deno's own errors.
  vi.stubGlobal('Deno', { errors: { BadResource, Http: class Http extends Error {} } })
  const telefunc = new Telefunc()
  server = createServer((req, res) => void answer(telefunc, req, res).catch(() => {}))
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  port = address.port
})
afterAll(() => {
  server.closeAllConnections()
  server.close()
  vi.unstubAllGlobals()
})

/** Serves `req` as Deno.serve hands a request over: its signal aborts once the response is written, and a read of its
 *  body that fails, as its connection closes, fails with Deno's `BadResource`, not Node's `aborted`. */
async function answer(telefunc: Telefunc, req: IncomingMessage, res: ServerResponse) {
  const signal = new AbortController()
  const init: RequestInit & { duplex: 'half' } = {
    method: req.method,
    headers: Object.entries(req.headers).flatMap(([name, value]) =>
      value === undefined ? [] : (Array.isArray(value) ? value : [value]).map((one): [string, string] => [name, one]),
    ),
    body: failingAsDeno(nodeRequestBody(req)),
    signal: signal.signal,
    duplex: 'half',
  }
  const response = await telefunc.serve({ request: new Request(`http://127.0.0.1${req.url}`, init), info: {} })
  res.writeHead(response?.status ?? 404, [...(response?.headers ?? [])])
  for await (const chunk of response?.body ?? []) res.write(chunk)
  res.end()
  signal.abort()
}

/** `body`, whose failed read fails with Deno's `BadResource`. */
function failingAsDeno(body: ReadableStream<Uint8Array>): ReadableStream<Uint8Array> {
  const reader = body.getReader()
  return new ReadableStream<Uint8Array>({
    pull: (controller) =>
      reader.read().then(
        ({ done, value }) => (done ? controller.close() : controller.enqueue(value)),
        (err: unknown) => controller.error(new BadResource('connection closed', { cause: err })),
      ),
    cancel: (reason) => reader.cancel(reason),
  })
}

/** The Channels the calls return, made by the spec (`requestBody.telefunc:channel`), and the `Blob`s handed to it. */
function holdChannels() {
  const made: Channel[] = []
  const kept: Blob[] = []
  const onChannel = (holder: { channel?: Channel }) => {
    holder.channel = new Channel()
    made.push(holder.channel)
  }
  const onKept = (blob: Blob) => void kept.push(blob)
  process.on('requestBody.telefunc:channel', onChannel)
  process.on('requestBody.telefunc:kept', onKept)
  return {
    made,
    kept,
    end() {
      process.off('requestBody.telefunc:channel', onChannel)
      process.off('requestBody.telefunc:kept', onKept)
      for (const channel of made) channel.abort()
    },
  }
}

/** A socket that has sent the head of a binary call to `name` with a `Blob` of `parts` × 64 KiB, and the call's
 *  metadata: the Blob's bytes are the row's to send. Resolves once the answer begins. */
async function startCall(name: string, parts: number) {
  const size = parts * PART
  const metadata = encoder.encode(
    JSON.stringify({ file, name, args: [`!TelefuncBlob:${JSON.stringify({ index: 0, size, type: '' })}`] }),
  )
  const start = new Uint8Array(4 + metadata.length)
  new DataView(start.buffer).setUint32(0, metadata.length)
  start.set(metadata, 4)
  const socket = connect(port, '127.0.0.1')
  socket.on('error', () => {})
  await once(socket, 'connect')
  const answered = new Promise<void>((resolve) =>
    socket.once('data', () => {
      socket.resume()
      resolve()
    }),
  )
  socket.write(
    `POST ${telefuncPath} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-telefunc-request: bin\r\nContent-Type: application/octet-stream\r\nContent-Length: ${start.length + size}\r\n\r\n`,
  )
  socket.write(start)
  await answered
  return { socket, size }
}

const part = () => new Uint8Array(PART).fill(120)
const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

test('a call that returns a Channel and keeps its File unread, sent after the answer: the File is read whole once its upload has ended (Telefunc given a request whose signal aborts once its response ends)', async () => {
  const channels = holdChannels()
  const { socket, size } = await startCall('returnsChannelKeepingBlob', 8)
  for (let sent = 0; sent < 8; sent++) socket.write(part())
  const blob = channels.kept[0]
  if (!blob) throw new Error('no Blob')
  const read = await blob.text().then(
    (text) => `read ${text.length} bytes`,
    (err: Error) => err.message,
  )
  channels.end()
  socket.destroy()
  expect(read).toBe(`read ${size} bytes`)
})

test('a call that returns a Channel and leaves its File unread, whose upload takes longer than the connect window: a page that attaches once the upload has ended opens the channel (Telefunc given a request whose signal aborts once its response ends)', async () => {
  const channels = holdChannels()
  const { socket } = await startCall('returnsChannelIgnoringBlob', 16)
  // The File in 16 parts, 100 ms apart: its upload ends about 1.6 s after the answer, past the connect window.
  for (let sent = 0; sent < 16; sent++) {
    await pause(100)
    socket.write(part())
  }
  const channel = channels.made[0]
  if (!channel) throw new Error('no channel')
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: {
      telefuncUrl: `http://127.0.0.1:${port}/_telefunc`,
      transports: ['sse'],
      idleTimeout: 0,
      connectionKey: crypto.randomUUID(),
    },
  })
  const opened = await page.opened.then(
    () => 'opened',
    (err: Error) => err.message,
  )
  page.abort()
  channels.end()
  socket.destroy()
  expect(opened).toBe('opened')
})
