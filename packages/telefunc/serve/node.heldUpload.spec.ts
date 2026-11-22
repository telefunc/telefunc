// A call whose telefunction returns a Channel holds its request while the channel lives. A browser delivers a call's
// answer only once it has sent the call's whole body, so a body nobody reads would keep that answer from the page:
// Telefunc reads the rest of the body at the answer, keeping it for the call's Files. These rows send such a call and
// read whether its body is read to its end while the channel still lives, over HTTP/1.1 (then the connection's next call
// is answered too) and over HTTP/2. Then a File the call reads as it arrives, and one it reads only once the call is done,
// are both read whole.

import { afterAll, beforeAll, expect, test } from 'vitest'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import {
  connect as connectHttp2,
  createServer as createHttp2Server,
  type Http2ServerRequest,
  type Http2ServerResponse,
} from 'node:http2'
import { connect } from 'node:net'
import { once } from 'node:events'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'
import { frameworkRequest } from './frameworkRequest.fixture.js'
import { WIRE_VERSION } from '../wire-protocol/constants.js'
import { Channel } from '../wire-protocol/channel/server/ServerChannel.js'

const file = '/requestBody.telefunc.js'
config.root = dirname(fileURLToPath(import.meta.url))
config.telefuncFiles = [`${config.root}${file}`]
config.disableNamingConvention = true

const telefunc = new Telefunc()
const encoder = new TextEncoder()
const telefuncPath = `/_telefunc?wire=${WIRE_VERSION}`
/** How long a row waits for what it expects before it reads it as not coming: the head ends no channel by itself sooner
 *  than its connect TTL (15 s). */
const WAIT_MS = 2000

/** The server's calls, as it took them. */
const calls: { req: IncomingMessage | Http2ServerRequest; res: ServerResponse | Http2ServerResponse }[] = []
const listening: { close(): void }[] = []

function nodeRequest(req: IncomingMessage, res: ServerResponse) {
  telefunc.serve({ req, res }).catch(() => {})
}
function fetchRequest(req: IncomingMessage | Http2ServerRequest, res: Answering) {
  telefunc
    .serve({ request: frameworkRequest(req, `http://127.0.0.1${telefuncPath}`) })
    .then(async (response) => {
      res.writeHead(response?.status ?? 404)
      for await (const chunk of response?.body ?? []) res.write(chunk)
      res.end()
    })
    .catch(() => {})
}
/** What `fetchRequest` writes its answer to: Node's response, of HTTP/1.1 or HTTP/2. */
type Answering = { writeHead(status: number): unknown; write(chunk: Uint8Array): unknown; end(): unknown }
const servers = { 'Node request': nodeRequest, 'Fetch Request': fetchRequest }
const vias = ['Node request', 'Fetch Request'] as const

async function portOf(server: { address(): ReturnType<Server['address']> }) {
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  return address.port
}

const http1 = new Map<keyof typeof servers, { server: Server; port: number }>()
let http2Port = 0

beforeAll(async () => {
  for (const via of vias) {
    const server = createServer((req, res) => {
      calls.push({ req, res })
      servers[via](req, res)
    })
    server.listen(0, '127.0.0.1')
    await once(server, 'listening')
    listening.push(server)
    http1.set(via, { server, port: await portOf(server) })
  }
  const server2 = createHttp2Server((req, res) => {
    calls.push({ req, res })
    fetchRequest(req, res)
  })
  server2.listen(0, '127.0.0.1')
  await once(server2, 'listening')
  listening.push(server2)
  http2Port = await portOf(server2)
})
afterAll(() => {
  for (const server of listening) server.close()
})

/** The event loop's queued work, run to its end: `turns` macrotask turns, each after its microtasks and I/O. */
async function settle(turns = 200) {
  for (let i = 0; i < turns; i++) await new Promise((resolve) => setImmediate(resolve))
}

/** `promise`'s value, or `'not by WAIT_MS'`. */
function within<T>(promise: Promise<T>): Promise<T | 'not by WAIT_MS'> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const late = new Promise<'not by WAIT_MS'>(
    (resolve) => (timer = setTimeout(() => resolve('not by WAIT_MS'), WAIT_MS)),
  )
  return Promise.race([promise, late]).finally(() => clearTimeout(timer))
}

/** A binary call to `name` with a `Blob` of `size` bytes: `[u32 length][metadata][the Blob's bytes]`. */
function blobCall(size: number, name: string): Uint8Array {
  const metadata = encoder.encode(
    JSON.stringify({ file, name, args: [`!TelefuncBlob:${JSON.stringify({ index: 0, size, type: '' })}`] }),
  )
  const body = new Uint8Array(4 + metadata.length + size)
  new DataView(body.buffer).setUint32(0, metadata.length)
  body.set(metadata, 4)
  body.fill(120, 4 + metadata.length)
  return body
}

function head(port: number, kind: 'bin' | 'txt', length: number): Uint8Array {
  return encoder.encode(
    `POST ${telefuncPath} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-telefunc-request: ${kind}\r\nContent-Type: ${kind === 'bin' ? 'application/octet-stream' : 'text/plain'}\r\nContent-Length: ${length}\r\n\r\n`,
  )
}

async function connected(port: number) {
  const socket = connect(port, '127.0.0.1')
  socket.on('error', () => {})
  socket.resume()
  await once(socket, 'connect')
  return socket
}

/** The `n`-th call (from 0) the server took from the client's port `from`, once it is answered: a call another row's
 *  connection carried, parsed late, is not this row's. */
async function answered(from: number | undefined, n: number) {
  const ours = () => calls.filter(({ req }) => req.socket.remotePort === from)
  while (ours().length <= n) await new Promise((resolve) => setImmediate(resolve))
  const call = ours()[n]
  if (call === undefined) throw new Error('no call')
  if (!call.res.writableFinished) await once(call.res, 'finish')
  return call
}

/** Whether the request's body is read to its end: Node emits `'end'` once its last byte is read. */
function bodyRead(req: IncomingMessage | Http2ServerRequest): Promise<'read to its end'> {
  if (req.readableEnded) return Promise.resolve('read to its end')
  return once(req, 'end').then((): 'read to its end' => 'read to its end')
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
    kept,
    end() {
      process.off('requestBody.telefunc:channel', onChannel)
      process.off('requestBody.telefunc:kept', onKept)
      for (const channel of made) channel.abort()
    },
  }
}

for (const via of vias) {
  test.each([512 * 1024, 32 * 1024 * 1024])(
    `a call that returns a Channel and leaves its %i-byte Blob unread: its body is read to its end while the channel lives, and the second call on the connection is answered (Telefunc given a ${via})`,
    async (size) => {
      const listening = http1.get(via)
      if (!listening) throw new Error('no server')
      const channels = holdChannels()
      const socket = await connected(listening.port)
      const first = blobCall(size, 'returnsChannelIgnoringBlob')
      const second = encoder.encode(JSON.stringify({ file, name: 'hello', args: [] }))
      for (const part of [
        head(listening.port, 'bin', first.length),
        first,
        head(listening.port, 'txt', second.length),
        second,
      ]) {
        socket.write(part)
      }
      const { req } = await answered(socket.localPort, 0)
      const body = await within(bodyRead(req))
      const next = await within(answered(socket.localPort, 1).then(({ res }) => res.statusCode))
      channels.end()
      socket.destroy()
      expect({ body, next }).toEqual({ body: 'read to its end', next: 200 })
    },
  )
}

test.each([512 * 1024, 32 * 1024 * 1024])(
  'over HTTP/2, a call that returns a Channel and leaves its %i-byte Blob unread: its body is read to its end while the channel lives (Telefunc given a Fetch Request)',
  async (size) => {
    const channels = holdChannels()
    // Node's client counts the body it has yet to send against its session's memory (10 MB by default).
    const session = connectHttp2(`http://127.0.0.1:${http2Port}`, { maxSessionMemory: 64 })
    session.on('error', () => {})
    await once(session, 'connect')
    const request = session.request({
      ':method': 'POST',
      ':path': telefuncPath,
      'x-telefunc-request': 'bin',
      'content-type': 'application/octet-stream',
    })
    request.on('error', () => {})
    request.end(blobCall(size, 'returnsChannelIgnoringBlob'))
    const status = await once(request, 'response').then(([headers]) => headers[':status'])
    const { req } = await answered(session.socket.localPort, 0)
    const body = await within(bodyRead(req))
    channels.end()
    request.close()
    session.close()
    expect({ status, body }).toEqual({ status: 200, body: 'read to its end' })
  },
)

test('a call that returns a Channel and reads its Blob as it arrives, sent after the answer: the Blob is read whole (Telefunc given a Node request)', async () => {
  const listening = http1.get('Node request')
  if (!listening) throw new Error('no server')
  const channels = holdChannels()
  const readEnded = once(process, 'requestBody.telefunc:readEnded').then(([outcome]) => outcome)
  const socket = await connected(listening.port)
  const size = 1024 * 1024
  const call = blobCall(size, 'returnsChannelReadingBlob')
  const blobStart = call.length - size
  socket.write(head(listening.port, 'bin', call.length))
  socket.write(call.subarray(0, blobStart))
  await answered(socket.localPort, 0)
  for (let at = blobStart; at < call.length; at += 64 * 1024) {
    socket.write(call.subarray(at, at + 64 * 1024))
    await settle(5)
  }
  const outcome = await within(readEnded)
  channels.end()
  socket.destroy()
  expect(outcome).toBe(`read ${size} bytes`)
})

for (const via of vias) {
  test(`a call that returns a Channel and leaves its Blob to be read once the call is done: the Blob is read whole (Telefunc given a ${via})`, async () => {
    const listening = http1.get(via)
    if (!listening) throw new Error('no server')
    const channels = holdChannels()
    const socket = await connected(listening.port)
    const size = 512 * 1024
    const call = blobCall(size, 'returnsChannelKeepingBlob')
    socket.write(head(listening.port, 'bin', call.length))
    socket.write(call)
    await answered(socket.localPort, 0)
    await settle()
    const blob = channels.kept[0]
    if (!blob) throw new Error('no Blob')
    const read = await within(blob.text().then((text) => text.length))
    channels.end()
    socket.destroy()
    expect(read).toBe(size)
  })
}
