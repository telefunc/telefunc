// node.heldUpload.spec.ts's rows past the bound: a call's kept bytes are held on the channels' ledger, so at most
// `config.channel.maxPendingBytes` of them, here 2 MiB. The body is still read to its end, and the connection's next
// call answered; a File whose bytes past the bound were dropped fails its read once the call is done, with what to do.
// `config.channel` can't change once a channel used it, so these rows have a file of their own.

import { afterAll, beforeAll, expect, test } from 'vitest'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
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
// The least it may be.
config.channel = { maxPendingBytes: 2 * 1024 * 1024 }

const telefunc = new Telefunc()
const encoder = new TextEncoder()
const telefuncPath = `/_telefunc?wire=${WIRE_VERSION}`
/** How long a row waits for what it expects before it reads it as not coming: the head ends no channel by itself sooner
 *  than its connect TTL (15 s). */
const WAIT_MS = 2000

/** The server's calls, as it took them. */
const calls: { req: IncomingMessage; res: ServerResponse }[] = []
const listening: { close(): void }[] = []

function nodeRequest(req: IncomingMessage, res: ServerResponse) {
  telefunc.serve({ req, res }).catch(() => {})
}
function fetchRequest(req: IncomingMessage, res: ServerResponse) {
  telefunc
    .serve({ request: frameworkRequest(req, `http://127.0.0.1${telefuncPath}`) })
    .then(async (response) => {
      res.writeHead(response?.status ?? 404)
      for await (const chunk of response?.body ?? []) res.write(chunk)
      res.end()
    })
    .catch(() => {})
}
const servers = { 'Node request': nodeRequest, 'Fetch Request': fetchRequest }
const vias = ['Node request', 'Fetch Request'] as const

async function portOf(server: { address(): ReturnType<Server['address']> }) {
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  return address.port
}

const http1 = new Map<keyof typeof servers, { server: Server; port: number }>()

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
})
afterAll(() => {
  for (const server of listening) server.close()
})

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
function bodyRead(req: IncomingMessage): Promise<'read to its end'> {
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

const dropped = (size: number) =>
  `The telefunction returned before it read this File (${size} bytes), and the server keeps no more of a File left unread than config.channel.maxMemory and config.channel.maxPendingBytes allow: the rest was dropped. Read a File before the telefunction returns, or while what it returns streams, to keep all of it.`

for (const via of vias) {
  test(`past the bound: a call that returns a Channel and leaves its 4 MiB Blob to be read once the call is done: its body is read to its end, the second call on the connection is answered, and the Blob's read fails saying why (Telefunc given a ${via})`, async () => {
    const listening = http1.get(via)
    if (!listening) throw new Error('no server')
    const channels = holdChannels()
    const socket = await connected(listening.port)
    const size = 4 * 1024 * 1024
    const first = blobCall(size, 'returnsChannelKeepingBlob')
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
    const blob = channels.kept[0]
    if (!blob) throw new Error('no Blob')
    const read = await within(
      blob.text().then(
        (text) => `read ${text.length} bytes`,
        (err: Error) => err.message,
      ),
    )
    channels.end()
    socket.destroy()
    expect({ body, next, read }).toEqual({ body: 'read to its end', next: 200, read: dropped(size) })
  })

  test(`within the bound: a call that returns a Channel and leaves its 1 MiB Blob to be read once the call is done: the Blob is read whole (Telefunc given a ${via})`, async () => {
    const listening = http1.get(via)
    if (!listening) throw new Error('no server')
    const channels = holdChannels()
    const socket = await connected(listening.port)
    const size = 1024 * 1024
    const call = blobCall(size, 'returnsChannelKeepingBlob')
    socket.write(head(listening.port, 'bin', call.length))
    socket.write(call)
    const { req } = await answered(socket.localPort, 0)
    const body = await within(bodyRead(req))
    const blob = channels.kept[0]
    if (!blob) throw new Error('no Blob')
    const read = await within(blob.text().then((text) => text.length))
    channels.end()
    socket.destroy()
    expect({ body, read }).toEqual({ body: 'read to its end', read: size })
  })
}
