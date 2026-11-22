// A call whose telefunction leaves its uploaded `Blob` unread, then a second call on the same keep-alive connection:
// both are answered, as Telefunc reads the first call's body off once its call is done. Node parses the second call off
// the connection only once the first call's body is read to its end. Both calls are sent at once. The row waits for the
// first call's answer to finish, then lets the event loop run a counted number of turns (`settle`, no clock) and reads
// which calls the server took and answered. Then the bound: a client that keeps sending a body nobody reads holds its
// connection only until the host's request timeout.

import { afterAll, beforeAll, expect, test } from 'vitest'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { connect } from 'node:net'
import { once } from 'node:events'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'
import { nodeReadableToWebRequest } from '../utils/nodeReadableToWebRequest.js'
import { WIRE_VERSION } from '../wire-protocol/constants.js'

// The app's telefunctions (requestBody.telefunc.js), which Telefunc loads from `config.telefuncFiles` as an app's.
const file = '/requestBody.telefunc.js'
config.root = dirname(fileURLToPath(import.meta.url))
config.telefuncFiles = [`${config.root}${file}`]
config.disableNamingConvention = true

const telefunc = new Telefunc()
const encoder = new TextEncoder()
const telefuncPath = `/_telefunc?wire=${WIRE_VERSION}`

/** Telefunc given Node's request (`serve({ req, res })`). */
function nodeRequest(req: IncomingMessage, res: ServerResponse) {
  telefunc.serve({ req, res }).catch(() => {})
}
/** Telefunc given a Fetch `Request` built from Node's request, as Hono's or srvx's Node server builds one. */
function fetchRequest(req: IncomingMessage, res: ServerResponse) {
  nodeReadableToWebRequest(req, `http://127.0.0.1${telefuncPath}`, 'POST', req.headers)
    .then(({ request }) => telefunc.serve({ request }))
    .then(async (response) => {
      res.writeHead(response?.status ?? 404)
      for await (const chunk of response?.body ?? []) res.write(chunk)
      res.end()
    })
    .catch(() => {})
}
const servers = { 'Node request': nodeRequest, 'Fetch Request': fetchRequest }
const vias = ['Node request', 'Fetch Request'] as const

/** The server's calls, as it took them: each one's response. */
const calls: ServerResponse[] = []
const listening: Server[] = []

/** A server that gives Telefunc `via`, with Node's `options`; its port. */
async function listen(
  via: keyof typeof servers,
  options: { requestTimeout?: number; connectionsCheckingInterval?: number } = {},
) {
  const server = createServer(options, (req, res) => {
    calls.push(res)
    servers[via](req, res)
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  listening.push(server)
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  return { server, port: address.port }
}
const ports = new Map<keyof typeof servers, { server: Server; port: number }>()

beforeAll(async () => {
  for (const via of vias) ports.set(via, await listen(via))
})
afterAll(() => {
  for (const server of listening) server.close()
})

/** The event loop's queued work, run to its end: `turns` macrotask turns, each after its microtasks and I/O. */
async function settle(turns = 200) {
  for (let i = 0; i < turns; i++) await new Promise((resolve) => setImmediate(resolve))
}

/** A binary call to `ignoresBlob` with a `Blob` of `size` bytes: `[u32 length][metadata][the Blob's bytes]`. */
function ignoredBlobCall(size: number): Uint8Array {
  const metadata = encoder.encode(
    JSON.stringify({
      file,
      name: 'ignoresBlob',
      args: [`!TelefuncBlob:${JSON.stringify({ index: 0, size, type: '' })}`],
    }),
  )
  const body = new Uint8Array(4 + metadata.length + size)
  new DataView(body.buffer).setUint32(0, metadata.length)
  body.set(metadata, 4)
  body.fill(120, 4 + metadata.length)
  return body
}

/** A POST's head; `length`, its `Content-Length`, where it differs from the body sent with it. */
function head(port: number, kind: 'bin' | 'txt', length: number): Uint8Array {
  return encoder.encode(
    `POST ${telefuncPath} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-telefunc-request: ${kind}\r\nContent-Type: ${kind === 'bin' ? 'application/octet-stream' : 'text/plain'}\r\nContent-Length: ${length}\r\n\r\n`,
  )
}

/** A socket to `port`, reading (and dropping) what the server answers. */
async function connected(port: number) {
  const socket = connect(port, '127.0.0.1')
  socket.on('error', () => {})
  socket.resume()
  await once(socket, 'connect')
  return socket
}

/** Resolves with the server's next call's response. */
function nextCall(server: Server): Promise<ServerResponse> {
  return new Promise((resolve) => server.once('request', (_req: IncomingMessage, res: ServerResponse) => resolve(res)))
}

for (const via of vias) {
  test.each([64 * 1024, 512 * 1024, 4 * 1024 * 1024])(
    `a call that leaves its %i-byte Blob unread, then a second call on the same connection: both are answered (Telefunc given a ${via})`,
    async (size) => {
      const listening = ports.get(via)
      if (!listening) throw new Error('no server')
      const { server, port } = listening
      calls.length = 0
      const socket = await connected(port)
      const firstTaken = nextCall(server)
      const first = ignoredBlobCall(size)
      const second = encoder.encode(JSON.stringify({ file, name: 'hello', args: [] }))
      for (const part of [head(port, 'bin', first.length), first, head(port, 'txt', second.length), second]) {
        socket.write(part)
      }
      const firstAnswer = await firstTaken
      if (!firstAnswer.writableFinished) await once(firstAnswer, 'finish')
      await settle()
      expect(calls.map((res) => ({ status: res.statusCode, answered: res.writableFinished }))).toEqual([
        { status: 200, answered: true },
        { status: 200, answered: true },
      ])
      socket.destroy()
    },
  )
}

test('a call answered while its client keeps sending the Blob it left unread: the connection ends at the host request timeout', async () => {
  // Node checks its connections' request timeout every `connectionsCheckingInterval`, so the cut comes that often.
  const { server, port } = await listen('Node request', { requestTimeout: 1500, connectionsCheckingInterval: 200 })
  const socket = await connected(port)
  const taken = nextCall(server)
  const start = ignoredBlobCall(10)
  socket.write(head(port, 'bin', start.length + 1024 * 1024))
  socket.write(start)
  const answer = await taken
  if (!answer.writableFinished) await once(answer, 'finish')
  const answered = performance.now()
  const trickle = setInterval(() => socket.write('x'), 100)
  await once(socket, 'close')
  clearInterval(trickle)
  expect({ status: answer.statusCode, cutAfterAnswer: performance.now() - answered < 5000 }).toEqual({
    status: 200,
    cutAfterAnswer: true,
  })
})
