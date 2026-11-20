import { afterAll, beforeAll, expect, onTestFinished, test, vi } from 'vitest'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { connect, type Socket } from 'node:net'
import { once } from 'node:events'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'
import { onBug } from '../node/server/runTelefunc/onBug.js'
import { StreamTruncatedError } from '../wire-protocol/wire/BodyReader.js'
import { nodeReadableToWebRequest } from '../utils/nodeReadableToWebRequest.js'

// The body of a binary call (a telefunction given a `Blob`) and of an SSE upload, read through Telefunc's body reader,
// fails on the app's side: printed and given to `onBug()` as the app's own error. A client that leaves stays quiet.
// Through a Fetch `Request` (`serve({ request })`), nothing tells the two apart: both are a truncation, as in 0.3.1.

vi.hoisted(() => vi.stubEnv('NODE_ENV', 'development'))
// The app's telefunctions (requestBody.telefunc.js), which Telefunc loads from `config.telefuncFiles` as an app's.
const file = '/requestBody.telefunc.js'
config.root = dirname(fileURLToPath(import.meta.url))
config.telefuncFiles = [`${config.root}${file}`]
config.disableNamingConvention = true

const telefunc = new Telefunc()
const FILE_SIZE = 1000
const encoder = new TextEncoder()

function u32(n: number): Uint8Array {
  const bytes = new Uint8Array(4)
  new DataView(bytes.buffer).setUint32(0, n)
  return bytes
}
function concat(...parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0))
  let at = 0
  for (const part of parts) {
    out.set(part, at)
    at += part.length
  }
  return out
}
/** `[u32 length][bytes]`, as a binary call's metadata and each SSE frame are sent. */
function frame(text: string): Uint8Array {
  const bytes = encoder.encode(text)
  return concat(u32(bytes.length), bytes)
}

/** A binary call's body up to the first bytes of its `Blob` argument, which declares `FILE_SIZE` bytes. */
const binaryStart = concat(
  frame(
    JSON.stringify({
      file,
      name: 'readsBlob',
      args: [`!TelefuncBlob:${JSON.stringify({ index: 0, size: FILE_SIZE, type: '' })}`],
    }),
  ),
  encoder.encode('x'.repeat(10)),
)
/** A binary call to `readsBoth` up to the first bytes of its first `Blob`. */
const twoBlobsStart = concat(
  frame(
    JSON.stringify({
      file,
      name: 'readsBoth',
      args: [0, 1].map((index) => `!TelefuncBlob:${JSON.stringify({ index, size: FILE_SIZE, type: '' })}`),
    }),
  ),
  encoder.encode('x'.repeat(10)),
)
/** A binary call's body cut in its metadata, which declares 1000 bytes. */
const binaryMetadataStart = concat(u32(1000), encoder.encode('{"file":'))
/** An SSE upload that opens its wire, then the first bytes of a frame that declares 100. */
const sseStart = concat(
  frame(JSON.stringify({ connId: 'upload-1', streamResponse: true })),
  u32(100),
  encoder.encode('x'),
)

const kinds = { binary: 'bin', sse: 'sse' }
const telefuncPath = '/_telefunc?wire=2'

let onRequest: ((req: IncomingMessage) => void) | undefined

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
const ports = new Map<keyof typeof servers, number>()
const listening: Server[] = []

beforeAll(async () => {
  for (const via of vias) {
    const server = createServer((req, res) => {
      servers[via](req, res)
      onRequest?.(req)
    })
    server.listen(0, '127.0.0.1')
    await once(server, 'listening')
    const address = server.address()
    if (address === null || typeof address === 'string') throw new Error('no port')
    ports.set(via, address.port)
    listening.push(server)
  }
})
afterAll(() => {
  for (const server of listening) server.close()
  vi.unstubAllEnvs()
})

let bugListener: ((err: unknown) => void) | undefined
onBug((err) => bugListener?.(err))
function recordErrors(): { printed: string[]; bugs: unknown[] } {
  const printed: string[] = []
  const bugs: unknown[] = []
  const error = vi.spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    printed.push(String(args[0]))
  })
  bugListener = (err) => bugs.push(err)
  onTestFinished(() => {
    error.mockRestore()
    bugListener = undefined
  })
  return { printed, bugs }
}

/** Resolves when `readsBlob` or `readsBoth` starts reading its argument. */
async function blobRead(): Promise<void> {
  await once(process, 'requestBody.telefunc:read')
}

/** Sends a POST of `kind` with `start` from a socket of the test's own to the server that gives Telefunc `via`; resolves
 *  once Telefunc read the first bytes. */
async function startUpload(kind: keyof typeof kinds, start: Uint8Array, via: keyof typeof servers = 'Node request') {
  const port = ports.get(via)
  if (port === undefined) throw new Error('no server')
  const arrived = new Promise<IncomingMessage>((resolve) => {
    onRequest = (req) => {
      onRequest = undefined
      resolve(req)
    }
  })
  const socket = connect(port, '127.0.0.1')
  socket.on('error', () => {})
  // Reads (and drops) what the server answers, so the socket sees the server's end and closes.
  socket.resume()
  await once(socket, 'connect')
  socket.write(
    `POST ${telefuncPath} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-telefunc-request: ${kinds[kind]}\r\nContent-Type: application/octet-stream\r\nContent-Length: 100000\r\n\r\n`,
  )
  socket.write(start)
  const req = await arrived
  await vi.waitFor(() => expect(req.readableDidRead).toBe(true))
  return { socket, req }
}

/** A Fetch request of `kind` whose body gives `start`, then fails with `error` once `when` resolves. */
function failingRequest(kind: keyof typeof kinds, start: Uint8Array, error: Error, when: Promise<void>): Request {
  let pulls = 0
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (pulls++ === 0) return controller.enqueue(start)
      await when
      controller.error(error)
    },
  })
  const init: RequestInit & { duplex: 'half' } = {
    method: 'POST',
    headers: { 'x-telefunc-request': kinds[kind] },
    body,
    duplex: 'half',
  }
  return new Request(`http://127.0.0.1${telefuncPath}`, init)
}

async function closed(socket: Socket) {
  if (!socket.destroyed) await once(socket, 'close')
}

test("a binary call whose request an app destroys while the telefunction reads its Blob has the app's error printed and given to onBug()", async () => {
  const { printed, bugs } = recordErrors()
  const appError = new Error('app body transform failed')
  const reading = blobRead()
  const { socket, req } = await startUpload('binary', binaryStart)
  await reading
  req.destroy(appError)
  await closed(socket)
  await vi.waitFor(() => expect(bugs).toHaveLength(1))
  expect(bugs[0]).toBe(appError)
  expect(printed).toEqual([expect.stringContaining('app body transform failed')])
})

test("a telefunction that reads a second Blob after the app failed the request's body gets the app's error there too", async () => {
  const { printed, bugs } = recordErrors()
  const appError = new Error('app body transform failed')
  const reading = blobRead()
  const { socket, req } = await startUpload('binary', twoBlobsStart)
  await reading
  req.destroy(appError)
  await closed(socket)
  await vi.waitFor(() => expect(bugs).toHaveLength(1))
  expect(bugs[0]).toBe(appError)
  expect(printed).toEqual([expect.stringContaining('app body transform failed')])
})

test("a binary call whose request an app destroys in its metadata has the app's error printed and given to onBug()", async () => {
  const { printed, bugs } = recordErrors()
  const appError = new Error('app body transform failed')
  const { socket, req } = await startUpload('binary', binaryMetadataStart)
  req.destroy(appError)
  await closed(socket)
  await vi.waitFor(() => expect(bugs).toHaveLength(1))
  expect(bugs[0]).toBe(appError)
  expect(printed).toEqual([expect.stringContaining('app body transform failed')])
})

// A Fetch body's failure says nothing of who failed it: it is taken for the client leaving, as in 0.3.1.
test("a binary call whose Fetch body an app's transform fails while the telefunction reads its Blob is answered 500 with a truncation, the app's error its cause", async () => {
  const { printed, bugs } = recordErrors()
  const appError = new Error('app body transform failed')
  const response = await telefunc.serve({ request: failingRequest('binary', binaryStart, appError, blobRead()) })
  expect(response?.status).toBe(500)
  expect(bugs).toEqual([expect.any(StreamTruncatedError)])
  expect(bugs[0]).toHaveProperty('cause', appError)
  expect(printed).toEqual([])
})

test("a binary call whose Fetch body an app's transform fails in its metadata is answered 500 with a truncation, the app's error its cause", async () => {
  const { printed, bugs } = recordErrors()
  const appError = new Error('app body transform failed')
  const response = await telefunc.serve({
    request: failingRequest('binary', binaryMetadataStart, appError, Promise.resolve()),
  })
  expect(response?.status).toBe(500)
  expect(bugs).toEqual([expect.any(StreamTruncatedError)])
  expect(bugs[0]).toHaveProperty('cause', appError)
  expect(printed).toEqual([])
})

test("an SSE upload whose request an app destroys after its wire opened has the app's error printed and given to onBug()", async () => {
  const { printed, bugs } = recordErrors()
  const appError = new Error('app body transform failed')
  const { socket, req } = await startUpload('sse', sseStart)
  req.destroy(appError)
  await closed(socket)
  await vi.waitFor(() => expect(bugs).toHaveLength(1))
  expect(bugs[0]).toBe(appError)
  expect(printed).toEqual([expect.stringContaining('app body transform failed')])
})

test("an SSE upload whose Fetch body an app's transform fails after its wire opened prints nothing", async () => {
  const { printed, bugs } = recordErrors()
  const appError = new Error('app body transform failed')
  const response = await telefunc.serve({ request: failingRequest('sse', sseStart, appError, Promise.resolve()) })
  // Nothing signals that the wire's reader is done with the failed read: give it as long as an app error takes above.
  await new Promise((resolve) => setTimeout(resolve, 500))
  expect(printed).toEqual([])
  expect(bugs).toEqual([])
  await response?.body?.cancel()
})

test.each(vias)(
  'a binary call whose client leaves while the telefunction reads its Blob prints nothing (Telefunc given a %s)',
  async (via) => {
    const { printed, bugs } = recordErrors()
    const reading = blobRead()
    const { socket } = await startUpload('binary', binaryStart, via)
    await reading
    socket.destroy()
    // The failed call reaches `onBug()`, as every failed call does, once the telefunction has settled.
    await vi.waitFor(() => expect(bugs).toHaveLength(1))
    expect(printed).toEqual([])
    // Node's own error is kept as the cause.
    expect(bugs[0]).toHaveProperty('cause.message', 'aborted')
  },
)

test.each(vias)(
  'a binary call whose client leaves in its metadata prints nothing (Telefunc given a %s)',
  async (via) => {
    const { printed, bugs } = recordErrors()
    const { socket } = await startUpload('binary', binaryMetadataStart, via)
    socket.destroy()
    await vi.waitFor(() => expect(bugs).toHaveLength(1))
    expect(printed).toEqual([])
    expect(bugs[0]).toHaveProperty('cause.message', 'aborted')
  },
)

test.each(vias)(
  'an SSE upload whose client leaves after its wire opened prints nothing (Telefunc given a %s)',
  async (via) => {
    const { printed } = recordErrors()
    const { socket } = await startUpload('sse', sseStart, via)
    socket.destroy()
    await closed(socket)
    // Nothing signals that the wire's reader is done with the failed read: give it as long as an app error takes above.
    await new Promise((resolve) => setTimeout(resolve, 500))
    expect(printed).toEqual([])
  },
)
