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
import { WIRE_VERSION } from '../wire-protocol/constants.js'
import { frameworkRequest } from './frameworkRequest.fixture.js'

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
/** A binary call to `holdsFirstReadsSecond` up to the first bytes of its first `Blob`. */
const holdsFirstStart = concat(
  frame(
    JSON.stringify({
      file,
      name: 'holdsFirstReadsSecond',
      args: [0, 1].map((index) => `!TelefuncBlob:${JSON.stringify({ index, size: FILE_SIZE, type: '' })}`),
    }),
  ),
  encoder.encode('x'.repeat(10)),
)
/** A binary call to `peeksFirstReadsBoth` up to the first bytes of its first `Blob`. */
const peeksFirstStart = concat(
  frame(
    JSON.stringify({
      file,
      name: 'peeksFirstReadsBoth',
      args: [0, 1].map((index) => `!TelefuncBlob:${JSON.stringify({ index, size: FILE_SIZE, type: '' })}`),
    }),
  ),
  encoder.encode('x'.repeat(10)),
)
/** A binary call to `startsReadingBlob` up to the first bytes of its `Blob`. */
const readStartedStart = concat(
  frame(
    JSON.stringify({
      file,
      name: 'startsReadingBlob',
      args: [`!TelefuncBlob:${JSON.stringify({ index: 0, size: FILE_SIZE, type: '' })}`],
    }),
  ),
  encoder.encode('x'.repeat(10)),
)
/** The whole body of a binary call to `keepsBlob`. */
const keepsBlobBody = concat(
  frame(
    JSON.stringify({
      file,
      name: 'keepsBlob',
      args: [`!TelefuncBlob:${JSON.stringify({ index: 0, size: FILE_SIZE, type: '' })}`],
    }),
  ),
  encoder.encode('x'.repeat(FILE_SIZE)),
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
const telefuncPath = `/_telefunc?wire=${WIRE_VERSION}`

let onRequest: ((req: IncomingMessage, res: ServerResponse) => void) | undefined

/** Telefunc given Node's request (`serve({ req, res })`). */
function nodeRequest(req: IncomingMessage, res: ServerResponse) {
  telefunc.serve({ req, res }).catch(() => {})
}
/** Telefunc given a Fetch `Request` built from Node's request, as Hono's or srvx's Node server builds one. */
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
const ports = new Map<keyof typeof servers, number>()
const listening: Server[] = []

beforeAll(async () => {
  for (const via of vias) {
    const server = createServer((req, res) => {
      servers[via](req, res)
      onRequest?.(req, res)
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
 *  once Telefunc read the first bytes, with the server's request and response. `answered` is the status code the server
 *  answers with, from its first bytes. */
async function startUpload(kind: keyof typeof kinds, start: Uint8Array, via: keyof typeof servers = 'Node request') {
  const port = ports.get(via)
  if (port === undefined) throw new Error('no server')
  const arrived = new Promise<{ req: IncomingMessage; res: ServerResponse }>((resolve) => {
    onRequest = (req, res) => {
      onRequest = undefined
      resolve({ req, res })
    }
  })
  const socket = connect(port, '127.0.0.1')
  socket.on('error', () => {})
  const answered = new Promise<number>((resolve) => {
    socket.once('data', (chunk: Buffer) => resolve(Number(chunk.toString('latin1').split(' ', 2)[1])))
  })
  // Reads (and drops) what the server answers, so the socket sees the server's end and closes.
  socket.resume()
  await once(socket, 'connect')
  socket.write(
    `POST ${telefuncPath} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-telefunc-request: ${kinds[kind]}\r\nContent-Type: application/octet-stream\r\nContent-Length: 100000\r\n\r\n`,
  )
  socket.write(start)
  const { req, res } = await arrived
  await vi.waitFor(() => expect(req.readableDidRead).toBe(true))
  return { socket, req, res, answered }
}

/** A binary call whose whole body is `body`, on a socket of its own: the status it is answered with. */
async function sendWhole(body: Uint8Array, via: keyof typeof servers) {
  const port = ports.get(via)
  if (port === undefined) throw new Error('no server')
  const socket = connect(port, '127.0.0.1')
  socket.on('error', () => {})
  const answered = new Promise<number>((resolve) => {
    socket.once('data', (chunk: Buffer) => resolve(Number(chunk.toString('latin1').split(' ', 2)[1])))
  })
  socket.resume()
  await once(socket, 'connect')
  socket.write(
    `POST ${telefuncPath} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-telefunc-request: bin\r\nContent-Type: application/octet-stream\r\nContent-Length: ${body.length}\r\n\r\n`,
  )
  socket.write(body)
  return { socket, answered }
}

test.each(vias)(
  'a binary call that reads its first `Blob` in part, releases that reader, and reads its rest with a new one a turn later while it reads its second, reads both whole (Telefunc given a %s)',
  async (via) => {
    const { printed, bugs } = recordErrors()
    const port = ports.get(via)
    if (port === undefined) throw new Error('no server')
    const rest = concat(encoder.encode('x'.repeat(FILE_SIZE - 10)), encoder.encode('y'.repeat(FILE_SIZE)))
    const socket = connect(port, '127.0.0.1')
    socket.on('error', () => {})
    let answer = ''
    const answered = new Promise<string>((resolve) => {
      socket.on('data', (chunk: Buffer) => {
        answer += chunk.toString('latin1')
        if (answer.includes(`${FILE_SIZE} ${FILE_SIZE}`) || answer.includes('\r\n0\r\n')) resolve(answer)
      })
    })
    await once(socket, 'connect')
    const peeked = blobRead()
    socket.write(
      `POST ${telefuncPath} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-telefunc-request: bin\r\nContent-Type: application/octet-stream\r\nContent-Length: ${peeksFirstStart.length + rest.length}\r\n\r\n`,
    )
    socket.write(peeksFirstStart)
    await peeked
    socket.write(rest)
    expect({ answer: await answered, printed, bugs }).toEqual({
      answer: expect.stringMatching(new RegExp(`^HTTP/1\\.1 200[\\s\\S]*${FILE_SIZE} ${FILE_SIZE}`)),
      printed: [],
      bugs: [],
    })
    socket.destroy()
  },
)

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
  // Its wire opened, so Telefunc took the request and read its body's metadata: a request it refused prints nothing too.
  expect(response?.status).toBe(200)
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
    const { socket, answered } = await startUpload('sse', sseStart, via)
    // Its wire opened, so Telefunc took the request and read its body's metadata: a request it refused prints nothing too.
    expect(await answered).toBe(200)
    socket.destroy()
    await closed(socket)
    // Nothing signals that the wire's reader is done with the failed read: give it as long as an app error takes above.
    await new Promise((resolve) => setTimeout(resolve, 500))
    expect(printed).toEqual([])
  },
)

test.each(vias)(
  "a Blob's read waiting behind a Blob whose reader the telefunction holds unread ends with the client's truncation once its client leaves (Telefunc given a %s)",
  async (via) => {
    recordErrors()
    const readEnded: { now: string } = { now: 'pending' }
    const onReadEnded = (outcome: string) => void (readEnded.now = outcome)
    process.on('requestBody.telefunc:readEnded', onReadEnded)
    onTestFinished(() => void process.off('requestBody.telefunc:readEnded', onReadEnded))
    const reading = blobRead()
    const { socket } = await startUpload('binary', holdsFirstStart, via)
    await reading
    socket.destroy()
    await vi.waitFor(() => expect(readEnded.now).not.toBe('pending'))
    expect(readEnded.now).toBe(`failed: ${new StreamTruncatedError('client').message}`)
  },
)

test.each(vias)(
  'a telefunction that starts reading its Blob and answers, its client still sending: that read goes on past the answer and reads the Blob whole, and nothing is printed (Telefunc given a %s)',
  async (via) => {
    const { printed, bugs } = recordErrors()
    const readEnded: { now: string } = { now: 'pending' }
    const onReadEnded = (outcome: string) => void (readEnded.now = outcome)
    process.on('requestBody.telefunc:readEnded', onReadEnded)
    onTestFinished(() => void process.off('requestBody.telefunc:readEnded', onReadEnded))
    const { socket, answered } = await startUpload('binary', readStartedStart, via)
    expect(await answered).toBe(200)
    // The rest of the Blob, after the answer.
    socket.write(encoder.encode('x'.repeat(FILE_SIZE - 10)))
    await vi.waitFor(() => expect(readEnded.now).not.toBe('pending'))
    expect({ readEnded: readEnded.now, printed, bugs }).toEqual({
      readEnded: `read ${FILE_SIZE} bytes`,
      printed: [],
      bugs: [],
    })
    socket.destroy()
  },
)

test.each(vias)(
  'a Blob its telefunction keeps and reads once the call is answered and its body read off to the end: the Blob is read whole (Telefunc given a %s)',
  async (via) => {
    const { printed, bugs } = recordErrors()
    const kept = new Promise<Blob>((resolve) => process.once('requestBody.telefunc:kept', resolve))
    const { socket, answered } = await sendWhole(keepsBlobBody, via)
    expect(await answered).toBe(200)
    // The read-off has read the body to its end.
    await settle()
    const read = await (await kept).text().then(
      (text) => `read ${text.length} bytes`,
      (err: unknown) => `failed: ${err instanceof Error ? err.message : String(err)}`,
    )
    expect({ read, printed, bugs }).toEqual({ read: `read ${FILE_SIZE} bytes`, printed: [], bugs: [] })
    socket.destroy()
  },
)

// The most metadata the server reads (`CALL_METADATA_MAX_BYTES`): 536,870,888 UTF-16 code units, V8's longest string, of
// at most 3 UTF-8 bytes each. A binary call that declares more is malformed, refused once its length is read, before any
// of its metadata.
const METADATA_BOUND = 1_610_612_664

/** A Fetch request of a binary call whose body gives `start` and stays open until `end()`. */
function openRequest(start: Uint8Array) {
  const sink: { controller: ReadableStreamDefaultController<Uint8Array> | null } = { controller: null }
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      sink.controller = controller
      controller.enqueue(start)
    },
  })
  const init: RequestInit & { duplex: 'half' } = {
    method: 'POST',
    headers: { 'x-telefunc-request': kinds.binary },
    body,
    duplex: 'half',
  }
  return { request: new Request(`http://127.0.0.1${telefuncPath}`, init), end: () => sink.controller?.close() }
}

/** The event loop's queued work, run to its end: `turns` macrotask turns, each after its microtasks. */
async function settle(turns = 50) {
  for (let i = 0; i < turns; i++) await new Promise((resolve) => setImmediate(resolve))
}

/** The status `serve()` answered with once the event loop ran dry, or `pending`. */
async function statusOnceSettled(serving: Promise<Response | undefined>) {
  const outcome: { now: number | 'pending' } = { now: 'pending' }
  serving.then((response) => void (outcome.now = response?.status ?? 404))
  await settle()
  return outcome.now
}

test('a binary call whose metadata declares more than the server reads is answered 400 before its metadata is sent (Telefunc given a Node request)', async () => {
  const { printed } = recordErrors()
  // Once the server has read the length, what it answered once the event loop ran dry, on its own side of the socket.
  const { socket, res } = await startUpload('binary', u32(METADATA_BOUND + 1))
  await settle()
  expect(res.headersSent ? res.statusCode : 'pending').toBe(400)
  socket.destroy()
  await closed(socket)
  expect(printed).toEqual([expect.stringContaining('more metadata than the server reads')])
})

test('a binary call whose metadata declares more than the server reads is answered 400 before its metadata is sent (Telefunc given a Fetch Request)', async () => {
  const { printed } = recordErrors()
  const { request, end } = openRequest(u32(METADATA_BOUND + 1))
  const serving = telefunc.serve({ request })
  expect(await statusOnceSettled(serving)).toBe(400)
  end()
  await serving
  expect(printed).toEqual([expect.stringContaining('more metadata than the server reads')])
})

test('a binary call whose metadata declares the most the server reads waits for its metadata (Telefunc given a Fetch Request)', async () => {
  recordErrors()
  const { request, end } = openRequest(u32(METADATA_BOUND))
  const serving = telefunc.serve({ request })
  expect(await statusOnceSettled(serving)).toBe('pending')
  // Its body ends short: the call is answered as a truncation is.
  end()
  await serving
})

test('a binary call without a body is refused as malformed: answered 400, with no bug (Telefunc given a Fetch Request)', async () => {
  const { printed, bugs } = recordErrors()
  const request = new Request(`http://127.0.0.1${telefuncPath}`, {
    method: 'POST',
    headers: { 'x-telefunc-request': kinds.binary },
  })
  const response = await telefunc.serve({ request })
  // In development a malformed request is printed as such, as every refused call is (`logParseError`).
  expect({ status: response?.status, printed, bugs }).toEqual({
    status: 400,
    printed: [expect.stringContaining('The binary request ends before its envelope is complete.')],
    bugs: [],
  })
})

/** A binary call's body that ends in its metadata, which declares 1000 bytes: its whole body, as its client sent it. */
const endedInMetadata = concat(u32(1000), encoder.encode('{"file":'))

test.each(vias)(
  'a binary call whose body ends before its envelope is complete is refused as malformed: answered 400, with no bug (Telefunc given a %s)',
  async (via) => {
    const { printed, bugs } = recordErrors()
    const answers: number[] = []
    for (const body of [new Uint8Array(0), u32(1000).subarray(0, 2), endedInMetadata]) {
      const { socket, answered } = await sendWhole(body, via)
      answers.push(await answered)
      socket.destroy()
    }
    const refusal = expect.stringContaining('The binary request ends before its envelope is complete.')
    expect({ answers, printed, bugs }).toEqual({
      answers: [400, 400, 400],
      printed: [refusal, refusal, refusal],
      bugs: [],
    })
  },
)

test('a binary call whose Fetch Request has an empty body is refused as malformed: answered 400, with no bug', async () => {
  const { printed, bugs } = recordErrors()
  const init: RequestInit & { duplex: 'half' } = {
    method: 'POST',
    headers: { 'x-telefunc-request': kinds.binary },
    body: new Uint8Array(0),
    duplex: 'half',
  }
  const response = await telefunc.serve({ request: new Request(`http://127.0.0.1${telefuncPath}`, init) })
  expect({ status: response?.status, printed, bugs }).toEqual({
    status: 400,
    printed: [expect.stringContaining('The binary request ends before its envelope is complete.')],
    bugs: [],
  })
})

// Telefunc's reader alone reads Node's request: the Fetch `Request` the Node adapter builds for the app reads it only
// once something reads its body, so no Web stream copies a binary call's body into a queue nothing reads.
test("a binary call through the Node adapter has its request read by Telefunc's reader alone: nothing else takes its data", async () => {
  const reading = blobRead()
  const { socket, req } = await startUpload('binary', binaryStart)
  await reading
  expect({ dataListeners: req.listenerCount('data'), flowing: req.readableFlowing }).toEqual({
    dataListeners: 0,
    flowing: false,
  })
  socket.destroy()
  await closed(socket)
})
