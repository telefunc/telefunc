import { afterAll, beforeAll, expect, onTestFinished, test, vi } from 'vitest'
import { createServer, type IncomingMessage, type Server } from 'node:http'
import { connect, type Socket } from 'node:net'
import { once } from 'node:events'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'
import { onBug } from '../node/server/runTelefunc/onBug.js'
import { StreamTruncatedError } from '../wire-protocol/wire/BodyReader.js'

// Development, where Telefunc also prints a malformed request: set before Telefunc first reads it, which it keeps.
vi.hoisted(() => vi.stubEnv('NODE_ENV', 'development'))
// The app's telefunctions (requestBody.telefunc.js), which Telefunc loads from `config.telefuncFiles` as an app's.
const file = '/requestBody.telefunc.js'
config.root = dirname(fileURLToPath(import.meta.url))
config.telefuncFiles = [`${config.root}${file}`]
config.disableNamingConvention = true

const telefunc = new Telefunc()
const body = JSON.stringify({ file, name: 'hello', args: [] })
const headers = { 'x-telefunc-request': 'txt', 'content-type': 'text/plain' }

let server: Server
let port: number
/** Called with the server's next request, right after Telefunc's `serve()` started on it, as an app's middleware is. */
let onRequest: ((req: IncomingMessage, serving: Promise<boolean>) => void) | undefined

beforeAll(async () => {
  server = createServer((req, res) => {
    const serving = telefunc.serve({ req, res })
    onRequest?.(req, serving)
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  port = address.port
})
afterAll(() => {
  server.close()
  vi.unstubAllEnvs()
})

/** What the server prints as an error while the test runs, what `onBug()` hears, and the unhandled rejections. */
function recordErrors(): { printed: string[]; bugs: unknown[]; unhandled: unknown[] } {
  const printed: string[] = []
  const bugs: unknown[] = []
  const unhandled: unknown[] = []
  const error = vi.spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    printed.push(String(args[0]))
  })
  bugListener = (err) => bugs.push(err)
  const record = (err: unknown) => unhandled.push(err)
  process.on('unhandledRejection', record)
  onTestFinished(() => {
    error.mockRestore()
    bugListener = undefined
    process.off('unhandledRejection', record)
  })
  return { printed, bugs, unhandled }
}
let bugListener: ((err: unknown) => void) | undefined
onBug((err) => bugListener?.(err))

/** Sends a text call's head and the first bytes of its body, which declares 1000 bytes, from a socket of the test's
 *  own; resolves once Telefunc reads the body. `middleware` runs as an app's would, before Telefunc reads. */
async function startTextCall(middleware?: (req: IncomingMessage) => void) {
  const arrived = new Promise<{ req: IncomingMessage; serving: Promise<boolean> }>((resolve) => {
    onRequest = (req, serving) => {
      onRequest = undefined
      middleware?.(req)
      resolve({ req, serving })
    }
  })
  const socket = connect(port, '127.0.0.1')
  socket.on('error', () => {})
  await once(socket, 'connect')
  socket.write(
    `POST /_telefunc HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-telefunc-request: txt\r\nContent-Type: text/plain\r\nContent-Length: 1000\r\n\r\n{"file":`,
  )
  const { req, serving } = await arrived
  // Telefunc's read of the body has begun once it was given the first bytes.
  await vi.waitFor(() => expect(req.readableDidRead).toBe(true))
  return { socket, req, serving }
}

/** The response of a text call made through `serve({ request })` with `init`'s body. */
async function fetchCall(init: { body: ReadableStream<unknown> | string; signal?: AbortSignal }): Promise<Response> {
  const requestInit: RequestInit & { duplex: 'half' } = { method: 'POST', headers, duplex: 'half', ...init }
  return requestCall(new Request(`http://127.0.0.1/_telefunc`, requestInit))
}
async function requestCall(request: Request): Promise<Response> {
  const response = await telefunc.serve({ request })
  if (!response) throw new Error('Telefunc left the request to the app')
  return response
}

/** A body that gives `first`, then fails as `fail()` says on the next read. */
function failingBody(first: string, fail: (controller: ReadableStreamDefaultController<Uint8Array>) => void) {
  let pulls = 0
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (pulls++ === 0) controller.enqueue(new TextEncoder().encode(first))
      else fail(controller)
    },
  })
}

async function closed(socket: Socket, req: IncomingMessage) {
  const reqClosed = once(req, 'close').catch(() => {})
  await Promise.all([once(socket, 'close'), reqClosed])
}

test('a text call whose client leaves in the middle of its body prints nothing, and serve() resolves', async () => {
  const { printed, bugs, unhandled } = recordErrors()
  const { socket, req, serving } = await startTextCall()
  socket.destroy()
  await closed(socket, req)
  await expect(serving).resolves.toBe(true)
  expect(printed).toEqual([])
  expect(unhandled).toEqual([])
  // As for a binary call whose body ends early: `onBug()` hears every call that fails, with Node's error as the cause.
  expect(bugs).toEqual([expect.any(StreamTruncatedError)])
  expect(bugs[0]).toHaveProperty('cause.message', 'aborted')
})

test('a text call whose client resets its connection in the middle of the body prints nothing', async () => {
  const { printed, bugs } = recordErrors()
  const { socket, req, serving } = await startTextCall()
  socket.resetAndDestroy()
  await closed(socket, req)
  await expect(serving).resolves.toBe(true)
  expect(printed).toEqual([])
  expect(bugs).toEqual([expect.any(StreamTruncatedError)])
})

test("an app that destroys a text call's request with its own error has that error printed and given to onBug()", async () => {
  const { printed, bugs } = recordErrors()
  const appError = new Error('app body transform failed')
  const { socket, req, serving } = await startTextCall()
  req.destroy(appError)
  await closed(socket, req)
  await expect(serving).resolves.toBe(true)
  expect(printed).toEqual([expect.stringContaining('app body transform failed')])
  expect(bugs).toHaveLength(1)
  expect(bugs[0]).toBe(appError)
})

test('an app that destroys the request with an error from reading another socket has it printed', async () => {
  const { printed, bugs } = recordErrors()
  // What a proxy middleware passes on when its upstream resets: a read error of a socket other than the client's.
  const upstreamError = Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET', syscall: 'read' })
  const { socket, req, serving } = await startTextCall()
  req.destroy(upstreamError)
  await closed(socket, req)
  await expect(serving).resolves.toBe(true)
  expect(printed).toEqual([expect.stringContaining('read ECONNRESET')])
  expect(bugs).toHaveLength(1)
  expect(bugs[0]).toBe(upstreamError)
})

test("a text call whose request an app's middleware decodes to strings is answered 500 with the platform's error", async () => {
  const { printed, bugs } = recordErrors()
  onRequest = (req) => {
    onRequest = undefined
    req.setEncoding('utf8')
  }
  const response = await fetch(`http://127.0.0.1:${port}/_telefunc`, { method: 'POST', headers, body })
  expect(response.status).toBe(500)
  expect(printed).toEqual([expect.stringContaining('Received non-Uint8Array chunk')])
  expect(bugs).toEqual([expect.any(TypeError)])
})

test('a telefunction that throws an error with the code of a reset connection is still printed', async () => {
  const { printed } = recordErrors()
  const response = await fetch(`http://127.0.0.1:${port}/_telefunc`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ file, name: 'callsAnotherServer', args: [] }),
  })
  expect(response.status).toBe(500)
  expect(printed).toEqual([expect.stringContaining('the other server reset the connection')])
})

test('a text call with its whole body is answered, through a socket and through a Fetch request', async () => {
  const { printed } = recordErrors()
  const response = await fetch(`http://127.0.0.1:${port}/_telefunc`, { method: 'POST', headers, body })
  expect(response.status).toBe(200)
  expect(await response.text()).toContain('hello')
  const fetchResponse = await fetchCall({ body })
  expect(fetchResponse.status).toBe(200)
  expect(await fetchResponse.text()).toContain('hello')
  expect(printed).toEqual([])
})

test("a Fetch request whose body an app's transform fails is answered 500, its error printed and given to onBug()", async () => {
  const { printed, bugs } = recordErrors()
  const appError = new Error('app body transform failed')
  const response = await fetchCall({ body: failingBody('{"file":', (controller) => controller.error(appError)) })
  expect(response.status).toBe(500)
  expect(printed).toEqual([expect.stringContaining('app body transform failed')])
  expect(bugs).toHaveLength(1)
  expect(bugs[0]).toBe(appError)
})

test("on Node, a Fetch request whose body fails is printed even when its signal is aborted: the signal is the code's that built the request, not Node's", async () => {
  const { printed, bugs } = recordErrors()
  const controller = new AbortController()
  const disconnected = new DOMException('client disconnected', 'AbortError')
  const response = await fetchCall({
    signal: controller.signal,
    body: failingBody('{"file":', (body) => {
      controller.abort()
      body.error(disconnected)
    }),
  })
  expect(response.status).toBe(500)
  expect(printed).toEqual([expect.stringContaining('client disconnected')])
  expect(bugs).toHaveLength(1)
  expect(bugs[0]).toBe(disconnected)
})

test("a Fetch request whose body was already read, or is locked, is answered 500 with the platform's error", async () => {
  const { printed, bugs } = recordErrors()
  const consumed = new Request('http://127.0.0.1/_telefunc', { method: 'POST', headers, body })
  await consumed.text()
  const released = new Request('http://127.0.0.1/_telefunc', { method: 'POST', headers, body })
  if (!released.body) throw new Error('no body')
  const reader = released.body.getReader()
  for (let chunk = await reader.read(); !chunk.done; chunk = await reader.read()) continue
  reader.releaseLock()
  const locked = new Request('http://127.0.0.1/_telefunc', { method: 'POST', headers, body })
  locked.body?.getReader()
  for (const request of [consumed, released, locked]) expect((await requestCall(request)).status).toBe(500)
  const unusable = expect.stringContaining('Body is unusable')
  expect(printed).toEqual([unusable, unusable, unusable])
  expect(bugs).toEqual([expect.any(TypeError), expect.any(TypeError), expect.any(TypeError)])
})

test("a Fetch request whose body gives strings is answered 500 with the platform's error", async () => {
  const { printed, bugs } = recordErrors()
  const strings = new ReadableStream<string>({
    start(controller) {
      controller.enqueue(body)
      controller.close()
    },
  })
  expect((await fetchCall({ body: strings })).status).toBe(500)
  expect(printed).toEqual([expect.stringContaining('Received non-Uint8Array chunk')])
  expect(bugs).toEqual([expect.any(TypeError)])
})
