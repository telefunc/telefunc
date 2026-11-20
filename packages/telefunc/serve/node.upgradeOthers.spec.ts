import { expect, onTestFinished, test } from 'vitest'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { connect } from 'node:net'
import { once } from 'node:events'
import { randomBytes } from 'node:crypto'
import type { Duplex } from 'node:stream'
import { Telefunc } from './node.js'
import { WIRE_VERSION } from '../wire-protocol/constants.js'
import { hasProp } from '../utils/hasProp.js'

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

// A request with `Upgrade` that isn't Telefunc's, on a server with Telefunc's WebSocket: it gets what the app's own
// listeners give it, as on the same server without Telefunc's WebSocket.

/** The app: its page at `/`, and 404 for any other path. */
function app(req: IncomingMessage, res: ServerResponse): void {
  if (req.url === '/') res.end('the page')
  else res.writeHead(404).end('not found')
}

/** An app's own WebSocket server, for `/app-ws`: it answers 418, so a row can tell its answer from anyone else's. */
function appUpgrade(req: IncomingMessage, socket: Duplex): void {
  if (req.url === '/app-ws') socket.end("HTTP/1.1 418 I'm a teapot\r\nContent-Length: 0\r\n\r\n")
}

async function listen(server: Server): Promise<number> {
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  onTestFinished(() => {
    server.closeAllConnections()
    server.close()
  })
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  return address.port
}

const websocket = () => [
  'Connection: Upgrade',
  'Upgrade: websocket',
  'Sec-WebSocket-Version: 13',
  `Sec-WebSocket-Key: ${randomBytes(16).toString('base64')}`,
]
const requests = {
  'a page request that offers h2c': [
    'GET / HTTP/1.1',
    'Connection: Upgrade, HTTP2-Settings',
    'Upgrade: h2c',
    'HTTP2-Settings: AAMAAABkAAQAoAAAAAIAAAAA',
  ],
  "a WebSocket handshake for the app's page": ['GET / HTTP/1.1', ...websocket()],
  "a WebSocket handshake for a page the app doesn't have": ['GET /no-such-page HTTP/1.1', ...websocket()],
  "a WebSocket handshake for the app's own WebSocket server": ['GET /app-ws HTTP/1.1', ...websocket()],
  "Telefunc's WebSocket handshake": [`GET /_telefunc?wire=${WIRE_VERSION} HTTP/1.1`, ...websocket()],
}

/** What the server writes to `request`: its status and body, null where it writes nothing within `waitMs`; and whether
 *  the socket is still open 500 ms after the first bytes or the close. */
async function answer(
  port: number,
  request: string[],
  waitMs = 10_000,
): Promise<{ status: number | null; body: string; open: boolean }> {
  const socket = connect(port, '127.0.0.1')
  socket.on('error', () => {})
  await once(socket, 'connect')
  let received = ''
  let open = true
  const { promise: answered, resolve } = Promise.withResolvers<void>()
  socket.on('data', (chunk: Buffer) => {
    received += chunk.toString('latin1')
    resolve()
  })
  socket.on('close', () => {
    open = false
    resolve()
  })
  socket.write(`${[...request, 'Host: localhost'].join('\r\n')}\r\n\r\n`)
  await Promise.race([answered, sleep(waitMs)])
  await sleep(500)
  socket.destroy()
  if (received === '') return { status: null, body: '', open }
  const [head = '', body = ''] = received.split('\r\n\r\n')
  return { status: Number(head.split(' ')[1]), body, open }
}

/** A server of the app's own; `telefunc` installs Telefunc's WebSocket on it, and `shouldUpgradeCallback` is the app's. */
async function server({
  telefunc,
  shouldUpgradeCallback,
}: { telefunc: boolean; shouldUpgradeCallback?: (req: IncomingMessage) => boolean }) {
  const server = createServer(app)
  // What `createServer({ shouldUpgradeCallback })` does with the option, which the typings here don't know.
  if (shouldUpgradeCallback) Reflect.set(server, 'shouldUpgradeCallback', shouldUpgradeCallback)
  if (telefunc) new Telefunc().installWebSocket(server)
  return { server, port: await listen(server) }
}

// Node has it from 22.21 and 24.9; its typings here don't.
const nodeHasTheCallback = hasProp(createServer(), 'shouldUpgradeCallback', 'function')

test
  .runIf(nodeHasTheCallback)
  .each([
    'a page request that offers h2c',
    "a WebSocket handshake for the app's page",
    "a WebSocket handshake for a page the app doesn't have",
  ] as const)("beside Telefunc's WebSocket alone, %s gets the app's own answer", async (request) => {
  const without = await server({ telefunc: false })
  const beside = await server({ telefunc: true })
  const appsAnswer = await answer(without.port, requests[request])
  expect(appsAnswer.status).not.toBe(null)
  expect(await answer(beside.port, requests[request])).toEqual(appsAnswer)
})

test("Telefunc's WebSocket handshake opens", async () => {
  const { port } = await server({ telefunc: true })
  expect((await answer(port, requests["Telefunc's WebSocket handshake"])).status).toBe(101)
})

test.each(['before', 'after'] as const)(
  "beside another WebSocket server added %s Telefunc's, a handshake for its path gets its answer, and Telefunc's opens",
  async (order) => {
    const { server: s, port } = await server({ telefunc: order === 'after' })
    s.on('upgrade', appUpgrade)
    if (order === 'before') new Telefunc().installWebSocket(s)
    expect((await answer(port, requests["a WebSocket handshake for the app's own WebSocket server"])).status).toBe(418)
    expect((await answer(port, requests["Telefunc's WebSocket handshake"])).status).toBe(101)
  },
)

test('beside another upgrade listener that ignores a request, the request gets what it gets with that listener alone: nothing, and its socket stays open', async () => {
  const alone = await server({ telefunc: false })
  alone.server.on('upgrade', () => {})
  const beside = await server({ telefunc: true })
  beside.server.on('upgrade', () => {})
  const request = requests["a WebSocket handshake for a page the app doesn't have"]
  const appsAnswer = await answer(alone.port, request, 500)
  expect(appsAnswer).toEqual({ status: null, body: '', open: true })
  expect(await answer(beside.port, request, 500)).toEqual(appsAnswer)
})

test.runIf(nodeHasTheCallback)(
  "an app's own `shouldUpgradeCallback` still decides for the requests that aren't Telefunc's, and Telefunc's handshake opens",
  async () => {
    const { server: s, port } = await server({ telefunc: true, shouldUpgradeCallback: (req) => req.url === '/app-ws' })
    s.on('upgrade', appUpgrade)
    expect((await answer(port, requests["a WebSocket handshake for the app's own WebSocket server"])).status).toBe(418)
    expect(await answer(port, requests["a WebSocket handshake for the app's page"])).toMatchObject({
      status: 200,
      body: 'the page',
    })
    expect((await answer(port, requests["Telefunc's WebSocket handshake"])).status).toBe(101)
  },
)

test.runIf(nodeHasTheCallback)(
  "removing Telefunc's upgrade listener gives the server its own `shouldUpgradeCallback` back",
  async () => {
    const own = (req: IncomingMessage) => req.url === '/app-ws'
    const { server: s } = await server({ telefunc: true, shouldUpgradeCallback: own })
    s.removeAllListeners('upgrade')
    expect(hasProp(s, 'shouldUpgradeCallback') && s.shouldUpgradeCallback).toBe(own)
  },
)

// Node before 22.21 and 24.9 has no `shouldUpgradeCallback`: it hands every request with `Upgrade` to the `upgrade`
// listeners once there is one.
test.skipIf(nodeHasTheCallback)(
  "on a Node without `shouldUpgradeCallback`, beside Telefunc's WebSocket alone, a request with Upgrade that isn't Telefunc's is answered 404 and closed",
  async () => {
    const { port } = await server({ telefunc: true })
    expect(await answer(port, requests["a WebSocket handshake for a page the app doesn't have"])).toMatchObject({
      status: 404,
      open: false,
    })
    expect(await answer(port, requests['a page request that offers h2c'])).toMatchObject({ status: 404, open: false })
  },
)
