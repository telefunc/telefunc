import { afterAll, beforeAll, expect, onTestFinished, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import { connect } from 'node:net'
import { once } from 'node:events'
import { Telefunc } from './node.js'
import { getChannelMux } from '../wire-protocol/channel/server/ChannelMux.js'
import { WIRE_VERSION, WS_CLOSE_WIRE_VERSION } from '../wire-protocol/constants.js'

let server: Server
let host: string

beforeAll(async () => {
  server = createServer((_req, res) => res.writeHead(404).end())
  new Telefunc().installWebSocket(server)
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  host = `127.0.0.1:${address.port}`
})
afterAll(() => server.close())

/** The process's unhandled rejections and uncaught exceptions while the test runs: either would end an app's server.
 *  `crashed` settles at the first. */
function recordCrashes(): { crashes: unknown[]; crashed: Promise<void> } {
  const crashes: unknown[] = []
  const { promise: crashed, resolve } = Promise.withResolvers<void>()
  const record = (err: unknown) => {
    crashes.push(err)
    resolve()
  }
  process.on('unhandledRejection', record)
  process.on('uncaughtExceptionMonitor', record)
  onTestFinished(() => {
    process.off('unhandledRejection', record)
    process.off('uncaughtExceptionMonitor', record)
  })
  return { crashes, crashed }
}

/** The status a raw WebSocket handshake for `target` with `hostHeader` is answered with. */
async function handshake(target: string, hostHeader: string | null): Promise<number> {
  const socket = connect(Number(host.split(':')[1]), '127.0.0.1')
  await once(socket, 'connect')
  socket.write(
    `GET ${target} HTTP/1.1\r\n${hostHeader === null ? '' : `Host: ${hostHeader}\r\n`}Connection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n`,
  )
  const [head] = (await once(socket, 'data')) as [Buffer]
  // A wire that opened stays open until the test's end: past its 101, the server reads it.
  await new Promise((resolve) => setTimeout(resolve, 50))
  socket.destroy()
  return Number(head.toString('latin1').split(' ')[1])
}

/** A page's WebSocket to the server, with the wire version `version` in its URL. */
async function openWire(version = WIRE_VERSION): Promise<WebSocket> {
  const ws = new WebSocket(`ws://${host}/_telefunc?wire=${version}`)
  ws.binaryType = 'arraybuffer'
  await once(ws, 'open')
  return ws
}

test.each([
  ['a list', () => `${host}, other.example`],
  ['a space', () => 'a b'],
  ['an IPv6 address with no closing bracket', () => '[::1'],
  ['a port past 65535', () => '127.0.0.1:99999'],
  ['credentials', () => `user@${host}`],
  ['a path', () => `${host}/x`],
  ['nothing', () => ''],
])('a WebSocket handshake whose Host has %s is answered 400, and the server goes on', async (_, hostHeader) => {
  const { crashes } = recordCrashes()
  expect(await handshake(`/_telefunc?wire=${WIRE_VERSION}`, hostHeader())).toBe(400)
  expect(await handshake(`/_telefunc?wire=${WIRE_VERSION}`, host)).toBe(101)
  expect(crashes).toEqual([])
})

test('a WebSocket handshake with no Host header is answered 400, and the server goes on', async () => {
  const { crashes } = recordCrashes()
  expect(await handshake(`/_telefunc?wire=${WIRE_VERSION}`, null)).toBe(400)
  expect(await handshake(`/_telefunc?wire=${WIRE_VERSION}`, host)).toBe(101)
  expect(crashes).toEqual([])
})

test('a WebSocket handshake whose request target is a whole URL opens, read by its path', async () => {
  const { crashes } = recordCrashes()
  expect(await handshake(`http://other.example/_telefunc?wire=${WIRE_VERSION}`, host)).toBe(101)
  expect(crashes).toEqual([])
})

test('a page on another wire version is told to reload: its WebSocket opens, then closes with the code a page reloads on', async () => {
  const ws = await openWire(WIRE_VERSION + 1)
  const [event] = (await once(ws, 'close')) as [CloseEvent]
  expect(event.code).toBe(WS_CLOSE_WIRE_VERSION)
  expect(event.reason).toBe('reload the page')
})

test.each([
  ['opens', 'onWireOpen', (_ws: WebSocket) => {}],
  ['receives a message', 'onWireMessage', (ws: WebSocket) => ws.send(new Uint8Array([0]))],
] as const)(
  'a wire whose handling throws when it %s is ended, the error is printed, and the server goes on',
  async (_, method, act) => {
    const { crashes, crashed } = recordCrashes()
    const printed = vi.spyOn(console, 'error').mockImplementation(() => {})
    onTestFinished(() => printed.mockRestore())
    const failure = new Error(`a failure in ${method}`)
    // Replaced, not spied on: a spy handles the promise its mock returns, which would hide the rejection.
    const mux = getChannelMux()
    const { onWireOpen, onWireMessage } = mux
    const restore = () => {
      mux.onWireOpen = onWireOpen
      mux.onWireMessage = onWireMessage
    }
    onTestFinished(restore)
    if (method === 'onWireOpen')
      mux.onWireOpen = () => {
        restore()
        throw failure
      }
    else
      mux.onWireMessage = () => {
        restore()
        return Promise.reject(failure)
      }
    const ws = await openWire()
    act(ws)
    // Without a catch, the throw ends the process instead of the wire.
    await Promise.race([once(ws, 'close'), crashed])
    expect(crashes).toEqual([])
    expect(printed.mock.calls.map((call) => String(call[0]))).toEqual([expect.stringContaining(failure.message)])
    const next = await openWire()
    next.close()
  },
)
