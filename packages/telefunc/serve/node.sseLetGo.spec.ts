// A page's streaming upload through the Node adapter (`serve({ req, res })`), idle once the server ended its session:
// one ping deadline after the end the server lets go of its body and answers it `retry`. Node's request is then as its
// owner had it: Telefunc's own reader was its only reader, and nothing else listens to it or resumes it. Each row waits
// for that answer, then lets the event loop run a counted number of turns (`settle`) and reads Node's request.

import { afterAll, beforeAll, expect, test } from 'vitest'
import { createServer, type IncomingMessage, type Server } from 'node:http'
import { connect, type Socket } from 'node:net'
import { once } from 'node:events'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT, WIRE_VERSION } from '../wire-protocol/constants.js'
import { encodeSseRequest, encodeSseRequestMetadata } from '../wire-protocol/wire/envelope.js'
import { encode } from '../wire-protocol/wire/frame.js'
import { pingDeadline } from '../wire-protocol/wire/pingDeadline.js'

const PING_INTERVAL = 1_000
config.channel = { pingInterval: PING_INTERVAL }
const DEADLINE = pingDeadline(PING_INTERVAL, CHANNEL_TRANSPORT.SSE)
const telefunc = new Telefunc()
const telefuncPath = `/_telefunc?wire=${WIRE_VERSION}`

/** Whether the app pauses the upload's request before it hands it to Telefunc. */
let pausesUpload = false
/** The upload's request, as the server took it. */
let upload: Promise<IncomingMessage> = new Promise(() => {})
let tookUpload: (req: IncomingMessage) => void = () => {}
let server: Server
let base: string
let port: number
beforeAll(async () => {
  server = createServer((req, res) => {
    if (req.headers['x-row'] === 'upload') {
      if (pausesUpload) req.pause()
      tookUpload(req)
    }
    telefunc.serve({ req, res }).catch(() => {})
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  port = address.port
  base = `http://127.0.0.1:${port}${telefuncPath}`
})
afterAll(() => {
  server.close()
})

/** The event loop's queued work, run to its end: `turns` macrotask turns, each after its microtasks and I/O. */
async function settle(turns = 100) {
  for (let i = 0; i < turns; i++) await new Promise((resolve) => setImmediate(resolve))
}

function post(body: BodyInit) {
  return fetch(base, { method: 'POST', headers: { 'x-telefunc-request': 'sse' }, body })
}

/** A page's wire, and its streaming upload on a socket of the row's own, idle once it is sent; then the server ends
 *  the session. Resolves once the upload is answered, with its request, its socket and its answer's status. */
async function uploadLetGo() {
  upload = new Promise((resolve) => (tookUpload = resolve))
  const connId = crypto.randomUUID()
  const wire = await post(
    encodeSseRequest({ connId, streamResponse: true }, [
      encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open: [] }),
    ]),
  )
  void wire.body?.pipeTo(new WritableStream()).catch(() => {})
  const socket: Socket = connect(port, '127.0.0.1')
  socket.on('error', () => {})
  const answered = new Promise<number>((resolve) => {
    socket.once('data', (chunk: Buffer) => resolve(Number(chunk.toString('latin1').split(' ', 2)[1])))
  })
  await once(socket, 'connect')
  socket.write(
    `POST ${telefuncPath} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-row: upload\r\nx-telefunc-request: sse\r\nContent-Type: application/octet-stream\r\nContent-Length: 100000\r\n\r\n`,
  )
  socket.write(encodeSseRequestMetadata({ connId, streamRequest: true }))
  const req = await upload
  await settle()
  // A frame only a server sends: the server ends the session.
  await post(encodeSseRequest({ connId }, [encode.attachResult(999, null)]))
  const status = await answered
  await settle()
  return { req, socket, status }
}

test(
  "R8. a page's upload the server let go of: Node's request has no `'data'` listener and isn't flowing",
  async () => {
    pausesUpload = false
    const { req, socket, status } = await uploadLetGo()
    expect({ status, dataListeners: req.listenerCount('data'), flowing: req.readableFlowing === true }).toEqual({
      status: 400,
      dataListeners: 0,
      flowing: false,
    })
    socket.destroy()
  },
  DEADLINE * 4,
)

test(
  "R9. a page's upload whose app paused its request before Telefunc: still paused after the let-go, and what the page sends then stays unread",
  async () => {
    pausesUpload = true
    const { req, socket, status } = await uploadLetGo()
    socket.write('x'.repeat(10))
    await settle()
    expect({ status, paused: req.isPaused(), unread: req.readableLength }).toEqual({
      status: 400,
      paused: true,
      unread: 10,
    })
    socket.destroy()
  },
  DEADLINE * 4,
)
