import { afterAll, beforeAll, expect, test } from 'vitest'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { connect, type Socket } from 'node:net'
import { once } from 'node:events'
import { setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'
import { Telefunc } from './node.js'
import { config } from '../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT, WIRE_MAX_RAW_FRAME_BYTES, WIRE_VERSION } from '../wire-protocol/constants.js'
import { encode } from '../wire-protocol/wire/frame.js'
import { pingDeadline } from '../wire-protocol/wire/pingDeadline.js'
import { nodeReadableToWebRequest } from '../utils/nodeReadableToWebRequest.js'

// The requests of a page's SSE wire carry the page's frames in their bodies: the one that opens the wire, and the
// stream-request upload. Once the wire is closed, the frame a body was bringing goes at once, and what the body still
// brings is read and dropped for one ping deadline at most, so that a page that ends its body hears the answer: past it,
// and past what Node buffers, the server reads no more of it, and lets its socket go, through Node's own request and
// through a Fetch `Request` built from it alike.

config.channel = { pingInterval: 1000 }
const DEADLINE = pingDeadline(1000, CHANNEL_TRANSPORT.SSE)
const telefunc = new Telefunc()
const telefuncPath = `/_telefunc?wire=${WIRE_VERSION}`

function u32(n: number): Uint8Array {
  const bytes = new Uint8Array(4)
  new DataView(bytes.buffer).setUint32(0, n)
  return bytes
}
const prefixed = (bytes: Uint8Array) => Buffer.concat([u32(bytes.length), bytes])
const metadata = (connId: string, kind: string) =>
  prefixed(new TextEncoder().encode(JSON.stringify({ connId, [kind]: true })))

/** Telefunc given Node's request (`serve({ req, res })`). */
function nodeRequest(req: IncomingMessage, res: ServerResponse) {
  telefunc.serve({ req, res }).catch(() => {})
}
/** Telefunc given a Fetch `Request` built from Node's request, as Hono's or srvx's Node server builds one. */
function fetchRequest(req: IncomingMessage, res: ServerResponse) {
  const { request } = nodeReadableToWebRequest(req, `http://127.0.0.1${telefuncPath}`, 'POST', req.headers)
  telefunc
    .serve({ request })
    .then(async (response) => {
      res.writeHead(response?.status ?? 404)
      for await (const chunk of response?.body ?? []) res.write(chunk)
      res.end()
    })
    .catch(() => {})
}
const servers = { 'Node request': nodeRequest, 'Fetch Request': fetchRequest }
type Via = keyof typeof servers
const vias: Via[] = ['Node request', 'Fetch Request']
const ports = new Map<Via, number>()
const listening: Server[] = []
const KEEP_ALIVE_TIMEOUT = 500
const requests = new Map<string, IncomingMessage>()

beforeAll(async () => {
  for (const via of vias) {
    const server = createServer((req, res) => {
      requests.set(String(req.headers['x-conn']), req)
      servers[via](req, res)
    })
    server.keepAliveTimeout = KEEP_ALIVE_TIMEOUT
    server.listen(0, '127.0.0.1')
    await once(server, 'listening')
    const address = server.address()
    if (address === null || typeof address === 'string') throw new Error('no port')
    ports.set(via, address.port)
    listening.push(server)
  }
})
afterAll(() => {
  for (const server of listening) server.closeAllConnections()
  for (const server of listening) server.close()
})

/** A POST of `kind` for `connId` from a socket of the test's own; `length` is its Content-Length. */
function post(
  via: Via,
  connId: string,
  kind: string,
  length: number,
  name = connId,
): { socket: Socket; text: () => string } {
  const port = ports.get(via)
  if (port === undefined) throw new Error('no server')
  const socket = connect(port, '127.0.0.1')
  socket.on('error', () => {})
  let text = ''
  socket.on('data', (chunk: Buffer) => void (text += chunk.toString('latin1')))
  socket.write(
    `POST ${telefuncPath} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-telefunc-request: sse\r\nx-conn: ${name}\r\nContent-Type: application/octet-stream\r\nContent-Length: ${length}\r\n\r\n`,
  )
  socket.write(metadata(connId, kind))
  return { socket, text: () => text }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const closes = {
  "the page's close POST": async (via: Via, connId: string) =>
    void post(via, connId, 'close', metadata(connId, 'close').length, `${connId}-close`),
  'a new open with the same connId': async (via: Via, connId: string) => {
    post(via, connId, 'streamResponse', 1_000_000_000, `${connId}-reopen`).socket.end()
  },
  'the ping deadline': async () => {},
}

for (const via of vias) {
  for (const [closeName, close] of Object.entries(closes)) {
    test(
      `after ${closeName}, nothing reads the open's body past one ping deadline, and its socket goes (${via})`,
      { timeout: 20_000 },
      async () => {
        const connId = crypto.randomUUID()
        const open = post(via, connId, 'streamResponse', 1_000_000_000)
        open.socket.write(prefixed(encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open: [] })))
        // A frame the page goes on sending: it declares the largest a frame may be.
        open.socket.write(u32(WIRE_MAX_RAW_FRAME_BYTES))
        const piece = new Uint8Array(16384).fill(120)
        // Silent until its wire ends, for the ping deadline to end it.
        let silent = closeName === 'the ping deadline'
        const trickle = setInterval(() => void (silent || open.socket.write(piece)), 100)
        try {
          while (!open.text().includes('data: ')) await sleep(10)
          await close(via, connId)
          // The wire's end is the end of its event stream, a chunked response's last chunk.
          while (!open.text().endsWith('0\r\n\r\n')) await sleep(10)
          silent = false
          // What Node buffers for a request nobody reads past the read-off, 64 KiB and as much again for a Fetch
          // `Request`'s body, fills in under a second.
          await sleep(DEADLINE + 2500)
          const req = requests.get(connId)
          const readAt = req?.socket.bytesRead ?? 0
          await sleep(2000)
          expect({ readSince: (req?.socket.bytesRead ?? 0) - readAt, released: req?.socket.destroyed }).toEqual({
            readSince: 0,
            released: true,
          })
        } finally {
          clearInterval(trickle)
          open.socket.destroy()
        }
      },
    )
  }
}

// A collection the test can force, as `node --expose-gc` gives.
setFlagsFromString('--expose-gc')
const gc: unknown = runInNewContext('gc')
/** The bytes of the process's `ArrayBuffer`s once what nothing holds is collected. */
async function heldBytes(): Promise<number> {
  if (typeof gc !== 'function') throw new Error('no gc')
  for (let i = 0; i < 4; i++) {
    await sleep(0)
    gc()
  }
  return process.memoryUsage().arrayBuffers
}

const PART = 32 * 1024 * 1024

for (const via of vias) {
  test(
    `once the ping deadline ends a wire whose open went silent mid-frame, the server lets that frame's bytes go (${via})`,
    { timeout: 20_000 },
    async () => {
      const connId = crypto.randomUUID()
      const open = post(via, connId, 'streamResponse', 1_000_000_000)
      open.socket.write(prefixed(encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open: [] })))
      open.socket.write(u32(WIRE_MAX_RAW_FRAME_BYTES))
      // Part of the frame, and then nothing.
      open.socket.write(new Uint8Array(PART).fill(120))
      try {
        while (!open.text().includes('data: ') || (requests.get(connId)?.socket.bytesRead ?? 0) < PART) await sleep(10)
        const before = await heldBytes()
        while (!open.text().endsWith('0\r\n\r\n')) await sleep(10)
        expect(before - (await heldBytes())).toBeGreaterThan(PART * 0.9)
      } finally {
        open.socket.destroy()
      }
    },
  )
}

/** A wire for `connId` whose open's body was its RECONCILE alone, once the server answered it. */
async function openedWire(via: Via, connId: string): Promise<{ socket: Socket; text: () => string }> {
  const reconcile = prefixed(encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open: [] }))
  const open = post(via, connId, 'streamResponse', metadata(connId, 'streamResponse').length + reconcile.length)
  open.socket.write(reconcile)
  while (!open.text().includes('data: ')) await sleep(10)
  return open
}

const UPLOADS = { 'stream-request': 'streamRequest', batch: 'batch' }

for (const via of vias) {
  for (const [uploadName, kind] of Object.entries(UPLOADS)) {
    test(
      `after the page's close POST, nothing reads its ${uploadName} body past one ping deadline, and its socket goes (${via})`,
      { timeout: 20_000 },
      async () => {
        const connId = crypto.randomUUID()
        const open = await openedWire(via, connId)
        const upload = post(via, connId, kind, 1_000_000_000, `${connId}-up`)
        // A frame the page goes on sending: it declares the largest a frame may be.
        upload.socket.write(u32(WIRE_MAX_RAW_FRAME_BYTES))
        const piece = new Uint8Array(16384).fill(120)
        const trickle = setInterval(() => void upload.socket.write(piece), 100)
        try {
          await sleep(500)
          await closes["the page's close POST"](via, connId)
          while (!open.text().endsWith('0\r\n\r\n')) await sleep(10)
          // What Node buffers for a request nobody reads past the read-off, 64 KiB and as much again for a Fetch
          // `Request`'s body, fills in under a second.
          await sleep(DEADLINE + 2500)
          const req = requests.get(`${connId}-up`)
          const readAt = req?.socket.bytesRead ?? 0
          await sleep(2000)
          expect({ readSince: (req?.socket.bytesRead ?? 0) - readAt, released: req?.socket.destroyed }).toEqual({
            readSince: 0,
            released: true,
          })
        } finally {
          clearInterval(trickle)
          open.socket.destroy()
          upload.socket.destroy()
        }
      },
    )

    test(
      `once the ping deadline ends a wire whose ${uploadName} went silent mid-frame, the server lets that frame's bytes go (${via})`,
      { timeout: 20_000 },
      async () => {
        const connId = crypto.randomUUID()
        const open = await openedWire(via, connId)
        const upload = post(via, connId, kind, 1_000_000_000, `${connId}-up`)
        upload.socket.write(u32(WIRE_MAX_RAW_FRAME_BYTES))
        // Part of the frame, and then nothing.
        upload.socket.write(new Uint8Array(PART).fill(120))
        try {
          while ((requests.get(`${connId}-up`)?.socket.bytesRead ?? 0) < PART) await sleep(10)
          const before = await heldBytes()
          while (!open.text().endsWith('0\r\n\r\n')) await sleep(10)
          expect(before - (await heldBytes())).toBeGreaterThan(PART * 0.9)
        } finally {
          open.socket.destroy()
          upload.socket.destroy()
        }
      },
    )

    test(
      `a ${uploadName} cut mid-frame as the server refused the wire is answered that it was refused one ping deadline after, not before (${via})`,
      { timeout: 20_000 },
      async () => {
        const connId = crypto.randomUUID()
        const open = await openedWire(via, connId)
        const upload = post(via, connId, kind, 1_000_000_000, `${connId}-up`)
        upload.socket.write(u32(WIRE_MAX_RAW_FRAME_BYTES))
        upload.socket.write(new Uint8Array(16384).fill(120))
        try {
          await sleep(500)
          // A frame past the largest a frame may be: the server refuses the wire.
          const oversize = u32(WIRE_MAX_RAW_FRAME_BYTES + 1)
          post(
            via,
            connId,
            'batch',
            metadata(connId, 'batch').length + oversize.length,
            `${connId}-batch`,
          ).socket.write(oversize)
          while (!open.text().endsWith('0\r\n\r\n')) await sleep(10)
          await sleep(DEADLINE - 500)
          const before = upload.text()
          await sleep(1500)
          expect([before, upload.text().split('\r\n')[0]]).toEqual(['', 'HTTP/1.1 400 Bad Request'])
        } finally {
          open.socket.destroy()
          upload.socket.destroy()
        }
      },
    )
  }
}
