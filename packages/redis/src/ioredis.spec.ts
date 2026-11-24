// The facts of ioredis the backend's requests rest on (`StoreRequest`), each against a server on loopback that answers
// every command with its own name and records the frames it got.
import { createServer, type Server, type Socket } from 'node:net'
import { Redis } from 'ioredis'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { sendRequest, StoreRequest } from './ioredis.js'

const servers: Server[] = []
const clients: Redis[] = []
afterEach(() => {
  for (const client of clients.splice(0)) client.disconnect()
  for (const server of servers.splice(0)) server.close()
  vi.restoreAllMocks()
})

/** A server that answers each command with its name as a bulk string, and the frames it got, each as its arguments. */
async function namingServer(): Promise<{ port: number; frames: Buffer[][] }> {
  const frames: Buffer[][] = []
  const server = createServer((socket) => {
    socket.on('error', () => {})
    let pending = Buffer.alloc(0)
    socket.on('data', (data) => {
      pending = Buffer.concat([pending, data])
      for (let frame = parseFrame(pending); frame !== null; frame = parseFrame(pending)) {
        pending = pending.subarray(frame.length)
        frames.push(frame.args)
        answer(socket, frame.args)
      }
    })
  })
  servers.push(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('the server has no port')
  return { port: address.port, frames }
}

/** The handshake's INFO gets a ready server's; every other command its own name. */
function answer(socket: Socket, args: Buffer[]): void {
  const name = String(args[0] ?? '').toLowerCase()
  const reply = name === 'info' ? 'loading:0\r\n' : name
  socket.write(`$${Buffer.byteLength(reply)}\r\n${reply}\r\n`)
}

/** The first whole RESP array of bulk strings in `data`, and its length in bytes; null until it is all there. */
function parseFrame(data: Buffer): { args: Buffer[]; length: number } | null {
  const line = (at: number) => {
    const end = data.indexOf('\r\n', at)
    return end === -1 ? null : { text: data.toString('latin1', at, end), next: end + 2 }
  }
  const head = line(0)
  if (head === null) return null
  const args: Buffer[] = []
  let at = head.next
  for (let i = 0; i < Number(head.text.slice(1)); i++) {
    const size = line(at)
    if (size === null) return null
    const end = size.next + Number(size.text.slice(1))
    if (data.length < end + 2) return null
    args.push(data.subarray(size.next, end))
    at = end + 2
  }
  return { args, length: at }
}

function connected(port: number, options: { enableOfflineQueue?: boolean } = {}): Redis {
  const client = new Redis({ host: '127.0.0.1', port, lazyConnect: true, enableReadyCheck: false, ...options })
  client.on('error', () => {})
  clients.push(client)
  return client
}

/** The sockets requests were serialized onto. */
function serializations(): object[] {
  const sockets: object[] = []
  const { toWritable } = StoreRequest.prototype
  vi.spyOn(StoreRequest.prototype, 'toWritable').mockImplementation(function (this: StoreRequest, socket) {
    sockets.push(socket)
    return toWritable.call(this, socket)
  })
  return sockets
}

describe('the facts of ioredis that a request rests on', () => {
  it('ioredis serializes a request onto the socket it writes it to, a ready one and one drained from its queue', async () => {
    const { port } = await namingServer()
    const client = connected(port, { enableOfflineQueue: true })
    const sockets = serializations()
    // Sent before the client connects, which it starts: ioredis queues it and writes it once connected.
    const ready = new Promise((resolve) => client.once('ready', resolve))
    const queued = sendRequest(client, new StoreRequest({ command: 'get' }, ['a'], false))
    await ready
    const direct = sendRequest(client, new StoreRequest({ command: 'get' }, ['b'], false))

    expect(await Promise.all([queued, direct])).toEqual(['get', 'get'])
    expect(sockets).toHaveLength(2)
    expect(sockets.every((socket) => socket === client.stream)).toBe(true)
  })

  it("a request over a slice reaches the socket slice by slice, the frame whole, and the next reply is the next request's", async () => {
    const { port, frames } = await namingServer()
    const client = connected(port)
    await client.connect()
    const writes: number[] = []
    const { write } = client.stream
    vi.spyOn(client.stream, 'write').mockImplementation(function (this: typeof client.stream, ...args) {
      const [chunk] = args
      writes.push(typeof chunk === 'string' || Buffer.isBuffer(chunk) ? Buffer.byteLength(chunk) : -1)
      return write.apply(this, args)
    })
    const value = Buffer.alloc(3 * 1024 * 1024 + 7, 'v')
    const set = sendRequest(client, new StoreRequest({ command: 'set' }, ['k', value], false))
    const get = sendRequest(client, new StoreRequest({ command: 'get' }, ['k'], true))

    expect(await set).toBe('set')
    expect(await get).toEqual(Buffer.from('get'))
    const [setFrame] = frames.slice(-2)
    expect(setFrame?.[2]?.equals(value)).toBe(true)
    // Three slices of 1 MiB and the rest of the SET, then the GET.
    const frame = Buffer.byteLength(`*3\r\n$3\r\nset\r\n$1\r\nk\r\n$${value.length}\r\n\r\n`) + value.length
    expect(writes).toHaveLength(5)
    expect(writes.slice(0, 3)).toEqual([1024 * 1024, 1024 * 1024, 1024 * 1024])
    expect(writes.slice(0, 4).reduce((sum, bytes) => sum + bytes, 0)).toBe(frame)
  })
})
