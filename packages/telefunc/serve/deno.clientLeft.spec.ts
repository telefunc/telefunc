// On Deno, a call that returns a Channel answers while its body is still being sent, and Telefunc reads the rest of that
// body on past the answer. These rows run such calls on real Deno, in both of its signal modes: without
// `--unstable-no-legacy-abort`, Deno aborts `request.signal` at every response; with it, only once the client left.
// Through `telefunc/deno` and through `serve({ request })` (answered with its stream, or with its string body):
// - a File sent on after the answer and read later is read whole;
// - a 32 MiB File left unread is taken whole, so that a browser gets the answer, and the channel closes by its connect
//   window;
// - where the client leaves after the answer, the File's read fails as its client's leaving, and the channel closes by
//   its connect window (`config.channel.connectTtl`, 1 s here).
// The server is deno.clientLeft.server.fixture.mjs; CI's Vitest job has Deno.

import { afterAll, beforeAll, describe, expect, onTestFinished, test } from 'vitest'
import { spawn, type ChildProcess } from 'node:child_process'
import { connect, type Socket } from 'node:net'
import { once } from 'node:events'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { WIRE_VERSION } from '../wire-protocol/constants.js'
import { isObject } from '../utils/isObject.js'

const here = dirname(fileURLToPath(import.meta.url))
const file = '/deno.clientLeft.telefunc.js'
const telefuncPath = `/_telefunc?wire=${WIRE_VERSION}`
const encoder = new TextEncoder()
const PART = 64 * 1024
type Mode = 'legacy' | 'modern'
const servers: ChildProcess[] = []
const ports = { legacy: 0, modern: 0 }

/** Starts the Deno server in `mode`; resolves with its port once it listens. */
async function startServer(mode: Mode): Promise<{ server: ChildProcess; port: number }> {
  const flags = mode === 'modern' ? ['--unstable-no-legacy-abort'] : []
  const server = spawn(
    'deno',
    ['run', '-A', '--sloppy-imports', '--node-modules-dir=manual', ...flags, 'deno.clientLeft.server.fixture.mjs'],
    { cwd: here, stdio: ['ignore', 'pipe', 'inherit'] },
  )
  const port = await new Promise<number>((resolve, reject) => {
    let out = ''
    server.stdout?.on('data', (data: Buffer) => {
      out += data.toString()
      const listening = /LISTENING (\d+)/.exec(out)
      if (listening) resolve(Number(listening[1]))
    })
    server.once('exit', (code) => reject(new Error(`deno exited with ${code}: ${out}`)))
  })
  return { server, port }
}

beforeAll(async () => {
  for (const mode of ['legacy', 'modern'] as const) {
    const { server, port } = await startServer(mode)
    servers.push(server)
    ports[mode] = port
  }
}, 60_000)
afterAll(() => {
  for (const server of servers) server.kill()
})

const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const part = () => new Uint8Array(PART).fill(120)

/** A socket that has sent the head and envelope of a binary call to `keepsFile` with a File of `parts` × 64 KiB, and its
 *  first part: the rest of the File's bytes are the row's to send. Resolves once the answer begins. */
async function startCall(port: number, entry: string, row: string, parts: number, readAfterMs: number | null) {
  const size = parts * PART
  const metadata = encoder.encode(
    JSON.stringify({
      file,
      name: 'keepsFile',
      args: [`!TelefuncBlob:${JSON.stringify({ index: 0, size, type: '' })}`, row, readAfterMs],
    }),
  )
  const start = new Uint8Array(4 + metadata.length)
  new DataView(start.buffer).setUint32(0, metadata.length)
  start.set(metadata, 4)
  const socket = connect(port, '127.0.0.1')
  socket.on('error', () => {})
  await once(socket, 'connect')
  const answered = new Promise<void>((resolve) =>
    socket.once('data', () => {
      socket.resume()
      resolve()
    }),
  )
  socket.write(
    `POST ${telefuncPath} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-entry: ${entry}\r\nx-telefunc-request: bin\r\nContent-Type: application/octet-stream\r\nContent-Length: ${start.length + size}\r\n\r\n`,
  )
  socket.write(start)
  socket.write(part())
  await answered
  return socket
}

/** Whether `socket` takes `parts` more parts, each write drained within 5 s, or where it stalls. */
async function sendAll(socket: Socket, parts: number): Promise<string> {
  for (let sent = 0; sent < parts; sent++) {
    if (socket.write(part())) continue
    const drained = await Promise.race([once(socket, 'drain').then(() => true), pause(5000).then(() => false)])
    if (!drained) return `stalled after ${sent + 1} of ${parts} parts`
  }
  return 'all sent'
}

/** How `row`'s File read and channel ended so far, as the server says, with the channel's close time left out. */
async function rowState(port: number, row: string): Promise<{ read: string; closed: string }> {
  const response = await fetch(`http://127.0.0.1:${port}${telefuncPath}`, {
    method: 'POST',
    headers: { 'x-entry': 'adapter', 'x-telefunc-request': 'txt' },
    body: JSON.stringify({ file, name: 'rowState', args: [row] }),
  })
  const body: unknown = await response.json()
  const ret = isObject(body) ? body.ret : undefined
  if (!isObject(ret) || typeof ret.read !== 'string' || typeof ret.closed !== 'string')
    throw new Error(`no row state: ${JSON.stringify(body)}`)
  return { read: ret.read, closed: ret.closed.replace(/closed at \d+ ms/, 'closed') }
}

const closedByTtl = 'closed: Channel timed out: no client connected within TTL after response was sent'
const entries = {
  adapter: 'telefunc/deno',
  stream: 'serve({ request }) and its stream',
  string: 'serve({ request }) and its string body',
}

for (const mode of ['legacy', 'modern'] as const) {
  describe(`Deno, ${mode === 'legacy' ? 'its signal aborted at every response' : '`--unstable-no-legacy-abort`'}`, () => {
    for (const [entry, through] of Object.entries(entries)) {
      test(`a call that returns a Channel and keeps its File, most of it sent after the answer, read 1 s later: the File is read whole, and the channel closes by its connect window (${through})`, async () => {
        const row = `${mode}-${entry}-late`
        const socket = await startCall(ports[mode], entry, row, 8, 1000)
        for (let sent = 1; sent < 8; sent++) {
          await pause(40)
          socket.write(part())
        }
        await pause(2500)
        const state = await rowState(ports[mode], row)
        socket.destroy()
        expect(state).toEqual({ read: `read ${8 * PART} bytes`, closed: closedByTtl })
      }, 15_000)

      test(`a call that returns a Channel and leaves its 32 MiB File unread: the server takes the whole upload, and the channel closes by its connect window (${through})`, async () => {
        const row = `${mode}-${entry}-big`
        const socket = await startCall(ports[mode], entry, row, 512, null)
        const sent = await sendAll(socket, 511)
        // The connect window counts from the upload's end: the channel closes by it once the server has taken the upload.
        await pause(2500)
        const state = await rowState(ports[mode], row)
        socket.destroy()
        expect({ sent, ...state }).toEqual({ sent: 'all sent', read: 'not read', closed: closedByTtl })
      }, 15_000)

      test(`a call that returns a Channel and reads its File, whose client leaves after the answer: the read fails as its client left, and the channel closes by its connect window (${through})`, async () => {
        const row = `${mode}-${entry}-after`
        // A server of its own, whose process makes the call and leaves (deno.clientLeft.server.fixture.mjs,
        // `callAndLeave`): behind other rows on its server, or with its client in another process, Deno at times fails
        // the read on its own as the client leaves, which hides a read left pending.
        const { server, port } = await startServer(mode)
        onTestFinished(() => void server.kill())
        const left = await fetch(`http://127.0.0.1:${port}/leave?entry=${entry}&row=${row}`)
        const leaving = await left.text()
        await pause(2000)
        expect({ leaving, ...(await rowState(port, row)) }).toEqual({
          leaving: 'left',
          read: 'failed: The client disconnected before the end of the request body',
          closed: closedByTtl,
        })
      }, 15_000)
    }
  })
}
