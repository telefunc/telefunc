// On Deno, a call that returns a Channel answers while its body is still being sent, and Telefunc reads the rest of that
// body on past the answer. These rows run such calls on real Deno, in both of its signal modes: without
// `--unstable-no-legacy-abort`, Deno aborts `request.signal` at every response; with it, only once the client left.
// Through `telefunc/deno`, through `serve({ request })` (answered with its stream, or with its string body), and on Deno's
// `node:http` through `serve({ readable })` (as an Express app under Deno) and through `serve({ request })` with a
// framework's own `Request`, both answered with their string body:
// - a File sent on after the answer and read later is read whole;
// - a 32 MiB File left unread is taken whole, so that a browser gets the answer, and the channel closes by its connect
//   window;
// - a File sent whole and read once its channel closed, while its client is still there, fails as the server having
//   answered before reading it, not as its client leaving;
// - where the client leaves after the answer, the channel closes by its connect window (`config.channel.connectTtl`,
//   1 s here), and the File's read fails as its client's leaving, or, where nothing says it left, as the channel closes.
// The server is deno.clientLeft.server.fixture.mjs, the call deno.clientLeft.client.fixture.ts; CI's Vitest job has
// Deno.

import { afterAll, beforeAll, describe, expect, onTestFinished, test } from 'vitest'
import { spawn, type ChildProcess } from 'node:child_process'
import type { Socket } from 'node:net'
import { once } from 'node:events'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { WIRE_VERSION } from '../wire-protocol/constants.js'
import { isObject } from '../utils/isObject.js'
import { PART, part, startCall } from './deno.clientLeft.client.fixture.js'

const here = dirname(fileURLToPath(import.meta.url))
const file = '/deno.clientLeft.telefunc.js'
const telefuncPath = `/_telefunc?wire=${WIRE_VERSION}`
type Mode = 'legacy' | 'modern'
/** The ports of a Deno server: `Deno.serve`'s, and its `node:http` server's. */
type Ports = { deno: number; node: number }
const servers: ChildProcess[] = []
const ports: Record<Mode, Ports> = { legacy: { deno: 0, node: 0 }, modern: { deno: 0, node: 0 } }

/** Starts the Deno server in `mode`; resolves with its ports once it listens. */
async function startServer(mode: Mode): Promise<{ server: ChildProcess; ports: Ports }> {
  const flags = mode === 'modern' ? ['--unstable-no-legacy-abort'] : []
  const server = spawn(
    'deno',
    ['run', '-A', '--sloppy-imports', '--node-modules-dir=manual', ...flags, 'deno.clientLeft.server.fixture.mjs'],
    { cwd: here, stdio: ['ignore', 'pipe', 'inherit'] },
  )
  const listening = await new Promise<Ports>((resolve, reject) => {
    let out = ''
    server.stdout?.on('data', (data: Buffer) => {
      out += data.toString()
      const line = /LISTENING (\d+) (\d+)/.exec(out)
      if (line) resolve({ deno: Number(line[1]), node: Number(line[2]) })
    })
    server.once('exit', (code) => reject(new Error(`deno exited with ${code}: ${out}`)))
  })
  return { server, ports: listening }
}

beforeAll(async () => {
  for (const mode of ['legacy', 'modern'] as const) {
    const started = await startServer(mode)
    servers.push(started.server)
    ports[mode] = started.ports
  }
}, 60_000)
afterAll(() => {
  for (const server of servers) server.kill()
})

const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

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
  node: "serve({ readable }) on Deno's node:http and its string body",
  framework: "serve({ request }) with a framework's Request on Deno's node:http and its string body",
}
/** The port a call through `entry` goes to. */
const portOf = (served: Ports, entry: string) => (entry === 'node' || entry === 'framework' ? served.node : served.deno)

for (const mode of ['legacy', 'modern'] as const) {
  describe(`Deno, ${mode === 'legacy' ? 'its signal aborted at every response' : '`--unstable-no-legacy-abort`'}`, () => {
    for (const [entry, through] of Object.entries(entries)) {
      test(`a call that returns a Channel and keeps its File, most of it sent after the answer, read 1 s later: the File is read whole, and the channel closes by its connect window (${through})`, async () => {
        const row = `${mode}-${entry}-late`
        const socket = await startCall(portOf(ports[mode], entry), entry, row, 8, 1000)
        for (let sent = 1; sent < 8; sent++) {
          await pause(40)
          socket.write(part())
        }
        await pause(2500)
        const state = await rowState(ports[mode].deno, row)
        socket.destroy()
        expect(state).toEqual({ read: `read ${8 * PART} bytes`, closed: closedByTtl })
      }, 15_000)

      test(`a call that returns a Channel and leaves its 32 MiB File unread: the server takes the whole upload, and the channel closes by its connect window (${through})`, async () => {
        const row = `${mode}-${entry}-big`
        const socket = await startCall(portOf(ports[mode], entry), entry, row, 512, null)
        const sent = await sendAll(socket, 511)
        // The connect window counts from the upload's end: the channel closes by it once the server has taken the upload.
        await pause(2500)
        const state = await rowState(ports[mode].deno, row)
        socket.destroy()
        expect({ sent, ...state }).toEqual({ sent: 'all sent', read: 'not read', closed: closedByTtl })
      }, 15_000)

      test(`a call that returns a Channel and keeps its File, sent whole, read once its channel closed, its client still there: the read fails as the server having answered first, not as its client leaving (${through})`, async () => {
        const row = `${mode}-${entry}-kept`
        const socket = await startCall(portOf(ports[mode], entry), entry, row, 8, 2000)
        const sent = await sendAll(socket, 7)
        await pause(3000)
        const state = await rowState(ports[mode].deno, row)
        socket.destroy()
        expect({ sent, ...state }).toEqual({
          sent: 'all sent',
          read: 'failed: The server answered before reading the request body to its end',
          closed: closedByTtl,
        })
      }, 15_000)

      test(`a call that returns a Channel and reads its File, whose client leaves after the answer: the read fails as its client left, and the channel closes by its connect window (${through})`, async () => {
        const row = `${mode}-${entry}-after`
        // A server of its own, whose process makes the call and leaves (deno.clientLeft.server.fixture.mjs,
        // `callAndLeave`): behind other rows on its server, or with its client in another process, Deno at times fails
        // the read on its own as the client leaves, which hides a read left pending.
        const own = await startServer(mode)
        onTestFinished(() => void own.server.kill())
        const left = await fetch(`http://127.0.0.1:${own.ports.deno}/leave?entry=${entry}&row=${row}`)
        const leaving = await left.text()
        await pause(2000)
        expect({ leaving, ...(await rowState(own.ports.deno, row)) }).toEqual({
          leaving: 'left',
          // A framework's own `Request` says nothing Telefunc can tell from Deno's end of the response, so its read ends
          // as the channel closes and the server lets the body go.
          read:
            entry === 'framework'
              ? 'failed: The server answered before reading the request body to its end'
              : 'failed: The client disconnected before the end of the request body',
          closed: closedByTtl,
        })
      }, 15_000)
    }
  })
}
