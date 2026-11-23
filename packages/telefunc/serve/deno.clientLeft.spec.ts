// On Deno, a call that returns a Channel answers while its body is still being sent, and Telefunc reads the rest of that
// body on past the answer. These rows run such calls on real Deno, in both of its signal modes: without
// `--unstable-no-legacy-abort`, Deno aborts `request.signal` at every response; with it, only once the client left.
// Through `telefunc/deno`, through `serve({ request })` (answered with its stream, or with its string body), and on Deno's
// `node:http` through `serve({ readable })` (as an Express app under Deno) and through `serve({ request })` with a
// framework's own `Request`, both answered with their string body:
// - a File sent on after the answer and read later is read whole;
// - a 32 MiB File left unread is taken whole, so that a browser gets the answer, and the channel closes by its connect
//   window;
// - a File sent whole and read once its channel closed, while its client is still there, is read whole, and so is one a
//   plain call reads after it returned: a File keeps its bytes past its call;
// - where the client leaves after the answer, the channel closes by its connect window (`config.channel.connectTtl`,
//   1 s here), and the File's read fails as its client's leaving: where nothing says it left, its upload, stopped short
//   for a whole window, is given up on as the channel closes.
// Each row is one call, which the spec hands as data to the Deno server (`run`). Its client there (`startCall`,
// deno.clientLeft.client.fixture.ts) sends it to its entry's own port, and the server answers with what the row's
// telefunction saw once its File's read and its channel ended (`seen`, deno.clientLeft.telefunc.js). The server is
// deno.clientLeft.server.fixture.mjs; CI's Vitest job has Deno.
// - entry: how an app on Deno hands Telefunc a request (`entries`, five);
// - signal mode: Deno with or without `--unstable-no-legacy-abort`, per process;
// - row: one call, its telefunction, a File of `parts` × 64 KiB, `readAfterMs`, and its `rest`;
// - rest: what the client does after the File's first part: send the other parts, or leave;
// - seen: what the telefunction saw, how its File's read ended and how its channel closed;
// - window: `WINDOW_MS`, `config.channel.connectTtl`.

import { afterAll, beforeAll, describe, expect, onTestFinished, test } from 'vitest'
import { spawn, type ChildProcess } from 'node:child_process'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { isObject } from '../utils/isObject.js'
import { PART, WINDOW_MS, type Row } from './deno.clientLeft.client.fixture.js'

const here = dirname(fileURLToPath(import.meta.url))
type Mode = 'legacy' | 'modern'
/** The entries a request goes through, each on a port of its own of the Deno server. */
const entries = {
  adapter: 'telefunc/deno',
  stream: 'serve({ request }) and its stream',
  string: 'serve({ request }) and its string body',
  node: "serve({ readable }) on Deno's node:http and its string body",
  framework: "serve({ request }) with a framework's Request on Deno's node:http and its string body",
}
/** A Deno server: its process, and the port of its control route (`POST /run`). */
type Served = { server: ChildProcess; control: number }
const served: Partial<Record<Mode, Served>> = {}

/** Starts the Deno server in `mode`; resolves once every entry listens. */
async function startServer(mode: Mode): Promise<Served> {
  const flags = mode === 'modern' ? ['--unstable-no-legacy-abort'] : []
  const server = spawn(
    'deno',
    ['run', '-A', '--sloppy-imports', '--node-modules-dir=manual', ...flags, 'deno.clientLeft.server.fixture.mjs'],
    { cwd: here, stdio: ['ignore', 'pipe', 'inherit'] },
  )
  const ready = await new Promise<string>((resolve, reject) => {
    let out = ''
    server.stdout?.on('data', (data: Buffer) => {
      out += data.toString()
      const line = /READY (.*)\n/.exec(out)?.[1]
      if (line !== undefined) resolve(line)
    })
    server.once('error', reject)
    server.once('exit', (code) => reject(new Error(`deno exited with ${code}: ${out}`)))
  })
  const parsed: unknown = JSON.parse(ready)
  if (!isObject(parsed) || typeof parsed.control !== 'number' || !isObject(parsed.entries))
    throw new Error(`no READY line: ${ready}`)
  // An entry the server lacks, or one this spec doesn't run, fails here rather than as a row.
  expect(Object.keys(parsed.entries).sort()).toEqual(Object.keys(entries).sort())
  return { server, control: parsed.control }
}

beforeAll(async () => {
  for (const mode of ['legacy', 'modern'] as const) served[mode] = await startServer(mode)
}, 60_000)
afterAll(() => {
  for (const one of Object.values(served)) one.server.kill()
})

/** Runs `row` through `entry` on `on`'s Deno server: how its client's rest went, and what its telefunction saw. */
async function run(
  on: Served | undefined,
  entry: string,
  row: Row,
): Promise<{ sent: string; read: string; closed: string }> {
  if (on === undefined) throw new Error('no Deno server')
  const response = await fetch(`http://127.0.0.1:${on.control}/run`, {
    method: 'POST',
    body: JSON.stringify({ entry, row }),
  })
  const seen: unknown = await response.json()
  if (
    !isObject(seen) ||
    typeof seen.sent !== 'string' ||
    typeof seen.read !== 'string' ||
    typeof seen.closed !== 'string'
  )
    throw new Error(`no row result: ${JSON.stringify(seen)}`)
  return { sent: seen.sent, read: seen.read, closed: seen.closed }
}

const closedByTtl = 'closed: Channel timed out: no client connected within TTL after response was sent'

for (const mode of ['legacy', 'modern'] as const) {
  describe(`Deno, ${mode === 'legacy' ? 'its signal aborted at every response' : '`--unstable-no-legacy-abort`'}`, () => {
    for (const [entry, through] of Object.entries(entries)) {
      test(`a call that returns a Channel and keeps its File, most of it sent after the answer, read 1 s later: the File is read whole, and the channel closes by its connect window (${through})`, async () => {
        const seen = await run(served[mode], entry, {
          telefunction: 'keepsFile',
          row: `${mode}-${entry}-late`,
          parts: 8,
          readAfterMs: WINDOW_MS,
          rest: { pacedMs: 40 },
        })
        expect(seen).toEqual({ sent: 'all sent', read: `read ${8 * PART} bytes`, closed: closedByTtl })
      }, 15_000)

      test(`a call that returns a Channel and leaves its 32 MiB File unread: the server takes the whole upload, and the channel closes by its connect window (${through})`, async () => {
        const seen = await run(served[mode], entry, {
          telefunction: 'keepsFile',
          row: `${mode}-${entry}-big`,
          parts: 512,
          readAfterMs: null,
          rest: { pacedMs: 0 },
        })
        expect(seen).toEqual({ sent: 'all sent', read: 'not read', closed: closedByTtl })
      }, 15_000)

      test(`a call that returns a Channel and keeps its File, sent whole, read once its channel closed, its client still there: the File is read whole (${through})`, async () => {
        const seen = await run(served[mode], entry, {
          telefunction: 'keepsFile',
          row: `${mode}-${entry}-kept`,
          parts: 8,
          readAfterMs: 2 * WINDOW_MS,
          rest: { pacedMs: 0 },
        })
        expect(seen).toEqual({ sent: 'all sent', read: `read ${8 * PART} bytes`, closed: closedByTtl })
      }, 15_000)

      test(`a plain call whose File, sent whole, it reads 1 s after it returned: the File is read whole (${through})`, async () => {
        const seen = await run(served[mode], entry, {
          telefunction: 'readsFileLater',
          row: `${mode}-${entry}-plain`,
          parts: 8,
          readAfterMs: WINDOW_MS,
          rest: { pacedMs: 0 },
        })
        expect(seen).toEqual({ sent: 'all sent', read: `read ${8 * PART} bytes`, closed: 'no channel' })
      }, 15_000)

      test(`a call that returns a Channel and reads its File, whose client leaves after the answer: the read fails as its client left, and the channel closes by its connect window (${through})`, async () => {
        // A server of its own: behind other rows on its server, Deno at times fails the read on its own as the client
        // leaves, which hides a read left pending.
        const own = await startServer(mode)
        onTestFinished(() => void own.server.kill())
        const seen = await run(own, entry, {
          telefunction: 'keepsFile',
          row: `${mode}-${entry}-after`,
          parts: 8,
          readAfterMs: 120,
          rest: { leaveAfterMs: 160 },
        })
        expect(seen).toEqual({
          sent: 'left',
          read: 'failed: The client disconnected before the end of the request body',
          closed: closedByTtl,
        })
      }, 15_000)
    }
  })
}
