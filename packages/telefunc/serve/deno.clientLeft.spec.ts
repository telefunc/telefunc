// On Deno, a channel can keep a request open while a file sent after the initial response is still being read.
// Exercise that path and client disconnects through each adapter, in both Deno abort modes.

import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import { spawn, type ChildProcess } from 'node:child_process'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { isObject } from '../utils/isObject.js'
import { PART, type Row } from './deno.clientLeft.client.fixture.js'

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
      test(`a call that returns a Channel and keeps its File, most of it sent after the answer, read after 120 ms: the File is read whole, and the channel closes by its connect window (${through})`, async () => {
        const seen = await run(served[mode], entry, {
          telefunction: 'keepsFile',
          row: `${mode}-${entry}-late`,
          parts: 8,
          readAfterMs: 120,
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

      test(`a call that returns a Channel and reads its File, whose client leaves after the answer: the read fails, and the channel closes by its connect window (${through})`, async () => {
        const seen = await run(served[mode], entry, {
          telefunction: 'keepsFile',
          row: `${mode}-${entry}-after`,
          parts: 8,
          readAfterMs: 120,
          rest: { leaveAfterMs: 160 },
        })
        // Deno's node:http framework Request doesn't signal the disconnect once its response has ended.
        const read =
          entry === 'framework'
            ? 'failed: The server answered before reading the request body to its end'
            : 'failed: The client disconnected before the end of the request body'
        expect(seen).toEqual({
          sent: 'left',
          read,
          closed: closedByTtl,
        })
      }, 15_000)
    }
  })
}
