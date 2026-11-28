import { afterAll, beforeAll, expect, it, onTestFinished } from 'vitest'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { createServer, request, type Server } from 'node:http'
import { once } from 'node:events'
import { join } from 'node:path'
import { Telefunc } from './node.js'
import { PROJECT_VERSION } from '../utils/PROJECT_VERSION.js'
import { WIRE_VERSION } from '../wire-protocol/constants.js'
import { hasProp } from '../utils/hasProp.js'

// The app's server imports nothing from dist/server: Telefunc finds dist/server/entry.mjs itself.
// The app lives under the repo, else the plugin's auto importer would load the entry of the last app built here.
const cwd = process.cwd()
const appDir = mkdtempSync(join(cwd, 'node_modules/.telefunc-node-adapter-'))
const loadBuildEntry = new URL(
  '../dist/node/server/runTelefunc/loadTelefuncFilesUsingVite/loadBuildEntry.js',
  import.meta.url,
)

beforeAll(() => {
  mkdirSync(join(appDir, 'dist/server'), { recursive: true })
  writeFileSync(
    join(appDir, 'dist/server/entry.mjs'),
    [
      `import { setTelefuncLoaders } from '${loadBuildEntry.href}'`,
      // A build entry that takes a while to load, as a large app's does.
      'await new Promise((resolve) => setTimeout(resolve, 300))',
      'setTelefuncLoaders({',
      `  loadTelefuncFiles: async () => ({ telefuncFilesGlob: { '/hello.telefunc.ts': async () => ({ hello: async () => 'hi' }) } }),`,
      `  loadManifest: () => ({ version: '${PROJECT_VERSION}', config: { telefuncUrl: '/api/tf' } }),`,
      '})',
    ].join('\n'),
  )
  process.chdir(appDir)
})

// Node has it from 22.21 and 24.9; its typings here don't.
const nodeHasTheCallback = hasProp(createServer(), 'shouldUpgradeCallback', 'function')

afterAll(() => {
  process.chdir(cwd)
  rmSync(appDir, { recursive: true, force: true })
})

// First: the rows below load the entry, after which a request is judged by the build's config anyway.
it.runIf(nodeHasTheCallback)(
  "a request that offers h2c gets the app's answer while the production entry loads, and the first WebSocket handshake opens",
  async () => {
    const server = createServer((_req, res) => res.writeHead(200).end('ordinary HTTP'))
    new Telefunc().installWebSocket(server)
    const port = await listen(server)
    const offersH2c = new Promise<[number | undefined, string]>((resolve, reject) => {
      const headers = {
        Connection: 'Upgrade, HTTP2-Settings',
        Upgrade: 'h2c',
        'HTTP2-Settings': 'AAMAAABkAAQAoAAAAAIAAAAA',
      }
      const req = request(`http://127.0.0.1:${port}/status`, { headers }, async (res) => {
        res.setEncoding('utf8')
        let body = ''
        for await (const chunk of res) body += chunk
        resolve([res.statusCode, body])
      })
      req.once('error', reject)
      req.end()
    })
    expect(await Promise.all([offersH2c, handshake(port)])).toEqual([[200, 'ordinary HTTP'], 'open'])
  },
)

it("a server's first WebSocket handshake to the build's telefuncUrl opens, before the production entry loaded", async () => {
  const server = createServer((_req, res) => res.writeHead(404).end())
  new Telefunc().installWebSocket(server)
  expect(await handshake(await listen(server))).toBe('open')
})

it("new Telefunc() serves the build's telefuncUrl when the production entry loads lazily", async () => {
  const telefunc = new Telefunc()
  for (const _ of [1, 2]) {
    const request = new Request('http://localhost/api/tf', {
      method: 'POST',
      headers: { 'x-telefunc-request': 'txt' },
      body: JSON.stringify({ file: '/hello.telefunc.ts', name: 'hello', args: [] }),
    })
    const response = await telefunc.serve({ request })
    expect(response?.status).toBe(200)
    expect(await response?.text()).toBe('{"ret":"hi"}')
  }
})

it("`serve({ request })` leaves a request for another path than the build's telefuncUrl to the app", async () => {
  const telefunc = new Telefunc()
  for (const path of ['/other', '//api/tf', '/api/tf/x'])
    expect(
      await telefunc.serve({ request: new Request(`http://localhost${path}`, { method: 'POST', body: '{}' }) }),
    ).toBe(undefined)
})

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

/** How a WebSocket handshake to the build's telefuncUrl ends: `open`, or `error`. */
async function handshake(port: number): Promise<string> {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/api/tf?wire=${WIRE_VERSION}`)
  const { promise: outcome, resolve } = Promise.withResolvers<string>()
  ws.onopen = () => resolve('open')
  ws.onerror = () => resolve('error')
  const ended = await outcome
  ws.close()
  return ended
}
