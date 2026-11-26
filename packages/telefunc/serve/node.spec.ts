import { afterAll, beforeAll, expect, it, onTestFinished } from 'vitest'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { createServer } from 'node:http'
import { once } from 'node:events'
import { join } from 'node:path'
import { Telefunc } from './node.js'
import { PROJECT_VERSION } from '../utils/PROJECT_VERSION.js'
import { WIRE_VERSION } from '../wire-protocol/constants.js'

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
      'setTelefuncLoaders({',
      `  loadTelefuncFiles: async () => ({ telefuncFilesGlob: { '/hello.telefunc.ts': async () => ({ hello: async () => 'hi' }) } }),`,
      `  loadManifest: () => ({ version: '${PROJECT_VERSION}', config: { telefuncUrl: '/api/tf' } }),`,
      '})',
    ].join('\n'),
  )
  process.chdir(appDir)
})

afterAll(() => {
  process.chdir(cwd)
  rmSync(appDir, { recursive: true, force: true })
})

// First: the rows below load the entry, after which a handshake is judged by the build's config anyway.
it("a server's first WebSocket handshake to the build's telefuncUrl opens, before the production entry loaded", async () => {
  const server = createServer((_req, res) => res.writeHead(404).end())
  new Telefunc().installWebSocket(server)
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  onTestFinished(() => {
    server.closeAllConnections()
    server.close()
  })
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  const ws = new WebSocket(`ws://127.0.0.1:${address.port}/api/tf?wire=${WIRE_VERSION}`)
  const { promise: outcome, resolve } = Promise.withResolvers<string>()
  ws.onopen = () => resolve('open')
  ws.onerror = () => resolve('error')
  expect(await outcome).toBe('open')
  ws.close()
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
