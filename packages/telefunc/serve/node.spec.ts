import { afterAll, beforeAll, expect, it } from 'vitest'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { Telefunc } from './node.js'
import { PROJECT_VERSION } from '../utils/PROJECT_VERSION.js'

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
