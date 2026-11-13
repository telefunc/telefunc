import { afterAll, beforeAll, expect, it } from 'vitest'
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { config, serve } from '../index.js'
import { PROJECT_VERSION } from '../../../utils/PROJECT_VERSION.js'

// The server code sets `config.telefuncFiles`, while a production build of another setup is left in dist/server.
// The app lives under the repo, else the plugin's auto importer would load the entry of the last app built here.
const cwd = process.cwd()
const appDir = mkdtempSync(join(cwd, 'node_modules/.telefunc-manual-files-'))
const loadBuildEntry = new URL(
  '../../../dist/node/server/runTelefunc/loadTelefuncFilesUsingVite/loadBuildEntry.js',
  import.meta.url,
)

beforeAll(() => {
  mkdirSync(join(appDir, 'dist/server'), { recursive: true })
  writeFileSync(
    join(appDir, 'dist/server/entry.mjs'),
    [
      `import { setTelefuncLoaders } from '${loadBuildEntry.href}'`,
      'setTelefuncLoaders({',
      '  loadTelefuncFiles: async () => ({ telefuncFilesGlob: {} }),',
      `  loadManifest: () => ({ version: '${PROJECT_VERSION}', config: { telefuncUrl: '/production-api' } }),`,
      '})',
    ].join('\n'),
  )
  writeFileSync(join(appDir, 'hello.telefunc.mjs'), "export async function hello() { return 'manual' }")
  process.chdir(appDir)
  config.root = appDir
  config.telefuncFiles = [join(appDir, 'hello.telefunc.mjs')]
})

afterAll(() => {
  process.chdir(cwd)
  rmSync(appDir, { recursive: true, force: true })
})

it("the server's telefuncFiles take precedence over a leftover production entry", async () => {
  for (const _ of [1, 2]) {
    const request = new Request('http://localhost/_telefunc', {
      method: 'POST',
      headers: { 'x-telefunc-request': 'txt' },
      body: JSON.stringify({ file: '/hello.telefunc.mjs', name: 'hello', args: [] }),
    })
    const httpResponse = await serve({ request })
    expect({ statusCode: httpResponse.statusCode, err: (httpResponse.err as Error | undefined)?.message }).toEqual({
      statusCode: 200,
      err: undefined,
    })
    expect(httpResponse.body).toBe('{"ret":"manual"}')
  }
})
