import { afterAll, beforeAll, expect, test } from 'vitest'
import { execFile } from 'node:child_process'
import { mkdir, mkdtemp, rm, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'

// How an app's tools resolve the package as built, `import 'telefunc/__internal'` of an extension shared by its server
// and its browser: a Worker's bundler also sets `browser` (Cloudflare's Vite plugin sets `workerd, worker, browser`).

const run = promisify(execFile)
const packageDir = fileURLToPath(new URL('../', import.meta.url))
let app = ''

beforeAll(async () => {
  app = await mkdtemp(join(tmpdir(), 'telefunc-internal-'))
  await mkdir(join(app, 'node_modules'))
  await symlink(packageDir, join(app, 'node_modules/telefunc'), 'dir')
})
afterAll(() => rm(app, { recursive: true, force: true }))

/** What `telefunc/__internal` exports and loads with the page, under the `conditions` of a tool. */
async function importUnder(conditions: string[]) {
  const script = `
    const internal = await import('telefunc/__internal')
    const loaded = globalThis._telefunc?.['wire-protocol/values/client/loadFeatures.ts']?.loaded ?? {}
    console.log(JSON.stringify({ exports: Object.keys(internal), loaded: Object.keys(loaded) }))`
  const flags = conditions.map((condition) => `--conditions=${condition}`)
  const { stdout } = await run(process.execPath, [...flags, '--input-type=module', '-e', script], { cwd: app })
  return JSON.parse(stdout)
}

test("a Worker's bundler resolves the server half: it loads no client code", async () => {
  expect(await importUnder(['workerd', 'worker', 'browser'])).toEqual({ exports: ['isPeerGone'], loaded: [] })
})

test("a browser's bundler resolves the browser half: it loads the channels' and broadcasts' code", async () => {
  expect(await importUnder(['browser'])).toEqual({ exports: ['isPeerGone'], loaded: ['channels', 'broadcasts'] })
})

test("TypeScript under the browser condition resolves the server half's types, the marker extensions' types import", async () => {
  await writeFile(
    join(app, 'tsconfig.json'),
    JSON.stringify({
      compilerOptions: {
        module: 'esnext',
        moduleResolution: 'bundler',
        customConditions: ['browser'],
        strict: true,
        noEmit: true,
        skipLibCheck: true,
        types: [],
      },
      files: ['extension.ts'],
    }),
  )
  await writeFile(
    join(app, 'extension.ts'),
    "import type { TELEFUNC_SHIELDS } from 'telefunc/__internal'\nexport type Shielded = { [TELEFUNC_SHIELDS]: true }\n",
  )
  const tsc = join(packageDir, 'node_modules/.bin/tsc')
  await run(tsc, ['-p', app])
})
