import { afterAll, beforeAll, expect, test } from 'vitest'
import { execFile } from 'node:child_process'
import { mkdir, mkdtemp, rm, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'

const run = promisify(execFile)
const packageDir = fileURLToPath(new URL('../', import.meta.url))
let app = ''

beforeAll(async () => {
  app = await mkdtemp(join(tmpdir(), 'telefunc-internal-'))
  await mkdir(join(app, 'node_modules'))
  await symlink(packageDir, join(app, 'node_modules/telefunc'), 'dir')
})
afterAll(() => rm(app, { recursive: true, force: true }))

async function importUnder(entry: string, conditions: string[]) {
  const script = `
    const internal = await import(${JSON.stringify(entry)})
    const loaded = globalThis._telefunc?.['wire-protocol/values/client/loadFeatures.ts']?.loaded ?? {}
    console.log(JSON.stringify({ exports: Object.keys(internal), loaded: Object.keys(loaded) }))`
  const flags = conditions.map((condition) => `--conditions=${condition}`)
  const { stdout } = await run(process.execPath, [...flags, '--input-type=module', '-e', script], { cwd: app })
  return JSON.parse(stdout)
}

const workerConditions = ['workerd', 'worker', 'browser']

test.each([['browser'], workerConditions].map((conditions) => ({ conditions })))(
  'the generic entry exports only the helper under $conditions',
  async ({ conditions }) => {
    expect(await importUnder('telefunc/__internal', conditions)).toEqual({ exports: ['isPeerGone'], loaded: [] })
  },
)

test.each(['channels', 'broadcasts'])('the %s entry registers its capability only in the browser', async (feature) => {
  const entry = `telefunc/__internal/${feature}`
  expect(await importUnder(entry, ['browser'])).toEqual({ exports: [], loaded: [feature] })
  expect(await importUnder(entry, workerConditions)).toEqual({ exports: [], loaded: [] })
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
        noUncheckedSideEffectImports: true,
      },
      files: ['extension.ts'],
    }),
  )
  await writeFile(
    join(app, 'extension.ts'),
    `import type { TELEFUNC_SHIELDS } from 'telefunc/__internal'
import 'telefunc/__internal/channels'
import 'telefunc/__internal/broadcasts'
export type Shielded = { [TELEFUNC_SHIELDS]: true }
`,
  )
  const tsc = join(packageDir, 'node_modules/.bin/tsc')
  await run(tsc, ['-p', app])
})
