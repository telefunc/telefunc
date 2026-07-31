import { spawnSync } from 'node:child_process'
import { buildSync } from 'esbuild'
import { expect, it } from 'vitest'

it.each([
  ['ignored', '', ['wrapped rejection']],
  ['handled', '.catch(() => {})', []],
])('keeps %s wrapped-method rejection reporting truthful', (_case, consume, expected) => {
  const bundled = buildSync({
    stdin: {
      contents: `
        import { wrapProxy } from './packages/telefunc/wire-protocol/wrapProxy.ts'
        const events = []; process.on('unhandledRejection', (reason) => events.push(reason.message)); wrapProxy({ fail: () => Promise.reject(new Error('wrapped rejection')) }).fail()${consume}
        setTimeout(() => process.stdout.write(JSON.stringify(events)), 0)
      `,
      resolveDir: process.cwd(),
    },
    bundle: true,
    format: 'esm',
    platform: 'node',
    target: 'node20',
    write: false,
  }).outputFiles[0]!.text
  const result = spawnSync(process.execPath, ['--input-type=module', '--eval', bundled], {
    encoding: 'utf8',
    timeout: 10_000,
  })

  expect(result.stderr).toBe('')
  expect(result.status).toBe(0)
  expect(JSON.parse(result.stdout)).toEqual(expected)
})
