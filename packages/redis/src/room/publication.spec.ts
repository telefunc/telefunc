import { existsSync, readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const packageRoot = fileURLToPath(new URL('../../', import.meta.url))

describe('@telefunc/redis Room publication boundary', () => {
  it('keeps fixtures, registration, specs, and the removed backend copy out of package emit', () => {
    const config = JSON.parse(readFileSync(`${packageRoot}/tsconfig.json`, 'utf8')) as { exclude?: string[] }
    expect(config.exclude).toEqual(
      expect.arrayContaining(['src/**/*.spec.ts', 'src/room/fixture.ts', 'src/room/register.ts']),
    )
    expect(existsSync(`${packageRoot}/src/room/backend.test-support.ts`)).toBe(false)
  })

  it('publishes only the package root and package metadata, with no test-hook root export', () => {
    const manifest = JSON.parse(readFileSync(`${packageRoot}/package.json`, 'utf8')) as {
      exports: Record<string, unknown>
      files: string[]
    }
    expect(Object.keys(manifest.exports).sort()).toEqual(['.', './package.json'])
    expect(manifest.files).toEqual(['dist'])
    const rootSource = readFileSync(`${packageRoot}/src/index.ts`, 'utf8')
    expect(rootSource).not.toMatch(/fixture|test-support|test-hooks|ForTest/)
  })
})
