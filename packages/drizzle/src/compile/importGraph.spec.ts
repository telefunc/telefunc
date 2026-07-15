import { readFileSync, readdirSync } from 'node:fs'
import { dirname, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const srcDir = resolve(dirname(fileURLToPath(import.meta.url)), '..')

function productionFiles(): string[] {
  return readdirSync(srcDir, { recursive: true, encoding: 'utf8' })
    .filter((entry) => entry.endsWith('.ts') && !entry.endsWith('.spec.ts'))
    .map((entry) => resolve(srcDir, entry))
}

function importsOf(file: string, moduleName: string): boolean {
  const source = readFileSync(file, 'utf8')
  // any static/dynamic import of the module (exact package or a subpath)
  const re = new RegExp(`from\\s+['"]${escape(moduleName)}(?:/[^'"]*)?['"]|import\\(['"]${escape(moduleName)}`, 'g')
  return re.test(source)
}

function escape(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

const under = (file: string, dirs: string[]): boolean =>
  dirs.some((dir) => relative(srcDir, file).replace(/\\/g, '/').startsWith(`${dir}/`))

describe('import-graph boundary (T4.F1)', () => {
  it('ir/ + compile/ + graph/ import zero drizzle-orm (the ORM-agnostic engine)', () => {
    const offenders = productionFiles()
      .filter((file) => under(file, ['ir', 'compile', 'graph']))
      .filter((file) => importsOf(file, 'drizzle-orm'))
      .map((file) => relative(srcDir, file))
    expect(offenders).toEqual([])
  })
})

describe('engine seam (T4.A1)', () => {
  it('@tanstack/db-ivm is imported ONLY in graph/ivm.ts across production src', () => {
    const importers = productionFiles()
      .filter((file) => importsOf(file, '@tanstack/db-ivm'))
      .map((file) => relative(srcDir, file).replace(/\\/g, '/'))
    expect(importers).toEqual(['graph/ivm.ts'])
  })
})
