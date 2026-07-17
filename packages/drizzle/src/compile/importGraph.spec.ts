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

/** Any import (static or dynamic) whose path lands in the `binding/` directory. */
function importsBinding(file: string): boolean {
  const source = readFileSync(file, 'utf8')
  return /from\s+['"][^'"]*\/binding\/[^'"]*['"]|import\(['"][^'"]*\/binding\//.test(source)
}

describe('import-graph boundary (T4.F1 / T5.J2)', () => {
  it('ir/ + compile/ + graph/ + router/ import zero drizzle-orm (the ORM-agnostic engine)', () => {
    const offenders = productionFiles()
      .filter((file) => under(file, ['ir', 'compile', 'graph', 'router']))
      .filter((file) => importsOf(file, 'drizzle-orm'))
      .map((file) => relative(srcDir, file))
    expect(offenders).toEqual([])
  })

  it('graph/ + router/ import zero binding/ (hydration is injected, R1)', () => {
    const offenders = productionFiles()
      .filter((file) => under(file, ['graph', 'router']))
      .filter((file) => importsBinding(file))
      .map((file) => relative(srcDir, file))
    expect(offenders).toEqual([])
  })
})

describe('engine seam (T4.A1 / T5.J1)', () => {
  it('@tanstack/db-ivm is imported ONLY in graph/ivm.ts across production src', () => {
    const importers = productionFiles()
      .filter((file) => importsOf(file, '@tanstack/db-ivm'))
      .map((file) => relative(srcDir, file).replace(/\\/g, '/'))
    expect(importers).toEqual(['graph/ivm.ts'])
  })
})
