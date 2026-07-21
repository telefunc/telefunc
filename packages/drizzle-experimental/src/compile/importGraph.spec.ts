import { readFileSync, readdirSync } from 'node:fs'
import { dirname, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const srcDir = resolve(dirname(fileURLToPath(import.meta.url)), '..')

/** Everything that SHIPS. Excludes the specs, and the `.testKit.ts` modules those specs share.
 *
 *  A test kit is not part of the engine — it exists so two split spec files can drive one subject through one
 *  set of fakes, and a spec has always been free to import drizzle-orm. Treating "not a .spec.ts" as "ships"
 *  would make the kit an offender for doing exactly what the spec it was extracted from already did.
 *
 *  That exemption is only safe because it CANNOT LEAK: the last case below pins that nothing which ships
 *  imports a test kit, so a `.testKit.ts` can never become a back door for a dependency this file forbids. */
function productionFiles(): string[] {
  return readdirSync(srcDir, { recursive: true, encoding: 'utf8' })
    .filter((entry) => entry.endsWith('.ts') && !entry.endsWith('.spec.ts') && !entry.endsWith('.testKit.ts'))
    .map((entry) => resolve(srcDir, entry))
}

function importsOf(file: string, moduleName: string): boolean {
  const source = readFileSync(file, 'utf8')
  // Any static, dynamic, or SIDE-EFFECT import of the module (exact package or a subpath). The side-effect
  // form `import 'drizzle-orm'` has no `from`, so a pattern anchored on `from` reports a file that imports
  // the module as clean — a false green in the one direction this whole file exists to prevent.
  const target = `['"]${escape(moduleName)}(?:/[^'"]*)?['"]`
  return new RegExp(`(?:from\\s+|import\\s*\\(?\\s*)${target}`, 'g').test(source)
}

function escape(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

const under = (file: string, dirs: string[]): boolean =>
  dirs.some((dir) => relative(srcDir, file).replace(/\\/g, '/').startsWith(`${dir}/`))

/** Any import — static, dynamic, or side-effect — whose path lands in the `binding/` directory. */
function importsBinding(file: string): boolean {
  const source = readFileSync(file, 'utf8')
  return /(?:from\s+|import\s*\(?\s*)['"][^'"]*\/binding\/[^'"]*['"]/.test(source)
}

/** Runtime-only relative imports/re-exports. `import type` is deliberately absent: it erases from the
 *  browser bundle, which is exactly the boundary this graph checks. */
function valueImportsOf(file: string): string[] {
  const source = readFileSync(file, 'utf8')
  const imports = source.matchAll(
    /(?:^|\n)\s*(?!import\s+type\b)(?:import(?:[^'\"]*?\sfrom\s+|\s*)|export\s+(?!type\b)[^'\"]*?\sfrom\s+)['\"](\.[^'\"]+)['\"]/g,
  )
  return [...imports].map((match) => resolve(dirname(file), match[1]!.replace(/\.js$/, '.ts')))
}

function valueImportClosure(entry: string): string[] {
  const seen = new Set<string>()
  const visit = (file: string): void => {
    if (seen.has(file)) return
    seen.add(file)
    for (const imported of valueImportsOf(file)) visit(imported)
  }
  visit(entry)
  return [...seen].map((file) => relative(srcDir, file).replace(/\\/g, '/')).sort()
}

describe('import-graph boundary', () => {
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

  it('nothing that ships imports a test kit — so the .testKit exemption cannot leak', () => {
    // The guard on the exemption above. `.testKit.ts` modules are excluded from `productionFiles()` and may
    // therefore import drizzle-orm freely; that is only sound while no shipping file can reach one. Without
    // this, `engine.ts → foo.testKit.ts → drizzle-orm` would satisfy every other case in this file.
    const offenders = productionFiles()
      .filter((file) => /(?:from\s+|import\s*\(?\s*)['"][^'"]*\.testKit\.js['"]/.test(readFileSync(file, 'utf8')))
      .map((file) => relative(srcDir, file))
    expect(offenders).toEqual([])
  })

  it('the tanstack-query entry pulls only its browser-safe runtime graph', () => {
    expect(valueImportClosure(resolve(srcDir, 'tanstack-query/index.ts'))).toEqual([
      'primitive/taps.ts',
      'primitive/wireClient.ts',
      'primitive/wireConstants.ts',
      'tanstack-query/index.ts',
      'tanstack-query/live.ts',
    ])
  })
})

describe('engine seam', () => {
  it('@tanstack/db-ivm is imported ONLY in graph/ivm.ts across production src', () => {
    const importers = productionFiles()
      .filter((file) => importsOf(file, '@tanstack/db-ivm'))
      .map((file) => relative(srcDir, file).replace(/\\/g, '/'))
    expect(importers).toEqual(['graph/ivm.ts'])
  })
})
