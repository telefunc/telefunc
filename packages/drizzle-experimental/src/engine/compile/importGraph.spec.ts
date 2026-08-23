import { readFileSync, readdirSync } from 'node:fs'
import { dirname, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import ts from 'typescript'
import { describe, expect, it } from 'vitest'

const srcDir = resolve(dirname(fileURLToPath(import.meta.url)), '../..')

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

/** Every runtime module specifier in one TypeScript source file: static imports, side-effect imports,
 *  re-exports, and dynamic imports. The compiler AST distinguishes erased type-only clauses from value
 *  edges; a regex that only understood `from` was the false-green this gate exists to prevent. */
const computedDynamicImport = '<computed dynamic import>'

function importSpecifiersIn(source: string, file: string, runtimeOnly: boolean): string[] {
  const tree = ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS)
  const specifiers: string[] = []

  for (const statement of tree.statements) {
    if (ts.isImportDeclaration(statement) && (!runtimeOnly || importDeclarationIsRuntime(statement))) {
      if (ts.isStringLiteralLike(statement.moduleSpecifier)) specifiers.push(statement.moduleSpecifier.text)
    }
    if (ts.isExportDeclaration(statement) && (!runtimeOnly || exportDeclarationIsRuntime(statement))) {
      if (statement.moduleSpecifier && ts.isStringLiteralLike(statement.moduleSpecifier))
        specifiers.push(statement.moduleSpecifier.text)
    }
  }

  const visitNonStatementImports = (node: ts.Node): void => {
    // `type T = import('./module.js').T` is an ImportTypeNode, not a CallExpression. It erases at runtime,
    // but the all-edges graph must still see it: a core type that names the Drizzle quarantine is still an
    // architectural dependency. ImportDeclaration and ExportDeclaration type edges are collected in the
    // statement pass above; the controls below pin all three spellings independently.
    if (!runtimeOnly && ts.isImportTypeNode(node)) {
      const argument = node.argument
      if (ts.isLiteralTypeNode(argument) && ts.isStringLiteralLike(argument.literal))
        specifiers.push(argument.literal.text)
    }
    if (ts.isCallExpression(node) && node.expression.kind === ts.SyntaxKind.ImportKeyword) {
      const argument = node.arguments[0]
      // A computed runtime edge cannot be proven browser-safe, so make it an explicit forbidden
      // specifier rather than silently omitting it from the graph.
      specifiers.push(argument && ts.isStringLiteralLike(argument) ? argument.text : computedDynamicImport)
    }
    ts.forEachChild(node, visitNonStatementImports)
  }
  ts.forEachChild(tree, visitNonStatementImports)
  return specifiers
}

function importSpecifiersOf(file: string, runtimeOnly: boolean): string[] {
  return importSpecifiersIn(readFileSync(file, 'utf8'), file, runtimeOnly)
}

const runtimeImportSpecifiersOf = (file: string): string[] => importSpecifiersOf(file, true)
const allImportSpecifiersOf = (file: string): string[] => importSpecifiersOf(file, false)

/** Whether one core module reaches into the Drizzle quarantine. Type-only edges count: they are still an
 *  architectural dependency. A computed dynamic edge fails closed because its target cannot be proven safe. */
function importSpecifiersReachDrizzle(specifiers: string[], file: string): boolean {
  return specifiers.some((specifier) => {
    if (specifier === computedDynamicImport) return true
    if (!specifier.startsWith('.')) return false
    const target = resolve(dirname(file), specifier.replace(/\.js$/, '.ts'))
    return under(target, ['drizzle'])
  })
}

function importsDrizzleQuarantine(file: string): boolean {
  return importSpecifiersReachDrizzle(allImportSpecifiersOf(file), file)
}

function importDeclarationIsRuntime(node: ts.ImportDeclaration): boolean {
  const clause = node.importClause
  if (!clause) return true // side-effect import
  if (clause.isTypeOnly) return false
  if (clause.name) return true // default value import
  const bindings = clause.namedBindings
  if (!bindings || ts.isNamespaceImport(bindings)) return true
  return bindings.elements.length === 0 || bindings.elements.some((element) => !element.isTypeOnly)
}

function exportDeclarationIsRuntime(node: ts.ExportDeclaration): boolean {
  if (node.isTypeOnly) return false
  const clause = node.exportClause
  if (!clause || ts.isNamespaceExport(clause)) return true
  return clause.elements.length === 0 || clause.elements.some((element) => !element.isTypeOnly)
}

function valueImportGraph(entry: string): { files: string[]; bare: string[] } {
  const seen = new Set<string>()
  const bare = new Set<string>()
  const visit = (file: string): void => {
    if (seen.has(file)) return
    seen.add(file)
    for (const specifier of runtimeImportSpecifiersOf(file)) {
      if (specifier.startsWith('.')) visit(resolve(dirname(file), specifier.replace(/\.js$/, '.ts')))
      else bare.add(specifier)
    }
  }
  visit(entry)
  return {
    files: [...seen].map((file) => relative(srcDir, file).replace(/\\/g, '/')).sort(),
    bare: [...bare].sort(),
  }
}

const browserRuntimePackageAllowlist = new Set(['@tanstack/query-core', 'telefunc/client'])

describe('import-edge classifier', () => {
  const controlFile = resolve(srcDir, 'engine/compile/typeEdgeControl.ts')

  it('counts import type and export type as architecture edges, but not runtime edges', () => {
    const source = [
      "import type { Plan } from '../../drizzle/writePlan.js'",
      "export type { CaptureSink } from '../../drizzle/writeChanges.js'",
    ].join('\n')

    expect(importSpecifiersIn(source, controlFile, false)).toEqual([
      '../../drizzle/writePlan.js',
      '../../drizzle/writeChanges.js',
    ])
    expect(importSpecifiersIn(source, controlFile, true)).toEqual([])
  })

  it('treats an inline import type into drizzle/ as a quarantine leak, but erases it from the runtime graph', () => {
    const source = "type Leak = import('../../drizzle/writePlan.js').Plan"

    const architectureEdges = importSpecifiersIn(source, controlFile, false)
    expect(architectureEdges).toEqual(['../../drizzle/writePlan.js'])
    expect(importSpecifiersReachDrizzle(architectureEdges, controlFile)).toBe(true)
    expect(importSpecifiersIn(source, controlFile, true)).toEqual([])
  })
})

describe('import-graph boundary', () => {
  it('ir/ + compile/ + graph/ + router/ import zero drizzle-orm (the ORM-agnostic engine)', () => {
    const coreFiles = productionFiles().filter((file) => under(file, ['ir', 'engine', 'bus', 'primitive', 'utils']))
    // `captureReport` is stable core, not an exemption hidden beside the Drizzle edge.
    expect(coreFiles.map((file) => relative(srcDir, file).replace(/\\/g, '/'))).toContain('bus/captureReport.ts')

    const offenders = coreFiles
      .filter((file) => importsOf(file, 'drizzle-orm') || importsDrizzleQuarantine(file))
      .map((file) => relative(srcDir, file))
    expect(offenders).toEqual([])
  })

  it('graph/ + router/ import zero binding/ (hydration is injected, R1)', () => {
    const offenders = productionFiles()
      .filter((file) => under(file, ['engine', 'bus']))
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
    expect(valueImportGraph(resolve(srcDir, 'tanstack-query/index.ts')).files).toEqual([
      'primitive/taps.ts',
      'primitive/wireClient.ts',
      'primitive/wireConstants.ts',
      'tanstack-query/index.ts',
      'tanstack-query/live.ts',
    ])
  })

  it('the tanstack-query runtime graph imports only explicitly browser-safe bare packages', () => {
    const { bare } = valueImportGraph(resolve(srcDir, 'tanstack-query/index.ts'))
    expect(bare.filter((specifier) => !browserRuntimePackageAllowlist.has(specifier))).toEqual([])
    expect(bare).toEqual(['@tanstack/query-core', 'telefunc/client']) // proves the classifier sees allowed imports too
  })
})

describe('engine seam', () => {
  it('@tanstack/db-ivm is imported ONLY in graph/ivm.ts across production src', () => {
    const importers = productionFiles()
      .filter((file) => importsOf(file, '@tanstack/db-ivm'))
      .map((file) => relative(srcDir, file).replace(/\\/g, '/'))
    expect(importers).toEqual(['engine/graph/ivm.ts'])
  })
})
