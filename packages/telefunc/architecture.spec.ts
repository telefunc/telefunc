import { relative, sep } from 'node:path'
import { fileURLToPath } from 'node:url'
import { ts } from 'ts-morph'
import { expect, test } from 'vitest'
import { channelOf } from './wire-protocol/wire/frame.js'
import { HEADER, writeHeader } from './wire-protocol/wire/header.js'
import { isChannelCtrlTag, isWireCtrlTag, TAG } from './wire-protocol/wire/tags.js'

const forbiddenImports = [
  [/^utils\//, /^(wire-protocol|node|client|serve)\//],
  [/^workarounds\//, /^(node|client|serve|wire-protocol)\//],
  [/^wire-protocol\/wire\//, /^(wire-protocol\/(channel|values|broadcast|adapter)|node|client|serve)\//],
  [/^wire-protocol\/wire\//, /^utils\/nodeReadableToWebRequest\.ts$/],
  [/^wire-protocol\/values\//, /^node\/server\/serverConfig\.ts$/],
  [/^wire-protocol\/broadcast\//, /^(wire-protocol\/channel\/server|node\/server)\//],
  [/^node\/server\/(context\/|runTelefunc\.ts$)/, /^workarounds\//],
  [/^node\/server\/runTelefunc\//, /^workarounds\/(?!denoUploadResponseHold\.ts$)/],
  [
    /^node\/server\/runTelefunc\/carryChannel\.ts$/,
    /^wire-protocol\/(adapter\/|channel\/server\/ServerBroadcast\.ts$)/,
  ],
  [/^wire-protocol\/channel\/server\/WireSender\.ts$/, /^wire-protocol\/channel\/server\/EventStream\.ts$/],
] as const

const codecOwners = new Set([
  'wire-protocol/wire/envelope.ts',
  'wire-protocol/wire/SseEvents.ts',
  'node/server/runTelefunc.ts',
])

test('production modules have no circular imports or forbidden dependencies', () => {
  const root = fileURLToPath(new URL('./', import.meta.url))
  const files = ts.sys
    .readDirectory(root, ['.ts'], ['**/node_modules', '**/dist'])
    .filter((file) => !/\.(spec|test|fixture|d)\.ts$/.test(file))
  const paths = new Map(files.map((file) => [file, relative(root, file).split(sep).join('/')]))
  const graph = new Map<string, string[]>()
  const violations: string[] = []
  const registries: string[] = []
  const options = { moduleResolution: ts.ModuleResolutionKind.Node16 }
  const cache = ts.createModuleResolutionCache(root, (path) => path, options)

  for (const [file, path] of paths) {
    const source = ts.createSourceFile(file, ts.sys.readFile(file)!, ts.ScriptTarget.Latest)
    const dependencies: string[] = []
    graph.set(path, dependencies)

    if (hasComment(source, /\b0\.3\.\d\b/)) violations.push(`${path} has a legacy version comment`)
    if (/^workarounds\/[^/]+\.ts$/.test(path) && !hasComment(source, /remove once/i)) {
      violations.push(`${path} needs a removal condition`)
    }

    function visit(node: ts.Node): void {
      const specifier = runtimeSpecifier(node)
      if (specifier && ts.isStringLiteralLike(specifier) && specifier.text.startsWith('.')) {
        const { resolvedModule } = ts.resolveModuleName(specifier.text, file, options, ts.sys, cache)
        const target = resolvedModule && paths.get(resolvedModule.resolvedFileName)
        if (target) {
          dependencies.push(target)
          if (forbiddenImports.some(([from, to]) => from.test(path) && to.test(target))) {
            violations.push(`${path} must not import ${target}`)
          }
        }
      }

      if (ts.isPrivateIdentifier(node)) violations.push(`${path} uses ${node.text}`)
      if (ts.isNewExpression(node)) {
        const ctor = ts.isPropertyAccessExpression(node.expression) ? node.expression.name : node.expression
        if (ts.isIdentifier(ctor)) {
          if (ctor.text === 'GcRegistry') registries.push(path)
          if ((ctor.text === 'TextEncoder' || ctor.text === 'TextDecoder') && !codecOwners.has(path)) {
            violations.push(`${path} constructs ${ctor.text}`)
          }
        }
      }
      ts.forEachChild(node, visit)
    }
    visit(source)
  }

  for (const cycle of circularImports(graph)) {
    violations.push(`Runtime import cycle among ${cycle.sort().join(', ')}`)
  }
  if (registries.length !== 1 || registries[0] !== 'utils/GcRegistry.ts') {
    violations.push(`Expected one GcRegistry in utils/GcRegistry.ts. Found in ${registries.join(', ') || 'no files'}`)
  }
  expect(violations).toEqual([])
})

test('every wire tag belongs to exactly one plane', () => {
  expect(isWireCtrlTag(0)).toBe(false)
  for (const [name, tag] of Object.entries(TAG)) {
    const planes = [isWireCtrlTag(tag), isChannelCtrlTag(tag), tag >= 0x10 && tag <= 0x2f]
    expect(planes.filter(Boolean), name).toHaveLength(1)
  }
})

test('wire-control tags never name a channel', () => {
  expect(isWireCtrlTag(TAG.PIECES_ACK_REQ)).toBe(true)
  for (const [name, tag] of Object.entries(TAG)) {
    if (!isWireCtrlTag(tag)) continue
    const header = new Uint8Array(HEADER)
    writeHeader(header, tag, 1, 0)
    expect(channelOf(header), name).toBeUndefined()
  }
})

function runtimeSpecifier(node: ts.Node): ts.Expression | undefined {
  if (ts.isImportDeclaration(node)) {
    const clause = node.importClause
    if (clause?.isTypeOnly) return
    if (!clause?.name && clause?.namedBindings && ts.isNamedImports(clause.namedBindings)) {
      const { elements } = clause.namedBindings
      if (elements.length > 0 && elements.every((binding) => binding.isTypeOnly)) return
    }
    return node.moduleSpecifier
  }
  if (ts.isExportDeclaration(node)) {
    if (node.isTypeOnly) return
    if (node.exportClause && ts.isNamedExports(node.exportClause)) {
      const { elements } = node.exportClause
      if (elements.length > 0 && elements.every((binding) => binding.isTypeOnly)) return
    }
    return node.moduleSpecifier
  }
  if (ts.isImportEqualsDeclaration(node) && !node.isTypeOnly && ts.isExternalModuleReference(node.moduleReference)) {
    return node.moduleReference.expression
  }
  if (ts.isCallExpression(node)) {
    const callee = node.expression
    if (callee.kind === ts.SyntaxKind.ImportKeyword || (ts.isIdentifier(callee) && callee.text === 'require')) {
      return node.arguments[0]
    }
  }
}

function hasComment(source: ts.SourceFile, pattern: RegExp): boolean {
  if (!pattern.test(source.text)) return false
  const nodes: ts.Node[] = [source]
  while (nodes.length > 0) {
    const node = nodes.pop()!
    const matches = ({ pos, end }: ts.CommentRange) => pattern.test(source.text.slice(pos, end))
    if (ts.getLeadingCommentRanges(source.text, node.pos)?.some(matches)) return true
    if (ts.getTrailingCommentRanges(source.text, node.end)?.some(matches)) return true
    nodes.push(...node.getChildren(source))
  }
  return false
}

/** Tarjan's strongly connected components, keeping only components with a cycle. */
function circularImports(graph: ReadonlyMap<string, readonly string[]>): string[][] {
  const visited = new Map<string, { index: number; low: number; onStack: boolean }>()
  const stack: string[] = []
  const cycles: string[][] = []

  function visit(path: string): void {
    const current = { index: visited.size, low: visited.size, onStack: true }
    visited.set(path, current)
    stack.push(path)
    const edges = graph.get(path)!
    for (const target of edges) {
      const neighbor = visited.get(target)
      if (!neighbor) {
        visit(target)
        current.low = Math.min(current.low, visited.get(target)!.low)
      } else if (neighbor.onStack) {
        current.low = Math.min(current.low, neighbor.index)
      }
    }

    if (current.low !== current.index) return
    const component: string[] = []
    for (;;) {
      const member = stack.pop()!
      component.push(member)
      visited.get(member)!.onStack = false
      if (member === path) break
    }
    if (component.length > 1 || edges.includes(path)) cycles.push(component)
  }

  for (const path of graph.keys()) if (!visited.has(path)) visit(path)
  return cycles
}
