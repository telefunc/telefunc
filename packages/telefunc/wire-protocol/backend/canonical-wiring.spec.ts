/** Proves the reviewed production wiring uses its canonical owners and dependency directions. */
import { readdirSync } from 'node:fs'
import { extname, join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Node, Project, SyntaxKind, type SourceFile } from 'ts-morph'
import { describe, expect, it } from 'vitest'
const root = fileURLToPath(new URL('../../../../', import.meta.url))
const files = ['packages/telefunc', 'packages/redis']
  .flatMap((dir) => walk(join(root, dir)))
  .filter((file) => !/\.(?:spec|test)\.ts$/.test(file))
const project = new Project({ skipAddingFilesFromTsConfig: true })
const sources = new Map(files.map((file) => [file, project.addSourceFileAtPath(join(root, file))]))
describe('canonical backend wiring', () => {
  it('wires both plane supervisors to the exported subscription owner', () => {
    expect(exportsOf('packages/telefunc/wire-protocol/backend/subscription-manager.ts')).toContain(
      'SubscriptionManager',
    )
    expect(
      importsFrom('packages/telefunc/wire-protocol/backend/broadcast/supervise.ts', '../subscription-manager.js'),
    ).toContain('SubscriptionManager')
    expect(
      constructedBy('packages/telefunc/wire-protocol/backend/broadcast/supervise.ts', 'SubscriptionManager'),
    ).toEqual(['superviseBroadcastDriver'])
    expect(
      importsFrom('packages/telefunc/wire-protocol/backend/room/supervise.ts', '../subscription-manager.js'),
    ).toContain('SubscriptionManager')
    expect(constructedBy('packages/telefunc/wire-protocol/backend/room/supervise.ts', 'SubscriptionManager')).toEqual([
      'superviseRoomDriver',
    ])
    expect(importsFrom('packages/telefunc/wire-protocol/backend/install.ts', './broadcast/supervise.js')).toContain(
      'superviseBroadcastDriver',
    )
    expect(importsFrom('packages/telefunc/wire-protocol/backend/install.ts', './room/supervise.js')).toContain(
      'superviseRoomDriver',
    )
  })
  it('wires lane consumers to the canonical lane key', () => {
    expect(reexportsFrom('packages/telefunc/backend.ts', './wire-protocol/backend/room/lane-key.js')).toContain(
      'laneKey',
    )
    expect(importsFrom('packages/redis/src/room/layout.ts', 'telefunc/backend')).toContain('laneKey')
    expect(
      reexportsFrom(
        'packages/telefunc/wire-protocol/server/adapter/cloudflare/room/codec.ts',
        '../../../../backend/room/lane-key.js',
      ),
    ).toContain('laneKey')
  })
  it('wires ordering consumers to the canonical frame contract', () => {
    expect(
      importsFrom(
        'packages/telefunc/wire-protocol/server/adapter/cloudflare/broadcast.ts',
        '../../../ordering-frame.js',
      ),
    ).toEqual(['decodeOrderingFrame', 'encodeOrderingFrame'])
    expect(importsFrom('packages/redis/src/room/layout.ts', 'telefunc/backend')).toContain('ORDERING_FRAME_LAYOUT')
  })
  it('keeps Redis Telefunc imports within the reviewed exact-peer surfaces', () => {
    const redisValues = files
      .filter((file) => file.startsWith('packages/redis/'))
      .flatMap((file) => importsFrom(file, 'telefunc/backend'))
    expect([...new Set(redisValues)].sort()).toEqual(['HEAD_TRANSITIONS', 'ORDERING_FRAME_LAYOUT', 'laneKey'])
    expect(importsFrom('packages/redis/src/index.ts', 'telefunc/__internal')).toEqual([
      'BACKEND_SPI_VERSION',
      'setDefaultBackend',
      'superviseBroadcastDriver',
    ])
  })
  it('keeps fused subscription sources out of plane modules', () => {
    const forbidden = new Set([
      'BackendDriver',
      'BackendSpi',
      'BackendSubscriptionSource',
      'subscriptionSourceKey',
      'superviseBackend',
    ])
    // biome-ignore format: keep the bounded identifier scan atomic
    const owners = files.filter((file) => source(file).getDescendantsOfKind(SyntaxKind.Identifier).some((node) => forbidden.has(node.getText()))).sort()
    // biome-ignore format: keep the two plane roots pinned together
    expect(owners.filter((file) => /^packages\/telefunc\/wire-protocol\/backend\/(?:broadcast|room)\//.test(file))).toEqual([])
    // biome-ignore format: keep the empty retired-name manifest pinned
    expect(owners).toEqual([])
  })
  it('keeps generic client core independent from Room', () => {
    // Static import declarations include value and type-only imports.
    // biome-ignore format: keep the bounded dependency scan visually atomic
    const edges = files.filter((file) => /^packages\/telefunc\/wire-protocol\/(?:client\/(?:response\/|(?:channel|connection|call-barrier|deadlineScheduler|session-registry)\.ts$)|channel\.ts$)/.test(file)).flatMap((file) => source(file).getImportDeclarations().flatMap((node) => { const target = node.getModuleSpecifierSourceFile(); if (target === undefined) return []; const path = relative(root, target.getFilePath()).replaceAll('\\', '/'); return path.startsWith('packages/telefunc/wire-protocol/room/') ? [`${file} -> ${path}`] : [] }))
    // biome-ignore format: keep the empty edge manifest pinned
    expect(edges.sort()).toEqual([])
  })
  it('keeps the Room protocol shape module from becoming a re-export mega-barrel', () => {
    const declarations = source('packages/telefunc/wire-protocol/room/protocol.ts').getExportDeclarations()
    expect(declarations.some((declaration) => declaration.getModuleSpecifierValue() !== undefined)).toBe(false)
  })
})
function walk(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    if (entry.name === 'dist' || entry.name === 'node_modules') return []
    const path = join(dir, entry.name)
    if (entry.isDirectory()) return walk(path)
    return extname(entry.name) === '.ts' ? [relative(root, path).replaceAll('\\', '/')] : []
  })
}
function source(file: string): SourceFile {
  const value = sources.get(file)
  if (value === undefined) throw new Error(`Production source not loaded: ${file}`)
  return value
}
function exportsOf(file: string): string[] {
  return source(file)
    .getExportDeclarations()
    .flatMap((declaration) => declaration.getNamedExports().map((specifier) => specifier.getName()))
}
function constructedBy(file: string, name: string): string[] {
  return source(file)
    .getDescendantsOfKind(SyntaxKind.NewExpression)
    .filter((expression) => expression.getExpression().getText() === name)
    .map((expression) => callableOwner(expression))
    .sort()
}
function callableOwner(node: Node): string {
  const callable = node.getFirstAncestor(
    (parent) => Node.isFunctionDeclaration(parent) || Node.isMethodDeclaration(parent) || Node.isArrowFunction(parent),
  )
  if (callable === undefined) return '<module>'
  if (Node.isFunctionDeclaration(callable)) return callable.getName() ?? '<anonymous>'
  if (Node.isMethodDeclaration(callable)) {
    const container = callable.getFirstAncestorByKind(SyntaxKind.ClassDeclaration)?.getName()
    return `${container === undefined ? '' : `${container}.`}${callable.getName()}`
  }
  return callable.getFirstAncestorByKind(SyntaxKind.VariableDeclaration)?.getName() ?? '<anonymous>'
}
function reexportsFrom(file: string, module: string): string[] {
  return source(file)
    .getExportDeclarations()
    .filter((declaration) => declaration.getModuleSpecifierValue() === module)
    .flatMap((declaration) => declaration.getNamedExports().map((value) => value.getName()))
    .sort()
}
function importsFrom(file: string, module: string): string[] {
  return source(file)
    .getImportDeclarations()
    .filter(
      (declaration) => declaration.getModuleSpecifierValue() === module && !declaration.getImportClause()?.isTypeOnly(),
    )
    .flatMap((declaration) =>
      declaration
        .getNamedImports()
        .filter((value) => !value.isTypeOnly())
        .map((value) => value.getName()),
    )
    .sort()
}
