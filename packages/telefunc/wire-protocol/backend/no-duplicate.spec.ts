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
const callbackCollections = nestedSetMapOwners()

describe('one backend mechanism', () => {
  it('keeps callback collection ownership explicit', () => {
    expect(callbackCollections).toEqual([
      'packages/redis/src/room/subscriber-transport.ts::RedisSubscriptionDriver._attempts',
      'packages/telefunc/wire-protocol/backend/memory/backend.ts::Generation.subs',
      'packages/telefunc/wire-protocol/backend/memory/backend.ts::MemoryBackendState.broadcastSubs',
      'packages/telefunc/wire-protocol/room/server/room.ts::ServerRoom._announcedTracks',
      'packages/telefunc/wire-protocol/server/mux.ts::ChannelMux.pendingRegisterWaiters',
      'packages/telefunc/wire-protocol/server/sse.ts::SseConnectionTransport.pendingConnections',
    ])
    expect(classOwners('SubscriptionManager')).toEqual([
      'packages/telefunc/wire-protocol/backend/subscriptions.ts::SubscriptionManager',
    ])
    expect(constructedBy('SubscriptionManager')).toEqual([
      'packages/telefunc/wire-protocol/backend/supervised-backend.ts::superviseBackend',
    ])
  })

  it('keeps one canonical lane encoder and imports it across adapters', () => {
    expect(laneEncoderOwners()).toEqual(['packages/telefunc/wire-protocol/backend/subscription-source.ts::laneKey'])
    expect(valuesFrom('packages/redis/src/room/layout.ts', 'telefunc/backend', 'import')).toContain('laneKey')
    expect(
      valuesFrom(
        'packages/telefunc/wire-protocol/server/adapter/cloudflare/room/codec.ts',
        '../../../../backend/subscription-source.js',
        'export',
      ),
    ).toContain('laneKey')
    expect(
      valuesFrom('packages/telefunc/backend.ts', './wire-protocol/backend/subscription-source.js', 'export'),
    ).toContain('laneKey')
  })

  it('keeps one JavaScript ordering codec and one explicit Redis dialect translation', () => {
    expect(orderingOperationOwners('setUint32')).toEqual([
      'packages/telefunc/wire-protocol/ordering-frame.ts::encodeOrderingFrame',
    ])
    expect(orderingOperationOwners('getUint32')).toEqual([
      'packages/redis/src/room/layout.ts::decodeRedisOrderingFrame',
      'packages/telefunc/wire-protocol/ordering-frame.ts::decodeOrderingFrame',
    ])
    expect(
      valuesFrom(
        'packages/telefunc/wire-protocol/server/adapter/cloudflare/broadcast.ts',
        '../../../ordering-frame.js',
        'import',
      ),
    ).toEqual(['decodeOrderingFrame', 'encodeOrderingFrame'])
  })

  it('keeps external adapters on canonical value helpers', () => {
    const redisValues = files
      .filter((file) => file.startsWith('packages/redis/'))
      .flatMap((file) => valuesFrom(file, 'telefunc/backend', 'import'))
    expect([...new Set(redisValues)].sort()).toEqual(['HEAD_TRANSITIONS', 'ORDERING_FRAME_LAYOUT', 'laneKey'])
    expect(valuesFrom('packages/redis/src/index.ts', 'telefunc/__internal', 'import')).toContain('setDefaultBackend')
  })

  it('keeps generic Broadcast and durable Room on their own backend verbs', () => {
    expect(calledMethods('packages/telefunc/wire-protocol/server/server-broadcast.ts')).toEqual(
      expect.arrayContaining(['publish', 'subscribe']),
    )
    expect(calledMethods('packages/telefunc/wire-protocol/server/server-broadcast.ts')).not.toEqual(
      expect.arrayContaining(['commitLane', 'subscribeLane']),
    )
    const roomImplementations = files.filter((file) => file.startsWith('packages/telefunc/wire-protocol/room/server/'))
    expect(calledMethods(...roomImplementations)).toEqual(expect.arrayContaining(['commitLane', 'subscribeLane']))
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

function nestedSetMapOwners(): string[] {
  return [...sources]
    .flatMap(([file, value]) =>
      value.getDescendants().flatMap((node) => {
        if (!Node.isPropertyDeclaration(node) && !Node.isPropertySignature(node)) return []
        const type = node.getTypeNode()?.getText() ?? node.getInitializer()?.getText() ?? ''
        if (!/^(?:new )?Map<[\s\S]*,\s*Set<[\s\S]*>>/.test(type)) return []
        const container = node
          .getFirstAncestor(
            (parent) =>
              Node.isClassDeclaration(parent) ||
              Node.isInterfaceDeclaration(parent) ||
              Node.isTypeAliasDeclaration(parent),
          )
          ?.getSymbol()
          ?.getName()
        return [`${file}::${container === undefined ? '' : `${container}.`}${node.getName()}`]
      }),
    )
    .sort()
}

function classOwners(name: string): string[] {
  return [...sources].flatMap(([file, value]) =>
    value
      .getClasses()
      .filter((declaration) => declaration.getName() === name)
      .map(() => `${file}::${name}`),
  )
}

function constructedBy(name: string): string[] {
  return [...sources].flatMap(([file, value]) =>
    value
      .getDescendantsOfKind(SyntaxKind.NewExpression)
      .filter((expression) => expression.getExpression().getText() === name)
      .map((expression) => `${file}::${callableOwner(expression)}`),
  )
}

function laneEncoderOwners(): string[] {
  const kinds = JSON.stringify(['binary', 'control', 'inbox', 'semantic'])
  return [...sources]
    .flatMap(([file, value]) =>
      value.getDescendantsOfKind(SyntaxKind.SwitchStatement).flatMap((statement) => {
        const labels = statement
          .getCaseBlock()
          .getClauses()
          .filter(Node.isCaseClause)
          .map((clause) => clause.getExpression().getText().replaceAll(/['"]/g, ''))
          .sort()
        const text = statement.getText()
        return JSON.stringify(labels) === kinds &&
          text.includes('encodeURIComponent') &&
          text.includes('binary:') &&
          text.includes('inbox:')
          ? [`${file}::${callableOwner(statement)}`]
          : []
      }),
    )
    .sort()
}

function orderingOperationOwners(operation: 'getUint32' | 'setUint32'): string[] {
  const owners = [...sources].flatMap(([file, value]) =>
    value.getDescendantsOfKind(SyntaxKind.CallExpression).flatMap((call) => {
      const expression = call.getExpression()
      const callable = call.getFirstAncestor(isCallable)
      return Node.isPropertyAccessExpression(expression) &&
        expression.getName() === operation &&
        callable?.getText().includes('ORDERING_FRAME_LAYOUT')
        ? [`${file}::${callableOwner(call)}`]
        : []
    }),
  )
  return [...new Set(owners)].sort()
}

function callableOwner(node: Node): string {
  const callable = node.getFirstAncestor(isCallable)
  if (callable === undefined) return '<module>'
  if (Node.isFunctionDeclaration(callable)) return callable.getName() ?? '<anonymous>'
  if (Node.isMethodDeclaration(callable)) {
    const container = callable.getFirstAncestorByKind(SyntaxKind.ClassDeclaration)?.getName()
    return `${container === undefined ? '' : `${container}.`}${callable.getName()}`
  }
  return callable.getFirstAncestorByKind(SyntaxKind.VariableDeclaration)?.getName() ?? '<anonymous>'
}

function isCallable(node: Node): boolean {
  return Node.isFunctionDeclaration(node) || Node.isMethodDeclaration(node) || Node.isArrowFunction(node)
}

function valuesFrom(file: string, module: string, kind: 'import' | 'export'): string[] {
  if (kind === 'export') {
    return source(file)
      .getExportDeclarations()
      .filter((declaration) => declaration.getModuleSpecifierValue() === module)
      .flatMap((declaration) => declaration.getNamedExports().map((value) => value.getName()))
      .sort()
  }
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

function calledMethods(...files: string[]): string[] {
  const methods = files.flatMap((file) =>
    source(file)
      .getDescendantsOfKind(SyntaxKind.CallExpression)
      .flatMap((call) => {
        const expression = call.getExpression()
        return Node.isPropertyAccessExpression(expression) ? [expression.getName()] : []
      }),
  )
  return [...new Set(methods)].sort()
}
