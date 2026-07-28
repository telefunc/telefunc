import { readdirSync, readFileSync } from 'node:fs'
import { extname, join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const root = fileURLToPath(new URL('../../../../', import.meta.url))
const production = ['packages/telefunc', 'packages/redis']
  .flatMap((dir) => sourceFiles(join(root, dir)))
  .filter((file) => !/\.(?:spec|test)\.ts$/.test(file) && !file.includes('/backend/conformance/'))

describe('one backend mechanism', () => {
  it('keeps one public seam and one supervised subscription owner', () => {
    const deleted = [
      'BroadcastAdapter',
      'DefaultBroadcastAdapter',
      'BroadcastTransport',
      'RoomBackendSpi',
      'ROOM_SPI_VERSION',
      'installRoomBackend',
      'getRoomBackend',
      'setDefaultRoomBackend',
    ]
    expect(deleted.flatMap((name) => hits(new RegExp(`\\b${name}\\b`)))).toEqual([])
    expect(filesWith(/\bclass SubscriptionManager\b/)).toEqual([
      'packages/telefunc/wire-protocol/backend/subscriptions.ts',
    ])
    expect(filesWith(/\bnew SubscriptionManager\b/)).toEqual([
      'packages/telefunc/wire-protocol/backend/supervised-backend.ts',
    ])
    expect(
      hits(/(?:Map|Set)<[^>\n]*BackendReceiver|BackendReceiver\[\]/).filter(
        ({ file }) => file !== 'packages/telefunc/wire-protocol/backend/subscriptions.ts',
      ),
    ).toEqual([])

    const generic = text('packages/telefunc/wire-protocol/server/server-broadcast.ts')
    expect(generic).toMatch(/\bgetBackend\(\)/)
    expect(generic).toMatch(/\.publish\(/)
    expect(generic).toMatch(/\.subscribe\(/)
    expect(generic).not.toMatch(/\.commitLane\(|\.subscribeLane\(/)
    const room = text('packages/telefunc/wire-protocol/room/server.ts')
    expect(room).toMatch(/\.commitLane\(/)
    expect(room).toMatch(/\.subscribeLane\(/)
  })

  it('keeps external backends on contract data and raw driver types only', () => {
    const redis = production.filter((file) => file.startsWith('packages/redis/'))
    const machinery = ['SubscriptionManager', 'encodeOrderingFrame', 'decodeOrderingFrame', 'assertHeadTransition']
    expect(
      machinery.flatMap((name) =>
        hits(new RegExp(`\\bimport(?:\\s+type)?\\s*\\{[^}]*\\b${name}\\b[^}]*\\}\\s+from\\b`), redis),
      ),
    ).toEqual([])

    const values = redis.flatMap((file) => importedValues(file, 'telefunc/backend'))
    expect([...new Set(values)].sort()).toEqual(['HEAD_TRANSITIONS', 'ORDERING_FRAME_LAYOUT'])
    expect(text('packages/redis/src/index.ts')).toMatch(
      /import \{ setDefaultBackend \} from 'telefunc\/__internal'/,
    )

    const core = production.filter((file) => file.startsWith('packages/telefunc/'))
    expect(
      hits(/\b(?:ioredis|redis\.error_reply|generateLuaHeadTransitionTable)\b/, core),
    ).toEqual([])
  })

  it('keeps one wide ordering codec and one head-transition rule set', () => {
    expect(filesWith(/\bfunction encodeOrderingFrame\b/)).toEqual([
      'packages/telefunc/wire-protocol/ordering-frame.ts',
    ])
    expect(filesWith(/\bfunction decodeOrderingFrame\b/)).toEqual([
      'packages/telefunc/wire-protocol/ordering-frame.ts',
    ])
    expect(filesWith(/\bconst HEAD_TRANSITIONS\s*=/)).toEqual([
      'packages/telefunc/wire-protocol/backend/head-transitions.ts',
    ])
    expect(hits(/\bI4I4I4\b|headerBytes\s*:\s*12|12-byte ordering/)).toEqual([])

    const frame = text('packages/telefunc/wire-protocol/ordering-frame.ts')
    expect(frame).toMatch(/headerBytes:\s*16/)
    expect(frame).toMatch(/wordBytes:\s*4/)
    expect(frame).toMatch(/endianness:\s*'big'/)
    const redis = text('packages/redis/src/room/layout.ts')
    expect(redis).toMatch(/HEAD_TRANSITIONS\.map/)
    expect(redis).toMatch(/ORDERING_FRAME_LAYOUT\.offsets/)
    expect(redis).toMatch(/ORDERING_FRAME_LAYOUT\.wordBytes/)
    const cloudflare = text('packages/telefunc/wire-protocol/server/adapter/cloudflare/broadcast.ts')
    expect(cloudflare).toMatch(/ORDERING_FRAME_LAYOUT/)
    expect(production.some((file) => file.endsWith('/room/transitions.ts'))).toBe(false)
  })
})

function sourceFiles(dir: string): string[] {
  const files: string[] = []
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.name === 'dist' || entry.name === 'node_modules') continue
    const path = join(dir, entry.name)
    if (entry.isDirectory()) files.push(...sourceFiles(path))
    else if (extname(entry.name) === '.ts') files.push(relative(root, path).replaceAll('\\', '/'))
  }
  return files
}

function text(file: string): string {
  return readFileSync(join(root, file), 'utf8')
}

function hits(pattern: RegExp, files = production): { file: string; match: string }[] {
  return files.flatMap((file) =>
    [...text(file).matchAll(new RegExp(pattern, pattern.flags.includes('g') ? pattern.flags : `${pattern.flags}g`))].map(
      (match) => ({ file, match: match[0] }),
    ),
  )
}

function filesWith(pattern: RegExp): string[] {
  return [...new Set(hits(pattern).map(({ file }) => file))]
}

function importedValues(file: string, module: string): string[] {
  const imports = text(file).matchAll(
    new RegExp(`import\\s+(?!type\\s+)\\{([^}]*)\\}\\s+from\\s+['"]${module}['"]`, 'g'),
  )
  return [...imports].flatMap((match) =>
    (match[1] ?? '')
      .split(',')
      .map((name) => name.trim())
      .filter((name) => name !== '' && !name.startsWith('type '))
      .map((name) => name.split(/\s+as\s+/)[0] as string),
  )
}
