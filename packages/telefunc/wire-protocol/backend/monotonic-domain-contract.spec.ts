import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'

const utf8 = (path: string): string => readFileSync(path, 'utf8')

describe('Room monotonic-domain publication contract', () => {
  it('documents seq as the standalone current-domain cursor and rejects the former reset wording', () => {
    const guide = utf8('docs/pages/room/+Page.mdx')
    expect(guide).toContain('standalone monotonic cursor')
    expect(guide).toContain('A close and recreation starts new domain instances at `seq = 1`')
    expect(guide).toContain('every `(member, binary track)` and every recipient inbox has its own domain')
    expect(guide).not.toMatch(/seq` breaks ties|resets when `timestamp` advances|sort on the pair, not `seq` alone/)
  })

  it('removes Room order TTLs and the unreleased legacy Broadcast Room commit seam', () => {
    const spi = utf8('packages/telefunc/wire-protocol/backend/spi.ts')
    const roomServer = utf8('packages/telefunc/wire-protocol/room/server.ts')
    const genericBroadcast = utf8('packages/telefunc/wire-protocol/server/broadcast.ts')
    expect(spi).not.toContain('orderTtlMs')
    expect(roomServer).not.toContain('orderTtlMs')
    expect(genericBroadcast).not.toContain('orderTtlMs')
  })

  it('keeps the released generic Redis frame at 12 bytes while Room uses its additive 16-byte codec', () => {
    const genericRedis = utf8('packages/redis/src/index.ts')
    const roomRedis = utf8('packages/redis/src/room/layout.ts')
    expect(genericRedis).toContain('const HEADER_BYTES = 12')
    expect(genericRedis).toContain("struct.pack('>I4I4I4'")
    expect(roomRedis).toContain('export const HEADER_BYTES = 16')
    expect(roomRedis).toContain("struct.pack('>I4I4I4I4'")
  })
})
