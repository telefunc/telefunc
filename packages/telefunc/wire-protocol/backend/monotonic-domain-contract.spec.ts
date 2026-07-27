import { existsSync, readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'

const utf8 = (path: string): string => readFileSync(path, 'utf8')

describe('monotonic ordering and one-mechanism publication contract', () => {
  it('documents seq as the standalone current-domain cursor and rejects the former reset wording', () => {
    const guide = utf8('docs/pages/room/+Page.mdx')
    expect(guide).toContain('standalone monotonic cursor')
    expect(guide).toContain('A close and recreation starts new domain instances at `seq = 1`')
    expect(guide).toContain('every `(member, binary track)` and every recipient inbox has its own domain')
    expect(guide).not.toMatch(/seq` breaks ties|resets when `timestamp` advances|sort on the pair, not `seq` alone/)
  })

  it('removes order TTLs and the legacy Broadcast adapter implementation', () => {
    const spi = utf8('packages/telefunc/wire-protocol/backend/spi.ts')
    const roomServer = utf8('packages/telefunc/wire-protocol/room/server.ts')
    expect(spi).not.toContain('orderTtlMs')
    expect(roomServer).not.toContain('orderTtlMs')
    expect(existsSync('packages/telefunc/wire-protocol/server/broadcast.ts')).toBe(false)
  })

  it('uses one wide ordering codec for generic Broadcast and Room', () => {
    const codec = utf8('packages/telefunc/wire-protocol/ordering-frame.ts')
    const sharedWire = utf8('packages/telefunc/wire-protocol/shared-ws.ts')
    const roomProtocol = utf8('packages/telefunc/wire-protocol/room/protocol.ts')
    expect(codec).toContain('const ORDERING_FRAME_LAYOUT = Object.freeze')
    expect(codec).toContain('view.setUint32(offsets.seqHigh')
    expect(sharedWire).toContain("from './ordering-frame.js'")
    expect(sharedWire).not.toContain('setUint32')
    expect(roomProtocol).not.toMatch(/frameRoomBinaryOrder|unframeRoomBinaryOrder/)
  })

  it('keeps subscription lifecycle and local fan-out in the shared manager', () => {
    const manager = utf8('packages/telefunc/wire-protocol/backend/subscriptions.ts')
    const memory = utf8('packages/telefunc/wire-protocol/backend/memory/backend.ts')
    const room = utf8('packages/telefunc/wire-protocol/room/server.ts')
    const broadcast = utf8('packages/telefunc/wire-protocol/server/server-broadcast.ts')
    expect(manager).toContain('class SubscriptionManager')
    expect(manager).toContain('SUBSCRIPTION_ESTABLISH_TIMEOUT_MS = 10_000')
    expect(manager).toContain('readonly #receivers = new Map<symbol, BackendReceiver>()')
    for (const policy of [memory, room, broadcast]) {
      expect(policy).not.toMatch(/ReadinessGeneration|#replanAttempts|#establishmentTimer/)
      expect(policy).not.toContain('SubscriptionManager')
    }
  })
})
