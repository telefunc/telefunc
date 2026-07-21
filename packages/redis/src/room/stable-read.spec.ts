// Redis-specific stable-read scenarios (convergence §W2 / spi.md §5.2 edit 3): the rev_before → SCAN/MGET
// → rev_after algorithm under a concurrent mutation forced INSIDE the read window, plus the bounded
// 8-attempt retry exhaustion. The window is driven deterministically by the backend's `stableReadProbe`
// seam (sanctioned by readiness-ordering §2.2's holdRegistration/holdRetainedRead hooks) rather than by
// racing wall-clock threads, so each scenario is reproducible.
//
// GATED: skips cleanly without TELEFUNC_TEST_REAL_REDIS (never fake green).

import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  bytes,
  cellsOf,
  nextId,
  openRoom,
  text,
} from '../../../telefunc/wire-protocol/backend/conformance/scenario.js'
import { revKey } from './layout.js'
import { createRedisFixture, type RedisBackendFixture } from './fixture.js'

const url = process.env.TELEFUNC_TEST_REAL_REDIS

describe.skipIf(url === undefined || url === '')('Redis stable-read — forced concurrent mutation', () => {
  let fx: RedisBackendFixture
  let roomId: string
  let inc: string

  beforeEach(async () => {
    fx = await createRedisFixture(url as string)
    roomId = nextId('room')
    inc = (await openRoom(fx.backend, roomId)).inc
  })

  afterEach(async () => {
    await fx.dispose()
  })

  const seed = async (key: string, value: string, ttlMs?: number): Promise<void> => {
    const revision = cellsOf(await fx.backend.readCells(roomId, inc, { prefix: '' })).revision
    const result = await fx.backend.compareExchangeCells(roomId, inc, revision, [
      { key, set: { bytes: bytes(value), ttlMs } },
    ])
    expect(result).toBe('committed')
  }

  const currentRev = async (): Promise<string> => (await fx.redis.get(revKey(fx.prefix, roomId, inc))) ?? '0'

  it('forced insert: the read fences, retries, and returns the post-insert snapshot', async () => {
    await seed('member/alice', 'A')
    const before = await currentRev()

    // One-shot: insert member/bob (a real cell CX that bumps rev) AFTER the first attempt enumerated,
    // so rev_after ≠ rev_before and the read must re-enumerate. compareExchangeCells never re-enters the
    // probe (only readCells fires it), so there is no recursion.
    let fired = 0
    fx.setStableReadProbe(async () => {
      if (fired++ > 0) return
      const rev = await currentRev()
      expect(await fx.backend.compareExchangeCells(roomId, inc, rev, [{ key: 'member/bob', set: { bytes: bytes('B') } }])).toBe(
        'committed',
      )
    })

    const read = cellsOf(await fx.backend.readCells(roomId, inc, { prefix: 'member/' }))
    fx.setStableReadProbe(null)

    // member/bob was inserted mid-read; its presence proves the read re-enumerated under the fence.
    expect([...read.cells.keys()].sort()).toEqual(['member/alice', 'member/bob'])
    expect(text(read.cells.get('member/bob') as Uint8Array)).toBe('B')
    expect(read.revision).not.toBe(before)
    expect(read.revision).toBe(await currentRev())
  })

  it('deliberate delete: the read fences to the post-delete revision, not the stale one', async () => {
    await seed('member/alice', 'A')
    const before = await currentRev()

    let fired = 0
    fx.setStableReadProbe(async () => {
      if (fired++ > 0) return
      const rev = await currentRev()
      expect(await fx.backend.compareExchangeCells(roomId, inc, rev, [{ key: 'member/alice' }])).toBe('committed')
    })

    const read = cellsOf(await fx.backend.readCells(roomId, inc, { prefix: 'member/' }))
    fx.setStableReadProbe(null)

    expect(read.cells.size).toBe(0)
    // the returned revision reflects the delete — a dropped fence would have returned the stale rev
    expect(read.revision).not.toBe(before)
    expect(read.revision).toBe(await currentRev())
  })

  it('logical expiry during the read: the cell is filtered, the revision is UNCHANGED (not a mutation)', async () => {
    await seed('member/alice', 'A', 30_000)
    const before = await currentRev()

    // Advancing authority time mid-read expires the cell LOGICALLY. That is silent — it bumps no
    // revision — so the read does NOT retry; the expiresAt filter simply hides it.
    let fired = 0
    fx.setStableReadProbe(() => {
      if (fired++ > 0) return
      fx.advanceAuthority(30_001)
    })

    const read = cellsOf(await fx.backend.readCells(roomId, inc, { prefix: 'member/' }))
    fx.setStableReadProbe(null)

    expect(read.cells.size).toBe(0) // filtered by the logical expiresAt, not deleted
    expect(read.revision).toBe(before) // a silent PX/logical expiry is never a revisioned mutation
  })

  it('read-retry exhaustion: rev churn on every attempt throws after the 8-attempt bound', async () => {
    await seed('member/alice', 'A')

    // Control that CAN pass: with no churn the read converges on the first attempt.
    expect(cellsOf(await fx.backend.readCells(roomId, inc, { prefix: 'member/' })).cells.size).toBe(1)

    // Churn rev on EVERY stable-read window → rev_before ≠ rev_after forever → the bounded read throws a
    // transport error (DISTINCT from Room core's write-contention loop).
    fx.setStableReadProbe(async () => {
      await fx.redis.incr(revKey(fx.prefix, roomId, inc))
    })
    await expect(fx.backend.readCells(roomId, inc, { prefix: 'member/' })).rejects.toThrow(/stable read did not converge/)
    fx.setStableReadProbe(null)
  })
})
