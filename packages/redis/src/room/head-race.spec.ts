// The barrier-forced I13(c) finalization race. spi.md's race-realization note (ratified 2026-07-20)
// requires a backend whose CX application is genuinely ASYNCHRONOUS (Redis: cxAppliesSynchronously=false)
// to run this variant IN ADDITION to the two serial linearizations the shared head.spec asserts — it
// drives both through `concurrentHeadCxBarrier`, which issues both head CXs before either is awaited and
// releases them in the given order (here: the single publisher connection's FIFO command queue).
//
// GATED: skips cleanly without TELEFUNC_TEST_REAL_REDIS.

import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { RoomHead } from '../../../telefunc/wire-protocol/backend/spi.js'
import {
  accepted,
  bytes,
  CLOSE_LEASE_MS,
  CONTROL,
  conflicted,
  enterClosing,
  finalizeClose,
  nextId,
  okHead,
  openRoom,
  readHeadOrThrow,
  takeoverClose,
} from '../../../telefunc/wire-protocol/backend/conformance/scenario.js'
import { createRedisFixture, type RedisBackendFixture } from './fixture.js'

const url = process.env.TELEFUNC_TEST_REAL_REDIS

describe.skipIf(url === undefined || url === '')('Redis barrier-forced I13(c) finalization race', () => {
  let fx: RedisBackendFixture

  beforeEach(async () => {
    fx = await createRedisFixture(url as string)
  })

  afterEach(async () => {
    await fx.dispose()
  })

  // Pause the original closer between its accepted closed-event and its terminal head CX, then expire the
  // lease — the exact state I13(c) races from.
  const pausedCloser = async (): Promise<{ roomId: string; inc: string; paused: RoomHead; leaseId: string }> => {
    const roomId = nextId('room')
    const { inc, head } = await openRoom(fx.backend, roomId)
    const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
    accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed-by-original'), { closingLease: leaseId }))
    fx.advanceAuthority(CLOSE_LEASE_MS + 1)
    return { roomId, inc, paused: closing, leaseId }
  }

  it('order [finalize, takeover]: the unreplaced original finalizes, the takeover conflicts', async () => {
    const { roomId, paused, leaseId } = await pausedCloser()
    const [finalized, taken] = await (fx.concurrentHeadCxBarrier as NonNullable<typeof fx.concurrentHeadCxBarrier>)(
      () => finalizeClose(fx.backend, roomId, paused, leaseId),
      () => takeoverClose(fx.backend, roomId, paused).then((t) => t.result),
    )
    const tombstone = okHead(finalized)
    expect(tombstone.state).toBe('closed')
    expect(tombstone.currentInc).toBeNull() // head went null only after the finalizing closer's own event
    conflicted(taken)
  })

  it('order [takeover, finalize]: the takeover wins, the superseded original aborts its tail', async () => {
    const { roomId, inc, paused, leaseId } = await pausedCloser()
    const [taken, finalized] = await (fx.concurrentHeadCxBarrier as NonNullable<typeof fx.concurrentHeadCxBarrier>)(
      () => takeoverClose(fx.backend, roomId, paused).then((t) => t.result),
      () => finalizeClose(fx.backend, roomId, paused, leaseId),
    )
    const fresh = okHead(taken)
    expect(fresh.state).toBe('closing')
    expect(fresh.closeLease?.id).not.toBe(leaseId) // a fresh, different lease
    conflicted(finalized) // the superseded closer cannot clear the head

    // no head-null path exists for the superseded closer: the head is still closing under the winner
    const still = await readHeadOrThrow(fx.backend, roomId)
    expect(still.state).toBe('closing')
    expect(still.currentInc).toBe(inc)
  })
})
