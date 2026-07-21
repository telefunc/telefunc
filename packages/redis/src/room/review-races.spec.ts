// Focused live-Redis schedules from the W2b independent review. These are intentionally outside the
// shared conformance modules: they exercise Redis-specific shared-connection, Lua-accounting and
// authority-time mechanisms while preserving the same public SPI outcomes.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  accepted,
  bytes,
  cellsOf,
  collector,
  CONTROL,
  deferred,
  enterClosing,
  finalizeClose,
  flush,
  nextId,
  okDeleted,
  okHead,
  openRoom,
  SEMANTIC,
  text,
} from '../../../telefunc/wire-protocol/backend/conformance/scenario.js'
import { createRedisFixture, type RedisBackendFixture } from './fixture.js'

const url = process.env.TELEFUNC_TEST_REAL_REDIS

describe.skipIf(url === undefined || url === '')('Redis independent-review race gates', () => {
  let fx: RedisBackendFixture

  beforeEach(async () => {
    fx = await createRedisFixture(url as string)
  })

  afterEach(async () => {
    vi.useRealTimers()
    await fx.dispose()
  })

  it('same-channel subscribers share the in-flight SUBSCRIBE ack before either becomes ready', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const entered = deferred()
    const release = deferred()
    let calls = 0
    fx.setBeforeSubscribe(async () => {
      calls++
      if (calls === 1) {
        entered.resolve()
        await release.promise
      }
    })

    const first = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    await entered.promise
    const second = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    let firstReady = false
    let secondReady = false
    void first.ready.then(() => {
      firstReady = true
    })
    void second.ready.then(() => {
      secondReady = true
    })
    await flush()
    expect(firstReady).toBe(false)
    expect(secondReady).toBe(false)
    expect(calls).toBe(1)

    release.resolve()
    await Promise.all([first.ready, second.ready])
    expect(calls).toBe(1)
    expect(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after-ready'))).receivers).toBe(1)
  })

  it('initial SUBSCRIBE failure rejects ready, retries exactly five times, then closes', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    let attempts = 0
    fx.setBeforeSubscribe(() => {
      attempts++
      throw new Error('forced subscribe failure')
    })

    const states: string[] = []
    const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
    sub.onStateChange((state) => states.push(state))
    await expect(sub.ready).rejects.toThrow('forced subscribe failure')
    await waitFor(() => sub.state() === 'closed')
    expect(attempts).toBe(5)
    expect(states).toEqual(['lost', 'closed'])
  })

  it('subscriber transport loss emits lost, re-SUBSCRIBE ack emits ready, and delivery resumes', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const latch = collector()
    const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
    await sub.ready
    const states: string[] = []
    sub.onStateChange((state) => states.push(state))

    await fx.redis.client('KILL', 'ID', String(fx.subscriberId))
    await waitFor(() => states.includes('lost') && states.includes('ready'))
    expect(states.slice(0, 2)).toEqual(['lost', 'ready'])
    expect(sub.state()).toBe('ready')

    const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after-reconnect')))
    await result.delivery
    await latch.waitFor(1)
    expect(latch.payloads()).toEqual(['after-reconnect'])
  })

  it('keeps gens registered through physical drop, blocking same-inc recreation until the final SREM', async () => {
    const roomId = nextId('room')
    const { inc, head } = await openRoom(fx.backend, roomId)
    const cells = cellsOf(await fx.backend.readCells(roomId, inc, { prefix: '' }))
    expect(
      await fx.backend.compareExchangeCells(roomId, inc, cells.revision, [{ key: 'old', set: { bytes: bytes('x') } }]),
    ).toBe('committed')
    const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
    accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
    const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))

    const reachedBoundary = deferred()
    const release = deferred()
    fx.setBeforeDropGenerationUnregister(async () => {
      reachedBoundary.resolve()
      await release.promise
    })
    const dropping = fx.backend.dropGeneration(roomId, inc)
    await reachedBoundary.promise
    try {
      await expect(
        fx.backend.compareExchangeHead(
          roomId,
          { expect: { rev: tombstone.rev } },
          { head: { currentInc: inc, state: 'open', config: tombstone.config } },
        ),
      ).rejects.toThrow(/surviving generation state/)
    } finally {
      release.resolve()
      await dropping
    }
    const recreated = okHead(
      await fx.backend.compareExchangeHead(
        roomId,
        { expect: { rev: tombstone.rev } },
        { head: { currentInc: inc, state: 'open', config: tombstone.config } },
      ),
    )
    expect(recreated.currentInc).toBe(inc)
    expect(cellsOf(await fx.backend.readCells(roomId, inc, { prefix: '' })).cells.size).toBe(0)
  })

  it('enforces the aggregate retained payload cap inside concurrent COMMIT records', async () => {
    await fx.dispose()
    fx = await createRedisFixture(url as string, { maxRetainedPayloadBytes: 100 })
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)

    const results = await Promise.allSettled([
      fx.backend.commitLane(roomId, inc, SEMANTIC, new Uint8Array(60), { retain: true }),
      fx.backend.commitLane(roomId, inc, CONTROL, new Uint8Array(60), { retain: true }),
    ])
    expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1)
    expect(results.filter((result) => result.status === 'rejected')).toHaveLength(1)
    expect(await fx.backend.listRetained(roomId, inc)).toHaveLength(1)

    await fx.backend.deleteRetained(roomId, inc)
    accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, new Uint8Array(100), { retain: true }))
    // Replacement subtracts only the old payload (not its 12-byte frame header), so the total remains 100.
    accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, new Uint8Array(100), { retain: true }))
    await expect(fx.backend.commitLane(roomId, inc, CONTROL, new Uint8Array(1), { retain: true })).rejects.toThrow(
      /retained aggregate/,
    )
  })

  it('uses Redis TIME for production tombstone and logical cell-expiry reads under caller skew', async () => {
    await fx.dispose()
    fx = await createRedisFixture(url as string, { useRedisAuthority: true })

    const cellRoom = nextId('room')
    const { inc: cellInc } = await openRoom(fx.backend, cellRoom)
    const read = cellsOf(await fx.backend.readCells(cellRoom, cellInc, { prefix: '' }))
    expect(
      await fx.backend.compareExchangeCells(cellRoom, cellInc, read.revision, [
        { key: 'ttl', set: { bytes: bytes('live'), ttlMs: 60_000 } },
      ]),
    ).toBe('committed')

    const tombRoom = nextId('room')
    const { inc: tombInc, head } = await openRoom(fx.backend, tombRoom)
    const { head: closing, leaseId } = await enterClosing(fx.backend, tombRoom, head)
    accepted(await fx.backend.commitLane(tombRoom, tombInc, CONTROL, bytes('closed'), { closingLease: leaseId }))
    okHead(await finalizeClose(fx.backend, tombRoom, closing, leaseId))

    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(Date.now() + 10 * 365 * 24 * 60 * 60 * 1_000)
    expect(
      text(cellsOf(await fx.backend.readCells(cellRoom, cellInc, { prefix: '' })).cells.get('ttl') as Uint8Array),
    ).toBe('live')
    expect((await fx.backend.readHead(tombRoom))?.head.state).toBe('closed')
  })

  it('keys subscription ownership by room and incarnation, so a same-inc peer room survives drop', async () => {
    const sharedInc = nextId('shared-inc')
    const roomA = nextId('room')
    const roomB = nextId('room')
    const headA = okHead(
      await fx.backend.compareExchangeHead(
        roomA,
        { expect: 'absent' },
        {
          head: { currentInc: sharedInc, state: 'open', config: bytes('a') },
        },
      ),
    )
    okHead(
      await fx.backend.compareExchangeHead(
        roomB,
        { expect: 'absent' },
        {
          head: { currentInc: sharedInc, state: 'open', config: bytes('b') },
        },
      ),
    )
    const latch = collector()
    const peer = fx.backend.subscribeLane(roomB, sharedInc, SEMANTIC, latch.receiver)
    await peer.ready

    const { head: closing, leaseId } = await enterClosing(fx.backend, roomA, headA)
    accepted(await fx.backend.commitLane(roomA, sharedInc, CONTROL, bytes('closed'), { closingLease: leaseId }))
    okHead(await finalizeClose(fx.backend, roomA, closing, leaseId))
    await fx.backend.dropGeneration(roomA, sharedInc)

    expect(peer.state()).toBe('ready')
    const result = accepted(await fx.backend.commitLane(roomB, sharedInc, SEMANTIC, bytes('still-live')))
    await result.delivery
    await latch.waitFor(1)
    expect(latch.payloads()).toEqual(['still-live'])
  })

  it('applies the selected HeadCx predicate before deleting a closed tombstone', async () => {
    const roomId = nextId('room')
    const { inc, head } = await openRoom(fx.backend, roomId)
    const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
    accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
    const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))

    const guarded = await fx.backend.compareExchangeHead(
      roomId,
      { expect: { rev: tombstone.rev, closingLease: leaseId } },
      { delete: true },
    )
    expect(guarded).toHaveProperty('conflict', true)
    expect((await fx.backend.readHead(roomId))?.head.state).toBe('closed')
    okDeleted(await fx.backend.compareExchangeHead(roomId, { expect: { rev: tombstone.rev } }, { delete: true }))
  })

  it('keeps a concurrent newer directory tag under stale compare-delete cleanup', async () => {
    const roomId = nextId('dir')
    await fx.backend.directoryPut(roomId, 'inc-old')
    const reachedApply = deferred()
    const release = deferred()
    fx.setBeforeDirectoryDeleteApply(async () => {
      reachedApply.resolve()
      await release.promise
    })
    const staleDelete = fx.backend.directoryDelete(roomId, 'inc-old')
    await reachedApply.promise
    await fx.backend.directoryPut(roomId, 'inc-new')
    release.resolve()
    await staleDelete
    expect((await fx.backend.directoryList(roomId)).entries).toEqual([{ roomId, incTag: 'inc-new' }])
  })

  it('rejects only the affected delivery when the ratified local PING flush fails', async () => {
    const roomId = nextId('room')
    const { inc } = await openRoom(fx.backend, roomId)
    const latch = collector()
    const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
    await sub.ready
    const subscriber = fx.subscriber as unknown as { ping: () => Promise<string> }
    const originalPing = subscriber.ping
    subscriber.ping = () => Promise.reject(new Error('forced PING transport failure'))
    const failed = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('accepted')))
    await expect(failed.delivery).rejects.toThrow('forced PING transport failure')
    subscriber.ping = originalPing

    const next = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('next')))
    await next.delivery
    await latch.waitFor(2)
    expect(latch.payloads()).toEqual(['accepted', 'next'])
  })
})

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`condition did not settle within ${timeoutMs} ms`)
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}
