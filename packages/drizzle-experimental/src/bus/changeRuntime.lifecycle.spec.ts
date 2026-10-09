// SUBSCRIPTION LIFECYCLE: what a detach must confirm before the db counts as idle, and what it must forget.
//
// Quiescence is what admits a change-transport rotation (changeTransport.ts). It has to mean the strong
// thing — nobody holding a ref, no listener attached, no unconfirmed detach, and no transition still in
// flight — because a db that merely LOOKS idle would let a swap strand a live listener on the old transport.
//
// The forgetting is the same invariant seen from the other side: sequence watermarks describe a position we
// only hold while subscribed, so carrying them across a detach lets the NEXT subscription judge live traffic
// against a number from the previous one.

import { beforeEach, describe, expect, it, vi } from 'vitest'
vi.mock('./dbRuntime.js', async () => (await import('./changeRuntime.registryMock.js')).dbRuntimeMock())

import { createInMemoryChangeTransport } from './changeTransport.js'
import { registryFor } from './dbRuntime.js'
import { acquireSubscription, changeTopicFor, isQuiescent, publishBatch, setChangeTransport } from './changeRuntime.js'
import { change, engine, flush, resetEngine, twoInstances, watching } from './changeRuntime.testKit.js'

beforeEach(resetEngine)

describe('write transport — a confirmed detach drops the sequence state it can no longer follow', () => {
  it('after re-subscribing, an origin is FIRST-SEEN again rather than judged against a stale watermark', async () => {
    // A detach means we stop following everyone's stream, so the watermarks describe a position we no longer
    // hold. Keeping them lets the NEXT subscription judge live traffic against a number from the previous
    // one — and since anything at or below a watermark is dropped as a duplicate, that is a silent missed
    // invalidation rather than an over-fire. (It also bounds the map: a long-lived process would otherwise
    // accumulate an entry per peer that ever restarted.)
    //
    // Honest about what this pins: it asserts the stated invariant — re-subscribing sees every origin as
    // first-seen — by replaying a payload across the cycle. Removing `seen.clear()` survived the entire
    // suite before this case existed, because the state it clears had no observable consequence anywhere.
    const { transport, dbA, dbB } = await twoInstances()
    const captured: string[] = []
    await transport.subscribe(changeTopicFor(dbA), (payload) => captured.push(payload))
    publishBatch(dbA, { changes: [change('users')] }) // seq 1
    await flush()

    const receiver = { $client: (dbB as { $client: object }).$client }
    setChangeTransport(receiver, transport)
    registryFor(receiver).router.register(watching('users'))
    const ingest = vi.fn()
    engine.perDb.set(receiver, ingest)

    const first = await acquireSubscription(receiver)
    transport.publish(changeTopicFor(receiver), captured[0]!) // seq 1 — the baseline for this subscription
    expect(ingest).toHaveBeenCalledTimes(1)

    first.release() // …detach confirms, and the watermark for that origin goes with it
    await flush()
    const second = await acquireSubscription(receiver)
    try {
      transport.publish(changeTopicFor(receiver), captured[0]!)
      // Judged against a CLEARED map, seq 1 is a fresh baseline and is applied. Judged against the stale one
      // (last 1, unknownBelow 1) it sits at the watermark and is dropped as a duplicate — no second call.
      expect(ingest).toHaveBeenCalledTimes(2)
    } finally {
      second.release()
      await flush()
    }
  })
})

describe('write transport — the quiescent boundary', () => {
  it('a db that never had a live read is quiescent', () => {
    expect(isQuiescent({})).toBe(true)
  })

  it('is NOT quiescent while a ref is held, and is again once it is released and settled', async () => {
    const db = {}
    setChangeTransport(db, createInMemoryChangeTransport())
    const ref = await acquireSubscription(db)
    expect(isQuiescent(db)).toBe(false)
    ref.release()
    await flush()
    expect(isQuiescent(db)).toBe(true)
  })

  it('is NOT quiescent mid-detach, before the unsubscribe has confirmed', async () => {
    const db = {}
    let confirmDetach: (() => void) | undefined
    setChangeTransport(db, {
      publish: () => {},
      subscribe: async () => ({ unsubscribe: () => new Promise<void>((resolve) => (confirmDetach = resolve)) }),
    })
    const ref = await acquireSubscription(db)
    ref.release()
    await flush()
    // `active` is cleared before its detach is awaited, so refs===0 and active===undefined ALREADY hold
    // here. Only the in-flight transition distinguishes this from a finished detach — rotating now would
    // swap the transport out from under a listener that is still attached to it.
    expect(isQuiescent(db)).toBe(false)
    confirmDetach!()
    await flush()
    expect(isQuiescent(db)).toBe(true)
  })

  it('is NOT quiescent while a detach is UNCONFIRMED, and the sequence state is not silently inherited', async () => {
    const warn = vi.spyOn(console, 'error').mockImplementation(() => {})
    const db = {}
    setChangeTransport(db, {
      publish: () => {},
      subscribe: async () => ({ unsubscribe: () => Promise.reject(new Error('broker unreachable')) }),
    })
    const ref = await acquireSubscription(db)
    ref.release()
    await flush()
    // The listener's status is unknown, so there is nothing safe to rotate to: a new transport would leave
    // the old listener attached and the next precise batch applied twice.
    expect(isQuiescent(db)).toBe(false)
    warn.mockRestore()
  })
})
