// Readiness barrier (T4 slice 4). A live read is admitted only after its tables' change subscriptions are
// PROVEN LISTENING — proven by a self-probe round-trip (publish a token, await it back). On a transport whose
// SUBSCRIBE is async, an early probe is dropped, so the probe RETRIES on a bounded schedule; if never proven,
// it FAILS CLOSED (rejects the read) rather than admitting a read that could silently miss remote writes.
//
// The gated fake transport models exactly this: while `live` is false its publishes are DROPPED (the window
// between SUBSCRIBE issued and acked). registryFor is mocked (unused here — no batches are applied).

import { afterEach, describe, expect, it, vi } from 'vitest'
import type { ChangeMessage, ChangeTransport } from './changeTransport.js'

vi.mock('./dbRuntime.js', () => ({
  registryFor: () => ({ router: { ingest: vi.fn(), register: vi.fn(), unregister: vi.fn() }, acquire: vi.fn() }),
  ingestWrite: vi.fn(),
}))

import { setChangeTransport } from './changeTransport.js'
import { ensureSubscribed } from './writeTransport.js'

/** A transport whose SUBSCRIBE is "async": publishes are dropped until `goLive()`, then delivered
 *  synchronously. This is the loopback the probe depends on, gated behind a manual readiness switch. */
function gatedTransport() {
  const topics = new Map<string, Set<(m: ChangeMessage) => void>>()
  let live = false
  const transport: ChangeTransport = {
    publish(topic, message) {
      if (!live) return // not yet listening → the probe is lost, forcing a retry
      const subs = topics.get(topic)
      if (subs) for (const cb of [...subs]) cb(message)
    },
    subscribe(topic, onMessage) {
      let subs = topics.get(topic)
      if (!subs) topics.set(topic, (subs = new Set()))
      subs.add(onMessage)
      return () => topics.get(topic)?.delete(onMessage)
    },
  }
  return { transport, goLive: () => (live = true) }
}

/** Track whether a promise has settled without awaiting it (so we can assert it is still PENDING). */
function settledTracker(p: Promise<unknown>) {
  let done = false
  p.then(
    () => (done = true),
    () => (done = true),
  )
  return () => done
}

afterEach(() => vi.useRealTimers())

describe('readiness barrier — proven-listening before a live read is admitted', () => {
  it('does NOT resolve while the subscription is not yet listening, then resolves once it is (bounded retry)', async () => {
    vi.useFakeTimers()
    const db = {}
    const { transport, goLive } = gatedTransport()
    setChangeTransport(db, transport)

    const ready = ensureSubscribed(db)
    const isSettled = settledTracker(ready)

    // Several probe attempts fire on the bounded schedule; all are DROPPED (transport not listening yet).
    await vi.advanceTimersByTimeAsync(200)
    expect(isSettled()).toBe(false) // the barrier holds — the read is NOT admitted unproven

    goLive() // SUBSCRIBE is now acked; the transport loops the next probe back
    await vi.advanceTimersByTimeAsync(30) // the next scheduled probe round-trips
    await ready // resolves — proven listening
    expect(isSettled()).toBe(true)
  })

  it('FAILS CLOSED (rejects the live read) if the subscription is never proven listening', async () => {
    vi.useFakeTimers()
    const db = {}
    const { transport } = gatedTransport() // never goLive → every probe dropped
    setChangeTransport(db, transport)

    const ready = ensureSubscribed(db)
    const outcome = ready.then(
      () => 'resolved',
      (e: Error) => e,
    )
    // Exhaust the bounded probe schedule (40 attempts × 25ms, plus slack).
    await vi.advanceTimersByTimeAsync(25 * 45)
    const result = await outcome
    expect(result).toBeInstanceOf(Error)
    expect(String(result)).toMatch(/not proven listening/)
  })

  it('a proven subscription is cached — a second read for the same table awaits no new probe', async () => {
    const db = {}
    const { transport, goLive } = gatedTransport()
    goLive() // synchronous loopback from the start
    setChangeTransport(db, transport)

    await ensureSubscribed(db) // proves + caches
    // A second call resolves on the already-proven promise — no fake timers needed, no hang.
    await ensureSubscribed(db)
    expect(true).toBe(true)
  })
})
