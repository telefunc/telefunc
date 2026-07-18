// Readiness barrier. A live read is admitted only once this db's change subscription has been ADMITTED by
// the transport — which is precisely what awaiting `subscribe()` means. Readiness is a CONTROL-PLANE fact,
// so it is asked of the control plane; the data-plane probe this replaced (publish a token, await it back on
// a bounded retry pump) demanded self-delivery from every adapter and still proved nothing about the cluster.
//
// The fake transport here holds its `subscribe()` promise open by hand, which is the real Redis shape: the
// SUBSCRIBE command is written, and the subscription does not exist until the broker acknowledges it.
// registryFor is mocked (unused here — no batches are applied).

import { describe, expect, it, vi } from 'vitest'
import type { ChangeSubscription, ChangeTransport } from './changeTransport.js'

vi.mock('./dbRuntime.js', () => ({
  registryFor: () => ({ router: { ingest: vi.fn(), register: vi.fn(), unregister: vi.fn() }, acquire: vi.fn() }),
  ingestWrite: vi.fn(),
}))

import { setChangeTransport } from './changeTransport.js'
import { ensureSubscribed } from './writeTransport.js'

/** A transport whose SUBSCRIBE is acknowledged by hand — `admit()` resolves the pending subscribe,
 *  `refuse()` rejects it. `subscribes` counts how many times the transport was actually asked. */
function brokerTransport() {
  const pending: { admit: () => void; refuse: (error: Error) => void }[] = []
  let subscribes = 0
  const transport: ChangeTransport = {
    publish() {},
    subscribe() {
      subscribes++
      return new Promise<ChangeSubscription>((resolve, reject) => {
        pending.push({ admit: () => resolve({ unsubscribe() {} }), refuse: reject })
      })
    },
  }
  return {
    transport,
    subscribeCount: () => subscribes,
    admit: () => pending.shift()?.admit(),
    refuse: () => pending.shift()?.refuse(new Error('SUBSCRIBE refused')),
  }
}

/** Track whether a promise has settled without awaiting it (so we can assert it is still PENDING). */
function settledTracker(p: Promise<unknown>) {
  let done = false
  p.then(
    () => (done = true),
    () => (done = true),
  )
  return async () => {
    await Promise.resolve() // drain the microtask queue so a settled promise has had its chance
    await Promise.resolve()
    return done
  }
}

describe('readiness barrier — admitted before a live read is', () => {
  it('does NOT resolve while the SUBSCRIBE is unacknowledged, then resolves once the broker admits it', async () => {
    const db = {}
    const broker = brokerTransport()
    setChangeTransport(db, broker.transport)

    const ready = ensureSubscribed(db)
    const isSettled = settledTracker(ready)

    expect(await isSettled()).toBe(false) // the barrier holds — the read is NOT admitted unproven

    broker.admit()
    await ready // resolves — the transport has the subscription
    expect(await isSettled()).toBe(true)
  })

  it('FAILS CLOSED (rejects the live read) when the transport refuses the subscription', async () => {
    const db = {}
    const broker = brokerTransport()
    setChangeTransport(db, broker.transport)

    const ready = ensureSubscribed(db)
    broker.refuse()

    await expect(ready).rejects.toThrow(/SUBSCRIBE refused/)
  })

  it('RETRIES after a refusal: a later read asks the transport again rather than reusing the failure', async () => {
    // A transient hiccup must not permanently wedge live reads for this db.
    const db = {}
    const broker = brokerTransport()
    setChangeTransport(db, broker.transport)

    await expect(
      (() => {
        const first = ensureSubscribed(db)
        broker.refuse()
        return first
      })(),
    ).rejects.toThrow()

    const second = ensureSubscribed(db)
    expect(broker.subscribeCount()).toBe(2) // asked again, not served the cached rejection
    broker.admit()
    await second
  })

  it('CONCURRENT reads share ONE underlying subscribe', async () => {
    const db = {}
    const broker = brokerTransport()
    setChangeTransport(db, broker.transport)

    const first = ensureSubscribed(db)
    const second = ensureSubscribed(db)
    expect(broker.subscribeCount()).toBe(1) // one subscription serves every live read on this db

    broker.admit()
    await Promise.all([first, second])
  })

  it('an ADMITTED subscription is cached — a later read subscribes nothing new', async () => {
    const db = {}
    const broker = brokerTransport()
    setChangeTransport(db, broker.transport)

    const ready = ensureSubscribed(db)
    broker.admit()
    await ready

    await ensureSubscribed(db)
    expect(broker.subscribeCount()).toBe(1)
  })
})
