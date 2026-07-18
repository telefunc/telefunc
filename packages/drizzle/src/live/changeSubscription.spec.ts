// The db's change-subscription lifecycle: readiness at one end, teardown at the other, and the refcount
// that joins them.
//
// READINESS. A live read is admitted only once the transport has ADMITTED this db's subscription — which is
// precisely what awaiting `subscribe()` means. Readiness is a CONTROL-PLANE fact, so it is asked of the
// control plane; the data-plane probe this replaced (publish a token, await it back on a bounded retry pump)
// demanded self-delivery from every adapter and still proved nothing about the cluster.
//
// TEARDOWN. Subscriptions used to be established and never dropped, so the callback closing over `db` pinned
// the db runtime for the lifetime of the process. Now every live read holds a ref, and the last one out
// detaches the listener.
//
// The fake transport is the real Redis shape: SUBSCRIBE is a command whose subscription does not exist until
// the broker acknowledges it, and UNSUBSCRIBE likewise takes until the listener is actually detached. Both
// are held open by hand here, because every interesting case lives inside those two windows.

import { describe, expect, it, vi } from 'vitest'
import type { ChangeSubscription, ChangeTransport } from './changeTransport.js'

const engine = vi.hoisted(() => ({ ingest: vi.fn() }))
vi.mock('./dbRuntime.js', () => ({
  registryFor: () => ({
    router: { ingest: engine.ingest, register: vi.fn(), unregister: vi.fn(), watchedTables: () => ['users'] },
    acquire: vi.fn(),
  }),
  ingestWrite: vi.fn(),
}))

import { setChangeTransport } from './changeTransport.js'
import { acquireSubscription } from './writeTransport.js'

/** A broker whose SUBSCRIBE and UNSUBSCRIBE are both acknowledged by hand.
 *
 *  `listenerPeak` is the instrument behind the no-overlap claim: if a re-subscribe ever began before the
 *  unsubscribe it follows had detached, two callbacks would be attached at once and the peak would read 2 —
 *  and a precise batch delivered to both would be applied twice. */
function brokerTransport(options: { autoAdmit?: boolean; holdUnsubscribe?: boolean } = {}) {
  const pendingSubscribes: { admit: () => void; refuse: (error: Error) => void }[] = []
  const pendingDetaches: (() => void)[] = []
  const listeners = new Set<(payload: string) => void>()
  let subscribes = 0
  let unsubscribes = 0
  let listenerPeak = 0

  const attach = (onPayload: (payload: string) => void): ChangeSubscription => {
    listeners.add(onPayload)
    listenerPeak = Math.max(listenerPeak, listeners.size)
    return {
      unsubscribe() {
        unsubscribes++
        if (!options.holdUnsubscribe) {
          listeners.delete(onPayload)
          return
        }
        return new Promise<void>((resolve) =>
          pendingDetaches.push(() => {
            listeners.delete(onPayload)
            resolve()
          }),
        )
      },
    }
  }

  const transport: ChangeTransport = {
    publish(_topic, payload) {
      for (const listener of [...listeners]) listener(payload)
    },
    subscribe(_topic, onPayload) {
      subscribes++
      return new Promise<ChangeSubscription>((resolve, reject) => {
        const entry = { admit: () => resolve(attach(onPayload)), refuse: reject }
        if (options.autoAdmit) entry.admit()
        else pendingSubscribes.push(entry)
      })
    },
  }

  return {
    transport,
    subscribeCount: () => subscribes,
    unsubscribeCount: () => unsubscribes,
    listenerCount: () => listeners.size,
    listenerPeak: () => listenerPeak,
    admit: () => pendingSubscribes.shift()?.admit(),
    refuse: () => pendingSubscribes.shift()?.refuse(new Error('SUBSCRIBE refused')),
    detach: () => pendingDetaches.shift()?.(),
    deliver: (payload: string) => transport.publish('', payload),
  }
}

function attached(db: object, options?: Parameters<typeof brokerTransport>[0]) {
  const broker = brokerTransport(options)
  setChangeTransport(db, broker.transport)
  return broker
}

/** Let every already-runnable continuation run, so "has not happened yet" means the code held it back and
 *  not that the assertion simply looked too early. */
const settle = async () => {
  for (let i = 0; i < 10; i++) await Promise.resolve()
}

/** Track whether a promise has settled without awaiting it (so we can assert it is still PENDING). */
function settledTracker(p: Promise<unknown>) {
  let done = false
  p.then(
    () => (done = true),
    () => (done = true),
  )
  return async () => {
    await settle()
    return done
  }
}

describe('change subscription — readiness', () => {
  it('does NOT resolve while the SUBSCRIBE is unacknowledged, then resolves once the broker admits it', async () => {
    const db = {}
    const broker = attached(db)

    const acquired = acquireSubscription(db)
    const isSettled = settledTracker(acquired)

    expect(await isSettled()).toBe(false) // the barrier holds — the read is NOT admitted unproven

    broker.admit()
    await acquired
    expect(await isSettled()).toBe(true)
  })

  it('FAILS CLOSED (rejects the live read) when the transport refuses the subscription', async () => {
    const db = {}
    const broker = attached(db)

    const acquired = acquireSubscription(db)
    await settle() // the transition is queued, not synchronous — let the SUBSCRIBE actually be issued
    broker.refuse()

    await expect(acquired).rejects.toThrow(/SUBSCRIBE refused/)
  })

  it('RETRIES after a refusal, leaving nothing subscribed behind', async () => {
    // A transient hiccup must not permanently wedge live reads for this db, and a refused attempt must not
    // leave a half-established subscription nobody owns.
    const db = {}
    const broker = attached(db)

    const first = acquireSubscription(db)
    await settle()
    broker.refuse()
    await expect(first).rejects.toThrow()
    await settle()
    expect(broker.listenerCount()).toBe(0)

    const second = acquireSubscription(db)
    await settle()
    expect(broker.subscribeCount()).toBe(2) // asked again, not served the cached rejection
    broker.admit()
    await second
    expect(broker.listenerCount()).toBe(1)
  })

  it('CONCURRENT reads share ONE underlying subscribe and hold a ref each', async () => {
    const db = {}
    const broker = attached(db)

    const first = acquireSubscription(db)
    const second = acquireSubscription(db)
    await settle()
    expect(broker.subscribeCount()).toBe(1) // one subscription serves every live read on this db

    broker.admit()
    const [refA, refB] = await Promise.all([first, second])

    refA.release()
    await settle()
    expect(broker.unsubscribeCount()).toBe(0) // the other owner still wants it
    refB.release()
    await settle()
    expect(broker.unsubscribeCount()).toBe(1)
  })

  it('an ADMITTED subscription is reused — a later read subscribes nothing new', async () => {
    const db = {}
    const broker = attached(db, { autoAdmit: true })

    await acquireSubscription(db)
    await acquireSubscription(db)
    expect(broker.subscribeCount()).toBe(1)
  })
})

describe('change subscription — teardown at the last release', () => {
  it('the LAST release detaches the listener exactly once, whichever owner goes first', async () => {
    const db = {}
    const broker = attached(db, { autoAdmit: true })
    const first = await acquireSubscription(db)
    const second = await acquireSubscription(db)

    second.release() // the second owner leaves first — order must not matter
    await settle()
    expect(broker.unsubscribeCount()).toBe(0)

    first.release()
    await settle()
    expect(broker.unsubscribeCount()).toBe(1)
  })

  it('after the last release the transport holds NO callback for this db, so a delivery reaches nothing', async () => {
    // The retention limit this closes, observed rather than GC-timed: the callback closes over `db`, so
    // while it is attached the db runtime cannot be collected. Its absence is the proof.
    const db = {}
    const broker = attached(db, { autoAdmit: true })
    const ref = await acquireSubscription(db)

    ref.release()
    await settle()

    expect(broker.listenerCount()).toBe(0)
    broker.deliver('{"version":1,"origin":"elsewhere","changes":[{"table":"users","kind":"coarse"}]}')
    expect(engine.ingest).not.toHaveBeenCalled() // nothing is listening on this db's behalf any more
  })

  it('release is IDEMPOTENT — a double release does not drop another owner’s ref', async () => {
    const db = {}
    const broker = attached(db, { autoAdmit: true })
    const first = await acquireSubscription(db)
    const second = await acquireSubscription(db)

    first.release()
    first.release() // e.g. a channel closing twice
    await settle()

    expect(broker.unsubscribeCount()).toBe(0) // `second` still holds the subscription
    second.release()
    await settle()
    expect(broker.unsubscribeCount()).toBe(1)
  })

  it('re-acquiring while the unsubscribe is IN FLIGHT waits for detachment — the callbacks never overlap', async () => {
    const db = {}
    const broker = attached(db, { autoAdmit: true, holdUnsubscribe: true })
    const first = await acquireSubscription(db)

    first.release()
    await settle() // the unsubscribe is now issued and awaiting the broker's detachment
    expect(broker.unsubscribeCount()).toBe(1)

    const reacquired = acquireSubscription(db)
    await settle()
    expect(broker.subscribeCount()).toBe(1) // NOT re-subscribed yet — the old listener is still attached

    broker.detach()
    await reacquired

    expect(broker.subscribeCount()).toBe(2) // only now
    expect(broker.listenerPeak()).toBe(1) // and never two listeners at once — no batch is applied twice
  })

  it('a re-acquire that arrives BEFORE the teardown runs simply keeps the subscription', async () => {
    // The refcount describes what is wanted, not a queue of commands: an owner arriving in the same tick as
    // the last one leaving must not cost a detach/attach round-trip against the broker.
    const db = {}
    const broker = attached(db, { autoAdmit: true, holdUnsubscribe: true })
    const first = await acquireSubscription(db)

    first.release()
    const reacquired = acquireSubscription(db) // same tick, before the teardown transition runs
    await reacquired

    expect(broker.unsubscribeCount()).toBe(0)
    expect(broker.subscribeCount()).toBe(1)
    expect(broker.listenerCount()).toBe(1)
  })
})
