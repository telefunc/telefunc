// RESOLUTION: which transport and which logical-database identity a db resolves to, and the exactly-one
// rule for when either may change. (Publish lives in changeRuntime.publish.spec.ts, ordering and the
// transport-era cut in changeRuntime.ordering.spec.ts, subscription teardown in
// changeRuntime.lifecycle.spec.ts.)
//
// The transport is SET-ONCE WHILE IN USE, and the resolution is frozen at first use — including when that
// resolution is the built-in default. Review blocker: the default was never recorded, so a later
// setChangeTransport() silently installed an override while readiness/subscriptions stayed proven on the
// default → remote writes on the new transport were missed with no error.
//
// QUIESCENCE IS THE ONE RULE. The freeze protects live subscriptions and the readiness they proved, so it
// ends where they end: a db that has only ever published may rotate. Every case below therefore establishes
// a REAL subscription to reach the forbidden state — asserting a `quiescent` parameter instead is what let
// this file once pin two opposite contracts for a single db state.

import { describe, expect, it } from 'vitest'
import { createInMemoryChangeTransport, defaultChangeTransport } from './changeTransport.js'
import {
  acquireSubscription,
  configureChangeRuntime,
  isQuiescent,
  namespaceFor,
  setChangeNamespace,
  setChangeTransport,
  transportFor,
} from './changeRuntime.js'

describe('changeTransport — set-once, frozen at first use', () => {
  it('freezes the DEFAULT while a subscription is live: a later different transport THROWS', async () => {
    const db = {}
    expect(transportFor(db)).toBe(defaultChangeTransport) // first use resolves the default…
    const ref = await acquireSubscription(db) // …and NOW there is a listener + readiness proof to strand
    try {
      expect(() => setChangeTransport(db, createInMemoryChangeTransport())).toThrow(/change transport/)
      expect(transportFor(db)).toBe(defaultChangeTransport) // and the resolution is unchanged
    } finally {
      ref.release()
    }
  })

  it('freezes an explicit override while a subscription is live: a later different transport THROWS', async () => {
    const db = {}
    const chosen = createInMemoryChangeTransport()
    setChangeTransport(db, chosen)
    expect(transportFor(db)).toBe(chosen)
    const ref = await acquireSubscription(db)
    try {
      expect(() => setChangeTransport(db, createInMemoryChangeTransport())).toThrow(/change transport/)
      expect(transportFor(db)).toBe(chosen)
    } finally {
      ref.release()
    }
  })

  it('rejects a conflicting override in the window where the db is IN USE but not yet RESOLVED', async () => {
    // The `configured`-but-not-`resolved` branch of the check, and the proof it is not dead code under the
    // unified quiescence rule. A subscription makes the db non-quiescent SYNCHRONOUSLY (refs++/settling++),
    // while the `transportFor` that resolves it runs a microtask later — so a db really can be in use with
    // an override registered and no resolution yet. A rotation admitted in that window would be picked up by
    // the very subscription being established, on a transport its caller never asked for.
    const db = {}
    const chosen = createInMemoryChangeTransport()
    setChangeTransport(db, chosen) // configured, NOT resolved — nothing has called transportFor
    const pending = acquireSubscription(db) // in use from this synchronous point onward
    // The window is ENTERED, not assumed: without this the test could pass against a quiescent db for the
    // wrong reason, which is the whole failure mode the ruling asked to rule out.
    expect(isQuiescent(db)).toBe(false)
    expect(() => setChangeTransport(db, createInMemoryChangeTransport())).toThrow(/change transport/)
    const ref = await pending
    expect(transportFor(db)).toBe(chosen) // the subscription got the transport its caller registered
    ref.release()
  })

  it('re-registering the SAME transport is a no-op (a repeated reactiveDrizzle call is idempotent)', () => {
    const db = {}
    const chosen = createInMemoryChangeTransport()
    setChangeTransport(db, chosen)
    transportFor(db)
    expect(() => setChangeTransport(db, chosen)).not.toThrow()
    expect(transportFor(db)).toBe(chosen)
  })

  it('distinct dbs resolve independently', () => {
    const dbA = {}
    const dbB = {}
    const chosen = createInMemoryChangeTransport()
    setChangeTransport(dbA, chosen)
    expect(transportFor(dbA)).toBe(chosen)
    expect(transportFor(dbB)).toBe(defaultChangeTransport)
  })

  // The freeze exists to protect live subscriptions and the readiness they proved. Where there is no such
  // thing to protect, it has nothing to enforce — so it does not.
  describe('the quiescent exception', () => {
    it('rotates a resolved transport when the db is quiescent, and later reads resolve to the NEW one', () => {
      const db = {}
      const first = createInMemoryChangeTransport()
      configureChangeRuntime(db, { transport: first })
      expect(transportFor(db)).toBe(first) // resolved and frozen…

      const second = createInMemoryChangeTransport()
      configureChangeRuntime(db, { transport: second })
      // …and unfrozen by the rotation: `transportFor` must RE-RESOLVE rather than hand back the old
      // resolution, or the swap would be accepted and then quietly ignored — the worst of both.
      expect(transportFor(db)).toBe(second)
    })

    it('rotates away from the DEFAULT: one publish-only write does not freeze the db forever', () => {
      const db = {}
      expect(transportFor(db)).toBe(defaultChangeTransport) // a write resolved the default
      const injected = createInMemoryChangeTransport()
      configureChangeRuntime(db, { transport: injected, namespace: 'orders' })
      expect(transportFor(db)).toBe(injected)
    })

    it('does NOT rotate while the db is in use — the mid-subscription swap stays forbidden', async () => {
      const db = {}
      const held = createInMemoryChangeTransport()
      configureChangeRuntime(db, { transport: held })
      transportFor(db)
      // A REAL subscription rather than an asserted flag: quiescence is the runtime's own state, so the only
      // way to reach the forbidden case is to hold the very thing the freeze exists to protect.
      const ref = await acquireSubscription(db)
      try {
        expect(() => configureChangeRuntime(db, { transport: createInMemoryChangeTransport() })).toThrow(
          /change transport/,
        )
        expect(transportFor(db)).toBe(held)
      } finally {
        ref.release()
      }
    })

    it('a quiescent rotation still installs atomically: a rejected namespace leaves the transport alone', () => {
      const db = {}
      const held = createInMemoryChangeTransport()
      configureChangeRuntime(db, { transport: held, namespace: 'orders' })
      transportFor(db)
      // Quiescence admits a TRANSPORT swap; it says nothing about the logical database's identity, which
      // stays set-once. The namespace throw must take the transport down with it.
      expect(() =>
        configureChangeRuntime(db, { transport: createInMemoryChangeTransport(), namespace: 'other' }),
      ).toThrow(/change namespace/)
      expect(transportFor(db)).toBe(held)
    })
  })
})

// ── logical-database identity ────────────────────────────────────────

describe('changeTransport — database identity, derived and frozen', () => {
  it('a REAL postgres-js client is a FUNCTION, and two dbs over it are ONE database', async () => {
    // The defect this pins: `typeof $client === 'object'` excluded functions, and postgres-js hands back its
    // `sql` client as a callable. Two runtimes over the identical client derived DIFFERENT namespaces, so
    // they published on different topics and every sibling invalidation was silently lost. Driven against
    // the installed driver rather than a synthetic callable — the shape is the whole point of the test.
    const { default: postgres } = await import('postgres')
    const { drizzle } = await import('drizzle-orm/postgres-js')
    const sql = postgres('postgres://user:pass@127.0.0.1:1/db', { max: 1, connect_timeout: 1 })
    try {
      const dbA = drizzle({ client: sql }) as unknown as object
      const dbB = drizzle({ client: sql }) as unknown as object
      expect(typeof (dbA as { $client: unknown }).$client).toBe('function') // the reason it broke
      expect((dbA as { $client: unknown }).$client).toBe((dbB as { $client: unknown }).$client)
      expect(namespaceFor(dbA)).toBe(namespaceFor(dbB)) // …one client, one database
    } finally {
      await sql.end({ timeout: 0 }).catch(() => {})
    }
  })

  it('dbs over DIFFERENT clients stay different databases (so the above is not blanket agreement)', () => {
    const clientOne = () => {}
    const clientTwo = () => {}
    expect(namespaceFor({ $client: clientOne })).not.toBe(namespaceFor({ $client: clientTwo }))
  })

  it('FREEZES the derived namespace: a later explicit one THROWS rather than moving the topic', () => {
    // Resolution admits subscriptions on that topic. Silently switching future publications to a new topic
    // while the listener stays on the old one is a systematic miss, not an over-fire.
    const db = { $client: {} }
    const derived = namespaceFor(db) // resolves + freezes
    expect(() => setChangeNamespace(db, 'orders')).toThrow(/change namespace/)
    expect(namespaceFor(db)).toBe(derived) // and the resolution is unchanged
  })

  it('freezes an EXPLICIT namespace too, and re-registering the same one is a no-op', () => {
    const db = {}
    setChangeNamespace(db, 'orders')
    expect(namespaceFor(db)).toBe('orders')
    expect(() => setChangeNamespace(db, 'other')).toThrow(/change namespace/)
    expect(() => setChangeNamespace(db, 'orders')).not.toThrow()
    expect(namespaceFor(db)).toBe('orders')
  })

  it('treats an EMPTY namespace as a configured value, not as an absent one', () => {
    // The two slots guarded absence differently before they shared a machine — the transport on falsiness,
    // the namespace on `undefined` alone — and that difference is load-bearing here: `''` reaches
    // `configureChangeRuntime` (the entry only requires a namespace alongside an injected transport), and
    // reading it as absent would silently resolve to the DERIVED identity instead, putting this db on a
    // different topic from the one its operator named.
    const db = { $client: {} }
    setChangeNamespace(db, '')
    expect(namespaceFor(db)).toBe('')
  })

  it('is STABLE across calls even when $client is a fresh getter each time', () => {
    // A db whose `$client` is a getter returning a new object would otherwise derive a new identity on every
    // call. The per-db resolution cache is what makes the topic stable regardless.
    const db = {
      get $client() {
        return {}
      },
    }
    expect(namespaceFor(db)).toBe(namespaceFor(db))
  })

  it('installs transport and namespace ATOMICALLY — a rejected namespace leaves no transport behind', () => {
    const db = {}
    const chosen = createInMemoryChangeTransport()
    setChangeNamespace(db, 'orders')
    // The namespace conflicts, so the whole install must fail — including the transport beside it.
    expect(() => configureChangeRuntime(db, { transport: chosen, namespace: 'different' })).toThrow(/change namespace/)
    expect(transportFor(db)).toBe(defaultChangeTransport) // NOT the half-installed `chosen`
  })
})

// ── the no-backlog clause ────────────────────────────────────────────

describe('changeTransport — the default delivers nothing published before a subscription existed', () => {
  it('a payload published BEFORE subscribe never reaches that subscriber', async () => {
    // The runtime depends on this: a live read's snapshot is taken after its subscription is admitted, so
    // anything published earlier is already in the data it read. Replaying such a payload afterwards would
    // apply it a SECOND time, and the first message from a publisher is indistinguishable from a legitimate
    // one — there is no cheap detection, so the obligation is contractual. This pins that the transport we
    // actually ship honours it; a store-and-forward adapter that does not is out of contract.
    const transport = createInMemoryChangeTransport()
    transport.publish('t', 'published-before-anyone-listened')

    const heard: string[] = []
    await transport.subscribe('t', (payload) => heard.push(payload))
    expect(heard).toEqual([]) // no backlog

    transport.publish('t', 'after')
    expect(heard).toEqual(['after']) // …and it is genuinely delivering, so the emptiness above means something
  })
})
