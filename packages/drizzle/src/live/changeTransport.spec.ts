// The change transport is SET-ONCE per db, and the resolution is frozen at FIRST USE — including when that
// resolution is the built-in default. Review blocker: the default was never recorded, so a later
// setChangeTransport() silently installed an override while readiness/subscriptions stayed proven on the
// default → remote writes on the new transport were missed with no error.

import { describe, expect, it } from 'vitest'
import {
  configureChanges,
  createInMemoryChangeTransport,
  defaultChangeTransport,
  namespaceFor,
  setChangeNamespace,
  setChangeTransport,
  transportFor,
} from './changeTransport.js'

describe('changeTransport — set-once, frozen at first use', () => {
  it('freezes the DEFAULT on first use: a later different transport THROWS (never silently ignored)', () => {
    const db = {}
    expect(transportFor(db)).toBe(defaultChangeTransport) // first use resolves + freezes the default
    expect(() => setChangeTransport(db, createInMemoryChangeTransport())).toThrow(/already in use/)
    expect(transportFor(db)).toBe(defaultChangeTransport) // and the resolution is unchanged
  })

  it('freezes an explicit override on first use: a later different transport THROWS', () => {
    const db = {}
    const chosen = createInMemoryChangeTransport()
    setChangeTransport(db, chosen)
    expect(transportFor(db)).toBe(chosen)
    expect(() => setChangeTransport(db, createInMemoryChangeTransport())).toThrow(/already in use/)
    expect(transportFor(db)).toBe(chosen)
  })

  it('rejects a conflicting override registered BEFORE first use too', () => {
    const db = {}
    setChangeTransport(db, createInMemoryChangeTransport())
    expect(() => setChangeTransport(db, createInMemoryChangeTransport())).toThrow(/already in use/)
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
    expect(() => setChangeNamespace(db, 'orders')).toThrow(/already in use/)
    expect(namespaceFor(db)).toBe(derived) // and the resolution is unchanged
  })

  it('freezes an EXPLICIT namespace too, and re-registering the same one is a no-op', () => {
    const db = {}
    setChangeNamespace(db, 'orders')
    expect(namespaceFor(db)).toBe('orders')
    expect(() => setChangeNamespace(db, 'other')).toThrow(/already in use/)
    expect(() => setChangeNamespace(db, 'orders')).not.toThrow()
    expect(namespaceFor(db)).toBe('orders')
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
    expect(() => configureChanges(db, { transport: chosen, namespace: 'different' })).toThrow(/already in use/)
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
