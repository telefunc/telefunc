// The change transport is SET-ONCE per db, and the resolution is frozen at FIRST USE — including when that
// resolution is the built-in default. Review blocker: the default was never recorded, so a later
// setChangeTransport() silently installed an override while readiness/subscriptions stayed proven on the
// default → remote writes on the new transport were missed with no error.

import { describe, expect, it } from 'vitest'
import {
  createInMemoryChangeTransport,
  defaultChangeTransport,
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
