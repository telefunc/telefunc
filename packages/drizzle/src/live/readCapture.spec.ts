// The engine read-capture pipeline (Ticket 6 §U3). Proves wrapLiveSelect end-to-end against a REAL
// PGlite db + the telefunc Live primitive: builder → IR → compile → registry.acquire (eager hydrate)
// → ClientLive; the serialize-time _activate redeems the token (flips redeemed=true) and the
// finally-sweep releases only the un-activated ones. The carrier/sync-mode concern is covered by the
// Generator's dbLiveRuntime.spec (fake engine); the seqAtRead fence's seq-comparison is covered by
// registry.spec §6.2b/§6.2c — here we exercise the real redeem/lease wiring behind the seam.

import { PGlite } from '@electric-sql/pglite'
import { sql } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import type { ClientLive } from 'telefunc'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { extractQueryShape } from '../extract/queryShape.js'
import type { ReadToken } from '../graph/registry.js'
import { type ReadCarrier, compilePlanFor, disposeUnredeemedReads, wrapLiveSelect } from './readCapture.js'

type UserRow = { id: number; tag: string }
const users = pg.pgTable('users', { id: pg.integer('id').primaryKey(), tag: pg.text('tag') })

let client: PGlite
let db: ReturnType<typeof drizzle>

beforeAll(async () => {
  client = new PGlite()
  db = drizzle({ client })
  await client.exec('create table users (id int primary key, tag text)')
  await client.query("insert into users (id, tag) values (1, 'a'), (2, 'b')")
})
afterAll(async () => {
  await client.close()
})

const carrierOf = (): ReadCarrier => ({ mintedTokens: [] })
const liveSelect = (builder: unknown, carrier: ReadCarrier): Promise<ClientLive<UserRow[]>> =>
  wrapLiveSelect(builder, carrier, db as object) as Promise<ClientLive<UserRow[]>>
/** Drive the wire replacer's serialize-time activation path (the Live source's subscribe). */
const activate = (live: ClientLive<unknown>): void => (live as unknown as { _activate(): void })._activate()

// ── the real pipeline ───────────────────────────────────────────────

describe('wrapLiveSelect — builder → IR → compile → acquire (eager hydrate) → ClientLive', () => {
  it('resolves to a ClientLive carrying the initial rows + an INERT minted token (not yet redeemed)', async () => {
    const carrier = carrierOf()
    const live = await liveSelect(db.select().from(users), carrier)
    expect([...live.data].sort((a, b) => a.id - b.id)).toEqual([
      { id: 1, tag: 'a' },
      { id: 2, tag: 'b' },
    ])
    expect(carrier.mintedTokens).toHaveLength(1) // one read token minted on the carrier
    expect(carrier.mintedTokens[0]!.redeemed).toBe(false) // inert until serialize-time activation
  })

  it('serialize-time _activate REDEEMS the token — proven by a second redeem throwing (not just the flag)', async () => {
    const carrier = carrierOf()
    const live = await liveSelect(db.select().from(users), carrier)
    const token = carrier.mintedTokens[0]!.token
    activate(live) // the wire replacer's _activate → the Live source subscribe → token.redeem()
    expect(carrier.mintedTokens[0]!.redeemed).toBe(true)
    expect(() => token.redeem()).toThrow() // REAL registry consequence: actually redeemed (double-redeem rejected) — not the co-set flag
  })

  it('#1 — a rejecting σ-read still leaves a sweepable token on the carrier (net-zero, no leak)', async () => {
    const carrier = carrierOf()
    // acquire succeeds (born-coarse for the pooled PGlite conn → no scan); the initial read then throws (1/0).
    await expect(liveSelect(db.select().from(users).where(sql`1 / 0 = 1`), carrier)).rejects.toThrow()
    expect(carrier.mintedTokens).toHaveLength(1) // OWNED before the fallible read → sweepable (buggy push-after-await gave 0)
    expect(carrier.mintedTokens[0]!.redeemed).toBe(false)
    expect(() => disposeUnredeemedReads(carrier)).not.toThrow() // released → net-zero
  })

  it('#5 — a pooled-unpinned connection compiles a COARSE plan (no precise hydration from a mismatched session)', () => {
    // PGlite's $client is not a pinned pg Client → isSingleSession=false → pooled-unpinned.
    const shape = extractQueryShape(db.select().from(users), { dialect: 'pg' })
    expect(compilePlanFor(db as object, shape)().coarse).toBe(true) // forced coarse; a precise plan would be .coarse=false
  })

  it('#2 (engine half) — an activated handle releases its lease on _release (channel close); the sweep then skips it, no double-release', async () => {
    const carrier = carrierOf()
    const live = await liveSelect(db.select().from(users), carrier)
    activate(live) // _activate → redeem → lease
    ;(live as unknown as { _release(): void })._release() // channel close/abort → source teardown → lease.release()
    expect(carrier.mintedTokens[0]!.redeemed).toBe(true) // activated (channel-owned)
    expect(() => disposeUnredeemedReads(carrier)).not.toThrow() // sweep skips the redeemed entry — lease already released, no double
  })
})

// ── the finally-sweep ───────────────────────────────────────────────

describe('disposeUnredeemedReads — the request finally-sweep', () => {
  it('releases only UN-redeemed tokens (activated ones are channel-owned and skipped)', () => {
    const releaseA = vi.fn()
    const releaseB = vi.fn()
    const carrier: ReadCarrier = {
      mintedTokens: [
        { token: { release: releaseA } as unknown as ReadToken, redeemed: false },
        { token: { release: releaseB } as unknown as ReadToken, redeemed: true },
      ],
    }
    disposeUnredeemedReads(carrier)
    expect(releaseA).toHaveBeenCalledTimes(1) // never serialized → released (net-zero)
    expect(releaseB).not.toHaveBeenCalled() // activated → its lease is owned by the wire channel
  })

  it('a never-serialized real handle is disposed by the sweep (net-zero, no leak)', async () => {
    const carrier = carrierOf()
    await liveSelect(db.select().from(users), carrier) // minted, never activated
    expect(carrier.mintedTokens[0]!.redeemed).toBe(false)
    expect(() => disposeUnredeemedReads(carrier)).not.toThrow() // releases the un-redeemed engine token
  })
})

// ── the chainable thenable proxy ────────────────────────────────────

describe('wrapLiveSelect — chainable thenable builder', () => {
  it('chain methods re-wrap (stay live + thenable); non-builder returns pass through untouched', () => {
    const toSQLResult = { sql: 'select', params: [] as unknown[] }
    const inner = { toSQL: () => toSQLResult, where: () => ({ toSQL: () => toSQLResult }) }
    const base = { toSQL: () => toSQLResult, from: () => inner }
    const wrapped = wrapLiveSelect(base, carrierOf(), {} as object) as Record<string, unknown>

    const afterFrom = (wrapped.from as () => Record<string, unknown>)()
    expect(typeof afterFrom.then).toBe('function') // the chain stays a thenable live-builder
    expect(typeof afterFrom.where).toBe('function') // …and keeps forwarding chain methods
    expect((wrapped.toSQL as () => unknown)()).toBe(toSQLResult) // a non-builder return is passed through raw
  })
})
