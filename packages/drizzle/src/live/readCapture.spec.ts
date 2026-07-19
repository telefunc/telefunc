// The engine read-capture pipeline. Proves wrapLiveSelect end-to-end against a REAL
// PGlite db + the telefunc Live primitive: builder → IR → compile → registry.acquire (eager hydrate)
// → Live; the serialize-time activation redeems the token into a channel lease. The seqAtRead fence's
// seq-comparison is covered by registry.notify.spec — here we exercise the real redeem/lease wiring behind the
// seam, and what happens to a read that never reaches the wire at all.
//
// NOTHING in this file provides a telefunc request context, deliberately: the read-capture engine binds to
// none, so `reactiveDrizzle(db)` at module level is the intended shape.

import { PGlite } from '@electric-sql/pglite'
import { entityKind, sql } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import type { Live } from '../primitive/live.js'
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest'
import { extractQueryShape } from '../extract/queryShape.js'
import type { AcquireRequest, ReadToken } from '../graph/registry.js'
import { registryFor } from './dbRuntime.js'
import { compilePlanFor, wrapLiveSelect } from './readCapture.js'

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

// The terminal `.live()` runs the read-capture pipeline (awaiting the builder itself gives plain rows).
const liveSelect = (builder: unknown): Promise<Live<UserRow[]>> =>
  (wrapLiveSelect(builder, db as object) as { live(): Promise<Live<UserRow[]>> }).live()
/** Drive the wire replacer's serialize-time activation path (the Live source's subscribe). */
const activate = (live: Live<unknown>): void => (live as unknown as { activate(): void }).activate()

/** Count what happens to each read's TOKEN, by wrapping the real registry's `acquire`. The token is
 *  otherwise private to the engine, and it is the thing whose release proves a read was reclaimed —
 *  asserting on anything else would be asserting on a proxy for the claim. */
type TrackedToken = { released: number; redeemed: number; token: ReadToken }
function trackTokens(): TrackedToken[] {
  const registry = registryFor(db as object)
  const original = registry.acquire.bind(registry)
  const seen: TrackedToken[] = []
  vi.spyOn(registry, 'acquire').mockImplementation(async (request: AcquireRequest) => {
    const acquired = await original(request)
    const tracked: TrackedToken = { released: 0, redeemed: 0, token: acquired.token }
    seen.push(tracked)
    const token: ReadToken = {
      instanceKey: acquired.token.instanceKey,
      seqAtRead: acquired.token.seqAtRead,
      redeem: () => {
        tracked.redeemed++
        return acquired.token.redeem()
      },
      release: () => {
        tracked.released++
        acquired.token.release()
      },
    }
    return { ...acquired, token }
  })
  return seen
}

afterEach(() => vi.restoreAllMocks())

/** A fake POOLED pg connection for the coarse control: pg dialect (stable entity kind) + a raw client
 *  that is a `Pool` (not a pinned `Client`) + NOT a PgliteDatabase → isSingleSession=false ⇒ coarse. */
class FakePgDialect {
  static [entityKind] = 'PgDialect'
}
class Pool {}
const poolFake = { dialect: new FakePgDialect(), $client: new Pool() } as object

// ── the real pipeline ───────────────────────────────────────────────

describe('wrapLiveSelect — builder → IR → compile → acquire (eager hydrate) → Live', () => {
  it('resolves to a Live carrying the initial rows, with its token INERT (not yet redeemed)', async () => {
    const tokens = trackTokens()
    const live = await liveSelect(db.select().from(users))
    expect([...live.data].sort((a, b) => a.id - b.id)).toEqual([
      { id: 1, tag: 'a' },
      { id: 2, tag: 'b' },
    ])
    expect(tokens).toHaveLength(1)
    expect(tokens[0]!.redeemed).toBe(0) // inert until serialize-time activation
    expect(tokens[0]!.released).toBe(0)
  })

  it('serialize-time activation REDEEMS the token — proven by a second redeem throwing (not just a counter)', async () => {
    const tokens = trackTokens()
    const live = await liveSelect(db.select().from(users))
    activate(live) // the wire replacer's activate → the Live source subscribe → token.redeem()
    expect(tokens[0]!.redeemed).toBe(1)
    // REAL registry consequence: the underlying token is genuinely redeemed, so a second redeem is rejected.
    expect(() => tokens[0]!.token.redeem()).toThrow()
  })

  it('#1 — a rejecting σ-read releases its token IMMEDIATELY (no waiting on a sweep or a collection)', async () => {
    const tokens = trackTokens()
    // PGlite is single-session ⇒ PRECISE: the predicate-only plan is stateless (no scan), acquire
    // succeeds, then the initial read throws (1/0) — the failure path that must still give the token back.
    await expect(liveSelect(db.select().from(users).where(sql`1 / 0 = 1`))).rejects.toThrow()
    expect(tokens).toHaveLength(1) // acquired before the fallible read…
    expect(tokens[0]!.released).toBe(1) // …and released on the way out, deterministically
    expect(tokens[0]!.redeemed).toBe(0)
  })

  it('#5 — PGlite (single-session) compiles a PRECISE plan (NOT over-coarsed)', () => {
    // PGlite is an in-process single connection ⇒ isSingleSession=true ⇒ precise; a plain SELECT is stateless.
    const shape = extractQueryShape(db.select().from(users), { dialect: 'pg' })
    expect(compilePlanFor(db as object, shape)().coarse).toBe(false) // the probe: real PGlite plan must be coarse=false
  })

  it('#5 — a pooled-unpinned connection (a real pg Pool) forces a COARSE plan (no precise hydration from a mismatched session)', () => {
    // The genuine coarse control: a Pool can serve the probe on one connection and the query on another.
    const shape = extractQueryShape(db.select().from(users), { dialect: 'pg' })
    expect(compilePlanFor(poolFake, shape)().coarse).toBe(true) // pooled ⇒ coarse; a never-coarse mutant gives false → RED
  })

  it('#2 (engine half) — an activated handle releases its lease on release (channel close)', async () => {
    const tokens = trackTokens()
    const live = await liveSelect(db.select().from(users))
    activate(live) // activate → redeem → lease
    ;(live as unknown as { release(): void }).release() // channel close/abort → source teardown → lease.release()
    expect(tokens[0]!.redeemed).toBe(1) // channel-owned: redeemed once…
    expect(tokens[0]!.released).toBe(0) // …and never released as an un-redeemed token (that would throw)
  })
})

// ── abandonment: a handle that never reaches the wire ────────────────
//
// This replaces a per-request sweep, which required `reactiveDrizzle(db)` to be called before the body's
// first await so it could bind to the request — a usage rule the owner rejected, and one that still missed
// any handle created after the sweep had already run. Ownership now follows the HANDLE: activated →
// the channel lease releases it; collected without ever being activated → the FinalizationRegistry does.
//
// The honest limit: this is unbounded by spec (an implementation may never call a finalizer) and in
// practice the next GC. `global.gc` is available because vitest.config.ts passes --expose-gc.

/** Let V8 actually run finalizers: they are scheduled after collection, not during `gc()`. */
async function collectGarbage(): Promise<void> {
  for (let i = 0; i < 5; i++) {
    ;(global as unknown as { gc: () => void }).gc()
    await new Promise((resolve) => setTimeout(resolve, 0))
  }
}

describe('an ABANDONED live read is reclaimed when its handle is collected', () => {
  it('releases the token once the never-serialized handle is collected', async () => {
    const tokens = trackTokens()
    // Create and drop: nothing keeps a reference, exactly like a telefunction that builds a Live and then
    // throws, or returns something else entirely.
    await (async () => {
      await liveSelect(db.select().from(users))
    })()
    expect(tokens[0]!.released).toBe(0) // still held — reclamation is not synchronous

    await collectGarbage()

    expect(tokens[0]!.released).toBe(1) // the finalizer gave the graph token back
  }, 30_000)

  it('does NOT release an ACTIVATED handle’s token while its channel is open', async () => {
    const tokens = trackTokens()
    const live = await liveSelect(db.select().from(users))
    activate(live) // serialized → the channel lease owns the token now

    await collectGarbage()

    expect(tokens[0]!.redeemed).toBe(1)
    expect(tokens[0]!.released).toBe(0) // untouched by the finalizer
    ;(live as unknown as { release(): void }).release() // and the channel still owns a working teardown
  }, 30_000)

  it('a handle SERIALIZED then CLOSED is collected without a second release', async () => {
    // The control that can actually disagree with the finalizer's redeemed-check, and the reason that check
    // exists. The case above cannot: it still holds `live`, so nothing is ever collected and a finalizer
    // that released indiscriminately would look identical. Here the handle is genuinely dropped AFTER its
    // channel closed, so the finalizer really does run on an already-redeemed read — and releasing an
    // already-redeemed token is a usage error that throws, inside a GC callback.
    const tokens = trackTokens()
    await (async () => {
      const live = await liveSelect(db.select().from(users))
      activate(live) // → redeem
      ;(live as unknown as { release(): void }).release() // channel closes → lease released
    })()

    await collectGarbage()

    expect(tokens[0]!.redeemed).toBe(1)
    expect(tokens[0]!.released).toBe(0) // the finalizer left the redeemed token alone
  }, 30_000)
})

// ── the chainable live builder proxy ────────────────────────────────

describe('wrapLiveSelect — chainable live builder', () => {
  it('chain methods re-wrap (stay live, keep the terminal `.live()`); non-builder returns pass through untouched', () => {
    const toSQLResult = { sql: 'select', params: [] as unknown[] }
    const inner = { toSQL: () => toSQLResult, where: () => ({ toSQL: () => toSQLResult }) }
    const base = { toSQL: () => toSQLResult, from: () => inner }
    const wrapped = wrapLiveSelect(base, {} as object) as Record<string, unknown>

    const afterFrom = (wrapped.from as () => Record<string, unknown>)()
    expect(typeof afterFrom.live).toBe('function') // the re-wrapped chain still carries the terminal
    expect(typeof afterFrom.where).toBe('function') // …and keeps forwarding chain methods
    expect((wrapped.toSQL as () => unknown)()).toBe(toSQLResult) // a non-builder return is passed through raw
  })
})
