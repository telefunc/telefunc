// Write-capture — slice 1 (structural + coarse). Proves the write path feeds the SAME db-scoped graphs
// the read path created: a write through the reactiveDrizzle proxy invalidates the affected live query and
// NOT an unaffected one (the router fans a coarse change only to graphs watching the mutated table). Uses a
// REAL PGlite db + the real registry/router/graph engine end-to-end (nothing mocked).

import { PGlite } from '@electric-sql/pglite'
import { sql } from 'drizzle-orm'
import { integer, pgMaterializedView, pgSchema, pgTable, text } from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { getRawContext, provideTelefuncContext } from 'telefunc'
import { reactiveDrizzle } from './reactiveDrizzle.js'
import { registryFor } from '../bus/dbRuntime.js'

// Deterministic flush — the cell coalesces its invalidation with queueMicrotask; a macrotask hop also
// clears the sync-mode context null timer between phases.
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

const users = pgTable('users', { id: integer('id').primaryKey(), name: text('name') })
const posts = pgTable('posts', { id: integer('id').primaryKey(), body: text('body') })

// Same relation NAME, different schemas — two physically distinct tables that a bare-name routing
// identity conflates (review finding #6).
const usersView = pgMaterializedView('users_mv', { id: integer('id') }).existing()
const aUsers = pgSchema('a').table('users', { id: integer('id').primaryKey() })
const bUsers = pgSchema('b').table('users', { id: integer('id').primaryKey() })

let client: PGlite
let base: ReturnType<typeof drizzle>

beforeAll(async () => {
  client = new PGlite()
  base = drizzle({ client })
  await client.exec('create table users (id int primary key, name text)')
  await client.exec('create table posts (id int primary key, body text)')
  await client.query("insert into users (id, name) values (1, 'a')")
  await client.query("insert into posts (id, body) values (1, 'x')")
  await client.exec('create schema a; create schema b')
  await client.exec('create table a.users (id int primary key); create table b.users (id int primary key)')
  await client.query('insert into a.users (id) values (1)')
  await client.query('insert into b.users (id) values (1)')
  await client.exec('create materialized view users_mv as select id from users')
})
afterAll(async () => {
  await client.close()
})

/** Serialize-time activation: redeems the read token and wires the graph's invalidation to `live`. */
const activate = (live: unknown): void => (live as { activate(): void }).activate()
/** Bind an invalidation tap the way an adapter does; returns the spy. */
const onInvalidate = (live: unknown): ReturnType<typeof vi.fn> => {
  const spy = vi.fn()
  ;(live as { onInvalidate(cb: () => void): () => void }).onInvalidate(spy)
  return spy
}

describe('write capture — a write feeds the same db-scoped graphs the reads created', () => {
  it('a write through the proxy invalidates the affected live query and not an unaffected one', async () => {
    provideTelefuncContext({})
    expect(getRawContext()).not.toBe(null)

    // The SAME reactive db the write goes through — reads and writes share one registry.
    const db = reactiveDrizzle(base) as unknown as {
      select: () => { from: (t: unknown) => { live: () => Promise<unknown> } }
      insert: (t: unknown) => { values: (v: unknown) => PromiseLike<unknown> }
    }

    // Two live reads over the same db: one on `users`, one on `posts`. Activate + tap each.
    const usersLive = await db.select().from(users).live()
    const postsLive = await db.select().from(posts).live()
    activate(usersLive)
    activate(postsLive)
    const usersInvalidated = onInvalidate(usersLive)
    const postsInvalidated = onInvalidate(postsLive)

    // Write to `users` through the proxy — runs the real INSERT, then feeds a coarse change for `users`.
    await db.insert(users).values({ id: 2, name: 'b' })
    await tick()

    expect(usersInvalidated).toHaveBeenCalledTimes(1) // the affected live query refetches
    expect(postsInvalidated).not.toHaveBeenCalled() // the unaffected one does not
  })
})

// The acceptance proof for cross-instance write capture, on the PUBLIC stack: a write on instance A
// invalidates ONLY the affected live query on instance B, and provably not an unaffected one. Two reactive
// dbs share the process-wide default transport, which encodes and decodes like any other — so this runs
// through the serialize/parse codec boundary rather than handing B a reference to A's own objects.
//
// Counted at each ROUTER, not at the Live handles: a handle coalesces its invalidations in a microtask, so
// one delivery and two look identical there — which is exactly how a broken echo suppression would hide.
describe('write capture — a write on A reaches only the affected live query on B', () => {
  it('delivers the changed ROW cross-instance, once, and leaves the unaffected query alone', async () => {
    provideTelefuncContext({})
    const baseA = drizzle({ client })
    const baseB = drizzle({ client })
    type Reader = {
      select: () => { from: (t: unknown) => { live: () => Promise<unknown> } }
      insert: (t: unknown) => { values: (v: unknown) => PromiseLike<unknown> }
    }
    const instanceA = reactiveDrizzle(baseA) as unknown as Reader
    const instanceB = reactiveDrizzle(baseB) as unknown as Reader

    activate(await instanceA.select().from(users).live())
    const remoteUsers = await instanceB.select().from(users).live()
    const remotePosts = await instanceB.select().from(posts).live()
    activate(remoteUsers)
    activate(remotePosts)
    const remoteUsersInvalidated = onInvalidate(remoteUsers)
    const remotePostsInvalidated = onInvalidate(remotePosts)

    const ingestA = vi.spyOn(registryFor(baseA).router, 'ingest')
    const ingestB = vi.spyOn(registryFor(baseB).router, 'ingest')

    await instanceA.insert(users).values({ id: 700, name: 'cross' })
    await tick()

    // B heard about it exactly once, and precisely: the row itself survived the wire, not a coarse marker.
    expect(ingestB).toHaveBeenCalledTimes(1)
    expect(ingestB).toHaveBeenCalledWith({
      changes: [{ table: 'users', kind: 'insert', new: { id: 700, name: 'cross' } }],
    })
    expect(remoteUsersInvalidated).toHaveBeenCalledTimes(1)
    expect(remotePostsInvalidated).not.toHaveBeenCalled() // the unaffected query on B does not fire

    // A fed its own graphs directly, before publishing — its own echo must not arrive on top of that.
    expect(ingestA).toHaveBeenCalledTimes(1)

    ingestA.mockRestore()
    ingestB.mockRestore()
  })
})

// A raw statement's touched tables are unknowable, so other instances are told with ONE coarse-all
// announcement, which coarsens every table the receiver watches. Publishing the same write's own coarse
// markers as well delivered the SAME invalidation to a remote watcher TWICE. Driven end-to-end here (real
// PGlite, real registries, two instances on the shared default transport) rather than simulated, so it
// fails if the wiring changes and not just if the helper does.
describe('write capture — a raw write reaches a remote live query exactly ONCE', () => {
  it('an autocommit raw write is DELIVERED to a remote instance a single time', async () => {
    provideTelefuncContext({})
    const baseA = drizzle({ client })
    const baseB = drizzle({ client })
    const instanceA = reactiveDrizzle(baseA) as unknown as {
      execute: (q: unknown) => Promise<unknown>
      select: () => { from: (t: unknown) => { live: () => Promise<unknown> } }
    }
    const instanceB = reactiveDrizzle(baseB) as unknown as {
      select: () => { from: (t: unknown) => { live: () => Promise<unknown> } }
    }

    // BOTH instances watch `users`. A must watch it too, or its raw write has no watched table to coarsen
    // and publishes no per-table markers at all — the redundant-delivery case would be unreachable and this
    // control would pass no matter what the code did.
    activate(await instanceA.select().from(users).live())
    const remoteLive = await instanceB.select().from(users).live()
    activate(remoteLive)

    // Counted at B's ROUTER, not at its Live handle: the handle coalesces invalidations in a microtask, so
    // one delivery and two look identical there. Verified by mutation — asserting on onInvalidate here does
    // NOT fail when the redundant per-table publication is restored, which would make it a false green.
    const ingestSpy = vi.spyOn(registryFor(baseB).router, 'ingest')

    await instanceA.execute(sql`insert into users (id, name) values (500, 'raw')`)
    await tick()

    expect(ingestSpy).toHaveBeenCalledTimes(1) // one write, one delivery — not two
    ingestSpy.mockRestore()
  })
})

// Mutations that are NOT insert/update/delete and NOT raw SQL, but change what a live query reads. Both
// fell straight through the reactive proxy to plain Drizzle and committed with nothing captured and nothing
// published — the same silent-bypass class as `tx.execute`.
//
// `batch` is absent on PGlite (it ships on libSQL / D1 / Neon-HTTP / sqlite-proxy); `refreshMaterializedView`
// is real here, and the transaction case below drives the genuine PGlite method against a genuine
// materialized view. The two top-level cases attach sentinel-returning stand-ins to a REAL reactive db so
// the caller's result can be pinned exactly — the proxy interception and the coarse path they exercise are
// the real ones. Each case asserts BOTH halves: the result comes back untouched, AND the query invalidates.
describe('write capture — non-SQL mutations still invalidate (refreshMaterializedView, batch)', () => {
  type ExoticDb = {
    select: () => { from: (t: unknown) => { live: () => Promise<unknown> } }
    refreshMaterializedView: (view: unknown) => Promise<string>
    batch: (items: unknown[]) => Promise<string[]>
    transaction: (cb: (tx: ExoticDb) => unknown) => Promise<unknown>
  }

  /** A reactive db whose base carries the exotic driver methods, returning a recognisable sentinel. */
  function exoticDb() {
    const base = drizzle({ client }) as unknown as Record<string, unknown>
    base.refreshMaterializedView = async () => 'refreshed-sentinel'
    base.batch = async (items: unknown[]) => items.map((_, i) => `batch-result-${i}`)
    return reactiveDrizzle(base as never) as unknown as ExoticDb
  }

  it('refreshMaterializedView invalidates a live query, and returns the driver result unchanged', async () => {
    provideTelefuncContext({})
    const db = exoticDb()
    const live = await db.select().from(users).live()
    activate(live)
    const invalidated = onInvalidate(live)

    const result = await db.refreshMaterializedView({})
    await tick()

    expect(result).toBe('refreshed-sentinel') // the caller's result is plain Drizzle's
    expect(invalidated).toHaveBeenCalledTimes(1) // ...and it was not a silent no-op
  })

  it('batch invalidates a live query, and returns every item result unchanged', async () => {
    provideTelefuncContext({})
    const db = exoticDb()
    const live = await db.select().from(users).live()
    activate(live)
    const invalidated = onInvalidate(live)

    const result = await db.batch([{}, {}])
    await tick()

    expect(result).toEqual(['batch-result-0', 'batch-result-1'])
    expect(invalidated).toHaveBeenCalledTimes(1)
  })

  it('refreshMaterializedView is intercepted INSIDE a transaction too, flushing on commit', async () => {
    // No stub here: PGlite's own transaction object really does expose refreshMaterializedView, so this
    // drives the REAL driver method against a REAL materialized view. The tx db is a different object from
    // the top-level one, and the proxy must intercept there as well — that is exactly how `tx.execute`
    // slipped through once already.
    provideTelefuncContext({})
    const rdb = reactiveDrizzle(drizzle({ client })) as unknown as {
      select: () => { from: (t: unknown) => { live: () => Promise<unknown> } }
      transaction: (
        cb: (tx: { refreshMaterializedView: (v: unknown) => Promise<unknown> }) => unknown,
      ) => Promise<unknown>
    }
    const live = await rdb.select().from(users).live()
    activate(live)
    const invalidated = onInvalidate(live)

    await rdb.transaction(async (tx) => {
      await tx.refreshMaterializedView(usersView)
    })
    await tick()

    expect(invalidated).toHaveBeenCalledTimes(1)
  })
})

// Review finding #6: writes routed on the bare `getTableName(table)`, so a write to `a.users` reached a live
// query on `b.users` (the Evaluator's crossSchemaRoutingProbe observed 1 fire on the provably-unaffected
// query). Routing now runs on the schema-qualified relation identity.
//
// Each case asserts BOTH halves: the write's OWN schema fires (a positive control, so the zero below cannot
// pass vacuously — e.g. if capture silently stopped emitting) and the same-named relation in the OTHER
// schema does not. Both directions are exercised so a pass cannot be an artifact of acquisition order.
describe('write capture — routing identity is SCHEMA-QUALIFIED, not the bare table name', () => {
  type SchemaDb = {
    select: () => { from: (t: unknown) => { live: () => Promise<unknown> } }
    insert: (t: unknown) => { values: (v: unknown) => PromiseLike<unknown> }
  }

  /** Two live reads on same-named relations in different schemas; write to `into`, report both taps.
   *  `into` spans both tables — drizzle carries the schema as a TYPE literal, so `typeof aUsers` alone
   *  would not accept `bUsers`. */
  async function fireCounts(into: typeof aUsers | typeof bUsers, id: number) {
    provideTelefuncContext({})
    const db = reactiveDrizzle(base) as unknown as SchemaDb
    const liveA = await db.select().from(aUsers).live()
    const liveB = await db.select().from(bUsers).live()
    activate(liveA)
    activate(liveB)
    const firedA = onInvalidate(liveA)
    const firedB = onInvalidate(liveB)
    await db.insert(into).values({ id })
    await tick()
    return { firedA, firedB }
  }

  it('a write to a.users invalidates a.users and NOT the same-named b.users', async () => {
    const { firedA, firedB } = await fireCounts(aUsers, 100)
    expect(firedA).toHaveBeenCalledTimes(1) // affected — the write's own relation
    expect(firedB).not.toHaveBeenCalled() // provably unaffected: a DIFFERENT physical table
  })

  it('a write to b.users invalidates b.users and NOT the same-named a.users', async () => {
    const { firedA, firedB } = await fireCounts(bUsers, 200)
    expect(firedB).toHaveBeenCalledTimes(1)
    expect(firedA).not.toHaveBeenCalled()
  })
})
