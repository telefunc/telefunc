// Write-capture — slice 1 (structural + coarse). Proves the write path feeds the SAME db-scoped graphs
// the read path created: a write through the reactiveDrizzle proxy invalidates the affected live query and
// NOT an unaffected one (the router fans a coarse change only to graphs watching the mutated table). Uses a
// REAL PGlite db + the real registry/router/graph engine end-to-end (nothing mocked).

import { PGlite } from '@electric-sql/pglite'
import { integer, pgSchema, pgTable, text } from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { getRawContext, provideTelefuncContext } from 'telefunc'
import { reactiveDrizzle } from './reactiveDrizzle.js'

// Deterministic flush — the cell coalesces its invalidation with queueMicrotask; a macrotask hop also
// clears the sync-mode context null timer between phases.
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

const users = pgTable('users', { id: integer('id').primaryKey(), name: text('name') })
const posts = pgTable('posts', { id: integer('id').primaryKey(), body: text('body') })

// Same relation NAME, different schemas — two physically distinct tables that a bare-name routing
// identity conflates (review finding #6).
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

    // Acquire the reactive db at the TOP (before the body's first await) — the SAME db the write goes through.
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

// Review finding #6: writes used the bare `getTableName(table)` and topics `__live__:{table}`, so a write
// to `a.users` reached a live query on `b.users` (the Evaluator's crossSchemaRoutingProbe observed 1 fire on
// the provably-unaffected query). Routing now runs on the schema-qualified relation identity.
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
