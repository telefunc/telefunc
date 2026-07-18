// Write-capture — slice 1 (structural + coarse). Proves the write path feeds the SAME db-scoped graphs
// the read path created: a write through the reactiveDrizzle proxy invalidates the affected live query and
// NOT an unaffected one (the router fans a coarse change only to graphs watching the mutated table). Uses a
// REAL PGlite db + the real registry/router/graph engine end-to-end (nothing mocked).

import { PGlite } from '@electric-sql/pglite'
import { integer, pgTable, text } from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { getRawContext, provideTelefuncContext } from 'telefunc'
import { reactiveDrizzle } from './reactiveDrizzle.js'

// Deterministic flush — the cell coalesces its invalidation with queueMicrotask; a macrotask hop also
// clears the sync-mode context null timer between phases.
const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

const users = pgTable('users', { id: integer('id').primaryKey(), name: text('name') })
const posts = pgTable('posts', { id: integer('id').primaryKey(), body: text('body') })

let client: PGlite
let base: ReturnType<typeof drizzle>

beforeAll(async () => {
  client = new PGlite()
  base = drizzle({ client })
  await client.exec('create table users (id int primary key, name text)')
  await client.exec('create table posts (id int primary key, body text)')
  await client.query("insert into users (id, name) values (1, 'a')")
  await client.query("insert into posts (id, body) values (1, 'x')")
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
