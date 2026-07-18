// Write-capture inside TRANSACTIONS (T4 slice 2). A committed transaction is ONE atomic graph tick: writes
// buffer until the outer COMMIT, then flush as one ChangeBatch; rollback discards; a nested transaction is a
// SAVEPOINT (its buffer merges on release, discards on savepoint-rollback). Real PGlite; ingestWrite is
// spied to inspect the flushed batch (registryFor stays real so reactiveDrizzle's select path still works).

import { PGlite } from '@electric-sql/pglite'
import { integer, pgTable, text } from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import { eq } from 'drizzle-orm'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { provideTelefuncContext } from 'telefunc'

vi.mock('./dbRuntime.js', async (importActual) => {
  const actual = await importActual<typeof import('./dbRuntime.js')>()
  return { ...actual, ingestWrite: vi.fn() }
})
import { ingestWrite } from './dbRuntime.js'
import { reactiveDrizzle } from './reactiveDrizzle.js'
import type { ChangeBatch } from '../router/events.js'

const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))
const users = pgTable('users', { id: integer('id').primaryKey(), name: text('name') })
const posts = pgTable('posts', { id: integer('id').primaryKey(), body: text('body') })

let client: PGlite
let base: ReturnType<typeof drizzle>

type TxDb = {
  insert: (t: unknown) => { values: (v: unknown) => PromiseLike<unknown> }
  transaction: (cb: (tx: TxDb) => unknown) => Promise<unknown>
}
type ReactiveDb = { transaction: (cb: (tx: TxDb) => unknown) => Promise<unknown> }

/** The batches passed to ingestWrite (spied), flattened change-arrays. */
const flushed = (): ChangeBatch[] => vi.mocked(ingestWrite).mock.calls.map(([, batch]) => batch as ChangeBatch)

beforeAll(async () => {
  client = new PGlite()
  base = drizzle({ client })
  await client.exec('create table users (id int primary key, name text)')
  await client.exec('create table posts (id int primary key, body text)')
})
afterAll(async () => {
  await client.close()
})
beforeEach(() => vi.mocked(ingestWrite).mockClear())
afterEach(async () => {
  await tick()
})

describe('write capture — transaction buffering', () => {
  it('a committed multi-table transaction flushes ONE batch (all changes, in order, precise)', async () => {
    provideTelefuncContext({})
    const db = reactiveDrizzle(base) as unknown as ReactiveDb
    await db.transaction(async (tx) => {
      await tx.insert(users).values({ id: 1, name: 'a' })
      await tx.insert(posts).values({ id: 1, body: 'x' })
    })
    await tick()
    // ONE ingest, one batch, both changes — and PRECISE (session props read from the top db, not the tx db).
    expect(flushed()).toEqual([
      {
        changes: [
          { table: 'users', kind: 'insert', new: { id: 1, name: 'a' } },
          { table: 'posts', kind: 'insert', new: { id: 1, body: 'x' } },
        ],
      },
    ])
  })

  it('a rolled-back transaction flushes NOTHING', async () => {
    provideTelefuncContext({})
    const db = reactiveDrizzle(base) as unknown as ReactiveDb
    await expect(
      db.transaction(async (tx) => {
        await tx.insert(users).values({ id: 2, name: 'b' })
        throw new Error('rollback')
      }),
    ).rejects.toThrow('rollback')
    await tick()
    expect(flushed()).toEqual([]) // no batch — the write never committed
    // and the row really rolled back:
    expect(await base.select().from(users).where(eq(users.id, 2))).toEqual([])
  })

  it('a released SAVEPOINT (nested commit) merges into the outer batch', async () => {
    provideTelefuncContext({})
    const db = reactiveDrizzle(base) as unknown as ReactiveDb
    await db.transaction(async (tx) => {
      await tx.insert(users).values({ id: 3, name: 'c' })
      await tx.transaction(async (tx2) => {
        await tx2.insert(posts).values({ id: 3, body: 'y' })
      })
    })
    await tick()
    expect(flushed()).toEqual([
      {
        changes: [
          { table: 'users', kind: 'insert', new: { id: 3, name: 'c' } },
          { table: 'posts', kind: 'insert', new: { id: 3, body: 'y' } },
        ],
      },
    ])
  })

  it('a rolled-back SAVEPOINT discards its changes; the outer commit keeps the rest', async () => {
    provideTelefuncContext({})
    const db = reactiveDrizzle(base) as unknown as ReactiveDb
    await db.transaction(async (tx) => {
      await tx.insert(users).values({ id: 4, name: 'd' })
      try {
        await tx.transaction(async (tx2) => {
          await tx2.insert(posts).values({ id: 4, body: 'z' })
          throw new Error('savepoint rollback')
        })
      } catch {
        // swallow — the outer transaction continues and commits
      }
    })
    await tick()
    // only the pre-savepoint change survives; the savepoint's post insert is discarded
    expect(flushed()).toEqual([{ changes: [{ table: 'users', kind: 'insert', new: { id: 4, name: 'd' } }] }])
    expect(await base.select().from(posts).where(eq(posts.id, 4))).toEqual([]) // really rolled back
  })
})
