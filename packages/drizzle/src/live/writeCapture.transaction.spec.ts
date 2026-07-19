// Write-capture inside TRANSACTIONS (T4 slice 2). A committed transaction is ONE atomic graph tick: writes
// buffer until the outer COMMIT, then flush as one ChangeBatch; rollback discards; a nested transaction is a
// SAVEPOINT (its buffer merges on release, discards on savepoint-rollback). Real PGlite; ingestWrite is
// spied to inspect the flushed batch (registryFor stays real so reactiveDrizzle's select path still works).

import { PGlite } from '@electric-sql/pglite'
import { integer, pgTable, text } from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import { eq, sql } from 'drizzle-orm'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { provideTelefuncContext } from 'telefunc'

vi.mock('./dbRuntime.js', async (importActual) => {
  const actual = await importActual<typeof import('./dbRuntime.js')>()
  return { ...actual, ingestWrite: vi.fn(), ingestLocal: vi.fn() }
})
import { ingestLocal, ingestWrite, registryFor } from './dbRuntime.js'
import { reactiveDrizzle } from './reactiveDrizzle.js'
import { createInMemoryChangeTransport, setChangeTransport } from './changeTransport.js'
import { changeTopicFor } from './writeTransport.js'
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
beforeEach(() => {
  vi.mocked(ingestWrite).mockClear()
  vi.mocked(ingestLocal).mockClear()
})
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

// `tx.execute(sql`…`)` passed straight through the transaction proxy, so a raw write inside a transaction
// committed with NOTHING published — the top-level db already coarsened raw SQL, the tx db did not.
//
// Timing is the whole point: a raw statement's touched tables are unknowable, so other instances are told
// on the change topic — and that announcement must wait for the outer COMMIT. Announcing it when
// the statement runs would tell every other instance to refetch state a rollback then erased.
describe('write capture — RAW SQL inside a transaction', () => {
  /** Watch the change topic — the raw payloads remote instances actually receive. */
  async function watchAnnouncements(db: object) {
    const transport = createInMemoryChangeTransport()
    setChangeTransport(db, transport)
    const seen: string[] = []
    await transport.subscribe(changeTopicFor(db), (payload) => seen.push(payload))
    return seen
  }

  it('a COMMITTED raw-SQL transaction announces to other instances (was: nothing published)', async () => {
    provideTelefuncContext({})
    const fresh = drizzle({ client })
    const announced = await watchAnnouncements(fresh)
    const db = reactiveDrizzle(fresh) as unknown as ReactiveDb & { execute: (q: unknown) => Promise<unknown> }

    await db.transaction(async (tx) => {
      await (tx as unknown as { execute: (q: unknown) => Promise<unknown> }).execute(
        sql`insert into users (id, name) values (10, 'raw')`,
      )
    })
    await tick()

    expect(await base.select().from(users).where(eq(users.id, 10))).toHaveLength(1) // it really committed
    expect(announced).toHaveLength(1) // and remote instances were told exactly once
  })

  it('a ROLLED-BACK raw-SQL transaction announces NOTHING', async () => {
    provideTelefuncContext({})
    const fresh = drizzle({ client })
    const announced = await watchAnnouncements(fresh)
    const db = reactiveDrizzle(fresh) as unknown as ReactiveDb

    await expect(
      db.transaction(async (tx) => {
        await (tx as unknown as { execute: (q: unknown) => Promise<unknown> }).execute(
          sql`insert into users (id, name) values (11, 'raw-rollback')`,
        )
        throw new Error('rollback')
      }),
    ).rejects.toThrow('rollback')
    await tick()

    expect(await base.select().from(users).where(eq(users.id, 11))).toEqual([]) // really rolled back
    // The announcement is the point: publishing it when the statement ran would have told every other
    // instance to refetch state that never existed.
    expect(announced).toEqual([])
  })
})

// Rank 9 of the premise audit: a transaction MIXING ORM writes with raw SQL used to publish its batch
// remotely AND a coarse-all — two messages, two remote refetches, for one commit — defending the duplicate
// as the price of local atomicity. False dichotomy: the whole buffer still lands locally in ONE tick via
// the local-only sink; only the redundant remote copy is dropped, because the coarse-all (which reseeds
// every remotely-watched graph) supersedes it.
describe('write capture — a raw-mixing transaction costs ONE remote message and ONE local tick', () => {
  it('flushes the buffer LOCALLY once and announces remotely once — the batch is not also published', async () => {
    provideTelefuncContext({})
    const fresh = drizzle({ client })
    const transport = createInMemoryChangeTransport()
    setChangeTransport(fresh, transport)
    const announced: string[] = []
    await transport.subscribe(changeTopicFor(fresh), (payload) => announced.push(payload))
    const db = reactiveDrizzle(fresh) as unknown as ReactiveDb & { execute: (q: unknown) => Promise<unknown> }

    await db.transaction(async (tx) => {
      await tx.insert(users).values({ id: 20, name: 'mixed' }) // an ORM write — precise capture
      await (tx as unknown as { execute: (q: unknown) => Promise<unknown> }).execute(
        sql`insert into posts (id, body) values (20, 'raw')`, // raw — tables unknowable
      )
    })
    await tick()

    expect(await base.select().from(users).where(eq(users.id, 20))).toHaveLength(1) // both really committed
    expect(await base.select().from(posts).where(eq(posts.id, 20))).toHaveLength(1)

    expect(announced).toHaveLength(1) // ONE remote message — the coarse-all; the batch was NOT also published
    expect(vi.mocked(ingestWrite)).not.toHaveBeenCalled() // the publishing sink was not used…
    expect(vi.mocked(ingestLocal)).toHaveBeenCalledTimes(1) // …the local-only sink was, in ONE atomic tick
    const [, batch] = vi.mocked(ingestLocal).mock.calls[0]!
    const changes = (batch as ChangeBatch).changes
    // The precise ORM write is in the tick. (Raw coarse markers would sit beside it, but this db has no
    // registered graphs, so the raw statement legitimately contributes none here.)
    expect(changes.some((c) => c.kind === 'insert' && (c.new as { id?: number })?.id === 20)).toBe(true)
  })

  it('a transaction WITHOUT raw SQL still publishes its batch (the local-only path is raw-gated)', async () => {
    provideTelefuncContext({})
    const db = reactiveDrizzle(base) as unknown as ReactiveDb
    await db.transaction(async (tx) => {
      await tx.insert(users).values({ id: 21, name: 'orm-only' })
    })
    await tick()
    expect(vi.mocked(ingestWrite)).toHaveBeenCalledTimes(1) // the ordinary publishing flush — unchanged
    expect(vi.mocked(ingestLocal)).not.toHaveBeenCalled()
  })
})

// The rank-9 gate's blocker: raw coarse markers used to be materialized at STATEMENT time, snapshotting the
// watch-set of a moment inside the still-open transaction. A graph registering between the raw statement and
// COMMIT was absent from them — unreachable by the local flush, and origin-suppressed out of the remote
// coarse-all (its own publisher's echo) — so it NEVER heard about the committed rows. Markers are now
// computed at the outer commit, against the watch-set that exists when the transaction actually lands.
describe('write capture — raw markers are computed at COMMIT, not at statement time', () => {
  /** A minimal routable graph, registered on the REAL router (registryFor is unmocked) so watchedTables()
   *  answers truthfully; the flush itself is observed on the mocked ingestLocal. */
  const watcher = (table: string) =>
    ({
      tables: [table],
      apply: () => ({ invalidated: false }),
      notifyKey: () => table,
      fault() {},
      reseed() {},
    }) as never

  it('a graph registering AFTER the raw statement but BEFORE commit is in the flushed markers', async () => {
    provideTelefuncContext({})
    const fresh = drizzle({ client })
    const db = reactiveDrizzle(fresh) as unknown as ReactiveDb & { execute: (q: unknown) => Promise<unknown> }

    await db.transaction(async (tx) => {
      await (tx as unknown as { execute: (q: unknown) => Promise<unknown> }).execute(
        sql`insert into users (id, name) values (30, 'late')`,
      )
      // A live read is admitted MID-TRANSACTION, after the raw statement ran: the Evaluator's schedule.
      registryFor(fresh).router.register(watcher('users'))
    })
    await tick()

    expect(await base.select().from(users).where(eq(users.id, 30))).toHaveLength(1) // really committed
    expect(vi.mocked(ingestLocal)).toHaveBeenCalledTimes(1)
    const [, batch] = vi.mocked(ingestLocal).mock.calls[0]!
    // The late graph's table IS in the commit flush — statement-time markers would have missed it.
    expect((batch as ChangeBatch).changes).toEqual(
      expect.arrayContaining([expect.objectContaining({ table: 'users', kind: 'coarse' })]),
    )
  })

  it('a raw-only transaction with NO watcher at statement time still coarsens the one present at commit', async () => {
    provideTelefuncContext({})
    const fresh = drizzle({ client })
    const db = reactiveDrizzle(fresh) as unknown as ReactiveDb & { execute: (q: unknown) => Promise<unknown> }

    await db.transaction(async (tx) => {
      await (tx as unknown as { execute: (q: unknown) => Promise<unknown> }).execute(
        sql`insert into posts (id, body) values (30, 'raw-late')`,
      )
      registryFor(fresh).router.register(watcher('posts'))
    })
    await tick()

    expect(vi.mocked(ingestLocal)).toHaveBeenCalledWith(fresh, {
      changes: [{ table: 'posts', kind: 'coarse' }],
    })
  })
})

// The in-transaction half of "capture's substituted statement must never fail the caller's write".
//
// On autocommit the recovery just re-runs the caller's statement. Inside a transaction it cannot:
// PostgreSQL aborts the WHOLE transaction on the refused statement, so the re-run would only get "current
// transaction is aborted". A SAVEPOINT around the substituted attempt is what gives it something to rewind
// to.
//
// The refusal is REAL, not simulated: a PostgreSQL role holding INSERT/UPDATE/DELETE but not SELECT is
// refused `… RETURNING *` with 42501 while its plain write commits. That is the actual production cause,
// so nothing here depends on a stand-in for it. (An earlier version of these cases filtered the wire with a
// proxy instead; it silently failed to intercept inside a transaction and the tests passed while asserting
// the wrong thing — which is why they are grounded in the database's own privileges now.)
describe('a refused substitution inside a TRANSACTION does not cost the caller their write', () => {
  /** A reactive db whose connection is a role that may WRITE users but not READ it. */
  async function writeOnlyDb() {
    const real = new PGlite()
    await real.exec('create table users (id int primary key, name text)')
    await real.exec('create role writer nologin')
    await real.exec('grant usage on schema public to writer')
    await real.exec('grant insert, update, delete on users to writer')
    await real.exec('set role writer')
    const asOwner = async () => {
      await real.exec('reset role')
      return real
    }
    return { real, asOwner, db: reactiveDrizzle(drizzle({ client: real })) as unknown as ReactiveDb }
  }

  it('the transaction COMMITS, both rows are there, and the batch coarsens', async () => {
    provideTelefuncContext({})
    const { db, asOwner } = await writeOnlyDb()
    await db.transaction(async (tx) => {
      await tx.insert(users).values({ id: 1, name: 'first' })
      await tx.insert(users).values({ id: 2, name: 'second' })
    })
    await tick()

    const owner = await asOwner()
    expect((await owner.query('select id, name from users order by id')).rows).toEqual([
      { id: 1, name: 'first' },
      { id: 2, name: 'second' },
    ])
    // Still ONE atomic tick — coarse, for the table capture was not allowed to read.
    expect(flushed()).toEqual([
      {
        changes: [
          { table: 'users', kind: 'coarse' },
          { table: 'users', kind: 'coarse' },
        ],
      },
    ])
    await owner.close()
  })

  it('a ROLLBACK still discards everything — the capture savepoints pin nothing', async () => {
    provideTelefuncContext({})
    const { db, asOwner } = await writeOnlyDb()
    await expect(
      db.transaction(async (tx) => {
        await tx.insert(users).values({ id: 1, name: 'doomed' })
        throw new Error('caller rolls back')
      }),
    ).rejects.toThrow('caller rolls back')
    await tick()
    const owner = await asOwner()
    expect((await owner.query('select * from users')).rows).toEqual([])
    expect(flushed()).toEqual([])
    await owner.close()
  })

  it('CONCURRENT writes on one transaction do not destroy each other’s savepoints', async () => {
    // `RELEASE SAVEPOINT` destroys every savepoint established after it, so save-A, save-B, release-A would
    // leave B with nothing to rewind to; B's release then errors, and a failed statement aborts the
    // transaction. Observed before the whole bracket was serialized: three concurrent inserts committed
    // ZERO rows. Unique names would not have helped — it is establishment order.
    provideTelefuncContext({})
    const { db, asOwner } = await writeOnlyDb()
    await db.transaction(async (tx) => {
      await Promise.all([
        tx.insert(users).values({ id: 1, name: 'a' }),
        tx.insert(users).values({ id: 2, name: 'b' }),
        tx.insert(users).values({ id: 3, name: 'c' }),
      ])
    })
    await tick()
    const owner = await asOwner()
    expect((await owner.query('select count(*)::int c from users')).rows).toEqual([{ c: 3 }])
    await owner.close()
  })

  it('the savepoint is NOT issued through the tx PROXY — that would coarsen the whole transaction', async () => {
    // The sharpest trap in this machinery: the proxy intercepts `execute` as raw SQL, so a SAVEPOINT sent
    // through it would be recorded as raw INTENT and coarsen every WATCHED table at commit rather than the
    // one table written. The registered graph below is what makes that visible.
    provideTelefuncContext({})
    const { db, asOwner } = await writeOnlyDb()
    const watcher = {
      tables: ['users', 'posts'],
      apply: () => ({}) as never,
      notifyKey: () => 'w',
      fault() {},
      reseed() {},
      coarsen() {},
    }
    registryFor(db as object).router.register(watcher)
    await db.transaction(async (tx) => {
      await tx.insert(users).values({ id: 1, name: 'x' })
    })
    await tick()
    registryFor(db as object).router.unregister(watcher)

    const tables = flushed().flatMap((batch) => batch.changes.map((change) => change.table))
    expect(tables).toEqual(['users']) // raw-SQL intent would have dragged `posts` in too
    const owner = await asOwner()
    await owner.close()
  })
})

// A NESTED transaction is a SAVEPOINT on the same physical connection, not a transaction of its own. Keying
// capture's write queue by each scope's own handle therefore gave parent and child SEPARATE queues, let
// their capture savepoints interleave, and turned a transaction plain Drizzle commits into a failure
// ("savepoint … does not exist" → 25P02).
describe('nested scopes of ONE physical transaction share one write queue', () => {
  async function writeOnlyDb() {
    const real = new PGlite()
    await real.exec('create table users (id int primary key, name text)')
    await real.exec('create role writer nologin')
    await real.exec('grant usage on schema public to writer')
    await real.exec('grant insert, update, delete on users to writer')
    await real.exec('set role writer') // capture's RETURNING is refused → every write takes the savepoint path
    const asOwner = async () => {
      await real.exec('reset role')
      return real
    }
    return { real, asOwner, db: reactiveDrizzle(drizzle({ client: real })) as unknown as ReactiveDb }
  }

  it('a write in the parent and a write in a nested savepoint both commit', async () => {
    provideTelefuncContext({})
    const { db, asOwner } = await writeOnlyDb()
    await db.transaction(async (tx) => {
      await tx.insert(users).values({ id: 1, name: 'parent' })
      await tx.transaction(async (nested) => {
        await nested.insert(users).values({ id: 2, name: 'nested' })
      })
      await tx.insert(users).values({ id: 3, name: 'parent-again' })
    })
    await tick()

    const owner = await asOwner()
    expect((await owner.query('select id from users order by id')).rows).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }])
    await owner.close()
  })

  it('interleaved parent and nested writes still commit — the queue is shared, not per scope', async () => {
    provideTelefuncContext({})
    const { db, asOwner } = await writeOnlyDb()
    await db.transaction(async (tx) => {
      // Concurrent ACROSS the scope boundary — a parent write racing a nested one. Keyed per scope these
      // land on different queues, so the parent's capture savepoint is taken between the nested one's
      // savepoint and its release, and the release then destroys a savepoint the other scope still needs.
      // Both writes inside one scope would share a queue either way and prove nothing.
      await Promise.all([
        tx.insert(users).values({ id: 10, name: 'p1' }),
        tx.transaction(async (nested) => {
          await nested.insert(users).values({ id: 11, name: 'n1' })
        }),
      ])
      await tx.insert(users).values({ id: 12, name: 'p2' })
    })
    await tick()

    const owner = await asOwner()
    expect((await owner.query('select count(*)::int c from users')).rows).toEqual([{ c: 3 }])
    await owner.close()
  })
})
