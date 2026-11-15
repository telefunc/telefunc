import { AsyncLocalStorage } from 'node:async_hooks'
import { sql, type SQL } from 'drizzle-orm'
import { drizzle } from 'drizzle-orm/pglite'
import { integer, pgTable, text } from 'drizzle-orm/pg-core'
import { REQUEST_CONTEXT } from 'telefunc'
import { describe, expect, it } from 'vitest'
import { liveDrizzle } from '@telefunc/live/drizzle'
import { emptyPGlite } from '../test/pglite.js'

// Only the SQL names these: each test creates `v` with the types it compares.
const a = pgTable('a', { id: integer('id').primaryKey(), v: text('v') })
const b = pgTable('b', { id: integer('id').primaryKey(), v: text('v') })

/** Whether `SELECT a.id FROM a JOIN b ON a.v = b.v WHERE <filter(b)>` goes stale when `a` gets `(1, inserted)`, `b`
 *  holding `(1, held)`. */
async function staleAfterInsert(
  types: { a: string; b: string },
  filter: (table: typeof b) => SQL,
  { held, inserted }: { held: string; inserted: string },
) {
  const pglite = await emptyPGlite()
  await pglite.exec(`create table a (id int primary key, v ${types.a});
    create table b (id int primary key, v ${types.b}); insert into b values (1, ${held})`)
  const db = liveDrizzle(drizzle(pglite))
  const read = () => db.select({ id: a.id }).from(a).innerJoin(b, sql`${a.v} = ${b.v}`).where(filter(b))
  // A telefunction call, as telefunc's server runs one: the response sends the `Live`.
  const store = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())
  const live = await store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () => read().live())
  expect(live.send()).toEqual([])
  let stale = false
  live.onStale(() => (stale = true))
  await db.execute(sql.raw(`insert into a values (1, ${inserted})`))
  await new Promise((resolve) => setTimeout(resolve, 50))
  const after = await read()
  live.release()
  await pglite.close()
  return { stale, after }
}

describe('a join of columns that compare differently', () => {
  // `numeric = float8` compares as `float8`: 0.100000000000000005 is 0.1 there, not as a `numeric`.
  const almostTenth = { held: '0.1', inserted: '0.100000000000000005' }
  it("doesn't route by the other column's value", async () => {
    const types = { a: 'numeric', b: 'float8' }
    const { stale, after } = await staleAfterInsert(types, (b) => sql`${b.v} = ${0.1}`, almostTenth)
    expect(after).toEqual([{ id: 1 }])
    expect(stale).toBe(true)
  })
  it("doesn't seed the join with the other column's values", async () => {
    const types = { a: 'numeric', b: 'float8' }
    const { stale, after } = await staleAfterInsert(types, (b) => sql`${b.id} = ${1}`, almostTenth)
    expect(after).toEqual([{ id: 1 }])
    expect(stale).toBe(true)
  })
  // `bpchar` ignores trailing spaces: `b.v = 'a  '` holds for `'a'`, and `a.v = b.v` compares `b.v` trimmed, as `text`.
  it("doesn't route a text column by a bpchar column's value", async () => {
    const types = { a: 'text', b: 'bpchar(3)' }
    const { stale, after } = await staleAfterInsert(types, (b) => sql`${b.v} = ${'a  '}`, {
      held: "'a'",
      inserted: "'a'",
    })
    expect(after).toEqual([{ id: 1 }])
    expect(stale).toBe(true)
  })
})

describe('a join of columns that compare alike', () => {
  const unrelated = { held: '5', inserted: '7' }
  it('routes a join of columns of one type by the value', async () => {
    const types = { a: 'numeric', b: 'numeric' }
    const { stale, after } = await staleAfterInsert(types, (b) => sql`${b.v} = ${5}`, unrelated)
    expect(after).toEqual([])
    expect(stale).toBe(false)
  })
  it('routes a join of integers of different widths by the value', async () => {
    const types = { a: 'int4', b: 'int8' }
    const { stale, after } = await staleAfterInsert(types, (b) => sql`${b.v} = ${5}`, unrelated)
    expect(after).toEqual([])
    expect(stale).toBe(false)
  })
  it('routes a join of text and varchar by the value', async () => {
    const types = { a: 'text', b: 'varchar' }
    const { stale, after } = await staleAfterInsert(types, (b) => sql`${b.v} = ${'x'}`, {
      held: "'x'",
      inserted: "'y'",
    })
    expect(after).toEqual([])
    expect(stale).toBe(false)
  })
})

const items = pgTable('items', { id: integer('id').primaryKey(), v: text('v') })

const liveDb = (pglite: Awaited<ReturnType<typeof emptyPGlite>>) => liveDrizzle(drizzle(pglite))
type LiveCell = { send(): unknown; onStale(onStale: () => void): void; release(): void }
type Read = PromiseLike<unknown> & { live(): Promise<LiveCell> }

/** A live read of `read` on a PGlite holding `schema`: the rows it sends and the database's own, whether `write` makes
 *  it stale, and its rows after. */
async function liveAfter(schema: string, read: (db: ReturnType<typeof liveDb>) => Read, write?: string) {
  const pglite = await emptyPGlite()
  await pglite.exec(schema)
  const db = liveDb(pglite)
  const store = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())
  const live = await store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () => read(db).live())
  const [sent, before] = [live.send(), await read(db)]
  let stale = false
  live.onStale(() => (stale = true))
  if (write) await db.execute(sql.raw(write))
  await new Promise((resolve) => setTimeout(resolve, 50))
  const after = await read(db)
  live.release()
  await pglite.close()
  return { sent, before, stale, after }
}

describe('a page of a read', () => {
  const schema =
    "create table items (id int primary key, v text); insert into items values (1, 'a'), (2, 'b'), (3, 'c')"
  // Without `ORDER BY` the database returns its rows in any order: a deleted row may have been before the page.
  it('goes stale without ORDER BY when a row it skipped is deleted', async () => {
    const page = (db: ReturnType<typeof liveDb>) => db.select({ id: items.id }).from(items).limit(1).offset(1)
    const { sent, stale, after } = await liveAfter(schema, page, 'delete from items where id = 1')
    expect(sent).toEqual([{ id: 2 }])
    expect(after).toEqual([{ id: 3 }])
    expect(stale).toBe(true)
  })
  it('stays fresh in its order when a row after it is deleted', async () => {
    const page = (db: ReturnType<typeof liveDb>) =>
      db.select({ id: items.id }).from(items).orderBy(items.id).limit(1).offset(1)
    const { sent, stale, after } = await liveAfter(schema, page, 'delete from items where id = 3')
    expect(sent).toEqual([{ id: 2 }])
    expect(after).toEqual([{ id: 2 }])
    expect(stale).toBe(false)
  })
})
