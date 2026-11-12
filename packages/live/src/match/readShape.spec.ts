import { AsyncLocalStorage } from 'node:async_hooks'
import { PGlite } from '@electric-sql/pglite'
import { sql, type SQL } from 'drizzle-orm'
import { drizzle } from 'drizzle-orm/pglite'
import { doublePrecision, integer, numeric, pgTable } from 'drizzle-orm/pg-core'
import { REQUEST_CONTEXT } from 'telefunc'
import { describe, expect, it } from 'vitest'
import { liveDrizzle } from '@telefunc/live/drizzle'

const a = pgTable('a', { id: integer('id').primaryKey(), v: numeric('v') })
const numericB = pgTable('b', { id: integer('id').primaryKey(), v: numeric('v') })
const floatB = pgTable('b', { id: integer('id').primaryKey(), v: doublePrecision('v') })

/** Whether `SELECT a.id FROM a JOIN b ON a.v = b.v WHERE <filter(b)>` goes stale when `a` gets `(1, value)`, `b`
 *  holding `(1, 0.1)`. */
async function staleAfterInsert(bType: 'numeric' | 'float8', filter: (b: typeof floatB) => SQL, value: string) {
  const pglite = new PGlite()
  await pglite.exec(`create table a (id int primary key, v numeric);
    create table b (id int primary key, v ${bType}); insert into b values (1, 0.1)`)
  const db = liveDrizzle(drizzle(pglite))
  const b = bType === 'numeric' ? numericB : floatB
  const read = () => db.select({ id: a.id }).from(a).innerJoin(b, sql`${a.v} = ${b.v}`).where(filter(b))
  // A telefunction call, as telefunc's server runs one: the response sends the `Live`.
  const store = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())
  const live = await store.run({ [REQUEST_CONTEXT]: { onClose() {} } }, () => read().live())
  expect(live.send()).toEqual([])
  let stale = false
  live.onStale(() => (stale = true))
  await db.execute(sql.raw(`insert into a values (1, ${value})`))
  await new Promise((resolve) => setTimeout(resolve, 50))
  const after = await read()
  live.release()
  await pglite.close()
  return { stale, after }
}

describe('a join of columns that compare differently', () => {
  // `numeric = float8` compares as `float8`: 0.100000000000000005 is 0.1 there, not as a `numeric`.
  it("doesn't route by the other column's value", async () => {
    const { stale, after } = await staleAfterInsert('float8', (b) => sql`${b.v} = ${0.1}`, '0.100000000000000005')
    expect(after).toEqual([{ id: 1 }])
    expect(stale).toBe(true)
  })
  it("doesn't seed the join with the other column's values", async () => {
    const { stale, after } = await staleAfterInsert('float8', (b) => sql`${b.id} = ${1}`, '0.100000000000000005')
    expect(after).toEqual([{ id: 1 }])
    expect(stale).toBe(true)
  })
  it('routes a join of columns of one type by the value', async () => {
    const { stale, after } = await staleAfterInsert('numeric', (b) => sql`${b.v} = ${0.1}`, '5')
    expect(after).toEqual([])
    expect(stale).toBe(false)
  })
})
