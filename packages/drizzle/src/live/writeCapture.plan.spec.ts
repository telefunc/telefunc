// The precision DECISION, observed independently of execution. The review noted that no-PK, MySQL and
// pooled-connection coarsening "look fail-closed" but had no executable control — and two of them cannot be
// driven by a real write here (there is no live MySQL, and a pooled PG client would need a real server). So
// planCapture is exercised directly: it is the single place precision is decided, which also gives the
// count/shape mismatch an observation seam.
//
// ISOLATION, stated honestly (verified by mutating each guard in turn):
//  - the no-PK, UPSERT and PK-changing cases ISOLATE their guard — removing it flips exactly that case;
//  - the MySQL and POOLED cases are OVER-DETERMINED: those dbs also fail the "reconstruction must be provably
//    faithful for this driver" gate, so removing the dialect or single-session guard alone does NOT flip them.
//    They assert the OUTCOME (such a write can never plan precise) rather than one guard. That is defence in
//    depth, not a defect — but it means they are not regression tests for those two guards specifically.

import { PGlite } from '@electric-sql/pglite'
import { integer, pgTable, text } from 'drizzle-orm/pg-core'
import { drizzle as pgDrizzle } from 'drizzle-orm/pglite'
import { drizzle as pgPoolDrizzle } from 'drizzle-orm/node-postgres'
import { mysqlTable, int as myInt, varchar } from 'drizzle-orm/mysql-core'
import { drizzle as mysqlDrizzle } from 'drizzle-orm/mysql2'
import { createConnection, createPool } from 'mysql2'
import { Pool } from 'pg'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { planCapture } from './writeCapture.js'

const keyed = pgTable('keyed', { id: integer('id').primaryKey(), name: text('name') })
const unkeyed = pgTable('unkeyed', { a: integer('a'), b: text('b') }) // NO primary key
const myUsers = mysqlTable('users', { id: myInt('id').primaryKey(), name: varchar('name', { length: 32 }) })

let client: PGlite
let pg: ReturnType<typeof pgDrizzle>

beforeAll(async () => {
  client = new PGlite()
  pg = pgDrizzle({ client })
})
afterAll(async () => await client.close())

/** The planner reads the BUILDER's shape; building one never executes it. */
const insertPlan = (db: object, builder: unknown, table: Parameters<typeof planCapture>[1]) =>
  planCapture(builder, table, 'insert', db)

describe('capture planning — fail-closed branches have executable controls', () => {
  it('a single-session PGlite write over a keyed table plans PRECISE (the positive control)', () => {
    const plan = insertPlan(pg, pg.insert(keyed).values({ id: 1, name: 'a' }), keyed)
    expect(plan.mode).toBe('precise')
  })

  it('NO primary key → coarse (a retraction could never be keyed)', () => {
    const plan = insertPlan(pg, pg.insert(unkeyed).values({ a: 1, b: 'x' }), unkeyed)
    expect(plan.mode).toBe('coarse')
  })

  it('MySQL → coarse (no RETURNING; precise MySQL is deferred pending a live lane)', () => {
    // A SINGLE CONNECTION, not a pool: session authority is provable here, so the ONLY reason this plans
    // coarse is the MySQL dialect itself. (With a pool it would coarsen via the pooled guard instead and the
    // case would pass for the wrong reason — removing the dialect guard would not flip it.)
    const my = mysqlDrizzle({
      client: createConnection({ host: 'my.example', port: 3307, database: 'd', user: 'u' }),
    })
    const plan = insertPlan(my, my.insert(myUsers).values({ id: 1, name: 'a' }), myUsers)
    expect(plan.mode).toBe('coarse')
  })

  it('POOLED (non-single-session) PG → coarse (session authority is unprovable — decision #6)', () => {
    const pooled = pgPoolDrizzle({ client: new Pool({ host: 'pg.example', port: 5433, database: 'd', user: 'u' }) })
    const plan = insertPlan(pooled, pooled.insert(keyed).values({ id: 1, name: 'a' }), keyed)
    expect(plan.mode).toBe('coarse')
  })

  it('UPSERT and PK-changing update plan coarse on a db that would otherwise be precise', () => {
    expect(insertPlan(pg, pg.insert(keyed).values({ id: 1, name: 'a' }).onConflictDoNothing(), keyed).mode).toBe(
      'coarse',
    )
    expect(planCapture(pg.update(keyed).set({ id: 2 }), keyed, 'update', pg).mode).toBe('coarse')
  })
})
