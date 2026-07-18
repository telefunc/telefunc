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
import { entityKind } from 'drizzle-orm'
import { mysqlTable, int as myInt, varchar } from 'drizzle-orm/mysql-core'
import { drizzle as mysqlDrizzle } from 'drizzle-orm/mysql2'
import { drizzle as pgPoolDrizzle } from 'drizzle-orm/node-postgres'
import { integer, pgTable, text } from 'drizzle-orm/pg-core'
import { drizzle as pgDrizzle } from 'drizzle-orm/pglite'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { captureMismatch, planCapture } from './writeCapture.js'

const keyed = pgTable('keyed', { id: integer('id').primaryKey(), name: text('name') })
const unkeyed = pgTable('unkeyed', { a: integer('a'), b: text('b') }) // NO primary key
const myUsers = mysqlTable('users', { id: myInt('id').primaryKey(), name: varchar('name', { length: 32 }) })

// HERMETIC BY CONSTRUCTION — no sockets. Planning reads only the db's SHAPE (dialect entityKind, driver
// entityKind, and the client constructor NAME that classifies single-session vs pooled); it never executes.
// Pointing a real mysql2/pg client at an unroutable host used to open an actual socket, whose async
// ENOTFOUND / PROTOCOL_CONNECTION_LOST surfaced as an unhandled error AFTER the assertions passed — the
// suite printed green and exited 1.
//
// The drizzle constructors below are the REAL ones, so the dialect/driver discriminators are genuine; only
// the client is a stub. `assertClassifierInputs` then pins every field the classifier actually reads, so if
// drizzle changes its shape this fails loudly instead of quietly planning against a fiction.
class Connection {
  config = {} // drizzle's mysql2 driver writes `supportBigNumbers` here
}
class Pool {}

/** These stubs implement only what the classifier reads, not the driver's full type — the cast is the
 *  point, and `assertClassifierInputs` is what keeps it honest at runtime. */
const asClient = (stub: object): never => stub as never

const kindOf = (value: unknown): string | undefined =>
  (value as { constructor?: { [entityKind]?: string } } | null)?.constructor?.[entityKind]

function assertClassifierInputs(db: object, expected: { dialect: string; driver: string; client: string }) {
  expect(kindOf((db as { dialect?: unknown }).dialect)).toBe(expected.dialect)
  expect(kindOf(db)).toBe(expected.driver)
  expect((db as { $client?: { constructor?: { name?: string } } }).$client?.constructor?.name).toBe(expected.client)
}

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
    const my = mysqlDrizzle({ client: asClient(new Connection()) })
    assertClassifierInputs(my, { dialect: 'MySqlDialect', driver: 'MySql2Database', client: 'Connection' })
    const plan = insertPlan(my, my.insert(myUsers).values({ id: 1, name: 'a' }), myUsers)
    expect(plan.mode).toBe('coarse')
  })

  it('POOLED (non-single-session) PG → coarse (session authority is unprovable — decision #6)', () => {
    const pooled = pgPoolDrizzle({ client: asClient(new Pool()) })
    assertClassifierInputs(pooled, { dialect: 'PgDialect', driver: 'NodePgDatabase', client: 'Pool' })
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

// The SHAPE/IDENTITY mismatch seam. Planning decides precision from the write's shape BEFORE it runs; this
// is the check after it runs, on the rows actually captured. It had no executable control — and the
// hidden-RETURNING path had no check at all, so a driver returning a narrowed row would have emitted a
// partial image as a precise change.
describe('capture mismatch — captured rows are verified, not trusted', () => {
  const columns = ['id', 'name']
  const pk = ['id']
  const full = [
    { id: 1, name: 'a' },
    { id: 2, name: 'b' },
  ]

  it('a full, keyed image reports NO mismatch (the positive control)', () => {
    expect(captureMismatch(full, columns, pk, 'insert')).toBeUndefined()
    expect(captureMismatch(full, columns, pk, 'update')).toBeUndefined()
    expect(captureMismatch(full, columns, pk, 'delete')).toBeUndefined()
  })

  it('an empty row set is not a mismatch — a write affecting no rows captures nothing', () => {
    expect(captureMismatch([], columns, pk, 'update')).toBeUndefined()
  })

  it('a row missing a demanded column is caught, and the ROW INDEX is reported', () => {
    const narrowed = [{ id: 1, name: 'a' }, { id: 2 }] // second row lacks `name`
    expect(captureMismatch(narrowed, columns, pk, 'insert')).toEqual({
      rowIndex: 1,
      reason: 'missing-columns',
      detail: 'name',
    })
  })

  it('checks EVERY row, not just the first — a widened first row cannot vouch for the rest', () => {
    // This is the regression the old single-row check could not catch: `rows[0]` carries every column, so a
    // `rows[0]`-only check passes while row 1 is a partial image.
    const narrowed = [{ id: 1, name: 'a' }, { id: 2 }]
    expect(columns.every((c) => c in narrowed[0]!)).toBe(true) // what the old check asked
    expect(captureMismatch(narrowed, columns, pk, 'insert')).toBeDefined() // what is now asked
  })

  it('a retraction whose PK value is absent or NULL is caught — a key of undefined keys nothing', () => {
    const noKey = [{ id: undefined, name: 'a' }]
    expect(captureMismatch(noKey, columns, pk, 'delete')).toEqual({
      rowIndex: 0,
      reason: 'missing-key',
      detail: 'id',
    })
    expect(captureMismatch([{ id: null, name: 'a' }], columns, pk, 'update')).toMatchObject({
      reason: 'missing-key',
    })
    // An INSERT carries the whole row and is not keyed, so the same rows are fine there.
    expect(captureMismatch(noKey, columns, pk, 'insert')).toBeUndefined()
  })

  it('a composite key is only satisfied when EVERY key field is present', () => {
    const composite = ['a', 'b']
    expect(captureMismatch([{ a: 1, b: 2 }], composite, composite, 'delete')).toBeUndefined()
    expect(captureMismatch([{ a: 1, b: null }], composite, composite, 'delete')).toMatchObject({
      reason: 'missing-key',
      detail: 'b',
    })
  })
})
