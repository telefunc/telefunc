// THE SUBSTITUTION MECHANISM'S OWN EDGES.
//
// The substitution rewrites a shared builder, installs a shadow on it, and lets drizzle decode rows the
// caller never asked for. Each of those is a place where capture can change something the caller owns
// WITHOUT the write itself being wrong — which is why none of them is caught by asserting results on a
// quiet, sequential, single-execution path.
//
// Three states, each driven against real PGlite and each paired with the PLAIN drizzle oracle it must match:
//   - the same builder executed CONCURRENTLY with itself, which reads capture's half-applied builder state;
//   - a valid STATEFUL `customType.fromDriver`, which counts how many times and in what order capture makes
//     the caller's decoders run;
//   - a builder the shadow cannot be installed on, which must cost precision and never the write.

import { PGlite } from '@electric-sql/pglite'
import { customType, integer, pgTable, text } from 'drizzle-orm/pg-core'
import { drizzle as pgDrizzle } from 'drizzle-orm/pglite'
import { eq } from 'drizzle-orm'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { captureMutation } from './writeCapture.js'
import { probeOldNewReturning } from '../binding/database.js'
import type { TableChange } from '../router/events.js'

// A VALID decoder that is not pure: it appends its own invocation number. Nothing here is pathological —
// `fromDriver` is called once per value per statement, and this simply makes each call visible. It is the
// instrument that turns "capture reuses drizzle's mapper" into the sharper question the mapper cannot answer
// on its own: how many times, over which values, and in what order.
let decodes = 0
const counted = customType<{ data: string; driverData: string }>({
  dataType: () => 'text',
  fromDriver: (value) => `${value}#${++decodes}`,
})
/** A decoder that throws, to force capture's own mapping to fail AFTER the caller's has run. */
const exploding = customType<{ data: string; driverData: string }>({
  dataType: () => 'text',
  fromDriver: () => {
    throw new Error('decoder exploded')
  },
})

const plainUsers = pgTable('users', { id: integer('id').primaryKey(), name: text('name') })
const stateful = pgTable('users', { id: integer('id').primaryKey(), a: counted('a'), b: counted('b') })
const mixed = pgTable('users', {
  id: integer('id').primaryKey(),
  value: counted('value'),
  payload: exploding('payload'),
})

const DDL = 'create table users (id int primary key, a text, b text)'
const MIXED_DDL = 'create table users (id int primary key, value text, payload text)'
const PLAIN_DDL = 'create table users (id int primary key, name text)'

type AnyBuilder = (table: unknown) => any
function capturing(db: object, op: 'insert' | 'update' | 'delete', method: (t: never) => unknown) {
  const batches: TableChange[][] = []
  const wrapped = captureMutation(op, method as (...a: unknown[]) => unknown, db, (changes) =>
    batches.push(changes),
  ) as AnyBuilder
  return { wrapped, batches }
}

/** Drizzle marks a write builder's `config` protected. These tests reach it on purpose — sealing it is the
 *  whole point — so the reach is named once here rather than cast at each site. */
const configOf = (builder: unknown): Record<string, unknown> => (builder as { config: Record<string, unknown> }).config

async function fresh(ddl = DDL) {
  const client = new PGlite()
  const db = pgDrizzle({ client })
  await client.exec(ddl)
  return { client, db }
}

beforeEach(() => {
  decodes = 0
})

describe('mechanism — one builder executed CONCURRENTLY with itself', () => {
  // Substitution rewrites `config.returning` on the shared builder and puts it back when the statement
  // finishes — an interval that spans an await. A second execution begun inside that interval planned against
  // CAPTURE's full RETURNING and took the caller-returning branch, so one of the two callers received
  // capture's rows. Sequential reuse never reaches this state, which is why it survived the last gate.

  it('PLAIN drizzle: the pair is the count-shape twice (the oracle)', async () => {
    const { client, db } = await fresh()
    await client.exec("insert into users values (1,'x','y')")
    const builder = db.update(stateful).set({ a: 'after' }).where(eq(stateful.id, 1))
    await expect(Promise.all([builder, builder])).resolves.toEqual([
      { rows: [], fields: [], affectedRows: 1 },
      { rows: [], fields: [], affectedRows: 1 },
    ])
    await client.close()
  })

  it('CAPTURED: the pair is identical — neither caller is handed capture’s temporary RETURNING', async () => {
    const { client, db } = await fresh()
    await client.exec("insert into users values (1,'x','y')")
    const { wrapped, batches } = capturing(db, 'update', db.update.bind(db))
    const builder = wrapped(stateful).set({ a: 'after' }).where(eq(stateful.id, 1))

    const pair = await Promise.all([builder, builder])

    expect(pair).toEqual([
      { rows: [], fields: [], affectedRows: 1 },
      { rows: [], fields: [], affectedRows: 1 },
    ])
    // Both executions were captured, and neither saw the other's half-applied builder.
    expect(batches).toHaveLength(2)
    expect(batches.flat().every((change) => change.kind === 'update')).toBe(true)
    // The shadow is gone: two overlapping taps restoring one `_prepare` out of order would leave one behind.
    expect(Object.hasOwn(builder, '_prepare')).toBe(false)
    await client.close()
  })

  it('DIFFERENT builders still run concurrently — the queue is per builder, not global', async () => {
    // The cost of the fix must not be "autocommit writes are serialized". Two distinct builders share no
    // mutable state, so they share no queue.
    const { client, db } = await fresh()
    await client.exec("insert into users values (1,'x','y'), (2,'x','y')")
    const { wrapped, batches } = capturing(db, 'update', db.update.bind(db))

    const results = await Promise.all([
      wrapped(stateful).set({ a: 'one' }).where(eq(stateful.id, 1)),
      wrapped(stateful).set({ a: 'two' }).where(eq(stateful.id, 2)),
    ])

    expect(results).toEqual([
      { rows: [], fields: [], affectedRows: 1 },
      { rows: [], fields: [], affectedRows: 1 },
    ])
    expect(batches).toHaveLength(2)
    expect((await client.query('select a from users order by id')).rows).toEqual([{ a: 'one' }, { a: 'two' }])
    await client.close()
  })
})

describe('mechanism — a borrowed builder is put back in BOTH the halves `.returning()` writes', () => {
  // `.returning()` records its selection TWICE: `returning` is what executes, and `returningFields` is what
  // `getSelectedFields()` reports. Capture's substitution overwrites both, so restoring only the first leaves
  // the builder DESCRIBING a selection it no longer runs — a builder that lies about itself to any caller who
  // introspects it, and to drizzle's own `$dynamic` composition.
  //
  // The existing frozen-config cases pin `returning`, and pinned it alone: dropping the `returningFields`
  // half of the restore left the whole suite green. This is the case that disagrees with that mutant.

  it('a WIDENED caller projection leaves getSelectedFields() reporting the caller’s own columns', async () => {
    const { client, db } = await fresh(PLAIN_DDL)
    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    // A PARTIAL projection is the case capture WIDENS: it substitutes a full-row RETURNING, captures that,
    // and projects `mine` back out — so both halves of the caller's selection really are overwritten and
    // really do have to come back.
    const builder = wrapped(plainUsers).values({ id: 1, name: 'a' }).returning({ mine: plainUsers.name })
    const callerFields = configOf(builder).returningFields

    await expect(builder).resolves.toEqual([{ mine: 'a' }])
    // The capture side genuinely widened — otherwise this test would pass without the substitution ever
    // having overwritten anything, and the restore it claims to pin would never have been exercised.
    expect(batches).toEqual([[{ table: 'users', kind: 'insert', new: { id: 1, name: 'a' } }]])

    // BOTH halves are the caller's again — the same objects they built, not equal ones written over the top.
    expect(configOf(builder).returningFields).toBe(callerFields)
    expect(Object.keys(builder.getSelectedFields())).toEqual(['mine'])
    await client.close()
  })
})

describe('mechanism — capture must not change WHICH decode the caller’s value comes from', () => {
  // A BOTH-IMAGES statement decodes every column twice (old and new); a widened full-row statement decodes in
  // TABLE order rather than the caller's. Reusing drizzle's mapper is therefore not enough on its own: the
  // caller's value has to come from the invocation their own statement would have made. It now does, because
  // their projection is decoded FIRST, before capture's mapper touches anything.

  it('OLD/NEW update: the caller’s value is the FIRST decode, exactly as plain drizzle’s is', async () => {
    const { client, db } = await fresh()
    expect(await probeOldNewReturning(db)).toBe(true) // the lane exists — this would be vacuous otherwise
    await client.exec("insert into users values (1,'x','y')")

    // THE ORACLE: plain drizzle decodes the new value once, so the caller sees invocation #1.
    decodes = 0
    const oracle = await db.update(stateful).set({ a: 'after' }).where(eq(stateful.id, 1)).returning({ v: stateful.a })
    expect(oracle).toEqual([{ v: 'after#1' }])
    expect(decodes).toBe(1)

    decodes = 0
    const { wrapped, batches } = capturing(db, 'update', db.update.bind(db))
    const result = await wrapped(stateful).set({ a: 'after2' }).where(eq(stateful.id, 1)).returning({ v: stateful.a })

    // `after2#2` is what capture's OLD/NEW mapping produces for the new half — the old image is decoded
    // first, so the caller used to be handed the SECOND invocation of their own decoder.
    expect(result).toEqual([{ v: 'after2#1' }])
    // Capture did decode more than the caller's statement would have — that is the documented residual — but
    // none of it reached them.
    expect(decodes).toBeGreaterThan(1)
    expect(batches).toHaveLength(1)
    await client.close()
  })

  it('widened full row: the caller’s ORDER decides their invocation order, not the table’s', async () => {
    // Capture's substituted selection is the table's columns in order (id, a, b). The caller asked for b
    // before a. Their decoders must run in THEIR order, or a stateful decoder pairs the wrong number with
    // the wrong column.
    const { client, db } = await fresh()
    await client.exec("insert into users values (1,'x','y')")

    decodes = 0
    const oracle = await db
      .update(stateful)
      .set({ a: 'z' })
      .where(eq(stateful.id, 1))
      .returning({ myB: stateful.b, myA: stateful.a })
    expect(oracle).toEqual([{ myB: 'y#1', myA: 'z#2' }]) // b first, a second — the caller's order

    decodes = 0
    const { wrapped } = capturing(db, 'update', db.update.bind(db))
    const result = await wrapped(stateful)
      .set({ a: 'z2' })
      .where(eq(stateful.id, 1))
      .returning({ myB: stateful.b, myA: stateful.a })

    expect(result).toEqual([{ myB: 'y#1', myA: 'z2#2' }])
    await client.close()
  })

  it('capture’s mapping THROWS after the caller’s ran: their value is still the first decode, decoded once', async () => {
    // The path the re-gate flagged as double-decoding: the caller's decoder ran once during capture's mapping,
    // that mapping then threw on a capture-only column, and the recovery decoded the caller's column AGAIN.
    // With the caller decoded first there is nothing left to redo — their result already exists.
    const { client, db } = await fresh(MIXED_DDL)
    await client.exec("insert into users values (1,'v','p')")

    decodes = 0
    const oracle = await db.update(mixed).set({ value: 'after' }).where(eq(mixed.id, 1)).returning({ v: mixed.value })
    expect(oracle).toEqual([{ v: 'after#1' }])
    expect(decodes).toBe(1)

    decodes = 0
    const { wrapped, batches } = capturing(db, 'update', db.update.bind(db))
    const result = await wrapped(mixed).set({ value: 'after2' }).where(eq(mixed.id, 1)).returning({ v: mixed.value })

    expect(result).toEqual([{ v: 'after2#1' }]) // #1, not #2 — no second pass over their column
    // Capture's own mapping died on `payload`, so the write coarsens while the caller is unaffected.
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    expect((await client.query('select value from users where id = 1')).rows).toEqual([{ value: 'after2' }])
    await client.close()
  })
})

describe('mechanism — a seam capture cannot install on costs PRECISION, never the write', () => {
  it('a non-extensible builder: plain drizzle resolves, and so does capture — coarsely', async () => {
    // `Object.defineProperty` used to run before the recovery's `try`, so a builder that could not be
    // shadowed rejected a write the database would have accepted, and left capture's RETURNING behind. The
    // shadow is now placed inside it: failing to place one refuses the SUBSTITUTION, not the statement.
    const { client, db } = await fresh(PLAIN_DDL)

    // THE ORACLE: plain drizzle does not care that the builder is sealed.
    const plainBuilder = db.insert(plainUsers).values({ id: 1, name: 'plain' })
    Object.preventExtensions(plainBuilder)
    await expect(plainBuilder).resolves.toEqual({ rows: [], fields: [], affectedRows: 1 })

    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    const builder = wrapped(plainUsers).values({ id: 2, name: 'captured' })
    Object.preventExtensions(builder)

    // The caller's write runs exactly as they wrote it…
    await expect(builder).resolves.toEqual({ rows: [], fields: [], affectedRows: 1 })
    expect((await client.query('select id, name from users order by id')).rows).toEqual([
      { id: 1, name: 'plain' },
      { id: 2, name: 'captured' },
    ])
    // …and capture pays for it in precision rather than in the caller's result.
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    await client.close()
  })

  it('a caller’s own RETURNING survives a refused substitution on a sealed builder', async () => {
    // The other half of failing closed: the substitution had already rewritten the builder when placement
    // failed, so the caller's own selection has to be put back before their statement runs.
    const { client, db } = await fresh(PLAIN_DDL)

    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    const builder = wrapped(plainUsers).values({ id: 1, name: 'a' }).returning({ mine: plainUsers.name })
    Object.preventExtensions(builder)

    await expect(builder).resolves.toEqual([{ mine: 'a' }]) // their projection, not capture's widened row
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    await client.close()
  })

  it('a FROZEN drizzle config: capture cannot even write its RETURNING, and the write is untouched', async () => {
    // One phase earlier than the sealed-builder case above, and the same principle. Rewriting the builder's
    // RETURNING is an assignment into drizzle's own `config`, which a caller is free to have frozen — so
    // building the substituted statement is fallible too, and used to throw a TypeError out of a write the
    // database would have accepted, before the caller's statement ever ran.
    const { client, db } = await fresh(PLAIN_DDL)

    // THE ORACLE: plain drizzle never calls `.returning()`, so a frozen config costs it nothing.
    const plainBuilder = db.insert(plainUsers).values({ id: 1, name: 'plain' })
    Object.freeze(configOf(plainBuilder))
    await expect(plainBuilder).resolves.toEqual({ rows: [], fields: [], affectedRows: 1 })

    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    const builder = wrapped(plainUsers).values({ id: 2, name: 'captured' })
    Object.freeze(builder.config)

    await expect(builder).resolves.toEqual({ rows: [], fields: [], affectedRows: 1 })
    expect((await client.query('select id, name from users order by id')).rows).toEqual([
      { id: 1, name: 'plain' },
      { id: 2, name: 'captured' },
    ])
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    // Nothing was written to the config, so nothing is written back to it: `returning` never appeared.
    expect('returning' in builder.config).toBe(false)
    await client.close()
  })

  it('a FROZEN config under a caller’s own RETURNING: their projection survives untouched', async () => {
    // The other side of the same phase. Here `returning` already EXISTS on the frozen config, so the
    // substitution's overwrite is rejected rather than its insertion — and the caller's own selection has to
    // come back through unharmed, by never having been replaced in the first place.
    const { client, db } = await fresh(PLAIN_DDL)
    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    const builder = wrapped(plainUsers).values({ id: 1, name: 'a' }).returning({ mine: plainUsers.name })
    const callerSelection = builder.config.returning
    Object.freeze(builder.config)

    await expect(builder).resolves.toEqual([{ mine: 'a' }])
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    // The very same selection object the caller built — not an equal one restored over the top of it.
    expect(builder.config.returning).toBe(callerSelection)
    await client.close()
  })

  it('restoring touches ONLY what changed — a refused construction writes nothing back', async () => {
    // The companion defect: `restore()` assigned both fields unconditionally, so on a config that rejects
    // assignment the cleanup threw on the way out of an already-decided statement. Comparing before
    // assigning means a config nothing was written to is a config nothing is written back to.
    const { client, db } = await fresh(PLAIN_DDL)
    const errors = vi.spyOn(console, 'error').mockImplementation(() => {})
    try {
      const { wrapped } = capturing(db, 'insert', db.insert.bind(db))
      const builder = wrapped(plainUsers).values({ id: 1, name: 'a' })
      Object.freeze(builder.config)
      await expect(builder).resolves.toEqual({ rows: [], fields: [], affectedRows: 1 })

      // Capture reports that it could not BUILD its statement — that degradation is expected and loud…
      const said = errors.mock.calls.map((call) => String(call[0]))
      expect(said.some((line) => line.includes('could not build the statement'))).toBe(true)
      // …but it never reports a failed RESTORE, because it never attempted an assignment it did not need.
      expect(said.some((line) => line.includes('could not restore the query builder'))).toBe(false)
    } finally {
      errors.mockRestore()
      await client.close()
    }
  })

  it('a single NON-WRITABLE returning property is refused the same way as a frozen config', async () => {
    // Narrower than `Object.freeze`: the config is otherwise ordinary and only the one property capture
    // wants is locked. The refusal must key off the assignment failing, not off the object being frozen.
    const { client, db } = await fresh(PLAIN_DDL)
    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    const builder = wrapped(plainUsers).values({ id: 1, name: 'a' })
    Object.defineProperty(builder.config, 'returning', { value: undefined, writable: false, configurable: false })

    await expect(builder).resolves.toEqual({ rows: [], fields: [], affectedRows: 1 })
    expect((await client.query('select id, name from users')).rows).toEqual([{ id: 1, name: 'a' }])
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    await client.close()
  })

  it('rows that do not arrive as an ARRAY yield an error, never a count nobody measured', async () => {
    // The observation's length is the whole basis of the count-shaped answer, so the container it is taken
    // from is checked rather than assumed. A driver (or a future drizzle) handing the mapper something other
    // than an array leaves the tap empty, and an empty tap rethrows.
    const real = new PGlite()
    await real.exec(DDL)
    const db = pgDrizzle({
      client: new Proxy(real, {
        get(object, prop, receiver) {
          const value = Reflect.get(object, prop, receiver)
          if (typeof value !== 'function') return value
          if (prop !== 'query') return (value as (...a: unknown[]) => unknown).bind(object)
          return async (...args: unknown[]) => {
            const result = (await (value as (...a: unknown[]) => Promise<unknown>).apply(object, args)) as {
              rows: unknown
            }
            // Only capture's statement carries a RETURNING, so only capture's mapper sees this.
            return /\breturning\b/i.test(String(args[0])) ? { ...result, rows: { notAnArray: true } } : result
          }
        },
      }),
    })

    const { wrapped, batches } = capturing(db, 'insert', db.insert.bind(db))
    let rejection: unknown
    await wrapped(stateful)
      .values({ id: 1, a: 'x', b: 'y' })
      .catch((error: unknown) => {
        rejection = error
      })

    expect(rejection).toBeInstanceOf(Error)
    expect(rejection).not.toMatchObject({ affectedRows: 0 })
    expect(rejection).not.toMatchObject({ affectedRows: 1 })
    // The write DID apply, so the table is still coarsened — emission follows the database, not the caller.
    expect(batches).toEqual([[{ table: 'users', kind: 'coarse' }]])
    await real.close()
  })
})
