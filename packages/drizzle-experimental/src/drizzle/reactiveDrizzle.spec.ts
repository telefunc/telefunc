import { beforeEach, describe, expect, it, vi } from 'vitest'

// The client surface imports the read-capture engine DIRECTLY. Mock it so these tests drive the proxy
// surface with a controllable stub. `vi.mock` is hoisted above the imports below.
vi.mock('./writeCapture.js', () => ({
  captureMutation: vi.fn((_op: unknown, baseMethod: unknown) => baseMethod),
}))
vi.mock('./readCapture.js', () => ({
  wrapLiveSelect: vi.fn((baseBuilder: unknown) => baseBuilder),
}))

import { MySqlDialect } from 'drizzle-orm/mysql-core'
import { PgDialect } from 'drizzle-orm/pg-core'
import { reactiveDrizzle } from './reactiveDrizzle.js'
import { captureMutation } from './writeCapture.js'
import { wrapLiveSelect } from './readCapture.js'

// These tests drive the proxy over FAKE dbs (the engine is mocked). reactiveDrizzle's real-db constraint
// is a TYPE surface — proven against real Drizzle in the contract spec — so here we call through a cast
// that lets a stub db past it; the runtime behaviour under test is unchanged.
//
// The stubs carry a REAL `PgDialect` because construction now rejects an unsupported database (see the
// MySQL pin below), and that check reads the dialect's discriminator. Attached in place with
// `Object.assign` rather than by spreading: the assertions below compare the db handed to the engine by
// IDENTITY, and a copy would pass a different object than the caller holds.
const pgDialect = new PgDialect()
const reactive = (db: object) => reactiveDrizzle(Object.assign(db, { dialect: pgDialect }) as never)

// The runtime type contract (row types survive the terminal `.live()`, teeth) lives in the dedicated
// HKT contract spec, exercised against REAL Drizzle builders — a hand-rolled db can't stand in for the
// HKT seam. These tests own the PROXY behaviour: direct-return, select-wrapping, plain-field forwarding.

describe('reactiveDrizzle — an unsupported database is refused, not silently degraded', () => {
  // MySQL has no RETURNING, and there is no verified row-capture lane for it without one: every write would
  // invalidate every live query on the table. Returning a db that LOOKS reactive and quietly over-fires is
  // the failure this pin exists to prevent, so the refusal must happen at CONSTRUCTION — before any read or
  // write has a chance to be the thing that discovers it.
  //
  // The fixture carries drizzle's REAL `MySqlDialect` (from driver-free mysql-core, so no mysql2
  // dependency): it is the exact object whose discriminator `dialectOf` reads on a real mysql2 db.
  const mysqlDb = { dialect: new MySqlDialect(), select: () => ({}) }

  it('throws AT CONSTRUCTION on a MySQL db, naming the supported set', () => {
    expect(() => reactiveDrizzle(mysqlDb as never)).toThrow(/does not support MySQL/)
    expect(() => reactiveDrizzle(mysqlDb as never)).toThrow(/PostgreSQL, PGlite, SQLite/)
  })

  it('…and the throw is what stops it: a supported db constructs fine through the same path', () => {
    // The control. Without it this pin would still pass if `reactiveDrizzle` threw on EVERY db.
    expect(() => reactive({ select: () => ({}) })).not.toThrow()
  })
})

describe('reactiveDrizzle — client surface', () => {
  beforeEach(() => {
    vi.mocked(captureMutation).mockClear()
    vi.mocked(wrapLiveSelect).mockReset()
    vi.mocked(wrapLiveSelect).mockImplementation((baseBuilder: unknown) => baseBuilder)
  })

  it('returns the proxied db DIRECTLY (no accessor), with NO request context of any kind', () => {
    // No telefunc context is provided anywhere in this file: reactiveDrizzle binding to a request would
    // throw here. Module-level `export const db = reactiveDrizzle(baseDb)` is the intended shape.
    const base = { tag: 'base', select: () => ({}) }
    const db = reactive(base)
    expect(typeof db).toBe('object')
  })

  it('plain fields forward untouched (observable-equivalence)', () => {
    const base = { tag: 'base', select: () => ({}) }
    const db = reactive(base) as unknown as { tag: string }
    expect(db.tag).toBe('base')
  })

  it('select() routes through the read-capture engine with this db’s own base builder', () => {
    const baseBuilder = { from: () => ({}) }
    const base = { select: vi.fn(() => baseBuilder) }
    const db = reactive(base) as unknown as { select: () => unknown }

    const built = db.select()
    expect(base.select).toHaveBeenCalledTimes(1)
    expect(wrapLiveSelect).toHaveBeenCalledTimes(1)
    const [passedBuilder, passedDb] = vi.mocked(wrapLiveSelect).mock.calls[0]!
    expect(passedBuilder).toBe(baseBuilder) // the engine wraps THIS db's own base builder
    expect(passedDb).toBe(base) // …keyed to THIS db, so reads and writes share one registry
    expect(built).toBe(baseBuilder) // the pass-through stub returns it untouched
  })

  it('the terminal `.live()` is the engine wrapper’s; `then`/`execute` stay plain rows', async () => {
    // The engine's wrapLiveSelect result mirrors readCapture's proxy: `.live()` captures (a Live),
    // `execute()`/`await` forward to PLAIN rows. The base builder itself has NO `.live` — nothing leaks.
    const rows = [{ id: 7 }]
    const liveBuilder = {
      from: (_t: unknown) => liveBuilder,
      execute: () => Promise.resolve(rows),
      live: () => Promise.resolve({ data: rows }),
    }
    vi.mocked(wrapLiveSelect).mockReturnValue(liveBuilder)
    const base = { select: () => ({}) } // base builder: no `.live`
    const db = reactive(base) as unknown as { select: () => typeof liveBuilder }

    const live = await db.select().from(0).live()
    expect(live).toEqual({ data: rows })
    const executed = await db.select().from(0).execute() // the un-captured terminal → plain rows
    expect(executed).toEqual(rows)
  })

  it('writes route through the mutation seam; the seam sees the op + the db (for ingest routing)', () => {
    const insert = vi.fn()
    const base = { select: () => ({}), insert }
    const db = reactive(base) as unknown as { insert: unknown }
    void db.insert // property access drives the proxy get
    // Capture is keyed to the db (the context's identityDb) so it reaches the SAME registry the reads
    // acquired from — and the entry's context is the autocommit one.
    expect(captureMutation).toHaveBeenCalledWith(
      'insert',
      expect.any(Function),
      expect.objectContaining({ sinkMode: 'autocommit', identityDb: base }),
    )
  })

  // EVERY write op, not just the one. The op list is a hand-maintained set, and dropping a member from it
  // routes that verb straight to plain Drizzle — the write commits and NOTHING is captured or published,
  // which is a silent missed invalidation rather than an error. Removing `delete` from it passed all 823
  // tests before this case existed: every other delete test calls `captureMutation`/`planCapture` directly
  // and so never asks whether the PROXY would have got there.
  it.each(['insert', 'update', 'delete'] as const)('`%s` routes through the mutation seam', (op) => {
    const base = { select: () => ({}), insert: vi.fn(), update: vi.fn(), delete: vi.fn() }
    const db = reactive(base) as unknown as Record<string, unknown>
    void db[op]
    expect(captureMutation).toHaveBeenCalledWith(
      op,
      expect.any(Function),
      expect.objectContaining({ sinkMode: 'autocommit', identityDb: base }),
    )
  })

  it('…and a NON-write member does not, so the check above is not just matching everything', () => {
    // The control for the case above: without it, a seam that captured every member access would satisfy
    // all three assertions while telling us nothing about the op list.
    const base = { select: () => ({}), insert: vi.fn(), somethingElse: vi.fn() }
    const db = reactive(base) as unknown as Record<string, unknown>
    void db.somethingElse
    expect(captureMutation).not.toHaveBeenCalled()
  })

  it('CTE-prefixed reads are NOT reactive: with() is forwarded unwrapped, so its builders carry no .live()', () => {
    // Only the proxy's OWN select() is wrapped. `with()` returns Drizzle's ordinary facade untouched, so
    // its select builders never gain a terminal `.live()` — the runtime matches the type contract, which
    // rejects `db.with(cte).select()…live()`.
    const cteBuilder = { from: () => ({ where: () => ({}) }) }
    const facade = { select: () => cteBuilder }
    const base = { select: () => ({}), with: () => facade }
    const db = reactive(base) as unknown as {
      with: (cte: unknown) => { select: () => { from: (t: unknown) => Record<string, unknown> } }
    }
    const built = db.with('sq').select().from('sq')
    expect(built.live).toBeUndefined() // no synthesized terminal on a with()-facade builder
    expect(wrapLiveSelect).not.toHaveBeenCalled() // the engine never wrapped a with() builder
  })
})
