import { beforeAll, describe, expect, it } from 'vitest'
import { reactiveDrizzle, _installDbLiveRuntime } from './reactiveDrizzle.js'
import type { ClientLive } from 'telefunc'

// ── Compile-time type-transform test (Ticket 6 §1, T6.A1/A2). Verified by tsc; never executed. Proves:
//    plain `db.select()...`  awaits to  `Row[]`             (unchanged — observable-equivalence)
//    `db.live.select()...`   awaits to  `ClientLive<Row[]>`  (the one owned re-typing seam)
//    plain fields are preserved. ────────────────────────────────────────────────────────────────────
type Row = { id: number; text: string }
interface SelectBuilder extends PromiseLike<Row[]> {
  from(t: unknown): SelectBuilder
  where(c: unknown): SelectBuilder
  toSQL(): { sql: string; params: unknown[] }
  // drizzle's QueryPromise terminal: runs the query to PLAIN rows. It is the structural marker the
  // transform keys on (a builder has `.execute()`), and its OWN result must stay plain — Finding 4.
  execute(): Promise<Row[]>
}
interface MockDb {
  select(): SelectBuilder
  tag: string
}
async function _typeTransform_compileCheck(): Promise<void> {
  const getDb = reactiveDrizzle({} as MockDb)
  const db = getDb()
  const plain: Row[] = await db.select().from(0).where(0) // plain path unchanged
  const live: ClientLive<Row[]> = await db.live.select().from(0).where(0) // remapped to ClientLive
  const tag: string = db.tag // plain field preserved
  // Finding 4 — TYPE-NEGATIVE: `.execute()` is the un-captured terminal → PLAIN rows, NOT ClientLive.
  const executed: Row[] = await db.live.select().from(0).execute()
  // @ts-expect-error `.execute()` must NOT type as ClientLive (it runs the query and resolves to rows).
  const executedLie: ClientLive<Row[]> = await db.live.select().from(0).execute()
  void plain
  void live
  void tag
  void executed
  void executedLie
}
void _typeTransform_compileCheck

describe('reactiveDrizzle — client surface (Ticket 6 §1)', () => {
  beforeAll(() => {
    // The real runtime (read-capture + write-capture) lands with EngineFix's U3 seam; a pass-through stub
    // exercises the client surface here. `_installDbLiveRuntime` is the documented install point.
    _installDbLiveRuntime({
      acquireCarrier: () => ({ __dbLiveCarrier: true }),
      wrapLiveSelect: (baseBuilder) => baseBuilder,
      captureMutation: (_op, baseMethod) => baseMethod,
    })
  })

  it('per-request accessor: plain fields forward (observable-equivalence), `.live` is added', () => {
    const base = { tag: 'base', select: () => ({ from: () => ({}) }) } as unknown as MockDb
    const db = reactiveDrizzle(base)()
    expect((db as unknown as { tag: string }).tag).toBe('base') // plain forwards untouched
    expect(typeof (db as unknown as { live: { select: unknown } }).live.select).toBe('function') // .live added
  })

  it('`reactiveDrizzle(db)` returns a callable per-request accessor (not the db itself)', () => {
    const acquire = reactiveDrizzle({ tag: 't', select: () => ({}) } as unknown as MockDb)
    expect(typeof acquire).toBe('function')
    expect(acquire()).not.toBe(undefined)
  })

  it('Finding 4 — `.execute()` yields plain rows and mints nothing; only the awaited-builder path captures', async () => {
    const mints: number[] = []
    const rows: Row[] = [{ id: 7, text: 'x' }]
    // The engine's wrapLiveSelect result: the terminal `then` captures (mint); `.execute()` is a plain
    // forwarded terminal (resolves to rows, never captured). Mirrors readCapture's proxy behaviour.
    const liveBuilder = {
      from: () => liveBuilder,
      where: () => liveBuilder,
      execute: () => Promise.resolve(rows),
      then: (onFulfilled: (v: unknown) => unknown, onRejected?: (e: unknown) => unknown) => {
        mints.push(1)
        return Promise.resolve({ live: true }).then(onFulfilled, onRejected)
      },
    }
    _installDbLiveRuntime({
      acquireCarrier: () => ({ __dbLiveCarrier: true }),
      wrapLiveSelect: () => liveBuilder,
      captureMutation: (_op, baseMethod) => baseMethod,
    })
    const db = reactiveDrizzle({ tag: 't', select: () => ({}) } as unknown as MockDb)()

    const executed = await db.live.select().from(0).execute() // forwarded terminal → plain rows, no mint
    expect(executed).toEqual(rows)
    expect(mints).toHaveLength(0)

    await db.live.select().from(0) // the awaited-builder terminal DOES capture
    expect(mints).toHaveLength(1)
  })
})
