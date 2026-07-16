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
  void plain
  void live
  void tag
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
})
