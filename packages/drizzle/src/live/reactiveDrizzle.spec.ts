import { beforeEach, describe, expect, it, vi } from 'vitest'

// The client surface imports the concrete runtime DIRECTLY (the `_installDbLiveRuntime` seam was removed in
// the auto-load full-remove). Mock both units so these tests drive the proxy surface with controllable
// stubs: the carrier lifecycle (`./dbLiveRuntime`) and the read-capture engine (`./readCapture`).
// `vi.mock` is hoisted above the imports below.
vi.mock('./dbLiveRuntime.js', () => ({
  acquireCarrier: vi.fn(() => ({ __dbLiveCarrier: true })),
  captureMutation: vi.fn((_op: unknown, baseMethod: unknown) => baseMethod),
}))
vi.mock('./readCapture.js', () => ({
  wrapLiveSelect: vi.fn((baseBuilder: unknown) => baseBuilder),
}))

import { reactiveDrizzle } from './reactiveDrizzle.js'
import { acquireCarrier, captureMutation } from './dbLiveRuntime.js'
import { wrapLiveSelect } from './readCapture.js'

// These tests drive the proxy over FAKE dbs (the engine is mocked). reactiveDrizzle's real-db constraint
// is a TYPE surface — proven against real Drizzle in the contract spec — so here we call through a cast
// that lets a stub db past it; the runtime behaviour under test is unchanged.
const reactive = (db: object) => reactiveDrizzle(db as never)

// The runtime type contract (row types survive the terminal `.live()`, teeth) lives in the dedicated
// HKT contract spec, exercised against REAL Drizzle builders — a hand-rolled db can't stand in for the
// HKT seam. These tests own the PROXY behaviour: direct-return, select-wrapping, plain-field forwarding.

describe('reactiveDrizzle — client surface', () => {
  beforeEach(() => {
    vi.mocked(acquireCarrier).mockClear()
    vi.mocked(captureMutation).mockClear()
    vi.mocked(wrapLiveSelect).mockReset()
    vi.mocked(wrapLiveSelect).mockImplementation((baseBuilder: unknown) => baseBuilder)
  })

  it('returns the proxied db DIRECTLY (no accessor), capturing the carrier once up front', () => {
    const base = { tag: 'base', select: () => ({}) }
    const db = reactive(base)
    // The db itself — not a function you must call to acquire it. The old `()` accessor is gone.
    expect(typeof db).toBe('object')
    // The carrier is captured NOW (before the body's first await), not lazily per read.
    expect(acquireCarrier).toHaveBeenCalledTimes(1)
  })

  it('plain fields forward untouched (observable-equivalence)', () => {
    const base = { tag: 'base', select: () => ({}) }
    const db = reactive(base) as unknown as { tag: string }
    expect(db.tag).toBe('base')
  })

  it('select() routes through the read-capture engine with this db’s own base builder + the carrier', () => {
    const baseBuilder = { from: () => ({}) }
    const base = { select: vi.fn(() => baseBuilder) }
    const db = reactive(base) as unknown as { select: () => unknown }

    const built = db.select()
    expect(base.select).toHaveBeenCalledTimes(1)
    expect(wrapLiveSelect).toHaveBeenCalledTimes(1)
    const [passedBuilder, carrier] = vi.mocked(wrapLiveSelect).mock.calls[0]!
    expect(passedBuilder).toBe(baseBuilder) // the engine wraps THIS db's own base builder
    expect(carrier).toEqual({ __dbLiveCarrier: true })
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

  it('writes route through the mutation seam (plain today); the seam sees op + carrier', () => {
    const insert = vi.fn()
    const base = { select: () => ({}), insert }
    const db = reactive(base) as unknown as { insert: unknown }
    void db.insert // property access drives the proxy get
    expect(captureMutation).toHaveBeenCalledWith('insert', expect.any(Function), { __dbLiveCarrier: true })
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
