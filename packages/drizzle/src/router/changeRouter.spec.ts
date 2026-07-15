// T5.G1–G4 — the positioned change router. `ingest` is driven with synthetic predecessor-
// linked batches (no live source this ticket): each accepted batch applies once, synchronously,
// in source order and never merged across batches (G1); a multi-table batch ticks a join graph
// once and notifies ≤1 per graph AND per attached identity, and a throwing apply fails closed to
// a coarse resync (G2); predecessor linkage dedups duplicates/stale and treats an LSN jump as
// non-gap (G3); a real gap resyncs coarsely and the cursor advances only after successful
// routing (G4). Deterministic — ingest is synchronous.

import { describe, expect, it } from 'vitest'
import type { ApplyOutcome } from '../graph/liveGraph.js'
import { type RoutableGraph, createRouter } from './changeRouter.js'
import type { ChangeBatch, TableChange } from './events.js'

type FakeGraph = { graph: RoutableGraph; applyLog: TableChange[][]; resyncCount: () => number }
function fakeGraph(
  tables: string[],
  keys: string[],
  opts: { throwOnApply?: boolean; invalidated?: boolean } = {},
): FakeGraph {
  const applyLog: TableChange[][] = []
  let resyncs = 0
  const graph: RoutableGraph = {
    tables,
    apply: (changes): ApplyOutcome => {
      applyLog.push(changes)
      if (opts.throwOnApply) throw new Error('boom')
      return { invalidated: opts.invalidated ?? true }
    },
    notifyKeys: () => keys,
    resync: () => {
      resyncs++
    },
  }
  return { graph, applyLog, resyncCount: () => resyncs }
}

const ins = (table: string, id: number): TableChange => ({ table, kind: 'insert', new: { id } })
const batch = (over: Partial<ChangeBatch> & { position: number; changes: TableChange[] }): ChangeBatch => ({
  sourceId: over.sourceId ?? 's',
  position: over.position,
  predecessor: over.predecessor ?? null,
  heartbeat: over.heartbeat,
  changes: over.changes,
})

// ── G1 ──────────────────────────────────────────────────────────────

describe('T5.G1 — apply once, synchronously, in source order', () => {
  it('an accepted batch applies exactly once; back-to-back accepted batches apply twice, in order (never merged)', () => {
    const router = createRouter({ notify: () => {} })
    const g = fakeGraph(['users'], ['I'])
    router.register(g.graph)
    router.ingest(batch({ position: 1, predecessor: null, changes: [ins('users', 1)] }))
    expect(g.applyLog.length).toBe(1) // applied synchronously, once
    router.ingest(batch({ position: 2, predecessor: 1, changes: [ins('users', 2)] }))
    expect(g.applyLog.length).toBe(2) // twice — application never merged across batches
    expect(g.applyLog[0]![0]!.new).toEqual({ id: 1 })
    expect(g.applyLog[1]![0]!.new).toEqual({ id: 2 })
  })

  it('the slice of a multi-change batch preserves source order', () => {
    const router = createRouter({ notify: () => {} })
    const g = fakeGraph(['users'], ['I'])
    router.register(g.graph)
    router.ingest(batch({ position: 1, changes: [ins('users', 10), ins('users', 20), ins('users', 30)] }))
    expect(g.applyLog[0]!.map((c) => c.new)).toEqual([{ id: 10 }, { id: 20 }, { id: 30 }])
  })
})

// ── G2 ──────────────────────────────────────────────────────────────

describe('T5.G2 — notify ≤1 per graph AND per identity; throwing apply fails closed', () => {
  it('a multi-table batch ticks a join graph once and notifies its identity once', () => {
    const notified: string[] = []
    const router = createRouter({ notify: (k) => notified.push(k) })
    const join = fakeGraph(['users', 'teams'], ['J'])
    router.register(join.graph)
    router.ingest(batch({ position: 1, changes: [ins('users', 1), ins('teams', 5)] }))
    expect(join.applyLog.length).toBe(1) // applied once with the union slice
    expect(join.applyLog[0]!.length).toBe(2)
    expect(notified).toEqual(['J']) // ≤1 notify per graph
  })

  it('an identity reached through two per-table sentinels for a two-table batch gets ONE coarse notification', () => {
    const notified: string[] = []
    const router = createRouter({ notify: (k) => notified.push(k) })
    const sUsers = fakeGraph(['users'], ['M'])
    const sTeams = fakeGraph(['teams'], ['M'])
    router.register(sUsers.graph)
    router.register(sTeams.graph)
    router.ingest(batch({ position: 1, changes: [ins('users', 1), ins('teams', 5)] }))
    expect(sUsers.applyLog.length).toBe(1)
    expect(sTeams.applyLog.length).toBe(1)
    expect(notified).toEqual(['M']) // deduped to one identity notification, not two
  })

  it('a throwing apply fails closed to a coarse resync and does NOT advance the cursor', () => {
    const notified: string[] = []
    const router = createRouter({ notify: (k) => notified.push(k) })
    const bad = fakeGraph(['users'], ['B'], { throwOnApply: true })
    router.register(bad.graph)
    router.ingest(batch({ position: 1, changes: [ins('users', 1)] }))
    expect(bad.resyncCount()).toBe(1) // fail closed → coarse resync
    expect(notified).toEqual(['B'])
    expect(router.inspect().counters.accepted).toBe(0) // cursor not advanced on a throwing apply
    expect(router.inspect().counters.degraded).toBe(1)
  })
})

// ── G3 ──────────────────────────────────────────────────────────────

describe('T5.G3 — predecessor-linked dedup', () => {
  it('accepts iff predecessor === lastAccepted; an LSN jump is not a gap; duplicates drop; restart is a fresh epoch', () => {
    const router = createRouter({ notify: () => {} })
    const g = fakeGraph(['users'], ['I'])
    router.register(g.graph)

    router.ingest(batch({ position: 5, predecessor: null, changes: [ins('users', 1)] })) // first frame sets the cursor
    expect(g.applyLog.length).toBe(1)

    router.ingest(batch({ position: 100, predecessor: 5, changes: [ins('users', 2)] })) // LSN jump 5→100, still linked
    expect(g.applyLog.length).toBe(2)
    expect(router.inspect().counters.gaps).toBe(0) // a linked non-adjacent position is NOT a gap

    router.ingest(batch({ position: 100, predecessor: 5, changes: [ins('users', 2)] })) // duplicate (position ≤ last)
    expect(g.applyLog.length).toBe(2)
    expect(router.inspect().counters.duplicates).toBe(1)

    router.ingest(batch({ sourceId: 's@epoch2', position: 1, predecessor: null, changes: [ins('users', 3)] })) // restart
    expect(g.applyLog.length).toBe(3) // fresh epoch → first frame accepted
  })
})

// ── G4 ──────────────────────────────────────────────────────────────

describe('T5.G4 — gap → coarse resync; cursor advances only after success', () => {
  it('a gapped frame resyncs coarsely (invalidate once + re-warm), is not applied, and advances the cursor', () => {
    const notified: string[] = []
    const router = createRouter({ notify: (k) => notified.push(k) })
    const g = fakeGraph(['users'], ['I'])
    router.register(g.graph)

    router.ingest(batch({ position: 1, predecessor: null, changes: [ins('users', 1)] }))
    expect(notified).toEqual(['I'])

    router.ingest(batch({ position: 7, predecessor: 5, changes: [ins('users', 2)] })) // predecessor 5 ≠ lastAccepted 1 → gap
    expect(router.inspect().counters.gaps).toBe(1)
    expect(g.resyncCount()).toBe(1) // re-warm stateful graphs
    expect(g.applyLog.length).toBe(1) // the gapped batch is NOT applied (coarse only)
    expect(notified).toEqual(['I', 'I']) // one coarse invalidation for the gap

    router.ingest(batch({ position: 8, predecessor: 7, changes: [ins('users', 3)] })) // linked to the advanced cursor
    expect(g.applyLog.length).toBe(2) // accepted — cursor had advanced to the gap position
  })
})
