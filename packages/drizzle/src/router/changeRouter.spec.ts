// T5.G — the change router over ordered, atomic TableChange batches. One commit = one graph tick:
// each batch applies once, synchronously, in source order, never merged (G1); a multi-table batch
// ticks a join graph once and notifies ≤1 per graph AND per attached identity, and a throwing apply
// is ISOLATED — its identity is coarse-notified while the other graphs in the batch are unaffected
// (G2). Transport reliability (ordering, gap/duplicate handling, positions) lives INSIDE the
// ChangeSource, not here. Deterministic — ingest is synchronous.

import { describe, expect, it, vi } from 'vitest'
import type { ApplyOutcome } from '../graph/liveGraph.js'
import { type RoutableGraph, createRouter } from './changeRouter.js'
import type { ChangeBatch, TableChange } from './events.js'

type FakeGraph = { graph: RoutableGraph; applyLog: TableChange[][] }
function fakeGraph(
  tables: string[],
  keys: string[],
  opts: { throwOnApply?: boolean; invalidated?: boolean } = {},
): FakeGraph {
  const applyLog: TableChange[][] = []
  const graph: RoutableGraph = {
    tables,
    apply: (changes): ApplyOutcome => {
      applyLog.push(changes)
      if (opts.throwOnApply) throw new Error('boom')
      return { invalidated: opts.invalidated ?? true }
    },
    notifyKeys: () => keys,
  }
  return { graph, applyLog }
}

const ins = (table: string, id: number): TableChange => ({ table, kind: 'insert', new: { id } })
const batch = (changes: TableChange[]): ChangeBatch => ({ changes })

// ── G1 ──────────────────────────────────────────────────────────────

describe('T5.G1 — apply once, synchronously, in order', () => {
  it('a batch applies exactly once; back-to-back batches apply twice, in order (never merged)', () => {
    const router = createRouter({ notify: () => {} })
    const g = fakeGraph(['users'], ['I'])
    router.register(g.graph)
    router.ingest(batch([ins('users', 1)]))
    expect(g.applyLog.length).toBe(1) // applied synchronously, once
    router.ingest(batch([ins('users', 2)]))
    expect(g.applyLog.length).toBe(2) // twice — application never merged across batches
    expect(g.applyLog[0]![0]!.new).toEqual({ id: 1 })
    expect(g.applyLog[1]![0]!.new).toEqual({ id: 2 })
  })

  it('the slice of a multi-change batch preserves source order', () => {
    const router = createRouter({ notify: () => {} })
    const g = fakeGraph(['users'], ['I'])
    router.register(g.graph)
    router.ingest(batch([ins('users', 10), ins('users', 20), ins('users', 30)]))
    expect(g.applyLog[0]!.map((c) => c.new)).toEqual([{ id: 10 }, { id: 20 }, { id: 30 }])
  })
})

// ── G2 ──────────────────────────────────────────────────────────────

describe('T5.G2 — notify ≤1 per graph AND per identity; a throwing apply is isolated', () => {
  it('a multi-table batch ticks a join graph once and notifies its identity once', () => {
    const notified: string[] = []
    const router = createRouter({ notify: (k) => notified.push(k) })
    const join = fakeGraph(['users', 'teams'], ['J'])
    router.register(join.graph)
    router.ingest(batch([ins('users', 1), ins('teams', 5)]))
    expect(join.applyLog.length).toBe(1) // applied once with the union slice
    expect(join.applyLog[0]!.length).toBe(2)
    expect(notified).toEqual(['J']) // ≤1 notify per graph
  })

  it('two graphs sharing one identity key get ONE notification for a multi-table batch', () => {
    const notified: string[] = []
    const router = createRouter({ notify: (k) => notified.push(k) })
    const a = fakeGraph(['users'], ['M'])
    const b = fakeGraph(['teams'], ['M'])
    router.register(a.graph)
    router.register(b.graph)
    router.ingest(batch([ins('users', 1), ins('teams', 5)]))
    expect(a.applyLog.length).toBe(1)
    expect(b.applyLog.length).toBe(1)
    expect(notified).toEqual(['M']) // deduped to one identity notification, not two
  })

  it('a throwing apply is isolated AND surfaced: identity coarse-notified, error counted + logged, co-batched graph applies', () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const notified: string[] = []
    const router = createRouter({ notify: (k) => notified.push(k) })
    const bad = fakeGraph(['users'], ['B'], { throwOnApply: true })
    const good = fakeGraph(['users'], ['G'])
    router.register(bad.graph)
    router.register(good.graph)
    router.ingest(batch([ins('users', 1)]))
    expect(bad.applyLog.length).toBe(1) // attempted
    expect(good.applyLog.length).toBe(1) // NOT skipped by the other graph's throw
    expect(notified.sort()).toEqual(['B', 'G']) // the thrower is coarse-notified; the healthy graph fires normally
    expect(router.inspect().applyErrors).toBe(1) // SURFACED: counted, never swallowed
    expect(errorSpy).toHaveBeenCalledTimes(1) // SURFACED: structured log
    errorSpy.mockRestore()
  })
})
