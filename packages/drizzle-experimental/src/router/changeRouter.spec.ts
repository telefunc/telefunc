// The change router over ordered, atomic TableChange batches. One commit = one graph tick:
// each batch applies once, synchronously, in source order, never merged (G1); a multi-table batch
// ticks a join graph once and notifies ≤1 per graph AND per attached identity, and a throwing apply
// is ISOLATED — its identity is coarse-notified while the other graphs in the batch are unaffected
// (G2). An in-batch `kind:'coarse'` marker demotes every graph on that table to coarse and never
// feeds it a row — an image-less mutation can't corrupt precise state (G3). Transport reliability
// (ordering, gap/duplicate handling, positions) lives with whoever emits the batches, not here.
// Deterministic — ingest is synchronous.

import { describe, expect, it, vi } from 'vitest'
import type { ApplyOutcome } from '../graph/liveGraph.js'
import { relationIdOf } from '../ir/relation.js'
import { type RoutableGraph, createRouter } from './changeRouter.js'
import type { ChangeBatch, TableChange } from './events.js'

type FakeGraph = { graph: RoutableGraph; applyLog: TableChange[][]; reseedCalls: { n: number } }
function fakeGraph(
  tables: string[],
  key: string,
  opts: { throwOnApply?: boolean; throwOnce?: boolean; invalidated?: boolean } = {},
): FakeGraph {
  const applyLog: TableChange[][] = []
  const reseedCalls = { n: 0 }
  let faulted = false
  const graph: RoutableGraph = {
    tables,
    apply: (changes): ApplyOutcome => {
      applyLog.push(changes)
      if (faulted) return { invalidated: true } // after a fault the graph is permanently coarse
      if (opts.throwOnApply || (opts.throwOnce && applyLog.length === 1)) throw new Error('boom')
      return { invalidated: opts.invalidated ?? true }
    },
    notifyKey: () => key,
    fault: () => {
      faulted = true
    },
    reseed: () => {
      reseedCalls.n++
      faulted = true // an explicit coarse event permanently demotes, exactly like fault
    },
  }
  return { graph, applyLog, reseedCalls }
}

const ins = (table: string, id: number): TableChange => ({ table, kind: 'insert', new: { id } })
const coarse = (table: string): TableChange => ({ table, kind: 'coarse' })
const batch = (changes: TableChange[]): ChangeBatch => ({ changes })

// ── G1 ──────────────────────────────────────────────────────────────

describe('apply once, synchronously, in order', () => {
  it('a batch applies exactly once; back-to-back batches apply twice, in order (never merged)', () => {
    const router = createRouter({ notify: () => {} })
    const g = fakeGraph(['users'], 'I')
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
    const g = fakeGraph(['users'], 'I')
    router.register(g.graph)
    router.ingest(batch([ins('users', 10), ins('users', 20), ins('users', 30)]))
    expect(g.applyLog[0]!.map((c) => c.new)).toEqual([{ id: 10 }, { id: 20 }, { id: 30 }])
  })
})

// ── G2 ──────────────────────────────────────────────────────────────

describe('notify ≤1 per graph AND per identity; a throwing apply is isolated', () => {
  it('a multi-table batch ticks a join graph once and notifies its identity once', () => {
    const notified: string[] = []
    const router = createRouter({ notify: (k) => notified.push(k) })
    const join = fakeGraph(['users', 'teams'], 'J')
    router.register(join.graph)
    router.ingest(batch([ins('users', 1), ins('teams', 5)]))
    expect(join.applyLog.length).toBe(1) // applied once with the union slice
    expect(join.applyLog[0]!.length).toBe(2)
    expect(notified).toEqual(['J']) // ≤1 notify per graph
  })

  it('two graphs sharing one identity key get ONE notification for a multi-table batch', () => {
    const notified: string[] = []
    const router = createRouter({ notify: (k) => notified.push(k) })
    const a = fakeGraph(['users'], 'M')
    const b = fakeGraph(['teams'], 'M')
    router.register(a.graph)
    router.register(b.graph)
    router.ingest(batch([ins('users', 1), ins('teams', 5)]))
    expect(a.applyLog.length).toBe(1)
    expect(b.applyLog.length).toBe(1)
    expect(notified).toEqual(['M']) // deduped to one identity notification, not two
  })

  it('a throwing apply is isolated AND surfaced: identity coarse-notified, error logged, co-batched graph applies', () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const notified: string[] = []
    const router = createRouter({ notify: (k) => notified.push(k) })
    const bad = fakeGraph(['users'], 'B', { throwOnApply: true })
    const good = fakeGraph(['users'], 'G')
    router.register(bad.graph)
    router.register(good.graph)
    router.ingest(batch([ins('users', 1)]))
    expect(bad.applyLog.length).toBe(1) // attempted
    expect(good.applyLog.length).toBe(1) // NOT skipped by the other graph's throw
    expect(notified.sort()).toEqual(['B', 'G']) // the thrower is coarse-notified; the healthy graph fires normally
    expect(errorSpy).toHaveBeenCalledTimes(1) // SURFACED: structured log, never swallowed
    errorSpy.mockRestore()
  })

  it('a throwing apply PERMANENTLY demotes the graph: a LATER SQL-changing batch still fires (no post-fault miss)', () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const notified: string[] = []
    const router = createRouter({ notify: (k) => notified.push(k) })
    // Throws on the FIRST apply, then (were it not faulted) would return invalidated:false — i.e. a
    // corrupt-precise graph that would MISS a later change. The permanent demote makes it coarse-fire.
    const g = fakeGraph(['users'], 'X', { throwOnce: true, invalidated: false })
    router.register(g.graph)
    router.ingest(batch([ins('users', 1)])) // first apply throws → router faults it to coarse
    notified.length = 0
    router.ingest(batch([ins('users', 2)])) // a later SQL-changing batch
    expect(g.applyLog.length).toBe(2) // applied again (not skipped)
    expect(notified).toEqual(['X']) // STILL fires via the permanent demote (coarse over-fire) — no miss
    errorSpy.mockRestore()
  })
})

// ── G3 — in-batch coarse marker ─────────────────────────────────────

describe('an in-batch coarse marker demotes the graph, never feeds it a row', () => {
  it('a coarse marker calls reseed (not apply) and notifies the identity', () => {
    const notified: string[] = []
    const router = createRouter({ notify: (k) => notified.push(k) })
    const g = fakeGraph(['users'], 'C')
    router.register(g.graph)
    router.ingest(batch([coarse('users')]))
    expect(g.reseedCalls.n).toBe(1) // routed to the labelled reseed()
    expect(g.applyLog.length).toBe(0) // NEVER fed the image-less change — no fabrication reaches apply
    expect(notified).toEqual(['C']) // coarse-notified so its subscribers re-read
  })

  it('a mixed batch (coarse + precise for the same graph) demotes once and never applies the precise slice', () => {
    const router = createRouter({ notify: () => {} })
    const g = fakeGraph(['users'], 'C')
    router.register(g.graph)
    router.ingest(batch([ins('users', 1), coarse('users')])) // one commit: precise + image-less together
    expect(g.reseedCalls.n).toBe(1)
    expect(g.applyLog.length).toBe(0) // atomic single tick: the precise change is moot once the graph is coarse
  })

  it('a coarse marker for one table does not demote a graph that does not watch it', () => {
    const router = createRouter({ notify: () => {} })
    const onUsers = fakeGraph(['users'], 'U')
    const onTeams = fakeGraph(['teams'], 'T')
    router.register(onUsers.graph)
    router.register(onTeams.graph)
    router.ingest(batch([coarse('users'), ins('teams', 5)]))
    expect(onUsers.reseedCalls.n).toBe(1) // watches users → reseeded
    expect(onUsers.applyLog.length).toBe(0)
    expect(onTeams.reseedCalls.n).toBe(0) // watches only teams → unaffected by the users coarse marker
    expect(onTeams.applyLog.length).toBe(1) // its precise change applies normally
  })
})

// The one configuration that can silently MISS an invalidation: one physical table declared both with and
// without a schema, read through one declaration and written through the other. The router cannot route
// them together (whether they are the same relation depends on search_path, and on a pooled connection that
// is unprovable), but it can refuse to let the miss be silent.
describe('a relation declared both with and without a schema is called out, not silently missed', () => {
  const qualified = relationIdOf({ name: 'users', schema: 'app' })
  const unqualified = relationIdOf({ name: 'users' })

  it('warns when both forms of one name appear, and names the relation', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const router = createRouter({ notify: () => {} })
    router.register(fakeGraph([unqualified], 'k1').graph) // the READ declares it bare…
    expect(warn).not.toHaveBeenCalled() // …nothing ambiguous yet
    router.ingest(batch([ins(qualified, 1)])) // …the WRITE arrives schema-qualified
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn.mock.calls[0]![0]).toMatch(/"users" is declared both with and without a schema/)
    warn.mockRestore()
  })

  it('warns ONCE per name — a standing misconfiguration, not a per-batch event', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const router = createRouter({ notify: () => {} })
    router.register(fakeGraph([unqualified], 'k1').graph)
    router.ingest(batch([ins(qualified, 1)]))
    router.ingest(batch([ins(qualified, 2)]))
    router.ingest(batch([ins(unqualified, 3)]))
    expect(warn).toHaveBeenCalledTimes(1)
    warn.mockRestore()
  })

  it('stays quiet for two genuinely different schema-qualified relations of the same name', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const router = createRouter({ notify: () => {} })
    router.register(fakeGraph([relationIdOf({ name: 'users', schema: 'a' })], 'k1').graph)
    router.ingest(batch([ins(relationIdOf({ name: 'users', schema: 'b' }), 1)]))
    // Both are qualified, so neither is ambiguous — they are simply different tables, and the qualified
    // identity keeping them apart is the whole point. A warning here would be noise on correct usage.
    expect(warn).not.toHaveBeenCalled()
    warn.mockRestore()
  })

  it('does not route the two declarations together — the identities stay separate', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const router = createRouter({ notify: () => {} })
    const bare = fakeGraph([unqualified], 'k1')
    router.register(bare.graph)
    router.ingest(batch([ins(qualified, 1)]))
    // Diagnosing the ambiguity must not quietly become "fix" it: linking them would invalidate a query on
    // one physical relation because an unrelated one in another schema changed.
    expect(bare.applyLog.length).toBe(0)
    warn.mockRestore()
  })
})
