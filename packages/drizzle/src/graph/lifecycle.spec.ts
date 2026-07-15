// T5.I1 — model-based lifecycle traces. A seeded walk enumerates trigger sequences generated
// ONLY from the §3.C-TT transition table and drives a real liveGraph, asserting at every step:
// the real state equals the legal target (no illegal transition is reachable), ≤1 fire per graph
// per batch, a warming/coarse graph never silently drops a substantive change and never claims
// exactness, and retired/destroyed graphs are inert (never rehydrated). Separate cases pin the
// unreachable edges (coarse→warming via resync, drift→re-warm). Deterministic — a fixed PRNG +
// deferred scans, never timers.

import { describe, expect, it } from 'vitest'
import type { SeedDescriptor, StatefulGraph } from '../compile/compile.js'
import type { Row } from '../compile/rowSpace.js'
import type { HydrationExecutor } from './hydrate.js'
import { type LiveGraph, createLiveGraph } from './liveGraph.js'

const flush = (): Promise<void> => new Promise((resolve) => setImmediate(resolve))
async function settle(graph: LiveGraph): Promise<void> {
  for (let i = 0; i < 1000 && graph.state() === 'warming'; i++) await flush()
}

type Deferred<T> = { promise: Promise<T>; resolve: (value: T) => void }
function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((res) => (resolve = res))
  return { promise, resolve }
}

function prng(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) | 0
    let t = Math.imul(state ^ (state >>> 15), 1 | state)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

function fakeSeed(): SeedDescriptor {
  return {
    inputId: 'users',
    table: 'users',
    alias: 'users',
    primaryKey: ['id'],
    columns: '*',
    residual: { kind: 'true' },
    shadowNeed: true,
  }
}
function statefulFake(): StatefulGraph {
  const noFire = { data: false, dirty: false, invalidated: false }
  return {
    seeds: [fakeSeed()],
    seedInput() {},
    flushSeed() {},
    feedInput() {},
    runBatch: () => noFire,
    apply: () => noFire,
  }
}

const MAX_STATE_ROWS = 100
const manyRows = (n: number): Row[] => Array.from({ length: n }, (_, i) => ({ id: 1000 + i }))
const manyChanges = (n: number) => manyRows(n).map((row) => ({ table: 'users', kind: 'insert' as const, new: row }))

function makeDriven(): { graph: LiveGraph; resolveScan: (rows: Row[]) => void } {
  const calls: Deferred<Row[]>[] = []
  const executor: HydrationExecutor = {
    scan: () => {
      const d = deferred<Row[]>()
      calls.push(d)
      return d.promise
    },
    fetchByKeys: async () => [],
  }
  const graph = createLiveGraph({
    kind: 'stateful',
    instanceKey: 'k',
    tables: ['users'],
    instantiate: () => statefulFake(),
    executor,
    maxStateRows: MAX_STATE_ROWS,
  })
  return { graph, resolveScan: (rows) => calls[calls.length - 1]?.resolve(rows) }
}

type S = 'warming' | 'live' | 'coarse' | 'retired' | 'destroyed'
type Edge = { trigger: string; to: S }

// §3.C-TT — the complete legal transition set (the walk only ever fires triggers from here).
const TT: Record<S, Edge[]> = {
  warming: [
    { trigger: 'warm-complete', to: 'live' },
    { trigger: 'demote-warming', to: 'coarse' },
    { trigger: 'drift', to: 'retired' },
    { trigger: 'destroy', to: 'destroyed' },
  ],
  live: [
    { trigger: 'gap', to: 'warming' },
    { trigger: 'state-limit', to: 'coarse' },
    { trigger: 'drift', to: 'retired' },
    { trigger: 'destroy', to: 'destroyed' },
  ],
  coarse: [
    { trigger: 'drift', to: 'retired' }, // coarse is a sink: only drift/destroy leave it
    { trigger: 'destroy', to: 'destroyed' },
  ],
  retired: [],
  destroyed: [],
}

async function drive(driven: { graph: LiveGraph; resolveScan: (rows: Row[]) => void }, trigger: string): Promise<void> {
  const g = driven.graph
  if (trigger === 'warm-complete') {
    driven.resolveScan([{ id: 1 }])
    await settle(g)
  } else if (trigger === 'demote-warming') {
    driven.resolveScan(manyRows(MAX_STATE_ROWS + 50)) // scan overflows the state bound
    await settle(g)
  } else if (trigger === 'gap') {
    g.rewarm()
  } else if (trigger === 'state-limit') {
    g.apply(manyChanges(MAX_STATE_ROWS + 50)) // live shadow overflows → demote
  } else if (trigger === 'drift') {
    g.retire()
  } else if (trigger === 'destroy') {
    g.destroy()
  }
}

/** Invariants that must hold in EVERY reachable state before the next transition. */
function probe(graph: LiveGraph, model: S): void {
  const before = graph.invalidationSeq()
  const beforeExact = graph.inspect().counters.exactFires
  const out = graph.apply([{ table: 'users', kind: 'insert', new: { id: 999_999 } }]) // fixed id → adds ≤1 shadow row
  const delta = graph.invalidationSeq() - before
  expect(delta).toBeLessThanOrEqual(1) // ≤1 fire per graph per batch
  if (model === 'warming' || model === 'coarse') {
    expect(out.invalidated).toBe(true) // never a silent drop (coarse over-fire is licensed)
    expect(graph.inspect().counters.exactFires).toBe(beforeExact) // a coarse/warming graph never claims exactness
  }
  if (model === 'retired' || model === 'destroyed') {
    expect(out.invalidated).toBe(false) // terminal — inert, never rehydrated
    expect(delta).toBe(0)
  }
}

describe('T5.I1 — model-based lifecycle invariants (legal transitions only)', () => {
  it('a seeded walk over §3.C-TT keeps the real graph in the modeled state and holds every step invariant', async () => {
    const rng = prng(0x51c5)
    for (let trace = 0; trace < 25; trace++) {
      const driven = makeDriven()
      let model: S = 'warming'
      expect(driven.graph.state()).toBe('warming') // born stateful → warming
      for (let step = 0; step < 14; step++) {
        probe(driven.graph, model)
        const legal: Edge[] = TT[model]
        if (legal.length === 0) break // terminal
        const edge: Edge = legal[Math.floor(rng() * legal.length)]!
        await drive(driven, edge.trigger)
        model = edge.to
        expect(driven.graph.state()).toBe(model) // no transition outside §3.C-TT is reachable
      }
    }
  })

  it('illegal transitions are unreachable: coarse and drift are sinks, terminals are inert', async () => {
    // coarse is a sink — a resync request does NOT re-warm it (no coarse→warming)
    const c = makeDriven()
    c.resolveScan(manyRows(MAX_STATE_ROWS + 50))
    await settle(c.graph)
    expect(c.graph.state()).toBe('coarse')
    c.graph.rewarm()
    expect(c.graph.state()).toBe('coarse')

    // drift retires terminally — there is NO drift→re-warm path
    const r = makeDriven()
    r.resolveScan([{ id: 1 }])
    await settle(r.graph)
    r.graph.retire()
    expect(r.graph.state()).toBe('retired')
    r.graph.rewarm()
    expect(r.graph.state()).toBe('retired')
    expect(r.graph.apply([{ table: 'users', kind: 'insert', new: { id: 1 } }]).invalidated).toBe(false) // never rehydrated

    // destroyed is terminal and inert
    const d = makeDriven()
    d.graph.destroy()
    expect(d.graph.apply([{ table: 'users', kind: 'insert', new: { id: 1 } }]).invalidated).toBe(false)
  })
})
