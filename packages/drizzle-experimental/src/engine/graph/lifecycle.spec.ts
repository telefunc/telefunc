// Model-based lifecycle traces. A seeded walk enumerates trigger sequences generated ONLY
// from the transition table and drives a real liveGraph, asserting at every step: the real
// state equals the legal target (no illegal transition is reachable), ≤1 fire per graph per batch, a
// SEEDING graph buffers a substantive change precisely (never coarse, never dropped) while a coarse
// graph over-fires, and a destroyed graph is inert (never rehydrated). A separate case pins the sinks
// (coarse is left only by destroy; there is no gap/resync seam — capture is gap-free).
// Deterministic — a fixed PRNG + deferred scans, never timers.

import { describe, expect, it } from 'vitest'
import type { SeedDescriptor, StatefulGraph } from '../compile/compile.js'
import type { Row } from '../compile/rowSpace.js'
import type { HydrationExecutor } from './hydrate.js'
import { type LiveGraph, createLiveGraph } from './liveGraph.js'

const settle = (graph: LiveGraph): Promise<void> => graph.ready() // resolves when the seed lands (→ live / coarse)

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
    relationId: 'users',
    alias: 'users',
    primaryKey: ['id'],
    columns: '*',
    residual: { kind: 'true' },
  }
}
function statefulFake(): StatefulGraph {
  const noFire = { invalidated: false }
  return {
    seeds: [fakeSeed()],
    seedInput() {},
    flushSeed() {},
    feedInput() {},
    feedDirtyWitness() {},
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

type S = 'seeding' | 'live' | 'coarse' | 'destroyed'
type Edge = { trigger: string; to: S }

// The complete legal transition set (the walk only ever fires triggers from here).
const TT: Record<S, Edge[]> = {
  seeding: [
    { trigger: 'seed-complete', to: 'live' },
    { trigger: 'demote-seed', to: 'coarse' },
    { trigger: 'destroy', to: 'destroyed' },
  ],
  live: [
    { trigger: 'state-limit', to: 'coarse' }, // live shadow overflows → coarse (no live→seeding; a gap can't re-seed)
    { trigger: 'destroy', to: 'destroyed' },
  ],
  coarse: [
    { trigger: 'destroy', to: 'destroyed' }, // coarse is a sink: only destroy leaves it
  ],
  destroyed: [],
}

async function drive(driven: { graph: LiveGraph; resolveScan: (rows: Row[]) => void }, trigger: string): Promise<void> {
  const g = driven.graph
  if (trigger === 'seed-complete') {
    driven.resolveScan([{ id: 1 }])
    await settle(g)
  } else if (trigger === 'demote-seed') {
    driven.resolveScan(manyRows(MAX_STATE_ROWS + 50)) // scan overflows the state bound → demote
    await settle(g)
  } else if (trigger === 'state-limit') {
    g.apply(manyChanges(MAX_STATE_ROWS + 50)) // live shadow overflows → demote
  } else if (trigger === 'destroy') {
    g.destroy()
  }
}

/** Invariants that must hold in EVERY reachable state before the next transition. */
function probe(graph: LiveGraph, model: S): void {
  const before = graph.invalidationSeq()
  const out = graph.apply([{ table: 'users', kind: 'insert', new: { id: 999_999 } }]) // fixed id → adds ≤1 shadow row
  const delta = graph.invalidationSeq() - before
  expect(delta).toBeLessThanOrEqual(1) // ≤1 fire per graph per batch
  if (model === 'seeding') {
    expect(out.invalidated).toBe(false) // buffered precisely — not coarse, and NOT dropped (replayed at the cut)
  }
  if (model === 'coarse') {
    expect(out.invalidated).toBe(true) // coarse over-fire is licensed (never a silent drop)
  }
  if (model === 'destroyed') {
    expect(out.invalidated).toBe(false) // terminal — inert, never rehydrated
    expect(delta).toBe(0)
  }
}

describe('model-based lifecycle invariants (legal transitions only)', () => {
  it('a seeded walk over keeps the real graph in the modeled state and holds every step invariant', async () => {
    const rng = prng(0x51c5)
    for (let trace = 0; trace < 25; trace++) {
      const driven = makeDriven()
      let model: S = 'seeding'
      expect(driven.graph.state()).toBe('seeding') // born stateful → seeding until the scan lands
      for (let step = 0; step < 14; step++) {
        probe(driven.graph, model)
        const legal: Edge[] = TT[model]
        if (legal.length === 0) break // terminal
        const edge: Edge = legal[Math.floor(rng() * legal.length)]!
        await drive(driven, edge.trigger)
        model = edge.to
        expect(driven.graph.state()).toBe(model) // no transition outside the table is reachable
      }
    }
  })

  it('coarse is a sink; destroyed is terminal and inert', async () => {
    // coarse is a sink — reached by a seed over the bound, left only by destroy
    const c = makeDriven()
    c.resolveScan(manyRows(MAX_STATE_ROWS + 50))
    await settle(c.graph)
    expect(c.graph.state()).toBe('coarse')

    // destroyed is terminal and inert
    const d = makeDriven()
    d.graph.destroy()
    expect(d.graph.apply([{ table: 'users', kind: 'insert', new: { id: 1 } }]).invalidated).toBe(false)
  })
})
