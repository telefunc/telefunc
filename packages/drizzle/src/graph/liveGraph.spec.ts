// T5.B2a / C2 / E1 / G2 / I2 — the live-graph state machine. Real compiled join graphs are
// warmed through the real drain loop (a controllable executor), then driven live: an old-inline
// retraction resolves without a shadow consult (B2a), warming coarse-invalidates AND journals
// every event (C2), RLS-gated stateful graphs are born coarse (E1), a graph fires at most once
// per batch (G2), and inspect() is bounded with one reason per transition (I2). Deterministic —
// deferred/immediate promises drive warming, never timers.

import { eq } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { type SeedDescriptor, type StatefulGraph, compileQuery } from '../compile/compile.js'
import type { Row } from '../compile/rowSpace.js'
import { extractQueryShape } from '../extract/queryShape.js'
import type { HydrationExecutor } from './hydrate.js'
import { type LiveGraph, type LiveGraphSpec, createLiveGraph } from './liveGraph.js'
import { createRegistry } from './registry.js'

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  score: pg.integer('score'),
})
const teams = pg.pgTable('teams', { id: pg.integer('id').primaryKey(), region: pg.text('region') })
const qb = new pg.QueryBuilder()

const flush = (): Promise<void> => new Promise((resolve) => setImmediate(resolve))
async function settle(graph: LiveGraph): Promise<void> {
  for (let i = 0; i < 1000 && graph.state() === 'warming'; i++) await flush()
}

const neverExecutor = (): HydrationExecutor => ({
  scan: () => new Promise<Row[]>(() => {}),
  fetchByKeys: () => Promise.resolve([]),
})

function fakeSeed(inputId: string, table: string, primaryKey: string[] = ['id']): SeedDescriptor {
  return { inputId, table, alias: inputId, primaryKey, columns: '*', residual: { kind: 'true' }, shadowNeed: true }
}
function statefulFake(seeds: SeedDescriptor[]): StatefulGraph {
  const noFire = { data: false, dirty: false, invalidated: false }
  return { seeds, seedInput() {}, flushSeed() {}, feedInput() {}, runBatch: () => noFire, apply: () => noFire }
}

// ── a real compiled users ⋈ teams graph, warmed through the drain loop ──

function joinExecutor(usersRows: Row[], teamsRows: Row[]): HydrationExecutor {
  return {
    scan: async (descriptor) => (descriptor.table === 'users' ? usersRows : teamsRows),
    fetchByKeys: async () => [],
  }
}
async function warmedJoin(usersRows: Row[], teamsRows: Row[]): Promise<LiveGraph> {
  const build = () => qb.select().from(users).innerJoin(teams, eq(teams.id, users.teamId))
  const spec: LiveGraphSpec = {
    kind: 'stateful',
    instanceKey: 'join',
    tables: ['users', 'teams'],
    instantiate: () => compileQuery(extractQueryShape(build(), { dialect: 'pg' })).instantiate() as StatefulGraph,
    executor: joinExecutor(usersRows, teamsRows),
    maxStateRows: 1e9,
  }
  const graph = createLiveGraph(spec)
  await settle(graph)
  expect(graph.state()).toBe('live')
  return graph
}

// ── B2a — old-inline retraction, no shadow consult ──────────────────

describe('T5.B2a — an old-inline retraction resolves without consulting the shadow', () => {
  it('an inline old for a row absent from the (complete) shadow still feeds the retraction, whereas key-only drops', async () => {
    // Baseline join: users{id:1,team_id:5} ⋈ teams{id:5}. Both shadows are complete.
    const inlineGraph = await warmedJoin([{ id: 1, team_id: 5 }], [{ id: 5 }])
    // Old carried INLINE for id=2 (never seeded): trusted directly → feeds the retraction → fires.
    const inline = inlineGraph.apply([{ table: 'users', kind: 'delete', old: { id: 2, team_id: 5 } }])
    expect(inline.invalidated).toBe(true)

    // The SAME retraction key-only misses the complete shadow → provably irrelevant → dropped.
    const keyOnlyGraph = await warmedJoin([{ id: 1, team_id: 5 }], [{ id: 5 }])
    const keyOnly = keyOnlyGraph.apply([{ table: 'users', kind: 'delete', key: { id: 2 } }])
    expect(keyOnly.invalidated).toBe(false) // would also fire if old-inline consulted the shadow
  })
})

// ── C2 — warming journals AND coarse-invalidates every event ────────

describe('T5.C2 — from activation, every event is journaled AND coarse-invalidates', () => {
  it('a warming event fires coarse and is drained via fetchByKeys once warming resumes (no silent drop)', async () => {
    let resolveScan!: (rows: Row[]) => void
    const scanPromise = new Promise<Row[]>((resolve) => (resolveScan = resolve))
    const fetchKeys: number[][] = []
    let resolveFetch!: (rows: Row[]) => void
    const fetchPromise = new Promise<Row[]>((resolve) => (resolveFetch = resolve))
    const executor: HydrationExecutor = {
      scan: () => scanPromise,
      fetchByKeys: (_descriptor, keys) => {
        fetchKeys.push(keys.map((k) => k.id as number))
        return fetchPromise
      },
    }
    const graph = createLiveGraph({
      kind: 'stateful',
      instanceKey: 'k',
      tables: ['users'],
      instantiate: () => statefulFake([fakeSeed('users', 'users')]),
      executor,
      maxStateRows: 1e9,
    })
    expect(graph.state()).toBe('warming')
    const out = graph.apply([{ table: 'users', kind: 'update', key: { id: 7 } }])
    expect(out.invalidated).toBe(true) // coarse-invalidate during warming
    expect(graph.inspect().counters.coarseFires).toBe(1)

    resolveScan([])
    await flush() // drain loop swaps the journal → re-fetches the journalled key
    expect(fetchKeys).toEqual([[7]]) // the event was journaled, not dropped
    resolveFetch([])
    await flush()
    expect(graph.state()).toBe('live')
  })
})

// ── E1 — RLS-gated stateful born coarse ─────────────────────────────

describe('T5.E1 — RLS-gated stateful graphs are born coarse', () => {
  it('the born-coarse gate (rlsEnabled true OR unknown) → coarse; no gate (false) → warms', () => {
    const gated = createLiveGraph({
      kind: 'stateful',
      instanceKey: 'k',
      tables: ['users'],
      instantiate: () => statefulFake([fakeSeed('users', 'users')]),
      executor: neverExecutor(),
      maxStateRows: 1e9,
      bornCoarse: 'rls', // the registry sets this for RlsStatus true OR 'unknown'
    })
    expect(gated.state()).toBe('coarse')

    const hydrating = createLiveGraph({
      kind: 'stateful',
      instanceKey: 'k2',
      tables: ['users'],
      instantiate: () => statefulFake([fakeSeed('users', 'users')]),
      executor: neverExecutor(),
      maxStateRows: 1e9,
    })
    expect(hydrating.state()).toBe('warming')
  })

  it('the registry maps a truthy rlsEnabled to the born-coarse gate', async () => {
    const registry = createRegistry({ maxStateRowsPerInput: 100 })
    const plan = {
      tables: ['users'],
      stateless: false,
      coarse: false,
      inputs: [{ alias: 'users', table: 'users', columns: '*' as const, shadowNeed: true }],
      instantiate: () => statefulFake([fakeSeed('users', 'users')]),
    }
    const r = await registry.acquire({
      planKey: 'p',
      instanceKey: 'i',
      tables: ['users'],
      rlsEnabled: true,
      compilePlan: () => plan,
      executor: neverExecutor(),
      notify: () => {},
    })
    expect(r.graph.state()).toBe('coarse')
  })
})

// ── G2 — ≤1 fire per graph per batch ────────────────────────────────

describe('T5.G2 — a graph fires at most once per batch', () => {
  it('a multi-change batch touching a graph several times advances the invalidation seq by at most one', async () => {
    const graph = await warmedJoin([{ id: 1, team_id: 5 }], [{ id: 5 }])
    const before = graph.invalidationSeq()
    const out = graph.apply([
      { table: 'users', kind: 'insert', new: { id: 2, team_id: 5 } },
      { table: 'teams', kind: 'insert', new: { id: 6, region: 'w' } },
      { table: 'users', kind: 'insert', new: { id: 3, team_id: 5 } },
    ])
    expect(out.invalidated).toBe(true)
    expect(graph.invalidationSeq() - before).toBe(1) // ≤1 fire per batch
  })
})

// ── I2 — bounded inspect ────────────────────────────────────────────

describe('T5.I2 — inspect() is bounded (one reason per transition, counters do not grow with events)', () => {
  it('the snapshot shape is fixed regardless of event volume; reason reflects only the last transition', async () => {
    const graph = await warmedJoin([{ id: 1, team_id: 5 }], [{ id: 5 }])
    const snap1 = graph.inspect()
    expect(snap1.state).toBe('live')
    const keys = ['coarseFires', 'demotions', 'dirtyFires', 'exactFires', 'resyncs', 'stateRows', 'warms']
    expect(Object.keys(snap1.counters).sort()).toEqual(keys)

    for (let i = 0; i < 50; i++) graph.apply([{ table: 'users', kind: 'insert', new: { id: 100 + i, team_id: 5 } }])
    const snap2 = graph.inspect()
    expect(Object.keys(snap2.counters).sort()).toEqual(keys) // no per-event growth
    expect(typeof snap2.reason).toBe('string') // ONE reason, not a growing log
    expect(snap2.counters.exactFires).toBeGreaterThan(snap1.counters.exactFires)

    graph.retire()
    expect(graph.inspect().reason).toBe('drift') // reason = last transition only
  })
})
