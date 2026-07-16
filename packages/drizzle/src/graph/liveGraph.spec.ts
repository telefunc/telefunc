// T5.B2a / C / E1 / G2 / I2 — the live-graph state machine. Real compiled join graphs are seeded
// through a controllable executor, then driven live: an old-inline retraction resolves without a
// shadow consult (B2a), a change routed DURING the scan is buffered and replayed exactly once as a
// PK-upsert (the one-shot seed-race guard, C), RLS-gated stateful graphs are born coarse (E1), a
// graph fires at most once per batch (G2), and inspect() is bounded with one reason per transition
// (I2). Deterministic — deferred/immediate promises drive the seed, never timers.

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

const settle = (graph: LiveGraph): Promise<void> => graph.ready() // resolves when the seed lands (→ live / coarse)

const neverExecutor = (): HydrationExecutor => ({
  scan: () => new Promise<Row[]>(() => {}), // never resolves → stays 'seeding'
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
  return { scan: async (descriptor) => (descriptor.table === 'users' ? usersRows : teamsRows) }
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

// ── C — the one-shot seed-race guard ────────────────────────────────

describe('T5.C — a change routed during the scan is buffered and replayed exactly once', () => {
  it('buffers precisely (no coarse-invalidate) and replays as a PK-upsert: dup collapses, new lands', async () => {
    // SELECT DISTINCT team_id FROM users, seeded through a gated scan so we can inject during 'seeding'.
    let resolveScan!: (rows: Row[]) => void
    const scanPromise = new Promise<Row[]>((resolve) => (resolveScan = resolve))
    const build = () => qb.selectDistinct({ team: users.teamId }).from(users)
    const graph = createLiveGraph({
      kind: 'stateful',
      instanceKey: 'd',
      tables: ['users'],
      instantiate: () => compileQuery(extractQueryShape(build(), { dialect: 'pg' })).instantiate() as StatefulGraph,
      executor: { scan: () => scanPromise },
      maxStateRows: 1e9,
    })
    expect(graph.state()).toBe('seeding')

    // Two changes routed during the scan: one DUPLICATES the snapshot's PK (id 1), one is NEW (id 2).
    const dup = graph.apply([{ table: 'users', kind: 'insert', new: { id: 1, team_id: 5 } }])
    const fresh = graph.apply([{ table: 'users', kind: 'insert', new: { id: 2, team_id: 9 } }])
    expect(dup.invalidated).toBe(false) // BUFFERED precisely — NOT coarse-invalidated (the warming behavior we cut)
    expect(fresh.invalidated).toBe(false)

    resolveScan([{ id: 1, team_id: 5 }]) // snapshot already contains id 1 (team 5)
    await graph.ready()
    expect(graph.state()).toBe('live')

    // After the synchronous cut the engine holds {(1,5),(2,9)} — id 1 ONCE (dup collapsed), id 2 landed.
    // Deleting id 1 removes the only team-5 row → distinct {5,9}→{9} FIRES (a phantom double would keep 5).
    expect(graph.apply([{ table: 'users', kind: 'delete', old: { id: 1, team_id: 5 } }]).invalidated).toBe(true)
    // Deleting id 2 removes the only team-9 row → distinct {9}→{} FIRES (proves the buffered id 2 landed).
    expect(graph.apply([{ table: 'users', kind: 'delete', old: { id: 2, team_id: 9 } }]).invalidated).toBe(true)
  })

  it('a PK-changing update replayed during the seed does not strand the old-PK row', async () => {
    let resolveScan!: (rows: Row[]) => void
    const scanPromise = new Promise<Row[]>((resolve) => (resolveScan = resolve))
    const build = () => qb.selectDistinct({ team: users.teamId }).from(users)
    const graph = createLiveGraph({
      kind: 'stateful',
      instanceKey: 'pk',
      tables: ['users'],
      instantiate: () => compileQuery(extractQueryShape(build(), { dialect: 'pg' })).instantiate() as StatefulGraph,
      executor: { scan: () => scanPromise },
      maxStateRows: 1e9,
    })
    expect(graph.state()).toBe('seeding')

    // A PK-CHANGING update routed during the scan: id 1 (team 5) → id 2 (team 9). The OLD row must be
    // retracted by its OWN pk (id 1), not by the new pk (id 2), or id 1 strands in the completed shadow.
    graph.apply([{ table: 'users', kind: 'update', old: { id: 1, team_id: 5 }, new: { id: 2, team_id: 9 } }])
    resolveScan([{ id: 1, team_id: 5 }]) // the snapshot SAW the old row (id 1, team 5)
    await graph.ready()
    expect(graph.state()).toBe('live')

    // The engine must hold ONLY {id 2, team 9}. An insert of a team-5 row introduces a NEW distinct
    // value → FIRES. If id 1 stranded (team 5 still present), team 5 would not be new → no fire.
    expect(graph.apply([{ table: 'users', kind: 'insert', new: { id: 3, team_id: 5 } }]).invalidated).toBe(true)
  })
})

describe('fault() permanently demotes a corrupt graph to coarse', () => {
  it('a faulted live graph goes coarse and every subsequent change coarse-fires (sound over-fire)', async () => {
    const graph = await warmedJoin([{ id: 1, team_id: 5 }], [{ id: 5 }])
    expect(graph.state()).toBe('live')
    graph.fault() // the router calls this after a caught apply() throw — the precise state is corrupt
    expect(graph.state()).toBe('coarse')
    // the corrupt precise state is abandoned; any watched change now coarse-fires → no post-fault miss
    expect(graph.apply([{ table: 'users', kind: 'insert', new: { id: 99, team_id: 5 } }]).invalidated).toBe(true)
  })
})

describe('coarsen(reason) intentionally demotes a graph to coarse with a labelled reason', () => {
  it('an explicit coarse event demotes a live graph to coarse (labelled) and every subsequent change coarse-fires', async () => {
    const graph = await warmedJoin([{ id: 1, team_id: 5 }], [{ id: 5 }])
    expect(graph.state()).toBe('live')
    graph.coarsen('coarse-event') // the router calls this for an image-less mutation it can't represent precisely
    expect(graph.state()).toBe('coarse')
    expect(graph.inspect().reason).toBe('coarse-event') // labelled — distinct from fault's 'apply-fault'
    // the precise state is abandoned; any watched change now coarse-fires → no post-demote miss
    expect(graph.apply([{ table: 'users', kind: 'insert', new: { id: 99, team_id: 5 } }]).invalidated).toBe(true)
  })

  it('coarsen on a terminal (retired/destroyed) graph is inert', async () => {
    const retired = await warmedJoin([{ id: 1, team_id: 5 }], [{ id: 5 }])
    retired.retire()
    retired.coarsen('coarse-event') // terminal wins → no-op
    expect(retired.state()).toBe('retired')

    const destroyed = await warmedJoin([{ id: 1, team_id: 5 }], [{ id: 5 }])
    destroyed.destroy()
    destroyed.coarsen('coarse-event')
    expect(destroyed.state()).toBe('destroyed')
  })
})

// ── E1 — RLS-gated stateful born coarse ─────────────────────────────

describe('T5.E1 — RLS-gated stateful graphs are born coarse', () => {
  it('the born-coarse gate (rlsEnabled true OR unknown) → coarse; no gate (false) → seeds', () => {
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
    expect(hydrating.state()).toBe('seeding')
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
    const keys = ['coarseFires', 'demotions', 'dirtyFires', 'exactFires', 'seeds', 'stateRows']
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
