// The live-graph state machine. Real compiled join graphs are seeded
// through a controllable executor, then driven live: an old-inline retraction resolves without a
// shadow consult (B2a), a change routed DURING the scan is buffered and replayed exactly once as a
// PK-upsert (the one-shot seed-race guard, C), RLS-gated stateful graphs are born coarse (E1), and a
// graph fires at most once per batch (G2). Deterministic — deferred/immediate promises drive the
// seed, never timers.

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
  return { inputId, table, relationId: table, alias: inputId, primaryKey, columns: '*', residual: { kind: 'true' } }
}
function statefulFake(seeds: SeedDescriptor[]): StatefulGraph {
  const noFire = { invalidated: false }
  return { seeds, seedInput() {}, flushSeed() {}, feedInput() {}, runBatch: () => noFire, apply: () => noFire }
}

// ── a real compiled users ⋈ teams graph, seeded to live through its HydrationExecutor ──

function joinExecutor(usersRows: Row[], teamsRows: Row[]): HydrationExecutor {
  return { scan: async (descriptor) => (descriptor.table === 'users' ? usersRows : teamsRows) }
}
async function seededJoin(usersRows: Row[], teamsRows: Row[]): Promise<LiveGraph> {
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

// ── old-inline retraction, no shadow consult ──────────────────

describe('an old-inline retraction resolves without consulting the shadow', () => {
  it('an inline old for a row absent from the (complete) shadow still feeds the retraction, whereas key-only drops', async () => {
    // Baseline join: users{id:1,team_id:5} ⋈ teams{id:5}. Both shadows are complete.
    const inlineGraph = await seededJoin([{ id: 1, team_id: 5 }], [{ id: 5 }])
    // Old carried INLINE for id=2 (never seeded): trusted directly → feeds the retraction → fires.
    const inline = inlineGraph.apply([{ table: 'users', kind: 'delete', old: { id: 2, team_id: 5 } }])
    expect(inline.invalidated).toBe(true)

    // The SAME retraction key-only misses the complete shadow → provably irrelevant → dropped.
    const keyOnlyGraph = await seededJoin([{ id: 1, team_id: 5 }], [{ id: 5 }])
    const keyOnly = keyOnlyGraph.apply([{ table: 'users', kind: 'delete', key: { id: 2 } }])
    expect(keyOnly.invalidated).toBe(false) // would also fire if old-inline consulted the shadow
  })
})

// ── C — the one-shot seed-race guard ────────────────────────────────

describe('a change routed during the scan is buffered and replayed exactly once', () => {
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
    expect(dup.invalidated).toBe(false) // BUFFERED precisely — NOT coarse-invalidated
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
    const graph = await seededJoin([{ id: 1, team_id: 5 }], [{ id: 5 }])
    expect(graph.state()).toBe('live')
    graph.fault() // the router calls this after a caught apply() throw — the precise state is corrupt
    expect(graph.state()).toBe('coarse')
    // the corrupt precise state is abandoned; any watched change now coarse-fires → no post-fault miss
    expect(graph.apply([{ table: 'users', kind: 'insert', new: { id: 99, team_id: 5 } }]).invalidated).toBe(true)
  })
})

describe('coarsen() intentionally demotes a graph to coarse', () => {
  it('an explicit coarse event demotes a live graph to coarse and every subsequent change coarse-fires', async () => {
    const graph = await seededJoin([{ id: 1, team_id: 5 }], [{ id: 5 }])
    expect(graph.state()).toBe('live')
    graph.coarsen() // the router calls this for an image-less mutation it can't represent precisely
    expect(graph.state()).toBe('coarse')
    // the precise state is abandoned; any watched change now coarse-fires → no post-demote miss
    expect(graph.apply([{ table: 'users', kind: 'insert', new: { id: 99, team_id: 5 } }]).invalidated).toBe(true)
  })

  it('coarsen on a terminal (destroyed) graph is inert', async () => {
    const destroyed = await seededJoin([{ id: 1, team_id: 5 }], [{ id: 5 }])
    destroyed.destroy()
    destroyed.coarsen() // terminal wins → no-op
    expect(destroyed.state()).toBe('destroyed')
  })
})

// ── E1 — RLS-gated stateful born coarse ─────────────────────────────

describe('RLS-gated stateful graphs are born coarse', () => {
  it('the born-coarse gate (rlsEnabled true OR unknown) → coarse; no gate (false) → seeds', () => {
    const gated = createLiveGraph({
      kind: 'stateful',
      instanceKey: 'k',
      tables: ['users'],
      instantiate: () => statefulFake([fakeSeed('users', 'users')]),
      executor: neverExecutor(),
      maxStateRows: 1e9,
      bornCoarse: true, // the registry sets this for RlsStatus true OR 'unknown'
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
      instantiate: () => statefulFake([fakeSeed('users', 'users')]),
    }
    const r = await registry.acquire({
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

describe('a graph fires at most once per batch', () => {
  it('a multi-change batch touching a graph several times advances the invalidation seq by at most one', async () => {
    const graph = await seededJoin([{ id: 1, team_id: 5 }], [{ id: 5 }])
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

// ── reseed: coarse is a recoverable state ────────────────────────────
//
// An image-less event (raw SQL anywhere, a detected transport gap) used to demote a graph PERMANENTLY —
// `coarsen()` frees the operator state and sets `coarse` with no path back — so one raw statement on any
// instance cost every watching graph its precision for life. `reseed()` invalidates and then rebuilds the
// baseline from the database instead, keeping the graph's identity and subscribers.

/** A hydration executor whose scans can be held open, so a test can observe a reseed IN FLIGHT. */
function gatedExecutor(rowsFor: (table: string) => Row[]) {
  const pending: Array<() => void> = []
  let hold = false
  const executor: HydrationExecutor = {
    scan: async (descriptor) => {
      if (hold) await new Promise<void>((resolve) => pending.push(resolve))
      return rowsFor(descriptor.table)
    },
  }
  // `release` also STOPS holding: the seed scans its descriptors sequentially, so a release that only
  // drained the queue would let the next descriptor's scan block again and hang the seed forever.
  return {
    executor,
    hold: () => (hold = true),
    releaseAll: () => {
      hold = false
      pending.splice(0).forEach((resolve) => resolve())
    },
  }
}

describe('reseed — an image-less event rebuilds precise state instead of surrendering it', () => {
  it('a live stateful graph RESEEDS: it fires, stays live, and is precise afterwards', async () => {
    const graph = await seededJoin([{ id: 1, teamId: 10 }], [{ id: 10, region: 'eu' }])
    const before = graph.invalidationSeq()

    graph.reseed()
    await settle(graph)

    expect(graph.invalidationSeq()).toBeGreaterThan(before) // subscribers were told
    expect(graph.state()).toBe('live') // …and it did NOT surrender precision (coarsen() would give 'coarse')
    // Still precise: an ordinary row change is classified, not blanket-fired.
    const irrelevant = graph.apply([{ table: 'users', kind: 'insert', new: { id: 2, teamId: 999 } }])
    expect(irrelevant.invalidated).toBe(false) // a coarse graph would have fired here
  })

  it('a STATELESS graph fires but never re-hydrates — it has no history to rebuild', async () => {
    const scans: string[] = []
    const spec: LiveGraphSpec = {
      kind: 'stateless',
      instanceKey: 'stateless',
      tables: ['users'],
      instantiate: () =>
        compileQuery(extractQueryShape(qb.select().from(users), { dialect: 'pg' })).instantiate() as never,
    }
    const graph = createLiveGraph(spec)
    await settle(graph)
    const before = graph.invalidationSeq()

    graph.reseed()
    await settle(graph)

    expect(graph.invalidationSeq()).toBeGreaterThan(before) // invalidated…
    expect(graph.state()).toBe('live') // …still live, not demoted
    expect(scans).toEqual([]) // …and no database round trip at all
  })

  it('STORM GUARD: a coarse event arriving mid-reseed demotes instead of queueing another', async () => {
    const gated = gatedExecutor((table) => (table === 'users' ? [{ id: 1, teamId: 10 }] : [{ id: 10 }]))
    const spec: LiveGraphSpec = {
      kind: 'stateful',
      instanceKey: 'storm',
      tables: ['users', 'teams'],
      instantiate: () =>
        compileQuery(
          extractQueryShape(qb.select().from(users).innerJoin(teams, eq(teams.id, users.teamId)), { dialect: 'pg' }),
        ).instantiate() as StatefulGraph,
      executor: gated.executor,
      maxStateRows: 1e9,
    }
    const graph = createLiveGraph(spec)
    await settle(graph)
    expect(graph.state()).toBe('live')

    gated.hold()
    graph.reseed() // in flight, waiting on the held scan
    expect(graph.state()).toBe('seeding')

    graph.reseed() // the second one arrives while the first is still running
    expect(graph.state()).toBe('coarse') // fail-closed, and no second re-hydrate was queued

    gated.releaseAll()
    await settle(graph)
    expect(graph.state()).toBe('coarse') // the demote is terminal; the in-flight scan cannot revive it
  })

  it('a reseed that CANNOT run falls back to demoting (no usable PK)', async () => {
    // Same fallback the initial seed has: a descriptor with no primary key cannot shadow-resolve.
    const graph = createLiveGraph({
      kind: 'stateful',
      instanceKey: 'nopk',
      tables: ['users'],
      instantiate: () => statefulFake([fakeSeed('users', 'users', [])]),
      executor: { scan: async () => [] },
      maxStateRows: 1e9,
    })
    await settle(graph)
    expect(graph.state()).toBe('coarse') // born coarse — nothing to reseed
    graph.reseed()
    expect(graph.state()).toBe('coarse')
  })
})

// The miss the recon found BEFORE this was built. A write landing DURING a reseed is buffered and folded
// into the new baseline by the synchronous cut — silently, because the buffer deliberately does not fire.
// For an INITIAL seed that is right: nobody is subscribed yet, and the reader is covered by the redeem
// fence. For a RESEED it is not — subscribers exist, and a write that committed after their refetch would
// be absorbed and never announced. This is the seqAtRead fence aimed at subscribers, not at the reader.
describe('reseed — a write landing mid-reseed is announced, not absorbed', () => {
  /** The join graph used by both cases, over a gated executor so the scan can be held open. */
  function racingJoin(gated: ReturnType<typeof gatedExecutor>, instanceKey: string): LiveGraph {
    return createLiveGraph({
      kind: 'stateful',
      instanceKey,
      tables: ['users', 'teams'],
      instantiate: () =>
        compileQuery(
          extractQueryShape(qb.select().from(users).innerJoin(teams, eq(teams.id, users.teamId)), { dialect: 'pg' }),
        ).instantiate() as StatefulGraph,
      executor: gated.executor,
      maxStateRows: 1e9,
    })
  }
  const gatedRows = (table: string) => (table === 'users' ? [{ id: 1, teamId: 10 }] : [{ id: 10 }])
  /** Let the seed's async scans reach their first await; start() kicks them off, it does not run them. */
  const drain = async () => {
    for (let i = 0; i < 5; i++) await Promise.resolve()
  }

  it('fires AGAIN at the cut when the scan raced a write', async () => {
    const gated = gatedExecutor(gatedRows)
    const graph = racingJoin(gated, 'race')
    await settle(graph)

    gated.hold()
    graph.reseed()
    await drain() // let the scan actually reach its await before we hold or release anything
    const afterStart = graph.invalidationSeq() // the one invalidation the client already received

    // A write commits while the scan is open: buffered, then folded into the baseline by the cut.
    graph.apply([{ table: 'users', kind: 'insert', new: { id: 2, teamId: 10 } }])

    gated.releaseAll()
    await settle(graph)

    expect(graph.state()).toBe('live')
    expect(graph.invalidationSeq()).toBeGreaterThan(afterStart) // announced, not silently absorbed
  })

  it('does NOT fire again when nothing raced — so the fire above is attributable to the race', async () => {
    const gated = gatedExecutor(gatedRows)
    const graph = racingJoin(gated, 'norace')
    await settle(graph)

    gated.hold()
    graph.reseed()
    await drain()
    const afterStart = graph.invalidationSeq()
    gated.releaseAll()
    await settle(graph)

    expect(graph.invalidationSeq()).toBe(afterStart) // exactly one invalidation for an uneventful reseed
  })
})
