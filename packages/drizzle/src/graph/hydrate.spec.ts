// T5.C — non-blocking warming + the atomic drain-loop cut. Deterministic paused injection
// (deferred promises), never timers. C1 (response never awaits + σ-bounded state), C3 (seed
// scan is the σ-pruned in-memory baseline; single + composite PK), C4 (THE BLOCKER — one muted
// feed, loop-until-empty drain, a mid-fetch event is not lost to limbo — failing-then-passing),
// C5 (journal overflow → coarse), C6 (resync re-warms a new generation, drift retires), and C7
// (a late async completion after destroy / demote / retire / supersede is inert).

import { gt, sql } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { type SeedDescriptor, type StatefulGraph, compileQuery } from '../compile/compile.js'
import type { Row } from '../compile/rowSpace.js'
import { extractQueryShape } from '../extract/queryShape.js'
import { JOURNAL_LIMIT, type HydrationExecutor, createWarming } from './hydrate.js'
import { type LiveGraphSpec, createLiveGraph } from './liveGraph.js'
import type { ShadowIndex } from './shadow.js'

// ── deterministic async plumbing ────────────────────────────────────

type Deferred<T> = { promise: Promise<T>; resolve: (value: T) => void; reject: (error: unknown) => void }
function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}
const flush = (): Promise<void> => new Promise((resolve) => setImmediate(resolve))

// ── real σ-scoped descriptors (built through the compiler) ──────────

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  score: pg.integer('score'),
})
const qb = new pg.QueryBuilder()

function usersDescriptor(): SeedDescriptor {
  const q = qb
    .select({ team: users.teamId, s: sql`sum(${users.score})` })
    .from(users)
    .where(gt(users.score, 0))
    .groupBy(users.teamId)
  const graph = compileQuery(extractQueryShape(q, { dialect: 'pg' })).instantiate() as StatefulGraph
  return graph.seeds.find((seed) => seed.table === 'users')!
}
/** A composite-PK input (hand-built: the drain-loop key logic — pkOf over several columns —
 *  is what C3's composite case exercises, independent of the compiler's PK extraction). */
function compositeDescriptor(): SeedDescriptor {
  return {
    inputId: 'memberships',
    table: 'memberships',
    alias: 'memberships',
    primaryKey: ['user_id', 'team_id'],
    columns: ['user_id', 'team_id', 'role'],
    residual: { kind: 'true' },
    shadowNeed: true,
  }
}

// ── liveGraph fakes (for the state-machine cases C6/C7) ─────────────

function fakeSeed(inputId: string, table: string, primaryKey: string[] = ['id']): SeedDescriptor {
  return { inputId, table, alias: inputId, primaryKey, columns: '*', residual: { kind: 'true' }, shadowNeed: true }
}
function statefulFake(seeds: SeedDescriptor[]): StatefulGraph {
  const noFire = { data: false, dirty: false, invalidated: false }
  return { seeds, seedInput() {}, flushSeed() {}, feedInput() {}, runBatch: () => noFire, apply: () => noFire }
}
function statefulSpec(executor: HydrationExecutor, over: { maxStateRows?: number } = {}): LiveGraphSpec {
  return {
    kind: 'stateful',
    instanceKey: 'k',
    tables: ['users'],
    instantiate: () => statefulFake([fakeSeed('users', 'users')]),
    executor,
    maxStateRows: over.maxStateRows ?? 1_000_000,
  }
}
function scanQueue(): { executor: HydrationExecutor; calls: Deferred<Row[]>[] } {
  const calls: Deferred<Row[]>[] = []
  return {
    calls,
    executor: {
      scan: () => {
        const d = deferred<Row[]>()
        calls.push(d)
        return d.promise
      },
      fetchByKeys: () => Promise.resolve([]),
    },
  }
}

// ── C1 — non-blocking + σ-bounded ───────────────────────────────────

describe('T5.C1 — response never awaits hydration; state is σ-bounded (not table size)', () => {
  it('start() returns before the scan resolves, and only σ-matching rows enter state', async () => {
    const descriptor = usersDescriptor() // σ = score > 0
    const scanRows: Row[] = []
    let matching = 0
    for (let i = 1; i <= 50; i++) {
      const score = i % 2 === 0 ? i : 0 // half the (large) table is σ-excluded
      if (score > 0) matching++
      scanRows.push({ id: i, team_id: 1, score })
    }
    const scanDef = deferred<Row[]>()
    let built: Map<string, ShadowIndex> | undefined
    const warming = createWarming({
      descriptors: [descriptor],
      executor: { scan: () => scanDef.promise, fetchByKeys: () => Promise.resolve([]) },
      maxStateRows: 1e9,
      hooks: { onComplete: (shadows) => (built = shadows), onDemote: () => expect.unreachable('unexpected demote') },
    })
    warming.start()
    expect(built).toBeUndefined() // non-blocking: start() did not await the scan
    scanDef.resolve(scanRows)
    await flush()
    expect(built).toBeDefined()
    expect(matching).toBeLessThan(50)
    expect(built!.get('users')!.size).toBe(matching) // σ-matching count, NOT the table size
  })
})

// ── C3 — σ-pruned in-memory baseline (single + composite PK) ─────────

describe('T5.C3 — seed scan = σ-scoped pruned baseline, single + composite PK', () => {
  it('single-PK: prunes to demanded columns and admits only σ-matching rows', async () => {
    const descriptor = usersDescriptor()
    const scanDef = deferred<Row[]>()
    let built: Map<string, ShadowIndex> | undefined
    const warming = createWarming({
      descriptors: [descriptor],
      executor: { scan: () => scanDef.promise, fetchByKeys: () => Promise.resolve([]) },
      maxStateRows: 1e9,
      hooks: { onComplete: (shadows) => (built = shadows), onDemote: () => expect.unreachable('unexpected demote') },
    })
    warming.start()
    scanDef.resolve([
      { id: 1, team_id: 5, score: 10, extra: 'drop-me' },
      { id: 2, team_id: 5, score: 0 }, // σ-excluded (0 > 0 is false)
    ])
    await flush()
    const rows = built!.get('users')!.rows()
    expect(rows.length).toBe(1)
    expect(rows[0]).not.toHaveProperty('extra') // pruned to demanded columns
    expect(rows[0]).toMatchObject({ id: 1, team_id: 5, score: 10 })
  })

  it('composite-PK: keys the baseline by the full composite key', async () => {
    const descriptor = compositeDescriptor()
    expect(descriptor.primaryKey).toEqual(['user_id', 'team_id'])
    const scanDef = deferred<Row[]>()
    let built: Map<string, ShadowIndex> | undefined
    const warming = createWarming({
      descriptors: [descriptor],
      executor: { scan: () => scanDef.promise, fetchByKeys: () => Promise.resolve([]) },
      maxStateRows: 1e9,
      hooks: { onComplete: (shadows) => (built = shadows), onDemote: () => expect.unreachable('unexpected demote') },
    })
    warming.start()
    scanDef.resolve([
      { user_id: 1, team_id: 5, role: 'admin' },
      { user_id: 1, team_id: 6, role: 'member' }, // same user, different team → distinct composite key
    ])
    await flush()
    expect(built!.get('memberships')!.size).toBe(2)
  })
})

// ── C4 — THE BLOCKER: atomic cut, no limbo miss ─────────────────────

describe('T5.C4 — drain loop atomic cut (single muted feed; mid-fetch event not lost)', () => {
  it('reconcile: a key still σ-matching is replaced, one that fell out of σ is retracted', async () => {
    const descriptor = usersDescriptor()
    let built: Map<string, ShadowIndex> | undefined
    const warming = createWarming({
      descriptors: [descriptor],
      executor: {
        scan: async () => [
          { id: 1, team_id: 5, score: 4 },
          { id: 2, team_id: 5, score: 4 },
        ],
        // id=1 now scores 9 (replace); id=2 now scores 0 → σ-excluded (retract).
        fetchByKeys: async () => [
          { id: 1, team_id: 5, score: 9 },
          { id: 2, team_id: 5, score: 0 },
        ],
      },
      maxStateRows: 1e9,
      hooks: { onComplete: (shadows) => (built = shadows), onDemote: () => expect.unreachable('unexpected demote') },
    })
    // Two key-only touches so the drain re-fetches both, then reconciles.
    warming.journal({ table: 'users', kind: 'update', key: { id: 1 } })
    warming.journal({ table: 'users', kind: 'update', key: { id: 2 } })
    warming.start()
    await flush()
    const rows = built!
      .get('users')!
      .rows()
      .map((r) => ({ id: r.id, score: r.score }))
    expect(rows).toEqual([{ id: 1, score: 9 }]) // id=1 replaced to 9; id=2 retracted (fell out of σ)
  })

  // The discriminating counterexample, run for real against BOTH drain strategies through the
  // SAME paused-injection scenario: E1 (id=2) is journaled before warm start (so the first swap is
  // non-empty), then E2 (id=3) is injected DURING the id=2 re-fetch await. The only difference is
  // `drainMode`. `'single-pass'` is the naive drain-once→flip form that provably LOSES E2 to limbo;
  // `'loop-until-empty'` is production and catches it. Returns the flipped multiset + the fetch log.
  async function midFetchScenario(
    drainMode: 'loop-until-empty' | 'single-pass',
  ): Promise<{ ids: number[]; fetchKeyLog: number[][] }> {
    const descriptor = usersDescriptor()
    const dbState = new Map<number, Row>([[1, { id: 1, team_id: 5, score: 5 }]])
    const fetchDeferreds: Deferred<Row[]>[] = []
    const fetchKeyLog: number[][] = []
    const executor: HydrationExecutor = {
      scan: async () => [...dbState.values()],
      fetchByKeys: (_descriptor, keys) => {
        fetchKeyLog.push(keys.map((k) => k.id as number))
        const def = deferred<Row[]>()
        fetchDeferreds.push(def)
        return def.promise
      },
    }
    let built: Map<string, ShadowIndex> | undefined
    const warming = createWarming({
      descriptors: [descriptor],
      executor,
      maxStateRows: 1e9,
      drainMode,
      hooks: { onComplete: (shadows) => (built = shadows), onDemote: () => expect.unreachable('unexpected demote') },
    })

    // E1 arrives before warm start → the first swapped journal is non-empty.
    dbState.set(2, { id: 2, team_id: 5, score: 8 })
    warming.journal({ table: 'users', kind: 'insert', new: { id: 2, team_id: 5, score: 8 }, key: { id: 2 } })
    warming.start()
    await flush() // scan resolves; drain pass 1 issues fetchByKeys([2]) and awaits (paused)
    expect(fetchKeyLog).toEqual([[2]])
    expect(built).toBeUndefined() // still draining — not yet flipped

    // E2 injected DURING the fetch await — the limbo event.
    dbState.set(3, { id: 3, team_id: 5, score: 9 })
    warming.journal({ table: 'users', kind: 'insert', new: { id: 3, team_id: 5, score: 9 }, key: { id: 3 } })
    fetchDeferreds[0]!.resolve([{ id: 2, team_id: 5, score: 8 }])
    await flush() // single-pass: flips now (E2 stranded). loop: swaps E2 → issues fetchByKeys([3]).

    // The loop form issued a second re-fetch that must be released; the single-pass form flipped
    // after one pass and issued none.
    if (fetchDeferreds[1]) {
      fetchDeferreds[1].resolve([{ id: 3, team_id: 5, score: 9 }])
      await flush() // drain swaps empty → onComplete → synchronous flip
    }

    const ids = built!
      .get('users')!
      .rows()
      .map((r) => r.id as number)
      .sort((a, b) => a - b)
    return { ids, fetchKeyLog }
  }

  it('the naive single-pass drain (TEST-ONLY seam) LOSES the mid-fetch event to limbo', async () => {
    const { ids, fetchKeyLog } = await midFetchScenario('single-pass')
    expect(fetchKeyLog).toEqual([[2]]) // flipped after ONE swap — E2 (id=3) never re-fetched
    expect(ids).not.toContain(3) // the discriminating miss
    expect(ids).toEqual([1, 2]) // scan {1} + the one drained batch {E1:2}; E2 stranded in the journal
  })

  it('the production loop-until-empty drain CATCHES the same mid-fetch event', async () => {
    const { ids, fetchKeyLog } = await midFetchScenario('loop-until-empty')
    expect(fetchKeyLog).toEqual([[2], [3]]) // the next swap re-fetched E2 (id=3)
    expect(ids).toContain(3)
    expect(ids).toEqual([1, 2, 3]) // reconciled multiset, NOT the single-pass {1,2}
  })
})

// ── C5 — journal overflow ───────────────────────────────────────────

describe('T5.C5 — journal overflow demotes to coarse', () => {
  it('the (JOURNAL_LIMIT + 1)th journalled event demotes, earlier ones do not', () => {
    let demoted: string | undefined
    const warming = createWarming({
      descriptors: [usersDescriptor()],
      executor: { scan: () => new Promise<Row[]>(() => {}), fetchByKeys: () => Promise.resolve([]) },
      maxStateRows: 1e9,
      hooks: { onComplete: () => expect.unreachable('unexpected complete'), onDemote: (reason) => (demoted = reason) },
    })
    warming.start()
    for (let i = 0; i < JOURNAL_LIMIT; i++) warming.journal({ table: 'users', kind: 'insert', new: { id: i } })
    expect(demoted).toBeUndefined()
    warming.journal({ table: 'users', kind: 'insert', new: { id: JOURNAL_LIMIT } })
    expect(demoted).toBe('journal-overflow')
  })
})

// ── C6 — re-warm vs retire ──────────────────────────────────────────

describe('T5.C6 — resync re-warms a new generation; drift retires (no re-warm)', () => {
  it('a resync re-runs seed+drain as a new generation; a retired graph never re-warms', async () => {
    const { executor, calls } = scanQueue()
    const graph = createLiveGraph(statefulSpec(executor))
    expect(graph.state()).toBe('warming')
    calls[0]!.resolve([{ id: 1 }])
    await flush()
    expect(graph.state()).toBe('live')

    const warms = graph.inspect().counters.warms
    graph.rewarm() // gap / source resync / shadow discontinuity
    expect(graph.state()).toBe('warming')
    expect(graph.inspect().counters.warms).toBe(warms + 1) // new generation
    expect(calls.length).toBe(2) // re-scanned

    graph.retire() // schema drift → terminal
    expect(graph.state()).toBe('retired')
    graph.rewarm() // there is NO drift → re-warm path
    expect(graph.state()).toBe('retired')
  })
})

// ── C7 — generation/abort token makes late async inert ──────────────

describe('T5.C7 — a late async completion is inert (destroy / demote / retire / supersede)', () => {
  it('destroy: a late scan never flips a destroyed graph live (a zero-ref destroy is never undone)', async () => {
    const { executor, calls } = scanQueue()
    const graph = createLiveGraph(statefulSpec(executor))
    graph.destroy()
    expect(graph.state()).toBe('destroyed')
    calls[0]!.resolve([{ id: 1 }])
    await flush()
    expect(graph.state()).toBe('destroyed')
  })

  it('retire: a late scan never flips a retired graph live', async () => {
    const { executor, calls } = scanQueue()
    const graph = createLiveGraph(statefulSpec(executor))
    graph.retire()
    calls[0]!.resolve([{ id: 1 }])
    await flush()
    expect(graph.state()).toBe('retired')
  })

  it('supersede: the superseded generation is inert; only the newer one flips live', async () => {
    const { executor, calls } = scanQueue()
    const graph = createLiveGraph(statefulSpec(executor))
    graph.rewarm() // gen2 supersedes gen1
    expect(calls.length).toBe(2)
    calls[0]!.resolve([{ id: 1 }]) // gen1 (superseded) resolves late
    await flush()
    expect(graph.state()).toBe('warming') // NOT flipped by the stale generation
    calls[1]!.resolve([{ id: 2 }]) // gen2 completes
    await flush()
    expect(graph.state()).toBe('live')
  })

  it('demote: after a journal-overflow demote, a late scan does not resurrect the graph', async () => {
    const { executor, calls } = scanQueue()
    const graph = createLiveGraph(statefulSpec(executor))
    for (let i = 0; i <= JOURNAL_LIMIT; i++) graph.apply([{ table: 'users', kind: 'insert', new: { id: i } }])
    expect(graph.state()).toBe('coarse')
    calls[0]!.resolve([{ id: 1 }])
    await flush()
    expect(graph.state()).toBe('coarse')
  })
})
