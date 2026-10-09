// Synchronous-cut hydration: the seed module. C1 (start() does not await the scan and state
// is σ-bounded, not table size), C3 (the seed scan is the σ-pruned in-memory baseline; single +
// composite PK), a seed exceeding the per-input state bound → coarse, a scan error → coarse (never a
// partial seed), and abort — a late scan completion after destroy / retire is inert (never seeds a
// dead graph). The one-shot seed-race replay is proven in liveGraph.lifecycle.spec; the drain-loop / journal
// it replaced are gone. Deterministic — deferred promises, never timers.

import { gt, sql } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { type SeedDescriptor, type StatefulGraph, compileQuery } from '../compile/compile.js'
import type { Row } from '../compile/rowSpace.js'
import { extractQueryShape } from '../../drizzle/extract/queryShape.js'
import { type HydrationExecutor, createSeed } from './hydrate.js'
import { type LiveGraphSpec, createLiveGraph } from './liveGraph.js'
import type { ShadowIndex } from './shadow.js'

type Deferred<T> = { promise: Promise<T>; resolve: (value: T) => void }
function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((res) => (resolve = res))
  return { promise, resolve }
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
/** A composite-PK input (hand-built: the shadow key logic — pkOf over several columns — is what
 *  C3's composite case exercises, independent of the compiler's PK extraction). */
function compositeDescriptor(): SeedDescriptor {
  return {
    inputId: 'memberships',
    table: 'memberships',
    relationId: 'memberships',
    alias: 'memberships',
    primaryKey: ['user_id', 'team_id'],
    columns: ['user_id', 'team_id', 'role'],
    residual: { kind: 'true' },
  }
}

// ── liveGraph fakes (for the abort/inert cases) ─────────────────────

function fakeSeed(inputId: string, table: string, primaryKey: string[] = ['id']): SeedDescriptor {
  return { inputId, table, relationId: table, alias: inputId, primaryKey, columns: '*', residual: { kind: 'true' } }
}
function statefulFake(seeds: SeedDescriptor[]): StatefulGraph {
  const noFire = { invalidated: false }
  return {
    seeds,
    seedInput() {},
    flushSeed() {},
    feedInput() {},
    feedDirtyWitness() {},
    runBatch: () => noFire,
    apply: () => noFire,
  }
}
function statefulSpec(executor: HydrationExecutor): LiveGraphSpec {
  return {
    kind: 'stateful',
    instanceKey: 'k',
    tables: ['users'],
    instantiate: () => statefulFake([fakeSeed('users', 'users')]),
    executor,
    maxStateRows: 1_000_000,
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
    },
  }
}

// ── non-blocking + σ-bounded ───────────────────────────────────

describe('start() does not await the scan; state is σ-bounded (not table size)', () => {
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
    const seed = createSeed({
      descriptors: [descriptor],
      executor: { scan: () => scanDef.promise },
      maxStateRows: 1e9,
      hooks: { onComplete: (shadows) => (built = shadows), onDemote: () => expect.unreachable('unexpected demote') },
    })
    seed.start()
    expect(built).toBeUndefined() // non-blocking: start() did not await the scan
    scanDef.resolve(scanRows)
    await flush()
    expect(built).toBeDefined()
    expect(matching).toBeLessThan(50)
    expect(built!.get('users')!.size).toBe(matching) // σ-matching count, NOT the table size
  })
})

// ── C3 — σ-pruned in-memory baseline (single + composite PK) ─────────

describe('seed scan = σ-scoped pruned baseline, single + composite PK', () => {
  it('single-PK: prunes to demanded columns and admits only σ-matching rows', async () => {
    const scanDef = deferred<Row[]>()
    let built: Map<string, ShadowIndex> | undefined
    const seed = createSeed({
      descriptors: [usersDescriptor()],
      executor: { scan: () => scanDef.promise },
      maxStateRows: 1e9,
      hooks: { onComplete: (shadows) => (built = shadows), onDemote: () => expect.unreachable('unexpected demote') },
    })
    seed.start()
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
    const seed = createSeed({
      descriptors: [descriptor],
      executor: { scan: () => scanDef.promise },
      maxStateRows: 1e9,
      hooks: { onComplete: (shadows) => (built = shadows), onDemote: () => expect.unreachable('unexpected demote') },
    })
    seed.start()
    scanDef.resolve([
      { user_id: 1, team_id: 5, role: 'admin' },
      { user_id: 1, team_id: 6, role: 'member' }, // same user, different team → distinct composite key
    ])
    await flush()
    expect(built!.get('memberships')!.size).toBe(2)
  })
})

// ── bounded state + fail-closed ─────────────────────────────────────

describe('a seed exceeding the per-input state bound demotes to coarse', () => {
  it('onDemote fires with state-row-limit and onComplete does not', async () => {
    let demoted: string | undefined
    let completed = false
    const seed = createSeed({
      descriptors: [usersDescriptor()],
      executor: { scan: async () => Array.from({ length: 5 }, (_, i) => ({ id: i, team_id: 1, score: i + 1 })) },
      maxStateRows: 3,
      hooks: { onComplete: () => (completed = true), onDemote: (reason) => (demoted = reason) },
    })
    seed.start()
    await flush()
    expect(demoted).toBe('state-row-limit')
    expect(completed).toBe(false)
  })
})

describe('a scan error demotes to coarse (never a partial seed)', () => {
  it('onDemote fires with hydration-error', async () => {
    let demoted: string | undefined
    const seed = createSeed({
      descriptors: [usersDescriptor()],
      executor: {
        scan: async () => {
          throw new Error('boom')
        },
      },
      maxStateRows: 1e9,
      hooks: { onComplete: () => expect.unreachable('unexpected complete'), onDemote: (reason) => (demoted = reason) },
    })
    seed.start()
    await flush()
    expect(demoted).toBe('hydration-error')
  })
})

// ── abort — a late completion is inert ──────────────────────────────

describe('a late scan completion after abort is inert (never seeds a dead graph)', () => {
  it('the seed module: abort() before the scan resolves → neither hook fires', async () => {
    const scanDef = deferred<Row[]>()
    let completed = false
    let demoted = false
    const seed = createSeed({
      descriptors: [usersDescriptor()],
      executor: { scan: () => scanDef.promise },
      maxStateRows: 1e9,
      hooks: { onComplete: () => (completed = true), onDemote: () => (demoted = true) },
    })
    seed.start()
    seed.abort()
    scanDef.resolve([{ id: 1, team_id: 1, score: 5 }])
    await flush()
    expect(completed).toBe(false)
    expect(demoted).toBe(false)
  })

  it('destroy during the seed: a late scan never flips a destroyed graph live', async () => {
    const { executor, calls } = scanQueue()
    const graph = createLiveGraph(statefulSpec(executor))
    expect(graph.state()).toBe('seeding')
    graph.destroy()
    expect(graph.state()).toBe('destroyed')
    calls[0]!.resolve([{ id: 1 }])
    await flush()
    expect(graph.state()).toBe('destroyed')
  })
})
