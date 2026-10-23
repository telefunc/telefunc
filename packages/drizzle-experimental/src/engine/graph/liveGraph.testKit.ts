// Shared fixtures for the live-graph specs (lifecycle / apply).
//
// SHARED because both specs drive the SAME real compiled `users ⋈ teams` graph. Everything here is
// deterministic — deferred/immediate promises drive the seed, never timers — and the graphs are REAL compiled
// join graphs rather than fakes wherever the case is about behaviour rather than about born-state.
//
// NOTE ON ROW SHAPE: rows are written with PHYSICAL column names (`team_id`), not drizzle's property names
// (`teamId`). A change row keyed by property name is silently INERT against a compiled graph — nothing
// matches the predicate, every case reports "not invalidated", and a test can appear to pass for a reason
// that has nothing to do with what it claims to pin.

import { eq } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { expect } from 'vitest'
import { type SeedDescriptor, type StatefulGraph, compileQuery } from '../compile/compile.js'
import type { Row } from '../compile/rowSpace.js'
import { extractQueryShape } from '../../drizzle/extract/queryShape.js'
import type { HydrationExecutor } from './hydrate.js'
import { type LiveGraph, type LiveGraphSpec, createLiveGraph } from './liveGraph.js'

export const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  score: pg.integer('score'),
})
export const teams = pg.pgTable('teams', { id: pg.integer('id').primaryKey(), region: pg.text('region') })
export const qb = new pg.QueryBuilder()

export const settle = (graph: LiveGraph): Promise<void> => graph.ready() // resolves when the seed lands (→ live / coarse)

export const neverExecutor = (): HydrationExecutor => ({
  scan: () => new Promise<Row[]>(() => {}), // never resolves → stays 'seeding'
})

export function fakeSeed(inputId: string, table: string, primaryKey: string[] = ['id']): SeedDescriptor {
  return { inputId, table, relationId: table, alias: inputId, primaryKey, columns: '*', residual: { kind: 'true' } }
}
export function statefulFake(seeds: SeedDescriptor[]): StatefulGraph {
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

// ── a real compiled users ⋈ teams graph, seeded to live through its HydrationExecutor ──

export function joinExecutor(usersRows: Row[], teamsRows: Row[]): HydrationExecutor {
  return { scan: async (descriptor) => (descriptor.table === 'users' ? usersRows : teamsRows) }
}
export async function seededJoin(usersRows: Row[], teamsRows: Row[]): Promise<LiveGraph> {
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
