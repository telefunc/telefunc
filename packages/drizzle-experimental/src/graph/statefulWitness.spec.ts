// A DIRTY-ONLY WITNESS in a STATEFUL graph must fire through the LIVE path — the defect this pins.
//
// A subquery inner table that is not a base input (a scalar/EXISTS subquery on an uncovered table) is
// registered as a dirty-only witness: it has no seed and no dataflow sink, and a real change to it dirties
// the batch. The COMPILED graph's `apply` pumps every registry feed, witnesses included, so the compile-tier
// tests (composition/existsStage) saw it fire. But the LIVE stateful driver does NOT call `apply`: it feeds
// each SEED input by descriptor via `feedInput`, and a witness has no descriptor — so a change to a witness
// table reached `applyLive`, matched no descriptor, and was silently dropped. A missed invalidation, on an
// ordinary query (a stateful query with a scalar subquery), not an exotic one.
//
// The fix wires witnesses into the live path (`feedDirtyWitness`). This is the live-path regression test:
// disabling that call reproduces the miss (the first case goes red), and the CONTROL — a change to the
// seeded base input — proves the graph is genuinely live and firing, so the first case is not passing
// because the graph is inert.

import { gt, sql } from 'drizzle-orm'
import { describe, expect, it } from 'vitest'
import { type StatefulGraph, compileQuery } from '../compile/compile.js'
import { extractQueryShape } from '../extract/queryShape.js'
import type { HydrationExecutor } from './hydrate.js'
import { type LiveGraph, createLiveGraph } from './liveGraph.js'
import { qb, settle, teams, users } from './liveGraph.testKit.js'

// Aggregate (GROUP BY → stateful) whose projection carries a scalar subquery on `teams`, which is NOT a base
// input of the outer query — so `buildBranch` registers `teams` as a dirty witness. Rows use PHYSICAL column
// names, or a change is silently inert against the compiled graph.
const build = () =>
  qb
    .select({ team: users.teamId, teamCount: sql`(${qb.select({ n: sql`count(*)` }).from(teams)})` })
    .from(users)
    .groupBy(users.teamId)

const usersOnly: HydrationExecutor = {
  scan: async (descriptor) => (descriptor.table === 'users' ? [{ id: 1, team_id: 5 }] : []),
}

async function seededWitnessGraph(): Promise<LiveGraph> {
  const graph = createLiveGraph({
    kind: 'stateful',
    instanceKey: 'stateful-witness',
    tables: ['users', 'teams'],
    instantiate: () => compileQuery(extractQueryShape(build(), { dialect: 'pg' })).instantiate() as StatefulGraph,
    executor: usersOnly,
    maxStateRows: 1e9,
  })
  await settle(graph)
  expect(graph.state()).toBe('live') // seeded from `users`, teams is a witness (never scanned)
  return graph
}

describe('a stateful graph invalidates on a change to its dirty-only witness (live path)', () => {
  it('a change to the scalar-subquery inner table fires the live query', async () => {
    const graph = await seededWitnessGraph()
    // `teams` feeds no dataflow and holds no shadow; the subquery's value can move on any real change to it,
    // so this must invalidate. On the pre-fix live path it did not — the miss.
    expect(graph.apply([{ table: 'teams', kind: 'insert', new: { id: 9, region: 'x' } }]).invalidated).toBe(true)
  })

  it('CONTROL: a change to the seeded base input still fires (the graph is genuinely live, not inert)', async () => {
    const graph = await seededWitnessGraph()
    expect(graph.apply([{ table: 'users', kind: 'insert', new: { id: 2, team_id: 7 } }]).invalidated).toBe(true)
  })

  it('CONTROL: a witness change that is a pure replay does NOT fire (dirty, not blanket)', async () => {
    // The witness fires on a REAL change, not on any event: an update whose row is unchanged is a replay and
    // must stay quiet, mirroring the compiled graph's `rowChanged` witness gate.
    const graph = await seededWitnessGraph()
    const row = { id: 9, region: 'x' }
    expect(graph.apply([{ table: 'teams', kind: 'update', old: row, new: { ...row } }]).invalidated).toBe(false)
  })
})

// ── the CROSS-ROLE case: one relation, both roles ───────────────────
//
// `covered` is computed PER BRANCH but the graph's feeds are per GRAPH, so under a set-op the SAME relation
// can be a base input of one arm and a subquery-only mention of another. Here `users` is a seed of arm 1 and
// a witness of arm 2. The roles are independent and BOTH must fire — which is why they are two collections
// rather than one feed list distinguished by "does it have a builder". Under that older shape the code
// happened to behave, but only because the test was feed-level; nothing made it structural.

const crossRole = () =>
  qb
    .select({ id: users.id })
    .from(users)
    .where(gt(users.id, 100)) // arm 1: `users` is a SEED, with an exact σ that excludes id ≤ 100
    .union(
      qb
        .select({ id: teams.id })
        .from(teams)
        .where(sql`(select count(*) from ${users}) > 0`), // arm 2: `users` appears ONLY in a subquery → WITNESS
    )

const compiledCrossRole = (): StatefulGraph =>
  compileQuery(extractQueryShape(crossRole(), { dialect: 'pg' })).instantiate() as StatefulGraph

describe('a relation that is a SEED in one set-op arm and a WITNESS in another', () => {
  it('registers the seed role — `users` is a hydrated input, not only a witness', () => {
    expect(compiledCrossRole().seeds.some((descriptor) => descriptor.relationId === 'users')).toBe(true)
  })

  it('fires the witness role for a row arm 1 excludes — attributable to the witness alone', () => {
    // id=1 fails arm 1's exact `id > 100`, so the SEED role contributes no data and no dirty. The only thing
    // that can invalidate here is the witness role of the same relation. Collapse the two roles into one
    // "is `users` a seed?" question and this goes false — a missed invalidation.
    expect(compiledCrossRole().apply([{ table: 'users', kind: 'insert', new: { id: 1 } }]).invalidated).toBe(true)
  })

  it('CONTROL: a pure replay on the cross-role relation stays quiet', () => {
    // Proves the case above is not passing because the graph fires on everything.
    const row = { id: 1 }
    expect(compiledCrossRole().apply([{ table: 'users', kind: 'update', old: row, new: { ...row } }]).invalidated).toBe(
      false,
    )
  })

  it('the LIVE path fires the witness role too — same rule, the other entry point', async () => {
    const graph = createLiveGraph({
      kind: 'stateful',
      instanceKey: 'cross-role',
      tables: ['users', 'teams'],
      instantiate: compiledCrossRole,
      executor: { scan: async () => [] },
      maxStateRows: 1e9,
    })
    await settle(graph)
    expect(graph.state()).toBe('live')
    expect(graph.apply([{ table: 'users', kind: 'insert', new: { id: 1 } }]).invalidated).toBe(true)
  })
})
