// STEADY-STATE APPLY: what a graph that is already live does with a change.
//
// The classification question, across all three kinds. Given a change, does the graph decide correctly
// whether its result moved — sparing the query when nothing it selects changed, and firing when something
// did? An old-inline retraction resolves without a shadow consult (B2a); a PK-less INSERT is classified
// rather than blanket-fired; both images let a STATELESS graph decide membership instead of assuming it; a
// graph fires at most once per batch (G2); a coarse graph fires only for tables it watches; and only a
// σ-matching tuple enters the shadow.
//
// How a graph REACHES the live state, and how it leaves it, is `liveGraph.lifecycle.spec.ts`'s subject.

import { eq, gt } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { type StatefulGraph, compileQuery } from '../compile/compile.js'
import { extractQueryShape } from '../extract/queryShape.js'
import { type LiveGraph, createLiveGraph } from './liveGraph.js'
import {
  fakeSeed,
  joinExecutor,
  neverExecutor,
  qb,
  seededJoin,
  settle,
  statefulFake,
  teams,
  users,
} from './liveGraph.testKit.js'

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

// ── where the no-PK INSERT win actually lands (premise audit #4/H4) ──
//
// Write capture now emits a precise `insert` for a table with no primary key: an insert carries its whole
// row and retracts nothing, so no key is needed to describe it. This is the consumer side of that claim,
// and it is deliberately split, because the win is real for one graph kind and hollow for the other.

describe('a PK-less table: a precise INSERT is classified, not blanket-fired', () => {
  const events = pg.pgTable('events', { level: pg.text('level'), msg: pg.text('msg') }) // NO primary key
  const statelessOverEvents = (): LiveGraph =>
    createLiveGraph({
      kind: 'stateless',
      instanceKey: 'nokey',
      tables: ['events'],
      instantiate: () =>
        compileQuery(
          extractQueryShape(qb.select().from(events).where(eq(events.level, 'error')), { dialect: 'pg' }),
        ).instantiate() as never,
    })

  it('STATELESS: a non-matching insert does NOT invalidate — this is the win', async () => {
    const graph = statelessOverEvents()
    await settle(graph)
    // Before capture could describe this row, the same write arrived as `kind:'coarse'` and fired every
    // time. The control for that is the second assertion: coarse still fires, so the first one is not
    // passing because the graph is inert.
    expect(graph.apply([{ table: 'events', kind: 'insert', new: { level: 'info', msg: 'noise' } }]).invalidated).toBe(
      false,
    )
    expect(graph.apply([{ table: 'events', kind: 'insert', new: { level: 'error', msg: 'boom' } }]).invalidated).toBe(
      true,
    )
  })

  it('STATEFUL: no win — a PK-less input is still born coarse, and ignores the precision', async () => {
    // Stated rather than glossed: `startSeeding` refuses a PK-less input because a key-only retraction
    // could never shadow-resolve, so a stateful consumer coarsens independently of what capture emits.
    const graph = createLiveGraph({
      kind: 'stateful',
      instanceKey: 'nokey-stateful',
      tables: ['events'],
      instantiate: () => statefulFake([fakeSeed('events', 'events', [])]),
      executor: neverExecutor(),
      maxStateRows: 1e9,
    })
    await settle(graph)
    expect(graph.state()).toBe('coarse')
  })
})

// ── what the OLD image buys a stateless graph (premise audit #5/H5) ──

describe('a STATELESS graph with both images decides membership instead of assuming it', () => {
  // The rows below use PHYSICAL column names (`team_id`), not drizzle's property names (`teamId`). A change
  // row keyed by property name is silently INERT here: nothing matches the predicate, every case reports
  // "not invalidated", and the win appears to hold for a reason that has nothing to do with the old image.
  // Written the wrong way first, and it took the in→out case — which MUST fire — to expose it.
  const projected = (): LiveGraph =>
    createLiveGraph({
      kind: 'stateless',
      instanceKey: 'both-images',
      tables: ['users'],
      instantiate: () =>
        compileQuery(
          extractQueryShape(qb.select({ id: users.id, teamId: users.teamId }).from(users).where(eq(users.teamId, 10)), {
            dialect: 'pg',
          }),
        ).instantiate() as never,
    })
  const outside = { id: 1, team_id: 99, score: 1 }
  const inside = { id: 2, team_id: 10, score: 1 }

  it('THE WIN: an update between two rows it does not want fires nothing once the old image is there', async () => {
    const graph = projected()
    await settle(graph)
    // Both images: the graph sees a row that was outside the predicate and stayed outside.
    expect(
      graph.apply([{ table: 'users', kind: 'update', old: outside, new: { ...outside, score: 2 } }]).invalidated,
    ).toBe(false)
    // The control — the SAME update with only the new image fires. `statelessApply` coarsens an image-less
    // change before the evaluator ever sees it: without the old row this could equally have been a row
    // LEAVING the set, and a stateless graph has no state to ask. That is the over-fire the new+old-PK
    // envelope was stuck with, and it is what the old image buys off.
    expect(graph.apply([{ table: 'users', kind: 'update', new: { ...outside, score: 3 } }]).invalidated).toBe(true)
  })

  it('an update that MOVES a row out of the set still fires — exact is not the same as quiet', async () => {
    const graph = projected()
    await settle(graph)
    expect(
      graph.apply([{ table: 'users', kind: 'update', old: inside, new: { ...inside, team_id: 99 } }]).invalidated,
    ).toBe(true)
    // …and an update WITHIN the set that changes nothing it selects does not.
    expect(
      graph.apply([{ table: 'users', kind: 'update', old: inside, new: { ...inside, score: 5 } }]).invalidated,
    ).toBe(false)
  })

  it('a DELETE carrying the removed row is decided too; a key-only one can only coarsen', async () => {
    const graph = projected()
    await settle(graph)
    expect(graph.apply([{ table: 'users', kind: 'delete', old: outside, key: { id: 1 } }]).invalidated).toBe(false)
    expect(graph.apply([{ table: 'users', kind: 'delete', old: inside, key: { id: 2 } }]).invalidated).toBe(true)
    // The control: the same retraction with only a key is image-less, so it coarsens.
    expect(graph.apply([{ table: 'users', kind: 'delete', key: { id: 1 } }]).invalidated).toBe(true)
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

describe('a coarse graph fires only for the tables it WATCHES', () => {
  it('a change on an unwatched table does not invalidate it', () => {
    // Sound-but-noisy rather than unsound: the failure mode here is over-firing, not a wrong row. Pinned
    // anyway, because "every write on the database invalidates every coarse graph" is a performance cliff
    // that no test would have reported.
    const graph = createLiveGraph({ kind: 'coarse', instanceKey: 'coarse-watch', tables: ['users'] })

    expect(graph.apply([{ table: 'orders', kind: 'insert', new: { id: 1 } }]).invalidated).toBe(false)
    expect(graph.invalidationSeq()).toBe(0)
    // …and the positive half, so the negative above is not passing vacuously.
    expect(graph.apply([{ table: 'users', kind: 'insert', new: { id: 1 } }]).invalidated).toBe(true)
  })
})

describe('only a σ-MATCHING new tuple enters the shadow', () => {
  it('a row updated OUT of the residual leaves the shadow, so it stops counting against maxStateRows', async () => {
    // WHERE THIS IS OBSERVABLE, and where it is not. The operator graph enforces the same σ, so a ghost
    // tuple left in the shadow is filtered out of every apply() outcome — its insert and its retraction
    // are both no-ops, and the invalidation results are identical either way. Asserting on `invalidated`
    // therefore CANNOT tell the two apart (measured: the mutation survives such a test), and shipping one
    // would be verification theatre.
    //
    // The shadow's SIZE is the honest observable: it is what `maxStateRows` bounds, so a shadow that
    // keeps rows the input no longer contains demotes a graph that should still be live — precision lost
    // to bookkeeping rather than to any real growth in watched state.
    const build = () => qb.select().from(users).innerJoin(teams, eq(teams.id, users.teamId)).where(gt(users.score, 10))
    const graph = createLiveGraph({
      kind: 'stateful',
      instanceKey: 'sigma-shadow',
      tables: ['users', 'teams'],
      instantiate: () => compileQuery(extractQueryShape(build(), { dialect: 'pg' })).instantiate() as StatefulGraph,
      executor: joinExecutor(
        [
          { id: 1, team_id: 5, score: 50 },
          { id: 2, team_id: 5, score: 50 },
        ],
        [{ id: 5 }],
      ),
      maxStateRows: 2,
    })
    await settle(graph)
    expect(graph.state()).toBe('live') // seeded at exactly the bound, which is not over it

    // id=1 moves OUT of `score > 10`. It is no longer part of this input, so it must leave the shadow.
    graph.apply([
      { table: 'users', kind: 'update', old: { id: 1, team_id: 5, score: 50 }, new: { id: 1, team_id: 5, score: 5 } },
    ])
    // Room for id=3 exists ONLY if id=1 actually left. A shadow still holding it is now 3 rows over a
    // bound of 2, and the graph demotes for state it does not really have.
    graph.apply([{ table: 'users', kind: 'insert', new: { id: 3, team_id: 5, score: 50 } }])

    expect(graph.state()).toBe('live')
  })
})
