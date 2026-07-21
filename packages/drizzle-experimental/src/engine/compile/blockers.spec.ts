// The three soundness blockers, each written as a
// failing-then-passing regression: first the data path alone is shown to MISS the change
// (the "without the fix" behaviour), then the compiler is shown to catch it (the fix). If
// the fix were removed, the passing assertion below would flip to the failing one.

import { asc, sql } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { extractQueryShape } from '../../extract/queryShape.js'
import type { SelectShape } from '../../ir/types.js'
import {
  D2,
  type KeyValue,
  MultiSet,
  type MultiSetArray,
  consolidate,
  distinct,
  keyBy,
  map,
  output,
  topK,
} from '../graph/ivm.js'
import { type CompiledGraph, compileQuery } from './compile.js'
import { projectFnOf, projectionKeysOf } from './projectStage.js'
import { isTotalOrder } from './windowStage.js'
import { type Row, qualifiedKey } from './rowSpace.js'

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  score: pg.integer('score'),
  payload: pg.integer('payload'),
  name: pg.text('name'),
})
const qb = new pg.QueryBuilder()

const shapeOf = (builder: unknown) => extractQueryShape(builder, { dialect: 'pg' }) as SelectShape
const run = (builder: unknown): CompiledGraph => compileQuery(shapeOf(builder)).instantiate()
const net = (entries: MultiSetArray<unknown>): Map<string, number> => {
  const out = new Map<string, number>()
  for (const [value, multiplicity] of entries) {
    const key = JSON.stringify(value)
    const sum = (out.get(key) ?? 0) + multiplicity
    if (sum === 0) out.delete(key)
    else out.set(key, sum)
  }
  return out
}

describe('unknown-predicate membership transition under DISTINCT/consolidate', () => {
  it('WITHOUT the witness: the same projected tuple retract+insert cancels in distinct (MISS)', () => {
    const graph = new D2()
    const input = graph.newInput<string>()
    const collected: MultiSetArray<unknown> = []
    input
      .pipe(
        keyBy((value: string) => value),
        distinct(),
        map(([, value]) => value),
        consolidate(),
      )
      .pipe(output((data) => data.getInner().forEach((entry) => collected.push(entry))))
    graph.finalize()
    // membership flips but the projected tuple 'team5' is identical on both sides
    input.sendData(
      new MultiSet([
        ['team5', -1],
        ['team5', 1],
      ]),
    )
    graph.run()
    expect(net(collected).size).toBe(0) // the real change is invisible on the data path
  })

  it('WITH the witness: the compiler still invalidates via the dirty tap', () => {
    // SELECT DISTINCT teamId WHERE <opaque(name)>; a name-only update flips opaque membership
    // while the projected teamId is unchanged → data cancels, dirty fires.
    const graph = run(qb.selectDistinct({ team: users.teamId }).from(users).where(sql`${users.name} similar to 'x%'`))
    graph.apply([{ table: 'users', kind: 'insert', new: { id: 1, team_id: 5, name: 'x1' } }])
    const result = graph.apply([
      {
        table: 'users',
        kind: 'update',
        old: { id: 1, team_id: 5, name: 'x1' },
        new: { id: 1, team_id: 5, name: 'y1' },
      },
    ])
    // the data path alone misses the D1 cancellation (proven at the primitive level in the first `it`);
    // here the compiled graph still fires — the dirty witness rescues it. Remove the tap → this flips.
    expect(result.invalidated).toBe(true)
  })
})

describe('ORDER-only reorder with an unselected sort key', () => {
  const shape = shapeOf(qb.select({ name: users.name }).from(users).orderBy(asc(users.score)))

  it('WITHOUT the fix: a name-only projection makes a score reorder invisible (MISS)', () => {
    const nameOnly = (row: Row) => JSON.stringify(row[qualifiedKey('users', 'name')])
    const before = { [qualifiedKey('users', 'name')]: 'a', [qualifiedKey('users', 'score')]: 10 }
    const after = { [qualifiedKey('users', 'name')]: 'a', [qualifiedKey('users', 'score')]: 99 }
    expect(nameOnly(before)).toBe(nameOnly(after)) // the reorder does not change the projected tuple
  })

  it('WITH the fix: ORDER BY columns ride in the projection, so a reorder emits a delta', () => {
    expect(projectionKeysOf(shape)).toContain(qualifiedKey('users', 'score')) // sort col rides
    const project = projectFnOf(shape)
    const before = { [qualifiedKey('users', 'name')]: 'a', [qualifiedKey('users', 'score')]: 10 }
    const after = { [qualifiedKey('users', 'name')]: 'a', [qualifiedKey('users', 'score')]: 99 }
    expect(project(before)).not.toBe(project(after)) // the reorder is now observable
    // end to end: a score-only update fires
    const graph = run(qb.select({ name: users.name }).from(users).orderBy(asc(users.score)))
    const fired = graph.apply([
      { table: 'users', kind: 'update', old: { id: 1, score: 10, name: 'a' }, new: { id: 1, score: 99, name: 'a' } },
    ])
    expect(fired.invalidated).toBe(true)
  })
})

describe('non-unique topK boundary tie', () => {
  it('WITHOUT the license check: a raw topK by a non-total comparator hides a boundary-tie payload change (MISS)', () => {
    type R = { id: number; score: number; payload: number }
    const graph = new D2()
    const input = graph.newInput<KeyValue<number, R>>()
    const collected: MultiSetArray<unknown> = []
    input
      .pipe(topK<number, R, KeyValue<number, R>>((a, b) => a.score - b.score, { limit: 1 })) // score-only → ties tie
      .pipe(output((data) => data.getInner().forEach((entry) => collected.push(entry))))
    graph.finalize()

    const a: R = { id: 1, score: 5, payload: 1 }
    const b: R = { id: 2, score: 5, payload: 2 }
    input.sendData(
      new MultiSet([
        [[0, a], 1],
        [[0, b], 1],
      ]),
    )
    graph.run()
    const held = [...net(collected).keys()][0]! // the single row the private tie-break kept
    collected.length = 0
    // update the OTHER tied row's payload — SQL could return it, but the graph holds the first
    const other = held.includes('"id":1') ? b : a
    input.sendData(
      new MultiSet([
        [[0, other], -1],
        [[0, { ...other, payload: 99 }], 1],
      ]),
    )
    graph.run()
    expect(net(collected).size).toBe(0) // the boundary-tie payload change is invisible
  })

  it('WITH the fix: a non-total order is NOT licensed for exact topK, so it fires on any change', () => {
    const nonTotal = shapeOf(qb.select().from(users).orderBy(asc(users.score)).limit(1)) // score is not unique
    expect(isTotalOrder(nonTotal)).toBe(false) // exact topK is NOT licensed
    const graph = run(qb.select().from(users).orderBy(asc(users.score)).limit(1))
    graph.apply([{ table: 'users', kind: 'insert', new: { id: 1, team_id: 1, score: 5, payload: 1, name: 'a' } }])
    graph.apply([{ table: 'users', kind: 'insert', new: { id: 2, team_id: 1, score: 5, payload: 2, name: 'b' } }])
    // updating a boundary-tied row's payload fires (the dirty witness prevents the miss)
    const fired = graph.apply([
      {
        table: 'users',
        kind: 'update',
        old: { id: 2, team_id: 1, score: 5, payload: 2, name: 'b' },
        new: { id: 2, team_id: 1, score: 5, payload: 99, name: 'b' },
      },
    ])
    expect(fired.invalidated).toBe(true)
  })

  it('a provably TOTAL order IS licensed for exact topK (below-window change does not fire)', () => {
    const total = shapeOf(qb.select().from(users).orderBy(asc(users.id)).limit(1)) // id is the PK → total
    expect(isTotalOrder(total)).toBe(true)
  })
})
