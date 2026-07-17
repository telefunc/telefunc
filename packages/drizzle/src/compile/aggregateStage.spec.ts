import { asc, avg, count, gt, max, min, sql, sum } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { extractQueryShape } from '../extract/queryShape.js'
import { type Change, type CompiledGraph, compileQuery } from './compile.js'

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  score: pg.integer('score'),
  name: pg.text('name'),
})
const qb = new pg.QueryBuilder()

const run = (builder: unknown): CompiledGraph =>
  compileQuery(extractQueryShape(builder, { dialect: 'pg' })).instantiate()
const ins = (row: Record<string, unknown>): Change => ({ table: 'users', kind: 'insert', new: row })
const del = (row: Record<string, unknown>): Change => ({ table: 'users', kind: 'delete', old: row })
const upd = (old: Record<string, unknown>, next: Record<string, unknown>): Change => ({
  table: 'users',
  kind: 'update',
  old,
  new: next,
})
const seed = (graph: CompiledGraph, ...commits: Change[][]): void => {
  for (const commit of commits) graph.apply(commit)
}

describe('aggregateStage — GROUP BY + COUNT', () => {
  it('count(*) crossing a group boundary fires; an unrelated group insert does not', () => {
    const graph = run(qb.select({ team: users.teamId, n: count() }).from(users).groupBy(users.teamId))
    seed(graph, [ins({ id: 1, team_id: 5, score: 10, name: 'a' })])
    expect(graph.apply([ins({ id: 2, team_id: 5, score: 20, name: 'b' })]).invalidated).toBe(true) // group 5: 1→2
  })

  it('a no-op update to an unselected/non-grouping column does not fire', () => {
    const graph = run(qb.select({ team: users.teamId, n: count() }).from(users).groupBy(users.teamId))
    seed(graph, [ins({ id: 1, team_id: 5, score: 10, name: 'a' })])
    expect(
      graph.apply([upd({ id: 1, team_id: 5, score: 10, name: 'a' }, { id: 1, team_id: 5, score: 10, name: 'z' })])
        .invalidated,
    ).toBe(false)
  })

  it('count(*) counts NULLs but count(col) ignores them (real empty baseline)', () => {
    const starCount = run(qb.select({ n: count() }).from(users)) // empty baseline
    starCount.apply([ins({ id: 1, team_id: 5, score: 10, name: 'a' })]) // 0→1
    // count(*) counts a NULL-name row → 1→2 → fire
    expect(starCount.apply([ins({ id: 2, team_id: 5, score: null, name: null })]).invalidated).toBe(true)

    const colCount = run(qb.select({ n: count(users.name) }).from(users)) // empty baseline
    // count(name) over empty is 0; a FIRST NULL-name row leaves it 0 → NO fire
    expect(colCount.apply([ins({ id: 1, team_id: 5, score: 10, name: null })]).invalidated).toBe(false)
    // a non-NULL name → 0→1 → fire
    expect(colCount.apply([ins({ id: 2, team_id: 5, score: 10, name: 'x' })]).invalidated).toBe(true)
  })

  it('adding a NULL to sum/avg does not fire, even over an EMPTY baseline', () => {
    // Regression: without the seeded unit-group zero state the FIRST null row would
    // over-fire (materializing sum=NULL for the first time). The genesis seed makes the empty
    // baseline already hold sum=NULL, so a first NULL row emits no delta.
    const graph = run(qb.select({ s: sum(users.score), a: avg(users.score) }).from(users)) // EMPTY baseline
    expect(graph.apply([ins({ id: 1, team_id: 5, score: null, name: 'a' })]).invalidated).toBe(false)
    // a real value DOES change sum/avg → fire
    expect(graph.apply([ins({ id: 2, team_id: 5, score: 30, name: 'c' })]).invalidated).toBe(true)
    // adding another NULL still no-ops
    expect(graph.apply([ins({ id: 3, team_id: 5, score: null, name: 'd' })]).invalidated).toBe(false)
  })
})

describe('aggregateStage — aggregate routes through window/projection', () => {
  it('GROUP BY id ORDER BY id LIMIT 1: a below-window group insert does NOT fire; a window change does', () => {
    // Regression: aggregate output must flow through topK, so a group strictly below
    // the top-1 window (total group order) leaves the SQL result unchanged → no fire.
    const graph = run(
      qb.select({ id: users.id, n: count() }).from(users).groupBy(users.id).orderBy(asc(users.id)).limit(1),
    )
    seed(graph, [ins({ id: 1, team_id: 5, score: 1, name: 'a' })])
    // id=2 is strictly below the top-1 window → SQL result unchanged → NOT invalidated
    expect(graph.apply([ins({ id: 2, team_id: 5, score: 1, name: 'b' })]).invalidated).toBe(false)
    // id=0 enters the window (displaces id=1) → SQL top-1 changes → fire
    expect(graph.apply([ins({ id: 0, team_id: 5, score: 1, name: 'z' })]).invalidated).toBe(true)
    // deleting the in-window group fires too (the routing did not over-suppress)
    expect(graph.apply([del({ id: 0, team_id: 5, score: 1, name: 'z' })]).invalidated).toBe(true)
  })
})

describe('aggregateStage — ungrouped empty baseline', () => {
  it('a first NULL row into sum / count(col) over an EMPTY graph does not fire; delete-to-empty still fires', () => {
    const s = run(qb.select({ s: sum(users.score) }).from(users)) // empty
    expect(s.apply([ins({ id: 1, team_id: 5, score: null, name: 'a' })]).invalidated).toBe(false) // NULL→NULL
    const c = run(qb.select({ c: count(users.name) }).from(users)) // empty
    expect(c.apply([ins({ id: 1, team_id: 5, score: 1, name: null })]).invalidated).toBe(false) // 0→0
    const d = run(qb.select({ s: sum(users.score) }).from(users))
    seed(d, [ins({ id: 1, team_id: 5, score: 5, name: 'a' })])
    expect(d.apply([del({ id: 1, team_id: 5, score: 5, name: 'a' })]).invalidated).toBe(true) // 5→NULL
  })
})

describe('aggregateStage — MIN/MAX extremum removal', () => {
  it('removing the current MIN reveals the next value', () => {
    const graph = run(qb.select({ m: min(users.score) }).from(users))
    seed(graph, [ins({ id: 1, team_id: 5, score: 3, name: 'a' })], [ins({ id: 2, team_id: 5, score: 7, name: 'b' })])
    // MIN is 3; removing id 1 reveals 7 → fire
    expect(graph.apply([del({ id: 1, team_id: 5, score: 3, name: 'a' })]).invalidated).toBe(true)
    // removing a non-extremum below-nothing element that is now the min... insert then remove a larger one
    const gmax = run(qb.select({ m: max(users.score) }).from(users))
    seed(gmax, [ins({ id: 1, team_id: 5, score: 3, name: 'a' })], [ins({ id: 2, team_id: 5, score: 7, name: 'b' })])
    expect(gmax.apply([del({ id: 2, team_id: 5, score: 7, name: 'b' })]).invalidated).toBe(true) // 7→3
  })
})

describe('aggregateStage — empty-set + GROUP BY NULL', () => {
  it('deleting the last row transitions count(*) to 0 (exact) and re-inserting fires again', () => {
    const graph = run(qb.select({ n: count() }).from(users))
    seed(graph, [ins({ id: 1, team_id: 5, score: 10, name: 'a' })])
    expect(graph.apply([del({ id: 1, team_id: 5, score: 10, name: 'a' })]).invalidated).toBe(true) // 1→0
    expect(graph.apply([ins({ id: 2, team_id: 5, score: 20, name: 'b' })]).invalidated).toBe(true) // 0→1
  })

  it('a NULL grouping value forms its own group', () => {
    const graph = run(qb.select({ team: users.teamId, n: count() }).from(users).groupBy(users.teamId))
    seed(graph, [ins({ id: 1, team_id: null, score: 10, name: 'a' })])
    // second NULL-team row joins the NULL group → count 1→2 → fire
    expect(graph.apply([ins({ id: 2, team_id: null, score: 20, name: 'b' })]).invalidated).toBe(true)
  })
})

describe('aggregateStage — opaque GROUP BY witnessed at the input adapter (blocker #3)', () => {
  it('a row moving between hidden groups (parity flip) fires the pre-reduce dirty witness, not the data path', () => {
    // GROUP BY (score % 2) → groupByOpaque: all rows collapse into ONE fake group, so a move between
    // real groups leaves the fake group's count(*) unchanged and the post-grouping DATA path cannot
    // observe it (data === false — the reduce cancellation). The base input is marked dirtyActive, so
    // the input adapter witnesses the full-row change (dirty === true). Asserting data === false pins
    // the counterexample: an unrelated data over-fire cannot false-green this test.
    const graph = run(qb.select({ n: count() }).from(users).groupBy(sql`${users.score} % 2`))
    seed(
      graph,
      [ins({ id: 1, team_id: 5, score: 2, name: 'a' })], // even
      [ins({ id: 2, team_id: 5, score: 4, name: 'b' })], // even → even group = 2
      [ins({ id: 3, team_id: 5, score: 1, name: 'c' })], // odd
      [ins({ id: 4, team_id: 5, score: 3, name: 'd' })], // odd  → odd group  = 2
    )
    // Move id=1 even→odd (score 2→5): SQL per-group count(*) bag {2,2} → {1,3} → result changes → must fire.
    const result = graph.apply([
      upd({ id: 1, team_id: 5, score: 2, name: 'a' }, { id: 1, team_id: 5, score: 5, name: 'a' }),
    ])
    // the data path alone misses the move (reduce cancellation); the pre-reduce dirty witness (input
    // adapter) rescues it → fires. The oracle differential pins that this move is never missed.
    expect(result.invalidated).toBe(true)
  })
})

describe('aggregateStage — HAVING + DISTINCT', () => {
  it('unknown HAVING (aggregate comparison) widens and taps dirty after the aggregate', () => {
    const graph = run(
      qb.select({ team: users.teamId, n: count() }).from(users).groupBy(users.teamId).having(gt(count(), 2)),
    )
    seed(graph, [ins({ id: 1, team_id: 5, score: 10, name: 'a' })])
    // a group-touching change fires after the aggregate (HAVING over count() is unprovable → dirty)
    expect(graph.apply([ins({ id: 2, team_id: 5, score: 20, name: 'b' })]).invalidated).toBe(true)
  })

  it('count(DISTINCT col) is exact — a duplicate value does not change it, a new one does', () => {
    const graph = run(qb.select({ n: sql<number>`count(distinct ${users.score})`.mapWith(Number) }).from(users))
    seed(graph, [ins({ id: 1, team_id: 5, score: 10, name: 'a' })])
    // the parser marks a raw sql() count(distinct) opaque → dirty degradation is acceptable here;
    // assert only that a real change never misses.
    expect(graph.apply([ins({ id: 2, team_id: 5, score: 20, name: 'b' })]).invalidated).toBe(true)
  })
})
