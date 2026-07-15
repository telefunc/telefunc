// T4.C1 — one named case per §5.4 composition-table row, each asserting the dirty tap at
// the exact frontier the table specifies. Over-firing is licensed for a degradation, but
// a miss never is; the frontier cases below check the tap is placed no wider than stated.

import { and, eq, count, gt, sql } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { extractQueryShape } from '../extract/queryShape.js'
import { type Change, type CompiledGraph, compileQuery } from './compile.js'

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  name: pg.text('name'),
})
const teams = pg.pgTable('teams', { id: pg.integer('id').primaryKey(), region: pg.text('region') })
const qb = new pg.QueryBuilder()

const run = (builder: unknown): CompiledGraph =>
  compileQuery(extractQueryShape(builder, { dialect: 'pg' })).instantiate()
const insUser = (row: Record<string, unknown>): Change => ({ table: 'users', kind: 'insert', new: row })
const insTeam = (row: Record<string, unknown>): Change => ({ table: 'teams', kind: 'insert', new: row })
const upd = (old: Record<string, unknown>, next: Record<string, unknown>): Change => ({
  table: 'users',
  kind: 'update',
  old,
  new: next,
})
const seed = (graph: CompiledGraph, ...commits: Change[][]): void => {
  for (const commit of commits) graph.apply(commit)
}
const opaqueName = sql`${users.name} similar to 'x%'`

describe('§5.4 — A AND unknown → tap AFTER A (T4.C1)', () => {
  it('an opaque-only change fires dirty only inside the A region', () => {
    const graph = run(
      qb
        .select({ team: users.teamId })
        .from(users)
        .where(and(eq(users.teamId, 5), opaqueName)),
    )
    // teamId 5 (in A), opaque column changes → dirty (tap after A)
    expect(graph.apply([upd({ id: 1, team_id: 5, name: 'a' }, { id: 1, team_id: 5, name: 'b' })]).dirty).toBe(true)
    // teamId 3 (fails A), same opaque change → NOT dirty (the tap sits after A)
    expect(graph.apply([upd({ id: 2, team_id: 3, name: 'a' }, { id: 2, team_id: 3, name: 'b' })]).dirty).toBe(false)
  })
})

describe('§5.4 — A OR unknown / NOT unknown → tap BEFORE (T4.C1)', () => {
  it('A OR unknown fires dirty on any change', () => {
    const graph = run(qb.select().from(users).where(sql`${users.teamId} = 5 or ${opaqueName}`))
    expect(graph.apply([insUser({ id: 1, team_id: 999, name: 'z' })]).dirty).toBe(true)
  })
  it('NOT unknown fires dirty on any change', () => {
    const graph = run(qb.select().from(users).where(sql`not (${opaqueName})`))
    expect(graph.apply([insUser({ id: 1, team_id: 1, name: 'q' })]).dirty).toBe(true)
  })
})

describe('§5.4 — equi-join + unknown residual → dirty at candidate pairs (T4.C1)', () => {
  it('a candidate (equi-matched) pair taps dirty; a non-matching change does not', () => {
    const graph = run(
      qb
        .select()
        .from(users)
        .innerJoin(teams, and(eq(teams.id, users.teamId), sql`${users.name} similar to ${teams.region}`)),
    )
    seed(graph, [insTeam({ id: 5, region: 'eu' })])
    expect(graph.apply([insUser({ id: 1, team_id: 5, name: 'a' })]).dirty).toBe(true) // candidate pair
    expect(graph.apply([insUser({ id: 2, team_id: 9, name: 'b' })]).dirty).toBe(false) // no equi match
  })
})

describe('§5.4 — no-equi-key join → dirty at BOTH σ-inputs (T4.C1)', () => {
  it('a change to either side of a cross join taps dirty', () => {
    const graph = run(qb.select().from(users).crossJoin(teams))
    expect(graph.apply([insUser({ id: 1, team_id: 5, name: 'a' })]).dirty).toBe(true)
    expect(graph.apply([insTeam({ id: 5, region: 'eu' })]).dirty).toBe(true)
  })
})

describe('§5.4 — unknown aggregate/HAVING → dirty AFTER the aggregate (T4.C1)', () => {
  it('a group-touching change taps dirty after the reduce (HAVING over count is unprovable)', () => {
    const graph = run(
      qb.select({ team: users.teamId, n: count() }).from(users).groupBy(users.teamId).having(gt(count(), 2)),
    )
    seed(graph, [insUser({ id: 1, team_id: 5, name: 'a' })])
    expect(graph.apply([insUser({ id: 2, team_id: 5, name: 'b' })]).dirty).toBe(true)
  })
})

describe('§5.4 — DISTINCT ON / set-op / window → dirty at input (T4.C1)', () => {
  it('DISTINCT ON taps dirty', () => {
    const graph = run(qb.selectDistinctOn([users.teamId]).from(users))
    expect(graph.apply([insUser({ id: 1, team_id: 5, name: 'a' })]).dirty).toBe(true)
  })
  it('INTERSECT taps dirty', () => {
    const graph = run(pg.intersect(qb.select({ id: users.id }).from(users), qb.select({ id: teams.id }).from(teams)))
    expect(graph.apply([insUser({ id: 1, team_id: 5, name: 'a' })]).dirty).toBe(true)
  })
  it('LIMIT without ORDER BY taps dirty on any matching change', () => {
    const graph = run(qb.select().from(users).limit(3))
    expect(graph.apply([insUser({ id: 1, team_id: 5, name: 'a' })]).dirty).toBe(true)
  })
})

describe('§5.4 — scalar subquery inner tables → dirty at the scalar input (T4.C1)', () => {
  it('a change to the scalar subquery inner table taps dirty', () => {
    const graph = run(qb.select({ id: users.id, c: sql`(${qb.select({ n: sql`count(*)` }).from(teams)})` }).from(users))
    expect(graph.apply([insTeam({ id: 7, region: 'x' })]).dirty).toBe(true)
  })
})

describe('§5.4 — opaque order/projection → dirty at input (T4.C1)', () => {
  it('an opaque projection taps dirty on any change past WHERE', () => {
    const graph = run(
      qb
        .select({ up: sql`upper(${users.name})` })
        .from(users)
        .where(eq(users.teamId, 5)),
    )
    expect(graph.apply([insUser({ id: 1, team_id: 5, name: 'a' })]).dirty).toBe(true)
    // a change outside WHERE does not fire (the tap sits at the projection input, after WHERE)
    expect(graph.apply([insUser({ id: 2, team_id: 3, name: 'b' })]).dirty).toBe(false)
  })
})

describe('§5.4 — event-tier bypass (T4.C1)', () => {
  it('a coarse plan invalidates directly, bypassing the dataflow', () => {
    const sub = qb.select({ id: users.id, teamId: users.teamId }).from(users).as('sub')
    const graph = run(qb.select().from(sub))
    const result = graph.apply([insUser({ id: 1, team_id: 5, name: 'a' })])
    expect(result.dirty).toBe(true)
    expect(result.data).toBe(false) // no dataflow — the invalidation is the event tier
  })
})
