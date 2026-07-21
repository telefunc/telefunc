import { and, eq, gt } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { extractQueryShape } from '../../extract/queryShape.js'
import { type Change, type CompiledGraph, compileQuery } from './compile.js'

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  name: pg.text('name'),
})
const teams = pg.pgTable('teams', {
  id: pg.integer('id').primaryKey(),
  region: pg.text('region'),
})
const qb = new pg.QueryBuilder()

const run = (builder: unknown): CompiledGraph =>
  compileQuery(extractQueryShape(builder, { dialect: 'pg' })).instantiate()
const insUser = (row: Record<string, unknown>): Change => ({ table: 'users', kind: 'insert', new: row })
const insTeam = (row: Record<string, unknown>): Change => ({ table: 'teams', kind: 'insert', new: row })
const delUser = (row: Record<string, unknown>): Change => ({ table: 'users', kind: 'delete', old: row })

/** Seed the graph's state without asserting on the (ignored) baseline fire. */
function seed(graph: CompiledGraph, ...commits: Change[][]): void {
  for (const commit of commits) graph.apply(commit)
}

describe('joinStage — INNER equi-join', () => {
  it('fires on a matching insert and not on a non-matching one', () => {
    const graph = run(qb.select().from(users).innerJoin(teams, eq(teams.id, users.teamId)))
    seed(graph, [insTeam({ id: 5, region: 'eu' })])
    expect(graph.apply([insUser({ id: 1, team_id: 5, name: 'a' })]).invalidated).toBe(true)
    // no team 9 exists → no matched pair
    expect(graph.apply([insUser({ id: 2, team_id: 9, name: 'b' })]).invalidated).toBe(false)
  })

  it('a NULL equi-key never matches (SQL NULL <> NULL)', () => {
    const graph = run(qb.select().from(users).innerJoin(teams, eq(teams.id, users.teamId)))
    // a team with NULL id and a user with NULL teamId must NOT join
    seed(graph, [insTeam({ id: null, region: 'x' })])
    expect(graph.apply([insUser({ id: 1, team_id: null, name: 'a' })]).invalidated).toBe(false)
  })

  it('retracts the matched pair when the driving row is deleted', () => {
    const graph = run(qb.select().from(users).innerJoin(teams, eq(teams.id, users.teamId)))
    seed(graph, [insTeam({ id: 5, region: 'eu' })], [insUser({ id: 1, team_id: 5, name: 'a' })])
    expect(graph.apply([delUser({ id: 1, team_id: 5, name: 'a' })]).invalidated).toBe(true)
  })
})

describe('joinStage — outer joins', () => {
  it('LEFT join null-extends an unmatched left row (fires) and matches when the right arrives', () => {
    const graph = run(qb.select().from(users).leftJoin(teams, eq(teams.id, users.teamId)))
    // user with no team yet → null-extended row exists → fire
    expect(graph.apply([insUser({ id: 1, team_id: 9, name: 'a' })]).invalidated).toBe(true)
    // the matching team arrives → the null-extension flips to a matched pair → fire
    expect(graph.apply([insTeam({ id: 9, region: 'eu' })]).invalidated).toBe(true)
  })

  it('a NULL-teamId row still null-extends under LEFT join (never matches, always kept)', () => {
    const graph = run(qb.select().from(users).leftJoin(teams, eq(teams.id, users.teamId)))
    seed(graph, [insTeam({ id: 5, region: 'eu' })])
    expect(graph.apply([insUser({ id: 1, team_id: null, name: 'a' })]).invalidated).toBe(true)
  })
})

describe('joinStage — degradation to live coarse', () => {
  it('a no-equi-key (cross) join degrades to a coarse plan (dirty from both sides)', () => {
    const plan = compileQuery(extractQueryShape(qb.select().from(users).crossJoin(teams), { dialect: 'pg' }))
    expect(plan.coarse).toBe(true)
    const graph = plan.instantiate()
    expect(graph.apply([insUser({ id: 1, team_id: 5, name: 'a' })]).invalidated).toBe(true)
    expect(graph.apply([insTeam({ id: 5, region: 'eu' })]).invalidated).toBe(true)
  })

  it('an OUTER join with a non-equi ON residual degrades (not a plain filter that drops null-extensions)', () => {
    const builder = qb
      .select()
      .from(users)
      .leftJoin(teams, and(eq(teams.id, users.teamId), gt(teams.id, users.id)))
    const plan = compileQuery(extractQueryShape(builder, { dialect: 'pg' }))
    expect(plan.coarse).toBe(true)
  })

  it('an INNER join whose ON has NO equi conjunct at all degrades (no key to build a keyed join on)', () => {
    // A non-cross join can still reach the empty-equi case — `innerJoin(teams, gt(...))` is a theta-join
    // with no `=` pair. It degrades on the SAME rule a cross join does, and this is the case that reaches
    // it past the `type === 'cross'` guard, so the classifier and the builder are both exercised here.
    const builder = qb.select().from(users).innerJoin(teams, gt(teams.id, users.teamId))
    const plan = compileQuery(extractQueryShape(builder, { dialect: 'pg' }))
    expect(plan.coarse).toBe(true)
    const graph = plan.instantiate()
    expect(graph.apply([insUser({ id: 1, team_id: 5, name: 'a' })]).invalidated).toBe(true)
    expect(graph.apply([insTeam({ id: 5, region: 'eu' })]).invalidated).toBe(true)
  })
})

describe('joinStage — INNER equi-join with an exact non-equi residual', () => {
  it('applies the residual as a widening filter (matched pair failing the residual is dropped)', () => {
    const builder = qb
      .select()
      .from(users)
      .innerJoin(teams, and(eq(teams.id, users.teamId), gt(users.id, teams.id)))
    const graph = run(builder)
    seed(graph, [insTeam({ id: 5, region: 'eu' })])
    // user id 1 < team id 5 → residual id>teamId fails → no matched pair
    expect(graph.apply([insUser({ id: 1, team_id: 5, name: 'a' })]).invalidated).toBe(false)
    // user id 9 > team id 5 → residual holds → fire
    expect(graph.apply([insUser({ id: 9, team_id: 5, name: 'b' })]).invalidated).toBe(true)
  })
})
