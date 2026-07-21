import { eq, exists, inArray, notInArray, sql } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { extractQueryShape } from '../../extract/queryShape.js'
import { type Change, type CompiledGraph, compileQuery } from './compile.js'

const users = pg.pgTable('users', { id: pg.integer('id').primaryKey(), teamId: pg.integer('team_id') })
const orders = pg.pgTable('orders', { id: pg.integer('id').primaryKey(), userId: pg.integer('user_id') })
const teams = pg.pgTable('teams', { id: pg.integer('id').primaryKey() })
const qb = new pg.QueryBuilder()

const run = (builder: unknown): CompiledGraph =>
  compileQuery(extractQueryShape(builder, { dialect: 'pg' })).instantiate()
const insUser = (row: Record<string, unknown>): Change => ({ table: 'users', kind: 'insert', new: row })
const insOrder = (row: Record<string, unknown>): Change => ({ table: 'orders', kind: 'insert', new: row })
const delOrder = (row: Record<string, unknown>): Change => ({ table: 'orders', kind: 'delete', old: row })
const seed = (graph: CompiledGraph, ...commits: Change[][]): void => {
  for (const commit of commits) graph.apply(commit)
}

describe('existsStage — EXISTS/IN semi-join', () => {
  it('an inner row satisfying the correlation makes the outer row appear (fires)', () => {
    const graph = run(
      qb
        .select()
        .from(users)
        .where(exists(qb.select().from(orders).where(eq(orders.userId, users.id)))),
    )
    seed(graph, [insUser({ id: 1, team_id: 5 })])
    // user 1 has no order yet → adding one makes it satisfy EXISTS → fire
    expect(graph.apply([insOrder({ id: 10, user_id: 1 })]).invalidated).toBe(true)
    // an order for a non-existent user 2 → no outer row to admit → no fire
    expect(graph.apply([insOrder({ id: 11, user_id: 2 })]).invalidated).toBe(false)
    // removing the only order for user 1 retracts it from the semi-join → fire
    expect(graph.apply([delOrder({ id: 10, user_id: 1 })]).invalidated).toBe(true)
  })

  it('IN (subquery) admits the outer row when its key appears inner-side', () => {
    const graph = run(
      qb
        .select()
        .from(users)
        .where(inArray(users.teamId, qb.select({ id: teams.id }).from(teams))),
    )
    seed(graph, [insUser({ id: 1, team_id: 5 })])
    expect(graph.apply([{ table: 'teams', kind: 'insert', new: { id: 5 } }]).invalidated).toBe(true)
    expect(graph.apply([{ table: 'teams', kind: 'insert', new: { id: 9 } }]).invalidated).toBe(false)
  })
})

describe('existsStage — NOT IN + NULL and scalar subqueries', () => {
  it('NOT IN degrades to dirty (the inner-NULL counterexample never misses)', () => {
    // NOT IN with an inner NULL: removing the inner NULL flips SQL excluded→included; a plain
    // anti-join would miss it, so this is a dirty degradation that fires on any inner change.
    const graph = run(
      qb
        .select()
        .from(users)
        .where(notInArray(users.teamId, qb.select({ id: teams.id }).from(teams))),
    )
    seed(graph, [insUser({ id: 1, team_id: 1 })])
    expect(graph.apply([{ table: 'teams', kind: 'insert', new: { id: null } }]).invalidated).toBe(true)
    expect(graph.apply([{ table: 'teams', kind: 'delete', old: { id: null } }]).invalidated).toBe(true)
  })

  it('a scalar subquery registers its inner table and fires dirty on an inner change', () => {
    const graph = run(
      qb.select({ id: users.id, teamCount: sql`(${qb.select({ n: sql`count(*)` }).from(teams)})` }).from(users),
    )
    seed(graph, [insUser({ id: 1, team_id: 5 })])
    // a change to the scalar subquery's inner table `teams` must invalidate
    expect(graph.apply([{ table: 'teams', kind: 'insert', new: { id: 7 } }]).invalidated).toBe(true)
  })
})
