import { and, eq, gt, or, sql } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it, vi } from 'vitest'
import { extractQueryShape } from '../../extract/queryShape.js'
import type { QueryShape } from '../../ir/types.js'
import { type Change, compileQuery } from './compile.js'

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  name: pg.text('name'),
  done: pg.boolean('done'),
})
const qb = new pg.QueryBuilder()

const shapeOf = (builder: unknown): QueryShape => extractQueryShape(builder, { dialect: 'pg' })
const planOf = (builder: unknown) => compileQuery(shapeOf(builder))
const run = (builder: unknown) => planOf(builder).instantiate()

const ins = (row: Record<string, unknown>): Change => ({ table: 'users', kind: 'insert', new: row })
const del = (row: Record<string, unknown>): Change => ({ table: 'users', kind: 'delete', old: row })
const upd = (old: Record<string, unknown>, next: Record<string, unknown>): Change => ({
  table: 'users',
  kind: 'update',
  old,
  new: next,
})

describe('compile — classification', () => {
  it('an untrackable coarse shape is a typed rejection, never an inputless graph', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const untrackable = extractQueryShape({ config: undefined, _: undefined }, { dialect: 'pg' })
    expect(untrackable).toMatchObject({ kind: 'coarse', untrackable: true })
    expect(() => compileQuery(untrackable)).toThrow(/untrackable read cannot be compiled/)
    warn.mockRestore()
  })

  it('a recoverable coarse shape compiles to a live coarse graph that fires on any change', () => {
    const sub = qb.select({ id: users.id, teamId: users.teamId }).from(users).as('sub')
    const plan = compileQuery(shapeOf(qb.select().from(sub)))
    expect(plan.coarse).toBe(true)
    const graph = plan.instantiate()
    expect(graph.apply([ins({ id: 1, team_id: 5 })]).invalidated).toBe(true)
  })

  it('a predicate-only plan is stateless; a joined plan is stateful', () => {
    const teams = pg.pgTable('teams', { id: pg.integer('id').primaryKey() })
    expect(planOf(qb.select().from(users).where(eq(users.teamId, 5))).stateless).toBe(true)
    expect(planOf(qb.select().from(users).innerJoin(teams, eq(teams.id, users.teamId))).stateless).toBe(false)
  })
})

describe('compile — stateless invalidation (row-space σ)', () => {
  it('SELECT * WHERE teamId=5 fires on a matching insert/delete, not a non-matching one', () => {
    const graph = run(qb.select().from(users).where(eq(users.teamId, 5)))
    expect(graph.apply([ins({ id: 1, team_id: 5, name: 'a', done: false })]).invalidated).toBe(true)
    expect(graph.apply([ins({ id: 2, team_id: 3, name: 'b', done: false })]).invalidated).toBe(false)
    expect(graph.apply([del({ id: 1, team_id: 5, name: 'a', done: false })]).invalidated).toBe(true)
  })

  it('an update crossing the σ boundary fires; a no-op update to an unselected column does not', () => {
    const graph = run(qb.select({ team: users.teamId }).from(users).where(eq(users.teamId, 5)))
    // team_id 5 → 3: leaves the set → fire
    expect(graph.apply([upd({ id: 1, team_id: 5 }, { id: 1, team_id: 3 })]).invalidated).toBe(true)
    // name is neither selected nor filtered → no fire
    const nameOnly = run(qb.select({ team: users.teamId }).from(users).where(eq(users.teamId, 5)))
    expect(nameOnly.apply([upd({ id: 1, team_id: 5, name: 'a' }, { id: 1, team_id: 5, name: 'b' })]).invalidated).toBe(
      false,
    )
  })

  it('a value change to a SELECTED column fires', () => {
    const graph = run(qb.select({ id: users.id, name: users.name }).from(users).where(eq(users.teamId, 5)))
    expect(graph.apply([upd({ id: 1, team_id: 5, name: 'a' }, { id: 1, team_id: 5, name: 'b' })]).invalidated).toBe(
      true,
    )
  })

  it('A AND B (both exact) does not fire dirty; both conjuncts filter', () => {
    const graph = run(
      qb
        .select()
        .from(users)
        .where(and(eq(users.teamId, 5), gt(users.id, 10))),
    )
    expect(graph.apply([ins({ id: 20, team_id: 5, name: 'x', done: false })]).invalidated).toBe(true)
    expect(graph.apply([ins({ id: 3, team_id: 5, name: 'y', done: false })]).invalidated).toBe(false) // fails id>10
    // id=21 satisfies both exact conjuncts → fires via the data path (not dirty). The oracle's
    // and(eq,gt) case pins this plan exact, so it never over-fires.
    expect(graph.apply([ins({ id: 21, team_id: 5, name: 'z', done: false })]).invalidated).toBe(true)
  })

  it('A OR unknown widens to TRUE and fires dirty on any change (tap before)', () => {
    const graph = run(
      qb
        .select()
        .from(users)
        .where(or(eq(users.teamId, 5), sql`${users.name} similar to 'x%'`)),
    )
    // A OR unknown widens to TRUE → fires on any change (dirty degradation past WHERE)
    expect(graph.apply([ins({ id: 1, team_id: 3, name: 'q', done: false })]).invalidated).toBe(true)
  })
})
