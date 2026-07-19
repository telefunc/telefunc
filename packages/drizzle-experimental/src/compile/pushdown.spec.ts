import { asc, eq, sql } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { extractQueryShape } from '../extract/queryShape.js'
import type { SelectShape } from '../ir/types.js'
import { qualifiedKey } from './rowSpace.js'
import { applyChange, pushdownOf } from './pushdown.js'

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  name: pg.text('name'),
  created: pg.integer('created'),
})
const teams = pg.pgTable('teams', { id: pg.integer('id').primaryKey() })
const qb = new pg.QueryBuilder()

const shapeOf = (builder: unknown) => extractQueryShape(builder, { dialect: 'pg' }) as SelectShape
const inputOf = (builder: unknown, alias = 'users') =>
  pushdownOf(shapeOf(builder)).inputs.find((plan) => plan.alias === alias)!

describe('pushdown — column pruning', () => {
  it('demands the PK, WHERE, SELECT, GROUP-adjacent and ORDER BY columns only', () => {
    const plan = inputOf(
      qb.select({ name: users.name }).from(users).where(eq(users.teamId, 5)).orderBy(asc(users.created)),
    )
    expect(plan.columns).not.toBe('*')
    const cols = new Set(plan.columns as string[])
    expect(cols).toEqual(new Set(['id', 'team_id', 'name', 'created'])) // pk + where + select + order
  })

  it('an opaque projection contributes its columns and marks the input dirty', () => {
    const plan = inputOf(qb.select({ up: sql`upper(${users.name})` }).from(users))
    expect(new Set(plan.columns as string[])).toContain('name')
    expect(plan.dirtyActive).toBe(true)
  })

  it('SELECT * demands every column', () => {
    expect(inputOf(qb.select().from(users)).columns).toBe('*')
  })
})

describe('pushdown — σ conjunct partition', () => {
  it('single-input conjuncts push to their input; a cross-input conjunct becomes the residual', () => {
    const shape = shapeOf(
      qb.select().from(users).innerJoin(teams, eq(teams.id, users.teamId)).where(eq(users.name, 'x')),
    )
    const { crossResidual } = pushdownOf(shape)
    // eq(users.name, 'x') references only `users` → pushed to that input, not the cross residual
    expect(crossResidual).toBeUndefined()
    const usersPlan = pushdownOf(shape).inputs.find((plan) => plan.alias === 'users')!
    expect(usersPlan.dirtyActive).toBe(false) // exact conjunct → no dirty
  })
})

describe('pushdown — input adapter (row-space σ + no-op fold)', () => {
  it('an update that keeps the pruned projection identical folds to nothing (no data)', () => {
    const plan = inputOf(qb.select({ team: users.teamId }).from(users).where(eq(users.teamId, 5)))
    const delta = applyChange(plan, {
      table: 'users',
      kind: 'update',
      old: { id: 1, team_id: 5, name: 'a', created: 1 },
      new: { id: 1, team_id: 5, name: 'b', created: 2 }, // only undemanded columns changed
    })
    expect(delta.data).toEqual([])
    expect(delta.dirty).toBe(false)
  })

  it('a row leaving the σ region emits a single retraction, keyed and qualified', () => {
    const plan = inputOf(qb.select({ team: users.teamId }).from(users).where(eq(users.teamId, 5)))
    const delta = applyChange(plan, {
      table: 'users',
      kind: 'update',
      old: { id: 1, team_id: 5, name: 'a', created: 1 },
      new: { id: 1, team_id: 9, name: 'a', created: 1 },
    })
    expect(delta.data).toHaveLength(1)
    const [row, multiplicity] = delta.data[0]!
    expect(multiplicity).toBe(-1)
    expect(row[qualifiedKey('users', 'team_id')]).toBe(5)
  })
})
