import { asc } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { extractQueryShape } from '../../extract/queryShape.js'
import type { SelectShape } from '../../ir/types.js'
import { type Change, type CompiledGraph, compileQuery } from './compile.js'
import { projectionKeysOf } from './projectStage.js'
import { qualifiedKey } from './rowSpace.js'

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  name: pg.text('name'),
})
const qb = new pg.QueryBuilder()

const shapeOf = (builder: unknown) => extractQueryShape(builder, { dialect: 'pg' }) as SelectShape
const run = (builder: unknown): CompiledGraph =>
  compileQuery(extractQueryShape(builder, { dialect: 'pg' })).instantiate()
const ins = (row: Record<string, unknown>): Change => ({ table: 'users', kind: 'insert', new: row })
const seed = (graph: CompiledGraph, ...commits: Change[][]): void => {
  for (const commit of commits) graph.apply(commit)
}

describe('projectStage — projected keys carry ORDER BY', () => {
  it('the projection key set includes SELECT columns AND ORDER BY columns', () => {
    const keys = projectionKeysOf(shapeOf(qb.select({ name: users.name }).from(users).orderBy(asc(users.teamId))))
    expect(keys).toContain(qualifiedKey('users', 'name'))
    expect(keys).toContain(qualifiedKey('users', 'team_id')) // sort column rides even though unselected
  })

  it('SELECT * observes every column', () => {
    expect(projectionKeysOf(shapeOf(qb.select().from(users)))).toBe('*')
  })
})

describe('projectStage — DISTINCT', () => {
  it('a duplicate distinct value is silent; a new value fires', () => {
    const graph = run(qb.selectDistinct({ team: users.teamId }).from(users))
    seed(graph, [ins({ id: 1, team_id: 5, name: 'a' })])
    // second row with the same teamId → DISTINCT set unchanged → no fire
    expect(graph.apply([ins({ id: 2, team_id: 5, name: 'b' })]).invalidated).toBe(false)
    // a new distinct teamId → fire
    expect(graph.apply([ins({ id: 3, team_id: 6, name: 'c' })]).invalidated).toBe(true)
  })
})
