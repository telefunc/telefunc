import { asc, sql } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { extractQueryShape } from '../../extract/queryShape.js'
import type { OrderKey } from '../../ir/types.js'
import { type Change, type CompiledGraph, compileQuery } from './compile.js'
import { comparatorFor } from './windowStage.js'
import { type Row, qualifiedKey } from './rowSpace.js'

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  score: pg.integer('score'),
  name: pg.text('name'),
})
const qb = new pg.QueryBuilder()

const run = (builder: unknown): CompiledGraph =>
  compileQuery(extractQueryShape(builder, { dialect: 'pg' })).instantiate()
const plan = (builder: unknown) => compileQuery(extractQueryShape(builder, { dialect: 'pg' }))
const ins = (row: Record<string, unknown>): Change => ({ table: 'users', kind: 'insert', new: row })
const del = (row: Record<string, unknown>): Change => ({ table: 'users', kind: 'delete', old: row })
const seed = (graph: CompiledGraph, ...commits: Change[][]): void => {
  for (const commit of commits) graph.apply(commit)
}

const scoreKey = qualifiedKey('users', 'score')
const rowOf = (score: number | null): Row => ({ [scoreKey]: score })
const orderScore = (direction: 'asc' | 'desc', nulls?: 'first' | 'last'): OrderKey => ({
  expr: { kind: 'col', ref: { table: 'users', column: 'score' } },
  direction,
  nulls,
})

describe('windowStage — default NULL ordering matrix', () => {
  it('pg: ASC → NULLs last, DESC → NULLs first', () => {
    const ascPg = comparatorFor([orderScore('asc')], 'pg')
    expect(ascPg(rowOf(null), rowOf(1))).toBeGreaterThan(0) // null sorts after → last
    const descPg = comparatorFor([orderScore('desc')], 'pg')
    expect(descPg(rowOf(null), rowOf(1))).toBeLessThan(0) // null sorts before → first
  })

  it('sqlite: ASC → NULLs first, DESC → NULLs last', () => {
    const ascd = comparatorFor([orderScore('asc')], 'sqlite')
    expect(ascd(rowOf(null), rowOf(1))).toBeLessThan(0) // null first
    const descd = comparatorFor([orderScore('desc')], 'sqlite')
    expect(descd(rowOf(null), rowOf(1))).toBeGreaterThan(0) // null last
  })

  it('explicit NULLS FIRST/LAST overrides the dialect default', () => {
    expect(comparatorFor([orderScore('asc', 'first')], 'pg')(rowOf(null), rowOf(1))).toBeLessThan(0)
    expect(comparatorFor([orderScore('desc', 'last')], 'pg')(rowOf(null), rowOf(1))).toBeGreaterThan(0)
  })
})

describe('windowStage — topK precision', () => {
  it('a below-window insert/update does NOT fire; an in-window change does (total order)', () => {
    const graph = run(qb.select().from(users).orderBy(asc(users.id)).limit(2))
    seed(graph, [ins({ id: 1, score: 10, name: 'a' })], [ins({ id: 2, score: 20, name: 'b' })])
    // window is {id 1, id 2}; id 5 is strictly below → no fire
    expect(graph.apply([ins({ id: 5, score: 50, name: 'e' })]).invalidated).toBe(false)
    // id 0 enters the window (displaces id 2) → fire
    expect(graph.apply([ins({ id: 0, score: 5, name: 'z' })]).invalidated).toBe(true)
    // deleting an in-window row → fire
    expect(graph.apply([del({ id: 0, score: 5, name: 'z' })]).invalidated).toBe(true)
  })
})

describe('windowStage — dirty degradations', () => {
  it('a placeholder LIMIT bound taps dirty on any change', () => {
    const graph = run(qb.select().from(users).orderBy(asc(users.id)).limit(sql.placeholder('n')))
    expect(graph.apply([ins({ id: 1, score: 10, name: 'a' })]).invalidated).toBe(true)
  })

  it('OFFSET without ORDER BY is dirty on any matching change', () => {
    const graph = run(qb.select().from(users).offset(2))
    expect(graph.apply([ins({ id: 1, score: 10, name: 'a' })]).invalidated).toBe(true)
  })

  it('an opaque ORDER expression degrades to dirty (non-total order)', () => {
    const p = plan(qb.select().from(users).orderBy(sql`${users.score} + ${users.id}`).limit(2))
    const graph = p.instantiate()
    expect(graph.apply([ins({ id: 1, score: 10, name: 'a' })]).invalidated).toBe(true)
  })
})
