// The σ-scoped shadow: PK → pruned tuple for σ-matching rows only (B1), the three-way
// retraction resolution hit / drop-on-complete / coarse-on-incomplete (B2), and size
// accounting toward the state bound (B3). The old-inline case (B2a) is resolved by liveGraph
// without consulting the shadow and is covered there.

import { gt, sql } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { type SeedDescriptor, type StatefulGraph, compileQuery } from '../compile/compile.js'
import { extractQueryShape } from '../extract/queryShape.js'
import { createShadow, matchesResidual, pkOf, pruneRow } from './shadow.js'

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  score: pg.integer('score'),
})
const qb = new pg.QueryBuilder()

function usersDescriptor(): SeedDescriptor {
  const q = qb
    .select({ team: users.teamId, s: sql`sum(${users.score})` })
    .from(users)
    .where(gt(users.score, 0))
    .groupBy(users.teamId)
  const graph = compileQuery(extractQueryShape(q, { dialect: 'pg' })).instantiate() as StatefulGraph
  return graph.seeds.find((seed) => seed.table === 'users')!
}

describe('σ-scoped PK → pruned tuple', () => {
  it('admits only σ-matching rows (NULL excludes, MISSING widens) and prunes to demanded columns', () => {
    const descriptor = usersDescriptor()
    expect(matchesResidual(descriptor, { id: 1, team_id: 5, score: 10 })).toBe(true)
    expect(matchesResidual(descriptor, { id: 2, team_id: 5, score: 0 })).toBe(false) // 0 > 0 is false
    expect(matchesResidual(descriptor, { id: 3, team_id: 5, score: null })).toBe(false) // NULL excludes
    expect(matchesResidual(descriptor, { id: 4, team_id: 5 })).toBe(true) // MISSING widens
    // Pruning keeps demanded columns (PK + group + filter), drops the rest.
    const pruned = pruneRow(descriptor, { id: 1, team_id: 5, score: 10, extra: 'x' })
    expect(pruned).not.toHaveProperty('extra')
    expect(pruned).toMatchObject({ id: 1, team_id: 5, score: 10 })
  })
})

describe('retraction resolution (key-only three-way)', () => {
  it('b) key-only + shadow hit → the exact pruned old tuple', () => {
    const descriptor = usersDescriptor()
    const shadow = createShadow()
    const pk = pkOf(descriptor, { id: 7 })!
    shadow.put(pk, { id: 7, team_id: 3, score: 9 })
    const match = shadow.resolve(pk)
    expect(match).toEqual({ kind: 'hit', old: { id: 7, team_id: 3, score: 9 } })
  })

  it('c) key-only + miss on COMPLETE state → drop (provably not σ-matching)', () => {
    const shadow = createShadow()
    shadow.complete = true
    expect(shadow.resolve('missing-pk')).toEqual({ kind: 'drop' })
  })

  it('d) key-only + miss on INCOMPLETE state → coarse (cannot prove irrelevance)', () => {
    const shadow = createShadow()
    shadow.complete = false
    expect(shadow.resolve('missing-pk')).toEqual({ kind: 'coarse' })
  })
})

describe('shadow size accounting', () => {
  it('put grows the count, remove shrinks it', () => {
    const descriptor = usersDescriptor()
    const shadow = createShadow()
    expect(shadow.size).toBe(0)
    shadow.put(pkOf(descriptor, { id: 1 })!, { id: 1, team_id: 1, score: 3 })
    shadow.put(pkOf(descriptor, { id: 2 })!, { id: 2, team_id: 1, score: 4 })
    expect(shadow.size).toBe(2)
    shadow.remove(pkOf(descriptor, { id: 1 })!)
    expect(shadow.size).toBe(1)
    expect(shadow.rows()).toEqual([{ id: 2, team_id: 1, score: 4 }])
  })
})
