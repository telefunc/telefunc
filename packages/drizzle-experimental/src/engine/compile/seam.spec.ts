// The runtime seam is concrete: per-input seed descriptors, a MUTED per-input seed,
// and the self-join no-double-feed integration. Seeding one alias must not fan rows into
// another alias of the same table, seeding emits no notification, and one real table change
// then applies to both aliases correctly.

import { and, eq, gt, lt } from 'drizzle-orm'
import { alias } from 'drizzle-orm/pg-core'
import * as pg from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { extractQueryShape } from '../../drizzle/extract/queryShape.js'
import { type Change, type StatefulGraph, compileQuery } from './compile.js'

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  score: pg.integer('score'),
})
const u2 = alias(users, 'u2')
const qb = new pg.QueryBuilder()

/** A self-join of users(u1) ⋈ users(u2) with DIFFERENT σ residuals per alias
 *  (u1.score > 100, u2.score < 10), joined on u2.id = u1.teamId. */
function selfJoin(): StatefulGraph {
  const q = qb
    .select()
    .from(users)
    .innerJoin(u2, eq(u2.id, users.teamId))
    .where(and(gt(users.score, 100), lt(u2.score, 10)))
  return compileQuery(extractQueryShape(q, { dialect: 'pg' })).instantiate() as StatefulGraph
}

describe('runtime seam (per-input muted seed, self-join)', () => {
  it('exposes one seed descriptor per input: distinct ids, same table, real PK', () => {
    const graph = selfJoin()
    expect(graph.seeds.length).toBe(2)
    const ids = graph.seeds.map((seed) => seed.inputId).sort()
    expect(ids).toEqual(['u2', 'users'])
    for (const seed of graph.seeds) {
      expect(seed.table).toBe('users')
      expect(seed.primaryKey).toEqual(['id'])
    }
    // The two aliases carry DIFFERENT σ residuals (compared without the opaque `src` handle).
    const noSrc = (_key: string, value: unknown) => (_key === 'src' ? undefined : value)
    const [a, b] = graph.seeds
    expect(JSON.stringify(a!.residual, noSrc)).not.toBe(JSON.stringify(b!.residual, noSrc))
  })

  it('seeds each input exactly once with NO cross-alias fan, muted, then one change hits both', () => {
    const graph = selfJoin()
    // u1 σ (score>100) baseline; u2 σ (score<10) baseline; they form exactly one join pair
    // via u2.id(7) = u1.teamId(7).
    graph.seedInput('users', [{ id: 1, team_id: 7, score: 200 }])
    graph.seedInput('u2', [{ id: 7, team_id: 9, score: 5 }])
    graph.flushSeed()

    // Muted: seeding left NO pending notification.
    expect(graph.apply([]).invalidated).toBe(false)

    // Deleting u2's row retracts the pair (proves both aliases were seeded independently — the
    // pair only exists if u1 kept team_id=7 AND u2 kept id=7).
    const delU2: Change = { table: 'users', kind: 'delete', old: { id: 7, team_id: 9, score: 5 } }
    expect(graph.apply([delU2]).invalidated).toBe(true)
  })

  it('a change matching only one alias never fabricates a pair in the other', () => {
    const graph = selfJoin()
    graph.seedInput('users', [{ id: 1, team_id: 7, score: 200 }])
    graph.seedInput('u2', [{ id: 7, team_id: 9, score: 5 }])
    graph.flushSeed()
    expect(graph.apply([]).invalidated).toBe(false)

    // Inserts into u2 only (score<10, no matching u1.teamId) → no new join output.
    const insU2: Change = { table: 'users', kind: 'insert', new: { id: 99, team_id: 50, score: 5 } }
    expect(graph.apply([insU2]).invalidated).toBe(false)
  })
})
