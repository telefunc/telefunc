import { eq, sql } from 'drizzle-orm'
import { QueryBuilder, integer, pgTable, text } from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { canonicalValue, type IdentityEnv, identityOf, instanceKeyOf, planKeyOf } from './identity.js'

const users = pgTable('users', { id: integer('id').primaryKey(), name: text('name'), teamId: integer('team_id') })
const qb = new QueryBuilder()
const env: IdentityEnv = {
  dialect: 'pg',
  semanticEnvironmentKey: 'sess:0|pg|NodePgDatabase|host',
  schemaFingerprint: 'fp',
}

describe('identityOf — two-level identity', () => {
  it('same shape, different param values → same planKey, different instanceKey', () => {
    const a = identityOf(qb.select().from(users).where(eq(users.teamId, 5)), env)
    const b = identityOf(qb.select().from(users).where(eq(users.teamId, 9)), env)
    expect(a.planKey).toBe(b.planKey)
    expect(a.instanceKey).not.toBe(b.instanceKey)
  })

  it('different query structure → different planKey', () => {
    const a = identityOf(qb.select().from(users).where(eq(users.teamId, 5)), env)
    const b = identityOf(qb.select().from(users).where(eq(users.id, 5)), env)
    expect(a.planKey).not.toBe(b.planKey)
  })

  it('placeholder query: planKey stable across bindings, instanceKey tracks resolved values', () => {
    const builder = qb
      .select()
      .from(users)
      .where(eq(users.id, sql.placeholder('uid')))
    const a = identityOf(builder, { ...env, bindings: { uid: 5 } })
    const b = identityOf(builder, { ...env, bindings: { uid: 9 } })
    expect(a.planKey).toBe(b.planKey)
    expect(a.instanceKey).not.toBe(b.instanceKey)
  })

  it('planKey folds in the semantic environment and schema fingerprint', () => {
    const builder = qb.select().from(users).where(eq(users.teamId, 5))
    const base = identityOf(builder, env)
    expect(identityOf(builder, { ...env, semanticEnvironmentKey: 'other' }).planKey).not.toBe(base.planKey)
    expect(identityOf(builder, { ...env, schemaFingerprint: 'other' }).planKey).not.toBe(base.planKey)
  })
})

describe('planKeyOf / instanceKeyOf / canonicalValue', () => {
  it('planKey is value-free; instanceKey appends canonicalized params', () => {
    const pk = planKeyOf({
      dialect: 'pg',
      semanticEnvironmentKey: 's',
      schemaFingerprint: 'f',
      sql: 'select 1 where a = $1',
    })
    expect(instanceKeyOf(pk, [5])).not.toBe(instanceKeyOf(pk, [6]))
    expect(instanceKeyOf(pk, [5])).toBe(instanceKeyOf(pk, [5]))
    expect(instanceKeyOf(pk, [5]).startsWith(pk)).toBe(true)
  })

  it('canonicalValue is stable and collapses structurally-equal values', () => {
    expect(canonicalValue(5)).not.toBe(canonicalValue(6))
    expect(canonicalValue(null)).toBe('null')
    expect(canonicalValue({ a: 1, b: 2 })).toBe(canonicalValue({ b: 2, a: 1 })) // key order
    expect(canonicalValue(new Date('2026-01-01T00:00:00Z'))).toBe(canonicalValue(new Date(Date.UTC(2026, 0, 1))))
  })
})
