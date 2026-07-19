import { eq, sql } from 'drizzle-orm'
import { QueryBuilder, integer, pgTable, text } from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { canonicalValue, type IdentityEnv, identityOf, instanceKeyOf, planKeyOf } from './identity.js'

const users = pgTable('users', { id: integer('id').primaryKey(), name: text('name'), teamId: integer('team_id') })
const qb = new QueryBuilder()
const env: IdentityEnv = {
  dialect: 'pg',
  semanticEnvironmentKey: 'env|pg|NodePgDatabase|db=app|role=svc|sp=public',
  schemaFingerprint: 'fp',
}

describe('identityOf — instance identity', () => {
  it('same query shape, different param values → different instanceKey (value-sensitive)', () => {
    const a = identityOf(qb.select().from(users).where(eq(users.teamId, 5)), env)
    const b = identityOf(qb.select().from(users).where(eq(users.teamId, 9)), env)
    expect(a.instanceKey).not.toBe(b.instanceKey)
  })

  it('different query structure → different instanceKey', () => {
    const a = identityOf(qb.select().from(users).where(eq(users.teamId, 5)), env)
    const b = identityOf(qb.select().from(users).where(eq(users.id, 5)), env)
    expect(a.instanceKey).not.toBe(b.instanceKey)
  })

  it('placeholder query: instanceKey tracks the resolved binding values', () => {
    const builder = qb
      .select()
      .from(users)
      .where(eq(users.id, sql.placeholder('uid')))
    const a = identityOf(builder, { ...env, bindings: { uid: 5 } })
    const b = identityOf(builder, { ...env, bindings: { uid: 9 } })
    expect(a.instanceKey).not.toBe(b.instanceKey)
  })

  it('instanceKey folds in the semantic environment and schema fingerprint (no cross-boundary sharing)', () => {
    const builder = qb.select().from(users).where(eq(users.teamId, 5))
    const base = identityOf(builder, env)
    // A graph is shared ONLY across acquisitions with an identical instanceKey — its structural
    // foundation (via planKeyOf) must fold in the security/schema context, so these do NOT collide.
    expect(identityOf(builder, { ...env, semanticEnvironmentKey: 'other' }).instanceKey).not.toBe(base.instanceKey)
    expect(identityOf(builder, { ...env, schemaFingerprint: 'other' }).instanceKey).not.toBe(base.instanceKey)
  })
})

describe('planKeyOf / instanceKeyOf', () => {
  const pk = planKeyOf({
    dialect: 'pg',
    semanticEnvironmentKey: 's',
    schemaFingerprint: 'f',
    sql: 'select 1 where a = $1',
  })

  it('instanceKey is deterministic and value-sensitive', () => {
    expect(instanceKeyOf(pk, [5])).toBe(instanceKeyOf(pk, [5]))
    expect(instanceKeyOf(pk, [5])).not.toBe(instanceKeyOf(pk, [6]))
  })

  it('component boundaries cannot be forged by moving a delimiter', () => {
    // planKey: a char shifted across the env/sql boundary must not collide
    const a = planKeyOf({ dialect: 'pg', semanticEnvironmentKey: 'ab', schemaFingerprint: 'f', sql: 'c' })
    const b = planKeyOf({ dialect: 'pg', semanticEnvironmentKey: 'a', schemaFingerprint: 'f', sql: 'bc' })
    expect(a).not.toBe(b)
  })

  it('param-list boundaries cannot collide', () => {
    expect(instanceKeyOf(pk, ['a', 'b'])).not.toBe(instanceKeyOf(pk, ['a,b']))
    expect(instanceKeyOf(pk, ['a', 'b'])).not.toBe(instanceKeyOf(pk, ['ab']))
    expect(instanceKeyOf(pk, [1])).not.toBe(instanceKeyOf(pk, [BigInt(1)]))
  })

  it('number 1 and string "1" produce distinct instance keys', () => {
    expect(instanceKeyOf(pk, [1])).not.toBe(instanceKeyOf(pk, ['1']))
  })
})

describe('planKey named gates', () => {
  const base = { dialect: 'pg', semanticEnvironmentKey: 'env', schemaFingerprint: 'fp', sql: 'select 1' }

  it('a database-authority (semantic environment) change changes the planKey', () => {
    expect(planKeyOf({ ...base, semanticEnvironmentKey: 'env2' })).not.toBe(planKeyOf(base))
  })
  it('a dialect change changes the planKey', () => {
    expect(planKeyOf({ ...base, dialect: 'sqlite' })).not.toBe(planKeyOf(base))
  })
})

describe('canonicalValue — collision-free', () => {
  it('numbers and bigints are distinct', () => {
    expect(canonicalValue(1)).not.toBe(canonicalValue(BigInt(1)))
    expect(canonicalValue(10)).not.toBe(canonicalValue(BigInt(10)))
  })

  it('number 1 and string "1" are distinct', () => {
    expect(canonicalValue(1)).not.toBe(canonicalValue('1'))
  })

  it('array element boundaries cannot collide', () => {
    expect(canonicalValue(['a', 'b'])).not.toBe(canonicalValue(['a,b']))
    expect(canonicalValue(['a', 'b'])).not.toBe(canonicalValue(['ab']))
    expect(canonicalValue(['a', 'b'])).not.toBe(canonicalValue([['a', 'b']]))
    // a separator the encoding itself emits, embedded in an element: ['a','b'] vs ['a,s:b']
    expect(canonicalValue(['a', 'b'])).not.toBe(canonicalValue(['a,s:b']))
  })

  it('null vs the string "null", Date vs its numeric/string form', () => {
    expect(canonicalValue(null)).not.toBe(canonicalValue('null'))
    expect(canonicalValue(undefined)).not.toBe(canonicalValue('undef'))
    expect(canonicalValue(new Date(0))).not.toBe(canonicalValue('0'))
    expect(canonicalValue(new Date(0))).not.toBe(canonicalValue(0))
  })

  it('structurally-equal values collapse (Date epoch, object key order)', () => {
    expect(canonicalValue(new Date('2026-01-01T00:00:00Z'))).toBe(canonicalValue(new Date(Date.UTC(2026, 0, 1))))
    expect(canonicalValue({ a: 1, b: 2 })).toBe(canonicalValue({ b: 2, a: 1 }))
    expect(canonicalValue(5)).not.toBe(canonicalValue(6))
  })
})
