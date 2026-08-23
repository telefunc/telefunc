import { alias, integer, pgSchema, pgTable, primaryKey, text, varchar } from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { relationIdOf } from '../../ir/relation.js'
import {
  colRefOf,
  primaryKeyOf,
  realTableNameOf,
  relationKeyOf,
  schemaFingerprint,
  tableFingerprint,
  tableRefOf,
} from './columns.js'

const users = pgTable('users', {
  id: integer('id').primaryKey(),
  name: text('name'),
  teamId: integer('team_id').notNull(),
})
const teams = pgTable('teams', { id: integer('id').primaryKey(), ownerId: integer('owner_id') })
const memberships = pgTable(
  'memberships',
  { userId: integer('user_id').notNull(), teamId: integer('team_id').notNull(), role: text('role') },
  (t) => [primaryKey({ columns: [t.userId, t.teamId] })],
)

describe('table & column resolution', () => {
  it('tableRefOf carries real name, alias, schema, routing identity and primary key', () => {
    expect(tableRefOf(users)).toEqual({
      name: 'users',
      alias: 'users',
      schema: undefined,
      id: 'users', // unqualified → the plain identity form
      primaryKey: ['id'],
    })
    const secured = pgSchema('app').table('acct', { id: integer('id').primaryKey() })
    expect(tableRefOf(secured)).toMatchObject({
      name: 'acct',
      schema: 'app',
      id: relationIdOf({ name: 'acct', schema: 'app' }),
    })
    // The identity — not the bare name — is what separates same-named relations in different schemas.
    const other = pgSchema('other').table('acct', { id: integer('id').primaryKey() })
    expect(tableRefOf(secured).id).not.toBe(tableRefOf(other).id)
    expect(tableRefOf(secured).name).toBe(tableRefOf(other).name)
  })

  it('aliased tables resolve to the real relation name', () => {
    const mgr = alias(users, 'mgr')
    expect(realTableNameOf(mgr)).toBe('users')
    expect(tableRefOf(mgr)).toMatchObject({ name: 'users', alias: 'mgr' })
  })

  it('primaryKeyOf reads single-column and composite keys', () => {
    expect(primaryKeyOf(users)).toEqual(['id'])
    expect(primaryKeyOf(memberships).sort()).toEqual(['team_id', 'user_id'])
    expect(primaryKeyOf(teams)).toEqual(['id'])
  })

  it('colRefOf keys by owning table alias and database column name', () => {
    expect(colRefOf(users.teamId)).toEqual({ table: 'users', column: 'team_id' })
  })
})

describe('tableFingerprint — canonical inputs each affect the key', () => {
  const base = pgTable('t', { id: integer('id').primaryKey(), a: integer('a') })

  it('is stable and independent of alias', () => {
    expect(tableFingerprint(base)).toBe(tableFingerprint(base))
    expect(tableFingerprint(alias(base, 'x'))).toBe(tableFingerprint(base))
  })

  it('changes when a column type changes', () => {
    const other = pgTable('t', { id: integer('id').primaryKey(), a: text('a') })
    expect(tableFingerprint(other)).not.toBe(tableFingerprint(base))
  })

  it('changes when nullability changes', () => {
    const other = pgTable('t', { id: integer('id').primaryKey(), a: integer('a').notNull() })
    expect(tableFingerprint(other)).not.toBe(tableFingerprint(base))
  })

  it('changes when the primary key changes', () => {
    const other = pgTable('t', { id: integer('id'), a: integer('a').primaryKey() })
    expect(tableFingerprint(other)).not.toBe(tableFingerprint(base))
  })

  it('changes when the relation identity changes', () => {
    const other = pgTable('t2', { id: integer('id').primaryKey(), a: integer('a') })
    expect(tableFingerprint(other)).not.toBe(tableFingerprint(base))
  })

  it('changes when the RLS bit changes', () => {
    const secured = pgTable('t', { id: integer('id').primaryKey(), a: integer('a') }).enableRLS()
    expect(tableFingerprint(secured)).not.toBe(tableFingerprint(base))
  })

  it('changes when a type parameter (varchar width) changes', () => {
    const narrow = pgTable('t', { id: integer('id').primaryKey(), name: varchar('name', { length: 10 }) })
    const wide = pgTable('t', { id: integer('id').primaryKey(), name: varchar('name', { length: 20 }) })
    expect(tableFingerprint(narrow)).not.toBe(tableFingerprint(wide))
  })

  it('changes when a column is added or renamed', () => {
    const added = pgTable('t', { id: integer('id').primaryKey(), a: integer('a'), b: integer('b') })
    expect(tableFingerprint(added)).not.toBe(tableFingerprint(base))
    const renamed = pgTable('t', { id: integer('id').primaryKey(), a: integer('a_renamed') })
    expect(tableFingerprint(renamed)).not.toBe(tableFingerprint(base))
  })

  it('injectively encodes relation identity — a dotted schema/name cannot collide', () => {
    const t1 = pgSchema('a').table('b.c', { id: integer('id').primaryKey() })
    const t2 = pgSchema('a.b').table('c', { id: integer('id').primaryKey() })
    expect(relationKeyOf(t1)).not.toBe(relationKeyOf(t2))
    expect(tableFingerprint(t1)).not.toBe(tableFingerprint(t2))
    expect(schemaFingerprint([t1])).not.toBe(schemaFingerprint([t2]))
  })

  it('folds an actual/unknown RLS bit supplied by the caller', () => {
    const t = pgTable('t', { id: integer('id').primaryKey() })
    expect(tableFingerprint(t, true)).not.toBe(tableFingerprint(t, false))
    expect(tableFingerprint(t, 'unknown')).not.toBe(tableFingerprint(t, false))
  })
})

describe('schemaFingerprint', () => {
  it('combines referenced relations order-independently and dedupes aliases', () => {
    const fp = schemaFingerprint([users, teams])
    expect(fp).toContain('users')
    expect(fp).toContain('teams')
    expect(schemaFingerprint([users, teams])).toBe(schemaFingerprint([teams, users]))
    expect(schemaFingerprint([users, alias(users, 'u2')])).toBe(schemaFingerprint([users]))
  })

  it('does not collapse same-named relations from different schemas', () => {
    const aUsers = pgSchema('a').table('users', { id: integer('id').primaryKey(), v: integer('v') })
    const bUsers = pgSchema('b').table('users', { id: integer('id').primaryKey(), v: integer('v') })
    const bUsersChanged = pgSchema('b').table('users', { id: integer('id').primaryKey(), v: text('v') })
    // b.users is present alongside a.users, so a change to it is detected regardless of input order
    expect(schemaFingerprint([aUsers, bUsers])).not.toBe(schemaFingerprint([aUsers, bUsersChanged]))
    expect(schemaFingerprint([bUsersChanged, aUsers])).not.toBe(schemaFingerprint([bUsers, aUsers]))
    // and the combined key is order-independent
    expect(schemaFingerprint([aUsers, bUsers])).toBe(schemaFingerprint([bUsers, aUsers]))
  })

  it('folds a per-relation actual/unknown RLS bit (keyed by relationKeyOf) into the combined key', () => {
    const t = pgTable('t', { id: integer('id').primaryKey() })
    const key = relationKeyOf(t)
    const rlsOn = new Map<string, true>([[key, true]])
    const rlsUnknown = new Map<string, 'unknown'>([[key, 'unknown']])
    expect(schemaFingerprint([t], rlsOn)).not.toBe(schemaFingerprint([t]))
    expect(schemaFingerprint([t], rlsOn)).not.toBe(schemaFingerprint([t], rlsUnknown))
  })
})
