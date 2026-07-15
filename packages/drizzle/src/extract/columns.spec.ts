import { eq } from 'drizzle-orm'
import { QueryBuilder, alias, boolean, integer, pgSchema, pgTable, primaryKey, text } from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { extractQueryShape } from './queryShape.js'
import {
  colRefOf,
  demandedColumns,
  primaryKeyOf,
  realTableNameOf,
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
  it('tableRefOf carries real name, alias, schema and primary key', () => {
    expect(tableRefOf(users)).toEqual({ name: 'users', alias: 'users', schema: undefined, primaryKey: ['id'] })
    const secured = pgSchema('app').table('acct', { id: integer('id').primaryKey() })
    expect(tableRefOf(secured)).toMatchObject({ name: 'acct', schema: 'app' })
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
})

describe('schemaFingerprint', () => {
  it('combines distinct referenced relations and dedupes by real name', () => {
    const fp = schemaFingerprint([users, teams])
    expect(fp).toContain('users')
    expect(fp).toContain('teams')
    expect(schemaFingerprint([users, teams])).toBe(schemaFingerprint([teams, users])) // order-independent
    expect(schemaFingerprint([users, alias(users, 'u2')])).toBe(schemaFingerprint([users])) // alias dedupes
  })
})

describe('demandedColumns', () => {
  it('demands every read column plus each relation primary key, keyed by real name', () => {
    const qb = new QueryBuilder()
    const shape = extractQueryShape(
      qb.select({ id: users.id }).from(users).innerJoin(teams, eq(teams.id, users.teamId)).where(eq(users.name, 'a')),
      { dialect: 'pg' },
    )
    const demand = demandedColumns(shape)
    expect(demand.get('users')).toEqual(new Set(['id', 'team_id', 'name']))
    expect(demand.get('teams')).toEqual(new Set(['id']))
  })

  it('is empty for a coarse shape', () => {
    expect(demandedColumns({ kind: 'coarse', tables: ['x'], reason: 'r' }).size).toBe(0)
  })
})
