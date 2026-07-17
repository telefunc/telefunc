import { eq } from 'drizzle-orm'
import { QueryBuilder, integer, pgTable } from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { isPartialSelect, selectConfigOf } from './drizzleShape.js'

const users = pgTable('users', { id: integer('id').primaryKey(), teamId: integer('team_id') })
const qb = new QueryBuilder()

describe('selectConfigOf', () => {
  it('reads the pinned select config shape', () => {
    const config = selectConfigOf(qb.select().from(users).where(eq(users.teamId, 5)))
    expect(config).not.toBeNull()
    expect(config?.table).toBeDefined()
    expect(config?.fields).toBeTypeOf('object')
    expect(config?.where).toBeDefined()
  })

  it('returns null when the shape is not the pinned one', () => {
    expect(selectConfigOf({ config: { totally: 'wrong' } })).toBeNull()
    expect(selectConfigOf({})).toBeNull()
    expect(selectConfigOf({ config: { table: {}, joins: 'not-an-array', fields: {} } })).toBeNull()
  })
})

describe('isPartialSelect', () => {
  it('distinguishes whole-row select() from an explicit projection', () => {
    expect(isPartialSelect(qb.select().from(users))).toBe(false)
    expect(isPartialSelect(qb.select({ id: users.id }).from(users))).toBe(true)
  })
})
