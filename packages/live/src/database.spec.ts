import pg from 'pg'
import { describe, expect, it } from 'vitest'
import { liveDatabaseOf } from './database.js'

// Nothing connects: a client's database is what its configuration names.
const pool = (url: string) => liveDatabaseOf(new pg.Pool({ connectionString: url }), {})

describe('the live database of a Postgres client', () => {
  it('is the one of the clients before it on the same database', () => {
    const first = pool('postgres://app@one.example:5432/shop')
    expect(pool('postgres://app@one.example:5432/shop')).toBe(first)
    expect(liveDatabaseOf(new pg.Client({ connectionString: 'postgres://app@one.example/shop' }), {})).toBe(first)
  })
  it("isn't the one of another database's clients", () => {
    const first = pool('postgres://app@two.example:5432/shop')
    expect(pool('postgres://app@two.example:5432/billing')).not.toBe(first)
    expect(pool('postgres://app@two.example:5433/shop')).not.toBe(first)
    expect(pool('postgres://app@other.example:5432/shop')).not.toBe(first)
  })
  it("isn't the one of another user's clients", () => {
    expect(pool('postgres://admin@three.example:5432/shop')).not.toBe(pool('postgres://app@three.example:5432/shop'))
  })
})
