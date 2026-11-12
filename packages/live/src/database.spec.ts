import pg from 'pg'
import { describe, expect, it } from 'vitest'
import { liveDatabaseOf } from './database.js'

// Nothing connects: a client's database is what its configuration names.
const pool = (url: string, options?: string) => liveDatabaseOf(new pg.Pool({ connectionString: url, options }), {})

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
  it("isn't the one of clients whose sessions start as another role or with another search_path", () => {
    const first = pool('postgres://app@four.example:5432/shop')
    const tenant = pool('postgres://app@four.example:5432/shop', '-c role=tenant')
    expect(tenant).not.toBe(first)
    expect(pool('postgres://app@four.example:5432/shop', '-c role=tenant')).toBe(tenant)
    expect(pool('postgres://app@four.example:5432/shop', '-c search_path=bb')).not.toBe(first)
  })
})
