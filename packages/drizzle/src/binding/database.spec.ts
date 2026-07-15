import { drizzle as myDrizzle } from 'drizzle-orm/mysql2'
import { drizzle as pgDrizzle } from 'drizzle-orm/node-postgres'
import { drizzle as sqliteDrizzle } from 'drizzle-orm/node-sqlite'
import { drizzle as pjDrizzle } from 'drizzle-orm/postgres-js'
import { createPool } from 'mysql2'
import { Pool } from 'pg'
import postgres from 'postgres'
import { afterAll, describe, expect, it } from 'vitest'
import { clientOf, dialectOf, driverOf, rlsEnabledOf, semanticEnvironmentKeyOf } from './database.js'

// Real driver db instances (never connected — construction is enough to read dialect,
// driver and $client). Cleaned up so vitest exits without open handles.
const cleanups: Array<() => Promise<unknown>> = []

const sqliteDb = sqliteDrizzle(':memory:')

const pgPool = new Pool({ host: 'pg.example', port: 5433, database: 'app', user: 'svc' })
const pgDb = pgDrizzle({ client: pgPool })
cleanups.push(() => pgPool.end())

const pgPool2 = new Pool({ host: 'pg.example', port: 5433, database: 'app', user: 'svc' }) // identical config
const pgDb2 = pgDrizzle({ client: pgPool2 })
cleanups.push(() => pgPool2.end())

const pjSql = postgres({ host: 'pj.example', port: 6000, database: 'pjdb', user: 'pjuser' })
const pjDb = pjDrizzle({ client: pjSql })
cleanups.push(() => pjSql.end())

const myPool = createPool({ host: 'my.example', port: 3307, database: 'mydb', user: 'myuser' })
const myDb = myDrizzle({ client: myPool })
cleanups.push(() => new Promise((resolve) => myPool.end(() => resolve(undefined))))

afterAll(async () => {
  await Promise.allSettled(cleanups.map((c) => c()))
})

describe('dialect & driver detection', () => {
  it('reads the dialect off each driver', () => {
    expect(dialectOf(sqliteDb)).toBe('sqlite')
    expect(dialectOf(pgDb)).toBe('pg')
    expect(dialectOf(pjDb)).toBe('pg')
    expect(dialectOf(myDb)).toBe('mysql')
  })

  it('reads the concrete driver entityKind', () => {
    expect(driverOf(sqliteDb)).toBe('NodeSQLiteDatabase')
    expect(driverOf(pgDb)).toBe('NodePgDatabase')
    expect(driverOf(pjDb)).toBe('PostgresJsDatabase')
    expect(driverOf(myDb)).toBe('MySql2Database')
  })

  it('exposes the raw $client', () => {
    expect(clientOf(pgDb)).toBe(pgPool)
  })
})

describe('semanticEnvironmentKeyOf — fail-closed', () => {
  it('is stable for one session', () => {
    expect(semanticEnvironmentKeyOf(pgDb)).toBe(semanticEnvironmentKeyOf(pgDb))
  })

  it('never shares across sessions, even with identical connection config', () => {
    expect(semanticEnvironmentKeyOf(pgDb)).not.toBe(semanticEnvironmentKeyOf(pgDb2))
  })

  it('carries the dialect and driver as a hint', () => {
    expect(semanticEnvironmentKeyOf(pgDb)).toContain('pg')
    expect(semanticEnvironmentKeyOf(sqliteDb)).toContain('sqlite')
    expect(semanticEnvironmentKeyOf(myDb)).toContain('mysql')
  })
})

describe('rlsEnabledOf — never defaults RLS off', () => {
  it('reports unknown for Postgres (needs a live catalog query)', async () => {
    await expect(rlsEnabledOf(pgDb, 'users')).resolves.toBe('unknown')
    await expect(rlsEnabledOf(pjDb, 'users')).resolves.toBe('unknown')
  })

  it('reports false for engines without per-table RLS', async () => {
    await expect(rlsEnabledOf(sqliteDb, 'todos')).resolves.toBe(false)
    await expect(rlsEnabledOf(myDb, 'users')).resolves.toBe(false)
  })
})
