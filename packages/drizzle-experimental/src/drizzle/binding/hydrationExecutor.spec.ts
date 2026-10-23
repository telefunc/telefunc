// #12 — schema-qualified seed identity. The seed must read the SCHEMA-QUALIFIED relation
// (`"schema"."table"`), not a same-named table resolved via the search path. The extractor/PlanKey
// already distinguish `analytics.users` from `public.users`; the seed SQL must too, or a live query
// on one schema's relation would hydrate from the other schema's table.

import { PGlite } from '@electric-sql/pglite'
import * as pg from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { type SeedDescriptor, type StatefulGraph, compileQuery } from '../../engine/compile/compile.js'
import { extractQueryShape } from '../extract/queryShape.js'
import { relationIdOf } from '../../ir/relation.js'
import { hydrationExecutorOf } from './hydrationExecutor.js'

let client: PGlite
let db: ReturnType<typeof drizzle>

beforeAll(async () => {
  client = new PGlite()
  db = drizzle({ client })
  await client.exec('create table users (id int primary key, tag text)') // public.users
  await client.exec('create schema analytics')
  await client.exec('create table analytics.users (id int primary key, tag text)')
  await client.query("insert into users (id, tag) values (1, 'public')")
  await client.query("insert into analytics.users (id, tag) values (1, 'analytics')")
})
afterAll(async () => {
  await client.close()
})

describe('#12 — the seed reads the schema-qualified relation', () => {
  it('a schema-qualified descriptor scans ITS schema table, not the search-path same-named table', async () => {
    const descriptor: SeedDescriptor = {
      inputId: 'u',
      table: 'users',
      schema: 'analytics',
      relationId: relationIdOf({ name: 'users', schema: 'analytics' }),
      alias: 'u',
      primaryKey: ['id'],
      columns: '*',
      residual: { kind: 'true' },
    }
    const rows = await hydrationExecutorOf(db).scan(descriptor)
    expect(rows).toEqual([{ id: 1, tag: 'analytics' }]) // NOT { tag: 'public' } from the search path
  })

  it('the compiler carries the schema onto the seed descriptor for a pgSchema table', () => {
    const analytics = pg.pgSchema('analytics')
    const usersA = analytics.table('users', { id: pg.integer('id').primaryKey(), tag: pg.text('tag') })
    const qb = new pg.QueryBuilder()
    const plan = compileQuery(extractQueryShape(qb.selectDistinct({ tag: usersA.tag }).from(usersA), { dialect: 'pg' }))
    const seed = (plan.instantiate() as StatefulGraph).seeds.find((s) => s.table === 'users')
    expect(seed?.schema).toBe('analytics') // threaded through pushdown → descriptorOf
  })
})
