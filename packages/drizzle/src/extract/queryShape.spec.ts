import { count, desc, eq, gt, sql } from 'drizzle-orm'
import * as mysqlCore from 'drizzle-orm/mysql-core'
import { drizzle as sqliteDrizzle } from 'drizzle-orm/node-sqlite'
import * as pgCore from 'drizzle-orm/pg-core'
import * as sqliteCore from 'drizzle-orm/sqlite-core'
import { describe, expect, it } from 'vitest'
import type { SelectShape } from '../ir/types.js'
import { crossCheckRenderedTables, extractQueryShape } from './queryShape.js'

// ── pg fixtures (driver-free core QueryBuilder) ─────────────────────
const pgUsers = pgCore.pgTable('users', {
  id: pgCore.integer('id').primaryKey(),
  name: pgCore.text('name'),
  teamId: pgCore.integer('team_id'),
  done: pgCore.boolean('done'),
})
const pgTeams = pgCore.pgTable('teams', { id: pgCore.integer('id').primaryKey(), ownerId: pgCore.integer('owner_id') })
const pgQb = new pgCore.QueryBuilder()

const asSelect = (shape: ReturnType<typeof extractQueryShape>): SelectShape => {
  expect(shape.kind).toBe('select')
  return shape as SelectShape
}
const pg = (builder: Parameters<typeof extractQueryShape>[0]) => extractQueryShape(builder, { dialect: 'pg' })

describe('extractQueryShape — pg mechanics', () => {
  it('from + where', () => {
    const s = asSelect(pg(pgQb.select().from(pgUsers).where(eq(pgUsers.teamId, 5))))
    expect(s.from).toMatchObject({ name: 'users', alias: 'users', primaryKey: ['id'] })
    expect(s.where).toMatchObject({ kind: 'compare', op: '=' })
    expect(s.tables).toEqual(['users'])
    expect(s.projection.star).toBe(true)
  })

  it('all four join types + cross join', () => {
    const on = eq(pgTeams.id, pgUsers.teamId)
    expect(asSelect(pg(pgQb.select().from(pgUsers).innerJoin(pgTeams, on))).joins[0]!.type).toBe('inner')
    expect(asSelect(pg(pgQb.select().from(pgUsers).leftJoin(pgTeams, on))).joins[0]!.type).toBe('left')
    expect(asSelect(pg(pgQb.select().from(pgUsers).rightJoin(pgTeams, on))).joins[0]!.type).toBe('right')
    expect(asSelect(pg(pgQb.select().from(pgUsers).fullJoin(pgTeams, on))).joins[0]!.type).toBe('full')

    const inner = asSelect(pg(pgQb.select().from(pgUsers).innerJoin(pgTeams, on)))
    expect(inner.joins[0]!.table.name).toBe('teams')
    expect(inner.joins[0]!.on).toBeDefined()
    expect(new Set(inner.tables)).toEqual(new Set(['users', 'teams']))

    const cross = asSelect(pg(pgQb.select().from(pgUsers).crossJoin(pgTeams)))
    expect(cross.joins[0]!.type).toBe('cross')
    expect(cross.joins[0]!.on).toBeUndefined()
  })

  it('self-join via alias resolves alias→real name', () => {
    const mgr = pgCore.alias(pgUsers, 'mgr')
    const s = asSelect(pg(pgQb.select().from(pgUsers).innerJoin(mgr, eq(mgr.id, pgUsers.teamId))))
    expect(s.joins[0]!.table).toMatchObject({ name: 'users', alias: 'mgr' })
    expect(s.tables).toEqual(['users'])
  })

  it('group by + having + aggregate projection', () => {
    const s = asSelect(
      pg(
        pgQb.select({ team: pgUsers.teamId, n: count() }).from(pgUsers).groupBy(pgUsers.teamId).having(gt(count(), 2)),
      ),
    )
    expect(s.groupBy).toEqual([{ table: 'users', column: 'team_id' }])
    // HAVING compares an aggregate (not a column), so the column-oriented predicate
    // parser records a sound unknown leaf — precise aggregate-HAVING is a compile concern.
    expect(s.having).toMatchObject({ kind: 'unknown' })
    const agg = s.projection.items.find((item) => item.kind === 'agg')
    expect(agg).toMatchObject({ kind: 'agg', call: { fn: 'count', star: true } })
  })

  it('distinct and distinct on', () => {
    expect(asSelect(pg(pgQb.selectDistinct().from(pgUsers))).distinct).toEqual({ on: true })
    const on = asSelect(pg(pgQb.selectDistinctOn([pgUsers.teamId]).from(pgUsers))).distinct
    expect(on).toEqual({ on: 'columns', columns: [{ table: 'users', column: 'team_id' }] })
  })

  it('set operations (union / unionAll / intersect / except)', () => {
    // the set-op builders mutate their operands, so each arm must be freshly built
    const arm = (team: number) => pgQb.select().from(pgUsers).where(eq(pgUsers.teamId, team))
    expect(asSelect(pg(pgCore.union(arm(1), arm(2)))).setOps[0]!.type).toBe('union')
    expect(asSelect(pg(pgCore.unionAll(arm(1), arm(2)))).setOps[0]!.type).toBe('unionAll')
    expect(asSelect(pg(pgCore.intersect(arm(1), arm(2)))).setOps[0]!.type).toBe('intersect')
    expect(asSelect(pg(pgCore.except(arm(1), arm(2)))).setOps[0]!.type).toBe('except')
  })

  it('order by / limit / offset, value and placeholder', () => {
    const s = asSelect(pg(pgQb.select().from(pgUsers).orderBy(desc(pgUsers.id)).limit(10).offset(2)))
    expect(s.orderBy[0]).toMatchObject({ expr: { kind: 'col', ref: { column: 'id' } }, direction: 'desc' })
    expect(s.limit).toEqual({ kind: 'value', value: 10 })
    expect(s.offset).toEqual({ kind: 'value', value: 2 })
    const dyn = asSelect(pg(pgQb.select().from(pgUsers).limit(sql.placeholder('lim'))))
    expect(dyn.limit).toEqual({ kind: 'placeholder', name: 'lim' })
  })

  it('window function is detected', () => {
    const s = asSelect(pg(pgQb.select({ rn: sql`row_number() over (order by ${pgUsers.id})`.as('rn') }).from(pgUsers)))
    expect(s.window).toBe(true)
  })

  it('a subquery in where contributes its tables', () => {
    const sub = pgQb.select({ id: pgTeams.id }).from(pgTeams)
    const s = asSelect(pg(pgQb.select().from(pgUsers).where(sql`${pgUsers.teamId} in ${sub}`)))
    expect(new Set(s.tables)).toEqual(new Set(['users', 'teams']))
  })
})

describe('extractQueryShape — coarse fallback', () => {
  it('a subquery/CTE FROM degrades to a CoarseShape recovering its tables', () => {
    const sub = pgQb.select({ id: pgUsers.id, teamId: pgUsers.teamId }).from(pgUsers).as('sub')
    const shape = pg(pgQb.select().from(sub))
    expect(shape.kind).toBe('coarse')
    expect(shape.tables).toContain('users')
  })

  it('an unreadable select config degrades to a CoarseShape via recovered used tables', () => {
    const real = pgQb.select().from(pgUsers).innerJoin(pgTeams, eq(pgTeams.id, pgUsers.teamId))
    const broken = Object.create(Object.getPrototypeOf(real))
    Object.assign(broken, real)
    broken.config = { not: 'a real config' } // mutate the pinned shape
    broken._ = undefined
    const shape = extractQueryShape(broken, { dialect: 'pg' })
    expect(shape.kind).toBe('coarse')
    expect(new Set(shape.tables)).toEqual(new Set(['users', 'teams']))
  })

  it('cross-check: a rendered relation missing from extraction degrades (rendered A JOIN B, extracted A)', () => {
    const onlyA: SelectShape = {
      kind: 'select',
      dialect: 'pg',
      from: { name: 'A', alias: 'A', primaryKey: [] },
      joins: [],
      projection: { items: [], star: true },
      groupBy: [],
      groupByOpaque: false,
      distinct: { on: false },
      orderBy: [],
      setOps: [],
      window: false,
      tables: ['A'],
    }
    const degraded = crossCheckRenderedTables(onlyA, ['A', 'B'])
    expect(degraded.kind).toBe('coarse')
    expect(new Set(degraded.tables)).toEqual(new Set(['A', 'B']))
    // a complete extraction passes the cross-check unchanged
    expect(crossCheckRenderedTables(onlyA, ['A'])).toBe(onlyA)
  })
})

// ── mysql fixtures (driver-free core QueryBuilder) ──────────────────
describe('extractQueryShape — mysql goldens', () => {
  const users = mysqlCore.mysqlTable('users', {
    id: mysqlCore.int('id').primaryKey(),
    teamId: mysqlCore.int('team_id'),
    name: mysqlCore.varchar('name', { length: 100 }),
  })
  const teams = mysqlCore.mysqlTable('teams', { id: mysqlCore.int('id').primaryKey() })
  const qb = new mysqlCore.QueryBuilder()
  const my = (b: Parameters<typeof extractQueryShape>[0]) => extractQueryShape(b, { dialect: 'mysql' })

  it('where + join + group/aggregate carry through the mysql builder', () => {
    const joined = asSelect(
      my(qb.select().from(users).innerJoin(teams, eq(teams.id, users.teamId)).where(eq(users.teamId, 3))),
    )
    expect(joined.dialect).toBe('mysql')
    expect(joined.joins[0]!.type).toBe('inner')
    expect(new Set(joined.tables)).toEqual(new Set(['users', 'teams']))

    const grouped = asSelect(my(qb.select({ team: users.teamId, n: count() }).from(users).groupBy(users.teamId)))
    expect(grouped.groupBy).toEqual([{ table: 'users', column: 'team_id' }])
  })
})

// ── sqlite fixtures (real node:sqlite DatabaseSync via node-sqlite) ──
describe('extractQueryShape — sqlite goldens (node-sqlite driver)', () => {
  const todos = sqliteCore.sqliteTable('todos', {
    id: sqliteCore.integer('id').primaryKey(),
    text: sqliteCore.text('text'),
    done: sqliteCore.integer('done', { mode: 'boolean' }),
    userId: sqliteCore.integer('user_id'),
  })
  const users = sqliteCore.sqliteTable('users', { id: sqliteCore.integer('id').primaryKey() })
  const db = sqliteDrizzle(':memory:') // constructs a real node:sqlite DatabaseSync internally
  const lite = (b: Parameters<typeof extractQueryShape>[0]) => extractQueryShape(b, { dialect: 'sqlite' })

  it('builds and extracts shapes through the node-sqlite driver on the pinned rc', () => {
    const filtered = asSelect(lite(db.select().from(todos).where(eq(todos.done, true)).limit(3)))
    expect(filtered.dialect).toBe('sqlite')
    expect(filtered.from.name).toBe('todos')
    expect(filtered.where).toMatchObject({ kind: 'compare', op: '=' })
    expect(filtered.limit).toEqual({ kind: 'value', value: 3 })

    const joined = asSelect(lite(db.select().from(todos).innerJoin(users, eq(users.id, todos.userId))))
    expect(joined.joins[0]!.type).toBe('inner')
    expect(new Set(joined.tables)).toEqual(new Set(['todos', 'users']))

    const grouped = asSelect(lite(db.select({ u: todos.userId, n: count() }).from(todos).groupBy(todos.userId)))
    expect(grouped.groupBy).toEqual([{ table: 'todos', column: 'user_id' }])

    const distinct = asSelect(lite(db.selectDistinct().from(todos)))
    expect(distinct.distinct).toEqual({ on: true })

    const united = asSelect(lite(sqliteCore.unionAll(db.select().from(todos), db.select().from(todos))))
    expect(united.setOps[0]!.type).toBe('unionAll')

    const ordered = asSelect(lite(db.select().from(todos).orderBy(desc(todos.id)).limit(5)))
    expect(ordered.orderBy[0]).toMatchObject({ direction: 'desc' })
    expect(ordered.limit).toEqual({ kind: 'value', value: 5 })
  })
})
