import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { dirname, join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { count, desc, eq, exists, gt, inArray, sql } from 'drizzle-orm'
import * as mysqlCore from 'drizzle-orm/mysql-core'
import { drizzle as sqliteDrizzle } from 'drizzle-orm/node-sqlite'
import * as pgCore from 'drizzle-orm/pg-core'
import * as sqliteCore from 'drizzle-orm/sqlite-core'
import { describe, expect, it, vi } from 'vitest'
import { relationIdOf } from '../ir/relation.js'
import type { CoarseShape, Predicate, ProjItem, SelectShape } from '../ir/types.js'
import { crossCheckRenderedTables, extractQueryShape, renderedRelationsFromSQL } from './queryShape.js'

const requireFrom = createRequire(import.meta.url)

/** The resolved installed drizzle-orm version, so a golden names the pinned rc. */
function drizzleVersion(): string {
  let dir = dirname(requireFrom.resolve('drizzle-orm'))
  for (let i = 0; i < 8; i++) {
    try {
      const pkg = JSON.parse(readFileSync(join(dir, 'package.json'), 'utf8'))
      if (pkg.name === 'drizzle-orm') return pkg.version
    } catch {}
    dir = dirname(dir)
  }
  throw new Error('drizzle-orm package.json not found')
}

// ── pg fixtures (driver-free core QueryBuilder) ─────────────────────
const pgUsers = pgCore.pgTable('users', {
  id: pgCore.integer('id').primaryKey(),
  name: pgCore.text('name'),
  teamId: pgCore.integer('team_id'),
  done: pgCore.boolean('done'),
})
const pgTeams = pgCore.pgTable('teams', { id: pgCore.integer('id').primaryKey(), ownerId: pgCore.integer('owner_id') })
const pgTags = pgCore.pgTable('tags', { id: pgCore.integer('id').primaryKey(), teamId: pgCore.integer('team_id') })
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

  it('three-table join', () => {
    const s = asSelect(
      pg(
        pgQb
          .select()
          .from(pgUsers)
          .innerJoin(pgTeams, eq(pgTeams.id, pgUsers.teamId))
          .leftJoin(pgTags, eq(pgTags.teamId, pgTeams.id)),
      ),
    )
    expect(s.joins.map((j) => j.type)).toEqual(['inner', 'left'])
    expect(new Set(s.tables)).toEqual(new Set(['users', 'teams', 'tags']))
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

describe('extractQueryShape — subquery correlation (L9)', () => {
  const orders = pgCore.pgTable('orders', { id: pgCore.integer('id').primaryKey(), userId: pgCore.integer('user_id') })

  it('EXISTS fills the recursive inner shape and the outer↔inner equi-correlation', () => {
    const s = asSelect(
      pg(
        pgQb
          .select()
          .from(pgUsers)
          .where(exists(pgQb.select().from(orders).where(eq(orders.userId, pgUsers.id)))),
      ),
    )
    const where = s.where as Extract<Predicate, { kind: 'exists' }>
    expect(where.kind).toBe('exists')
    expect((where.inner as SelectShape).from.name).toBe('orders')
    expect(where.correlations).toEqual([
      { outer: { table: 'users', column: 'id' }, inner: { table: 'orders', column: 'user_id' } },
    ])
  })

  it('IN (subquery) fills the inner shape and the semi-join correlation', () => {
    const s = asSelect(
      pg(
        pgQb
          .select()
          .from(pgUsers)
          .where(inArray(pgUsers.teamId, pgQb.select({ id: pgTeams.id }).from(pgTeams))),
      ),
    )
    const where = s.where as Extract<Predicate, { kind: 'exists' }>
    expect(where.kind).toBe('exists')
    expect(where.inColumn).toEqual({ table: 'users', column: 'team_id' })
    expect((where.inner as SelectShape).from.name).toBe('teams')
    expect(where.correlations).toEqual([
      { outer: { table: 'users', column: 'team_id' }, inner: { table: 'teams', column: 'id' } },
    ])
  })

  it('a scalar subquery in WHERE fills the unknown-leaf inner shape', () => {
    const s = asSelect(
      pg(
        pgQb
          .select()
          .from(pgUsers)
          .where(eq(pgUsers.teamId, pgQb.select({ id: pgTeams.id }).from(pgTeams))),
      ),
    )
    const where = s.where as Extract<Predicate, { kind: 'unknown' }>
    expect(where.kind).toBe('unknown')
    expect((where.inner as SelectShape).from.name).toBe('teams')
    expect(new Set(s.tables)).toEqual(new Set(['users', 'teams']))
  })

  it('a scalar subquery in the projection fills the opaque item inner shape', () => {
    const s = asSelect(
      pg(
        pgQb
          .select({ id: pgUsers.id, teamCount: sql`(${pgQb.select({ id: pgTeams.id }).from(pgTeams)})` })
          .from(pgUsers),
      ),
    )
    const opaque = s.projection.items.find((item) => item.kind === 'opaque') as Extract<ProjItem, { kind: 'opaque' }>
    expect(opaque).toBeDefined()
    expect((opaque.inner as SelectShape).from.name).toBe('teams')
    expect(new Set(s.tables)).toEqual(new Set(['users', 'teams']))
  })

  const correlation = { outer: { table: 'users', column: 'id' }, inner: { table: 'teams', column: 'owner_id' } }

  it('a CORRELATED scalar subquery in WHERE attaches the outer↔inner correlation', () => {
    const s = asSelect(
      pg(
        pgQb
          .select()
          .from(pgUsers)
          .where(
            eq(pgUsers.teamId, pgQb.select({ id: pgTeams.id }).from(pgTeams).where(eq(pgTeams.ownerId, pgUsers.id))),
          ),
      ),
    )
    expect((s.where as Extract<Predicate, { kind: 'unknown' }>).correlations).toEqual([correlation])
  })

  it('a CORRELATED scalar subquery in the projection attaches the correlation', () => {
    const s = asSelect(
      pg(
        pgQb
          .select({
            id: pgUsers.id,
            owned: sql`(${pgQb.select({ id: pgTeams.id }).from(pgTeams).where(eq(pgTeams.ownerId, pgUsers.id))})`,
          })
          .from(pgUsers),
      ),
    )
    const opaque = s.projection.items.find((item) => item.kind === 'opaque') as Extract<ProjItem, { kind: 'opaque' }>
    expect(opaque.correlations).toEqual([correlation])
  })

  it('an UNCORRELATED scalar subquery has no correlations (no false positives)', () => {
    const s = asSelect(
      pg(
        pgQb
          .select()
          .from(pgUsers)
          .where(eq(pgUsers.teamId, pgQb.select({ id: pgTeams.id }).from(pgTeams))),
      ),
    )
    expect((s.where as Extract<Predicate, { kind: 'unknown' }>).correlations).toEqual([])
  })
})

describe('extractQueryShape — coarse fallback', () => {
  it('a subquery/CTE FROM degrades to a CoarseShape recovering its tables', () => {
    const sub = pgQb.select({ id: pgUsers.id, teamId: pgUsers.teamId }).from(pgUsers).as('sub')
    const shape = pg(pgQb.select().from(sub))
    expect(shape.kind).toBe('coarse')
    expect(shape.tables).toContain('users')
  })

  it('an unreadable select config degrades to a CoarseShape recovered from the rendered SQL', () => {
    const real = pgQb.select().from(pgUsers).innerJoin(pgTeams, eq(pgTeams.id, pgUsers.teamId))
    // config fails the pinned-shape assertion, but the SQL still renders both relations
    const broken = { config: { table: {}, fields: {}, joins: 'not-an-array' }, toSQL: () => real.toSQL() }
    const shape = extractQueryShape(broken, { dialect: 'pg' })
    expect(shape.kind).toBe('coarse')
    expect(new Set(shape.tables)).toEqual(new Set(['users', 'teams']))
  })

  it('production cross-check discovers a pg join the config underreports but the SQL renders', () => {
    // A pinned-shape wrapper hides the join in config.joins, but toSQL still renders
    // A INNER JOIN B — the cross-check reads the rendered SQL, so it degrades.
    const wrapper = {
      config: { table: pgUsers, fields: { id: pgUsers.id }, joins: [] },
      toSQL: () => ({
        sql: 'select "users"."id" from "users" inner join "teams" on "teams"."id" = "users"."team_id"',
        params: [],
      }),
    }
    const shape = extractQueryShape(wrapper, { dialect: 'pg' })
    expect(shape.kind).toBe('coarse')
    expect(new Set(shape.tables)).toEqual(new Set(['users', 'teams']))
  })

  it('production cross-check discovers a MySQL backtick join the config underreports', () => {
    const wrapper = {
      config: { table: pgUsers, fields: { id: pgUsers.id }, joins: [] },
      toSQL: () => ({
        sql: 'select `users`.`id` from `users` inner join `teams` on `teams`.`id` = `users`.`team_id`',
        params: [],
      }),
    }
    const shape = extractQueryShape(wrapper, { dialect: 'mysql' })
    expect(shape.kind).toBe('coarse')
    expect(new Set(shape.tables)).toEqual(new Set(['users', 'teams']))
  })

  it('an untrackable read with no recoverable relation is a typed untrackable coarse shape (M3)', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const shape = extractQueryShape({ config: undefined, _: undefined }, { dialect: 'pg' })
    expect(shape.kind).toBe('coarse')
    expect(shape as CoarseShape).toMatchObject({ tables: [], untrackable: true })
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('untrackable read'))
    warn.mockRestore()
  })

  // What `untrackable` actually costs, pinned. The rejection reads as though CTE, subquery and raw-SQL
  // reads were the population it turns away; they are not — each recovers its real relations from the
  // rendered SQL and routes soundly. Only a read that depends on NO relation reaches the rejection, and
  // for those a live subscription would be meaningless rather than merely coarse. Pinned because it is
  // the whole argument for leaving the rejection in place: if a shape below ever starts coming back
  // `untrackable`, a real read has silently joined that population and the trade needs re-deciding.
  describe('the untrackable rejection turns away only relation-less reads', () => {
    const cte = pgQb.$with('sq').as(pgQb.select().from(pgUsers))

    it('a CTE-prefixed read recovers the CTE body’s real relation, so it is trackable', () => {
      const shape = extractQueryShape(pgQb.with(cte).select().from(cte), { dialect: 'pg' })
      expect((shape as CoarseShape).untrackable).toBeUndefined()
      // `users` is the routable one. `sq` is the CTE's own name, recovered as an inert extra: no change
      // ever carries it, and a real table of that name would only over-invalidate. Over-coverage is the
      // safe direction — the read still hears every write to `users`.
      expect(new Set(shape.tables)).toEqual(new Set(['users', 'sq']))
    })

    it('a subquery in FROM recovers the inner relation, so it is trackable', () => {
      const shape = extractQueryShape(pgQb.select().from(pgQb.select().from(pgUsers).as('s')), { dialect: 'pg' })
      expect((shape as CoarseShape).untrackable).toBeUndefined()
      expect(shape.tables).toContain('users')
    })

    it('a raw-SQL FROM naming a quoted table is trackable', () => {
      const shape = extractQueryShape(pgQb.select().from(sql`"users"` as never), { dialect: 'pg' })
      expect((shape as CoarseShape).untrackable).toBeUndefined()
      expect(shape.tables).toContain('users')
    })

    it('a read with no FROM depends on no relation, so there is nothing to subscribe to', () => {
      const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
      const shape = extractQueryShape(pgQb.select({ n: sql<number>`1` }), { dialect: 'pg' })
      expect(shape as CoarseShape).toMatchObject({ tables: [], untrackable: true })
      warn.mockRestore()
    })

    it('a read FROM a set-returning function depends on no relation either', () => {
      const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
      const shape = extractQueryShape(pgQb.select().from(sql`generate_series(1, 10)` as never), { dialect: 'pg' })
      expect(shape as CoarseShape).toMatchObject({ tables: [], untrackable: true })
      warn.mockRestore()
    })
  })

  it('unit cross-check: rendered A JOIN B, extracted A → coarse; complete extraction passes', () => {
    const onlyA: SelectShape = {
      kind: 'select',
      dialect: 'pg',
      from: { name: 'A', alias: 'A', id: relationIdOf({ name: 'A' }), primaryKey: [] },
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
    expect(crossCheckRenderedTables(onlyA, ['A'])).toBe(onlyA)
  })
})

describe('renderedRelationsFromSQL — every dialect quoting', () => {
  const rel = (sqlText: string) => renderedRelationsFromSQL({ toSQL: () => ({ sql: sqlText, params: [] }) })

  it('double-quoted (pg/sqlite) identifiers, schema-qualified and aliased', () => {
    expect(rel('select * from "users" inner join "teams" on 1=1')).toEqual(['users', 'teams'])
    // A rendered schema qualification is KEPT in the identity — dropping it to the bare name is what let
    // a write on `a.users` reach a live query on `b.users`.
    expect(rel('select * from "a"."users"')).toEqual([relationIdOf({ name: 'users', schema: 'a' })])
    expect(rel('select * from "a"."users"')).not.toEqual(rel('select * from "b"."users"'))
    expect(rel('select * from "users" "u"')).toEqual(['users']) // alias not captured
  })

  it('backtick (mysql) identifiers, schema-qualified', () => {
    expect(rel('select * from `users` inner join `teams` on 1=1')).toEqual(['users', 'teams'])
    expect(rel('select * from `db`.`users`')).toEqual([relationIdOf({ name: 'users', schema: 'db' })])
  })

  it('bracket quoting is never recognized — drizzle emits none', () => {
    expect(rel('select * from [users] join [teams] on 1=1')).toEqual([])
  })

  it('the word "from" inside a string literal is not a relation', () => {
    expect(rel(`select * from "users" where note = 'from here'`)).toEqual(['users'])
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

  it('ordering, limit/offset and placeholder rows carry through', () => {
    const ordered = asSelect(my(qb.select().from(users).orderBy(desc(users.id)).limit(10).offset(5)))
    expect(ordered.orderBy[0]).toMatchObject({ direction: 'desc' })
    expect(ordered.limit).toEqual({ kind: 'value', value: 10 })
    expect(ordered.offset).toEqual({ kind: 'value', value: 5 })
    const dyn = asSelect(my(qb.select().from(users).limit(sql.placeholder('lim'))))
    expect(dyn.limit).toEqual({ kind: 'placeholder', name: 'lim' })
  })
})

// ── sqlite fixtures (explicit node:sqlite DatabaseSync via node-sqlite) ──
describe('extractQueryShape — sqlite goldens (explicit node:sqlite DatabaseSync)', () => {
  const todos = sqliteCore.sqliteTable('todos', {
    id: sqliteCore.integer('id').primaryKey(),
    text: sqliteCore.text('text'),
    done: sqliteCore.integer('done', { mode: 'boolean' }),
    userId: sqliteCore.integer('user_id'),
  })
  const users = sqliteCore.sqliteTable('users', { id: sqliteCore.integer('id').primaryKey() })
  const client = new DatabaseSync(':memory:') // the contract-mandated explicit construction
  const db = sqliteDrizzle({ client })
  const lite = (b: Parameters<typeof extractQueryShape>[0]) => extractQueryShape(b, { dialect: 'sqlite' })

  it('is built on the pinned drizzle-orm@1.0.0-rc.4', () => {
    expect(drizzleVersion()).toBe('1.0.0-rc.4')
  })

  it('builds and extracts shapes through node-sqlite + node:sqlite DatabaseSync', () => {
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

    const paginated = asSelect(lite(db.select().from(todos).limit(sql.placeholder('n')).offset(2)))
    expect(paginated.limit).toEqual({ kind: 'placeholder', name: 'n' })
    expect(paginated.offset).toEqual({ kind: 'value', value: 2 })
  })
})
