// The differential oracle over LIVE-SEEDED stateful graphs. Every stateful mechanic runs
// through the REAL registry + real compiled StatefulGraph seam + a real PGlite-backed
// HydrationExecutor: the graph SEEDS synchronously from PGlite (σ-scoped, pruned) — acquire BLOCKS
// on the seed, so it is PRECISE from tick one. H1 = seeds
// through the real seam; H2 = per-mechanic invalidation decisions (each join type, aggregate
// crossings incl. MIN/MAX retraction, HAVING, distinct, below-window insert, unselected-column
// update); H3 = graph fired IFF the SQL result changed (both directions; a false negative OR false
// positive is a hard fail) over ≥5 replayable seeds × 30 commits per exact mechanic. Comparison is
// bag-correct (no ORDER BY) or ordered + tie-canonical (with it).

import { PGlite } from '@electric-sql/pglite'
import { count, eq, gt, max, min, sql, sum } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { hydrationExecutorOf } from '../../drizzle/binding/hydrationExecutor.js'
import { type Change, compileQuery } from '../compile/compile.js'
import { extractQueryShape } from '../../drizzle/extract/queryShape.js'
import { canonicalValue } from '../../ir/canonical.js'
import type { LiveGraph } from './liveGraph.js'
import { createRegistry } from './registry.js'

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  name: pg.text('name'),
  score: pg.integer('score'),
})
const teams = pg.pgTable('teams', { id: pg.integer('id').primaryKey(), region: pg.text('region') })
// Distinct aliases of `users` so a set-op can chain several arms over the one table the generator
// mutates (distinct aliases avoid the duplicate-seed-input id; all route from real table `users`).
const u2 = pg.alias(users, 'u2')
const u3 = pg.alias(users, 'u3')

let client: PGlite
let db: ReturnType<typeof drizzle>

beforeAll(async () => {
  client = new PGlite()
  db = drizzle({ client })
  await client.exec('create table users (id int primary key, team_id int, name text, score int)')
  await client.exec('create table teams (id int primary key, region text)')
})
afterAll(async () => {
  await client.close()
})

// ── deterministic replayable generator (persisted seed corpus) ──────

function prng(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) | 0
    let t = Math.imul(state ^ (state >>> 15), 1 | state)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}
const NAMES = ['a1', 'a2', 'b1', 'x9', 'ax']
const pick = <T>(rng: () => number, xs: T[]): T => xs[Math.floor(rng() * xs.length)]!
const maybeNull = <T>(rng: () => number, value: T): T | null => (rng() < 0.2 ? null : value)
const genTeam = (rng: () => number) => maybeNull(rng, pick(rng, [1, 2, 3]))
const genScore = (rng: () => number) => maybeNull(rng, 1 + Math.floor(rng() * 8))
const genName = (rng: () => number) => pick(rng, NAMES)

type DbRow = Record<string, unknown>
const rowsOf = (result: unknown): DbRow[] => (result as { rows: DbRow[] }).rows

type UsersState = { ids: number[]; nextId: { value: number } }
async function mutateUsers(rng: () => number, st: UsersState): Promise<Change> {
  const op = st.ids.length === 0 ? 'insert' : pick(rng, ['insert', 'insert', 'update', 'delete'])
  if (op === 'insert') {
    const id = st.nextId.value++
    const inserted = rowsOf(
      await client.query('insert into users (id, team_id, name, score) values ($1,$2,$3,$4) returning *', [
        id,
        genTeam(rng),
        genName(rng),
        genScore(rng),
      ]),
    )
    st.ids.push(id)
    return { table: 'users', kind: 'insert', new: inserted[0]! }
  }
  const id = pick(rng, st.ids)
  const old = rowsOf(await client.query('select * from users where id = $1', [id]))[0]!
  if (op === 'delete') {
    await client.query('delete from users where id = $1', [id])
    st.ids.splice(st.ids.indexOf(id), 1)
    return { table: 'users', kind: 'delete', old }
  }
  const updated = rowsOf(
    await client.query('update users set team_id=$2, name=$3, score=$4 where id=$1 returning *', [
      id,
      genTeam(rng),
      genName(rng),
      genScore(rng),
    ]),
  )
  return { table: 'users', kind: 'update', old, new: updated[0]! }
}

type JoinState = { userIds: number[]; teamIds: number[]; next: { u: number; t: number } }
async function mutateJoin(rng: () => number, st: JoinState): Promise<Change> {
  const target = rng() < 0.5 || (st.userIds.length === 0 && st.teamIds.length === 0) ? 'users' : 'teams'
  if (target === 'teams') {
    const op = st.teamIds.length === 0 ? 'insert' : pick(rng, ['insert', 'insert', 'delete'])
    if (op === 'insert') {
      const id = pick(rng, [1, 2, 3, st.next.t++])
      const inserted = rowsOf(
        await client.query('insert into teams (id, region) values ($1,$2) on conflict do nothing returning *', [
          id,
          genName(rng),
        ]),
      )
      if (inserted[0]) {
        st.teamIds.push(id)
        return { table: 'teams', kind: 'insert', new: inserted[0] }
      }
      return { table: 'teams', kind: 'insert', new: { id: -1, region: null } } // conflict → inert change
    }
    const id = pick(rng, st.teamIds)
    const old = rowsOf(await client.query('select * from teams where id=$1', [id]))[0]!
    await client.query('delete from teams where id=$1', [id])
    st.teamIds.splice(st.teamIds.indexOf(id), 1)
    return { table: 'teams', kind: 'delete', old }
  }
  // mutateUsers uses `nextId.value++` for a new id; the throwaway object pre-increments st.next.u.
  return mutateUsers(rng, { ids: st.userIds, nextId: { value: st.next.u++ } })
}

// ── signatures (bag-correct + tie-canonical) ────────────────────────

function sortedRow(row: DbRow): DbRow {
  const out: DbRow = {}
  for (const key of Object.keys(row).sort()) out[key] = row[key]
  return out
}
function bagSignature(rows: DbRow[]): string {
  return rows
    .map((row) => canonicalValue(sortedRow(row)))
    .sort()
    .join('|')
}
function orderedSignature(rows: DbRow[], sortCols: string[]): string {
  const groups: Array<{ key: string; items: string[] }> = []
  for (const row of rows) {
    const key = canonicalValue(sortCols.map((col) => row[col]))
    if (groups.length === 0 || groups[groups.length - 1]!.key !== key) groups.push({ key, items: [] })
    groups[groups.length - 1]!.items.push(canonicalValue(sortedRow(row)))
  }
  return groups.map((group) => `${group.key}[${group.items.slice().sort().join(',')}]`).join('~')
}

// ── the live-hydrated oracle harness ────────────────────────────────

type Mechanic<St> = {
  name: string
  tables: string[]
  build: () => unknown
  sigSql: string
  sortCols?: string[]
  exact: boolean
  newState: () => St
  mutate: (rng: () => number, st: St) => Promise<Change>
  reset: () => Promise<void>
}

async function seedToLive(graph: LiveGraph, where: string): Promise<void> {
  await graph.ready() // acquire already blocked on the synchronous seed; this is instant
  expect(graph.state(), `${where}: graph must seed through the real seam to live`).toBe('live')
}

async function acquireLive<St>(m: Mechanic<St>, key: string): Promise<LiveGraph> {
  const registry = createRegistry({ maxStateRowsPerInput: 1e9 })
  const acquired = await registry.acquire({
    instanceKey: key,
    tables: m.tables,
    rlsStatus: false,
    compilePlan: () => compileQuery(extractQueryShape(m.build(), { dialect: 'pg' })),
    executor: hydrationExecutorOf(db),
    notify: () => {},
  })
  return acquired.graph
}

const SEEDS = [1, 7, 42, 1337, 90210]
const STEPS = 30

async function runLive<St>(m: Mechanic<St>): Promise<void> {
  for (const seed of SEEDS) {
    await m.reset()
    const rng = prng(seed)
    const st = m.newState()
    // 1. Seed a non-trivial baseline into PGlite (NOT fed to the graph — the seed below hydrates it).
    for (let i = 0; i < 12; i++) await m.mutate(rng, st)
    // 2. Acquire — which blocks on the seed from PGlite — then assert it reached live.
    const graph = await acquireLive(m, `${m.name}-${seed}`)
    await seedToLive(graph, `${m.name} seed=${seed}`)
    // 3. Post-flip differential: fired IFF the SQL result changed (both directions).
    const sig = async (): Promise<string> => {
      const rows = rowsOf(await client.query(m.sigSql))
      return m.sortCols ? orderedSignature(rows, m.sortCols) : bagSignature(rows)
    }
    let prev = await sig()
    for (let step = 0; step < STEPS; step++) {
      const change = await m.mutate(rng, st)
      const fired = graph.apply([change]).invalidated
      const next = await sig()
      const changed = next !== prev
      const where = `${m.name} [seed=${seed} step=${step} op=${change.kind}] (live)`
      if (m.exact) expect(fired, `${where}: exact plan must fire IFF the result changed`).toBe(changed)
      else if (changed) expect(fired, `${where}: a real change must never be missed`).toBe(true)
      prev = next
    }
  }
}

// ── per-mechanic hydrated invalidation, post-flip iff ────

const usersState = (): UsersState => ({ ids: [], nextId: { value: 1 } })
const joinState = (): JoinState => ({ userIds: [], teamIds: [], next: { u: 1, t: 100 } })
const resetUsers = async (): Promise<void> => {
  await client.exec('delete from users')
}
const resetJoin = async () => {
  await client.exec('delete from users')
  await client.exec('delete from teams')
}

describe('differential oracle against LIVE-HYDRATED stateful graphs', () => {
  it(
    'inner join (match/non-match): exact iff post-flip',
    () =>
      runLive({
        name: 'innerJoin',
        tables: ['users', 'teams'],
        build: () => db.select().from(users).innerJoin(teams, eq(teams.id, users.teamId)),
        sigSql:
          'select users.id uid, users.team_id, users.name uname, users.score, teams.id tid, teams.region from users inner join teams on teams.id = users.team_id',
        exact: true,
        newState: joinState,
        mutate: mutateJoin,
        reset: resetJoin,
      }),
    30_000,
  )

  it(
    'left join (null-extension): exact iff post-flip',
    () =>
      runLive({
        name: 'leftJoin',
        tables: ['users', 'teams'],
        build: () => db.select().from(users).leftJoin(teams, eq(teams.id, users.teamId)),
        sigSql:
          'select users.id uid, users.team_id, users.name uname, users.score, teams.id tid, teams.region from users left join teams on teams.id = users.team_id',
        exact: true,
        newState: joinState,
        mutate: mutateJoin,
        reset: resetJoin,
      }),
    30_000,
  )

  it(
    'sum group by (aggregate crossing + unselected-column update = no fire): exact iff',
    () =>
      runLive({
        name: 'sumGroup',
        tables: ['users'],
        build: () =>
          db
            .select({ team: users.teamId, s: sum(users.score) })
            .from(users)
            .groupBy(users.teamId),
        sigSql: 'select team_id, sum(score) s from users group by team_id',
        exact: true,
        newState: usersState,
        mutate: mutateUsers,
        reset: resetUsers,
      }),
    30_000,
  )

  it(
    'min group by (MIN retraction reveals the next value): exact iff',
    () =>
      runLive({
        name: 'minGroup',
        tables: ['users'],
        build: () =>
          db
            .select({ team: users.teamId, m: min(users.score) })
            .from(users)
            .groupBy(users.teamId),
        sigSql: 'select team_id, min(score) m from users group by team_id',
        exact: true,
        newState: usersState,
        mutate: mutateUsers,
        reset: resetUsers,
      }),
    30_000,
  )

  it(
    'max group by (MAX retraction): exact iff',
    () =>
      runLive({
        name: 'maxGroup',
        tables: ['users'],
        build: () =>
          db
            .select({ team: users.teamId, m: max(users.score) })
            .from(users)
            .groupBy(users.teamId),
        sigSql: 'select team_id, max(score) m from users group by team_id',
        exact: true,
        newState: usersState,
        mutate: mutateUsers,
        reset: resetUsers,
      }),
    30_000,
  )

  it(
    'distinct (suppression): exact iff post-flip',
    () =>
      runLive({
        name: 'distinct',
        tables: ['users'],
        build: () => db.selectDistinct({ team: users.teamId }).from(users),
        sigSql: 'select distinct team_id from users',
        exact: true,
        newState: usersState,
        mutate: mutateUsers,
        reset: resetUsers,
      }),
    30_000,
  )

  it(
    'topK window (below-window insert = no fire): exact iff post-flip',
    () =>
      runLive({
        name: 'window',
        tables: ['users'],
        build: () => db.select().from(users).orderBy(users.score, users.id).limit(3),
        sigSql: 'select id, team_id, name, score from users order by score, id limit 3',
        sortCols: ['score', 'id'],
        exact: true,
        newState: usersState,
        mutate: mutateUsers,
        reset: resetUsers,
      }),
    30_000,
  )

  it(
    'HAVING over an aggregate (licensed dirty): changed ⇒ fired (never missed)',
    () =>
      runLive({
        name: 'having',
        tables: ['users'],
        build: () =>
          db
            .select({ team: users.teamId, s: sum(users.score) })
            .from(users)
            .groupBy(users.teamId)
            .having(sql`sum(${users.score}) > 10`),
        sigSql: 'select team_id, sum(score) s from users group by team_id having sum(score) > 10',
        exact: false,
        newState: usersState,
        mutate: mutateUsers,
        reset: resetUsers,
      }),
    30_000,
  )

  it(
    'mixed UNION / UNION ALL chain (dirty — never miss): live-hydrated',
    () =>
      runLive({
        name: 'mixedSetOp',
        tables: ['users'],
        build: () =>
          pg.unionAll(
            pg.union(
              db.select({ id: users.id }).from(users).where(gt(users.score, 3)),
              db.select({ id: u2.id }).from(u2).where(eq(u2.teamId, 2)),
            ),
            db.select({ id: u3.id }).from(u3),
          ),
        sigSql:
          '(select users.id from users where score > 3 union select u2.id from users u2 where u2.team_id = 2) union all select u3.id from users u3',
        exact: false,
        newState: usersState,
        mutate: mutateUsers,
        reset: resetUsers,
      }),
    30_000,
  )

  it(
    'opaque GROUP BY (score % 2) membership move (dirty — never miss): live-hydrated',
    () =>
      runLive({
        name: 'opaqueGroup',
        tables: ['users'],
        build: () => db.select({ n: count() }).from(users).groupBy(sql`${users.score} % 2`),
        sigSql: 'select count(*) n from users group by (score % 2)',
        exact: false,
        newState: usersState,
        mutate: mutateUsers,
        reset: resetUsers,
      }),
    30_000,
  )
})

// The one-shot seed-race guard (a change routed during the scan lands exactly once, precise from
// tick one) is proven directly in liveGraph.lifecycle.spec against a real compiled graph + gated scan.
