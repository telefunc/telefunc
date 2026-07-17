// The differential soundness gate. For each stateless mechanic (and each named
// degradation) we compile the drizzle query, then drive a property-generated,
// deterministically-seeded (hence replayable) sequence of commits against a PGlite
// database. After every commit we compare `graph invalidated?` against `did the SQL result
// change?`, where "changed" is an injectively-encoded BAG (multiplicity + column types)
// when there is NO ORDER BY, and an ordered serialization canonicalized WITHIN each
// equal-sort-key group when there is (so an equal-sort-key permutation is oracle-
// equivalent). EXACT plans must fire IFF the result changed — both directions. Degraded
// plans may over-fire only where licensed, but a false negative is ALWAYS a failure.

import { PGlite } from '@electric-sql/pglite'
import { and, between, count, eq, gt, inArray, isNull, like, sql, sum } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { extractQueryShape } from '../extract/queryShape.js'
import { type Change, compileQuery } from './compile.js'
import { canonicalValue } from '../utils/canonical.js'

const users = pg.pgTable('users', {
  id: pg.integer('id').primaryKey(),
  teamId: pg.integer('team_id'),
  name: pg.text('name'),
  score: pg.integer('score'),
})
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

// ── Deterministic, replayable generator ─────────────────────────────

/** mulberry32 — a small deterministic PRNG; the fixed seed list below IS the persisted,
 *  replayable seed corpus. */
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

/** Apply one random mutation to PGlite and return the corresponding change (rows keyed by
 *  db column name, matching the graph's rowView). */
async function mutate(rng: () => number, ids: number[], nextId: { value: number }): Promise<Change> {
  const op = ids.length === 0 ? 'insert' : pick(rng, ['insert', 'insert', 'update', 'delete'])
  if (op === 'insert') {
    const id = nextId.value++
    const inserted = rowsOf(
      await client.query('insert into users (id, team_id, name, score) values ($1,$2,$3,$4) returning *', [
        id,
        genTeam(rng),
        genName(rng),
        genScore(rng),
      ]),
    )
    ids.push(id)
    return { table: 'users', kind: 'insert', new: inserted[0]! }
  }
  const id = pick(rng, ids)
  const old = rowsOf(await client.query('select * from users where id = $1', [id]))[0]!
  if (op === 'delete') {
    await client.query('delete from users where id = $1', [id])
    ids.splice(ids.indexOf(id), 1)
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

// ── Result signatures ───────────────────────────────────────────────

function bagSignature(rows: DbRow[]): string {
  return rows
    .map((row) => canonicalValue(sortedRow(row)))
    .sort()
    .join('|')
}

/** Ordered serialization keyed by the sort columns, canonicalized within each equal-sort-
 *  key group — an equal-sort-key permutation is equivalent, a sort-key-value change is not. */
function orderedSignature(rows: DbRow[], sortCols: string[]): string {
  const groups: Array<{ key: string; items: string[] }> = []
  for (const row of rows) {
    const key = canonicalValue(sortCols.map((col) => row[col]))
    if (groups.length === 0 || groups[groups.length - 1]!.key !== key) groups.push({ key, items: [] })
    groups[groups.length - 1]!.items.push(canonicalValue(sortedRow(row)))
  }
  return groups.map((group) => `${group.key}[${group.items.slice().sort().join(',')}]`).join('~')
}

function sortedRow(row: DbRow): DbRow {
  const out: DbRow = {}
  for (const key of Object.keys(row).sort()) out[key] = row[key]
  return out
}

// ── The oracle ──────────────────────────────────────────────────────

type Spec = {
  name: string
  build: () => unknown
  exact: boolean
  sortCols?: string[] // present ⇒ ORDER BY signature
}

const SEEDS = [1, 7, 42, 1337, 90210]
const STEPS = 30

async function oracle(spec: Spec): Promise<void> {
  for (const seed of SEEDS) {
    await client.exec('delete from users')
    const graph = compileQuery(extractQueryShape(spec.build(), { dialect: 'pg' })).instantiate()
    const rng = prng(seed)
    const ids: number[] = []
    const nextId = { value: 1 }
    const sig = async () =>
      spec.sortCols
        ? orderedSignature(rowsOf(await client.query(renderSql(spec))), spec.sortCols)
        : bagSignature(rowsOf(await client.query(renderSql(spec))))
    let prev = await sig()
    for (let step = 0; step < STEPS; step++) {
      const change = await mutate(rng, ids, nextId)
      const fired = graph.apply([change]).invalidated
      const next = await sig()
      const changed = next !== prev
      const where = `seed=${seed} step=${step} op=${change.kind}`
      if (spec.exact) {
        expect(fired, `${spec.name} [${where}]: exact plan must fire IFF the result changed`).toBe(changed)
      } else if (changed) {
        expect(fired, `${spec.name} [${where}]: a real change must never be missed`).toBe(true)
      }
      prev = next
    }
  }
}

// The comparison always reads the FULL row (so bag/ordered signatures see every column and
// the sort keys), while the graph is compiled from the spec's actual (possibly narrower)
// query — the graph's fire must still agree with the projected result changing.
function renderSql(spec: Spec): string {
  const rendered = (spec.build() as { toSQL(): { sql: string; params: unknown[] } }).toSQL()
  // Substitute the (few, simple) params inline for the raw comparison read.
  let sqlText = rendered.sql
  rendered.params.forEach((param, index) => {
    const literal = param === null ? 'null' : typeof param === 'number' ? String(param) : `'${String(param)}'`
    sqlText = sqlText.replace(`$${index + 1}`, literal)
  })
  return sqlText
}

describe('oracle — exact stateless mechanics', () => {
  it('WHERE eq', () =>
    oracle({ name: 'eq', build: () => db.select().from(users).where(eq(users.teamId, 2)), exact: true }))
  it('WHERE and(eq, gt)', () =>
    oracle({
      name: 'and',
      build: () =>
        db
          .select()
          .from(users)
          .where(and(eq(users.teamId, 2), gt(users.score, 3))),
      exact: true,
    }))
  it('WHERE between', () =>
    oracle({
      name: 'between',
      build: () =>
        db
          .select()
          .from(users)
          .where(between(users.score, 3, 6)),
      exact: true,
    }))
  it('WHERE in', () =>
    oracle({
      name: 'in',
      build: () =>
        db
          .select()
          .from(users)
          .where(inArray(users.teamId, [1, 2])),
      exact: true,
    }))
  it('WHERE like', () =>
    oracle({ name: 'like', build: () => db.select().from(users).where(like(users.name, 'a%')), exact: true }))
  it('WHERE isNull', () =>
    oracle({ name: 'isNull', build: () => db.select().from(users).where(isNull(users.score)), exact: true }))
  it('narrow projection (unselected-column change does not fire)', () =>
    oracle({
      name: 'projection',
      build: () => db.select({ team: users.teamId }).from(users).where(eq(users.teamId, 2)),
      exact: true,
    }))
})

describe('oracle — ORDER BY (tie-canonical bag)', () => {
  it('ORDER BY score, name (no LIMIT) fires on a sort-key change, not an equal-key permutation', () =>
    oracle({
      name: 'orderBy',
      build: () => db.select({ score: users.score, name: users.name }).from(users).orderBy(users.score, users.name),
      exact: true,
      sortCols: ['score', 'name'],
    }))
})

describe('oracle — licensed degradations (never miss)', () => {
  it('LIMIT without ORDER BY (dirty)', () =>
    oracle({ name: 'limit', build: () => db.select().from(users).limit(3), exact: false }))
  it('opaque WHERE (dirty)', () =>
    oracle({
      name: 'opaque',
      build: () => db.select().from(users).where(sql`${users.name} similar to 'a%'`),
      exact: false,
    }))
  it('mixed UNION / UNION ALL chain (dirty — SQL is left-associative, so the mixed distinct boundary degrades)', () =>
    oracle({
      name: 'mixedSetOp',
      build: () =>
        pg.unionAll(
          pg.union(
            db.select({ id: users.id }).from(users).where(gt(users.score, 3)),
            db.select({ id: u2.id }).from(u2).where(eq(u2.teamId, 2)),
          ),
          db.select({ id: u3.id }).from(u3),
        ),
      exact: false,
    }))
  it('opaque GROUP BY (score % 2) — a membership move between hidden groups must never be missed', () =>
    oracle({
      name: 'opaqueGroup',
      build: () => db.select({ n: count() }).from(users).groupBy(sql`${users.score} % 2`),
      exact: false,
    }))
})

describe('oracle — ungrouped aggregate over an empty baseline (exact)', () => {
  it('sum(score): fired IFF the result changed, with NO first-row false positive', () =>
    oracle({ name: 'sumAgg', build: () => db.select({ s: sum(users.score) }).from(users), exact: true }))
  it('count(name): fired IFF the result changed', () =>
    oracle({ name: 'countCol', build: () => db.select({ c: count(users.name) }).from(users), exact: true }))
})

// ── Stateful (delta-level) differential: an INNER equi-join fed the SAME commits as the
// DB, comparing the graph's incremental fire against the re-run join result. Hydration is
// so the graph state is built up purely from the fed deltas.

const teams = pg.pgTable('teams', { id: pg.integer('id').primaryKey(), region: pg.text('region') })

async function mutateJoin(
  rng: () => number,
  userIds: number[],
  teamIds: number[],
  next: { u: number; t: number },
): Promise<Change> {
  const target = rng() < 0.5 || (userIds.length === 0 && teamIds.length === 0) ? 'users' : 'teams'
  if (target === 'teams') {
    const op = teamIds.length === 0 ? 'insert' : pick(rng, ['insert', 'insert', 'delete'])
    if (op === 'insert') {
      const id = pick(rng, [1, 2, 3, next.t++])
      const inserted = rowsOf(
        await client.query('insert into teams (id, region) values ($1,$2) on conflict do nothing returning *', [
          id,
          genName(rng),
        ]),
      )
      if (inserted[0]) {
        teamIds.push(id)
        return { table: 'teams', kind: 'insert', new: inserted[0] }
      }
      return { table: 'teams', kind: 'insert', new: { id: -1, region: null } } // conflict → inert change
    }
    const id = pick(rng, teamIds)
    const old = rowsOf(await client.query('select * from teams where id=$1', [id]))[0]!
    await client.query('delete from teams where id=$1', [id])
    teamIds.splice(teamIds.indexOf(id), 1)
    return { table: 'teams', kind: 'delete', old }
  }
  return mutate(rng, userIds, { value: next.u++ })
}

describe('oracle — stateful INNER join (delta-level, hand-fed)', () => {
  it('graph fire agrees with the re-run join result over generated commits', async () => {
    for (const seed of SEEDS) {
      await client.exec('delete from users')
      await client.exec('delete from teams')
      const build = () => db.select().from(users).innerJoin(teams, eq(teams.id, users.teamId))
      const graph = compileQuery(extractQueryShape(build(), { dialect: 'pg' })).instantiate()
      const rng = prng(seed)
      const userIds: number[] = []
      const teamIds: number[] = []
      const next = { u: 100, t: 100 }
      const sig = async () =>
        bagSignature(
          rowsOf(
            await client.query(
              'select users.id uid, users.team_id, teams.id tid, teams.region from users inner join teams on teams.id = users.team_id',
            ),
          ),
        )
      let prev = await sig()
      for (let step = 0; step < STEPS; step++) {
        const change = await mutateJoin(rng, userIds, teamIds, next)
        const fired = graph.apply([change]).invalidated
        const nextSig = await sig()
        const changed = nextSig !== prev
        if (changed) expect(fired, `join [seed=${seed} step=${step}]: a real change must never be missed`).toBe(true)
        prev = nextSig
      }
    }
  })
})
