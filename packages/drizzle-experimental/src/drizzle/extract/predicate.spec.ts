import {
  and,
  between,
  eq,
  exists,
  gt,
  gte,
  ilike,
  inArray,
  isNotNull,
  isNull,
  like,
  lt,
  lte,
  ne,
  not,
  notBetween,
  notExists,
  notIlike,
  notInArray,
  notLike,
  or,
  sql,
} from 'drizzle-orm'
import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { dirname, join } from 'node:path'
import { QueryBuilder, alias, boolean, integer, pgTable, text, timestamp } from 'drizzle-orm/pg-core'
import { describe, expect, it } from 'vitest'
import { eval3, rowView } from '../../ir/eval.js'
import type { CompareOp, Dialect, Predicate, ScalarExpr, Tri } from '../../ir/types.js'
import { conjunctsOf, extractPredicate, parsePredicate, toNNF } from './predicate.js'

const requireFrom = createRequire(import.meta.url)
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

const todos = pgTable('todos', {
  id: integer('id').primaryKey(),
  text: text('text'),
  done: boolean('done'),
  userId: integer('user_id'),
  createdAt: timestamp('created_at'),
})
const users = pgTable('users', { id: integer('id').primaryKey(), name: text('name') })
const qb = new QueryBuilder()

type Cond = Parameters<typeof extractPredicate>[0]
const ev = (cond: Cond, record: Record<string, unknown>, dialect?: Dialect): Tri =>
  eval3(extractPredicate(cond, { dialect }).predicate, rowView(record))

describe('extractPredicate — operators', () => {
  it('no condition is TRUE', () => {
    const r = extractPredicate(undefined)
    expect(r.predicate).toEqual({ kind: 'true' })
    expect(r.exact).toBe(true)
    expect(r.tables).toEqual([])
  })

  it('comparison operators', () => {
    expect(ev(eq(todos.userId, 42), { user_id: 42 })).toBe(true)
    expect(ev(eq(todos.userId, 42), { user_id: 7 })).toBe(false)
    expect(ev(ne(todos.userId, 42), { user_id: 7 })).toBe(true)
    expect(ev(ne(todos.userId, 42), { user_id: 42 })).toBe(false)
    expect(ev(gt(todos.id, 5), { id: 6 })).toBe(true)
    expect(ev(gt(todos.id, 5), { id: 5 })).toBe(false)
    expect(ev(gte(todos.id, 5), { id: 5 })).toBe(true)
    expect(ev(lt(todos.id, 5), { id: 4 })).toBe(true)
    expect(ev(lte(todos.id, 5), { id: 6 })).toBe(false)
  })

  it('records referenced tables and exactness', () => {
    const r = extractPredicate(eq(todos.userId, 1))
    expect(r.tables).toEqual(['todos'])
    expect(r.exact).toBe(true)
  })

  it('and / or / not, including nesting', () => {
    const condition = and(eq(todos.userId, 1), or(eq(todos.done, false), gt(todos.id, 100)))
    expect(ev(condition, { user_id: 1, done: false, id: 1 })).toBe(true)
    expect(ev(condition, { user_id: 1, done: true, id: 200 })).toBe(true)
    expect(ev(condition, { user_id: 1, done: true, id: 1 })).toBe(false)
    expect(ev(condition, { user_id: 2, done: false, id: 1 })).toBe(false)
    expect(ev(not(eq(todos.done, true)), { done: false })).toBe(true)
    expect(ev(not(eq(todos.done, true)), { done: true })).toBe(false)
  })

  it('inArray / notInArray, including the empty list', () => {
    expect(ev(inArray(todos.id, [1, 2, 3]), { id: 2 })).toBe(true)
    expect(ev(inArray(todos.id, [1, 2, 3]), { id: 4 })).toBe(false)
    expect(ev(notInArray(todos.id, [1, 2]), { id: 3 })).toBe(true)
    expect(ev(notInArray(todos.id, [1, 2]), { id: 1 })).toBe(false)
    expect(ev(inArray(todos.id, []), { id: 1 })).toBe(false)
    expect(ev(notInArray(todos.id, []), { id: 1 })).toBe(true)
  })

  it('the five named negations parse directly (notInArray/notBetween/notLike/notIlike/notExists)', () => {
    expect(ev(notInArray(todos.id, [1, 2]), { id: 3 })).toBe(true)
    expect(ev(notBetween(todos.id, 10, 20), { id: 25 })).toBe(true)
    expect(ev(notBetween(todos.id, 10, 20), { id: 15 })).toBe(false)
    expect(ev(notLike(todos.text, 'buy%'), { text: 'sell' })).toBe(true)
    expect(ev(notLike(todos.text, 'buy%'), { text: 'buy milk' })).toBe(false)
    expect(ev(notIlike(todos.text, 'buy%'), { text: 'SELL' })).toBe(true)
    expect(ev(notIlike(todos.text, 'buy%'), { text: 'BUY' })).toBe(false)

    const r = extractPredicate(notExists(qb.select().from(users).where(eq(users.id, todos.userId))))
    expect(r.predicate).toMatchObject({ kind: 'exists', negated: true })
    expect(r.tables).toContain('users')
    expect(eval3(r.predicate, rowView({ user_id: 1 }))).toBeUndefined()
  })

  it('positive exists(subquery) records the inner tables and is unknown per row (J1)', () => {
    const r = extractPredicate(exists(qb.select().from(users).where(eq(users.id, todos.userId))))
    expect(r.predicate).toMatchObject({ kind: 'exists', negated: false })
    expect(r.tables).toContain('users')
    expect(eval3(r.predicate, rowView({ user_id: 1 }))).toBeUndefined()
  })

  it('an escaped %/_ in a LIKE pattern is a literal', () => {
    expect(ev(like(todos.text, 'a\\%b'), { text: 'a%b' })).toBe(true)
    expect(ev(like(todos.text, 'a\\%b'), { text: 'axyzb' })).toBe(false)
    // sqlite has no default escape: the backslash is literal, % stays a wildcard
    expect(ev(like(todos.text, 'a\\%b'), { text: 'a\\zzb' }, 'sqlite')).toBe(true)
  })

  it('like / ilike with % and _ wildcards', () => {
    expect(ev(like(todos.text, 'buy%'), { text: 'buy milk' })).toBe(true)
    expect(ev(like(todos.text, 'buy%'), { text: 'sell milk' })).toBe(false)
    expect(ev(like(todos.text, 'buy%'), { text: 'BUY milk' })).toBe(false)
    expect(ev(ilike(todos.text, 'buy%'), { text: 'BUY milk' })).toBe(true)
    expect(ev(like(todos.text, 'b_y'), { text: 'buy' })).toBe(true)
    expect(ev(like(todos.text, 'b_y'), { text: 'buuy' })).toBe(false)
    expect(ev(like(todos.text, 'a.c'), { text: 'abc' })).toBe(false) // metacharacters stay literal
  })

  it('sqlite LIKE is case-insensitive by dialect', () => {
    expect(ev(like(todos.text, 'buy%'), { text: 'BUY milk' }, 'sqlite')).toBe(true)
    expect(ev(like(todos.text, 'buy%'), { text: 'BUY milk' }, 'pg')).toBe(false)
  })

  it('isNull / isNotNull distinguish NULL from a value', () => {
    expect(ev(isNull(todos.text), { text: null })).toBe(true)
    expect(ev(isNull(todos.text), { text: 'x' })).toBe(false)
    expect(ev(isNotNull(todos.text), { text: 'x' })).toBe(true)
    expect(ev(isNotNull(todos.text), { text: null })).toBe(false)
  })

  it('between, and between inside and() is not split apart', () => {
    expect(ev(between(todos.id, 10, 20), { id: 15 })).toBe(true)
    expect(ev(between(todos.id, 10, 20), { id: 9 })).toBe(false)
    const combined = and(between(todos.id, 10, 20), eq(todos.done, false))
    expect(ev(combined, { id: 15, done: false })).toBe(true)
    expect(ev(combined, { id: 15, done: true })).toBe(false)
    expect(ev(combined, { id: 25, done: false })).toBe(false)
  })

  it('column-to-column comparison', () => {
    expect(ev(eq(todos.id, todos.userId), { id: 3, user_id: 3 })).toBe(true)
    expect(ev(eq(todos.id, todos.userId), { id: 3, user_id: 4 })).toBe(false)
  })

  it('placeholders are resolvable, not inexact: unknown per row, table recorded', () => {
    const r = extractPredicate(eq(todos.id, sql.placeholder('uid')))
    expect(r.exact).toBe(true)
    expect(r.tables).toEqual(['todos'])
    expect(eval3(r.predicate, rowView({ id: 5 }))).toBeUndefined()
  })

  it('a missing column is unknown (widens) and survives negation', () => {
    expect(ev(eq(todos.userId, 42), {})).toBeUndefined()
    expect(ev(not(eq(todos.userId, 42)), {})).toBeUndefined()
    expect(ev(inArray(todos.id, [1]), {})).toBeUndefined()
    expect(ev(like(todos.text, 'x%'), {})).toBeUndefined()
    expect(ev(isNull(todos.text), {})).toBeUndefined() // MISSING ≠ known-null
  })

  it('a captured NULL comparison is unknown (faithful SQL 3VL)', () => {
    expect(ev(eq(todos.userId, 42), { user_id: null })).toBeUndefined()
    expect(ev(gt(todos.id, 5), { id: null })).toBeUndefined()
    expect(ev(inArray(todos.id, [1, 2]), { id: null })).toBeUndefined()
  })

  it('cross-producer value coercion', () => {
    expect(ev(eq(todos.done, true), { done: 1 })).toBe(true) // sqlite 0/1 booleans
    expect(ev(eq(todos.done, true), { done: 0 })).toBe(false)
    expect(ev(gt(todos.id, 5), { id: '6' })).toBe(true) // pg numeric as string
    const cutoff = new Date('2026-01-01T00:00:00Z')
    expect(ev(gt(todos.createdAt, cutoff), { created_at: '2026-06-01T00:00:00Z' })).toBe(true)
    expect(ev(gt(todos.createdAt, cutoff), { created_at: '2025-06-01T00:00:00Z' })).toBe(false)
  })

  it('raw sql`` degrades to an unknown leaf and marks the condition inexact', () => {
    const r = extractPredicate(and(eq(todos.userId, 1), sql`char_length(${todos.text}) > 3`))
    expect(r.exact).toBe(false)
    expect(ev(and(eq(todos.userId, 1), sql`char_length(${todos.text}) > 3`), { user_id: 2, text: 'abcd' })).toBe(false)
    expect(ev(and(eq(todos.userId, 1), sql`char_length(${todos.text}) > 3`), { user_id: 1, text: 'x' })).toBeUndefined()
  })

  it('col IN (subquery) becomes a correlated semi-join leaf recording both tables', () => {
    const sub = qb.select({ id: users.id }).from(users).where(eq(users.name, 'a'))
    const r = extractPredicate(inArray(todos.userId, sub))
    expect(r.predicate).toMatchObject({
      kind: 'exists',
      negated: false,
      inColumn: { table: 'todos', column: 'user_id' },
    })
    expect(r.tables).toEqual(expect.arrayContaining(['todos', 'users']))
  })

  it('a scalar subquery operand becomes an unknown leaf carrying the subquery', () => {
    const sub = qb.select({ id: users.id }).from(users)
    const r = extractPredicate(eq(todos.userId, sub))
    expect(r.predicate).toMatchObject({ kind: 'unknown' })
    expect((r.predicate as Extract<Predicate, { kind: 'unknown' }>).subquery).toBeDefined()
    expect(r.tables).toEqual(expect.arrayContaining(['todos', 'users']))
  })

  it('aliased tables resolve to their original name', () => {
    const t2 = alias(todos, 't2')
    const r = extractPredicate(eq(t2.userId, 7))
    expect(r.tables).toEqual(['todos'])
    expect(eval3(r.predicate, rowView({ user_id: 7 }))).toBe(true)
  })

  it('conjunctsOf splits top-level and(), flattens nesting, keeps or()/between whole', () => {
    const p = (c: Cond) => parsePredicate(c).predicate
    expect(conjunctsOf(p(and(eq(todos.userId, 1), eq(todos.done, false), gt(todos.id, 5))))).toHaveLength(3)
    expect(conjunctsOf(p(and(and(eq(todos.userId, 1), eq(todos.done, false)), gt(todos.id, 5))))).toHaveLength(3)
    expect(conjunctsOf(p(or(eq(todos.userId, 1), eq(todos.done, false))))).toHaveLength(1)
    expect(conjunctsOf(p(between(todos.id, 1, 2)))).toHaveLength(1)
    expect(conjunctsOf(p(eq(todos.userId, 1)))).toHaveLength(1)
  })
})

// ── NNF equivalence property ────────────────────────────────────────
// eval3(nnf(p)) === eval3(p) for every predicate and row. This is why NNF is safe:
// the transform preserves the Kleene truth value (including NULL-comparison unknowns).
// The run asserts coverage so a "passing" seed can't silently skip truth values,
// NULL/MISSING rows, deep NOT nesting, or any native negation pair.

const NNF_SEED = 0x5eed

describe(`toNNF — truth-preserving (seed=0x${NNF_SEED.toString(16)})`, () => {
  const OPS: CompareOp[] = ['=', '<>', '>', '>=', '<', '<=']
  const COLS = ['a', 'b', 'c']
  const LITERALS = [0, 1, 2, null]

  const rng = mulberry32(NNF_SEED)
  const pick = <T>(xs: readonly T[]): T => xs[Math.floor(rng() * xs.length)]!
  const col = (): ScalarExpr => ({ kind: 'col', ref: { table: 't', column: pick(COLS) } })
  const operand = (): ScalarExpr => (rng() < 0.7 ? { kind: 'lit', value: pick(LITERALS) } : col())

  const leaf = (): Predicate => {
    const r = rng()
    if (r < 0.4) return { kind: 'compare', op: pick(OPS), left: col(), right: operand() }
    if (r < 0.55) return { kind: 'in', expr: col(), values: [operand(), operand()], negated: rng() < 0.5 }
    if (r < 0.68) return { kind: 'isNull', expr: col(), negated: rng() < 0.5 }
    if (r < 0.8) return { kind: 'between', expr: col(), low: operand(), high: operand() }
    if (r < 0.9)
      return {
        kind: 'like',
        expr: col(),
        pattern: pick(['a%', '%b', '_c', 'x\\%y', null]),
        caseInsensitive: rng() < 0.5,
        escapeChar: rng() < 0.5 ? '\\' : null,
        negated: rng() < 0.5,
      }
    if (r < 0.96) return { kind: 'exists', negated: rng() < 0.5, tables: ['x'] }
    return { kind: 'unknown', tables: ['x'] }
  }

  const gen = (depth: number): Predicate => {
    if (depth <= 0 || rng() < 0.45) return leaf()
    const r = rng()
    if (r < 0.34) return { kind: 'and', parts: [gen(depth - 1), gen(depth - 1)] }
    if (r < 0.67) return { kind: 'or', parts: [gen(depth - 1), gen(depth - 1)] }
    return { kind: 'not', operand: gen(depth - 1) }
  }

  const genRow = () => {
    const record: Record<string, unknown> = {}
    for (const c of COLS) {
      const v = pick(['__missing__', null, 0, 1, 2, 'x'])
      if (v !== '__missing__') record[c] = v
    }
    return {
      row: rowView(record),
      hasNull: Object.values(record).includes(null),
      hasMissing: Object.keys(record).length < COLS.length,
    }
  }

  // What each generated predicate exercises, to assert real coverage after the run.
  const truths = new Set<string>()
  const negatedKinds = new Set<string>()
  let sawNullRow = false
  let sawMissingRow = false
  let maxNotDepth = 0

  const recordNegations = (p: Predicate, notDepth: number): void => {
    if (p.kind === 'not') {
      const target = p.operand.kind === 'compare' ? `compare:${p.operand.op}` : p.operand.kind
      negatedKinds.add(target)
      maxNotDepth = Math.max(maxNotDepth, notDepth + 1)
      recordNegations(p.operand, notDepth + 1)
    } else {
      maxNotDepth = Math.max(maxNotDepth, notDepth)
      if (p.kind === 'and' || p.kind === 'or') for (const part of p.parts) recordNegations(part, 0)
    }
  }

  it('eval3(nnf(p)) === eval3(p) over randomized predicates × rows', () => {
    for (let i = 0; i < 4000; i++) {
      const p = gen(4)
      const { row, hasNull, hasMissing } = genRow()
      const result = eval3(p, row)
      expect(eval3(toNNF(p), row)).toBe(result)
      truths.add(String(result))
      sawNullRow ||= hasNull
      sawMissingRow ||= hasMissing
      recordNegations(p, 0)
    }
  })

  it('the run exercised every truth value, NULL and MISSING rows, deep NOT nesting, and each native negation', () => {
    expect(truths).toEqual(new Set(['true', 'false', 'undefined']))
    expect(sawNullRow).toBe(true)
    expect(sawMissingRow).toBe(true)
    expect(maxNotDepth).toBeGreaterThanOrEqual(2) // nested NOT (double negation and beyond)
    // every native negation the NNF rewrite handles was actually hit
    for (const op of OPS) expect(negatedKinds).toContain(`compare:${op}`) // eq↔ne, lt↔gte, …
    for (const kind of ['in', 'like', 'isNull', 'between', 'exists', 'unknown', 'and', 'or', 'not']) {
      expect(negatedKinds).toContain(kind)
    }
  })
})

describe('predicate goldens are built on the pinned rc', () => {
  it('resolves drizzle-orm@1.0.0-rc.4', () => {
    expect(drizzleVersion()).toBe('1.0.0-rc.4')
  })
})

// Named composition cases for an unknown leaf, and that NNF preserves them.
describe('unknown-leaf composition', () => {
  const A: Predicate = {
    kind: 'compare',
    op: '=',
    left: { kind: 'col', ref: { table: 't', column: 'a' } },
    right: { kind: 'lit', value: 1 },
  }
  const U: Predicate = { kind: 'unknown', tables: [] }
  const both = (p: Predicate, record: Record<string, unknown>, expected: Tri) => {
    expect(eval3(p, rowView(record))).toBe(expected)
    expect(eval3(toNNF(p), rowView(record))).toBe(expected)
  }

  it('A AND unknown: true∧unknown = unknown, false∧unknown = false', () => {
    both({ kind: 'and', parts: [A, U] }, { a: 1 }, undefined)
    both({ kind: 'and', parts: [A, U] }, { a: 2 }, false)
  })
  it('A OR unknown: true∨unknown = true, false∨unknown = unknown', () => {
    both({ kind: 'or', parts: [A, U] }, { a: 1 }, true)
    both({ kind: 'or', parts: [A, U] }, { a: 2 }, undefined)
  })
  it('NOT unknown = unknown', () => {
    both({ kind: 'not', operand: U }, {}, undefined)
  })
})

function mulberry32(seed: number): () => number {
  let a = seed >>> 0
  return () => {
    a |= 0
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}
