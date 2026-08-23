import { describe, expect, it } from 'vitest'
import { collapseQuad, compareValues, eval3, evalK, rowView } from './eval.js'
import type { Predicate, Quad } from './types.js'

const col = (column: string) => ({ kind: 'col', ref: { table: 't', column } }) as const
const lit = (value: unknown) => ({ kind: 'lit', value }) as const

describe('rowView', () => {
  it('distinguishes a captured NULL from a not-captured (MISSING) column', () => {
    const view = rowView({ a: null })
    expect(view.get('a')).toEqual({ present: true, value: null })
    expect(view.get('b')).toEqual({ present: false })
  })

  it('an inherited property name is MISSING, not a captured cell', () => {
    const view = rowView({})
    expect(view.get('toString')).toEqual({ present: false })
    expect(view.get('constructor')).toEqual({ present: false })
    expect(view.get('hasOwnProperty')).toEqual({ present: false })
    // so a predicate over an absent inherited-named column widens (missing), never false
    const isNullInherited: Predicate = { kind: 'isNull', expr: col('toString'), negated: false }
    expect(evalK(isNullInherited, view)).toBe('missing')
    const eqInherited: Predicate = { kind: 'compare', op: '=', left: col('toString'), right: lit(1) }
    expect(evalK(eqInherited, view)).toBe('missing')
  })
})

describe('evalK — NULL vs MISSING signal', () => {
  const eqA5: Predicate = { kind: 'compare', op: '=', left: col('a'), right: lit(5) }

  it('a comparison is true/false on a value, and keeps WHY it is unknown', () => {
    expect(evalK(eqA5, rowView({ a: 5 }))).toBe(true)
    expect(evalK(eqA5, rowView({ a: 6 }))).toBe(false)
    expect(evalK(eqA5, rowView({ a: null }))).toBe('null') // captured NULL → WHERE-excludes
    expect(evalK(eqA5, rowView({}))).toBe('missing') // not captured → σ-widens
  })

  it('the two unknown causes are distinguishable: a captured NULL is not a missing column', () => {
    const nullResult = evalK(eqA5, rowView({ a: null }))
    const missingResult = evalK(eqA5, rowView({}))
    expect(nullResult).not.toBe(missingResult)
    // and eval3 still collapses both to the faithful Kleene unknown
    expect(eval3(eqA5, rowView({ a: null }))).toBeUndefined()
    expect(eval3(eqA5, rowView({}))).toBeUndefined()
  })

  it('is null resolves a captured NULL to a definite boolean; MISSING stays missing', () => {
    const isNullA: Predicate = { kind: 'isNull', expr: col('a'), negated: false }
    expect(evalK(isNullA, rowView({ a: null }))).toBe(true)
    expect(evalK(isNullA, rowView({ a: 1 }))).toBe(false)
    expect(evalK(isNullA, rowView({}))).toBe('missing')
  })

  it('missing dominates null when causes merge (widening is sound)', () => {
    const and: Predicate = {
      kind: 'and',
      parts: [
        { kind: 'compare', op: '=', left: col('a'), right: lit(1) },
        { kind: 'compare', op: '=', left: col('b'), right: lit(1) },
      ],
    }
    expect(evalK(and, rowView({ a: null }))).toBe('missing') // a=null (null) ∧ b missing → missing
  })
})

// ── Independent full Kleene truth-table oracle ───────────────
// evalK's and/or/not must match a reference four-valued Kleene table for every
// operand combination, not just the cases the other specs happen to hit.

describe('evalK — Kleene truth-table oracle', () => {
  const VALUES: Quad[] = [true, false, 'null', 'missing']
  const mergeCause = (a: Quad, b: Quad): Quad => (a === 'missing' || b === 'missing' ? 'missing' : 'null')
  const andRef = (a: Quad, b: Quad): Quad =>
    a === false || b === false ? false : a === true && b === true ? true : mergeCause(a, b)
  const orRef = (a: Quad, b: Quad): Quad =>
    a === true || b === true ? true : a === false && b === false ? false : mergeCause(a, b)
  const notRef = (a: Quad): Quad => (a === true ? false : a === false ? true : a)

  // Force a leaf on its own column to a specific Quad, and provide the row cell for it.
  const force = (column: string, v: Quad): { pred: Predicate; cell?: [string, unknown] } => {
    const pred: Predicate = { kind: 'compare', op: '=', left: col(column), right: lit(1) }
    if (v === true) return { pred, cell: [column, 1] }
    if (v === false) return { pred, cell: [column, 2] }
    if (v === 'null') return { pred, cell: [column, null] }
    return { pred } // 'missing' → omit the column
  }
  const rowFrom = (...cells: (readonly [string, unknown] | undefined)[]) =>
    rowView(Object.fromEntries(cells.filter((c): c is [string, unknown] => c !== undefined)))

  it('and / or over every operand pair', () => {
    for (const x of VALUES) {
      for (const y of VALUES) {
        const a = force('p', x)
        const b = force('q', y)
        const row = rowFrom(a.cell, b.cell)
        expect(evalK({ kind: 'and', parts: [a.pred, b.pred] }, row)).toBe(andRef(x, y))
        expect(evalK({ kind: 'or', parts: [a.pred, b.pred] }, row)).toBe(orRef(x, y))
      }
    }
  })

  it('not over every operand', () => {
    for (const x of VALUES) {
      const a = force('p', x)
      expect(evalK({ kind: 'not', operand: a.pred }, rowFrom(a.cell))).toBe(notRef(x))
    }
  })

  it('collapseQuad maps both unknown causes to the Tri unknown', () => {
    expect(collapseQuad(true)).toBe(true)
    expect(collapseQuad(false)).toBe(false)
    expect(collapseQuad('null')).toBeUndefined()
    expect(collapseQuad('missing')).toBeUndefined()
  })
})

describe('eval3 — LIKE / ILIKE with SQL escapes', () => {
  const like = (
    pattern: string | null,
    caseInsensitive: boolean,
    escapeChar: string | null = '\\',
    negated = false,
  ): Predicate => ({ kind: 'like', expr: col('t'), pattern, caseInsensitive, escapeChar, negated })

  it('% and _ wildcards, with metacharacters kept literal', () => {
    expect(eval3(like('a%', false), rowView({ t: 'abc' }))).toBe(true)
    expect(eval3(like('a%', false), rowView({ t: 'xbc' }))).toBe(false)
    expect(eval3(like('a_c', false), rowView({ t: 'abc' }))).toBe(true)
    expect(eval3(like('a_c', false), rowView({ t: 'abbc' }))).toBe(false)
    expect(eval3(like('a.c', false), rowView({ t: 'abc' }))).toBe(false)
  })

  it('an escaped %/_ is a literal, not a wildcard', () => {
    // pattern a\%b (pg's default backslash escape) matches the literal "a%b" only
    expect(eval3(like('a\\%b', false, '\\'), rowView({ t: 'a%b' }))).toBe(true)
    expect(eval3(like('a\\%b', false, '\\'), rowView({ t: 'axyzb' }))).toBe(false)
    expect(eval3(like('a\\_b', false, '\\'), rowView({ t: 'a_b' }))).toBe(true)
    expect(eval3(like('a\\_b', false, '\\'), rowView({ t: 'axb' }))).toBe(false)
    // with no escape char (sqlite default) the backslash is a literal and % is a wildcard
    expect(eval3(like('a\\%b', false, null), rowView({ t: 'a\\zzb' }))).toBe(true)
  })

  it('case sensitivity follows the flag; NULL/MISSING/opaque pattern are unknown', () => {
    expect(eval3(like('a%', false), rowView({ t: 'ABC' }))).toBe(false)
    expect(eval3(like('a%', true), rowView({ t: 'ABC' }))).toBe(true)
    expect(eval3(like('a%', false), rowView({ t: null }))).toBeUndefined()
    expect(eval3(like('a%', false), rowView({}))).toBeUndefined()
    expect(eval3(like(null, false), rowView({ t: 'abc' }))).toBeUndefined()
  })

  it('negation inverts a definite match, leaves unknown unknown', () => {
    expect(eval3(like('a%', false, '\\', true), rowView({ t: 'abc' }))).toBe(false)
    expect(eval3(like('a%', false, '\\', true), rowView({ t: 'xyz' }))).toBe(true)
    expect(eval3(like('a%', false, '\\', true), rowView({ t: null }))).toBeUndefined()
  })
})

describe('compareValues — cross-producer coercion', () => {
  it('numbers, numeric strings, and their order', () => {
    expect(compareValues(5, 5)).toBe(0)
    expect(compareValues(4, 5)).toBe(-1)
    expect(compareValues('6', 5)).toBe(1) // pg numeric arrives as string
    expect(compareValues(5, '5')).toBe(0)
  })

  it('booleans coerce to 0/1 (sqlite storage)', () => {
    expect(compareValues(true, 1)).toBe(0)
    expect(compareValues(false, 0)).toBe(0)
    expect(compareValues(true, 0)).toBe(1)
  })

  it('bigints, dates vs ISO strings', () => {
    expect(compareValues(BigInt(10), 10)).toBe(0)
    expect(compareValues(new Date('2026-01-01'), '2026-01-01')).toBe(0)
    expect(compareValues(new Date('2026-06-01'), new Date('2026-01-01'))).toBe(1)
  })

  it('strings order lexically; json/arrays are equal-or-incomparable', () => {
    expect(compareValues('a', 'b')).toBe(-1)
    expect(compareValues({ x: 1 }, { x: 1 })).toBe(0)
    expect(compareValues({ x: 1 }, { x: 2 })).toBeUndefined()
    expect(compareValues('x', 5)).toBeUndefined()
  })
})
