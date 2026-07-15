import { describe, expect, it } from 'vitest'
import { compareValues, eval3, rowView } from './eval.js'
import type { Predicate } from './types.js'

const col = (column: string) => ({ kind: 'col', ref: { table: 't', column } }) as const
const lit = (value: unknown) => ({ kind: 'lit', value }) as const

describe('rowView', () => {
  it('distinguishes a captured NULL from a not-captured (MISSING) column', () => {
    const view = rowView({ a: null })
    expect(view.get('a')).toEqual({ present: true, value: null })
    expect(view.get('b')).toEqual({ present: false })
  })
})

describe('eval3 — NULL vs MISSING three-valued logic', () => {
  const eqA5: Predicate = { kind: 'compare', op: '=', left: col('a'), right: lit(5) }
  const isNullA: Predicate = { kind: 'isNull', expr: col('a'), negated: false }

  it('a comparison is true/false on a value, unknown on NULL or MISSING', () => {
    expect(eval3(eqA5, rowView({ a: 5 }))).toBe(true)
    expect(eval3(eqA5, rowView({ a: 6 }))).toBe(false)
    expect(eval3(eqA5, rowView({ a: null }))).toBeUndefined()
    expect(eval3(eqA5, rowView({}))).toBeUndefined()
  })

  it('is null is decided only for a captured value — MISSING stays unknown', () => {
    expect(eval3(isNullA, rowView({ a: null }))).toBe(true)
    expect(eval3(isNullA, rowView({ a: 1 }))).toBe(false)
    expect(eval3(isNullA, rowView({}))).toBeUndefined()
  })

  it('and/or short-circuit past unknown per Kleene logic', () => {
    const and: Predicate = { kind: 'and', parts: [eqA5, { kind: 'compare', op: '=', left: col('b'), right: lit(1) }] }
    expect(eval3(and, rowView({ a: 6, b: null }))).toBe(false) // false ∧ unknown = false
    expect(eval3(and, rowView({ a: 5, b: null }))).toBeUndefined() // true ∧ unknown = unknown
    const or: Predicate = { kind: 'or', parts: [eqA5, { kind: 'compare', op: '=', left: col('b'), right: lit(1) }] }
    expect(eval3(or, rowView({ a: 6, b: 1 }))).toBe(true) // unknown-free true wins
    expect(eval3(or, rowView({ a: null, b: 0 }))).toBeUndefined() // unknown ∨ false = unknown
  })
})

describe('eval3 — LIKE / ILIKE', () => {
  const like = (pattern: string | null, caseInsensitive: boolean, negated = false): Predicate => ({
    kind: 'like',
    expr: col('t'),
    pattern,
    caseInsensitive,
    negated,
  })

  it('% and _ wildcards, with metacharacters kept literal', () => {
    expect(eval3(like('a%', false), rowView({ t: 'abc' }))).toBe(true)
    expect(eval3(like('a%', false), rowView({ t: 'xbc' }))).toBe(false)
    expect(eval3(like('a_c', false), rowView({ t: 'abc' }))).toBe(true)
    expect(eval3(like('a_c', false), rowView({ t: 'abbc' }))).toBe(false)
    expect(eval3(like('a.c', false), rowView({ t: 'abc' }))).toBe(false)
  })

  it('case sensitivity follows the flag; NULL/MISSING/opaque pattern are unknown', () => {
    expect(eval3(like('a%', false), rowView({ t: 'ABC' }))).toBe(false)
    expect(eval3(like('a%', true), rowView({ t: 'ABC' }))).toBe(true)
    expect(eval3(like('a%', false), rowView({ t: null }))).toBeUndefined()
    expect(eval3(like('a%', false), rowView({}))).toBeUndefined()
    expect(eval3(like(null, false), rowView({ t: 'abc' }))).toBeUndefined()
  })

  it('negation inverts a definite match, leaves unknown unknown', () => {
    expect(eval3(like('a%', false, true), rowView({ t: 'abc' }))).toBe(false)
    expect(eval3(like('a%', false, true), rowView({ t: 'xyz' }))).toBe(true)
    expect(eval3(like('a%', false, true), rowView({ t: null }))).toBeUndefined()
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
    expect(compareValues('x', 5)).toBeUndefined() // non-numeric string vs number
  })
})
