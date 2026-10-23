import { describe, expect, it } from 'vitest'
import { canonicalRow, correlationKey, equiKey, keyedRow, positionalRow, sortedContent } from './encoding.js'

// These encodings ARE the definition of "same row" for every incremental operator: a retraction cancels an
// insert exactly when their encodings match. So each property below is a cancellation rule, and getting one
// wrong is either a delta that fails to cancel (a stale live query) or two different rows that cancel each
// other (a wrong one). Mutation found the whole family unpinned when it moved here — the stage specs exercise
// the encoders through several layers, which is enough to catch a crash and not enough to catch a collision.

describe('row encodings — what makes two rows the same row', () => {
  it('distinguishes an ABSENT column from one explicitly set to undefined, and from NULL', () => {
    // A column capture could not read is not a column that is NULL, and neither is a present-but-undefined
    // one: absence is "unknown", the others are values. Collapsing them lets a partially-captured row cancel
    // a fully-captured one. The present/absent MARKER is what carries this — `canonicalValue` alone maps a
    // missing key and an undefined one to the same encoding, so dropping the marker survives every
    // null-vs-absent case and only this one catches it.
    expect(keyedRow({ a: undefined }, ['a'])).not.toBe(keyedRow({}, ['a']))
    expect(positionalRow({ a: undefined }, ['a'])).not.toBe(positionalRow({}, ['a']))
    expect(keyedRow({ a: null }, ['a'])).not.toBe(keyedRow({}, ['a']))
  })

  it('is independent of key ORDER for the name-ordered encodings', () => {
    // Rows reach an operator from different producers — a scan, a join, a replayed shadow — and their key
    // insertion order is not part of the row's identity. If it leaked in, a retraction built one way would
    // not cancel the insert built the other.
    expect(keyedRow({ a: 1, b: 2 }, ['b', 'a'])).toBe(keyedRow({ b: 2, a: 1 }, ['a', 'b']))
    expect(canonicalRow({ a: 1, b: 2 })).toBe(canonicalRow({ b: 2, a: 1 }))
    expect(sortedContent({ b: 2, a: 1 })).toEqual(sortedContent({ a: 1, b: 2 }))
    expect(Object.keys(sortedContent({ b: 2, a: 1 }))).toEqual(['a', 'b'])
  })

  it('KEEPS key order for the positional encoding — the one place order IS identity', () => {
    // UNION dedup compares projections positionally: `SELECT a, b` and `SELECT b, a` are different
    // projections of the same row and must not dedup against each other.
    expect(positionalRow({ a: 1, b: 2 }, ['a', 'b'])).not.toBe(positionalRow({ a: 1, b: 2 }, ['b', 'a']))
  })

  it('gives a NULL equi-key a sentinel no other row shares — NULL never matches NULL', () => {
    // SQL's `NULL <> NULL`. A shared "null key" would make every NULL-keyed row on one side join every
    // NULL-keyed row on the other, which is rows appearing that the database would never return.
    const one = equiKey({ k: null, v: 1 }, ['k'])
    const other = equiKey({ k: null, v: 2 }, ['k'])
    expect(one).not.toBe(other)
    // …but the SAME row keys identically, or its retraction could not cancel its own insert.
    expect(equiKey({ k: null, v: 1 }, ['k'])).toBe(one)
    // An absent key is treated as unknown too, not as a value.
    expect(equiKey({ v: 1 }, ['k'])).not.toBe(equiKey({ k: 1, v: 1 }, ['k']))
  })

  it('gives a NULL correlation value the same treatment — `NULL IN (…)` is not true', () => {
    const one = correlationKey({ k: null, v: 1 }, 'k')
    const other = correlationKey({ k: null, v: 2 }, 'k')
    expect(one).not.toBe(other)
    expect(correlationKey({ k: null, v: 1 }, 'k')).toBe(one)
    expect(correlationKey({ v: 1 }, 'k')).not.toBe(correlationKey({ k: 1, v: 1 }, 'k'))
  })

  it('keeps type-distinct values distinct', () => {
    // The cross-producer hazard: a captured JS value and a scanned one are two representations of one SQL
    // value, but `1` and `'1'` are not the same value and must not cancel.
    expect(keyedRow({ a: 1 }, ['a'])).not.toBe(keyedRow({ a: '1' }, ['a']))
    // `BigInt(1)` rather than the `1n` literal: the spec tsconfig targets below ES2020, where that syntax
    // is an error. The distinction being asserted is the runtime one, so the constructor says it just as well.
    expect(keyedRow({ a: 1 }, ['a'])).not.toBe(keyedRow({ a: BigInt(1) }, ['a']))
    expect(equiKey({ a: 1 }, ['a'])).not.toBe(equiKey({ a: '1' }, ['a']))
  })

  it('separates the two keyspaces — an equi-key never collides with a correlation key', () => {
    // They are built from the same content by different rules; a shared prefix would let a future merge
    // silently make them comparable.
    expect(equiKey({ k: null }, ['k'])).not.toBe(correlationKey({ k: null }, 'k'))
    expect(equiKey({ k: 1 }, ['k'])).not.toBe(correlationKey({ k: 1 }, 'k'))
  })
})
