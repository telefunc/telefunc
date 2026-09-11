// The routing identity's defining property is INJECTIVITY: two distinct relations must never share an
// identity, or a write on one invalidates a live query on the other (review finding #6). A table name may
// be ANY string, so these cases deliberately use names that attack the encoding itself — names containing
// dots and names shaped like the framed form.

import { describe, expect, it } from 'vitest'
import { describeRelationId, parseRelationId, relationIdOf } from './relation.js'

describe('relation identity — the two encoding forms', () => {
  it('an unqualified relation keeps its readable bare name (the common case stays legible in logs)', () => {
    expect(relationIdOf({ name: 'todos' })).toBe('todos')
  })

  it('a schema-qualified relation encodes both parts, and same-named relations differ', () => {
    const a = relationIdOf({ name: 'users', schema: 'a' })
    const b = relationIdOf({ name: 'users', schema: 'b' })
    expect(a).not.toBe(b)
    // ...and neither collides with the UNQUALIFIED relation of the same name.
    expect(a).not.toBe(relationIdOf({ name: 'users' }))
    expect(b).not.toBe(relationIdOf({ name: 'users' }))
  })
})

describe('relation identity — injectivity against adversarial names', () => {
  it('a dot in a name cannot forge a schema split (`a` + `b.c` vs `a.b` + `c`)', () => {
    expect(relationIdOf({ schema: 'a', name: 'b.c' })).not.toBe(relationIdOf({ schema: 'a.b', name: 'c' }))
  })

  it('an unqualified name SHAPED like the framed form cannot forge a qualified identity', () => {
    const qualified = relationIdOf({ name: 'users', schema: 'a' })
    // A relation actually named `1:a5:users` — byte-identical to the framed encoding of `a.users`.
    const impostor = relationIdOf({ name: qualified })
    expect(impostor).not.toBe(qualified)
    // It is pushed into the framed form rather than used verbatim, which is what keeps the spaces disjoint.
    expect(impostor).not.toBe('1:a5:users')
  })
})

describe('relation identity — parse round-trip', () => {
  const cases = [
    { name: 'todos' },
    { name: 'users', schema: 'a' },
    { name: 'b.c', schema: 'a' },
    { name: 'c', schema: 'a.b' },
    { name: '1:a5:users' },
    { name: '*' },
    { name: '*', schema: 'a' },
  ]

  it('every form decodes back to the exact schema/name it was built from', () => {
    for (const relation of cases) {
      expect(parseRelationId(relationIdOf(relation))).toEqual(relation)
    }
  })

  it('the decoded parts are what a catalog probe needs to address the relation', () => {
    // readCapture's RLS probe leans on this: a qualified relation is probed in ITS schema, not `public`.
    expect(parseRelationId(relationIdOf({ name: 'users', schema: 'analytics' }))).toEqual({
      name: 'users',
      schema: 'analytics',
    })
  })

  it('describeRelationId renders a human-readable relation (diagnostics only)', () => {
    expect(describeRelationId(relationIdOf({ name: 'users', schema: 'a' }))).toBe('a.users')
    expect(describeRelationId(relationIdOf({ name: 'todos' }))).toBe('todos')
  })

  it('a malformed identity is rejected rather than silently decoded as something else', () => {
    expect(() => parseRelationId('9:short')).toThrow(/malformed relation identity/)
  })
})
