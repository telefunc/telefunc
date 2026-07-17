import { describe, expect, it } from 'vitest'
import * as telefunc from '../index.js'

// The public live surface exported from 'telefunc' (§3.G G1/G4) is exactly THREE concepts: a `Live<T>`
// (read `.data`), `Live.derived(…)`, and that's it. The producer cell, its verbs (set/update/invalidate/
// onData/onInvalidate), the activation lifecycle, and the tag statics are all INTERNAL — reachable only
// via `telefunc/__internal` (bindings) or not at all. The legacy request-bag/tag functions
// (addLiveSource/takeLiveSources/liveTag/invalidateTag) were removed with the old TanStack wrapper path.
describe('telefunc live exports', () => {
  it('exports the Live namespace; the legacy bag/tag functions are gone', () => {
    const Live = (telefunc as Record<string, unknown>).Live as Record<string, unknown>
    expect(typeof Live).toBe('object') // a namespace value, not a constructor — `new Live()` is not public
    expect(typeof Live.derived).toBe('function')
    for (const removed of ['liveTag', 'invalidateTag', 'addLiveSource', 'takeLiveSources']) {
      expect((telefunc as Record<string, unknown>)[removed]).toBeUndefined()
    }
  })

  it('the public Live namespace carries ONLY `derived` — no producer verbs, no tag statics', () => {
    const Live = (telefunc as Record<string, unknown>).Live as object
    // The whole public value surface. A new key here is a public-API change and must be deliberate.
    expect(Object.keys(Live)).toEqual(['derived'])
    // The old statics are gone from the public namespace (they live on the internal cell now).
    for (const internal of ['invalidate', 'onInvalidate']) {
      expect((Live as Record<string, unknown>)[internal]).toBeUndefined()
    }
  })

  it('the producer cell is NOT publicly reachable', () => {
    // `LiveCell` is the boundary: unexported from the package root, so the producer verbs cannot be
    // reached from 'telefunc'. Bindings import it from 'telefunc/__internal' instead.
    expect((telefunc as Record<string, unknown>).LiveCell).toBeUndefined()
    expect((telefunc as Record<string, unknown>).ClientLive).toBeUndefined()
  })
})
