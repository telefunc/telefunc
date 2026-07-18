import { describe, expect, it } from 'vitest'
import * as telefunc from '../index.js'

// The public live surface exported from 'telefunc' is exactly TWO concepts: a `Live<T>` (read `.data`)
// and `Live.derived(…)`. The producer cell and its surviving verbs (invalidate/onInvalidate/close plus
// the activation lifecycle) are INTERNAL — a server-side binding reaches them via the extension host
// (`TelefuncServerExtension.setup(host)` → `host.createLive`), not via any public 'telefunc' export. The
// delta-push verbs (set/update/onData) and the tag statics were DELETED outright — gone, not hidden — as
// were the legacy request-bag/tag functions (addLiveSource/takeLiveSources/liveTag/invalidateTag), removed
// with the old TanStack wrapper path.
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
    // The old tag statics (`Live.invalidate`/`Live.onInvalidate`) were DELETED with the tag stack — not
    // relocated — and must not reappear on the public namespace.
    for (const removed of ['invalidate', 'onInvalidate']) {
      expect((Live as Record<string, unknown>)[removed]).toBeUndefined()
    }
  })

  it('the producer cell is NOT publicly reachable', () => {
    // `LiveCell` is the boundary: unexported from the package root, so the producer verbs cannot be
    // reached from 'telefunc'. Nor from 'telefunc/__internal' — a binding receives a producer through
    // the extension host's `setup(host)` instead, never by importing the class.
    expect((telefunc as Record<string, unknown>).LiveCell).toBeUndefined()
    expect((telefunc as Record<string, unknown>).ClientLive).toBeUndefined()
  })
})
