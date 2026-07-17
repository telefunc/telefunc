import { describe, expect, it } from 'vitest'
import * as telefunc from '../index.js'

// The public live surface exported from 'telefunc' (§3.G G1/G4) is the general-purpose `Live` primitive
// (+ its `ClientLive` type). The legacy request-bag/tag functions (addLiveSource/takeLiveSources/liveTag/
// invalidateTag) were removed with the old TanStack wrapper path — invalidation is now the
// `Live.invalidate` / `Live.onInvalidate` statics. Telemetry is INTERNAL-only.
describe('telefunc live exports (§3.G)', () => {
  it('T1.G1/G4 exports the Live primitive; the legacy bag/tag functions are gone', () => {
    expect(typeof (telefunc as Record<string, unknown>).Live).toBe('function')
    for (const removed of ['liveTag', 'invalidateTag', 'addLiveSource', 'takeLiveSources']) {
      expect((telefunc as Record<string, unknown>)[removed]).toBeUndefined()
    }
  })
})
