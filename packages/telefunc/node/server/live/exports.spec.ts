import { describe, expect, it } from 'vitest'
import * as telefunc from '../index.js'

// The audit-mandated live/tag/telemetry surface must be exported from 'telefunc' (§3.G G1/G4).
describe('telefunc live exports (§3.G)', () => {
  it('T1.G1/G4 exports the live seam, tag API, and typed telemetry seam', () => {
    for (const name of [
      'liveTag',
      'invalidateTag',
      'addLiveSource',
      'incrementCounter',
      'snapshotCounters',
      // Sprint 2 ruling A — the source-take seam @telefunc/tanstack-query consumes:
      'takeLiveSources',
    ]) {
      expect(typeof (telefunc as Record<string, unknown>)[name]).toBe('function')
    }
  })
})
