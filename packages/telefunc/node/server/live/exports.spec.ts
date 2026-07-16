import { describe, expect, it } from 'vitest'
import * as telefunc from '../index.js'

// The live/tag surface must be exported from 'telefunc' (§3.G G1/G4). Telemetry is INTERNAL-only — the
// public telemetry surface was removed as speculative bloat for an unreleased subsystem (owner MVP-lean
// ruling superseding the earlier §3.G telemetry-export mandate).
describe('telefunc live exports (§3.G)', () => {
  it('T1.G1/G4 exports the live seam + tag API (telemetry is internal-only)', () => {
    for (const name of [
      'liveTag',
      'invalidateTag',
      'addLiveSource',
      // Sprint 2 ruling A — the source-take seam @telefunc/tanstack-query consumes:
      'takeLiveSources',
    ]) {
      expect(typeof (telefunc as Record<string, unknown>)[name]).toBe('function')
    }
  })
})
