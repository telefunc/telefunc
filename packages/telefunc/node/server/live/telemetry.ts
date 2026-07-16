export { incrementCounter, _resetCountersForTesting, _snapshotCountersForTesting }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'

// Internal live-subsystem correctness counters — they drive real behavior (a settled tag batch that
// failed to publish → detected, never silent; a tag-journal overflow → unconditional fence replay).
// Kept INTERNAL: no public 'telefunc' export and no runtime key-contract — the closed-set/exact-key
// machinery was purely a public-surface lock with no internal need (owner MVP-lean ruling; telemetry is
// no longer a public-surface requirement, superseding the earlier §3.G G1/G4 export mandate).

type LiveCounterName = 'live.tagHub.publishFailure' | 'live.tagHub.journalOverflow'

function freshCounters(): Record<LiveCounterName, number> {
  return { 'live.tagHub.publishFailure': 0, 'live.tagHub.journalOverflow': 0 }
}

const globalObject = getGlobalObject<{ counters: Record<LiveCounterName, number> }>('telemetry.ts', () => ({
  counters: freshCounters(),
}))

/** Bump an internal correctness counter (compile-time constrained to the fixed set). */
function incrementCounter(name: LiveCounterName, by = 1): void {
  globalObject.counters[name] += by
}

/** @internal test-only reset. */
function _resetCountersForTesting(): void {
  globalObject.counters = freshCounters()
}

/** @internal test-only — a bounded copy of the counters. */
function _snapshotCountersForTesting(): Record<LiveCounterName, number> {
  return { ...globalObject.counters }
}
