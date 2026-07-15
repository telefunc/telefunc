export { incrementCounter, snapshotCounters, _resetCountersForTesting }
export type { LiveCounterName }

import { assertUsage } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'

// The closed set of live-subsystem counters. New counters are added HERE, deliberately, at
// the type level — never minted at runtime — so the registry stays bounded (application code
// can't grow it by passing arbitrary strings). `@telefunc/drizzle` widens this union later.
const LIVE_COUNTER_NAMES = [
  'live.limiter.rejected', // a per-connection live-query reservation was refused (cap hit)
  'live.tagHub.publishFailure', // a settled tag batch failed to publish (detected, never silent)
  'live.tagHub.journalOverflow', // the tag journal overflowed → unconditional fence replay
] as const

type LiveCounterName = (typeof LIVE_COUNTER_NAMES)[number]

const LIVE_COUNTER_NAME_SET: ReadonlySet<string> = new Set(LIVE_COUNTER_NAMES)

const globalObject = getGlobalObject<{ counters: Record<LiveCounterName, number> }>('telemetry.ts', () => ({
  counters: freshCounters(),
}))

// A null-prototype record so no inherited key (`toString`, `constructor`, …) is ever present.
function freshCounters(): Record<LiveCounterName, number> {
  const counters = Object.create(null) as Record<LiveCounterName, number>
  for (const name of LIVE_COUNTER_NAMES) counters[name] = 0
  return counters
}

/** Bump a counter. `name` must be a member of the closed `LiveCounterName` set — checked against the
 *  set (not `in`, which would accept inherited object keys). An out-of-set name asserts and never
 *  allocates a new key. */
function incrementCounter(name: LiveCounterName, by = 1): void {
  assertUsage(LIVE_COUNTER_NAME_SET.has(name), `Unknown live counter "${name}" — not a LiveCounterName`)
  globalObject.counters[name] += by
}

/** A bounded copy of the fixed counter record; mutating it never touches the registry. */
function snapshotCounters(): Record<LiveCounterName, number> {
  return { ...globalObject.counters }
}

/** @internal test-only reset. */
function _resetCountersForTesting(): void {
  globalObject.counters = freshCounters()
}
