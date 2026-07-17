export { _activationStateForTesting, _listenerCountForTesting }

import type { Live } from './live.js'
import type { TagHub } from './tagHub.js'

// Test-only introspection kept in a DEDICATED module — NOT methods on `Live`/`TagHub`, and NOT re-exported
// by index.ts — so no introspection surface lands on the package's public API (cf the vetoed `isLive`).
// Reads TS-private (runtime-present) fields through structural casts.

/** Activation snapshot for a cell: lease count, tap counts, and whether a source subscription is live. */
function _activationStateForTesting(live: Live<unknown>): {
  lease: number
  invalidateListeners: number
  dataListeners: number
  sourceActive: boolean
} {
  const cell = live as unknown as {
    lease: number
    invalidateTaps: unknown[]
    dataTaps: unknown[]
    sourceTeardowns: unknown[]
  }
  return {
    lease: cell.lease,
    invalidateListeners: cell.invalidateTaps.length,
    dataListeners: cell.dataTaps.length,
    sourceActive: cell.sourceTeardowns.length > 0,
  }
}

/** Registered index listeners for `tag` on a hub — 0 proves a captured-but-never-subscribed fence leaked
 *  nothing. */
function _listenerCountForTesting(hub: TagHub, tag: string): number {
  const internals = hub as unknown as { index: Map<string, Set<unknown>> }
  return internals.index.get(tag)?.size ?? 0
}
