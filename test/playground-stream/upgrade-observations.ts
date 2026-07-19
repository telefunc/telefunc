export { readUpgradeObservations, resetUpgradeObservations }

/**
 * Server-side oracle for "the SSE→WS barrier upgrade actually completed".
 *
 * There is no client-observable signal that says so. `page.waitForEvent('websocket')` fires on the
 * PROBE socket, so an upgrade that probes and then aborts satisfies it — a can't-fail gate. The
 * authoritative event is the server's barrier COMMIT, which is upgrade-only by construction and
 * fires exactly once per successful upgrade (unlike `finalizeUpgrade`, which every ordinary
 * reconcile shares). telefunc counts it in an `@internal @test-only` global slot; this module
 * surfaces it on the existing `/api/cleanup-state` channel.
 *
 * Read through `globalThis` rather than an import: telefunc's `__internal` entry is deliberately
 * type-only (a runtime import from it would drag the server graph into client bundles), and the
 * playground already reaches server-side test state this way — see `serverCloseReconnectStore` in
 * `+server.ts`. Keeps the production surface to two counters with no new package export.
 *
 * SINGLE-INSTANCE ONLY. These counters live in process memory, not Redis, so they are meaningful
 * for the `dev`/`preview` suites and NOT for the clustered docker suite, where the API request may
 * land on an instance other than the one that ran the upgrade.
 */
const TELEFUNC_GLOBALS_KEY = '_telefunc'
const UPGRADE_OBSERVER_KEY = 'wire-protocol/server/upgrade-observer.ts'

type UpgradeObservations = { prepared: number; committed: number }

function getSlot(): UpgradeObservations | undefined {
  const all = (globalThis as Record<string, any>)[TELEFUNC_GLOBALS_KEY]
  return all?.[UPGRADE_OBSERVER_KEY]
}

/**
 * `-1` when the slot is absent, never `0`. telefunc creates it eagerly at module load, so absence
 * means the oracle itself is broken (renamed file, a copy of the module that never loaded) rather
 * than an upgrade that did not happen. Collapsing the two onto `0` would let a silently-detached
 * oracle read exactly like a genuine failure, and the e2e would blame the wrong thing.
 */
function readUpgradeObservations(): Record<string, string> {
  const slot = getSlot()
  return {
    upgrade_observerPresent: slot ? 'true' : 'false',
    upgrade_preparedCount: String(slot?.prepared ?? -1),
    upgrade_committedCount: String(slot?.committed ?? -1),
  }
}

function resetUpgradeObservations(): void {
  const slot = getSlot()
  if (!slot) return
  slot.prepared = 0
  slot.committed = 0
}
