export { recordUpgradePrepared, recordUpgradeCommitted, getUpgradeObservations, resetUpgradeObservations }
export type { UpgradeObservations }

import { getGlobalObject } from '../../utils/getGlobalObject.js'

/**
 * @internal @test-only Deliberately absent from the package's documented API.
 *
 * Counts the two authoritative server-side events of the barrier upgrade so a browser e2e can
 * assert that the upgrade actually *happened* — the property no client-observable surface can
 * establish today. `page.waitForEvent('websocket')` fires on the PROBE socket, so an upgrade that
 * probes and then aborts satisfies it; `finalizeUpgrade` is shared with every ordinary reconcile.
 * The barrier commit is upgrade-only by construction and fires exactly once per successful commit,
 * which is why it is the event counted here.
 *
 * `prepared` exists to separate two different failures that both leave `committed === 0`: an
 * upgrade that was never attempted (capability gate off, transports misconfigured) from one that
 * was attempted and then failed. It also bounds retry-thrash — a healthy upgrade prepares once.
 *
 * Counters only: no per-attempt state is retained, so this cannot grow.
 *
 * Kept in its OWN global-object slot rather than as a field on the mux's. `getGlobalObject` is
 * keyed by filename and returns the FIRST copy's object, so a new field added to an existing
 * factory reads back `undefined` wherever a second copy of that module is loaded (dev serves a
 * Vite-transformed copy alongside the Node one). A distinct key has no such shared history: the
 * first caller creates it and every copy then observes the same counters.
 */
type UpgradeObservations = {
  /** PREPAREs the server accepted and answered with READY. */
  prepared: number
  /** Barrier commits that RESOLVED — the upgrade is done and the probe wire owns the session. */
  committed: number
}

/** The slot's key. Exported so the one consumer that reads it across a package boundary (the
 *  e2e playground's `/api/cleanup-state`) names it rather than spelling it out again. */
const UPGRADE_OBSERVER_KEY = 'wire-protocol/server/upgrade-observer.ts'

function getGlobals(): UpgradeObservations {
  return getGlobalObject(UPGRADE_OBSERVER_KEY, () => ({ prepared: 0, committed: 0 }))
}

// Created eagerly at module load — the factory needs nothing initialized, unlike the mux's. This is
// what lets a reader tell "telefunc's upgrade path is loaded and nothing happened" (slot present,
// zeroes) from "the slot is not there at all" (a renamed file, a copy that never loaded). Without
// it both read as zero and a broken oracle is indistinguishable from a failed upgrade.
getGlobals()

function recordUpgradePrepared(): void {
  getGlobals().prepared++
}

function recordUpgradeCommitted(): void {
  getGlobals().committed++
}

function getUpgradeObservations(): UpgradeObservations {
  return { ...getGlobals() }
}

/** Test-suite hygiene: lets an e2e establish a zero baseline per page load. */
function resetUpgradeObservations(): void {
  const g = getGlobals()
  g.prepared = 0
  g.committed = 0
}
