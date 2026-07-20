export { recordUpgradePrepared, recordUpgradeCommitted }

import { getGlobalObject } from '../../utils/getGlobalObject.js'

/**
 * @internal @test-only Deliberately absent from the package's documented API.
 *
 * Counts the barrier upgrade's two authoritative server-side events so a browser e2e can assert the
 * upgrade actually *happened* — which no client-observable surface establishes today
 * (`page.waitForEvent('websocket')` fires on the PROBE socket, so an upgrade that aborts satisfies
 * it). `prepared` separates the two failures that both leave `committed === 0`: never attempted, and
 * attempted then failed.
 *
 * ⚠️ Its OWN global-object slot, not a field on the mux's: `getGlobalObject` is keyed by filename and
 * returns the FIRST copy's object, so a field added to an existing factory reads back `undefined`
 * wherever a second copy of that module is loaded (dev serves a Vite-transformed copy beside Node's).
 */
type UpgradeObservations = {
  /** PREPARE frames the server accepted and answered with READY. */
  prepared: number
  /** Barrier commits that RESOLVED — the upgrade is done and the probe wire owns the session. */
  committed: number
}

/** ⚠️ This literal is duplicated in `test/playground-stream/upgrade-observations.ts`, which reads the
 *  slot off `globalThis` rather than importing it — adding a package export for a test-only surface
 *  is not worth it. Rename the two together. */
const UPGRADE_OBSERVER_KEY = 'wire-protocol/server/upgrade-observer.ts'

function getGlobals(): UpgradeObservations {
  return getGlobalObject(UPGRADE_OBSERVER_KEY, () => ({ prepared: 0, committed: 0 }))
}

// Eager, so a reader can tell "loaded, nothing happened" (slot present, zeroes) from "slot absent"
// (renamed file, a copy that never loaded) — otherwise a broken oracle reads as a failed upgrade.
getGlobals()

function recordUpgradePrepared(): void {
  getGlobals().prepared++
}

function recordUpgradeCommitted(): void {
  getGlobals().committed++
}
