export { recordUpgradePrepared, recordUpgradeCommitted }

import { getGlobalObject } from '../../utils/getGlobalObject.js'

/**
 * @internal @test-only Counts the upgrade's two authoritative server-side events so the browser e2e
 * can assert the upgrade actually happened — no client-observable surface establishes that
 * (`waitForEvent('websocket')` fires on the PROBE socket, so an aborted upgrade satisfies it).
 *
 * ⚠️ Its OWN global slot, not a field on the mux's: `getGlobalObject` is keyed by filename and
 * returns the FIRST copy's object, so a field added to an existing factory reads back `undefined`
 * wherever a second copy of that module is loaded (dev serves a Vite-transformed copy beside Node's).
 */
type UpgradeObservations = {
  prepared: number
  committed: number
}

/** Duplicated in `test/playground-stream/upgrade-observations.ts`, which reads the slot off
 *  `globalThis` rather than importing it. Rename the two together. */
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
