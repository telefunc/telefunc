// Reads the server-side upgrade counters off `globalThis` rather than importing them: the observer
// is deliberately not a package export, and dev serves a Vite-transformed copy of it beside Node's.
// `observerPresent` distinguishes a broken oracle (slot absent — file renamed, copy never loaded)
// from a genuinely failed upgrade; both would otherwise read as zeroes.
export { readUpgradeObservations, resetUpgradeObservations }

const TELEFUNC_GLOBALS_KEY = '_telefunc'
/** Duplicated from `wire-protocol/server/upgrade-observer.ts`. Rename the two together. */
const UPGRADE_OBSERVER_KEY = 'wire-protocol/server/upgrade-observer.ts'

type UpgradeObservations = { prepared: number; committed: number }

function getSlot(): UpgradeObservations | undefined {
  const all = (globalThis as Record<string, any>)[TELEFUNC_GLOBALS_KEY]
  return all?.[UPGRADE_OBSERVER_KEY]
}

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
