export { GcRegistry }

import { unrefTimer } from '../utils/unrefTimer.js'

type CloseFn = () => void | Promise<void>

type Entry = {
  ref: WeakRef<object>
  close: CloseFn
  cleaned: boolean
}

/**
 * GC-tracked cleanup with two reclaim paths:
 *
 * 1. **FinalizationRegistry** — primary path. Fires when the engine notices the
 *    target is unreachable. Timing is engine-defined and can be deferred
 *    indefinitely (especially for ephemeron cycles or under low GC pressure).
 *
 * 2. **Periodic WeakRef scan** — fallback. Walks all entries on a fixed
 *    interval, calls `deref()` on each, and fires cleanup if the WeakRef has
 *    been collected but the FinalizationRegistry callback hasn't run yet.
 *
 * Both paths funnel into the same idempotent `fireCleanup`, so neither double-runs.
 */
class GcRegistry {
  private finalReg: FinalizationRegistry<Entry>
  private entries = new Set<Entry>()

  constructor(periodicScanMs = 5000) {
    this.finalReg = new FinalizationRegistry<Entry>((entry) => this.fireCleanup(entry))
    unrefTimer(setInterval(() => this.scan(), periodicScanMs))
  }

  register(target: object, close: CloseFn): void {
    const entry: Entry = { ref: new WeakRef(target), close, cleaned: false }
    this.entries.add(entry)
    // Pass `entry` as both held value and unregister token so we can deregister
    // from inside fireCleanup without keeping a separate token map.
    this.finalReg.register(target, entry, entry)
  }

  private scan(): void {
    for (const entry of this.entries) {
      if (entry.ref.deref() === undefined) this.fireCleanup(entry)
    }
  }

  private fireCleanup(entry: Entry): void {
    if (entry.cleaned) return
    entry.cleaned = true
    this.entries.delete(entry)
    this.finalReg.unregister(entry)
    try {
      void entry.close()
    } catch {
      // Cleanup errors are swallowed — there's no caller to surface them to,
      // and a throw here would break other entries' cleanup in the same scan.
    }
  }
}
