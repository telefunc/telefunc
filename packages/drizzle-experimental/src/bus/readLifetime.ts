export { createReadLifetime, invalidationSink, registerAbandonedRead }
export type { ReadLifetime }

import type { ReadToken } from '../engine/graph/registry.js'
import type { SubscriptionRef } from './changeRuntime.js'

/** The two refs a live read holds from the moment a graph token joins its change subscription until the read
 *  ends — under ONE owner that knows WHICH of the three ways it ended. The token and the subscription have
 *  the same owner at every moment, so they travel together; `redeemed` is this object's PRIVATE state, not a
 *  field a caller reads, because whether activation has happened is the whole of what distinguishes abandon's
 *  two behaviors, and `token.release()` on a redeemed token is a usage error rather than a no-op. */
type ReadLifetime = {
  /** Serialize-time activation: redeem the token and hand back the teardown the channel lease runs on close. */
  activate(): () => void
  /** The handle was collected without ever being serialized: give both refs back — UNLESS activation
   *  already did, in which case the channel lease owns them and touching them here would throw. */
  abandon(): void
  /** The read failed before it produced a handle to own these: give both back now, deterministically,
   *  rather than leaving them for a collector that has nothing to collect. */
  fail(): void
}

function createReadLifetime(token: ReadToken, subscription: SubscriptionRef): ReadLifetime {
  let redeemed = false
  const releaseBoth = () => {
    token.release()
    subscription.release()
  }
  return {
    activate() {
      const lease = token.redeem()
      redeemed = true
      return () => {
        lease.release()
        subscription.release()
      }
    },
    abandon() {
      if (redeemed) return // activated: the wire channel's lease owns these and releases them on close
      releaseBoth()
    },
    fail: releaseBoth,
  }
}

/** Reclaims an ABANDONED live read once its handle is collected — the finalizer simply asks the lifetime to
 *  abandon itself.
 *
 *  The registered value must not reach the Live, or the Live is never collected and this never fires. That is
 *  why the graph's `notify` goes through `invalidationSink()` below: the token closes over `notify`, and the
 *  finalizer holds the lifetime that holds the token, so a `notify` that closed over the Live strongly would
 *  keep every abandoned handle alive forever — a silently dead net that still looks like a net. */
const abandonedReads = new FinalizationRegistry<ReadLifetime>((lifetime) => lifetime.abandon())

/** Register the handle as the weak target, while the lifetime alone is held as the finalizer value. */
function registerAbandonedRead(target: object, lifetime: ReadLifetime): void {
  abandonedReads.register(target, lifetime)
}

/** The graph's invalidation sink for one read, holding its invalidatable target WEAKLY.
 *
 *  Deliberately its own function so its closure scope contains the WeakRef and nothing else: closures
 *  declared in one scope share a context, so building this inline beside a strong target binding could
 *  retain the target through that shared context and defeat the finalizer. A collected target simply stops
 *  receiving invalidations, which is correct — nobody is holding it to receive them. */
function invalidationSink<T extends { invalidate(): void }>(): { notify: () => void; hold: (target: T) => void } {
  let ref: WeakRef<T> | undefined
  return {
    notify: () => ref?.deref()?.invalidate(),
    hold: (target) => {
      ref = new WeakRef(target)
    },
  }
}
