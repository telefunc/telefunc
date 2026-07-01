export { macrotaskYield }

import { getGlobalObject } from '../../utils/getGlobalObject.js'

/**
 * Process-wide macrotask-boundary yield with sub-millisecond cost.
 *
 * `setTimeout(0)` would also yield to macrotasks but browsers clamp it to
 * ~4 ms after a few nested calls (HTML spec, "timer nesting level ≥ 5"),
 * which dominates the throttle budget. `MessageChannel.postMessage` is a
 * macrotask too but has no clamp — typical resume latency is sub-ms.
 *
 * Concurrent `yield()` calls coalesce: only one `postMessage` is in flight
 * at a time, and when its `onmessage` fires every queued waiter resumes in
 * enqueue order. So N senders that all need to breathe at once cost one
 * macrotask, not N.
 */

class MacrotaskYield {
  private channel: MessageChannel | null = null
  private waiters: Array<() => void> = []
  private posted = false

  yield(): Promise<void> {
    return new Promise<void>((resolve) => {
      this.waiters.push(resolve)
      if (this.posted) return
      this.posted = true
      this.ensureChannel().port2.postMessage(null)
    })
  }

  /** Constructed on first yield, not at module load — some toolchains evaluate the
   *  module in a context without `MessageChannel` (Cloudflare Vite plugin
   *  pre-bundling). Workers / Node have it at runtime. */
  private ensureChannel(): MessageChannel {
    if (this.channel) return this.channel
    const channel = new MessageChannel()
    channel.port1.onmessage = () => {
      this.posted = false
      const waiters = this.waiters.splice(0)
      for (const w of waiters) w()
    }
    // Node: don't keep the process alive just for this channel.
    const port = channel.port1 as { unref?: () => void }
    port.unref?.()
    this.channel = channel
    return channel
  }
}

const macrotaskYield = getGlobalObject<{ instance: MacrotaskYield }>('flow-control/macrotask-yield.ts', () => ({
  instance: new MacrotaskYield(),
})).instance
